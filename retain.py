#!/usr/bin/env python3
"""retain — prune a FINISHED project workdir down to what the research needs.

    ./retain.py                      # dry run over every finished project
    ./retain.py jq zstd              # dry run over the named projects
    ./retain.py --apply              # actually delete (dry run is the default)

Why. memo/ plus html/ is 68-96% of a workdir, so a large corpus does not fit
on disk if both are kept for every project. The research needs only
<name>-dataset.parquet (2.4-6.7 MB per project) and the project's metrics.tsv
row. docs/DESIGN.md section 6 already classes memo/ and html/ as disposable;
this script is that policy, executable.

Cost of dropping memo/: blobExec loses its blob-to-token cache, so a later
incremental re-run of that project re-tokenizes from scratch. Accepted for a
FINISHED project, because the parquet is the product. Not accepted while a run
is in progress: there memo/ is a live cache, not a leftover, and the liveness
guard below is what keeps this script off it. A validated project IS re-run —
`--from-step 2` re-runs keep the stamp from the earlier run — so the stamp alone
never means "idle".

Safety. Dry run is the default; --apply is required before anything is removed.
Every guard lives in prune(), because prune() is where the deletes happen and
both entry points reach it. Each guard is documented in full where it runs:

  * output_dir sanity — output_dir_refusal()
  * one plain project name — check_name()
  * the project is finished — finished()
  * the project is idle — live()
  * only <output_dir>/<project>/{memo,html} is ever removed — check_target()
  * protected/unreadable entries refused, non-directories skipped — scan()

Finished is NOT idle, and conflating them is how this script would destroy a
running job. The <name>.validated stamp means "this project finished
successfully at some point in the past". It does not mean "nothing is using this
workdir now", and the two come apart on every `--from-step 2` re-run of an
already-validated project: the stamp is present for the whole re-run while
blobExec reads and writes memo/ as its blob-to-token cache. Deleting it there is
silent destruction of hours of work, not a reclaim. Hence live(), keyed on the
lock the orchestrator actually holds rather than on any file in the workdir —
run_pipeline_process.sh deletes the workdir at FROM_STEP=1 and again from its
EXIT trap, so a lock kept inside the workdir would go with it.

A live project is SKIPPED, one project at a time; --apply does not refuse the
whole sweep because some other project is live. Reasons, in order: the corpus
run is long and parallel, so during a multi-day build some project is nearly
always live, and an any-live refusal would make retention impossible exactly
when disk pressure is highest — ctp.py defers projects below its 150 GB floor,
so "cannot prune because a run is live" plus "cannot run because disk is low" is
a deadlock; the blast radius is per project, since workdirs do not share state
and the guard is keyed on that project's own lock; and ctp.py's
`run --drop-memo` prunes from inside a live run by design, so an any-live
refusal would contradict the one caller that prunes during a run. Against that:
an abort is louder and would also spare the live run this script's stat() I/O.
That is a scheduling cost, not a data-safety one — run the sweep under nice and
ionice — and the LIVE line in the summary is the loud signal.

A refusal, a skip and a live project all make the exit status non-zero, but they
differ in blast radius. A refusal means the walk did not understand this
project, so under --apply nothing at all is deleted for it. A skip means only
that this one subtree is not a directory to remove, which says nothing about its
siblings, so the others are still pruned — and a live project, per above, is
left alone entirely. The output directory comes from pipeline.cfg, read the same
way ctp.py reads it.

How dry run and --apply differ is documented on prune() itself, where the two
modes are implemented.

Shared entry point: prune() is also called by `ctp.py run --drop-memo`, so the
post-run cleanup and this script delete through exactly one code path.

Stdlib only.
"""

from __future__ import annotations

import argparse
import configparser
import fcntl
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

# .resolve() canonicalizes to /local/home form, matching ctp.py — the two
# scripts must agree on the output dir string or the guards below misfire.
CORPUS = Path(__file__).resolve().parent
_cfg = configparser.ConfigParser()
_cfg.read(CORPUS / "pipeline.cfg")


def _cfg_path(key: str, default: str) -> Path:
    raw = _cfg.get("paths", key, fallback=default)
    return (CORPUS / Path(raw).expanduser()).resolve()


OUT = _cfg_path("output_dir", "../cregit-workspace/corpus-files")

# ctp.py's state directory, mirrored here. ctp.py defines STATE = CORPUS /
# "state" and the per-project lock as state_dir(name) / ".lock"; both scripts
# compute CORPUS as Path(__file__).resolve().parent from this same directory, so
# the two agree. Spelled out rather than imported: ctp.py already imports retain,
# and importing it back would be a cycle that drags the whole orchestrator in to
# read one constant.
#
# Not configurable, because ctp.py does not make it configurable. If STATE ever
# moves into pipeline.cfg, it must move in both files at once — a stale copy here
# would silently look for locks where there are none and report every project
# idle, which is the failure this guard exists to prevent.
STATE = CORPUS / "state"

# The only subtrees this script may remove, relative to a project workdir.
DISPOSABLE = ("memo", "html")

# Never removed. None of these can sit inside a disposable subtree today, so
# the check is belt-and-braces against a future DISPOSABLE entry being wrong:
# finding any of them inside a target aborts that project.
PROTECTED_NAMES = frozenset({"metrics.tsv", "runs.log", "ctp.duckdb", ".git"})
PROTECTED_SUFFIXES = (".parquet", ".validated")


def say(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def human(n: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if n < 1024:
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PiB"


def _note(skipped: list[str]) -> str:
    """A log fragment naming the skipped subtrees, empty when there are none."""
    if not skipped:
        return ""
    return f", {len(skipped)} skipped ({', '.join(f'{s}/' for s in skipped)})"


def is_protected(name: str) -> bool:
    return name in PROTECTED_NAMES or name.endswith(PROTECTED_SUFFIXES)


def scan(root: Path) -> tuple[int, int, list[str]]:
    """Walk root without following symlinks.

    Returns (disk_bytes, entries, violations). disk_bytes is st_blocks * 512 —
    the space the filesystem actually gives back, which is what `du` reports
    and what the disk budget cares about. A non-empty violations list means the
    caller must refuse to delete this subtree.
    """
    violations: list[str] = []
    try:
        total = root.stat(follow_symlinks=False).st_blocks * 512
    except OSError as exc:
        return 0, 0, [f"cannot stat {root} ({exc})"]

    entries = 0
    stack = [root]
    while stack:
        current = stack.pop()
        try:
            with os.scandir(current) as it:
                children = list(it)
        except OSError as exc:
            violations.append(f"unreadable {current} ({exc})")
            continue
        for entry in children:
            entries += 1
            if is_protected(entry.name):
                violations.append(f"protected entry inside subtree: {entry.path}")
            try:
                total += entry.stat(follow_symlinks=False).st_blocks * 512
            except OSError as exc:
                violations.append(f"cannot stat {entry.path} ({exc})")
                continue
            if entry.is_dir(follow_symlinks=False):
                stack.append(Path(entry.path))
    return total, entries, violations


def check_target(workdir: Path, subtree: str) -> Path:
    """Return the path to remove, or raise ValueError explaining the refusal."""
    if subtree not in DISPOSABLE:
        raise ValueError(f"{subtree!r} is not one of the disposable subtrees {DISPOSABLE}")
    target = workdir / subtree
    if target.is_symlink():
        raise ValueError(f"{target} is a symlink")
    resolved = target.resolve()
    if not resolved.is_relative_to(OUT):
        raise ValueError(f"{resolved} is outside output_dir {OUT}")
    if resolved.name != subtree or resolved.parent.parent != OUT:
        raise ValueError(f"{resolved} is not <output_dir>/<project>/{subtree}")
    return target


def output_dir_refusal() -> str | None:
    """Why OUT is too dangerous to walk, or None when it is safe.

    Sanity, not security: a misread pipeline.cfg that left output_dir at / or ~
    would make this script walk, measure and offer to delete the whole machine.

    Both entry points call this — main() for the command line, prune() for
    ctp.py run --drop-memo. The check used to sit in main() alone, so the
    --drop-memo path, the one that runs once per project, never enforced it. A
    stated safety rule must hold where the deletes actually happen.
    """
    if OUT in (Path("/"), Path.home()):
        return "that is the filesystem root or the home directory"
    if len(OUT.parts) < 3:
        return f"that path has only {len(OUT.parts)} part(s)"
    return None


def check_name(name: str) -> None:
    """Raise ValueError unless name is one plain project directory name.

    prune() builds every path as OUT / name, and OUT / "../evil" or OUT / "/etc"
    resolves outside OUT. finished() does not object, because the parquet and
    stamp lookups follow the traversal too, so without this check the only thing
    standing between a typo and a delete outside output_dir is check_target's
    resolved-path comparison. check_target stays — defence in depth — but the
    refusal belongs here, before anything is read.

    The four rejects are deliberately one statement. On POSIX an absolute path
    always contains "/", so a separate branch for it could never be taken.
    """
    if not name or "/" in name or ".." in name or Path(name).is_absolute():
        raise ValueError(f"{name!r} is not one plain project name")


def lock_path(name: str) -> Path:
    """ctp.py's single-instance lock for one project.

    Spelled exactly as ctp.py spells it — state_dir(name) / ".lock", where
    state_dir is STATE / name. Deliberately outside the workdir: the runner
    deletes the workdir at FROM_STEP=1 and again from its EXIT trap, so a lock
    kept inside it would vanish with it and every run would look idle.

    Call this only with a name check_name() has passed. STATE / "../x" escapes
    STATE exactly the way OUT / "../x" escapes OUT, and a lookup outside STATE
    would answer the wrong question.
    """
    return STATE / name / ".lock"


def _held_by_this_process(lockfile: Path) -> bool:
    """True when THIS process already has lockfile open.

    flock locks belong to an open file description, not to a process: a second fd
    on a file this process has already locked is refused exactly as another
    process's would be (verified, not assumed). That matters because ctp.py
    reaches prune() from `run --drop-memo` while still holding the project's own
    lock — the call sits inside the try whose finally closes the lockfile. A
    liveness guard that did not except the caller's own lock would therefore
    refuse every single --drop-memo prune, memo/ would survive for all 1,423
    projects, and the disk-frugal corpus run would fill the disk instead. The
    guard has to distinguish "another run owns this workdir" from "the run asking
    owns it", and this is that distinction.

    Reads /proc/self/fd, so it is Linux-only. Without procfs it says False, which
    makes the caller's own lock look foreign: the prune is then skipped rather
    than performed, which is the safe way to be wrong.
    """
    try:
        want = lockfile.stat()
    except OSError:
        return False
    try:
        fds = os.listdir("/proc/self/fd")
    except OSError:
        return False
    for fd in fds:
        try:
            got = os.stat(f"/proc/self/fd/{fd}")
        except OSError:
            continue  # closed under us, or the directory handle itself
        if (got.st_dev, got.st_ino) == (want.st_dev, want.st_ino):
            return True
    return False


def lock_held(lockfile: Path) -> bool:
    """True when an flock on lockfile cannot be taken, i.e. someone holds it.

    This is the third copy of this predicate in the repo: ctp.py's _lock_held and
    consolidate.py's lock_held are the other two. Duplicated deliberately.
    consolidate.py imports duckdb at module scope and duckdb comes from the devenv
    shell, not from .venv, so `import consolidate` would make retain.py
    unimportable outside devenv — and retain.py is stdlib-only on purpose, because
    it is what `ctp.py run --drop-memo` calls on every project. Eight lines of
    stdlib are the cheaper of the two dependencies. Keep the three in step.

    Two deliberate differences from those two copies:

      * opened "r", not "w". flock ignores the open mode on Linux, while "w"
        truncates the very file it is inspecting; this script never writes
        anything under state/.
      * a lock file that exists but cannot be opened counts as HELD. The question
        being asked is "is it safe to delete this", and the safe answer to "I
        cannot tell" is no.

    The probe takes the lock for the microseconds before it releases it, so a run
    starting in that window sees BlockingIOError and defers the project — one
    deferred project, no data lost. Both other copies have the same window.
    """
    if not lockfile.exists():
        return False
    try:
        with lockfile.open("r") as f:
            try:
                fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            fcntl.flock(f, fcntl.LOCK_UN)
            return False
    except OSError:
        return True


def live(name: str) -> bool:
    """True when a pipeline run other than this caller is working on name.

    This is the "is anything using it" test, and the <name>.validated stamp is
    not — see the module docstring on why finished and idle are different
    questions.
    """
    lockfile = lock_path(name)
    if _held_by_this_process(lockfile):
        return False
    return lock_held(lockfile)


def finished(name: str) -> tuple[bool, str]:
    """A project is finished when ctp.py stamped it AND the parquet is real."""
    workdir = OUT / name
    if not workdir.is_dir():
        return False, "no workdir"
    parquet = workdir / f"{name}-dataset.parquet"
    if not parquet.is_file():
        return False, f"missing keeper {parquet.name}"
    if parquet.stat().st_size == 0:
        return False, f"empty keeper {parquet.name}"
    stamp = workdir / f"{name}.validated"
    if not stamp.is_file() or stamp.stat().st_size == 0:
        return False, f"missing or empty ctp stamp {stamp.name}"
    return True, f"{parquet.name} ({human(parquet.stat().st_size)})"


def finished_projects() -> list[str]:
    """Every finished project, in name order.

    The predicate is finished(), not a second copy of it. This lister used to
    check only that the stamp file existed, so a project with a zero-byte stamp
    was listed here and then refused by finished(), and a whole-corpus dry run
    exited 1 naming a project the user never asked about. One definition of
    finished, used everywhere.
    """
    if not OUT.is_dir():
        return []
    return [d.name for d in sorted(OUT.iterdir())
            if d.is_dir() and finished(d.name)[0]]


def remove_tree(target: Path) -> list[str]:
    """Delete target. Return the per-entry errors, empty when it all went.

    shutil.rmtree raises out of the middle of a tree. Unwrapped, one permission
    error part-way through a project would raise out of prune() and out of
    main(), abandoning every project after it with a traceback instead of a
    per-project SKIP and a non-zero exit. onexc collects each failure instead of
    raising, so the caller can report this project and carry on with the next.
    """
    errors: list[str] = []

    def collect(func, path, exc) -> None:
        errors.append(f"{path} ({exc})")

    shutil.rmtree(target, onexc=collect)
    return errors


def prune(name: str, subtrees: tuple[str, ...] = DISPOSABLE,
          *, apply: bool = False) -> tuple[int, bool]:
    """Prune one project's disposable subtrees.

    Returns (bytes, ok). ok=False means the project was not pruned and the
    caller must exit non-zero. Nothing is removed unless apply is True. The
    signature is the one ctp.py calls as prune(name, ("memo",), apply=True), and
    a refusal is a (0, False) return rather than an exception, so that the
    --drop-memo path keeps its per-project SKIP instead of dying.

    Two phases. Phase one measures every requested subtree and sorts it into one
    of four outcomes; it never deletes. Phase two deletes, and only runs when
    phase one refused nothing.

      * absent — no such path. Normal, for example a project run with
        --skip-html. Not a problem at all.
      * skipped — the path exists but is not a directory, so there is no tree to
        remove. Reported, and it makes ok False, but it does not touch its
        siblings: a stray file named memo says nothing about html/. It used to
        cost the whole project's reclaim.
      * refused — check_target objected, the walk could not read the subtree, or
        the subtree holds a protected name. The walk did not understand this
        project.
      * measured — clean, and in phase two removed.

    The two modes differ on purpose:

      * DRY RUN keeps going after a refusal. It measures every subtree, reports
        each refusal, and returns a complete reclaimable total. A refused memo/
        used to hide html/ from the total, so a capacity plan built over a partly
        blocked corpus read low — and that plan decides whether the corpus fits
        on disk. ok is still False, because the project is not fully prunable.
      * APPLY fails closed. One refusal anywhere in the project deletes nothing
        for that project, not even a subtree that passed.
    """
    refusal = output_dir_refusal()
    if refusal:
        say(f"{name} — SKIP: refusing to operate on output_dir {OUT}, {refusal}")
        return 0, False

    try:
        check_name(name)
    except ValueError as exc:
        say(f"SKIP: {exc}")
        return 0, False

    # Before finished(), and before any stat of the workdir, so the reason
    # reported is the one that matters and the live run is not asked for I/O it
    # did not want. The guard is here, not in main(), because ctp.py calls
    # prune() directly: a check in main() alone would leave the --drop-memo path
    # — the one that runs on every project in the corpus — unguarded, which is
    # exactly the mistake output_dir_refusal() was moved down here to fix.
    if live(name):
        say(f"{name} — SKIP: LIVE, another run holds {lock_path(name)}. memo/ is "
            f"blobExec's cache while it runs, so nothing was measured or deleted")
        return 0, False

    ok, why = finished(name)
    if not ok:
        say(f"{name} — SKIP: not finished ({why})")
        return 0, False

    workdir = OUT / name
    plan: list[tuple[Path, str, int]] = []
    refused: list[str] = []
    skipped: list[str] = []
    measured = 0

    for subtree in subtrees:
        try:
            target = check_target(workdir, subtree)
        except ValueError as exc:
            say(f"{name} — REFUSE {subtree}/: {exc}")
            refused.append(subtree)
            continue
        if not target.exists():
            say(f"{name}    {subtree}/ absent, nothing to do")
            continue
        if not target.is_dir():
            # A stray FILE named memo used to raise NotADirectoryError out of
            # scandir, become an "unreadable" violation, and cost the whole
            # project. It is its own case: there is no tree to remove here, and
            # that says nothing about the sibling subtrees.
            say(f"{name} — SKIP {subtree}/: exists but is not a directory, "
                f"leaving it alone")
            skipped.append(subtree)
            continue
        size, entries, violations = scan(target)
        if violations:
            for violation in violations[:5]:
                say(f"{name} — REFUSE {subtree}/: {violation}")
            refused.append(subtree)
            continue
        say(f"{name}    {'measured' if apply else 'would delete'} "
            f"{subtree}/ — {human(size)} in {entries} entries")
        plan.append((target, subtree, size))
        measured += size

    if refused:
        blocked = ", ".join(f"{s}/" for s in refused)
        if apply:
            say(f"{name} — SKIP: {len(refused)} refused subtree(s) ({blocked}),"
                f" so nothing was deleted for this project")
            return 0, False
        say(f"{name} — reclaimable {human(measured)} over the subtrees that "
            f"passed, {len(refused)} refused ({blocked}) | keeping {why}")
        return measured, False

    if not apply:
        say(f"{name} — reclaimable {human(measured)}{_note(skipped)}"
            f" | keeping {why}")
        return measured, not skipped

    reclaimed = 0
    for target, subtree, size in plan:
        errors = remove_tree(target)
        if errors:
            for err in errors[:5]:
                say(f"{name} — SKIP {subtree}/: cannot remove {err}")
            # rmtree removes entries as it walks, so the tree can be half gone.
            # Say which, because the caller must not treat this project as
            # pruned, and its reclaimed bytes are not knowable.
            state = "still present" if target.exists() else "gone"
            say(f"{name} — SKIP {subtree}/: PARTIAL DELETE, {subtree}/ is "
                f"{state} and this project is NOT pruned")
            return reclaimed, False
        say(f"{name}    deleted {subtree}/ — {human(size)}")
        reclaimed += size

    say(f"{name} — reclaimed {human(reclaimed)}{_note(skipped)}"
        f" | keeping {why}")
    return reclaimed, not skipped


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("projects", nargs="*",
                    help="project names (default: every finished project)")
    ap.add_argument("--apply", action="store_true",
                    help="actually delete; without it nothing is removed")
    args = ap.parse_args()

    refusal = output_dir_refusal()
    if refusal:
        sys.exit(f"refusing to operate on output_dir {OUT}: {refusal}")
    if not OUT.is_dir():
        say(f"output_dir does not exist: {OUT}")
        return 1

    names = args.projects or finished_projects()
    if not names:
        say(f"no finished projects under {OUT}")
        return 0

    say(f"output_dir  {OUT}")
    say(f"mode        {'APPLY — deleting' if args.apply else 'DRY RUN — nothing is deleted'}")
    say("disposable  " + ", ".join(f"{s}/" for s in DISPOSABLE))
    say(f"projects    {len(names)}: {' '.join(names)}")
    free_before = shutil.disk_usage(OUT).free

    total = 0
    skipped: list[str] = []
    running: list[str] = []
    for name in names:
        got, ok = prune(name, apply=args.apply)
        total += got
        if not ok:
            # Two disjoint lists, because the causes and the remedies differ: a
            # live project needs nothing but a later re-run, while an unvalidated
            # or refused one needs looking at. The predicate is prune()'s own
            # live(), called again only to label the summary — prune() remains the
            # single guard. If the run ends in between, the project lands under
            # SKIPPED instead, which is still true: it was not pruned.
            (running if live(name) else skipped).append(name)

    verb = "reclaimed" if args.apply else "reclaimable"
    say(f"TOTAL {verb} {human(total)} over "
        f"{len(names) - len(skipped) - len(running)} project(s)")
    if args.apply:
        free_after = shutil.disk_usage(OUT).free
        say(f"disk free {human(free_before)} -> {human(free_after)}")
    else:
        say("nothing was deleted — re-run with --apply to reclaim it")
    if running:
        # Named separately from SKIPPED on purpose. This is not a fault and needs
        # no investigation: a pipeline run owns these workdirs, and the remedy is
        # to sweep again once it finishes. Still non-zero, because the sweep did
        # not do all the work it was asked to do.
        say(f"LIVE {len(running)} project(s), a running pipeline holds the lock, "
            f"left untouched: {' '.join(running)}")
    if skipped:
        # "not pruned", not "not finished": a project is also skipped when a
        # subtree is refused or a delete fails, and the summary must not claim
        # those projects were unfinished. In a dry run the TOTAL above still
        # counts the subtrees of these projects that passed.
        say(f"SKIPPED {len(skipped)} project(s), not pruned: {' '.join(skipped)}")
    if skipped or running:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
