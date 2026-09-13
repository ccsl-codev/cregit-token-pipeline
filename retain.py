#!/usr/bin/env python3
"""retain — prune a FINISHED project workdir down to what the research needs.

    ./retain.py                      # dry run over every finished project
    ./retain.py jq zstd              # dry run over the named projects
    ./retain.py --apply              # actually delete (dry run is the default)

Why. The corpus grows to 1,423 projects on a 2.0 TB disk with ~1.2 TB free.
Measured pilot workdirs (4 projects, `du -sh`):

    project  workdir   memo/           html/
    jq        274 MB    123 MB (45%)    94 MB
    libuv     907 MB    643 MB (71%)   164 MB
    tmux      2.6 GB    2.3 GB (88%)   255 MB
    zstd      2.3 GB    1.9 GB (83%)   234 MB

memo/ plus html/ is 68-96% of a workdir. Keeping both for 1,423 projects needs
1.4-3.5 TB, so the corpus does not fit. Dropping both leaves roughly 155 GB.
The research needs only <name>-dataset.parquet (2.4-6.7 MB per project) and the
project's metrics.tsv row. docs/DESIGN.md section 6 already classes memo/ and
html/ as disposable; this script is that policy, executable.

Cost of dropping memo/: blobExec loses its blob-to-token cache, so a later
incremental re-run of that project re-tokenizes from scratch. Accepted — the
parquet is the product and a validated project is never re-run.

Safety. Dry run is the default; --apply is required before anything is removed.
Every guard lives in prune(), because prune() is where the deletes happen and
both entry points reach it:

  * output_dir must not be /, ~, or a path of fewer than three parts
  * the project name must be one plain directory name — no /, no .., not
    absolute, not empty — so that OUT / name cannot escape OUT
  * the project must be finished: <name>-dataset.parquet must exist and be
    non-empty, and ctp.py's <name>.validated stamp must be present and
    non-empty. finished() is the only definition of finished, and
    finished_projects() uses it too
  * only <output_dir>/<project>/{memo,html} is ever removed, checked against
    the resolved path
  * the walk refuses a subtree that holds a protected name or that it cannot
    read, and skips one that is not a directory

A refusal and a skip both make the exit status non-zero, but they differ in
blast radius. A refusal means the walk did not understand this project, so under
--apply nothing at all is deleted for it. A skip means only that this one
subtree is not a directory to remove, which says nothing about its siblings, so
the others are still pruned. The output directory comes from pipeline.cfg, read
the same way ctp.py reads it.

Dry run and --apply differ on purpose. A dry run measures every subtree even
after a refusal, so the reclaimable total is complete and a capacity plan built
over a partly blocked corpus does not read low. An --apply run deletes nothing
for a project that had any refusal at all: a refusal means the walk did not
understand that project, so no subtree of it is safe to remove.

Shared entry point: prune() is also called by `ctp.py run --drop-memo`, so the
post-run cleanup and this script delete through exactly one code path.

Stdlib only.
"""

from __future__ import annotations

import argparse
import configparser
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
    --drop-memo path, the one that runs 1,423 times, never enforced it. A stated
    safety rule must hold where the deletes actually happen.
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
    error part-way through project 200 raised out of prune() and out of main(),
    so the remaining 1,223 projects were abandoned with a traceback instead of a
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
        blocked corpus read low — and that plan decides whether 1,423 projects
        fit on disk. ok is still False, because the project is not fully
        prunable.
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
    for name in names:
        got, ok = prune(name, apply=args.apply)
        total += got
        if not ok:
            skipped.append(name)

    verb = "reclaimed" if args.apply else "reclaimable"
    say(f"TOTAL {verb} {human(total)} over {len(names) - len(skipped)} project(s)")
    if args.apply:
        free_after = shutil.disk_usage(OUT).free
        say(f"disk free {human(free_before)} -> {human(free_after)}")
    else:
        say("nothing was deleted — re-run with --apply to reclaim it")
    if skipped:
        # "not pruned", not "not finished": a project is also skipped when a
        # subtree is refused or a delete fails, and the summary must not claim
        # those projects were unfinished. In a dry run the TOTAL above still
        # counts the subtrees of these projects that passed.
        say(f"SKIPPED {len(skipped)} project(s), not pruned: {' '.join(skipped)}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
