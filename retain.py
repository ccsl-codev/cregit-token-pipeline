#!/usr/bin/env python3
"""Prune finished, idle project workdirs to their parquet by deleting memo/ and html/.
Usage: retain.py [--apply] [project ...]; a dry run unless --apply. Finished is not idle: a
--from-step re-run keeps the stamp, so live() checks ctp's lock, and a live project is skipped."""
from __future__ import annotations

import argparse
import configparser
import fcntl
import os
import shutil
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# Resolved exactly as ctp.py resolves it: the guards compare these path strings.
CORPUS = Path(__file__).resolve().parent
_cfg = configparser.ConfigParser()
_cfg.read(CORPUS / "pipeline.cfg")


def _cfg_path(key: str, default: str) -> Path:
    raw = _cfg.get("paths", key, fallback=default)
    return (CORPUS / Path(raw).expanduser()).resolve()


OUT = _cfg_path("output_dir", "../cregit-workspace/corpus-files")

# ctp.py's per-project logs and lock. Outside OUT, because the runner deletes
# OUT/<name> at FROM_STEP=1 and on failure. ctp.py and consolidate.py import these.
STATE = CORPUS / "state"

DISPOSABLE = ("memo", "html")

# Belt-and-braces against a wrong DISPOSABLE entry: one of these inside a target
# aborts that project.
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
    if not skipped:
        return ""
    return f", {len(skipped)} skipped ({', '.join(f'{s}/' for s in skipped)})"


def is_protected(name: str) -> bool:
    return name in PROTECTED_NAMES or name.endswith(PROTECTED_SUFFIXES)


def scan(root: Path) -> tuple[int, int, list[str]]:
    """(disk_bytes, entries, violations), not following symlinks. disk_bytes counts
    st_blocks * 512, what `du` reports. Any violation means: do not delete root."""
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
    """Why OUT is too dangerous to walk, or None. Sanity, not security: a misread
    pipeline.cfg leaving output_dir at / or ~ would offer to delete the machine."""
    if OUT in (Path("/"), Path.home()):
        return "that is the filesystem root or the home directory"
    if len(OUT.parts) < 3:
        return f"that path has only {len(OUT.parts)} part(s)"
    return None


def check_name(name: str) -> None:
    """Raise ValueError unless name is one plain directory name: OUT / "../x" escapes
    OUT. On POSIX an absolute path always contains "/", so the last test never decides."""
    if not name or "/" in name or ".." in name or Path(name).is_absolute():
        raise ValueError(f"{name!r} is not one plain project name")


def state_dir(name: str) -> Path:
    return STATE / name


def lock_path(name: str) -> Path:
    """Call only with a name check_name() has passed: STATE / "../x" escapes STATE."""
    return state_dir(name) / ".lock"


def _held_by_this_process(lockfile: Path) -> bool:
    """True when THIS process has lockfile open. flock refuses a second fd even within one
    process, and ctp.py prunes from `run --drop-memo` while it holds the project's lock.
    Linux-only (/proc/self/fd); without procfs it says False, so the prune is skipped."""
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
    """True when an flock on lockfile cannot be taken. Opened "r", because "w"
    truncates the file it probes. An unreadable lock counts as held: the safe
    answer to "is it safe to delete this" when we cannot tell is no."""
    if not lockfile.exists():
        return False
    try:
        with lockfile.open("r") as f:
            try:
                # A run starting in this instant defers once; no data is lost.
                fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            fcntl.flock(f, fcntl.LOCK_UN)
            return False
    except OSError:
        return True


def project_state(name: str, workdir: Path) -> str:
    """DONE, RUNNING, FAILED or QUEUED. state_dir is checked as well as workdir,
    because the runner deletes the workdir when the pipeline fails."""
    if (workdir / f"{name}.validated").exists():
        return "DONE"
    if lock_held(lock_path(name)):
        return "RUNNING"
    if state_dir(name).exists() or workdir.exists():
        return "FAILED"
    return "QUEUED"


def live(name: str) -> bool:
    """True when a pipeline run other than this caller is working on name. The stamp
    cannot answer this: a --from-step re-run keeps it while blobExec uses memo/."""
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
    """Every finished project, in name order, by finished() itself."""
    if not OUT.is_dir():
        return []
    return [d.name for d in sorted(OUT.iterdir())
            if d.is_dir() and finished(d.name)[0]]


def remove_tree(target: Path) -> list[str]:
    """Delete target and return the per-entry errors. onexc collects them, so one bad
    entry costs this project a SKIP instead of raising out of the whole sweep."""
    errors: list[str] = []

    def collect(func, path, exc) -> None:
        errors.append(f"{path} ({exc})")

    shutil.rmtree(target, onexc=collect)
    return errors


def _prune_keeper(name: str) -> str | None:
    """finished()'s note on what is kept, or None after saying why name may not be pruned."""
    refusal = output_dir_refusal()
    if refusal:
        say(f"{name} — SKIP: refusing to operate on output_dir {OUT}, {refusal}")
        return None

    try:
        check_name(name)
    except ValueError as exc:
        say(f"SKIP: {exc}")
        return None

    # Before finished() and any stat, so a live run is spared the I/O.
    if live(name):
        say(f"{name} — SKIP: LIVE, another run holds {lock_path(name)}. memo/ is "
            f"blobExec's cache while it runs, so nothing was measured or deleted")
        return None

    ok, why = finished(name)
    if not ok:
        say(f"{name} — SKIP: not finished ({why})")
        return None
    return why


def _measure_subtree(name: str, workdir: Path, subtree: str, apply: bool):
    """(kind, subtree), or ("measured", (target, subtree, size)) for a clean subtree."""
    try:
        target = check_target(workdir, subtree)
    except ValueError as exc:
        say(f"{name} — REFUSE {subtree}/: {exc}")
        return "refused", subtree
    if not target.exists():
        say(f"{name}    {subtree}/ absent, nothing to do")
        return "absent", subtree
    if not target.is_dir():
        # A stray file says nothing about the sibling subtrees, so only it is skipped.
        say(f"{name} — SKIP {subtree}/: exists but is not a directory, "
            f"leaving it alone")
        return "skipped", subtree
    size, entries, violations = scan(target)
    if violations:
        for violation in violations[:5]:
            say(f"{name} — REFUSE {subtree}/: {violation}")
        return "refused", subtree
    say(f"{name}    {'measured' if apply else 'would delete'} "
        f"{subtree}/ — {human(size)} in {entries} entries")
    return "measured", (target, subtree, size)


def _delete_plan(name: str, plan: list[tuple[Path, str, int]]) -> tuple[int, bool]:
    """(bytes reclaimed, ok). Stops at the first subtree that cannot be fully removed."""
    reclaimed = 0
    for target, subtree, size in plan:
        errors = remove_tree(target)
        if errors:
            for err in errors[:5]:
                say(f"{name} — SKIP {subtree}/: cannot remove {err}")
            # rmtree removes as it walks, so the tree can be half gone.
            state = "still present" if target.exists() else "gone"
            say(f"{name} — SKIP {subtree}/: PARTIAL DELETE, {subtree}/ is "
                f"{state} and this project is NOT pruned")
            return reclaimed, False
        say(f"{name}    deleted {subtree}/ — {human(size)}")
        reclaimed += size
    return reclaimed, True


def _report_refused(name: str, refused: list[str], measured: int, why: str,
                    apply: bool) -> tuple[int, bool]:
    blocked = ", ".join(f"{s}/" for s in refused)
    if apply:
        say(f"{name} — SKIP: {len(refused)} refused subtree(s) ({blocked}),"
            f" so nothing was deleted for this project")
        return 0, False
    say(f"{name} — reclaimable {human(measured)} over the subtrees that "
        f"passed, {len(refused)} refused ({blocked}) | keeping {why}")
    return measured, False


def prune(name: str, subtrees: tuple[str, ...] = DISPOSABLE,
          *, apply: bool = False) -> tuple[int, bool]:
    """(bytes, ok); ok=False means not pruned. ctp.py calls prune(name, ("memo",),
    apply=True). A dry run measures past a refusal so the total stays complete; apply
    fails closed: one refused subtree deletes nothing for the project."""
    why = _prune_keeper(name)
    if why is None:
        return 0, False

    by_kind = defaultdict(list)
    for subtree in subtrees:
        kind, payload = _measure_subtree(name, OUT / name, subtree, apply)
        by_kind[kind].append(payload)
    refused, skipped, plan = by_kind["refused"], by_kind["skipped"], by_kind["measured"]
    measured = sum(size for _, _, size in plan)

    if refused:
        return _report_refused(name, refused, measured, why, apply)

    if not apply:
        say(f"{name} — reclaimable {human(measured)}{_note(skipped)}"
            f" | keeping {why}")
        return measured, not skipped

    reclaimed, ok = _delete_plan(name, plan)
    if not ok:
        return reclaimed, False
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
            # Live needs only a later sweep; a skip needs looking at. prune() stays the guard.
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
        say(f"LIVE {len(running)} project(s), a running pipeline holds the lock, "
            f"left untouched: {' '.join(running)}")
    if skipped:
        say(f"SKIPPED {len(skipped)} project(s), not pruned: {' '.join(skipped)}")
    if skipped or running:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
