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
A project is pruned only when it is finished: <name>-dataset.parquet must exist
and be non-empty, and ctp.py's <name>.validated stamp must be present. Anything
else is skipped with a warning and makes the exit status non-zero. Only
<output_dir>/<project>/{memo,html} is ever removed, and the walk refuses the
subtree if it finds a protected name inside it. The output directory comes from
pipeline.cfg, read the same way ctp.py reads it.

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
    """Every project ctp.py stamped DONE, in name order."""
    if not OUT.is_dir():
        return []
    return [d.name for d in sorted(OUT.iterdir())
            if d.is_dir() and (d / f"{d.name}.validated").is_file()]


def prune(name: str, subtrees: tuple[str, ...] = DISPOSABLE,
          *, apply: bool = False) -> tuple[int, bool]:
    """Prune one project's disposable subtrees.

    Returns (reclaimed_bytes, ok). ok=False means the project was skipped and
    the caller should exit non-zero. Nothing is removed unless apply is True.
    """
    ok, why = finished(name)
    if not ok:
        say(f"{name} — SKIP: not finished ({why})")
        return 0, False

    workdir = OUT / name
    reclaimed = 0
    for subtree in subtrees:
        try:
            target = check_target(workdir, subtree)
        except ValueError as exc:
            say(f"{name} — SKIP {subtree}/: refusing, {exc}")
            return reclaimed, False
        if not target.exists():
            say(f"{name}    {subtree}/ absent, nothing to do")
            continue
        size, entries, violations = scan(target)
        if violations:
            for v in violations[:5]:
                say(f"{name} — SKIP {subtree}/: {v}")
            return reclaimed, False
        say(f"{name}    {'delete' if apply else 'would delete'} "
            f"{subtree}/ — {human(size)} in {entries} entries")
        if apply:
            shutil.rmtree(target)
        reclaimed += size

    say(f"{name} — {'reclaimed' if apply else 'reclaimable'} {human(reclaimed)}"
        f" | keeping {why}")
    return reclaimed, True


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("projects", nargs="*",
                    help="project names (default: every finished project)")
    ap.add_argument("--apply", action="store_true",
                    help="actually delete; without it nothing is removed")
    args = ap.parse_args()

    if OUT in (Path("/"), Path.home()) or len(OUT.parts) < 3:
        sys.exit(f"refusing to operate on output_dir {OUT}")
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
        say(f"SKIPPED {len(skipped)} project(s), not finished: {' '.join(skipped)}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
