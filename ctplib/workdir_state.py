"""What an earlier attempt left in the workdir of a project, and how the next attempt resumes."""

from __future__ import annotations

from pathlib import Path

import retain
from ctplib import console, toolchain


def check_memo_dir(name: str, opts: dict, workdir: Path) -> bool:
    """False, after saying why, when --memo-dir would put the memo inside the
    workdir the runner wipes."""
    memo_dir_opt = opts.get("memo_dir")
    if not memo_dir_opt:
        return True
    memo_dir = Path(memo_dir_opt).resolve() / name
    resolved_work = workdir.resolve()
    if memo_dir == resolved_work or resolved_work in memo_dir.parents:
        console.say(
            f"{name} — refusing to run: --memo-dir puts the memo at "
            f"{memo_dir}, inside the work directory the runner deletes. "
            "Point --memo-dir outside the corpus output directory."
        )
        return False
    return True


def checkout_matches(name: str, workdir: Path, pinned: str) -> tuple[bool, str]:
    """(ok, sha): which commit the runner blamed. ok is False when a pinned
    project's checkout is not the pinned commit, or cannot be read."""
    seen = toolchain.runner_checkout(name, workdir)
    if not pinned:
        console.say(f"{name} — unpinned, checked out {seen or 'an unreadable HEAD'}")
        return True, seen
    if seen != pinned:
        console.say(
            f"{name} ✗ pinned {pinned}, but the runner checked out {seen or 'an unreadable HEAD'}"
        )
        return False, seen
    console.say(f"{name} — checked out the pinned commit {pinned}")
    return True, seen


def stale_workdir(name: str, workdir: Path, pinned: str) -> str:
    """The commit an earlier attempt left in the workdir when it is not the pinned
    one, else "". Such a workdir holds work for another commit, so the runner may
    wipe it even when its memo is large (--force-clean)."""
    head = toolchain.git_head(workdir / f"{name}-original.git")
    return head if head and pinned and head != pinned else ""


def resume_step(name: str, workdir: Path, pinned: str) -> int:
    """2 when an earlier attempt left a clone of the right commit and a blob map, so
    the runner can resume tokenizing instead of refusing to wipe a big memo; else 1."""
    bare = workdir / f"{name}-original.git"
    if not (workdir / f"{name}-blobmap.db").is_file() or not bare.is_dir():
        return 1
    head = toolchain.git_head(bare)
    if not head or (pinned and head != pinned):
        return 1
    return 2


def derived_artifacts(name: str) -> tuple[str, ...]:
    """What steps 3 to 10 rebuild from the tokenized repo. The same list as the
    runner's drop_refold_derived_artifacts: step 6 cannot clone over an existing
    working clone, and blame of a stale sha must not survive."""
    return (
        "blame",
        "html",
        f"{name}-original",
        f"{name}-cregit",
        f"{name}-original.db",
        f"{name}-cregit.db",
        f"{name}-persons.db",
        f"{name}-persons.xls",
        f"{name}-dataset.parquet",
    )


def drop_derived(name: str, workdir: Path) -> tuple[list[str], list[str]]:
    """Delete the derived artifacts, keeping the bare repos, the blob map and the memo.
    Returns (dropped, errors)."""
    dropped, errors = [], []
    for entry in derived_artifacts(name):
        path = workdir / entry
        if not path.exists() and not path.is_symlink():
            continue
        if path.is_dir() and not path.is_symlink():
            errs = retain.remove_tree(path)
        else:
            try:
                path.unlink()
                errs = []
            except OSError as exc:
                errs = [f"{path} ({exc})"]
        (errors.extend(errs) if errs else dropped.append(entry))
    return dropped, errors
