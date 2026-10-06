"""The audit ledger: one JSON object per line, one line per project and per step.

The file is append-only. Each row is written whole, under an exclusive flock, and
fsynced before the call returns, so two runs or two threads never interleave a row
and a crash loses at most the row being written. Nothing rewrites or truncates it.

Every row carries enough to audit its step alone: the run, the manifest row, the
pinned and checked-out commits, the tool versions and the flags. A project's last
row has step "project" and gives its state: done, done-dirty, failed, deferred or
skipped. done-dirty means the Parquet is valid but the cleanup did not finish."""
from __future__ import annotations

import fcntl
import json
import os
import sys
import threading
from dataclasses import dataclass, field
from pathlib import Path

VERSION = 1
FINAL_STEP = "project"
DONE = "done"
DONE_DIRTY = "done-dirty"
FAILED = "failed"
DEFERRED = "deferred"
SKIPPED = "skipped"
STATES = (DONE, DONE_DIRTY, FAILED, DEFERRED, SKIPPED)

_thread_lock = threading.Lock()


def append(path: Path, row: dict) -> None:
    """Append one row. Raises OSError when the row could not be made durable."""
    data = (json.dumps(row, ensure_ascii=False, default=str) + "\n").encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    with _thread_lock:
        fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            view = memoryview(data)
            while view:
                written = os.write(fd, view)
                view = view[written:]
            os.fsync(fd)
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)


def read(path: Path) -> list[dict]:
    """Every whole row. A torn last line, from a crash mid-write, is skipped with a note."""
    if not path.exists():
        return []
    rows = []
    lines = path.read_text(encoding="utf-8").splitlines()
    for lineno, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except ValueError:
            print(f"ledger: {path}:{lineno} is not a whole JSON row, skipped",
                  file=sys.stderr)
    return rows


@dataclass
class History:
    """What the ledger says about one project."""
    state: str = ""            # the last final row's state, "" when there is none
    failures: int = 0          # final rows with state failed
    attempts: int = 0          # final rows of any state except skipped and deferred
    last_failed_step: str = ""
    rows: list = field(default_factory=list)


def histories(rows: list[dict]) -> dict[str, History]:
    """project name -> History, in ledger order."""
    out: dict[str, History] = {}
    for row in rows:
        name = row.get("project")
        if not name:
            continue
        h = out.setdefault(name, History())
        h.rows.append(row)
        if row.get("step") != FINAL_STEP:
            continue
        state = row.get("state", "")
        if state in (SKIPPED, DEFERRED):
            continue
        h.state = state
        h.attempts += 1
        if state == FAILED:
            h.failures += 1
            h.last_failed_step = row.get("failed_step", "")
    return out


def project_rows(rows: list[dict], name: str) -> list[dict]:
    return [r for r in rows if r.get("project") == name]
