"""Timestamps and the event lines ctp prints."""

from __future__ import annotations

import threading
import time
from datetime import UTC, datetime

_print_lock = threading.Lock()


def now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def say(msg: str) -> None:
    with _print_lock:
        print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def utc(ts: float) -> str:
    """ISO 8601 in UTC, the same shape as now_iso()."""
    return time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(ts))
