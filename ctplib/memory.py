"""Memory sizes: parse a DuckDB size, read MemAvailable, warn when a run does not fit."""

from __future__ import annotations

import re
from pathlib import Path

import retain

_SIZE_RE = re.compile(r"^(\d+(?:\.\d+)?)\s?(B|K|M|G|T|KB|MB|GB|TB|KIB|MIB|GIB|TIB)$")
_SIZE_UNITS = {
    "B": 1,
    "K": 1024,
    "KB": 1024,
    "KIB": 1024,
    "M": 1024**2,
    "MB": 1024**2,
    "MIB": 1024**2,
    "G": 1024**3,
    "GB": 1024**3,
    "GIB": 1024**3,
    "T": 1024**4,
    "TB": 1024**4,
    "TIB": 1024**4,
}

# memory_limit bounds DuckDB's buffer manager, not the process; RSS runs about this much higher.
SETTLE_RATIO = 1.4


def size_to_bytes(text: str) -> int:
    """Bytes for a DuckDB size string, else ValueError. Mirrors parse_memory_limit()
    in generate_dataset.py, so a bad value fails before the run, not hours in at step 10."""
    cleaned = text.strip()
    if cleaned.endswith("%"):
        raise ValueError(
            f"--memory-limit takes an absolute size, not a percentage (got {text!r}). "
            "A percentage measures total RAM, and only the free part is usable."
        )
    m = _SIZE_RE.match(cleaned.upper())
    if not m:
        raise ValueError(f"cannot read {text!r} as a memory size. Example: 3GB")
    return int(float(m.group(1)) * _SIZE_UNITS[m.group(2)])


def available_bytes() -> int | None:
    """MemAvailable from /proc/meminfo, or None when it cannot be read."""
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        return None
    return None


def memory_budget_warning(limit: str, jobs: int) -> str | None:
    """A warning when SETTLE_RATIO x jobs x limit does not fit in MemAvailable, else None."""
    available = available_bytes()
    if available is None:
        return None
    want = int(size_to_bytes(limit) * SETTLE_RATIO * jobs)
    if want <= available:
        return None
    return (
        f"memory budget: {jobs} concurrent x {limit} settles at about "
        f"{retain.human(want)}, but only {retain.human(available)} is available. "
        f"Step 10 can exhaust RAM and the kernel or the harness will kill the "
        f"run. Lower --memory-limit or --jobs."
    )


def mem_available_gb(meminfo: Path = Path("/proc/meminfo")) -> float | None:
    """MemAvailable in GiB, or None where the kernel does not say."""
    try:
        for line in meminfo.read_text().splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) / 2**20
    except (OSError, ValueError, IndexError):
        return None
    return None
