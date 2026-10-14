"""Sample the CPU, memory and disk use of a running phase into resources.tsv."""

from __future__ import annotations

import os
import shutil
import threading
import time
from pathlib import Path

from ctplib import config, console, metrics

RESOURCE_SAMPLE_S = 15
RESOURCE_FIELDS = (
    "iso",
    "project",
    "class",
    "phase",
    "elapsed_s",
    "tree_rss_mb",
    "tree_procs",
    "cpu_pct",
    "mem_used_mb",
    "disk_free_gb",
    "load1",
)


def _proc_ppid_rss(proc_root: Path = Path("/proc")) -> dict[int, tuple[int, int]]:
    """Map pid -> (ppid, rss_kb) for every readable process. One that exits
    between listdir and read is skipped: cregit reaps short-lived processes constantly."""
    table: dict[int, tuple[int, int]] = {}
    for entry in proc_root.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            # Field 4 is ppid, field 24 rss in pages. Field 2, the command name,
            # may hold spaces and brackets, so split after the last ")".
            raw = (entry / "stat").read_text()
            fields = raw[raw.rindex(")") + 2 :].split()
            table[int(entry.name)] = (int(fields[1]), int(fields[21]) * 4)
        except (OSError, ValueError, IndexError):
            continue
    return table


def tree_usage(root_pid: int, table: dict | None = None) -> tuple[int, int]:
    """(RSS in MB, process count) for root_pid and its descendants: the pipeline
    shell spawns perl, git and srcml, so the child's own RSS understates the run.
    `seen` stops a cycle in a malformed parent map from hanging the sampler."""
    table = _proc_ppid_rss() if table is None else table
    children: dict[int, list[int]] = {}
    for pid, (ppid, _rss) in table.items():
        children.setdefault(ppid, []).append(pid)

    seen, stack, rss_kb = set(), [root_pid], 0
    while stack:
        pid = stack.pop()
        if pid in seen or pid not in table:
            continue
        seen.add(pid)
        rss_kb += table[pid][1]
        stack.extend(children.get(pid, ()))
    return rss_kb // 1024, len(seen)


def _cpu_jiffies() -> tuple[int, int]:
    """(busy, total) jiffies from /proc/stat, for a delta between two samples."""
    fields = [int(v) for v in Path("/proc/stat").read_text().split("\n")[0].split()[1:]]
    total = sum(fields)
    idle = fields[3] + (fields[4] if len(fields) > 4 else 0)
    return total - idle, total


def _mem_used_mb() -> int:
    """Used memory in MB: total minus available, per /proc/meminfo."""
    info = {}
    for line in Path("/proc/meminfo").read_text().splitlines():
        key, _, rest = line.partition(":")
        info[key] = int(rest.split()[0])
    return (info.get("MemTotal", 0) - info.get("MemAvailable", 0)) // 1024


class ResourceSampler:
    """Samples RAM, CPU and disk while one phase runs: every sample to resources.tsv,
    the maxima to .peak. A daemon thread with every sample wrapped, so a sampling
    failure can never fail the phase it measures."""

    def __init__(self, project: dict, phase: str, interval: int = RESOURCE_SAMPLE_S):
        self.name = project["name"]
        self.cls = project["size_class"]
        self.phase = phase
        self.interval = interval
        self.pid: int | None = None
        self.peak = dict(
            tree_rss_mb=0,
            tree_procs=0,
            cpu_pct=0.0,
            mem_used_mb=0,
            disk_free_gb_min=None,
            samples=0,
        )
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._started = time.time()

    def start(self, pid: int) -> None:
        self.pid = pid
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> dict:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=self.interval + 5)
        return self.peak

    def _loop(self) -> None:
        busy, total = _cpu_jiffies()
        # Sample before the first wait, or a phase shorter than the interval records nothing.
        while True:
            try:
                busy, total = self._sample(busy, total)
            except Exception as e:
                console.say(f"{self.name} resource sample failed: {e}")
            if self._stop.wait(self.interval):
                return

    def _sample(self, prev_busy: int, prev_total: int) -> tuple[int, int]:
        busy, total = _cpu_jiffies()
        span = total - prev_total
        cpu_pct = round(100.0 * (busy - prev_busy) / span, 1) if span > 0 else 0.0
        rss_mb, procs = tree_usage(self.pid) if self.pid else (0, 0)
        mem_mb = _mem_used_mb()
        free_gb = shutil.disk_usage(config.OUT).free // 2**30
        row = (
            console.now_iso(),
            self.name,
            self.cls,
            self.phase,
            int(time.time() - self._started),
            rss_mb,
            procs,
            cpu_pct,
            mem_mb,
            free_gb,
            os.getloadavg()[0],
        )

        p = self.peak
        p["tree_rss_mb"] = max(p["tree_rss_mb"], rss_mb)
        p["tree_procs"] = max(p["tree_procs"], procs)
        p["cpu_pct"] = max(p["cpu_pct"], cpu_pct)
        p["mem_used_mb"] = max(p["mem_used_mb"], mem_mb)
        p["disk_free_gb_min"] = (
            free_gb if p["disk_free_gb_min"] is None else min(p["disk_free_gb_min"], free_gb)
        )
        p["samples"] += 1

        with metrics.TSV_LOCK:
            if not config.RESOURCES.exists():
                config.RESOURCES.write_text("\t".join(RESOURCE_FIELDS) + "\n")
            with config.RESOURCES.open("a") as f:
                f.write("\t".join(str(v) for v in row) + "\n")
        return busy, total
