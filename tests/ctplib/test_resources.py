"""Tests for ctplib/resources.py: the resource sampler."""

from __future__ import annotations

import os
import shutil
import threading
from types import SimpleNamespace

from ctplib import (
    resources,
)

# --------------------------------------------------------------------------- #
# resource sampling
# --------------------------------------------------------------------------- #


def test_tree_usage_sums_descendants_not_just_the_direct_child(sandbox):
    """The pipeline is a shell that spawns perl, git and srcml. Reporting only the
    shell's own RSS would understate every run, which is the number the cost
    model depends on."""
    table = {10: (1, 1024), 11: (10, 2048), 12: (11, 4096), 99: (1, 8192)}
    rss_mb, procs = resources.tree_usage(10, table)
    assert procs == 3, "grandchild 12 was not walked"
    assert rss_mb == (1024 + 2048 + 4096) // 1024


def test_tree_usage_returns_zero_when_the_process_already_exited(sandbox):
    """A phase can finish between Popen and the first sample. That must read as
    zero, not raise, because the sampler runs inside every phase."""
    assert resources.tree_usage(424242, {1: (0, 4096)}) == (0, 0)


def test_tree_usage_survives_a_cycle_in_the_parent_map(sandbox):
    """A malformed parent map must not hang the sampler. /proc is read
    process-by-process, so an inconsistent snapshot is possible."""
    table = {20: (21, 1024), 21: (20, 1024)}
    rss_mb, procs = resources.tree_usage(20, table)
    assert procs == 2 and rss_mb == 2


def test_stop_before_start_returns_an_empty_peak(sandbox, jq):
    """run_phase stops the sampler in a finally block, so stop() can be reached
    on a path where start() never ran."""
    peak = resources.ResourceSampler(jq, "pipeline").stop()
    assert peak["samples"] == 0
    assert peak["disk_free_gb_min"] is None


def test_a_failing_sample_never_breaks_the_phase(sandbox, jq, capsys):
    """Measurement must not fail the thing it measures. The loop reports the
    failure and keeps sampling."""
    sampler = resources.ResourceSampler(jq, "pipeline", interval=0.01)
    calls = {"n": 0}
    done = threading.Event()

    def flaky(prev_busy, prev_total):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("/proc vanished")
        if calls["n"] >= 3:
            done.set()
        return prev_busy, prev_total

    sampler._sample = flaky
    sampler.start(os.getpid())
    assert done.wait(timeout=10), "the loop stopped after the failing sample"
    sampler.stop()
    assert "resource sample failed" in capsys.readouterr().out


def test_the_ledger_header_matches_the_declared_fields(sandbox, jq):
    """A consumer splits on these columns, so the header and the row must agree."""
    sampler = resources.ResourceSampler(jq, "pipeline", interval=0.01)
    sampler.pid = os.getpid()
    sampler._sample(0, 0)

    lines = (sandbox.root / "resources.tsv").read_text().splitlines()
    assert lines[0].split("\t") == list(resources.RESOURCE_FIELDS)
    assert len(lines[1].split("\t")) == len(resources.RESOURCE_FIELDS)
    assert lines[1].split("\t")[1:4] == ["jq", "S", "pipeline"]


def test_peaks_are_maxima_and_disk_is_a_low_water_mark(sandbox, jq, monkeypatch):
    """Capacity planning needs the worst moment, not the last one."""
    sampler = resources.ResourceSampler(jq, "pipeline", interval=0.01)
    sampler.pid = os.getpid()

    sizes = iter([(500, 3), (200, 1)])  # second sample is smaller
    monkeypatch.setattr(resources, "tree_usage", lambda *a, **k: next(sizes))
    frees = iter([900 * 2**30, 100 * 2**30])  # disk shrinks
    monkeypatch.setattr(shutil, "disk_usage", lambda p: SimpleNamespace(free=next(frees)))

    sampler._sample(0, 0)
    sampler._sample(0, 0)
    peak = sampler.peak
    assert peak["tree_rss_mb"] == 500, "peak fell back to the later sample"
    assert peak["tree_procs"] == 3
    assert peak["disk_free_gb_min"] == 100, "low-water mark not kept"
    assert peak["samples"] == 2


def test_cpu_percent_is_zero_when_no_jiffies_elapsed(sandbox, jq, monkeypatch):
    """Two samples inside one clock tick must not divide by zero."""
    monkeypatch.setattr(resources, "_cpu_jiffies", lambda: (100, 200))
    sampler = resources.ResourceSampler(jq, "pipeline", interval=0.01)
    sampler.pid = os.getpid()
    sampler._sample(100, 200)
    assert sampler.peak["cpu_pct"] == 0.0


def test_proc_scan_skips_a_process_that_vanished_mid_scan(sandbox, tmp_path):
    """cregit starts and reaps short-lived perl and git processes constantly, so a
    /proc entry can disappear between the listdir and the read. That must be
    skipped, not raise inside the sampler thread."""
    fake_proc = tmp_path / "proc"
    fake_proc.mkdir()
    (fake_proc / "self").mkdir()  # non-numeric, ignored
    (fake_proc / "7").mkdir()  # numeric, but no stat file
    good = fake_proc / "9"
    good.mkdir()
    # Real /proc/<pid>/stat, built by field position so the offsets are visible.
    # A comm of "(git log --oneline)" holds spaces and parentheses, which is why
    # the parser splits after the LAST ')' instead of on whitespace.
    stat = ["0"] * 52
    stat[0] = "9"  # field 1  pid
    stat[1] = "(git log --oneline)"  # field 2  comm, spaces and parens
    stat[2] = "S"  # field 3  state
    stat[3] = "1234"  # field 4  ppid
    stat[23] = "777"  # field 24 rss, in pages
    (good / "stat").write_text(" ".join(stat) + "\n")

    table = resources._proc_ppid_rss(fake_proc)
    assert 7 not in table, "an unreadable entry was not skipped"
    assert list(table) == [9]
    assert table[9] == (1234, 777 * 4), "ppid or rss read from the wrong offset"


def test_proc_scan_agrees_with_real_proc_for_our_own_process(sandbox):
    """The synthetic layout above only proves the offsets are self-consistent.
    This proves they match the kernel's real format."""
    table = resources._proc_ppid_rss()
    assert os.getpid() in table
    ppid, rss_kb = table[os.getpid()]
    assert ppid == os.getppid(), "parsed ppid disagrees with os.getppid()"
    assert rss_kb > 0, "a running interpreter cannot have zero resident memory"
