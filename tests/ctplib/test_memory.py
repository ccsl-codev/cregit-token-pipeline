"""Tests for ctplib/memory.py: the memory budget of step 10."""

from __future__ import annotations

import pytest

from ctplib import (
    memory,
)

from fakes import GIB

# --------------------------------------------------------------------------- #
# step 10 memory budget
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "text,want",
    [
        ("1B", 1),
        ("2K", 2048),
        ("2KB", 2048),
        ("2KiB", 2048),
        ("3MB", 3 * 1024**2),
        ("3M", 3 * 1024**2),
        ("3MiB", 3 * 1024**2),
        ("8GB", 8 * 1024**3),
        ("8G", 8 * 1024**3),
        ("8GiB", 8 * 1024**3),
        ("1T", 1024**4),
        ("1TB", 1024**4),
        ("1TiB", 1024**4),
        ("1.5 GiB", int(1.5 * 1024**3)),
        ("  3GB  ", 3 * 1024**3),
        ("3gb", 3 * 1024**3),
    ],
)
def test_size_to_bytes_reads_every_unit_duckdb_accepts(text, want):
    """The budget arithmetic is only as good as the parse, and DuckDB accepts
    all of these spellings."""
    assert memory.size_to_bytes(text) == want


def test_size_to_bytes_refuses_a_percentage():
    """A percentage measures total RAM; only the free part is usable."""
    with pytest.raises(ValueError) as exc:
        memory.size_to_bytes("80%")
    assert "not a percentage" in str(exc.value)


@pytest.mark.parametrize("bad", ["3gigs", "", "GB", "3 4GB", "-3GB", "3PB"])
def test_size_to_bytes_refuses_anything_it_cannot_read(bad):
    """Guessing at a typo would cap the heap at the wrong number silently."""
    with pytest.raises(ValueError) as exc:
        memory.size_to_bytes(bad)
    assert "cannot read" in str(exc.value)


@pytest.mark.parametrize(
    "meminfo, want",
    [
        # MemAvailable, not MemFree: the reclaimable page cache is usable.
        pytest.param(
            "MemTotal:       31000000 kB\nMemFree:          900000 kB\n"
            "MemAvailable:    5500000 kB\n",
            5500000 * 1024,
            id="mem-available",
        ),
        pytest.param("MemTotal:       31000000 kB\n", None, id="key-absent"),
        pytest.param("MemAvailable:    not-a-number kB\n", None, id="unparsable"),
        pytest.param("MemAvailable:\n", None, id="truncated"),
        pytest.param(None, None, id="no-proc"),
    ],
)
def test_available_bytes_reads_mem_available_or_says_none(tmp_path, monkeypatch, meminfo, want):
    """The budget warning is advisory, so an odd /proc must not raise."""
    path = tmp_path / "meminfo"
    if meminfo is not None:
        path.write_text(meminfo)
    monkeypatch.setattr(memory, "Path", lambda _p: path)
    assert memory.available_bytes() == want


def test_memory_budget_warning_names_the_shortfall(monkeypatch):
    """2 x 8GB at the 1.4 settle ratio against 6 GiB free."""
    monkeypatch.setattr(memory, "available_bytes", lambda: 6 * 1024**3)
    warning = memory.memory_budget_warning("8GB", 2)
    assert warning is not None
    assert "2 concurrent x 8GB" in warning
    assert "22.4 GiB" in warning, "1.4 x 2 x 8GB, the measured settle ratio"
    assert "6.0 GiB" in warning


@pytest.mark.parametrize(
    "free, limit, jobs, warns",
    [
        pytest.param(8 * GIB, "3GB", 1, False, id="1.4x1x3GB-fits-8GiB"),
        pytest.param(None, "8GB", 4, False, id="ram-unknown"),
        pytest.param(10 * GIB, "3GB", 2, False, id="1.4x2x3GB-fits-10GiB"),
        pytest.param(10 * GIB, "3GB", 3, True, id="1.4x3x3GB-exceeds-10GiB"),
    ],
)
def test_memory_budget_warning_multiplies_by_jobs(monkeypatch, free, limit, jobs, warns):
    monkeypatch.setattr(memory, "available_bytes", lambda: free)
    assert (memory.memory_budget_warning(limit, jobs) is not None) is warns
