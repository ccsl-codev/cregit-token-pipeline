"""Tests for ctplib/report.py: status and progress."""

from __future__ import annotations

import argparse
from pathlib import Path

import pytest

import retain
from ctplib import (
    config,
    metrics,
    report,
)

from fakes import VALID_ROW, write_manifest

# --------------------------------------------------------------------------- #
# cmd_status
# --------------------------------------------------------------------------- #


def test_cmd_status_classifies_every_project_state(sandbox, capsys, held_lock):
    """One screen, four states. A wrong state sends an operator to re-run a
    project that is already running."""
    write_manifest(
        sandbox.root,
        "done\thttps://d.git\tcommunity\t\\.[ch]$\tS",
        "running\thttps://r.git\tcommunity\t\\.[ch]$\tM",
        "failed\thttps://f.git\tenterprise\t\\.[ch]$\tL",
        "queued\thttps://q.git\tcommunity\t\\.[ch]$\tS",
    )
    (sandbox.out / "done").mkdir()
    (sandbox.out / "done" / "done.validated").write_text("rows=1\nbytes=2\n")
    (sandbox.out / "failed").mkdir()
    (sandbox.out / "running").mkdir()
    metrics.record_metric("done", "S", "validate", 3, 0, Path("/logs/validate-1.log"))

    with held_lock(retain.lock_path("running")):
        assert report.cmd_status(argparse.Namespace(manifest="manifest.tsv")) == 0

    out = capsys.readouterr().out
    states = {line.split()[0]: line.split()[2] for line in out.splitlines()[1:5]}
    assert states == dict(done="DONE", running="RUNNING", failed="FAILED", queued="QUEUED")
    assert "progress: 1/4 validated" in out
    assert "validate(3s,rc=0)" in out


def test_cmd_status_works_before_the_output_dir_exists(sandbox, monkeypatch, capsys):
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(config, "OUT", sandbox.out / "not-yet")
    assert report.cmd_status(argparse.Namespace(manifest="manifest.tsv")) == 0
    assert "G free" in capsys.readouterr().out


def test_cmd_status_works_without_a_metrics_file(sandbox, capsys):
    """A fresh checkout has no ledger yet; status must not crash."""
    write_manifest(sandbox.root, VALID_ROW)
    assert report.cmd_status(argparse.Namespace(manifest="manifest.tsv")) == 0
    assert "—" in capsys.readouterr().out


# --------------------------------------------------------------------------- #
# cmd_progress
# --------------------------------------------------------------------------- #


def progress_args(**over):
    base = dict(manifest="manifest.tsv", last=5)
    base.update(over)
    return argparse.Namespace(**base)


def write_metrics(tmp_path: Path, *rows: tuple) -> Path:
    path = tmp_path / "metrics.tsv"
    head = "iso_start\tproject\tclass\tphase\tduration_s\trc\tlog\n"
    body = "".join(
        f"2026-09-15T00:00:00+00:00\t{n}\t{c}\t{ph}\t{d}\t{rc}\tlog\n" for n, c, ph, d, rc in rows
    )
    path.write_text(head + body)
    return path


@pytest.mark.parametrize(
    "done,total,filled",
    [
        (0, 10, 0),
        (5, 10, 15),
        (10, 10, 30),
        (0, 0, 0),
    ],
)
def test_bar_fills_in_proportion(done, total, filled):
    """A bar is 30 cells wide whatever the numbers."""
    out = report.bar(done, total)
    assert len(out) == 30
    assert out.count("█") == filled


def test_finished_runs_keeps_only_successful_pipeline_rows(sandbox):
    """A validate row is not a run, and rc!=0 is not a finish."""
    write_metrics(
        sandbox.root,
        ("a", "S", "pipeline", 100, 0),
        ("a", "S", "validate", 0, 0),
        ("b", "M", "pipeline", 200, 1),
        ("c", "M", "pipeline", 300, 0),
    )
    assert report.finished_runs() == [("a", "S", 100), ("c", "M", 300)]


def test_progress_prints_a_bar_and_the_class_breakdown(sandbox, capsys):
    write_manifest(
        sandbox.root,
        "a\thttps://x/a.git\tcommunity\t\\.c$\tS",
        "b\thttps://x/b.git\tcommunity\t\\.c$\tM",
    )
    (sandbox.out / "a").mkdir()
    (sandbox.out / "a" / "a.validated").write_text("rows=1\n")

    assert report.cmd_progress(progress_args()) == 0
    out = capsys.readouterr().out
    assert "1/2" in out
    assert "50.0%" in out
    assert "S 1/1" in out
    assert "M 0/1" in out


def test_progress_lists_the_running_and_counts_the_failed(sandbox, capsys, held_lock):
    write_manifest(
        sandbox.root,
        "a\thttps://x/a.git\tcommunity\t\\.c$\tS",
        "b\thttps://x/b.git\tcommunity\t\\.c$\tS",
    )
    retain.state_dir("b").mkdir(parents=True)
    with held_lock(retain.lock_path("a")):
        assert report.cmd_progress(progress_args()) == 0
    out = capsys.readouterr().out
    assert "FAILED 1" in out
    assert "\nrunning\n  a " in out


def test_progress_lists_the_last_finished_newest_first(sandbox, capsys):
    write_manifest(sandbox.root, "a\thttps://x/a.git\tcommunity\t\\.c$\tS")
    write_metrics(
        sandbox.root,
        ("one", "S", "pipeline", 60, 0),
        ("two", "S", "pipeline", 120, 0),
        ("three", "S", "pipeline", 60, 0),
    )
    assert report.cmd_progress(progress_args(last=2)) == 0
    out = capsys.readouterr().out
    assert out.index("three") < out.index("two")
    assert "2.0 min" in out
    assert "one" not in out


def test_progress_survives_an_empty_manifest(sandbox, capsys):
    """--only can match nothing, and a crash here would be a poor status tool."""
    write_manifest(sandbox.root)

    assert report.cmd_progress(progress_args()) == 0
    out = capsys.readouterr().out
    assert "0/0" in out
    assert "0.0%" in out
