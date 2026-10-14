"""Tests for ctplib/phases.py: the log files and the -latest symlink."""

from __future__ import annotations

import subprocess
from datetime import datetime
from pathlib import Path

import retain
from ctplib import (
    phases,
    runtime,
)

from fakes import FakeRunner

# --------------------------------------------------------------------------- #
# log files and the -latest symlink
# --------------------------------------------------------------------------- #


def logs_of(sandbox, name="jq", phase="pipeline"):
    return sorted((retain.state_dir(name) / "logs").glob(f"{phase}-2*.log"))


def test_a_second_attempt_does_not_overwrite_the_first_log(sandbox, monkeypatch, clock, jq):
    """The first log is the evidence of why the first attempt failed."""
    runner = FakeRunner(payload=b"first attempt\n")
    monkeypatch.setattr(subprocess, "Popen", runner)

    phases.run_phase(jq, "pipeline", ["./run_pipeline_process.sh"])
    runner.payload = b"second attempt\n"
    clock.moment = datetime(2026, 1, 2, 3, 4, 40)
    phases.run_phase(jq, "pipeline", ["./run_pipeline_process.sh"])

    logs = logs_of(sandbox)
    assert [p.name for p in logs] == [
        "pipeline-20260102T030405.log",
        "pipeline-20260102T030440.log",
    ]
    assert logs[0].read_bytes() == b"first attempt\n"


def test_two_attempts_in_the_same_second_keep_both_logs(sandbox, monkeypatch, clock, jq):
    runner = FakeRunner(payload=b"first attempt\n")
    monkeypatch.setattr(subprocess, "Popen", runner)

    phases.run_phase(jq, "pipeline", ["./run_pipeline_process.sh"])
    runner.payload = b"second attempt\n"
    phases.run_phase(jq, "pipeline", ["./run_pipeline_process.sh"])

    logs = logs_of(sandbox)
    assert len(logs) == 2
    assert logs[0].read_bytes() == b"first attempt\n"


def test_latest_symlink_points_at_the_newest_attempt(sandbox, monkeypatch, clock, jq):
    """Visibility contract: `tail -f <phase>-latest.log` must follow the run in
    progress, not a stale attempt."""
    monkeypatch.setattr(subprocess, "Popen", FakeRunner())

    phases.run_phase(jq, "pipeline", ["./run_pipeline_process.sh"])
    clock.moment = datetime(2026, 1, 2, 3, 4, 40)
    phases.run_phase(jq, "pipeline", ["./run_pipeline_process.sh"])

    latest = retain.state_dir("jq") / "logs" / "pipeline-latest.log"
    assert latest.is_symlink()
    assert Path(latest.readlink()) == logs_of(sandbox)[-1]


def test_latest_symlink_is_created_on_the_first_attempt(sandbox, monkeypatch, clock, jq):
    """No prior symlink exists on a fresh project; the swap must still work."""
    monkeypatch.setattr(subprocess, "Popen", FakeRunner())
    (sandbox.out / "jq").mkdir()
    stamp = sandbox.out / "jq" / "jq.validated"
    phases.run_phase(jq, "validate", ["python3", "validate.py", "p.parquet", str(stamp)])
    latest = retain.state_dir("jq") / "logs" / "validate-latest.log"
    assert latest.is_symlink()
    assert not (retain.state_dir("jq") / "logs" / ".validate-latest.tmp").exists()


def test_run_phase_records_the_live_phase_for_the_heartbeat(sandbox, monkeypatch, jq):
    """The heartbeat reads _live; an empty _live makes a long run look idle."""
    monkeypatch.setattr(subprocess, "Popen", FakeRunner())
    phases.run_phase(jq, "pipeline", ["./run_pipeline_process.sh"])
    assert runtime.live["jq"][0] == "pipeline"
