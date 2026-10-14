"""Tests for ctplib/metrics.py: the metrics.tsv rows."""

from __future__ import annotations

import subprocess

import pytest

import retain
from ctplib import (
    metrics,
    project_steps,
)

from fakes import FakeRunner

# --------------------------------------------------------------------------- #
# metrics ledger
# --------------------------------------------------------------------------- #


def test_metrics_is_append_only_with_seven_fields_per_row(sandbox):
    """Rewriting metrics.tsv destroys the benchmark history."""
    log = retain.state_dir("jq") / "logs" / "pipeline-1.log"
    metrics.record_metric("jq", "S", "pipeline", 34, 139, log)
    first = (sandbox.root / "metrics.tsv").read_text().splitlines()[1]

    metrics.record_metric("jq", "S", "pipeline", 11, 1, log)
    lines = (sandbox.root / "metrics.tsv").read_text().splitlines()

    assert lines[0].split("\t") == [
        "iso_start",
        "project",
        "class",
        "phase",
        "duration_s",
        "rc",
        "log",
    ]
    assert lines[1] == first, "the first attempt row was rewritten"
    assert len(lines) == 3
    assert all(len(line.split("\t")) == 7 for line in lines)


def test_metrics_row_carries_the_return_code_and_log_path(sandbox, runner, clock, jq):
    monkey_rc = FakeRunner(pipeline_rc=2)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(subprocess, "Popen", monkey_rc)
        project_steps.run_project(jq)

    row = (sandbox.root / "metrics.tsv").read_text().splitlines()[1].split("\t")
    assert row[1:4] == ["jq", "S", "pipeline"]
    assert row[5] == "2"
    assert row[6].endswith("pipeline-20260102T030405.log")
