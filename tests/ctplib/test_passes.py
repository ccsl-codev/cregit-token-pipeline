"""Tests for ctplib/passes.py: `ctp.py run` and the heartbeat."""

from __future__ import annotations

import threading
import time

import pytest

from ctplib import (
    console,
    passes,
    project_run,
    runtime,
)

from fakes import forbidden

# --------------------------------------------------------------------------- #
# heartbeat
# --------------------------------------------------------------------------- #


def beat_once(results: dict) -> str:
    stop = threading.Event()
    lines: list[str] = []
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(passes, "HEARTBEAT_S", 0)
        mp.setattr(console, "say", lambda msg: (lines.append(msg), stop.set()))
        passes.heartbeat(stop, results)
    assert len(lines) == 1
    return lines[0]


def test_heartbeat_reports_running_projects_done_failed_and_disk():
    runtime.live["jq"] = ("pipeline", time.time())
    line = beat_once(
        {
            "jq": project_run.RunOutcome.DONE,
            "zstd": project_run.RunOutcome.FAILED,
            "tmux": project_run.RunOutcome.SKIPPED,
        }
    )
    assert "jq(pipeline" in line
    assert "done 2 failed 1" in line
    assert "G free" in line


def test_heartbeat_shows_a_dash_when_nothing_is_running():
    assert "running: —" in beat_once({})


def test_heartbeat_returns_immediately_when_already_stopped():
    """cmd_run sets the event in a finally block; the thread must exit."""
    stop = threading.Event()
    stop.set()
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(console, "say", forbidden)
        passes.heartbeat(stop, {})
