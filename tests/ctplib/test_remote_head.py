"""Tests for ctplib/remote_head.py: --latest."""

from __future__ import annotations

import json
import subprocess

import pytest

import pin
from ctplib import (
    project_run,
    project_steps,
    remote_head,
    runtime,
    toolchain,
)

from fakes import OTHER_SHA, PINNED_SHA, FakeRunner, fake_parquet, ledger_rows

# --latest: pin each project to the remote head when its first attempt starts.


@pytest.fixture
def latest(monkeypatch, sandbox):
    monkeypatch.setitem(runtime.opts, "latest", True)
    calls = []

    def remote_head(url, timeout=None):
        calls.append((url, timeout))
        return "master", OTHER_SHA

    monkeypatch.setattr(pin, "remote_head", remote_head)
    return calls


def test_latest_pins_the_remote_head_and_records_the_snapshot(
    monkeypatch, sandbox, latest, pinned_jq
):
    r = FakeRunner(checkout=OTHER_SHA)
    monkeypatch.setattr(subprocess, "Popen", r)
    monkeypatch.setattr(subprocess, "run", r)
    assert project_steps.run_project(pinned_jq) == project_run.RunOutcome.DONE
    argv = r.argv("clone")
    assert argv[argv.index("--commit") + 1] == OTHER_SHA
    assert latest == [("https://github.com/jqlang/jq.git", remote_head.LATEST_TIMEOUT_S)]
    [resolve] = ledger_rows(sandbox, "jq", "resolve")
    assert resolve["resolve"]["resolved_sha"] == OTHER_SHA
    assert resolve["resolve"]["snapshot_sha"] == PINNED_SHA
    assert resolve["resolve"]["reused"] is False
    rows = ledger_rows(sandbox, "jq")
    assert all(row["pinned_sha"] == OTHER_SHA and row["snapshot_sha"] == PINNED_SHA for row in rows)
    assert rows[-1]["state"] == "done"
    saved = json.loads(remote_head.latest_path("jq").read_text())
    assert saved["resolved_sha"] == OTHER_SHA and saved["branch"] == "master"


def test_latest_reuses_the_commit_of_an_earlier_attempt(monkeypatch, sandbox, latest, pinned_jq):
    path = remote_head.latest_path("jq")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"resolved_sha": OTHER_SHA, "snapshot_sha": PINNED_SHA}))
    r = FakeRunner(checkout=OTHER_SHA)
    monkeypatch.setattr(subprocess, "Popen", r)
    monkeypatch.setattr(subprocess, "run", r)
    assert project_steps.run_project(pinned_jq) == project_run.RunOutcome.DONE
    assert latest == []
    argv = r.argv("clone")
    assert argv[argv.index("--commit") + 1] == OTHER_SHA
    [resolve] = ledger_rows(sandbox, "jq", "resolve")
    assert resolve["resolve"]["reused"] is True


def test_latest_fails_the_project_when_the_remote_head_cannot_be_read(
    monkeypatch, sandbox, runner, pinned_jq
):
    monkeypatch.setitem(runtime.opts, "latest", True)

    def unreachable(url, timeout=None):
        raise pin.PinError("cannot reach it")

    monkeypatch.setattr(pin, "remote_head", unreachable)
    assert project_steps.run_project(pinned_jq) == project_run.RunOutcome.FAILED
    assert runner.calls == []
    [row] = ledger_rows(sandbox, "jq", "resolve")
    assert row["status"] == "failed" and "cannot reach it" in row["detail"]
    assert ledger_rows(sandbox, "jq", "project")[0]["failed_step"] == "resolve"
    assert not remote_head.latest_path("jq").exists()


def test_without_latest_the_manifest_commit_is_pinned(sandbox, runner, pinned_jq):
    project_steps.run_project(pinned_jq)
    assert ledger_rows(sandbox, "jq", "resolve") == []
    assert all("snapshot_sha" not in row for row in ledger_rows(sandbox, "jq"))


def test_a_workdir_of_another_commit_is_wiped_with_force_clean(
    monkeypatch, sandbox, runner, pinned_jq
):
    monkeypatch.setattr(
        toolchain,
        "git_head",
        lambda repo: OTHER_SHA if repo.name == "jq-original.git" else PINNED_SHA,
    )
    assert project_steps.run_project(pinned_jq) == project_run.RunOutcome.DONE
    assert "--force-clean" in runner.argv("pipeline")
    [row] = ledger_rows(sandbox, "jq", "pipeline")
    assert row["resume"] == {"stale_workdir": OTHER_SHA}


def test_a_fresh_workdir_is_not_force_cleaned(sandbox, runner, pinned_jq):
    project_steps.run_project(pinned_jq)
    assert "--force-clean" not in runner.argv("pipeline")


def test_a_post_validation_attempt_records_the_latest_commit(
    monkeypatch, sandbox, runner, pinned_jq, joining
):
    monkeypatch.setitem(runtime.opts, "latest", True)
    monkeypatch.setattr(pin, "remote_head", lambda url, timeout=None: pytest.fail("read"))
    path = remote_head.latest_path("jq")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"resolved_sha": OTHER_SHA, "snapshot_sha": PINNED_SHA}))
    fake_parquet(sandbox)
    (sandbox.out / "jq" / "jq.validated").write_text("rows=7\nbytes=9\n")
    assert (
        project_steps.run_project(pinned_jq, resume_validated=True) == project_run.RunOutcome.DONE
    )
    rows = ledger_rows(sandbox, "jq")
    assert rows and all(
        r["pinned_sha"] == OTHER_SHA and r["snapshot_sha"] == PINNED_SHA for r in rows
    )
