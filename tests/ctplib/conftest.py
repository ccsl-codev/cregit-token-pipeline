"""Fixtures of the ctplib tests. The autouse `sandbox` fixture moves every path into
tmp_path and makes subprocess.run and Popen raise. So no test touches real data or
starts a process; a test that needs a process installs a fake."""

from __future__ import annotations

import subprocess
import time
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

import retain
from ctplib import (
    config,
    console,
    phases,
    project_run,
    project_steps,
    runtime,
    scheduler,
    toolchain,
)

from fakes import (
    PINNED_SHA,
    REAL_RUNNER_USAGE,
    VALID_ROW,
    Clock,
    FakeProjects,
    FakeRunner,
    forbidden,
    write_manifest,
)


@pytest.fixture(autouse=True)
def sandbox(tmp_path, monkeypatch):
    """Redirect every path and side effect of ctp into tmp_path."""
    out = tmp_path / "corpus-files"
    out.mkdir()
    cregit = tmp_path / "cregit"
    cregit.mkdir()
    # An empty CREGIT would give cmd_run's flag preflight nothing to read.
    (cregit / "run_pipeline_process.sh").write_text(REAL_RUNNER_USAGE)

    monkeypatch.setattr(config, "CORPUS", tmp_path)
    monkeypatch.setattr(config, "OUT", out)
    monkeypatch.setattr(config, "CREGIT", cregit)
    monkeypatch.setattr(config, "METRICS", tmp_path / "metrics.tsv")
    monkeypatch.setattr(config, "RUNS_LOG", tmp_path / "runs.log")
    monkeypatch.setattr(config, "RESOURCES", tmp_path / "resources.tsv")
    monkeypatch.setattr(config, "LEDGER", tmp_path / "ledger.jsonl")
    # tool_versions starts git and srcml; its own tests call REAL_TOOL_VERSIONS.
    monkeypatch.setattr(toolchain, "tool_versions", lambda: {"ctp_commit": "test"})
    monkeypatch.setattr(retain, "STATE", tmp_path / "state")
    monkeypatch.setattr(retain, "OUT", out)
    # A real disk_usage call stays, but the floor can never trip by accident.
    monkeypatch.setattr(config, "DISK_FLOOR_GB", 0)
    monkeypatch.setattr(time, "sleep", lambda *a, **k: None)
    monkeypatch.setattr(subprocess, "run", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)

    for shared in (runtime.opts, runtime.env, runtime.live, runtime.run_info, runtime.lock_fds):
        shared.clear()
    yield SimpleNamespace(root=tmp_path, out=out, cregit=cregit)
    for shared in (runtime.opts, runtime.env, runtime.live, runtime.run_info, runtime.lock_fds):
        shared.clear()


@pytest.fixture
def clock(monkeypatch):
    c = Clock(datetime(2026, 1, 2, 3, 4, 5))
    monkeypatch.setattr(phases, "datetime", c)
    monkeypatch.setattr(console, "datetime", c)
    return c


@pytest.fixture
def jq():
    return dict(
        name="jq",
        url="https://github.com/jqlang/jq.git",
        category="community",
        file_filter=r"\.[ch]$",
        size_class="S",
    )


@pytest.fixture
def runner(monkeypatch):
    r = FakeRunner()
    monkeypatch.setattr(subprocess, "run", r)
    monkeypatch.setattr(subprocess, "Popen", r)
    return r


@pytest.fixture
def runner_script(sandbox):
    """Write a stand-in run_pipeline_process.sh into the sandboxed CREGIT."""

    def _write(text: str = REAL_RUNNER_USAGE) -> Path:
        path = sandbox.cregit / "run_pipeline_process.sh"
        path.write_text(text)
        return path

    return _write


@pytest.fixture
def ready(sandbox, monkeypatch, runner_script):
    """A sandbox one cmd_run call away from a run that starts no process."""
    runner_script()
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(toolchain, "capture_devenv_env", lambda: {"PATH": "/x"})
    monkeypatch.setattr(project_steps, "run_project", lambda p: project_run.RunOutcome.DONE)
    return sandbox


@pytest.fixture
def must_not_start(sandbox, monkeypatch, runner_script):
    """Same sandbox, but starting the run fails the test: every refusal must
    come before the devenv capture."""
    runner_script()
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(toolchain, "capture_devenv_env", forbidden)
    monkeypatch.setattr(project_steps, "run_project", forbidden)
    return sandbox


@pytest.fixture
def pinned_jq(jq):
    return {**jq, "commit": PINNED_SHA}


@pytest.fixture
def joining(sandbox):
    manifest = write_manifest(sandbox.root, VALID_ROW)
    runtime.opts.update(join=True, manifest=str(manifest))
    return manifest


@pytest.fixture
def fake_projects(monkeypatch):
    def install(**kw):
        fake = FakeProjects(**kw)
        monkeypatch.setattr(project_steps, "run_project", fake)
        return fake

    return install


@pytest.fixture
def no_gates(monkeypatch):
    monkeypatch.setattr(scheduler, "gate_reason", lambda opts: None)
