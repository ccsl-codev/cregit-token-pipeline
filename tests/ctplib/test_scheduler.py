"""Tests for ctplib/scheduler.py: order, gates, stop, resume and retries."""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

import pytest

import ledger
from ctplib import (
    cli,
    config,
    memory,
    phases,
    project_run,
    project_steps,
    runtime,
    scheduler,
)

from fakes import (
    REAL_RUNNER_USAGE,
    VALID_ROW,
    FakeRunner,
    ledger_rows,
    manifest_rows,
    projects_of,
    run_args,
    write_manifest,
)

# --------------------------------------------------------------------------- #
# census: order, gates, stop, resume, retries
# --------------------------------------------------------------------------- #


def census_args(**over):
    base = vars(run_args())
    base.update(workers=1, min_free_mem_gb=0, disk_floor_gb=0, stop_file="", poll=0.01)
    base.pop("jobs")
    base.update(over)
    return argparse.Namespace(**base)


def stamp(sandbox, name):
    (sandbox.out / name).mkdir(parents=True, exist_ok=True)
    (sandbox.out / name / f"{name}.validated").write_text("rows=1\n")


def history_row(name, state, **extra):
    return {"project": name, "step": "project", "state": state, **extra}


def test_census_runs_in_manifest_order_and_records_the_queue_position(
    sandbox, fake_projects, no_gates
):
    fake = fake_projects()
    projects = projects_of(sandbox, "c", "a", "b")
    results = scheduler.schedule(projects, 1, 0, sandbox.root / "STOP", poll=0.01)
    assert fake.calls == [("c", 1, 3, 1, False), ("a", 2, 3, 1, False), ("b", 3, 3, 1, False)]
    assert all(v == project_run.RunOutcome.DONE for v in results.values())


def test_census_skips_done_projects_and_resumes_the_rest(sandbox, fake_projects, no_gates):
    log = sandbox.root / "ledger.jsonl"
    stamp(sandbox, "a")
    ledger.append(log, history_row("a", "done"))
    ledger.append(log, history_row("b", "failed", failed_step="pipeline"))
    stamp(sandbox, "c")
    ledger.append(log, history_row("c", "done-dirty"))
    fake = fake_projects()
    results = scheduler.schedule(
        projects_of(sandbox, "a", "b", "c", "d"), 1, 1, sandbox.root / "STOP", poll=0.01
    )
    assert results["a"] == project_run.RunOutcome.SKIPPED
    # b failed once: its second attempt. c validated but is dirty: only the later steps.
    assert fake.calls == [("b", 2, 4, 2, False), ("c", 3, 4, 2, True), ("d", 4, 4, 1, False)]


def test_census_gives_up_after_the_retries_across_restarts(
    sandbox, fake_projects, no_gates, capsys
):
    log = sandbox.root / "ledger.jsonl"
    for _ in range(2):
        ledger.append(log, history_row("a", "failed", failed_step="clone"))
    fake = fake_projects()
    results = scheduler.schedule(projects_of(sandbox, "a"), 1, 1, sandbox.root / "STOP", poll=0.01)
    assert fake.calls == [] and results["a"] == project_run.RunOutcome.FAILED
    assert "failed 2 times (last at clone)" in capsys.readouterr().out


@pytest.mark.parametrize(
    "first, retries, attempt",
    [
        pytest.param(project_run.RunOutcome.FAILED, 1, 2, id="a-failure-is-retried"),
        pytest.param(project_run.RunOutcome.DEFERRED, 0, 1, id="a-deferral-is-tried-again"),
    ],
)
def test_a_project_is_tried_again_after_the_rest(
    sandbox, fake_projects, no_gates, first, retries, attempt
):
    """A retry counts as a new attempt; a deferral does not, and needs no retries."""
    fake = fake_projects(outcomes={"a": [first, project_run.RunOutcome.DONE]})
    results = scheduler.schedule(
        projects_of(sandbox, "a", "b"), 1, retries, sandbox.root / "STOP", poll=0.01
    )
    assert fake.names() == ["a", "b", "a"]
    assert fake.calls[2][3] == attempt and results["a"] == project_run.RunOutcome.DONE


def test_census_gives_up_on_a_project_that_keeps_killing_its_run(
    sandbox, fake_projects, no_gates, capsys
):
    log = sandbox.root / "ledger.jsonl"
    for run in ("r1", "r2"):
        ledger.append(
            log,
            {"project": "a", "run_id": run, "attempt": 1, "step": "pipeline", "status": "started"},
        )
    fake = fake_projects()
    scheduler.schedule(projects_of(sandbox, "a"), 1, 1, sandbox.root / "STOP", poll=0.01)
    assert fake.calls == []
    assert "failed 2 times (last at pipeline (interrupted))" in capsys.readouterr().out


def test_one_interrupted_run_is_retried(sandbox, fake_projects, no_gates):
    ledger.append(
        sandbox.root / "ledger.jsonl",
        {"project": "a", "run_id": "r1", "attempt": 1, "step": "pipeline", "status": "started"},
    )
    fake = fake_projects()
    scheduler.schedule(projects_of(sandbox, "a"), 1, 1, sandbox.root / "STOP", poll=0.01)
    assert fake.calls == [("a", 1, 1, 2, False)]


def test_census_without_retries_runs_a_failure_once(sandbox, fake_projects, no_gates):
    fake = fake_projects(outcomes={"a": [project_run.RunOutcome.FAILED] * 3})
    results = scheduler.schedule(projects_of(sandbox, "a"), 1, 0, sandbox.root / "STOP", poll=0.01)
    assert fake.names() == ["a"] and results["a"] == project_run.RunOutcome.FAILED


def test_a_retry_after_validation_resumes_at_the_later_steps(sandbox, fake_projects, no_gates):
    def failed_after_validating(name):
        stamp(sandbox, name)

    fake = fake_projects(
        outcomes={"a": [project_run.RunOutcome.FAILED, project_run.RunOutcome.DONE]},
        hook=failed_after_validating,
    )
    scheduler.schedule(projects_of(sandbox, "a"), 1, 1, sandbox.root / "STOP", poll=0.01)
    assert [c[4] for c in fake.calls] == [False, True]


def test_the_stop_file_lets_the_running_project_finish_and_starts_no_other(
    sandbox, fake_projects, no_gates, capsys
):
    stop = sandbox.root / "STOP"
    fake = fake_projects(hook=lambda name: stop.touch())
    results = scheduler.schedule(projects_of(sandbox, "a", "b", "c"), 1, 0, stop, poll=0.01)
    assert fake.names() == ["a"] and results == {"a": project_run.RunOutcome.DONE}
    assert f"{stop} exists" in runtime.run_info["stop_reason"]
    assert "No new project starts" in capsys.readouterr().out


def test_a_restart_after_a_stop_continues_with_the_next_project(sandbox, monkeypatch, no_gates):
    """The real run_project, so the ledger the second census reads is real."""
    monkeypatch.setattr(subprocess, "Popen", FakeRunner())
    monkeypatch.setattr(subprocess, "run", FakeRunner())
    stop = sandbox.root / "STOP"
    projects = projects_of(sandbox, "a", "b")
    real = project_steps.run_project

    def run_then_stop(*a, **k):
        outcome = real(*a, **k)
        stop.touch()
        return outcome

    monkeypatch.setattr(project_steps, "run_project", run_then_stop)
    first = scheduler.schedule(projects, 1, 0, stop, poll=0.01)
    assert set(first) == {"a"}

    stop.unlink()
    runtime.run_info.clear()
    monkeypatch.setattr(project_steps, "run_project", real)
    second = scheduler.schedule(projects, 1, 0, stop, poll=0.01)
    assert second == {"a": project_run.RunOutcome.SKIPPED, "b": project_run.RunOutcome.DONE}
    finals = [(r["project"], r["state"]) for r in ledger_rows(sandbox, step="project")]
    assert finals == [("a", "done"), ("b", "done")]


def test_a_stop_signal_stops_dispatch(sandbox, fake_projects, no_gates):
    def signal_during(name):
        runtime.run_info["stop_reason"] = "signal SIGTERM"
        runtime.stop.set()

    fake = fake_projects(hook=signal_during)
    try:
        scheduler.schedule(projects_of(sandbox, "a", "b"), 1, 0, sandbox.root / "STOP", poll=0.01)
    finally:
        runtime.stop.clear()
    assert fake.names() == ["a"] and runtime.run_info["stop_reason"] == "signal SIGTERM"


def test_a_broken_ledger_stops_dispatch(sandbox, fake_projects, no_gates):
    def break_ledger(name):
        runtime.run_info["ledger_broken"] = "OSError: disk full"

    fake = fake_projects(hook=break_ledger)
    scheduler.schedule(projects_of(sandbox, "a", "b"), 1, 0, sandbox.root / "STOP", poll=0.01)
    assert fake.names() == ["a"] and "disk full" in runtime.run_info["stop_reason"]


def test_a_closed_gate_waits_then_starts(sandbox, fake_projects, monkeypatch, capsys):
    answers = iter(["memory 1.0G available, below the 8G floor"] * 3)
    monkeypatch.setattr(scheduler, "gate_reason", lambda opts: next(answers, None))
    fake = fake_projects()
    scheduler.schedule(projects_of(sandbox, "a"), 1, 0, sandbox.root / "STOP", poll=0.01)
    assert fake.names() == ["a"]
    out = capsys.readouterr().out
    assert out.count("waiting to start a: memory 1.0G available") == 1


def test_a_closed_gate_and_a_stop_end_the_census_without_starting(
    sandbox, fake_projects, monkeypatch
):
    stop = sandbox.root / "STOP"
    calls = []

    def gate(opts):
        calls.append(1)
        stop.touch()
        return "disk 1G free, below the 150G floor"

    monkeypatch.setattr(scheduler, "gate_reason", gate)
    fake = fake_projects()
    assert scheduler.schedule(projects_of(sandbox, "a"), 1, 0, stop, poll=0.01) == {}
    assert fake.calls == [] and calls


def test_a_lone_deferred_project_waits_a_poll_before_its_next_try(
    sandbox, fake_projects, no_gates, monkeypatch
):
    sleeps = []
    monkeypatch.setattr(time, "sleep", lambda s: sleeps.append(s))
    fake = fake_projects(
        outcomes={"a": [project_run.RunOutcome.DEFERRED, project_run.RunOutcome.DONE]}
    )
    scheduler.schedule(projects_of(sandbox, "a"), 1, 0, sandbox.root / "STOP", poll=0.5)
    assert fake.names() == ["a", "a"] and 0.5 in sleeps


def test_a_crashing_worker_fails_only_its_project(sandbox, fake_projects, no_gates, capsys):
    def crash(name):
        if name == "a":
            raise RuntimeError("boom")

    fake_projects(hook=crash)
    results = scheduler.schedule(
        projects_of(sandbox, "a", "b"), 1, 0, sandbox.root / "STOP", poll=0.01
    )
    assert results == {"a": project_run.RunOutcome.FAILED, "b": project_run.RunOutcome.DONE}
    assert "crashed the worker: RuntimeError: boom" in capsys.readouterr().out


def test_census_runs_up_to_workers_projects_at_once(sandbox, fake_projects, no_gates):
    barrier = threading.Barrier(2, timeout=10)
    fake = fake_projects(hook=lambda name: barrier.wait())
    results = scheduler.schedule(
        projects_of(sandbox, "a", "b"), 2, 0, sandbox.root / "STOP", poll=0.01
    )
    assert sorted(fake.names()) == ["a", "b"] and not barrier.broken
    assert set(results.values()) == {project_run.RunOutcome.DONE}


@pytest.mark.parametrize(
    "state, failures, stamped, expected",
    [
        ("done", 0, True, "skip"),
        ("done", 0, False, "run"),  # done once, but the outputs are gone
        ("done-dirty", 0, True, "post"),
        ("failed", 1, True, "post"),
        ("failed", 1, False, "run"),
        ("failed", 2, False, "give-up"),
        ("", 0, False, "run"),
    ],
)
def test_plan(sandbox, state, failures, stamped, expected):
    if stamped:
        stamp(sandbox, "a")
    h = ledger.History(state=state, failures=failures)
    assert scheduler.plan({"name": "a"}, h, retries=1) == expected


def test_mem_available_reads_proc_meminfo(tmp_path):
    meminfo = tmp_path / "meminfo"
    meminfo.write_text("MemTotal: 32000000 kB\nMemAvailable: 8388608 kB\n")
    assert memory.mem_available_gb(meminfo) == 8.0
    meminfo.write_text("MemTotal: 1 kB\n")
    assert memory.mem_available_gb(meminfo) is None
    assert memory.mem_available_gb(tmp_path / "absent") is None


def test_gate_reason_names_disk_then_memory(sandbox, monkeypatch):
    monkeypatch.setattr(config, "DISK_FLOOR_GB", 10**9)
    assert scheduler.gate_reason({}).startswith("disk")
    monkeypatch.setattr(config, "DISK_FLOOR_GB", 0)
    monkeypatch.setattr(memory, "mem_available_gb", lambda: 2.0)
    assert scheduler.gate_reason({"min_free_mem_gb": 4}).startswith("memory 2.0G")
    assert scheduler.gate_reason({"min_free_mem_gb": 1}) is None
    monkeypatch.setattr(memory, "mem_available_gb", lambda: None)
    assert scheduler.gate_reason({"min_free_mem_gb": 4}) is None


@pytest.fixture
def census_ready(ready, monkeypatch):
    write_manifest(ready.root, *manifest_rows("a", "b"))
    monkeypatch.setattr(scheduler, "install_signal_handlers", lambda: None)
    return ready


def test_cmd_census_runs_end_to_end_options(census_ready, fake_projects, no_gates):
    fake = fake_projects()
    assert scheduler.cmd_census(census_args()) == 0
    assert fake.names() == ["a", "b"]
    assert runtime.opts["join"] and runtime.opts["cleanup"] and runtime.opts["own_session"]
    rows = ledger.read(census_ready.root / "ledger.jsonl")
    assert rows[0]["command"] == "census" and rows[-1]["step"] == "run-end"


def test_cmd_census_exits_3_when_stopped_cleanly(census_ready, fake_projects, no_gates):
    stop = census_ready.root / "STOP"
    fake_projects(hook=lambda name: stop.touch())
    assert scheduler.cmd_census(census_args()) == 3
    assert ledger.read(census_ready.root / "ledger.jsonl")[-1]["stop_reason"]


def test_cmd_census_exits_1_on_a_failure(census_ready, fake_projects, no_gates):
    fake_projects(outcomes={"a": [project_run.RunOutcome.FAILED]})
    assert scheduler.cmd_census(census_args(retries=0)) == 1


def test_cmd_census_refuses_to_start_with_a_stop_file(census_ready):
    (census_ready.root / "STOP").touch()
    with pytest.raises(SystemExit, match="Remove it to start"):
        scheduler.cmd_census(census_args())


def test_cmd_census_takes_a_custom_stop_file(census_ready, fake_projects, no_gates):
    stop = census_ready.root / "my-stop"
    stop.touch()
    with pytest.raises(SystemExit, match="my-stop exists"):
        scheduler.cmd_census(census_args(stop_file=str(stop)))


def test_cmd_census_needs_a_worker(census_ready):
    with pytest.raises(SystemExit, match="--workers"):
        scheduler.cmd_census(census_args(workers=0))


def test_cmd_census_sets_the_disk_floor(census_ready, fake_projects, no_gates, monkeypatch):
    fake_projects()
    scheduler.cmd_census(census_args(disk_floor_gb=77))
    assert config.DISK_FLOOR_GB == 77


def test_cmd_census_warns_about_unpinned_projects(census_ready, fake_projects, no_gates, capsys):
    write_manifest(census_ready.root, VALID_ROW)
    fake_projects()
    scheduler.cmd_census(census_args())
    assert "1 of 1 projects are unpinned" in capsys.readouterr().out


def test_cmd_census_with_nothing_to_run(census_ready):
    write_manifest(census_ready.root)
    assert scheduler.cmd_census(census_args()) == 0


def test_cmd_census_needs_commit_url_for_a_pinned_manifest(census_ready, runner_script):
    runner_script(REAL_RUNNER_USAGE.replace("--commit-url", "--commit-link"))
    with pytest.raises(SystemExit, match="--commit-url"):
        scheduler.cmd_census(census_args())


def test_the_first_signal_stops_dispatch_and_the_second_terminates(monkeypatch):
    handlers = {}
    monkeypatch.setattr(signal, "signal", lambda sig, h: handlers.setdefault(sig, h))
    killed = []
    monkeypatch.setattr(os, "killpg", lambda pid, sig: killed.append((pid, sig)))
    scheduler.install_signal_handlers()
    runtime.procs["a"] = SimpleNamespace(pid=4242)
    try:
        handlers[signal.SIGTERM](signal.SIGTERM, None)
        assert runtime.stop.is_set() and killed == []
        assert runtime.run_info["stop_reason"] == "signal SIGTERM"
        handlers[signal.SIGINT](signal.SIGINT, None)
        assert killed == [(4242, signal.SIGTERM)]
    finally:
        runtime.stop.clear()
        runtime.procs.clear()


def test_a_vanished_process_group_is_ignored(monkeypatch):
    handlers = {}
    monkeypatch.setattr(signal, "signal", lambda sig, h: handlers.setdefault(sig, h))

    def gone(pid, sig):
        raise ProcessLookupError(pid)

    monkeypatch.setattr(os, "killpg", gone)
    scheduler.install_signal_handlers()
    runtime.procs["a"] = SimpleNamespace(pid=1)
    runtime.stop.set()
    try:
        handlers[signal.SIGTERM](signal.SIGTERM, None)
    finally:
        runtime.stop.clear()
        runtime.procs.clear()


def test_a_census_child_gets_its_own_session(sandbox, runner, jq):
    runtime.opts.update(own_session=True)
    phases.run_phase(jq, "pipeline", ["./run_pipeline_process.sh"])
    assert runner.calls[0].kwargs["start_new_session"] is True


def test_main_dispatches_census(monkeypatch):
    seen = {}
    monkeypatch.setattr(scheduler, "cmd_census", lambda args: seen.update(vars(args)) or 0)
    monkeypatch.setattr(sys, "argv", ["ctp.py", "census", "--workers", "3"])
    assert cli.main() == 0
    assert seen["workers"] == 3 and seen["min_free_mem_gb"] == scheduler.MIN_FREE_MEM_GB
    assert seen["drop_html"] is False and seen["anonymize"] == ""
