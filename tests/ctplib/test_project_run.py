"""Tests for ctplib/project_run.py: the ledger rows of each step, and audit."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import time
from types import SimpleNamespace

import pytest

import ledger
import retain
from ctplib import (
    config,
    passes,
    phases,
    pinning,
    project_run,
    project_steps,
    report,
    run_ledger,
    runtime,
    toolchain,
)

from fakes import (
    OTHER_SHA,
    PINNED_SHA,
    REAL_TOOL_VERSIONS,
    VALID_ROW,
    FakeRunner,
    fake_parquet,
    ledger_rows,
    run_args,
    write_manifest,
)

# --------------------------------------------------------------------------- #
# audit ledger
# --------------------------------------------------------------------------- #


def test_an_internal_error_still_writes_the_final_row(monkeypatch, sandbox, runner, jq):
    def boom(ctx):
        raise KeyError("surprise")

    monkeypatch.setattr(project_steps, "cleanup_step", boom)
    assert project_steps.run_project(jq) == project_run.RunOutcome.FAILED
    [final] = ledger_rows(sandbox, "jq", "project")
    assert final["failed_step"] == "internal" and "KeyError" in final["detail"]


def test_each_step_writes_a_started_row_before_it_runs(sandbox, runner, jq):
    project_steps.run_project(jq)
    rows = ledger_rows(sandbox, "jq", started=True)
    assert [(r["step"], r["status"]) for r in rows] == [
        ("pipeline", "started"),
        ("pipeline", "ok"),
        ("firm", "started"),
        ("firm", "ok"),
        ("validate", "started"),
        ("validate", "ok"),
        ("cleanup", "ok"),
        ("project", "done"),
    ]
    assert rows[0]["argv"][0] == "./run_pipeline_process.sh"


def test_a_done_project_writes_one_row_per_step_then_its_state(sandbox, runner, jq):
    fake_parquet(sandbox)
    runtime.run_info.update(run_id="r1", tools={"ctp_commit": "abc"}, flags={"jobs": 1})
    assert project_steps.run_project(jq, queue_pos=3, queue_total=9) == project_run.RunOutcome.DONE
    rows = ledger_rows(sandbox, "jq")
    assert [r["step"] for r in rows] == ["pipeline", "firm", "validate", "cleanup", "project"]
    assert [r["status"] for r in rows] == ["ok", "ok", "ok", "ok", "done"]
    for row in rows:
        assert row["run_id"] == "r1" and row["queue_pos"] == 3 and row["queue_total"] == 9
        assert row["manifest_row"] == jq
        assert row["pinned_sha"] == "unpinned"
        assert row["tools"] == {"ctp_commit": "abc"} and row["flags"] == {"jobs": 1}
        assert row["file_mask_sha256"] == hashlib.sha256(jq["file_filter"].encode()).hexdigest()
        assert row["start_utc"] <= row["end_utc"]
    assert rows[0]["exit_code"] == 0 and rows[0]["log"].endswith(".log")
    assert rows[-1]["state"] == "done"


def test_the_validate_row_carries_the_parquet_hash_and_row_count(sandbox, runner, jq):
    data = b"PAR1 some bytes PAR1"
    path = fake_parquet(sandbox, data=data)
    project_steps.run_project(jq)
    [row] = ledger_rows(sandbox, "jq", "validate")
    assert row["parquet"] == {
        "path": str(path),
        "bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
        "rows": 42,
    }
    assert ledger_rows(sandbox, "jq", "project")[0]["parquet"] == row["parquet"]


def test_a_pinned_project_records_the_pin_and_the_checkout(sandbox, runner, pinned_jq):
    project_steps.run_project(pinned_jq)
    rows = ledger_rows(sandbox, "jq")
    assert [r["step"] for r in rows] == [
        "clone",
        "pipeline",
        "firm",
        "validate",
        "cleanup",
        "project",
    ]
    assert rows[0]["pin"]["checked_out_sha"] == PINNED_SHA
    assert all(r["pinned_sha"] == PINNED_SHA for r in rows)
    assert rows[0]["checked_out_sha"] == ""
    assert all(r["checked_out_sha"] == PINNED_SHA for r in rows[1:])
    assert rows[4]["cleanup"]["removed"] == [str(pinning.pinned_clone_path("jq"))]


def test_a_clone_without_a_result_file_fails(monkeypatch, sandbox, pinned_jq):
    monkeypatch.setattr(subprocess, "Popen", FakeRunner(pin_result=False))
    assert project_steps.run_project(pinned_jq) == project_run.RunOutcome.FAILED
    [row] = ledger_rows(sandbox, "jq", "clone")
    assert row["status"] == "failed" and "no pin result" in row["detail"]


def test_a_checkout_mismatch_is_recorded_on_the_pipeline_row(monkeypatch, sandbox, pinned_jq):
    r = FakeRunner(checkout=OTHER_SHA)
    monkeypatch.setattr(subprocess, "Popen", r)
    monkeypatch.setattr(subprocess, "run", r)
    project_steps.run_project(pinned_jq)
    [row] = ledger_rows(sandbox, "jq", "pipeline")
    assert row["status"] == "failed" and row["exit_code"] == 1
    assert row["checked_out_sha"] == OTHER_SHA and "not the pinned" in row["detail"]
    [final] = ledger_rows(sandbox, "jq", "project")
    assert final["state"] == "failed" and final["failed_step"] == "pipeline"


def test_a_failed_pipeline_names_the_failed_step(monkeypatch, sandbox, jq):
    monkeypatch.setattr(subprocess, "Popen", FakeRunner(pipeline_rc=2))
    assert project_steps.run_project(jq) == project_run.RunOutcome.FAILED
    rows = ledger_rows(sandbox, "jq")
    assert [(r["step"], r["status"]) for r in rows] == [
        ("pipeline", "failed"),
        ("project", "failed"),
    ]
    assert rows[0]["exit_code"] == 2 and rows[1]["failed_step"] == "pipeline"


def test_a_phase_that_cannot_start_is_recorded(monkeypatch, sandbox, jq):
    monkeypatch.setattr(subprocess, "Popen", FakeRunner(raise_on="validate"))
    assert project_steps.run_project(jq) == project_run.RunOutcome.FAILED
    [row] = ledger_rows(sandbox, "jq", "validate")
    assert row["status"] == "failed" and row["exit_code"] is None
    assert "could not start" in row["detail"]
    assert ledger_rows(sandbox, "jq", "project")[0]["failed_step"] == "validate"


def test_a_held_lock_is_recorded_as_deferred(sandbox, runner, jq, held_lock):
    with held_lock(retain.lock_path("jq")):
        assert project_steps.run_project(jq) == project_run.RunOutcome.DEFERRED
    [row] = ledger_rows(sandbox, "jq")
    assert row["state"] == "deferred" and "lock" in row["detail"]


def test_the_disk_floor_is_recorded_as_deferred(monkeypatch, sandbox, runner, jq):
    monkeypatch.setattr(config, "DISK_FLOOR_GB", 10**9)
    assert project_steps.run_project(jq) == project_run.RunOutcome.DEFERRED
    assert ledger_rows(sandbox, "jq")[0]["detail"].startswith("disk below")


def test_a_refused_memo_dir_is_a_preflight_failure(sandbox, runner, jq):
    runtime.opts.update(memo_dir=str(sandbox.out))
    assert project_steps.run_project(jq) == project_run.RunOutcome.FAILED
    [row] = ledger_rows(sandbox, "jq")
    assert row["failed_step"] == "preflight"


def test_a_refused_prune_makes_the_project_done_dirty(monkeypatch, sandbox, runner, jq):
    monkeypatch.setattr(retain, "prune", lambda *a, **k: (0, False))
    runtime.opts.update(drop_memo=True)
    assert project_steps.run_project(jq) == project_run.RunOutcome.DONE_DIRTY
    [cleanup] = ledger_rows(sandbox, "jq", "cleanup")
    assert cleanup["status"] == "failed" and cleanup["cleanup"]["errors"]
    assert ledger_rows(sandbox, "jq", "project")[0]["state"] == "done-dirty"


def test_a_staging_clone_that_cannot_be_removed_makes_it_done_dirty(
    monkeypatch, sandbox, runner, pinned_jq
):
    monkeypatch.setattr(pinning, "drop_pinned_clone", lambda name: ["permission denied"])
    assert project_steps.run_project(pinned_jq) == project_run.RunOutcome.DONE_DIRTY


def test_done_dirty_is_a_success_that_a_retry_pass_does_not_rerun(monkeypatch, jq):
    calls = []

    def fake(project):
        calls.append(project["name"])
        return project_run.RunOutcome.DONE_DIRTY

    monkeypatch.setattr(project_steps, "run_project", fake)
    results = passes.run_passes([jq], jobs=1, retries=2)
    assert results == {"jq": project_run.RunOutcome.DONE_DIRTY} and calls == ["jq"]


def test_the_ledger_is_append_only_across_runs(sandbox, runner, jq):
    fake_parquet(sandbox)
    project_steps.run_project(jq)
    path = sandbox.root / "ledger.jsonl"
    first, inode = path.read_bytes(), path.stat().st_ino
    (sandbox.out / "jq" / "jq.validated").unlink()
    project_steps.run_project(jq)
    assert path.read_bytes().startswith(first) and len(path.read_bytes()) > len(first)
    assert path.stat().st_ino == inode


def test_a_failed_ledger_write_stops_the_run_loudly(monkeypatch, capsys, sandbox, runner, jq):
    def boom(path, row):
        raise OSError("disk full")

    monkeypatch.setattr(ledger, "append", boom)
    project_steps.run_project(jq)
    assert "disk full" in runtime.run_info["ledger_broken"]
    assert "LEDGER WRITE FAILED" in capsys.readouterr().out


def test_each_phase_hands_the_project_lock_to_its_process(sandbox, runner, jq):
    project_steps.run_project(jq)
    for call in runner.calls:
        [fd] = call.kwargs["pass_fds"]
        assert isinstance(fd, int)


def test_run_phase_without_kept_fds_passes_none(sandbox, runner, jq):
    phases.run_phase(jq, "pipeline", ["./run_pipeline_process.sh"])
    assert "pass_fds" not in runner.calls[0].kwargs


def test_cmd_run_writes_run_start_and_run_end_rows(ready):
    assert passes.cmd_run(run_args(jobs=2)) == 0
    rows = ledger.read(ready.root / "ledger.jsonl")
    assert [r["step"] for r in rows] == ["run-start", "run-end"]
    start, end = rows
    assert start["run_id"] == end["run_id"] == runtime.run_info["run_id"]
    assert start["tools"] == {"ctp_commit": "test"}
    flags = start["flags"]
    assert flags["cli"]["jobs"] == 2
    assert flags["manifest_path"] == str(ready.root / "manifest.tsv")
    assert (
        flags["manifest_sha256"]
        == hashlib.sha256((ready.root / "manifest.tsv").read_bytes()).hexdigest()
    )
    assert end["exit_code"] == 0 and end["outcomes"] == {"done": 1}


def test_cmd_run_stops_when_the_ledger_cannot_be_written(ready, monkeypatch):
    def boom(path, row):
        raise OSError("read-only file system")

    monkeypatch.setattr(ledger, "append", boom)
    with pytest.raises(SystemExit, match="cannot write the ledger"):
        passes.cmd_run(run_args())


def test_end_ledger_run_survives_a_failed_write(monkeypatch, capsys):
    def boom(path, row):
        raise OSError("gone")

    monkeypatch.setattr(ledger, "append", boom)
    run_ledger.end_ledger_run(0, time.time(), {})
    assert "cannot write the run-end row" in capsys.readouterr().out


def test_run_flags_records_every_option_and_the_manifest(sandbox):
    manifest = write_manifest(sandbox.root, VALID_ROW)
    runtime.opts.update(shard_classes=("L",), drop_memo=True)
    flags = run_ledger.run_flags(run_args(jobs=4), manifest)
    assert flags["effective"]["shard_classes"] == ["L"]
    assert flags["effective"]["drop_memo"] is True and flags["cli"]["drop_memo"] is False
    assert flags["cli"]["jobs"] == 4 and "fn" not in flags["cli"]
    assert flags["manifest_sha256"] == hashlib.sha256(manifest.read_bytes()).hexdigest()
    assert run_ledger.run_flags(run_args(), sandbox.root / "absent.tsv")["manifest_sha256"] == ""


def test_tool_versions_names_every_tool(monkeypatch, sandbox):
    outputs = {
        ("git", "-C", str(sandbox.root), "rev-parse", "HEAD"): "a" * 40,
        (
            "git",
            "-C",
            str(sandbox.root),
            "status",
            "--porcelain",
            "--untracked-files=no",
        ): " M ctp.py",
        ("git", "-C", str(sandbox.cregit), "rev-parse", "HEAD"): "b" * 40,
        ("git", "-C", str(sandbox.cregit), "status", "--porcelain", "--untracked-files=no"): "",
        ("srcml", "--version"): "srcml: 1.1.0\nlibsrcml: 1.1.0",
        ("git", "--version"): "git version 2.55.0",
    }
    monkeypatch.setattr(toolchain, "_capture", lambda cmd, cwd=None: outputs.get(tuple(cmd), ""))
    jar = sandbox.cregit / toolchain.BLOBEXEC_JAR
    jar.parent.mkdir(parents=True)
    jar.write_bytes(b"jar bytes")
    tools = REAL_TOOL_VERSIONS()
    assert tools["ctp_commit"] == "a" * 40 + "-dirty"
    assert tools["cregit_commit"] == "b" * 40
    assert tools["srcml_version"] == "srcml: 1.1.0"
    assert tools["blobexec_jar_sha256"] == hashlib.sha256(b"jar bytes").hexdigest()
    assert tools["git_version"] == "git version 2.55.0"


def test_tool_versions_leaves_a_missing_tool_blank(monkeypatch, sandbox):
    monkeypatch.setattr(toolchain, "_capture", lambda cmd, cwd=None: "")
    runtime.env.update(PATH=str(sandbox.root / "empty-bin"))
    tools = REAL_TOOL_VERSIONS()
    assert tools["ctp_commit"] == "" and tools["srcml_version"] == ""
    assert tools["srcml_path"] == "" and tools["blobexec_jar_sha256"] == ""


@pytest.mark.parametrize(
    "result, expected",
    [
        (SimpleNamespace(returncode=0, stdout=" out \n"), "out"),
        (SimpleNamespace(returncode=1, stdout="out"), ""),
    ],
)
def test_capture_returns_stdout_only_on_success(monkeypatch, result, expected):
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: result)
    assert toolchain._capture(["x"]) == expected


def test_capture_is_blank_when_the_tool_is_missing(monkeypatch):
    def boom(*a, **k):
        raise FileNotFoundError("srcml")

    monkeypatch.setattr(subprocess, "run", boom)
    assert toolchain._capture(["srcml"]) == ""


def test_audit_prints_one_line_per_step(sandbox, runner, pinned_jq, capsys):
    project_steps.run_project(pinned_jq)
    capsys.readouterr()
    assert report.cmd_audit(argparse.Namespace(project="jq", json=False)) == 0
    out = capsys.readouterr().out.splitlines()
    assert [line.split()[1] for line in out] == [
        "clone",
        "pipeline",
        "firm",
        "validate",
        "cleanup",
        "project",
    ]
    assert f"checked_out={PINNED_SHA}" in out[1]
    assert "state=done" in out[-1] and "removed=1 errors=0" in out[4]


def test_audit_json_prints_whole_rows(sandbox, runner, jq, capsys):
    fake_parquet(sandbox)
    project_steps.run_project(jq)
    capsys.readouterr()
    report.cmd_audit(argparse.Namespace(project="jq", json=True))
    rows = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert rows[-1]["state"] == "done"
    [validated] = [r for r in rows if r["step"] == "validate" and r["status"] == "ok"]
    assert validated["parquet"]["rows"] == 42
    assert [r["status"] for r in rows if r["step"] == "pipeline"] == ["started", "ok"]


def test_audit_marks_a_step_that_never_ended(sandbox, capsys):
    path = sandbox.root / "ledger.jsonl"
    base = {"project": "jq", "run_id": "r1", "attempt": 1}
    ledger.append(path, {**base, "step": "clone", "status": "started"})
    ledger.append(path, {**base, "step": "clone", "status": "ok"})
    ledger.append(path, {**base, "step": "pipeline", "status": "started"})
    report.cmd_audit(argparse.Namespace(project="jq", json=False))
    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 2
    assert "pipeline  interrupted" in lines[1] and "the run died here" in lines[1]


def test_audit_of_an_unknown_project_fails(sandbox, capsys):
    assert report.cmd_audit(argparse.Namespace(project="nope", json=False)) == 1
    assert "no rows" in capsys.readouterr().err


def test_audit_line_shows_a_failure_and_its_detail():
    line = report.audit_line(
        {"step": "project", "state": "failed", "failed_step": "clone", "detail": "commit missing"}
    )
    assert "state=failed" in line and "failed_step=clone" in line and "commit missing" in line
