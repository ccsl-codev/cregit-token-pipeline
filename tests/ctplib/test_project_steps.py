"""Tests for ctplib/project_steps.py: the steps of one project, the lock, the outcomes,
schema, join and cleanup."""

from __future__ import annotations

import argparse
import hashlib
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import retain
from ctplib import (
    cli,
    config,
    passes,
    preflight,
    project_run,
    project_steps,
    report,
    runtime,
    toolchain,
)

from fakes import FakeRunner, fake_parquet, ledger_rows, lock_is_free, run_args

# --------------------------------------------------------------------------- #
# idempotence and locking
# --------------------------------------------------------------------------- #


def test_validated_project_is_skipped_and_starts_no_subprocess(sandbox, runner, jq):
    """Idempotence contract: a completion stamp means the work is done. If this
    fails, a resumed run re-tokenizes finished projects for hours."""
    workdir = sandbox.out / "jq"
    workdir.mkdir()
    (workdir / "jq.validated").write_text("rows=42\nbytes=1\n")

    assert project_steps.run_project(jq) == "skipped"
    assert runner.calls == []


def test_second_concurrent_run_of_the_same_project_is_deferred(sandbox, runner, jq, held_lock):
    """Locking contract: one project, one worker. Two tokenizers in the same
    workdir corrupt blobExec's incremental state."""
    (sandbox.out / "jq").mkdir()
    with held_lock(retain.lock_path("jq")):
        assert project_steps.run_project(jq) == "deferred"
    assert runner.calls == []


@pytest.mark.parametrize(
    "runner_kwargs, outcome",
    [
        pytest.param({}, "done", id="success"),
        pytest.param(dict(pipeline_rc=2), "failed", id="failure"),
        pytest.param(dict(raise_on="pipeline"), "failed", id="popen-raises"),
    ],
)
def test_the_lock_is_released_after_every_outcome(monkeypatch, jq, runner_kwargs, outcome):
    """A leaked lock makes the next run report the project as RUNNING for ever."""
    monkeypatch.setattr(subprocess, "Popen", FakeRunner(**runner_kwargs))
    assert project_steps.run_project(jq) == outcome
    assert lock_is_free(retain.lock_path("jq"))


# --------------------------------------------------------------------------- #
# return states
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "pipeline_rc, validate_rc, expected",
    [
        pytest.param(0, 0, "done", id="both-ok"),
        pytest.param(2, 0, "failed", id="pipeline-exit-2"),
        pytest.param(139, 0, "failed", id="pipeline-signal"),
        pytest.param(0, 1, "failed", id="validate-gate-rejects"),
    ],
)
def test_run_project_return_state(monkeypatch, jq, pipeline_rc, validate_rc, expected):
    """run_project returns exactly one of done|skipped|failed|deferred; cmd_run
    computes the run's exit code from these strings."""
    monkeypatch.setattr(
        subprocess, "Popen", FakeRunner(pipeline_rc=pipeline_rc, validate_rc=validate_rc)
    )
    assert project_steps.run_project(jq) == expected


def test_run_project_defers_below_the_disk_floor(monkeypatch, runner, jq):
    """Deferring beats filling the disk: a truncated workdir would validate as
    FAILED and waste the tokenizer time already spent."""
    monkeypatch.setattr(config, "DISK_FLOOR_GB", 10**9)
    assert project_steps.run_project(jq) == "deferred"
    assert runner.calls == []


def test_a_done_project_writes_the_stamp_that_makes_the_next_run_skip(sandbox, runner, jq):
    """Closes the idempotence loop: state after 'done' must be the state that
    yields 'skipped'."""
    assert project_steps.run_project(jq) == "done"
    assert (sandbox.out / "jq" / "jq.validated").exists()
    assert project_steps.run_project(jq) == "skipped"


# --------------------------------------------------------------------------- #
# schema check and join
# --------------------------------------------------------------------------- #


def test_join_runs_the_schema_check_then_the_join(sandbox, runner, jq, joining):
    assert project_steps.run_project(jq) == project_run.RunOutcome.DONE
    assert [c.phase for c in runner.calls] == ["pipeline", "firm", "validate", "schema", "join"]
    parquet = str(sandbox.out / "jq" / "jq-dataset.parquet")
    assert runner.argv("schema") == ["python3", str(sandbox.root / "validate_schema.py"), parquet]
    assert runner.argv("join") == [
        "python3",
        str(sandbox.root / "consolidate.py"),
        "--join",
        "jq",
        "--manifest",
        str(joining),
    ]
    steps = [r["step"] for r in ledger_rows(sandbox, "jq")]
    assert steps == ["pipeline", "firm", "validate", "schema", "join", "cleanup", "project"]


@pytest.mark.parametrize("step", ["schema", "join"])
def test_a_failed_schema_check_or_join_fails_the_project(monkeypatch, sandbox, jq, joining, step):
    r = FakeRunner(rcs={step: 1})
    monkeypatch.setattr(subprocess, "Popen", r)
    assert project_steps.run_project(jq) == project_run.RunOutcome.FAILED
    [final] = ledger_rows(sandbox, "jq", "project")
    assert final["failed_step"] == step
    assert not ledger_rows(sandbox, "jq", "cleanup")
    if step == "schema":
        assert "join" not in [c.phase for c in r.calls]


def test_without_join_no_schema_or_join_step_runs(runner, jq):
    project_steps.run_project(jq)
    assert [c.phase for c in runner.calls] == ["pipeline", "firm", "validate"]


def test_a_validated_project_resumes_at_the_steps_after_validation(sandbox, runner, jq, joining):
    fake_parquet(sandbox)
    (sandbox.out / "jq" / "jq.validated").write_text("rows=7\nbytes=9\n")
    assert project_steps.run_project(jq, resume_validated=True) == project_run.RunOutcome.DONE
    assert [c.phase for c in runner.calls] == ["schema", "join"]
    [final] = ledger_rows(sandbox, "jq", "project")
    assert final["state"] == "done" and final["parquet"]["rows"] == 7


def test_a_validated_project_is_still_skipped_by_default(sandbox, runner, jq, joining):
    (sandbox.out / "jq").mkdir()
    (sandbox.out / "jq" / "jq.validated").write_text("rows=7\n")
    assert project_steps.run_project(jq) == project_run.RunOutcome.SKIPPED
    assert runner.calls == [] and ledger_rows(sandbox) == []


def test_resuming_ignores_the_disk_floor(monkeypatch, sandbox, runner, jq, joining):
    """The steps after validation write almost nothing; the floor guards a new clone."""
    monkeypatch.setattr(config, "DISK_FLOOR_GB", 10**9)
    (sandbox.out / "jq").mkdir()
    (sandbox.out / "jq" / "jq.validated").write_text("rows=7\n")
    assert project_steps.run_project(jq, resume_validated=True) == project_run.RunOutcome.DONE


def test_run_options_carry_join_and_the_absolute_manifest(sandbox, runner_script):
    runner_script()
    opts = preflight.run_options(run_args(join=True))
    assert opts["join"] is True
    assert opts["manifest"] == str((sandbox.root / "manifest.tsv").resolve())
    assert preflight.run_options(run_args())["join"] is False


def test_cmd_db_forwards_each_manifest_as_an_absolute_path(sandbox, monkeypatch):
    monkeypatch.setattr(toolchain, "capture_devenv_env", lambda: {})
    seen = {}
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda args, **k: seen.update(args=args) or SimpleNamespace(returncode=0),
    )
    assert report.cmd_db(argparse.Namespace(manifest=["a.tsv", "b.tsv"])) == 0
    assert seen["args"][2:] == [
        "--manifest",
        str(sandbox.root / "a.tsv"),
        "--manifest",
        str(sandbox.root / "b.tsv"),
    ]


# --------------------------------------------------------------------------- #
# cleanup, --drop-html and --anonymize
# --------------------------------------------------------------------------- #


def runner_leftovers(sandbox, name="jq") -> Path:
    """The work files a real runner leaves beside the Parquet."""
    workdir = sandbox.out / name
    for d in (f"{name}-original.git", f"{name}-cregit", "memo", "blame", "html"):
        (workdir / d).mkdir(parents=True, exist_ok=True)
        (workdir / d / "f").write_bytes(b"x" * 4096)
    (workdir / f"{name}-blobmap.db").write_bytes(b"d" * 4096)
    (workdir / "pipeline.log").write_text("log\n")
    return workdir


def test_cleanup_removes_the_work_files_and_keeps_the_parquet(sandbox, runner, jq):
    fake_parquet(sandbox)
    workdir = runner_leftovers(sandbox)
    runtime.opts.update(cleanup=True)
    assert project_steps.run_project(jq) == project_run.RunOutcome.DONE
    assert sorted(p.name for p in workdir.iterdir()) == [
        "html",
        "jq-dataset.parquet",
        "jq.validated",
    ]
    [row] = ledger_rows(sandbox, "jq", "cleanup")
    assert row["status"] == "ok"
    assert set(row["cleanup"]["removed"]) == {
        "jq-original.git",
        "jq-cregit",
        "memo",
        "blame",
        "jq-blobmap.db",
        "pipeline.log",
    }
    assert row["cleanup"]["bytes_freed"] > 0
    assert sorted(row["cleanup"]["kept"]) == ["html", "jq-dataset.parquet", "jq.validated"]
    # The logs and the ledger live outside the workdir and stay.
    assert list((sandbox.root / "state" / "jq" / "logs").glob("pipeline-*.log"))


def test_drop_html_removes_html_in_the_cleanup(sandbox, runner, jq):
    fake_parquet(sandbox)
    workdir = runner_leftovers(sandbox)
    runtime.opts.update(cleanup=True, drop_html=True)
    project_steps.run_project(jq)
    assert not (workdir / "html").exists()


def test_a_failed_cleanup_makes_the_project_done_dirty(monkeypatch, sandbox, runner, jq):
    fake_parquet(sandbox)
    runner_leftovers(sandbox)
    runtime.opts.update(cleanup=True)
    monkeypatch.setattr(
        retain,
        "clean_workdir",
        lambda name, drop_html: {
            "removed": [],
            "kept": [],
            "errors": ["memo (Permission denied)"],
            "bytes_freed": 0,
        },
    )
    assert project_steps.run_project(jq) == project_run.RunOutcome.DONE_DIRTY
    [row] = ledger_rows(sandbox, "jq", "cleanup")
    assert row["status"] == "failed" and row["cleanup"]["errors"] == ["memo (Permission denied)"]
    assert ledger_rows(sandbox, "jq", "project")[0]["state"] == "done-dirty"


def test_without_cleanup_the_work_files_stay(sandbox, runner, jq):
    fake_parquet(sandbox)
    workdir = runner_leftovers(sandbox)
    project_steps.run_project(jq)
    assert (workdir / "memo").exists() and (workdir / "jq-original.git").exists()


@pytest.fixture
def anonymizer(sandbox):
    script = sandbox.root / "anonymize_parquet.py"
    script.write_text("# stand-in\n")
    runtime.opts.update(anonymize=str(script))
    return script


def test_anonymize_runs_after_the_schema_check_and_before_the_join(
    sandbox, runner, jq, joining, anonymizer
):
    project_steps.run_project(jq)
    assert [c.phase for c in runner.calls] == [
        "pipeline",
        "firm",
        "validate",
        "schema",
        "anonymize",
        "join",
    ]
    workdir = sandbox.out / "jq"
    assert runner.argv("anonymize") == [
        "python3",
        str(anonymizer),
        str(workdir / "anon"),
        str(workdir / "jq-dataset.parquet"),
    ]
    [row] = ledger_rows(sandbox, "jq", "anonymize")
    [out] = row["anonymized"]
    assert out["path"] == str(workdir / "anon" / "jq-dataset.parquet")
    assert out["sha256"] == hashlib.sha256(b"PAR1 anon PAR1").hexdigest()


def test_anonymize_without_join_still_runs(sandbox, runner, jq, anonymizer):
    assert project_steps.run_project(jq) == project_run.RunOutcome.DONE
    assert [c.phase for c in runner.calls] == ["pipeline", "firm", "validate", "anonymize"]


def test_an_anonymizer_that_writes_nothing_fails_the_project(monkeypatch, sandbox, jq, anonymizer):
    monkeypatch.setattr(subprocess, "Popen", FakeRunner(anon_output=False))
    assert project_steps.run_project(jq) == project_run.RunOutcome.FAILED
    [row] = ledger_rows(sandbox, "jq", "anonymize")
    assert row["exit_code"] == 1 and "wrote no Parquet" in row["detail"]


def test_drop_html_needs_cleanup(must_not_start):
    with pytest.raises(SystemExit, match="--drop-html deletes html/ in the cleanup"):
        passes.cmd_run(run_args(drop_html=True))


def test_anonymize_needs_an_existing_script(must_not_start):
    with pytest.raises(SystemExit, match="is not a file"):
        passes.cmd_run(run_args(anonymize="/nonexistent/anon.py"))


def test_run_options_carry_cleanup_drop_html_and_the_anonymizer(sandbox, runner_script):
    runner_script()
    script = sandbox.root / "anon.py"
    script.write_text("")
    opts = preflight.run_options(run_args(cleanup=True, drop_html=True, anonymize=str(script)))
    assert opts["cleanup"] is True and opts["drop_html"] is True
    assert opts["anonymize"] == str(script.resolve())
    defaults = preflight.run_options(run_args())
    assert (defaults["cleanup"], defaults["drop_html"], defaults["anonymize"]) == (False, False, "")


def test_drop_html_and_anonymize_are_off_by_default_on_the_cli(monkeypatch):
    seen = {}
    monkeypatch.setattr(passes, "cmd_run", lambda args: seen.update(vars(args)) or 0)
    monkeypatch.setattr(sys, "argv", ["ctp.py", "run"])
    cli.main()
    assert seen["drop_html"] is False and seen["anonymize"] == "" and seen["cleanup"] is False
