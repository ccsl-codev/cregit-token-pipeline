"""Tests for ctplib/workdir_state.py: --memo-dir, and how an attempt resumes from the
workdir an earlier attempt left."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

import retain
from ctplib import (
    project_run,
    project_steps,
    runtime,
    scheduler,
    toolchain,
    workdir_state,
)

from fakes import OTHER_SHA, PINNED_SHA, FakeRunner, ledger_rows, projects_of

# --------------------------------------------------------------------------- #
# --memo-dir
# --------------------------------------------------------------------------- #


def test_memo_dir_is_forwarded_as_one_subdirectory_per_project(sandbox, runner, jq):
    """tokenBySha.pl keys the memo on the sha1 of the file contents alone, so a
    shared directory would serve one project's tokens to another."""
    memo_root = sandbox.root / "memos"
    memo_root.mkdir()
    runtime.opts.update(skip_html=False, drop_memo=False, memo_dir=str(memo_root))
    project_steps.run_project(jq)

    argv = runner.argv("pipeline")
    assert argv[argv.index("--memo-dir") + 1] == str(memo_root / "jq")


def test_a_memo_dir_inside_the_work_directory_is_refused(sandbox, runner, jq):
    """A memo the runner's own wipe can reach is worse than no flag at all: the
    operator believes it is safe and it is not."""
    runtime.opts.update(
        skip_html=False, drop_memo=False, memo_dir=str(sandbox.out / "jq" / "inner")
    )
    assert project_steps.run_project(jq) == "failed"


# --------------------------------------------------------------------------- #
# census resumes an interrupted project at step 2
# --------------------------------------------------------------------------- #


def leftover_attempt(sandbox, name="jq"):
    workdir = sandbox.out / name
    (workdir / f"{name}-original.git").mkdir(parents=True)
    (workdir / f"{name}-blobmap.db").write_bytes(b"map")
    return workdir


def test_an_interrupted_pinned_project_resumes_at_step_2(monkeypatch, sandbox, pinned_jq):
    leftover_attempt(sandbox)
    r = FakeRunner()
    monkeypatch.setattr(subprocess, "Popen", r)
    monkeypatch.setattr(subprocess, "run", r)
    runtime.opts.update(auto_resume=True, from_step=1)
    assert project_steps.run_project(pinned_jq) == project_run.RunOutcome.DONE
    assert runner_argv_tail(r) == "2"
    [row] = ledger_rows(sandbox, "jq", "pipeline")
    assert row["argv"][-1] == "2"


def runner_argv_tail(r):
    return r.argv("pipeline")[-1]


def test_a_clone_of_another_commit_restarts_at_step_1(monkeypatch, sandbox, pinned_jq):
    leftover_attempt(sandbox)
    r = FakeRunner(checkout=OTHER_SHA)
    monkeypatch.setattr(subprocess, "run", r)
    assert workdir_state.resume_step("jq", sandbox.out / "jq", PINNED_SHA) == 1


def test_no_blob_map_means_step_1(sandbox):
    (sandbox.out / "jq" / "jq-original.git").mkdir(parents=True)
    assert workdir_state.resume_step("jq", sandbox.out / "jq", PINNED_SHA) == 1


@pytest.mark.parametrize(
    "head, step",
    [
        pytest.param("", 1, id="an-unreadable-clone-means-step-1"),
        pytest.param(OTHER_SHA, 2, id="an-unpinned-leftover-resumes-at-step-2"),
    ],
)
def test_resume_step_of_an_unpinned_leftover(sandbox, monkeypatch, head, step):
    leftover_attempt(sandbox)
    monkeypatch.setattr(toolchain, "git_head", lambda repo: head)
    assert workdir_state.resume_step("jq", sandbox.out / "jq", "") == step


def test_run_without_auto_resume_keeps_step_1(monkeypatch, sandbox, pinned_jq):
    leftover_attempt(sandbox)
    r = FakeRunner()
    monkeypatch.setattr(subprocess, "Popen", r)
    monkeypatch.setattr(subprocess, "run", r)
    project_steps.run_project(pinned_jq)
    assert "2" not in r.argv("pipeline")[-1:]


def test_an_explicit_from_step_wins_over_auto_resume(monkeypatch, sandbox, pinned_jq):
    leftover_attempt(sandbox)
    r = FakeRunner()
    monkeypatch.setattr(subprocess, "Popen", r)
    monkeypatch.setattr(subprocess, "run", r)
    runtime.opts.update(auto_resume=True, from_step=7)
    project_steps.run_project(pinned_jq)
    assert r.argv("pipeline")[-1] == "7"


def test_next_job_takes_the_first_job_whose_wait_is_over():
    from collections import deque

    late = scheduler.Job(1, {"name": "a"}, 1, 0, False, not_before=100.0)
    ready = scheduler.Job(2, {"name": "b"}, 1, 0, False)
    queue = deque([late, ready])
    assert scheduler.next_job(queue, now=50.0) is ready and list(queue) == [late]
    assert scheduler.next_job(queue, now=50.0) is None
    assert scheduler.next_job(queue, now=100.0) is late and not queue


def test_a_deferred_project_backs_off(sandbox, fake_projects, no_gates, capsys):
    fake = fake_projects(
        outcomes={"a": [project_run.RunOutcome.DEFERRED] * 2 + [project_run.RunOutcome.DONE]}
    )
    scheduler.schedule(projects_of(sandbox, "a"), 1, 0, sandbox.root / "STOP", poll=0.01)
    assert fake.names() == ["a", "a", "a"]
    out = capsys.readouterr().out
    assert "deferred; next try in 0s" in out


def test_the_deferral_wait_is_capped(monkeypatch, sandbox, fake_projects, no_gates, capsys):
    monkeypatch.setattr(scheduler, "DEFER_MAX_S", 0.05)
    fake_projects(outcomes={"a": [project_run.RunOutcome.DEFERRED, project_run.RunOutcome.DONE]})
    scheduler.schedule(projects_of(sandbox, "a"), 1, 0, sandbox.root / "STOP", poll=10)
    assert "deferred; next try in 0s" in capsys.readouterr().out


def test_a_step_2_resume_drops_the_derived_artifacts_first(monkeypatch, sandbox, pinned_jq):
    workdir = leftover_attempt(sandbox)
    for entry in ("blame", "jq-original", "jq-cregit"):
        (workdir / entry).mkdir()
    for entry in ("jq-cregit.db", "jq-dataset.parquet"):
        (workdir / entry).write_bytes(b"x")
    (workdir / "memo").mkdir()
    (workdir / "jq-cregit.git").mkdir()
    seen = {}
    r = FakeRunner()

    def popen(args, **kwargs):
        if FakeRunner.phase_of(args) == "pipeline":
            seen["left"] = sorted(p.name for p in workdir.iterdir())
        return r(args, **kwargs)

    monkeypatch.setattr(subprocess, "Popen", popen)
    monkeypatch.setattr(subprocess, "run", r)
    runtime.opts.update(auto_resume=True, from_step=1)
    project_steps.run_project(pinned_jq)
    # The bare repos, the blob map and the memo stay; the rest is rebuilt.
    assert seen["left"] == ["jq-blobmap.db", "jq-cregit.git", "jq-original.git", "memo"]
    [row] = ledger_rows(sandbox, "jq", "pipeline")
    assert row["resume"] == {
        "from_step": 2,
        "dropped": ["blame", "jq-original", "jq-cregit", "jq-cregit.db", "jq-dataset.parquet"],
    }


def test_a_resume_that_cannot_drop_fails_without_running_the_pipeline(
    monkeypatch, sandbox, pinned_jq
):
    workdir = leftover_attempt(sandbox)
    (workdir / "blame").mkdir()
    r = FakeRunner()
    monkeypatch.setattr(subprocess, "Popen", r)
    monkeypatch.setattr(subprocess, "run", r)
    monkeypatch.setattr(retain, "remove_tree", lambda p: [f"{p} (busy)"])
    runtime.opts.update(auto_resume=True, from_step=1)
    assert project_steps.run_project(pinned_jq) == project_run.RunOutcome.FAILED
    assert "pipeline" not in [c.phase for c in r.calls]
    [row] = ledger_rows(sandbox, "jq", "pipeline")
    assert "could not drop" in row["detail"]


def test_drop_derived_reports_a_file_it_cannot_unlink(monkeypatch, sandbox):
    workdir = sandbox.out / "jq"
    workdir.mkdir()
    (workdir / "jq-cregit.db").write_bytes(b"x")
    real_unlink = Path.unlink

    def unlink(self, *a, **k):
        if self.name == "jq-cregit.db":
            raise PermissionError("read-only")
        return real_unlink(self, *a, **k)

    monkeypatch.setattr(Path, "unlink", unlink)
    dropped, errors = workdir_state.drop_derived("jq", workdir)
    assert dropped == [] and "read-only" in errors[0]
