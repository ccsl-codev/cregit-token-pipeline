"""Tests for the pinned commit: ctplib/pinning.py and the clone step."""

from __future__ import annotations

import subprocess
from types import SimpleNamespace

import pytest

from ctplib import (
    manifest_file,
    passes,
    pinning,
    project_run,
    project_steps,
    toolchain,
)

from fakes import (
    OTHER_SHA,
    PINNED_ROW,
    PINNED_SHA,
    REAL_RUNNER_USAGE,
    VALID_ROW,
    FakeRunner,
    run_args,
    write_manifest,
)

# --------------------------------------------------------------------------- #
# pinned commit
# --------------------------------------------------------------------------- #


def test_manifest_reads_a_sixth_column_as_the_pinned_commit(tmp_path):
    [row] = manifest_file.read_manifest(write_manifest(tmp_path, PINNED_ROW), None)
    assert row["commit"] == PINNED_SHA


def test_manifest_without_the_column_is_unpinned(tmp_path):
    [row] = manifest_file.read_manifest(write_manifest(tmp_path, VALID_ROW), None)
    assert "commit" not in row


@pytest.mark.parametrize("commit", ["abc123", PINNED_SHA.upper(), PINNED_SHA + "0", ""])
def test_manifest_rejects_a_commit_that_is_not_a_full_sha(tmp_path, commit):
    with pytest.raises(ValueError, match="40-character"):
        manifest_file.read_manifest(write_manifest(tmp_path, f"{VALID_ROW}\t{commit}"), None)


def test_manifest_rejects_seven_fields(tmp_path):
    with pytest.raises(ValueError, match="pinned commit"):
        manifest_file.read_manifest(write_manifest(tmp_path, f"{PINNED_ROW}\textra"), None)


def test_a_pinned_project_clones_first_then_runs_from_the_staging_clone(sandbox, runner, pinned_jq):
    assert project_steps.run_project(pinned_jq) == project_run.RunOutcome.DONE
    assert [c.phase for c in runner.calls] == ["clone", "pipeline", "git", "firm", "validate"]
    staging = sandbox.out / ".ctp-pinned" / "jq.git"
    assert runner.argv("clone") == [
        "python3",
        str(sandbox.root / "pin.py"),
        "--url",
        "https://github.com/jqlang/jq.git",
        "--commit",
        PINNED_SHA,
        "--dest",
        str(staging),
        "--result",
        str(pinning.pin_result_path("jq")),
    ]
    argv = runner.argv("pipeline")
    assert argv[argv.index("--repo-url") + 1] == str(staging)
    assert argv[argv.index("--commit-url") + 1] == "https://github.com/jqlang/jq/commit/"


def test_an_unpinned_project_clones_the_url_itself(runner, jq):
    assert project_steps.run_project(jq) == project_run.RunOutcome.DONE
    assert [c.phase for c in runner.calls] == ["pipeline", "firm", "validate"]
    argv = runner.argv("pipeline")
    assert argv[argv.index("--repo-url") + 1] == jq["url"]
    assert "--commit-url" not in argv


def test_a_failed_clone_stops_before_the_pipeline(monkeypatch, pinned_jq):
    r = FakeRunner(rcs={"clone": 3})
    monkeypatch.setattr(subprocess, "Popen", r)
    assert project_steps.run_project(pinned_jq) == project_run.RunOutcome.FAILED
    assert [c.phase for c in r.calls] == ["clone"]


def test_a_checkout_that_differs_from_the_pin_fails_the_project(monkeypatch, capsys, pinned_jq):
    r = FakeRunner(checkout=OTHER_SHA)
    monkeypatch.setattr(subprocess, "Popen", r)
    monkeypatch.setattr(subprocess, "run", r)
    assert project_steps.run_project(pinned_jq) == project_run.RunOutcome.FAILED
    assert "validate" not in [c.phase for c in r.calls]
    assert f"but the runner checked out {OTHER_SHA}" in capsys.readouterr().out


def test_an_unreadable_checkout_fails_a_pinned_project(monkeypatch, pinned_jq):
    monkeypatch.setattr(subprocess, "Popen", FakeRunner())
    monkeypatch.setattr(toolchain, "runner_checkout", lambda name, workdir: "")
    assert project_steps.run_project(pinned_jq) == project_run.RunOutcome.FAILED


def test_the_staging_clone_is_removed_once_the_project_validates(sandbox, runner, pinned_jq):
    staging = pinning.pinned_clone_path("jq")
    staging.mkdir(parents=True)
    (staging / "HEAD").write_text("ref: refs/heads/master\n")
    assert project_steps.run_project(pinned_jq) == project_run.RunOutcome.DONE
    assert not staging.exists()


def test_a_symlinked_staging_clone_is_not_followed(sandbox, tmp_path):
    victim = tmp_path / "victim"
    victim.mkdir()
    (victim / "keep").write_text("x")
    staging = pinning.pinned_clone_path("jq")
    staging.parent.mkdir(parents=True)
    staging.symlink_to(victim)
    assert pinning.drop_pinned_clone("jq")
    assert (victim / "keep").exists()


def test_git_head_is_empty_for_a_missing_repo(tmp_path):
    assert toolchain.git_head(tmp_path / "absent") == ""


@pytest.mark.parametrize(
    "result, expected",
    [
        (SimpleNamespace(returncode=0, stdout=f"{PINNED_SHA}\n"), PINNED_SHA),
        (SimpleNamespace(returncode=0, stdout="not a sha\n"), ""),
        (SimpleNamespace(returncode=128, stdout=f"{PINNED_SHA}\n"), ""),
    ],
)
def test_git_head_returns_only_a_full_sha(monkeypatch, tmp_path, result, expected):
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: result)
    assert toolchain.git_head(tmp_path) == expected


def test_git_head_is_empty_when_git_cannot_start(monkeypatch, tmp_path):
    def boom(*a, **k):
        raise OSError("no git")

    monkeypatch.setattr(subprocess, "run", boom)
    assert toolchain.git_head(tmp_path) == ""


def test_a_pinned_manifest_needs_a_runner_with_commit_url(must_not_start, runner_script):
    runner_script(REAL_RUNNER_USAGE.replace("--commit-url", "--commit-link"))
    write_manifest(must_not_start.root, PINNED_ROW)
    with pytest.raises(SystemExit, match="--commit-url"):
        passes.cmd_run(run_args())


def test_a_pinned_manifest_runs_on_a_runner_with_commit_url(ready):
    write_manifest(ready.root, PINNED_ROW)
    assert passes.cmd_run(run_args()) == 0


@pytest.mark.parametrize(
    "url, expected",
    [
        ("https://github.com/jqlang/jq.git", "https://github.com/jqlang/jq/commit/"),
        ("https://gitlab.com/a/b/", "https://gitlab.com/a/b/commit/"),
        ("https://example.org/r", "https://example.org/r/commit/"),
    ],
)
def test_commit_url_matches_the_runner_default(url, expected):
    assert pinning.commit_url(url) == expected
