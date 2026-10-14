"""Tests for ctplib/preflight.py: the refusals before a run and the provenance guard."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from ctplib import (
    cli,
    memory,
    passes,
    project_run,
    project_steps,
    runtime,
    toolchain,
)

from fakes import GIB, REAL_RUNNER_USAGE, VALID_ROW, forbidden, run_args, write_manifest

# --------------------------------------------------------------------------- #
# refusals before the run starts
# --------------------------------------------------------------------------- #


def memos(root: Path) -> str:
    (root / "memos").mkdir()
    return str(root / "memos")


def sidecar(root: Path) -> str:
    (root / "project_meta.json").write_text("{}")
    return str(root / "project_meta.json")


def lacks(usage_text: str) -> str:
    return REAL_RUNNER_USAGE.replace(usage_text, "--no-such-flag")


# (runner usage, run_args overrides, substrings of the refusal). A callable
# override is given the sandbox root, for options that need a real path.
REFUSALS = [
    pytest.param(lacks("--mask"), {}, ["does not accept: --mask"], id="runner-lacks-mask"),
    pytest.param(
        lacks("--gc MODE"), dict(gc="none"), ["--gc is not implemented"], id="runner-lacks-gc"
    ),
    pytest.param(
        lacks("--skip-html"),
        dict(skip_html=True),
        ["--skip-html is not implemented"],
        id="runner-lacks-skip-html",
    ),
    pytest.param(
        lacks("--memo-dir DIR"), dict(memo_dir=memos), ["--memo-dir"], id="runner-lacks-memo-dir"
    ),
    pytest.param(lacks("--shards N"), dict(shards=6), ["--shards needs"], id="runner-lacks-shards"),
    pytest.param(
        lacks("--jobs N"), dict(blame_jobs=8), ["--blame-jobs needs --jobs"], id="runner-lacks-jobs"
    ),
    pytest.param(
        lacks("--memory-limit SIZE"),
        dict(memory_limit="3GB"),
        ["--memory-limit is not implemented"],
        id="runner-lacks-memory-limit",
    ),
    pytest.param(
        lacks("--duckdb-threads N"),
        dict(duckdb_threads=2),
        ["--duckdb-threads is not implemented"],
        id="runner-lacks-duckdb-threads",
    ),
    pytest.param(
        lacks("--project-key NAME"),
        dict(project_meta=sidecar),
        ["--project-key"],
        id="runner-lacks-project-key",
    ),
    pytest.param(
        lacks("--mask-widened"),
        dict(mask_widened=True, from_step=2),
        ["--mask-widened is not implemented"],
        id="runner-lacks-mask-widened",
    ),
    pytest.param(
        lacks("--retokenize EXTS"),
        dict(retokenize="rs", from_step=2),
        ["--retokenize is not implemented"],
        id="runner-lacks-retokenize",
    ),
    pytest.param(
        None, dict(from_step=0), ["--from-step must be 1 or greater"], id="from-step-zero"
    ),
    pytest.param(
        None, dict(memo_dir=memos, drop_memo=True), ["contradict"], id="memo-dir-with-drop-memo"
    ),
    pytest.param(
        None,
        dict(memo_dir=lambda root: str(root / "absent")),
        ["not an existing directory"],
        id="memo-dir-absent",
    ),
    pytest.param(
        None, dict(blame_jobs=-1), ["--blame-jobs cannot be negative"], id="negative-blame-jobs"
    ),
    pytest.param(
        None,
        dict(duckdb_threads=-1),
        ["--duckdb-threads cannot be negative"],
        id="negative-duckdb-threads",
    ),
    # A percentage measures total RAM, and only the free part is usable.
    pytest.param(
        None,
        dict(memory_limit="80%"),
        ["--memory-limit", "not a percentage"],
        id="memory-limit-percentage",
    ),
    pytest.param(
        None,
        dict(project_meta=lambda root: str(root / "absent.json")),
        ["does not exist", "validate_schema.py"],
        id="sidecar-absent",
    ),
    # Step 1 deletes the blob map that both flags work on.
    pytest.param(
        None, dict(mask_widened=True, from_step=1), ["--from-step 2"], id="mask-widened-at-step-1"
    ),
    pytest.param(
        None,
        dict(mask_widened=True, from_step=2, shards=4),
        ["no recorded mask to widen"],
        id="mask-widened-with-shards",
    ),
    # Step 3 and later skip step 2, so the tokens would never be remade.
    *(
        pytest.param(
            None,
            dict(retokenize="rs", from_step=step),
            ["--from-step 2 exactly"],
            id=f"retokenize-at-step-{step}",
        )
        for step in (1, 3, 7)
    ),
    pytest.param(
        None,
        dict(retokenize="rs", from_step=2, shards=4),
        ["no cached tokenizations to invalidate"],
        id="retokenize-with-shards",
    ),
    pytest.param(
        None,
        dict(retokenize="rs", mask_widened=True, from_step=2),
        ["cannot be used in the same run"],
        id="retokenize-with-mask-widened",
    ),
    pytest.param(
        None,
        dict(allow_empty_provenance=False, from_step=10),
        ["reaches step 10"],
        id="provenance-gap-at-step-10",
    ),
]


@pytest.mark.parametrize("usage, over, said", REFUSALS)
def test_run_refuses_before_anything_starts(must_not_start, runner_script, usage, over, said):
    if usage is not None:
        runner_script(usage)
    over = {k: v(must_not_start.root) if callable(v) else v for k, v in over.items()}
    with pytest.raises(SystemExit) as exc:
        passes.cmd_run(run_args(**over))
    for text in said:
        assert text in str(exc.value)
    assert not (must_not_start.root / "runs.log").exists()


def accepted(over, opts=None, said=(), unsaid=(), ram=None, *, id):
    return pytest.param(over, opts or {}, said, unsaid, ram, id=id)


# run_args overrides, the _OPTS they must produce, and what the log must and
# must not say. ram patches available_bytes for the memory budget check.
ACCEPTED = [
    accepted(dict(gc="plain"), dict(gc="plain"), id="gc"),
    accepted(dict(skip_html=True), dict(skip_html=True), id="skip-html"),
    accepted(dict(memo_dir=memos), dict(memo_dir=lambda root: str(root / "memos")), id="memo-dir"),
    accepted(dict(blame_jobs=12), dict(blame_jobs=12), id="blame-jobs"),
    accepted(
        dict(memory_limit="3GB", duckdb_threads=2, jobs=2),
        dict(memory_limit="3GB", duckdb_threads=2),
        unsaid=["memory budget"],
        ram=32 * GIB,
        id="memory-flags-that-fit",
    ),
    accepted(
        dict(memory_limit="8GB", jobs=2),
        said=["WARNING: memory budget"],
        ram=6 * GIB,
        id="memory-budget-that-does-not-fit",
    ),
    accepted(
        dict(project_meta=sidecar, allow_empty_provenance=False),
        dict(project_meta=lambda root: str(root / "project_meta.json")),
        unsaid=["--allow-empty-provenance"],
        id="sidecar-needs-no-escape-hatch",
    ),
    accepted(
        dict(shards=2, shard_classes=" L , M ,"),
        dict(shard_classes=("L", "M")),
        id="shard-classes-parsed",
    ),
    accepted(
        dict(shards=6, shard_classes="L,M"),
        said=["sharding 6-way", "L, M"],
        id="shard-plan-announced",
    ),
    accepted(dict(from_step=3), said=["resuming at step 3"], id="resume-announced"),
    accepted(
        dict(mask_widened=True, from_step=2),
        said=["REUSE", "identity rows are discarded", "raw source"],
        id="mask-widened-announced",
    ),
    accepted(dict(from_step=2), unsaid=["--mask-widened"], id="no-mask-widened-no-notice"),
    # The sidecar records the manifest's mask, so an override makes it wrong.
    accepted(
        dict(mask=r"\.rs$"),
        said=["--mask overrides the manifest", r"\.rs$", "will not match"],
        id="mask-override-warned",
    ),
    accepted(dict(retokenize="rs", from_step=2), said=["DISCARD", "rs"], id="retokenize-announced"),
    accepted(dict(retokenize="   ", from_step=1), id="blank-retokenize-is-absent"),
    accepted(
        dict(allow_empty_provenance=True),
        dict(project_meta=""),
        said=["WARNING: --allow-empty-provenance", "--project-meta absent", "stratum"],
        id="escape-hatch-itemised",
    ),
    # Step 10 is the last step, so --from-step 11 can publish no Parquet.
    accepted(
        dict(allow_empty_provenance=False, from_step=11),
        unsaid=["refusing"],
        id="past-step-10-not-gated",
    ),
]


@pytest.mark.parametrize("over, opts, said, unsaid, ram", ACCEPTED)
def test_run_accepts_and_announces(ready, monkeypatch, capsys, over, opts, said, unsaid, ram):
    if ram is not None:
        monkeypatch.setattr(memory, "available_bytes", lambda: ram)

    def resolve(value):
        return value(ready.root) if callable(value) else value

    assert passes.cmd_run(run_args(**{k: resolve(v) for k, v in over.items()})) == 0
    for key, value in opts.items():
        assert runtime.opts[key] == resolve(value)
    out = capsys.readouterr().out
    for text in said:
        assert text in out
    for text in unsaid:
        assert text not in out


def test_cmd_run_with_an_empty_manifest_does_nothing(sandbox, monkeypatch, capsys):
    """No manifest rows means no devenv capture and no runs.log row."""
    write_manifest(sandbox.root)
    monkeypatch.setattr(toolchain, "capture_devenv_env", forbidden)
    assert passes.cmd_run(run_args()) == 0
    assert "nothing to run" in capsys.readouterr().out
    assert not (sandbox.root / "runs.log").exists()


def test_cmd_run_appends_a_start_and_an_end_row_to_runs_log(sandbox, monkeypatch):
    """Visibility contract: one start/end row per invocation, appended."""
    write_manifest(sandbox.root, VALID_ROW)
    (sandbox.root / "runs.log").write_text("2026-08-28T03:22:20+00:00\trun-start\tjobs=2\n")
    monkeypatch.setattr(toolchain, "capture_devenv_env", lambda: {"PATH": "/x"})
    monkeypatch.setattr(project_steps, "run_project", lambda p: project_run.RunOutcome.DONE)

    assert passes.cmd_run(run_args(jobs=2)) == 0
    lines = (sandbox.root / "runs.log").read_text().splitlines()
    assert len(lines) == 3
    assert lines[0].endswith("jobs=2")
    assert "run-start\tjobs=2\tprojects=1" in lines[1]
    assert "run-end\trc=0" in lines[2]


def test_cmd_run_returns_one_when_a_project_fails(sandbox, monkeypatch):
    """The exit code is what a cron wrapper or CI checks."""
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(toolchain, "capture_devenv_env", lambda: {"PATH": "/x"})
    monkeypatch.setattr(project_steps, "run_project", lambda p: project_run.RunOutcome.FAILED)
    assert passes.cmd_run(run_args()) == 1
    assert "run-end\trc=1" in (sandbox.root / "runs.log").read_text()


def test_cmd_run_starts_no_retry_pass_once_every_project_is_done(ready, capsys):
    assert passes.cmd_run(run_args(retries=3)) == 0
    assert "retry pass" not in capsys.readouterr().out


def test_cmd_run_retries_only_the_projects_that_are_not_done(sandbox, monkeypatch, capsys):
    """A retry pass must skip the finished projects, or a long corpus run
    repeats work it already paid for."""
    write_manifest(
        sandbox.root,
        VALID_ROW,
        "zstd\thttps://github.com/facebook/zstd.git\tenterprise\t\\.[ch]$\tS",
    )
    monkeypatch.setattr(toolchain, "capture_devenv_env", lambda: {"PATH": "/x"})

    seen: list[str] = []

    def flaky(project):
        seen.append(project["name"])
        if project["name"] == "zstd" and seen.count("zstd") == 1:
            return project_run.RunOutcome.FAILED
        return project_run.RunOutcome.DONE

    monkeypatch.setattr(project_steps, "run_project", flaky)
    assert passes.cmd_run(run_args(retries=1)) == 0
    assert seen == ["jq", "zstd", "zstd"]
    assert "retry pass 1: ['zstd']" in capsys.readouterr().out


# --------------------------------------------------------------------------- #
# --project-meta
# --------------------------------------------------------------------------- #


def test_a_relative_sidecar_path_is_made_absolute(sandbox, monkeypatch, runner_script):
    """The runner starts with cwd=CREGIT, but the path is typed relative to
    this repository."""
    runner_script()
    write_manifest(sandbox.root, VALID_ROW)
    meta = sandbox.root / "project_meta.json"
    meta.write_text("{}")
    monkeypatch.chdir(sandbox.root)
    monkeypatch.setattr(toolchain, "capture_devenv_env", lambda: {"PATH": "/x"})
    monkeypatch.setattr(project_steps, "run_project", lambda p: project_run.RunOutcome.DONE)

    assert (
        passes.cmd_run(run_args(project_meta="project_meta.json", allow_empty_provenance=False))
        == 0
    )
    assert Path(runtime.opts["project_meta"]).is_absolute()
    assert Path(runtime.opts["project_meta"]) == meta.resolve()


# --------------------------------------------------------------------------- #
# the provenance guard
# --------------------------------------------------------------------------- #


def test_a_run_without_the_sidecar_is_refused_and_names_the_columns(must_not_start):
    """ "provenance" alone does not say what is blank, and a refusal that does not
    name its escape hatch sends the operator to read the source."""
    with pytest.raises(SystemExit) as exc:
        passes.cmd_run(run_args(allow_empty_provenance=False))
    message = str(exc.value)
    for text in (
        "refusing this run",
        "--project-meta",
        "--allow-empty-provenance",
        "stratum",
        "history_cluster",
        "29",
        "validate.py passes it",
    ):
        assert text in message
    assert "firm" not in message
    assert not (must_not_start.root / "runs.log").exists()


def test_the_escape_hatch_is_off_until_it_is_typed(monkeypatch):
    """It must never arrive by default."""
    seen: dict = {}
    monkeypatch.setattr(passes, "cmd_run", lambda args: seen.update(vars(args)) or 0)

    monkeypatch.setattr(sys, "argv", ["ctp.py", "run"])
    assert cli.main() == 0
    assert seen["allow_empty_provenance"] is False

    seen.clear()
    monkeypatch.setattr(sys, "argv", ["ctp.py", "run", "--allow-empty-provenance"])
    assert cli.main() == 0
    assert seen["allow_empty_provenance"] is True


def test_the_escape_hatch_is_refused_when_nothing_would_be_blank(must_not_start):
    """Left in a launcher script, the hatch would silence the guard on the next
    run that does omit a flag."""
    with pytest.raises(SystemExit) as exc:
        passes.cmd_run(
            run_args(allow_empty_provenance=True, project_meta=sidecar(must_not_start.root))
        )
    assert "nothing to allow" in str(exc.value)
    assert "wrapper" in str(exc.value)


def test_a_sidecar_path_that_is_not_a_file_is_refused_by_its_own_check(must_not_start):
    """The sharper "does not exist" message must win over the guard's, which
    would send the operator looking for a flag they did pass."""
    typo = str(must_not_start.root / "typo")
    with pytest.raises(SystemExit) as exc:
        passes.cmd_run(run_args(allow_empty_provenance=False, project_meta=typo))
    message = str(exc.value)
    assert "--project-meta" in message and "does not exist" in message
    assert "refusing this run" not in message


@pytest.mark.parametrize("flag", ["--project-key", "--firm-map", "--firm-canonical"])
def test_ctp_run_refuses_a_flag_it_does_not_own(monkeypatch, capsys, flag):
    """run_project derives --project-key from the manifest name. cregit master
    has no firm columns, so ctp takes no firm flags."""
    monkeypatch.setattr(sys, "argv", ["ctp.py", "run", flag, "x"])
    monkeypatch.setattr(passes, "cmd_run", forbidden)
    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 2
    assert "unrecognized arguments" in capsys.readouterr().err
