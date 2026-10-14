"""Tests for ctplib/runner_args.py: the arguments ctp gives the runner."""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

import retain
from ctplib import (
    cli,
    manifest_file,
    passes,
    preflight,
    project_run,
    project_steps,
    report,
    runner_args,
    runtime,
    toolchain,
)
from file_mask import UNIVERSAL_MASK

from fakes import REAL_RUNNER_USAGE, VALID_ROW, FakeRunner, forbidden, run_args, write_manifest

# --------------------------------------------------------------------------- #
# script_supports
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "text, flag, expected",
    [
        pytest.param(REAL_RUNNER_USAGE, "--skip-html", True, id="flag-present"),
        pytest.param(REAL_RUNNER_USAGE, "--file-filter", False, id="flag-absent"),
        pytest.param(REAL_RUNNER_USAGE, "--work-dir", False, id="prefix-is-not-a-match"),
    ],
)
def test_script_supports_reads_the_configured_runner(runner_script, text, flag, expected):
    """The refusal in cmd_run is only as good as this probe."""
    runner_script(text)
    assert runner_args.script_supports(flag) is expected


def test_script_supports_returns_false_when_the_script_is_missing(sandbox):
    """A missing checkout must report 'unsupported', never raise: cmd_run calls
    this before anything else and a traceback there hides the real problem."""
    (sandbox.cregit / "run_pipeline_process.sh").unlink()
    assert runner_args.script_supports("--skip-html") is False


# --------------------------------------------------------------------------- #
# flag handling
# --------------------------------------------------------------------------- #


# flag -> value it must carry, or None for a bare flag.
FORWARDED = [
    pytest.param(dict(skip_html=True), {"--skip-html": None}, id="skip-html"),
    pytest.param(dict(gc="none"), {"--gc": "none"}, id="gc"),
    pytest.param(dict(blame_jobs=8), {"--jobs": "8"}, id="blame-jobs-as-runner-jobs"),
    pytest.param(dict(memory_limit="3GB"), {"--memory-limit": "3GB"}, id="memory-limit"),
    pytest.param(dict(duckdb_threads=2), {"--duckdb-threads": "2"}, id="duckdb-threads"),
    # The key is the manifest name, so it is not the operator's to get wrong.
    pytest.param(
        dict(project_meta="/somewhere/project_meta.json"),
        {"--project-meta": "/somewhere/project_meta.json", "--project-key": "jq"},
        id="sidecar-and-its-key",
    ),
    pytest.param(dict(mask_widened=True, from_step=2), {"--mask-widened": None}, id="mask-widened"),
    pytest.param(dict(retokenize="rs", from_step=2), {"--retokenize": "rs"}, id="retokenize"),
    pytest.param(
        dict(shards=4, shard_classes=("S", "M")),
        {"--mode": "sharded", "--shards": "4"},
        id="shards-for-a-listed-class",
    ),
]


NOT_SENT = [
    pytest.param(dict(skip_html=False), ["--skip-html"], id="skip-html-off"),
    pytest.param(dict(gc=None), ["--gc"], id="no-gc"),
    pytest.param(dict(memo_dir=""), ["--memo-dir"], id="no-memo-dir"),
    pytest.param(dict(blame_jobs=0), ["--jobs"], id="no-blame-jobs"),
    pytest.param(dict(blame_jobs=8), ["--blame-jobs"], id="old-blame-jobs-name"),
    pytest.param(
        dict(memory_limit=None, duckdb_threads=0),
        ["--memory-limit", "--duckdb-threads"],
        id="no-memory-flags",
    ),
    pytest.param(dict(project_meta=""), ["--project-meta", "--project-key"], id="no-sidecar"),
    pytest.param(dict(from_step=2, mask_widened=False), ["--mask-widened"], id="no-mask-widened"),
    pytest.param(dict(from_step=2, retokenize=""), ["--retokenize"], id="no-retokenize"),
    pytest.param(dict(from_step=1), ["1"], id="step-one-is-the-runner-default"),
    pytest.param(dict(shards=0, shard_classes=("L",)), ["--mode", "--shards"], id="no-shards"),
    pytest.param(dict(shards=1, shard_classes=("S",)), ["--mode"], id="one-shard"),
    pytest.param(dict(shards=6, shard_classes=("L",)), ["--mode"], id="class-not-listed"),
    pytest.param({}, ["--mask-widened", "--retokenize", "--mode"], id="keys-unset"),
]


@pytest.mark.parametrize("opts, sent", FORWARDED)
def test_an_option_reaches_the_runner_argv(runner, jq, opts, sent):
    runtime.opts.update({"skip_html": False, "drop_memo": False, **opts})
    project_steps.run_project(jq)
    argv = runner.argv("pipeline")
    for flag, value in sent.items():
        assert argv.count(flag) == 1
        if value is not None:
            assert argv[argv.index(flag) + 1] == value


@pytest.mark.parametrize("opts, absent", NOT_SENT)
def test_an_option_left_off_sends_no_flag(runner, jq, opts, absent):
    runtime.opts.update({"skip_html": False, "drop_memo": False, **opts})
    project_steps.run_project(jq)
    argv = runner.argv("pipeline")
    for flag in absent:
        assert flag not in argv


def test_an_unset_from_step_means_a_full_run(runner, jq):
    """run_project reads _OPTS directly, so a caller that never set it must
    get step 1, not a crash."""
    runtime.opts.update(skip_html=False, drop_memo=False)
    project_steps.run_project(jq)
    assert runner.argv("pipeline")[-1] == "\\.[ch]$"


def test_from_step_is_appended_last_because_it_is_positional(runner, jq):
    """FROM_STEP is a positional argument. The runner reads it from the tail of
    argv, so it must follow every flag, including --skip-html."""
    runtime.opts.update(skip_html=True, drop_memo=False, from_step=3)
    project_steps.run_project(jq)
    argv = runner.argv("pipeline")
    assert argv[-1] == "3"
    assert argv[-2] == "--skip-html"


def test_no_memo_is_an_alias_for_drop_memo(monkeypatch):
    """The alias must set the same flag, or the disk-frugal run silently keeps
    memo/ and fills the disk."""
    seen = {}
    monkeypatch.setattr(sys, "argv", ["ctp.py", "run", "--no-memo"])
    monkeypatch.setattr(passes, "cmd_run", lambda args: seen.update(vars(args)) or 0)
    assert cli.main() == 0
    assert seen["drop_memo"] is True


def test_no_memo_warns_that_it_does_not_prevent_the_write(sandbox, monkeypatch, capsys):
    """Honesty contract: memo/ is always written because the tokenizer needs
    BFG_MEMO_DIR. The alias must say so instead of implying prevention."""
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(sys, "argv", ["ctp.py", "run", "--no-memo"])
    monkeypatch.setattr(toolchain, "capture_devenv_env", lambda: {"PATH": "/x"})
    monkeypatch.setattr(project_steps, "run_project", lambda p: project_run.RunOutcome.DONE)

    args = run_args(jobs=1, retries=0, skip_html=False, drop_memo=True)
    passes.cmd_run(args)
    out = capsys.readouterr().out
    assert "alias for --drop-memo" in out
    assert "does NOT prevent the write" in out


def test_drop_memo_prunes_only_after_the_project_validates(sandbox, runner, jq):
    """Ordering contract: pruning before the stamp exists would delete memo/
    from a project that then fails validation and has to be re-tokenized."""
    stamp = sandbox.out / "jq" / "jq.validated"
    seen = []

    def fake_prune(name, subtrees, *, apply):
        seen.append(dict(name=name, subtrees=subtrees, apply=apply, stamped=stamp.exists()))
        return 1024, True

    runtime.opts.update(skip_html=False, drop_memo=True)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(retain, "prune", fake_prune)
        assert project_steps.run_project(jq) == "done"

    assert seen == [dict(name="jq", subtrees=("memo",), apply=True, stamped=True)]


@pytest.mark.parametrize(
    "runner_kwargs",
    [
        pytest.param(dict(pipeline_rc=2), id="pipeline-fails"),
        pytest.param(dict(validate_rc=1), id="validation-fails"),
    ],
)
def test_drop_memo_does_not_prune_a_failed_project(monkeypatch, jq, runner_kwargs):
    """A failed project keeps memo/ so blobExec can resume incrementally."""
    monkeypatch.setattr(subprocess, "Popen", FakeRunner(**runner_kwargs))
    monkeypatch.setattr(retain, "prune", forbidden)
    runtime.opts.update(skip_html=False, drop_memo=True)
    assert project_steps.run_project(jq) == "failed"


def test_drop_memo_reports_when_retain_refuses(monkeypatch, capsys, runner, jq):
    """retain.prune re-checks the keepers itself. A refusal must be visible,
    and must not turn a validated project into a failure: it is done-dirty."""
    monkeypatch.setattr(retain, "prune", lambda *a, **k: (0, False))
    runtime.opts.update(skip_html=False, drop_memo=True)
    outcome = project_steps.run_project(jq)
    assert outcome == "done-dirty" and outcome.is_success
    assert "retain refused the prune" in capsys.readouterr().out


# --------------------------------------------------------------------------- #
# argument construction
# --------------------------------------------------------------------------- #


def test_pipeline_argv_is_built_exactly_like_this(sandbox, runner, jq):
    """The runner takes --work and --mask; --work-dir or --file-filter exit 2."""
    runtime.opts.update(skip_html=False, drop_memo=False)
    project_steps.run_project(jq)

    assert runner.argv("pipeline") == [
        "./run_pipeline_process.sh",
        "--repo-url",
        "https://github.com/jqlang/jq.git",
        "--repo-name",
        "jq",
        "--work",
        str(sandbox.out / "jq"),
        "--mask",
        r"\.[ch]$",
    ]
    assert runner.calls[0].kwargs["cwd"] == sandbox.cregit
    sent = {a for a in runner.argv("pipeline") if a.startswith("--")}
    assert sent == set(preflight.REQUIRED_RUNNER_FLAGS), "the preflight constant drifted"


class WipingRunner(FakeRunner):
    """Fails the pipeline after `rm -rf "$WORK"`, as run_pipeline_process.sh
    does at FROM_STEP=1 and from its EXIT trap."""

    def __call__(self, args, **kwargs):
        if self.phase_of(args) == "pipeline":
            shutil.rmtree(Path(args[args.index("--work") + 1]))
        return super().__call__(args, **kwargs)


def test_the_log_and_the_lock_survive_the_runner_deleting_the_workdir(sandbox, monkeypatch, jq):
    monkeypatch.setattr(subprocess, "Popen", WipingRunner(pipeline_rc=2))
    assert project_steps.run_project(jq) == "failed"

    assert not (sandbox.out / "jq").exists()
    assert list((retain.state_dir("jq") / "logs").glob("pipeline-*.log"))
    assert retain.lock_path("jq").exists()
    assert not retain.lock_path("jq").is_relative_to(sandbox.out / "jq")


def test_status_reports_failed_after_the_runner_wiped_the_workdir(sandbox, monkeypatch, capsys, jq):
    """The workdir is gone, so only state_dir shows the project was attempted."""
    monkeypatch.setattr(subprocess, "Popen", WipingRunner(pipeline_rc=2))
    project_steps.run_project(jq)

    write_manifest(sandbox.root, VALID_ROW)
    report.cmd_status(argparse.Namespace(manifest="manifest.tsv"))
    line = next(x for x in capsys.readouterr().out.splitlines() if x.startswith("jq "))
    assert "FAILED" in line


def test_validate_argv_is_built_exactly_like_this(sandbox, runner, jq):
    project_steps.run_project(jq)
    assert runner.argv("validate") == [
        "python3",
        str(sandbox.root / "validate.py"),
        str(sandbox.out / "jq" / "jq-dataset.parquet"),
        str(sandbox.out / "jq" / "jq.validated"),
    ]


@pytest.mark.parametrize(
    "opts, tail",
    [
        pytest.param({}, [], id="duckdb-defaults"),
        pytest.param(
            dict(memory_limit="4GB", duckdb_threads=2),
            ["--memory-limit", "4GB", "--threads", "2"],
            id="runner-limits",
        ),
    ],
)
def test_firm_argv_is_built_exactly_like_this(sandbox, runner, jq, opts, tail):
    runtime.opts.update(opts)
    project_steps.run_project(jq)
    assert runner.argv("firm") == [
        "python3",
        str(sandbox.root / "firm_attribution.py"),
        str(sandbox.out / "jq" / "jq-dataset.parquet"),
        *tail,
    ]


def test_the_firm_phase_runs_between_pipeline_and_validate(sandbox, runner, jq):
    """validate gates the 70-column form, so it must see the file after firm."""
    assert project_steps.run_project(jq) == "done"
    assert [c.phase for c in runner.calls] == ["pipeline", "firm", "validate"]


def test_a_failed_firm_phase_fails_the_project_unstamped(sandbox, monkeypatch, jq):
    runner = FakeRunner(firm_rc=1)
    monkeypatch.setattr(subprocess, "Popen", runner)
    assert project_steps.run_project(jq) == "failed"
    assert [c.phase for c in runner.calls] == ["pipeline", "firm"]
    assert not (sandbox.out / "jq" / "jq.validated").exists()


# --------------------------------------------------------------------------- #
# sharding
# --------------------------------------------------------------------------- #


def test_shard_class_helper_needs_both_a_count_and_a_matching_class():
    """The two conditions are independent, so both are checked here rather than
    inferred from run_project's argv."""
    L = dict(size_class="L")
    runtime.opts.clear()
    runtime.opts.update(shards=6, shard_classes=("L",))
    assert runner_args.shard_class(L) is True
    runtime.opts.update(shards=1)
    assert runner_args.shard_class(L) is False
    runtime.opts.update(shards=6, shard_classes=("M",))
    assert runner_args.shard_class(L) is False
    runtime.opts.clear()
    assert runner_args.shard_class(L) is False, "an unset _OPTS must not shard"


# --------------------------------------------------------------------------- #
# the universal mask and --mask
# --------------------------------------------------------------------------- #


def test_a_blank_file_filter_column_falls_back_to_the_universal_mask(tmp_path):
    """blobExec rejects an empty mask, so a blank column means the universal one."""
    row = "jq\thttps://github.com/jqlang/jq.git\tcommunity\t\tS"
    projects = manifest_file.read_manifest(write_manifest(tmp_path, row), None)
    assert projects[0]["file_filter"] == UNIVERSAL_MASK


def test_the_manifests_mask_is_what_reaches_the_runner(sandbox, runner, jq):
    """The Parquet's file_mask column must describe the mask that actually ran."""
    runtime.opts.update(skip_html=False, drop_memo=False)
    project_steps.run_project(dict(jq, file_filter=UNIVERSAL_MASK))
    argv = runner.argv("pipeline")
    assert argv[argv.index("--mask") + 1] == UNIVERSAL_MASK


def test_mask_overrides_the_manifest_for_one_deliberate_run(sandbox, runner, jq):
    """The escape hatch: one project, one mask, without editing the manifest."""
    runtime.opts.update(skip_html=False, drop_memo=False, mask=r"\.java$")
    project_steps.run_project(dict(jq, file_filter=UNIVERSAL_MASK))
    argv = runner.argv("pipeline")
    assert argv[argv.index("--mask") + 1] == r"\.java$"


# --tokenizer-worker: the runner's worker pool in step 2.


WORKER_USAGE = (
    REAL_RUNNER_USAGE + "#   --tokenizer-worker  tokenize with long-lived worker processes\n"
)


def test_tokenizer_worker_is_passed_to_the_runner(sandbox, runner, jq):
    opts = {**runtime.opts, "tokenizer_worker": True}
    argv = runner_args.build_pipeline_args(jq, opts, sandbox.out / "jq")
    assert "--tokenizer-worker" in argv
    assert "--tokenizer-worker" not in runner_args.build_pipeline_args(
        jq, runtime.opts, sandbox.out / "jq"
    )


def test_tokenizer_worker_needs_a_runner_that_has_it(must_not_start, runner_script):
    runner_script(REAL_RUNNER_USAGE)
    with pytest.raises(SystemExit) as exc:
        passes.cmd_run(run_args(tokenizer_worker=True))
    assert "--tokenizer-worker is not implemented" in str(exc.value)


def test_tokenizer_worker_and_shards_are_refused_together(must_not_start, runner_script):
    runner_script(WORKER_USAGE)
    with pytest.raises(SystemExit) as exc:
        passes.cmd_run(run_args(tokenizer_worker=True, shards=4))
    assert "--tokenizer-worker needs the runner's pipeline mode" in str(exc.value)


def test_tokenizer_worker_is_an_effective_option(sandbox, runner_script):
    runner_script(WORKER_USAGE)
    assert preflight.run_options(run_args(tokenizer_worker=True))["tokenizer_worker"] is True
