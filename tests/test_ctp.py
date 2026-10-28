"""Unit tests for ctp.py, the corpus orchestrator.

No test starts a real process. The autouse `sandbox` fixture replaces
subprocess.run and subprocess.Popen with a guard that raises, and moves every
path constant (CORPUS, OUT, CREGIT, STATE, METRICS, RUNS_LOG) into tmp_path. A test
that needs a subprocess installs its own recording fake. Nothing here touches
the real corpus-files tree, metrics.tsv, runs.log or ctp.duckdb.

Several tests carry `xfail(strict=True)`. Each one states a contract the module
docstring promises but the code does not keep. They are documentation, not
requests: do not "fix" them by changing the assertion.
"""

from __future__ import annotations

import argparse
import fcntl
import os
import shutil
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

import ctp
from file_mask import UNIVERSAL_MASK


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #

VALID_ROW = "jq\thttps://github.com/jqlang/jq.git\tcommunity\t\\.[ch]$\tS"

# The flags the configured runner (cregit-issue61/run_pipeline_process.sh)
# actually advertises. Used to build a stand-in script in tmp_path so no test
# reads the real checkout.
#
# Keep this in step with the real runner's usage() text. It drifted once and the
# drift was invisible: cregit-issue61 7a70a92 renamed --blame-jobs to --jobs on
# 2026-09-18, mid-run, and because script_supports() only greps the script while
# these tests grep this copy, the suite stayed green while every real invocation
# exited at the preflight. Re-read the runner's usage() when a flag changes.
REAL_RUNNER_USAGE = """\
#!/bin/sh
# usage: run_pipeline_process.sh --repo-url URL [options] [FROM_STEP]
#   --repo-url URL    git URL of the repository to process (REQUIRED)
#   --repo-name NAME  short name used to prefix the output files
#   --commit-url URL  commit browse URL
#   --mask REGEX      regex selecting the files to tokenize; quote it
#   --work DIR        working/output directory
#   --skip-html       do not generate the HTML views
#   --memo-dir DIR    where to memoize tokenized blobs (default: <work>/memo)
#   --gc MODE         how to pack the generated cregit repo after tokenizing
#   --memory-limit SIZE    forward the DuckDB heap cap to step 10
#   --duckdb-threads N     forward the DuckDB sorting thread count to step 10
#   --project-meta PATH    forward the provenance sidecar to step 10
#   --project-key NAME     which key of the sidecar this project is
#   --firm-map PATH        forward the domain->firm CSV to step 10
#   --firm-canonical PATH  forward the canonical firm-name table to step 10
#   --mask-widened    resume across a mask change, reusing blob_map
#   --retokenize EXTS re-tokenize only these extensions after a tokenizer fix
#   --mode MODE       tokenizer mode
#   --shards N        shard count
#   --jobs N      concurrent blame/HTML processes
# unknown arguments exit 2
"""


class FakeRunner:
    """Recording stand-in for subprocess.run. Starts no process.

    Recognises the two phases ctp drives, returns a scripted return code for
    each, and writes the completion stamp when the validate phase succeeds —
    which is what the real validate.py does.
    """

    def __init__(self, *, pipeline_rc=0, validate_rc=0, raise_on=None, payload=b""):
        self.calls: list[SimpleNamespace] = []
        self.pipeline_rc = pipeline_rc
        self.validate_rc = validate_rc
        self.raise_on = raise_on
        self.payload = payload

    @staticmethod
    def phase_of(args) -> str:
        return "pipeline" if str(args[0]).endswith("run_pipeline_process.sh") else "validate"

    def __call__(self, args, **kwargs):
        phase = self.phase_of(args)
        self.calls.append(SimpleNamespace(phase=phase, args=list(args), kwargs=kwargs))
        if self.raise_on == phase:
            raise OSError(f"no such file or directory: {args[0]}")
        stream = kwargs.get("stdout")
        if self.payload and hasattr(stream, "write"):
            stream.write(self.payload)
        rc = self.pipeline_rc if phase == "pipeline" else self.validate_rc
        if phase == "validate" and rc == 0:
            stamp = Path(args[3])
            # A relative stamp path lands in the repository root, outside
            # tmp_path. One test did exactly that and left a stray file named `s`
            # in the working tree. Refuse it here, so the whole class is closed.
            assert stamp.is_absolute(), (
                f"stamp path must be absolute, got {stamp!r}. A relative path "
                f"escapes tmp_path and writes into the repository.")
            stamp.write_text("rows=42\nbytes=123456\n")
        # Serves both call shapes. run_phase uses Popen, so it needs .pid and
        # .wait(); older assertions read .returncode. The pid is our own, so the
        # resource sampler walks a real process tree without starting anything.
        return SimpleNamespace(returncode=rc, pid=os.getpid(), wait=lambda: rc)

    def argv(self, phase: str) -> list[str]:
        return [c.args for c in self.calls if c.phase == phase][0]


class Clock:
    """Frozen stand-in for ctp.datetime. ctp only ever calls .now()."""

    def __init__(self, moment: datetime):
        self.moment = moment

    def now(self, tz=None):
        return self.moment.replace(tzinfo=tz) if tz else self.moment


def forbidden(*args, **kwargs):
    raise AssertionError(
        "the test tried to start a real process; monkeypatch subprocess first")


@contextmanager
def held_lock(path: Path):
    """Hold an exclusive flock on `path` for the duration of the block."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fh = path.open("w")
    fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        yield
    finally:
        fcntl.flock(fh, fcntl.LOCK_UN)
        fh.close()


def lock_is_free(path: Path) -> bool:
    with path.open("w") as fh:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        fcntl.flock(fh, fcntl.LOCK_UN)
        return True


def write_manifest(tmp_path: Path, *lines: str, name: str = "manifest.tsv") -> Path:
    path = tmp_path / name
    path.write_text("".join(f"{line}\n" for line in lines))
    return path


# --------------------------------------------------------------------------- #
# fixtures
# --------------------------------------------------------------------------- #

@pytest.fixture(autouse=True)
def sandbox(tmp_path, monkeypatch):
    """Redirect every path and side effect of ctp into tmp_path.

    Locks in nothing about behaviour; it exists so a failing test can never
    write to the live corpus or launch run_pipeline_process.sh.
    """
    out = tmp_path / "corpus-files"
    out.mkdir()
    cregit = tmp_path / "cregit"
    cregit.mkdir()
    # The sandbox stands for a REAL configured checkout, so it advertises the
    # flags the real runner advertises. An empty CREGIT is what let ctp send
    # --work-dir/--file-filter unnoticed: cmd_run's preflight had nothing to read.
    # A test that needs another runner calls the runner_script fixture.
    (cregit / "run_pipeline_process.sh").write_text(REAL_RUNNER_USAGE)

    monkeypatch.setattr(ctp, "CORPUS", tmp_path)
    monkeypatch.setattr(ctp, "OUT", out)
    monkeypatch.setattr(ctp, "CREGIT", cregit)
    monkeypatch.setattr(ctp, "METRICS", tmp_path / "metrics.tsv")
    monkeypatch.setattr(ctp, "RUNS_LOG", tmp_path / "runs.log")
    monkeypatch.setattr(ctp, "RESOURCES", tmp_path / "resources.tsv")
    # STATE holds the logs and the per-project lock. Without this patch the tests
    # would write both into the real repository. That is the stray-file class the
    # absolute-stamp assertion in FakeRunner already closed once.
    monkeypatch.setattr(ctp, "STATE", tmp_path / "state")
    # A real disk_usage call stays, but the floor can never trip by accident.
    monkeypatch.setattr(ctp, "DISK_FLOOR_GB", 0)
    monkeypatch.setattr(ctp.time, "sleep", lambda *a, **k: None)
    monkeypatch.setattr(ctp.subprocess, "run", forbidden)
    monkeypatch.setattr(ctp.subprocess, "Popen", forbidden)

    for shared in (ctp._OPTS, ctp._ENV, ctp._live):
        shared.clear()
    yield SimpleNamespace(root=tmp_path, out=out, cregit=cregit)
    for shared in (ctp._OPTS, ctp._ENV, ctp._live):
        shared.clear()


@pytest.fixture
def clock(monkeypatch):
    c = Clock(datetime(2026, 1, 2, 3, 4, 5))
    monkeypatch.setattr(ctp, "datetime", c)
    return c


@pytest.fixture
def jq():
    return dict(name="jq", url="https://github.com/jqlang/jq.git",
                category="community", file_filter=r"\.[ch]$", size_class="S")


@pytest.fixture
def runner(monkeypatch):
    r = FakeRunner()
    monkeypatch.setattr(ctp.subprocess, "run", r)
    monkeypatch.setattr(ctp.subprocess, "Popen", r)
    return r


@pytest.fixture
def runner_script(sandbox):
    """Write a stand-in run_pipeline_process.sh into the sandboxed CREGIT."""
    def _write(text: str = REAL_RUNNER_USAGE) -> Path:
        path = sandbox.cregit / "run_pipeline_process.sh"
        path.write_text(text)
        return path
    return _write


# --------------------------------------------------------------------------- #
# manifest parsing
# --------------------------------------------------------------------------- #

def test_manifest_parses_a_valid_row(tmp_path):
    """Every manifest field must reach the project dict under its own key."""
    projects = ctp.read_manifest(write_manifest(tmp_path, VALID_ROW), None)
    assert projects == [dict(name="jq", url="https://github.com/jqlang/jq.git",
                             category="community", file_filter=r"\.[ch]$",
                             size_class="S")]


def test_manifest_skips_comment_lines(tmp_path):
    """A '#' line is documentation. Parsing it would break every run."""
    path = write_manifest(tmp_path, "# name  url  category  filter  class", VALID_ROW)
    assert [p["name"] for p in ctp.read_manifest(path, None)] == ["jq"]


def test_manifest_skips_blank_and_whitespace_only_lines(tmp_path):
    """Blank separators are allowed; a blank line must not become a project."""
    path = write_manifest(tmp_path, "", "   ", VALID_ROW, "\t", "")
    assert len(ctp.read_manifest(path, None)) == 1


@pytest.mark.parametrize("row, fields", [
    pytest.param("jq\thttps://x.git\tcommunity\t\\.[ch]$", 4, id="four-fields"),
    pytest.param("jq\thttps://x.git\tcommunity\t\\.[ch]$\tS\textra", 6, id="six-fields"),
])
def test_manifest_rejects_a_row_without_exactly_five_fields(tmp_path, row, fields):
    """Five tab-separated fields are the contract. A short or long row is a
    corrupt manifest, and running the corpus off it would mislabel projects."""
    with pytest.raises(ValueError):
        ctp.read_manifest(write_manifest(tmp_path, row), None)


@pytest.mark.xfail(strict=True, reason=(
    "read_manifest lets the raw tuple-unpacking ValueError escape. The message "
    "is 'not enough values to unpack (expected 5, got 4)' and names neither the "
    "offending line nor its number, so an operator cannot find the bad row."))
def test_manifest_error_names_the_offending_line(tmp_path):
    """A parse error must point at the row that caused it."""
    path = write_manifest(tmp_path, VALID_ROW, "zstd\thttps://z.git\tenterprise\t\\.[ch]$")
    with pytest.raises(ValueError) as exc:
        ctp.read_manifest(path, None)
    assert "zstd" in str(exc.value)


def test_manifest_accepts_an_unknown_size_class_today(tmp_path):
    """Documents current behaviour: size_class is copied through unchecked."""
    row = "jq\thttps://x.git\tcommunity\t\\.[ch]$\tXL"
    assert ctp.read_manifest(write_manifest(tmp_path, row), None)[0]["size_class"] == "XL"


@pytest.mark.xfail(strict=True, reason=(
    "read_manifest performs no size_class validation. The docstring says "
    "S | M | L and the class drives the --jobs mix, so a typo such as 'XL' "
    "silently reaches metrics.tsv and the DuckDB index."))
def test_manifest_rejects_a_bad_size_class(tmp_path):
    """size_class must be one of S, M, L."""
    row = "jq\thttps://x.git\tcommunity\t\\.[ch]$\tXL"
    with pytest.raises(ValueError):
        ctp.read_manifest(write_manifest(tmp_path, row), None)


def test_manifest_empty_returns_no_projects(tmp_path):
    """An empty manifest is not an error; cmd_run reports 'nothing to run'."""
    assert ctp.read_manifest(write_manifest(tmp_path), None) == []


def test_manifest_only_filter_keeps_just_the_named_projects(tmp_path):
    """--only must not silently widen to the whole corpus."""
    path = write_manifest(
        tmp_path, VALID_ROW,
        "zstd\thttps://github.com/facebook/zstd.git\tenterprise\t\\.[ch]$\tS",
        "tmux\thttps://github.com/tmux/tmux.git\tcommunity\t\\.[ch]$\tS")
    assert [p["name"] for p in ctp.read_manifest(path, {"zstd"})] == ["zstd"]


# --------------------------------------------------------------------------- #
# script_supports
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("text, flag, expected", [
    pytest.param(REAL_RUNNER_USAGE, "--skip-html", True, id="flag-present"),
    pytest.param(REAL_RUNNER_USAGE, "--file-filter", False, id="flag-absent"),
    pytest.param(REAL_RUNNER_USAGE, "--work-dir", False, id="prefix-is-not-a-match"),
])
def test_script_supports_reads_the_configured_runner(runner_script, text, flag, expected):
    """The refusal in cmd_run is only as good as this probe."""
    runner_script(text)
    assert ctp.script_supports(flag) is expected


def test_script_supports_returns_false_when_the_script_is_missing(sandbox):
    """A missing checkout must report 'unsupported', never raise: cmd_run calls
    this before anything else and a traceback there hides the real problem."""
    (sandbox.cregit / "run_pipeline_process.sh").unlink()
    assert ctp.script_supports("--skip-html") is False


# --------------------------------------------------------------------------- #
# idempotence and locking
# --------------------------------------------------------------------------- #

def test_validated_project_is_skipped_and_starts_no_subprocess(sandbox, runner, jq):
    """Idempotence contract: a completion stamp means the work is done. If this
    fails, a resumed run re-tokenizes finished projects for hours."""
    workdir = sandbox.out / "jq"
    workdir.mkdir()
    (workdir / "jq.validated").write_text("rows=42\nbytes=1\n")

    assert ctp.run_project(jq) == "skipped"
    assert runner.calls == []


def test_second_concurrent_run_of_the_same_project_is_deferred(sandbox, runner, jq):
    """Locking contract: one project, one worker. Two tokenizers in the same
    workdir corrupt blobExec's incremental state."""
    (sandbox.out / "jq").mkdir()
    with held_lock(ctp.lock_path("jq")):
        assert ctp.run_project(jq) == "deferred"
    assert runner.calls == []


def test_lock_is_released_after_a_successful_project(sandbox, runner, jq):
    """A leaked lock makes the next run report the project as RUNNING for ever."""
    assert ctp.run_project(jq) == "done"
    assert lock_is_free(ctp.lock_path("jq"))


def test_lock_is_released_after_a_failed_project(sandbox, monkeypatch, jq):
    """Same contract on the failure path, which is the common one."""
    monkeypatch.setattr(ctp.subprocess, "Popen", FakeRunner(pipeline_rc=2))
    assert ctp.run_project(jq) == "failed"
    assert lock_is_free(ctp.lock_path("jq"))


def test_lock_is_released_when_the_subprocess_raises(sandbox, monkeypatch, jq):
    """The exception escapes run_project (see the xfail below), but the finally
    block must still hand the lock back."""
    monkeypatch.setattr(ctp.subprocess, "Popen", FakeRunner(raise_on="pipeline"))
    with pytest.raises(OSError):
        ctp.run_project(jq)
    assert lock_is_free(ctp.lock_path("jq"))


# --------------------------------------------------------------------------- #
# flag handling
# --------------------------------------------------------------------------- #

def test_skip_html_is_forwarded_to_the_runner(runner, jq):
    """--skip-html only saves the 94-255 MB per project if it reaches the
    runner argv."""
    ctp._OPTS.update(skip_html=True, drop_memo=False)
    ctp.run_project(jq)
    assert runner.argv("pipeline")[-1] == "--skip-html"


def test_skip_html_is_absent_when_the_flag_is_off(runner, jq):
    """The default run must not change the runner's behaviour."""
    ctp._OPTS.update(skip_html=False, drop_memo=False)
    ctp.run_project(jq)
    assert "--skip-html" not in runner.argv("pipeline")


def test_from_step_is_appended_last_because_it_is_positional(runner, jq):
    """FROM_STEP is a positional argument. The runner reads it from the tail of
    argv, so it must follow every flag, including --skip-html."""
    ctp._OPTS.update(skip_html=True, drop_memo=False, from_step=3)
    ctp.run_project(jq)
    argv = runner.argv("pipeline")
    assert argv[-1] == "3"
    assert argv[-2] == "--skip-html"


def test_from_step_one_sends_no_positional_at_all(runner, jq):
    """Step 1 is the runner's own default. Sending it explicitly would change
    nothing, and an absent argument cannot be misread."""
    ctp._OPTS.update(skip_html=False, drop_memo=False, from_step=1)
    ctp.run_project(jq)
    assert "1" not in runner.argv("pipeline")


def test_a_missing_from_step_option_behaves_as_step_one(runner, jq):
    """run_project reads _OPTS directly, so a caller that never set from_step
    must still get a full run rather than a crash."""
    ctp._OPTS.clear()
    ctp._OPTS.update(skip_html=False, drop_memo=False)
    ctp.run_project(jq)
    assert runner.argv("pipeline")[-1] == "\\.[ch]$"


def test_gc_mode_is_forwarded_to_the_runner(runner, jq):
    """The repack default costs hours per project at corpus scale, so the
    choice has to reach the runner argv to mean anything."""
    ctp._OPTS.update(skip_html=False, drop_memo=False, gc="none")
    ctp.run_project(jq)
    argv = runner.argv("pipeline")
    assert argv[argv.index("--gc") + 1] == "none"


def test_no_gc_option_leaves_the_runner_default_alone(runner, jq):
    """Omitting --gc must not silently pick a mode for the runner."""
    ctp._OPTS.update(skip_html=False, drop_memo=False, gc=None)
    ctp.run_project(jq)
    assert "--gc" not in runner.argv("pipeline")


def test_run_refuses_to_start_when_the_runner_lacks_gc(
        sandbox, monkeypatch, runner_script):
    """A checkout without --gc still packs unconditionally, and an unguarded
    repack failure fires the EXIT trap that deletes the workdir. Refuse rather
    than let --gc look effective while the old danger remains."""
    runner_script(REAL_RUNNER_USAGE.replace("--gc MODE", "--no-such-flag"))
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", forbidden)

    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(run_args(gc="none"))
    assert "--gc is not implemented" in str(exc.value)
    assert not (sandbox.root / "runs.log").exists()


def test_run_accepts_gc_on_a_checkout_that_implements_it(
        sandbox, monkeypatch, runner_script):
    """The refusal must not fire on the patched runner."""
    runner_script()
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", lambda: {"PATH": "/x"})
    monkeypatch.setattr(ctp, "run_project", lambda p: "done")

    assert ctp.cmd_run(run_args(gc="plain")) == 0
    assert ctp._OPTS["gc"] == "plain"


def test_run_rejects_a_from_step_below_one(sandbox, monkeypatch, runner_script):
    """Step 0 is not a step. Catch it before the devenv capture, because the
    runner would treat it as a full run and wipe the workdir it was meant to
    resume."""
    runner_script()
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", forbidden)

    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(run_args(from_step=0))
    assert "--from-step must be 1 or greater" in str(exc.value)


def test_run_announces_a_resume_so_the_operator_sees_it(
        sandbox, monkeypatch, runner_script, capsys):
    """A resume keeps whatever is already on disk. Say so, because the
    difference between step 1 and step 3 is 15.2 h of tokenizing."""
    runner_script()
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", lambda: {"PATH": "/x"})
    monkeypatch.setattr(ctp, "run_project", lambda p: "done")

    assert ctp.cmd_run(run_args(from_step=3)) == 0
    assert "resuming at step 3" in capsys.readouterr().out


def test_run_refuses_to_start_when_the_runner_lacks_a_required_flag(
        sandbox, monkeypatch, runner_script):
    """A defect, turned into a gate. Every project passes --work and --mask, so a
    runner that does not know them costs one exit-2 per project and produces
    nothing. Refuse once instead, before the devenv capture starts anything.
    """
    runner_script(REAL_RUNNER_USAGE.replace("--mask", "--file-filter"))
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", forbidden)

    args = run_args(jobs=1, retries=0, skip_html=False, drop_memo=False)
    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(args)
    assert "does not accept: --mask" in str(exc.value)
    assert not (sandbox.root / "runs.log").exists()


def test_run_refuses_to_start_when_the_runner_lacks_skip_html(
        sandbox, monkeypatch, runner_script):
    """Prevention contract: rather than generate the HTML and delete it later,
    the run must refuse. The check happens before the devenv capture, so no
    process starts."""
    runner_script(REAL_RUNNER_USAGE.replace("--skip-html", "--no-such-flag"))
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", forbidden)

    args = run_args(jobs=1, retries=0, skip_html=True, drop_memo=False)
    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(args)
    assert "--skip-html is not implemented" in str(exc.value)
    assert not (sandbox.root / "runs.log").exists()


def test_run_starts_when_the_runner_does_support_skip_html(
        sandbox, monkeypatch, runner_script):
    """The refusal must not fire on a checkout that does implement the flag."""
    runner_script()
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", lambda: {"PATH": "/x"})
    monkeypatch.setattr(ctp, "run_project", lambda p: "done")

    args = run_args(jobs=1, retries=0, skip_html=True, drop_memo=False)
    assert ctp.cmd_run(args) == 0
    assert ctp._OPTS["skip_html"] is True


def test_no_memo_is_an_alias_for_drop_memo(monkeypatch):
    """The alias must set the same flag, or the disk-frugal run silently keeps
    memo/ and fills the disk."""
    seen = {}
    monkeypatch.setattr(ctp.sys, "argv", ["ctp.py", "run", "--no-memo"])
    monkeypatch.setattr(ctp, "cmd_run", lambda args: seen.update(vars(args)) or 0)
    assert ctp.main() == 0
    assert seen["drop_memo"] is True


def test_no_memo_warns_that_it_does_not_prevent_the_write(
        sandbox, monkeypatch, capsys):
    """Honesty contract: memo/ is always written because the tokenizer needs
    BFG_MEMO_DIR. The alias must say so instead of implying prevention."""
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp.sys, "argv", ["ctp.py", "run", "--no-memo"])
    monkeypatch.setattr(ctp, "capture_devenv_env", lambda: {"PATH": "/x"})
    monkeypatch.setattr(ctp, "run_project", lambda p: "done")

    args = run_args(jobs=1, retries=0, skip_html=False, drop_memo=True)
    ctp.cmd_run(args)
    out = capsys.readouterr().out
    assert "alias for --drop-memo" in out
    assert "does NOT prevent the write" in out


def test_drop_memo_prunes_only_after_the_project_validates(sandbox, runner, jq):
    """Ordering contract: pruning before the stamp exists would delete memo/
    from a project that then fails validation and has to be re-tokenized."""
    stamp = sandbox.out / "jq" / "jq.validated"
    seen = []

    def fake_prune(name, subtrees, *, apply):
        seen.append(dict(name=name, subtrees=subtrees, apply=apply,
                         stamped=stamp.exists()))
        return 1024, True

    ctp._OPTS.update(skip_html=False, drop_memo=True)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(ctp.retain, "prune", fake_prune)
        assert ctp.run_project(jq) == "done"

    assert seen == [dict(name="jq", subtrees=("memo",), apply=True, stamped=True)]


def test_drop_memo_does_not_prune_when_the_pipeline_fails(monkeypatch, jq):
    """A failed project keeps memo/ so blobExec can resume incrementally."""
    monkeypatch.setattr(ctp.subprocess, "Popen", FakeRunner(pipeline_rc=2))
    monkeypatch.setattr(ctp.retain, "prune", forbidden)
    ctp._OPTS.update(skip_html=False, drop_memo=True)
    assert ctp.run_project(jq) == "failed"


def test_drop_memo_does_not_prune_when_validation_fails(monkeypatch, jq):
    """Same for a project whose parquet fails the gate."""
    monkeypatch.setattr(ctp.subprocess, "Popen", FakeRunner(validate_rc=1))
    monkeypatch.setattr(ctp.retain, "prune", forbidden)
    ctp._OPTS.update(skip_html=False, drop_memo=True)
    assert ctp.run_project(jq) == "failed"


def test_drop_memo_reports_when_retain_refuses(monkeypatch, capsys, runner, jq):
    """retain.prune re-checks the keepers itself. A refusal must be visible,
    and must not turn a validated project into a failure."""
    monkeypatch.setattr(ctp.retain, "prune", lambda *a, **k: (0, False))
    ctp._OPTS.update(skip_html=False, drop_memo=True)
    assert ctp.run_project(jq) == "done"
    assert "retain refused the prune" in capsys.readouterr().out


# --------------------------------------------------------------------------- #
# --memo-dir: the memo has to survive the wipe.
#
# run_pipeline_process.sh deletes the whole work directory at FROM_STEP=1, and
# the memo used to live inside it. That was affordable while a re-run could
# resume; it is not any more. The mask changed corpus-wide on 2026-09-19 and
# Mapping.open refuses to resume against a different stored mask, so all 64
# re-run projects rebuild from step 1. torvalds__linux holds ~2.6 million memo
# entries against 3,228,137 blobs, and tokenizeByBlobId/tokenBySha.pl serves a
# memo hit without invoking srcml at all — so those entries are the difference
# between a commit walk and a cold tokenize of the largest repository here.
# --------------------------------------------------------------------------- #

def test_memo_dir_is_forwarded_as_one_subdirectory_per_project(sandbox, runner, jq):
    """One directory per project, never one shared one: tokenBySha.pl keys the
    memo on sha1 of the file CONTENTS, with neither the repository nor the
    extension in the key, so a shared directory would serve one project's tokens
    to another and identical bytes under a different extension are a different
    language."""
    memo_root = sandbox.root / "memos"
    memo_root.mkdir()
    ctp._OPTS.update(skip_html=False, drop_memo=False, memo_dir=str(memo_root))
    ctp.run_project(jq)

    argv = runner.argv("pipeline")
    assert argv[argv.index("--memo-dir") + 1] == str(memo_root / "jq")
    # And outside the directory the runner deletes, which is the whole point.
    assert not str(memo_root / "jq").startswith(str(sandbox.out / "jq"))


def test_no_memo_dir_sends_no_flag_so_the_default_is_untouched(sandbox, runner, jq):
    """Nothing existing may change behaviour: without the flag the runner keeps
    putting the memo in <work>/memo exactly as before."""
    ctp._OPTS.update(skip_html=False, drop_memo=False, memo_dir="")
    ctp.run_project(jq)
    assert "--memo-dir" not in runner.argv("pipeline")


def test_a_memo_dir_inside_the_work_directory_is_refused(sandbox, runner, jq):
    """A memo the runner's own wipe can reach is worse than no flag at all: the
    operator believes it is safe and it is not."""
    ctp._OPTS.update(skip_html=False, drop_memo=False,
                     memo_dir=str(sandbox.out / "jq" / "inner"))
    assert ctp.run_project(jq) == "failed"


def test_run_refuses_memo_dir_together_with_drop_memo(
        sandbox, monkeypatch, runner_script):
    """One preserves the memo, the other deletes it. retain.prune only looks at
    <workdir>/memo, so the combination would preserve everything while reporting
    a prune."""
    runner_script()
    write_manifest(sandbox.root, VALID_ROW)
    memo_root = sandbox.root / "memos"
    memo_root.mkdir()
    monkeypatch.setattr(ctp, "capture_devenv_env", forbidden)

    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(run_args(memo_dir=str(memo_root), drop_memo=True))
    assert "contradict" in str(exc.value)


def test_run_refuses_a_memo_dir_that_does_not_exist(
        sandbox, monkeypatch, runner_script):
    """A typo would quietly start a second corpus of memos instead of reusing the
    2.6 million entries the flag exists to reuse."""
    runner_script()
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", forbidden)

    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(run_args(memo_dir=str(sandbox.root / "absent")))
    assert "not an existing directory" in str(exc.value)


def test_run_refuses_a_memo_dir_on_a_runner_that_cannot_place_it(
        sandbox, monkeypatch, runner_script):
    """Same rule as --memory-limit and --project-meta: an unpatched checkout
    hard-codes BFG_MEMO_DIR to <work>/memo and would drop the flag, so the memo
    would be deleted by the very run that was told to keep it."""
    runner_script(REAL_RUNNER_USAGE.replace("--memo-dir DIR", "--no-such-flag"))
    write_manifest(sandbox.root, VALID_ROW)
    memo_root = sandbox.root / "memos"
    memo_root.mkdir()
    monkeypatch.setattr(ctp, "capture_devenv_env", forbidden)

    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(run_args(memo_dir=str(memo_root)))
    assert "--memo-dir" in str(exc.value)


def test_run_accepts_a_memo_dir_on_a_patched_runner(
        sandbox, monkeypatch, runner_script):
    runner_script()
    write_manifest(sandbox.root, VALID_ROW)
    memo_root = sandbox.root / "memos"
    memo_root.mkdir()
    monkeypatch.setattr(ctp, "capture_devenv_env", lambda: {"PATH": "/x"})
    monkeypatch.setattr(ctp, "run_project", lambda p: "done")

    assert ctp.cmd_run(run_args(memo_dir=str(memo_root))) == 0
    assert ctp._OPTS["memo_dir"] == str(memo_root)


# --------------------------------------------------------------------------- #
# argument construction
# --------------------------------------------------------------------------- #

def test_pipeline_argv_is_built_exactly_like_this(sandbox, runner, jq):
    """Locks in the argv ctp builds.

    The flag names are the runner's: run_pipeline_process.sh takes --work and
    --mask. ctp used to send --work-dir and --file-filter, which the runner
    rejects with exit 2, so every project failed before doing any work. A defect.
    """
    ctp._OPTS.update(skip_html=False, drop_memo=False)
    ctp.run_project(jq)

    assert runner.argv("pipeline") == [
        "./run_pipeline_process.sh",
        "--repo-url", "https://github.com/jqlang/jq.git",
        "--repo-name", "jq",
        "--work", str(sandbox.out / "jq"),
        "--mask", r"\.[ch]$",
    ]
    assert runner.calls[0].kwargs["cwd"] == sandbox.cregit


def test_the_log_survives_the_runner_deleting_the_workdir(sandbox, monkeypatch, jq):
    """A defect. run_pipeline_process.sh runs `rm -rf "$WORK"` at FROM_STEP=1 and
    again from its EXIT trap on failure. $WORK is the project workdir. The log of
    the failing run used to live inside it, so the evidence died with the run.

    The fake runner deletes the workdir exactly as the real one does.
    """
    class WipingRunner(FakeRunner):
        def __call__(self, args, **kwargs):
            if self.phase_of(args) == "pipeline":
                shutil.rmtree(sandbox.out / "jq")
            return super().__call__(args, **kwargs)

    monkeypatch.setattr(ctp.subprocess, "Popen", WipingRunner(pipeline_rc=2))
    assert ctp.run_project(jq) == "failed"

    logs = sorted((ctp.state_dir("jq") / "logs").glob("pipeline-*.log"))
    assert logs, "the runner's wipe destroyed the log of its own failure"
    assert not (sandbox.out / "jq").exists()


def test_the_lock_survives_the_runner_deleting_the_workdir(sandbox, monkeypatch, jq):
    """Same wipe, the other casualty. An unlinked lock file still satisfies THIS
    process, so the guard looked healthy while a second ctp.py could create a new
    file and take its own lock on the same project.
    """
    class WipingRunner(FakeRunner):
        def __call__(self, args, **kwargs):
            if self.phase_of(args) == "pipeline":
                shutil.rmtree(sandbox.out / "jq")
            return super().__call__(args, **kwargs)

    monkeypatch.setattr(ctp.subprocess, "Popen", WipingRunner(pipeline_rc=2))
    ctp.run_project(jq)

    assert ctp.lock_path("jq").exists(), "the lock file went with the workdir"
    assert not ctp.lock_path("jq").is_relative_to(sandbox.out / "jq")


def test_status_reports_failed_after_the_runner_wiped_the_workdir(sandbox, monkeypatch, capsys, jq):
    """A failed project must not read as QUEUED. The old rule inferred FAILED from
    the workdir existing, which the runner deletes on failure. state_dir records
    the attempt instead, and the runner never touches it."""
    class WipingRunner(FakeRunner):
        def __call__(self, args, **kwargs):
            if self.phase_of(args) == "pipeline":
                shutil.rmtree(sandbox.out / "jq")
            return super().__call__(args, **kwargs)

    monkeypatch.setattr(ctp.subprocess, "Popen", WipingRunner(pipeline_rc=2))
    ctp.run_project(jq)
    monkeypatch.setattr(ctp.subprocess, "Popen", forbidden)

    write_manifest(sandbox.root, VALID_ROW)
    ctp.cmd_status(argparse.Namespace(manifest="manifest.tsv"))
    line = [l for l in capsys.readouterr().out.splitlines() if l.startswith("jq ")][0]
    assert "FAILED" in line


def test_validate_argv_is_built_exactly_like_this(sandbox, runner, jq):
    """The gate must run the repo's own validate.py on the project parquet and
    write the stamp the idempotence check looks for."""
    ctp.run_project(jq)
    assert runner.argv("validate") == [
        "python3", str(sandbox.root / "validate.py"),
        str(sandbox.out / "jq" / "jq-dataset.parquet"),
        str(sandbox.out / "jq" / "jq.validated"),
    ]


def test_pipeline_argv_uses_flags_the_runner_accepts(runner_script, runner, jq):
    """Every long flag ctp passes must be advertised by the runner.

    This is the general form of that defect. It compares the argv against the runner's own
    usage text, so it catches a rename in either direction. Keep it even though
    REQUIRED_RUNNER_FLAGS now preflights: the constant can drift from the argv,
    and this test reads the argv itself.
    """
    runner_script()
    ctp._OPTS.update(skip_html=False, drop_memo=False)
    ctp.run_project(jq)

    unsupported = [a for a in runner.argv("pipeline")
                   if a.startswith("--") and not ctp.script_supports(a)]
    assert unsupported == []


def test_required_runner_flags_are_exactly_what_run_project_sends(runner, jq):
    """The preflight constant must not drift from the argv it guards.

    cmd_run checks REQUIRED_RUNNER_FLAGS before starting. If run_project later
    gains a flag that the constant does not list, the preflight passes and the
    run fails per project instead — the same defect again, with a check that looked green.
    """
    ctp._OPTS.update(skip_html=False, drop_memo=False)
    ctp.run_project(jq)

    sent = {a for a in runner.argv("pipeline") if a.startswith("--")}
    assert sent == set(ctp.REQUIRED_RUNNER_FLAGS)


# --------------------------------------------------------------------------- #
# metrics ledger
# --------------------------------------------------------------------------- #

def test_metrics_is_append_only_with_seven_fields_per_row(sandbox):
    """Visibility contract: metrics.tsv is an append-only ledger, one row per
    phase attempt. Rewriting it destroys the benchmark history."""
    log = ctp.state_dir("jq") / "logs" / "pipeline-1.log"
    ctp.record_metric("jq", "S", "pipeline", 34, 139, log)
    first = (sandbox.root / "metrics.tsv").read_text().splitlines()[1]

    ctp.record_metric("jq", "S", "pipeline", 11, 1, log)
    lines = (sandbox.root / "metrics.tsv").read_text().splitlines()

    assert lines[0].split("\t") == ["iso_start", "project", "class", "phase",
                                    "duration_s", "rc", "log"]
    assert lines[1] == first, "the first attempt row was rewritten"
    assert len(lines) == 3
    assert all(len(line.split("\t")) == 7 for line in lines)


def test_metrics_header_is_written_once(sandbox):
    """A repeated header would break `read_csv(header=true)` in consolidate.py."""
    for _ in range(3):
        ctp.record_metric("jq", "S", "validate", 1, 0, Path("x.log"))
    text = (sandbox.root / "metrics.tsv").read_text()
    assert text.count("iso_start") == 1


def test_metrics_row_carries_the_return_code_and_log_path(sandbox, runner, clock, jq):
    """The ledger is how a failed phase is diagnosed later, so rc and the log
    path must both land in the row."""
    monkey_rc = FakeRunner(pipeline_rc=2)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(ctp.subprocess, "Popen", monkey_rc)
        ctp.run_project(jq)

    row = (sandbox.root / "metrics.tsv").read_text().splitlines()[1].split("\t")
    assert row[1:4] == ["jq", "S", "pipeline"]
    assert row[5] == "2"
    assert row[6].endswith("pipeline-20260102T030405.log")


# --------------------------------------------------------------------------- #
# log files and the -latest symlink
# --------------------------------------------------------------------------- #

def logs_of(sandbox, name="jq", phase="pipeline"):
    return sorted((ctp.state_dir(name) / "logs").glob(f"{phase}-2*.log"))


def test_a_second_attempt_does_not_overwrite_the_first_log(
        sandbox, monkeypatch, clock, jq):
    """Visibility contract: logs are never overwritten. Losing the first log
    loses the evidence of why the first attempt failed."""
    runner = FakeRunner(payload=b"first attempt\n")
    monkeypatch.setattr(ctp.subprocess, "Popen", runner)

    ctp.run_phase(jq, "pipeline", ["./run_pipeline_process.sh"])
    runner.payload = b"second attempt\n"
    clock.moment = datetime(2026, 1, 2, 3, 4, 40)
    ctp.run_phase(jq, "pipeline", ["./run_pipeline_process.sh"])

    logs = logs_of(sandbox)
    assert [p.name for p in logs] == ["pipeline-20260102T030405.log",
                                      "pipeline-20260102T030440.log"]
    assert logs[0].read_bytes() == b"first attempt\n"


@pytest.mark.xfail(strict=True, reason=(
    "Defect: the log name has one-second resolution "
    "(f'{phase}-{datetime.now():%Y%m%dT%H%M%S}.log'), so two attempts inside "
    "the same second collide and the second truncates the first. The runner "
    "exits 2 immediately on the current argv, so a retry pass hits this."))
def test_two_attempts_in_the_same_second_keep_both_logs(
        sandbox, monkeypatch, clock, jq):
    """The 'never overwritten' promise must hold even for a fast retry."""
    runner = FakeRunner(payload=b"first attempt\n")
    monkeypatch.setattr(ctp.subprocess, "Popen", runner)

    ctp.run_phase(jq, "pipeline", ["./run_pipeline_process.sh"])
    runner.payload = b"second attempt\n"
    ctp.run_phase(jq, "pipeline", ["./run_pipeline_process.sh"])

    logs = logs_of(sandbox)
    assert len(logs) == 2
    assert logs[0].read_bytes() == b"first attempt\n"


def test_latest_symlink_points_at_the_newest_attempt(
        sandbox, monkeypatch, clock, jq):
    """Visibility contract: `tail -f <phase>-latest.log` must follow the run in
    progress, not a stale attempt."""
    monkeypatch.setattr(ctp.subprocess, "Popen", FakeRunner())

    ctp.run_phase(jq, "pipeline", ["./run_pipeline_process.sh"])
    clock.moment = datetime(2026, 1, 2, 3, 4, 40)
    ctp.run_phase(jq, "pipeline", ["./run_pipeline_process.sh"])

    latest = ctp.state_dir("jq") / "logs" / "pipeline-latest.log"
    assert latest.is_symlink()
    assert Path(latest.readlink()) == logs_of(sandbox)[-1]


def test_latest_symlink_is_created_on_the_first_attempt(
        sandbox, monkeypatch, clock, jq):
    """No prior symlink exists on a fresh project; the swap must still work."""
    monkeypatch.setattr(ctp.subprocess, "Popen", FakeRunner())
    # run_project creates the workdir; run_phase no longer does it by accident,
    # because the log directory moved out of the workdir. See STATE in ctp.py.
    (sandbox.out / "jq").mkdir()
    stamp = sandbox.out / "jq" / "jq.validated"
    ctp.run_phase(jq, "validate",
                  ["python3", "validate.py", "p.parquet", str(stamp)])
    latest = ctp.state_dir("jq") / "logs" / "validate-latest.log"
    assert latest.is_symlink()
    assert not (ctp.state_dir("jq") / "logs" / ".validate-latest.tmp").exists()


def test_run_phase_records_the_live_phase_for_the_heartbeat(
        sandbox, monkeypatch, jq):
    """The heartbeat reads _live; an empty _live makes a long run look idle."""
    monkeypatch.setattr(ctp.subprocess, "Popen", FakeRunner())
    ctp.run_phase(jq, "pipeline", ["./run_pipeline_process.sh"])
    assert ctp._live["jq"][0] == "pipeline"


# --------------------------------------------------------------------------- #
# return states
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("pipeline_rc, validate_rc, expected", [
    pytest.param(0, 0, "done", id="both-ok"),
    pytest.param(2, 0, "failed", id="pipeline-exit-2"),
    pytest.param(139, 0, "failed", id="pipeline-signal"),
    pytest.param(0, 1, "failed", id="validate-gate-rejects"),
])
def test_run_project_return_state(monkeypatch, jq, pipeline_rc, validate_rc, expected):
    """run_project returns exactly one of done|skipped|failed|deferred; cmd_run
    computes the run's exit code from these strings."""
    monkeypatch.setattr(ctp.subprocess, "Popen",
                        FakeRunner(pipeline_rc=pipeline_rc, validate_rc=validate_rc))
    assert ctp.run_project(jq) == expected


def test_run_project_defers_below_the_disk_floor(monkeypatch, runner, jq):
    """Deferring beats filling the disk: a truncated workdir would validate as
    FAILED and waste the tokenizer time already spent."""
    monkeypatch.setattr(ctp, "DISK_FLOOR_GB", 10 ** 9)
    assert ctp.run_project(jq) == "deferred"
    assert runner.calls == []


def test_a_done_project_writes_the_stamp_that_makes_the_next_run_skip(sandbox, runner, jq):
    """Closes the idempotence loop: state after 'done' must be the state that
    yields 'skipped'."""
    assert ctp.run_project(jq) == "done"
    assert (sandbox.out / "jq" / "jq.validated").exists()
    assert ctp.run_project(jq) == "skipped"


@pytest.mark.xfail(strict=True, reason=(
    "Defect: an exception from subprocess (for example the runner script being "
    "absent, OSError) is not caught. It escapes run_project, propagates through "
    "ThreadPoolExecutor.map in cmd_run and aborts the whole corpus run instead "
    "of marking one project failed."))
def test_run_project_reports_failed_when_the_subprocess_raises(monkeypatch, jq):
    """One unusable project must not end the run."""
    monkeypatch.setattr(ctp.subprocess, "Popen", FakeRunner(raise_on="pipeline"))
    assert ctp.run_project(jq) == "failed"


# --------------------------------------------------------------------------- #
# devenv capture
# --------------------------------------------------------------------------- #

def test_capture_devenv_env_returns_the_parsed_environment(monkeypatch):
    """Captured once and shared by every worker: concurrent `devenv shell`
    invocations race on the GC root in CREGIT."""
    def fake_run(args, **kwargs):
        assert args[0] == "bash"
        assert kwargs["capture_output"] and kwargs["text"]
        return SimpleNamespace(returncode=0, stdout='noise\n{"PATH": "/nix/bin"}\n',
                               stderr="")
    monkeypatch.setattr(ctp.subprocess, "run", fake_run)
    assert ctp.capture_devenv_env() == {"PATH": "/nix/bin"}


def test_capture_devenv_env_exits_when_devenv_fails(monkeypatch):
    """Without the environment nothing can run, so fail loudly and early."""
    monkeypatch.setattr(ctp.subprocess, "run", lambda *a, **k: SimpleNamespace(
        returncode=1, stdout="", stderr="error: not an absolute path: \"nix\""))
    with pytest.raises(SystemExit) as exc:
        ctp.capture_devenv_env()
    assert "devenv environment capture failed" in str(exc.value)


# --------------------------------------------------------------------------- #
# cmd_run
# --------------------------------------------------------------------------- #

def run_args(**over):
    """A complete `ctp run` Namespace. Every cmd_run option belongs here, so
    adding one is a single edit rather than one per test.

    allow_empty_provenance defaults to True here and ONLY here. On the real CLI it
    defaults to False, and cmd_run then refuses any step-10 run that leaves
    --project-meta, --firm-map or --firm-canonical out. Every test in this file
    except the provenance-guard section leaves all three empty because it is
    testing something else entirely, so without the hatch each one would exit on a
    refusal it never asked about. The guard's own default — False — is exercised
    explicitly by the tests below, which pass allow_empty_provenance=False.
    """
    base = dict(manifest="manifest.tsv", only=None, jobs=1, retries=0,
                skip_html=False, drop_memo=False, memo_dir="",
                shards=0, shard_classes="L",
                from_step=1, gc=None, blame_jobs=0,
                memory_limit=None, duckdb_threads=0, project_meta="",
                firm_map="", firm_canonical="", allow_empty_provenance=True,
                mask="", mask_widened=False, retokenize="", reblame=False)
    base.update(over)
    return argparse.Namespace(**base)


def test_cmd_run_with_an_empty_manifest_does_nothing(sandbox, monkeypatch, capsys):
    """No manifest rows means no devenv capture and no runs.log row."""
    write_manifest(sandbox.root)
    monkeypatch.setattr(ctp, "capture_devenv_env", forbidden)
    assert ctp.cmd_run(run_args()) == 0
    assert "nothing to run" in capsys.readouterr().out
    assert not (sandbox.root / "runs.log").exists()


def test_cmd_run_appends_a_start_and_an_end_row_to_runs_log(sandbox, monkeypatch):
    """Visibility contract: one start/end row per invocation, appended."""
    write_manifest(sandbox.root, VALID_ROW)
    (sandbox.root / "runs.log").write_text("2026-08-28T03:22:20+00:00\trun-start\tjobs=2\n")
    monkeypatch.setattr(ctp, "capture_devenv_env", lambda: {"PATH": "/x"})
    monkeypatch.setattr(ctp, "run_project", lambda p: "done")

    assert ctp.cmd_run(run_args(jobs=2)) == 0
    lines = (sandbox.root / "runs.log").read_text().splitlines()
    assert len(lines) == 3
    assert lines[0].endswith("jobs=2")
    assert "run-start\tjobs=2\tprojects=1" in lines[1]
    assert "run-end\trc=0" in lines[2]


def test_cmd_run_returns_one_when_a_project_fails(sandbox, monkeypatch):
    """The exit code is what a cron wrapper or CI checks."""
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", lambda: {"PATH": "/x"})
    monkeypatch.setattr(ctp, "run_project", lambda p: "failed")
    assert ctp.cmd_run(run_args()) == 1
    assert "run-end\trc=1" in (sandbox.root / "runs.log").read_text()


def test_cmd_run_retries_only_the_projects_that_are_not_done(
        sandbox, monkeypatch, capsys):
    """A retry pass must skip the finished projects, or a long corpus run
    repeats work it already paid for."""
    write_manifest(sandbox.root, VALID_ROW,
                   "zstd\thttps://github.com/facebook/zstd.git\tenterprise\t\\.[ch]$\tS")
    monkeypatch.setattr(ctp, "capture_devenv_env", lambda: {"PATH": "/x"})

    seen: list[str] = []

    def flaky(project):
        seen.append(project["name"])
        if project["name"] == "zstd" and seen.count("zstd") == 1:
            return "failed"
        return "done"

    monkeypatch.setattr(ctp, "run_project", flaky)
    assert ctp.cmd_run(run_args(retries=1)) == 0
    assert seen == ["jq", "zstd", "zstd"]
    assert "retry pass 1: ['zstd']" in capsys.readouterr().out


def test_cmd_run_stops_retrying_once_every_project_is_done(sandbox, monkeypatch):
    """The retry loop must break, not burn the remaining passes."""
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", lambda: {"PATH": "/x"})
    calls = []
    monkeypatch.setattr(ctp, "run_project", lambda p: calls.append(p["name"]) or "done")
    assert ctp.cmd_run(run_args(retries=3)) == 0
    assert calls == ["jq"]


# --------------------------------------------------------------------------- #
# heartbeat
# --------------------------------------------------------------------------- #

def test_heartbeat_reports_running_projects_done_failed_and_disk():
    """Live progress contract. Without it a 35-minute phase looks like a hang."""
    ctp._live["jq"] = ("pipeline", time.time())
    stop = threading.Event()
    lines: list[str] = []

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(ctp, "HEARTBEAT_S", 0)
        mp.setattr(ctp, "say", lambda msg: (lines.append(msg), stop.set()))
        ctp.heartbeat(stop, {"jq": "done", "zstd": "failed", "tmux": "skipped"})

    assert len(lines) == 1
    assert "jq(pipeline" in lines[0]
    assert "done 2 failed 1" in lines[0]
    assert "G free" in lines[0]


def test_heartbeat_shows_a_dash_when_nothing_is_running():
    """An empty running list must still print a line."""
    stop = threading.Event()
    lines: list[str] = []
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(ctp, "HEARTBEAT_S", 0)
        mp.setattr(ctp, "say", lambda msg: (lines.append(msg), stop.set()))
        ctp.heartbeat(stop, {})
    assert "running: —" in lines[0]


def test_heartbeat_returns_immediately_when_already_stopped():
    """cmd_run sets the event in a finally block; the thread must exit."""
    stop = threading.Event()
    stop.set()
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(ctp, "say", forbidden)
        ctp.heartbeat(stop, {})


# --------------------------------------------------------------------------- #
# cmd_status and _lock_held
# --------------------------------------------------------------------------- #

def test_cmd_status_classifies_every_project_state(sandbox, capsys):
    """One screen, four states. A wrong state sends an operator to re-run a
    project that is already running."""
    write_manifest(
        sandbox.root,
        "done\thttps://d.git\tcommunity\t\\.[ch]$\tS",
        "running\thttps://r.git\tcommunity\t\\.[ch]$\tM",
        "failed\thttps://f.git\tenterprise\t\\.[ch]$\tL",
        "queued\thttps://q.git\tcommunity\t\\.[ch]$\tS")
    (sandbox.out / "done").mkdir()
    (sandbox.out / "done" / "done.validated").write_text("rows=1\nbytes=2\n")
    (sandbox.out / "failed").mkdir()
    (sandbox.out / "running").mkdir()
    ctp.record_metric("done", "S", "validate", 3, 0, Path("/logs/validate-1.log"))

    with held_lock(ctp.lock_path("running")):
        assert ctp.cmd_status(argparse.Namespace(manifest="manifest.tsv")) == 0

    out = capsys.readouterr().out
    states = {line.split()[0]: line.split()[2] for line in out.splitlines()[1:5]}
    assert states == dict(done="DONE", running="RUNNING",
                          failed="FAILED", queued="QUEUED")
    assert "progress: 1/4 validated" in out
    assert "validate(3s,rc=0)" in out


def test_cmd_status_works_without_a_metrics_file(sandbox, capsys):
    """A fresh checkout has no ledger yet; status must not crash."""
    write_manifest(sandbox.root, VALID_ROW)
    assert ctp.cmd_status(argparse.Namespace(manifest="manifest.tsv")) == 0
    assert "—" in capsys.readouterr().out


def test_lock_held_is_false_when_the_lockfile_is_absent(sandbox):
    """A project that never started has no lock and must read QUEUED."""
    assert ctp._lock_held(ctp.lock_path("nope")) is False


def test_lock_held_distinguishes_a_free_lock_from_a_held_one(sandbox):
    """A stale lockfile left by a killed run must not read as RUNNING."""
    lockfile = ctp.lock_path("jq")
    lockfile.parent.mkdir(parents=True)
    lockfile.touch()
    assert ctp._lock_held(lockfile) is False
    with held_lock(lockfile):
        assert ctp._lock_held(lockfile) is True


# --------------------------------------------------------------------------- #
# cmd_db and main
# --------------------------------------------------------------------------- #

def test_cmd_db_runs_consolidate_inside_the_captured_environment(sandbox, monkeypatch):
    """consolidate.py needs duckdb, which only the devenv environment provides."""
    monkeypatch.setattr(ctp, "capture_devenv_env", lambda: {"PATH": "/nix/bin"})
    seen = {}

    def fake_run(args, **kwargs):
        seen.update(args=list(args), **kwargs)
        return SimpleNamespace(returncode=7)

    monkeypatch.setattr(ctp.subprocess, "run", fake_run)
    assert ctp.cmd_db(argparse.Namespace()) == 7
    assert seen["args"] == ["python3", str(sandbox.root / "consolidate.py")]
    assert seen["cwd"] == sandbox.cregit
    assert seen["env"] == {"PATH": "/nix/bin"}


@pytest.mark.parametrize("argv, target", [
    pytest.param(["ctp.py", "run"], "cmd_run", id="run"),
    pytest.param(["ctp.py", "status"], "cmd_status", id="status"),
    pytest.param(["ctp.py", "db"], "cmd_db", id="db"),
])
def test_main_dispatches_each_subcommand(monkeypatch, argv, target):
    """A broken dispatch table makes the CLI silently do the wrong thing."""
    called = []
    monkeypatch.setattr(ctp.sys, "argv", argv)
    monkeypatch.setattr(ctp, target, lambda args: called.append(target) or 0)
    assert ctp.main() == 0
    assert called == [target]


def test_main_requires_a_subcommand(monkeypatch):
    """`./ctp.py` with no verb must not fall through to a default action."""
    monkeypatch.setattr(ctp.sys, "argv", ["ctp.py"])
    with pytest.raises(SystemExit):
        ctp.main()


def test_run_defaults_are_two_jobs_and_one_retry(monkeypatch):
    """These defaults are the documented disk/throughput compromise."""
    seen = {}
    monkeypatch.setattr(ctp.sys, "argv", ["ctp.py", "run"])
    monkeypatch.setattr(ctp, "cmd_run", lambda args: seen.update(vars(args)) or 0)
    ctp.main()
    assert seen["jobs"] == 2
    assert seen["retries"] == 1
    assert seen["manifest"] == "manifest.tsv"
    assert seen["skip_html"] is False
    assert seen["drop_memo"] is False


def test_say_prefixes_every_line_with_a_timestamp(capsys, clock):
    """Log lines are read next to metrics.tsv rows, so they need a clock."""
    ctp.say("jq ▶ pipeline started")
    assert capsys.readouterr().out == "[03:04:05] jq ▶ pipeline started\n"


def test_now_iso_is_utc_with_second_resolution(clock):
    """metrics.tsv rows are compared across hosts, so the stamp must be UTC."""
    assert ctp.now_iso() == "2026-01-02T03:04:05+00:00"


# --------------------------------------------------------------------------- #
# resource sampling
# --------------------------------------------------------------------------- #

def test_tree_usage_sums_descendants_not_just_the_direct_child(sandbox):
    """The pipeline is a shell that spawns perl, git and srcml. Reporting only the
    shell's own RSS would understate every run, which is the number the cost
    model depends on."""
    table = {10: (1, 1024), 11: (10, 2048), 12: (11, 4096), 99: (1, 8192)}
    rss_mb, procs = ctp.tree_usage(10, table)
    assert procs == 3, "grandchild 12 was not walked"
    assert rss_mb == (1024 + 2048 + 4096) // 1024
    assert 99 not in (10, 11, 12), "unrelated process must not be counted"


def test_tree_usage_returns_zero_when_the_process_already_exited(sandbox):
    """A phase can finish between Popen and the first sample. That must read as
    zero, not raise, because the sampler runs inside every phase."""
    assert ctp.tree_usage(424242, {1: (0, 4096)}) == (0, 0)


def test_tree_usage_survives_a_cycle_in_the_parent_map(sandbox):
    """A malformed parent map must not hang the sampler. /proc is read
    process-by-process, so an inconsistent snapshot is possible."""
    table = {20: (21, 1024), 21: (20, 1024)}
    rss_mb, procs = ctp.tree_usage(20, table)
    assert procs == 2 and rss_mb == 2


def test_stop_before_start_returns_an_empty_peak(sandbox, jq):
    """run_phase stops the sampler in a finally block, so stop() can be reached
    on a path where start() never ran."""
    peak = ctp.ResourceSampler(jq, "pipeline").stop()
    assert peak["samples"] == 0
    assert peak["disk_free_gb_min"] is None


def test_a_failing_sample_never_breaks_the_phase(sandbox, jq, capsys):
    """Measurement must not fail the thing it measures. The loop reports the
    failure and keeps sampling."""
    sampler = ctp.ResourceSampler(jq, "pipeline", interval=0.01)
    calls = {"n": 0}
    done = threading.Event()

    def flaky(prev_busy, prev_total):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("/proc vanished")
        if calls["n"] >= 3:
            done.set()
        return prev_busy, prev_total

    sampler._sample = flaky
    sampler.start(os.getpid())
    assert done.wait(timeout=10), "the loop stopped after the failing sample"
    sampler.stop()
    assert "resource sample failed" in capsys.readouterr().out


def test_the_ledger_header_matches_the_declared_fields(sandbox, jq):
    """A consumer splits on these columns, so the header and the row must agree."""
    sampler = ctp.ResourceSampler(jq, "pipeline", interval=0.01)
    sampler.pid = os.getpid()
    sampler._sample(0, 0)

    lines = (sandbox.root / "resources.tsv").read_text().splitlines()
    assert lines[0].split("\t") == list(ctp.RESOURCE_FIELDS)
    assert len(lines[1].split("\t")) == len(ctp.RESOURCE_FIELDS)
    assert lines[1].split("\t")[1:4] == ["jq", "S", "pipeline"]


def test_peaks_are_maxima_and_disk_is_a_low_water_mark(sandbox, jq, monkeypatch):
    """Capacity planning needs the worst moment, not the last one."""
    sampler = ctp.ResourceSampler(jq, "pipeline", interval=0.01)
    sampler.pid = os.getpid()

    sizes = iter([(500, 3), (200, 1)])          # second sample is smaller
    monkeypatch.setattr(ctp, "tree_usage", lambda *a, **k: next(sizes))
    frees = iter([900 * 2**30, 100 * 2**30])    # disk shrinks
    monkeypatch.setattr(ctp.shutil, "disk_usage",
                        lambda p: SimpleNamespace(free=next(frees)))

    sampler._sample(0, 0)
    sampler._sample(0, 0)
    peak = sampler.peak
    assert peak["tree_rss_mb"] == 500, "peak fell back to the later sample"
    assert peak["tree_procs"] == 3
    assert peak["disk_free_gb_min"] == 100, "low-water mark not kept"
    assert peak["samples"] == 2


def test_metrics_row_width_is_unchanged_by_resource_sampling(sandbox, runner, jq):
    """resources.tsv is a separate ledger on purpose. metrics.tsv's seven-field
    row is a published contract and widening it would break every consumer."""
    ctp.run_project(jq)
    for line in (sandbox.root / "metrics.tsv").read_text().splitlines():
        assert len(line.split("\t")) == 7


def test_cpu_percent_is_zero_when_no_jiffies_elapsed(sandbox, jq, monkeypatch):
    """Two samples inside one clock tick must not divide by zero."""
    monkeypatch.setattr(ctp, "_cpu_jiffies", lambda: (100, 200))
    sampler = ctp.ResourceSampler(jq, "pipeline", interval=0.01)
    sampler.pid = os.getpid()
    sampler._sample(100, 200)
    assert sampler.peak["cpu_pct"] == 0.0


def test_proc_scan_skips_a_process_that_vanished_mid_scan(sandbox, tmp_path):
    """cregit starts and reaps short-lived perl and git processes constantly, so a
    /proc entry can disappear between the listdir and the read. That must be
    skipped, not raise inside the sampler thread."""
    fake_proc = tmp_path / "proc"
    fake_proc.mkdir()
    (fake_proc / "self").mkdir()                     # non-numeric, ignored
    (fake_proc / "7").mkdir()                        # numeric, but no stat file
    good = fake_proc / "9"
    good.mkdir()
    # Real /proc/<pid>/stat, built by field position so the offsets are visible.
    # A comm of "(git log --oneline)" holds spaces and parentheses, which is why
    # the parser splits after the LAST ')' instead of on whitespace.
    stat = ["0"] * 52
    stat[0] = "9"                       # field 1  pid
    stat[1] = "(git log --oneline)"     # field 2  comm, spaces and parens
    stat[2] = "S"                       # field 3  state
    stat[3] = "1234"                    # field 4  ppid
    stat[23] = "777"                    # field 24 rss, in pages
    (good / "stat").write_text(" ".join(stat) + "\n")

    table = ctp._proc_ppid_rss(fake_proc)
    assert 7 not in table, "an unreadable entry was not skipped"
    assert list(table) == [9]
    assert table[9] == (1234, 777 * 4), "ppid or rss read from the wrong offset"


def test_proc_scan_agrees_with_real_proc_for_our_own_process(sandbox):
    """The synthetic layout above only proves the offsets are self-consistent.
    This proves they match the kernel's real format."""
    table = ctp._proc_ppid_rss()
    assert os.getpid() in table
    ppid, rss_kb = table[os.getpid()]
    assert ppid == os.getppid(), "parsed ppid disagrees with os.getppid()"
    assert rss_kb > 0, "a running interpreter cannot have zero resident memory"


# --------------------------------------------------------------------------- #
# sharding
# --------------------------------------------------------------------------- #

def test_sharding_is_off_unless_asked_for(runner, jq):
    """Sharding costs transient disk, so it must never be the default. Three
    concurrent S-class projects already fill this box."""
    ctp._OPTS.update(skip_html=False, drop_memo=False, shards=0, shard_classes=("L",))
    ctp.run_project(jq)
    argv = runner.argv("pipeline")
    assert "--mode" not in argv and "--shards" not in argv


def test_one_shard_is_not_sharding(runner, jq):
    """--shards 1 would pay the sharded-mode overhead for no parallelism, so it
    is treated as off rather than honoured literally."""
    ctp._OPTS.update(skip_html=False, drop_memo=False, shards=1, shard_classes=("S",))
    ctp.run_project(jq)
    assert "--mode" not in runner.argv("pipeline")


def test_an_l_class_project_is_sharded(runner):
    """The measured case. On Linux --mode pipeline left ~14 of 16 cores idle,
    because the per-blob chain spawns three processes and the pipelined walk
    never keeps 16 of them in flight."""
    big = dict(name="linux", url="https://example.invalid/linux.git",
               category="community", file_filter=r"\.[ch]$", size_class="L")
    ctp._OPTS.update(skip_html=False, drop_memo=False, shards=6, shard_classes=("L",))
    ctp.run_project(big)
    argv = runner.argv("pipeline")
    assert argv[argv.index("--mode") + 1] == "sharded"
    assert argv[argv.index("--shards") + 1] == "6"


def test_an_s_class_project_is_not_sharded_by_default(runner, jq):
    """--shard-classes defaults to L. An S project gains nothing and would only
    multiply the disk."""
    ctp._OPTS.update(skip_html=False, drop_memo=False, shards=6, shard_classes=("L",))
    ctp.run_project(jq)
    assert "--mode" not in runner.argv("pipeline")


def test_shard_classes_is_configurable(runner, jq):
    """M class may be worth sharding once measured, so the classes are a list
    rather than a hard-coded L."""
    ctp._OPTS.update(skip_html=False, drop_memo=False, shards=4,
                     shard_classes=("S", "M"))
    ctp.run_project(jq)
    assert runner.argv("pipeline").count("--mode") == 1


def test_run_refuses_to_shard_when_the_runner_cannot(
        sandbox, monkeypatch, runner_script):
    """Same rule as --skip-html: refuse once, before the devenv capture, rather
    than discover per project that the checkout has no sharded mode."""
    runner_script(REAL_RUNNER_USAGE.replace("--shards N", "--nope N"))
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", forbidden)

    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(run_args(shards=6))
    assert "--shards needs" in str(exc.value)
    assert not (sandbox.root / "runs.log").exists()


def test_run_announces_the_shard_plan(sandbox, monkeypatch, capsys):
    """A run that silently changed tokenizer mode would be hard to explain later
    from the logs alone."""
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", lambda: {"PATH": "/x"})
    monkeypatch.setattr(ctp, "run_project", lambda p: "done")

    assert ctp.cmd_run(run_args(shards=6, shard_classes="L,M")) == 0
    out = capsys.readouterr().out
    assert "sharding 6-way" in out
    assert "L, M" in out


def test_shard_classes_are_parsed_into_a_tuple(sandbox, monkeypatch):
    """Whitespace and a trailing comma are normal in a hand-typed flag."""
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", lambda: {"PATH": "/x"})
    monkeypatch.setattr(ctp, "run_project", lambda p: "done")

    ctp.cmd_run(run_args(shards=2, shard_classes=" L , M ,"))
    assert ctp._OPTS["shard_classes"] == ("L", "M")


def test_shard_class_helper_needs_both_a_count_and_a_matching_class():
    """The two conditions are independent, so both are checked here rather than
    inferred from run_project's argv."""
    L = dict(size_class="L")
    ctp._OPTS.clear()
    ctp._OPTS.update(shards=6, shard_classes=("L",))
    assert ctp.shard_class(L) is True
    ctp._OPTS.update(shards=1)
    assert ctp.shard_class(L) is False
    ctp._OPTS.update(shards=6, shard_classes=("M",))
    assert ctp.shard_class(L) is False
    ctp._OPTS.clear()
    assert ctp.shard_class(L) is False, "an unset _OPTS must not shard"


def test_blame_jobs_is_sent_to_the_runner_as_jobs(runner, jq):
    """Blame is the bottleneck. The worker count only helps if it reaches the
    runner argv — and it must use the runner's name for the flag.

    cregit-issue61 7a70a92 renamed the runner's --blame-jobs to --jobs on
    2026-09-18. ctp.py kept sending the old name, so its preflight refused every
    project and the corpus run could not be restarted. ctp.py's own CLI name is
    still --blame-jobs, because ctp.py's --jobs means concurrent projects.
    """
    ctp._OPTS.update(skip_html=False, drop_memo=False, blame_jobs=8)
    ctp.run_project(jq)
    argv = runner.argv("pipeline")
    assert argv[argv.index("--jobs") + 1] == "8"
    assert "--blame-jobs" not in argv


def test_no_blame_jobs_leaves_the_runner_default_alone(runner, jq):
    """Zero means "not asked for", so the runner keeps its own default."""
    ctp._OPTS.update(skip_html=False, drop_memo=False, blame_jobs=0)
    ctp.run_project(jq)
    assert "--jobs" not in runner.argv("pipeline")


def test_run_refuses_blame_jobs_on_a_runner_that_blames_serially(
        sandbox, monkeypatch, runner_script):
    """Accepting the flag against an unpatched checkout would promise a speedup
    the runner cannot deliver, and the difference is days per project."""
    runner_script(REAL_RUNNER_USAGE.replace("--jobs N", "--no-such-flag"))
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", forbidden)

    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(run_args(blame_jobs=8))
    assert "--blame-jobs needs --jobs" in str(exc.value)


def test_run_rejects_a_negative_blame_jobs(sandbox, monkeypatch, runner_script):
    """A negative count is a typo, and the runner would reject it hours later."""
    runner_script()
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", forbidden)

    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(run_args(blame_jobs=-1))
    assert "--blame-jobs cannot be negative" in str(exc.value)


def test_run_accepts_blame_jobs_on_a_patched_runner(
        sandbox, monkeypatch, runner_script):
    """The refusal must not fire on the patched runner."""
    runner_script()
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", lambda: {"PATH": "/x"})
    monkeypatch.setattr(ctp, "run_project", lambda p: "done")

    assert ctp.cmd_run(run_args(blame_jobs=12)) == 0
    assert ctp._OPTS["blame_jobs"] == 12


# --------------------------------------------------------------------------- #
# Step 10 memory budget. Added after the 2026-09-15 corpus run was killed for
# low memory: the generator's own 8GB default, times two concurrent projects,
# needed ~22 GB on a 30 GB box that already gave ~17 GB to other software.
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("text,want", [
    ("1B", 1),
    ("2K", 2048),
    ("2KB", 2048),
    ("2KiB", 2048),
    ("3MB", 3 * 1024 ** 2),
    ("3M", 3 * 1024 ** 2),
    ("3MiB", 3 * 1024 ** 2),
    ("8GB", 8 * 1024 ** 3),
    ("8G", 8 * 1024 ** 3),
    ("8GiB", 8 * 1024 ** 3),
    ("1T", 1024 ** 4),
    ("1TB", 1024 ** 4),
    ("1TiB", 1024 ** 4),
    ("1.5 GiB", int(1.5 * 1024 ** 3)),
    ("  3GB  ", 3 * 1024 ** 3),
    ("3gb", 3 * 1024 ** 3),
])
def test_size_to_bytes_reads_every_unit_duckdb_accepts(text, want):
    """The budget arithmetic is only as good as the parse, and DuckDB accepts
    all of these spellings."""
    assert ctp.size_to_bytes(text) == want


def test_size_to_bytes_refuses_a_percentage():
    """A percentage measures TOTAL RAM. Only the free part is usable, and on
    this box 17 of 30 GB belongs to other software."""
    with pytest.raises(ValueError) as exc:
        ctp.size_to_bytes("80%")
    assert "not a percentage" in str(exc.value)


@pytest.mark.parametrize("bad", ["3gigs", "", "GB", "3 4GB", "-3GB", "3PB"])
def test_size_to_bytes_refuses_anything_it_cannot_read(bad):
    """Guessing at a typo would cap the heap at the wrong number silently."""
    with pytest.raises(ValueError) as exc:
        ctp.size_to_bytes(bad)
    assert "cannot read" in str(exc.value)


def test_available_bytes_reads_mem_available(tmp_path, monkeypatch):
    """MemAvailable, not MemFree: the reclaimable page cache is usable."""
    meminfo = tmp_path / "meminfo"
    meminfo.write_text("MemTotal:       31000000 kB\n"
                       "MemFree:          900000 kB\n"
                       "MemAvailable:    5500000 kB\n")
    monkeypatch.setattr(ctp, "Path", lambda _p: meminfo)
    assert ctp.available_bytes() == 5500000 * 1024


def test_available_bytes_returns_none_when_the_key_is_absent(tmp_path, monkeypatch):
    """An unexpected /proc format must not raise. The warning is advisory."""
    meminfo = tmp_path / "meminfo"
    meminfo.write_text("MemTotal:       31000000 kB\n")
    monkeypatch.setattr(ctp, "Path", lambda _p: meminfo)
    assert ctp.available_bytes() is None


def test_available_bytes_returns_none_when_proc_cannot_be_read(monkeypatch):
    """A non-Linux host has no /proc/meminfo. Warn nothing rather than crash."""
    def boom(_p):
        raise OSError("no /proc here")
    monkeypatch.setattr(ctp, "Path", boom)
    assert ctp.available_bytes() is None


def test_available_bytes_returns_none_on_an_unparsable_value(tmp_path, monkeypatch):
    """A malformed number must not raise either."""
    meminfo = tmp_path / "meminfo"
    meminfo.write_text("MemAvailable:    not-a-number kB\n")
    monkeypatch.setattr(ctp, "Path", lambda _p: meminfo)
    assert ctp.available_bytes() is None


def test_available_bytes_returns_none_on_a_truncated_line(tmp_path, monkeypatch):
    """A line with no value field must not raise."""
    meminfo = tmp_path / "meminfo"
    meminfo.write_text("MemAvailable:\n")
    monkeypatch.setattr(ctp, "Path", lambda _p: meminfo)
    assert ctp.available_bytes() is None


def test_memory_budget_warning_names_the_shortfall(monkeypatch):
    """This is the exact case that killed the run: 2 x 8GB against 6 GB free."""
    monkeypatch.setattr(ctp, "available_bytes", lambda: 6 * 1024 ** 3)
    warning = ctp.memory_budget_warning("8GB", 2)
    assert warning is not None
    assert "2 concurrent x 8GB" in warning
    assert "22.4 GiB" in warning, "1.4 x 2 x 8GB, the measured settle ratio"
    assert "6.0 GiB" in warning


def test_memory_budget_warning_is_silent_when_the_budget_fits(monkeypatch):
    """1.4 x 1 x 3GB is 4.2 GiB, which fits in 8 GiB."""
    monkeypatch.setattr(ctp, "available_bytes", lambda: 8 * 1024 ** 3)
    assert ctp.memory_budget_warning("3GB", 1) is None


def test_memory_budget_warning_is_silent_when_ram_is_unknown(monkeypatch):
    """No reading means no claim. Refusing to run would be worse."""
    monkeypatch.setattr(ctp, "available_bytes", lambda: None)
    assert ctp.memory_budget_warning("8GB", 4) is None


def test_memory_budget_counts_every_concurrent_job(monkeypatch):
    """The limit is per generator, not per run, so --jobs multiplies it."""
    monkeypatch.setattr(ctp, "available_bytes", lambda: 10 * 1024 ** 3)
    assert ctp.memory_budget_warning("3GB", 2) is None, "1.4 x 2 x 3GB = 8.4 GiB"
    assert ctp.memory_budget_warning("3GB", 3) is not None, "1.4 x 3 x 3GB = 12.6 GiB"


def test_memory_limit_is_forwarded_to_the_runner(runner, jq):
    """Step 10 is the only step that can exhaust RAM. The cap only helps if it
    reaches the runner argv."""
    ctp._OPTS.update(skip_html=False, drop_memo=False, memory_limit="3GB")
    ctp.run_project(jq)
    argv = runner.argv("pipeline")
    assert argv[argv.index("--memory-limit") + 1] == "3GB"


def test_duckdb_threads_is_forwarded_to_the_runner(runner, jq):
    """Each sorting thread holds its own buffers, so the count bounds the peak."""
    ctp._OPTS.update(skip_html=False, drop_memo=False, duckdb_threads=2)
    ctp.run_project(jq)
    argv = runner.argv("pipeline")
    assert argv[argv.index("--duckdb-threads") + 1] == "2"


def test_no_memory_flags_leave_the_generator_default_alone(runner, jq):
    """Unset means "not asked for", so generate_dataset.py keeps its own 8GB."""
    ctp._OPTS.update(skip_html=False, drop_memo=False,
                     memory_limit=None, duckdb_threads=0)
    ctp.run_project(jq)
    argv = runner.argv("pipeline")
    assert "--memory-limit" not in argv
    assert "--duckdb-threads" not in argv


def test_run_refuses_memory_limit_on_a_runner_that_ignores_it(
        sandbox, monkeypatch, runner_script):
    """Accepting the flag against an unpatched checkout would drop it silently,
    and step 10 would run at 8GB anyway — the failure this guard exists for."""
    runner_script(REAL_RUNNER_USAGE.replace("--memory-limit SIZE", "--no-such-flag"))
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", forbidden)

    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(run_args(memory_limit="3GB"))
    assert "--memory-limit is not implemented" in str(exc.value)


def test_run_refuses_duckdb_threads_on_a_runner_that_ignores_it(
        sandbox, monkeypatch, runner_script):
    """Same rule as --memory-limit."""
    runner_script(REAL_RUNNER_USAGE.replace("--duckdb-threads N", "--no-such-flag"))
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", forbidden)

    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(run_args(duckdb_threads=2))
    assert "--duckdb-threads is not implemented" in str(exc.value)


def test_run_rejects_a_negative_duckdb_threads(sandbox, monkeypatch, runner_script):
    """A negative count is a typo, and step 10 is the last step."""
    runner_script()
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", forbidden)

    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(run_args(duckdb_threads=-1))
    assert "--duckdb-threads cannot be negative" in str(exc.value)


def test_run_rejects_a_bad_memory_limit_before_the_run(
        sandbox, monkeypatch, runner_script):
    """Step 10 is hours in. A typo must surface now, not after tokenizing."""
    runner_script()
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", forbidden)

    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(run_args(memory_limit="80%"))
    assert "--memory-limit" in str(exc.value)
    assert "not a percentage" in str(exc.value)


def test_run_warns_when_the_memory_budget_does_not_fit(
        sandbox, monkeypatch, runner_script, capsys):
    """The warning is the guard whose absence killed the 2026-09-15 run."""
    runner_script()
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", lambda: {"PATH": "/x"})
    monkeypatch.setattr(ctp, "run_project", lambda p: "done")
    monkeypatch.setattr(ctp, "available_bytes", lambda: 6 * 1024 ** 3)

    assert ctp.cmd_run(run_args(memory_limit="8GB", jobs=2)) == 0
    assert "WARNING: memory budget" in capsys.readouterr().out


def test_run_does_not_warn_when_the_memory_budget_fits(
        sandbox, monkeypatch, runner_script, capsys):
    """A warning on a safe setting would train the operator to ignore it."""
    runner_script()
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", lambda: {"PATH": "/x"})
    monkeypatch.setattr(ctp, "run_project", lambda p: "done")
    monkeypatch.setattr(ctp, "available_bytes", lambda: 32 * 1024 ** 3)

    assert ctp.cmd_run(run_args(memory_limit="3GB", jobs=2)) == 0
    assert "memory budget" not in capsys.readouterr().out


def test_run_accepts_the_memory_flags_on_a_patched_runner(
        sandbox, monkeypatch, runner_script):
    """The refusals must not fire on the patched runner."""
    runner_script()
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", lambda: {"PATH": "/x"})
    monkeypatch.setattr(ctp, "run_project", lambda p: "done")
    monkeypatch.setattr(ctp, "available_bytes", lambda: 32 * 1024 ** 3)

    assert ctp.cmd_run(run_args(memory_limit="3GB", duckdb_threads=2)) == 0
    assert ctp._OPTS["memory_limit"] == "3GB"
    assert ctp._OPTS["duckdb_threads"] == 2


# --------------------------------------------------------------------------- #
# --project-meta: the per-project provenance sidecar. Every Parquet carries 29
# metadata columns, and the sidecar is what fills them. Getting this wrong is
# quiet: the run succeeds and the columns are blank.
# --------------------------------------------------------------------------- #

def test_the_sidecar_is_forwarded_with_this_project_as_the_key(runner, jq):
    """The sidecar is keyed by the manifest name, which is what the runner is
    already told through --repo-name, so the key is not the operator's to get
    wrong."""
    ctp._OPTS.update(skip_html=False, drop_memo=False,
                     project_meta="/somewhere/project_meta.json")
    ctp.run_project(jq)
    argv = runner.argv("pipeline")
    assert argv[argv.index("--project-meta") + 1] == "/somewhere/project_meta.json"
    assert argv[argv.index("--project-key") + 1] == jq["name"]


def test_no_sidecar_sends_neither_flag(runner, jq):
    """Absent means absent: the generator then writes the metadata columns empty
    and the file still matches the corpus contract."""
    ctp._OPTS.update(skip_html=False, drop_memo=False, project_meta="")
    ctp.run_project(jq)
    argv = runner.argv("pipeline")
    assert "--project-meta" not in argv
    assert "--project-key" not in argv


def test_run_refuses_a_sidecar_path_that_does_not_exist(
        sandbox, monkeypatch, runner_script):
    """Without this the whole corpus would be regenerated with blank provenance,
    which a consumer cannot tell from provenance that is genuinely unknown."""
    runner_script()
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", forbidden)

    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(run_args(project_meta=str(sandbox.root / "absent.json")))
    assert "does not exist" in str(exc.value)
    assert "project_meta.py" in str(exc.value)


def test_run_refuses_a_sidecar_on_a_runner_that_cannot_forward_it(
        sandbox, monkeypatch, runner_script):
    """Same rule as --memory-limit: an unpatched checkout would drop the flag and
    the columns would be blank anyway."""
    runner_script(REAL_RUNNER_USAGE.replace("--project-key NAME", "--no-such-flag"))
    write_manifest(sandbox.root, VALID_ROW)
    meta = sandbox.root / "project_meta.json"
    meta.write_text("{}")
    monkeypatch.setattr(ctp, "capture_devenv_env", forbidden)

    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(run_args(project_meta=str(meta)))
    assert "--project-key" in str(exc.value)


def test_run_accepts_a_sidecar_on_a_patched_runner(
        sandbox, monkeypatch, runner_script):
    runner_script()
    write_manifest(sandbox.root, VALID_ROW)
    meta = sandbox.root / "project_meta.json"
    meta.write_text("{}")
    monkeypatch.setattr(ctp, "capture_devenv_env", lambda: {"PATH": "/x"})
    monkeypatch.setattr(ctp, "run_project", lambda p: "done")

    assert ctp.cmd_run(run_args(project_meta=str(meta))) == 0
    assert ctp._OPTS["project_meta"] == str(meta)


def test_a_relative_sidecar_path_is_made_absolute(
        sandbox, monkeypatch, runner_script):
    """The runner is started with cwd=CREGIT, but the path is typed relative to
    this repository. Unresolved, the runner rejected its own sidecar with rc=2 —
    which is exactly what happened on the first real run."""
    runner_script()
    write_manifest(sandbox.root, VALID_ROW)
    meta = sandbox.root / "project_meta.json"
    meta.write_text("{}")
    monkeypatch.chdir(sandbox.root)
    monkeypatch.setattr(ctp, "capture_devenv_env", lambda: {"PATH": "/x"})
    monkeypatch.setattr(ctp, "run_project", lambda p: "done")

    assert ctp.cmd_run(run_args(project_meta="project_meta.json")) == 0
    assert Path(ctp._OPTS["project_meta"]).is_absolute()
    assert Path(ctp._OPTS["project_meta"]) == meta.resolve()


# --------------------------------------------------------------------------- #
# the provenance guard
#
# The defect these tests exist for: `ctp.py run` used to accept --project-meta,
# --firm-map and --firm-canonical as optional, and when they were absent it simply
# did not forward them. generate_dataset.py treats an absent sidecar and an absent
# firm map as supported backwards-compatible modes, so the run SUCCEEDED and
# published a 70-column, schema-conforming Parquet whose 29 provenance columns and
# 3 firm columns were empty strings. validate.py passed it, because the schema was
# right.
#
# It is not hypothetical. On 2026-09-21 a torvalds__linux run ran 6h48m and died
# inside step 10 for an unrelated reason; had it finished it would have published
# the largest project in the corpus silently inconsistent with the other 185.
#
# The argument for an exit rather than a louder note: omitting --project-meta, the
# 29-column half, printed NOTHING at any layer. Only the 3 firm columns had a
# note, and a note is line 1 of a log whose other 105 lines are 30-second
# heartbeats. The tests below therefore pin the refusal, the itemised warning the
# escape hatch prints, and the fact that the hatch cannot arrive by default.
# --------------------------------------------------------------------------- #

def provenance_kwargs(root: Path, **override) -> dict:
    """Real on-disk values for the three provenance flags.

    The files must genuinely exist: cmd_run's per-flag checks refuse a path that
    is not a file, and that is a DIFFERENT refusal from the guard's. Pass
    e.g. project_meta="" to leave one flag out.
    """
    meta = root / "project_meta.json"
    meta.write_text('{"jq": {}}')
    firm = root / "affiliation.merged.csv"
    firm.write_text("domain,company,source\nredhat.com,Red Hat,gitdm\n")
    canonical = root / "firm_canonical.csv"
    canonical.write_text("firm_raw,firm\nRed Hat,Red Hat\n")
    kwargs = dict(project_meta=str(meta), firm_map=str(firm),
                  firm_canonical=str(canonical))
    kwargs.update(override)
    return kwargs


@pytest.fixture
def ready(sandbox, monkeypatch, runner_script):
    """A sandbox one cmd_run call away from a run that starts no process."""
    runner_script()
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", lambda: {"PATH": "/x"})
    monkeypatch.setattr(ctp, "run_project", lambda p: "done")
    return sandbox


@pytest.fixture
def must_not_start(sandbox, monkeypatch, runner_script):
    """Same sandbox, but starting the run at all fails the test. The guard has to
    fire before the devenv capture: a refusal that arrives after 6h48m of work is
    the defect, not the fix."""
    runner_script()
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", forbidden)
    monkeypatch.setattr(ctp, "run_project", forbidden)
    return sandbox


def test_all_three_provenance_flags_present_needs_no_escape_hatch(ready):
    """The canonical corpus invocation — what tmp/wave.sh sends — must pass the
    guard with allow_empty_provenance at its real CLI default of False."""
    args = run_args(allow_empty_provenance=False,
                    **provenance_kwargs(ready.root))
    assert ctp.cmd_run(args) == 0
    assert ctp._OPTS["project_meta"].endswith("project_meta.json")
    assert ctp._OPTS["firm_map"].endswith("affiliation.merged.csv")
    assert ctp._OPTS["firm_canonical"].endswith("firm_canonical.csv")


@pytest.mark.parametrize("absent, expected", [
    pytest.param({"project_meta": ""}, "--project-meta", id="no-sidecar"),
    # --firm-canonical goes with it: without a map there is no firm_raw to
    # canonicalise, and cmd_run already refuses that pair on its own grounds, so
    # keeping it here would test the older check instead of this one.
    pytest.param({"firm_map": "", "firm_canonical": ""}, "--firm-map",
                 id="no-firm-map"),
    pytest.param({"firm_canonical": ""}, "--firm-canonical",
                 id="no-canonical-table"),
])
def test_each_provenance_flag_missing_on_its_own_is_refused(
        must_not_start, absent, expected):
    """One flag left out is enough. Each one owns a different slice of the
    columns, so any single omission publishes a project that disagrees with the
    rest of the corpus."""
    kwargs = provenance_kwargs(must_not_start.root, **absent)
    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(run_args(allow_empty_provenance=False, **kwargs))
    message = str(exc.value)
    assert "refusing this run" in message
    assert expected in message
    assert "--allow-empty-provenance" in message, (
        "a refusal that does not name its escape hatch sends the operator to "
        "read the source")
    assert not (must_not_start.root / "runs.log").exists()


def test_the_refusal_names_every_missing_flag_and_the_columns_at_stake(
        must_not_start):
    """All three absent is the 2026-09-21 invocation exactly. The message must
    list all three in one pass — reporting them one per re-run would cost three
    round trips — and must name columns, because "provenance" alone does not tell
    an operator what a consumer will see."""
    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(run_args(allow_empty_provenance=False))
    message = str(exc.value)
    for flag in ("--project-meta", "--firm-map"):
        assert flag in message
    for column in ("stratum", "history_cluster", "firm_raw", "firm_source"):
        assert column in message, f"{column} is blanked but is not named"
    assert "29" in message and "3 firm columns" in message
    # Without this sentence the reader has no reason to believe the run would not
    # simply have failed, which is the misconception that let it happen.
    assert "validate.py passes it" in message


def test_the_escape_hatch_lets_a_deliberate_blank_run_proceed(ready):
    """A blanket refusal would break a fixture, a one-project smoke run, and a
    corpus whose sidecar does not exist yet."""
    assert ctp.cmd_run(run_args(allow_empty_provenance=True)) == 0
    assert ctp._OPTS["project_meta"] == ""
    assert ctp._OPTS["firm_map"] == ""


def test_the_escape_hatch_itemises_what_it_gave_up(ready, capsys):
    """Opting out of the only check between this run and a quietly wrong dataset
    must be legible in the log, per flag, without inferring it from the absence of
    a refusal."""
    assert ctp.cmd_run(run_args(allow_empty_provenance=True)) == 0
    out = capsys.readouterr().out
    assert "WARNING: --allow-empty-provenance" in out
    assert "--project-meta absent" in out
    assert "--firm-map absent" in out
    assert "stratum" in out


def test_the_escape_hatch_is_off_until_it_is_typed(monkeypatch):
    """It must never arrive by default. This is the difference between the guard
    and the note it replaces."""
    seen: dict = {}
    monkeypatch.setattr(ctp, "cmd_run", lambda args: seen.update(vars(args)) or 0)

    monkeypatch.setattr(ctp.sys, "argv", ["ctp.py", "run"])
    assert ctp.main() == 0
    assert seen["allow_empty_provenance"] is False

    seen.clear()
    monkeypatch.setattr(ctp.sys, "argv",
                        ["ctp.py", "run", "--allow-empty-provenance"])
    assert ctp.main() == 0
    assert seen["allow_empty_provenance"] is True


def test_the_escape_hatch_is_refused_when_nothing_would_be_blank(must_not_start):
    """Left in a launcher script the hatch would silence the guard on the next run
    that does omit a flag, which is how the warning it replaces became useless.
    So it is only ever valid in the same breath as an omission."""
    kwargs = provenance_kwargs(must_not_start.root)
    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(run_args(allow_empty_provenance=True, **kwargs))
    assert "nothing to allow" in str(exc.value)
    assert "wrapper" in str(exc.value)


def test_a_run_that_cannot_reach_step_10_is_not_gated(ready, capsys):
    """Step 10 is the LAST step of run_pipeline_process.sh, so --from-step 11
    reaches no step at all: every step guard evaluates false and the runner exits
    0 having done nothing. Such a run writes no Parquet, so it cannot write a
    blank one, and gating it would refuse a harmless no-op."""
    assert ctp.cmd_run(run_args(allow_empty_provenance=False, from_step=11)) == 0
    assert "refusing" not in capsys.readouterr().out


def test_a_run_starting_at_step_10_is_still_gated(must_not_start):
    """The boundary. --from-step 10 runs the dataset step and nothing else, which
    is exactly how a Parquet gets republished, so it is the case that most needs
    the guard rather than the one that escapes it."""
    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(run_args(allow_empty_provenance=False, from_step=10))
    assert "reaches step 10" in str(exc.value)


@pytest.mark.parametrize("absent, expected", [
    pytest.param("project_meta", "--project-meta", id="sidecar"),
    pytest.param("firm_canonical", "--firm-canonical", id="canonical-table"),
])
def test_a_provenance_path_that_is_not_a_file_is_refused_by_its_own_check(
        must_not_start, absent, expected):
    """The guard deliberately does NOT re-check that the paths exist. Three layers
    already do, and every one of them fails hard: cmd_run's own per-flag checks
    (exercised further in tests/test_firm_attribution.py), then
    run_pipeline_process.sh's argument validation (exit 2, before the clone), then
    generate_dataset.py at the top of step 10. A path typo therefore cannot reach
    the Parquet, so a fourth copy of the check would only be a fourth thing to
    keep in step.

    What these two assertions pin is the ORDERING: the sharper "is not a file"
    message must win, because the guard's general one would send an operator
    looking for a missing flag they did in fact pass."""
    kwargs = provenance_kwargs(must_not_start.root,
                               **{absent: str(must_not_start.root / "typo")})
    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(run_args(allow_empty_provenance=False, **kwargs))
    message = str(exc.value)
    assert expected in message
    assert "does not exist" in message or "is not a file" in message
    assert "refusing this run" not in message


def test_project_key_is_not_an_operator_flag(monkeypatch, capsys):
    """The brief for this guard listed --project-key as a fourth defaultable flag.
    It is not one: ctp has no such option. run_project derives the key from the
    manifest name and sends it with the sidecar unconditionally, so it cannot be
    forgotten or mistyped and needs no guard. Pinned here because the next reader
    will make the same assumption."""
    monkeypatch.setattr(ctp.sys, "argv",
                        ["ctp.py", "run", "--project-key", "jq"])
    monkeypatch.setattr(ctp, "cmd_run", forbidden)
    with pytest.raises(SystemExit) as exc:
        ctp.main()
    assert exc.value.code == 2
    assert "unrecognized arguments" in capsys.readouterr().err


# --------------------------------------------------------------------------- #
# cmd_progress
# --------------------------------------------------------------------------- #

def progress_args(**over):
    """A complete `ctp progress` Namespace."""
    base = dict(manifest="manifest.tsv", last=5, jobs=2)
    base.update(over)
    return argparse.Namespace(**base)


def write_candidates(tmp_path: Path, *rows: tuple) -> Path:
    """candidates.csv with only the columns commit_counts reads."""
    path = tmp_path / "candidates.csv"
    lines = ["clone_url,commits"]
    lines += [f"{url},{commits}" for url, commits in rows]
    path.write_text("\n".join(lines) + "\n")
    return path


def write_metrics(tmp_path: Path, *rows: tuple) -> Path:
    path = tmp_path / "metrics.tsv"
    head = "iso_start\tproject\tclass\tphase\tduration_s\trc\tlog\n"
    body = "".join(f"2026-09-15T00:00:00+00:00\t{n}\t{c}\t{ph}\t{d}\t{rc}\tlog\n"
                   for n, c, ph, d, rc in rows)
    path.write_text(head + body)
    return path


@pytest.mark.parametrize("done,total,filled", [
    (0, 10, 0),
    (5, 10, 15),
    (10, 10, 30),
    (0, 0, 0),
])
def test_bar_fills_in_proportion(done, total, filled):
    """A bar is 30 cells wide whatever the numbers."""
    out = ctp.bar(done, total)
    assert len(out) == 30
    assert out.count("█") == filled


def test_bar_does_not_divide_by_zero_on_an_empty_manifest():
    """An empty manifest is a real case: --only can match nothing."""
    assert ctp.bar(0, 0) == "░" * 30


def test_commit_counts_joins_on_clone_url_not_name(sandbox):
    """The manifest name is a SLUG: it lowercases and maps `_` and `.` to `-`.
    Joining on name silently drops those projects."""
    write_candidates(sandbox.root,
                     ("https://github.com/amd/esmi_oob_library.git", 4321))
    projects = [dict(name="amd__esmi-oob-library",
                     url="https://github.com/amd/esmi_oob_library.git",
                     size_class="S")]
    assert ctp.commit_counts(projects) == {"amd__esmi-oob-library": 4321}


def test_commit_counts_skips_a_row_with_no_commit_count(sandbox):
    """An un-enriched candidate row has an empty commits field."""
    write_candidates(sandbox.root, ("https://x/a.git", ""))
    projects = [dict(name="a", url="https://x/a.git", size_class="S")]
    assert ctp.commit_counts(projects) == {}


def test_commit_counts_returns_empty_without_candidates_csv(sandbox):
    """Progress must still print. Only the ETA depends on this file."""
    projects = [dict(name="a", url="https://x/a.git", size_class="S")]
    assert ctp.commit_counts(projects) == {}


def test_finished_runs_returns_empty_without_metrics(sandbox):
    assert ctp.finished_runs() == []


def test_finished_runs_keeps_only_successful_pipeline_rows(sandbox):
    """A validate row is not a run, and rc!=0 is not a finish."""
    write_metrics(sandbox.root,
                  ("a", "S", "pipeline", 100, 0),
                  ("a", "S", "validate", 0, 0),
                  ("b", "M", "pipeline", 200, 1),
                  ("c", "M", "pipeline", 300, 0))
    assert ctp.finished_runs() == [("a", "S", 100), ("c", "M", 300)]


def test_measured_rate_is_none_below_the_sample_floor(sandbox):
    """One project is an anecdote, not a rate."""
    write_metrics(sandbox.root, ("a", "S", "pipeline", 100, 0))
    write_candidates(sandbox.root, ("https://x/a.git", 1000))
    projects = [dict(name="a", url="https://x/a.git", size_class="S")]
    assert ctp.measured_rate(ctp.commit_counts(projects)) is None


def test_measured_rate_takes_the_median_of_an_odd_sample(sandbox):
    """The median rejects a --from-step resume, which looks impossibly fast:
    kamailio's step-10 resume took 74 s for 61k commits."""
    write_metrics(sandbox.root,
                  ("resume", "M", "pipeline", 74, 0),
                  ("a", "M", "pipeline", 100, 0),
                  ("b", "M", "pipeline", 120, 0))
    commits = {"resume": 61000, "a": 1000, "b": 1000}
    median, samples = ctp.measured_rate(commits)
    assert samples == 3
    assert median == 100.0, "the 1.2 s/1k resume must not drag the median"


def test_measured_rate_averages_the_middle_two_of_an_even_sample(sandbox):
    write_metrics(sandbox.root,
                  ("a", "M", "pipeline", 100, 0),
                  ("b", "M", "pipeline", 120, 0))
    median, samples = ctp.measured_rate({"a": 1000, "b": 1000})
    assert (median, samples) == (110.0, 2)


def test_measured_rate_ignores_a_project_with_no_commit_count(sandbox):
    """Two finished runs but one unknown size means one usable point."""
    write_metrics(sandbox.root,
                  ("a", "M", "pipeline", 100, 0),
                  ("unknown", "M", "pipeline", 999, 0))
    assert ctp.measured_rate({"a": 1000}) is None


def test_progress_prints_a_bar_and_the_class_breakdown(sandbox, capsys):
    """The headline numbers: how many of how many, and per size class."""
    write_manifest(sandbox.root,
                   "a\thttps://x/a.git\tcommunity\t\\.c$\tS",
                   "b\thttps://x/b.git\tcommunity\t\\.c$\tM")
    (sandbox.out / "a").mkdir()
    (sandbox.out / "a" / "a.validated").write_text("rows=1\n")

    assert ctp.cmd_progress(progress_args()) == 0
    out = capsys.readouterr().out
    assert "1/2" in out
    assert "50.0%" in out
    assert "S 1/1" in out
    assert "M 0/1" in out


def test_progress_reports_failed_projects(sandbox, capsys):
    """A failed project is neither done nor running, and it must be visible."""
    write_manifest(sandbox.root, "a\thttps://x/a.git\tcommunity\t\\.c$\tS")
    (sandbox.root / "state" / "a").mkdir(parents=True)

    assert ctp.cmd_progress(progress_args()) == 0
    assert "FAILED 1" in capsys.readouterr().out


def test_progress_lists_the_running_projects(sandbox, capsys):
    """A held lock means running."""
    write_manifest(sandbox.root, "a\thttps://x/a.git\tcommunity\t\\.c$\tS")
    with held_lock(ctp.lock_path("a")):
        assert ctp.cmd_progress(progress_args()) == 0
    out = capsys.readouterr().out
    assert "running" in out
    assert "a" in out


def test_progress_lists_the_last_finished_newest_first(sandbox, capsys):
    """The question is "what just finished", so newest goes first."""
    write_manifest(sandbox.root, "a\thttps://x/a.git\tcommunity\t\\.c$\tS")
    write_metrics(sandbox.root,
                  ("older", "S", "pipeline", 60, 0),
                  ("newer", "S", "pipeline", 120, 0))

    assert ctp.cmd_progress(progress_args(last=2)) == 0
    out = capsys.readouterr().out
    assert out.index("newer") < out.index("older")
    assert "2.0 min" in out


def test_progress_honours_the_last_limit(sandbox, capsys):
    write_manifest(sandbox.root, "a\thttps://x/a.git\tcommunity\t\\.c$\tS")
    write_metrics(sandbox.root,
                  ("one", "S", "pipeline", 60, 0),
                  ("two", "S", "pipeline", 60, 0),
                  ("three", "S", "pipeline", 60, 0))

    assert ctp.cmd_progress(progress_args(last=1)) == 0
    out = capsys.readouterr().out
    assert "three" in out
    assert "one" not in out


def test_progress_estimates_an_eta_from_the_measured_rate(sandbox, capsys):
    """1000 commits left at 100 s per 1k, over 2 jobs, is 50 s.

    The rate sample is drawn from THIS manifest, because commit_counts only
    covers manifest rows. A finished project outside the manifest contributes
    nothing, which is what we want: the rate should describe the work left.
    """
    write_manifest(sandbox.root,
                   "a\thttps://x/a.git\tcommunity\t\\.c$\tS",
                   "b\thttps://x/b.git\tcommunity\t\\.c$\tS",
                   "c\thttps://x/c.git\tcommunity\t\\.c$\tS")
    write_candidates(sandbox.root,
                     ("https://x/a.git", 1000), ("https://x/b.git", 1000),
                     ("https://x/c.git", 1000))
    write_metrics(sandbox.root,
                  ("a", "S", "pipeline", 100, 0),
                  ("b", "S", "pipeline", 100, 0))
    for name in ("a", "b"):
        (sandbox.out / name).mkdir()
        (sandbox.out / name / f"{name}.validated").write_text("rows=1\n")

    assert ctp.cmd_progress(progress_args(jobs=2)) == 0
    out = capsys.readouterr().out
    assert "100.0 s per 1k commits" in out
    assert "1,000 commits left" in out, "the two done projects must not count"
    assert "0.0 h at --jobs 2" in out, "1000 commits / 2 jobs at 100 s/1k = 50 s"


def test_progress_eta_treats_zero_jobs_as_one(sandbox, capsys):
    """--jobs 0 must not divide by zero."""
    write_manifest(sandbox.root,
                   "a\thttps://x/a.git\tcommunity\t\\.c$\tS",
                   "b\thttps://x/b.git\tcommunity\t\\.c$\tS")
    write_candidates(sandbox.root,
                     ("https://x/a.git", 1000), ("https://x/b.git", 1000))
    write_metrics(sandbox.root,
                  ("a", "S", "pipeline", 100, 0),
                  ("b", "S", "pipeline", 100, 0))

    assert ctp.cmd_progress(progress_args(jobs=0)) == 0
    assert "at --jobs 0" in capsys.readouterr().out


def test_progress_says_so_when_it_cannot_estimate(sandbox, capsys):
    """No candidates.csv means no commit counts, so no ETA. Say why."""
    write_manifest(sandbox.root, "a\thttps://x/a.git\tcommunity\t\\.c$\tS")

    assert ctp.cmd_progress(progress_args()) == 0
    assert "not enough finished projects yet" in capsys.readouterr().out


def test_progress_survives_an_empty_manifest(sandbox, capsys):
    """--only can match nothing, and a crash here would be a poor status tool."""
    write_manifest(sandbox.root)

    assert ctp.cmd_progress(progress_args()) == 0
    out = capsys.readouterr().out
    assert "0/0" in out
    assert "0.0%" in out


def test_project_state_reports_each_of_the_four_states(sandbox):
    """cmd_status and cmd_progress share this, so it is worth pinning directly."""
    assert ctp.project_state("nothing") == "QUEUED"

    (sandbox.out / "failed").mkdir()
    assert ctp.project_state("failed") == "FAILED"

    (sandbox.out / "done").mkdir()
    (sandbox.out / "done" / "done.validated").write_text("rows=1\n")
    assert ctp.project_state("done") == "DONE"

    with held_lock(ctp.lock_path("busy")):
        assert ctp.project_state("busy") == "RUNNING"


# --------------------------------------------------------------------------- #
# the universal mask, and the one-project override
# --------------------------------------------------------------------------- #

def test_a_blank_file_filter_column_falls_back_to_the_universal_mask(tmp_path):
    """An empty mask is not "no filter": blobExec rejects it, and anything that
    accepted it would select every file in the repository. A hand-written manifest
    that leaves the column blank means "whatever the tokenizer can parse"."""
    row = "jq\thttps://github.com/jqlang/jq.git\tcommunity\t\tS"
    projects = ctp.read_manifest(write_manifest(tmp_path, row), None)
    assert projects[0]["file_filter"] == UNIVERSAL_MASK


def test_the_manifests_mask_is_what_reaches_the_runner(sandbox, runner, jq):
    """No override: the manifest column is the mask, because project_meta.py reads
    that same column for the Parquet's file_mask, and the two must describe the
    same run."""
    ctp._OPTS.update(skip_html=False, drop_memo=False)
    ctp.run_project(dict(jq, file_filter=UNIVERSAL_MASK))
    argv = runner.argv("pipeline")
    assert argv[argv.index("--mask") + 1] == UNIVERSAL_MASK


def test_mask_overrides_the_manifest_for_one_deliberate_run(sandbox, runner, jq):
    """The escape hatch: one project, one mask, without editing the manifest."""
    ctp._OPTS.update(skip_html=False, drop_memo=False, mask=r"\.java$")
    ctp.run_project(dict(jq, file_filter=UNIVERSAL_MASK))
    argv = runner.argv("pipeline")
    assert argv[argv.index("--mask") + 1] == r"\.java$"


# --------------------------------------------------------------------------- #
# --mask-widened: reuse the tokenizations across a mask change
#
# The corpus holds 198 blob maps and every one of them records an OLD per-language
# mask, so without this flag the re-run tokenizes 13.6 million (blob, path) pairs
# from cold to reach the 5.6% that are genuinely new. The flag is off by default
# and the refusal it steps around is correct for every other case, so the tests
# here are as much about what it refuses as about what it forwards.
# --------------------------------------------------------------------------- #

def test_mask_widened_is_forwarded_to_the_runner(runner, jq):
    ctp._OPTS.update(skip_html=False, drop_memo=False, from_step=2,
                     mask_widened=True)
    ctp.run_project(jq)
    argv = runner.argv("pipeline")
    assert "--mask-widened" in argv
    # Still before the positional FROM_STEP, which the runner reads from the tail.
    assert argv[-1] == "2"


def test_no_mask_widened_sends_no_flag_so_the_refusal_stays_the_default(runner, jq):
    """The default is a full rebuild on a mask change. Nothing in this change may
    make that happen by accident."""
    ctp._OPTS.update(skip_html=False, drop_memo=False, from_step=2,
                     mask_widened=False)
    ctp.run_project(jq)
    assert "--mask-widened" not in runner.argv("pipeline")


def test_a_missing_mask_widened_option_sends_no_flag(runner, jq):
    """run_project reads _OPTS directly, so a caller that never set the key must
    get the safe behaviour rather than a crash."""
    ctp._OPTS.clear()
    ctp._OPTS.update(skip_html=False, drop_memo=False)
    ctp.run_project(jq)
    assert "--mask-widened" not in runner.argv("pipeline")


def test_mask_widened_at_step_one_is_refused_because_step_one_deletes_the_workdir(
        sandbox, monkeypatch, capsys):
    """The flag exists to preserve the blob map, and step 1 deletes the directory
    holding it. Running anyway would preserve nothing and look like a success."""
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", forbidden)
    monkeypatch.setattr(ctp, "run_project", forbidden)
    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(run_args(mask_widened=True, from_step=1))
    assert "--from-step 2" in str(exc.value)


def test_mask_widened_is_refused_on_a_runner_that_cannot_forward_it(
        sandbox, monkeypatch, runner_script, capsys):
    """Dropped silently, blobExec refuses every project on the recorded mask and
    the corpus run reads as broken rather than as a missing feature."""
    runner_script(REAL_RUNNER_USAGE.replace("--mask-widened", "--nope-widened"))
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", forbidden)
    monkeypatch.setattr(ctp, "run_project", forbidden)
    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(run_args(mask_widened=True, from_step=2))
    assert "--mask-widened is not implemented" in str(exc.value)


def test_mask_widened_is_refused_together_with_sharding(sandbox, monkeypatch):
    """Each shard builds a fresh blob map, so there is no recorded mask to widen."""
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", forbidden)
    monkeypatch.setattr(ctp, "run_project", forbidden)
    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(run_args(mask_widened=True, from_step=2, shards=4))
    assert "no recorded mask to widen" in str(exc.value)


def test_mask_widened_is_announced_with_what_it_discards(sandbox, monkeypatch, capsys):
    """This is the one flag that reuses a cache the tool otherwise refuses. A log
    a reader cannot tell that from is not good enough."""
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", lambda: {"PATH": "/x"})
    monkeypatch.setattr(ctp, "run_project", lambda p: "done")

    assert ctp.cmd_run(run_args(mask_widened=True, from_step=2)) == 0
    out = capsys.readouterr().out
    assert "REUSE" in out
    assert "identity rows are discarded" in out
    assert "raw source" in out


def test_no_mask_widened_announces_nothing(sandbox, monkeypatch, capsys):
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", lambda: {"PATH": "/x"})
    monkeypatch.setattr(ctp, "run_project", lambda p: "done")

    assert ctp.cmd_run(run_args(from_step=2)) == 0
    assert "--mask-widened" not in capsys.readouterr().out


def test_an_override_warns_that_the_recorded_mask_will_not_match(sandbox, monkeypatch,
                                                                capsys):
    """file_mask in the Parquet comes from the sidecar, which reads the manifest.
    An override therefore makes the recorded mask a lie about how those tokens
    were produced, and the operator has to be told."""
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", lambda: {"PATH": "/x"})
    monkeypatch.setattr(ctp, "run_project", lambda p: "done")

    assert ctp.cmd_run(run_args(mask=r"\.rs$")) == 0
    out = capsys.readouterr().out
    assert r"--mask overrides the manifest" in out
    assert r"\.rs$" in out
    assert "will not match" in out


# ---------------------------------------------------------------------------
# --retokenize: discard ONE extension's cached tokenizations after the
# tokenizer that produced them was corrected.
#
# This is not --mask-widened's problem and must not be confused with it. The
# mask decides WHICH files are tokenized; this decides HOW. The blob map keys
# reuse on the command string and the mask, and rebuilding a tokenizer binary
# changes neither — which is exactly how a 16-day-stale rust_tokenizer shipped
# shifted token columns and the run exited 0.
# ---------------------------------------------------------------------------

def test_retokenize_is_forwarded_to_the_runner(runner, jq):
    ctp._OPTS.update(skip_html=False, drop_memo=False, from_step=2,
                     mask_widened=False, retokenize="rs")
    ctp.run_project(jq)
    argv = runner.argv("pipeline")
    assert "--retokenize" in argv
    assert argv[argv.index("--retokenize") + 1] == "rs"
    # Still before the positional FROM_STEP, which the runner reads from the tail.
    assert argv[-1] == "2"


def test_no_retokenize_sends_no_flag_so_nothing_is_discarded_by_accident(runner, jq):
    """The default must never delete cached work. This flag is opt-in only."""
    ctp._OPTS.update(skip_html=False, drop_memo=False, from_step=2,
                     mask_widened=False, retokenize="")
    ctp.run_project(jq)
    assert "--retokenize" not in runner.argv("pipeline")


def test_a_missing_retokenize_option_sends_no_flag(runner, jq):
    """run_project reads _OPTS directly, so a caller that never set the key gets
    the safe behaviour rather than a crash."""
    ctp._OPTS.clear()
    ctp._OPTS.update(skip_html=False, drop_memo=False)
    ctp.run_project(jq)
    assert "--retokenize" not in runner.argv("pipeline")


def test_retokenize_needs_step_two_exactly_not_two_or_more(sandbox, monkeypatch):
    """Step 1 deletes the blob map this flag edits. Step 3 and later SKIP step 2,
    so the tokens would never be remade and the rest of the pipeline would run over
    the stale ones and exit 0 — the worst outcome, because it looks like success."""
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", forbidden)
    monkeypatch.setattr(ctp, "run_project", forbidden)
    for bad in (1, 3, 7):
        with pytest.raises(SystemExit) as exc:
            ctp.cmd_run(run_args(retokenize="rs", from_step=bad))
        assert "--from-step 2 exactly" in str(exc.value)


def test_retokenize_is_refused_on_a_runner_that_cannot_forward_it(
        sandbox, monkeypatch, runner_script):
    """Dropped silently, step 2 reuses the very tokenizations the operator asked to
    discard and the run exits 0. Nothing in the output would say the corrected
    tokenizer never ran."""
    runner_script(REAL_RUNNER_USAGE.replace("--retokenize EXTS", "--nope EXTS"))
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", forbidden)
    monkeypatch.setattr(ctp, "run_project", forbidden)
    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(run_args(retokenize="rs", from_step=2))
    assert "--retokenize is not implemented" in str(exc.value)


def test_retokenize_and_mask_widened_are_refused_together(sandbox, monkeypatch):
    """Each verifies a different invariant of the blob map. Together neither check
    means anything: one reuses rows across a mask change, the other deletes rows a
    tokenizer change invalidated."""
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", forbidden)
    monkeypatch.setattr(ctp, "run_project", forbidden)
    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(run_args(retokenize="rs", mask_widened=True, from_step=2))
    assert "cannot be used in the same run" in str(exc.value)


def test_retokenize_is_refused_together_with_sharding(sandbox, monkeypatch):
    """Each shard builds a fresh blob map, so there are no cached tokenizations to
    invalidate."""
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", forbidden)
    monkeypatch.setattr(ctp, "run_project", forbidden)
    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(run_args(retokenize="rs", from_step=2, shards=4))
    assert "no cached tokenizations to invalidate" in str(exc.value)


def test_retokenize_is_announced_with_what_it_discards(sandbox, monkeypatch, capsys):
    """This flag DELETES cached work. A reader of the log must see which extensions
    lost their tokenizations without inferring it from the absence of a refusal."""
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", lambda: {"PATH": "/x"})
    monkeypatch.setattr(ctp, "run_project", lambda p: "done")

    assert ctp.cmd_run(run_args(retokenize="rs", from_step=2)) == 0
    out = capsys.readouterr().out
    assert "DISCARD" in out
    assert "rs" in out


def test_a_blank_retokenize_is_treated_as_absent(sandbox, monkeypatch):
    """Whitespace from a shell variable that expanded to nothing must not count as
    a request, and must not trip the step-2 refusal on an ordinary run."""
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", lambda: {"PATH": "/x"})
    monkeypatch.setattr(ctp, "run_project", lambda p: "done")
    assert ctp.cmd_run(run_args(retokenize="   ", from_step=1)) == 0
