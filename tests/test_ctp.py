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


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #

VALID_ROW = "jq\thttps://github.com/jqlang/jq.git\tcommunity\t\\.[ch]$\tS"

# The flags the configured runner (cregit-issue61/run_pipeline_process.sh)
# actually advertises. Used to build a stand-in script in tmp_path so no test
# reads the real checkout.
REAL_RUNNER_USAGE = """\
#!/bin/sh
# usage: run_pipeline_process.sh --repo-url URL [options] [FROM_STEP]
#   --repo-url URL    git URL of the repository to process (REQUIRED)
#   --repo-name NAME  short name used to prefix the output files
#   --commit-url URL  commit browse URL
#   --mask REGEX      regex selecting the files to tokenize; quote it
#   --work DIR        working/output directory
#   --skip-html       do not generate the HTML views
#   --mode MODE       tokenizer mode
#   --shards N        shard count
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


def test_run_refuses_to_start_when_the_runner_lacks_a_required_flag(
        sandbox, monkeypatch, runner_script):
    """Defect D1, turned into a gate. Every project passes --work and --mask, so a
    runner that does not know them costs one exit-2 per project and produces
    nothing. Refuse once instead, before the devenv capture starts anything.
    """
    runner_script(REAL_RUNNER_USAGE.replace("--mask", "--file-filter"))
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", forbidden)

    args = argparse.Namespace(manifest="manifest.tsv", only=None, jobs=1, retries=0,
                              skip_html=False, drop_memo=False)
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

    args = argparse.Namespace(manifest="manifest.tsv", only=None, jobs=1, retries=0,
                              skip_html=True, drop_memo=False)
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

    args = argparse.Namespace(manifest="manifest.tsv", only=None, jobs=1, retries=0,
                              skip_html=True, drop_memo=False)
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

    args = argparse.Namespace(manifest="manifest.tsv", only=None, jobs=1, retries=0,
                              skip_html=False, drop_memo=True)
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
# argument construction
# --------------------------------------------------------------------------- #

def test_pipeline_argv_is_built_exactly_like_this(sandbox, runner, jq):
    """Locks in the argv ctp builds.

    The flag names are the runner's: run_pipeline_process.sh takes --work and
    --mask. ctp used to send --work-dir and --file-filter, which the runner
    rejects with exit 2, so every project failed before doing any work. Defect D1.
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
    """Defect D2. run_pipeline_process.sh runs `rm -rf "$WORK"` at FROM_STEP=1 and
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

    This is the general form of D1. It compares the argv against the runner's own
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
    run fails per project instead — D1 again, with a check that looked green.
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
    base = dict(manifest="manifest.tsv", only=None, jobs=1, retries=0,
                skip_html=False, drop_memo=False)
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
