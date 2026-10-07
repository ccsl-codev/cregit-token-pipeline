"""Unit tests for ctp.py. The autouse `sandbox` fixture moves every path constant
into tmp_path and makes subprocess.run and Popen raise, so no test touches the
real corpus or starts a process; a test that needs one installs a fake."""

from __future__ import annotations

import argparse
import hashlib
import json
import fcntl
import os
import shutil
import threading
import time
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

import ctp
import ledger
import retain
from file_mask import UNIVERSAL_MASK

REAL_TOOL_VERSIONS = ctp.tool_versions


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #

VALID_ROW = "jq\thttps://github.com/jqlang/jq.git\tcommunity\t\\.[ch]$\tS"

# The real runner's usage() text. Keep it in step by hand: script_supports()
# greps the real script, but these tests grep this copy, so drift stays green.
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
#   --mask-widened    resume across a mask change, reusing blob_map
#   --retokenize EXTS re-tokenize only these extensions after a tokenizer fix
#   --mode MODE       tokenizer mode
#   --shards N        shard count
#   --jobs N      concurrent blame/HTML processes
# unknown arguments exit 2
"""


PINNED_SHA = "0123456789abcdef0123456789abcdef01234567"
OTHER_SHA = "fedcba9876543210fedcba9876543210fedcba98"
PINNED_ROW = VALID_ROW + "\t" + PINNED_SHA

# The script each python3 step runs, by phase.
STEP_SCRIPTS = {"pin.py": "clone", "firm_attribution.py": "firm", "validate.py": "validate",
                "validate_schema.py": "schema", "consolidate.py": "join",
                "anonymize_parquet.py": "anonymize"}


class FakeRunner:
    """Recording stand-in for subprocess.Popen and subprocess.run. Returns a
    scripted rc per phase, writes the stamp when validate succeeds, as
    validate.py does, and the result file when clone succeeds, as pin.py does.
    `checkout` is the HEAD that `git rev-parse` reports for the runner's clone."""

    def __init__(self, *, pipeline_rc=0, firm_rc=0, validate_rc=0, raise_on=None,
                 payload=b"", rcs=None, checkout=PINNED_SHA, pin_result=True,
                 anon_output=True):
        self.calls: list[SimpleNamespace] = []
        self.rcs = {"pipeline": pipeline_rc, "firm": firm_rc, "validate": validate_rc,
                    **(rcs or {})}
        self.raise_on = raise_on
        self.payload = payload
        self.checkout = checkout
        self.pin_result = pin_result
        self.anon_output = anon_output

    @staticmethod
    def phase_of(args) -> str:
        if str(args[0]).endswith("run_pipeline_process.sh"):
            return "pipeline"
        if args[0] == "git":
            return "git"
        return STEP_SCRIPTS.get(Path(args[1]).name, "validate") if len(args) > 1 else "validate"

    def rc_of(self, phase: str) -> int:
        return self.rcs.get(phase, 0)

    def __call__(self, args, **kwargs):
        phase = self.phase_of(args)
        self.calls.append(SimpleNamespace(phase=phase, args=list(args), kwargs=kwargs))
        if self.raise_on == phase:
            raise OSError(f"no such file or directory: {args[0]}")
        if phase == "git":
            return SimpleNamespace(returncode=0, stdout=f"{self.checkout}\n")
        stream = kwargs.get("stdout")
        if self.payload and hasattr(stream, "write"):
            stream.write(self.payload)
        rc = self.rc_of(phase)
        if phase == "pipeline" and rc == 0 and "--commit-url" in args:
            # A pinned run's working clone, which runner_checkout reads. Unpinned
            # runs leave none, so a test that fakes only Popen starts no git.
            work = Path(args[args.index("--work") + 1])
            name = args[args.index("--repo-name") + 1]
            (work / f"{name}-original").mkdir(parents=True, exist_ok=True)
        if phase == "anonymize" and rc == 0 and self.anon_output:
            outdir = Path(args[2])
            outdir.mkdir(parents=True, exist_ok=True)
            (outdir / Path(args[3]).name).write_bytes(b"PAR1 anon PAR1")
        if phase == "clone" and rc == 0 and self.pin_result:
            commit = args[args.index("--commit") + 1]
            Path(args[args.index("--result") + 1]).write_text(
                f'{{"pinned_sha": "{commit}", "checked_out_sha": "{commit}"}}\n')
        if phase == "validate" and rc == 0:
            stamp = Path(args[3])
            # A relative stamp path would escape tmp_path into the repository.
            assert stamp.is_absolute(), (
                f"stamp path must be absolute, got {stamp!r}. A relative path "
                f"escapes tmp_path and writes into the repository.")
            stamp.write_text("rows=42\nbytes=123456\n")
        # Our own pid, so the resource sampler walks a real process tree.
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
    """Redirect every path and side effect of ctp into tmp_path."""
    out = tmp_path / "corpus-files"
    out.mkdir()
    cregit = tmp_path / "cregit"
    cregit.mkdir()
    # An empty CREGIT would give cmd_run's flag preflight nothing to read.
    (cregit / "run_pipeline_process.sh").write_text(REAL_RUNNER_USAGE)

    monkeypatch.setattr(ctp, "CORPUS", tmp_path)
    monkeypatch.setattr(ctp, "OUT", out)
    monkeypatch.setattr(ctp, "CREGIT", cregit)
    monkeypatch.setattr(ctp, "METRICS", tmp_path / "metrics.tsv")
    monkeypatch.setattr(ctp, "RUNS_LOG", tmp_path / "runs.log")
    monkeypatch.setattr(ctp, "RESOURCES", tmp_path / "resources.tsv")
    monkeypatch.setattr(ctp, "LEDGER", tmp_path / "ledger.jsonl")
    # tool_versions starts git and srcml; its own tests call REAL_TOOL_VERSIONS.
    monkeypatch.setattr(ctp, "tool_versions", lambda: {"ctp_commit": "test"})
    monkeypatch.setattr(retain, "STATE", tmp_path / "state")
    monkeypatch.setattr(retain, "OUT", out)
    # A real disk_usage call stays, but the floor can never trip by accident.
    monkeypatch.setattr(ctp, "DISK_FLOOR_GB", 0)
    monkeypatch.setattr(ctp.time, "sleep", lambda *a, **k: None)
    monkeypatch.setattr(ctp.subprocess, "run", forbidden)
    monkeypatch.setattr(ctp.subprocess, "Popen", forbidden)

    for shared in (ctp._OPTS, ctp._ENV, ctp._live, ctp._RUN, ctp._lock_fds):
        shared.clear()
    yield SimpleNamespace(root=tmp_path, out=out, cregit=cregit)
    for shared in (ctp._OPTS, ctp._ENV, ctp._live, ctp._RUN, ctp._lock_fds):
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


@pytest.fixture
def ready(sandbox, monkeypatch, runner_script):
    """A sandbox one cmd_run call away from a run that starts no process."""
    runner_script()
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", lambda: {"PATH": "/x"})
    monkeypatch.setattr(ctp, "run_project", lambda p: ctp.RunOutcome.DONE)
    return sandbox


@pytest.fixture
def must_not_start(sandbox, monkeypatch, runner_script):
    """Same sandbox, but starting the run fails the test: every refusal must
    come before the devenv capture."""
    runner_script()
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "capture_devenv_env", forbidden)
    monkeypatch.setattr(ctp, "run_project", forbidden)
    return sandbox


# --------------------------------------------------------------------------- #
# manifest parsing
# --------------------------------------------------------------------------- #

def test_manifest_parses_a_valid_row(tmp_path):
    projects = ctp.read_manifest(write_manifest(tmp_path, VALID_ROW), None)
    assert projects == [dict(name="jq", url="https://github.com/jqlang/jq.git",
                             category="community", file_filter=r"\.[ch]$",
                             size_class="S")]


def test_manifest_skips_comment_lines(tmp_path):
    path = write_manifest(tmp_path, "# name  url  category  filter  class", VALID_ROW)
    assert [p["name"] for p in ctp.read_manifest(path, None)] == ["jq"]


def test_manifest_skips_blank_and_whitespace_only_lines(tmp_path):
    path = write_manifest(tmp_path, "", "   ", VALID_ROW, "\t", "")
    assert len(ctp.read_manifest(path, None)) == 1


@pytest.mark.parametrize("row, fields", [
    pytest.param("jq\thttps://x.git\tcommunity\t\\.[ch]$", 4, id="four-fields"),
    pytest.param("jq\thttps://x.git\tcommunity\t\\.[ch]$\tS\textra", 6, id="six-fields"),
])
def test_manifest_rejects_a_row_without_exactly_five_fields(tmp_path, row, fields):
    """A short or long row is a corrupt manifest that would mislabel projects."""
    with pytest.raises(ValueError):
        ctp.read_manifest(write_manifest(tmp_path, row), None)


def test_manifest_error_names_the_offending_line(tmp_path):
    path = write_manifest(tmp_path, VALID_ROW, "zstd\thttps://z.git\tenterprise\t\\.[ch]$")
    with pytest.raises(ValueError) as exc:
        ctp.read_manifest(path, None)
    assert "zstd" in str(exc.value)


def test_manifest_rejects_a_bad_size_class(tmp_path):
    row = "jq\thttps://x.git\tcommunity\t\\.[ch]$\tXL"
    with pytest.raises(ValueError):
        ctp.read_manifest(write_manifest(tmp_path, row), None)


def test_manifest_empty_returns_no_projects(tmp_path):
    assert ctp.read_manifest(write_manifest(tmp_path), None) == []


def test_manifest_only_filter_keeps_just_the_named_projects(tmp_path):
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


def test_second_concurrent_run_of_the_same_project_is_deferred(sandbox, runner, jq, held_lock):
    """Locking contract: one project, one worker. Two tokenizers in the same
    workdir corrupt blobExec's incremental state."""
    (sandbox.out / "jq").mkdir()
    with held_lock(ctp.lock_path("jq")):
        assert ctp.run_project(jq) == "deferred"
    assert runner.calls == []


@pytest.mark.parametrize("runner_kwargs, outcome", [
    pytest.param({}, "done", id="success"),
    pytest.param(dict(pipeline_rc=2), "failed", id="failure"),
    pytest.param(dict(raise_on="pipeline"), "failed", id="popen-raises"),
])
def test_the_lock_is_released_after_every_outcome(monkeypatch, jq, runner_kwargs, outcome):
    """A leaked lock makes the next run report the project as RUNNING for ever."""
    monkeypatch.setattr(ctp.subprocess, "Popen", FakeRunner(**runner_kwargs))
    assert ctp.run_project(jq) == outcome
    assert lock_is_free(ctp.lock_path("jq"))


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
    pytest.param(dict(project_meta="/somewhere/project_meta.json"),
                 {"--project-meta": "/somewhere/project_meta.json", "--project-key": "jq"},
                 id="sidecar-and-its-key"),
    pytest.param(dict(mask_widened=True, from_step=2), {"--mask-widened": None},
                 id="mask-widened"),
    pytest.param(dict(retokenize="rs", from_step=2), {"--retokenize": "rs"}, id="retokenize"),
    pytest.param(dict(shards=4, shard_classes=("S", "M")),
                 {"--mode": "sharded", "--shards": "4"}, id="shards-for-a-listed-class"),
]

NOT_SENT = [
    pytest.param(dict(skip_html=False), ["--skip-html"], id="skip-html-off"),
    pytest.param(dict(gc=None), ["--gc"], id="no-gc"),
    pytest.param(dict(memo_dir=""), ["--memo-dir"], id="no-memo-dir"),
    pytest.param(dict(blame_jobs=0), ["--jobs"], id="no-blame-jobs"),
    pytest.param(dict(blame_jobs=8), ["--blame-jobs"], id="old-blame-jobs-name"),
    pytest.param(dict(memory_limit=None, duckdb_threads=0),
                 ["--memory-limit", "--duckdb-threads"], id="no-memory-flags"),
    pytest.param(dict(project_meta=""), ["--project-meta", "--project-key"], id="no-sidecar"),
    pytest.param(dict(from_step=2, mask_widened=False), ["--mask-widened"],
                 id="no-mask-widened"),
    pytest.param(dict(from_step=2, retokenize=""), ["--retokenize"], id="no-retokenize"),
    pytest.param(dict(from_step=1), ["1"], id="step-one-is-the-runner-default"),
    pytest.param(dict(shards=0, shard_classes=("L",)), ["--mode", "--shards"], id="no-shards"),
    pytest.param(dict(shards=1, shard_classes=("S",)), ["--mode"], id="one-shard"),
    pytest.param(dict(shards=6, shard_classes=("L",)), ["--mode"], id="class-not-listed"),
    pytest.param({}, ["--mask-widened", "--retokenize", "--mode"], id="keys-unset"),
]


@pytest.mark.parametrize("opts, sent", FORWARDED)
def test_an_option_reaches_the_runner_argv(runner, jq, opts, sent):
    ctp._OPTS.update({"skip_html": False, "drop_memo": False, **opts})
    ctp.run_project(jq)
    argv = runner.argv("pipeline")
    for flag, value in sent.items():
        assert argv.count(flag) == 1
        if value is not None:
            assert argv[argv.index(flag) + 1] == value


@pytest.mark.parametrize("opts, absent", NOT_SENT)
def test_an_option_left_off_sends_no_flag(runner, jq, opts, absent):
    ctp._OPTS.update({"skip_html": False, "drop_memo": False, **opts})
    ctp.run_project(jq)
    argv = runner.argv("pipeline")
    for flag in absent:
        assert flag not in argv


def test_an_unset_from_step_means_a_full_run(runner, jq):
    """run_project reads _OPTS directly, so a caller that never set it must
    get step 1, not a crash."""
    ctp._OPTS.update(skip_html=False, drop_memo=False)
    ctp.run_project(jq)
    assert runner.argv("pipeline")[-1] == "\\.[ch]$"


def test_from_step_is_appended_last_because_it_is_positional(runner, jq):
    """FROM_STEP is a positional argument. The runner reads it from the tail of
    argv, so it must follow every flag, including --skip-html."""
    ctp._OPTS.update(skip_html=True, drop_memo=False, from_step=3)
    ctp.run_project(jq)
    argv = runner.argv("pipeline")
    assert argv[-1] == "3"
    assert argv[-2] == "--skip-html"


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
    monkeypatch.setattr(ctp, "run_project", lambda p: ctp.RunOutcome.DONE)

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


@pytest.mark.parametrize("runner_kwargs", [
    pytest.param(dict(pipeline_rc=2), id="pipeline-fails"),
    pytest.param(dict(validate_rc=1), id="validation-fails"),
])
def test_drop_memo_does_not_prune_a_failed_project(monkeypatch, jq, runner_kwargs):
    """A failed project keeps memo/ so blobExec can resume incrementally."""
    monkeypatch.setattr(ctp.subprocess, "Popen", FakeRunner(**runner_kwargs))
    monkeypatch.setattr(ctp.retain, "prune", forbidden)
    ctp._OPTS.update(skip_html=False, drop_memo=True)
    assert ctp.run_project(jq) == "failed"


def test_drop_memo_reports_when_retain_refuses(monkeypatch, capsys, runner, jq):
    """retain.prune re-checks the keepers itself. A refusal must be visible,
    and must not turn a validated project into a failure: it is done-dirty."""
    monkeypatch.setattr(ctp.retain, "prune", lambda *a, **k: (0, False))
    ctp._OPTS.update(skip_html=False, drop_memo=True)
    outcome = ctp.run_project(jq)
    assert outcome == "done-dirty" and outcome.is_success
    assert "retain refused the prune" in capsys.readouterr().out


# --------------------------------------------------------------------------- #
# --memo-dir
# --------------------------------------------------------------------------- #

def test_memo_dir_is_forwarded_as_one_subdirectory_per_project(sandbox, runner, jq):
    """tokenBySha.pl keys the memo on the sha1 of the file contents alone, so a
    shared directory would serve one project's tokens to another."""
    memo_root = sandbox.root / "memos"
    memo_root.mkdir()
    ctp._OPTS.update(skip_html=False, drop_memo=False, memo_dir=str(memo_root))
    ctp.run_project(jq)

    argv = runner.argv("pipeline")
    assert argv[argv.index("--memo-dir") + 1] == str(memo_root / "jq")


def test_a_memo_dir_inside_the_work_directory_is_refused(sandbox, runner, jq):
    """A memo the runner's own wipe can reach is worse than no flag at all: the
    operator believes it is safe and it is not."""
    ctp._OPTS.update(skip_html=False, drop_memo=False,
                     memo_dir=str(sandbox.out / "jq" / "inner"))
    assert ctp.run_project(jq) == "failed"


# --------------------------------------------------------------------------- #
# argument construction
# --------------------------------------------------------------------------- #

def test_pipeline_argv_is_built_exactly_like_this(sandbox, runner, jq):
    """The runner takes --work and --mask; --work-dir or --file-filter exit 2."""
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
    sent = {a for a in runner.argv("pipeline") if a.startswith("--")}
    assert sent == set(ctp.REQUIRED_RUNNER_FLAGS), "the preflight constant drifted"


class WipingRunner(FakeRunner):
    """Fails the pipeline after `rm -rf "$WORK"`, as run_pipeline_process.sh
    does at FROM_STEP=1 and from its EXIT trap."""

    def __call__(self, args, **kwargs):
        if self.phase_of(args) == "pipeline":
            shutil.rmtree(Path(args[args.index("--work") + 1]))
        return super().__call__(args, **kwargs)


def test_the_log_and_the_lock_survive_the_runner_deleting_the_workdir(
        sandbox, monkeypatch, jq):
    monkeypatch.setattr(ctp.subprocess, "Popen", WipingRunner(pipeline_rc=2))
    assert ctp.run_project(jq) == "failed"

    assert not (sandbox.out / "jq").exists()
    assert list((ctp.state_dir("jq") / "logs").glob("pipeline-*.log"))
    assert ctp.lock_path("jq").exists()
    assert not ctp.lock_path("jq").is_relative_to(sandbox.out / "jq")


def test_status_reports_failed_after_the_runner_wiped_the_workdir(sandbox, monkeypatch, capsys, jq):
    """The workdir is gone, so only state_dir shows the project was attempted."""
    monkeypatch.setattr(ctp.subprocess, "Popen", WipingRunner(pipeline_rc=2))
    ctp.run_project(jq)

    write_manifest(sandbox.root, VALID_ROW)
    ctp.cmd_status(argparse.Namespace(manifest="manifest.tsv"))
    line = [l for l in capsys.readouterr().out.splitlines() if l.startswith("jq ")][0]
    assert "FAILED" in line


def test_validate_argv_is_built_exactly_like_this(sandbox, runner, jq):
    ctp.run_project(jq)
    assert runner.argv("validate") == [
        "python3", str(sandbox.root / "validate.py"),
        str(sandbox.out / "jq" / "jq-dataset.parquet"),
        str(sandbox.out / "jq" / "jq.validated"),
    ]


@pytest.mark.parametrize("opts, tail", [
    pytest.param({}, [], id="duckdb-defaults"),
    pytest.param(dict(memory_limit="4GB", duckdb_threads=2),
                 ["--memory-limit", "4GB", "--threads", "2"], id="runner-limits"),
])
def test_firm_argv_is_built_exactly_like_this(sandbox, runner, jq, opts, tail):
    ctp._OPTS.update(opts)
    ctp.run_project(jq)
    assert runner.argv("firm") == [
        "python3", str(sandbox.root / "firm_attribution.py"),
        str(sandbox.out / "jq" / "jq-dataset.parquet"), *tail,
    ]


def test_the_firm_phase_runs_between_pipeline_and_validate(sandbox, runner, jq):
    """validate gates the 70-column form, so it must see the file after firm."""
    assert ctp.run_project(jq) == "done"
    assert [c.phase for c in runner.calls] == ["pipeline", "firm", "validate"]


def test_a_failed_firm_phase_fails_the_project_unstamped(sandbox, monkeypatch, jq):
    runner = FakeRunner(firm_rc=1)
    monkeypatch.setattr(ctp.subprocess, "Popen", runner)
    assert ctp.run_project(jq) == "failed"
    assert [c.phase for c in runner.calls] == ["pipeline", "firm"]
    assert not (sandbox.out / "jq" / "jq.validated").exists()


# --------------------------------------------------------------------------- #
# metrics ledger
# --------------------------------------------------------------------------- #

def test_metrics_is_append_only_with_seven_fields_per_row(sandbox):
    """Rewriting metrics.tsv destroys the benchmark history."""
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


def test_metrics_row_carries_the_return_code_and_log_path(sandbox, runner, clock, jq):
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
    """The first log is the evidence of why the first attempt failed."""
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


def test_two_attempts_in_the_same_second_keep_both_logs(
        sandbox, monkeypatch, clock, jq):
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
    """A complete `ctp run` Namespace. allow_empty_provenance is True here, unlike
    the CLI, so tests of other flags do not hit the provenance refusal; the
    guard's own tests pass False."""
    base = dict(manifest="manifest.tsv", only=None, jobs=1, retries=0,
                skip_html=False, drop_memo=False, memo_dir="",
                shards=0, shard_classes="L",
                from_step=1, gc=None, blame_jobs=0,
                memory_limit=None, duckdb_threads=0, project_meta="",
                allow_empty_provenance=True,
                mask="", mask_widened=False, retokenize="", reblame=False)
    base.update(over)
    return argparse.Namespace(**base)


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
    pytest.param(lacks("--gc MODE"), dict(gc="none"), ["--gc is not implemented"],
                 id="runner-lacks-gc"),
    pytest.param(lacks("--skip-html"), dict(skip_html=True),
                 ["--skip-html is not implemented"], id="runner-lacks-skip-html"),
    pytest.param(lacks("--memo-dir DIR"), dict(memo_dir=memos), ["--memo-dir"],
                 id="runner-lacks-memo-dir"),
    pytest.param(lacks("--shards N"), dict(shards=6), ["--shards needs"],
                 id="runner-lacks-shards"),
    pytest.param(lacks("--jobs N"), dict(blame_jobs=8), ["--blame-jobs needs --jobs"],
                 id="runner-lacks-jobs"),
    pytest.param(lacks("--memory-limit SIZE"), dict(memory_limit="3GB"),
                 ["--memory-limit is not implemented"], id="runner-lacks-memory-limit"),
    pytest.param(lacks("--duckdb-threads N"), dict(duckdb_threads=2),
                 ["--duckdb-threads is not implemented"], id="runner-lacks-duckdb-threads"),
    pytest.param(lacks("--project-key NAME"), dict(project_meta=sidecar), ["--project-key"],
                 id="runner-lacks-project-key"),
    pytest.param(lacks("--mask-widened"), dict(mask_widened=True, from_step=2),
                 ["--mask-widened is not implemented"], id="runner-lacks-mask-widened"),
    pytest.param(lacks("--retokenize EXTS"), dict(retokenize="rs", from_step=2),
                 ["--retokenize is not implemented"], id="runner-lacks-retokenize"),
    pytest.param(None, dict(from_step=0), ["--from-step must be 1 or greater"],
                 id="from-step-zero"),
    pytest.param(None, dict(memo_dir=memos, drop_memo=True), ["contradict"],
                 id="memo-dir-with-drop-memo"),
    pytest.param(None, dict(memo_dir=lambda root: str(root / "absent")),
                 ["not an existing directory"], id="memo-dir-absent"),
    pytest.param(None, dict(blame_jobs=-1), ["--blame-jobs cannot be negative"],
                 id="negative-blame-jobs"),
    pytest.param(None, dict(duckdb_threads=-1), ["--duckdb-threads cannot be negative"],
                 id="negative-duckdb-threads"),
    # A percentage measures total RAM, and only the free part is usable.
    pytest.param(None, dict(memory_limit="80%"), ["--memory-limit", "not a percentage"],
                 id="memory-limit-percentage"),
    pytest.param(None, dict(project_meta=lambda root: str(root / "absent.json")),
                 ["does not exist", "validate_schema.py"], id="sidecar-absent"),
    # Step 1 deletes the blob map that both flags work on.
    pytest.param(None, dict(mask_widened=True, from_step=1), ["--from-step 2"],
                 id="mask-widened-at-step-1"),
    pytest.param(None, dict(mask_widened=True, from_step=2, shards=4),
                 ["no recorded mask to widen"], id="mask-widened-with-shards"),
    # Step 3 and later skip step 2, so the tokens would never be remade.
    *(pytest.param(None, dict(retokenize="rs", from_step=step), ["--from-step 2 exactly"],
                   id=f"retokenize-at-step-{step}") for step in (1, 3, 7)),
    pytest.param(None, dict(retokenize="rs", from_step=2, shards=4),
                 ["no cached tokenizations to invalidate"], id="retokenize-with-shards"),
    pytest.param(None, dict(retokenize="rs", mask_widened=True, from_step=2),
                 ["cannot be used in the same run"], id="retokenize-with-mask-widened"),
    pytest.param(None, dict(allow_empty_provenance=False, from_step=10),
                 ["reaches step 10"], id="provenance-gap-at-step-10"),
]


@pytest.mark.parametrize("usage, over, said", REFUSALS)
def test_run_refuses_before_anything_starts(must_not_start, runner_script, usage, over, said):
    if usage is not None:
        runner_script(usage)
    over = {k: v(must_not_start.root) if callable(v) else v for k, v in over.items()}
    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(run_args(**over))
    for text in said:
        assert text in str(exc.value)
    assert not (must_not_start.root / "runs.log").exists()


def accepted(over, opts=None, said=(), unsaid=(), ram=None, *, id):
    return pytest.param(over, opts or {}, said, unsaid, ram, id=id)


GIB = 1024 ** 3

# run_args overrides, the _OPTS they must produce, and what the log must and
# must not say. ram patches available_bytes for the memory budget check.
ACCEPTED = [
    accepted(dict(gc="plain"), dict(gc="plain"), id="gc"),
    accepted(dict(skip_html=True), dict(skip_html=True), id="skip-html"),
    accepted(dict(memo_dir=memos), dict(memo_dir=lambda root: str(root / "memos")),
             id="memo-dir"),
    accepted(dict(blame_jobs=12), dict(blame_jobs=12), id="blame-jobs"),
    accepted(dict(memory_limit="3GB", duckdb_threads=2, jobs=2),
             dict(memory_limit="3GB", duckdb_threads=2), unsaid=["memory budget"],
             ram=32 * GIB, id="memory-flags-that-fit"),
    accepted(dict(memory_limit="8GB", jobs=2), said=["WARNING: memory budget"],
             ram=6 * GIB, id="memory-budget-that-does-not-fit"),
    accepted(dict(project_meta=sidecar, allow_empty_provenance=False),
             dict(project_meta=lambda root: str(root / "project_meta.json")),
             unsaid=["--allow-empty-provenance"], id="sidecar-needs-no-escape-hatch"),
    accepted(dict(shards=2, shard_classes=" L , M ,"), dict(shard_classes=("L", "M")),
             id="shard-classes-parsed"),
    accepted(dict(shards=6, shard_classes="L,M"), said=["sharding 6-way", "L, M"],
             id="shard-plan-announced"),
    accepted(dict(from_step=3), said=["resuming at step 3"], id="resume-announced"),
    accepted(dict(mask_widened=True, from_step=2),
             said=["REUSE", "identity rows are discarded", "raw source"],
             id="mask-widened-announced"),
    accepted(dict(from_step=2), unsaid=["--mask-widened"], id="no-mask-widened-no-notice"),
    # The sidecar records the manifest's mask, so an override makes it wrong.
    accepted(dict(mask=r"\.rs$"),
             said=["--mask overrides the manifest", r"\.rs$", "will not match"],
             id="mask-override-warned"),
    accepted(dict(retokenize="rs", from_step=2), said=["DISCARD", "rs"],
             id="retokenize-announced"),
    accepted(dict(retokenize="   ", from_step=1), id="blank-retokenize-is-absent"),
    accepted(dict(allow_empty_provenance=True), dict(project_meta=""),
             said=["WARNING: --allow-empty-provenance", "--project-meta absent",
                   "stratum"], id="escape-hatch-itemised"),
    # Step 10 is the last step, so --from-step 11 can publish no Parquet.
    accepted(dict(allow_empty_provenance=False, from_step=11), unsaid=["refusing"],
             id="past-step-10-not-gated"),
]


@pytest.mark.parametrize("over, opts, said, unsaid, ram", ACCEPTED)
def test_run_accepts_and_announces(ready, monkeypatch, capsys, over, opts, said, unsaid, ram):
    if ram is not None:
        monkeypatch.setattr(ctp, "available_bytes", lambda: ram)

    def resolve(value):
        return value(ready.root) if callable(value) else value

    assert ctp.cmd_run(run_args(**{k: resolve(v) for k, v in over.items()})) == 0
    for key, value in opts.items():
        assert ctp._OPTS[key] == resolve(value)
    out = capsys.readouterr().out
    for text in said:
        assert text in out
    for text in unsaid:
        assert text not in out


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
    monkeypatch.setattr(ctp, "run_project", lambda p: ctp.RunOutcome.DONE)

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
    monkeypatch.setattr(ctp, "run_project", lambda p: ctp.RunOutcome.FAILED)
    assert ctp.cmd_run(run_args()) == 1
    assert "run-end\trc=1" in (sandbox.root / "runs.log").read_text()


def test_cmd_run_starts_no_retry_pass_once_every_project_is_done(ready, capsys):
    assert ctp.cmd_run(run_args(retries=3)) == 0
    assert "retry pass" not in capsys.readouterr().out


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
            return ctp.RunOutcome.FAILED
        return ctp.RunOutcome.DONE

    monkeypatch.setattr(ctp, "run_project", flaky)
    assert ctp.cmd_run(run_args(retries=1)) == 0
    assert seen == ["jq", "zstd", "zstd"]
    assert "retry pass 1: ['zstd']" in capsys.readouterr().out


# --------------------------------------------------------------------------- #
# heartbeat
# --------------------------------------------------------------------------- #

def beat_once(results: dict) -> str:
    stop = threading.Event()
    lines: list[str] = []
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(ctp, "HEARTBEAT_S", 0)
        mp.setattr(ctp, "say", lambda msg: (lines.append(msg), stop.set()))
        ctp.heartbeat(stop, results)
    assert len(lines) == 1
    return lines[0]


def test_heartbeat_reports_running_projects_done_failed_and_disk():
    ctp._live["jq"] = ("pipeline", time.time())
    line = beat_once({"jq": ctp.RunOutcome.DONE, "zstd": ctp.RunOutcome.FAILED,
                      "tmux": ctp.RunOutcome.SKIPPED})
    assert "jq(pipeline" in line
    assert "done 2 failed 1" in line
    assert "G free" in line


def test_heartbeat_shows_a_dash_when_nothing_is_running():
    assert "running: —" in beat_once({})


def test_heartbeat_returns_immediately_when_already_stopped():
    """cmd_run sets the event in a finally block; the thread must exit."""
    stop = threading.Event()
    stop.set()
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(ctp, "say", forbidden)
        ctp.heartbeat(stop, {})


# --------------------------------------------------------------------------- #
# cmd_status
# --------------------------------------------------------------------------- #

def test_cmd_status_classifies_every_project_state(sandbox, capsys, held_lock):
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


def test_cmd_status_works_before_the_output_dir_exists(sandbox, monkeypatch, capsys):
    write_manifest(sandbox.root, VALID_ROW)
    monkeypatch.setattr(ctp, "OUT", sandbox.out / "not-yet")
    assert ctp.cmd_status(argparse.Namespace(manifest="manifest.tsv")) == 0
    assert "G free" in capsys.readouterr().out


def test_cmd_status_works_without_a_metrics_file(sandbox, capsys):
    """A fresh checkout has no ledger yet; status must not crash."""
    write_manifest(sandbox.root, VALID_ROW)
    assert ctp.cmd_status(argparse.Namespace(manifest="manifest.tsv")) == 0
    assert "—" in capsys.readouterr().out


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


# --------------------------------------------------------------------------- #
# step 10 memory budget
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
    """A percentage measures total RAM; only the free part is usable."""
    with pytest.raises(ValueError) as exc:
        ctp.size_to_bytes("80%")
    assert "not a percentage" in str(exc.value)


@pytest.mark.parametrize("bad", ["3gigs", "", "GB", "3 4GB", "-3GB", "3PB"])
def test_size_to_bytes_refuses_anything_it_cannot_read(bad):
    """Guessing at a typo would cap the heap at the wrong number silently."""
    with pytest.raises(ValueError) as exc:
        ctp.size_to_bytes(bad)
    assert "cannot read" in str(exc.value)


@pytest.mark.parametrize("meminfo, want", [
    # MemAvailable, not MemFree: the reclaimable page cache is usable.
    pytest.param("MemTotal:       31000000 kB\nMemFree:          900000 kB\n"
                 "MemAvailable:    5500000 kB\n", 5500000 * 1024, id="mem-available"),
    pytest.param("MemTotal:       31000000 kB\n", None, id="key-absent"),
    pytest.param("MemAvailable:    not-a-number kB\n", None, id="unparsable"),
    pytest.param("MemAvailable:\n", None, id="truncated"),
    pytest.param(None, None, id="no-proc"),
])
def test_available_bytes_reads_mem_available_or_says_none(tmp_path, monkeypatch, meminfo, want):
    """The budget warning is advisory, so an odd /proc must not raise."""
    path = tmp_path / "meminfo"
    if meminfo is not None:
        path.write_text(meminfo)
    monkeypatch.setattr(ctp, "Path", lambda _p: path)
    assert ctp.available_bytes() == want


def test_memory_budget_warning_names_the_shortfall(monkeypatch):
    """2 x 8GB at the 1.4 settle ratio against 6 GiB free."""
    monkeypatch.setattr(ctp, "available_bytes", lambda: 6 * 1024 ** 3)
    warning = ctp.memory_budget_warning("8GB", 2)
    assert warning is not None
    assert "2 concurrent x 8GB" in warning
    assert "22.4 GiB" in warning, "1.4 x 2 x 8GB, the measured settle ratio"
    assert "6.0 GiB" in warning


@pytest.mark.parametrize("free, limit, jobs, warns", [
    pytest.param(8 * GIB, "3GB", 1, False, id="1.4x1x3GB-fits-8GiB"),
    pytest.param(None, "8GB", 4, False, id="ram-unknown"),
    pytest.param(10 * GIB, "3GB", 2, False, id="1.4x2x3GB-fits-10GiB"),
    pytest.param(10 * GIB, "3GB", 3, True, id="1.4x3x3GB-exceeds-10GiB"),
])
def test_memory_budget_warning_multiplies_by_jobs(monkeypatch, free, limit, jobs, warns):
    monkeypatch.setattr(ctp, "available_bytes", lambda: free)
    assert (ctp.memory_budget_warning(limit, jobs) is not None) is warns


# --------------------------------------------------------------------------- #
# --project-meta
# --------------------------------------------------------------------------- #

def test_a_relative_sidecar_path_is_made_absolute(
        sandbox, monkeypatch, runner_script):
    """The runner starts with cwd=CREGIT, but the path is typed relative to
    this repository."""
    runner_script()
    write_manifest(sandbox.root, VALID_ROW)
    meta = sandbox.root / "project_meta.json"
    meta.write_text("{}")
    monkeypatch.chdir(sandbox.root)
    monkeypatch.setattr(ctp, "capture_devenv_env", lambda: {"PATH": "/x"})
    monkeypatch.setattr(ctp, "run_project", lambda p: ctp.RunOutcome.DONE)

    assert ctp.cmd_run(run_args(project_meta="project_meta.json",
                                allow_empty_provenance=False)) == 0
    assert Path(ctp._OPTS["project_meta"]).is_absolute()
    assert Path(ctp._OPTS["project_meta"]) == meta.resolve()


# --------------------------------------------------------------------------- #
# the provenance guard
# --------------------------------------------------------------------------- #

def test_a_run_without_the_sidecar_is_refused_and_names_the_columns(must_not_start):
    """"provenance" alone does not say what is blank, and a refusal that does not
    name its escape hatch sends the operator to read the source."""
    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(run_args(allow_empty_provenance=False))
    message = str(exc.value)
    for text in ("refusing this run", "--project-meta", "--allow-empty-provenance",
                 "stratum", "history_cluster", "29", "validate.py passes it"):
        assert text in message
    assert "firm" not in message
    assert not (must_not_start.root / "runs.log").exists()


def test_the_escape_hatch_is_off_until_it_is_typed(monkeypatch):
    """It must never arrive by default."""
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
    """Left in a launcher script, the hatch would silence the guard on the next
    run that does omit a flag."""
    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(run_args(allow_empty_provenance=True,
                             project_meta=sidecar(must_not_start.root)))
    assert "nothing to allow" in str(exc.value)
    assert "wrapper" in str(exc.value)


def test_a_sidecar_path_that_is_not_a_file_is_refused_by_its_own_check(must_not_start):
    """The sharper "does not exist" message must win over the guard's, which
    would send the operator looking for a flag they did pass."""
    typo = str(must_not_start.root / "typo")
    with pytest.raises(SystemExit) as exc:
        ctp.cmd_run(run_args(allow_empty_provenance=False, project_meta=typo))
    message = str(exc.value)
    assert "--project-meta" in message and "does not exist" in message
    assert "refusing this run" not in message


@pytest.mark.parametrize("flag", ["--project-key", "--firm-map", "--firm-canonical"])
def test_ctp_run_refuses_a_flag_it_does_not_own(monkeypatch, capsys, flag):
    """run_project derives --project-key from the manifest name. cregit master
    has no firm columns, so ctp takes no firm flags."""
    monkeypatch.setattr(ctp.sys, "argv", ["ctp.py", "run", flag, "x"])
    monkeypatch.setattr(ctp, "cmd_run", forbidden)
    with pytest.raises(SystemExit) as exc:
        ctp.main()
    assert exc.value.code == 2
    assert "unrecognized arguments" in capsys.readouterr().err


# --------------------------------------------------------------------------- #
# cmd_progress
# --------------------------------------------------------------------------- #

def progress_args(**over):
    base = dict(manifest="manifest.tsv", last=5)
    base.update(over)
    return argparse.Namespace(**base)


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


def test_finished_runs_keeps_only_successful_pipeline_rows(sandbox):
    """A validate row is not a run, and rc!=0 is not a finish."""
    write_metrics(sandbox.root,
                  ("a", "S", "pipeline", 100, 0),
                  ("a", "S", "validate", 0, 0),
                  ("b", "M", "pipeline", 200, 1),
                  ("c", "M", "pipeline", 300, 0))
    assert ctp.finished_runs() == [("a", "S", 100), ("c", "M", 300)]


def test_progress_prints_a_bar_and_the_class_breakdown(sandbox, capsys):
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


def test_progress_lists_the_running_and_counts_the_failed(sandbox, capsys, held_lock):
    write_manifest(sandbox.root, "a\thttps://x/a.git\tcommunity\t\\.c$\tS",
                   "b\thttps://x/b.git\tcommunity\t\\.c$\tS")
    ctp.state_dir("b").mkdir(parents=True)
    with held_lock(ctp.lock_path("a")):
        assert ctp.cmd_progress(progress_args()) == 0
    out = capsys.readouterr().out
    assert "FAILED 1" in out
    assert "\nrunning\n  a " in out


def test_progress_lists_the_last_finished_newest_first(sandbox, capsys):
    write_manifest(sandbox.root, "a\thttps://x/a.git\tcommunity\t\\.c$\tS")
    write_metrics(sandbox.root,
                  ("one", "S", "pipeline", 60, 0),
                  ("two", "S", "pipeline", 120, 0),
                  ("three", "S", "pipeline", 60, 0))
    assert ctp.cmd_progress(progress_args(last=2)) == 0
    out = capsys.readouterr().out
    assert out.index("three") < out.index("two")
    assert "2.0 min" in out
    assert "one" not in out


def test_progress_survives_an_empty_manifest(sandbox, capsys):
    """--only can match nothing, and a crash here would be a poor status tool."""
    write_manifest(sandbox.root)

    assert ctp.cmd_progress(progress_args()) == 0
    out = capsys.readouterr().out
    assert "0/0" in out
    assert "0.0%" in out


# --------------------------------------------------------------------------- #
# the universal mask and --mask
# --------------------------------------------------------------------------- #

def test_a_blank_file_filter_column_falls_back_to_the_universal_mask(tmp_path):
    """blobExec rejects an empty mask, so a blank column means the universal one."""
    row = "jq\thttps://github.com/jqlang/jq.git\tcommunity\t\tS"
    projects = ctp.read_manifest(write_manifest(tmp_path, row), None)
    assert projects[0]["file_filter"] == UNIVERSAL_MASK


def test_the_manifests_mask_is_what_reaches_the_runner(sandbox, runner, jq):
    """The Parquet's file_mask column must describe the mask that actually ran."""
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
# pinned commit
# --------------------------------------------------------------------------- #

@pytest.fixture
def pinned_jq(jq):
    return {**jq, "commit": PINNED_SHA}


def test_manifest_reads_a_sixth_column_as_the_pinned_commit(tmp_path):
    [row] = ctp.read_manifest(write_manifest(tmp_path, PINNED_ROW), None)
    assert row["commit"] == PINNED_SHA


def test_manifest_without_the_column_is_unpinned(tmp_path):
    [row] = ctp.read_manifest(write_manifest(tmp_path, VALID_ROW), None)
    assert "commit" not in row


@pytest.mark.parametrize("commit", ["abc123", PINNED_SHA.upper(), PINNED_SHA + "0", ""])
def test_manifest_rejects_a_commit_that_is_not_a_full_sha(tmp_path, commit):
    with pytest.raises(ValueError, match="40-character"):
        ctp.read_manifest(write_manifest(tmp_path, f"{VALID_ROW}\t{commit}"), None)


def test_manifest_rejects_seven_fields(tmp_path):
    with pytest.raises(ValueError, match="pinned commit"):
        ctp.read_manifest(write_manifest(tmp_path, f"{PINNED_ROW}\textra"), None)


def test_a_pinned_project_clones_first_then_runs_from_the_staging_clone(
        sandbox, runner, pinned_jq):
    assert ctp.run_project(pinned_jq) == ctp.RunOutcome.DONE
    assert [c.phase for c in runner.calls] == ["clone", "pipeline", "git", "firm", "validate"]
    staging = sandbox.out / ".ctp-pinned" / "jq.git"
    assert runner.argv("clone") == [
        "python3", str(sandbox.root / "pin.py"),
        "--url", "https://github.com/jqlang/jq.git", "--commit", PINNED_SHA,
        "--dest", str(staging), "--result", str(ctp.pin_result_path("jq"))]
    argv = runner.argv("pipeline")
    assert argv[argv.index("--repo-url") + 1] == str(staging)
    assert argv[argv.index("--commit-url") + 1] == "https://github.com/jqlang/jq/commit/"


def test_an_unpinned_project_clones_the_url_itself(runner, jq):
    assert ctp.run_project(jq) == ctp.RunOutcome.DONE
    assert [c.phase for c in runner.calls] == ["pipeline", "firm", "validate"]
    argv = runner.argv("pipeline")
    assert argv[argv.index("--repo-url") + 1] == jq["url"]
    assert "--commit-url" not in argv


def test_a_failed_clone_stops_before_the_pipeline(monkeypatch, pinned_jq):
    r = FakeRunner(rcs={"clone": 3})
    monkeypatch.setattr(ctp.subprocess, "Popen", r)
    assert ctp.run_project(pinned_jq) == ctp.RunOutcome.FAILED
    assert [c.phase for c in r.calls] == ["clone"]


def test_a_checkout_that_differs_from_the_pin_fails_the_project(
        monkeypatch, capsys, pinned_jq):
    r = FakeRunner(checkout=OTHER_SHA)
    monkeypatch.setattr(ctp.subprocess, "Popen", r)
    monkeypatch.setattr(ctp.subprocess, "run", r)
    assert ctp.run_project(pinned_jq) == ctp.RunOutcome.FAILED
    assert "validate" not in [c.phase for c in r.calls]
    assert f"but the runner checked out {OTHER_SHA}" in capsys.readouterr().out


def test_an_unreadable_checkout_fails_a_pinned_project(monkeypatch, pinned_jq):
    monkeypatch.setattr(ctp.subprocess, "Popen", FakeRunner())
    monkeypatch.setattr(ctp, "runner_checkout", lambda name, workdir: "")
    assert ctp.run_project(pinned_jq) == ctp.RunOutcome.FAILED


def test_the_staging_clone_is_removed_once_the_project_validates(sandbox, runner, pinned_jq):
    staging = ctp.pinned_clone_path("jq")
    staging.mkdir(parents=True)
    (staging / "HEAD").write_text("ref: refs/heads/master\n")
    assert ctp.run_project(pinned_jq) == ctp.RunOutcome.DONE
    assert not staging.exists()


def test_a_symlinked_staging_clone_is_not_followed(sandbox, tmp_path):
    victim = tmp_path / "victim"
    victim.mkdir()
    (victim / "keep").write_text("x")
    staging = ctp.pinned_clone_path("jq")
    staging.parent.mkdir(parents=True)
    staging.symlink_to(victim)
    assert ctp.drop_pinned_clone("jq")
    assert (victim / "keep").exists()


def test_git_head_is_empty_for_a_missing_repo(tmp_path):
    assert ctp.git_head(tmp_path / "absent") == ""


@pytest.mark.parametrize("result, expected", [
    (SimpleNamespace(returncode=0, stdout=f"{PINNED_SHA}\n"), PINNED_SHA),
    (SimpleNamespace(returncode=0, stdout="not a sha\n"), ""),
    (SimpleNamespace(returncode=128, stdout=f"{PINNED_SHA}\n"), ""),
])
def test_git_head_returns_only_a_full_sha(monkeypatch, tmp_path, result, expected):
    monkeypatch.setattr(ctp.subprocess, "run", lambda *a, **k: result)
    assert ctp.git_head(tmp_path) == expected


def test_git_head_is_empty_when_git_cannot_start(monkeypatch, tmp_path):
    def boom(*a, **k):
        raise OSError("no git")
    monkeypatch.setattr(ctp.subprocess, "run", boom)
    assert ctp.git_head(tmp_path) == ""


def test_a_pinned_manifest_needs_a_runner_with_commit_url(must_not_start, runner_script):
    runner_script(REAL_RUNNER_USAGE.replace("--commit-url", "--commit-link"))
    write_manifest(must_not_start.root, PINNED_ROW)
    with pytest.raises(SystemExit, match="--commit-url"):
        ctp.cmd_run(run_args())


def test_a_pinned_manifest_runs_on_a_runner_with_commit_url(ready):
    write_manifest(ready.root, PINNED_ROW)
    assert ctp.cmd_run(run_args()) == 0


@pytest.mark.parametrize("url, expected", [
    ("https://github.com/jqlang/jq.git", "https://github.com/jqlang/jq/commit/"),
    ("https://gitlab.com/a/b/", "https://gitlab.com/a/b/commit/"),
    ("https://example.org/r", "https://example.org/r/commit/"),
])
def test_commit_url_matches_the_runner_default(url, expected):
    assert ctp.commit_url(url) == expected



# --------------------------------------------------------------------------- #
# audit ledger
# --------------------------------------------------------------------------- #

def ledger_rows(sandbox, project=None, step=None, started=False):
    """The ledger's rows, without the started rows unless started=True."""
    rows = ledger.read(sandbox.root / "ledger.jsonl")
    return [r for r in rows if (project is None or r.get("project") == project)
            and (step is None or r.get("step") == step)
            and (started or r.get("status") != "started")]


def test_an_internal_error_still_writes_the_final_row(monkeypatch, sandbox, runner, jq):
    def boom(ctx):
        raise KeyError("surprise")
    monkeypatch.setattr(ctp, "cleanup_step", boom)
    assert ctp.run_project(jq) == ctp.RunOutcome.FAILED
    [final] = ledger_rows(sandbox, "jq", "project")
    assert final["failed_step"] == "internal" and "KeyError" in final["detail"]


def test_each_step_writes_a_started_row_before_it_runs(sandbox, runner, jq):
    ctp.run_project(jq)
    rows = ledger_rows(sandbox, "jq", started=True)
    assert [(r["step"], r["status"]) for r in rows] == [
        ("pipeline", "started"), ("pipeline", "ok"), ("firm", "started"), ("firm", "ok"),
        ("validate", "started"), ("validate", "ok"), ("cleanup", "ok"), ("project", "done")]
    assert rows[0]["argv"][0] == "./run_pipeline_process.sh"


def fake_parquet(sandbox, name="jq", data=b"PAR1 fake parquet PAR1") -> Path:
    path = sandbox.out / name / f"{name}-dataset.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def test_a_done_project_writes_one_row_per_step_then_its_state(sandbox, runner, jq):
    fake_parquet(sandbox)
    ctp._RUN.update(run_id="r1", tools={"ctp_commit": "abc"}, flags={"jobs": 1})
    assert ctp.run_project(jq, queue_pos=3, queue_total=9) == ctp.RunOutcome.DONE
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
    ctp.run_project(jq)
    [row] = ledger_rows(sandbox, "jq", "validate")
    assert row["parquet"] == {"path": str(path), "bytes": len(data),
                              "sha256": hashlib.sha256(data).hexdigest(), "rows": 42}
    assert ledger_rows(sandbox, "jq", "project")[0]["parquet"] == row["parquet"]


def test_a_pinned_project_records_the_pin_and_the_checkout(sandbox, runner, pinned_jq):
    ctp.run_project(pinned_jq)
    rows = ledger_rows(sandbox, "jq")
    assert [r["step"] for r in rows] == [
        "clone", "pipeline", "firm", "validate", "cleanup", "project"]
    assert rows[0]["pin"]["checked_out_sha"] == PINNED_SHA
    assert all(r["pinned_sha"] == PINNED_SHA for r in rows)
    assert rows[0]["checked_out_sha"] == ""
    assert all(r["checked_out_sha"] == PINNED_SHA for r in rows[1:])
    assert rows[4]["cleanup"]["removed"] == [str(ctp.pinned_clone_path("jq"))]


def test_a_clone_without_a_result_file_fails(monkeypatch, sandbox, pinned_jq):
    monkeypatch.setattr(ctp.subprocess, "Popen", FakeRunner(pin_result=False))
    assert ctp.run_project(pinned_jq) == ctp.RunOutcome.FAILED
    [row] = ledger_rows(sandbox, "jq", "clone")
    assert row["status"] == "failed" and "no pin result" in row["detail"]


def test_a_checkout_mismatch_is_recorded_on_the_pipeline_row(monkeypatch, sandbox, pinned_jq):
    r = FakeRunner(checkout=OTHER_SHA)
    monkeypatch.setattr(ctp.subprocess, "Popen", r)
    monkeypatch.setattr(ctp.subprocess, "run", r)
    ctp.run_project(pinned_jq)
    [row] = ledger_rows(sandbox, "jq", "pipeline")
    assert row["status"] == "failed" and row["exit_code"] == 1
    assert row["checked_out_sha"] == OTHER_SHA and "not the pinned" in row["detail"]
    [final] = ledger_rows(sandbox, "jq", "project")
    assert final["state"] == "failed" and final["failed_step"] == "pipeline"


def test_a_failed_pipeline_names_the_failed_step(monkeypatch, sandbox, jq):
    monkeypatch.setattr(ctp.subprocess, "Popen", FakeRunner(pipeline_rc=2))
    assert ctp.run_project(jq) == ctp.RunOutcome.FAILED
    rows = ledger_rows(sandbox, "jq")
    assert [(r["step"], r["status"]) for r in rows] == [("pipeline", "failed"),
                                                        ("project", "failed")]
    assert rows[0]["exit_code"] == 2 and rows[1]["failed_step"] == "pipeline"


def test_a_phase_that_cannot_start_is_recorded(monkeypatch, sandbox, jq):
    monkeypatch.setattr(ctp.subprocess, "Popen", FakeRunner(raise_on="validate"))
    assert ctp.run_project(jq) == ctp.RunOutcome.FAILED
    [row] = ledger_rows(sandbox, "jq", "validate")
    assert row["status"] == "failed" and row["exit_code"] is None
    assert "could not start" in row["detail"]
    assert ledger_rows(sandbox, "jq", "project")[0]["failed_step"] == "validate"


def test_a_held_lock_is_recorded_as_deferred(sandbox, runner, jq, held_lock):
    with held_lock(ctp.lock_path("jq")):
        assert ctp.run_project(jq) == ctp.RunOutcome.DEFERRED
    [row] = ledger_rows(sandbox, "jq")
    assert row["state"] == "deferred" and "lock" in row["detail"]


def test_the_disk_floor_is_recorded_as_deferred(monkeypatch, sandbox, runner, jq):
    monkeypatch.setattr(ctp, "DISK_FLOOR_GB", 10**9)
    assert ctp.run_project(jq) == ctp.RunOutcome.DEFERRED
    assert ledger_rows(sandbox, "jq")[0]["detail"].startswith("disk below")


def test_a_refused_memo_dir_is_a_preflight_failure(sandbox, runner, jq):
    ctp._OPTS.update(memo_dir=str(sandbox.out))
    assert ctp.run_project(jq) == ctp.RunOutcome.FAILED
    [row] = ledger_rows(sandbox, "jq")
    assert row["failed_step"] == "preflight"


def test_a_refused_prune_makes_the_project_done_dirty(monkeypatch, sandbox, runner, jq):
    monkeypatch.setattr(ctp.retain, "prune", lambda *a, **k: (0, False))
    ctp._OPTS.update(drop_memo=True)
    assert ctp.run_project(jq) == ctp.RunOutcome.DONE_DIRTY
    [cleanup] = ledger_rows(sandbox, "jq", "cleanup")
    assert cleanup["status"] == "failed" and cleanup["cleanup"]["errors"]
    assert ledger_rows(sandbox, "jq", "project")[0]["state"] == "done-dirty"


def test_a_staging_clone_that_cannot_be_removed_makes_it_done_dirty(
        monkeypatch, sandbox, runner, pinned_jq):
    monkeypatch.setattr(ctp, "drop_pinned_clone", lambda name: ["permission denied"])
    assert ctp.run_project(pinned_jq) == ctp.RunOutcome.DONE_DIRTY


def test_done_dirty_is_a_success_that_a_retry_pass_does_not_rerun(monkeypatch, jq):
    calls = []

    def fake(project):
        calls.append(project["name"])
        return ctp.RunOutcome.DONE_DIRTY
    monkeypatch.setattr(ctp, "run_project", fake)
    results = ctp.run_passes([jq], jobs=1, retries=2)
    assert results == {"jq": ctp.RunOutcome.DONE_DIRTY} and calls == ["jq"]


def test_the_ledger_is_append_only_across_runs(sandbox, runner, jq):
    fake_parquet(sandbox)
    ctp.run_project(jq)
    path = sandbox.root / "ledger.jsonl"
    first, inode = path.read_bytes(), path.stat().st_ino
    (sandbox.out / "jq" / "jq.validated").unlink()
    ctp.run_project(jq)
    assert path.read_bytes().startswith(first) and len(path.read_bytes()) > len(first)
    assert path.stat().st_ino == inode


def test_a_failed_ledger_write_stops_the_run_loudly(monkeypatch, capsys, sandbox, runner, jq):
    def boom(path, row):
        raise OSError("disk full")
    monkeypatch.setattr(ctp.ledger, "append", boom)
    ctp.run_project(jq)
    assert "disk full" in ctp._RUN["ledger_broken"]
    assert "LEDGER WRITE FAILED" in capsys.readouterr().out


def test_each_phase_hands_the_project_lock_to_its_process(sandbox, runner, jq):
    ctp.run_project(jq)
    for call in runner.calls:
        [fd] = call.kwargs["pass_fds"]
        assert isinstance(fd, int)


def test_run_phase_without_kept_fds_passes_none(sandbox, runner, jq):
    ctp.run_phase(jq, "pipeline", ["./run_pipeline_process.sh"])
    assert "pass_fds" not in runner.calls[0].kwargs


def test_cmd_run_writes_run_start_and_run_end_rows(ready):
    assert ctp.cmd_run(run_args(jobs=2)) == 0
    rows = ledger.read(ready.root / "ledger.jsonl")
    assert [r["step"] for r in rows] == ["run-start", "run-end"]
    start, end = rows
    assert start["run_id"] == end["run_id"] == ctp._RUN["run_id"]
    assert start["tools"] == {"ctp_commit": "test"}
    flags = start["flags"]
    assert flags["cli"]["jobs"] == 2
    assert flags["manifest_path"] == str(ready.root / "manifest.tsv")
    assert flags["manifest_sha256"] == hashlib.sha256(
        (ready.root / "manifest.tsv").read_bytes()).hexdigest()
    assert end["exit_code"] == 0 and end["outcomes"] == {"done": 1}


def test_cmd_run_stops_when_the_ledger_cannot_be_written(ready, monkeypatch):
    def boom(path, row):
        raise OSError("read-only file system")
    monkeypatch.setattr(ctp.ledger, "append", boom)
    with pytest.raises(SystemExit, match="cannot write the ledger"):
        ctp.cmd_run(run_args())


def test_end_ledger_run_survives_a_failed_write(monkeypatch, capsys):
    def boom(path, row):
        raise OSError("gone")
    monkeypatch.setattr(ctp.ledger, "append", boom)
    ctp.end_ledger_run(0, time.time(), {})
    assert "cannot write the run-end row" in capsys.readouterr().out


def test_run_flags_records_every_option_and_the_manifest(sandbox):
    manifest = write_manifest(sandbox.root, VALID_ROW)
    ctp._OPTS.update(shard_classes=("L",), drop_memo=True)
    flags = ctp.run_flags(run_args(jobs=4), manifest)
    assert flags["effective"]["shard_classes"] == ["L"]
    assert flags["effective"]["drop_memo"] is True and flags["cli"]["drop_memo"] is False
    assert flags["cli"]["jobs"] == 4 and "fn" not in flags["cli"]
    assert flags["manifest_sha256"] == hashlib.sha256(manifest.read_bytes()).hexdigest()
    assert ctp.run_flags(run_args(), sandbox.root / "absent.tsv")["manifest_sha256"] == ""


def test_tool_versions_names_every_tool(monkeypatch, sandbox):
    outputs = {
        ("git", "-C", str(sandbox.root), "rev-parse", "HEAD"): "a" * 40,
        ("git", "-C", str(sandbox.root), "status", "--porcelain",
         "--untracked-files=no"): " M ctp.py",
        ("git", "-C", str(sandbox.cregit), "rev-parse", "HEAD"): "b" * 40,
        ("git", "-C", str(sandbox.cregit), "status", "--porcelain",
         "--untracked-files=no"): "",
        ("srcml", "--version"): "srcml: 1.1.0\nlibsrcml: 1.1.0",
        ("git", "--version"): "git version 2.55.0",
    }
    monkeypatch.setattr(ctp, "_capture", lambda cmd, cwd=None: outputs.get(tuple(cmd), ""))
    jar = sandbox.cregit / ctp.BLOBEXEC_JAR
    jar.parent.mkdir(parents=True)
    jar.write_bytes(b"jar bytes")
    tools = REAL_TOOL_VERSIONS()
    assert tools["ctp_commit"] == "a" * 40 + "-dirty"
    assert tools["cregit_commit"] == "b" * 40
    assert tools["srcml_version"] == "srcml: 1.1.0"
    assert tools["blobexec_jar_sha256"] == hashlib.sha256(b"jar bytes").hexdigest()
    assert tools["git_version"] == "git version 2.55.0"


def test_tool_versions_leaves_a_missing_tool_blank(monkeypatch, sandbox):
    monkeypatch.setattr(ctp, "_capture", lambda cmd, cwd=None: "")
    ctp._ENV.update(PATH=str(sandbox.root / "empty-bin"))
    tools = REAL_TOOL_VERSIONS()
    assert tools["ctp_commit"] == "" and tools["srcml_version"] == ""
    assert tools["srcml_path"] == "" and tools["blobexec_jar_sha256"] == ""


@pytest.mark.parametrize("result, expected", [
    (SimpleNamespace(returncode=0, stdout=" out \n"), "out"),
    (SimpleNamespace(returncode=1, stdout="out"), ""),
])
def test_capture_returns_stdout_only_on_success(monkeypatch, result, expected):
    monkeypatch.setattr(ctp.subprocess, "run", lambda *a, **k: result)
    assert ctp._capture(["x"]) == expected


def test_capture_is_blank_when_the_tool_is_missing(monkeypatch):
    def boom(*a, **k):
        raise FileNotFoundError("srcml")
    monkeypatch.setattr(ctp.subprocess, "run", boom)
    assert ctp._capture(["srcml"]) == ""


def test_audit_prints_one_line_per_step(sandbox, runner, pinned_jq, capsys):
    ctp.run_project(pinned_jq)
    capsys.readouterr()
    assert ctp.cmd_audit(argparse.Namespace(project="jq", json=False)) == 0
    out = capsys.readouterr().out.splitlines()
    assert [line.split()[1] for line in out] == [
        "clone", "pipeline", "firm", "validate", "cleanup", "project"]
    assert f"checked_out={PINNED_SHA}" in out[1]
    assert "state=done" in out[-1] and "removed=1 errors=0" in out[4]


def test_audit_json_prints_whole_rows(sandbox, runner, jq, capsys):
    fake_parquet(sandbox)
    ctp.run_project(jq)
    capsys.readouterr()
    ctp.cmd_audit(argparse.Namespace(project="jq", json=True))
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
    ctp.cmd_audit(argparse.Namespace(project="jq", json=False))
    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 2
    assert "pipeline  interrupted" in lines[1] and "the run died here" in lines[1]


def test_audit_of_an_unknown_project_fails(sandbox, capsys):
    assert ctp.cmd_audit(argparse.Namespace(project="nope", json=False)) == 1
    assert "no rows" in capsys.readouterr().err


def test_audit_line_shows_a_failure_and_its_detail():
    line = ctp.audit_line({"step": "project", "state": "failed", "failed_step": "clone",
                           "detail": "commit missing"})
    assert "state=failed" in line and "failed_step=clone" in line and "commit missing" in line


# --------------------------------------------------------------------------- #
# schema check and join
# --------------------------------------------------------------------------- #

@pytest.fixture
def joining(sandbox):
    manifest = write_manifest(sandbox.root, VALID_ROW)
    ctp._OPTS.update(join=True, manifest=str(manifest))
    return manifest


def test_join_runs_the_schema_check_then_the_join(sandbox, runner, jq, joining):
    assert ctp.run_project(jq) == ctp.RunOutcome.DONE
    assert [c.phase for c in runner.calls] == ["pipeline", "firm", "validate", "schema", "join"]
    parquet = str(sandbox.out / "jq" / "jq-dataset.parquet")
    assert runner.argv("schema") == ["python3", str(sandbox.root / "validate_schema.py"), parquet]
    assert runner.argv("join") == ["python3", str(sandbox.root / "consolidate.py"),
                                   "--join", "jq", "--manifest", str(joining)]
    steps = [r["step"] for r in ledger_rows(sandbox, "jq")]
    assert steps == ["pipeline", "firm", "validate", "schema", "join", "cleanup", "project"]


@pytest.mark.parametrize("step", ["schema", "join"])
def test_a_failed_schema_check_or_join_fails_the_project(monkeypatch, sandbox, jq, joining, step):
    r = FakeRunner(rcs={step: 1})
    monkeypatch.setattr(ctp.subprocess, "Popen", r)
    assert ctp.run_project(jq) == ctp.RunOutcome.FAILED
    [final] = ledger_rows(sandbox, "jq", "project")
    assert final["failed_step"] == step
    assert not ledger_rows(sandbox, "jq", "cleanup")
    if step == "schema":
        assert "join" not in [c.phase for c in r.calls]


def test_without_join_no_schema_or_join_step_runs(runner, jq):
    ctp.run_project(jq)
    assert [c.phase for c in runner.calls] == ["pipeline", "firm", "validate"]


def test_a_validated_project_resumes_at_the_steps_after_validation(
        sandbox, runner, jq, joining):
    fake_parquet(sandbox)
    (sandbox.out / "jq" / "jq.validated").write_text("rows=7\nbytes=9\n")
    assert ctp.run_project(jq, resume_validated=True) == ctp.RunOutcome.DONE
    assert [c.phase for c in runner.calls] == ["schema", "join"]
    [final] = ledger_rows(sandbox, "jq", "project")
    assert final["state"] == "done" and final["parquet"]["rows"] == 7


def test_a_validated_project_is_still_skipped_by_default(sandbox, runner, jq, joining):
    (sandbox.out / "jq").mkdir()
    (sandbox.out / "jq" / "jq.validated").write_text("rows=7\n")
    assert ctp.run_project(jq) == ctp.RunOutcome.SKIPPED
    assert runner.calls == [] and ledger_rows(sandbox) == []


def test_resuming_ignores_the_disk_floor(monkeypatch, sandbox, runner, jq, joining):
    """The steps after validation write almost nothing; the floor guards a new clone."""
    monkeypatch.setattr(ctp, "DISK_FLOOR_GB", 10**9)
    (sandbox.out / "jq").mkdir()
    (sandbox.out / "jq" / "jq.validated").write_text("rows=7\n")
    assert ctp.run_project(jq, resume_validated=True) == ctp.RunOutcome.DONE


def test_run_options_carry_join_and_the_absolute_manifest(sandbox, runner_script):
    runner_script()
    opts = ctp.run_options(run_args(join=True))
    assert opts["join"] is True
    assert opts["manifest"] == str((sandbox.root / "manifest.tsv").resolve())
    assert ctp.run_options(run_args())["join"] is False


def test_cmd_db_forwards_each_manifest_as_an_absolute_path(sandbox, monkeypatch):
    monkeypatch.setattr(ctp, "capture_devenv_env", lambda: {})
    seen = {}
    monkeypatch.setattr(ctp.subprocess, "run",
                        lambda args, **k: seen.update(args=args) or SimpleNamespace(returncode=0))
    assert ctp.cmd_db(argparse.Namespace(manifest=["a.tsv", "b.tsv"])) == 0
    assert seen["args"][2:] == ["--manifest", str(sandbox.root / "a.tsv"),
                                "--manifest", str(sandbox.root / "b.tsv")]


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
    ctp._OPTS.update(cleanup=True)
    assert ctp.run_project(jq) == ctp.RunOutcome.DONE
    assert sorted(p.name for p in workdir.iterdir()) == [
        "html", "jq-dataset.parquet", "jq.validated"]
    [row] = ledger_rows(sandbox, "jq", "cleanup")
    assert row["status"] == "ok"
    assert set(row["cleanup"]["removed"]) == {
        "jq-original.git", "jq-cregit", "memo", "blame", "jq-blobmap.db", "pipeline.log"}
    assert row["cleanup"]["bytes_freed"] > 0
    assert sorted(row["cleanup"]["kept"]) == ["html", "jq-dataset.parquet", "jq.validated"]
    # The logs and the ledger live outside the workdir and stay.
    assert list((sandbox.root / "state" / "jq" / "logs").glob("pipeline-*.log"))


def test_drop_html_removes_html_in_the_cleanup(sandbox, runner, jq):
    fake_parquet(sandbox)
    workdir = runner_leftovers(sandbox)
    ctp._OPTS.update(cleanup=True, drop_html=True)
    ctp.run_project(jq)
    assert not (workdir / "html").exists()


def test_a_failed_cleanup_makes_the_project_done_dirty(monkeypatch, sandbox, runner, jq):
    fake_parquet(sandbox)
    runner_leftovers(sandbox)
    ctp._OPTS.update(cleanup=True)
    monkeypatch.setattr(ctp.retain, "clean_workdir", lambda name, drop_html: {
        "removed": [], "kept": [], "errors": ["memo (Permission denied)"], "bytes_freed": 0})
    assert ctp.run_project(jq) == ctp.RunOutcome.DONE_DIRTY
    [row] = ledger_rows(sandbox, "jq", "cleanup")
    assert row["status"] == "failed" and row["cleanup"]["errors"] == ["memo (Permission denied)"]
    assert ledger_rows(sandbox, "jq", "project")[0]["state"] == "done-dirty"


def test_without_cleanup_the_work_files_stay(sandbox, runner, jq):
    fake_parquet(sandbox)
    workdir = runner_leftovers(sandbox)
    ctp.run_project(jq)
    assert (workdir / "memo").exists() and (workdir / "jq-original.git").exists()


@pytest.fixture
def anonymizer(sandbox):
    script = sandbox.root / "anonymize_parquet.py"
    script.write_text("# stand-in\n")
    ctp._OPTS.update(anonymize=str(script))
    return script


def test_anonymize_runs_after_the_schema_check_and_before_the_join(
        sandbox, runner, jq, joining, anonymizer):
    ctp.run_project(jq)
    assert [c.phase for c in runner.calls] == [
        "pipeline", "firm", "validate", "schema", "anonymize", "join"]
    workdir = sandbox.out / "jq"
    assert runner.argv("anonymize") == ["python3", str(anonymizer), str(workdir / "anon"),
                                        str(workdir / "jq-dataset.parquet")]
    [row] = ledger_rows(sandbox, "jq", "anonymize")
    [out] = row["anonymized"]
    assert out["path"] == str(workdir / "anon" / "jq-dataset.parquet")
    assert out["sha256"] == hashlib.sha256(b"PAR1 anon PAR1").hexdigest()


def test_anonymize_without_join_still_runs(sandbox, runner, jq, anonymizer):
    assert ctp.run_project(jq) == ctp.RunOutcome.DONE
    assert [c.phase for c in runner.calls] == ["pipeline", "firm", "validate", "anonymize"]


def test_an_anonymizer_that_writes_nothing_fails_the_project(
        monkeypatch, sandbox, jq, anonymizer):
    monkeypatch.setattr(ctp.subprocess, "Popen", FakeRunner(anon_output=False))
    assert ctp.run_project(jq) == ctp.RunOutcome.FAILED
    [row] = ledger_rows(sandbox, "jq", "anonymize")
    assert row["exit_code"] == 1 and "wrote no Parquet" in row["detail"]


def test_drop_html_needs_cleanup(must_not_start):
    with pytest.raises(SystemExit, match="--drop-html deletes html/ in the cleanup"):
        ctp.cmd_run(run_args(drop_html=True))


def test_anonymize_needs_an_existing_script(must_not_start):
    with pytest.raises(SystemExit, match="is not a file"):
        ctp.cmd_run(run_args(anonymize="/nonexistent/anon.py"))


def test_run_options_carry_cleanup_drop_html_and_the_anonymizer(sandbox, runner_script):
    runner_script()
    script = sandbox.root / "anon.py"
    script.write_text("")
    opts = ctp.run_options(run_args(cleanup=True, drop_html=True, anonymize=str(script)))
    assert opts["cleanup"] is True and opts["drop_html"] is True
    assert opts["anonymize"] == str(script.resolve())
    defaults = ctp.run_options(run_args())
    assert (defaults["cleanup"], defaults["drop_html"], defaults["anonymize"]) == (False, False, "")


def test_drop_html_and_anonymize_are_off_by_default_on_the_cli(monkeypatch):
    seen = {}
    monkeypatch.setattr(ctp, "cmd_run", lambda args: seen.update(vars(args)) or 0)
    monkeypatch.setattr(ctp.sys, "argv", ["ctp.py", "run"])
    ctp.main()
    assert seen["drop_html"] is False and seen["anonymize"] == "" and seen["cleanup"] is False


# --------------------------------------------------------------------------- #
# census: order, gates, stop, resume, retries
# --------------------------------------------------------------------------- #

def census_args(**over):
    base = vars(run_args())
    base.update(workers=1, min_free_mem_gb=0, disk_floor_gb=0, stop_file="", poll=0.01)
    base.pop("jobs")
    base.update(over)
    return argparse.Namespace(**base)


def manifest_rows(*names):
    return [f"{n}\thttps://example.org/{n}.git\tcommunity\t\\.[ch]$\tS\t{PINNED_SHA}"
            for n in names]


def projects_of(sandbox, *names):
    return ctp.read_manifest(write_manifest(sandbox.root, *manifest_rows(*names)), None)


class FakeProjects:
    """Stand-in for run_project: records each call, returns scripted outcomes,
    and can run a hook inside the call."""

    def __init__(self, outcomes=None, hook=None):
        self.calls: list[tuple] = []
        self.outcomes = {k: list(v) for k, v in (outcomes or {}).items()}
        self.hook = hook
        self.lock = threading.Lock()

    def __call__(self, project, pos=None, total=None, attempt=1, post=False):
        with self.lock:
            self.calls.append((project["name"], pos, total, attempt, post))
        if self.hook:
            self.hook(project["name"])
        queue = self.outcomes.get(project["name"])
        return queue.pop(0) if queue else ctp.RunOutcome.DONE

    def names(self):
        return [c[0] for c in self.calls]


@pytest.fixture
def fake_projects(monkeypatch):
    def install(**kw):
        fake = FakeProjects(**kw)
        monkeypatch.setattr(ctp, "run_project", fake)
        return fake
    return install


@pytest.fixture
def no_gates(monkeypatch):
    monkeypatch.setattr(ctp, "gate_reason", lambda opts: None)


def stamp(sandbox, name):
    (sandbox.out / name).mkdir(parents=True, exist_ok=True)
    (sandbox.out / name / f"{name}.validated").write_text("rows=1\n")


def history_row(name, state, **extra):
    return {"project": name, "step": "project", "state": state, **extra}


def test_census_runs_in_manifest_order_and_records_the_queue_position(
        sandbox, fake_projects, no_gates):
    fake = fake_projects()
    projects = projects_of(sandbox, "c", "a", "b")
    results = ctp.schedule(projects, 1, 0, sandbox.root / "STOP", poll=0.01)
    assert fake.calls == [("c", 1, 3, 1, False), ("a", 2, 3, 1, False), ("b", 3, 3, 1, False)]
    assert all(v == ctp.RunOutcome.DONE for v in results.values())


def test_census_skips_done_projects_and_resumes_the_rest(sandbox, fake_projects, no_gates):
    log = sandbox.root / "ledger.jsonl"
    stamp(sandbox, "a")
    ledger.append(log, history_row("a", "done"))
    ledger.append(log, history_row("b", "failed", failed_step="pipeline"))
    stamp(sandbox, "c")
    ledger.append(log, history_row("c", "done-dirty"))
    fake = fake_projects()
    results = ctp.schedule(projects_of(sandbox, "a", "b", "c", "d"), 1, 1,
                           sandbox.root / "STOP", poll=0.01)
    assert results["a"] == ctp.RunOutcome.SKIPPED
    # b failed once: its second attempt. c validated but is dirty: only the later steps.
    assert fake.calls == [("b", 2, 4, 2, False), ("c", 3, 4, 2, True), ("d", 4, 4, 1, False)]


def test_census_gives_up_after_the_retries_across_restarts(
        sandbox, fake_projects, no_gates, capsys):
    log = sandbox.root / "ledger.jsonl"
    for _ in range(2):
        ledger.append(log, history_row("a", "failed", failed_step="clone"))
    fake = fake_projects()
    results = ctp.schedule(projects_of(sandbox, "a"), 1, 1, sandbox.root / "STOP", poll=0.01)
    assert fake.calls == [] and results["a"] == ctp.RunOutcome.FAILED
    assert "failed 2 times (last at clone)" in capsys.readouterr().out


def test_census_retries_a_failure_within_the_run(sandbox, fake_projects, no_gates):
    fake = fake_projects(outcomes={"a": [ctp.RunOutcome.FAILED, ctp.RunOutcome.DONE]})
    results = ctp.schedule(projects_of(sandbox, "a", "b"), 1, 1, sandbox.root / "STOP",
                           poll=0.01)
    assert fake.names() == ["a", "b", "a"]
    assert fake.calls[2][3] == 2 and results["a"] == ctp.RunOutcome.DONE


def test_census_gives_up_on_a_project_that_keeps_killing_its_run(
        sandbox, fake_projects, no_gates, capsys):
    log = sandbox.root / "ledger.jsonl"
    for run in ("r1", "r2"):
        ledger.append(log, {"project": "a", "run_id": run, "attempt": 1,
                            "step": "pipeline", "status": "started"})
    fake = fake_projects()
    ctp.schedule(projects_of(sandbox, "a"), 1, 1, sandbox.root / "STOP", poll=0.01)
    assert fake.calls == []
    assert "failed 2 times (last at pipeline (interrupted))" in capsys.readouterr().out


def test_one_interrupted_run_is_retried(sandbox, fake_projects, no_gates):
    ledger.append(sandbox.root / "ledger.jsonl", {"project": "a", "run_id": "r1",
                                                  "attempt": 1, "step": "pipeline",
                                                  "status": "started"})
    fake = fake_projects()
    ctp.schedule(projects_of(sandbox, "a"), 1, 1, sandbox.root / "STOP", poll=0.01)
    assert fake.calls == [("a", 1, 1, 2, False)]


def test_census_without_retries_runs_a_failure_once(sandbox, fake_projects, no_gates):
    fake = fake_projects(outcomes={"a": [ctp.RunOutcome.FAILED] * 3})
    results = ctp.schedule(projects_of(sandbox, "a"), 1, 0, sandbox.root / "STOP", poll=0.01)
    assert fake.names() == ["a"] and results["a"] == ctp.RunOutcome.FAILED


def test_a_retry_after_validation_resumes_at_the_later_steps(sandbox, fake_projects, no_gates):
    def failed_after_validating(name):
        stamp(sandbox, name)
    fake = fake_projects(outcomes={"a": [ctp.RunOutcome.FAILED, ctp.RunOutcome.DONE]},
                         hook=failed_after_validating)
    ctp.schedule(projects_of(sandbox, "a"), 1, 1, sandbox.root / "STOP", poll=0.01)
    assert [c[4] for c in fake.calls] == [False, True]


def test_the_stop_file_lets_the_running_project_finish_and_starts_no_other(
        sandbox, fake_projects, no_gates, capsys):
    stop = sandbox.root / "STOP"
    fake = fake_projects(hook=lambda name: stop.touch())
    results = ctp.schedule(projects_of(sandbox, "a", "b", "c"), 1, 0, stop, poll=0.01)
    assert fake.names() == ["a"] and results == {"a": ctp.RunOutcome.DONE}
    assert f"{stop} exists" in ctp._RUN["stop_reason"]
    assert "No new project starts" in capsys.readouterr().out


def test_a_restart_after_a_stop_continues_with_the_next_project(sandbox, monkeypatch, no_gates):
    """The real run_project, so the ledger the second census reads is real."""
    monkeypatch.setattr(ctp.subprocess, "Popen", FakeRunner())
    monkeypatch.setattr(ctp.subprocess, "run", FakeRunner())
    stop = sandbox.root / "STOP"
    projects = projects_of(sandbox, "a", "b")
    real = ctp.run_project

    def run_then_stop(*a, **k):
        outcome = real(*a, **k)
        stop.touch()
        return outcome
    monkeypatch.setattr(ctp, "run_project", run_then_stop)
    first = ctp.schedule(projects, 1, 0, stop, poll=0.01)
    assert set(first) == {"a"}

    stop.unlink()
    ctp._RUN.clear()
    monkeypatch.setattr(ctp, "run_project", real)
    second = ctp.schedule(projects, 1, 0, stop, poll=0.01)
    assert second == {"a": ctp.RunOutcome.SKIPPED, "b": ctp.RunOutcome.DONE}
    finals = [(r["project"], r["state"]) for r in ledger_rows(sandbox, step="project")]
    assert finals == [("a", "done"), ("b", "done")]


def test_a_stop_signal_stops_dispatch(sandbox, fake_projects, no_gates):
    def signal_during(name):
        ctp._RUN["stop_reason"] = "signal SIGTERM"
        ctp._STOP.set()
    fake = fake_projects(hook=signal_during)
    try:
        ctp.schedule(projects_of(sandbox, "a", "b"), 1, 0, sandbox.root / "STOP", poll=0.01)
    finally:
        ctp._STOP.clear()
    assert fake.names() == ["a"] and ctp._RUN["stop_reason"] == "signal SIGTERM"


def test_a_broken_ledger_stops_dispatch(sandbox, fake_projects, no_gates):
    def break_ledger(name):
        ctp._RUN["ledger_broken"] = "OSError: disk full"
    fake = fake_projects(hook=break_ledger)
    ctp.schedule(projects_of(sandbox, "a", "b"), 1, 0, sandbox.root / "STOP", poll=0.01)
    assert fake.names() == ["a"] and "disk full" in ctp._RUN["stop_reason"]


def test_a_closed_gate_waits_then_starts(sandbox, fake_projects, monkeypatch, capsys):
    answers = iter(["memory 1.0G available, below the 8G floor"] * 3)
    monkeypatch.setattr(ctp, "gate_reason", lambda opts: next(answers, None))
    fake = fake_projects()
    ctp.schedule(projects_of(sandbox, "a"), 1, 0, sandbox.root / "STOP", poll=0.01)
    assert fake.names() == ["a"]
    out = capsys.readouterr().out
    assert out.count("waiting to start a: memory 1.0G available") == 1


def test_a_closed_gate_and_a_stop_end_the_census_without_starting(
        sandbox, fake_projects, monkeypatch):
    stop = sandbox.root / "STOP"
    calls = []

    def gate(opts):
        calls.append(1)
        stop.touch()
        return "disk 1G free, below the 150G floor"
    monkeypatch.setattr(ctp, "gate_reason", gate)
    fake = fake_projects()
    assert ctp.schedule(projects_of(sandbox, "a"), 1, 0, stop, poll=0.01) == {}
    assert fake.calls == [] and calls


def test_a_deferred_project_is_tried_again_after_the_rest(sandbox, fake_projects, no_gates):
    fake = fake_projects(outcomes={"a": [ctp.RunOutcome.DEFERRED, ctp.RunOutcome.DONE]})
    results = ctp.schedule(projects_of(sandbox, "a", "b"), 1, 0, sandbox.root / "STOP",
                           poll=0.01)
    assert fake.names() == ["a", "b", "a"] and results["a"] == ctp.RunOutcome.DONE


def test_a_lone_deferred_project_waits_a_poll_before_its_next_try(
        sandbox, fake_projects, no_gates, monkeypatch):
    sleeps = []
    monkeypatch.setattr(ctp.time, "sleep", lambda s: sleeps.append(s))
    fake = fake_projects(outcomes={"a": [ctp.RunOutcome.DEFERRED, ctp.RunOutcome.DONE]})
    ctp.schedule(projects_of(sandbox, "a"), 1, 0, sandbox.root / "STOP", poll=0.5)
    assert fake.names() == ["a", "a"] and 0.5 in sleeps


def test_a_crashing_worker_fails_only_its_project(sandbox, fake_projects, no_gates, capsys):
    def crash(name):
        if name == "a":
            raise RuntimeError("boom")
    fake = fake_projects(hook=crash)
    results = ctp.schedule(projects_of(sandbox, "a", "b"), 1, 0, sandbox.root / "STOP",
                           poll=0.01)
    assert results == {"a": ctp.RunOutcome.FAILED, "b": ctp.RunOutcome.DONE}
    assert "crashed the worker: RuntimeError: boom" in capsys.readouterr().out


def test_census_runs_up_to_workers_projects_at_once(sandbox, fake_projects, no_gates):
    barrier = threading.Barrier(2, timeout=10)
    fake = fake_projects(hook=lambda name: barrier.wait())
    results = ctp.schedule(projects_of(sandbox, "a", "b"), 2, 0, sandbox.root / "STOP",
                           poll=0.01)
    assert sorted(fake.names()) == ["a", "b"] and not barrier.broken
    assert set(results.values()) == {ctp.RunOutcome.DONE}


@pytest.mark.parametrize("state, failures, stamped, expected", [
    ("done", 0, True, "skip"),
    ("done", 0, False, "run"),        # done once, but the outputs are gone
    ("done-dirty", 0, True, "post"),
    ("failed", 1, True, "post"),
    ("failed", 1, False, "run"),
    ("failed", 2, False, "give-up"),
    ("", 0, False, "run"),
])
def test_plan(sandbox, state, failures, stamped, expected):
    if stamped:
        stamp(sandbox, "a")
    h = ledger.History(state=state, failures=failures)
    assert ctp.plan({"name": "a"}, h, retries=1) == expected


def test_mem_available_reads_proc_meminfo(tmp_path):
    meminfo = tmp_path / "meminfo"
    meminfo.write_text("MemTotal: 32000000 kB\nMemAvailable: 8388608 kB\n")
    assert ctp.mem_available_gb(meminfo) == 8.0
    meminfo.write_text("MemTotal: 1 kB\n")
    assert ctp.mem_available_gb(meminfo) is None
    assert ctp.mem_available_gb(tmp_path / "absent") is None


def test_gate_reason_names_disk_then_memory(sandbox, monkeypatch):
    monkeypatch.setattr(ctp, "DISK_FLOOR_GB", 10**9)
    assert ctp.gate_reason({}).startswith("disk")
    monkeypatch.setattr(ctp, "DISK_FLOOR_GB", 0)
    monkeypatch.setattr(ctp, "mem_available_gb", lambda: 2.0)
    assert ctp.gate_reason({"min_free_mem_gb": 4}).startswith("memory 2.0G")
    assert ctp.gate_reason({"min_free_mem_gb": 1}) is None
    monkeypatch.setattr(ctp, "mem_available_gb", lambda: None)
    assert ctp.gate_reason({"min_free_mem_gb": 4}) is None


@pytest.fixture
def census_ready(ready, monkeypatch):
    write_manifest(ready.root, *manifest_rows("a", "b"))
    monkeypatch.setattr(ctp, "install_signal_handlers", lambda: None)
    return ready


def test_cmd_census_runs_end_to_end_options(census_ready, fake_projects, no_gates):
    fake = fake_projects()
    assert ctp.cmd_census(census_args()) == 0
    assert fake.names() == ["a", "b"]
    assert ctp._OPTS["join"] and ctp._OPTS["cleanup"] and ctp._OPTS["own_session"]
    rows = ledger.read(census_ready.root / "ledger.jsonl")
    assert rows[0]["command"] == "census" and rows[-1]["step"] == "run-end"


def test_cmd_census_exits_3_when_stopped_cleanly(census_ready, fake_projects, no_gates):
    stop = census_ready.root / "STOP"
    fake_projects(hook=lambda name: stop.touch())
    assert ctp.cmd_census(census_args()) == 3
    assert ledger.read(census_ready.root / "ledger.jsonl")[-1]["stop_reason"]


def test_cmd_census_exits_1_on_a_failure(census_ready, fake_projects, no_gates):
    fake_projects(outcomes={"a": [ctp.RunOutcome.FAILED]})
    assert ctp.cmd_census(census_args(retries=0)) == 1


def test_cmd_census_refuses_to_start_with_a_stop_file(census_ready):
    (census_ready.root / "STOP").touch()
    with pytest.raises(SystemExit, match="Remove it to start"):
        ctp.cmd_census(census_args())


def test_cmd_census_takes_a_custom_stop_file(census_ready, fake_projects, no_gates):
    stop = census_ready.root / "my-stop"
    stop.touch()
    with pytest.raises(SystemExit, match="my-stop exists"):
        ctp.cmd_census(census_args(stop_file=str(stop)))


def test_cmd_census_needs_a_worker(census_ready):
    with pytest.raises(SystemExit, match="--workers"):
        ctp.cmd_census(census_args(workers=0))


def test_cmd_census_sets_the_disk_floor(census_ready, fake_projects, no_gates, monkeypatch):
    fake_projects()
    ctp.cmd_census(census_args(disk_floor_gb=77))
    assert ctp.DISK_FLOOR_GB == 77


def test_cmd_census_warns_about_unpinned_projects(census_ready, fake_projects, no_gates, capsys):
    write_manifest(census_ready.root, VALID_ROW)
    fake_projects()
    ctp.cmd_census(census_args())
    assert "1 of 1 projects are unpinned" in capsys.readouterr().out


def test_cmd_census_with_nothing_to_run(census_ready):
    write_manifest(census_ready.root)
    assert ctp.cmd_census(census_args()) == 0


def test_cmd_census_needs_commit_url_for_a_pinned_manifest(census_ready, runner_script):
    runner_script(REAL_RUNNER_USAGE.replace("--commit-url", "--commit-link"))
    with pytest.raises(SystemExit, match="--commit-url"):
        ctp.cmd_census(census_args())


def test_the_first_signal_stops_dispatch_and_the_second_terminates(monkeypatch):
    handlers = {}
    monkeypatch.setattr(ctp.signal, "signal", lambda sig, h: handlers.setdefault(sig, h))
    killed = []
    monkeypatch.setattr(ctp.os, "killpg", lambda pid, sig: killed.append((pid, sig)))
    ctp.install_signal_handlers()
    ctp._procs["a"] = SimpleNamespace(pid=4242)
    try:
        handlers[ctp.signal.SIGTERM](ctp.signal.SIGTERM, None)
        assert ctp._STOP.is_set() and killed == []
        assert ctp._RUN["stop_reason"] == "signal SIGTERM"
        handlers[ctp.signal.SIGINT](ctp.signal.SIGINT, None)
        assert killed == [(4242, ctp.signal.SIGTERM)]
    finally:
        ctp._STOP.clear()
        ctp._procs.clear()


def test_a_vanished_process_group_is_ignored(monkeypatch):
    handlers = {}
    monkeypatch.setattr(ctp.signal, "signal", lambda sig, h: handlers.setdefault(sig, h))

    def gone(pid, sig):
        raise ProcessLookupError(pid)
    monkeypatch.setattr(ctp.os, "killpg", gone)
    ctp.install_signal_handlers()
    ctp._procs["a"] = SimpleNamespace(pid=1)
    ctp._STOP.set()
    try:
        handlers[ctp.signal.SIGTERM](ctp.signal.SIGTERM, None)
    finally:
        ctp._STOP.clear()
        ctp._procs.clear()


def test_a_census_child_gets_its_own_session(sandbox, runner, jq):
    ctp._OPTS.update(own_session=True)
    ctp.run_phase(jq, "pipeline", ["./run_pipeline_process.sh"])
    assert runner.calls[0].kwargs["start_new_session"] is True


def test_main_dispatches_census(monkeypatch):
    seen = {}
    monkeypatch.setattr(ctp, "cmd_census", lambda args: seen.update(vars(args)) or 0)
    monkeypatch.setattr(ctp.sys, "argv", ["ctp.py", "census", "--workers", "3"])
    assert ctp.main() == 0
    assert seen["workers"] == 3 and seen["min_free_mem_gb"] == ctp.MIN_FREE_MEM_GB
    assert seen["drop_html"] is False and seen["anonymize"] == ""


# --------------------------------------------------------------------------- #
# CTP_CONFIG
# --------------------------------------------------------------------------- #

def test_config_is_forwarded_to_the_steps_when_ctp_config_is_set(monkeypatch, sandbox):
    cfg = sandbox.root / "e2e.cfg"
    cfg.write_text("[paths]\n")
    monkeypatch.setenv("CTP_CONFIG", str(cfg))
    monkeypatch.setattr(ctp.retain, "CONFIG", cfg)
    ctp.forward_config()
    assert ctp._ENV["CTP_CONFIG"] == str(cfg)


def test_config_is_not_forwarded_by_default(monkeypatch):
    monkeypatch.delenv("CTP_CONFIG", raising=False)
    ctp.forward_config()
    assert "CTP_CONFIG" not in ctp._ENV


def test_run_flags_name_the_config_and_the_directories(sandbox, monkeypatch):
    cfg = sandbox.root / "e2e.cfg"
    cfg.write_text("[paths]\n")
    monkeypatch.setattr(ctp.retain, "CONFIG", cfg)
    flags = ctp.run_flags(run_args(), write_manifest(sandbox.root, VALID_ROW))
    assert flags["config_path"] == str(cfg)
    assert flags["config_sha256"] == hashlib.sha256(b"[paths]\n").hexdigest()
    assert flags["cregit_dir"] == str(sandbox.cregit) and flags["output_dir"] == str(sandbox.out)
    monkeypatch.setattr(ctp.retain, "CONFIG", sandbox.root / "absent.cfg")
    assert ctp.run_flags(run_args(), sandbox.root / "manifest.tsv")["config_sha256"] == ""


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
    monkeypatch.setattr(ctp.subprocess, "Popen", r)
    monkeypatch.setattr(ctp.subprocess, "run", r)
    ctp._OPTS.update(auto_resume=True, from_step=1)
    assert ctp.run_project(pinned_jq) == ctp.RunOutcome.DONE
    assert runner_argv_tail(r) == "2"
    [row] = ledger_rows(sandbox, "jq", "pipeline")
    assert row["argv"][-1] == "2"


def runner_argv_tail(r):
    return r.argv("pipeline")[-1]


def test_a_clone_of_another_commit_restarts_at_step_1(monkeypatch, sandbox, pinned_jq):
    leftover_attempt(sandbox)
    r = FakeRunner(checkout=OTHER_SHA)
    monkeypatch.setattr(ctp.subprocess, "run", r)
    assert ctp.resume_step("jq", sandbox.out / "jq", PINNED_SHA) == 1


def test_no_blob_map_means_step_1(sandbox):
    (sandbox.out / "jq" / "jq-original.git").mkdir(parents=True)
    assert ctp.resume_step("jq", sandbox.out / "jq", PINNED_SHA) == 1


def test_an_unreadable_clone_means_step_1(sandbox, monkeypatch):
    leftover_attempt(sandbox)
    monkeypatch.setattr(ctp, "git_head", lambda repo: "")
    assert ctp.resume_step("jq", sandbox.out / "jq", "") == 1


def test_an_unpinned_leftover_resumes_at_step_2(sandbox, monkeypatch):
    leftover_attempt(sandbox)
    monkeypatch.setattr(ctp, "git_head", lambda repo: OTHER_SHA)
    assert ctp.resume_step("jq", sandbox.out / "jq", "") == 2


def test_run_without_auto_resume_keeps_step_1(monkeypatch, sandbox, pinned_jq):
    leftover_attempt(sandbox)
    r = FakeRunner()
    monkeypatch.setattr(ctp.subprocess, "Popen", r)
    monkeypatch.setattr(ctp.subprocess, "run", r)
    ctp.run_project(pinned_jq)
    assert "2" not in r.argv("pipeline")[-1:]


def test_an_explicit_from_step_wins_over_auto_resume(monkeypatch, sandbox, pinned_jq):
    leftover_attempt(sandbox)
    r = FakeRunner()
    monkeypatch.setattr(ctp.subprocess, "Popen", r)
    monkeypatch.setattr(ctp.subprocess, "run", r)
    ctp._OPTS.update(auto_resume=True, from_step=7)
    ctp.run_project(pinned_jq)
    assert r.argv("pipeline")[-1] == "7"


def test_next_job_takes_the_first_job_whose_wait_is_over():
    from collections import deque
    late = ctp.Job(1, {"name": "a"}, 1, 0, False, not_before=100.0)
    ready = ctp.Job(2, {"name": "b"}, 1, 0, False)
    queue = deque([late, ready])
    assert ctp.next_job(queue, now=50.0) is ready and list(queue) == [late]
    assert ctp.next_job(queue, now=50.0) is None
    assert ctp.next_job(queue, now=100.0) is late and not queue


def test_a_deferred_project_backs_off(sandbox, fake_projects, no_gates, capsys):
    fake = fake_projects(outcomes={"a": [ctp.RunOutcome.DEFERRED] * 2 + [ctp.RunOutcome.DONE]})
    ctp.schedule(projects_of(sandbox, "a"), 1, 0, sandbox.root / "STOP", poll=0.01)
    assert fake.names() == ["a", "a", "a"]
    out = capsys.readouterr().out
    assert "deferred; next try in 0s" in out


def test_the_deferral_wait_is_capped(monkeypatch, sandbox, fake_projects, no_gates, capsys):
    monkeypatch.setattr(ctp, "DEFER_MAX_S", 0.05)
    fake_projects(outcomes={"a": [ctp.RunOutcome.DEFERRED, ctp.RunOutcome.DONE]})
    ctp.schedule(projects_of(sandbox, "a"), 1, 0, sandbox.root / "STOP", poll=10)
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
    monkeypatch.setattr(ctp.subprocess, "Popen", popen)
    monkeypatch.setattr(ctp.subprocess, "run", r)
    ctp._OPTS.update(auto_resume=True, from_step=1)
    ctp.run_project(pinned_jq)
    # The bare repos, the blob map and the memo stay; the rest is rebuilt.
    assert seen["left"] == ["jq-blobmap.db", "jq-cregit.git", "jq-original.git", "memo"]
    [row] = ledger_rows(sandbox, "jq", "pipeline")
    assert row["resume"] == {"from_step": 2, "dropped": [
        "blame", "jq-original", "jq-cregit", "jq-cregit.db", "jq-dataset.parquet"]}


def test_a_resume_that_cannot_drop_fails_without_running_the_pipeline(
        monkeypatch, sandbox, pinned_jq):
    workdir = leftover_attempt(sandbox)
    (workdir / "blame").mkdir()
    r = FakeRunner()
    monkeypatch.setattr(ctp.subprocess, "Popen", r)
    monkeypatch.setattr(ctp.subprocess, "run", r)
    monkeypatch.setattr(ctp.retain, "remove_tree", lambda p: [f"{p} (busy)"])
    ctp._OPTS.update(auto_resume=True, from_step=1)
    assert ctp.run_project(pinned_jq) == ctp.RunOutcome.FAILED
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
    dropped, errors = ctp.drop_derived("jq", workdir)
    assert dropped == [] and "read-only" in errors[0]


# --latest: pin each project to the remote head when its first attempt starts.

@pytest.fixture
def latest(monkeypatch, sandbox):
    monkeypatch.setitem(ctp._OPTS, "latest", True)
    calls = []

    def remote_head(url, timeout=None):
        calls.append((url, timeout))
        return "master", OTHER_SHA
    monkeypatch.setattr(ctp.pin, "remote_head", remote_head)
    return calls


def test_latest_pins_the_remote_head_and_records_the_snapshot(monkeypatch, sandbox, latest,
                                                               pinned_jq):
    r = FakeRunner(checkout=OTHER_SHA)
    monkeypatch.setattr(ctp.subprocess, "Popen", r)
    monkeypatch.setattr(ctp.subprocess, "run", r)
    assert ctp.run_project(pinned_jq) == ctp.RunOutcome.DONE
    argv = r.argv("clone")
    assert argv[argv.index("--commit") + 1] == OTHER_SHA
    assert latest == [("https://github.com/jqlang/jq.git", ctp.LATEST_TIMEOUT_S)]
    [resolve] = ledger_rows(sandbox, "jq", "resolve")
    assert resolve["resolve"]["resolved_sha"] == OTHER_SHA
    assert resolve["resolve"]["snapshot_sha"] == PINNED_SHA
    assert resolve["resolve"]["reused"] is False
    rows = ledger_rows(sandbox, "jq")
    assert all(row["pinned_sha"] == OTHER_SHA and row["snapshot_sha"] == PINNED_SHA
               for row in rows)
    assert rows[-1]["state"] == "done"
    saved = json.loads(ctp.latest_path("jq").read_text())
    assert saved["resolved_sha"] == OTHER_SHA and saved["branch"] == "master"


def test_latest_reuses_the_commit_of_an_earlier_attempt(monkeypatch, sandbox, latest,
                                                         pinned_jq):
    path = ctp.latest_path("jq")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"resolved_sha": OTHER_SHA, "snapshot_sha": PINNED_SHA}))
    r = FakeRunner(checkout=OTHER_SHA)
    monkeypatch.setattr(ctp.subprocess, "Popen", r)
    monkeypatch.setattr(ctp.subprocess, "run", r)
    assert ctp.run_project(pinned_jq) == ctp.RunOutcome.DONE
    assert latest == []
    argv = r.argv("clone")
    assert argv[argv.index("--commit") + 1] == OTHER_SHA
    [resolve] = ledger_rows(sandbox, "jq", "resolve")
    assert resolve["resolve"]["reused"] is True


def test_latest_fails_the_project_when_the_remote_head_cannot_be_read(
        monkeypatch, sandbox, runner, pinned_jq):
    monkeypatch.setitem(ctp._OPTS, "latest", True)

    def unreachable(url, timeout=None):
        raise ctp.pin.PinError("cannot reach it")
    monkeypatch.setattr(ctp.pin, "remote_head", unreachable)
    assert ctp.run_project(pinned_jq) == ctp.RunOutcome.FAILED
    assert runner.calls == []
    [row] = ledger_rows(sandbox, "jq", "resolve")
    assert row["status"] == "failed" and "cannot reach it" in row["detail"]
    assert ledger_rows(sandbox, "jq", "project")[0]["failed_step"] == "resolve"
    assert not ctp.latest_path("jq").exists()


def test_without_latest_the_manifest_commit_is_pinned(sandbox, runner, pinned_jq):
    ctp.run_project(pinned_jq)
    assert ledger_rows(sandbox, "jq", "resolve") == []
    assert all("snapshot_sha" not in row for row in ledger_rows(sandbox, "jq"))


def test_a_workdir_of_another_commit_is_wiped_with_force_clean(monkeypatch, sandbox, runner,
                                                               pinned_jq):
    monkeypatch.setattr(ctp, "git_head", lambda repo: OTHER_SHA
                        if repo.name == "jq-original.git" else PINNED_SHA)
    assert ctp.run_project(pinned_jq) == ctp.RunOutcome.DONE
    assert "--force-clean" in runner.argv("pipeline")
    [row] = ledger_rows(sandbox, "jq", "pipeline")
    assert row["resume"] == {"stale_workdir": OTHER_SHA}


def test_a_fresh_workdir_is_not_force_cleaned(sandbox, runner, pinned_jq):
    ctp.run_project(pinned_jq)
    assert "--force-clean" not in runner.argv("pipeline")
