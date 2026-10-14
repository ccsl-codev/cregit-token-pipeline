"""Fakes and constants that the ctplib tests share."""

from __future__ import annotations

import argparse
import fcntl
import os
import threading
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import ledger
from ctplib import (
    manifest_file,
    project_run,
    toolchain,
)

REAL_TOOL_VERSIONS = toolchain.tool_versions


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
STEP_SCRIPTS = {
    "pin.py": "clone",
    "firm_attribution.py": "firm",
    "validate.py": "validate",
    "validate_schema.py": "schema",
    "consolidate.py": "join",
    "anonymize_parquet.py": "anonymize",
}


class FakeRunner:
    """Recording stand-in for subprocess.Popen and subprocess.run. Returns a
    scripted rc per phase, writes the stamp when validate succeeds, as
    validate.py does, and the result file when clone succeeds, as pin.py does.
    `checkout` is the HEAD that `git rev-parse` reports for the runner's clone."""

    def __init__(
        self,
        *,
        pipeline_rc=0,
        firm_rc=0,
        validate_rc=0,
        raise_on=None,
        payload=b"",
        rcs=None,
        checkout=PINNED_SHA,
        pin_result=True,
        anon_output=True,
    ):
        self.calls: list[SimpleNamespace] = []
        self.rcs = {
            "pipeline": pipeline_rc,
            "firm": firm_rc,
            "validate": validate_rc,
            **(rcs or {}),
        }
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
                f'{{"pinned_sha": "{commit}", "checked_out_sha": "{commit}"}}\n'
            )
        if phase == "validate" and rc == 0:
            stamp = Path(args[3])
            # A relative stamp path would escape tmp_path into the repository.
            assert stamp.is_absolute(), (
                f"stamp path must be absolute, got {stamp!r}. A relative path "
                f"escapes tmp_path and writes into the repository."
            )
            stamp.write_text("rows=42\nbytes=123456\n")
        # Our own pid, so the resource sampler walks a real process tree.
        return SimpleNamespace(returncode=rc, pid=os.getpid(), wait=lambda: rc)

    def argv(self, phase: str) -> list[str]:
        return next(c.args for c in self.calls if c.phase == phase)


class Clock:
    """Frozen stand-in for ctp.datetime. ctp only ever calls .now()."""

    def __init__(self, moment: datetime):
        self.moment = moment

    def now(self, tz=None):
        return self.moment.replace(tzinfo=tz) if tz else self.moment


def forbidden(*args, **kwargs):
    raise AssertionError("the test tried to start a real process; monkeypatch subprocess first")


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


def run_args(**over):
    """A complete `ctp run` Namespace. allow_empty_provenance is True here, unlike
    the CLI, so tests of other flags do not hit the provenance refusal; the
    guard's own tests pass False."""
    base = dict(
        manifest="manifest.tsv",
        only=None,
        jobs=1,
        retries=0,
        skip_html=False,
        drop_memo=False,
        memo_dir="",
        shards=0,
        shard_classes="L",
        from_step=1,
        gc=None,
        blame_jobs=0,
        memory_limit=None,
        duckdb_threads=0,
        project_meta="",
        allow_empty_provenance=True,
        mask="",
        mask_widened=False,
        retokenize="",
        reblame=False,
    )
    base.update(over)
    return argparse.Namespace(**base)


GIB = 1024**3


def ledger_rows(sandbox, project=None, step=None, started=False):
    """The ledger's rows, without the started rows unless started=True."""
    rows = ledger.read(sandbox.root / "ledger.jsonl")
    return [
        r
        for r in rows
        if (project is None or r.get("project") == project)
        and (step is None or r.get("step") == step)
        and (started or r.get("status") != "started")
    ]


def fake_parquet(sandbox, name="jq", data=b"PAR1 fake parquet PAR1") -> Path:
    path = sandbox.out / name / f"{name}-dataset.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def manifest_rows(*names):
    return [
        f"{n}\thttps://example.org/{n}.git\tcommunity\t\\.[ch]$\tS\t{PINNED_SHA}" for n in names
    ]


def projects_of(sandbox, *names):
    return manifest_file.read_manifest(write_manifest(sandbox.root, *manifest_rows(*names)), None)


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
        return queue.pop(0) if queue else project_run.RunOutcome.DONE

    def names(self):
        return [c[0] for c in self.calls]
