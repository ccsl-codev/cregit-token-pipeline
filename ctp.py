#!/usr/bin/env python3
"""ctp — cregit-token-pipeline runner: runs cregit over every project in manifest.tsv.
Per project: pipeline -> firm -> validate -> stamp; a validated project is skipped.
Ledgers: metrics.tsv (one row per phase attempt), runs.log, state/<name>/logs (never overwritten)."""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from pathlib import Path

import ledger
import pin
import retain
from ctplib import (
    artifacts,
    config,
    console,
    manifest_file,
    memory,
    metrics,
    pinning,
    resources,
    runner_args,
    runtime,
    toolchain,
)
from retain import lock_path, state_dir

HEARTBEAT_S = 30


# Sent for every project; cmd_run preflights them, so an upstream rename fails once.
REQUIRED_RUNNER_FLAGS = ("--repo-url", "--repo-name", "--work", "--mask")

# The last step, which writes the Parquet. The runner accepts --from-step 11+ and
# exits 0 having done nothing, so such a run cannot publish a blank Parquet.
DATASET_STEP = 10


def unique_log_name(logdir: Path, phase: str) -> Path:
    """A retry inside the same second gets a _N suffix, which sorts after the plain name."""
    stamp = f"{datetime.now():%Y%m%dT%H%M%S}"
    logfile = logdir / f"{phase}-{stamp}.log"
    n = 1
    while logfile.exists():
        logfile = logdir / f"{phase}-{stamp}_{n}.log"
        n += 1
    return logfile


def run_phase(project: dict, phase: str, args: list[str], keep_fds: tuple = ()) -> int:
    name, cls = project["name"], project["size_class"]
    logdir = state_dir(name) / "logs"
    logdir.mkdir(parents=True, exist_ok=True)
    logfile = unique_log_name(logdir, phase)

    runtime.live[name] = (phase, time.time())
    console.say(f"{name} ▶ {phase} started (log: {logfile})")
    latest = logdir / f"{phase}-latest.log"
    tmp = logdir / f".{phase}-latest.tmp"
    logfile.touch()
    tmp.unlink(missing_ok=True)  # a crash can leave this behind; symlink_to refuses to overwrite
    tmp.symlink_to(logfile)
    tmp.rename(latest)
    start = time.time()
    sampler = resources.ResourceSampler(project, phase)
    # Popen, not run: the sampler needs the child's pid to attribute RSS to this project.
    with logfile.open("wb") as lf:
        # keep_fds hands the project's flock to the child, so a process tree that
        # outlives this runner still holds the lock, and no second run can start.
        # A census child gets its own session: Ctrl-C reaches only ctp, which
        # then lets the running projects finish instead of killing them mid-step.
        extra = {"pass_fds": keep_fds} if keep_fds else {}
        if runtime.opts.get("own_session"):
            extra["start_new_session"] = True
        proc = subprocess.Popen(
            args, cwd=config.CREGIT, env=runtime.env, stdout=lf, stderr=subprocess.STDOUT, **extra
        )
        runtime.procs[name] = proc
        sampler.start(proc.pid)
        try:
            rc = proc.wait()
        finally:
            runtime.procs.pop(name, None)
            peak = sampler.stop()
    duration = int(time.time() - start)

    metrics.record_metric(name, cls, phase, duration, rc, logfile)
    mark = "✓" if rc == 0 else f"✗ rc={rc}"
    console.say(
        f"{name} {mark} {phase} in {duration}s"
        + (
            f" | peak {peak['tree_rss_mb']}MB rss, {peak['tree_procs']} procs,"
            f" {peak['cpu_pct']}% cpu, {peak['disk_free_gb_min']}G disk low"
            if peak["samples"]
            else ""
        )
    )
    return rc


class RunOutcome(StrEnum):
    """The result of one run_project call. Values match the strings already
    written to metrics.tsv and runs.log."""

    DONE = "done"
    DONE_DIRTY = "done-dirty"
    SKIPPED = "skipped"
    FAILED = "failed"
    DEFERRED = "deferred"

    @property
    def is_success(self) -> bool:
        """The Parquet is valid. done-dirty is a success that left work files behind."""
        return self in (RunOutcome.DONE, RunOutcome.DONE_DIRTY, RunOutcome.SKIPPED)


def check_memo_dir(name: str, opts: dict, workdir: Path) -> bool:
    """False, after saying why, when --memo-dir would put the memo inside the
    workdir the runner wipes."""
    memo_dir_opt = opts.get("memo_dir")
    if not memo_dir_opt:
        return True
    memo_dir = Path(memo_dir_opt).resolve() / name
    resolved_work = workdir.resolve()
    if memo_dir == resolved_work or resolved_work in memo_dir.parents:
        console.say(
            f"{name} — refusing to run: --memo-dir puts the memo at "
            f"{memo_dir}, inside the work directory the runner deletes. "
            "Point --memo-dir outside the corpus output directory."
        )
        return False
    return True


def checkout_matches(name: str, workdir: Path, pinned: str) -> tuple[bool, str]:
    """(ok, sha): which commit the runner blamed. ok is False when a pinned
    project's checkout is not the pinned commit, or cannot be read."""
    seen = toolchain.runner_checkout(name, workdir)
    if not pinned:
        console.say(f"{name} — unpinned, checked out {seen or 'an unreadable HEAD'}")
        return True, seen
    if seen != pinned:
        console.say(
            f"{name} ✗ pinned {pinned}, but the runner checked out {seen or 'an unreadable HEAD'}"
        )
        return False, seen
    console.say(f"{name} — checked out the pinned commit {pinned}")
    return True, seen


def latest_log(name: str, phase: str) -> str:
    link = state_dir(name) / "logs" / f"{phase}-latest.log"
    try:
        return str(link.resolve(strict=True))
    except OSError:
        return ""


@dataclass
class ProjectRun:
    """One attempt at one project, and the context every ledger row carries."""

    project: dict
    queue_pos: int | None = None
    queue_total: int | None = None
    attempt: int = 1
    checked_out_sha: str = ""
    latest: str = ""  # --latest: the remote head this attempt pins, else ""
    parquet: dict | None = None
    failed_step: str = ""
    started: float = field(default_factory=time.time)

    @property
    def name(self) -> str:
        return self.project["name"]

    @property
    def pinned(self) -> str:
        return self.latest or self.project.get(manifest_file.PINNED_FIELD, "")

    def base(self) -> dict:
        mask = runtime.opts.get("mask") or self.project["file_filter"]
        return {
            "v": ledger.VERSION,
            "run_id": runtime.run_info.get("run_id", ""),
            "project": self.name,
            "queue_pos": self.queue_pos,
            "queue_total": self.queue_total,
            "attempt": self.attempt,
            "manifest_row": dict(self.project),
            "pinned_sha": self.pinned or manifest_file.UNPINNED,
            # The manifest commit, when --latest pinned another one.
            **(
                {"snapshot_sha": self.project.get(manifest_file.PINNED_FIELD, "")}
                if self.latest
                else {}
            ),
            "checked_out_sha": self.checked_out_sha,
            "file_mask_sha256": artifacts.sha256_text(mask),
            "tools": runtime.run_info.get("tools", {}),
            "flags": runtime.run_info.get("flags", {}),
        }

    def record(self, step: str, status: str, start: float, end: float, **extra) -> None:
        row = self.base()
        row.update(
            step=step,
            status=status,
            start_utc=console.utc(start),
            end_utc=console.utc(end),
            duration_s=round(end - start, 3),
        )
        row.update(extra)
        try:
            ledger.append(config.LEDGER, row)
        except OSError as exc:
            # The audit trail is the point of the run: stop handing out new projects.
            runtime.run_info["ledger_broken"] = f"{type(exc).__name__}: {exc}"
            console.say(f"{self.name} ✗ LEDGER WRITE FAILED ({exc}); the run stops dispatching")

    def finish(self, state: str, **extra) -> RunOutcome:
        if state == ledger.FAILED:
            extra.setdefault("failed_step", self.failed_step)
        if self.parquet is not None:
            extra.setdefault("parquet", self.parquet)
        self.record(ledger.FINAL_STEP, state, self.started, time.time(), state=state, **extra)
        return RunOutcome(state)


def run_step(ctx: ProjectRun, step: str, argv: list[str], after=None) -> int:
    """Run one phase and append its ledger rows: one when it starts, so a crash shows
    which step was running, and one when it ends. `after(rc)` may veto a success and
    add fields: it returns (rc, extra). Re-raises OSError after recording it."""
    start = time.time()
    ctx.record(step, "started", start, start, argv=argv)
    try:
        rc = run_phase(ctx.project, step, argv, keep_fds=_held_fds(ctx.name))
    except OSError as exc:
        ctx.record(
            step,
            "failed",
            start,
            time.time(),
            exit_code=None,
            argv=argv,
            detail=f"could not start: {exc}",
        )
        ctx.failed_step = step
        raise
    extra: dict = {}
    if after is not None:
        rc, extra = after(rc)
    ctx.record(
        step,
        "ok" if rc == 0 else "failed",
        start,
        time.time(),
        exit_code=rc,
        argv=argv,
        log=latest_log(ctx.name, step),
        **extra,
    )
    if rc != 0:
        ctx.failed_step = step
    return rc


def _held_fds(name: str) -> tuple:
    fd = runtime.lock_fds.get(name)
    return (fd,) if fd is not None else ()


LATEST_TIMEOUT_S = 300


def latest_path(name: str) -> Path:
    return state_dir(name) / "latest.json"


def recorded_latest(name: str) -> str:
    """The commit an earlier --latest attempt chose for this project, or ""."""
    try:
        sha = json.loads(latest_path(name).read_text()).get("resolved_sha", "")
    except (OSError, ValueError, AttributeError):
        return ""
    return sha if pin.SHA_RE.match(sha) else ""


def resolve_latest(ctx: ProjectRun) -> bool:
    """--latest: pin this attempt to the remote head of the project's default branch.
    The first attempt reads it with `git ls-remote` and keeps the choice in
    state/<name>/latest.json; a later attempt reuses it, so a resumed run keeps its
    finished work at one commit. Records a 'resolve' ledger row. False on a failure."""
    start = time.time()
    path = latest_path(ctx.name)
    reused = True
    try:
        rec = json.loads(path.read_text())
        if not pin.SHA_RE.match(rec.get("resolved_sha", "")):
            raise ValueError(f"no resolved_sha in {path}")
    except (OSError, ValueError):
        reused = False
        try:
            branch, sha = pin.remote_head(ctx.project["url"], timeout=LATEST_TIMEOUT_S)
        except pin.PinError as exc:
            sha, branch, error = "", "", str(exc)
        else:
            error = "" if pin.SHA_RE.match(sha) else "the remote gave no HEAD commit"
        if error:
            ctx.record("resolve", "failed", start, time.time(), detail=error)
            ctx.failed_step = "resolve"
            return False
        rec = {
            "url": ctx.project["url"],
            "snapshot_sha": ctx.project.get(manifest_file.PINNED_FIELD, ""),
            "resolved_sha": sha,
            "branch": branch,
            "resolved_at": console.now_iso(),
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(rec) + "\n")
        tmp.replace(path)
    ctx.latest = rec["resolved_sha"]
    console.say(
        f"{ctx.name} — latest: {'reusing' if reused else 'resolved'} {ctx.latest} "
        f"(snapshot {rec.get('snapshot_sha') or 'none'})"
    )
    ctx.record("resolve", "ok", start, time.time(), resolve={**rec, "reused": reused})
    return True


def stale_workdir(name: str, workdir: Path, pinned: str) -> str:
    """The commit an earlier attempt left in the workdir when it is not the pinned
    one, else "". Such a workdir holds work for another commit, so the runner may
    wipe it even when its memo is large (--force-clean)."""
    head = toolchain.git_head(workdir / f"{name}-original.git")
    return head if head and pinned and head != pinned else ""


def resume_step(name: str, workdir: Path, pinned: str) -> int:
    """2 when an earlier attempt left a clone of the right commit and a blob map, so
    the runner can resume tokenizing instead of refusing to wipe a big memo; else 1."""
    bare = workdir / f"{name}-original.git"
    if not (workdir / f"{name}-blobmap.db").is_file() or not bare.is_dir():
        return 1
    head = toolchain.git_head(bare)
    if not head or (pinned and head != pinned):
        return 1
    return 2


def derived_artifacts(name: str) -> tuple[str, ...]:
    """What steps 3 to 10 rebuild from the tokenized repo. The same list as the
    runner's drop_refold_derived_artifacts: step 6 cannot clone over an existing
    working clone, and blame of a stale sha must not survive."""
    return (
        "blame",
        "html",
        f"{name}-original",
        f"{name}-cregit",
        f"{name}-original.db",
        f"{name}-cregit.db",
        f"{name}-persons.db",
        f"{name}-persons.xls",
        f"{name}-dataset.parquet",
    )


def drop_derived(name: str, workdir: Path) -> tuple[list[str], list[str]]:
    """Delete the derived artifacts, keeping the bare repos, the blob map and the memo.
    Returns (dropped, errors)."""
    dropped, errors = [], []
    for entry in derived_artifacts(name):
        path = workdir / entry
        if not path.exists() and not path.is_symlink():
            continue
        if path.is_dir() and not path.is_symlink():
            errs = retain.remove_tree(path)
        else:
            try:
                path.unlink()
                errs = []
            except OSError as exc:
                errs = [f"{path} ({exc})"]
        (errors.extend(errs) if errs else dropped.append(entry))
    return dropped, errors


def build_steps(ctx: ProjectRun, workdir: Path, stamp: Path) -> bool:
    """clone (when pinned), pipeline, firm, validate. True when the project validated."""
    name = ctx.name

    def after_clone(rc: int):
        if rc != 0:
            return rc, {}
        try:
            result = json.loads(pinning.pin_result_path(name).read_text())
        except (OSError, ValueError) as exc:
            return 1, {"detail": f"no pin result: {exc}"}
        return rc, {"pin": result}

    def after_pipeline(rc: int):
        if rc != 0:
            return rc, {}
        ok, seen = checkout_matches(name, workdir, ctx.pinned)
        ctx.checked_out_sha = seen
        if not ok:
            return 1, {
                "detail": f"checked out {seen or 'nothing readable'}, not the pinned {ctx.pinned}"
            }
        return rc, {}

    def after_validate(rc: int):
        if rc != 0:
            return rc, {}
        ctx.parquet = artifacts.parquet_info(name)
        return rc, {"parquet": ctx.parquet}

    if ctx.pinned and runtime.opts.get("latest") and not resolve_latest(ctx):
        return False
    if (
        ctx.pinned
        and run_step(ctx, "clone", pinning.pin_args(ctx.project, ctx.latest), after_clone) != 0
    ):
        return False
    opts = runtime.opts
    resume: dict = {}
    stale = stale_workdir(name, workdir, ctx.pinned)
    if stale:
        console.say(
            f"{name} — the workdir holds commit {stale}, not {ctx.pinned}; "
            "the runner wipes it (--force-clean)"
        )
        resume = {"stale_workdir": stale}
        opts = {**runtime.opts, "force_clean": True}
    if runtime.opts.get("auto_resume") and runtime.opts.get("from_step", 1) == 1:
        step = 1 if stale else resume_step(name, workdir, ctx.pinned)
        if step > 1:
            dropped, errors = drop_derived(name, workdir)
            console.say(
                f"{name} — an earlier attempt left its clone and blob map; resuming the "
                f"runner at step {step}"
                + (f", after dropping {', '.join(dropped)}" if dropped else "")
            )
            resume = {"from_step": step, "dropped": dropped}
            if errors:
                now = time.time()
                ctx.record(
                    "pipeline",
                    "failed",
                    now,
                    now,
                    exit_code=None,
                    resume=resume,
                    detail=f"could not drop the derived artifacts: {errors}",
                )
                ctx.failed_step = "pipeline"
                return False
            opts = {**runtime.opts, "from_step": step}

    def after_pipeline_resumed(rc: int):
        rc, extra = after_pipeline(rc)
        return rc, ({**extra, "resume": resume} if resume else extra)

    if (
        run_step(
            ctx,
            "pipeline",
            runner_args.build_pipeline_args(ctx.project, opts, workdir),
            after_pipeline_resumed,
        )
        != 0
    ):
        return False
    # firm adds the 3 firm columns to cregit's 67; validate gates the 70.
    later = dict(runner_args.project_phases(ctx.project, runtime.opts, workdir, stamp))
    if run_step(ctx, "firm", later["firm"]) != 0:
        return False
    return run_step(ctx, "validate", later["validate"], after_validate) == 0


def publish_steps(ctx: ProjectRun) -> bool:
    """schema (validate_schema.py), anonymize (only with --anonymize), then join
    (an incremental consolidate into ctp.duckdb). True when every step passes."""
    name = ctx.name
    parquet = config.OUT / name / f"{name}-dataset.parquet"
    if (
        runtime.opts.get("join")
        and run_step(
            ctx, "schema", ["python3", str(config.CORPUS / "validate_schema.py"), str(parquet)]
        )
        != 0
    ):
        return False
    if runtime.opts.get("anonymize"):

        def after_anonymize(rc: int):
            outputs = artifacts.anon_outputs(name)
            if rc == 0 and not outputs:
                return 1, {"detail": f"the anonymizer wrote no Parquet to {retain.ANON_DIR}/"}
            return rc, {"anonymized": outputs}

        # The interface of anonymize_parquet.py: an output directory, then the inputs.
        if (
            run_step(
                ctx,
                "anonymize",
                [
                    "python3",
                    runtime.opts["anonymize"],
                    str(config.OUT / name / retain.ANON_DIR),
                    str(parquet),
                ],
                after_anonymize,
            )
            != 0
        ):
            return False
    if not runtime.opts.get("join"):
        return True
    return (
        run_step(
            ctx,
            "join",
            [
                "python3",
                str(config.CORPUS / "consolidate.py"),
                "--join",
                name,
                "--manifest",
                runtime.opts["manifest"],
            ],
        )
        == 0
    )


def cleanup_step(ctx: ProjectRun) -> bool:
    """Remove the staging clone; then, with --cleanup, every bulky work file (the
    clones, memo/, blame/, the work databases), or only memo/ under --drop-memo.
    The Parquet, its stamp, the logs and the ledger stay. True when clean."""
    name = ctx.name
    start = time.time()
    errors = pinning.drop_pinned_clone(name)
    removed = [] if errors else ([str(pinning.pinned_clone_path(name))] if ctx.pinned else [])
    for err in errors:
        console.say(f"{name} — staging clone not fully removed: {err}")
    if runtime.opts.get("cleanup"):
        result = retain.clean_workdir(name, drop_html=bool(runtime.opts.get("drop_html")))
        removed += [r["entry"] for r in result["removed"]]
        errors += result["errors"]
        for err in result["errors"]:
            console.say(f"{name} — cleanup could not remove: {err}")
        console.say(
            f"{name} — cleanup freed {retain.human(result['bytes_freed'])}, kept "
            f"{', '.join(result['kept']) or 'nothing'}"
        )
        ctx.record(
            "cleanup",
            "ok" if not errors else "failed",
            start,
            time.time(),
            cleanup={
                "removed": removed,
                "errors": errors,
                "kept": result["kept"],
                "bytes_freed": result["bytes_freed"],
                "removed_bytes": {r["entry"]: r["bytes"] for r in result["removed"]},
            },
        )
        return not errors
    if runtime.opts.get("drop_memo"):
        # prune checks the keepers itself.
        _reclaimed, ok = retain.prune(name, ("memo",), apply=True)
        if ok:
            removed.append("memo")
        else:
            errors.append("retain refused to prune memo/ (see the run log)")
            console.say(f"{name} — memo/ kept, retain refused the prune (see above)")
    ctx.record(
        "cleanup",
        "ok" if not errors else "failed",
        start,
        time.time(),
        cleanup={"removed": removed, "errors": errors},
    )
    return not errors


def run_project(
    project: dict,
    queue_pos: int | None = None,
    queue_total: int | None = None,
    attempt: int = 1,
    resume_validated: bool = False,
) -> RunOutcome:
    """Runs one project's steps and appends a ledger row for each. Returns a RunOutcome.
    A validated project is skipped, unless resume_validated: then only the steps after
    validation run (schema, join, cleanup), as after a crash between them."""
    ctx = ProjectRun(project, queue_pos, queue_total, attempt)
    name = project["name"]
    if runtime.opts.get("latest") and project.get(manifest_file.PINNED_FIELD):
        # An attempt that only runs the steps after validation clones nothing, so
        # its rows carry the commit the validated run chose.
        ctx.latest = recorded_latest(name)
    workdir = config.OUT / name
    stamp = workdir / f"{name}.validated"
    post_only = stamp.exists()
    if post_only and not resume_validated:
        console.say(f"{name} — already validated, skip")
        return RunOutcome.SKIPPED

    workdir.mkdir(parents=True, exist_ok=True)
    state_dir(name).mkdir(parents=True, exist_ok=True)

    with lock_path(name).open("w") as lockfile:
        try:
            fcntl.flock(lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            console.say(f"{name} — another run holds the lock, skipping")
            return ctx.finish(ledger.DEFERRED, detail="another process holds the project lock")

        runtime.lock_fds[name] = lockfile.fileno()
        try:
            if not post_only and shutil.disk_usage(config.OUT).free < config.DISK_FLOOR_GB * 2**30:
                console.say(f"{name} — disk below {config.DISK_FLOOR_GB}G floor, deferred")
                return ctx.finish(ledger.DEFERRED, detail=f"disk below {config.DISK_FLOOR_GB}G")

            if not post_only and not check_memo_dir(name, runtime.opts, workdir):
                ctx.failed_step = "preflight"
                return ctx.finish(ledger.FAILED, detail="--memo-dir is inside the workdir")

            try:
                if post_only:
                    console.say(
                        f"{name} — validated earlier; resuming at the steps after validation"
                    )
                    ctx.parquet = artifacts.parquet_info(name)
                    ctx.checked_out_sha = toolchain.runner_checkout(name, workdir)
                elif not build_steps(ctx, workdir, stamp):
                    return ctx.finish(ledger.FAILED)
                if (
                    runtime.opts.get("join") or runtime.opts.get("anonymize")
                ) and not publish_steps(ctx):
                    return ctx.finish(ledger.FAILED)
            except OSError as exc:
                console.say(f"{name} ✗ a phase could not start: {exc}")
                return ctx.finish(ledger.FAILED)

            clean = cleanup_step(ctx)
            return ctx.finish(ledger.DONE if clean else ledger.DONE_DIRTY)
        except Exception as exc:
            # A bug here must not leave the attempt without its final row.
            console.say(f"{name} ✗ internal error: {type(exc).__name__}: {exc}")
            ctx.failed_step = ctx.failed_step or "internal"
            return ctx.finish(ledger.FAILED, detail=f"internal error: {type(exc).__name__}: {exc}")
        finally:
            runtime.lock_fds.pop(name, None)
            runtime.live.pop(name, None)


def heartbeat(stop: threading.Event, results: dict) -> None:
    while not stop.wait(HEARTBEAT_S):
        running = ", ".join(
            f"{n}({p} {int(time.time() - t)}s)" for n, (p, t) in sorted(runtime.live.items())
        )
        free_gb = shutil.disk_usage(config.OUT).free // 2**30
        done = sum(1 for v in results.values() if v.is_success)
        failed = sum(1 for v in results.values() if v == RunOutcome.FAILED)
        console.say(
            f"♥ running: {running or '—'} | done {done} failed {failed} | disk {free_gb}G free"
        )


# What forwarding each optional flag to a checkout without it would do.
REQUIRES: dict[str, str] = {
    "--skip-html": (
        "Patch that checkout to guard its HTML step, then re-run. Refusing to "
        "start: generating the HTML and deleting it later is not what the flag says."
    ),
    "--reblame": (
        "That checkout's step 7 cannot replace existing .blame files, so the run "
        "would skip every one of them and exit 0. Refusing to start."
    ),
    "--memo-dir": (
        "That checkout hard-codes BFG_MEMO_DIR to <work>/memo, which a step-1 "
        "run deletes, so the flag would be silently dropped. Patch it first."
    ),
    "--shards": (
        "Point pipeline.cfg at a checkout that supports sharded mode, "
        "or drop --shards and accept the single-process rate."
    ),
    "--gc": (
        "That checkout still packs unconditionally, and an unguarded "
        "repack failure deletes the workdir. Patch it before relying on --gc."
    ),
    "--blame-jobs": (
        "That checkout blames serially, at roughly 3 files per minute. "
        "Patch it before relying on the flag."
    ),
    "--memory-limit": (
        "That checkout runs step 10 at the generator's own default, so the "
        "flag would be silently dropped. Patch it before relying on it."
    ),
    "--duckdb-threads": "Patch it before relying on the flag.",
    "--mask-widened": (
        "That checkout would drop the flag, and blobExec would then refuse "
        "every project whose recorded mask differs from the manifest's "
        "(exit 3)."
    ),
    "--tokenizer-worker": (
        "That checkout has no tokenizer worker pool, so step 2 would "
        "start one tokenizer process per blob. Point pipeline.cfg at a "
        "checkout with cregit #102 or later."
    ),
    "--retokenize": (
        "That checkout would drop the flag, step 2 would reuse the cached "
        "tokenizations you asked to discard, and the run would exit 0 having "
        "changed nothing. Check pipeline.cfg points at a checkout that has it."
    ),
}

# Runner flags behind a ctp flag, where they are not just the same name.
CHECKS: dict[str, tuple[str, ...]] = {
    "--blame-jobs": ("--jobs",),
    "--shards": ("--mode", "--shards"),
}


def preflight_runner_flags(args: argparse.Namespace) -> tuple[bool, str]:
    """Exit before the run if the checkout cannot honour a flag ctp will forward, or
    two of ctp's own flags contradict each other: one message, not one rc=2 per project.
    Returns (mask_widened, retokenize)."""
    missing = [f for f in REQUIRED_RUNNER_FLAGS if not runner_args.script_supports(f)]
    if missing:
        sys.exit(
            f"{config.CREGIT}/run_pipeline_process.sh does not accept: {', '.join(missing)}.\n"
            "ctp.py passes these to every project and the runner exits 2 on an\n"
            "unknown flag. Check pipeline.cfg points at the right checkout, or\n"
            "update REQUIRED_RUNNER_FLAGS and run_project together."
        )
    if args.reblame and args.from_step > 7:
        sys.exit(
            f"--reblame needs --from-step 7 or less (got {args.from_step}).\n"
            "The re-blame happens inside step 7; from step 8 the flag is skipped and\n"
            "step 10 rebuilds the Parquet from the blame already on disk."
        )
    refuse_memo_conflicts(args)
    if getattr(args, "tokenizer_worker", False) and args.shards > 1:
        sys.exit(
            "--tokenizer-worker needs the runner's pipeline mode, and --shards runs "
            "sharded mode. Use one of them."
        )
    # Our own values before the runner probe, so a typo is not reported as "not implemented".
    refuse_bad_values(args)

    mask_widened = bool(args.mask_widened)
    retokenize = (args.retokenize or "").strip()
    refuse_unsupported_flags(requested_runner_flags(args, mask_widened, retokenize))
    if mask_widened:
        refuse_mask_widened_conflicts(args)
    if retokenize:
        refuse_retokenize_conflicts(args, mask_widened)
    return mask_widened, retokenize


def refuse_memo_conflicts(args: argparse.Namespace) -> None:
    if args.drop_memo and "--no-memo" in sys.argv:
        console.say(
            "note: --no-memo is an alias for --drop-memo and does NOT prevent the write. "
            "The tokenizer requires BFG_MEMO_DIR, so memo/ is built and then deleted."
        )
    if not args.memo_dir:
        return
    if args.drop_memo:
        sys.exit(
            "--memo-dir and --drop-memo contradict each other: one puts the memo "
            "where no wipe can reach it, the other deletes it after each project.\n"
            "--drop-memo also only prunes <workdir>/memo, so it would not even find "
            "an external memo. Pick one."
        )
    if not Path(args.memo_dir).is_dir():
        sys.exit(
            f"--memo-dir {args.memo_dir} is not an existing directory. Create it "
            "first: a typo here would quietly start a second corpus of memos "
            "instead of reusing the one you meant."
        )


def refuse_bad_values(args: argparse.Namespace) -> None:
    for flag, value in (
        ("--blame-jobs", args.blame_jobs),
        ("--duckdb-threads", args.duckdb_threads),
    ):
        if value < 0:
            sys.exit(f"{flag} cannot be negative (got {value}).")
    if args.memory_limit:
        try:
            memory.size_to_bytes(args.memory_limit)
        except ValueError as exc:
            sys.exit(f"--memory-limit: {exc}")
    if args.from_step < 1:
        sys.exit(f"--from-step must be 1 or greater (got {args.from_step}).")


def requested_runner_flags(
    args: argparse.Namespace, mask_widened: bool, retokenize: str
) -> list[str]:
    """The REQUIRES keys this invocation asks for, in REQUIRES order."""
    requested = {
        "--skip-html": args.skip_html,
        "--reblame": args.reblame,
        "--memo-dir": bool(args.memo_dir),
        "--shards": args.shards > 1,
        "--gc": bool(args.gc),
        "--blame-jobs": bool(args.blame_jobs),
        "--memory-limit": bool(args.memory_limit),
        "--duckdb-threads": bool(args.duckdb_threads),
        "--mask-widened": mask_widened,
        "--retokenize": bool(retokenize),
        "--tokenizer-worker": bool(getattr(args, "tokenizer_worker", False)),
    }
    return [flag for flag in REQUIRES if requested[flag]]


def refuse_unsupported_flags(flags: list[str]) -> None:
    for flag in flags:
        gap = [f for f in CHECKS.get(flag, (flag,)) if not runner_args.script_supports(f)]
        if not gap:
            continue
        if flag in CHECKS:
            sys.exit(
                f"{flag} needs {', '.join(gap)}, which "
                f"{config.CREGIT}/run_pipeline_process.sh does not advertise.\n{REQUIRES[flag]}"
            )
        sys.exit(
            f"{flag} is not implemented by {config.CREGIT}/run_pipeline_process.sh."
            f"\n{REQUIRES[flag]}"
        )


def refuse_mask_widened_conflicts(args: argparse.Namespace) -> None:
    if args.from_step < 2:
        sys.exit(
            "--mask-widened needs --from-step 2 or greater. Step 1 deletes the project\n"
            "workdir, taking with it the blob map this flag reuses and the cregit.git\n"
            "its new_blob ids live in, so there would be nothing left to preserve."
        )
    if args.shards > 1:
        sys.exit(
            "--mask-widened cannot be combined with sharding: each shard builds a fresh\n"
            "blob map, so there is no recorded mask to widen."
        )


def refuse_retokenize_conflicts(args: argparse.Namespace, mask_widened: bool) -> None:
    if args.from_step != 2:
        sys.exit(
            f"--retokenize needs --from-step 2 exactly (got {args.from_step}).\n"
            "Step 1 deletes the blob map this flag edits. Step 3 and later skip step 2,\n"
            "so the tokens would never be remade and the rest of the pipeline would run\n"
            "over the stale ones and exit 0."
        )
    if mask_widened:
        sys.exit(
            "--mask-widened and --retokenize cannot be used in the same run. Each one\n"
            "verifies a different invariant of the blob map, and together neither check\n"
            "means anything: one reuses rows across a mask change, the other deletes rows\n"
            "a tokenizer change invalidated."
        )
    if args.shards > 1:
        sys.exit(
            "--retokenize cannot be combined with sharding: each shard builds a fresh\n"
            "blob map, so there are no cached tokenizations to invalidate."
        )


def refuse_unless_runner_accepts(flag: str, needed: tuple[str, ...], consequence: str = "") -> None:
    missing = [f for f in needed if not runner_args.script_supports(f)]
    if missing:
        sys.exit(
            f"{flag} needs {', '.join(missing)}, which "
            f"{config.CREGIT}/run_pipeline_process.sh does not accept.{consequence}"
        )


def resolve_project_meta(path: str) -> str:
    if not Path(path).exists():
        sys.exit(
            f"--project-meta {path} does not exist. "
            "The sidecar format is documented in validate_schema.py, "
            "in the comment above CREGIT_COLUMNS."
        )
    refuse_unless_runner_accepts("--project-meta", ("--project-meta", "--project-key"))
    # Absolute: the runner starts with cwd=CREGIT, not this repository.
    return str(Path(path).resolve())


def provenance_gaps(project_meta: str) -> list[tuple[str, str]]:
    """(flag, what publishing without it costs) for each missing provenance flag."""
    if project_meta:
        return []
    return [
        (
            "--project-meta",
            "29 provenance columns empty on every row: clone_url, "
            "provenance_status, source, stratum, fact, contested, "
            "label_date, owner, repo, roster_name, roster_lang, "
            "language, commits, size_class, size_kb, stars, pushed_at, "
            "license, owner_type, archived, fork, history_cluster, "
            "history_shared_with, history_relation, history_includes, "
            "history_first, history_created, manifest_category, "
            "file_mask",
        )
    ]


PROVENANCE_REFUSAL_TAIL = (
    "This does not fail anything. The file keeps all 70 columns in the "
    "right order, so validate.py passes it and no consumer can tell a "
    "blank column from provenance that is genuinely unknown, which is "
    "why a long, expensive run can finish and publish silently "
    "inconsistent with the rest of the corpus.\n"
    "Pass the flag this run is missing:\n"
    "  --project-meta <your-project-meta>.json\n"
    "If blank columns are genuinely what you want — a fixture, a "
    "one-project smoke run, a corpus whose sidecar does not exist yet — "
    "say so with --allow-empty-provenance."
)


def enforce_provenance(gaps: list[tuple[str, str]], args: argparse.Namespace) -> None:
    """Exit when a run reaching step 10 has a provenance gap, unless
    --allow-empty-provenance. Refuses rather than warns: nothing later can tell
    a blank column from provenance that is genuinely unknown."""
    allow_empty = bool(args.allow_empty_provenance)
    if allow_empty and not gaps:
        sys.exit(
            "--allow-empty-provenance has nothing to allow: --project-meta "
            "is present, so no column would be blank. Drop the flag — left "
            "in a wrapper it would silence the guard on the next run that "
            "omits --project-meta."
        )
    if not gaps or args.from_step > DATASET_STEP:
        return
    itemised = "".join(f"  {flag} absent — {cost}\n" for flag, cost in gaps)
    if not allow_empty:
        sys.exit(
            f"refusing this run: it reaches step {DATASET_STEP} and would publish a "
            f"Parquet with blank provenance.\n{itemised}{PROVENANCE_REFUSAL_TAIL}"
        )
    console.say(
        "WARNING: --allow-empty-provenance: this run will publish a Parquet "
        "with blank provenance, on purpose."
    )
    for flag, cost in gaps:
        console.say(f"         {flag} absent — {cost}")
    console.say(
        "         Do not mix these rows into the corpus: they are "
        "schema-valid and indistinguishable from rows whose provenance is "
        "genuinely unknown."
    )


def announce_run(opts: dict, jobs: int) -> None:
    """Log every choice that changes what this run publishes or how it behaves,
    so a reader of the log need not infer it from the absence of a message."""
    if opts["mask"]:
        console.say(
            f"WARNING: --mask overrides the manifest for every project in this run: {opts['mask']}"
        )
        console.say(
            "         the --project-meta sidecar records the MANIFEST's mask, so the "
            "Parquet's file_mask column will not match this run."
        )
    if opts["memory_limit"]:
        warning = memory.memory_budget_warning(opts["memory_limit"], jobs)
        if warning:
            console.say(f"WARNING: {warning}")
    if opts["from_step"] > 1:
        console.say(f"resuming at step {opts['from_step']}: the runner keeps the existing workdir")
    if opts["retokenize"]:
        console.say(
            f"--retokenize {opts['retokenize']}: step 2 will DISCARD the cached tokenizations "
            f"for these extensions and redo them."
        )
        console.say(
            "         Every other extension's cached work is kept. blobExec drops the "
            "blob_map rows and the memo entries together, and refuses the run (exit 7) "
            "if the request would have invalidated nothing — so a typo cannot pass as "
            "a successful re-run."
        )
    if opts["mask_widened"]:
        console.say(
            "--mask-widened: step 2 will REUSE each project's existing tokenizations "
            "instead of redoing them."
        )
        console.say(
            "         blobExec verifies per project, against the rows: every "
            "already-tokenized path must still be selected by the new mask, and the "
            "retained new_blob ids must exist in cregit.git. Either check failing "
            "refuses that project (rc 3) and changes nothing."
        )
        console.say(
            "         tree_map, commit_map, ref_map and blob_map's identity rows are "
            "discarded, so files the wider mask newly selects are tokenized rather "
            "than passed through as raw source."
        )
    if opts["shards"] > 1:
        console.say(
            f"sharding {opts['shards']}-way for size class"
            f"{'es' if len(opts['shard_classes']) > 1 else ''} {', '.join(opts['shard_classes'])}"
        )


def resolve_anonymizer(path: str) -> str:
    if not path:
        return ""
    if not Path(path).is_file():
        sys.exit(
            f"--anonymize {path} is not a file. It is run as "
            "`python3 SCRIPT <outdir> <dataset.parquet>`, the interface of "
            "anonymize_parquet.py."
        )
    return str(Path(path).resolve())


def refuse_cleanup_conflicts(args: argparse.Namespace) -> None:
    if getattr(args, "drop_html", False) and not getattr(args, "cleanup", False):
        sys.exit(
            "--drop-html deletes html/ in the cleanup step. Add --cleanup, or use "
            "--skip-html to not write the HTML at all."
        )


def run_options(args: argparse.Namespace) -> dict:
    """The checked options run_project reads through _OPTS. Exits on a refusal."""
    refuse_cleanup_conflicts(args)
    anonymize = resolve_anonymizer(getattr(args, "anonymize", "") or "")
    mask_widened, retokenize = preflight_runner_flags(args)
    project_meta = resolve_project_meta(args.project_meta) if args.project_meta else ""
    enforce_provenance(provenance_gaps(project_meta), args)
    return dict(
        skip_html=args.skip_html,
        drop_memo=args.drop_memo,
        reblame=args.reblame,
        memo_dir=args.memo_dir,
        shards=args.shards,
        shard_classes=tuple(c.strip() for c in args.shard_classes.split(",") if c.strip()),
        from_step=args.from_step,
        gc=args.gc,
        mask_widened=mask_widened,
        retokenize=retokenize,
        tokenizer_worker=bool(getattr(args, "tokenizer_worker", False)),
        blame_jobs=args.blame_jobs,
        memory_limit=args.memory_limit,
        duckdb_threads=args.duckdb_threads,
        mask=args.mask,
        project_meta=project_meta,
        join=bool(getattr(args, "join", False)),
        manifest=str((config.CORPUS / args.manifest).resolve()),
        cleanup=bool(getattr(args, "cleanup", False)),
        drop_html=bool(getattr(args, "drop_html", False)),
        anonymize=anonymize,
    )


def run_flags(args: argparse.Namespace, manifest: Path) -> dict:
    """Every option of this run, JSON-safe, with the manifest it read. `effective` is
    what run_project reads after the checks; `cli` is what was typed."""
    flags: dict = {
        "effective": {
            k: (list(v) if isinstance(v, tuple) else v) for k, v in sorted(runtime.opts.items())
        },
        "cli": {k: v for k, v in sorted(vars(args).items()) if k != "fn"},
        "argv": sys.argv[1:],
    }
    flags["manifest_path"] = str(manifest)
    flags["config_path"] = str(retain.CONFIG)
    flags["config_sha256"] = artifacts.sha256_file(retain.CONFIG) if retain.CONFIG.is_file() else ""
    flags["cregit_dir"] = str(config.CREGIT)
    flags["output_dir"] = str(config.OUT)
    try:
        flags["manifest_sha256"] = artifacts.sha256_file(manifest)
    except OSError:
        flags["manifest_sha256"] = ""
    return flags


def start_ledger_run(args: argparse.Namespace, manifest: Path, command: str) -> None:
    """Fill _RUN, then append the run-start row."""
    toolchain.forward_config()
    runtime.run_info.clear()
    runtime.run_info["run_id"] = f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{os.getpid()}"
    runtime.run_info["tools"] = toolchain.tool_versions()
    runtime.run_info["flags"] = run_flags(args, manifest)
    now = time.time()
    try:
        ledger.append(
            config.LEDGER,
            {
                "v": ledger.VERSION,
                "run_id": runtime.run_info["run_id"],
                "step": "run-start",
                "command": command,
                "start_utc": console.utc(now),
                "tools": runtime.run_info["tools"],
                "flags": runtime.run_info["flags"],
            },
        )
    except OSError as exc:
        sys.exit(f"cannot write the ledger {config.LEDGER}: {exc}")
    console.say(f"ledger: {config.LEDGER} (run {runtime.run_info['run_id']})")


def end_ledger_run(rc: int, started: float, results: dict) -> None:
    counts: dict = {}
    for outcome in results.values():
        counts[str(outcome)] = counts.get(str(outcome), 0) + 1
    try:
        ledger.append(
            config.LEDGER,
            {
                "v": ledger.VERSION,
                "run_id": runtime.run_info.get("run_id", ""),
                "step": "run-end",
                "exit_code": rc,
                "start_utc": console.utc(started),
                "end_utc": console.utc(time.time()),
                "duration_s": round(time.time() - started, 3),
                "outcomes": counts,
                **(
                    {"stop_reason": runtime.run_info["stop_reason"]}
                    if runtime.run_info.get("stop_reason")
                    else {}
                ),
            },
        )
    except OSError as exc:
        console.say(f"WARNING: cannot write the run-end row to {config.LEDGER}: {exc}")


def run_passes(projects: list[dict], jobs: int, retries: int) -> dict:
    """name -> RunOutcome after the first pass and up to `retries` passes over the rest."""
    results: dict = {}
    stop = threading.Event()
    threading.Thread(target=heartbeat, args=(stop, results), daemon=True).start()
    try:
        for attempt in range(1 + retries):
            todo = [
                p
                for p in projects
                if not (results.get(p["name"]) and results[p["name"]].is_success)
            ]
            if not todo:
                break
            if attempt:
                console.say(f"retry pass {attempt}: {[p['name'] for p in todo]}")
            with ThreadPoolExecutor(max_workers=jobs) as pool:
                for p, res in zip(todo, pool.map(run_project, todo), strict=True):
                    results[p["name"]] = res
    finally:
        stop.set()
    return results


def cmd_run(args: argparse.Namespace) -> int:
    config.OUT.mkdir(parents=True, exist_ok=True)
    projects = manifest_file.read_manifest(
        config.CORPUS / args.manifest, set(args.only.split(",")) if args.only else None
    )
    if not projects:
        console.say("nothing to run (empty manifest / --only filter matched nothing)")
        return 0

    runtime.opts.update(run_options(args))
    if any(p.get(manifest_file.PINNED_FIELD) for p in projects):
        refuse_unless_runner_accepts(
            "a pinned manifest",
            ("--commit-url",),
            " Pinned projects run from a local staging clone, so the runner needs "
            "--commit-url to keep the real commit links.",
        )
    announce_run(runtime.opts, args.jobs)

    run_start = time.time()
    console.say("capturing devenv environment (once)...")
    runtime.env.update(toolchain.capture_devenv_env())
    # A deleted or private repository must fail its clone, not wait for a password.
    runtime.env.setdefault("GIT_TERMINAL_PROMPT", "0")
    console.say(f"devenv environment captured ({len(runtime.env)} vars)")
    start_ledger_run(args, config.CORPUS / args.manifest, "run")
    with config.RUNS_LOG.open("a") as f:
        f.write(f"{console.now_iso()}\trun-start\tjobs={args.jobs}\tprojects={len(projects)}\n")

    results = run_passes(projects, args.jobs, args.retries)

    rc = 0 if all(v.is_success for v in results.values()) else 1
    end_ledger_run(rc, run_start, results)
    with config.RUNS_LOG.open("a") as f:
        f.write(
            f"{console.now_iso()}\trun-end\trc={rc}\tduration_s={int(time.time() - run_start)}\n"
        )

    console.say(
        f"run finished rc={rc} — " + " ".join(f"{n}:{v}" for n, v in sorted(results.items()))
    )
    return rc


# --------------------------------------------------------------------------- #
# census: the scheduler for a long end-to-end run
# --------------------------------------------------------------------------- #

STOP_NAME = "STOP"
MIN_FREE_MEM_GB = 8
POLL_S = 15


def gate_reason(opts: dict) -> str | None:
    """Why a new project may not start now, or None."""
    free_gb = shutil.disk_usage(config.OUT).free / 2**30
    if free_gb < config.DISK_FLOOR_GB:
        return f"disk {free_gb:.0f}G free, below the {config.DISK_FLOOR_GB}G floor"
    mem = memory.mem_available_gb()
    floor = opts.get("min_free_mem_gb", MIN_FREE_MEM_GB)
    if mem is not None and mem < floor:
        return f"memory {mem:.1f}G available, below the {floor}G floor"
    return None


def stop_reason(stop_file: Path) -> str | None:
    if runtime.stop.is_set():
        return runtime.run_info.get("stop_reason") or "stop requested"
    if runtime.run_info.get("ledger_broken"):
        return f"the ledger cannot be written: {runtime.run_info['ledger_broken']}"
    if stop_file.exists():
        return f"{stop_file} exists"
    return None


# What the scheduler does with a project, from its ledger history and its stamp.
SKIP, POST, RUN, GIVE_UP = "skip", "post", "run", "give-up"


def plan(project: dict, history: ledger.History, retries: int) -> str:
    name = project["name"]
    stamped = (config.OUT / name / f"{name}.validated").exists()
    if history.state == ledger.DONE and stamped:
        return SKIP
    if history.failures + history.interrupted > retries:
        return GIVE_UP
    # Validated but not done: a crash after validation, a done-dirty cleanup, or
    # a project an earlier `ctp.py run` validated. Only the later steps run.
    return POST if stamped else RUN


@dataclass
class Job:
    """One queued project of a census."""

    pos: int
    project: dict
    attempt: int
    failures: int
    post: bool
    defers: int = 0
    not_before: float = 0.0


# A deferred project (its lock is held elsewhere) waits poll * 2**defers, up to this.
DEFER_MAX_S = 600


def next_job(queue: deque, now: float) -> Job | None:
    """The first job whose wait is over, taken out of the queue; None if all wait."""
    for i, job in enumerate(queue):
        if job.not_before <= now:
            del queue[i]
            return job
    return None


def schedule(
    projects: list[dict], workers: int, retries: int, stop_file: Path, poll: float = POLL_S
) -> dict:
    """Run projects in manifest order with up to `workers` at once. Returns name -> outcome."""
    total = len(projects)
    histories = ledger.histories(ledger.read(config.LEDGER))
    results: dict = {}
    queue: deque = deque()
    for pos, project in enumerate(projects, 1):
        name = project["name"]
        h = histories.get(name, ledger.History())
        decision = plan(project, h, retries)
        if decision == SKIP:
            results[name] = RunOutcome.SKIPPED
        elif decision == GIVE_UP:
            console.say(
                f"{name} — failed {h.failures + h.interrupted} times "
                f"(last at {h.last_failed_step or '?'}), more than --retries {retries}; "
                "not retried"
            )
            results[name] = RunOutcome.FAILED
        else:
            queue.append(
                Job(pos, project, h.attempts + 1, h.failures + h.interrupted, decision == POST)
            )
    skipped = sum(1 for v in results.values() if v == RunOutcome.SKIPPED)
    console.say(
        f"census: {total} projects, {skipped} already done, {len(queue)} to run, {workers} workers"
    )

    stop_heartbeat = threading.Event()
    threading.Thread(target=heartbeat, args=(stop_heartbeat, results), daemon=True).start()
    running: dict = {}
    waiting_since: float | None = None
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            while queue or running:
                reason = stop_reason(stop_file)
                if reason and not runtime.run_info.get("stop_reason"):
                    runtime.run_info["stop_reason"] = reason
                    console.say(
                        f"stopping: {reason}. No new project starts; "
                        f"{len(running)} running will finish"
                    )
                if not reason and queue and len(running) < workers:
                    gate = gate_reason(runtime.opts)
                    job = next_job(queue, time.time()) if gate is None else None
                    if job is not None:
                        waiting_since = None
                        fut = pool.submit(
                            run_project, job.project, job.pos, total, job.attempt, job.post
                        )
                        running[fut] = job
                        continue
                    if gate is not None and waiting_since is None:
                        waiting_since = time.time()
                        console.say(f"waiting to start {queue[0].project['name']}: {gate}")
                if not running:
                    if reason or not queue:
                        break
                    time.sleep(poll)
                    continue
                done, _ = wait(list(running), timeout=poll, return_when=FIRST_COMPLETED)
                for fut in done:
                    job = running.pop(fut)
                    name = job.project["name"]
                    try:
                        outcome = fut.result()
                    except Exception as exc:  # one project must not end the run
                        console.say(f"{name} ✗ crashed the worker: {type(exc).__name__}: {exc}")
                        outcome = RunOutcome.FAILED
                    results[name] = outcome
                    if outcome == RunOutcome.FAILED:
                        job.failures += 1
                        if job.failures <= retries:
                            console.say(
                                f"{name} — failed, retry {job.failures} of {retries} queued"
                            )
                            job.attempt += 1
                            job.post = (config.OUT / name / f"{name}.validated").exists()
                            queue.append(job)
                    elif outcome == RunOutcome.DEFERRED:
                        # Another process holds it: try again later, waiting longer each time.
                        job.defers += 1
                        wait_s = min(poll * 2**job.defers, DEFER_MAX_S)
                        job.not_before = time.time() + wait_s
                        console.say(f"{name} — deferred; next try in {wait_s:.0f}s")
                        queue.append(job)
    finally:
        stop_heartbeat.set()
    return results


def install_signal_handlers() -> None:
    """First SIGINT or SIGTERM: stop starting projects. Second: also terminate the
    running ones, whose rows then record the failure."""

    def handler(signum, _frame):
        if not runtime.stop.is_set():
            runtime.run_info["stop_reason"] = f"signal {signal.Signals(signum).name}"
            runtime.stop.set()
            console.say(
                f"{signal.Signals(signum).name}: finishing the running projects; "
                "send it again to terminate them"
            )
            return
        for name, proc in list(runtime.procs.items()):
            console.say(f"terminating {name} (process group {proc.pid})")
            with contextlib.suppress(OSError):
                os.killpg(proc.pid, signal.SIGTERM)

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


def cmd_census(args: argparse.Namespace) -> int:
    """Every project of the manifest, end to end, in manifest order."""
    stop_file = Path(args.stop_file).resolve() if args.stop_file else config.CORPUS / STOP_NAME
    if stop_file.exists():
        sys.exit(f"{stop_file} exists, so the census would stop at once. Remove it to start.")
    if args.workers < 1:
        sys.exit("--workers must be at least 1")
    config.OUT.mkdir(parents=True, exist_ok=True)
    manifest = config.CORPUS / args.manifest
    projects = manifest_file.read_manifest(
        manifest, set(args.only.split(",")) if args.only else None
    )
    if not projects:
        console.say("nothing to run (empty manifest / --only filter matched nothing)")
        return 0
    unpinned = sum(1 for p in projects if not p.get(manifest_file.PINNED_FIELD))
    if unpinned:
        console.say(
            f"WARNING: {unpinned} of {len(projects)} projects are unpinned: each runs the "
            "remote HEAD of its clone day, and the ledger says so"
        )

    args.join, args.cleanup = True, True
    runtime.opts.update(run_options(args))
    runtime.opts.update(
        own_session=True,
        min_free_mem_gb=args.min_free_mem_gb,
        auto_resume=True,
        latest=bool(getattr(args, "latest", False)),
    )
    if runtime.opts["latest"]:
        console.say(
            "--latest: each project pins the remote head of its default branch when its "
            "first attempt starts; the ledger records it and the manifest commit"
        )
    config.DISK_FLOOR_GB = args.disk_floor_gb
    if len(projects) > unpinned:
        refuse_unless_runner_accepts(
            "a pinned manifest",
            ("--commit-url",),
            " Pinned projects run from a local staging clone, so the runner needs "
            "--commit-url to keep the real commit links.",
        )
    announce_run(runtime.opts, args.workers)
    console.say(f"stop file: {stop_file} (touch it to stop after the running projects)")

    run_start = time.time()
    console.say("capturing devenv environment (once)...")
    runtime.env.update(toolchain.capture_devenv_env())
    runtime.env.setdefault("GIT_TERMINAL_PROMPT", "0")
    start_ledger_run(args, manifest, "census")
    install_signal_handlers()
    with config.RUNS_LOG.open("a") as f:
        f.write(
            f"{console.now_iso()}\tcensus-start\tworkers={args.workers}\tprojects={len(projects)}\n"
        )

    results = schedule(projects, args.workers, args.retries, stop_file, args.poll)

    stopped = bool(runtime.run_info.get("stop_reason"))
    rc = 0 if all(v.is_success for v in results.values()) and not stopped else 1
    if stopped and all(v.is_success for v in results.values()):
        rc = 3
    end_ledger_run(rc, run_start, results)
    with config.RUNS_LOG.open("a") as f:
        f.write(
            f"{console.now_iso()}\tcensus-end\trc={rc}\tduration_s={int(time.time() - run_start)}\n"
        )
    counts: dict = {}
    for v in results.values():
        counts[str(v)] = counts.get(str(v), 0) + 1
    console.say(
        f"census finished rc={rc}: "
        + ", ".join(f"{k} {n}" for k, n in sorted(counts.items()))
        + (f" (stopped: {runtime.run_info['stop_reason']})" if stopped else "")
    )
    return rc


def cmd_status(args: argparse.Namespace) -> int:
    projects = manifest_file.read_manifest(config.CORPUS / args.manifest, None)
    last: dict = {}
    if config.METRICS.exists():
        for line in config.METRICS.read_text().splitlines()[1:]:
            ts, name, _cls, phase, dur, rc, _log = line.split("\t")
            last[name] = f"{ts} {phase}({dur}s,rc={rc})"

    print(f"{'PROJECT':<16} {'CLASS':<6} {'STATE':<9} LAST_ACTIVITY")
    done = 0
    for p in projects:
        state = retain.project_state(p["name"], config.OUT / p["name"])
        if state == "DONE":
            done += 1
        print(f"{p['name']:<16} {p['size_class']:<6} {state:<9} {last.get(p['name'], '—')}")

    # Before the first run OUT may not exist; measure the file system it will be on.
    free_gb = (
        shutil.disk_usage(next(p for p in (config.OUT, *config.OUT.parents) if p.exists())).free
        // 2**30
    )
    print(f"\nprogress: {done}/{len(projects)} validated | disk {free_gb}G free")
    return 0


BAR_WIDTH = 30


def bar(done: int, total: int, width: int = BAR_WIDTH) -> str:
    """A fixed-width progress bar. Never divides by zero on an empty manifest."""
    filled = round(width * done / total) if total else 0
    return "█" * filled + "░" * (width - filled)


def finished_runs() -> list[tuple]:
    """(name, cls, duration_s) for every successful pipeline run, oldest first."""
    if not config.METRICS.exists():
        return []
    runs = []
    for line in config.METRICS.read_text().splitlines()[1:]:
        _ts, name, cls, phase, dur, rc, _log = line.split("\t")
        if phase == "pipeline" and rc == "0":
            runs.append((name, cls, int(dur)))
    return runs


def class_breakdown(projects: list[dict], states: dict) -> str:
    """ "S 3/10  M 1/4": validated over total, for each size class present."""
    per_class = []
    for cls in manifest_file.SIZE_CLASSES:
        members = [p for p in projects if p["size_class"] == cls]
        if members:
            hits = sum(1 for p in members if states[p["name"]] == "DONE")
            per_class.append(f"{cls} {hits}/{len(members)}")
    return "  ".join(per_class)


def cmd_progress(args: argparse.Namespace) -> int:
    """A one-screen progress bar and the last finished projects."""
    projects = manifest_file.read_manifest(config.CORPUS / args.manifest, None)
    states = {p["name"]: retain.project_state(p["name"], config.OUT / p["name"]) for p in projects}
    by_state: dict[str, list[dict]] = {}
    for p in projects:
        by_state.setdefault(states[p["name"]], []).append(p)
    done, failed = len(by_state.get("DONE", [])), len(by_state.get("FAILED", []))
    total = len(projects)
    pct = 100 * done / total if total else 0.0

    print(f"corpus  [{bar(done, total)}]  {done}/{total}  {pct:.1f}%")
    print(
        f"        {class_breakdown(projects, states)}"
        f"{'  |  FAILED ' + str(failed) if failed else ''}"
    )
    print_listing(
        "running", [f"{p['name']:<34} {p['size_class']}" for p in by_state.get("RUNNING", [])]
    )
    recent = finished_runs()[-args.last :]
    print_listing(
        f"last {len(recent)} finished",
        [f"{name:<34} {cls}  {dur / 60:6.1f} min" for name, cls, dur in reversed(recent)],
    )
    return 0


def print_listing(title: str, rows: list[str]) -> None:
    """A blank line, the title and the indented rows; nothing when rows is empty."""
    if rows:
        print(f"\n{title}")
        for row in rows:
            print(f"  {row}")


AUDIT_FIELDS = ("start_utc", "step", "status", "exit_code", "duration_s")


def audit_line(row: dict) -> str:
    cells = [str(row.get(k, "")) for k in AUDIT_FIELDS]
    notes = []
    if row.get("step") == ledger.FINAL_STEP:
        notes.append(f"state={row.get('state')}")
        if row.get("failed_step"):
            notes.append(f"failed_step={row['failed_step']}")
    if row.get("parquet", {}).get("sha256") and row.get("step") in ("validate", ledger.FINAL_STEP):
        pq = row["parquet"]
        notes.append(f"parquet rows={pq.get('rows')} sha256={pq['sha256'][:16]}")
    if row.get("step") == "pipeline" and row.get("checked_out_sha"):
        notes.append(f"checked_out={row['checked_out_sha']} pinned={row.get('pinned_sha')}")
    if row.get("cleanup"):
        c = row["cleanup"]
        notes.append(f"removed={len(c.get('removed', []))} errors={len(c.get('errors', []))}")
    if row.get("detail"):
        notes.append(str(row["detail"]))
    return "  ".join(cells + notes)


def interrupted(rows: list[dict]) -> list[dict]:
    """rows without each started row whose step ended; one that never ended is kept,
    marked interrupted: its process died mid-step."""
    ended = {
        (r.get("run_id"), r.get("attempt"), r.get("step"))
        for r in rows
        if r.get("status") != "started"
    }
    out = []
    for r in rows:
        if r.get("status") != "started":
            out.append(r)
        elif (r.get("run_id"), r.get("attempt"), r.get("step")) not in ended:
            out.append(
                {
                    **r,
                    "status": "interrupted",
                    "detail": "started, and no end row: the run died here",
                }
            )
    return out


def cmd_audit(args: argparse.Namespace) -> int:
    """Print one project's ledger rows; --json prints every row whole."""
    rows = ledger.project_rows(ledger.read(config.LEDGER), args.project)
    if not rows:
        print(f"{args.project}: no rows in {config.LEDGER}", file=sys.stderr)
        return 1
    for row in rows if args.json else interrupted(rows):
        print(json.dumps(row, ensure_ascii=False) if args.json else audit_line(row))
    return 0


def cmd_db(args: argparse.Namespace) -> int:
    """Rebuild ctp.duckdb (derived index over stamps/metrics/parquets)."""
    config.OUT.mkdir(parents=True, exist_ok=True)
    runtime.env.update(toolchain.capture_devenv_env())
    toolchain.forward_config()
    manifests = [
        arg
        for m in (getattr(args, "manifest", None) or [])
        for arg in ("--manifest", str((config.CORPUS / m).resolve()))
    ]
    return subprocess.run(
        ["python3", str(config.CORPUS / "consolidate.py"), *manifests],
        cwd=config.CREGIT,
        env=runtime.env,
    ).returncode


def add_project_options(p: argparse.ArgumentParser) -> None:
    """The options of one project run, shared by run and census."""
    p.add_argument(
        "--skip-html",
        action="store_true",
        help="never generate the HTML views (94-255 MB per project)",
    )
    p.add_argument(
        "--tokenizer-worker",
        action="store_true",
        help="tokenize in step 2 with the runner's worker pool (long-lived "
        "tokenizer processes) instead of one process per blob",
    )
    p.add_argument(
        "--reblame",
        action="store_true",
        help="re-blame every file in step 7 instead of skipping those "
        "with .blame output, after the blame itself changed. "
        "Not for resuming an interrupted run",
    )
    p.add_argument(
        "--drop-memo",
        "--no-memo",
        action="store_true",
        help="delete memo/ (45-88%% of the workdir) once a project "
        "validates. The tokenizer still writes it first",
    )
    p.add_argument(
        "--memo-dir",
        default="",
        metavar="DIR",
        help="keep each project's memo in DIR/<project>, outside the "
        "workdir a step-1 run deletes. DIR must exist. "
        "Cannot be combined with --drop-memo",
    )
    p.add_argument(
        "--shards",
        type=int,
        default=0,
        help="tokenize in N shards (needs >1). Costs transient disk, "
        "so it is limited to --shard-classes",
    )
    p.add_argument(
        "--shard-classes", default="L", help="comma-separated size classes to shard (default L)"
    )
    p.add_argument(
        "--from-step",
        type=int,
        default=1,
        metavar="N",
        help="resume the runner at step N. Only step 1 wipes the "
        "workdir, so N>1 keeps finished work",
    )
    p.add_argument(
        "--retokenize",
        metavar="EXTS",
        default="",
        help="re-tokenize only these extensions (comma separated, no "
        "dots), after a tokenizer fix. Needs --from-step 2 "
        "exactly; not with --mask-widened or sharding",
    )
    p.add_argument(
        "--mask-widened",
        action="store_true",
        help="reuse existing tokenizations across a mask change. "
        "Needs --from-step 2 or more; blobExec verifies each project",
    )
    p.add_argument(
        "--gc",
        choices=("none", "plain", "aggressive"),
        help="how the runner packs the generated repo. Omit for the runner's default",
    )
    p.add_argument(
        "--memory-limit",
        metavar="SIZE",
        help="DuckDB memory limit for step 10, an absolute size such "
        "as 3GB. The process settles at about 1.4x, so budget "
        "1.4 x --jobs x SIZE. Omit for the generator's 8GB",
    )
    p.add_argument(
        "--duckdb-threads",
        type=int,
        default=0,
        metavar="N",
        help="DuckDB threads for step 10; fewer lower the peak. Omit for the generator's default",
    )
    p.add_argument(
        "--project-meta",
        default="",
        metavar="PATH",
        help="JSON provenance sidecar keyed by project name (format: "
        "validate_schema.py, above CREGIT_COLUMNS). Without it "
        "29 columns are blank and ctp refuses the run",
    )
    p.add_argument(
        "--allow-empty-provenance",
        action="store_true",
        help="publish with blank provenance columns, e.g. "
        "for a fixture or a smoke run. Refused when nothing "
        "would be blank",
    )
    p.add_argument(
        "--mask",
        default="",
        metavar="REGEX",
        help="tokenize these files instead of the manifest's mask. "
        "Use with --only: a changed mask forces a full rebuild",
    )
    p.add_argument(
        "--blame-jobs",
        type=int,
        default=0,
        metavar="N",
        help="parallel blame workers; the output does not depend "
        "on N. Omit for the runner's default of 1",
    )
    p.add_argument(
        "--drop-html",
        action="store_true",
        help="with --cleanup, delete html/ too. Off by default: an open decision",
    )
    p.add_argument(
        "--anonymize",
        metavar="SCRIPT",
        default="",
        help="run `python3 SCRIPT <workdir>/anon <parquet>` after the "
        "schema check. Off by default: an open decision",
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    run_p = sub.add_parser("run", help="run the corpus")
    run_p.add_argument("--jobs", type=int, default=2, help="concurrent projects")
    run_p.add_argument("--retries", type=int, default=1, help="extra passes over failed projects")
    run_p.add_argument("--only", help="comma-separated project names to restrict to")
    run_p.add_argument("--manifest", default="manifest.tsv")
    add_project_options(run_p)
    run_p.add_argument(
        "--join",
        action="store_true",
        help="after each validated project, check its schema with "
        "validate_schema.py and join it into ctp.duckdb",
    )
    run_p.add_argument(
        "--cleanup",
        action="store_true",
        help="after each validated project, delete the clones, memo/, "
        "blame/ and work databases; keep the Parquet, stamp and logs",
    )
    run_p.set_defaults(fn=cmd_run)

    ce_p = sub.add_parser(
        "census",
        help="every project end to end: clone at the pinned commit, pipeline, "
        "validate, schema, join, cleanup; resumable",
    )
    ce_p.add_argument("--manifest", default="manifest.tsv")
    ce_p.add_argument(
        "--workers",
        "--jobs",
        dest="workers",
        type=int,
        default=2,
        help="projects at once (default 2)",
    )
    ce_p.add_argument(
        "--retries",
        type=int,
        default=1,
        help="retries per failed project, counted across restarts (default 1)",
    )
    ce_p.add_argument("--only", help="comma-separated project names to restrict to")
    ce_p.add_argument(
        "--latest",
        action="store_true",
        help="pin each project to the remote head of its default branch when "
        "its first attempt starts, not to the manifest commit. The ledger "
        "records both; state/<name>/latest.json keeps the choice for "
        "later attempts (delete it to read the head again)",
    )
    ce_p.add_argument(
        "--min-free-mem-gb",
        type=float,
        default=MIN_FREE_MEM_GB,
        metavar="G",
        help=f"start a project only with this much MemAvailable (default {MIN_FREE_MEM_GB})",
    )
    ce_p.add_argument(
        "--disk-floor-gb",
        type=int,
        default=config.DISK_FLOOR_GB,
        metavar="G",
        help=f"start a project only with this much free disk in the output "
        f"directory (default {config.DISK_FLOOR_GB})",
    )
    ce_p.add_argument(
        "--stop-file",
        default="",
        metavar="PATH",
        help="touch this file to stop after the running projects "
        "(default: STOP in this repository)",
    )
    ce_p.add_argument(
        "--poll",
        type=float,
        default=POLL_S,
        metavar="S",
        help=f"seconds between gate and stop checks (default {POLL_S:g})",
    )
    add_project_options(ce_p)
    ce_p.set_defaults(fn=cmd_census)

    st_p = sub.add_parser("status", help="one-screen pipeline status")
    st_p.add_argument("--manifest", default="manifest.tsv")
    st_p.set_defaults(fn=cmd_status)

    pr_p = sub.add_parser("progress", help="progress bar and last finished projects")
    pr_p.add_argument("--manifest", default="manifest.tsv")
    pr_p.add_argument(
        "--last",
        type=int,
        default=5,
        metavar="N",
        help="how many finished projects to list (default 5)",
    )
    pr_p.set_defaults(fn=cmd_progress)

    au_p = sub.add_parser("audit", help="one project's ledger rows")
    au_p.add_argument("project")
    au_p.add_argument("--json", action="store_true", help="print each row whole")
    au_p.set_defaults(fn=cmd_audit)

    db_p = sub.add_parser("db", help="rebuild ctp.duckdb (tracking table + unified tokens view)")
    db_p.add_argument(
        "--manifest",
        action="append",
        metavar="PATH",
        help="manifest to index, repeatable (default manifest.tsv)",
    )
    db_p.set_defaults(fn=cmd_db)

    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
