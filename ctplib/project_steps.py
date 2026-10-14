"""The steps of one project, and run_project, which runs them in order."""

from __future__ import annotations

import fcntl
import json
import shutil
import time
from functools import partial
from pathlib import Path

import ledger
import retain
from ctplib import (
    artifacts,
    config,
    console,
    manifest_file,
    pinning,
    project_run,
    remote_head,
    runner_args,
    runtime,
    toolchain,
    workdir_state,
)
from retain import lock_path, state_dir


def check_pin_result(name: str, rc: int):
    """After the clone: pin.py must have written its result file."""
    if rc != 0:
        return rc, {}
    try:
        result = json.loads(pinning.pin_result_path(name).read_text())
    except (OSError, ValueError) as exc:
        return 1, {"detail": f"no pin result: {exc}"}
    return rc, {"pin": result}


def check_checkout(ctx: project_run.ProjectRun, workdir: Path, resume: dict, rc: int):
    """After the pipeline: the runner must have checked out the pinned commit."""
    extra = {"resume": resume} if resume else {}
    if rc != 0:
        return rc, extra
    ok, seen = workdir_state.checkout_matches(ctx.name, workdir, ctx.pinned)
    ctx.checked_out_sha = seen
    if not ok:
        detail = f"checked out {seen or 'nothing readable'}, not the pinned {ctx.pinned}"
        return 1, {"detail": detail, **extra}
    return rc, extra


def record_parquet(ctx: project_run.ProjectRun, rc: int):
    """After validate: keep the Parquet record for the ledger rows."""
    if rc != 0:
        return rc, {}
    ctx.parquet = artifacts.parquet_info(ctx.name)
    return rc, {"parquet": ctx.parquet}


def pipeline_options(ctx: project_run.ProjectRun, workdir: Path) -> tuple[dict, dict] | None:
    """The runner options and the resume record for this attempt. A workdir of another
    commit is wiped; a workdir with a clone and a blob map resumes at step 2.
    None when the derived artifacts cannot be dropped: the attempt failed."""
    name = ctx.name
    opts = runtime.opts
    resume: dict = {}
    stale = workdir_state.stale_workdir(name, workdir, ctx.pinned)
    if stale:
        console.say(
            f"{name} — the workdir holds commit {stale}, not {ctx.pinned}; "
            "the runner wipes it (--force-clean)"
        )
        resume = {"stale_workdir": stale}
        opts = {**runtime.opts, "force_clean": True}
    if not (runtime.opts.get("auto_resume") and runtime.opts.get("from_step", 1) == 1):
        return opts, resume
    step = 1 if stale else workdir_state.resume_step(name, workdir, ctx.pinned)
    if step <= 1:
        return opts, resume
    dropped, errors = workdir_state.drop_derived(name, workdir)
    console.say(
        f"{name} — an earlier attempt left its clone and blob map; resuming the "
        f"runner at step {step}" + (f", after dropping {', '.join(dropped)}" if dropped else "")
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
        return None
    return {**runtime.opts, "from_step": step}, resume


def build_steps(ctx: project_run.ProjectRun, workdir: Path, stamp: Path) -> bool:
    """clone (when pinned), pipeline, firm, validate. True when the project validated."""
    if ctx.pinned and runtime.opts.get("latest") and not remote_head.resolve_latest(ctx):
        return False
    if ctx.pinned:
        clone = pinning.pin_args(ctx.project, ctx.latest)
        if project_run.run_step(ctx, "clone", clone, partial(check_pin_result, ctx.name)) != 0:
            return False
    options = pipeline_options(ctx, workdir)
    if options is None:
        return False
    opts, resume = options
    pipeline = runner_args.build_pipeline_args(ctx.project, opts, workdir)
    after = partial(check_checkout, ctx, workdir, resume)
    if project_run.run_step(ctx, "pipeline", pipeline, after) != 0:
        return False
    # firm adds the 3 firm columns to cregit's 67; validate gates the 70.
    later = dict(runner_args.project_phases(ctx.project, runtime.opts, workdir, stamp))
    if project_run.run_step(ctx, "firm", later["firm"]) != 0:
        return False
    after = partial(record_parquet, ctx)
    return project_run.run_step(ctx, "validate", later["validate"], after) == 0


def publish_steps(ctx: project_run.ProjectRun) -> bool:
    """schema (validate_schema.py), anonymize (only with --anonymize), then join
    (an incremental consolidate into ctp.duckdb). True when every step passes."""
    name = ctx.name
    parquet = config.OUT / name / f"{name}-dataset.parquet"
    if (
        runtime.opts.get("join")
        and project_run.run_step(
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
            project_run.run_step(
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
        project_run.run_step(
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


def cleanup_step(ctx: project_run.ProjectRun) -> bool:
    """Remove the staging clone; then, with --cleanup, every bulky work file (the
    clones, memo/, blame/, the work databases), or only memo/ under --drop-memo.
    The Parquet, its stamp, the logs and the ledger stay. True when clean."""
    name = ctx.name
    start = time.time()
    errors = pinning.drop_pinned_clone(name)
    removed = [str(pinning.pinned_clone_path(name))] if ctx.pinned and not errors else []
    for err in errors:
        console.say(f"{name} — staging clone not fully removed: {err}")
    if runtime.opts.get("cleanup"):
        return cleanup_workdir(ctx, start, removed, errors)
    if runtime.opts.get("drop_memo"):
        drop_memo(name, removed, errors)
    ctx.record(
        "cleanup",
        "ok" if not errors else "failed",
        start,
        time.time(),
        cleanup={"removed": removed, "errors": errors},
    )
    return not errors


def cleanup_workdir(
    ctx: project_run.ProjectRun, start: float, removed: list[str], errors: list[str]
) -> bool:
    """--cleanup: delete every bulky work file and record what went. True when clean."""
    name = ctx.name
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


def drop_memo(name: str, removed: list[str], errors: list[str]) -> None:
    """--drop-memo: delete memo/ only. prune checks the keepers itself."""
    _reclaimed, ok = retain.prune(name, ("memo",), apply=True)
    if ok:
        removed.append("memo")
    else:
        errors.append("retain refused to prune memo/ (see the run log)")
        console.say(f"{name} — memo/ kept, retain refused the prune (see above)")


def validate_and_publish(
    ctx: project_run.ProjectRun, workdir: Path, stamp: Path, post_only: bool
) -> bool:
    """The steps up to validation (or, post_only, the record of an earlier validation),
    then schema, anonymize and join. True when every step passed."""
    if post_only:
        console.say(f"{ctx.name} — validated earlier; resuming at the steps after validation")
        ctx.parquet = artifacts.parquet_info(ctx.name)
        ctx.checked_out_sha = toolchain.runner_checkout(ctx.name, workdir)
    elif not build_steps(ctx, workdir, stamp):
        return False
    publishes = runtime.opts.get("join") or runtime.opts.get("anonymize")
    return not publishes or publish_steps(ctx)


def run_locked(
    ctx: project_run.ProjectRun, workdir: Path, stamp: Path, post_only: bool
) -> project_run.RunOutcome:
    """The steps of one project, with its lock held. Returns the final outcome."""
    name = ctx.name
    if not post_only and shutil.disk_usage(config.OUT).free < config.DISK_FLOOR_GB * 2**30:
        console.say(f"{name} — disk below {config.DISK_FLOOR_GB}G floor, deferred")
        return ctx.finish(ledger.DEFERRED, detail=f"disk below {config.DISK_FLOOR_GB}G")

    if not post_only and not workdir_state.check_memo_dir(name, runtime.opts, workdir):
        ctx.failed_step = "preflight"
        return ctx.finish(ledger.FAILED, detail="--memo-dir is inside the workdir")

    try:
        if not validate_and_publish(ctx, workdir, stamp, post_only):
            return ctx.finish(ledger.FAILED)
    except OSError as exc:
        console.say(f"{name} ✗ a phase could not start: {exc}")
        return ctx.finish(ledger.FAILED)

    clean = cleanup_step(ctx)
    return ctx.finish(ledger.DONE if clean else ledger.DONE_DIRTY)


def run_project(
    project: dict,
    queue_pos: int | None = None,
    queue_total: int | None = None,
    attempt: int = 1,
    resume_validated: bool = False,
) -> project_run.RunOutcome:
    """Runs one project's steps and appends a ledger row for each. Returns a RunOutcome.
    A validated project is skipped, unless resume_validated: then only the steps after
    validation run (schema, join, cleanup), as after a crash between them."""
    ctx = project_run.ProjectRun(project, queue_pos, queue_total, attempt)
    name = project["name"]
    if runtime.opts.get("latest") and project.get(manifest_file.PINNED_FIELD):
        # An attempt that only runs the steps after validation clones nothing, so
        # its rows carry the commit the validated run chose.
        ctx.latest = remote_head.recorded_latest(name)
    workdir = config.OUT / name
    stamp = workdir / f"{name}.validated"
    post_only = stamp.exists()
    if post_only and not resume_validated:
        console.say(f"{name} — already validated, skip")
        return project_run.RunOutcome.SKIPPED

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
            return run_locked(ctx, workdir, stamp, post_only)
        except Exception as exc:
            # A bug here must not leave the attempt without its final row.
            console.say(f"{name} ✗ internal error: {type(exc).__name__}: {exc}")
            ctx.failed_step = ctx.failed_step or "internal"
            return ctx.finish(ledger.FAILED, detail=f"internal error: {type(exc).__name__}: {exc}")
        finally:
            runtime.lock_fds.pop(name, None)
            runtime.live.pop(name, None)
