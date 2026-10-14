#!/usr/bin/env python3
"""ctp — cregit-token-pipeline runner: runs cregit over every project in manifest.tsv.
Per project: pipeline -> firm -> validate -> stamp; a validated project is skipped.
Ledgers: metrics.tsv (one row per phase attempt), runs.log, state/<name>/logs (never overwritten)."""

from __future__ import annotations

import argparse
import contextlib
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
from dataclasses import dataclass
from pathlib import Path

import ledger
import retain
from ctplib import (
    artifacts,
    config,
    console,
    manifest_file,
    memory,
    preflight,
    project_run,
    project_steps,
    runtime,
    toolchain,
)

HEARTBEAT_S = 30


def heartbeat(stop: threading.Event, results: dict) -> None:
    while not stop.wait(HEARTBEAT_S):
        running = ", ".join(
            f"{n}({p} {int(time.time() - t)}s)" for n, (p, t) in sorted(runtime.live.items())
        )
        free_gb = shutil.disk_usage(config.OUT).free // 2**30
        done = sum(1 for v in results.values() if v.is_success)
        failed = sum(1 for v in results.values() if v == project_run.RunOutcome.FAILED)
        console.say(
            f"♥ running: {running or '—'} | done {done} failed {failed} | disk {free_gb}G free"
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
                for p, res in zip(todo, pool.map(project_steps.run_project, todo), strict=True):
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

    runtime.opts.update(preflight.run_options(args))
    if any(p.get(manifest_file.PINNED_FIELD) for p in projects):
        preflight.refuse_unless_runner_accepts(
            "a pinned manifest",
            ("--commit-url",),
            " Pinned projects run from a local staging clone, so the runner needs "
            "--commit-url to keep the real commit links.",
        )
    preflight.announce_run(runtime.opts, args.jobs)

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
            results[name] = project_run.RunOutcome.SKIPPED
        elif decision == GIVE_UP:
            console.say(
                f"{name} — failed {h.failures + h.interrupted} times "
                f"(last at {h.last_failed_step or '?'}), more than --retries {retries}; "
                "not retried"
            )
            results[name] = project_run.RunOutcome.FAILED
        else:
            queue.append(
                Job(pos, project, h.attempts + 1, h.failures + h.interrupted, decision == POST)
            )
    skipped = sum(1 for v in results.values() if v == project_run.RunOutcome.SKIPPED)
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
                            project_steps.run_project,
                            job.project,
                            job.pos,
                            total,
                            job.attempt,
                            job.post,
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
                        outcome = project_run.RunOutcome.FAILED
                    results[name] = outcome
                    if outcome == project_run.RunOutcome.FAILED:
                        job.failures += 1
                        if job.failures <= retries:
                            console.say(
                                f"{name} — failed, retry {job.failures} of {retries} queued"
                            )
                            job.attempt += 1
                            job.post = (config.OUT / name / f"{name}.validated").exists()
                            queue.append(job)
                    elif outcome == project_run.RunOutcome.DEFERRED:
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
    runtime.opts.update(preflight.run_options(args))
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
        preflight.refuse_unless_runner_accepts(
            "a pinned manifest",
            ("--commit-url",),
            " Pinned projects run from a local staging clone, so the runner needs "
            "--commit-url to keep the real commit links.",
        )
    preflight.announce_run(runtime.opts, args.workers)
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
