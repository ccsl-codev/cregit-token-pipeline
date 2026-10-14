"""ctp.py census: run every project end to end in manifest order. Gates on memory and
disk, a stop file and signals control when a project starts."""

from __future__ import annotations

import argparse
import contextlib
import os
import shutil
import signal
import sys
import threading
import time
from collections import Counter, deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from pathlib import Path

import ledger
from ctplib import (
    config,
    console,
    manifest_file,
    memory,
    passes,
    preflight,
    project_run,
    project_steps,
    run_ledger,
    runtime,
    toolchain,
)

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
    """One queued project of a dataset run."""

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


def initial_queue(projects: list[dict], retries: int) -> tuple[dict, deque]:
    """Plan each project from its ledger history. A skipped or given-up project gets its
    outcome now; every other project gets a Job in the queue."""
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
    return results, queue


def note_stop(stop_file: Path, running: int) -> str | None:
    """The stop reason, or None. The first reason goes into the run record and the log."""
    reason = stop_reason(stop_file)
    if reason and not runtime.run_info.get("stop_reason"):
        runtime.run_info["stop_reason"] = reason
        console.say(f"stopping: {reason}. No new project starts; {running} running will finish")
    return reason


def finished_outcome(fut, name: str) -> project_run.RunOutcome:
    try:
        return fut.result()
    except Exception as exc:  # one project must not end the run
        console.say(f"{name} ✗ crashed the worker: {type(exc).__name__}: {exc}")
        return project_run.RunOutcome.FAILED


def requeue(
    job: Job, outcome: project_run.RunOutcome, queue: deque, retries: int, poll: float
) -> None:
    """Queue a failed job again while it has retries left, and a deferred job after a wait."""
    name = job.project["name"]
    if outcome == project_run.RunOutcome.FAILED:
        job.failures += 1
        if job.failures > retries:
            return
        console.say(f"{name} — failed, retry {job.failures} of {retries} queued")
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


@dataclass
class RunQueue:
    """The queue, the running jobs and the results of one schedule() call."""

    queue: deque
    results: dict
    total: int
    workers: int
    retries: int
    poll: float
    running: dict = field(default_factory=dict)
    wait_logged: bool = False

    def try_start(self, pool: ThreadPoolExecutor, reason: str | None) -> bool:
        """Start the next job, unless a stop, a full pool, a gate or a wait holds it.
        True when a job started."""
        if reason or not self.queue or len(self.running) >= self.workers:
            return False
        gate = gate_reason(runtime.opts)
        job = next_job(self.queue, time.time()) if gate is None else None
        if job is not None:
            self.wait_logged = False
            fut = pool.submit(
                project_steps.run_project, job.project, job.pos, self.total, job.attempt, job.post
            )
            self.running[fut] = job
            return True
        if gate is not None and not self.wait_logged:
            self.wait_logged = True
            console.say(f"waiting to start {self.queue[0].project['name']}: {gate}")
        return False

    def collect(self) -> None:
        """Wait up to poll seconds, then record and requeue each finished job."""
        done, _ = wait(list(self.running), timeout=self.poll, return_when=FIRST_COMPLETED)
        for fut in done:
            job = self.running.pop(fut)
            name = job.project["name"]
            self.results[name] = finished_outcome(fut, name)
            requeue(job, self.results[name], self.queue, self.retries, self.poll)


def schedule(
    projects: list[dict], workers: int, retries: int, stop_file: Path, poll: float = POLL_S
) -> dict:
    """Run projects in manifest order with up to `workers` at once. Returns name -> outcome."""
    results, queue = initial_queue(projects, retries)
    skipped = sum(1 for v in results.values() if v == project_run.RunOutcome.SKIPPED)
    console.say(
        f"census: {len(projects)} projects, {skipped} already done, {len(queue)} to run, "
        f"{workers} workers"
    )

    stop_heartbeat = threading.Event()
    threading.Thread(target=passes.heartbeat, args=(stop_heartbeat, results), daemon=True).start()
    jobs = RunQueue(queue, results, len(projects), workers, retries, poll)
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            while jobs.queue or jobs.running:
                reason = note_stop(stop_file, len(jobs.running))
                if jobs.try_start(pool, reason):
                    continue
                if jobs.running:
                    jobs.collect()
                    continue
                if reason or not jobs.queue:
                    break
                time.sleep(poll)
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


def stop_file_of(args: argparse.Namespace) -> Path:
    """The stop file, after the checks that refuse to start: it must not exist yet."""
    stop_file = Path(args.stop_file).resolve() if args.stop_file else config.CORPUS / STOP_NAME
    if stop_file.exists():
        sys.exit(f"{stop_file} exists, so the census would stop at once. Remove it to start.")
    if args.workers < 1:
        sys.exit("--workers must be at least 1")
    return stop_file


def set_run_options(args: argparse.Namespace) -> None:
    """Join and clean up every project, resume a leftover workdir, gate on memory."""
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


def run_exit_code(results: dict, stopped: bool) -> int:
    """0 when every project succeeded, 3 when they did but a stop cut the run short, else 1."""
    rc = 0 if all(v.is_success for v in results.values()) and not stopped else 1
    if stopped and all(v.is_success for v in results.values()):
        rc = 3
    return rc


def cmd_census(args: argparse.Namespace) -> int:
    """Every project of the manifest, end to end, in manifest order."""
    stop_file = stop_file_of(args)
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

    set_run_options(args)
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
    run_ledger.start_ledger_run(args, manifest, "census")
    install_signal_handlers()
    with config.RUNS_LOG.open("a") as f:
        f.write(
            f"{console.now_iso()}\tcensus-start\tworkers={args.workers}\tprojects={len(projects)}\n"
        )

    results = schedule(projects, args.workers, args.retries, stop_file, args.poll)

    stopped = bool(runtime.run_info.get("stop_reason"))
    rc = run_exit_code(results, stopped)
    run_ledger.end_ledger_run(rc, run_start, results)
    with config.RUNS_LOG.open("a") as f:
        f.write(
            f"{console.now_iso()}\tcensus-end\trc={rc}\tduration_s={int(time.time() - run_start)}\n"
        )
    counts = Counter(str(v) for v in results.values())
    console.say(
        f"census finished rc={rc}: "
        + ", ".join(f"{k} {n}" for k, n in sorted(counts.items()))
        + (f" (stopped: {runtime.run_info['stop_reason']})" if stopped else "")
    )
    return rc
