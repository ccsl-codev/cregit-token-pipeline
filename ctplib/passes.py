"""ctp.py run: run every project in a thread pool, then retry the failed ones in more passes."""

from __future__ import annotations

import argparse
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from ctplib import (
    config,
    console,
    manifest_file,
    preflight,
    project_run,
    project_steps,
    run_ledger,
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
        preflight.require_commit_url()
    preflight.announce_run(runtime.opts, args.jobs)

    run_start = time.time()
    console.say("capturing devenv environment (once)...")
    runtime.env.update(toolchain.capture_devenv_env())
    # A deleted or private repository must fail its clone, not wait for a password.
    runtime.env.setdefault("GIT_TERMINAL_PROMPT", "0")
    console.say(f"devenv environment captured ({len(runtime.env)} vars)")
    run_ledger.start_ledger_run(args, config.CORPUS / args.manifest, "run")
    with config.RUNS_LOG.open("a") as f:
        f.write(f"{console.now_iso()}\trun-start\tjobs={args.jobs}\tprojects={len(projects)}\n")

    results = run_passes(projects, args.jobs, args.retries)

    rc = 0 if all(v.is_success for v in results.values()) else 1
    run_ledger.end_ledger_run(rc, run_start, results)
    with config.RUNS_LOG.open("a") as f:
        f.write(
            f"{console.now_iso()}\trun-end\trc={rc}\tduration_s={int(time.time() - run_start)}\n"
        )

    console.say(
        f"run finished rc={rc} — " + " ".join(f"{n}:{v}" for n, v in sorted(results.items()))
    )
    return rc
