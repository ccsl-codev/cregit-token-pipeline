"""Run one phase of one project as a child process, with its own log file and metrics row."""

from __future__ import annotations

import subprocess
import time
from datetime import datetime
from pathlib import Path

from ctplib import config, console, metrics, resources, runtime
from retain import state_dir


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


def latest_log(name: str, phase: str) -> str:
    link = state_dir(name) / "logs" / f"{phase}-latest.log"
    try:
        return str(link.resolve(strict=True))
    except OSError:
        return ""
