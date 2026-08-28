#!/usr/bin/env python3
"""Corpus runner: cregit over every project in manifest.tsv.

    ./corpus_runner.py run [--jobs 2] [--retries 1] [--only jq,zstd]
    ./corpus_runner.py status

Per project: pipeline (devenv shell) -> validate -> stamp. Idempotent — a
validated project is skipped; a failed/interrupted one resumes via blobExec's
incremental engine. Stdlib only.

Benchmarking/visibility contract (shared with the previous shell runner):
  - metrics.tsv   append-only ledger, one row per phase attempt:
                  iso_start  project  class  phase  duration_s  rc  log
  - runs.log      one start/end row per runner invocation
  - logs are never overwritten: corpus-files/<name>/logs/<phase>-<ts>.log
    (<phase>-latest.log symlink points at the newest attempt)
  - live progress: event lines on phase start/end + heartbeat summary every
    30s (RUNNING projects with elapsed time, done/failed counts, disk free)
"""

from __future__ import annotations

import argparse
import fcntl
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import configparser

# .resolve() canonicalizes to /local/home form — blobExec's meta table refuses
# resume if the command path string drifts (/home vs /local/home symlink alias).
CORPUS = Path(__file__).resolve().parent
_cfg = configparser.ConfigParser()
_cfg.read(CORPUS / "pipeline.cfg")


def _cfg_path(key: str, default: str) -> Path:
    raw = _cfg.get("paths", key, fallback=default)
    return (CORPUS / Path(raw).expanduser()).resolve()


CREGIT = _cfg_path("cregit_dir", "../cregit-workspace/cregit")
OUT = _cfg_path("output_dir", "../cregit-workspace/corpus-files")
OUT.mkdir(parents=True, exist_ok=True)
METRICS = CORPUS / "metrics.tsv"
RUNS_LOG = CORPUS / "runs.log"

DEVENV = Path.home() / ".nix-profile/bin/devenv"
# devenv needs the nix daemon env; a bare PATH prepend fails in non-login
# shells with: error: not an absolute path: "nix"
NIX_DAEMON_SH = "/nix/var/nix/profiles/default/etc/profile.d/nix-daemon.sh"

DISK_FLOOR_GB = 150
HEARTBEAT_S = 30

_metrics_lock = threading.Lock()
_print_lock = threading.Lock()
# name -> (phase, started_at) for the heartbeat; mutated by worker threads
_live: dict = {}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def say(msg: str) -> None:
    with _print_lock:
        print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def read_manifest(path: Path, only: set | None) -> list[dict]:
    projects = []
    for line in path.read_text().splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        name, url, category, file_filter, size_class = line.split("\t")
        if only and name not in only:
            continue
        projects.append(dict(name=name, url=url, category=category,
                             file_filter=file_filter, size_class=size_class))
    return projects


def record_metric(project: str, cls: str, phase: str, duration: int, rc: int, log: Path) -> None:
    with _metrics_lock:
        if not METRICS.exists():
            METRICS.write_text("iso_start\tproject\tclass\tphase\tduration_s\trc\tlog\n")
        with METRICS.open("a") as f:
            f.write(f"{now_iso()}\t{project}\t{cls}\t{phase}\t{duration}\t{rc}\t{log}\n")


# Captured once by cmd_run; concurrent `devenv shell` invocations race on the
# GC root in CREGIT ("Failed to remove existing GC root"), so jobs must not
# each start their own devenv.
_ENV: dict = {}


def capture_devenv_env() -> dict:
    """Resolve the devenv shell once and return its full environment."""
    out = subprocess.run(
        ["bash", "-c",
         f'. {NIX_DAEMON_SH}; cd "{CREGIT}"; exec "{DEVENV}" shell -- '
         'python3 -c "import os, json; print(json.dumps(dict(os.environ)))"'],
        capture_output=True, text=True, timeout=300)
    if out.returncode != 0:
        sys.exit(f"devenv environment capture failed:\n{out.stdout}\n{out.stderr}")
    import json
    return json.loads(out.stdout.splitlines()[-1])


def run_phase(project: dict, phase: str, args: list[str]) -> int:
    name, cls = project["name"], project["size_class"]
    logdir = OUT / name / "logs"
    logdir.mkdir(parents=True, exist_ok=True)
    logfile = logdir / f"{phase}-{datetime.now():%Y%m%dT%H%M%S}.log"

    _live[name] = (phase, time.time())
    say(f"{name} ▶ {phase} started (log: {logfile})")
    latest = logdir / f"{phase}-latest.log"
    tmp = logdir / f".{phase}-latest.tmp"
    logfile.touch()
    tmp.symlink_to(logfile)
    tmp.rename(latest)
    start = time.time()
    with logfile.open("wb") as lf:
        rc = subprocess.run(args, cwd=CREGIT, env=_ENV,
                            stdout=lf, stderr=subprocess.STDOUT).returncode
    duration = int(time.time() - start)

    record_metric(name, cls, phase, duration, rc, logfile)
    mark = "✓" if rc == 0 else f"✗ rc={rc}"
    say(f"{name} {mark} {phase} in {duration}s")
    return rc


def run_project(project: dict) -> str:
    """Returns: done | skipped | failed | deferred"""
    name = project["name"]
    workdir = OUT / name
    stamp = workdir / f"{name}.validated"
    if stamp.exists():
        say(f"{name} — already validated, skip")
        return "skipped"

    workdir.mkdir(parents=True, exist_ok=True)

    # Single-instance guard per project (held for the whole job).
    lockfile = (workdir / ".lock").open("w")
    try:
        fcntl.flock(lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        say(f"{name} — another run holds the lock, skipping")
        return "deferred"

    try:
        if shutil.disk_usage(OUT).free < DISK_FLOOR_GB * 2**30:
            say(f"{name} — disk below {DISK_FLOOR_GB}G floor, deferred")
            return "deferred"

        rc = run_phase(project, "pipeline", [
            "./run_pipeline_process.sh",
            "--repo-url", project["url"],
            "--repo-name", name,
            "--work-dir", str(workdir),
            "--file-filter", project["file_filter"],
        ])
        if rc != 0:
            return "failed"

        rc = run_phase(project, "validate", [
            "python3", str(CORPUS / "validate.py"),
            str(workdir / f"{name}-dataset.parquet"), str(stamp),
        ])
        return "done" if rc == 0 else "failed"
    finally:
        _live.pop(name, None)
        lockfile.close()


def heartbeat(stop: threading.Event, results: dict) -> None:
    while not stop.wait(HEARTBEAT_S):
        running = ", ".join(f"{n}({p} {int(time.time() - t)}s)" for n, (p, t) in sorted(_live.items()))
        free_gb = shutil.disk_usage(OUT).free // 2**30
        done = sum(1 for v in results.values() if v in ("done", "skipped"))
        failed = sum(1 for v in results.values() if v == "failed")
        say(f"♥ running: {running or '—'} | done {done} failed {failed} | disk {free_gb}G free")


def cmd_run(args: argparse.Namespace) -> int:
    projects = read_manifest(CORPUS / args.manifest, set(args.only.split(",")) if args.only else None)
    if not projects:
        say("nothing to run (empty manifest / --only filter matched nothing)")
        return 0

    run_start = time.time()
    say("capturing devenv environment (once)...")
    _ENV.update(capture_devenv_env())
    say(f"devenv environment captured ({len(_ENV)} vars)")
    with RUNS_LOG.open("a") as f:
        f.write(f"{now_iso()}\trun-start\tjobs={args.jobs}\tprojects={len(projects)}\n")

    results: dict = {}
    stop = threading.Event()
    hb = threading.Thread(target=heartbeat, args=(stop, results), daemon=True)
    hb.start()

    try:
        for attempt in range(1 + args.retries):
            todo = [p for p in projects if results.get(p["name"]) not in ("done", "skipped")]
            if not todo:
                break
            if attempt:
                say(f"retry pass {attempt}: {[p['name'] for p in todo]}")
            with ThreadPoolExecutor(max_workers=args.jobs) as pool:
                for p, res in zip(todo, pool.map(run_project, todo)):
                    results[p["name"]] = res
    finally:
        stop.set()

    rc = 0 if all(v in ("done", "skipped") for v in results.values()) else 1
    with RUNS_LOG.open("a") as f:
        f.write(f"{now_iso()}\trun-end\trc={rc}\tduration_s={int(time.time() - run_start)}\n")

    say(f"run finished rc={rc} — " + " ".join(f"{n}:{v}" for n, v in sorted(results.items())))
    return rc


def cmd_status(args: argparse.Namespace) -> int:
    projects = read_manifest(CORPUS / args.manifest, None)
    last: dict = {}
    if METRICS.exists():
        for line in METRICS.read_text().splitlines()[1:]:
            ts, name, _cls, phase, dur, rc, _log = line.split("\t")
            last[name] = f"{ts} {phase}({dur}s,rc={rc})"

    print(f"{'PROJECT':<16} {'CLASS':<6} {'STATE':<9} LAST_ACTIVITY")
    done = 0
    for p in projects:
        workdir = OUT / p["name"]
        if (workdir / f"{p['name']}.validated").exists():
            state, done = "DONE", done + 1
        elif _lock_held(workdir / ".lock"):
            state = "RUNNING"
        elif workdir.exists():
            state = "FAILED"
        else:
            state = "QUEUED"
        print(f"{p['name']:<16} {p['size_class']:<6} {state:<9} {last.get(p['name'], '—')}")

    free_gb = shutil.disk_usage(OUT).free // 2**30
    print(f"\nprogress: {done}/{len(projects)} validated | disk {free_gb}G free")
    return 0


def _lock_held(lockfile: Path) -> bool:
    if not lockfile.exists():
        return False
    with lockfile.open("w") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return False
        except BlockingIOError:
            return True


def cmd_db(args: argparse.Namespace) -> int:
    """Rebuild corpus.duckdb (derived index over stamps/metrics/parquets)."""
    _ENV.update(capture_devenv_env())
    return subprocess.run(["python3", str(CORPUS / "consolidate.py")],
                          cwd=CREGIT, env=_ENV).returncode


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    run_p = sub.add_parser("run", help="run the corpus")
    run_p.add_argument("--jobs", type=int, default=2, help="concurrent projects")
    run_p.add_argument("--retries", type=int, default=1, help="extra passes over failed projects")
    run_p.add_argument("--only", help="comma-separated project names to restrict to")
    run_p.add_argument("--manifest", default="manifest.tsv")
    run_p.set_defaults(fn=cmd_run)

    st_p = sub.add_parser("status", help="one-screen corpus status")
    st_p.add_argument("--manifest", default="manifest.tsv")
    st_p.set_defaults(fn=cmd_status)

    db_p = sub.add_parser("db", help="rebuild corpus.duckdb (tracking table + unified tokens view)")
    db_p.set_defaults(fn=cmd_db)

    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
