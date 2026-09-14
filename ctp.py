#!/usr/bin/env python3
"""ctp — cregit-token-pipeline runner. Runs cregit over every project in manifest.tsv.

    ./ctp.py run [--jobs 2] [--retries 1] [--only jq,zstd]
    ./ctp.py run --skip-html --drop-memo        # disk-frugal corpus run
    ./ctp.py status
    ./ctp.py db

Disk flags (the corpus does not fit otherwise — see retain.py and DESIGN.md §6):
  --skip-html  prevention. Forwarded to run_pipeline_process.sh, which skips the
               HTML step outright. 94-255 MB per project never written.
  --drop-memo  cleanup, NOT prevention. memo/ is 45-88% of a workdir but the
               tokenizer requires BFG_MEMO_DIR, so memo/ is always written; this
               deletes it (via retain.prune) once the project validates.

Per project: pipeline (devenv shell) -> validate -> stamp. Idempotent — a
validated project is skipped; a failed/interrupted one resumes via blobExec's
incremental engine. Stdlib only.

Benchmarking/visibility contract (shared with the previous shell runner):
  - metrics.tsv   append-only ledger, one row per phase attempt:
                  iso_start  project  class  phase  duration_s  rc  log
  - runs.log      one start/end row per runner invocation
  - logs are never overwritten: state/<name>/logs/<phase>-<ts>.log
    (<phase>-latest.log symlink points at the newest attempt). They live beside
    metrics.tsv, NOT in the project workdir, because run_pipeline_process.sh
    deletes that workdir on a clean restart. See STATE below.
  - live progress: event lines on phase start/end + heartbeat summary every
    30s (RUNNING projects with elapsed time, done/failed counts, disk free)
"""

from __future__ import annotations

import argparse
import fcntl
import os
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import configparser

import retain  # shared prune code path for --drop-memo

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

# run_pipeline_process.sh owns OUT/<name>: at FROM_STEP=1 it runs `rm -rf "$WORK"`
# for a clean restart, and its EXIT trap repeats that on failure. So the
# orchestrator must keep its own files somewhere else. Two things used to live in
# the wiped directory and both broke silently:
#   * logs/ — deleted while the pipeline was still writing into it, so the log of
#     the failure went to an unlinked inode and the evidence was lost.
#   * .lock — the open handle survived, so THIS process kept its flock, but a
#     second ctp.py created a new file and took its own lock. The
#     single-instance guard passed while guarding nothing.
# STATE sits beside metrics.tsv and runs.log, the other orchestrator artefacts.
# Keeping it out of OUT also preserves retain.py's invariant that every child of
# OUT is a project workdir (retain.check_target relies on that depth).
STATE = CORPUS / "state"

DEVENV = Path.home() / ".nix-profile/bin/devenv"
# devenv needs the nix daemon env; a bare PATH prepend fails in non-login
# shells with: error: not an absolute path: "nix"
NIX_DAEMON_SH = "/nix/var/nix/profiles/default/etc/profile.d/nix-daemon.sh"

DISK_FLOOR_GB = 150
HEARTBEAT_S = 30

# Resource ledger. Deliberately NOT extra columns on metrics.tsv: that file's
# seven-field row is a published contract with a test asserting the width, and
# every consumer splits on it. A second ledger costs one file and breaks nothing.
RESOURCES = CORPUS / "resources.tsv"
RESOURCE_SAMPLE_S = 15
RESOURCE_FIELDS = ("iso", "project", "class", "phase", "elapsed_s",
                   "tree_rss_mb", "tree_procs", "cpu_pct", "mem_used_mb",
                   "disk_free_gb", "load1")

# Flags run_project sends for every project. Keep this list and the pipeline_args
# list in run_project in step: cmd_run preflights these names against the
# configured checkout, so an upstream rename fails once instead of per project.
REQUIRED_RUNNER_FLAGS = ("--repo-url", "--repo-name", "--work", "--mask")

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

# Run options the worker threads need (disk flags). Set once by cmd_run, for the
# same reason as _ENV: run_project is called through pool.map and takes only the
# project dict.
_OPTS: dict = {}


def script_supports(flag: str) -> bool:
    """True when the configured run_pipeline_process.sh advertises `flag`."""
    try:
        return flag in (CREGIT / "run_pipeline_process.sh").read_text()
    except OSError:
        return False


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


def _proc_ppid_rss(proc_root: Path = Path("/proc")) -> dict[int, tuple[int, int]]:
    """Map pid -> (ppid, rss_kb) for every readable process.

    Read from /proc because ctp is stdlib-only. A process that exits between the
    listdir and the read is skipped, which is normal during a build: cregit starts
    and reaps short-lived perl and git processes constantly.

    `proc_root` is injectable so a test can present a malformed or vanishing
    entry without racing a real process.
    """
    table: dict[int, tuple[int, int]] = {}
    for entry in proc_root.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            # /proc/<pid>/stat: field 4 is ppid, field 24 is rss in pages. The
            # command name in field 2 may hold spaces and brackets, so split after
            # the closing parenthesis rather than on whitespace from the start.
            raw = (entry / "stat").read_text()
            fields = raw[raw.rindex(")") + 2:].split()
            table[int(entry.name)] = (int(fields[1]), int(fields[21]) * 4)
        except (OSError, ValueError, IndexError):
            continue
    return table


def tree_usage(root_pid: int, table: dict | None = None) -> tuple[int, int]:
    """Total resident memory in MB, and process count, for root_pid and its
    descendants.

    The pipeline is a shell that spawns perl, git and srcml, so the RSS of the
    direct child alone understates the run by a wide margin. Walking descendants
    is what makes the number usable for capacity planning.

    `table` is injectable so a test can supply a malformed parent map. The
    `pid in seen` guard exists for that case: a cycle must not hang the sampler,
    and the sampler runs inside every phase.
    """
    table = _proc_ppid_rss() if table is None else table
    children: dict[int, list[int]] = {}
    for pid, (ppid, _rss) in table.items():
        children.setdefault(ppid, []).append(pid)

    seen, stack, rss_kb = set(), [root_pid], 0
    while stack:
        pid = stack.pop()
        if pid in seen or pid not in table:
            continue
        seen.add(pid)
        rss_kb += table[pid][1]
        stack.extend(children.get(pid, ()))
    return rss_kb // 1024, len(seen)


def _cpu_jiffies() -> tuple[int, int]:
    """(busy, total) jiffies from /proc/stat, for a delta between two samples."""
    fields = [int(v) for v in
              Path("/proc/stat").read_text().split("\n")[0].split()[1:]]
    total = sum(fields)
    idle = fields[3] + (fields[4] if len(fields) > 4 else 0)
    return total - idle, total


def _mem_used_mb() -> int:
    """Used memory in MB: total minus available, per /proc/meminfo."""
    info = {}
    for line in Path("/proc/meminfo").read_text().splitlines():
        key, _, rest = line.partition(":")
        info[key] = int(rest.split()[0])
    return (info.get("MemTotal", 0) - info.get("MemAvailable", 0)) // 1024


class ResourceSampler:
    """Samples RAM, CPU and disk on an interval while one phase runs.

    Two outputs, because they answer different questions:

      resources.tsv  every sample, so a slow phase can be read as a time series
      .peak          the maxima, which is what capacity planning needs

    The thread is a daemon and every sample is wrapped, so a sampling failure can
    never fail the phase it is measuring. Measurement must not break the run.
    """

    def __init__(self, project: dict, phase: str, interval: int = RESOURCE_SAMPLE_S):
        self.name = project["name"]
        self.cls = project["size_class"]
        self.phase = phase
        self.interval = interval
        self.pid: int | None = None
        self.peak = dict(tree_rss_mb=0, tree_procs=0, cpu_pct=0.0,
                         mem_used_mb=0, disk_free_gb_min=None, samples=0)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._started = time.time()

    def start(self, pid: int) -> None:
        self.pid = pid
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> dict:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=self.interval + 5)
        return self.peak

    def _loop(self) -> None:
        busy, total = _cpu_jiffies()
        # Sample once immediately: a phase shorter than the interval would
        # otherwise record nothing at all.
        while True:
            try:
                busy, total = self._sample(busy, total)
            except Exception as e:                      # never break the phase
                say(f"{self.name} resource sample failed: {e}")
            if self._stop.wait(self.interval):
                return

    def _sample(self, prev_busy: int, prev_total: int) -> tuple[int, int]:
        busy, total = _cpu_jiffies()
        span = total - prev_total
        cpu_pct = round(100.0 * (busy - prev_busy) / span, 1) if span > 0 else 0.0
        rss_mb, procs = tree_usage(self.pid) if self.pid else (0, 0)
        mem_mb = _mem_used_mb()
        free_gb = shutil.disk_usage(OUT).free // 2**30
        row = (now_iso(), self.name, self.cls, self.phase,
               int(time.time() - self._started), rss_mb, procs, cpu_pct,
               mem_mb, free_gb, os.getloadavg()[0])

        p = self.peak
        p["tree_rss_mb"] = max(p["tree_rss_mb"], rss_mb)
        p["tree_procs"] = max(p["tree_procs"], procs)
        p["cpu_pct"] = max(p["cpu_pct"], cpu_pct)
        p["mem_used_mb"] = max(p["mem_used_mb"], mem_mb)
        p["disk_free_gb_min"] = (free_gb if p["disk_free_gb_min"] is None
                                 else min(p["disk_free_gb_min"], free_gb))
        p["samples"] += 1

        with _metrics_lock:
            if not RESOURCES.exists():
                RESOURCES.write_text("\t".join(RESOURCE_FIELDS) + "\n")
            with RESOURCES.open("a") as f:
                f.write("\t".join(str(v) for v in row) + "\n")
        return busy, total


def state_dir(name: str) -> Path:
    """The orchestrator's own directory for one project: logs and the lock.

    Never inside the project workdir. run_pipeline_process.sh deletes the workdir
    at FROM_STEP=1 and again from its EXIT trap on failure. Its existence also
    means "this project was attempted", which is how cmd_status tells FAILED from
    QUEUED after such a wipe.
    """
    return STATE / name


def lock_path(name: str) -> Path:
    """The project's single-instance lock. run_project and cmd_status must build
    this the same way, so neither may spell it out on its own."""
    return state_dir(name) / ".lock"


def shard_class(project: dict) -> bool:
    """True when this project should be tokenized in shards.

    Opt-in, and only for the size classes named by --shard-classes. Sharding buys
    tokenizer throughput and costs transient disk, so it is worth it exactly where
    one project would otherwise leave the box idle: the L class. For S and M,
    three concurrent projects already fill the machine and sharding would only
    multiply the disk.
    """
    return (_OPTS.get("shards", 0) > 1
            and project["size_class"] in _OPTS.get("shard_classes", ()))


def run_phase(project: dict, phase: str, args: list[str]) -> int:
    name, cls = project["name"], project["size_class"]
    logdir = state_dir(name) / "logs"
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
    sampler = ResourceSampler(project, phase)
    # Popen, not run: the sampler needs the child's pid to walk its descendants.
    # The pipeline is a shell that spawns perl, git and srcml, so the direct
    # child's own RSS understates the run badly. At --jobs 3 a system-wide figure
    # cannot be attributed to a project, and attribution is the point.
    with logfile.open("wb") as lf:
        proc = subprocess.Popen(args, cwd=CREGIT, env=_ENV,
                                stdout=lf, stderr=subprocess.STDOUT)
        sampler.start(proc.pid)
        try:
            rc = proc.wait()
        finally:
            peak = sampler.stop()
    duration = int(time.time() - start)

    record_metric(name, cls, phase, duration, rc, logfile)
    mark = "✓" if rc == 0 else f"✗ rc={rc}"
    say(f"{name} {mark} {phase} in {duration}s"
        + (f" | peak {peak['tree_rss_mb']}MB rss, {peak['tree_procs']} procs,"
           f" {peak['cpu_pct']}% cpu, {peak['disk_free_gb_min']}G disk low"
           if peak["samples"] else ""))
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
    state_dir(name).mkdir(parents=True, exist_ok=True)

    # Single-instance guard per project (held for the whole job). Lives in STATE,
    # not in workdir, because the runner deletes workdir out from under it.
    lockfile = lock_path(name).open("w")
    try:
        fcntl.flock(lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        say(f"{name} — another run holds the lock, skipping")
        return "deferred"

    try:
        if shutil.disk_usage(OUT).free < DISK_FLOOR_GB * 2**30:
            say(f"{name} — disk below {DISK_FLOOR_GB}G floor, deferred")
            return "deferred"

        # Flag names are the runner's, not ours. run_pipeline_process.sh accepts
        # --work and --mask; it exits 2 on anything it does not know. ctp.py used
        # to send --work-dir and --file-filter, so every project failed rc=2
        # before doing any work. cmd_run now preflights these names, so a rename
        # upstream stops the run once with a clear message.
        pipeline_args = [
            "./run_pipeline_process.sh",
            "--repo-url", project["url"],
            "--repo-name", name,
            "--work", str(workdir),
            "--mask", project["file_filter"],
        ]
        if _OPTS.get("skip_html"):
            pipeline_args.append("--skip-html")
        # Sharding is for the L class only. Measured on Linux: --mode pipeline
        # leaves ~14 of 16 cores idle, because the per-blob chain spawns three
        # processes and the pipelined walk never keeps 16 of them in flight. A
        # shard is an independent process with its own worker pool, so N shards
        # multiply tokenizer throughput. It costs transient disk, which is why it
        # is opt-in rather than the default: three concurrent S-class projects
        # already fill this box.
        if shard_class(project):
            pipeline_args += ["--mode", "sharded", "--shards", str(_OPTS["shards"])]
        if _OPTS.get("gc"):
            pipeline_args += ["--gc", _OPTS["gc"]]
        if _OPTS.get("blame_jobs"):
            pipeline_args += ["--blame-jobs", str(_OPTS["blame_jobs"])]
        # FROM_STEP is positional and must come last. The runner only wipes the
        # workdir when it is 1, so a resume keeps whatever finished before.
        from_step = _OPTS.get("from_step", 1)
        if from_step > 1:
            pipeline_args.append(str(from_step))
        rc = run_phase(project, "pipeline", pipeline_args)
        if rc != 0:
            return "failed"

        rc = run_phase(project, "validate", [
            "python3", str(CORPUS / "validate.py"),
            str(workdir / f"{name}-dataset.parquet"), str(stamp),
        ])
        if rc != 0:
            return "failed"

        if _OPTS.get("drop_memo"):
            # Cleanup, not prevention: memo/ is already on disk. retain.prune
            # re-checks the keepers itself and refuses if they are not there.
            _reclaimed, ok = retain.prune(name, ("memo",), apply=True)
            if not ok:
                say(f"{name} — memo/ kept, retain refused the prune (see above)")
        return "done"
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

    # Fail before the run, not 1,900 times during it. Every project passes these
    # flags, so a name the runner does not know costs one rc=2 per project and
    # produces no artefact. This is defect D1, caught by a check instead of by a
    # wasted run.
    missing = [f for f in REQUIRED_RUNNER_FLAGS if not script_supports(f)]
    if missing:
        sys.exit(f"{CREGIT}/run_pipeline_process.sh does not accept: {', '.join(missing)}.\n"
                 "ctp.py passes these to every project and the runner exits 2 on an\n"
                 "unknown flag. Check pipeline.cfg points at the right checkout, or\n"
                 "update REQUIRED_RUNNER_FLAGS and run_project together.")

    # Fail before the run rather than quietly doing something else: --skip-html
    # only means anything if the configured checkout implements it.
    if args.skip_html and not script_supports("--skip-html"):
        sys.exit(f"--skip-html is not implemented by {CREGIT}/run_pipeline_process.sh.\n"
                 "Patch that checkout to guard its HTML step, then re-run. Refusing to\n"
                 "start: generating the HTML and deleting it later is not what the flag says.")
    if args.drop_memo and "--no-memo" in sys.argv:
        say("note: --no-memo is an alias for --drop-memo and does NOT prevent the write. "
            "The tokenizer requires BFG_MEMO_DIR, so memo/ is built and then deleted.")
    # Same rule as --skip-html: refuse before the run rather than discover per
    # project that the configured checkout cannot shard.
    if args.shards > 1:
        missing = [f for f in ("--mode", "--shards") if not script_supports(f)]
        if missing:
            sys.exit(f"--shards needs {', '.join(missing)}, which "
                     f"{CREGIT}/run_pipeline_process.sh does not advertise.\n"
                     "Point pipeline.cfg at a checkout that supports sharded mode,\n"
                     "or drop --shards and accept the single-process rate.")
    # Same rule again: refuse before the run rather than per project.
    if args.gc and not script_supports("--gc"):
        sys.exit(f"--gc is not implemented by {CREGIT}/run_pipeline_process.sh.\n"
                 "That checkout still packs unconditionally, and an unguarded\n"
                 "repack failure deletes the workdir. Patch it before relying on --gc.")
    if args.blame_jobs and not script_supports("--blame-jobs"):
        sys.exit(f"--blame-jobs is not implemented by {CREGIT}/run_pipeline_process.sh.\n"
                 "That checkout blames serially, at roughly 3 files per minute.\n"
                 "Patch it before relying on the flag.")
    if args.blame_jobs < 0:
        sys.exit(f"--blame-jobs cannot be negative (got {args.blame_jobs}).")
    if args.from_step < 1:
        sys.exit(f"--from-step must be 1 or greater (got {args.from_step}).")
    shard_classes = tuple(c.strip() for c in args.shard_classes.split(",") if c.strip())
    _OPTS.update(skip_html=args.skip_html, drop_memo=args.drop_memo,
                 shards=args.shards, shard_classes=shard_classes,
                 from_step=args.from_step, gc=args.gc,
                 blame_jobs=args.blame_jobs)
    if args.from_step > 1:
        say(f"resuming at step {args.from_step}: the runner keeps the existing workdir")
    if args.shards > 1:
        say(f"sharding {args.shards}-way for size class"
            f"{'es' if len(shard_classes) > 1 else ''} {', '.join(shard_classes)}")

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
        elif _lock_held(lock_path(p["name"])):
            state = "RUNNING"
        elif state_dir(p["name"]).exists() or workdir.exists():
            # state_dir first: the runner deletes the workdir when the pipeline
            # fails, so a workdir-only test would report a failed project QUEUED.
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
    """Rebuild ctp.duckdb (derived index over stamps/metrics/parquets)."""
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
    run_p.add_argument("--skip-html", action="store_true",
                       help="forward --skip-html to run_pipeline_process.sh so the HTML "
                            "views (94-255 MB per project) are never generated")
    run_p.add_argument("--drop-memo", "--no-memo", action="store_true",
                       help="delete memo/ (45-88%% of the workdir) once a project "
                            "validates. NOT prevention: the tokenizer requires "
                            "BFG_MEMO_DIR, so memo/ is written first, then removed")
    run_p.add_argument("--shards", type=int, default=0,
                       help="tokenize in N shards (needs >1 to take effect). "
                            "Measured: --mode pipeline leaves ~14 of 16 cores idle "
                            "on an L-class project. Costs transient disk, so it is "
                            "opt-in and limited to --shard-classes")
    run_p.add_argument("--shard-classes", default="L",
                       help="comma-separated size classes to shard (default L). "
                            "S and M gain nothing: three concurrent projects "
                            "already fill the box")
    run_p.add_argument("--from-step", type=int, default=1, metavar="N",
                       help="resume the runner at step N instead of cloning again. "
                            "Only step 1 wipes the workdir, so N>1 keeps finished "
                            "work. Use this after a late failure: the Linux run "
                            "lost its repack at the end of step 2 with 15.2 h of "
                            "tokenizing already on disk, and --from-step 3 skips "
                            "the clone, the tokenize and the repack")
    run_p.add_argument("--gc", choices=("none", "plain", "aggressive"),
                       help="forward --gc to run_pipeline_process.sh, which packs "
                            "the generated repo after tokenizing. Omit to accept "
                            "the runner's own default")
    run_p.add_argument("--blame-jobs", type=int, default=0, metavar="N",
                       help="run the blame step with N parallel workers. Blame is "
                            "the bottleneck: measured serially on Linux it managed "
                            "3 files per minute against 64,536 files, which is 14 "
                            "days. Each file is independent, so N does not change "
                            "the output. Omit to accept the runner's default of 1")
    run_p.set_defaults(fn=cmd_run)

    st_p = sub.add_parser("status", help="one-screen pipeline status")
    st_p.add_argument("--manifest", default="manifest.tsv")
    st_p.set_defaults(fn=cmd_status)

    db_p = sub.add_parser("db", help="rebuild ctp.duckdb (tracking table + unified tokens view)")
    db_p.set_defaults(fn=cmd_db)

    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
