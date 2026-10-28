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
  --memo-dir   the opposite trade: keep each project's memo in DIR/<project>,
               outside the workdir a step-1 run deletes, so a from-scratch
               re-run still gets every memo hit. Mutually exclusive with
               --drop-memo.

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
import csv
import fcntl
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import configparser

import retain  # shared prune code path for --drop-memo
from file_mask import UNIVERSAL_MASK

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

# Step 10 is the DuckDB generator, the step that writes the Parquet, and it is
# the LAST step: run_pipeline_process.sh ends at its end_step (verified against
# cregit-issue61, whose file ends there; the script says so itself in the comment
# above its --project-meta validation). So --from-step 11 or higher describes a
# run that reaches no step at all — the runner accepts any digit string as
# FROM_STEP without an upper bound, every step guard evaluates false, and it
# exits 0 having done nothing. Such a run cannot publish a Parquet, so it cannot
# publish a blank one, which is why the provenance guard in cmd_run does not gate
# it.
DATASET_STEP = 10

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
        # An empty file_filter means "the universal mask". Every generated
        # manifest now writes it out, so this is the path taken by a hand-written
        # manifest that leaves the column blank — and it must not send an empty
        # mask, which blobExec rejects and which would otherwise select every
        # file in the repository.
        projects.append(dict(name=name, url=url, category=category,
                             file_filter=file_filter or UNIVERSAL_MASK,
                             size_class=size_class))
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


_SIZE_RE = re.compile(r"^(\d+(?:\.\d+)?)\s?(B|K|M|G|T|KB|MB|GB|TB|KIB|MIB|GIB|TIB)$")
_SIZE_UNITS = {"B": 1, "K": 1024, "KB": 1024, "KIB": 1024,
               "M": 1024 ** 2, "MB": 1024 ** 2, "MIB": 1024 ** 2,
               "G": 1024 ** 3, "GB": 1024 ** 3, "GIB": 1024 ** 3,
               "T": 1024 ** 4, "TB": 1024 ** 4, "TIB": 1024 ** 4}

# Measured 2026-09-15: memory_limit bounds DuckDB's buffer manager, not the
# process. RssAnon settled at 11.0-11.6 GB under an 8GB limit.
SETTLE_RATIO = 1.4


def size_to_bytes(text: str) -> int:
    """Bytes for a DuckDB size string. Raises ValueError on anything else.

    The rule mirrors parse_memory_limit() in generate_dataset.py, so ctp refuses
    a bad value before the run instead of at step 10, which is hours in.
    """
    cleaned = text.strip()
    if cleaned.endswith("%"):
        raise ValueError(
            f"--memory-limit takes an absolute size, not a percentage (got {text!r}). "
            "A percentage measures total RAM, and only the free part is usable.")
    m = _SIZE_RE.match(cleaned.upper())
    if not m:
        raise ValueError(f"cannot read {text!r} as a memory size. Example: 3GB")
    return int(float(m.group(1)) * _SIZE_UNITS[m.group(2)])


def available_bytes() -> int | None:
    """MemAvailable from /proc/meminfo, or None when it cannot be read."""
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        return None
    return None


def memory_budget_warning(limit: str, jobs: int) -> str | None:
    """Warn when 1.4 x jobs x limit does not fit in MemAvailable.

    This is the guard whose absence killed the 2026-09-15 run: two projects at
    the generator's 8GB default need ~22 GB, and this box had 6 GB free.
    """
    available = available_bytes()
    if available is None:
        return None
    want = int(size_to_bytes(limit) * SETTLE_RATIO * jobs)
    if want <= available:
        return None
    return (f"memory budget: {jobs} concurrent x {limit} settles at about "
            f"{retain.human(want)}, but only {retain.human(available)} is available. "
            f"Step 10 can exhaust RAM and the kernel or the harness will kill the "
            f"run. Lower --memory-limit or --jobs.")


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
            # The manifest's mask, which every generated manifest fills with the
            # universal mask. --mask overrides it for one deliberate run.
            "--mask", _OPTS.get("mask") or project["file_filter"],
        ]
        if _OPTS.get("skip_html"):
            pipeline_args.append("--skip-html")
        # Replace every .blame file instead of skipping the ones that exist.
        # blameRepoFiles.pl skips existing output, so a step-7 resume re-blames
        # NOTHING without this and step 10 then rebuilds the Parquet from the old
        # blame. cmd_run has checked the runner advertises the flag.
        if _OPTS.get("reblame"):
            pipeline_args.append("--reblame")
        # Reuse the tokenizations already in this project's blob map across a mask
        # change. cmd_run has already checked the runner advertises it and that
        # --from-step is 2 or more; blobExec does the per-project verification.
        if _OPTS.get("mask_widened"):
            pipeline_args.append("--mask-widened")
        # Invalidate the tokenizations of ONE extension set, because the tokenizer
        # that produced them was corrected. This is not --mask-widened's problem:
        # the mask decides WHICH files are tokenized, this decides HOW, and the
        # blob map keys reuse on the command string and the mask, neither of which
        # a rebuilt binary changes. The runner supplies --memo-dir alongside it,
        # because a dropped blob_map row whose memo entry survives is served the
        # stale tokens straight back.
        if _OPTS.get("retokenize"):
            pipeline_args += ["--retokenize", _OPTS["retokenize"]]
        # Put the memo outside the work directory, which a FROM_STEP=1 run
        # deletes. It matters now because the mask changed corpus-wide and
        # blobExec refuses to resume against a different one (Mapping.open), so
        # every re-run starts from step 1 — and a memo hit returns without
        # invoking srcml at all. torvalds__linux holds ~2.6 million memo entries
        # against 3,228,137 blobs, so preserving them turns a cold tokenize into
        # a commit walk.
        #
        # One SUBDIRECTORY PER PROJECT, never one shared directory: tokenBySha.pl
        # keys the memo on sha1 of the file contents, with neither the repository
        # nor the extension in the key. Two projects sharing a directory would
        # serve each other's entries, and identical bytes under a different
        # extension are a different language and different tokens.
        if _OPTS.get("memo_dir"):
            memo_dir = Path(_OPTS["memo_dir"]).resolve() / name
            resolved_work = workdir.resolve()
            if memo_dir == resolved_work or resolved_work in memo_dir.parents:
                say(f"{name} — refusing to run: --memo-dir puts the memo at "
                    f"{memo_dir}, inside the work directory the runner deletes. "
                    "Point --memo-dir outside the corpus output directory.")
                return "failed"
            pipeline_args += ["--memo-dir", str(memo_dir)]
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
        # The runner calls this --jobs. cregit-issue61 7a70a92 renamed it from
        # --blame-jobs on 2026-09-18, mid-run, and ctp.py kept sending the old
        # name — so every invocation exited at the preflight. ctp.py's own CLI
        # name stays --blame-jobs, because its --jobs means concurrent projects.
        if _OPTS.get("blame_jobs"):
            pipeline_args += ["--jobs", str(_OPTS["blame_jobs"])]
        # Step 10 is the only step that can exhaust RAM. Measured 2026-09-15: at
        # the generator's own 8GB default, two concurrent projects need ~22 GB and
        # the run died on this 30 GB box, which already gives ~17 GB to other
        # software. Budget 1.4 x --jobs x --memory-limit.
        if _OPTS.get("memory_limit"):
            pipeline_args += ["--memory-limit", _OPTS["memory_limit"]]
        if _OPTS.get("duckdb_threads"):
            pipeline_args += ["--duckdb-threads", str(_OPTS["duckdb_threads"])]
        # Per-project provenance for the Parquet. The sidecar is keyed by the
        # manifest name, and generate_dataset.py refuses a key it cannot find, so
        # a stale sidecar fails loudly instead of writing 29 blank columns.
        if _OPTS.get("project_meta"):
            pipeline_args += ["--project-meta", str(_OPTS["project_meta"]),
                              "--project-key", name]
        # Firm attribution. The same file for every project — it is keyed by
        # e-mail domain, not by project — so unlike --project-key there is
        # nothing per-project to send. The generator refuses a map with a
        # repeated domain, because a duplicate key would multiply token rows
        # through the LEFT JOIN and nothing downstream would notice.
        if _OPTS.get("firm_map"):
            pipeline_args += ["--firm-map", str(_OPTS["firm_map"])]
            if _OPTS.get("firm_canonical"):
                pipeline_args += ["--firm-canonical",
                                  str(_OPTS["firm_canonical"])]
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
    # produces no artefact. This is a defect, caught by a check instead of by a
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
    # Same reasoning as --skip-html: a checkout that does not implement --reblame
    # would run step 7, skip every file that already has .blame output, and exit 0
    # having changed nothing. That is the exact failure the flag exists to close, so
    # refuse rather than report a re-blame that did not happen.
    if args.reblame and not script_supports("--reblame"):
        sys.exit(f"--reblame is not implemented by {CREGIT}/run_pipeline_process.sh.\n"
                 "That checkout's step 7 cannot replace existing .blame files, so the run\n"
                 "would skip every one of them and exit 0. Refusing to start.")
    if args.reblame and args.from_step > 7:
        sys.exit(f"--reblame needs --from-step 7 or less (got {args.from_step}).\n"
                 "The re-blame happens inside step 7; from step 8 the flag is skipped and\n"
                 "step 10 rebuilds the Parquet from the blame already on disk.")
    if args.drop_memo and "--no-memo" in sys.argv:
        say("note: --no-memo is an alias for --drop-memo and does NOT prevent the write. "
            "The tokenizer requires BFG_MEMO_DIR, so memo/ is built and then deleted.")
    # --memo-dir keeps the memo; --drop-memo deletes it. Asking for both is not a
    # preference to resolve, it is a mistake to report: retain.prune only ever
    # looks at <workdir>/memo, so the combination would silently preserve
    # everything and report a prune that pruned nothing.
    if args.memo_dir and args.drop_memo:
        sys.exit("--memo-dir and --drop-memo contradict each other: one puts the memo "
                 "where no wipe can reach it, the other deletes it after each project.\n"
                 "--drop-memo also only prunes <workdir>/memo, so it would not even find "
                 "an external memo. Pick one.")
    if args.memo_dir:
        if not script_supports("--memo-dir"):
            sys.exit(f"--memo-dir is not implemented by {CREGIT}/run_pipeline_process.sh.\n"
                     "That checkout hard-codes BFG_MEMO_DIR to <work>/memo, which a step-1\n"
                     "run deletes, so the flag would be silently dropped. Patch it first.")
        if not Path(args.memo_dir).is_dir():
            sys.exit(f"--memo-dir {args.memo_dir} is not an existing directory. Create it "
                     "first: a typo here would quietly start a second corpus of memos "
                     "instead of reusing the one you meant.")
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
    # Validate our own value before probing the runner. A negative count is the
    # caller's mistake either way, and reporting it as "not implemented" sends
    # them to patch a checkout that is not the problem.
    if args.blame_jobs < 0:
        sys.exit(f"--blame-jobs cannot be negative (got {args.blame_jobs}).")
    # The runner calls this --jobs; cregit-issue61 7a70a92 renamed it from
    # --blame-jobs on 2026-09-18. ctp.py keeps --blame-jobs as its own CLI name,
    # because ctp.py's --jobs already means concurrent projects.
    if args.blame_jobs and not script_supports("--jobs"):
        sys.exit(f"--blame-jobs needs --jobs, which {CREGIT}/run_pipeline_process.sh\n"
                 "does not advertise. That checkout blames serially, at roughly 3 files\n"
                 "per minute. Patch it before relying on the flag.")
    if args.memory_limit and not script_supports("--memory-limit"):
        sys.exit(f"--memory-limit is not implemented by {CREGIT}/run_pipeline_process.sh.\n"
                 "That checkout runs step 10 at the generator's own default, so the\n"
                 "flag would be silently dropped. Patch it before relying on it.")
    if args.duckdb_threads and not script_supports("--duckdb-threads"):
        sys.exit(f"--duckdb-threads is not implemented by {CREGIT}/run_pipeline_process.sh.\n"
                 "Patch it before relying on the flag.")
    if args.duckdb_threads < 0:
        sys.exit(f"--duckdb-threads cannot be negative (got {args.duckdb_threads}).")
    # Same rule again, and the stakes are higher: without the check the run would
    # produce a corpus of Parquets carrying blank provenance, which no consumer
    # could tell from provenance that is genuinely unknown.
    project_meta = ""
    if args.project_meta:
        if not Path(args.project_meta).exists():
            sys.exit(f"--project-meta {args.project_meta} does not exist. "
                     "Generate it with project_meta.py.")
        missing = [f for f in ("--project-meta", "--project-key")
                   if not script_supports(f)]
        if missing:
            sys.exit(f"--project-meta needs {', '.join(missing)}, which "
                     f"{CREGIT}/run_pipeline_process.sh does not accept.")
        # Absolute, because the runner is started with cwd=CREGIT while this path
        # was typed relative to this repository. Sending it through unresolved
        # made the runner reject its own sidecar (rc=2) on the first real run.
        project_meta = str(Path(args.project_meta).resolve())
    # The firm map, under the same rules and for a sharper reason: a map that
    # never arrives does not fail the run, it publishes blank firm columns across
    # the whole corpus, and firm attribution is the measurement this corpus
    # exists for. getattr, because callers build this Namespace directly.
    firm_map = firm_canonical = ""
    if getattr(args, "firm_map", ""):
        if not Path(args.firm_map).is_file():
            sys.exit(f"--firm-map {args.firm_map} is not a file. Build it with "
                     "build_domain_map.py.")
        missing = [f for f in ("--firm-map", "--firm-canonical")
                   if not script_supports(f)]
        if missing:
            sys.exit(f"--firm-map needs {', '.join(missing)}, which "
                     f"{CREGIT}/run_pipeline_process.sh does not accept.\n"
                     "That checkout would run step 10 without the firm join, so "
                     "every Parquet would carry three blank firm columns.")
        # Absolute, for the same reason as --project-meta: the runner is started
        # with cwd=CREGIT while this path was typed relative to this repository.
        firm_map = str(Path(args.firm_map).resolve())
        if getattr(args, "firm_canonical", ""):
            if not Path(args.firm_canonical).is_file():
                sys.exit(f"--firm-canonical {args.firm_canonical} is not a file.")
            firm_canonical = str(Path(args.firm_canonical).resolve())
    elif getattr(args, "firm_canonical", ""):
        sys.exit("--firm-canonical without --firm-map has no firm_raw to "
                 "canonicalise. Pass data/affiliation.merged.csv too.")
    # Refuse a bad size now. Step 10 is the last step, so the alternative is
    # finding the typo after every earlier step has already run.
    if args.memory_limit:
        try:
            size_to_bytes(args.memory_limit)
        except ValueError as exc:
            sys.exit(f"--memory-limit: {exc}")
    if args.from_step < 1:
        sys.exit(f"--from-step must be 1 or greater (got {args.from_step}).")
    # --mask-widened, under the same rule as every other forwarded flag, and with
    # the sharpest stakes of any of them: an unimplemented flag would be dropped
    # and blobExec would refuse every project on the recorded mask (exit 3), which
    # reads as a failed corpus rather than as a missing feature.
    mask_widened = bool(getattr(args, "mask_widened", False))
    if mask_widened:
        if not script_supports("--mask-widened"):
            sys.exit(f"--mask-widened is not implemented by {CREGIT}/run_pipeline_process.sh.\n"
                     "That checkout would drop the flag, and blobExec would then refuse every\n"
                     "project whose recorded mask differs from the manifest's (exit 3).")
        # The flag exists to preserve the work in the workdir, and step 1 deletes
        # the workdir. Sending both would run, preserve nothing, and look like a
        # success. The runner refuses this too; refusing here means it costs one
        # message rather than one clone per project.
        if args.from_step < 2:
            sys.exit("--mask-widened needs --from-step 2 or greater. Step 1 deletes the project\n"
                     "workdir, taking with it the blob map this flag reuses and the cregit.git\n"
                     "its new_blob ids live in, so there would be nothing left to preserve.")
        if args.shards > 1:
            sys.exit("--mask-widened cannot be combined with sharding: each shard builds a fresh\n"
                     "blob map, so there is no recorded mask to widen.")
    # --retokenize, under the same rule, and with stakes of its own. An unimplemented
    # flag would be DROPPED and step 2 would then reuse the very tokenizations the
    # operator asked to discard, and exit 0. That reads as a successful re-run rather
    # than as a missing feature, which is the worst of the two failures: nothing in
    # the output says the corrected tokenizer never ran.
    retokenize = (getattr(args, "retokenize", "") or "").strip()
    if retokenize:
        if not script_supports("--retokenize"):
            sys.exit(f"--retokenize is not implemented by {CREGIT}/run_pipeline_process.sh.\n"
                     "That checkout would drop the flag, step 2 would reuse the cached\n"
                     "tokenizations you asked to discard, and the run would exit 0 having\n"
                     "changed nothing. Check pipeline.cfg points at a checkout that has it.")
        # The runner requires EXACTLY 2, not 2-or-more. Step 1 deletes the blob map
        # this flag edits; step 3 and later skip step 2 altogether, so the run would
        # rebuild blame, HTML and the dataset over tokens nobody re-made.
        if args.from_step != 2:
            sys.exit(f"--retokenize needs --from-step 2 exactly (got {args.from_step}).\n"
                     "Step 1 deletes the blob map this flag edits. Step 3 and later skip step 2,\n"
                     "so the tokens would never be remade and the rest of the pipeline would run\n"
                     "over the stale ones and exit 0.")
        if mask_widened:
            sys.exit("--mask-widened and --retokenize cannot be used in the same run. Each one\n"
                     "verifies a different invariant of the blob map, and together neither check\n"
                     "means anything: one reuses rows across a mask change, the other deletes rows\n"
                     "a tokenizer change invalidated.")
        if args.shards > 1:
            sys.exit("--retokenize cannot be combined with sharding: each shard builds a fresh\n"
                     "blob map, so there are no cached tokenizations to invalidate.")
    # ------------------------------------------------------------------ #
    # The provenance guard. It REFUSES rather than warns, for three reasons.
    #
    # 1. For 29 of the 32 columns there was nothing to warn WITH. Omitting
    #    --project-meta produced no diagnostic at any layer: not here, not in
    #    run_pipeline_process.sh, and not in generate_dataset.py, whose
    #    load_project_meta() treats an absent sidecar as a supported
    #    backwards-compatible mode and fills all 29 with ''. The only notice that
    #    existed was the `say("note: no --firm-map ...")` this block replaces, and
    #    it covered 3 columns.
    #
    # 2. A note cannot carry this weight in this log. The 2026-09-21
    #    torvalds__linux resume ran 6 h 48 m and wrote 113 lines, 105 of them
    #    30-second heartbeats. A note is line 1, scrolled past inside the first
    #    minute of an overnight run, and the operator who reads the log next reads
    #    the tail. (That run's log holds no `note:` line, so it did pass
    #    --firm-map; what it cannot tell us either way is --project-meta, because
    #    omitting that printed nothing at all. Which is the point of reason 1.)
    #
    # 3. Nothing downstream catches the result. The file still has all 70 columns
    #    in the right order and the right types, so validate.py and
    #    validate_schema.py both pass it, and a consumer reading `stratum = ''`
    #    cannot distinguish a project whose stratum was never supplied from one
    #    that genuinely has none. A quietly wrong dataset is worse than a failed
    #    run: the cost of a false refusal is one re-typed command, the cost of a
    #    miss is hours of compute and a corpus whose members disagree with each
    #    other. torvalds__linux is the largest of 186; its re-run with the flags
    #    filled stratum on 100% of rows, firm on 83.6% and history_cluster on
    #    100%, so those columns were never "unknown", only never asked for.
    #
    # Refusing is also the house idiom in cmd_run. Every other flag combination
    # that would run to completion while doing the wrong thing exits here with a
    # message naming the consequence: --memo-dir with --drop-memo, --retokenize
    # with the wrong --from-step, --firm-canonical without --firm-map. This is the
    # same shape, deliberately.
    #
    # The escape hatch is --allow-empty-provenance, because a blanket refusal
    # would break three legitimate uses: a test fixture, a one-project smoke run,
    # and a corpus whose sidecar does not exist yet. It is store_true with no
    # default, so it cannot arrive except by being typed, and taking it prints
    # what it gave up.
    #
    # NOT re-checked here: that the paths exist and are readable. That check is
    # already made three times over and it fails hard every time — this function
    # above (`--project-meta ... does not exist`, `--firm-map ... is not a file`),
    # run_pipeline_process.sh's own argument validation (exit 2 before the clone),
    # and generate_dataset.py at the top of step 10 (exit 1, or an uncaught
    # FileNotFoundError for the sidecar). A path typo therefore cannot reach the
    # Parquet, so adding a fourth existence check here would only duplicate the
    # first one. The failure this guard exists for is the opposite one: a flag
    # that is not there at all, which no layer treats as an error because an
    # absent sidecar is a supported backwards-compatible mode in the generator.
    #
    # Placed last, after the per-flag checks, so that a typo'd path or a checkout
    # that cannot forward the flag is still reported by its own sharper message
    # rather than by this general one.
    gaps: list[tuple[str, str]] = []
    if not project_meta:
        gaps.append(("--project-meta",
                     "29 provenance columns empty on every row: clone_url, "
                     "provenance_status, source, stratum, fact, contested, "
                     "label_date, owner, repo, roster_name, roster_lang, "
                     "language, commits, size_class, size_kb, stars, pushed_at, "
                     "license, owner_type, archived, fork, history_cluster, "
                     "history_shared_with, history_relation, history_includes, "
                     "history_first, history_created, manifest_category, "
                     "file_mask"))
    if not firm_map:
        gaps.append(("--firm-map",
                     "3 firm columns empty on every row: firm_raw, firm, "
                     "firm_source — and firm attribution is the measurement this "
                     "corpus exists for"))
    elif not firm_canonical:
        # Not an empty column, a wrong one, which is why it is listed separately:
        # `firm` gets firm_raw's value verbatim, so the 48 split spellings stay
        # split and every firm's share is understated.
        gaps.append(("--firm-canonical",
                     "`firm` repeats `firm_raw` instead of the reviewed canonical "
                     "name, so the 48 split spellings stay split and every firm's "
                     "share is understated"))
    allow_empty = bool(getattr(args, "allow_empty_provenance", False))
    reaches_dataset = args.from_step <= DATASET_STEP
    if gaps and reaches_dataset and not allow_empty:
        sys.exit(
            f"refusing this run: it reaches step {DATASET_STEP} and would publish a "
            "Parquet with blank provenance.\n"
            + "".join(f"  {flag} absent — {cost}\n" for flag, cost in gaps)
            + "This does not fail anything. The file keeps all 70 columns in the "
              "right order, so validate.py passes it and no consumer can tell a "
              "blank column from provenance that is genuinely unknown.\n"
              "This is why it matters: a torvalds__linux run on 2026-09-21 spent "
              "6h48m and died in step 10 for an unrelated reason. Had it "
              "succeeded it would have published the largest project in the "
              "corpus silently inconsistent with the other 185.\n"
              "Pass the flags — this is what tmp/wave.sh does, and its comment says "
              "they cannot be defaulted on:\n"
              "  --project-meta project_meta.json \\\n"
              "  --firm-map data/affiliation.merged.csv \\\n"
              "  --firm-canonical data/firm_canonical.csv\n"
              "If blank columns are genuinely what you want — a fixture, a "
              "one-project smoke run, a corpus whose sidecar does not exist yet — "
              "say so with --allow-empty-provenance.")
    if allow_empty and not gaps:
        # Refused rather than ignored, so the hatch cannot settle into a launcher
        # script and silence a later run that does need the guard. It is only ever
        # correct to type it in the same breath as leaving a flag out.
        sys.exit("--allow-empty-provenance has nothing to allow: --project-meta, "
                 "--firm-map and --firm-canonical are all present, so no column "
                 "would be blank. Drop the flag — left in a wrapper it would "
                 "silence the guard on the next run that does omit one.")
    if allow_empty and gaps and reaches_dataset:
        # Loud, and itemised, because the operator has just opted out of the only
        # check standing between this run and a quietly wrong dataset. A reader of
        # the log must be able to see which columns were given up without
        # inferring it from the absence of a refusal.
        say("WARNING: --allow-empty-provenance: this run will publish a Parquet "
            "with blank provenance, on purpose.")
        for flag, cost in gaps:
            say(f"         {flag} absent — {cost}")
        say("         Do not mix these rows into the corpus: they are "
            "schema-valid and indistinguishable from rows whose provenance is "
            "genuinely unknown.")
    shard_classes = tuple(c.strip() for c in args.shard_classes.split(",") if c.strip())
    _OPTS.update(skip_html=args.skip_html, drop_memo=args.drop_memo,
                 reblame=getattr(args, "reblame", False),
                 memo_dir=args.memo_dir,
                 shards=args.shards, shard_classes=shard_classes,
                 from_step=args.from_step, gc=args.gc,
                 mask_widened=mask_widened,
                 retokenize=retokenize,
                 blame_jobs=args.blame_jobs,
                 memory_limit=args.memory_limit,
                 duckdb_threads=args.duckdb_threads,
                 # getattr, because callers build this Namespace directly; an
                 # absent --mask means "use the manifest's", which is the default.
                 mask=getattr(args, "mask", ""),
                 project_meta=project_meta,
                 firm_map=firm_map, firm_canonical=firm_canonical)
    if _OPTS["mask"]:
        # Loud, because the mask in the Parquet's file_mask column comes from the
        # sidecar, which reads the manifest — so an override makes the recorded
        # mask a lie unless the operator updates the manifest too.
        say(f"WARNING: --mask overrides the manifest for every project in this "
            f"run: {_OPTS['mask']}")
        say("         project_meta.json records the MANIFEST's mask, so the "
            "Parquet's file_mask column will not match this run.")
    if args.memory_limit:
        warning = memory_budget_warning(args.memory_limit, args.jobs)
        if warning:
            say(f"WARNING: {warning}")
    if args.from_step > 1:
        say(f"resuming at step {args.from_step}: the runner keeps the existing workdir")
    if retokenize:
        # Loud, because this flag DELETES cached work. A reader of the log must be
        # able to see which extensions lost their tokenizations without inferring it
        # from the absence of a refusal.
        say(f"--retokenize {retokenize}: step 2 will DISCARD the cached tokenizations "
            f"for these extensions and redo them.")
        say("         Every other extension's cached work is kept. blobExec drops the "
            "blob_map rows and the memo entries together, and refuses the run (exit 7) "
            "if the request would have invalidated nothing — so a typo cannot pass as "
            "a successful re-run.")
    if mask_widened:
        # Loud, because this is the one flag that lets a blob map recorded under one
        # mask be reused under another, and the reader of a log should not have to
        # infer that from the absence of a refusal.
        say("--mask-widened: step 2 will REUSE each project's existing tokenizations "
            "instead of redoing them.")
        say("         blobExec verifies per project, against the rows: every "
            "already-tokenized path must still be selected by the new mask, and the "
            "retained new_blob ids must exist in cregit.git. Either check failing "
            "refuses that project (rc 3) and changes nothing.")
        say("         tree_map, commit_map, ref_map and blob_map's identity rows are "
            "discarded, so files the wider mask newly selects are tokenized rather "
            "than passed through as raw source.")
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


def project_state(name: str) -> str:
    """DONE, RUNNING, FAILED or QUEUED for one project, from the filesystem."""
    workdir = OUT / name
    if (workdir / f"{name}.validated").exists():
        return "DONE"
    if _lock_held(lock_path(name)):
        return "RUNNING"
    # state_dir first: the runner deletes the workdir when the pipeline fails, so
    # a workdir-only test would report a failed project QUEUED.
    if state_dir(name).exists() or workdir.exists():
        return "FAILED"
    return "QUEUED"


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
        state = project_state(p["name"])
        if state == "DONE":
            done += 1
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


BAR_WIDTH = 30
# Only a full run measures the rate. metrics.tsv cannot tell a full run from a
# --from-step resume, and a resume looks impossibly fast: kamailio's step-10
# resume took 74 s for 61k commits. The median over projects rejects those.
MIN_RATE_SAMPLES = 2


def bar(done: int, total: int, width: int = BAR_WIDTH) -> str:
    """A fixed-width progress bar. Never divides by zero on an empty manifest."""
    filled = round(width * done / total) if total else 0
    return "█" * filled + "░" * (width - filled)


def commit_counts(projects: list[dict]) -> dict:
    """Commits per project name, read from candidates.csv.

    Joins on `clone_url`, NOT on name: the manifest name is a slug that
    lowercases and maps `_` and `.` to `-`, so four of the 200 drawn projects do
    not match by name. Returns {} when the file is absent, so progress still
    prints without an ETA.
    """
    path = CORPUS / "candidates.csv"
    if not path.exists():
        return {}
    by_url = {}
    with path.open(newline="") as fh:
        for row in csv.DictReader(fh):
            if row.get("commits"):
                by_url[row["clone_url"]] = int(row["commits"])
    return {p["name"]: by_url[p["url"]] for p in projects if p["url"] in by_url}


def finished_runs() -> list[tuple]:
    """(name, cls, duration_s) for every successful pipeline run, oldest first."""
    if not METRICS.exists():
        return []
    runs = []
    for line in METRICS.read_text().splitlines()[1:]:
        _ts, name, cls, phase, dur, rc, _log = line.split("\t")
        if phase == "pipeline" and rc == "0":
            runs.append((name, cls, int(dur)))
    return runs


def measured_rate(commits: dict) -> tuple[float, int] | None:
    """Median seconds per 1000 commits over the finished runs, and the sample size.

    None when too few projects have finished to say anything.
    """
    rates = []
    for name, _cls, dur in finished_runs():
        if commits.get(name):
            rates.append(dur / commits[name] * 1000)
    if len(rates) < MIN_RATE_SAMPLES:
        return None
    rates.sort()
    mid = len(rates) // 2
    median = rates[mid] if len(rates) % 2 else (rates[mid - 1] + rates[mid]) / 2
    return median, len(rates)


def cmd_progress(args: argparse.Namespace) -> int:
    """A one-screen progress bar, the last finished projects, and an ETA."""
    projects = read_manifest(CORPUS / args.manifest, None)
    states = {p["name"]: project_state(p["name"]) for p in projects}
    done = [p for p in projects if states[p["name"]] == "DONE"]
    running = [p for p in projects if states[p["name"]] == "RUNNING"]
    failed = [p for p in projects if states[p["name"]] == "FAILED"]
    total = len(projects)
    pct = 100 * len(done) / total if total else 0.0

    print(f"corpus  [{bar(len(done), total)}]  {len(done)}/{total}  {pct:.1f}%")
    per_class = []
    for cls in ("S", "M", "L"):
        members = [p for p in projects if p["size_class"] == cls]
        if members:
            hits = sum(1 for p in members if states[p["name"]] == "DONE")
            per_class.append(f"{cls} {hits}/{len(members)}")
    print(f"        {'  '.join(per_class)}"
          f"{'  |  FAILED ' + str(len(failed)) if failed else ''}")

    commits = commit_counts(projects)
    rate = measured_rate(commits)
    if rate and commits:
        median, samples = rate
        left = sum(commits[p["name"]] for p in projects
                   if states[p["name"]] != "DONE" and p["name"] in commits)
        seconds = left / 1000 * median / max(args.jobs, 1)
        finish = datetime.now(timezone.utc) + timedelta(seconds=seconds)
        print(f"rate    {median:.1f} s per 1k commits (median of {samples} projects)")
        print(f"eta     {seconds / 3600:.1f} h at --jobs {args.jobs}"
              f"  ->  {finish.strftime('%Y-%m-%d %H:%M')} UTC"
              f"  ({left:,} commits left)")
    else:
        print(f"rate    not enough finished projects yet "
              f"(need {MIN_RATE_SAMPLES}, and candidates.csv for commit counts)")

    if running:
        print("\nrunning")
        for p in running:
            print(f"  {p['name']:<34} {p['size_class']}")
    recent = finished_runs()[-args.last:]
    if recent:
        print(f"\nlast {len(recent)} finished")
        for name, cls, dur in reversed(recent):
            print(f"  {name:<34} {cls}  {dur / 60:6.1f} min")
    return 0


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
    run_p.add_argument("--reblame", action="store_true",
                       help="re-blame every file in step 7 instead of skipping the "
                            "ones that already have .blame output. Needed whenever the "
                            "blame itself changed (a git blame flag, a formatBlame.pl "
                            "fix): without it a step-7 resume reports every file as "
                            "already done, exits 0, and step 10 rebuilds the Parquet "
                            "from the old blame. Do NOT use it to resume an interrupted "
                            "run — the skip is what makes a resume cheap")
    run_p.add_argument("--drop-memo", "--no-memo", action="store_true",
                       help="delete memo/ (45-88%% of the workdir) once a project "
                            "validates. NOT prevention: the tokenizer requires "
                            "BFG_MEMO_DIR, so memo/ is written first, then removed")
    run_p.add_argument("--memo-dir", default="", metavar="DIR",
                       help="keep each project's memo in DIR/<project> instead of "
                            "<workdir>/memo, so a from-scratch run (step 1, which "
                            "deletes the workdir) still gets every memo hit. A hit "
                            "returns without invoking srcml at all, so this is the "
                            "difference between re-walking the commits and "
                            "tokenizing from cold: torvalds__linux holds ~2.6 "
                            "million memo entries against 3,228,137 blobs. Needed "
                            "because a changed mask forces a step-1 rebuild — "
                            "blobExec records the mask and refuses to resume "
                            "against a different one. DIR must exist; each project "
                            "gets its own subdirectory, because the memo key is a "
                            "content hash that names neither repository nor "
                            "extension. Cannot be combined with --drop-memo")
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
    run_p.add_argument("--retokenize", metavar="EXTS", default="",
                       help="re-tokenize only these extensions, because the tokenizer "
                            "that produced their cached tokens was corrected. Comma "
                            "separated, without dots: --retokenize rs. Needs "
                            "--from-step 2 exactly, and cannot be combined with "
                            "--mask-widened or sharding. This is NOT a mask change: "
                            "the mask decides WHICH files are tokenized, this decides "
                            "HOW. It exists because the blob map keys reuse on the "
                            "command string and the mask, and rebuilding a tokenizer "
                            "binary changes neither, so a plain resume would serve the "
                            "poisoned tokens back and exit 0. blobExec drops the "
                            "matching blob_map rows and the memo entries together, in "
                            "one transaction with the tree, commit and ref maps, and "
                            "refuses the run rather than invalidating nothing.")
    run_p.add_argument("--mask-widened", action="store_true",
                       help="reuse each project's existing tokenizations across a "
                            "MASK CHANGE instead of rebuilding from cold. Needs "
                            "--from-step 2 or more, because step 1 deletes the "
                            "workdir that holds both the blob map and the "
                            "cregit.git its ids point into. Without this flag a "
                            "mask change is refused, which is the correct default "
                            "and is not being weakened: blobExec still verifies, "
                            "per project and against the rows rather than by "
                            "comparing regexes, that every already-tokenized path "
                            "is still selected and that the retained new_blob ids "
                            "resolve in cregit.git. Valid because the mask decides "
                            "WHICH files are tokenized and never HOW — the language "
                            "comes from the extension, per file. tree_map, "
                            "commit_map, ref_map and blob_map's identity rows are "
                            "discarded, so a newly selected file is tokenized "
                            "rather than passed through as raw source")
    run_p.add_argument("--gc", choices=("none", "plain", "aggressive"),
                       help="forward --gc to run_pipeline_process.sh, which packs "
                            "the generated repo after tokenizing. Omit to accept "
                            "the runner's own default")
    run_p.add_argument("--memory-limit", metavar="SIZE",
                       help="forward --memory-limit to step 10, the DuckDB "
                            "generator. Omit to accept that script's own 8GB "
                            "default. Measured 2026-09-15: the limit bounds "
                            "DuckDB's buffers, not the process, which settles at "
                            "about 1.4x the limit. Two projects at 8GB therefore "
                            "need ~22 GB, and the corpus run died on this 30 GB "
                            "box. Budget 1.4 x --jobs x SIZE. Takes an absolute "
                            "size such as 3GB, never a percentage")
    run_p.add_argument("--duckdb-threads", type=int, default=0, metavar="N",
                       help="forward --duckdb-threads to step 10. Each sorting "
                            "thread holds its own buffers, so fewer threads lower "
                            "the peak. Omit to accept the generator's default")
    run_p.add_argument("--project-meta", default="", metavar="PATH",
                       help="JSON sidecar from project_meta.py, carrying each "
                            "project's provenance (which roster found it, which "
                            "stratum it was assigned, its shared-history cluster "
                            "and the mask it was tokenized with) into the "
                            "Parquet. OMITTING THIS SILENTLY EMPTIES 29 COLUMNS "
                            "on every row of the project: clone_url, "
                            "provenance_status, source, stratum, fact, "
                            "contested, label_date, owner, repo, roster_name, "
                            "roster_lang, language, commits, size_class, "
                            "size_kb, stars, pushed_at, license, owner_type, "
                            "archived, fork, the seven history_* columns, "
                            "manifest_category and file_mask. Nothing fails: the "
                            "file keeps all 70 columns, validate.py passes it, "
                            "and a blank column is indistinguishable from "
                            "provenance that is genuinely unknown. ctp therefore "
                            "REFUSES a run that reaches step 10 without it, "
                            "unless you pass --allow-empty-provenance. The "
                            "matching --project-key is not yours to pass: ctp "
                            "sends the manifest name per project, so it cannot "
                            "be forgotten or mistyped")
    run_p.add_argument("--firm-map", default="", metavar="PATH",
                       help="domain->firm CSV (data/affiliation.merged.csv) to "
                            "join per row against person_domain, filling "
                            "firm_raw and firm_source. Unlike --project-meta "
                            "this is not a per-project constant: it is a real "
                            "join, so the map stays an external auditable file. "
                            "OMITTING THIS SILENTLY EMPTIES 3 COLUMNS on every "
                            "row — firm_raw, firm and firm_source — and firm "
                            "attribution is the measurement this corpus exists "
                            "for. The run still succeeds and still validates, so "
                            "ctp REFUSES a run that reaches step 10 without it, "
                            "unless you pass --allow-empty-provenance")
    run_p.add_argument("--firm-canonical", default="", metavar="PATH",
                       help="the reviewed canonical-name table "
                            "(data/firm_canonical.csv) that fills the `firm` "
                            "column. Needs --firm-map. OMITTING THIS DOES NOT "
                            "EMPTY `firm`, it fills it WRONGLY: `firm` repeats "
                            "`firm_raw` verbatim, so the 48 split spellings stay "
                            "split and every firm's share is understated. ctp "
                            "REFUSES a run that reaches step 10 with --firm-map "
                            "but without this, unless you pass "
                            "--allow-empty-provenance")
    run_p.add_argument("--allow-empty-provenance", action="store_true",
                       help="publish Parquets whose provenance and firm columns "
                            "are blank, which ctp refuses by default. There is "
                            "no way to reach this except by typing it, and that "
                            "is the point: omitting --project-meta used to print "
                            "nothing at all, at any layer, while emptying 29 "
                            "columns of a 6h48m run. Legitimate uses "
                            "are a test fixture, a one-project smoke run, and a "
                            "corpus whose sidecar does not exist yet. Taking it "
                            "prints, itemised, which columns were given up. Do "
                            "not leave it in a launcher script: ctp refuses it "
                            "when nothing would actually be blank, so it cannot "
                            "sit there silencing a later run")
    run_p.add_argument("--mask", default="", metavar="REGEX",
                       help="tokenize these files instead of the mask in the "
                            "manifest, for every project in this invocation. "
                            "Intended for one project at a time, with --only: "
                            "changing a project's mask forces a full rebuild, "
                            "because blobExec records the mask in the blob map "
                            "and refuses to resume against a different one. The "
                            "default is the manifest's file_filter column, which "
                            "generated manifests fill with the universal mask")
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

    pr_p = sub.add_parser("progress",
                          help="progress bar, last finished projects and an ETA")
    pr_p.add_argument("--manifest", default="manifest.tsv")
    pr_p.add_argument("--last", type=int, default=5, metavar="N",
                      help="how many finished projects to list (default 5)")
    pr_p.add_argument("--jobs", type=int, default=2,
                      help="concurrency to assume for the ETA (default 2). Set it "
                           "to the --jobs the run actually uses")
    pr_p.set_defaults(fn=cmd_progress)

    db_p = sub.add_parser("db", help="rebuild ctp.duckdb (tracking table + unified tokens view)")
    db_p.set_defaults(fn=cmd_db)

    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
