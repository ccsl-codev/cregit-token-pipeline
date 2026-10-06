#!/usr/bin/env python3
"""ctp — cregit-token-pipeline runner: runs cregit over every project in manifest.tsv.
Per project: pipeline -> validate -> stamp; a validated project is skipped.
Ledgers: metrics.tsv (one row per phase attempt), runs.log, state/<name>/logs (never overwritten)."""

from __future__ import annotations

import argparse
import fcntl
import functools
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path

import configparser

import retain
from retain import lock_path, state_dir
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
# Not created at import: cmd_run and cmd_db create it when they need it.
METRICS = CORPUS / "metrics.tsv"
RUNS_LOG = CORPUS / "runs.log"


DEVENV = Path.home() / ".nix-profile/bin/devenv"
# devenv needs the nix daemon env; a bare PATH prepend fails in non-login
# shells with: error: not an absolute path: "nix"
NIX_DAEMON_SH = "/nix/var/nix/profiles/default/etc/profile.d/nix-daemon.sh"

DISK_FLOOR_GB = 150
HEARTBEAT_S = 30

# A separate ledger, because metrics.tsv's seven-field row is a published contract.
RESOURCES = CORPUS / "resources.tsv"
RESOURCE_SAMPLE_S = 15
RESOURCE_FIELDS = ("iso", "project", "class", "phase", "elapsed_s",
                   "tree_rss_mb", "tree_procs", "cpu_pct", "mem_used_mb",
                   "disk_free_gb", "load1")

# Sent for every project; cmd_run preflights them, so an upstream rename fails once.
REQUIRED_RUNNER_FLAGS = ("--repo-url", "--repo-name", "--work", "--mask")

# The last step, which writes the Parquet. The runner accepts --from-step 11+ and
# exits 0 having done nothing, so such a run cannot publish a blank Parquet.
DATASET_STEP = 10

MANIFEST_FIELDS = ("name", "url", "category", "file_filter", "size_class")
SIZE_CLASSES = ("S", "M", "L")

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
    for lineno, line in enumerate(path.read_text().splitlines(), 1):
        if not line.strip() or line.startswith("#"):
            continue
        fields = line.split("\t")
        if len(fields) != len(MANIFEST_FIELDS):
            raise ValueError(f"{path}:{lineno}: expected {len(MANIFEST_FIELDS)} tab-separated"
                             f" fields, got {len(fields)}: {line!r}")
        row = dict(zip(MANIFEST_FIELDS, fields))
        if row["size_class"] not in SIZE_CLASSES:
            raise ValueError(f"{path}:{lineno}: size_class {row['size_class']!r}"
                             f" is not one of {', '.join(SIZE_CLASSES)}")
        if only and row["name"] not in only:
            continue
        # blobExec rejects an empty mask; a blank column means the universal one.
        row["file_filter"] = row["file_filter"] or UNIVERSAL_MASK
        projects.append(row)
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

# Set once by cmd_run: run_project goes through pool.map and takes only the project.
_OPTS: dict = {}


@functools.lru_cache(maxsize=None)
def _script_supports_cached(cregit: Path, flag: str) -> bool:
    try:
        text = (cregit / "run_pipeline_process.sh").read_text()
    except OSError:
        return False
    # A whole flag: --mask is not supported merely because --mask-widened appears.
    return re.search(rf"(?<![\w-]){re.escape(flag)}(?![\w-])", text) is not None


def script_supports(flag: str) -> bool:
    """True when the configured run_pipeline_process.sh advertises `flag`.
    The cache keys on CREGIT too, so it stays right when CREGIT is reassigned."""
    return _script_supports_cached(CREGIT, flag)


_SIZE_RE = re.compile(r"^(\d+(?:\.\d+)?)\s?(B|K|M|G|T|KB|MB|GB|TB|KIB|MIB|GIB|TIB)$")
_SIZE_UNITS = {"B": 1, "K": 1024, "KB": 1024, "KIB": 1024,
               "M": 1024 ** 2, "MB": 1024 ** 2, "MIB": 1024 ** 2,
               "G": 1024 ** 3, "GB": 1024 ** 3, "GIB": 1024 ** 3,
               "T": 1024 ** 4, "TB": 1024 ** 4, "TIB": 1024 ** 4}

# memory_limit bounds DuckDB's buffer manager, not the process; RSS runs about this much higher.
SETTLE_RATIO = 1.4


def size_to_bytes(text: str) -> int:
    """Bytes for a DuckDB size string, else ValueError. Mirrors parse_memory_limit()
    in generate_dataset.py, so a bad value fails before the run, not hours in at step 10."""
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
    """A warning when SETTLE_RATIO x jobs x limit does not fit in MemAvailable, else None."""
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
    """Map pid -> (ppid, rss_kb) for every readable process. One that exits
    between listdir and read is skipped: cregit reaps short-lived processes constantly."""
    table: dict[int, tuple[int, int]] = {}
    for entry in proc_root.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            # Field 4 is ppid, field 24 rss in pages. Field 2, the command name,
            # may hold spaces and brackets, so split after the last ")".
            raw = (entry / "stat").read_text()
            fields = raw[raw.rindex(")") + 2:].split()
            table[int(entry.name)] = (int(fields[1]), int(fields[21]) * 4)
        except (OSError, ValueError, IndexError):
            continue
    return table


def tree_usage(root_pid: int, table: dict | None = None) -> tuple[int, int]:
    """(RSS in MB, process count) for root_pid and its descendants: the pipeline
    shell spawns perl, git and srcml, so the child's own RSS understates the run.
    `seen` stops a cycle in a malformed parent map from hanging the sampler."""
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
    """Samples RAM, CPU and disk while one phase runs: every sample to resources.tsv,
    the maxima to .peak. A daemon thread with every sample wrapped, so a sampling
    failure can never fail the phase it measures."""

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
        # Sample before the first wait, or a phase shorter than the interval records nothing.
        while True:
            try:
                busy, total = self._sample(busy, total)
            except Exception as e:
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


def shard_class(project: dict) -> bool:
    """True when --shards asks for sharding and this project's class is in --shard-classes.
    For S and M, concurrent projects already fill the machine; shards would only cost disk."""
    return (_OPTS.get("shards", 0) > 1
            and project["size_class"] in _OPTS.get("shard_classes", ()))


def unique_log_name(logdir: Path, phase: str) -> Path:
    """A retry inside the same second gets a _N suffix, which sorts after the plain name."""
    stamp = f"{datetime.now():%Y%m%dT%H%M%S}"
    logfile = logdir / f"{phase}-{stamp}.log"
    n = 1
    while logfile.exists():
        logfile = logdir / f"{phase}-{stamp}_{n}.log"
        n += 1
    return logfile


def run_phase(project: dict, phase: str, args: list[str]) -> int:
    name, cls = project["name"], project["size_class"]
    logdir = state_dir(name) / "logs"
    logdir.mkdir(parents=True, exist_ok=True)
    logfile = unique_log_name(logdir, phase)

    _live[name] = (phase, time.time())
    say(f"{name} ▶ {phase} started (log: {logfile})")
    latest = logdir / f"{phase}-latest.log"
    tmp = logdir / f".{phase}-latest.tmp"
    logfile.touch()
    tmp.unlink(missing_ok=True)  # a crash can leave this behind; symlink_to refuses to overwrite
    tmp.symlink_to(logfile)
    tmp.rename(latest)
    start = time.time()
    sampler = ResourceSampler(project, phase)
    # Popen, not run: the sampler needs the child's pid to attribute RSS to this project.
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


class RunOutcome(StrEnum):
    """The result of one run_project call. Values match the strings already
    written to metrics.tsv and runs.log."""
    DONE = "done"
    SKIPPED = "skipped"
    FAILED = "failed"
    DEFERRED = "deferred"

    @property
    def is_success(self) -> bool:
        return self in (RunOutcome.DONE, RunOutcome.SKIPPED)


def check_memo_dir(name: str, opts: dict, workdir: Path) -> bool:
    """False, after saying why, when --memo-dir would put the memo inside the
    workdir the runner wipes."""
    memo_dir_opt = opts.get("memo_dir")
    if not memo_dir_opt:
        return True
    memo_dir = Path(memo_dir_opt).resolve() / name
    resolved_work = workdir.resolve()
    if memo_dir == resolved_work or resolved_work in memo_dir.parents:
        say(f"{name} — refusing to run: --memo-dir puts the memo at "
            f"{memo_dir}, inside the work directory the runner deletes. "
            "Point --memo-dir outside the corpus output directory.")
        return False
    return True


_SWITCH_FLAGS = (("skip_html", "--skip-html"), ("reblame", "--reblame"),
                 ("mask_widened", "--mask-widened"))
# The runner calls blame_jobs --jobs; ctp's own --jobs means concurrent projects.
_VALUE_FLAGS = (("gc", "--gc"), ("blame_jobs", "--jobs"),
                ("memory_limit", "--memory-limit"), ("duckdb_threads", "--duckdb-threads"))


def value_args(opts: dict, table: tuple) -> list[str]:
    """[flag, value] for every (option, flag) in table whose option is set."""
    return [arg for opt, flag in table if opts.get(opt) for arg in (flag, str(opts[opt]))]


def build_pipeline_args(project: dict, opts: dict, workdir: Path) -> list[str]:
    """The run_pipeline_process.sh argv for one project. Assumes check_memo_dir
    has already refused an unsafe --memo-dir."""
    name = project["name"]
    pipeline_args = [
        "./run_pipeline_process.sh",
        "--repo-url", project["url"],
        "--repo-name", name,
        "--work", str(workdir),
        "--mask", opts.get("mask") or project["file_filter"],
    ]
    pipeline_args += [flag for opt, flag in _SWITCH_FLAGS if opts.get(opt)]
    pipeline_args += value_args(opts, (("retokenize", "--retokenize"),))
    # One subdirectory per project: tokenBySha.pl keys the memo on the content
    # sha1 alone, so projects sharing one would serve each other's tokens.
    if opts.get("memo_dir"):
        pipeline_args += ["--memo-dir", str(Path(opts["memo_dir"]).resolve() / name)]
    if shard_class(project):
        pipeline_args += ["--mode", "sharded", "--shards", str(opts["shards"])]
    pipeline_args += value_args(opts, _VALUE_FLAGS)
    # The sidecar is keyed by manifest name; the generator refuses a key it cannot find.
    if opts.get("project_meta"):
        pipeline_args += ["--project-meta", str(opts["project_meta"]), "--project-key", name]
    # FROM_STEP is positional and must come last.
    from_step = opts.get("from_step", 1)
    if from_step > 1:
        pipeline_args.append(str(from_step))
    return pipeline_args


def run_project(project: dict) -> RunOutcome:
    """Runs one project's pipeline and validate phases. Returns a RunOutcome."""
    name = project["name"]
    workdir = OUT / name
    stamp = workdir / f"{name}.validated"
    if stamp.exists():
        say(f"{name} — already validated, skip")
        return RunOutcome.SKIPPED

    workdir.mkdir(parents=True, exist_ok=True)
    state_dir(name).mkdir(parents=True, exist_ok=True)

    with lock_path(name).open("w") as lockfile:
        try:
            fcntl.flock(lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            say(f"{name} — another run holds the lock, skipping")
            return RunOutcome.DEFERRED

        try:
            if shutil.disk_usage(OUT).free < DISK_FLOOR_GB * 2**30:
                say(f"{name} — disk below {DISK_FLOOR_GB}G floor, deferred")
                return RunOutcome.DEFERRED

            if not check_memo_dir(name, _OPTS, workdir):
                return RunOutcome.FAILED

            try:
                rc = run_phase(project, "pipeline", build_pipeline_args(project, _OPTS, workdir))
                if rc == 0:
                    rc = run_phase(project, "validate", [
                        "python3", str(CORPUS / "validate.py"),
                        str(workdir / f"{name}-dataset.parquet"), str(stamp),
                    ])
            except OSError as exc:
                say(f"{name} ✗ a phase could not start: {exc}")
                return RunOutcome.FAILED
            if rc != 0:
                return RunOutcome.FAILED

            if _OPTS.get("drop_memo"):
                # prune checks the keepers itself.
                _reclaimed, ok = retain.prune(name, ("memo",), apply=True)
                if not ok:
                    say(f"{name} — memo/ kept, retain refused the prune (see above)")
            return RunOutcome.DONE
        finally:
            _live.pop(name, None)


def heartbeat(stop: threading.Event, results: dict) -> None:
    while not stop.wait(HEARTBEAT_S):
        running = ", ".join(f"{n}({p} {int(time.time() - t)}s)" for n, (p, t) in sorted(_live.items()))
        free_gb = shutil.disk_usage(OUT).free // 2**30
        done = sum(1 for v in results.values() if v.is_success)
        failed = sum(1 for v in results.values() if v == RunOutcome.FAILED)
        say(f"♥ running: {running or '—'} | done {done} failed {failed} | disk {free_gb}G free")


# What forwarding each optional flag to a checkout without it would do.
REQUIRES: dict[str, str] = {
    "--skip-html": ("Patch that checkout to guard its HTML step, then re-run. Refusing to "
                    "start: generating the HTML and deleting it later is not what the flag says."),
    "--reblame": ("That checkout's step 7 cannot replace existing .blame files, so the run "
                  "would skip every one of them and exit 0. Refusing to start."),
    "--memo-dir": ("That checkout hard-codes BFG_MEMO_DIR to <work>/memo, which a step-1 "
                   "run deletes, so the flag would be silently dropped. Patch it first."),
    "--shards": ("Point pipeline.cfg at a checkout that supports sharded mode, "
                 "or drop --shards and accept the single-process rate."),
    "--gc": ("That checkout still packs unconditionally, and an unguarded "
             "repack failure deletes the workdir. Patch it before relying on --gc."),
    "--blame-jobs": ("That checkout blames serially, at roughly 3 files per minute. "
                     "Patch it before relying on the flag."),
    "--memory-limit": ("That checkout runs step 10 at the generator's own default, so the "
                        "flag would be silently dropped. Patch it before relying on it."),
    "--duckdb-threads": "Patch it before relying on the flag.",
    "--mask-widened": ("That checkout would drop the flag, and blobExec would then refuse "
                        "every project whose recorded mask differs from the manifest's "
                        "(exit 3)."),
    "--retokenize": ("That checkout would drop the flag, step 2 would reuse the cached "
                      "tokenizations you asked to discard, and the run would exit 0 having "
                      "changed nothing. Check pipeline.cfg points at a checkout that has it."),
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
    missing = [f for f in REQUIRED_RUNNER_FLAGS if not script_supports(f)]
    if missing:
        sys.exit(f"{CREGIT}/run_pipeline_process.sh does not accept: {', '.join(missing)}.\n"
                 "ctp.py passes these to every project and the runner exits 2 on an\n"
                 "unknown flag. Check pipeline.cfg points at the right checkout, or\n"
                 "update REQUIRED_RUNNER_FLAGS and run_project together.")
    if args.reblame and args.from_step > 7:
        sys.exit(f"--reblame needs --from-step 7 or less (got {args.from_step}).\n"
                 "The re-blame happens inside step 7; from step 8 the flag is skipped and\n"
                 "step 10 rebuilds the Parquet from the blame already on disk.")
    refuse_memo_conflicts(args)
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
        say("note: --no-memo is an alias for --drop-memo and does NOT prevent the write. "
            "The tokenizer requires BFG_MEMO_DIR, so memo/ is built and then deleted.")
    if not args.memo_dir:
        return
    if args.drop_memo:
        sys.exit("--memo-dir and --drop-memo contradict each other: one puts the memo "
                 "where no wipe can reach it, the other deletes it after each project.\n"
                 "--drop-memo also only prunes <workdir>/memo, so it would not even find "
                 "an external memo. Pick one.")
    if not Path(args.memo_dir).is_dir():
        sys.exit(f"--memo-dir {args.memo_dir} is not an existing directory. Create it "
                 "first: a typo here would quietly start a second corpus of memos "
                 "instead of reusing the one you meant.")


def refuse_bad_values(args: argparse.Namespace) -> None:
    for flag, value in (("--blame-jobs", args.blame_jobs),
                        ("--duckdb-threads", args.duckdb_threads)):
        if value < 0:
            sys.exit(f"{flag} cannot be negative (got {value}).")
    if args.memory_limit:
        try:
            size_to_bytes(args.memory_limit)
        except ValueError as exc:
            sys.exit(f"--memory-limit: {exc}")
    if args.from_step < 1:
        sys.exit(f"--from-step must be 1 or greater (got {args.from_step}).")


def requested_runner_flags(args: argparse.Namespace, mask_widened: bool,
                           retokenize: str) -> list[str]:
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
    }
    return [flag for flag in REQUIRES if requested[flag]]


def refuse_unsupported_flags(flags: list[str]) -> None:
    for flag in flags:
        gap = [f for f in CHECKS.get(flag, (flag,)) if not script_supports(f)]
        if not gap:
            continue
        if flag in CHECKS:
            sys.exit(f"{flag} needs {', '.join(gap)}, which "
                     f"{CREGIT}/run_pipeline_process.sh does not advertise.\n{REQUIRES[flag]}")
        sys.exit(f"{flag} is not implemented by {CREGIT}/run_pipeline_process.sh.\n{REQUIRES[flag]}")


def refuse_mask_widened_conflicts(args: argparse.Namespace) -> None:
    if args.from_step < 2:
        sys.exit("--mask-widened needs --from-step 2 or greater. Step 1 deletes the project\n"
                 "workdir, taking with it the blob map this flag reuses and the cregit.git\n"
                 "its new_blob ids live in, so there would be nothing left to preserve.")
    if args.shards > 1:
        sys.exit("--mask-widened cannot be combined with sharding: each shard builds a fresh\n"
                 "blob map, so there is no recorded mask to widen.")


def refuse_retokenize_conflicts(args: argparse.Namespace, mask_widened: bool) -> None:
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


def refuse_unless_runner_accepts(flag: str, needed: tuple[str, ...], consequence: str = "") -> None:
    missing = [f for f in needed if not script_supports(f)]
    if missing:
        sys.exit(f"{flag} needs {', '.join(missing)}, which "
                 f"{CREGIT}/run_pipeline_process.sh does not accept.{consequence}")


def resolve_project_meta(path: str) -> str:
    if not Path(path).exists():
        sys.exit(f"--project-meta {path} does not exist. "
                 "The sidecar format is documented in validate_schema.py, "
                 "in the comment above EXPECTED_COLUMNS.")
    refuse_unless_runner_accepts("--project-meta", ("--project-meta", "--project-key"))
    # Absolute: the runner starts with cwd=CREGIT, not this repository.
    return str(Path(path).resolve())


def provenance_gaps(project_meta: str) -> list[tuple[str, str]]:
    """(flag, what publishing without it costs) for each missing provenance flag."""
    if project_meta:
        return []
    return [("--project-meta",
             "29 provenance columns empty on every row: clone_url, "
             "provenance_status, source, stratum, fact, contested, "
             "label_date, owner, repo, roster_name, roster_lang, "
             "language, commits, size_class, size_kb, stars, pushed_at, "
             "license, owner_type, archived, fork, history_cluster, "
             "history_shared_with, history_relation, history_includes, "
             "history_first, history_created, manifest_category, "
             "file_mask")]


PROVENANCE_REFUSAL_TAIL = (
    "This does not fail anything. The file keeps all 67 columns in the "
    "right order, so validate.py passes it and no consumer can tell a "
    "blank column from provenance that is genuinely unknown, which is "
    "why a long, expensive run can finish and publish silently "
    "inconsistent with the rest of the corpus.\n"
    "Pass the flag this run is missing:\n"
    "  --project-meta <your-project-meta>.json\n"
    "If blank columns are genuinely what you want — a fixture, a "
    "one-project smoke run, a corpus whose sidecar does not exist yet — "
    "say so with --allow-empty-provenance.")


def enforce_provenance(gaps: list[tuple[str, str]], args: argparse.Namespace) -> None:
    """Exit when a run reaching step 10 has a provenance gap, unless
    --allow-empty-provenance. Refuses rather than warns: nothing later can tell
    a blank column from provenance that is genuinely unknown."""
    allow_empty = bool(args.allow_empty_provenance)
    if allow_empty and not gaps:
        sys.exit("--allow-empty-provenance has nothing to allow: --project-meta "
                 "is present, so no column would be blank. Drop the flag — left "
                 "in a wrapper it would silence the guard on the next run that "
                 "omits --project-meta.")
    if not gaps or args.from_step > DATASET_STEP:
        return
    itemised = "".join(f"  {flag} absent — {cost}\n" for flag, cost in gaps)
    if not allow_empty:
        sys.exit(f"refusing this run: it reaches step {DATASET_STEP} and would publish a "
                 f"Parquet with blank provenance.\n{itemised}{PROVENANCE_REFUSAL_TAIL}")
    say("WARNING: --allow-empty-provenance: this run will publish a Parquet "
        "with blank provenance, on purpose.")
    for flag, cost in gaps:
        say(f"         {flag} absent — {cost}")
    say("         Do not mix these rows into the corpus: they are "
        "schema-valid and indistinguishable from rows whose provenance is "
        "genuinely unknown.")


def announce_run(opts: dict, jobs: int) -> None:
    """Log every choice that changes what this run publishes or how it behaves,
    so a reader of the log need not infer it from the absence of a message."""
    if opts["mask"]:
        say(f"WARNING: --mask overrides the manifest for every project in this "
            f"run: {opts['mask']}")
        say("         the --project-meta sidecar records the MANIFEST's mask, so the "
            "Parquet's file_mask column will not match this run.")
    if opts["memory_limit"]:
        warning = memory_budget_warning(opts["memory_limit"], jobs)
        if warning:
            say(f"WARNING: {warning}")
    if opts["from_step"] > 1:
        say(f"resuming at step {opts['from_step']}: the runner keeps the existing workdir")
    if opts["retokenize"]:
        say(f"--retokenize {opts['retokenize']}: step 2 will DISCARD the cached tokenizations "
            f"for these extensions and redo them.")
        say("         Every other extension's cached work is kept. blobExec drops the "
            "blob_map rows and the memo entries together, and refuses the run (exit 7) "
            "if the request would have invalidated nothing — so a typo cannot pass as "
            "a successful re-run.")
    if opts["mask_widened"]:
        say("--mask-widened: step 2 will REUSE each project's existing tokenizations "
            "instead of redoing them.")
        say("         blobExec verifies per project, against the rows: every "
            "already-tokenized path must still be selected by the new mask, and the "
            "retained new_blob ids must exist in cregit.git. Either check failing "
            "refuses that project (rc 3) and changes nothing.")
        say("         tree_map, commit_map, ref_map and blob_map's identity rows are "
            "discarded, so files the wider mask newly selects are tokenized rather "
            "than passed through as raw source.")
    if opts["shards"] > 1:
        say(f"sharding {opts['shards']}-way for size class"
            f"{'es' if len(opts['shard_classes']) > 1 else ''} {', '.join(opts['shard_classes'])}")


def run_options(args: argparse.Namespace) -> dict:
    """The checked options run_project reads through _OPTS. Exits on a refusal."""
    mask_widened, retokenize = preflight_runner_flags(args)
    project_meta = resolve_project_meta(args.project_meta) if args.project_meta else ""
    enforce_provenance(provenance_gaps(project_meta), args)
    return dict(skip_html=args.skip_html, drop_memo=args.drop_memo,
                reblame=args.reblame,
                memo_dir=args.memo_dir,
                shards=args.shards,
                shard_classes=tuple(c.strip() for c in args.shard_classes.split(",") if c.strip()),
                from_step=args.from_step, gc=args.gc,
                mask_widened=mask_widened,
                retokenize=retokenize,
                blame_jobs=args.blame_jobs,
                memory_limit=args.memory_limit,
                duckdb_threads=args.duckdb_threads,
                mask=args.mask,
                project_meta=project_meta)


def run_passes(projects: list[dict], jobs: int, retries: int) -> dict:
    """name -> RunOutcome after the first pass and up to `retries` passes over the rest."""
    results: dict = {}
    stop = threading.Event()
    threading.Thread(target=heartbeat, args=(stop, results), daemon=True).start()
    try:
        for attempt in range(1 + retries):
            todo = [p for p in projects
                    if results.get(p["name"]) not in (RunOutcome.DONE, RunOutcome.SKIPPED)]
            if not todo:
                break
            if attempt:
                say(f"retry pass {attempt}: {[p['name'] for p in todo]}")
            with ThreadPoolExecutor(max_workers=jobs) as pool:
                for p, res in zip(todo, pool.map(run_project, todo)):
                    results[p["name"]] = res
    finally:
        stop.set()
    return results


def cmd_run(args: argparse.Namespace) -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    projects = read_manifest(CORPUS / args.manifest, set(args.only.split(",")) if args.only else None)
    if not projects:
        say("nothing to run (empty manifest / --only filter matched nothing)")
        return 0

    _OPTS.update(run_options(args))
    announce_run(_OPTS, args.jobs)

    run_start = time.time()
    say("capturing devenv environment (once)...")
    _ENV.update(capture_devenv_env())
    say(f"devenv environment captured ({len(_ENV)} vars)")
    with RUNS_LOG.open("a") as f:
        f.write(f"{now_iso()}\trun-start\tjobs={args.jobs}\tprojects={len(projects)}\n")

    results = run_passes(projects, args.jobs, args.retries)

    rc = 0 if all(v.is_success for v in results.values()) else 1
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
        state = retain.project_state(p["name"], OUT / p["name"])
        if state == "DONE":
            done += 1
        print(f"{p['name']:<16} {p['size_class']:<6} {state:<9} {last.get(p['name'], '—')}")

    # Before the first run OUT may not exist; measure the file system it will be on.
    free_gb = shutil.disk_usage(next(p for p in (OUT, *OUT.parents) if p.exists())).free // 2**30
    print(f"\nprogress: {done}/{len(projects)} validated | disk {free_gb}G free")
    return 0


BAR_WIDTH = 30


def bar(done: int, total: int, width: int = BAR_WIDTH) -> str:
    """A fixed-width progress bar. Never divides by zero on an empty manifest."""
    filled = round(width * done / total) if total else 0
    return "█" * filled + "░" * (width - filled)


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


def class_breakdown(projects: list[dict], states: dict) -> str:
    """"S 3/10  M 1/4": validated over total, for each size class present."""
    per_class = []
    for cls in SIZE_CLASSES:
        members = [p for p in projects if p["size_class"] == cls]
        if members:
            hits = sum(1 for p in members if states[p["name"]] == "DONE")
            per_class.append(f"{cls} {hits}/{len(members)}")
    return "  ".join(per_class)


def cmd_progress(args: argparse.Namespace) -> int:
    """A one-screen progress bar and the last finished projects."""
    projects = read_manifest(CORPUS / args.manifest, None)
    states = {p["name"]: retain.project_state(p["name"], OUT / p["name"]) for p in projects}
    by_state: dict[str, list[dict]] = {}
    for p in projects:
        by_state.setdefault(states[p["name"]], []).append(p)
    done, failed = len(by_state.get("DONE", [])), len(by_state.get("FAILED", []))
    total = len(projects)
    pct = 100 * done / total if total else 0.0

    print(f"corpus  [{bar(done, total)}]  {done}/{total}  {pct:.1f}%")
    print(f"        {class_breakdown(projects, states)}"
          f"{'  |  FAILED ' + str(failed) if failed else ''}")
    print_listing("running", [f"{p['name']:<34} {p['size_class']}"
                              for p in by_state.get("RUNNING", [])])
    recent = finished_runs()[-args.last:]
    print_listing(f"last {len(recent)} finished",
                  [f"{name:<34} {cls}  {dur / 60:6.1f} min" for name, cls, dur in reversed(recent)])
    return 0


def print_listing(title: str, rows: list[str]) -> None:
    """A blank line, the title and the indented rows; nothing when rows is empty."""
    if rows:
        print(f"\n{title}")
        for row in rows:
            print(f"  {row}")


def cmd_db(args: argparse.Namespace) -> int:
    """Rebuild ctp.duckdb (derived index over stamps/metrics/parquets)."""
    OUT.mkdir(parents=True, exist_ok=True)
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
                       help="never generate the HTML views (94-255 MB per project)")
    run_p.add_argument("--reblame", action="store_true",
                       help="re-blame every file in step 7 instead of skipping those "
                            "with .blame output, after the blame itself changed. "
                            "Not for resuming an interrupted run")
    run_p.add_argument("--drop-memo", "--no-memo", action="store_true",
                       help="delete memo/ (45-88%% of the workdir) once a project "
                            "validates. The tokenizer still writes it first")
    run_p.add_argument("--memo-dir", default="", metavar="DIR",
                       help="keep each project's memo in DIR/<project>, outside the "
                            "workdir a step-1 run deletes. DIR must exist. "
                            "Cannot be combined with --drop-memo")
    run_p.add_argument("--shards", type=int, default=0,
                       help="tokenize in N shards (needs >1). Costs transient disk, "
                            "so it is limited to --shard-classes")
    run_p.add_argument("--shard-classes", default="L",
                       help="comma-separated size classes to shard (default L)")
    run_p.add_argument("--from-step", type=int, default=1, metavar="N",
                       help="resume the runner at step N. Only step 1 wipes the "
                            "workdir, so N>1 keeps finished work")
    run_p.add_argument("--retokenize", metavar="EXTS", default="",
                       help="re-tokenize only these extensions (comma separated, no "
                            "dots), after a tokenizer fix. Needs --from-step 2 "
                            "exactly; not with --mask-widened or sharding")
    run_p.add_argument("--mask-widened", action="store_true",
                       help="reuse existing tokenizations across a mask change. "
                            "Needs --from-step 2 or more; blobExec verifies each project")
    run_p.add_argument("--gc", choices=("none", "plain", "aggressive"),
                       help="how the runner packs the generated repo. Omit for "
                            "the runner's default")
    run_p.add_argument("--memory-limit", metavar="SIZE",
                       help="DuckDB memory limit for step 10, an absolute size such "
                            "as 3GB. The process settles at about 1.4x, so budget "
                            "1.4 x --jobs x SIZE. Omit for the generator's 8GB")
    run_p.add_argument("--duckdb-threads", type=int, default=0, metavar="N",
                       help="DuckDB threads for step 10; fewer lower the peak. "
                            "Omit for the generator's default")
    run_p.add_argument("--project-meta", default="", metavar="PATH",
                       help="JSON provenance sidecar keyed by project name (format: "
                            "validate_schema.py, above EXPECTED_COLUMNS). Without it "
                            "29 columns are blank and ctp refuses the run")
    run_p.add_argument("--allow-empty-provenance", action="store_true",
                       help="publish with blank provenance columns, e.g. "
                            "for a fixture or a smoke run. Refused when nothing "
                            "would be blank")
    run_p.add_argument("--mask", default="", metavar="REGEX",
                       help="tokenize these files instead of the manifest's mask. "
                            "Use with --only: a changed mask forces a full rebuild")
    run_p.add_argument("--blame-jobs", type=int, default=0, metavar="N",
                       help="parallel blame workers; the output does not depend "
                            "on N. Omit for the runner's default of 1")
    run_p.set_defaults(fn=cmd_run)

    st_p = sub.add_parser("status", help="one-screen pipeline status")
    st_p.add_argument("--manifest", default="manifest.tsv")
    st_p.set_defaults(fn=cmd_status)

    pr_p = sub.add_parser("progress",
                          help="progress bar and last finished projects")
    pr_p.add_argument("--manifest", default="manifest.tsv")
    pr_p.add_argument("--last", type=int, default=5, metavar="N",
                      help="how many finished projects to list (default 5)")
    pr_p.set_defaults(fn=cmd_progress)

    db_p = sub.add_parser("db", help="rebuild ctp.duckdb (tracking table + unified tokens view)")
    db_p.set_defaults(fn=cmd_db)

    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
