"""The run-start and run-end rows of the ledger, and the flags they record."""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import ledger
import retain
from ctplib import artifacts, config, console, runtime, toolchain


def append_runs_log(event: str, *fields: str) -> None:
    """One tab-separated row of runs.log: the time, the event, then its fields."""
    with config.RUNS_LOG.open("a") as f:
        f.write("\t".join((console.now_iso(), event, *fields)) + "\n")


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
