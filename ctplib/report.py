"""The read-only commands: status, progress and audit, and db, which rebuilds ctp.duckdb."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys

import ledger
import retain
from ctplib import config, manifest_file, runtime, toolchain


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
