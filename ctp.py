#!/usr/bin/env python3
"""ctp — cregit-token-pipeline runner: runs cregit over every project in manifest.tsv.
Per project: pipeline -> firm -> validate -> stamp; a validated project is skipped.
Ledgers: metrics.tsv (one row per phase attempt), runs.log, state/<name>/logs (never overwritten)."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys

import ledger
import retain
from ctplib import (
    config,
    manifest_file,
    passes,
    runtime,
    scheduler,
    toolchain,
)

# --------------------------------------------------------------------------- #
# census: the scheduler for a long end-to-end run
# --------------------------------------------------------------------------- #


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
    run_p.set_defaults(fn=passes.cmd_run)

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
        default=scheduler.MIN_FREE_MEM_GB,
        metavar="G",
        help="start a project only with this much MemAvailable "
        f"(default {scheduler.MIN_FREE_MEM_GB})",
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
        default=scheduler.POLL_S,
        metavar="S",
        help=f"seconds between gate and stop checks (default {scheduler.POLL_S:g})",
    )
    add_project_options(ce_p)
    ce_p.set_defaults(fn=scheduler.cmd_census)

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
