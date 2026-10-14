"""The command line of ctp.py: the subcommands, their options and the dispatch."""

from __future__ import annotations

import argparse

from ctplib import config, passes, report, scheduler

# The text of --help. Keep it byte for byte.
DESCRIPTION = (
    "ctp — cregit-token-pipeline runner: runs cregit over every project in manifest.tsv.\n"
    "Per project: pipeline -> firm -> validate -> stamp; a validated project is skipped.\n"
    "Ledgers: metrics.tsv (one row per phase attempt), runs.log, state/<name>/logs"
    " (never overwritten)."
)


def add_project_options(p: argparse.ArgumentParser) -> None:
    """The options of one project run, shared by `run` and `census`."""
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
    args = build_parser().parse_args()
    return args.fn(args)


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=DESCRIPTION)
    sub = ap.add_subparsers(dest="cmd", required=True)
    add_run_parser(sub)
    add_end_to_end_parser(sub)
    add_report_parsers(sub)
    return ap


def add_run_parser(sub) -> None:
    """`ctp.py run`: thread-pool passes over the manifest."""
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


def add_end_to_end_parser(sub) -> None:
    """`ctp.py census`: every project end to end, resumable."""
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


def add_report_parsers(sub) -> None:
    """status, progress, audit and db: they read the records and run no project."""
    st_p = sub.add_parser("status", help="one-screen pipeline status")
    st_p.add_argument("--manifest", default="manifest.tsv")
    st_p.set_defaults(fn=report.cmd_status)

    pr_p = sub.add_parser("progress", help="progress bar and last finished projects")
    pr_p.add_argument("--manifest", default="manifest.tsv")
    pr_p.add_argument(
        "--last",
        type=int,
        default=5,
        metavar="N",
        help="how many finished projects to list (default 5)",
    )
    pr_p.set_defaults(fn=report.cmd_progress)

    au_p = sub.add_parser("audit", help="one project's ledger rows")
    au_p.add_argument("project")
    au_p.add_argument("--json", action="store_true", help="print each row whole")
    au_p.set_defaults(fn=report.cmd_audit)

    db_p = sub.add_parser("db", help="rebuild ctp.duckdb (tracking table + unified tokens view)")
    db_p.add_argument(
        "--manifest",
        action="append",
        metavar="PATH",
        help="manifest to index, repeatable (default manifest.tsv)",
    )
    db_p.set_defaults(fn=report.cmd_db)
