"""Checks before a run starts: the runner flags, the provenance sidecar and the option
conflicts. A failed check exits the process."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from ctplib import config, console, memory, runner_args

# Sent for every project; cmd_run preflights them, so an upstream rename fails once.
REQUIRED_RUNNER_FLAGS = ("--repo-url", "--repo-name", "--work", "--mask")

# The last step, which writes the Parquet. The runner accepts --from-step 11+ and
# exits 0 having done nothing, so such a run cannot publish a blank Parquet.
DATASET_STEP = 10

# What forwarding each optional flag to a checkout without it would do.
REQUIRES: dict[str, str] = {
    "--skip-html": (
        "Patch that checkout to guard its HTML step, then re-run. Refusing to "
        "start: generating the HTML and deleting it later is not what the flag says."
    ),
    "--reblame": (
        "That checkout's step 7 cannot replace existing .blame files, so the run "
        "would skip every one of them and exit 0. Refusing to start."
    ),
    "--memo-dir": (
        "That checkout hard-codes BFG_MEMO_DIR to <work>/memo, which a step-1 "
        "run deletes, so the flag would be silently dropped. Patch it first."
    ),
    "--shards": (
        "Point pipeline.cfg at a checkout that supports sharded mode, "
        "or drop --shards and accept the single-process rate."
    ),
    "--gc": (
        "That checkout still packs unconditionally, and an unguarded "
        "repack failure deletes the workdir. Patch it before relying on --gc."
    ),
    "--blame-jobs": (
        "That checkout blames serially, at roughly 3 files per minute. "
        "Patch it before relying on the flag."
    ),
    "--memory-limit": (
        "That checkout runs step 10 at the generator's own default, so the "
        "flag would be silently dropped. Patch it before relying on it."
    ),
    "--duckdb-threads": "Patch it before relying on the flag.",
    "--mask-widened": (
        "That checkout would drop the flag, and blobExec would then refuse "
        "every project whose recorded mask differs from the manifest's "
        "(exit 3)."
    ),
    "--tokenizer-worker": (
        "That checkout has no tokenizer worker pool, so step 2 would "
        "start one tokenizer process per blob. Point pipeline.cfg at a "
        "checkout with cregit #102 or later."
    ),
    "--retokenize": (
        "That checkout would drop the flag, step 2 would reuse the cached "
        "tokenizations you asked to discard, and the run would exit 0 having "
        "changed nothing. Check pipeline.cfg points at a checkout that has it."
    ),
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
    missing = [f for f in REQUIRED_RUNNER_FLAGS if not runner_args.script_supports(f)]
    if missing:
        sys.exit(
            f"{config.CREGIT}/run_pipeline_process.sh does not accept: {', '.join(missing)}.\n"
            "ctp.py passes these to every project and the runner exits 2 on an\n"
            "unknown flag. Check pipeline.cfg points at the right checkout, or\n"
            "update REQUIRED_RUNNER_FLAGS and run_project together."
        )
    if args.reblame and args.from_step > 7:
        sys.exit(
            f"--reblame needs --from-step 7 or less (got {args.from_step}).\n"
            "The re-blame happens inside step 7; from step 8 the flag is skipped and\n"
            "step 10 rebuilds the Parquet from the blame already on disk."
        )
    refuse_memo_conflicts(args)
    if getattr(args, "tokenizer_worker", False) and args.shards > 1:
        sys.exit(
            "--tokenizer-worker needs the runner's pipeline mode, and --shards runs "
            "sharded mode. Use one of them."
        )
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
        console.say(
            "note: --no-memo is an alias for --drop-memo and does NOT prevent the write. "
            "The tokenizer requires BFG_MEMO_DIR, so memo/ is built and then deleted."
        )
    if not args.memo_dir:
        return
    if args.drop_memo:
        sys.exit(
            "--memo-dir and --drop-memo contradict each other: one puts the memo "
            "where no wipe can reach it, the other deletes it after each project.\n"
            "--drop-memo also only prunes <workdir>/memo, so it would not even find "
            "an external memo. Pick one."
        )
    if not Path(args.memo_dir).is_dir():
        sys.exit(
            f"--memo-dir {args.memo_dir} is not an existing directory. Create it "
            "first: a typo here would quietly start a second corpus of memos "
            "instead of reusing the one you meant."
        )


def refuse_bad_values(args: argparse.Namespace) -> None:
    for flag, value in (
        ("--blame-jobs", args.blame_jobs),
        ("--duckdb-threads", args.duckdb_threads),
    ):
        if value < 0:
            sys.exit(f"{flag} cannot be negative (got {value}).")
    if args.memory_limit:
        try:
            memory.size_to_bytes(args.memory_limit)
        except ValueError as exc:
            sys.exit(f"--memory-limit: {exc}")
    if args.from_step < 1:
        sys.exit(f"--from-step must be 1 or greater (got {args.from_step}).")


def requested_runner_flags(
    args: argparse.Namespace, mask_widened: bool, retokenize: str
) -> list[str]:
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
        "--tokenizer-worker": bool(getattr(args, "tokenizer_worker", False)),
    }
    return [flag for flag in REQUIRES if requested[flag]]


def refuse_unsupported_flags(flags: list[str]) -> None:
    for flag in flags:
        gap = [f for f in CHECKS.get(flag, (flag,)) if not runner_args.script_supports(f)]
        if not gap:
            continue
        if flag in CHECKS:
            sys.exit(
                f"{flag} needs {', '.join(gap)}, which "
                f"{config.CREGIT}/run_pipeline_process.sh does not advertise.\n{REQUIRES[flag]}"
            )
        sys.exit(
            f"{flag} is not implemented by {config.CREGIT}/run_pipeline_process.sh."
            f"\n{REQUIRES[flag]}"
        )


def refuse_mask_widened_conflicts(args: argparse.Namespace) -> None:
    if args.from_step < 2:
        sys.exit(
            "--mask-widened needs --from-step 2 or greater. Step 1 deletes the project\n"
            "workdir, taking with it the blob map this flag reuses and the cregit.git\n"
            "its new_blob ids live in, so there would be nothing left to preserve."
        )
    if args.shards > 1:
        sys.exit(
            "--mask-widened cannot be combined with sharding: each shard builds a fresh\n"
            "blob map, so there is no recorded mask to widen."
        )


def refuse_retokenize_conflicts(args: argparse.Namespace, mask_widened: bool) -> None:
    if args.from_step != 2:
        sys.exit(
            f"--retokenize needs --from-step 2 exactly (got {args.from_step}).\n"
            "Step 1 deletes the blob map this flag edits. Step 3 and later skip step 2,\n"
            "so the tokens would never be remade and the rest of the pipeline would run\n"
            "over the stale ones and exit 0."
        )
    if mask_widened:
        sys.exit(
            "--mask-widened and --retokenize cannot be used in the same run. Each one\n"
            "verifies a different invariant of the blob map, and together neither check\n"
            "means anything: one reuses rows across a mask change, the other deletes rows\n"
            "a tokenizer change invalidated."
        )
    if args.shards > 1:
        sys.exit(
            "--retokenize cannot be combined with sharding: each shard builds a fresh\n"
            "blob map, so there are no cached tokenizations to invalidate."
        )


def refuse_unless_runner_accepts(flag: str, needed: tuple[str, ...], consequence: str = "") -> None:
    missing = [f for f in needed if not runner_args.script_supports(f)]
    if missing:
        sys.exit(
            f"{flag} needs {', '.join(missing)}, which "
            f"{config.CREGIT}/run_pipeline_process.sh does not accept.{consequence}"
        )


def resolve_project_meta(path: str) -> str:
    if not Path(path).exists():
        sys.exit(
            f"--project-meta {path} does not exist. "
            "The sidecar format is documented in validate_schema.py, "
            "in the comment above CREGIT_COLUMNS."
        )
    refuse_unless_runner_accepts("--project-meta", ("--project-meta", "--project-key"))
    # Absolute: the runner starts with cwd=CREGIT, not this repository.
    return str(Path(path).resolve())


def provenance_gaps(project_meta: str) -> list[tuple[str, str]]:
    """(flag, what publishing without it costs) for each missing provenance flag."""
    if project_meta:
        return []
    return [
        (
            "--project-meta",
            "29 provenance columns empty on every row: clone_url, "
            "provenance_status, source, stratum, fact, contested, "
            "label_date, owner, repo, roster_name, roster_lang, "
            "language, commits, size_class, size_kb, stars, pushed_at, "
            "license, owner_type, archived, fork, history_cluster, "
            "history_shared_with, history_relation, history_includes, "
            "history_first, history_created, manifest_category, "
            "file_mask",
        )
    ]


PROVENANCE_REFUSAL_TAIL = (
    "This does not fail anything. The file keeps all 70 columns in the "
    "right order, so validate.py passes it and no consumer can tell a "
    "blank column from provenance that is genuinely unknown, which is "
    "why a long, expensive run can finish and publish silently "
    "inconsistent with the rest of the corpus.\n"
    "Pass the flag this run is missing:\n"
    "  --project-meta <your-project-meta>.json\n"
    "If blank columns are genuinely what you want — a fixture, a "
    "one-project smoke run, a corpus whose sidecar does not exist yet — "
    "say so with --allow-empty-provenance."
)


def enforce_provenance(gaps: list[tuple[str, str]], args: argparse.Namespace) -> None:
    """Exit when a run reaching step 10 has a provenance gap, unless
    --allow-empty-provenance. Refuses rather than warns: nothing later can tell
    a blank column from provenance that is genuinely unknown."""
    allow_empty = bool(args.allow_empty_provenance)
    if allow_empty and not gaps:
        sys.exit(
            "--allow-empty-provenance has nothing to allow: --project-meta "
            "is present, so no column would be blank. Drop the flag — left "
            "in a wrapper it would silence the guard on the next run that "
            "omits --project-meta."
        )
    if not gaps or args.from_step > DATASET_STEP:
        return
    itemised = "".join(f"  {flag} absent — {cost}\n" for flag, cost in gaps)
    if not allow_empty:
        sys.exit(
            f"refusing this run: it reaches step {DATASET_STEP} and would publish a "
            f"Parquet with blank provenance.\n{itemised}{PROVENANCE_REFUSAL_TAIL}"
        )
    console.say(
        "WARNING: --allow-empty-provenance: this run will publish a Parquet "
        "with blank provenance, on purpose."
    )
    for flag, cost in gaps:
        console.say(f"         {flag} absent — {cost}")
    console.say(
        "         Do not mix these rows into the corpus: they are "
        "schema-valid and indistinguishable from rows whose provenance is "
        "genuinely unknown."
    )


def announce_run(opts: dict, jobs: int) -> None:
    """Log every choice that changes what this run publishes or how it behaves,
    so a reader of the log need not infer it from the absence of a message."""
    if opts["mask"]:
        console.say(
            f"WARNING: --mask overrides the manifest for every project in this run: {opts['mask']}"
        )
        console.say(
            "         the --project-meta sidecar records the MANIFEST's mask, so the "
            "Parquet's file_mask column will not match this run."
        )
    if opts["memory_limit"]:
        warning = memory.memory_budget_warning(opts["memory_limit"], jobs)
        if warning:
            console.say(f"WARNING: {warning}")
    if opts["from_step"] > 1:
        console.say(f"resuming at step {opts['from_step']}: the runner keeps the existing workdir")
    if opts["retokenize"]:
        console.say(
            f"--retokenize {opts['retokenize']}: step 2 will DISCARD the cached tokenizations "
            f"for these extensions and redo them."
        )
        console.say(
            "         Every other extension's cached work is kept. blobExec drops the "
            "blob_map rows and the memo entries together, and refuses the run (exit 7) "
            "if the request would have invalidated nothing — so a typo cannot pass as "
            "a successful re-run."
        )
    if opts["mask_widened"]:
        console.say(
            "--mask-widened: step 2 will REUSE each project's existing tokenizations "
            "instead of redoing them."
        )
        console.say(
            "         blobExec verifies per project, against the rows: every "
            "already-tokenized path must still be selected by the new mask, and the "
            "retained new_blob ids must exist in cregit.git. Either check failing "
            "refuses that project (rc 3) and changes nothing."
        )
        console.say(
            "         tree_map, commit_map, ref_map and blob_map's identity rows are "
            "discarded, so files the wider mask newly selects are tokenized rather "
            "than passed through as raw source."
        )
    if opts["shards"] > 1:
        console.say(
            f"sharding {opts['shards']}-way for size class"
            f"{'es' if len(opts['shard_classes']) > 1 else ''} {', '.join(opts['shard_classes'])}"
        )


def resolve_anonymizer(path: str) -> str:
    if not path:
        return ""
    if not Path(path).is_file():
        sys.exit(
            f"--anonymize {path} is not a file. It is run as "
            "`python3 SCRIPT <outdir> <dataset.parquet>`, the interface of "
            "anonymize_parquet.py."
        )
    return str(Path(path).resolve())


def refuse_cleanup_conflicts(args: argparse.Namespace) -> None:
    if getattr(args, "drop_html", False) and not getattr(args, "cleanup", False):
        sys.exit(
            "--drop-html deletes html/ in the cleanup step. Add --cleanup, or use "
            "--skip-html to not write the HTML at all."
        )


def run_options(args: argparse.Namespace) -> dict:
    """The checked options run_project reads through _OPTS. Exits on a refusal."""
    refuse_cleanup_conflicts(args)
    anonymize = resolve_anonymizer(getattr(args, "anonymize", "") or "")
    mask_widened, retokenize = preflight_runner_flags(args)
    project_meta = resolve_project_meta(args.project_meta) if args.project_meta else ""
    enforce_provenance(provenance_gaps(project_meta), args)
    return dict(
        skip_html=args.skip_html,
        drop_memo=args.drop_memo,
        reblame=args.reblame,
        memo_dir=args.memo_dir,
        shards=args.shards,
        shard_classes=tuple(c.strip() for c in args.shard_classes.split(",") if c.strip()),
        from_step=args.from_step,
        gc=args.gc,
        mask_widened=mask_widened,
        retokenize=retokenize,
        tokenizer_worker=bool(getattr(args, "tokenizer_worker", False)),
        blame_jobs=args.blame_jobs,
        memory_limit=args.memory_limit,
        duckdb_threads=args.duckdb_threads,
        mask=args.mask,
        project_meta=project_meta,
        join=bool(getattr(args, "join", False)),
        manifest=str((config.CORPUS / args.manifest).resolve()),
        cleanup=bool(getattr(args, "cleanup", False)),
        drop_html=bool(getattr(args, "drop_html", False)),
        anonymize=anonymize,
    )
