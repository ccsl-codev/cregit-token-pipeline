# cregit-token-pipeline

Batch orchestrator that runs the [cregit](https://github.com/cregit/cregit)
token-level analysis pipeline over a corpus of git repositories and
consolidates the per-project outputs into one queryable dataset.

Built for a study of corporate knowledge concentration in FLOSS codebases
(~101 projects: enterprise-backed, community-backed, and the Linux kernel).
Stdlib Python only; DuckDB is required inside the cregit environment for
validation and consolidation.

## Background

This pipeline generalizes the single-repository setup of the earlier
[cbsoft-vem2026-corporate-truck-factor](https://github.com/EllianCarlos/cbsoft-vem2026-corporate-truck-factor)
work, which ran cregit over one repository (the Linux kernel) with hand-rolled
scripts. This project turns the same token-level method into a repeatable
multi-project pipeline.

## Quickstart

```
# 1. point pipeline.cfg at your cregit checkout and an output directory
# 2. list projects in manifest.tsv (name, url, category, file_filter, size_class)
./ctp.py run --jobs 3        # run everything not yet validated
./ctp.py status              # one-screen progress view
./ctp.py db                  # rebuild ctp.duckdb (tracking + tokens view)
```

Each project runs cregit's `run_pipeline_process.sh` (clone → tokenize via
incremental blobExec → blame → per-project parquet), then a validation gate,
then writes a `.validated` stamp. Runs are idempotent: validated projects are
skipped, interrupted ones resume via blobExec's incremental engine, and failed
ones are retried on the next pass.

## Architecture

```
manifest.tsv ──► ctp.py run ──► <out>/<name>/<name>-dataset.parquet
                   │  (ThreadPool, per-project flock,        + logs/ per attempt
                   │   disk floor, retry passes)
                   ├──► validate.py  (row count / schema gate → .validated stamp)
                   └──► metrics.tsv  (append-only ledger: one row per phase attempt)

ctp.py db ──► ctp.duckdb
                          ├─ projects       (state per project: DONE/RUNNING/FAILED/QUEUED)
                          ├─ phase_metrics  (the timing ledger, SQL-queryable)
                          └─ tokens (view)  (all validated parquets unified,
                                             + category/size_class columns)
```

Ground truth is always the files (stamps, manifest, ledger); `ctp.duckdb`
is a derived index, rebuilt on demand — the runner never dual-writes state.

## Observability

- event lines on every phase start/finish, heartbeat every 30 s
  (running projects with elapsed time, done/failed counts, disk free)
- per-attempt logs under `<out>/<name>/logs/<phase>-<timestamp>.log`,
  never overwritten; `<phase>-latest.log` symlink tracks the live attempt
- `metrics.tsv`: `iso_start  project  class  phase  duration_s  rc  log`

## Operational notes

- Paths are resolved to canonical form before invoking cregit: blobExec
  stores its invocation in a meta table and refuses incremental resume if
  the path string changes (e.g. via a symlinked home directory).
- The cregit environment (srcml, ctags, java, duckdb) is resolved ONCE per
  run via `devenv shell` and passed to all job subprocesses — concurrent
  `devenv shell` invocations race on a shared GC root and fail.
- Stop a run by killing the runner PID (not `pkill -f`, which can self-match
  and orphan pipeline children).
