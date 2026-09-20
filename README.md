# cregit-token-pipeline

Builds a **token-level code authorship dataset** over a stratified corpus of
FLOSS repositories, by orchestrating the
[cregit](https://github.com/cregit/cregit) per-project pipeline and adding
corpus-level selection, provenance, validation and anonymization around it.

Each project yields one Parquet file with the same 70 columns and **one row per
token occurrence per file**, carrying the commit and the person that last touched
that token, the person's employer where it can be resolved, and 29 columns
recording how the project entered the corpus and how it was labelled.

Stdlib Python only; DuckDB, srcML, ctags, Java and Perl come from the cregit
checkout's `devenv shell`.

## Start here

| If you want to | Read |
| --- | --- |
| understand the dataset, its grain and all 70 columns | **[`docs/DATASET-SCHEMA.md`](docs/DATASET-SCHEMA.md)** |
| know what is wrong with the data before you analyse it | **[`docs/LIMITATIONS.md`](docs/LIMITATIONS.md)** |
| know how a project got its `stratum`, or label one yourself | [`docs/CODEBOOK.md`](docs/CODEBOOK.md) |
| know why the pipeline is built this way | [`docs/DESIGN.md`](docs/DESIGN.md) |
| see the sampling frame, exclusions and the per-cell draw | [`docs/CORPUS-REVIEW.md`](docs/CORPUS-REVIEW.md), [`docs/CORPUS-SAMPLE.md`](docs/CORPUS-SAMPLE.md) |
| see the strata checked against an independent published labelling | [`docs/SPINELLIS-VALIDATION.md`](docs/SPINELLIS-VALIDATION.md) |

The three corpus reports are **generated** — by `select_corpus.py review`,
`select_corpus.py sample` and `validate_spinellis.py`. Do not edit them by hand.

The authority on the schema is `validate_schema.py`, not any document.

## Quickstart

```sh
# 1. point pipeline.cfg at a cregit checkout with rustTokenizer, and an output dir
# 2. pick a manifest (TSV: name, url, category, file_mask, size_class).
#    --manifest defaults to manifest.tsv, which is only 4 legacy pilots.
M=manifest.phase1-sm.tsv
./ctp.py run --manifest $M --jobs 3 --skip-html --drop-memo
./ctp.py status --manifest $M                    # one-screen progress view
./validate_schema.py <out>/*/*-dataset.parquet   # schema gate over the corpus
./ctp.py db                                      # rebuild ctp.duckdb + tokens view
```

Full regeneration, from rosters to an anonymized release, is the step table in
[`docs/DATASET-SCHEMA.md` §2](docs/DATASET-SCHEMA.md#2-regenerating-it).

Each project runs cregit's `run_pipeline_process.sh` (clone → tokenize via the
incremental blob-map engine → blame → per-project Parquet), then a validation
gate, then a `.validated` stamp. Runs are idempotent: validated projects are
skipped, interrupted ones resume from the blob map, failed ones are retried on the
next pass.

## Architecture

```
candidates.csv ──► select_corpus.py sample ──► manifest.*.tsv
                                                   │
project_meta.json (29 provenance fields) ──────────┤
data/affiliation.merged.csv (domain → firm) ───────┤
                                                   ▼
                                              ctp.py run
                     (ThreadPool, per-project flock, disk floor, retry passes)
                                                   │
                        <out>/<name>/<name>-dataset.parquet   (70 columns)
                                                   │
                     ├──► validate.py        rows/size gate → .validated stamp
                     ├──► validate_schema.py column/type/order gate
                     ├──► retain.py          delete memo/ and html/
                     └──► metrics.tsv        append-only, one row per phase attempt

ctp.py db ──► ctp.duckdb ├─ projects       DONE/RUNNING/FAILED/QUEUED per project
                         ├─ phase_metrics  the timing ledger, SQL-queryable
                         └─ tokens (view)  every schema-conforming parquet, unified

anonymize_parquet.py ──► pseudonymized parquets ──► verify_anon.py
```

Ground truth is always the files — stamps, manifests, `metrics.tsv`.
`ctp.duckdb` is a derived index, rebuilt on demand; the runner never dual-writes
state.

## Observability

- Event lines on every phase start and finish; a heartbeat every 30 s listing
  running projects with elapsed time, done/failed counts and disk free.
- Per-attempt logs under `state/<project>/logs/<phase>-<timestamp>.log`, never
  overwritten, with a `<phase>-latest.log` symlink on the live attempt. They sit
  beside `metrics.tsv` rather than in the project work directory, because a
  from-scratch run deletes that directory.
- `metrics.tsv`: `iso_start  project  class  phase  duration_s  rc  log`.

## Operational notes

- Paths are resolved to canonical form before cregit is invoked. The tokenizer's
  blob map stores its invocation string in a meta table and refuses incremental
  resume if that string changes — including via a symlinked home directory.
- The cregit environment is resolved **once** per run and passed to every job
  subprocess; concurrent `devenv shell` invocations race on a shared GC root and
  fail.
- A mask change also defeats resume. Pass `--mask-widened --from-step 2` to widen
  a mask without rebuilding from scratch.
- Stop a run by killing the runner PID. Not `pkill -f`, which can self-match and
  orphan pipeline children.

## Tests

```sh
./run_tests.sh              # whole suite, then the coverage report (80% floor)
./run_tests.sh -k domain    # one subset
```

Tests needing network or an authenticated `gh` carry the `network` marker and are
deselected, so a result does not depend on a rate-limit window. Create the
environment once with `python3 -m venv .venv && .venv/bin/pip install pytest coverage`.
