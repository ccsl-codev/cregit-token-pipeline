# cregit-token-pipeline

Runs the [cregit](https://github.com/cregit/cregit) per-project pipeline over a
manifest of FLOSS repositories that you supply, then validates, indexes and
prunes the result. It does not select repositories or draw a sample — you
write the manifest; this tool runs cregit over it. It does not anonymize the
output.

Each project yields one Parquet file with the same 70 columns and **one row per
token occurrence per file**, carrying the commit and the person that last touched
that token. cregit writes the first 67; ctp then adds the author's firm in 3 more
(see [Firm attribution](#firm-attribution)). 29 of the 70 columns carry per-project provenance — how you found
the project, how you labelled it, which mask tokenized it — filled from an
optional JSON sidecar you supply. An absent sidecar leaves its columns empty,
not missing. `validate_schema.py` is the
authority on the full column list; read the comment above `CREGIT_COLUMNS`
there for what each column means.

Stdlib Python only; DuckDB, srcML, ctags, Java and Perl come from the cregit
checkout's `devenv shell`.

## Start here

Read [`docs/DESIGN.md`](docs/DESIGN.md) to see why the pipeline is built this
way. The authority on the schema is `validate_schema.py`, not any document.

## Quickstart

```sh
# 1. point pipeline.cfg at a cregit master checkout (bb6f985 or later), and an output dir
# 2. write a manifest: a TSV file, one project per line, five fields —
#    name  url  category  file_filter  size_class
#    and an optional sixth, commit: a 40-hex SHA to pin. A pinned project is
#    cloned at exactly that commit; a five-field row runs the remote HEAD.
#    manifest.tsv in this repository is the worked example: 4 small public
#    pilot projects (jq, zstd, libuv, tmux). Write your own for a real run.
M=manifest.tsv
./ctp.py run --manifest $M --jobs 3 --skip-html --drop-memo \
    --allow-empty-provenance
./ctp.py status --manifest $M                    # one-screen progress view
./validate_schema.py <out>/*/*-dataset.parquet   # re-check the schema over the corpus
./ctp.py db                                      # rebuild ctp.duckdb + tokens view
```

That command runs on a fresh clone. The provenance sidecar does not ship with
this repository, so the 29 provenance columns come out blank, and
`--allow-empty-provenance` is how you say that is what you meant. `ctp.py run` refuses to write a Parquet with blank
provenance unless you say so, because nothing downstream can tell a blank
column from provenance that is genuinely unknown.

### Provenance

`--project-meta <path>` is optional and does not ship with this repository. It
is a JSON sidecar, keyed by project name, that fills the 29 provenance columns
— how you found the project, how you labelled it, which mask tokenized it. ctp
forwards the path unchanged to cregit. The format is documented in
`validate_schema.py`, in the comment above `CREGIT_COLUMNS`.

Pass it instead of `--allow-empty-provenance`. The two flags are mutually
exclusive: `--allow-empty-provenance` is refused when nothing would be blank.

### Firm attribution

After cregit writes a project's 67 columns, `firm_attribution.py` adds three:

| Column | Value |
| --- | --- |
| `firm_raw` | the `company` of `person_domain` in `data/affiliation.merged.csv` |
| `firm` | the reviewed name from `data/firm_canonical.csv`, else `firm_raw` |
| `firm_source` | the `source` of the map row (`gitdm`, `patch`, `rich`, `cncf-gitdm`, ...) |

All three are `''` when the domain is not in the map. The domain match ignores
case. The map holds the 895 rows of the VEM 2026 Corporate Truck Factor map
unchanged (sources `gitdm`, `patch`, `rich`), widened to 4,041 domains. The
logic is the firm join that cregit's `feat/firm-attribution` branch used.
The step accepts its own output, so a re-run applies a changed map.

Each project runs cregit's `run_pipeline_process.sh` (clone → tokenize via the
incremental blob-map engine → blame → per-project Parquet), then a validation
gate, then a `.validated` stamp. Runs are idempotent: validated projects are
skipped, interrupted ones resume from the blob map, failed ones are retried on the
next pass.

## Architecture

```
manifest.*.tsv (you write it: name, url, category, file_filter, size_class)
                                                   │
project_meta.json (optional sidecar) ──────────────┤
                                                   ▼
                                              ctp.py run
                     (ThreadPool, per-project flock, disk floor, retry passes)
                                                   │
                        <out>/<name>/<name>-dataset.parquet   (67 columns)
                                                   │
                     firm_attribution.py adds firm_raw, firm, firm_source  (70 columns)
                                                   │
                     ├──► validate.py        rows/size/70-column gate → .validated stamp
                     ├──► validate_schema.py the same column gate over many files
                     ├──► retain.py          delete memo/ and html/
                     └──► metrics.tsv        append-only, one row per phase attempt

ctp.py db ──► ctp.duckdb ├─ projects       DONE/RUNNING/FAILED/QUEUED per project
                         ├─ phase_metrics  the timing ledger, SQL-queryable
                         └─ tokens (view)  every schema-conforming parquet, unified
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

Create the environment once with
`python3 -m venv .venv && .venv/bin/pip install -e '.[dev]'`.

`./run_tests.sh` works on a clean clone: no file needs a manual edit first, and
CI deselects nothing. Tests that assert facts about the cregit checkout named in
`pipeline.cfg` self-skip when that checkout is absent, which it is on any runner.
Tests that read a real Parquet need DuckDB and self-skip without it, so install
the `dev` extra to run the whole suite.
