# cregit-token-pipeline

Builds a **token-level code authorship dataset** from a manifest of FLOSS
repositories, by orchestrating the
[cregit](https://github.com/cregit/cregit) per-project pipeline and adding
provenance, validation and anonymization around it. A manifest of projects goes
in; one validated Parquet per project comes out.

Each project yields one Parquet file with the same 70 columns and **one row per
token occurrence per file**, carrying the commit and the person that last touched
that token, and the person's employer where it can be resolved. A sidecar
supplies the 29 per-project columns.

Stdlib Python only, except pyarrow for the footer rename tool; DuckDB, srcML,
ctags, Java and Perl come from the cregit checkout's `devenv shell`.

## Start here

| If you want to | Read |
| --- | --- |
| understand the dataset, its grain and all 70 columns | **[`docs/DATASET-SCHEMA.md`](docs/DATASET-SCHEMA.md)** |
| know the method limits before you analyze the data | **[`docs/LIMITATIONS.md`](docs/LIMITATIONS.md)** |
| know why the pipeline is built this way | [`docs/DESIGN.md`](docs/DESIGN.md) |

The authority on the schema is `validate_schema.py`, not any document.

## Quickstart

```sh
# 1. point pipeline.cfg at a cregit checkout with rustTokenizer, and an output dir
# 2. write a manifest (TSV: name, url, category, file_mask, size_class).
#    --manifest defaults to manifest.tsv, a 4-project example.
# 3. write the sidecar: a JSON object keyed by manifest name, with the 29
#    fields of validate_schema.META_FIELDS for each project.
M=my-run.tsv
./ctp.py run --manifest $M --jobs 3 --skip-html --drop-memo \
    --project-meta sidecar.json \
    --firm-map data/affiliation.merged.csv \
    --firm-canonical data/firm_canonical.csv
./ctp.py status --manifest $M                    # one-screen progress view
./validate_schema.py <out>/*/*-dataset.parquet   # schema gate over the dataset
./consolidate.py --manifest $M   # in devenv shell: rebuild ctp.duckdb + tokens view
```

The step table in
[`docs/DATASET-SCHEMA.md` §2](docs/DATASET-SCHEMA.md#2-regenerating-it) lists
every step from a manifest to an anonymized release.

Each project runs cregit's `run_pipeline_process.sh` (clone → tokenize via the
incremental blob-map engine → blame → per-project Parquet), then a validation
gate, then a `.validated` stamp. Runs are idempotent: validated projects are
skipped, interrupted ones resume from the blob map, failed ones are retried on the
next pass.

## Architecture

```
manifest.*.tsv (one row per project) ──────────────┐
                                                   │
sidecar JSON (29 provenance fields) ───────────────┤
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

## Renaming and dropping columns without re-encoding the data

`parquet_rename.py` renames top-level columns of a Parquet file by writing a new
footer only. Bytes `[0, footer_start)` (the magic, column chunks, pages,
dictionaries, Bloom filters and page indexes) are copied unchanged, so every
offset in the footer stays valid. The new footer differs only in the renamed
`SchemaElement` names, the first element of each `path_in_schema`, and the
field names in the `ARROW:schema` key-value. With no mapping the output is
byte-identical to the input.

```sh
./parquet_rename.py in.parquet out.parquet --rename old_name=new_name
./parquet_rename.py in.parquet out.parquet --map mapping.json      # {"old": "new"}
./parquet_rename.py in.parquet tail.bin --rename a=b --tail-only   # new footer tail only
./parquet_rename_check.py --rename old_name=new_name in.parquet out.parquet
./parquet_rename_check.py --map mapping.json --pairs pairs.tsv     # OLD<TAB>NEW lines
```

The copy uses a reflink when the file system can share extents, then
`copy_file_range`, then plain reads and writes (`--copy read-write` forces the
last). `--tail-only` writes only the new footer, its length and `PAR1`; the
renamed file is `in.parquet[0:prefix_len]` followed by that tail, which lets a
caller build the output without copying the data (for example a multipart upload
that copies the prefix server-side).

The rename refuses an encrypted file, a name that is not a top-level column, a
rename to the same or an empty name, a mapping that makes duplicate names, a
footer or `ARROW:schema` that does not re-encode to the same bytes, and an old
name that also appears in another key-value or in Arrow metadata (pass
`--allow-mentions` to keep those unchanged).

`--drop NAME` (repeatable, with or without `--rename` in the same pass) removes
top-level columns. The output is `PAR1`, then the kept column chunks, Bloom
filters, column indexes and offset indexes in their original order, then a new
footer. Chunks, Bloom filters and column indexes are copied byte for byte; only
the offset indexes are re-encoded, because they hold absolute page offsets. The
footer loses the dropped columns' schema elements, column chunks and column
orders, and every offset, row group size and sort column index is moved to
match. A drop refuses every column, a dropped sort column, a column both
renamed and dropped, and a file whose data part the footer's regions do not
tile exactly.

```sh
./parquet_rename.py in.parquet out.parquet --drop a --drop b --rename c=d
./parquet_rename_check.py --drop a --drop b --rename c=d in.parquet out.parquet
```

For a drop the check proves that every kept column chunk, Bloom filter and
column index has the same bytes at its new offset, that each offset index is
the old one moved, that the page index points at real data pages, and that the
footer equals the old one with the columns removed and the offsets blanked. It
then runs the pyarrow, DuckDB and Bloom checks below on the kept columns.

`--set NAME=VALUE` gives a top-level string column one value in every row.
`--set-null NAME` makes the column null. Use them alone or with `--drop`,
`--rename` and `--set-kv KEY=VALUE` in the same pass. `--set-kv` changes the
value of a file key-value that the footer already has (and the same key in the
`ARROW:schema` metadata); the check takes the same `--set-kv`. NAME is the name
in the input file. The tool
writes a new chunk for that column in each row group, with pyarrow. All other
regions are copied byte for byte, as for a drop. The new chunk keeps the
physical type, the logical type, the codec, the encoding family (dictionary,
PLAIN or DELTA_*), the data page version, the page checksums and the rows per
page. The column gets new statistics with the same fields as before. It gets
a new column index, offset index and Bloom filter only if it had one before.
The schema and `ARROW:schema` do not change, because the type does not
change.

```sh
./parquet_rename.py in.parquet out.parquet --set language=Go --set-null license
./parquet_rename.py --values values.tsv --out-dir out/ --drop a --rename b=c
./parquet_rename_check.py --set language=Go --set-null license in.parquet out.parquet
./parquet_rename_check.py --values values.tsv --out-dir out/ --drop a --rename b=c
```

`values.tsv` has `FILE<TAB>COLUMN<TAB>OLD<TAB>NEW` lines. `\N` is a null. The
output of FILE is `out/` with the base name of FILE. The tool refuses a file
when its column does not hold OLD, and continues with the next file. It exits
2 when it refuses a file.

A replacement refuses a nested or repeated column, a column that is not a
string, a null for a required column, a column that is also dropped, an
encrypted file and a file with no rows. It also refuses a column with more
than one distinct value. Give `--force-multi` to replace all of them; then OLD
must be one of the values.

For a replacement the check runs the drop checks on every other column. For
the replaced column it proves that each new chunk keeps the old form (type,
codec, path, value count, encoding family, page version, checksums, the
fields that are present) and that its statistics give the new value. It also
proves that the row group sizes are the sums of the new chunk sizes, that
pyarrow and DuckDB read the new value in every row, that OLD held the
expected old value, and that a Bloom probe finds the new value and no old
value.

`parquet_rename_check.py` proves each pair: the data prefix hashes are equal; the
footers are equal field by field except the renamed names; pyarrow reads the new
file with the mapped schema, the same row groups and rows, and the same SHA-256
per column; DuckDB gives the same `count(*)`, and `count()` and `sum(hash())` of
each renamed column; and a value from each Bloom-filtered column is found by
`parquet_bloom_probe` in both files. It prints one JSON line per pair and exits
1 on any difference.

## Tests

```sh
./run_tests.sh              # whole suite, then the coverage report (80% floor)
./run_tests.sh -k domain    # one subset
```

Tests needing network or an authenticated `gh` carry the `network` marker and are
deselected, so a result does not depend on a rate-limit window. Create the
environment once with `python3 -m venv .venv && .venv/bin/pip install pytest coverage`.
