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
ctags, Java and Perl come from the cregit checkout's `devenv shell`. The
optional language filter also needs git and tokei 14; `nix develop` in this
repository puts that tokei on PATH.

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

## Language filter (optional)

`language_filter.py` drops the manifest rows that are not mostly written in the
languages the tokenizer handles, before the run spends hours on them. Its input
is a manifest with a sixth field, the pinned commit (40 lowercase hex digits).

```sh
nix develop                                  # tokei 14 on PATH
./language_filter.py --manifest pinned.tsv --out-dir lang/                 # largest mode
./language_filter.py --manifest pinned.tsv --out-dir lang/ --mode share --threshold 0.30
```

For each row it makes an empty repository in a temporary folder, fetches that one
commit with depth 1 (never the history), checks it out and deletes `.git`. git
runs without the machine's git configuration. The step then removes the vendored
directories and the Qt translation files, runs `tokei --hidden --no-ignore
--output json` on the folder, and deletes the folder.

The count of a language is tokei's `code` lines plus the code embedded in its
files (tokei's `children`), as in tokei's own total. `C Header` is merged into C
and `C++ Header` into C++. Only programming languages are counted: the table
[`data/tokei14_programming.tsv`](data/tokei14_programming.tsv) says, for each
tokei 14 language, whether it is one. A language is programming when Linguist
gives its name `type: programming`, or when a hand decision for a tokei name that
Linguist does not have says so. So Shell, Makefile, CMake and Autoconf count, and
JSON, YAML, TOML, XML, Markdown, Plain Text, HTML, CSS, SVG, reStructuredText,
AsciiDoc and INI do not. A language that the table does not list is not counted
and is named in `unknown_languages`. `build_programming_table.py` rebuilds the
table from Linguist's `languages.yml` and `tokei --languages`; its docstring
names the Linguist commit of the shipped table.

A vendored directory is one whose name is in `--vendored-dirs`, at any depth,
compared without case; a symbolic link with such a name is kept. A `.ts` file
that starts with `<?xml`, `<!DOCTYPE TS` or `<TS`, after an optional UTF-8 byte
order mark and white space, is a Qt translation file, and tokei 14 would count it
as TypeScript, so it is removed.

| Option | Default | Meaning |
| --- | --- | --- |
| `--targets` | `C,C++,Java,Rust` (`file_mask.TOKENIZABLE_LANGUAGES`) | the target languages, as tokei names them. Each must be programming in the table |
| `--mode` | `largest` | `largest` keeps a row whose largest language is a target. `share` keeps a row whose target share is at least `--threshold` |
| `--threshold` | none | the share mode's threshold in (0, 1], for example 0.30 or 0.50 |
| `--programming-table` | `data/tokei14_programming.tsv` | which tokei languages are counted |
| `--vendored-dirs` | `3rdparty,deps,external,third-party,third_party,thirdparty,vendor,vendored` | directory names removed. Add `node_modules` if you want it. Empty removes none |
| `--fetch-cmd` | `git fetch --quiet --depth 1 --no-tags {url} {commit}` | the fetch, run in the empty repository with no shell |
| `--work-dir` | the system temporary folder | where the temporary checkouts go |
| `--tokei`, `--timeout`, `--jobs` | `tokei`, 3600 s, 1 | the executable, the limit per git or tokei command, rows measured at once |

It writes three files to `--out-dir`:

- `kept.tsv`: the kept manifest rows, copied verbatim, after one comment line
  that names the rule and the tokei version.
- `dropped.tsv`: `name`, `url`, `commit`, `reason`.
- `languages.tsv`: one row per manifest row, with these columns.

| Column | Meaning |
| --- | --- |
| `name`, `url`, `commit` | from the manifest |
| `status` | `measured`, or `not measured: <why>`: `no pinned commit`, `fetch by commit refused`, `fetch failed`, `checkout failed`, `timed out`, `tokei failed` |
| `kept` | `yes` or `no`. A row that was not measured is dropped |
| `largest_language` | the programming language with the most code lines; a tie goes to the name that sorts first (code point order) |
| `target_share` | target-language code lines / all programming-language code lines, 6 decimals |
| `programming_code`, `nonprogramming_code` | the code lines of the counted languages, and of the others |
| `unknown_languages` | tokei languages with code that the table does not list |
| `qt_ts_removed`, `vendored_dirs_removed`, `vendored_files_removed` | what the two removals took out |
| `tokei_version` | from `tokei --version`. Another major version than 14 prints a warning |
| `detail` | the first line of the git or tokei error, for a row that was not measured |
| `code:<language>` | code lines per programming language, one column per language seen in the run |

Exit status: 0 every row was measured, 1 at least one was not (the files are
still written), 2 wrong invocation or no usable tokei. `ctp.py run` reads five
fields, so cut the sixth before a run (`cut -f1-5`); the run then clones HEAD,
not the measured commit. The limits are in
[`docs/LIMITATIONS.md` §8](docs/LIMITATIONS.md#8-language-filter).

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
