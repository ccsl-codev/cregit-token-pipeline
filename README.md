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

Stdlib Python only; DuckDB, srcML, ctags, Java and Perl come from the cregit
checkout's `devenv shell`. The optional language filter also needs git and
tokei 14; `nix develop` in this repository puts that tokei on PATH.

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

## Tests

```sh
./run_tests.sh              # whole suite, then the coverage report (80% floor)
./run_tests.sh -k domain    # one subset
```

Tests needing network or an authenticated `gh` carry the `network` marker and are
deselected, so a result does not depend on a rate-limit window. Create the
environment once with `python3 -m venv .venv && .venv/bin/pip install pytest coverage`.
