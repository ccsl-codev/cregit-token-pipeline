# Re-encoding the Parquet files

`reencode_parquet.py` writes each project Parquet file again with other writer
settings and proves that the content did not change. It is a dataset-level step:
it runs after the per-project files are final (after `anonymize_parquet.py`, for
example), and it changes only the bytes of the files, never a value, a row or the
row order.

The default settings are the settings that wrote the published files of the
dataset (release 1.0.0): smaller files than the DuckDB writer gives, and Bloom
filters on the columns that a reader filters on.

## Install

```sh
pip install -e '.[reencode]'   # pyarrow 26.0.1; duckdb comes with the package
```

The tool checks the pyarrow and duckdb versions at the start against
`--pyarrow-version` and `--duckdb-version` (defaults 26.0.1 and 1.5.5). Another
version gives other bytes, so the check stops the run. An empty value
(`--pyarrow-version ''`) accepts any version; each log line records the versions
that ran. Run it with `python -I -B`.

## Commands

```sh
# one file: OUT appears only when all checks pass
python -I -B reencode_parquet.py one IN.parquet OUT.parquet --log reencode.jsonl

# a folder, in parallel, largest file first; a second run resumes
python -I -B reencode_parquet.py run --src IN_DIR --dst OUT_DIR --log reencode.jsonl \
    --sums SHA256SUMS --summary summary.json --progress progress.json

# counts, bytes, failures and versions of a log
python -I -B reencode_parquet.py summary --log reencode.jsonl --src IN_DIR

# the SHA256SUMS of a root folder after the run (ROOT/parquet from the log, other files from their bytes)
python -I -B reencode_parquet.py make-sums --root ROOT --log reencode.jsonl --out ROOT/SHA256SUMS

# the writer settings and their sha256
python -I -B reencode_parquet.py settings
```

`one` exits 0 when the file passes and 1 otherwise. `run` exits 0 when every file
of `--src` has an OK line, 1 when some file failed, 2 on a bad input (a
`--sums` list that does not match `--src`, a bad `--read-batch-for`) and 3 when
`--stop-file` stopped it. `summary` exits 0 when the log is complete.

`--sums` pins the sha256 of each input: the rows of the list under
`--sums-prefix` (default `parquet/`) must name exactly the files of `--src`, and
a file whose sha256 differs fails.

### Memory

Each file runs in its own process. `run` reserves `--mem-small` GB for a file
(`--mem-big` GB for a file of `--big-bytes` or more) and starts no new process
while the reserved sum would pass `--mem-total` (default 85% of the available
memory). A watchdog kills a process over `--mem-kill` GB; the file gets an ERROR
line and a second run does it again. `--threads-small` and `--threads-big` set the
DuckDB and pyarrow threads of each kind of file.

`--read-batch N` sets the rows of one Arrow batch that DuckDB gives the writer
(default 262,144). The Arrow appender of DuckDB stops at 2 GiB in one string
buffer, so a file with very long values in one column (long commit messages, for
example) needs a smaller batch: `--read-batch-for FILE=N` sets it for one input
file. The read batch is not a writer setting: the output bytes and the settings
sha256 are the same.

## Settings

Each setting has a key (for `--config FILE`, a JSON object) and an option. The
options change the config file, which changes the defaults. `run` passes the
settings to each file process.

| Key | Option | Default | Meaning |
| --- | --- | --- | --- |
| `pyarrow_version` | `--pyarrow-version` | `26.0.1` | the pyarrow version that must run (empty: any) |
| `duckdb_version` | `--duckdb-version` | `1.5.5` | the duckdb version that must run (empty: any) |
| `format_version` | `--format-version` | `2.6` | the Parquet format version |
| `data_page_version` | `--data-page-version` | `1.0` | data page V1 (`1.0`) or V2 (`2.0`) |
| `compression` | `--compression` | `zstd` | `zstd`, `gzip`, `brotli`, `snappy`, `lz4` or `none` |
| `compression_level` | `--compression-level` | `19` | the level (null in the config for a codec without levels) |
| `row_group_rows` | `--row-group-rows` | `1000000` | the rows of each row group (the last one smaller) |
| `data_page_size` | `--data-page-size` | `8388608` | the largest data page, in bytes (8 MB) |
| `max_rows_per_page` | `--max-rows-per-page` | `1000000` | the most rows in one data page |
| `dictionary_pagesize_limit` | `--dictionary-pagesize-limit` | `67108864` | the largest dictionary page (64 MB); past it the writer falls back to PLAIN |
| `no_dictionary` | `--no-dictionary COLUMN` | `token_value`, `source_text`, `source_col` | PLAIN with no dictionary |
| `column_encoding` | `--column-encoding COLUMN=ENCODING` | `token_index`, `source_line`: `DELTA_BINARY_PACKED` | a column encoding (no dictionary) |
| `bloom_columns` | `--bloom-column COLUMN`, `--no-bloom` | `file_path`, `token_value`, `source_text`, `cregit_commit_sha`, `personid` | the columns with a Bloom filter |
| `bloom_fpp` | `--bloom-fpp` | `0.01` | the false positive rate of each Bloom filter |
| `expect_kv_keys` | `--expect-kv-key KEY` | none | the footer keys that each input must have (none: no key check) |

The list options repeat and replace the default list. Every column not in
`no_dictionary` or `column_encoding` gets a dictionary (a list column by its leaf
`<name>.list.element`). Fixed, and part of the settings sha256: statistics on, no
page index, no `ARROW:schema` footer entry, compliant nested types, the footer
key-value entries of the input copied in their order, and the ndv of each Bloom
filter set to the largest count of distinct non-null values of the column in one
row group (at least 1).

### The settings of the published files

The published files were written with the defaults and the footer key check of
their five entries:

```json
{"expect_kv_keys": ["ctp_schema_version", "organizations_version", "organizations_mapping_sha256",
                    "pseudonymization_version", "pseudonymization_registry_sha256"]}
```

`settings` prints the sha256 `e98a53aed2038837fae2cf20d9de407729517f9290faaa44ab33c3762c9fcfbb`
for these settings, the value in each log line of that run.
`tests/test_reencode_parquet.py` checks it.

## The content check

Each output is written as `OUT.tmp`, checked against its input, and renamed to
`OUT` only when every check passes. Otherwise the log line has `FAIL` (a check
failed) or `ERROR` (an exception), and the tool removes `OUT.tmp`.

| Check | What it compares |
| --- | --- |
| `rows`, `duckdb_read` | the DuckDB row count |
| `rowhash` | the sum of the 64-bit row hashes `hash(c1, ..., cN)` over all rows |
| `colhash` | one sum of the value hashes for each column |
| `orderhash` | the sum of `hash(file_row_number, row hash)`: the row order |
| `describe` | DuckDB `DESCRIBE`: the names, types and nullability |
| `parquet_schema`, `arrow_schema` | the Parquet schema text and the Arrow schema derived from it |
| `kv`, `kv_input_keys` | the footer key-value list (in order, without `ARROW:schema`); the expected keys |
| `pyarrow_decode` | pyarrow decodes every page of every column; the row count |
| `encodings` | only PLAIN, PLAIN_DICTIONARY, RLE_DICTIONARY, RLE and the configured column encodings, in the column chunk lists and in the header of every page |
| `page_version` | only data pages of the configured version and dictionary pages |
| `compression` | the configured codec on every column chunk |
| `row_groups` | full row groups, the last one smaller |
| `bloom` | a Bloom filter on the configured columns only |
| `format_version` | the configured Parquet format version |
| `in_sha256_pinned` | the sha256 of the input is the pinned one (`--want-sha`, `--sums`) |

## The log

`--log` is a JSON Lines file with one line for each attempt; the last line of a
file wins. A line holds the file name, the status, the sizes and sha256 values
of the input and output, the rows, the checks, the content fingerprint, the
footer entries, the Bloom filter ndv values, the read batch, the settings sha256,
the versions (python, pyarrow, duckdb and the sha256 of the tool) and the times
and peak memory. An OK line holds no path. The tool refuses to write a line with an at
sign, so no e-mail address can enter the log.

Log lines of version 1 name the `compression` check `zstd` and the
`page_version` check `pages_v1`.
