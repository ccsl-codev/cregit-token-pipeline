#!/usr/bin/env python3
"""Re-encode Parquet files with pyarrow, with no data change, and prove that the content is the same.

One input file gives one output file. DuckDB reads the rows in the file order (so the row order does not change)
and pyarrow writes them with the writer settings. The default settings are the settings that wrote the published
files of the dataset (release 1.0.0): pyarrow 26.0.1, Parquet format 2.6 with data page V1, ZSTD level 19, row
groups of 1,000,000 rows (the last one smaller), data pages of at most 8 MB and 1,000,000 rows, dictionary pages of
at most 64 MB, token_value, source_text and source_col PLAIN with no dictionary, token_index and source_line
DELTA_BINARY_PACKED, a dictionary on every other column, statistics on, no page index, no ARROW:schema entry, and
Bloom filters (fpp 0.01) on file_path, token_value, source_text, cregit_commit_sha and personid. The ndv of each
Bloom filter is the largest count of distinct non-null values of the column in one row group. The Arrow batches of
DuckDB go to the writer as the chunks of one table of one row group (no copy into one chunk, which doubles the
peak memory). The footer key-value entries of the input are copied in their order (without ARROW:schema).
`--config FILE` (a JSON object) and the options of each sub-command change the settings; `settings` prints them
and their sha256, which each log line records.

The content check of each output against its input (all must pass, else the output is removed and the exit code
is not 0):
  - DuckDB: the row count, the sum of the 64-bit row hashes hash(c1, ..., cN) over all rows, one sum of the value
    hashes for each column, and an order-dependent sum of hash(file_row_number, row hash);
  - DuckDB DESCRIBE (name, type, null) is the same; the Parquet schema text and the Arrow schema derived from it
    are the same (the text without field ids and the redundant Int(64, signed) annotation);
  - the footer key-value entries are the same list (no ARROW:schema), and, with --expect-kv-key, the input has
    exactly these keys;
  - pyarrow decodes every page of every column of the output, and the row count is the same;
  - the output has only PLAIN, PLAIN_DICTIONARY, RLE_DICTIONARY, RLE and the configured column encodings (the
    column chunk lists and the header of every page), only data pages of the configured version and dictionary
    pages, the configured compression on every column chunk, the configured format version, full row groups
    (the last one smaller), and a Bloom filter on the configured columns only;
  - optional: the sha256 of the input is the pinned sha256 (--want-sha, or the --sums list of `run`).

Sub-commands:
  one  SRC DST [--want-sha SHA] [--result JSON] [--log JSONL] [--threads N] [--duck-mem SIZE] [--tmp DIR]
       [--read-batch N] [settings options]
       re-encode and check one file; DST appears only when all checks pass (written as DST.tmp first);
       --read-batch: the rows of one DuckDB Arrow batch (default 262,144; the log line records it as read_batch;
       it is not a writer setting, so the settings sha256 is the same)
  run  --src DIR --dst DIR --log JSONL [--sums SHA256SUMS] [--sums-prefix P] [--jobs N] [--mem-total GB]
       [--mem-small GB] [--mem-big GB] [--mem-kill GB] [--big-bytes B] [--threads-small N] [--threads-big N]
       [--only NAME ...] [--trust-log] [--stop-file PATH] [--progress JSON] [--summary JSON] [--work DIR]
       [--read-batch N] [--read-batch-for FILE=N ...] [settings options]
       many files in parallel, largest first (LPT); resumes (a file with an OK log line and its output with the
       logged size and sha256 is skipped); each file runs in its own process; a memory watchdog kills a process
       over --mem-kill; no new process starts while the reserved memory would pass --mem-total; --read-batch N
       for all files, --read-batch-for FILE=N (repeatable) for one input file (a file whose string column holds
       more than 2 GiB in one batch of the default: the Arrow appender of DuckDB stops at 2 GiB of one string
       buffer)
  summary --log JSONL [--src DIR | --sums SHA256SUMS] [--sums-prefix P] [--out JSON] [settings options]
       counts, bytes in and out, failures, versions and settings of the last line of each file; complete: every
       file OK, one set of python/pyarrow/duckdb, the settings sha256 of the given settings
  make-sums --root ROOT --log JSONL --out FILE [--data-dir NAME]
       the SHA256SUMS of a root folder: the files of ROOT/NAME (default parquet) from the log (the size of each
       file is checked), every other regular file of ROOT from its bytes (C order of the paths)
  settings [settings options]
       print the writer settings and their sha256

Runtime: Python 3.12 or later with pyarrow and duckdb. The versions are checked at the start against
--pyarrow-version and --duckdb-version (an empty value accepts any version; the log records the versions).
Run it with python -I -B.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import platform
import resource
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone

LOG_VERSION = 2
READ_BATCH = 262_144
BASE_ENCODINGS = {"PLAIN", "PLAIN_DICTIONARY", "RLE_DICTIONARY", "RLE"}
COMPRESSIONS = {"zstd": "ZSTD", "snappy": "SNAPPY", "gzip": "GZIP", "brotli": "BROTLI", "lz4": "LZ4_RAW",
                "none": "UNCOMPRESSED"}
DATA_PAGE_TYPE = {"1.0": "DATA_PAGE", "2.0": "DATA_PAGE_V2"}
COLUMN_ENCODINGS = {"PLAIN", "DELTA_BINARY_PACKED", "DELTA_LENGTH_BYTE_ARRAY", "DELTA_BYTE_ARRAY",
                    "BYTE_STREAM_SPLIT"}
# The writer settings of the published files of the dataset (release 1.0.0). docs/REENCODE.md documents each key.
DEFAULTS = {
    "pyarrow_version": "26.0.1",
    "duckdb_version": "1.5.5",
    "format_version": "2.6",
    "data_page_version": "1.0",
    "compression": "zstd",
    "compression_level": 19,
    "row_group_rows": 1_000_000,
    "data_page_size": 8 << 20,
    "max_rows_per_page": 1_000_000,
    "dictionary_pagesize_limit": 64 << 20,
    "no_dictionary": ["token_value", "source_text", "source_col"],
    "column_encoding": {"source_line": "DELTA_BINARY_PACKED", "token_index": "DELTA_BINARY_PACKED"},
    "bloom_columns": ["file_path", "token_value", "source_text", "cregit_commit_sha", "personid"],
    "bloom_fpp": 0.01,
    "expect_kv_keys": [],
}


def settings_record(cfg: dict) -> dict:
    """The settings that the sha256 covers (the form of the log lines of the dataset build)."""
    n = len(cfg["expect_kv_keys"])
    return {
        "writer": f"pyarrow {cfg['pyarrow_version']} ParquetWriter",
        "reader": f"duckdb {cfg['duckdb_version']} read_parquet in file order, Arrow batches of {READ_BATCH} rows",
        "version": cfg["format_version"], "data_page_version": cfg["data_page_version"],
        "compression": cfg["compression"], "compression_level": cfg["compression_level"],
        "row_group_rows": cfg["row_group_rows"], "data_page_size": cfg["data_page_size"],
        "max_rows_per_page": cfg["max_rows_per_page"], "dictionary_pagesize_limit": cfg["dictionary_pagesize_limit"],
        "no_dictionary": list(cfg["no_dictionary"]), "column_encoding": dict(cfg["column_encoding"]),
        "dictionary": "every other column (list columns by the leaf <name>.list.element)",
        "bloom_columns": list(cfg["bloom_columns"]), "bloom_fpp": cfg["bloom_fpp"],
        "bloom_ndv": f"max count of distinct non-null values in one group of {cfg['row_group_rows']} rows, at least 1",
        "write_statistics": True, "write_page_index": False, "store_schema": False, "use_compliant_nested_type": True,
        "key_value_metadata": (f"the {n} entries of the input, in their order" if n
                               else "the entries of the input, in their order"),
        "row_group_input": "the DuckDB Arrow batches as chunks of one table (no combine_chunks copy)",
    }


def check_settings(cfg: dict) -> None:
    unknown = sorted(set(cfg) - set(DEFAULTS))
    if unknown:
        raise ValueError(f"unknown settings: {', '.join(unknown)}")
    if cfg["compression"] not in COMPRESSIONS:
        raise ValueError(f"compression {cfg['compression']!r}: one of {', '.join(sorted(COMPRESSIONS))}")
    if cfg["data_page_version"] not in ("1.0", "2.0"):
        raise ValueError(f"data_page_version {cfg['data_page_version']!r}: 1.0 or 2.0")
    for k in ("row_group_rows", "data_page_size", "max_rows_per_page", "dictionary_pagesize_limit"):
        if not isinstance(cfg[k], int) or cfg[k] < 1:
            raise ValueError(f"{k} {cfg[k]!r}: an integer of at least 1")
    if not 0 < float(cfg["bloom_fpp"]) < 1:
        raise ValueError(f"bloom_fpp {cfg['bloom_fpp']!r}: between 0 and 1")
    bad = sorted(e for e in cfg["column_encoding"].values() if e not in COLUMN_ENCODINGS)
    if bad:
        raise ValueError(f"column_encoding {bad}: one of {', '.join(sorted(COLUMN_ENCODINGS))}")
    both = sorted(set(cfg["no_dictionary"]) & set(cfg["column_encoding"]))
    if both:
        raise ValueError(f"{both}: in no_dictionary and column_encoding (a column encoding has no dictionary)")


def configure(overrides: dict | None = None) -> dict:
    """Set the module settings: DEFAULTS changed by overrides (the keys of DEFAULTS)."""
    global CFG, PYARROW_VERSION, DUCKDB_VERSION, ROW_GROUP_ROWS, BLOOM_COLUMNS, BLOOM_FPP, NO_DICTIONARY
    global COLUMN_ENCODING, KV_KEYS, ALLOWED_ENCODINGS, SETTINGS, SETTINGS_SHA256
    cfg = json.loads(json.dumps(DEFAULTS))
    cfg.update(json.loads(json.dumps(overrides or {})))
    check_settings(cfg)
    CFG = cfg
    PYARROW_VERSION, DUCKDB_VERSION = cfg["pyarrow_version"], cfg["duckdb_version"]
    ROW_GROUP_ROWS = cfg["row_group_rows"]
    BLOOM_COLUMNS, BLOOM_FPP = list(cfg["bloom_columns"]), cfg["bloom_fpp"]
    NO_DICTIONARY, COLUMN_ENCODING = list(cfg["no_dictionary"]), dict(cfg["column_encoding"])
    KV_KEYS = list(cfg["expect_kv_keys"])
    ALLOWED_ENCODINGS = BASE_ENCODINGS | set(COLUMN_ENCODING.values())
    SETTINGS = settings_record(cfg)
    SETTINGS_SHA256 = hashlib.sha256(json.dumps(SETTINGS, sort_keys=True).encode()).hexdigest()
    return cfg


configure()
ENCODING_NAMES = {0: "PLAIN", 1: "GROUP_VAR_INT", 2: "PLAIN_DICTIONARY", 3: "RLE", 4: "BIT_PACKED",
                  5: "DELTA_BINARY_PACKED", 6: "DELTA_LENGTH_BYTE_ARRAY", 7: "DELTA_BYTE_ARRAY",
                  8: "RLE_DICTIONARY", 9: "BYTE_STREAM_SPLIT"}
PAGE_TYPES = {0: "DATA_PAGE", 1: "INDEX_PAGE", 2: "DICTIONARY_PAGE", 3: "DATA_PAGE_V2"}


def utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def self_sha256() -> str:
    with open(os.path.abspath(__file__), "rb") as fh:
        return hashlib.sha256(fh.read()).hexdigest()


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for b in iter(lambda: fh.read(8 << 20), b""):
            h.update(b)
    return h.hexdigest()


def versions() -> dict:
    import duckdb
    import pyarrow as pa
    return {"python": platform.python_version(), "pyarrow": pa.__version__, "duckdb": duckdb.__version__,
            "reencode_sha256": self_sha256()}


def check_versions() -> None:
    v = versions()
    if (PYARROW_VERSION and v["pyarrow"] != PYARROW_VERSION) or (DUCKDB_VERSION and v["duckdb"] != DUCKDB_VERSION):
        sys.exit(f"ABORT: pyarrow {v['pyarrow']} and duckdb {v['duckdb']}; this tool needs pyarrow "
                 f"{PYARROW_VERSION} and duckdb {DUCKDB_VERSION}")


def sql_str(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


# ---------------------------------------------------------------- DuckDB
def con_new(threads: int, mem: str, tmp: str | None):
    import duckdb
    con = duckdb.connect()
    con.execute(f"SET threads={int(threads)}")
    con.execute(f"SET memory_limit='{mem}'")
    con.execute("SET enable_progress_bar=false")
    con.execute("SET preserve_insertion_order=true")
    if tmp:
        con.execute(f"SET temp_directory={sql_str(tmp)}")
    return con


def fingerprint(path: str, threads: int, mem: str, tmp: str | None) -> dict:
    """The content hash: row count, sum of the row hashes over all columns, an order-dependent sum,
    one sum of the value hashes for each column; and DuckDB DESCRIBE."""
    con = con_new(threads, mem, tmp)
    p = sql_str(path)
    desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet({p})").fetchall()
    cols = [r[0] for r in desc]
    q = ", ".join(f'"{c}"' for c in cols)
    per_col = ", ".join(f'sum(hash("{c}")::HUGEINT)::VARCHAR' for c in cols)
    sql = (f"SELECT count(*), sum(h::HUGEINT)::VARCHAR, sum(hash(file_row_number, h)::HUGEINT)::VARCHAR, {per_col} "
           f"FROM (SELECT file_row_number, hash({q}) AS h, * FROM read_parquet({p}, file_row_number=true))")
    r = con.execute(sql).fetchone()
    con.close()
    return {"rows": int(r[0]), "rowhash": r[1], "orderhash": r[2], "colhash": list(r[3:]),
            "describe": [[d[0], d[1], d[2]] for d in desc]}


def ndv_duckdb(path: str, threads: int, mem: str, tmp: str | None) -> dict:
    """The ndv rule with DuckDB (the tests compare it with ndv_pyarrow)."""
    if not BLOOM_COLUMNS:
        return {}
    con = con_new(threads, mem, tmp)
    sel = ", ".join(f'count(DISTINCT "{c}") AS c{i}' for i, c in enumerate(BLOOM_COLUMNS))
    mx = ", ".join(f"max(c{i})" for i in range(len(BLOOM_COLUMNS)))
    r = con.execute(f"SELECT {mx} FROM (SELECT {sel} FROM read_parquet({sql_str(path)}, file_row_number=true) "
                    f"GROUP BY file_row_number // {ROW_GROUP_ROWS})").fetchone()
    con.close()
    return {c: max(int(v or 0), 1) for c, v in zip(BLOOM_COLUMNS, r)}


# ---------------------------------------------------------------- pyarrow
def ndv_pyarrow(path: str) -> dict:
    """The ndv rule with bounded memory: the Bloom filter columns in groups of ROW_GROUP_ROWS rows in file order."""
    if not BLOOM_COLUMNS:
        return {}
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    best = {c: 0 for c in BLOOM_COLUMNS}

    def take(t):
        for c in BLOOM_COLUMNS:
            best[c] = max(best[c], pc.count_distinct(t.column(c), mode="only_valid").as_py())

    f = pq.ParquetFile(path)
    buf, n = [], 0
    for b in f.iter_batches(batch_size=READ_BATCH, columns=BLOOM_COLUMNS, use_threads=False):
        buf.append(b)
        n += b.num_rows
        while n >= ROW_GROUP_ROWS:
            t = pa.Table.from_batches(buf)
            take(t.slice(0, ROW_GROUP_ROWS))
            rest = t.slice(ROW_GROUP_ROWS)
            buf, n = rest.to_batches(), rest.num_rows
    if n:
        take(pa.Table.from_batches(buf))
    return {c: max(v, 1) for c, v in best.items()}


def kv_list(path: str) -> list:
    """The footer key-value entries in the file order (DuckDB keeps the order; the dict of pyarrow does not)."""
    import duckdb
    con = duckdb.connect()
    rows = con.execute("SELECT key, value FROM parquet_kv_metadata(?)", [path]).fetchall()
    con.close()
    return [[k.decode(), v.decode()] for k, v in rows]


def parquet_schema_text(path: str) -> str:
    """The Parquet schema without field ids, the root group name and the Int(64, signed) annotation."""
    import pyarrow.parquet as pq
    lines = str(pq.ParquetFile(path).schema).splitlines()[2:]
    return "\n".join(ln.replace(" field_id=-1", "").replace(" (Int(bitWidth=64, isSigned=true))", "")
                     for ln in lines)


def arrow_schema_text(path: str) -> str:
    import pyarrow.parquet as pq
    f = pq.ParquetFile(path)
    return (f.schema.to_arrow_schema().remove_metadata().to_string(show_field_metadata=False)
            .replace("large_string", "string").replace("large_list", "list"))


def writer_kwargs(schema, ndv: dict) -> dict:
    """The ParquetWriter options of the settings."""
    import pyarrow as pa
    nodict = set(NO_DICTIONARY) | set(COLUMN_ENCODING)
    dict_cols = [f"{f.name}.list.element" if pa.types.is_list(f.type) else f.name
                 for f in schema if f.name not in nodict]
    leaf_enc = {}
    for c, e in COLUMN_ENCODING.items():
        t = schema.field(c).type
        leaf_enc[f"{c}.list.element" if pa.types.is_list(t) else c] = e
    kw = dict(
        version=SETTINGS["version"], data_page_version=SETTINGS["data_page_version"], compression=CFG["compression"],
        compression_level=SETTINGS["compression_level"], use_dictionary=dict_cols, column_encoding=leaf_enc,
        write_statistics=True, write_page_index=False, store_schema=False,
        data_page_size=SETTINGS["data_page_size"], dictionary_pagesize_limit=SETTINGS["dictionary_pagesize_limit"],
        max_rows_per_page=SETTINGS["max_rows_per_page"], use_compliant_nested_type=True,
    )
    kw["bloom_filter_options"] = {c: {"ndv": ndv[c], "fpp": BLOOM_FPP} for c in BLOOM_COLUMNS}
    return kw


def write(src: str, out: str, ndv: dict, kv: list, threads: int, mem: str, tmp: str | None,
          read_batch: int = READ_BATCH) -> None:
    """DuckDB reads the rows in file order, pyarrow
    writes row groups of ROW_GROUP_ROWS rows. read_batch: the rows of one DuckDB Arrow batch (default READ_BATCH);
    a smaller value for a file whose string column holds more than 2 GiB in one batch of READ_BATCH rows (the
    Arrow Appender of DuckDB stops at 2 GiB of one regular string buffer)."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    con = con_new(threads, mem, tmp)
    reader = con.execute(f"SELECT * FROM read_parquet({sql_str(src)})").to_arrow_reader(int(read_batch))
    schema = reader.schema
    w = pq.ParquetWriter(out, schema, **writer_kwargs(schema, ndv))
    buf, nbuf = [], 0
    for b in reader:
        buf.append(b)
        nbuf += b.num_rows
        while nbuf >= ROW_GROUP_ROWS:
            # no combine_chunks: the copy into one chunk doubles the peak memory;
            # the pages can end at other rows, the values and the settings are the same
            t = pa.Table.from_batches(buf, schema=schema)
            w.write_table(t.slice(0, ROW_GROUP_ROWS), row_group_size=ROW_GROUP_ROWS)
            rest = t.slice(ROW_GROUP_ROWS)
            buf, nbuf = rest.to_batches(), rest.num_rows
            del t, rest
    if nbuf:
        w.write_table(pa.Table.from_batches(buf, schema=schema), row_group_size=ROW_GROUP_ROWS)
    w.add_key_value_metadata({k: v for k, v in kv})
    w.close()
    con.close()


def pyarrow_decode(path: str, threads: int) -> int:
    import pyarrow.parquet as pq
    f = pq.ParquetFile(path)
    n = 0
    for b in f.iter_batches(batch_size=READ_BATCH // 4, use_threads=threads > 1):
        n += b.num_rows
    return n


# ---------------------------------------------------------------- page headers (Thrift compact protocol)
class _Buf:
    def __init__(self, data: bytes):
        self.d, self.i = data, 0

    def byte(self) -> int:
        b = self.d[self.i]
        self.i += 1
        return b

    def varint(self) -> int:
        shift = r = 0
        while True:
            b = self.byte()
            r |= (b & 0x7F) << shift
            if not b & 0x80:
                return r
            shift += 7

    def zigzag(self) -> int:
        n = self.varint()
        return (n >> 1) ^ -(n & 1)

    def skip(self, t: int) -> None:
        if t in (1, 2):
            return
        if t == 3:
            self.i += 1
        elif t in (4, 5, 6):
            self.varint()
        elif t == 7:
            self.i += 8
        elif t == 8:
            n = self.varint()
            self.i += n
        elif t in (9, 10):
            h = self.byte()
            n, et = h >> 4, h & 0x0F
            if n == 15:
                n = self.varint()
            for _ in range(n):
                if et in (1, 2):
                    self.i += 1
                else:
                    self.skip(et)
        elif t == 11:
            n = self.varint()
            if n:
                kt = self.byte()
                for _ in range(n):
                    self.skip(kt >> 4)
                    self.skip(kt & 0x0F)
        elif t == 12:
            self.struct(lambda fid, ft: None)
        else:
            raise ValueError(f"thrift type {t}")
        if self.i > len(self.d):
            raise IndexError("short buffer")

    def struct(self, on_field) -> None:
        """Read one struct; on_field(fid, type) returns True when it read the value itself."""
        last = 0
        while True:
            h = self.byte()
            if h == 0:
                return
            t, delta = h & 0x0F, h >> 4
            fid = last + delta if delta else self.zigzag()
            last = fid
            if not on_field(fid, t):
                self.skip(t)


def _page_header(data: bytes) -> tuple[dict, int]:
    """Parse one PageHeader; return the fields we need and the header length."""
    b = _Buf(data)
    ph: dict = {"encodings": []}

    def sub(fid, t, keys):
        def f(sfid, st):
            if st == 5 and sfid in keys:
                ph["encodings"].append(b.zigzag())
                return True
            return False
        return f

    def top(fid, t):
        if t == 5 and fid in (1, 2, 3):
            ph[{1: "type", 2: "usize", 3: "csize"}[fid]] = b.zigzag()
            return True
        if t == 12 and fid == 5:      # DataPageHeader: encoding, definition_level_encoding, repetition_level_encoding
            b.struct(sub(fid, t, (2, 3, 4)))
            return True
        if t == 12 and fid == 7:      # DictionaryPageHeader: encoding
            b.struct(sub(fid, t, (2,)))
            return True
        if t == 12 and fid == 8:      # DataPageHeaderV2: encoding
            b.struct(sub(fid, t, (4,)))
            return True
        return False

    b.struct(top)
    if b.i > len(data):
        raise IndexError("short buffer")
    return ph, b.i


def scan_pages(path: str) -> dict:
    """Read the header of every page of every column chunk."""
    import pyarrow.parquet as pq
    md = pq.ParquetFile(path).metadata
    types: dict[str, int] = {}
    encs: set[str] = set()
    pages = 0
    with open(path, "rb") as fh:
        for i in range(md.num_row_groups):
            rg = md.row_group(i)
            for j in range(rg.num_columns):
                c = rg.column(j)
                start = c.dictionary_page_offset if c.has_dictionary_page and c.dictionary_page_offset else \
                    c.data_page_offset
                end = start + c.total_compressed_size
                pos = start
                while pos < end:
                    size = 4096
                    while True:
                        fh.seek(pos)
                        data = fh.read(size)
                        try:
                            ph, hl = _page_header(data)
                            break
                        except IndexError:
                            if len(data) < size:
                                raise
                            size *= 8
                    tn = PAGE_TYPES.get(ph.get("type"), str(ph.get("type")))
                    types[tn] = types.get(tn, 0) + 1
                    encs.update(ENCODING_NAMES.get(e, str(e)) for e in ph["encodings"])
                    pages += 1
                    pos += hl + ph["csize"]
                if pos != end:
                    raise ValueError(f"row group {i} column {j}: pages end at {pos}, the chunk at {end}")
    return {"pages": pages, "page_types": types, "page_encodings": sorted(encs)}


def layout_checks(path: str) -> dict:
    """Column chunk facts of the output: encodings, compression, row groups, Bloom filters, format version."""
    import pyarrow.parquet as pq
    md = pq.ParquetFile(path).metadata
    encs, comps, bloom_cols, bloom_bytes, data = set(), set(), set(), 0, 0
    sizes = []
    for i in range(md.num_row_groups):
        rg = md.row_group(i)
        sizes.append(rg.num_rows)
        for j in range(rg.num_columns):
            c = rg.column(j)
            encs.update(c.encodings)
            comps.add(c.compression)
            data += c.total_compressed_size
            if c.bloom_filter_offset is not None and c.bloom_filter_offset > 0:
                bloom_cols.add(c.path_in_schema)
                bloom_bytes += c.bloom_filter_length or 0
    rg_ok = all(n == ROW_GROUP_ROWS for n in sizes[:-1]) and (not sizes or 0 < sizes[-1] <= ROW_GROUP_ROWS)
    return {"chunk_encodings": sorted(encs), "compressions": sorted(comps), "row_groups": md.num_row_groups,
            "row_groups_ok": rg_ok, "bloom_columns": sorted(bloom_cols), "bloom_bytes": bloom_bytes,
            "data_bytes": data, "footer_bytes": md.serialized_size, "format_version": md.format_version,
            "created_by": md.created_by}


# ---------------------------------------------------------------- one file
class _Job(threading.Thread):
    def __init__(self, fn, *args):
        super().__init__(daemon=True)
        self.fn, self.args, self.value, self.exc, self.secs = fn, args, None, None, 0.0

    def run(self):
        t = time.perf_counter()
        try:
            self.value = self.fn(*self.args)
        except BaseException as e:  # noqa: BLE001 - reported by the caller
            self.exc = e
        self.secs = round(time.perf_counter() - t, 2)

    def get(self):
        self.join()
        if self.exc:
            raise self.exc
        return self.value


def cpu_s() -> float:
    r = resource.getrusage(resource.RUSAGE_SELF)
    return r.ru_utime + r.ru_stime


def reencode_one(src: str, dst: str, want_sha: str | None = None, threads: int = 1, mem: str = "2GB",
                 tmp: str | None = None, read_batch: int = READ_BATCH) -> dict:
    """Re-encode src to dst and check it; return the log record. dst exists only when the status is OK."""
    import pyarrow.parquet as pq
    t0, c0 = time.perf_counter(), cpu_s()
    rec: dict = {"v": LOG_VERSION, "file": os.path.basename(src), "status": "ERROR", "error": "",
                 "start_utc": utc(), "settings_sha256": SETTINGS_SHA256, "threads": threads,
                 "read_batch": int(read_batch)}
    out_tmp = dst + ".tmp"
    times: dict = {}
    try:
        rec["versions"] = versions()
        rec["in_bytes"] = os.path.getsize(src)
        rec["want_sha256"] = want_sha or ""
        in_sha = _Job(sha256_file, src)
        in_sha.start()
        kv_in = kv_list(src)
        in_md = pq.ParquetFile(src).metadata
        rec["row_groups_in"] = in_md.num_row_groups
        rec["created_by_in"] = in_md.created_by
        pq_in, arrow_in = parquet_schema_text(src), arrow_schema_text(src)
        in_fp = _Job(fingerprint, src, threads, mem, tmp)
        in_fp.start()
        t = time.perf_counter()
        ndv = ndv_pyarrow(src)
        times["ndv_s"] = round(time.perf_counter() - t, 2)
        rec["bloom_ndv"] = ndv
        if os.path.exists(out_tmp):
            os.remove(out_tmp)
        t = time.perf_counter()
        write(src, out_tmp, ndv, [kv for kv in kv_in if kv[0] != "ARROW:schema"], threads, mem, tmp,
              read_batch=read_batch)
        times["write_s"] = round(time.perf_counter() - t, 2)
        t = time.perf_counter()
        out_sha = _Job(sha256_file, out_tmp)
        out_fp = _Job(fingerprint, out_tmp, threads, mem, tmp)
        decode = _Job(pyarrow_decode, out_tmp, threads)
        for j in (out_sha, out_fp, decode):
            j.start()
        pages = scan_pages(out_tmp)
        lay = layout_checks(out_tmp)
        kv_out = kv_list(out_tmp)
        pq_out, arrow_out = parquet_schema_text(out_tmp), arrow_schema_text(out_tmp)
        fi, fo = in_fp.get(), out_fp.get()
        times["in_check_s"] = in_fp.secs
        n_decoded = decode.get()
        rec["in_sha256"] = in_sha.get()
        rec["out_sha256"] = out_sha.get()
        times["out_check_s"] = round(time.perf_counter() - t, 2)
        rec["out_bytes"] = os.path.getsize(out_tmp)
        rec["rows"] = fi["rows"]
        rec.update({k: lay[k] for k in ("row_groups", "bloom_bytes", "data_bytes", "footer_bytes",
                                        "format_version", "created_by")})
        rec["row_groups_out"] = rec.pop("row_groups")
        rec["created_by_out"] = rec.pop("created_by")
        rec["encodings"] = sorted(set(lay["chunk_encodings"]) | set(pages["page_encodings"]))
        rec["page_types"] = pages["page_types"]
        rec["pages"] = pages["pages"]
        kv_in_clean = [kv for kv in kv_in if kv[0] != "ARROW:schema"]
        checks = {
            "in_sha256_pinned": (not want_sha) or rec["in_sha256"] == want_sha,
            "rows": fi["rows"] == fo["rows"],
            "rowhash": fi["rowhash"] == fo["rowhash"],
            "orderhash": fi["orderhash"] == fo["orderhash"],
            "colhash": fi["colhash"] == fo["colhash"] and len(fi["colhash"]) == len(fi["describe"]),
            "describe": fi["describe"] == fo["describe"],
            "parquet_schema": pq_in == pq_out,
            "arrow_schema": arrow_in == arrow_out,
            "kv_input_keys": (not KV_KEYS) or sorted(k for k, _ in kv_in_clean) == sorted(KV_KEYS),
            "kv": kv_out == kv_in_clean,
            "pyarrow_decode": n_decoded == fi["rows"],
            "duckdb_read": fo["rows"] == fi["rows"],
            "encodings": set(rec["encodings"]) <= ALLOWED_ENCODINGS,
            "page_version": set(pages["page_types"]) <= {DATA_PAGE_TYPE[CFG["data_page_version"]], "DICTIONARY_PAGE"},
            "compression": lay["compressions"] == [COMPRESSIONS[CFG["compression"]]],
            "row_groups": lay["row_groups_ok"],
            "bloom": lay["bloom_columns"] == sorted(BLOOM_COLUMNS),
            "format_version": lay["format_version"] == CFG["format_version"],
        }
        rec["checks"] = checks
        rec["columns"] = len(fi["describe"])
        rec["fingerprint"] = {"rowhash": fi["rowhash"], "orderhash": fi["orderhash"], "colhash": fi["colhash"]}
        rec["kv"] = kv_in_clean
        bad = sorted(k for k, v in checks.items() if not v)
        if bad:
            rec["status"] = "FAIL"
            rec["error"] = "check failed: " + ", ".join(bad)
        else:
            os.replace(out_tmp, dst)
            rec["status"] = "OK"
    except Exception as e:  # noqa: BLE001 - every failure goes to the log
        rec["status"] = "ERROR"
        rec["error"] = f"{type(e).__name__}: {str(e)[:300]}"
    finally:
        if rec["status"] != "OK" and os.path.exists(out_tmp):
            os.remove(out_tmp)
    times["total_s"] = round(time.perf_counter() - t0, 2)
    rec["times"] = times
    rec["cpu_s"] = round(cpu_s() - c0, 2)
    rec["peak_rss_bytes"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
    rec["end_utc"] = utc()
    return rec


def append_log(log: str, rec: dict) -> None:
    line = json.dumps(rec, sort_keys=True, separators=(",", ":")) + "\n"
    if chr(64) in line:
        raise SystemExit("ABORT: a log line holds the at sign")
    with open(log, "a") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        fh.write(line)
        fh.flush()
        os.fsync(fh.fileno())
        fcntl.flock(fh, fcntl.LOCK_UN)


def cmd_one(a) -> int:
    check_versions()
    if a.read_batch < 1:
        sys.exit(f"ABORT: --read-batch {a.read_batch}")
    rec = reencode_one(a.src, a.dst, a.want_sha, a.threads, a.duck_mem, a.tmp, a.read_batch)
    if a.result:
        with open(a.result + ".part", "w") as fh:
            json.dump(rec, fh, sort_keys=True)
        os.replace(a.result + ".part", a.result)
    if a.log:
        append_log(a.log, rec)
    if not a.result:
        print(json.dumps({k: rec.get(k) for k in ("file", "status", "error", "in_bytes", "out_bytes", "rows",
                                                  "times", "cpu_s", "peak_rss_bytes")}, sort_keys=True))
    return 0 if rec["status"] == "OK" else 1


# ---------------------------------------------------------------- the driver
def read_log(log: str) -> dict:
    """The last line of each file."""
    last = {}
    if os.path.exists(log):
        with open(log) as fh:
            for line in fh:
                line = line.strip()
                if line:
                    r = json.loads(line)
                    last[r["file"]] = r
    return last


def read_sums(path: str | None, prefix: str = "parquet/") -> dict:
    sums = {}
    if path:
        with open(path) as fh:
            for line in fh:
                h, rel = line.rstrip("\n").split("  ", 1)
                if rel.startswith(prefix):
                    sums[rel[len(prefix):]] = h
    return sums


def rss_of(pid: int) -> int:
    try:
        with open(f"/proc/{pid}/status") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        pass
    return 0


def mem_available() -> int:
    with open("/proc/meminfo") as fh:
        for line in fh:
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) * 1024
    return 0


GB = 1 << 30


def cmd_run(a) -> int:
    check_versions()
    names = sorted(n for n in os.listdir(a.src) if n.endswith(".parquet"))
    if a.only:
        keep = set(a.only)
        names = [n for n in names if n in keep]
    sums = read_sums(a.sums, a.sums_prefix)
    if a.sums:
        miss = [n for n in names if n not in sums]
        extra = sorted(set(sums) - set(names)) if not a.only else []
        if miss or extra:
            print(f"ABORT: {len(miss)} input files are not in {os.path.basename(a.sums)}, {len(extra)} rows of it have "
                  f"no input file (first: {(miss + extra)[:2]})", flush=True)
            return 2
    rb_for = {}
    for x in a.read_batch_for:
        f, _, v = x.partition("=")
        if not f or not v.isdigit() or int(v) < 1 or not os.path.isfile(os.path.join(a.src, f)):
            print(f"ABORT: --read-batch-for {x!r}: want FILE=N with FILE an input file of --src and N >= 1", flush=True)
            return 2
        rb_for[f] = int(v)
    if a.read_batch < 1:
        print(f"ABORT: --read-batch {a.read_batch}", flush=True)
        return 2
    os.makedirs(a.dst, exist_ok=True)
    work = a.work or os.path.join(os.path.dirname(os.path.abspath(a.log)), "reencode-work")
    for d in ("results", "stderr", "tmp"):
        os.makedirs(os.path.join(work, d), exist_ok=True)
    cfg_path = os.path.join(work, "settings.json")
    with open(cfg_path, "w") as fh:
        json.dump(CFG, fh, sort_keys=True)
    last = read_log(a.log)
    size = {n: os.path.getsize(os.path.join(a.src, n)) for n in names}
    todo, skipped = [], 0
    for n in names:
        r = last.get(n)
        d = os.path.join(a.dst, n)
        if r and r["status"] == "OK" and os.path.exists(d) and os.path.getsize(d) == r["out_bytes"] \
                and (a.trust_log or sha256_file(d) == r["out_sha256"]) \
                and (not sums or r["in_sha256"] == sums[n]):
            skipped += 1
            continue
        todo.append(n)
    todo.sort(key=lambda n: (-size[n], n))          # LPT: the largest first
    total_in = sum(size[n] for n in todo)
    print(f"[{utc()}] files {len(names)}; skip {skipped} (OK in the log, the output has the logged sha256); "
          f"to do {len(todo)} ({total_in} B); jobs {a.jobs}; memory total {a.mem_total} GB, small {a.mem_small} GB, "
          f"big {a.mem_big} GB (input >= {a.big_bytes} B), kill at {a.mem_kill} GB; settings {SETTINGS_SHA256[:12]}; "
          f"read batch {a.read_batch}{''.join(f', {f} {v}' for f, v in sorted(rb_for.items()))}",
          flush=True)
    running: dict = {}           # pid -> dict(name, proc, reserve, start, peak)
    counts: dict = {}
    done_in = done_out = 0
    t0 = time.time()
    stop = False
    lock = threading.Lock()

    def watchdog():
        while True:
            time.sleep(0.25)
            with lock:
                items = list(running.items())
            for pid, j in items:
                r = rss_of(pid)
                j["peak"] = max(j["peak"], r)
                if r > a.mem_kill * GB and not j.get("killed"):
                    j["killed"] = f"memory limit: RSS {r} B over {a.mem_kill} GB"
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except OSError:
                        pass
            if not items and getattr(watchdog, "end", False):
                return

    wd = threading.Thread(target=watchdog, daemon=True)
    wd.start()

    def progress():
        if not a.progress:
            return
        with lock:
            run = [{"file": j["name"], "in_bytes": size[j["name"]], "elapsed_s": round(time.time() - j["start"]),
                    "rss_bytes": j["peak"]} for j in running.values()]
        p = {"utc": utc(), "elapsed_s": round(time.time() - t0), "files": len(names), "skipped": skipped,
             "todo": len(todo), "finished": sum(counts.values()), "counts": counts, "done_in_bytes": done_in,
             "done_out_bytes": done_out, "todo_in_bytes": total_in, "running": run,
             "mem_available_bytes": mem_available()}
        with open(a.progress + ".part", "w") as fh:
            json.dump(p, fh, sort_keys=True, indent=1)
        os.replace(a.progress + ".part", a.progress)

    def reserve(n):
        return a.mem_big if size[n] >= a.big_bytes else a.mem_small

    def start(n):
        res = os.path.join(work, "results", n + ".json")
        for p in (res, os.path.join(a.dst, n + ".tmp")):
            if os.path.exists(p):
                os.remove(p)
        thr = a.threads_big if size[n] >= a.big_bytes else a.threads_small
        cmd = [sys.executable, "-I", "-B", os.path.abspath(__file__), "one", os.path.join(a.src, n),
               os.path.join(a.dst, n), "--result", res, "--threads", str(thr),
               "--duck-mem", f"{max(1, int(reserve(n) * 0.4))}GB", "--tmp", os.path.join(work, "tmp"),
               "--read-batch", str(rb_for.get(n, a.read_batch)), "--config", cfg_path]
        if sums:
            cmd += ["--want-sha", sums[n]]
        err = open(os.path.join(work, "stderr", n + ".txt"), "w")
        p = subprocess.Popen(cmd, stdout=err, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
        err.close()
        with lock:
            running[p.pid] = {"name": n, "proc": p, "reserve": reserve(n), "start": time.time(), "peak": 0,
                              "result": res}

    def finish(pid, status, ru):
        nonlocal done_in, done_out
        with lock:
            j = running.pop(pid)
        n = j["name"]
        rec = None
        if os.path.exists(j["result"]):
            with open(j["result"]) as fh:
                rec = json.load(fh)
        if rec is None:
            sig = os.WTERMSIG(status) if os.WIFSIGNALED(status) else None
            rec = {"v": LOG_VERSION, "file": n, "status": "ERROR", "in_bytes": size[n],
                   "error": j.get("killed") or f"the process ended with {'signal ' + str(sig) if sig else 'exit ' + str(os.WEXITSTATUS(status))} and no result",
                   "settings_sha256": SETTINGS_SHA256, "read_batch": rb_for.get(n, a.read_batch), "end_utc": utc()}
            tmpf = os.path.join(a.dst, n + ".tmp")
            if os.path.exists(tmpf):
                os.remove(tmpf)
        rec["driver_peak_rss_bytes"] = max(j["peak"], ru.ru_maxrss * 1024)
        rec["driver_wall_s"] = round(time.time() - j["start"], 2)
        append_log(a.log, rec)
        counts[rec["status"]] = counts.get(rec["status"], 0) + 1
        if rec["status"] == "OK":
            done_in += rec["in_bytes"]
            done_out += rec["out_bytes"]
        k = sum(counts.values())
        if rec["status"] != "OK" or k % 100 == 0 or k == len(todo) or size[n] >= a.big_bytes:
            print(f"[{utc()}] [{k}/{len(todo)}] {time.time() - t0:.0f}s {counts} {n} {rec['status']} "
                  f"{rec.get('in_bytes')} -> {rec.get('out_bytes')} B, {rec.get('driver_wall_s')} s, peak RSS "
                  f"{rec['driver_peak_rss_bytes'] >> 20} MB {rec.get('error', '')}", flush=True)

    queue = list(todo)
    last_prog = 0.0
    while queue or running:
        if a.stop_file and os.path.exists(a.stop_file) and not stop:
            stop = True
            print(f"[{utc()}] stop file found: no new file starts; {len(running)} running, {len(queue)} not started",
                  flush=True)
        while queue and not stop and len(running) < a.jobs:
            n = queue[0]
            with lock:
                used = sum(j["reserve"] for j in running.values())
            if running and used + reserve(n) > a.mem_total:
                # LPT with memory: take the largest file that fits, else wait
                fit = [m for m in queue if used + reserve(m) <= a.mem_total]
                if not fit:
                    break
                n = fit[0]
            queue.remove(n)
            start(n)
        if stop and not running:
            break
        try:
            pid, status, ru = os.wait4(-1, os.WNOHANG)
        except ChildProcessError:
            pid = 0
        if pid and pid in running:
            finish(pid, status, ru)
            continue
        if time.time() - last_prog > 30:
            progress()
            last_prog = time.time()
        time.sleep(0.5)
    watchdog.end = True
    progress()
    s = summarize(read_log(a.log), names, sums)
    s["run"] = {"wall_s": round(time.time() - t0), "jobs": a.jobs, "skipped": skipped, "this_run": counts,
                "stopped": stop, "end_utc": utc()}
    if a.summary:
        with open(a.summary, "w") as fh:
            json.dump(s, fh, sort_keys=True, indent=1)
    print(summary_line(s), flush=True)
    if stop:
        return 3
    return 0 if s["complete"] else 1


def summarize(last: dict, names: list, sums: dict) -> dict:
    recs = [last[n] for n in names if n in last]
    by = {}
    for r in recs:
        by[r["status"]] = by.get(r["status"], 0) + 1
    ok = [r for r in recs if r["status"] == "OK"]
    fails = [{"file": r["file"], "status": r["status"], "error": r.get("error", "")[:300]}
             for r in recs if r["status"] != "OK"]
    missing = [n for n in names if n not in last]
    s = {"files": len(names), "logged": len(recs), "status": by, "missing": len(missing),
         "missing_first": missing[:5], "failures": fails[:50], "failures_total": len(fails),
         "ok_in_bytes": sum(r["in_bytes"] for r in ok), "ok_out_bytes": sum(r["out_bytes"] for r in ok),
         "ok_rows": sum(r["rows"] for r in ok),
         "ratio": round(sum(r["out_bytes"] for r in ok) / max(1, sum(r["in_bytes"] for r in ok)), 4),
         "cpu_s": round(sum(r.get("cpu_s", 0) for r in ok), 1),
         "max_peak_rss_bytes": max([r.get("driver_peak_rss_bytes", r.get("peak_rss_bytes", 0)) for r in ok],
                                   default=0),
         "max_total_s": max([r.get("times", {}).get("total_s", 0) for r in ok], default=0),
         # one set of python, pyarrow and duckdb; the tool (reencode_sha256) may have more than one version
         # (the retry of one file with a changed tool), so each one is listed with its count
         "versions": sorted({json.dumps({k: v for k, v in r.get("versions", {}).items() if k != "reencode_sha256"},
                                        sort_keys=True) for r in ok}),
         "reencode_sha256": {h: sum(1 for r in ok if r.get("versions", {}).get("reencode_sha256", "") == h)
                             for h in sorted({r.get("versions", {}).get("reencode_sha256", "") for r in ok})},
         # the rows of one DuckDB Arrow batch of write(); a line without the key used READ_BATCH
         "read_batch": {str(b): sum(1 for r in ok if r.get("read_batch", READ_BATCH) == b)
                        for b in sorted({r.get("read_batch", READ_BATCH) for r in ok})},
         "read_batch_not_default": sorted(r["file"] for r in ok if r.get("read_batch", READ_BATCH) != READ_BATCH),
         "settings_sha256": sorted({r.get("settings_sha256", "") for r in ok}),
         "in_sha256_pinned": sum(1 for r in ok if r.get("want_sha256")) if sums else 0}
    if sums:
        bad = [r["file"] for r in ok if r["in_sha256"] != sums.get(r["file"])]
        s["in_sha256_not_pinned_value"] = bad[:5]
        s["in_sha256_mismatch"] = len(bad)
    s["complete"] = (len(ok) == len(names) and not missing and len(s["versions"]) == 1
                     and s["settings_sha256"] == [SETTINGS_SHA256] and not s.get("in_sha256_mismatch"))
    return s


def summary_line(s: dict) -> str:
    gb = lambda b: f"{b / 1e9:.2f} GB"  # noqa: E731
    return (f"reencode summary: {s['files']} files; status {s['status']}; missing {s['missing']}; in "
            f"{s['ok_in_bytes']} B ({gb(s['ok_in_bytes'])}) -> out {s['ok_out_bytes']} B ({gb(s['ok_out_bytes'])}, "
            f"{100 * s['ratio']:.1f}%); rows {s['ok_rows']}; CPU {s['cpu_s'] / 3600:.1f} h; max peak RSS "
            f"{s['max_peak_rss_bytes'] >> 20} MB; failures {s['failures_total']}; complete {s['complete']}")


def cmd_summary(a) -> int:
    last = read_log(a.log)
    sums = read_sums(a.sums, a.sums_prefix)
    if a.src:
        names = sorted(n for n in os.listdir(a.src) if n.endswith(".parquet"))
    elif sums:
        names = sorted(sums)
    else:
        names = sorted(last)
    s = summarize(last, names, sums)
    if a.out:
        with open(a.out, "w") as fh:
            json.dump(s, fh, sort_keys=True, indent=1)
    print(summary_line(s))
    return 0 if s["complete"] else 1


def cmd_make_sums(a) -> int:
    last = read_log(a.log)
    data = a.data_dir.strip("/")
    pdir = os.path.join(a.root, data)
    names = sorted(os.listdir(pdir))
    bad = [n for n in names if not n.endswith(".parquet")]
    if bad:
        sys.exit(f"ABORT: {data}/ has other files: {bad[:3]}")
    lines, total = [], 0
    for n in names:
        r = last.get(n)
        if not r or r["status"] != "OK":
            sys.exit(f"ABORT: {n} has no OK line as the last line of the log")
        sz = os.path.getsize(os.path.join(pdir, n))
        if sz != r["out_bytes"]:
            sys.exit(f"ABORT: {n}: size {sz} is not the logged {r['out_bytes']}")
        total += sz
        lines.append((f"{data}/{n}", r["out_sha256"]))
    root, out = os.path.abspath(a.root), os.path.abspath(a.out)
    for dirpath, dirnames, filenames in os.walk(root):
        if dirpath == root:
            dirnames[:] = [d for d in dirnames if d != data]
        for n in filenames:
            p = os.path.join(dirpath, n)
            if p == out:
                continue
            rel = os.path.relpath(p, root).replace(os.sep, "/")
            if n.startswith(".") or not os.path.isfile(p):
                sys.exit(f"ABORT: unexpected entry: {rel}")
            total += os.path.getsize(p)
            lines.append((rel, sha256_file(p)))
    lines.sort(key=lambda x: x[0].encode())
    with open(a.out, "w") as fh:
        for rel, h in lines:
            fh.write(f"{h}  {rel}\n")
    print(f"make-sums: {len(lines)} files ({len(names)} re-encoded files), {total} B -> {os.path.basename(a.out)} "
          f"{sha256_file(a.out)[:12]}")
    return 0



def cmd_settings(a) -> int:
    print(json.dumps(SETTINGS, sort_keys=True, indent=1))
    print(f"settings_sha256 {SETTINGS_SHA256}")
    return 0


# option -> (settings key, type); the list options replace the default list
SETTING_OPTIONS = {
    "--pyarrow-version": ("pyarrow_version", str), "--duckdb-version": ("duckdb_version", str),
    "--format-version": ("format_version", str), "--data-page-version": ("data_page_version", str),
    "--compression": ("compression", str), "--compression-level": ("compression_level", int),
    "--row-group-rows": ("row_group_rows", int), "--data-page-size": ("data_page_size", int),
    "--max-rows-per-page": ("max_rows_per_page", int),
    "--dictionary-pagesize-limit": ("dictionary_pagesize_limit", int), "--bloom-fpp": ("bloom_fpp", float),
}
LIST_OPTIONS = {"--no-dictionary": "no_dictionary", "--bloom-column": "bloom_columns",
                "--expect-kv-key": "expect_kv_keys"}


def add_settings_options(p) -> None:
    g = p.add_argument_group("writer settings (default: the settings of the published dataset files)")
    g.add_argument("--config", metavar="JSON", help="a JSON object with any of the setting keys of `settings`; "
                   "the options below change it")
    for opt, (key, typ) in SETTING_OPTIONS.items():
        g.add_argument(opt, type=typ, dest=key, default=None, help=f"default {DEFAULTS[key]!r}")
    for opt, key in LIST_OPTIONS.items():
        g.add_argument(opt, dest=key, action="append", default=None, metavar="COLUMN",
                       help=f"repeatable, replaces the default {DEFAULTS[key]}")
    g.add_argument("--column-encoding", dest="column_encoding", action="append", default=None,
                   metavar="COLUMN=ENCODING", help=f"repeatable, replaces the default {DEFAULTS['column_encoding']}")
    g.add_argument("--no-bloom", action="store_true", help="no Bloom filter")


def settings_from_args(a) -> dict:
    """The overrides of the settings: --config first, then the options."""
    over: dict = {}
    if getattr(a, "config", None):
        with open(a.config) as fh:
            over = json.load(fh)
        if not isinstance(over, dict):
            raise ValueError(f"{a.config}: not a JSON object")
    for key, _ in SETTING_OPTIONS.values():
        if getattr(a, key, None) is not None:
            over[key] = getattr(a, key)
    for key in LIST_OPTIONS.values():
        if getattr(a, key, None) is not None:
            over[key] = list(getattr(a, key))
    if getattr(a, "column_encoding", None) is not None:
        enc = {}
        for x in a.column_encoding:
            c, _, e = x.partition("=")
            if not c or not e:
                raise ValueError(f"--column-encoding {x!r}: want COLUMN=ENCODING")
            enc[c] = e
        over["column_encoding"] = enc
    if getattr(a, "no_bloom", False):
        over["bloom_columns"] = []
    return over


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sp = p.add_subparsers(dest="cmd", required=True)
    o = sp.add_parser("one", help="re-encode and check one file")
    o.add_argument("src")
    o.add_argument("dst")
    o.add_argument("--want-sha")
    o.add_argument("--result")
    o.add_argument("--log")
    o.add_argument("--threads", type=int, default=1)
    o.add_argument("--duck-mem", default="2GB")
    o.add_argument("--tmp")
    o.add_argument("--read-batch", type=int, default=READ_BATCH)
    add_settings_options(o)
    r = sp.add_parser("run", help="re-encode and check the files of a folder in parallel")
    r.add_argument("--src", required=True)
    r.add_argument("--dst", required=True)
    r.add_argument("--log", required=True)
    r.add_argument("--sums")
    r.add_argument("--sums-prefix", default="parquet/")
    r.add_argument("--jobs", type=int, default=os.cpu_count() or 1)
    r.add_argument("--mem-total", type=float, default=max(1.0, mem_available() / GB * 0.85))
    r.add_argument("--mem-small", type=float, default=2.0)
    r.add_argument("--mem-big", type=float, default=6.0)
    r.add_argument("--mem-kill", type=float, default=16.0)
    r.add_argument("--big-bytes", type=int, default=1_000_000_000)
    r.add_argument("--threads-small", type=int, default=1)
    r.add_argument("--threads-big", type=int, default=4)
    r.add_argument("--only", nargs="*")
    r.add_argument("--trust-log", action="store_true")
    r.add_argument("--stop-file")
    r.add_argument("--progress")
    r.add_argument("--summary")
    r.add_argument("--work")
    r.add_argument("--read-batch", type=int, default=READ_BATCH)
    r.add_argument("--read-batch-for", action="append", default=[], metavar="FILE=N")
    add_settings_options(r)
    s = sp.add_parser("summary", help="summarize a log")
    s.add_argument("--log", required=True)
    s.add_argument("--src")
    s.add_argument("--sums")
    s.add_argument("--sums-prefix", default="parquet/")
    s.add_argument("--out")
    add_settings_options(s)
    m = sp.add_parser("make-sums", help="write the SHA256SUMS of a root folder")
    m.add_argument("--root", required=True)
    m.add_argument("--log", required=True)
    m.add_argument("--out", required=True)
    m.add_argument("--data-dir", default="parquet")
    t = sp.add_parser("settings", help="print the writer settings and their sha256")
    add_settings_options(t)
    a = p.parse_args(argv)
    try:
        configure(settings_from_args(a))
    except (ValueError, OSError) as e:
        p.error(str(e))
    return {"one": cmd_one, "run": cmd_run, "summary": cmd_summary, "make-sums": cmd_make_sums,
            "settings": cmd_settings}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
