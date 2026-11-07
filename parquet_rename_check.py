#!/usr/bin/env python3
"""Prove that a rename or drop by parquet_rename.py changed nothing else.

For each pair (OLD, NEW) and the renames and drops that were applied.  With
no drop:

  prefix    bytes [0, old footer start) of both files have the same SHA-256
            and NEW is that prefix followed by its footer, length and PAR1;
  footer    NEW's footer decodes and differs from OLD's footer, compared field
            by field, only in the top-level SchemaElement names, the first
            element of path_in_schema, and the ARROW:schema value, each mapped;
            the decoded Arrow schemas are equal with the names mapped,
            metadata included;

With drops, the footer and prefix checks are replaced by:

  footer    NEW's footer equals OLD's with the dropped columns removed and the
            names mapped (schema, column chunks, column_orders, sorting
            columns, row group sizes), compared field by field with the
            offset fields blanked;
  chunks    every kept column chunk, Bloom filter and column index has the
            same bytes (SHA-256) at its new offset; every page offset keeps
            its place relative to its chunk; each offset index is the old one
            with its page offsets moved by the chunk's move;

and for every pair:

  page index every offset index entry of NEW points at a data page header
            whose size matches, starting at the first data page, and the
            column index has one entry per page;
  arrow     pyarrow reads NEW: the Arrow schema is OLD's without the dropped
            columns and with the names mapped, the row group count and the
            rows of every row group are equal, and a SHA-256 of every kept
            column (read row group by row group, IPC-serialized) is equal;
  duckdb    DuckDB reads NEW: count(*), the column list, and count() and
            sum(hash()) of each renamed column equal OLD's;
  bloom     for every column with a Bloom filter, a value taken from its first
            row group is probed in both files with DuckDB parquet_bloom_probe:
            the answers are equal and the first row group is not excluded.

With replaced values (--set, --set-null or --values), the footer and chunks
checks of a drop run, and for each replaced column:

  footer    its ColumnChunks are left out of the field by field comparison and
            the row group sizes are the sums of the new chunk sizes;
  chunks    each new chunk has the old type, codec, path, key-values, value
            count, encoding family, data page version, page checksums,
            file_offset form, and the same Bloom filter, column index, offset
            index and statistics fields present, with statistics that give
            the new value;
  values    pyarrow reads the new value in every row of NEW, and OLD holds the
            expected old value (from --values);
  duckdb    count(), count(DISTINCT), min() and max() of the column give the
            new value in every row;
  bloom     a probe finds the new value in every row group of NEW, and no
            row group of NEW passes a probe of an old value.

The pyarrow column hashes and the DuckDB sums leave the replaced columns out.

One JSON line per pair on stdout.  The exit status is 1 when any pair has a
difference, 2 on a usage error.

Usage:
  parquet_rename_check.py --rename OLD=NEW [...] OLD.parquet NEW.parquet [...]
  parquet_rename_check.py --map mapping.json --pairs pairs.tsv
  parquet_rename_check.py --drop NAME [...] [--rename OLD=NEW ...] OLD NEW
  parquet_rename_check.py --set NAME=VALUE [--set-null NAME] [--drop ...] OLD NEW
  parquet_rename_check.py --values values.tsv --out-dir DIR [--drop ...]
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import struct
import sys
import time
from concurrent.futures import ThreadPoolExecutor

# `python -I` leaves the script's directory off sys.path; the tool sits next to it.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import parquet_rename as pr  # noqa: E402

CHUNK = 8 << 20


def sha256_range(path: str, length: int | None = None) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        left = length
        while left is None or left > 0:
            buf = fh.read(CHUNK if left is None else min(CHUNK, left))
            if not buf:
                break
            h.update(buf)
            if left is not None:
                left -= len(buf)
    return h.hexdigest()


# ----------------------------------------------------------------- footer --
def diff_trees(a, b, path=()):
    """Yield (path, a, b) for every place where two parsed Thrift trees differ.

    A struct step in the path is the field id, a container step the index.
    A difference in shape (field ids, types, header forms, sizes) is reported
    at the struct or container that holds it, with the value None.
    """
    if isinstance(a, list) and isinstance(b, list):
        sa = [(f.fid, f.ttype, f.short) for f in a]
        sb = [(f.fid, f.ttype, f.short) for f in b]
        if sa != sb:
            yield path, None, None
            return
        for fa, fb in zip(a, b):
            yield from diff_trees(fa.value, fb.value, path + (fa.fid,))
    elif isinstance(a, pr.TList) and isinstance(b, pr.TList):
        if (a.ttype, a.etype, a.short, len(a.items)) != (b.ttype, b.etype, b.short, len(b.items)):
            yield path, None, None
            return
        for i, (x, y) in enumerate(zip(a.items, b.items)):
            yield from diff_trees(x, y, path + (i,))
    elif isinstance(a, pr.TMap) and isinstance(b, pr.TMap):
        if (a.ktype, a.vtype, len(a.items)) != (b.ktype, b.vtype, len(b.items)):
            yield path, None, None
            return
        for i, ((ka, va), (kb, vb)) in enumerate(zip(a.items, b.items)):
            yield from diff_trees(ka, kb, path + (i, "key"))
            yield from diff_trees(va, vb, path + (i, "value"))
    elif type(a) is not type(b) or a != b:
        yield path, a, b


def show(v):
    if isinstance(v, bytes):
        return v[:60].decode("utf-8", "replace")
    return None if v is None else str(v)[:60]


def footer_problems(old_meta: list, new_meta: list, mapping: dict[str, str]) -> list[str]:
    enc = {o.encode("utf-8"): n.encode("utf-8") for o, n in mapping.items()}
    tops = {i: n for i, n in pr.top_level(pr.get(old_meta, pr.FMD_SCHEMA).items)}
    kvs = pr.kv_items(old_meta, pr.FMD_KV)
    arrow_kv = {i for i, kv in enumerate(kvs) if pr.get(kv, pr.KV_KEY) == pr.ARROW_SCHEMA_KEY}
    problems = []
    for path, a, b in diff_trees(old_meta, new_meta):
        p = path
        if len(p) == 3 and p[0] == pr.FMD_SCHEMA and p[2] == pr.SE_NAME and p[1] in tops:
            if a in enc and enc[a] == b:
                continue
        elif (len(p) == 7 and p[0] == pr.FMD_ROW_GROUPS and p[2] == pr.RG_COLUMNS
              and p[4:] == (pr.CC_META_DATA, pr.CMD_PATH_IN_SCHEMA, 0)):
            if a in enc and enc[a] == b:
                continue
        elif len(p) == 3 and p[0] == pr.FMD_KV and p[1] in arrow_kv and p[2] == pr.KV_VALUE:
            continue  # compared as Arrow schemas below
        problems.append(f"footer differs at {'.'.join(map(str, p))}: "
                        f"{show(a)!r} -> {show(b)!r}")
    new_tops = {i: n for i, n in pr.top_level(pr.get(new_meta, pr.FMD_SCHEMA).items)}
    for i, name in tops.items():
        want = mapping.get(name, name)
        if new_tops.get(i) != want:
            problems.append(f"top-level column {i}: {name!r} should be {want!r}, "
                            f"is {new_tops.get(i)!r}")
    new_kvs = pr.kv_items(new_meta, pr.FMD_KV)
    for i in arrow_kv:
        if i >= len(new_kvs) or pr.get(new_kvs[i], pr.KV_KEY) != pr.ARROW_SCHEMA_KEY:
            problems.append("ARROW:schema key-value is missing in the new footer")
            continue
        problems += arrow_schema_problems(pr.get(kvs[i], pr.KV_VALUE),
                                          pr.get(new_kvs[i], pr.KV_VALUE), mapping)
    return problems


def arrow_schema_problems(old_value: bytes, new_value: bytes, mapping,
                          drops=()) -> list[str]:
    import pyarrow as pa

    try:
        old = pa.ipc.read_schema(pa.py_buffer(base64.b64decode(old_value)))
        new = pa.ipc.read_schema(pa.py_buffer(base64.b64decode(new_value, validate=True)))
    except Exception as e:  # noqa: BLE001 - any decode failure is a finding
        return [f"ARROW:schema does not decode: {e}"]
    want = pr.renamed_arrow_schema(old, mapping, drops)
    if not new.equals(want, check_metadata=True):
        return ["ARROW:schema is not the old schema with the names mapped"]
    return []


# ------------------------------------------------------------------- data --
def open_for_hashing(path: str):
    """A ParquetFile that reads string and binary columns as dictionaries.

    Dictionary-encoded text then stays as stored instead of being expanded to
    one string per row, which is what makes a full read affordable.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    text = (pa.types.is_string, pa.types.is_large_string, pa.types.is_binary,
            pa.types.is_large_binary)
    pf = pq.ParquetFile(path)
    md = pf.metadata
    delta = set()  # pyarrow cannot read DELTA_* byte arrays as dictionaries
    for rg in range(md.num_row_groups):
        for c in range(md.num_columns):
            col = md.row_group(rg).column(c)
            if any(e.startswith("DELTA_") for e in col.encodings):
                delta.add(col.path_in_schema)
    cols = [f.name for f in pf.schema_arrow
            if any(t(f.type) for t in text) and f.name not in delta]
    return pq.ParquetFile(path, read_dictionary=cols)


def column_hashes(pf, names: list[str] | None = None) -> dict[str, str]:
    """SHA-256 per column over all row groups, one row group in memory at a time.

    Each column chunk is hashed as an IPC-serialized record batch under a fixed
    field name, so equal data under different names gives equal hashes.
    """
    import pyarrow as pa

    names = pf.schema_arrow.names if names is None else names
    hashers = [hashlib.sha256() for _ in names]
    with ThreadPoolExecutor(max_workers=os.cpu_count() or 1) as pool:
        for rg in range(pf.metadata.num_row_groups):
            tbl = pf.read_row_group(rg, columns=names, use_threads=True)

            def update(i, tbl=tbl):
                for chunk in tbl.column(i).chunks:
                    hashers[i].update(pa.RecordBatch.from_arrays([chunk], names=["c"]).serialize())

            list(pool.map(update, range(len(names))))
    return {n: h.hexdigest() for n, h in zip(names, hashers)}


def arrow_problems(old: str, new: str, mapping, rep: dict, drops=(), values=()) -> list[str]:
    import pyarrow.parquet as pq

    po, pn = pq.ParquetFile(old), pq.ParquetFile(new)
    mo, mn = po.metadata, pn.metadata
    problems = []
    rep["row_groups"] = mn.num_row_groups
    rep["rows"] = mn.num_rows
    if mo.num_row_groups != mn.num_row_groups:
        return [f"row groups {mo.num_row_groups} -> {mn.num_row_groups}"]
    if mo.num_rows != mn.num_rows:
        problems.append(f"rows {mo.num_rows} -> {mn.num_rows}")
    for i in range(mo.num_row_groups):
        if mo.row_group(i).num_rows != mn.row_group(i).num_rows:
            problems.append(f"row group {i} rows differ")
    want = pr.renamed_arrow_schema(po.schema_arrow, mapping, drops)
    if not pn.schema_arrow.equals(want, check_metadata=True):
        problems.append("pyarrow schema is not the old schema with the names mapped")
        return problems
    kept = [n for n in po.schema_arrow.names if n not in drops and n not in values]
    ho = column_hashes(open_for_hashing(old), kept)
    hn = column_hashes(open_for_hashing(new), [mapping.get(n, n) for n in kept])
    rep["column_sha256"] = hn
    for name, digest in ho.items():
        if hn.get(mapping.get(name, name)) != digest:
            problems.append(f"column {name!r} data hash differs")
    return problems


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def duckdb_problems(old: str, new: str, mapping, rep: dict, drops=(), values=None) -> list[str]:
    import duckdb

    con = duckdb.connect()
    problems = []
    try:
        no = con.execute("SELECT count(*) FROM read_parquet(?)", [old]).fetchone()[0]
        nn = con.execute("SELECT count(*) FROM read_parquet(?)", [new]).fetchone()[0]
        rep["duckdb_count"] = nn
        if no != nn:
            problems.append(f"DuckDB count(*) {no} -> {nn}")
        names = [d[0] for d in con.execute("SELECT * FROM read_parquet(?) LIMIT 0",
                                           [old]).description]
        got = [d[0] for d in con.execute("SELECT * FROM read_parquet(?) LIMIT 0",
                                         [new]).description]
        if got != [mapping.get(n, n) for n in names if n not in drops]:
            problems.append("DuckDB columns are not the old ones, dropped and renamed")
        for o, n in mapping.items():
            if o in (values or {}):
                continue
            a = con.execute(f"SELECT count({q(o)}), sum(hash({q(o)})) FROM read_parquet(?)",
                            [old]).fetchone()
            b = con.execute(f"SELECT count({q(n)}), sum(hash({q(n)})) FROM read_parquet(?)",
                            [new]).fetchone()
            if a != b:
                problems.append(f"DuckDB column {o!r} -> {n!r}: {a} != {b}")
        for o, value in (values or {}).items():
            n = mapping.get(o, o)
            got = con.execute(f"SELECT count({q(n)}), count(DISTINCT {q(n)}), min({q(n)}), "
                              f"max({q(n)}) FROM read_parquet(?)", [new]).fetchone()
            want = (0, 0, None, None) if value is None else (nn, 1, value, value)
            if got != want:
                problems.append(f"DuckDB column {n!r} does not hold the new value in every row")
    finally:
        con.close()
    return problems


def bloom_columns(meta: list) -> list[str]:
    """Top-level leaf columns with a Bloom filter in some row group."""
    out = []
    for rg in pr.kv_items(meta, pr.FMD_ROW_GROUPS):
        for cc in pr.get(rg, pr.RG_COLUMNS).items:
            cmd = pr.get(cc, pr.CC_META_DATA)
            path = pr.get(cmd, pr.CMD_PATH_IN_SCHEMA).items
            if pr.has(cmd, pr.CMD_BLOOM_FILTER_OFFSET) and len(path) == 1:
                name = path[0].decode("utf-8")
                if name not in out:
                    out.append(name)
    return out


def bloom_problems(old: str, new: str, old_meta: list, mapping, rep: dict,
                   drops=(), values=None) -> list[str]:
    import duckdb
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    values = values or {}
    cols = [c for c in bloom_columns(old_meta) if c not in drops]
    rep["bloom_columns"] = [mapping.get(c, c) for c in cols]
    if not cols:
        return []
    replaced = [c for c in cols if c in values]
    cols = [c for c in cols if c not in values]
    pf = pq.ParquetFile(old)
    con = duckdb.connect()
    problems = []
    sql = ("SELECT row_group_id, bloom_filter_excludes FROM parquet_bloom_probe(?, ?, ?) "
           "ORDER BY row_group_id")
    try:
        for c in cols:
            arr = pc.drop_null(pf.read_row_group(0, columns=[c]).column(0))
            if len(arr) == 0:
                problems.append(f"Bloom column {c!r}: no value in row group 0 to probe")
                continue
            value = arr[0].as_py()
            a = con.execute(sql, [old, c, value]).fetchall()
            b = con.execute(sql, [new, mapping.get(c, c), value]).fetchall()
            if a != b:
                problems.append(f"Bloom probe of {c!r} differs: {a} != {b}")
            elif not b or b[0] != (0, False):
                problems.append(f"Bloom filter of {c!r} excludes a value of row group 0")
        for c in replaced:
            problems += replaced_bloom_problems(con, sql, old, new, c, mapping.get(c, c),
                                                values[c], rep)
    finally:
        con.close()
    return problems


def old_probe_values(old: str, name: str, value) -> list:
    """Up to three old values of a column, other than the new value and null."""
    return sorted(v for v in pr.old_values(old, name) if v is not None and v != value)[:3]


def replaced_bloom_problems(con, sql: str, old: str, new: str, o: str, n: str, value,
                            rep: dict) -> list[str]:
    problems = []
    if value is not None:
        got = con.execute(sql, [new, n, value]).fetchall()
        if not got or any(excl for _, excl in got):
            problems.append(f"Bloom filter of {n!r} does not find the new value in every "
                            "row group")
    probes = old_probe_values(old, o, value)
    for v in probes:
        got = con.execute(sql, [new, n, v]).fetchall()
        if not got or not all(excl for _, excl in got):
            problems.append(f"Bloom filter of {n!r} still finds an old value")
    rep.setdefault("bloom_old_values_probed", {})[n] = len(probes)
    return problems


def value_problems(old: str, new: str, mapping, values: dict, expect: dict,
                   force_multi: bool, rep: dict) -> list[str]:
    """NEW holds the new value in every row; OLD held the expected old value."""
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    problems = []
    pf = pq.ParquetFile(new)
    for o, value in values.items():
        n = mapping.get(o, o)
        for i in range(pf.metadata.num_row_groups):
            col = pf.read_row_group(i, columns=[n]).column(0)
            if value is None:
                ok = col.null_count == len(col)
            else:
                ok = col.null_count == 0 and pc.all(pc.equal(col, value)).as_py() is not False
            if not ok:
                problems.append(f"row group {i} of {n!r} does not hold the new value in every row")
                break
        if o in expect:
            seen = pr.old_values(old, o)
            if (expect[o] not in seen) if force_multi else (seen != {expect[o]}):
                problems.append(f"OLD column {o!r} does not hold the expected old value")
    rep["values_checked"] = len(values)
    return problems


# ------------------------------------------------------------ column drops --
MASKED = {
    "rg": (pr.RG_FILE_OFFSET,),
    "cc": (pr.CC_FILE_OFFSET, pr.CC_OFFSET_INDEX_OFFSET, pr.CC_OFFSET_INDEX_LENGTH,
           pr.CC_COLUMN_INDEX_OFFSET),
    "cmd": (pr.CMD_DATA_PAGE_OFFSET, pr.CMD_INDEX_PAGE_OFFSET, pr.CMD_DICTIONARY_PAGE_OFFSET,
            pr.CMD_BLOOM_FILTER_OFFSET),
}


def mask_offsets(meta: list) -> list:
    """Blank the offset fields (they are proved by content instead)."""
    for rg in pr.kv_items(meta, pr.FMD_ROW_GROUPS):
        for f in rg:
            if f.fid in MASKED["rg"]:
                f.value = None
        for cc in pr.get(rg, pr.RG_COLUMNS).items:
            for f in cc:
                if f.fid in MASKED["cc"]:
                    f.value = None
            for f in pr.get(cc, pr.CC_META_DATA) or []:
                if f.fid in MASKED["cmd"]:
                    f.value = None
    return meta


def expected_footer(old_footer: bytes, mapping, drops) -> tuple[list, set]:
    """The old footer with the columns dropped and the names mapped.

    Written apart from the tool on purpose: the schema, column lists, sizes,
    sort columns and column orders the new footer must have.  Offsets are
    left as they were; the caller blanks them in both footers.
    """
    meta = pr.parse_footer(old_footer)
    schema_list = pr.get(meta, pr.FMD_SCHEMA)
    spans = pr.schema_spans(schema_list.items)
    nleaves, dropped, new_index = pr.leaf_plan(spans, drops)
    enc = {o.encode("utf-8"): n.encode("utf-8") for o, n in mapping.items()}

    elems = [schema_list.items[0]]
    for i, end, name, _ in spans:
        if name in drops:
            continue
        if name in mapping:
            pr.set_value(schema_list.items[i], pr.SE_NAME, mapping[name].encode("utf-8"))
        elems += schema_list.items[i:end]
    pr.set_value(elems[0], pr.SE_NUM_CHILDREN, len(spans) - len(drops))
    pr.canonical_list(schema_list, elems)

    for rg in pr.kv_items(meta, pr.FMD_ROW_GROUPS):
        cols = pr.get(rg, pr.RG_COLUMNS)
        kept = [cc for c, cc in enumerate(cols.items) if c not in dropped]
        for fid, size in ((pr.RG_TOTAL_BYTE_SIZE, pr.CMD_TOTAL_UNCOMPRESSED_SIZE),
                          (pr.RG_TOTAL_COMPRESSED_SIZE, pr.CMD_TOTAL_COMPRESSED_SIZE)):
            if pr.has(rg, fid):
                pr.set_value(rg, fid, sum(pr.get(pr.get(cc, pr.CC_META_DATA), size)
                                          for cc in kept))
        for sc in pr.kv_items(rg, pr.RG_SORTING_COLUMNS):
            idx = pr.get(sc, pr.SC_COLUMN_IDX)
            pr.set_value(sc, pr.SC_COLUMN_IDX, new_index.get(idx, -1))
        for cc in kept:
            path = pr.get(pr.get(cc, pr.CC_META_DATA), pr.CMD_PATH_IN_SCHEMA).items
            if path[0] in enc:
                path[0] = enc[path[0]]
        pr.canonical_list(cols, kept)
    orders = pr.get(meta, pr.FMD_COLUMN_ORDERS)
    if orders is not None:
        pr.canonical_list(orders, [o for c, o in enumerate(orders.items) if c not in dropped])
    return meta, dropped


def sha256_at(fd: int, off: int, n: int) -> str:
    h = hashlib.sha256()
    while n > 0:
        buf = os.pread(fd, min(CHUNK, n), off)
        if not buf:
            break
        h.update(buf)
        off += len(buf)
        n -= len(buf)
    return h.hexdigest()


def relative(old_s, new_s, fid, o0, n0) -> bool:
    """An offset field points at the same place relative to its chunk start."""
    a, b = pr.get(old_s, fid), pr.get(new_s, fid)
    if a is None or b is None or a == 0 or b == 0:
        return a == b
    return a - o0 == b - n0


def replaced_leaves(meta: list, names) -> dict[int, str]:
    """Leaf index -> name of the top-level leaf columns in names."""
    out, leaf = {}, 0
    for _, _, name, n in pr.schema_spans(pr.get(meta, pr.FMD_SCHEMA).items):
        if name in names:
            out[leaf] = name
        leaf += n
    return out


def mask_replaced(meta: list, leaves) -> None:
    """Blank the ColumnChunks of the replaced leaves and the row group sizes."""
    for rg in pr.kv_items(meta, pr.FMD_ROW_GROUPS):
        for f in rg:
            if f.fid in (pr.RG_TOTAL_BYTE_SIZE, pr.RG_TOTAL_COMPRESSED_SIZE):
                f.value = None
        items = pr.get(rg, pr.RG_COLUMNS).items
        for c in leaves:
            if c < len(items):
                items[c] = []


def row_group_size_problems(rg: list, r: int) -> list[str]:
    cmds = [pr.get(cc, pr.CC_META_DATA) for cc in pr.get(rg, pr.RG_COLUMNS).items]
    problems = []
    for fid, size in ((pr.RG_TOTAL_BYTE_SIZE, pr.CMD_TOTAL_UNCOMPRESSED_SIZE),
                      (pr.RG_TOTAL_COMPRESSED_SIZE, pr.CMD_TOTAL_COMPRESSED_SIZE)):
        if pr.has(rg, fid) and pr.get(rg, fid) != sum(pr.get(m, size) for m in cmds):
            problems.append(f"row group {r}: field {fid} is not the sum of its columns")
    return problems


def present(fields: list, fids) -> list[bool]:
    return [pr.has(fields, f) for f in fids]


def replaced_chunk_problems(fo: int, fn: int, occ: list, ncc: list, rows: int, value,
                            name: bytes, where: str) -> list[str]:
    """A replaced chunk keeps the old chunk's form and holds the new value."""
    problems = []
    ocmd, ncmd = pr.get(occ, pr.CC_META_DATA), pr.get(ncc, pr.CC_META_DATA)
    for fid, what in ((pr.CMD_TYPE, "type"), (pr.CMD_CODEC, "codec"), (pr.CMD_KV, "key-values")):
        if any(True for _ in diff_trees(pr.get(ocmd, fid), pr.get(ncmd, fid))):
            problems.append(f"{where}: {what} differs")
    if pr.get(ncmd, pr.CMD_PATH_IN_SCHEMA).items != [name]:
        problems.append(f"{where}: path_in_schema is not the column name")
    if pr.get(ncmd, pr.CMD_NUM_VALUES) != rows:
        problems.append(f"{where}: num_values is not the row count")
    if pr.encoding_family(ocmd)[0] != pr.encoding_family(ncmd)[0]:
        problems.append(f"{where}: encoding family differs")
    if pr.page_shape(fo, ocmd)[:2] != pr.page_shape(fn, ncmd)[:2]:
        problems.append(f"{where}: data page version or page checksums differ")
    cmd_fids = (pr.CMD_BLOOM_FILTER_OFFSET, pr.CMD_BLOOM_FILTER_LENGTH, pr.CMD_STATISTICS,
                pr.CMD_ENCODING_STATS, pr.CMD_SIZE_STATISTICS)
    cc_fids = (pr.CC_COLUMN_INDEX_OFFSET, pr.CC_OFFSET_INDEX_OFFSET)
    if present(ocmd, cmd_fids) != present(ncmd, cmd_fids) or \
            present(occ, cc_fids) != present(ncc, cc_fids):
        problems.append(f"{where}: Bloom filter, page index or statistics presence differs")
    if (pr.get(occ, pr.CC_FILE_OFFSET) == 0) != (pr.get(ncc, pr.CC_FILE_OFFSET) == 0):
        problems.append(f"{where}: file_offset form differs")
    ost, nst = pr.get(ocmd, pr.CMD_STATISTICS), pr.get(ncmd, pr.CMD_STATISTICS)
    if ost is not None and nst is not None:
        v = None if value is None else value.encode("utf-8")
        want = {pr.ST_NULL_COUNT: rows if v is None else 0,
                pr.ST_DISTINCT_COUNT: 0 if v is None else 1}
        for fid in (pr.ST_MAX, pr.ST_MIN, pr.ST_MAX_VALUE, pr.ST_MIN_VALUE):
            want[fid] = v
        for fid, w in want.items():
            if pr.has(nst, fid) and pr.get(nst, fid) != w:
                problems.append(f"{where}: statistics field {fid} does not give the new value")
        if v is not None and [f.fid for f in nst] != [f.fid for f in ost]:
            problems.append(f"{where}: statistics fields differ")
    return problems


def drop_footer_problems(old: str, new: str, old_footer: bytes, new_meta: list,
                         mapping, drops, rep: dict, values=None) -> list[str]:
    problems = []
    values = values or {}
    want, dropped = expected_footer(old_footer, mapping, drops)
    got = pr.parse_footer(pr.encode(new_meta))
    leaves = replaced_leaves(want, {mapping.get(o, o) for o in values})
    back = {mapping.get(o, o): o for o in values}
    sizes = []
    if values:
        for r, rg in enumerate(pr.kv_items(got, pr.FMD_ROW_GROUPS)):
            sizes += row_group_size_problems(rg, r)
        mask_replaced(want, leaves)
        mask_replaced(got, leaves)
    kvs = pr.kv_items(want, pr.FMD_KV)
    arrow_kv = {i for i, kv in enumerate(kvs) if pr.get(kv, pr.KV_KEY) == pr.ARROW_SCHEMA_KEY}
    for path, a, b in diff_trees(mask_offsets(want), mask_offsets(got)):
        if len(path) == 3 and path[0] == pr.FMD_KV and path[1] in arrow_kv \
                and path[2] == pr.KV_VALUE:
            continue
        problems.append(f"footer differs at {'.'.join(map(str, path))}: "
                        f"{show(a)!r} -> {show(b)!r}")
    new_kvs = pr.kv_items(new_meta, pr.FMD_KV)
    for i in arrow_kv:
        if i < len(new_kvs) and pr.get(new_kvs[i], pr.KV_KEY) == pr.ARROW_SCHEMA_KEY:
            problems += arrow_schema_problems(pr.get(kvs[i], pr.KV_VALUE),
                                              pr.get(new_kvs[i], pr.KV_VALUE), mapping, drops)
    if problems:
        return problems

    old_meta = pr.parse_footer(old_footer)
    fo, fn = os.open(old, os.O_RDONLY), os.open(new, os.O_RDONLY)
    same = reencoded = 0
    try:
        for r, (org, nrg) in enumerate(zip(pr.kv_items(old_meta, pr.FMD_ROW_GROUPS),
                                           pr.kv_items(new_meta, pr.FMD_ROW_GROUPS))):
            ocols = [cc for c, cc in enumerate(pr.get(org, pr.RG_COLUMNS).items)
                     if c not in dropped]
            starts = []
            for c, (occ, ncc) in enumerate(zip(ocols, pr.get(nrg, pr.RG_COLUMNS).items)):
                where = f"row group {r} column {c}"
                ocmd, ncmd = pr.get(occ, pr.CC_META_DATA), pr.get(ncc, pr.CC_META_DATA)
                o0, n = pr.chunk_span(ocmd)
                n0, _ = pr.chunk_span(ncmd)
                starts.append(n0)
                if c in leaves:
                    problems += replaced_chunk_problems(
                        fo, fn, occ, ncc, pr.get(org, pr.RG_NUM_ROWS), values[back[leaves[c]]],
                        leaves[c].encode("utf-8"), where)
                    reencoded += 1
                    continue
                if sha256_at(fo, o0, n) != sha256_at(fn, n0, n):
                    problems.append(f"{where}: column chunk bytes differ")
                    continue
                same += 1
                for fid in (pr.CMD_DATA_PAGE_OFFSET, pr.CMD_INDEX_PAGE_OFFSET,
                            pr.CMD_DICTIONARY_PAGE_OFFSET):
                    if not relative(ocmd, ncmd, fid, o0, n0):
                        problems.append(f"{where}: page offset field {fid} moved")
                if not relative(occ, ncc, pr.CC_FILE_OFFSET, o0, n0):
                    problems.append(f"{where}: file_offset moved")
                if pr.has(ocmd, pr.CMD_BLOOM_FILTER_OFFSET):
                    ob = pr.get(ocmd, pr.CMD_BLOOM_FILTER_OFFSET)
                    nb = pr.get(ncmd, pr.CMD_BLOOM_FILTER_OFFSET)
                    bl = (pr.get(ocmd, pr.CMD_BLOOM_FILTER_LENGTH)
                          if pr.has(ocmd, pr.CMD_BLOOM_FILTER_LENGTH) else pr.bloom_length(fo, ob))
                    if sha256_at(fo, ob, bl) != sha256_at(fn, nb, bl):
                        problems.append(f"{where}: Bloom filter bytes differ")
                if pr.has(occ, pr.CC_COLUMN_INDEX_OFFSET):
                    ln = pr.get(occ, pr.CC_COLUMN_INDEX_LENGTH)
                    if sha256_at(fo, pr.get(occ, pr.CC_COLUMN_INDEX_OFFSET), ln) != \
                            sha256_at(fn, pr.get(ncc, pr.CC_COLUMN_INDEX_OFFSET), ln):
                        problems.append(f"{where}: column index bytes differ")
                if pr.has(occ, pr.CC_OFFSET_INDEX_OFFSET):
                    ooi = pr.parse_footer(os.pread(fo, pr.get(occ, pr.CC_OFFSET_INDEX_LENGTH),
                                                   pr.get(occ, pr.CC_OFFSET_INDEX_OFFSET)))
                    noi = pr.parse_footer(os.pread(fn, pr.get(ncc, pr.CC_OFFSET_INDEX_LENGTH),
                                                   pr.get(ncc, pr.CC_OFFSET_INDEX_OFFSET)))
                    for loc in pr.kv_items(ooi, pr.OI_PAGE_LOCATIONS):
                        pr.set_value(loc, pr.PL_OFFSET, pr.get(loc, pr.PL_OFFSET) - o0 + n0)
                    if any(True for _ in diff_trees(ooi, noi)):
                        problems.append(f"{where}: offset index is not the old one moved")
            if pr.has(nrg, pr.RG_FILE_OFFSET) and starts \
                    and pr.get(nrg, pr.RG_FILE_OFFSET) != min(starts):
                problems.append(f"row group {r}: file_offset is not its first chunk")
    finally:
        os.close(fo)
        os.close(fn)
    rep["column_chunks_identical"] = same
    if values:
        rep["column_chunks_reencoded"] = reencoded
    return problems + sizes


def page_index_problems(path: str, meta: list, rep: dict) -> list[str]:
    """Every offset index entry points at a page header of the stated size."""
    problems, pages = [], 0
    fd = os.open(path, os.O_RDONLY)
    try:
        for r, rg in enumerate(pr.kv_items(meta, pr.FMD_ROW_GROUPS)):
            for c, cc in enumerate(pr.get(rg, pr.RG_COLUMNS).items):
                if not pr.has(cc, pr.CC_OFFSET_INDEX_OFFSET):
                    continue
                where = f"row group {r} column {c}"
                cmd = pr.get(cc, pr.CC_META_DATA)
                lo, n = pr.chunk_span(cmd)
                oi = pr.parse_footer(os.pread(fd, pr.get(cc, pr.CC_OFFSET_INDEX_LENGTH),
                                              pr.get(cc, pr.CC_OFFSET_INDEX_OFFSET)))
                locs = pr.kv_items(oi, pr.OI_PAGE_LOCATIONS)
                if not locs or pr.get(locs[0], pr.PL_OFFSET) != pr.get(cmd, pr.CMD_DATA_PAGE_OFFSET):
                    problems.append(f"{where}: offset index does not start at the first data page")
                    continue
                for loc in locs:
                    off = pr.get(loc, pr.PL_OFFSET)
                    size = pr.get(loc, pr.PL_COMPRESSED_PAGE_SIZE)
                    if not lo <= off < lo + n:
                        problems.append(f"{where}: page location {off} is outside the chunk")
                        break
                    header, hlen = pr.struct_at(fd, off)
                    if pr.get(header, pr.PH_TYPE) not in (pr.PAGE_DATA, pr.PAGE_DATA_V2) or \
                            hlen + pr.get(header, pr.PH_COMPRESSED_PAGE_SIZE) != size:
                        problems.append(f"{where}: no data page of {size} bytes at {off}")
                        break
                    pages += 1
                if pr.has(cc, pr.CC_COLUMN_INDEX_OFFSET):
                    ci = pr.parse_footer(os.pread(fd, pr.get(cc, pr.CC_COLUMN_INDEX_LENGTH),
                                                  pr.get(cc, pr.CC_COLUMN_INDEX_OFFSET)))
                    if len(pr.get(ci, pr.CI_NULL_PAGES).items) != len(locs):
                        problems.append(f"{where}: column index and offset index page counts")
    except (pr.RenameError, ValueError) as e:
        problems.append(f"page index does not decode: {e}")
    finally:
        os.close(fd)
    rep["page_index_pages"] = pages
    return problems


# ------------------------------------------------------------------ check --
def check_pair(old: str, new: str, mapping: dict[str, str], drops=(), values=None,
               expect=None, force_multi: bool = False) -> dict:
    t0 = time.monotonic()
    drops = list(drops)
    values = dict(values or {})
    rep: dict = dict(old=old, new=new, renamed=dict(mapping), dropped=drops)
    if values:
        rep["replaced"] = values
    problems: list[str] = []

    def stage(name, fn, *args):
        try:
            problems.extend(fn(*args))
        except Exception as e:  # noqa: BLE001 - every failure is a finding
            problems.append(f"{name}: {type(e).__name__}: {e}")

    try:
        _, ostart, ofooter = pr.read_footer(old)
        nsize, nstart, nfooter = pr.read_footer(new)
        old_meta = pr.parse_footer(ofooter)
        new_meta = pr.parse_footer(nfooter)
    except (pr.RenameError, ValueError) as e:
        rep.update(ok=False, problems=[f"footer: {e}"])
        return rep

    rep["old_size"], rep["new_size"] = os.path.getsize(old), nsize
    if drops or values:
        stage("footer", drop_footer_problems, old, new, ofooter, new_meta, mapping, drops, rep,
              values)
    elif nstart != ostart:
        problems.append(f"footer start {ostart} -> {nstart}")
    else:
        rep["prefix_len"] = ostart
        rep["prefix_sha256"] = ho = sha256_range(old, ostart)
        if sha256_range(new, nstart) != ho:
            problems.append("bytes before the footer differ")
    if nsize != nstart + len(nfooter) + 8:
        problems.append("new file size does not match its footer")  # pragma: no cover
    with open(new, "rb") as fh:
        fh.seek(nsize - 8)
        if fh.read(8) != struct.pack("<I", len(nfooter)) + pr.MAGIC:
            problems.append("new tail is not footer length + PAR1")  # pragma: no cover
    if not drops and not values:
        stage("footer", footer_problems, old_meta, new_meta, mapping)
    stage("page index", page_index_problems, new, new_meta, rep)
    stage("arrow", arrow_problems, old, new, mapping, rep, drops, values)
    if values:
        stage("values", value_problems, old, new, mapping, values, expect or {}, force_multi,
              rep)
    stage("duckdb", duckdb_problems, old, new, mapping, rep, drops, values)
    stage("bloom", bloom_problems, old, new, old_meta, mapping, rep, drops, values)
    rep["ok"] = not problems
    rep["problems"] = problems
    rep["seconds"] = round(time.monotonic() - t0, 3)
    return rep


def read_pairs(path: str) -> list[tuple[str, str]]:
    pairs = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) != 2:
                raise pr.RenameError(f"{path}: expected OLD<TAB>NEW, got {line!r}")
            pairs.append((parts[0], parts[1]))
    return pairs


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("files", nargs="*", metavar="OLD NEW")
    pr.mapping_args(ap)
    pr.value_args(ap)
    ap.add_argument("--pairs", metavar="TSV", help="a file of OLD<TAB>NEW lines")
    a = ap.parse_args(argv)
    try:
        mapping = pr.parse_mapping(a.rename, a.map)
        if len(a.files) % 2:
            raise pr.RenameError("files must come in OLD NEW pairs")
        values = pr.parse_values(a.set, a.set_null)
        pairs = [(o, n, values, {}) for o, n in zip(a.files[::2], a.files[1::2])]
        if a.pairs:
            pairs += [(o, n, values, {}) for o, n in read_pairs(a.pairs)]
        if a.values:
            if pairs or values:
                raise pr.RenameError("--values takes no pairs, --set or --set-null")
            a.src = a.dst = None
            pairs = pr.value_jobs(a)
        if not pairs:
            raise pr.RenameError("no pairs to check")
    except pr.RenameError as e:
        print(f"parquet_rename_check: {e}", file=sys.stderr)
        return 2
    bad = 0
    for old, new, vals, expect in pairs:
        rep = check_pair(old, new, mapping, a.drop, vals, expect, a.force_multi)
        bad += not rep["ok"]
        print(json.dumps(rep), flush=True)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
