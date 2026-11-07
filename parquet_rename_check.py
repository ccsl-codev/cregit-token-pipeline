#!/usr/bin/env python3
"""Prove that a footer-only rename by parquet_rename.py changed nothing else.

For each pair (OLD, NEW) and the mapping that was applied:

  prefix    bytes [0, old footer start) of both files have the same SHA-256
            and NEW is that prefix followed by its footer, length and PAR1;
  footer    NEW's footer decodes and differs from OLD's footer, compared field
            by field, only in the top-level SchemaElement names, the first
            element of path_in_schema, and the ARROW:schema value, each mapped;
            the decoded Arrow schemas are equal with the names mapped,
            metadata included;
  arrow     pyarrow reads NEW: the Arrow schema is OLD's with the names
            mapped, the row group count and the rows of every row group are
            equal, and a SHA-256 of every column (read row group by row
            group, IPC-serialized) is equal;
  duckdb    DuckDB reads NEW: count(*), and count() and sum(hash()) of each
            renamed column equal OLD's;
  bloom     for every column with a Bloom filter, a value taken from its first
            row group is probed in both files with DuckDB parquet_bloom_probe:
            the answers are equal and the first row group is not excluded.

One JSON line per pair on stdout.  The exit status is 1 when any pair has a
difference, 2 on a usage error.

Usage:
  parquet_rename_check.py --rename OLD=NEW [...] OLD.parquet NEW.parquet [...]
  parquet_rename_check.py --map mapping.json --pairs pairs.tsv
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


def arrow_schema_problems(old_value: bytes, new_value: bytes, mapping) -> list[str]:
    import pyarrow as pa

    try:
        old = pa.ipc.read_schema(pa.py_buffer(base64.b64decode(old_value)))
        new = pa.ipc.read_schema(pa.py_buffer(base64.b64decode(new_value, validate=True)))
    except Exception as e:  # noqa: BLE001 - any decode failure is a finding
        return [f"ARROW:schema does not decode: {e}"]
    want = pr.renamed_arrow_schema(old, mapping)
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
    cols = [f.name for f in pq.ParquetFile(path).schema_arrow if any(t(f.type) for t in text)]
    return pq.ParquetFile(path, read_dictionary=cols)


def column_hashes(pf) -> dict[str, str]:
    """SHA-256 per column over all row groups, one row group in memory at a time.

    Each column chunk is hashed as an IPC-serialized record batch under a fixed
    field name, so equal data under different names gives equal hashes.
    """
    import pyarrow as pa

    names = pf.schema_arrow.names
    hashers = [hashlib.sha256() for _ in names]
    with ThreadPoolExecutor(max_workers=os.cpu_count() or 1) as pool:
        for rg in range(pf.metadata.num_row_groups):
            tbl = pf.read_row_group(rg, use_threads=True)

            def update(i, tbl=tbl):
                for chunk in tbl.column(i).chunks:
                    hashers[i].update(pa.RecordBatch.from_arrays([chunk], names=["c"]).serialize())

            list(pool.map(update, range(len(names))))
    return {n: h.hexdigest() for n, h in zip(names, hashers)}


def arrow_problems(old: str, new: str, mapping, rep: dict) -> list[str]:
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
    want = pr.renamed_arrow_schema(po.schema_arrow, mapping)
    if not pn.schema_arrow.equals(want, check_metadata=True):
        problems.append("pyarrow schema is not the old schema with the names mapped")
        return problems
    ho = column_hashes(open_for_hashing(old))
    hn = column_hashes(open_for_hashing(new))
    rep["column_sha256"] = hn
    for name, digest in ho.items():
        if hn.get(mapping.get(name, name)) != digest:
            problems.append(f"column {name!r} data hash differs")
    return problems


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def duckdb_problems(old: str, new: str, mapping, rep: dict) -> list[str]:
    import duckdb

    con = duckdb.connect()
    problems = []
    try:
        no = con.execute("SELECT count(*) FROM read_parquet(?)", [old]).fetchone()[0]
        nn = con.execute("SELECT count(*) FROM read_parquet(?)", [new]).fetchone()[0]
        rep["duckdb_count"] = nn
        if no != nn:
            problems.append(f"DuckDB count(*) {no} -> {nn}")
        for o, n in mapping.items():
            a = con.execute(f"SELECT count({q(o)}), sum(hash({q(o)})) FROM read_parquet(?)",
                            [old]).fetchone()
            b = con.execute(f"SELECT count({q(n)}), sum(hash({q(n)})) FROM read_parquet(?)",
                            [new]).fetchone()
            if a != b:
                problems.append(f"DuckDB column {o!r} -> {n!r}: {a} != {b}")
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


def bloom_problems(old: str, new: str, old_meta: list, mapping, rep: dict) -> list[str]:
    import duckdb
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    cols = bloom_columns(old_meta)
    rep["bloom_columns"] = [mapping.get(c, c) for c in cols]
    if not cols:
        return []
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
    finally:
        con.close()
    return problems


# ------------------------------------------------------------------ check --
def check_pair(old: str, new: str, mapping: dict[str, str]) -> dict:
    t0 = time.monotonic()
    rep: dict = dict(old=old, new=new, renamed=dict(mapping))
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

    rep["prefix_len"] = ostart
    if nstart != ostart:
        problems.append(f"footer start {ostart} -> {nstart}")
    else:
        rep["prefix_sha256"] = ho = sha256_range(old, ostart)
        if sha256_range(new, nstart) != ho:
            problems.append("bytes before the footer differ")
    if nsize != nstart + len(nfooter) + 8:
        problems.append("new file size does not match its footer")  # pragma: no cover
    with open(new, "rb") as fh:
        fh.seek(nsize - 8)
        if fh.read(8) != struct.pack("<I", len(nfooter)) + pr.MAGIC:
            problems.append("new tail is not footer length + PAR1")  # pragma: no cover
    stage("footer", footer_problems, old_meta, new_meta, mapping)
    stage("arrow", arrow_problems, old, new, mapping, rep)
    stage("duckdb", duckdb_problems, old, new, mapping, rep)
    stage("bloom", bloom_problems, old, new, old_meta, mapping, rep)
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
    ap.add_argument("--pairs", metavar="TSV", help="a file of OLD<TAB>NEW lines")
    a = ap.parse_args(argv)
    try:
        mapping = pr.parse_mapping(a.rename, a.map)
        if len(a.files) % 2:
            raise pr.RenameError("files must come in OLD NEW pairs")
        pairs = list(zip(a.files[::2], a.files[1::2]))
        if a.pairs:
            pairs += read_pairs(a.pairs)
        if not pairs:
            raise pr.RenameError("no pairs to check")
    except pr.RenameError as e:
        print(f"parquet_rename_check: {e}", file=sys.stderr)
        return 2
    bad = 0
    for old, new in pairs:
        rep = check_pair(old, new, mapping)
        bad += not rep["ok"]
        print(json.dumps(rep), flush=True)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
