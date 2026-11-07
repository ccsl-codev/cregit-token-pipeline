"""Tests for replacing one column's values with parquet_rename.py, and its check."""

from __future__ import annotations

import json
import os

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
duckdb = pytest.importorskip("duckdb")

import parquet_rename as pr  # noqa: E402
import parquet_rename_check as prc  # noqa: E402
from test_parquet_drop import bump, cc_at, cmd_at  # noqa: E402
from test_parquet_rename import meta_of, with_footer  # noqa: E402

N = 4000


def proj_table(n: int = N, *, language="C", ltype=None):
    ltype = ltype or pa.string()
    schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("language", ltype),
        pa.field("license", pa.string()),
        pa.field("owner", pa.string()),
        pa.field("tags", pa.list_(pa.string())),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("score", pa.float64()),
    ], metadata={"tool_version": "7"})
    return pa.table({
        "id": list(range(n)),
        "language": pa.array([language] * n, pa.string()).cast(ltype),
        "license": pa.array([None] * n, pa.string()),
        "owner": [f"org-{i % 31}" for i in range(n)],
        "tags": [[f"t{i % 5}"] if i % 3 else None for i in range(n)],
        "kind": ["lib"] * n,
        "score": [i / 7 for i in range(n)],
    }, schema=schema)


def write_proj(path, *, rows: int = N, rg: int = 1000, bloom: bool = True,
               page_index: bool = True, ltype=None, null_bloom: bool = False, **kw):
    blooms = {"language": {"ndv": 8, "fpp": 0.01}, "owner": True}
    if null_bloom:
        blooms["license"] = True
    pq.write_table(proj_table(rows, ltype=ltype), path, row_group_size=rg,
                   compression=kw.pop("compression", "zstd"),
                   write_page_index=page_index,
                   bloom_filter_options=blooms if bloom else None,
                   sorting_columns=[pq.SortingColumn(0)], **kw)
    return path


@pytest.fixture
def src(tmp_path):
    return write_proj(tmp_path / "src.parquet")


def replace(src, dst, values, mapping=None, drops=(), **kw):
    return pr.rename_file(str(src), str(dst), mapping or {}, drops=drops, values=values, **kw)


def check(old, new, values, mapping=None, drops=(), expect=None, force_multi=False):
    return prc.check_pair(str(old), str(new), mapping or {}, drops, values, expect, force_multi)


def arrow_kv(path) -> bytes:
    for kv in pr.kv_items(meta_of(path), pr.FMD_KV):
        if pr.get(kv, pr.KV_KEY) == pr.ARROW_SCHEMA_KEY:
            return pr.get(kv, pr.KV_VALUE)
    return b""


def probe(path, col, value):
    return duckdb.execute("SELECT bloom_filter_excludes FROM parquet_bloom_probe(?, ?, ?)",
                          [str(path), col, value]).fetchall()


# ---------------------------------------------------------------- values --
@pytest.mark.parametrize("rg", [N, 700], ids=["one-row-group", "many-row-groups"])
def test_set_one_value(src, tmp_path, rg):
    src = write_proj(tmp_path / "s.parquet", rg=rg)
    dst = tmp_path / "d.parquet"
    rep = replace(src, dst, {"language": "Go"})
    assert rep["replaced"] == {"language": dict(new="Go", old="C", old_distinct=1)}
    groups = pq.ParquetFile(dst).metadata.num_row_groups
    assert rep["regions_reencoded"] == 4 * groups
    got = pq.read_table(dst)
    assert got.column("language").to_pylist() == ["Go"] * N
    assert got.drop_columns(["language"]).equals(proj_table().drop_columns(["language"]))
    assert arrow_kv(dst) == arrow_kv(src)
    assert probe(dst, "language", "Go") == [(False,)] * groups
    assert probe(dst, "language", "C") == [(True,)] * groups
    res = check(src, dst, {"language": "Go"}, expect={"language": "C"})
    assert res["ok"], res["problems"]
    assert res["column_chunks_reencoded"] == groups
    assert res["column_chunks_identical"] == 6 * groups
    assert res["bloom_old_values_probed"] == {"language": 1}


@pytest.mark.parametrize("bloom", [True, False], ids=["bloom", "no-bloom"])
@pytest.mark.parametrize("page_index", [True, False], ids=["page-index", "no-page-index"])
def test_bloom_and_page_index_are_kept_as_they_were(tmp_path, bloom, page_index):
    src = write_proj(tmp_path / "s.parquet", bloom=bloom, page_index=page_index)
    dst = tmp_path / "d.parquet"
    replace(src, dst, {"language": "Rust"})
    old, new = cc_at(meta_of(src), 2, 1), cc_at(meta_of(dst), 2, 1)
    for fid in (pr.CC_COLUMN_INDEX_OFFSET, pr.CC_OFFSET_INDEX_OFFSET):
        assert pr.has(old, fid) == pr.has(new, fid) == page_index
    assert pr.has(pr.get(new, pr.CC_META_DATA), pr.CMD_BLOOM_FILTER_OFFSET) == bloom
    res = check(src, dst, {"language": "Rust"})
    assert res["ok"], res["problems"]
    assert res["page_index_pages"] >= (28 if page_index else 0)


def test_set_with_drop_and_rename_in_one_pass(src, tmp_path):
    dst = tmp_path / "d.parquet"
    mapping = {"language": "lang", "owner": "org"}
    rep = replace(src, dst, {"language": "Go", "license": "MIT"}, mapping, ["tags", "score"])
    assert rep["columns_kept"] == 5
    pf = pq.ParquetFile(dst)
    assert pf.schema_arrow.names == ["id", "lang", "license", "org", "kind"]
    assert pf.metadata.row_group(0).sorting_columns == (pq.SortingColumn(0),)
    t = pf.read()
    assert set(t.column("lang").to_pylist()) == {"Go"}
    assert set(t.column("license").to_pylist()) == {"MIT"}
    assert probe(dst, "lang", "Go") == [(False,)] * 4
    res = check(src, dst, {"language": "Go", "license": "MIT"}, mapping, ["tags", "score"])
    assert res["ok"], res["problems"]
    assert res["bloom_columns"] == ["lang", "org"]
    other = tmp_path / "rw.parquet"
    replace(src, other, {"language": "Go", "license": "MIT"}, mapping, ["tags", "score"],
            copy="read-write", fsync=False)
    assert other.read_bytes() == dst.read_bytes()


def test_set_null_and_fill_a_null(tmp_path):
    src = write_proj(tmp_path / "s.parquet", null_bloom=True)
    assert pr.has(cmd_at(meta_of(src), 0, 2), pr.CMD_BLOOM_FILTER_OFFSET)
    dst = tmp_path / "d.parquet"
    values = {"language": None, "license": "Apache-2.0"}
    rep = replace(src, dst, values)
    assert rep["replaced"]["license"]["old"] is None
    t = pq.read_table(dst)
    assert t.column("language").null_count == N
    assert set(t.column("license").to_pylist()) == {"Apache-2.0"}
    stats = pr.get(cmd_at(meta_of(dst), 0, 1), pr.CMD_STATISTICS)
    assert pr.get(stats, pr.ST_NULL_COUNT) == 1000 and not pr.has(stats, pr.ST_MIN_VALUE)
    assert probe(dst, "language", "C") == [(True,)] * 4
    assert probe(dst, "license", "Apache-2.0") == [(False,)] * 4
    res = check(src, dst, values, expect={"language": "C", "license": None})
    assert res["ok"], res["problems"]


@pytest.mark.parametrize("kw", [
    dict(use_dictionary=False),
    dict(use_dictionary=False, column_encoding={"language": "DELTA_BYTE_ARRAY"}),
    dict(use_dictionary=False, column_encoding={"language": "DELTA_LENGTH_BYTE_ARRAY"}),
    dict(data_page_version="2.0"),
    dict(write_page_checksum=True),
    dict(version="1.0"),
    dict(compression="snappy"),
    dict(compression="gzip"),
    dict(compression="none"),
    dict(compression="lz4"),
    dict(compression="brotli"),
    dict(ltype=pa.large_string()),
    dict(ltype=pa.dictionary(pa.int32(), pa.string())),
], ids=["plain", "delta-byte-array", "delta-length", "page-v2", "checksum", "plain-dictionary",
        "snappy", "gzip", "none", "lz4", "brotli", "large-string", "arrow-dictionary"])
def test_the_chunk_keeps_its_form(tmp_path, kw):
    if "column_encoding" in kw:
        kw["column_encoding"] = kw["column_encoding"] | {"owner": "PLAIN"}
    src = write_proj(tmp_path / "s.parquet", **kw)
    dst = tmp_path / "d.parquet"
    replace(src, dst, {"language": "Kotlin"})
    old, new = cmd_at(meta_of(src), 1, 1), cmd_at(meta_of(dst), 1, 1)
    assert pr.get(new, pr.CMD_CODEC) == pr.get(old, pr.CMD_CODEC)
    assert pr.encoding_family(new) == pr.encoding_family(old)
    with open(src, "rb") as a, open(dst, "rb") as b:
        assert pr.page_shape(b.fileno(), new) == pr.page_shape(a.fileno(), old)
    assert pq.ParquetFile(dst).schema_arrow == pq.ParquetFile(src).schema_arrow
    res = check(src, dst, {"language": "Kotlin"})
    assert res["ok"], res["problems"]


def test_set_on_a_duckdb_written_file(tmp_path):
    a = tmp_path / "a.parquet"
    duckdb.sql(
        "COPY (SELECT i AS id, 'C' AS language, 'MIT' AS license, 'org-' || (i % 9) AS owner, "
        "[i % 5] AS tags FROM range(9000) t(i)) "
        f"TO '{a}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 2048)")
    old = cmd_at(meta_of(a), 0, 1)
    assert pr.has(pr.get(old, pr.CMD_STATISTICS), pr.ST_DISTINCT_COUNT)
    b = tmp_path / "b.parquet"
    replace(a, b, {"language": "C++"}, {"license": "spdx"}, ["tags"])
    new = cmd_at(meta_of(b), 0, 1)
    assert [f.fid for f in new] == [f.fid for f in old]
    assert pr.get(pr.get(new, pr.CMD_STATISTICS), pr.ST_DISTINCT_COUNT) == 1
    res = check(a, b, {"language": "C++"}, {"license": "spdx"}, ["tags"],
                expect={"language": "C"})
    assert res["ok"], res["problems"]
    assert duckdb.execute("SELECT DISTINCT language FROM read_parquet(?)",
                          [str(b)]).fetchall() == [("C++",)]


def test_force_multi_replaces_every_value(src, tmp_path):
    dst = tmp_path / "d.parquet"
    with pytest.raises(pr.RenameError, match="31 distinct values"):
        replace(src, dst, {"owner": "org-x"})
    with pytest.raises(pr.RenameError, match="is not one of the 31"):
        replace(src, dst, {"owner": "org-x"}, force_multi=True, expect={"owner": "nobody"})
    rep = replace(src, dst, {"owner": "org-x"}, force_multi=True, expect={"owner": "org-3"})
    assert rep["replaced"]["owner"] == dict(new="org-x", old=None, old_distinct=31)
    res = check(src, dst, {"owner": "org-x"}, expect={"owner": "org-3"}, force_multi=True)
    assert res["ok"], res["problems"]
    assert res["bloom_old_values_probed"] == {"owner": 3}


def test_the_expected_old_value_must_match(src, tmp_path):
    dst = tmp_path / "d.parquet"
    with pytest.raises(pr.RenameError, match="old value of 'language' is 'C', not 'Go'"):
        replace(src, dst, {"language": "Rust"}, expect={"language": "Go"})
    assert not dst.exists()


# -------------------------------------------------------------- refusals --
@pytest.mark.parametrize("values, drops, match", [
    ({"tags": "x"}, (), "nested or repeated"),
    ({"id": "1"}, (), "not a string column"),
    ({"score": "1.5"}, (), "not a string column"),
    ({"kind": None}, (), "required column; a null does not fit"),
    ({"nope": "x"}, (), "no top-level column 'nope' to set"),
    ({"language": "x"}, ("language",), "both set and dropped"),
    ({"language": "\udcff"}, (), "not valid UTF-8"),
])
def test_bad_values_are_refused(src, tmp_path, values, drops, match):
    dst = tmp_path / "d.parquet"
    with pytest.raises(pr.RenameError, match=match):
        replace(src, dst, values, drops=drops)
    assert not dst.exists() and not os.path.exists(str(dst) + ".tmp")


def test_encrypted_files_are_refused(src, tmp_path):
    raw = bytearray(src.read_bytes())
    raw[-4:] = pr.MAGIC_ENCRYPTED
    enc = tmp_path / "e.parquet"
    enc.write_bytes(bytes(raw))
    with pytest.raises(pr.RenameError, match="encrypted"):
        replace(enc, tmp_path / "d.parquet", {"language": "Go"})
    meta = meta_of(src)
    cc_at(meta, 0, 3).append(pr.Field(pr.CC_CRYPTO_METADATA, pr.T_STRUCT, [], True))
    bad = with_footer(src, tmp_path / "c.parquet", meta)
    with pytest.raises(pr.RenameError, match="encrypted"):
        replace(bad, tmp_path / "d.parquet", {"language": "Go"})


def test_tail_only_and_empty_files_are_refused(src, tmp_path):
    with pytest.raises(pr.RenameError, match="--tail-only cannot"):
        replace(src, tmp_path / "t.bin", {"language": "Go"}, tail_only=True)
    empty = write_proj(tmp_path / "empty.parquet", rows=0)
    with pytest.raises(pr.RenameError, match="no rows"):
        replace(empty, tmp_path / "d.parquet", {"language": "Go"})


def test_a_string_type_is_needed_in_both_schemas(src, tmp_path):
    binary = tmp_path / "b.parquet"
    pq.write_table(pa.table({"language": pa.array([b"C"] * 10, pa.binary())}), binary)
    with pytest.raises(pr.RenameError, match="not a string column"):
        replace(binary, tmp_path / "d.parquet", {"language": "Go"})
    elem = [pr.Field(pr.SE_TYPE, pr.T_I32, pr.BYTE_ARRAY, True),
            pr.Field(pr.SE_CONVERTED_TYPE, pr.T_I32, pr.CONVERTED_UTF8, True)]
    assert pr.is_string_leaf(elem)
    enum = elem[:1] + [pr.Field(pr.SE_LOGICAL_TYPE, pr.T_STRUCT,
                                [pr.Field(4, pr.T_STRUCT, [], True)], True)]
    assert not pr.is_string_leaf(enum)
    schema = pa.schema([pa.field("language", pa.int32())])
    ctx = dict(schema=[[], elem], spans=[(1, 2, "language", 1)], arrow_schema=schema)
    with pytest.raises(pr.RenameError, match="ARROW:schema does not give"):
        pr.replace_targets(ctx, {"language": "Go"}, ())


def test_chunk_forms_the_tool_cannot_write_are_refused(src, tmp_path):
    cmd = cmd_at(meta_of(src), 0, 1)
    pr.set_value(cmd, pr.CMD_ENCODINGS, pr.TList(pr.T_LIST, pr.T_I32, [9], True))
    with pytest.raises(pr.RenameError, match="encodings"):
        pr.encoding_family(cmd)
    with pytest.raises(pr.RenameError, match="codec 5"):
        pr.encode_column("language", "Go", True, (10, 5, "dictionary", False, 1, False, 10,
                                                  False, False))
    with pytest.raises(pr.RenameError, match="statistics field 9"):
        pr.rebuilt_statistics([pr.Field(9, pr.T_I64, 1, True)], "Go", 10)
    e = pr.encode_column("language", "Go", True, (1000, 6, "dictionary", False, 1, False,
                                                  1000, False, False))
    old = cmd_at(meta_of(src), 0, 1)
    old.append(pr.Field(17, pr.T_STRUCT, [], True))
    with pytest.raises(pr.RenameError, match="field 17"):
        pr.merged_cmd(old, e)
    pr.set_value(old, pr.CMD_CODEC, 1)
    with pytest.raises(pr.RenameError, match="another codec"):
        pr.merged_cmd(old, e)
    reg = pr.Region("bloom", 0, 10, 0, 0)
    with pytest.raises(pr.RenameError, match="no bloom"):
        pr.put_replacements([reg], {(0, 0): e}, {})


def test_file_offset_forms(src):
    ch = pr.Region("chunk", 100, 50, 0, 0)
    ch.new_off, ch.length = 10, 20
    for old, new in ((0, 0), (100, 10), (150, 30)):
        cc = [pr.Field(pr.CC_FILE_OFFSET, pr.T_I64, old, True)]
        pr.move_file_offset(cc, (100, 50), ch)
        assert pr.get(cc, pr.CC_FILE_OFFSET) == new
    with pytest.raises(pr.RenameError, match="file_offset"):
        pr.move_file_offset([pr.Field(pr.CC_FILE_OFFSET, pr.T_I64, 120, True)], (100, 50), ch)


def test_page_shape_refuses_mixed_or_short_chunks(src):
    cmd = cmd_at(meta_of(src), 0, 1)
    bump(cmd, pr.CMD_TOTAL_COMPRESSED_SIZE, -1)
    with open(src, "rb") as fh, pytest.raises(pr.RenameError, match="page versions"):
        pr.page_shape(fh.fileno(), cmd)


# ----------------------------------------------------------------- check --
def test_check_catches_a_wrong_value_and_tampering(src, tmp_path):
    dst = tmp_path / "d.parquet"
    replace(src, dst, {"language": "Go"})
    res = check(src, dst, {"language": "Zig"}, expect={"language": "Go"})
    assert not res["ok"]
    text = " ".join(res["problems"])
    for part in ("statistics field 6", "does not hold the new value in every row",
                 "DuckDB column 'language'", "does not find the new value",
                 "OLD column 'language' does not hold"):
        assert part in text

    meta = meta_of(dst)
    new = cmd_at(meta, 1, 1)
    pr.set_value(new, pr.CMD_CODEC, 1)
    pr.set_value(new, pr.CMD_NUM_VALUES, 7)
    bump(pr.get(meta, pr.FMD_ROW_GROUPS).items[2], pr.RG_TOTAL_BYTE_SIZE, 1)
    pr.get(new, pr.CMD_PATH_IN_SCHEMA).items[0] = b"other"
    bad = with_footer(dst, tmp_path / "bad.parquet", meta)
    res = check(src, bad, {"language": "Go"})
    text = " ".join(res["problems"])
    for part in ("row group 1 column 1: codec differs", "num_values is not the row count",
                 "row group 2: field 2 is not the sum", "path_in_schema is not the column name"):
        assert part in text, text


def test_check_catches_presence_and_form_changes(src, tmp_path):
    dst = tmp_path / "d.parquet"
    replace(src, dst, {"language": "Go"})
    meta = meta_of(dst)
    cmd = cmd_at(meta, 0, 1)
    cmd[:] = [f for f in cmd if f.fid != pr.CMD_SIZE_STATISTICS]
    cmd = pr.normalized(cmd)
    cc = cc_at(meta, 0, 1)
    pr.set_value(cc, pr.CC_FILE_OFFSET, 5)
    pr.set_value(cmd, pr.CMD_ENCODINGS, pr.TList(pr.T_LIST, pr.T_I32, [0], True))
    stats = pr.get(cmd, pr.CMD_STATISTICS)
    stats[:] = [f for f in stats if f.fid != pr.ST_MIN_VALUE]
    stats[:] = pr.normalized(stats)
    bad = with_footer(dst, tmp_path / "bad.parquet", meta)
    text = " ".join(check(src, bad, {"language": "Go"})["problems"])
    for part in ("presence differs", "file_offset form differs", "encoding family differs",
                 "statistics fields differ"):
        assert part in text, text


def test_check_catches_a_flipped_byte_in_a_kept_chunk(src, tmp_path):
    dst = tmp_path / "d.parquet"
    replace(src, dst, {"language": "Go"})
    start, n = pr.chunk_span(cmd_at(meta_of(dst), 2, 3))
    raw = bytearray(dst.read_bytes())
    raw[start + n // 2] ^= 0xFF
    bad = tmp_path / "bad.parquet"
    bad.write_bytes(bytes(raw))
    res = check(src, bad, {"language": "Go"})
    assert "row group 2 column 3: column chunk bytes differ" in res["problems"]


def test_check_catches_a_bloom_that_keeps_the_old_value(src, tmp_path):
    dst = tmp_path / "d.parquet"
    replace(src, dst, {"language": "Go"})
    meta = meta_of(dst)
    for r in range(4):
        lang, owner = cmd_at(meta, r, 1), cmd_at(meta, r, 3)
        for fid in (pr.CMD_BLOOM_FILTER_OFFSET, pr.CMD_BLOOM_FILTER_LENGTH):
            pr.set_value(lang, fid, pr.get(owner, fid))
    bad = with_footer(dst, tmp_path / "bad.parquet", meta)
    problems = check(src, bad, {"language": "Go"})["problems"]
    assert "Bloom filter of 'language' does not find the new value in every row group" in problems


# ------------------------------------------------------------------- CLI --
def test_cli_set_and_check(src, tmp_path, capsys):
    dst = tmp_path / "d.parquet"
    assert pr.main([str(src), str(dst), "--set", "language=Go", "--set-null", "license",
                    "--drop", "tags", "--no-fsync"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] and out["replaced"]["language"]["new"] == "Go"
    assert prc.main(["--set", "language=Go", "--set-null", "license", "--drop", "tags",
                     str(src), str(dst)]) == 0
    capsys.readouterr()
    assert prc.main(["--set", "language=Java", "--drop", "tags", str(src), str(dst)]) == 1


def test_cli_values_file(tmp_path, capsys):
    a = write_proj(tmp_path / "a.parquet")
    b = write_proj(tmp_path / "b.parquet")
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    tsv = tmp_path / "values.tsv"
    tsv.write_text("# file, column, old, new\n\n"
                   f"{a}\tlanguage\tC\tGo\n{a}\tlicense\t\\N\tMIT\n"
                   f"{b}\tlanguage\tRust\tGo\n", encoding="utf-8")
    assert pr.main(["--values", str(tsv), "--out-dir", str(out_dir), "--rename", "owner=org",
                    "--no-fsync"]) == 2
    lines = [json.loads(x) for x in capsys.readouterr().out.splitlines()]
    assert [x["ok"] for x in lines] == [True, False]
    assert "not 'Rust'" in lines[1]["error"]
    assert (out_dir / "a.parquet").exists() and not (out_dir / "b.parquet").exists()
    good = tmp_path / "good.tsv"
    good.write_text("".join(tsv.read_text().splitlines(True)[:4]), encoding="utf-8")
    assert prc.main(["--values", str(good), "--out-dir", str(out_dir),
                     "--rename", "owner=org"]) == 0
    rep = json.loads(capsys.readouterr().out)
    assert rep["ok"] and rep["replaced"] == {"language": "Go", "license": "MIT"}


def test_cli_errors(src, tmp_path, capsys):
    tsv = tmp_path / "v.tsv"
    tsv.write_text(f"{src}\tlanguage\tC\tGo\n", encoding="utf-8")
    for argv in ([str(src), str(tmp_path / "d.parquet"), "--set", "language"],
                 [str(src), str(tmp_path / "d.parquet"), "--set", "a=1", "--set-null", "a"],
                 [str(src), str(tmp_path / "d.parquet"), "--set", "a=1", "--set", "a=2"],
                 ["--values", str(tsv)],
                 ["--values", str(tsv), "--out-dir", str(tmp_path), str(src), "x"],
                 [str(src)]):
        assert pr.main(argv) == 2
        assert json.loads(capsys.readouterr().out)["ok"] is False
    assert pr.main([str(tmp_path / "missing.parquet"), str(tmp_path / "d.parquet"),
                    "--set", "language=Go"]) == 2
    assert "missing" in json.loads(capsys.readouterr().out)["error"]
    twice = tmp_path / "twice.tsv"
    other = tmp_path / "sub"
    other.mkdir()
    twice.write_text(f"{src}\tlanguage\tC\tGo\n{other / src.name}\tlanguage\tC\tGo\n",
                     encoding="utf-8")
    assert pr.main(["--values", str(twice), "--out-dir", str(tmp_path / "o")]) == 2
    assert "two files" in capsys.readouterr().out
    for bad, match in (("a\tb\tc\n", "expected FILE"),
                       ("f\tc\to\tn\nf\tc\to\tm\n", "set twice for one file")):
        tsv.write_text(bad, encoding="utf-8")
        with pytest.raises(pr.RenameError, match=match):
            pr.read_values_tsv(str(tsv))
    tsv.write_text(f"{src}\tlanguage\tC\tGo\n", encoding="utf-8")
    assert prc.main(["--values", str(tsv), "--out-dir", str(tmp_path), str(src), "x"]) == 2
    assert "--values takes no pairs" in capsys.readouterr().err
