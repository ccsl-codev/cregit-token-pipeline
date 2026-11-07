"""Tests for dropping columns with parquet_rename.py and proving it with the check."""

from __future__ import annotations

import json
import struct

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
duckdb = pytest.importorskip("duckdb")

import parquet_rename as pr  # noqa: E402
import parquet_rename_check as prc  # noqa: E402
from test_parquet_rename import meta_of, table, with_footer, write  # noqa: E402


@pytest.fixture
def src(tmp_path):
    return write(tmp_path / "src.parquet")


def drop(src, dst, drops, mapping=None, **kw):
    return pr.rename_file(str(src), str(dst), mapping or {}, drops=drops, **kw)


def check(old, new, drops, mapping=None):
    return prc.check_pair(str(old), str(new), mapping or {}, drops)


def cc_at(meta, r=0, c=0):
    return pr.get(pr.get(meta, pr.FMD_ROW_GROUPS).items[r], pr.RG_COLUMNS).items[c]


def cmd_at(meta, r=0, c=0):
    return pr.get(cc_at(meta, r, c), pr.CC_META_DATA)


def bump(fields, fid, by):
    pr.set_value(fields, fid, pr.get(fields, fid) + by)


# ---------------------------------------------------------------- drops --
def test_drop_one_column(src, tmp_path):
    dst = tmp_path / "d.parquet"
    rep = drop(src, dst, ["score"])
    assert rep["columns_kept"] == 6 and rep["leaves_dropped"] == 1
    assert rep["regions_kept"]["chunk"] == 24
    assert rep["out_size"] == dst.stat().st_size < src.stat().st_size
    assert pq.read_table(dst).equals(table().drop_columns(["score"]))
    res = check(src, dst, ["score"])
    assert res["ok"], res["problems"]
    assert res["column_chunks_identical"] == 24
    assert res["page_index_pages"] >= 24
    assert "score" not in res["column_sha256"]


def test_drop_a_bloom_column_a_list_column_and_more(src, tmp_path):
    dst = tmp_path / "d.parquet"
    drops = ["name", "tags", "flag"]
    drop(src, dst, drops)
    pf = pq.ParquetFile(dst)
    assert pf.schema_arrow.names == ["id", "status", "score", "ts"]
    assert pf.metadata.row_group(0).sorting_columns == (pq.SortingColumn(0),)
    res = check(src, dst, drops)
    assert res["ok"], res["problems"]
    assert res["bloom_columns"] == ["id"]


def test_drop_and_rename_in_one_pass(src, tmp_path):
    dst = tmp_path / "d.parquet"
    mapping = {"status": "audit_status", "name": "person"}
    rep = drop(src, dst, ["tags", "score"], mapping)
    assert rep["column_chunks_renamed"] == 8
    assert pq.read_table(dst).column_names == ["id", "person", "audit_status", "flag", "ts"]
    assert pq.ParquetFile(dst).schema_arrow.field("person").metadata == {b"comment": b"a person"}
    res = check(src, dst, ["tags", "score"], mapping)
    assert res["ok"], res["problems"]
    assert res["bloom_columns"] == ["id", "person"]
    other = tmp_path / "rw.parquet"
    rep = drop(src, other, ["tags", "score"], mapping, copy="read-write", fsync=False)
    assert rep["copy"] == "read-write"
    assert other.read_bytes() == dst.read_bytes()


def test_drop_on_a_duckdb_written_file(tmp_path):
    a = tmp_path / "a.parquet"
    duckdb.sql(
        "COPY (SELECT i AS id, 'p' || (i % 97) AS name, (i % 3)::VARCHAR AS status, "
        "[i % 5, i % 7] AS tags, i / 7 AS score FROM range(9000) t(i)) "
        f"TO '{a}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 2048)")
    assert pq.ParquetFile(a).metadata.num_row_groups > 1
    b = tmp_path / "b.parquet"
    drop(a, b, ["tags", "name"], {"status": "audit_status"})
    res = check(a, b, ["tags", "name"], {"status": "audit_status"})
    assert res["ok"], res["problems"]


def test_drop_without_bloom_filter_length(src, tmp_path):
    meta = meta_of(src)
    for rg in pr.kv_items(meta, pr.FMD_ROW_GROUPS):
        for cc in pr.get(rg, pr.RG_COLUMNS).items:
            cmd = pr.get(cc, pr.CC_META_DATA)
            cmd[:] = [f for f in cmd if f.fid != pr.CMD_BLOOM_FILTER_LENGTH]
    old = with_footer(src, tmp_path / "old.parquet", meta)
    dst = tmp_path / "d.parquet"
    rep = drop(old, dst, ["score"])
    assert rep["regions_kept"]["bloom"] == 8
    res = check(old, dst, ["score"])
    assert res["ok"], res["problems"]


# ------------------------------------------------------------- refusals --
@pytest.mark.parametrize("drops, mapping, match", [
    (["id"], {}, "sort column"),
    (["id", "name", "status", "tags", "score", "flag", "ts"], {}, "every column"),
    (["nope"], {}, "no top-level column 'nope' to drop"),
    (["tags.list.element"], {}, "no top-level column"),
    (["score"], {"score": "x"}, "both renamed and dropped"),
    (["score", "score"], {}, "twice"),
    (["score"], {"flag": "ts"}, "duplicate"),
])
def test_bad_drops_are_refused(src, tmp_path, drops, mapping, match):
    with pytest.raises(pr.RenameError, match=match):
        drop(src, tmp_path / "d.parquet", drops, mapping)
    assert not (tmp_path / "d.parquet").exists()


def test_drop_refuses_mentions_and_tail_only(tmp_path):
    s = write(tmp_path / "s.parquet", meta={"note": "score is a float"})
    with pytest.raises(pr.RenameError, match="old or dropped name"):
        drop(s, tmp_path / "d.parquet", ["score"])
    assert drop(s, tmp_path / "d.parquet", ["score"], allow_mentions=True)["mentions_kept"]
    with pytest.raises(pr.RenameError, match="tail-only"):
        drop(s, tmp_path / "t.bin", ["score"], tail_only=True)


def shorter_chunk(m):
    bump(cmd_at(m), pr.CMD_TOTAL_COMPRESSED_SIZE, -1)


def external_chunk(m):
    cc_at(m).insert(0, pr.Field(pr.CC_FILE_PATH, pr.T_BINARY, b"x", True))


def wrong_total(m):
    pr.set_value(pr.get(m, pr.FMD_ROW_GROUPS).items[0], pr.RG_TOTAL_BYTE_SIZE, 1)


def wrong_rg_offset(m):
    pr.set_value(pr.get(m, pr.FMD_ROW_GROUPS).items[0], pr.RG_FILE_OFFSET, 5)


def short_orders(m):
    pr.get(m, pr.FMD_COLUMN_ORDERS).items.pop()


def stray_file_offset(m):
    pr.set_value(cc_at(m), pr.CC_FILE_OFFSET, 3)


def no_index_length(m):
    cc = cc_at(m)
    cc[:] = [f for f in cc if f.fid != pr.CC_COLUMN_INDEX_LENGTH]


def no_page_offset(m):
    cmd = cmd_at(m)
    cmd[:] = [f for f in cmd if f.fid not in (pr.CMD_DATA_PAGE_OFFSET,
                                              pr.CMD_DICTIONARY_PAGE_OFFSET)]


def extra_chunk(m):
    cols = pr.get(pr.get(m, pr.FMD_ROW_GROUPS).items[0], pr.RG_COLUMNS)
    cols.items.append(cols.items[0])


@pytest.mark.parametrize("edit, match", [
    (shorter_chunk, "not tiled"),
    (external_chunk, "another file"),
    (wrong_total, "total_byte_size"),
    (wrong_rg_offset, "file_offset is not its first chunk"),
    (short_orders, "column_orders"),
    (stray_file_offset, "outside its column chunk"),
    (no_index_length, "has no length"),
    (no_page_offset, "no page offset"),
    (extra_chunk, "not tiled|one column chunk per leaf"),
])
def test_drop_refuses_footers_it_cannot_carry(src, tmp_path, edit, match):
    meta = meta_of(src)
    edit(meta)
    bad = with_footer(src, tmp_path / "bad.parquet", meta)
    with pytest.raises(pr.RenameError, match=match):
        drop(bad, tmp_path / "d.parquet", ["score"])


def test_drop_refuses_bytes_after_the_last_region(src, tmp_path):
    _, start, footer = pr.read_footer(str(src))
    data = src.read_bytes()
    padded = tmp_path / "p.parquet"
    padded.write_bytes(data[:start] + b"\0" * 8 + data[start:])
    with pytest.raises(pr.RenameError, match="not reached by the footer"):
        drop(padded, tmp_path / "d.parquet", ["score"])


def test_drop_refuses_a_bad_offset_index(src, tmp_path):
    cc = cc_at(meta_of(src))
    off, n = pr.get(cc, pr.CC_OFFSET_INDEX_OFFSET), pr.get(cc, pr.CC_OFFSET_INDEX_LENGTH)
    data = bytearray(src.read_bytes())
    data[off:off + n] = b"\xff" * n
    broken = tmp_path / "b.parquet"
    broken.write_bytes(bytes(data))
    with pytest.raises(pr.RenameError, match="offset index does not decode"):
        drop(broken, tmp_path / "d.parquet", ["score"])


# ----------------------------------------------------------------- check --
def test_check_catches_a_moved_offset(src, tmp_path):
    dst = tmp_path / "d.parquet"
    drop(src, dst, ["score"])
    meta = meta_of(dst)
    bump(cmd_at(meta, 1, 2), pr.CMD_DATA_PAGE_OFFSET, 1)
    bad = with_footer(dst, tmp_path / "bad.parquet", meta)
    res = check(src, bad, ["score"])
    assert not res["ok"]
    assert "row group 1 column 2: page offset field 9 moved" in res["problems"]
    assert any("offset index does not start" in p for p in res["problems"])


def test_check_catches_a_flipped_byte(src, tmp_path):
    dst = tmp_path / "d.parquet"
    drop(src, dst, ["score"])
    off = pq.ParquetFile(dst).metadata.row_group(2).column(0).data_page_offset + 30
    b = bytearray(dst.read_bytes())
    b[off] ^= 0xFF
    dst.write_bytes(bytes(b))
    res = check(src, dst, ["score"])
    assert not res["ok"]
    assert "row group 2 column 0: column chunk bytes differ" in res["problems"]


def test_check_catches_wrong_drops_and_moved_regions(src, tmp_path):
    dst = tmp_path / "d.parquet"
    drop(src, dst, ["score"])
    res = check(src, dst, ["flag"])
    assert not res["ok"]
    assert any(p.startswith("footer differs") for p in res["problems"])

    meta = meta_of(dst)
    pr.set_value(cc_at(meta, 0, 1), pr.CC_COLUMN_INDEX_OFFSET,
                 pr.get(cc_at(meta, 0, 2), pr.CC_COLUMN_INDEX_OFFSET))
    bump(cmd_at(meta, 0, 0), pr.CMD_BLOOM_FILTER_OFFSET, 1)
    pr.set_value(pr.get(meta, pr.FMD_ROW_GROUPS).items[3], pr.RG_FILE_OFFSET, 4)
    for fid in (pr.CC_OFFSET_INDEX_OFFSET, pr.CC_OFFSET_INDEX_LENGTH):
        pr.set_value(cc_at(meta, 1, 1), fid, pr.get(cc_at(meta, 1, 2), fid))
    pr.set_value(cc_at(meta, 2, 0), pr.CC_FILE_OFFSET, 77)
    bad = with_footer(dst, tmp_path / "bad.parquet", meta)
    probs = check(src, bad, ["score"])["problems"]
    assert "row group 0 column 1: column index bytes differ" in probs
    assert "row group 0 column 0: Bloom filter bytes differ" in probs
    assert "row group 3: file_offset is not its first chunk" in probs
    assert "row group 1 column 1: offset index is not the old one moved" in probs
    assert "row group 2 column 0: file_offset moved" in probs


def test_check_catches_a_bad_arrow_schema_after_a_drop(src, tmp_path):
    dst = tmp_path / "d.parquet"
    drop(src, dst, ["score"])
    meta = meta_of(dst)
    for kv in pr.kv_items(meta, pr.FMD_KV):
        if pr.get(kv, pr.KV_KEY) == pr.ARROW_SCHEMA_KEY:
            pr.set_value(kv, pr.KV_VALUE, pr.encode_arrow_schema(table().schema))
    res = check(src, with_footer(dst, tmp_path / "a.parquet", meta), ["score"])
    assert "ARROW:schema is not the old schema with the names mapped" in res["problems"]


def test_page_index_check_finds_bad_entries(src, tmp_path):
    meta = meta_of(src)
    cc = cc_at(meta)
    off, n = pr.get(cc, pr.CC_OFFSET_INDEX_OFFSET), pr.get(cc, pr.CC_OFFSET_INDEX_LENGTH)
    raw_oi = src.read_bytes()[off:off + n]
    _, start, _ = pr.read_footer(str(src))
    data = src.read_bytes()[:start]
    path = tmp_path / "p.parquet"

    def run(edit_locs):
        oi = pr.parse_footer(raw_oi)
        edit_locs(pr.kv_items(oi, pr.OI_PAGE_LOCATIONS))
        raw = pr.encode(oi)
        m = meta_of(src)
        pr.set_value(cc_at(m), pr.CC_OFFSET_INDEX_OFFSET, len(data))
        pr.set_value(cc_at(m), pr.CC_OFFSET_INDEX_LENGTH, len(raw))
        foot = pr.encode(m)
        path.write_bytes(data + raw + foot + struct.pack("<I", len(foot)) + pr.MAGIC)
        return prc.page_index_problems(str(path), m, {})

    def copy_of_first(locs, offset=None):
        extra = pr.parse_footer(pr.encode(locs[0]))
        if offset is not None:
            pr.set_value(extra, pr.PL_OFFSET, offset)
        locs.append(extra)

    assert run(lambda locs: None) == []
    assert "outside the chunk" in run(lambda locs: copy_of_first(locs, 10 ** 7))[0]
    assert "no data page" in run(lambda locs: bump(locs[0], pr.PL_COMPRESSED_PAGE_SIZE, 1))[0]
    assert "does not start" in run(lambda locs: locs.clear())[0]
    assert "page counts" in run(copy_of_first)[0]
    path.write_bytes(b"PAR1")
    assert "does not decode" in prc.page_index_problems(str(path), meta_of(src), {})[0]


def test_cli_drop(src, tmp_path, capsys):
    dst = tmp_path / "d.parquet"
    assert pr.main([str(src), str(dst), "--drop", "tags", "--drop", "score",
                    "--rename", "status=audit_status", "--no-fsync"]) == 0
    assert json.loads(capsys.readouterr().out)["dropped"] == ["tags", "score"]
    assert prc.main(["--drop", "tags", "--drop", "score", "--rename", "status=audit_status",
                     str(src), str(dst)]) == 0
    assert json.loads(capsys.readouterr().out)["ok"]
    assert prc.main(["--drop", "tags", str(src), str(dst)]) == 1
