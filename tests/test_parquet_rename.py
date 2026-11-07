"""Tests for parquet_rename.py and parquet_rename_check.py on synthetic files."""

from __future__ import annotations

import base64
import json
import struct
from pathlib import Path

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
pytest.importorskip("duckdb")

import parquet_rename as pr  # noqa: E402
import parquet_rename_check as prc  # noqa: E402

ROWS = 6000


def table(meta: dict | None = None):
    n = ROWS
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), metadata={"comment": "a person"}),
            pa.field("status", pa.string()),
            pa.field("tags", pa.list_(pa.string())),
            pa.field("score", pa.float64()),
            pa.field("flag", pa.bool_()),
            pa.field("ts", pa.timestamp("us", tz="UTC")),
        ],
        metadata=meta if meta is not None else {"tool_version": "7"},
    )
    return pa.table(
        {
            "id": list(range(n)),
            "name": [f"person-{i % 997}" if i % 13 else None for i in range(n)],
            "status": [("ok", "late", "gone")[i % 3] for i in range(n)],
            "tags": [[f"t{i % 5}", f"u{i % 7}"] if i % 4 else None for i in range(n)],
            "score": [i / 7 for i in range(n)],
            "flag": [i % 2 == 0 for i in range(n)],
            "ts": [i * 1_000_000 for i in range(n)],
        },
        schema=schema,
    )


def write(path: Path, *, store_schema: bool = True, meta: dict | None = None) -> Path:
    pq.write_table(
        table(meta), path,
        compression="zstd", compression_level=19,
        use_dictionary=True, data_page_version="1.0",
        write_page_index=True, row_group_size=1500,
        bloom_filter_options={"id": {"ndv": ROWS, "fpp": 0.01}, "name": True},
        store_schema=store_schema,
        sorting_columns=[pq.SortingColumn(0)],
    )
    return path


@pytest.fixture
def src(tmp_path):
    return write(tmp_path / "src.parquet")


def with_footer(base: Path, out: Path, meta: list) -> Path:
    """`base` with its footer replaced by an encoded `meta`."""
    _, start, _ = pr.read_footer(str(base))
    new = pr.encode(meta)
    out.write_bytes(base.read_bytes()[:start] + new + struct.pack("<I", len(new)) + pr.MAGIC)
    return out


def meta_of(path: Path) -> list:
    return pr.parse_footer(pr.read_footer(str(path))[2])


def check(old, new, mapping):
    return prc.check_pair(str(old), str(new), mapping)


# ------------------------------------------------------------ round trip --
@pytest.mark.parametrize("store_schema", [True, False])
def test_identity_is_byte_identical(tmp_path, store_schema):
    s = write(tmp_path / "s.parquet", store_schema=store_schema)
    _, _, footer = pr.read_footer(str(s))
    assert pr.encode(pr.parse_footer(footer)) == footer
    rep = pr.rename_file(str(s), str(tmp_path / "d.parquet"), {})
    assert (tmp_path / "d.parquet").read_bytes() == s.read_bytes()
    assert rep["arrow_schema"] is store_schema
    assert check(s, tmp_path / "d.parquet", {})["ok"]


def test_read_write_copy_gives_same_bytes(src, tmp_path):
    a, b = tmp_path / "a.parquet", tmp_path / "b.parquet"
    pr.rename_file(str(src), str(a), {"status": "audit_status"})
    rep = pr.rename_file(str(src), str(b), {"status": "audit_status"}, copy="read-write",
                         fsync=False)
    assert rep["copy"] == "read-write"
    assert a.read_bytes() == b.read_bytes()


def test_codec_keeps_long_headers_and_containers():
    w = pr.Writer()
    lst = pr.TList(pr.T_LIST, pr.T_I32, list(range(20)), False)
    bools = pr.TList(pr.T_LIST, pr.T_TRUE, [1, 2, 1], True)
    m = pr.TMap(pr.T_BINARY, pr.T_I64, [(b"k", -5)])
    empty = pr.TMap(None, None, [])
    fields = [
        pr.Field(1, pr.T_I8, 7, True),
        pr.Field(40, pr.T_LIST, lst, False),
        pr.Field(41, pr.T_TRUE, None, True),
        pr.Field(42, pr.T_LIST, bools, True),
        pr.Field(43, pr.T_MAP, m, True),
        pr.Field(44, pr.T_MAP, empty, True),
        pr.Field(45, pr.T_DOUBLE, b"\0" * 8, True),
        pr.Field(46, pr.T_UUID, b"\1" * 16, True),
        pr.Field(47, pr.T_I16, -3, True),
    ]
    w.struct(fields)
    data = bytes(w.out)
    assert pr.encode(pr.parse_footer(data)) == data
    with pytest.raises(ValueError):
        pr.parse_footer(data + b"\0")
    with pytest.raises(ValueError):
        pr.parse_footer(data[:-3])
    with pytest.raises(ValueError):
        pr.encode([pr.Field(1, 14, None, True)])
    with pytest.raises(ValueError):
        pr.parse_footer(bytes([0x1E, 0]))
    with pytest.raises(ValueError):
        pr.encode([pr.Field(20, pr.T_I8, 1, True)])
    with pytest.raises(ValueError):
        pr.encode([pr.Field(1, pr.T_LIST, pr.TList(pr.T_LIST, pr.T_I8, [0] * 15, True), True)])


# ---------------------------------------------------------------- renames --
def test_one_rename(src, tmp_path):
    dst = tmp_path / "d.parquet"
    rep = pr.rename_file(str(src), str(dst), {"status": "audit_status"})
    assert rep["column_chunks_renamed"] == 4  # one leaf in each of 4 row groups
    assert rep["prefix_len"] < rep["in_size"]
    assert dst.read_bytes()[:rep["prefix_len"]] == src.read_bytes()[:rep["prefix_len"]]
    t = pq.read_table(dst)
    assert t.column_names == ["id", "name", "audit_status", "tags", "score", "flag", "ts"]
    assert t.rename_columns(table().column_names).equals(table())
    assert t.schema.metadata[b"tool_version"] == b"7"
    res = check(src, dst, {"status": "audit_status"})
    assert res["ok"], res["problems"]
    assert res["bloom_columns"] == ["id", "name"]
    assert res["row_groups"] == 4 and res["rows"] == ROWS
    assert "audit_status" in res["column_sha256"]


def test_two_renames_with_a_list_and_a_bloom_column(src, tmp_path):
    dst = tmp_path / "d.parquet"
    mapping = {"tags": "labels", "name": "person"}
    rep = pr.rename_file(str(src), str(dst), mapping)
    assert rep["column_chunks_renamed"] == 8
    pf = pq.ParquetFile(dst)
    assert pf.schema_arrow.field("person").metadata == {b"comment": b"a person"}
    assert pf.metadata.row_group(0).column(3).path_in_schema == "labels.list.element"
    res = check(src, dst, mapping)
    assert res["ok"], res["problems"]
    assert res["bloom_columns"] == ["id", "person"]


def test_swap_of_two_names(src, tmp_path):
    dst = tmp_path / "d.parquet"
    mapping = {"score": "flag", "flag": "score"}
    pr.rename_file(str(src), str(dst), mapping)
    assert pq.read_table(dst).column("score").type == pa.bool_()
    assert check(src, dst, mapping)["ok"]


def test_tail_only_rebuilds_the_full_file(src, tmp_path):
    full, tail = tmp_path / "full.parquet", tmp_path / "tail.bin"
    pr.rename_file(str(src), str(full), {"status": "audit_status"})
    rep = pr.rename_file(str(src), str(tail), {"status": "audit_status"}, tail_only=True)
    assert rep["copy"] == "tail-only"
    assert src.read_bytes()[:rep["prefix_len"]] + tail.read_bytes() == full.read_bytes()


def test_mentions_are_refused_unless_allowed(tmp_path):
    s = write(tmp_path / "s.parquet", meta={"note": "status is the label column"})
    with pytest.raises(pr.RenameError, match="key-value 'note'"):
        pr.rename_file(str(s), str(tmp_path / "d.parquet"), {"status": "audit_status"})
    rep = pr.rename_file(str(s), str(tmp_path / "d.parquet"), {"status": "audit_status"},
                         allow_mentions=True)
    assert rep["mentions_kept"]
    assert check(s, tmp_path / "d.parquet", {"status": "audit_status"})["ok"]


def test_field_metadata_mention_is_found(src):
    _, _, footer = pr.read_footer(str(src))
    meta = pr.parse_footer(footer)
    kv = pr.kv_items(meta, pr.FMD_KV)
    schema = pr.arrow_schema_of(pr.get(kv[-1], pr.KV_VALUE))
    found = pr.mentions(meta, schema, [b"person"])
    assert found == ["ARROW:schema field 'name' metadata 'comment'"]


# -------------------------------------------------------------- refusals --
@pytest.mark.parametrize("mapping, match", [
    ({"nope": "x"}, "no top-level column 'nope'"),
    ({"tags.list.element": "x"}, "no top-level column"),
    ({"status": "name"}, "duplicate"),
    ({"status": "z", "flag": "z"}, "duplicate"),
    ({"status": "status"}, "to itself"),
    ({"status": ""}, "empty new name"),
])
def test_bad_mappings_are_refused(src, tmp_path, mapping, match):
    with pytest.raises(pr.RenameError, match=match):
        pr.rename_file(str(src), str(tmp_path / "d.parquet"), mapping)
    assert not (tmp_path / "d.parquet").exists()


def test_encrypted_footer_is_refused(src, tmp_path):
    enc = tmp_path / "enc.parquet"
    enc.write_bytes(src.read_bytes()[:-4] + pr.MAGIC_ENCRYPTED)
    with pytest.raises(pr.RenameError, match="encrypted"):
        pr.rename_file(str(enc), str(tmp_path / "d.parquet"), {})


def test_encryption_metadata_is_refused(src, tmp_path):
    meta = meta_of(src)
    meta.append(pr.Field(pr.FMD_ENCRYPTION_ALGORITHM, pr.T_STRUCT, [], True))
    f = with_footer(src, tmp_path / "e.parquet", meta)
    with pytest.raises(pr.RenameError, match="encryption metadata"):
        pr.rename_file(str(f), str(tmp_path / "d.parquet"), {})
    meta = meta_of(src)
    cc = pr.get(pr.get(meta, pr.FMD_ROW_GROUPS).items[0], pr.RG_COLUMNS).items[0]
    cc.append(pr.Field(pr.CC_CRYPTO_METADATA, pr.T_STRUCT, [], False))
    f = with_footer(src, tmp_path / "c.parquet", meta)
    with pytest.raises(pr.RenameError, match="column chunk is encrypted"):
        pr.rename_file(str(f), str(tmp_path / "d.parquet"), {})


def test_not_parquet_and_bad_lengths_are_refused(tmp_path, src):
    small = tmp_path / "small"
    small.write_bytes(b"PAR1PAR1")
    with pytest.raises(pr.RenameError, match="too small"):
        pr.read_footer(str(small))
    junk = tmp_path / "junk"
    junk.write_bytes(b"x" * 64)
    with pytest.raises(pr.RenameError, match="no PAR1"):
        pr.read_footer(str(junk))
    big = tmp_path / "big"
    big.write_bytes(b"PAR1" + b"\0" * 8 + struct.pack("<I", 999) + pr.MAGIC)
    with pytest.raises(pr.RenameError, match="does not fit"):
        pr.read_footer(str(big))
    bad = tmp_path / "bad"
    bad.write_bytes(b"PAR1" + b"\x19\xfc" + struct.pack("<I", 2) + pr.MAGIC)
    with pytest.raises(pr.RenameError, match="does not decode"):
        pr.rename_file(str(bad), str(tmp_path / "d"), {})
    with pytest.raises(pr.RenameError, match="is the input"):
        pr.rename_file(str(src), str(src), {})


def test_non_canonical_footer_encoding_is_refused(src, tmp_path):
    _, start, footer = pr.read_footer(str(src))
    # The first field header (version, i32, delta 1) in the long form instead.
    assert footer[0] == 0x15
    alt = bytes([0x05, 0x02]) + footer[1:]
    f = tmp_path / "alt.parquet"
    f.write_bytes(src.read_bytes()[:start] + alt + struct.pack("<I", len(alt)) + pr.MAGIC)
    meta = pr.parse_footer(alt)
    assert pr.encode(meta) == alt  # the long form is kept, so this one round-trips
    pad = alt[:1] + bytes([0x82, 0x00]) + alt[2:]  # a non-minimal varint
    f.write_bytes(src.read_bytes()[:start] + pad + struct.pack("<I", len(pad)) + pr.MAGIC)
    with pytest.raises(pr.RenameError, match="same bytes"):
        pr.rename_file(str(f), str(tmp_path / "d.parquet"), {})


def test_arrow_schema_problems_are_refused(src, tmp_path):
    def variant(value: bytes, name: str) -> Path:
        meta = meta_of(src)
        for kv in pr.kv_items(meta, pr.FMD_KV):
            if pr.get(kv, pr.KV_KEY) == pr.ARROW_SCHEMA_KEY:
                pr.set_value(kv, pr.KV_VALUE, value)
        return with_footer(src, tmp_path / name, meta)

    kv = [k for k in pr.kv_items(meta_of(src), pr.FMD_KV)
          if pr.get(k, pr.KV_KEY) == pr.ARROW_SCHEMA_KEY][0]
    value = pr.get(kv, pr.KV_VALUE)
    with pytest.raises(pr.RenameError, match="not valid base64"):
        pr.rename_file(str(variant(b" " + value, "sp.parquet")), str(tmp_path / "d"), {})
    raw = base64.b64decode(value)
    longer = raw + b"\0" * (8 if (len(raw) + 8) % 3 else 16)
    with pytest.raises(pr.RenameError, match="re-encode"):
        pr.rename_file(str(variant(base64.b64encode(longer), "long.parquet")),
                       str(tmp_path / "d"), {})
    padded = base64.b64encode(longer)
    i = len(padded.rstrip(b"=")) - 1
    alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
    loose = padded[:i] + bytes([alphabet[alphabet.index(padded[i]) + 1]]) + padded[i + 1:]
    assert base64.b64decode(loose) == longer
    with pytest.raises(pr.RenameError, match="canonical base64"):
        pr.rename_file(str(variant(loose, "loose.parquet")), str(tmp_path / "d"), {})
    other = pa.schema([pa.field("x", pa.int8())])
    with pytest.raises(pr.RenameError, match="differ from the Parquet schema"):
        pr.rename_file(str(variant(pr.encode_arrow_schema(other), "o.parquet")),
                       str(tmp_path / "d"), {})
    meta = meta_of(src)
    kvl = pr.get(meta, pr.FMD_KV)
    kvl.items.append(kvl.items[-1])
    with pytest.raises(pr.RenameError, match="two ARROW:schema"):
        pr.rename_file(str(with_footer(src, tmp_path / "two.parquet", meta)),
                       str(tmp_path / "d"), {})


def test_schema_tree_mismatch_is_refused(src):
    meta = meta_of(src)
    schema = pr.get(meta, pr.FMD_SCHEMA).items
    pr.set_value(schema[0], pr.SE_NUM_CHILDREN, 2)
    with pytest.raises(pr.RenameError, match="schema tree covers"):
        pr.top_level(schema)


# ------------------------------------------------------------------ check --
def test_check_catches_a_flipped_byte_in_a_page(src, tmp_path):
    dst = tmp_path / "d.parquet"
    pr.rename_file(str(src), str(dst), {"status": "audit_status"})
    off = pq.ParquetFile(src).metadata.row_group(1).column(4).data_page_offset + 40
    b = bytearray(dst.read_bytes())
    b[off] ^= 0xFF
    dst.write_bytes(bytes(b))
    res = check(src, dst, {"status": "audit_status"})
    assert not res["ok"]
    assert "bytes before the footer differ" in res["problems"]
    assert len(res["problems"]) >= 2  # the data read fails or its hash differs too


def test_check_catches_a_wrong_footer_value(src, tmp_path):
    dst = tmp_path / "d.parquet"
    pr.rename_file(str(src), str(dst), {"status": "audit_status"})
    meta = meta_of(dst)
    pr.set_value(pr.get(meta, pr.FMD_ROW_GROUPS).items[2], 2, 12345)  # total_byte_size
    bad = with_footer(dst, tmp_path / "bad.parquet", meta)
    res = check(src, bad, {"status": "audit_status"})
    assert not res["ok"]
    assert any(p.startswith("footer differs at 4.2.2") for p in res["problems"])


def test_check_catches_a_wrong_mapping_and_shapes(src, tmp_path):
    dst = tmp_path / "d.parquet"
    pr.rename_file(str(src), str(dst), {"status": "audit_status"})
    res = check(src, dst, {"status": "audit_state"})
    assert not res["ok"]
    assert any("should be 'audit_state'" in p for p in res["problems"])
    assert any(p.startswith("footer differs at 2.3.4") for p in res["problems"])
    assert any("ARROW:schema is not" in p for p in res["problems"])
    assert any(p.startswith("arrow:") or "pyarrow schema" in p for p in res["problems"])
    assert any(p.startswith("duckdb:") for p in res["problems"])

    meta = meta_of(dst)
    meta.append(pr.Field(30, pr.T_I32, 1, False))
    res = check(src, with_footer(dst, tmp_path / "x.parquet", meta), {"status": "audit_status"})
    assert "footer differs at : None -> None" in res["problems"]


def test_check_catches_a_bad_or_missing_arrow_schema(src, tmp_path):
    dst = tmp_path / "d.parquet"
    pr.rename_file(str(src), str(dst), {"status": "audit_status"})
    meta = meta_of(dst)
    for kv in pr.kv_items(meta, pr.FMD_KV):
        if pr.get(kv, pr.KV_KEY) == pr.ARROW_SCHEMA_KEY:
            pr.set_value(kv, pr.KV_VALUE, b"!!")
    res = check(src, with_footer(dst, tmp_path / "a.parquet", meta), {"status": "audit_status"})
    assert any("ARROW:schema does not decode" in p for p in res["problems"])
    meta = meta_of(dst)
    kvl = pr.get(meta, pr.FMD_KV)
    kvl.items[:] = [kv for kv in kvl.items if pr.get(kv, pr.KV_KEY) != pr.ARROW_SCHEMA_KEY]
    res = check(src, with_footer(dst, tmp_path / "m.parquet", meta), {"status": "audit_status"})
    assert not res["ok"]


def test_check_bloom_without_filters_and_with_empty_column(tmp_path):
    a = tmp_path / "a.parquet"
    pq.write_table(pa.table({"v": pa.array([None, None], pa.string()), "w": [1, 2]}), a,
                   bloom_filter_options={"v": True})
    b = tmp_path / "b.parquet"
    pr.rename_file(str(a), str(b), {"w": "x"})
    res = check(a, b, {"w": "x"})
    assert any("no value in row group 0" in p for p in res["problems"])
    pq.write_table(pa.table({"w": [1, 2]}), a)
    pr.rename_file(str(a), str(b), {"w": "x"})
    res = check(a, b, {"w": "x"})
    assert res["ok"] and res["bloom_columns"] == []


def test_check_reports_unreadable_files(src, tmp_path):
    junk = tmp_path / "junk"
    junk.write_bytes(b"x" * 64)
    res = check(src, junk, {})
    assert not res["ok"] and res["problems"][0].startswith("footer:")
    other = write(tmp_path / "o.parquet", meta={"tool_version": "8"})
    res = check(src, other, {})
    assert not res["ok"]


def test_check_row_group_and_footer_start_differences(src, tmp_path):
    other = tmp_path / "o.parquet"
    pq.write_table(table(), other, row_group_size=3000)
    res = check(src, other, {})
    assert any(p.startswith("footer start") for p in res["problems"])
    assert any(p.startswith("row groups 4 -> 2") for p in res["problems"])


# -------------------------------------------------------------------- CLI --
def test_cli_rename_and_check(src, tmp_path, capsys):
    mp = tmp_path / "map.json"
    mp.write_text(json.dumps({"status": "audit_status"}))
    dst = tmp_path / "d.parquet"
    assert pr.main([str(src), str(dst), "--map", str(mp), "--no-fsync"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] and out["renamed"] == {"status": "audit_status"}

    pairs = tmp_path / "pairs.tsv"
    pairs.write_text(f"# old\tnew\n\n{src}\t{dst}\n")
    assert prc.main(["--map", str(mp), "--pairs", str(pairs)]) == 0
    line = json.loads(capsys.readouterr().out)
    assert line["ok"]
    assert prc.main(["--rename", "status=wrong", str(src), str(dst)]) == 1
    assert not json.loads(capsys.readouterr().out)["ok"]


def test_cli_errors(src, tmp_path, capsys):
    assert pr.main([str(src), str(tmp_path / "d"), "--rename", "nope=x"]) == 2
    assert "no top-level column" in json.loads(capsys.readouterr().out)["error"]
    assert pr.main([str(src), str(tmp_path / "d"), "--rename", "noequals"]) == 2
    capsys.readouterr()
    assert pr.main([str(src), str(tmp_path / "d"), "--rename", "a=b", "--rename", "a=c"]) == 2
    capsys.readouterr()
    bad = tmp_path / "bad.json"
    bad.write_text("[1]")
    assert pr.main([str(src), str(tmp_path / "d"), "--map", str(bad)]) == 2
    capsys.readouterr()
    assert prc.main([str(src)]) == 2
    assert prc.main([]) == 2
    tsv = tmp_path / "p.tsv"
    tsv.write_text("only-one-column\n")
    assert prc.main(["--pairs", str(tsv)]) == 2
    assert "parquet_rename_check:" in capsys.readouterr().err
