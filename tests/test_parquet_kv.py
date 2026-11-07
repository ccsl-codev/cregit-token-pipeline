"""Tests for --set-kv: a footer-only change of a file key-value, alone or in a rename or drop pass."""

from __future__ import annotations

import copy
import hashlib
import json

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
duckdb = pytest.importorskip("duckdb")

import parquet_rename as pr  # noqa: E402
import parquet_rename_check as prc  # noqa: E402
from test_parquet_rename import meta_of, with_footer, write  # noqa: E402

KV = {"ctp_schema_version": "3"}


@pytest.fixture
def src(tmp_path):
    return write(tmp_path / "src.parquet")  # pyarrow, ARROW:schema, key tool_version = 7


@pytest.fixture
def dk(tmp_path):
    """A DuckDB-written file (no ARROW:schema) with three file key-values."""
    a = tmp_path / "dk.parquet"
    duckdb.sql(
        "COPY (SELECT i AS id, 'p' || (i % 97) AS name, (i % 3)::VARCHAR AS status, "
        "[i % 5, i % 7] AS tags, i / 7 AS score FROM range(9000) t(i)) "
        f"TO '{a}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 2048, "
        "KV_METADATA {ctp_schema_version: '2', organizations_version: 'v4.9', other: 'x'})")
    return a


def kv_of(path):
    return {k.decode(): v.decode() for k, v in (pq.ParquetFile(path).metadata.metadata or {}).items()}


def prefix_sha(path, n):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read(n)).hexdigest()


def run(src, dst, kv, mapping=None, drops=()):
    return pr.rename_file(str(src), str(dst), mapping or {}, drops=drops, kv_set=kv, copy="read-write")


def check(old, new, kv, mapping=None, drops=()):
    return prc.check_pair(str(old), str(new), mapping or {}, drops, kv)


# ----------------------------------------------------------------- set --
def test_set_kv_alone_changes_only_the_footer(dk, tmp_path):
    dst = tmp_path / "o.parquet"
    rep = run(dk, dst, KV)
    assert rep["kv_set"] == {"ctp_schema_version": ["2", "3"]}
    assert rep["kv_set_in_arrow_schema"] == []
    _, start, _ = pr.read_footer(str(dk))
    assert prefix_sha(dk, start) == prefix_sha(dst, start)
    before, after = kv_of(dk), kv_of(dst)
    assert after == dict(before, ctp_schema_version="3")
    assert pq.ParquetFile(dst).schema_arrow.metadata[b"ctp_schema_version"] == b"3"
    res = check(dk, dst, KV)
    assert res["ok"], res["problems"]
    assert res["kv_set"] == KV
    assert pq.read_table(dst).equals(pq.read_table(dk).replace_schema_metadata(
        dict(pq.read_table(dk).schema.metadata, ctp_schema_version="3")))


def test_set_kv_in_arrow_schema_metadata_too(src, tmp_path):
    dst = tmp_path / "o.parquet"
    rep = run(src, dst, {"tool_version": "8"})
    assert rep["kv_set_in_arrow_schema"] == ["tool_version"]
    assert pq.ParquetFile(dst).schema_arrow.metadata[b"tool_version"] == b"8"
    res = check(src, dst, {"tool_version": "8"})
    assert res["ok"], res["problems"]


def test_set_kv_with_rename(src, tmp_path):
    dst = tmp_path / "o.parquet"
    rep = run(src, dst, {"tool_version": "8"}, {"status": "audit_status"})
    assert rep["column_chunks_renamed"] > 0 and rep["kv_set"]["tool_version"] == ["7", "8"]
    res = check(src, dst, {"tool_version": "8"}, {"status": "audit_status"})
    assert res["ok"], res["problems"]
    assert "audit_status" in pq.ParquetFile(dst).schema_arrow.names


@pytest.mark.parametrize("fixture", ["src", "dk"])
def test_set_kv_with_drops(fixture, request, tmp_path):
    f = request.getfixturevalue(fixture)
    kv = {"tool_version": "8"} if fixture == "src" else KV
    dst = tmp_path / "o.parquet"
    rep = run(f, dst, kv, {"status": "audit_status"}, ["tags", "name"])
    assert rep["leaves_dropped"] >= 2 and set(rep["kv_set"]) == set(kv)
    res = check(f, dst, kv, {"status": "audit_status"}, ["tags", "name"])
    assert res["ok"], res["problems"]
    def plain(p):
        return {k: v for k, v in kv_of(p).items() if k != "ARROW:schema"}

    assert plain(dst) == dict(plain(f), **kv)


def test_cli_set_kv(dk, tmp_path, capsys):
    dst = tmp_path / "o.parquet"
    args = ["--drop", "tags", "--rename", "status=audit_status", "--set-kv", "ctp_schema_version=3"]
    assert pr.main([str(dk), str(dst), *args, "--copy", "read-write", "--no-fsync"]) == 0
    assert json.loads(capsys.readouterr().out)["kv_set"] == {"ctp_schema_version": ["2", "3"]}
    assert prc.main([*args, str(dk), str(dst)]) == 0
    assert json.loads(capsys.readouterr().out)["ok"]
    assert prc.main(["--drop", "tags", "--rename", "status=audit_status", str(dk), str(dst)]) == 1
    capsys.readouterr()
    assert pr.main([str(dk), str(tmp_path / "x.parquet"), "--set-kv", "nokey=1"]) == 2
    assert "no such key-value" in json.loads(capsys.readouterr().out)["error"]
    assert prc.main(["--set-kv", "=1", str(dk), str(dst)]) == 2


# ------------------------------------------------------------- refusals --
@pytest.mark.parametrize("pairs, match", [
    (["=1"], "empty key"),
    (["a=1", "a=2"], "given twice"),
    (["ARROW:schema=x"], "ARROW:schema"),
    (["novalue"], "expected KEY=VALUE"),
])
def test_bad_set_kv_args_are_refused(pairs, match):
    with pytest.raises(pr.RenameError, match=match):
        pr.parse_kv(pairs)


def test_set_kv_refuses_footers(src, dk, tmp_path):
    with pytest.raises(pr.RenameError, match="no such key-value"):
        run(dk, tmp_path / "a.parquet", {"missing": "1"})
    with pytest.raises(pr.RenameError, match="no such key-value"):
        run(dk, tmp_path / "b.parquet", {"missing": "1"}, drops=["tags"])
    with pytest.raises(pr.RenameError, match="ARROW:schema"):
        run(src, tmp_path / "c.parquet", {"ARROW:schema": "x"})
    m = meta_of(dk)
    kvs = pr.get(m, pr.FMD_KV)
    kvs.items.append(copy.deepcopy(kvs.items[0]))
    two = with_footer(dk, tmp_path / "two.parquet", m)
    key = pr.get(kvs.items[0], pr.KV_KEY).decode()
    with pytest.raises(pr.RenameError, match="such key-values"):
        run(two, tmp_path / "d.parquet", {key: "1"})
    m = meta_of(dk)
    kv0 = pr.get(m, pr.FMD_KV).items[0]
    kv0[:] = [f for f in kv0 if f.fid != pr.KV_VALUE]
    novalue = with_footer(dk, tmp_path / "nv.parquet", m)
    with pytest.raises(pr.RenameError, match="no value field"):
        run(novalue, tmp_path / "e.parquet", {pr.get(kv0, pr.KV_KEY).decode(): "1"})
    assert not list(tmp_path.glob("[a-e].parquet"))


# --------------------------------------------------------------- checks --
def test_check_finds_a_wrong_or_extra_kv_change(dk, src, tmp_path):
    dst = tmp_path / "o.parquet"
    run(dk, dst, {"ctp_schema_version": "3", "other": "y"})
    res = check(dk, dst, KV)  # the change of other is not expected
    assert not res["ok"] and any("footer differs" in p for p in res["problems"])
    assert not check(dk, dst, {"ctp_schema_version": "4", "other": "y"})["ok"]
    res = check(dk, dst, {"missing": "1"})
    assert not res["ok"] and any("no such key-value" in p for p in res["problems"])
    d2 = tmp_path / "d2.parquet"
    run(dk, d2, KV, drops=["tags"])
    assert check(dk, d2, KV, drops=["tags"])["ok"]
    res = check(dk, d2, {}, drops=["tags"])
    assert not res["ok"] and any("footer differs" in p for p in res["problems"])
    a2 = tmp_path / "a2.parquet"
    run(src, a2, {"tool_version": "8"}, drops=["tags"])
    res = check(src, a2, {"tool_version": "9"}, drops=["tags"])
    assert not res["ok"] and any("ARROW:schema" in p for p in res["problems"])


def test_check_finds_a_kv_change_the_tool_did_not_make(dk, tmp_path):
    m = meta_of(dk)
    for kv in pr.get(m, pr.FMD_KV).items:
        if pr.get(kv, pr.KV_KEY) == b"organizations_version":
            pr.set_value(kv, pr.KV_VALUE, b"v5.0")
    bad = with_footer(dk, tmp_path / "bad.parquet", m)
    res = check(dk, bad, {})
    assert not res["ok"] and any("footer differs" in p for p in res["problems"])
    assert not check(dk, bad, {"organizations_version": "v4.9"})["ok"]
    assert check(dk, bad, {"organizations_version": "v5.0"})["ok"]
