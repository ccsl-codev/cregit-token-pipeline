"""Tests for one pass of parquet_rename.py that drops, renames, sets key-values and values."""

from __future__ import annotations

import json

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
duckdb = pytest.importorskip("duckdb")

import parquet_rename as pr  # noqa: E402
import parquet_rename_check as prc  # noqa: E402
from test_parquet_replace import write_proj  # noqa: E402

DROPS = ["tags", "score"]
MAPPING = {"kind": "kind_of_file"}
KV = {"tool_version": "8"}


@pytest.fixture
def src(tmp_path):
    return write_proj(tmp_path / "src.parquet")


def test_all_four_changes_in_one_pass(src, tmp_path):
    dst = tmp_path / "dst.parquet"
    rep = pr.rename_file(str(src), str(dst), MAPPING, drops=DROPS, kv_set=KV,
                         values={"language": "Rust", "license": "MIT"},
                         expect={"language": "C", "license": None}, copy="read-write")
    assert rep["dropped"] == DROPS and rep["renamed"] == MAPPING
    assert rep["kv_set"] == {"tool_version": ["7", "8"]}
    assert set(rep["replaced"]) == {"language", "license"}
    md = pq.ParquetFile(dst).metadata
    assert md.metadata[b"tool_version"] == b"8"
    t = pq.read_table(dst)
    assert t.column_names == ["id", "language", "license", "owner", "kind_of_file"]
    assert t.schema.metadata[b"tool_version"] == b"8"
    assert set(t.column("language").to_pylist()) == {"Rust"}
    assert set(t.column("license").to_pylist()) == {"MIT"}
    res = prc.check_pair(str(src), str(dst), MAPPING, DROPS,
                         {"language": "Rust", "license": "MIT"},
                         {"language": "C", "license": None}, kv_set=KV)
    assert res["ok"], res["problems"]
    assert res["kv_set"] == KV


def test_check_needs_the_kv_of_the_pass(src, tmp_path):
    dst = tmp_path / "dst.parquet"
    pr.rename_file(str(src), str(dst), MAPPING, drops=DROPS, kv_set=KV,
                   values={"language": "Rust"}, copy="read-write")
    res = prc.check_pair(str(src), str(dst), MAPPING, DROPS, {"language": "Rust"})
    assert not res["ok"]
    assert any("footer" in p or "schema" in p for p in res["problems"])


def test_a_pass_without_values_is_the_kv_pass(src, tmp_path):
    a, b = tmp_path / "a.parquet", tmp_path / "b.parquet"
    pr.rename_file(str(src), str(a), MAPPING, drops=DROPS, kv_set=KV, copy="read-write")
    pr.rename_file(str(src), str(b), MAPPING, drops=DROPS, kv_set=KV, values={},
                   copy="read-write")
    assert a.read_bytes() == b.read_bytes()


def test_values_tsv_with_drops_and_kv_on_the_command_line(src, tmp_path, capsys):
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    tsv = tmp_path / "values.tsv"
    tsv.write_text(f"{src}\tlanguage\tC\tJava\n{src}\tlicense\t\\N\tGPL-3.0;MIT\n")
    args = ["--values", str(tsv), "--out-dir", str(out_dir), "--drop", "tags",
            "--rename", "kind=kind_of_file", "--set-kv", "tool_version=8"]
    assert pr.main(args + ["--copy", "read-write"]) == 0
    line = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert line["ok"] and line["kv_set"] == {"tool_version": ["7", "8"]}
    dst = out_dir / src.name
    assert set(pq.read_table(dst).column("license").to_pylist()) == {"GPL-3.0;MIT"}
    assert prc.main(args) == 0
    bad = tmp_path / "bad.tsv"
    bad.write_text(f"{src}\tlanguage\tGo\tJava\n")
    assert prc.main(["--values", str(bad), "--out-dir", str(out_dir), "--drop", "tags",
                     "--rename", "kind=kind_of_file", "--set-kv", "tool_version=8"]) == 1


def test_values_refuse_a_wrong_old_value_with_kv(src, tmp_path, capsys):
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    tsv = tmp_path / "values.tsv"
    tsv.write_text(f"{src}\tlanguage\t\tJava\n")
    assert pr.main(["--values", str(tsv), "--out-dir", str(out_dir),
                    "--set-kv", "tool_version=8"]) == 2
    line = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert line["ok"] is False and "language" in line["error"]
    assert not (out_dir / src.name).exists()
