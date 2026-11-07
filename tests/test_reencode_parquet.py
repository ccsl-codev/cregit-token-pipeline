"""Tests of reencode_parquet.py, on synthetic Parquet files only.

The files have the column kinds that the default settings name (the PLAIN, the DELTA_BINARY_PACKED and the Bloom
filter columns), other text, integer and boolean columns, a list column, NULL values, several row groups and footer
key-value entries. The row groups of the output are made small, so the output has several row groups too.
The tests skip when pyarrow or duckdb is missing.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
duckdb = pytest.importorskip("duckdb")

import reencode_parquet as R  # noqa: E402

KV = [("ctp_schema_version", "2"), ("example_version", "1"), ("example_sha256", "7b" * 32)]
COLUMNS = [
    ("repo_name", "VARCHAR"), ("file_path", "VARCHAR"), ("token_index", "BIGINT"), ("token_value", "VARCHAR"),
    ("source_text", "VARCHAR"), ("source_line", "BIGINT"), ("source_col", "BIGINT"),
    ("cregit_commit_sha", "VARCHAR"), ("personid", "VARCHAR"), ("is_merge", "BOOLEAN"),
    ("commit_summary", "VARCHAR"), ("footer_signed_off_by", "VARCHAR[]"),
]
# The settings of the published files, with the footer key check of the dataset build.
RELEASE_CONFIG = {"expect_kv_keys": ["ctp_schema_version", "organizations_version", "organizations_mapping_sha256",
                                     "pseudonymization_version", "pseudonymization_registry_sha256"]}
RELEASE_SETTINGS_SHA256 = "e98a53aed2038837fae2cf20d9de407729517f9290faaa44ab33c3762c9fcfbb"
ANY_VERSION = ["--pyarrow-version", "", "--duckdb-version", ""]


def literal(name: str, typ: str) -> str:
    if typ == "VARCHAR[]":
        return (f"(CASE WHEN i % 7 = 0 THEN NULL WHEN i % 3 = 0 THEN []::VARCHAR[] "
                f"ELSE ['{name}-' || (i % 5)::VARCHAR, 'v' || (i % 11)::VARCHAR] END) AS {name}")
    if typ == "BIGINT":
        return f"(i + {len(name)})::BIGINT AS {name}"
    if typ == "BOOLEAN":
        return f"(i % 2 = 0) AS {name}"
    if name in ("file_path", "personid", "cregit_commit_sha"):
        return f"'{name}-' || (i // 97)::VARCHAR AS {name}"
    if name in ("token_value", "source_text"):
        return f"(CASE WHEN i % 13 = 0 THEN NULL ELSE 'tok' || (i % 389)::VARCHAR END) AS {name}"
    return f"(CASE WHEN i % 17 = 0 THEN NULL ELSE '{name}:' || (i % 23)::VARCHAR END) AS {name}"


def make_file(path: Path, rows: int = 5000, kv=KV, row_group: int = 2048) -> Path:
    sel = ", ".join(literal(n, t) for n, t in COLUMNS)
    kvs = ", ".join(f"{k}: '{v}'" for k, v in kv)
    path.parent.mkdir(parents=True, exist_ok=True)
    duckdb.execute(f"COPY (SELECT {sel} FROM range({rows}) t(i) ORDER BY i) TO '{path}' "
                   f"(FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE {row_group}, KV_METADATA {{{kvs}}})")
    return path


def sha(p) -> str:
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


@pytest.fixture(autouse=True)
def small_groups():
    R.configure({"row_group_rows": 1500, "pyarrow_version": "", "duckdb_version": ""})
    yield
    R.configure()


def test_defaults_are_the_settings_of_the_published_files():
    R.configure()
    assert R.PYARROW_VERSION == "26.0.1" and R.DUCKDB_VERSION == "1.5.5"
    assert R.SETTINGS["version"] == "2.6" and R.SETTINGS["data_page_version"] == "1.0"
    assert R.SETTINGS["compression"] == "zstd" and R.SETTINGS["compression_level"] == 19
    assert R.ROW_GROUP_ROWS == 1_000_000 and R.SETTINGS["data_page_size"] == 8 << 20
    assert R.NO_DICTIONARY == ["token_value", "source_text", "source_col"]
    assert R.COLUMN_ENCODING == {"source_line": "DELTA_BINARY_PACKED", "token_index": "DELTA_BINARY_PACKED"}
    assert R.BLOOM_COLUMNS == ["file_path", "token_value", "source_text", "cregit_commit_sha", "personid"]
    R.configure(RELEASE_CONFIG)
    assert R.SETTINGS_SHA256 == RELEASE_SETTINGS_SHA256


def test_versions_are_checked():
    v = R.versions()
    assert set(v) == {"python", "pyarrow", "duckdb", "reencode_sha256"}
    R.configure({"pyarrow_version": v["pyarrow"], "duckdb_version": v["duckdb"]})
    R.check_versions()
    R.configure({"pyarrow_version": "", "duckdb_version": ""})
    R.check_versions()
    R.configure({"pyarrow_version": "0.0.1"})
    with pytest.raises(SystemExit, match="ABORT"):
        R.check_versions()


@pytest.mark.parametrize("bad, msg", [
    ({"nope": 1}, "unknown settings"),
    ({"compression": "xz"}, "compression"),
    ({"data_page_version": "3.0"}, "data_page_version"),
    ({"row_group_rows": 0}, "row_group_rows"),
    ({"bloom_fpp": 1.5}, "bloom_fpp"),
    ({"column_encoding": {"a": "WEIRD"}}, "column_encoding"),
    ({"no_dictionary": ["token_index"]}, "no_dictionary and column_encoding"),
])
def test_bad_settings_are_refused(bad, msg):
    with pytest.raises(ValueError, match=msg):
        R.configure(bad)


def test_settings_from_options_and_config_file(tmp_path, capsys):
    cfg = tmp_path / "c.json"
    cfg.write_text(json.dumps({"compression_level": 3, "bloom_columns": ["file_path"]}))
    assert R.main(["settings", "--config", str(cfg), "--row-group-rows", "10", "--no-dictionary", "source_text",
                   "--column-encoding", "token_index=DELTA_BINARY_PACKED"]) == 0
    out = capsys.readouterr().out
    assert R.CFG["compression_level"] == 3 and R.CFG["row_group_rows"] == 10
    assert R.CFG["bloom_columns"] == ["file_path"] and R.CFG["no_dictionary"] == ["source_text"]
    assert R.CFG["column_encoding"] == {"token_index": "DELTA_BINARY_PACKED"}
    assert f"settings_sha256 {R.SETTINGS_SHA256}" in out
    assert R.main(["settings", "--no-bloom"]) == 0 and R.CFG["bloom_columns"] == []
    with pytest.raises(SystemExit):
        R.main(["settings", "--column-encoding", "token_index"])
    cfg.write_text("[1]")
    with pytest.raises(SystemExit):
        R.main(["settings", "--config", str(cfg)])


def test_release_settings_on_the_command_line(capsys):
    args = ["settings"]
    for k in RELEASE_CONFIG["expect_kv_keys"]:
        args += ["--expect-kv-key", k]
    assert R.main(args) == 0
    assert capsys.readouterr().out.strip().endswith(RELEASE_SETTINGS_SHA256)


def test_one_file_ok(tmp_path):
    src = make_file(tmp_path / "in" / "alpha__one.parquet")
    dst = tmp_path / "out" / src.name
    dst.parent.mkdir()
    rec = R.reencode_one(str(src), str(dst), want_sha=sha(src), threads=2, mem="1GB", tmp=str(tmp_path))
    assert rec["status"] == "OK", rec["error"]
    assert all(rec["checks"].values())
    assert dst.is_file() and not Path(str(dst) + ".tmp").exists()
    assert rec["out_sha256"] == sha(dst) and rec["in_sha256"] == sha(src)
    assert rec["rows"] == 5000 and rec["columns"] == len(COLUMNS)
    md = pq.ParquetFile(dst).metadata
    assert [md.row_group(i).num_rows for i in range(md.num_row_groups)] == [1500, 1500, 1500, 500]
    assert set(rec["encodings"]) <= R.ALLOWED_ENCODINGS
    assert "DELTA_BINARY_PACKED" in rec["encodings"]
    assert set(rec["page_types"]) <= {"DATA_PAGE", "DICTIONARY_PAGE"}
    assert md.metadata is not None and b"ARROW:schema" not in md.metadata
    cols = {md.row_group(0).column(j).path_in_schema: md.row_group(0).column(j) for j in range(md.num_columns)}
    for c in R.BLOOM_COLUMNS:
        assert cols[c].bloom_filter_offset
    assert not cols["repo_name"].bloom_filter_offset
    assert cols["token_value"].encodings and "RLE_DICTIONARY" not in cols["token_value"].encodings
    assert "DELTA_BINARY_PACKED" in cols["token_index"].encodings
    assert "RLE_DICTIONARY" in cols["footer_signed_off_by.list.element"].encodings
    assert rec["settings_sha256"] == R.SETTINGS_SHA256
    line = json.dumps(rec)
    assert chr(64) not in line and str(tmp_path) not in line   # no address, no machine path in the log


def test_other_settings_are_written_and_checked(tmp_path):
    R.configure({"row_group_rows": 1000, "compression": "gzip", "compression_level": 6, "data_page_version": "2.0",
                 "bloom_columns": [], "column_encoding": {"source_line": "DELTA_BINARY_PACKED"},
                 "no_dictionary": ["token_value"], "pyarrow_version": "", "duckdb_version": ""})
    src = make_file(tmp_path / "theta__eight.parquet", rows=2500)
    dst = tmp_path / "o.parquet"
    rec = R.reencode_one(str(src), str(dst))
    assert rec["status"] == "OK", rec["error"]
    md = pq.ParquetFile(dst).metadata
    assert [md.row_group(i).num_rows for i in range(md.num_row_groups)] == [1000, 1000, 500]
    assert "DATA_PAGE_V2" in rec["page_types"] and rec["bloom_bytes"] == 0
    assert md.row_group(0).column(0).compression == "GZIP"


def test_footer_keys_survive_in_order(tmp_path):
    src = make_file(tmp_path / "beta__two.parquet", rows=900)
    dst = tmp_path / "o.parquet"
    rec = R.reencode_one(str(src), str(dst))
    assert rec["status"] == "OK", rec["error"]
    got = [tuple(x) for x in R.kv_list(str(dst))]
    assert got == [tuple(x) for x in R.kv_list(str(src))] == KV
    assert rec["kv"] == [list(x) for x in KV]


def test_expected_footer_keys(tmp_path):
    R.configure({"row_group_rows": 1500, "expect_kv_keys": [k for k, _ in KV], "pyarrow_version": "",
                 "duckdb_version": ""})
    src = make_file(tmp_path / "gamma__three.parquet", rows=300)
    assert R.reencode_one(str(src), str(tmp_path / "ok.parquet"))["status"] == "OK"
    src2 = make_file(tmp_path / "gamma__four.parquet", rows=300, kv=KV + [("extra_key", "1")])
    dst = tmp_path / "o.parquet"
    rec = R.reencode_one(str(src2), str(dst))
    assert rec["status"] == "FAIL" and "kv_input_keys" in rec["error"]
    assert not dst.exists() and not Path(str(dst) + ".tmp").exists()


def _patched_write(change):
    """A writer that writes the right file, then changes it with change(table) (the output of a broken writer)."""
    real = R.write

    def w(src, out, ndv, kv, threads, mem, tmp, read_batch=R.READ_BATCH):
        real(src, out, ndv, kv, threads, mem, tmp, read_batch=read_batch)
        t = change(pq.read_table(out))
        with pq.ParquetWriter(out, t.schema, **R.writer_kwargs(t.schema, ndv)) as wr:
            wr.write_table(t, row_group_size=R.ROW_GROUP_ROWS)
            wr.add_key_value_metadata(dict(kv))
    return w


def test_changed_value_fails(tmp_path, monkeypatch):
    src = make_file(tmp_path / "delta__four.parquet", rows=3000)

    def change(t):
        i = t.schema.get_field_index("token_value")
        vals = t.column(i).to_pylist()
        vals[1234] = (vals[1234] or "") + "x"
        return t.set_column(i, "token_value", pa.array(vals, pa.string()))
    monkeypatch.setattr(R, "write", _patched_write(change))
    dst = tmp_path / "o.parquet"
    rec = R.reencode_one(str(src), str(dst))
    assert rec["status"] == "FAIL"
    assert {"rowhash", "colhash", "orderhash"} <= {k for k, v in rec["checks"].items() if not v}
    assert rec["checks"]["rows"] and rec["checks"]["kv"]
    assert not dst.exists()


def test_changed_order_fails(tmp_path, monkeypatch):
    src = make_file(tmp_path / "epsilon__five.parquet", rows=2000)

    def change(t):
        idx = list(range(t.num_rows))
        idx[10], idx[11] = idx[11], idx[10]
        return t.take(pa.array(idx))
    monkeypatch.setattr(R, "write", _patched_write(change))
    rec = R.reencode_one(str(src), str(tmp_path / "o.parquet"))
    assert rec["status"] == "FAIL"
    bad = {k for k, v in rec["checks"].items() if not v}
    assert bad == {"orderhash"}, bad    # the same rows and values, another order


def test_writer_error_is_an_error_record(tmp_path, monkeypatch):
    src = make_file(tmp_path / "iota__nine.parquet", rows=100)

    def boom(*a, **k):
        raise RuntimeError("disk full")
    monkeypatch.setattr(R, "write", boom)
    rec = R.reencode_one(str(src), str(tmp_path / "o.parquet"))
    assert rec["status"] == "ERROR" and "disk full" in rec["error"]


def test_pinned_sha_mismatch_fails(tmp_path):
    src = make_file(tmp_path / "zeta__six.parquet", rows=200)
    rec = R.reencode_one(str(src), str(tmp_path / "o.parquet"), want_sha="0" * 64)
    assert rec["status"] == "FAIL" and "in_sha256_pinned" in rec["error"]


def test_ndv_rule_is_the_same_in_pyarrow_and_duckdb(tmp_path):
    src = make_file(tmp_path / "eta__seven.parquet", rows=4100)
    assert R.ndv_pyarrow(str(src)) == R.ndv_duckdb(str(src), 2, "1GB", None)


def test_page_scan_finds_v2_pages_and_other_encodings(tmp_path):
    t = pa.table({"a": pa.array(range(5000), pa.int64()), "b": pa.array([float(i) for i in range(5000)])})
    p = tmp_path / "v2.parquet"
    pq.write_table(t, p, data_page_version="2.0", use_dictionary=False,
                   column_encoding={"a": "DELTA_BINARY_PACKED", "b": "BYTE_STREAM_SPLIT"})
    s = R.scan_pages(str(p))
    assert "DATA_PAGE_V2" in s["page_types"]
    assert "BYTE_STREAM_SPLIT" in s["page_encodings"] and "DELTA_BINARY_PACKED" in s["page_encodings"]
    p1 = tmp_path / "v1.parquet"
    pq.write_table(t, p1, data_page_version="1.0")
    s1 = R.scan_pages(str(p1))
    assert set(s1["page_types"]) == {"DATA_PAGE", "DICTIONARY_PAGE"}


def test_one_command(tmp_path, capsys):
    src = make_file(tmp_path / "kappa__ten.parquet", rows=400)
    dst, log, res = tmp_path / "o.parquet", tmp_path / "l.jsonl", tmp_path / "r.json"
    base = ["one", str(src), str(dst), "--row-group-rows", "150"] + ANY_VERSION
    assert R.main(base + ["--log", str(log)]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "OK"
    assert json.loads(log.read_text())["status"] == "OK"
    assert R.main(base + ["--result", str(res), "--want-sha", "0" * 64]) == 1
    assert json.loads(res.read_text())["status"] == "FAIL"
    with pytest.raises(SystemExit):
        R.main(base + ["--read-batch", "0"])


def test_log_refuses_the_at_sign(tmp_path):
    with pytest.raises(SystemExit):
        R.append_log(str(tmp_path / "l.jsonl"), {"file": "a" + chr(64) + "b"})


def test_driver_runs_resumes_and_redoes_a_changed_output(tmp_path):
    src = tmp_path / "src"
    names = []
    for i, n in enumerate(["a__a", "b__b", "c__c"]):
        names.append(make_file(src / f"{n}.parquet", rows=800 + 700 * i).name)
    sums = tmp_path / "SHA256SUMS"
    sums.write_text("".join(f"{sha(src / n)}  parquet/{n}\n" for n in sorted(names)))
    dst, log = tmp_path / "dst", tmp_path / "log.jsonl"
    args = ["run", "--src", str(src), "--dst", str(dst), "--log", str(log), "--sums", str(sums), "--jobs", "2",
            "--mem-total", "8", "--mem-small", "1", "--mem-kill", "6", "--summary", str(tmp_path / "s.json"),
            "--progress", str(tmp_path / "p.json"), "--work", str(tmp_path / "work"),
            "--row-group-rows", "1000"] + ANY_VERSION
    assert R.main(args) == 0
    lines = [json.loads(x) for x in log.read_text().splitlines()]
    assert sorted(r["file"] for r in lines) == sorted(names) and all(r["status"] == "OK" for r in lines)
    assert lines[0]["file"] == "c__c.parquet"            # LPT: the largest first
    assert pq.ParquetFile(dst / "c__c.parquet").metadata.num_row_groups == 3   # the children get the settings
    s = json.loads((tmp_path / "s.json").read_text())
    assert s["complete"] and s["status"] == {"OK": 3} and s["in_sha256_pinned"] == 3
    assert all(r["driver_peak_rss_bytes"] > 0 for r in lines)
    # the second run skips all; a changed output is done again
    assert R.main(args) == 0
    assert len(log.read_text().splitlines()) == 3
    b = bytearray((dst / names[0]).read_bytes())
    b[100] ^= 1
    (dst / names[0]).write_bytes(bytes(b))
    assert R.main(args) == 0
    lines = [json.loads(x) for x in log.read_text().splitlines()]
    assert len(lines) == 4 and lines[-1]["file"] == names[0] and lines[-1]["status"] == "OK"
    # summary: complete with the settings of the run, not complete with other settings
    summ = ["summary", "--log", str(log), "--sums", str(sums), "--out", str(tmp_path / "s2.json")] + ANY_VERSION
    assert R.main(summ + ["--row-group-rows", "1000"]) == 0
    assert R.main(summ) == 1
    assert R.main(["summary", "--log", str(log), "--src", str(src), "--row-group-rows", "1000"] + ANY_VERSION) == 0
    # make-sums: the data rows from the log, every other file from its bytes
    root = tmp_path / "root"
    (root / "parquet").mkdir(parents=True)
    (root / "other" / "deep").mkdir(parents=True)
    for n in names:
        os.link(dst / n, root / "parquet" / n)
    (root / "other" / "info.parquet").write_bytes(b"x")
    (root / "other" / "deep" / "notes.txt").write_bytes(b"y")
    out = root / "SHA256SUMS"
    make_sums = ["make-sums", "--root", str(root), "--log", str(log), "--out", str(out)]
    assert R.main(make_sums) == 0
    rows = [ln.split("  ") for ln in out.read_text().splitlines()]
    assert [r for _, r in rows] == (["other/deep/notes.txt", "other/info.parquet"]
                                    + [f"parquet/{n}" for n in sorted(names)])
    assert all(h == sha(root / r) for h, r in rows)
    (root / "other" / ".hidden").write_bytes(b"z")
    with pytest.raises(SystemExit, match="unexpected entry"):
        R.main(make_sums)
    (root / "other" / ".hidden").unlink()
    (root / "parquet" / names[0]).unlink()
    (root / "parquet" / names[0]).write_bytes(b"short")
    with pytest.raises(SystemExit, match="is not the logged"):
        R.main(make_sums)
    (root / "parquet" / "x.txt").write_bytes(b"")
    with pytest.raises(SystemExit, match="other files"):
        R.main(make_sums)


def test_driver_stops_on_a_wrong_sums_list(tmp_path):
    src = tmp_path / "src"
    make_file(src / "a__a.parquet", rows=100)
    sums = tmp_path / "SHA256SUMS"
    sums.write_text(f"{'0' * 64}  data/b__b.parquet\n")
    rc = R.main(["run", "--src", str(src), "--dst", str(tmp_path / "d"), "--log", str(tmp_path / "l.jsonl"),
                 "--sums", str(sums), "--sums-prefix", "data/", "--jobs", "1"] + ANY_VERSION)
    assert rc == 2 and not (tmp_path / "l.jsonl").exists()


def test_driver_stop_file(tmp_path):
    src = tmp_path / "src"
    make_file(src / "a__a.parquet", rows=100)
    stop = tmp_path / "STOP"
    stop.write_text("")
    rc = R.main(["run", "--src", str(src), "--dst", str(tmp_path / "d"), "--log", str(tmp_path / "l.jsonl"),
                 "--jobs", "1", "--stop-file", str(stop), "--work", str(tmp_path / "w")] + ANY_VERSION)
    assert rc == 3


def test_driver_memory_kill_is_an_error_line(tmp_path):
    src = tmp_path / "src"
    make_file(src / "a__a.parquet", rows=200000, row_group=100000)
    log = tmp_path / "l.jsonl"
    rc = R.main(["run", "--src", str(src), "--dst", str(tmp_path / "d"), "--log", str(log), "--jobs", "1",
                 "--mem-kill", "0.05", "--work", str(tmp_path / "w")] + ANY_VERSION)
    assert rc == 1
    r = json.loads(log.read_text().splitlines()[-1])
    assert r["status"] == "ERROR" and "memory limit" in r["error"]
    assert not (tmp_path / "d" / "a__a.parquet").exists()


def test_read_batch_is_logged_and_keeps_the_content(tmp_path):
    src = make_file(tmp_path / "a__a.parquet", rows=6000)
    r1 = R.reencode_one(str(src), str(tmp_path / "o1.parquet"))
    r2 = R.reencode_one(str(src), str(tmp_path / "o2.parquet"), read_batch=1000)
    assert r1["status"] == "OK" and r2["status"] == "OK", (r1.get("error"), r2.get("error"))
    assert r1["read_batch"] == R.READ_BATCH and r2["read_batch"] == 1000
    assert r1["settings_sha256"] == r2["settings_sha256"] == R.SETTINGS_SHA256
    assert r1["fingerprint"] == r2["fingerprint"] and r1["rows"] == r2["rows"] == 6000


def test_driver_read_batch_for_one_file_and_two_tool_versions(tmp_path):
    src = tmp_path / "src"
    names = [make_file(src / f"{n}.parquet", rows=900 + 300 * i).name for i, n in enumerate(["a__a", "b__b"])]
    dst, log = tmp_path / "dst", tmp_path / "log.jsonl"
    base = ["run", "--src", str(src), "--dst", str(dst), "--log", str(log), "--jobs", "1", "--mem-total", "8",
            "--mem-small", "1", "--mem-kill", "6", "--work", str(tmp_path / "w"),
            "--summary", str(tmp_path / "s.json")] + ANY_VERSION
    assert R.main(base + ["--read-batch-for", "nope.parquet=10"]) == 2 and not log.exists()
    assert R.main(base + ["--read-batch-for", f"{names[0]}=0"]) == 2
    assert R.main(base + ["--read-batch", "0"]) == 2
    assert R.main(base + ["--read-batch-for", f"{names[0]}=256", "--only", names[0], names[1]]) == 0
    lines = {json.loads(x)["file"]: json.loads(x) for x in log.read_text().splitlines()}
    assert lines[names[0]]["read_batch"] == 256 and lines[names[1]]["read_batch"] == R.READ_BATCH
    # a line of another tool version (another sha256, no read_batch key): still complete, both listed
    old = dict(lines[names[1]])
    old["versions"] = dict(old["versions"], reencode_sha256="ab" * 32)
    old.pop("read_batch")
    log.write_text(json.dumps(lines[names[0]]) + "\n" + json.dumps(old) + "\n")
    s = R.summarize(R.read_log(str(log)), sorted(names), {})
    assert s["complete"] and len(s["versions"]) == 1 and len(s["reencode_sha256"]) == 2
    assert s["read_batch"] == {"256": 1, str(R.READ_BATCH): 1} and s["read_batch_not_default"] == [names[0]]
    # another pyarrow version is not complete
    old["versions"]["pyarrow"] = "1.0.0"
    log.write_text(json.dumps(lines[names[0]]) + "\n" + json.dumps(old) + "\n")
    assert not R.summarize(R.read_log(str(log)), sorted(names), {})["complete"]
