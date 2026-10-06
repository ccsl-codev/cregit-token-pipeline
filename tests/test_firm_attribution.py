"""firm_attribution.py and the two lookup tables it ships with. The data tests read
the committed files on purpose: they are what every corpus run joins against."""
from __future__ import annotations

import csv
import sys
from collections import Counter
from pathlib import Path

import pytest

import firm_attribution as fa
from validate_schema import CREGIT_COLUMNS, EXPECTED_COLUMNS

DUCKDB_IS_STUBBED = getattr(sys.modules["duckdb"], "__doc__", "").startswith("Test stub")
needs_duckdb = pytest.mark.skipif(DUCKDB_IS_STUBBED, reason="needs real duckdb")

MAP_HEADER = "domain,company,kind,source"
CANON_HEADER = "firm_raw,firm,decision,note"

# The SELECT and JOIN that cregit feat/firm-attribution e215e16 built in firm_sql(),
# copied verbatim with both tables given; `e` there is the persons row behind person_domain.
OLD_CREGIT_JOIN = """
    SELECT e.token_index,
           coalesce(fm.company, '')          AS firm_raw,
           coalesce(nullif(fc.firm, ''), fm.company, '') AS firm,
           coalesce(fm.source, '')           AS firm_source
    FROM (SELECT token_index, person_domain AS domain FROM read_parquet($src)) e
            LEFT JOIN (SELECT lower(domain) AS domain, company, source
                       FROM read_csv_auto($firm_map,
                                          header=true, all_varchar=true)) fm
                   ON fm.domain = lower(e.domain)
            LEFT JOIN (SELECT lower(trim(firm_raw)) AS firm_raw, firm
                       FROM read_csv_auto($firm_canonical,
                                          header=true, all_varchar=true)) fc
                   ON fc.firm_raw = lower(trim(fm.company))
    ORDER BY e.token_index
"""


def rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def write_csv(path: Path, header: str, *lines: str) -> Path:
    path.write_text("\n".join((header, *lines)) + "\n", encoding="utf-8")
    return path


def cregit_parquet(path: Path, domains: list[str | None]) -> Path:
    """cregit's 67 columns, one token per domain, token_index in row order."""
    import duckdb

    values = {"repo_name": "'proj'", "file_path": "'a.c'", "token_index": "d.i",
              "person_domain": "d.dom"}
    select = ", ".join(f"{values.get(n, f'CAST(NULL AS {t})')} AS {n}"
                       for n, t in CREGIT_COLUMNS)
    con = duckdb.connect()
    con.execute("CREATE TABLE d (i BIGINT, dom VARCHAR)")
    con.executemany("INSERT INTO d VALUES (?, ?)", list(enumerate(domains)))
    con.execute(f"COPY (SELECT {select} FROM d ORDER BY i) TO $p (FORMAT PARQUET)",
                {"p": str(path)})
    con.close()
    return path


def query(sql: str, **params) -> list[tuple]:
    import duckdb

    return duckdb.connect().execute(sql, {k: str(v) for k, v in params.items()}).fetchall()


@pytest.fixture
def tables(tmp_path):
    firm_map = write_csv(
        tmp_path / "map.csv", MAP_HEADER,
        "intel.com,Intel,company,gitdm",
        "Upper.Example,Upper Co,company,patch",
        "ibm.com,International Business Machines,company,cncf-gitdm",
        "us.ibm.com,  ibm corp ,company,rich",
        "360.cn,360,company,gitdm",
        "blank.example,Blank Canon,company,gitdm",
        "gmail.com,(Independent),free_provider,builtin")
    canon = write_csv(
        tmp_path / "canon.csv", CANON_HEADER,
        "International Business Machines,IBM,merge,full name",
        "IBM Corp,IBM,merge,suffix and case differ from the map",
        "Blank Canon,,merge,empty canonical falls back to firm_raw")
    return firm_map, canon


EDGE_DOMAINS = ["intel.com", "INTEL.COM", "upper.example", "ibm.com", "us.ibm.com",
                "360.cn", "blank.example", "gmail.com", "unmapped.example", "", None]


# --------------------------------------------------------------------------- #
# the step against the old cregit join
# --------------------------------------------------------------------------- #

@needs_duckdb
def test_the_firm_columns_match_the_old_cregit_join(tmp_path, tables):
    firm_map, canon = tables
    parquet = cregit_parquet(tmp_path / "p.parquet", EDGE_DOMAINS)
    old = query(OLD_CREGIT_JOIN, src=parquet, firm_map=firm_map, firm_canonical=canon)

    fa.add_firm_columns(parquet, firm_map, canon)
    new = query("SELECT token_index, firm_raw, firm, firm_source FROM read_parquet($p)",
                p=parquet)
    assert new == old
    assert new[3] == (3, "International Business Machines", "IBM", "cncf-gitdm")
    assert new[-1] == (10, "", "", "")


@needs_duckdb
def test_the_output_is_cregit_columns_then_the_firm_columns(tmp_path, tables):
    from validate_schema import read_schema

    parquet = cregit_parquet(tmp_path / "p.parquet", ["intel.com"])
    fa.add_firm_columns(parquet, *tables)
    assert read_schema(str(parquet)) == list(EXPECTED_COLUMNS)


@needs_duckdb
def test_rows_keep_their_count_and_their_order(tmp_path, tables):
    domains = ["unmapped.example", "intel.com", None, "gmail.com"] * 25
    parquet = cregit_parquet(tmp_path / "p.parquet", domains)
    fa.add_firm_columns(parquet, *tables)
    got = query("SELECT token_index, person_domain FROM read_parquet($p)", p=parquet)
    assert got == list(enumerate(domains))


@needs_duckdb
def test_a_second_run_recomputes_from_the_current_map(tmp_path, tables):
    firm_map, canon = tables
    parquet = cregit_parquet(tmp_path / "p.parquet", ["intel.com"])
    fa.add_firm_columns(parquet, firm_map, canon)
    write_csv(firm_map, MAP_HEADER, "intel.com,Intel Corporation,company,correction")

    assert fa.input_refusal(parquet) is None
    fa.add_firm_columns(parquet, firm_map, canon)
    assert query("SELECT firm_raw, firm_source FROM read_parquet($p)", p=parquet) == [
        ("Intel Corporation", "correction")]


@needs_duckdb
def test_a_file_in_neither_form_is_refused_and_left_untouched(
        tmp_path, tables, monkeypatch, capsys):
    import duckdb

    whole = cregit_parquet(tmp_path / "whole.parquet", ["intel.com"])
    parquet = tmp_path / "p.parquet"
    duckdb.connect().execute(
        "COPY (SELECT * EXCLUDE (repo_tag) FROM read_parquet($s)) TO $d (FORMAT PARQUET)",
        {"s": str(whole), "d": str(parquet)})
    before = parquet.read_bytes()
    monkeypatch.setattr(fa, "FIRM_MAP", tables[0])
    monkeypatch.setattr(fa, "FIRM_CANONICAL", tables[1])

    assert fa.main([str(parquet)]) == fa.EXIT_REFUSED
    assert "66 columns, neither cregit's 67 nor ctp's 70" in capsys.readouterr().err
    assert parquet.read_bytes() == before


@needs_duckdb
def test_a_failed_join_keeps_the_input_and_leaves_no_temp_file(tmp_path, tables):
    parquet = cregit_parquet(tmp_path / "p.parquet", ["intel.com"])
    before = parquet.read_bytes()
    with pytest.raises(Exception):
        fa.add_firm_columns(parquet, tmp_path / "absent.csv", tables[1])
    assert parquet.read_bytes() == before
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "canon.csv", "map.csv", "p.parquet"]


@needs_duckdb
def test_main_rewrites_the_file_and_says_so(tmp_path, tables, monkeypatch, capsys):
    parquet = cregit_parquet(tmp_path / "p.parquet", ["intel.com"])
    monkeypatch.setattr(fa, "FIRM_MAP", tables[0])
    monkeypatch.setattr(fa, "FIRM_CANONICAL", tables[1])
    assert fa.main([str(parquet), "--memory-limit", "1GB", "--threads", "1"]) == 0
    assert capsys.readouterr().out.startswith(f"OK {parquet}")
    assert query("SELECT firm FROM read_parquet($p)", p=parquet) == [("Intel",)]


# --------------------------------------------------------------------------- #
# the lookup-table preflight: a repeated key multiplies token rows
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("header, lines, column, expected", [
    pytest.param(MAP_HEADER, ["a.com,A,company,gitdm", " A.COM ,A2,company,patch"],
                 "domain", "1 repeated key(s) in `domain`: a.com", id="map-case-and-space"),
    pytest.param(CANON_HEADER, ["Acme,ACME,merge,n", "acme,ACME,merge,n"],
                 "firm_raw", "1 repeated key(s) in `firm_raw`: acme", id="canonical"),
    pytest.param("dom,company", ["a.com,A"], "domain", "no `domain` column",
                 id="no-key-column"),
])
def test_a_bad_lookup_table_is_refused(tmp_path, header, lines, column, expected):
    path = write_csv(tmp_path / "t.csv", header, *lines)
    assert expected in fa.table_refusal(path, column)


def test_a_bad_table_stops_main_before_the_parquet_is_read(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(fa, "FIRM_MAP", write_csv(
        tmp_path / "m.csv", MAP_HEADER, "a.com,A,company,gitdm", "a.com,B,company,gitdm"))
    monkeypatch.setattr(fa, "read_schema", lambda p: pytest.fail("parquet was read"))
    assert fa.main([str(tmp_path / "never.parquet")]) == fa.EXIT_REFUSED
    assert "multiplies token rows" in capsys.readouterr().err


def test_no_arguments_is_a_usage_error():
    with pytest.raises(SystemExit) as exc:
        fa.main([])
    assert exc.value.code == 2


# --------------------------------------------------------------------------- #
# the shipped domain map
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("path, column", [
    pytest.param(fa.FIRM_MAP, "domain", id="map"),
    pytest.param(fa.FIRM_CANONICAL, "firm_raw", id="canonical"),
])
def test_the_shipped_tables_pass_the_preflight(path, column):
    assert fa.table_refusal(path, column) is None


def test_the_map_holds_the_vem_paper_rows_unchanged_in_count():
    """VEM 2026 pipeline/data/affiliation.csv: 895 rows, sources rich, gitdm, patch."""
    by_source = Counter(r["source"] for r in rows(fa.FIRM_MAP))
    assert (by_source["gitdm"], by_source["patch"], by_source["rich"]) == (768, 72, 55)
    assert sum(by_source.values()) == 4041


def test_qti_qualcomm_com_resolves_to_qualcomm_not_cern():
    """The row read CERN from a single-person inference until 2026-09-20."""
    row = next(r for r in rows(fa.FIRM_MAP) if r["domain"] == "qti.qualcomm.com")
    assert (row["company"], row["source"]) == ("Qualcomm", "correction")


def test_the_other_qualcomm_domains_were_already_right_and_still_are():
    by_domain = {r["domain"]: r for r in rows(fa.FIRM_MAP)}
    for domain in ("oss.qualcomm.com", "qca.qualcomm.com", "quicinc.com",
                   "codeaurora.org"):
        assert (by_domain[domain]["company"], by_domain[domain]["source"]) == (
            "Qualcomm", "patch")


def test_collibra_com_resolves_to_collibra_not_medidata():
    row = next(r for r in rows(fa.FIRM_MAP) if r["domain"] == "collibra.com")
    assert (row["company"], row["kind"], row["source"]) == (
        "Collibra", "company", "correction")


# --------------------------------------------------------------------------- #
# the shipped canonical-name table
# --------------------------------------------------------------------------- #

def canonical_rows():
    return rows(fa.FIRM_CANONICAL)


def test_every_row_declares_a_decision_and_a_reason():
    for r in canonical_rows():
        assert r["decision"] in ("merge", "keep"), r
        assert len(r["note"].strip()) > 20, r


def test_a_merge_changes_the_name_and_a_keep_does_not():
    for r in canonical_rows():
        assert (r["firm"] != r["firm_raw"]) == (r["decision"] == "merge"), r


def test_no_merge_target_is_itself_merged_away():
    """One hop only: a chain A->B->C would leave `firm` holding B on some rows."""
    merges = [r for r in canonical_rows() if r["decision"] == "merge"]
    assert {r["firm"] for r in merges} & {r["firm_raw"] for r in merges} == set()


def test_every_name_in_the_table_actually_appears_in_the_map():
    """A dead row reads as a reviewed decision about a firm the corpus lacks."""
    companies = {r["company"] for r in rows(fa.FIRM_MAP)}
    for r in canonical_rows():
        assert r["firm_raw"] in companies, r["firm_raw"]
        assert r["firm"] in companies, r["firm"]


def test_the_rejections_are_recorded_rather_than_dropped():
    keeps = {r["firm_raw"] for r in canonical_rows() if r["decision"] == "keep"}
    assert {"Independent", "AWS", "Azure", "Samsung SDS", "Yahoo! Japan",
            "Hewlett", "China Mobile International"} <= keeps
    assert all("REJECTED" in r["note"] for r in canonical_rows() if r["decision"] == "keep")


@pytest.mark.parametrize("raw, canonical", [
    ("International Business Machines", "IBM"), ("Salesforce.com", "Salesforce"),
    ("SalesForce", "Salesforce"), ("NETFLIX", "Netflix"), ("TWITTER", "Twitter"),
    ("YANDEX", "Yandex"), ("ADOBE", "Adobe"), ("YELP", "Yelp"), ("RAPID7", "Rapid7"),
    ("NIKE", "Nike"), ("NEW RELIC", "New Relic"), ("F5 NETWORKS", "F5 Networks"),
])
def test_a_split_spelling_resolves_to_one_name(raw, canonical):
    assert {r["firm_raw"]: r["firm"] for r in canonical_rows()}[raw] == canonical


def test_the_table_collapses_forty_seven_firms_out_of_ninety_eight_strings():
    merges = [r for r in canonical_rows() if r["decision"] == "merge"]
    targets = {r["firm"] for r in merges}
    assert len(targets) == 47
    assert len(targets | {r["firm_raw"] for r in merges}) == 98
    assert sum(1 for r in canonical_rows() if r["decision"] == "keep") == 11
