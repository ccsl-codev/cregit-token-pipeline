"""schema3.py: the ctp schema 3 write, the project_facts table and the check.

The inputs are small synthetic Parquet files with the column layouts that the
cregit generator writes. No value in them is a real person.
"""
from __future__ import annotations

import io
import json
import os
from pathlib import Path

import pytest

import schema3 as S
import validate_schema as vs

duckdb = pytest.importorskip("duckdb")

SALT = b"0123456789abcdef0123456789abcdef-schema3-test-salt"
TEST_PSL = """// ===BEGIN ICANN DOMAINS===
com
org
example
// ===END ICANN DOMAINS===
"""
CREGIT_3 = [(c, t) for c, t in vs.EXPECTED_COLUMNS_3 if c not in S.ORG_NAMES]
# The 67 columns of a cregit file with the 29 fields (ctp schema 1 without firm).
CREGIT_1 = [(c, t) for c, t in vs.EXPECTED_COLUMNS if c not in vs.FIRM_COLUMNS]
DOMAINS = ["corp.example", "other.example", "", "nodot", "a@corp.example", None, "CORP.example "]


def values(col: str, typ: str) -> str:
    """SQL for one column over a row number i."""
    if col == "person_domain":
        lit = ", ".join("NULL" if d is None else f"'{d}'" for d in DOMAINS)
        return f"list_extract([{lit}], (i % {len(DOMAINS)}) + 1)"
    if col in ("author_name", "committer_name", "person_name"):
        return "'Name ' || (i % 5)::VARCHAR"
    if col in ("author_email", "committer_email", "person_email"):
        return "'p' || (i % 5)::VARCHAR || chr(64) || 'corp.example'"
    if col == "personid":
        return "'name ' || (i % 5)::VARCHAR"
    if col == "commit_summary":
        return "'fix ' || i::VARCHAR"
    if col in ("provenance_status", "audit_frame_file"):
        return "'frame:f.csv@sha256:0123456789ab'"
    if col == "fact":
        return "'roster:asf;owner_kind:user'"
    if col == "file_path":
        return "'src/f' || (i // 3)::VARCHAR || '.c'"
    if typ == "BIGINT":
        return "i"
    if typ == "VARCHAR[]":
        return "CASE WHEN i % 4 = 0 THEN ['Name ' || (i % 5)::VARCHAR || ' <p' || (i % 5)::VARCHAR || chr(64) || 'corp.example>'] ELSE [] END"
    return f"'{col}-' || (i % 3)::VARCHAR"


def make_file(path: Path, layout, rows: int = 30, kv: dict | None = None) -> str:
    items = ", ".join(f"CAST({values(c, t)} AS {t}) AS \"{c}\"" for c, t in layout)
    meta = ""
    if kv:
        meta = ", KV_METADATA {" + ", ".join(f"{k}: '{v}'" for k, v in kv.items()) + "}"
    duckdb.sql(f"COPY (SELECT {items} FROM range({rows}) r(i) ORDER BY i) TO '{path}' "
               f"(FORMAT PARQUET{meta})")
    return str(path)


@pytest.fixture
def links(tmp_path):
    d = tmp_path / "domain_links.parquet"
    p = tmp_path / "project_links.parquet"
    duckdb.sql(f"COPY (SELECT * FROM (VALUES ('corp.example', 'corp'), ('x.org', 'xorg')) "
               f"v(domain, firm_based_on_domain)) TO '{d}' (FORMAT PARQUET)")
    duckdb.sql(f"COPY (SELECT * FROM (VALUES ('proj', 'corp', true, 'namespace'), "
               f"('other', NULL, false, '-')) v(project, firm_based_on_namespace, "
               f"project_enterprise_driven, project_enterprise_basis)) TO '{p}' (FORMAT PARQUET)")
    return str(d), str(p)


def run_write(src, dst, links, project="proj", **kw):
    return S.write(src, dst, project=project, domain_links=links[0], project_links=links[1],
                   organizations_version="v9", out=io.StringIO(), **kw)


def read(path, sql):
    return duckdb.sql(sql.replace("{F}", f"read_parquet('{path}')")).fetchall()


def kv_of(path):
    return dict(duckdb.sql(f"SELECT decode(key), decode(value) FROM parquet_kv_metadata('{path}')")
                .fetchall())


# --------------------------------------------------------------------------- #
# the contract
# --------------------------------------------------------------------------- #

def test_schema_3_has_61_columns_in_the_release_order():
    names = [c for c, _ in vs.EXPECTED_COLUMNS_3]
    assert len(names) == 61 and len(set(names)) == 61
    assert names[:3] == ["repo_name", "clone_url", "audit_frame_file"]
    assert names[19:21] == ["fork", "file_path"]
    assert names[-4:] == list(S.ORG_NAMES)
    assert not set(vs.SCHEMA_3_DROPPED) & set(names)
    assert "provenance_status" not in names
    assert dict(vs.EXPECTED_COLUMNS_3)["project_enterprise_driven"] == "BOOLEAN"
    assert len(vs.META_FIELDS_3) == 19 and vs.META_FIELDS_3[1] == "audit_frame_file"
    assert len(vs.EXPECTED_COLUMNS_2) == 71


def test_contract_names_each_schema_and_refuses_another():
    assert vs.contract("1") is vs.EXPECTED_COLUMNS
    assert vs.contract("3") is vs.EXPECTED_COLUMNS_3
    with pytest.raises(ValueError):
        vs.contract("4")


@pytest.mark.parametrize("layout", ["cregit3", "cregit1", "schema1"])
def test_each_known_input_layout_projects_to_the_57_base_columns(layout):
    cols = {"cregit3": [c for c, _ in CREGIT_3], "cregit1": [c for c, _ in CREGIT_1],
            "schema1": [c for c, _ in vs.EXPECTED_COLUMNS]}[layout]
    pairs = S.source_projection(cols)
    assert [n for _, n in pairs] == [c for c, _ in S.BASE_COLUMNS]
    if layout != "cregit3":
        assert ("provenance_status", "audit_frame_file") in pairs


@pytest.mark.parametrize("change, match", [
    (lambda c: c + ["surprise"], "unknown"),
    (lambda c: [x for x in c if x != "token_index"], "missing"),
    (lambda c: c + ["firm_based_on_domain"], "already has organization"),
])
def test_an_unknown_or_short_input_is_refused(change, match):
    with pytest.raises(S.Schema3Error, match=match):
        S.source_projection(change([c for c, _ in CREGIT_3]))


def test_the_mapping_hash_follows_the_pairs_and_not_their_order():
    a = S.mapping_sha256([("a.org", "x"), ("b.org", None)], [("p", "x")])
    b = S.mapping_sha256([("b.org", ""), ("a.org", "x")], [("p", "x")])
    c = S.mapping_sha256([("a.org", "y"), ("b.org", None)], [("p", "x")])
    assert a == b != c and len(a) == 64


# --------------------------------------------------------------------------- #
# write, without pseudonyms
# --------------------------------------------------------------------------- #

def test_the_write_gives_schema_3_with_the_organization_columns(tmp_path, links):
    src = make_file(tmp_path / "proj-dataset.parquet", CREGIT_3)
    dst = str(tmp_path / "out.parquet")
    res = run_write(src, dst, links)
    assert res["rows"] == 30 and not res["pseudonymized"]
    got = read(dst, "DESCRIBE SELECT * FROM {F}")
    assert [(r[0], r[1]) for r in got] == list(vs.EXPECTED_COLUMNS_3)
    kv = kv_of(dst)
    assert kv["ctp_schema_version"] == "3" and kv["organizations_version"] == "v9"
    assert kv["organizations_mapping_sha256"] == res["mapping_sha256"]
    assert "pseudonymization_version" not in kv
    rows = read(dst, "SELECT person_domain, firm_based_on_domain, firm_based_on_namespace, "
                     "project_enterprise_driven, project_enterprise_basis FROM {F} LIMIT 7")
    by_domain = {r[0]: r[1] for r in rows}
    assert by_domain["corp.example"] == "corp"            # in the links
    assert by_domain["CORP.example "] == "corp"           # lower(trim()) key
    assert by_domain["other.example"] == S.UNMATCHED_REAL  # a real domain, no link
    for d in ("", "nodot", "a@corp.example", None):
        assert by_domain[d] == S.NO_REAL_DOMAIN
    assert {r[2:] for r in rows} == {("corp", True, "namespace")}
    assert S.check_file(duckdb.connect(), dst, pseudonymized=False) == []


def test_the_write_keeps_every_value_and_the_row_order(tmp_path, links):
    src = make_file(tmp_path / "a.parquet", CREGIT_1)
    dst = str(tmp_path / "b.parquet")
    run_write(src, dst, links)
    cols = [c for c, _ in S.BASE_COLUMNS]
    old = ", ".join("provenance_status" if c == "audit_frame_file" else f'"{c}"' for c in cols)
    new = ", ".join(f'"{c}"' for c in cols)
    assert read(src, f"SELECT {old} FROM {{F}}") == read(dst, f"SELECT {new} FROM {{F}}")


def test_a_project_with_no_link_row_gets_null_project_columns(tmp_path, links):
    src = make_file(tmp_path / "a.parquet", CREGIT_3)
    dst = str(tmp_path / "b.parquet")
    run_write(src, dst, links, project="absent")
    assert read(dst, "SELECT DISTINCT firm_based_on_namespace, project_enterprise_driven, "
                     "project_enterprise_basis FROM {F}") == [(None, None, None)]


def test_the_write_in_place_is_skipped_when_the_file_is_already_schema_3(tmp_path, links):
    src = make_file(tmp_path / "a.parquet", CREGIT_3)
    run_write(src, src, links)
    before = Path(src).read_bytes()
    res = run_write(src, src, links)
    assert res["skipped"] and Path(src).read_bytes() == before


def test_repeated_domains_in_the_links_are_refused(tmp_path, links):
    d = tmp_path / "bad.parquet"
    duckdb.sql(f"COPY (SELECT * FROM (VALUES ('a.org', 'x'), ('a.org', 'y')) "
               f"v(domain, firm_based_on_domain)) TO '{d}' (FORMAT PARQUET)")
    src = make_file(tmp_path / "a.parquet", CREGIT_3)
    with pytest.raises(S.Schema3Error, match="repeated"):
        run_write(src, str(tmp_path / "b.parquet"), (str(d), links[1]))
    assert not (tmp_path / "b.parquet").exists()
    assert not list(tmp_path.glob(".b.parquet.*")), "the temporary files were left"


def test_an_empty_organizations_version_is_refused(tmp_path, links):
    with pytest.raises(S.Schema3Error, match="organizations-version"):
        S.write("a", "b", project="p", domain_links=links[0], project_links=links[1],
                organizations_version="")


# --------------------------------------------------------------------------- #
# write, with pseudonyms
# --------------------------------------------------------------------------- #

@pytest.fixture
def anon(tmp_path):
    salt = tmp_path / "private" / "salt"
    salt.parent.mkdir()
    salt.write_bytes(SALT)
    os.chmod(salt, 0o600)
    psl = tmp_path / "psl.dat"
    psl.write_text(TEST_PSL)
    return {"salt_file": str(salt), "psl": str(psl), "registry": "",
            "registry_dir": str(tmp_path / "private" / "reg"), "scan": True}


def test_the_pseudonymized_write_replaces_the_identities_only(tmp_path, links, anon):
    out = tmp_path / "out"
    out.mkdir()
    src = make_file(tmp_path / "proj-dataset.parquet", CREGIT_3)
    dst = str(out / "proj-dataset.parquet")
    res = run_write(src, dst, links, pseudonymize=anon)
    assert res["pseudonymized"] and res["identity_values"] > 0
    assert Path(anon["registry_dir"], "proj.registry.parquet").is_file()
    kv = kv_of(dst)
    assert kv["pseudonymization_version"] == "2"
    assert kv["pseudonymization_registry_sha256"] == res["registry_sha256"]
    assert kv["ctp_schema_version"] == "3"
    assert [(r[0], r[1]) for r in read(dst, "DESCRIBE SELECT * FROM {F}")] == list(vs.EXPECTED_COLUMNS_3)
    # The token columns pass; the names change; the domain of an address is kept.
    assert read(src, "SELECT token_value, file_path FROM {F}") == \
        read(dst, "SELECT token_value, file_path FROM {F}")
    names = {r[0] for r in read(dst, "SELECT DISTINCT author_name FROM {F}")}
    assert names and all(n.startswith("Author ") for n in names)
    emails = {r[0] for r in read(dst, "SELECT DISTINCT author_email FROM {F}")}
    assert all(e.endswith(chr(64) + "corp.example") and not e.startswith("p") for e in emails)
    # Rule E: the domain of 'a@corp.example' is corp.example, so is its organization.
    rows = dict(read(dst, "SELECT person_domain, firm_based_on_domain FROM {F} WHERE "
                          "person_domain = 'corp.example'"))
    assert rows == {"corp.example": "corp"}
    assert S.check_file(duckdb.connect(), dst, pseudonymized=True) == []
    assert not list(out.glob(".*")), "the private file was left beside the output"


def test_a_registry_under_the_output_folder_is_refused(tmp_path, links, anon):
    src = make_file(tmp_path / "proj-dataset.parquet", CREGIT_3)
    anon["registry_dir"] = str(tmp_path / "reg")
    with pytest.raises(S.Schema3Error, match="private storage"):
        run_write(src, str(tmp_path / "x.parquet"), links, pseudonymize=anon)


def test_a_registry_of_another_salt_is_refused(tmp_path, links, anon):
    out = tmp_path / "out"
    out.mkdir()
    src = make_file(tmp_path / "proj-dataset.parquet", CREGIT_3)
    run_write(src, str(out / "a.parquet"), links, pseudonymize=anon)
    other = tmp_path / "private" / "salt2"
    other.write_bytes(SALT[::-1])
    os.chmod(other, 0o600)
    anon2 = dict(anon, salt_file=str(other), registry_dir="",
                 registry=str(Path(anon["registry_dir"]) / "proj.registry.parquet"))
    with pytest.raises(S.Schema3Error, match="another salt"):
        run_write(src, str(out / "b.parquet"), links, pseudonymize=anon2)


def test_an_existing_registry_gives_the_same_file(tmp_path, links, anon):
    out = tmp_path / "out"
    out.mkdir()
    src = make_file(tmp_path / "proj-dataset.parquet", CREGIT_3)
    a, b = str(out / "a.parquet"), str(out / "b.parquet")
    run_write(src, a, links, pseudonymize=anon)
    reg = str(Path(anon["registry_dir"]) / "proj.registry.parquet")
    run_write(src, b, links, pseudonymize=dict(anon, registry=reg, registry_dir=""))
    assert read(a, "SELECT * FROM {F}") == read(b, "SELECT * FROM {F}")


# --------------------------------------------------------------------------- #
# project_facts, check, main
# --------------------------------------------------------------------------- #

def test_split_fact_splits_items_at_the_first_colon():
    assert S.split_fact("roster:asf;owner_kind:user;company_pool") == [
        ("roster", "asf"), ("owner_kind", "user"), ("company_pool", None)]
    assert S.split_fact("") == [] and S.split_fact(";;") == []
    assert S.split_fact("a:b:c") == [("a", "b:c")]


def test_the_facts_table_has_one_row_per_project_and_fact(tmp_path):
    side = tmp_path / "s.json"
    side.write_text(json.dumps({"p1": {"fact": "roster:asf;owner_kind:org"},
                                "p2": {"fact": ""}, "p3": {}}))
    out = tmp_path / "f" / S.FACTS_COLUMNS[0]
    assert S.write_facts(str(side), str(out), ["p2", "p1", "p3", "p1"]) == 2
    rows = read(out, "SELECT * FROM {F}")
    assert rows == [("p1", "owner_kind", "org"), ("p1", "roster", "asf")]
    assert [r[0] for r in read(out, "DESCRIBE SELECT * FROM {F}")] == list(S.FACTS_COLUMNS)
    assert S.write_facts(str(side), str(tmp_path / "e.parquet"), ["p2"]) == 0
    assert read(tmp_path / "e.parquet", "SELECT count(*) FROM {F}") == [(0,)]
    with pytest.raises(S.Schema3Error, match="not in the sidecar"):
        S.write_facts(str(side), str(out), ["p9"])


def test_check_reports_a_file_that_is_not_schema_3(tmp_path, links, capsys):
    bad = make_file(tmp_path / "bad.parquet", CREGIT_3, kv={"ctp_schema_version": "2"})
    good = str(tmp_path / "good.parquet")
    run_write(make_file(tmp_path / "a.parquet", CREGIT_3), good, links)
    assert S.main(["check", good]) == 0
    assert S.main(["check", good, bad]) == S.EXIT_FAIL
    out = capsys.readouterr().out
    assert "OK   " + good in out and "drifts from ctp schema 3" in out
    assert "not '3'" in out and "no organizations_version" in out


def test_main_writes_and_refuses_bad_option_sets(tmp_path, links, anon, capsys):
    src = make_file(tmp_path / "a.parquet", CREGIT_3)
    base = ["write", src, str(tmp_path / "b.parquet"), "--project", "proj",
            "--domain-links", links[0], "--project-links", links[1],
            "--organizations-version", "v9"]
    assert S.main(base) == 0
    assert json.loads(capsys.readouterr().out.strip())["rows"] == 30
    assert S.main(base + ["--pseudonymize"]) == S.EXIT_USAGE
    assert S.main(base + ["--salt-file", anon["salt_file"]]) == S.EXIT_USAGE
    assert S.main(base + ["--pseudonymize", "--salt-file", anon["salt_file"],
                          "--registry", "r", "--registry-dir", "d"]) == S.EXIT_USAGE
    bad = ["write", src, str(tmp_path / "c.parquet"), "--project", "proj",
           "--domain-links", str(tmp_path / "a.parquet"), "--project-links", links[1],
           "--organizations-version", "v9"]
    assert S.main(bad) == S.EXIT_FAIL
    assert "schema3:" in capsys.readouterr().err


def test_main_facts_prints_the_counts(tmp_path, capsys):
    side = tmp_path / "s.json"
    side.write_text(json.dumps({"p": {"fact": "roster:kde"}}))
    assert S.main(["facts", "--sidecar", str(side), "--out", str(tmp_path / "f.parquet"), "p"]) == 0
    assert json.loads(capsys.readouterr().out) == {"facts_rows": 1, "projects": 1}
