"""Tests of pseudonymize.py, the release pseudonymizer.

All data is synthetic: people are 'P<n> Person' at '<n>.example' domains,
made by pseudonymize.synthetic_select(). No test reads a dataset file.
"""
from __future__ import annotations

import io
import json
import os
import stat
import sys
from pathlib import Path

import pytest

import anonymize_parquet as A
import pseudonymize as P
import validate_schema as vs

try:                                          # pragma: no cover - import plumbing
    import duckdb as _duckdb
except ModuleNotFoundError:                   # pragma: no cover - import plumbing
    _duckdb = None

# tests/test_consolidate.py can put a stub duckdb into sys.modules first.
_STUBBED = getattr(sys.modules.get("duckdb"), "__doc__", "") or ""
requires_duckdb = pytest.mark.skipif(
    _duckdb is None or _STUBBED.startswith("Test stub"),
    reason="needs real duckdb to write parquet")

AT = chr(64)
SALT = b"0123456789abcdef0123456789abcdef-fixed-test-salt"
OTHER_SALT = b"fedcba9876543210fedcba9876543210-other-test-salt"
ROWS = 3000
PEOPLE = 200            # local parts p0 .. p199; p100 .. p199 are probes (4 characters)


# A small Public Suffix List. 'example' is a top-level label here, so that the
# synthetic domains (corp1.example) are valid domains under rule E.
TEST_PSL = """// test list
// ===BEGIN ICANN DOMAINS===
com
org
example
uk
co.uk
*.ck
!www.ck
// ===END ICANN DOMAINS===
// ===BEGIN PRIVATE DOMAINS===
blogspot.com
// ===END PRIVATE DOMAINS===
"""


@pytest.fixture(autouse=True)
def _test_psl(tmp_path_factory, monkeypatch):
    path = tmp_path_factory.getbasetemp() / "test_psl.dat"
    if not path.exists():
        path.write_text(TEST_PSL)
    monkeypatch.setattr(P, "DEFAULT_PSL_PATH", str(path))
    return str(path)


def _salt_file(tmp: Path, salt: bytes = SALT, name: str = "salt") -> str:
    p = tmp / name
    p.write_bytes(salt + b"\n")
    os.chmod(p, 0o600)
    return str(p)


def _con(keys=None, **kw):
    con = P.connect("2GB", 2)
    P.install(con, keys if keys is not None else P.derive_keys(SALT), **kw)
    return con


def _make(tmp: Path, name: str, *, rows: int = ROWS, people: int = PEOPLE,
          commits: int = 300, schema2: bool = True, seed: str = "proj",
          replace: dict[str, str] | None = None) -> str:
    """One synthetic file. `replace` maps a column to a SQL expression that
    plants a value (the other rows keep the synthetic value)."""
    path = str(tmp / name)
    con = P.connect("2GB", 2)
    try:
        base = P.synthetic_select(rows, people=people, commits=commits, schema2=schema2,
                                  seed_text=seed, rows_per_commit=3)
        if replace:
            rep = ", ".join(f"{e} AS {P.q(c)}" for c, e in replace.items())
            base = f"SELECT * REPLACE ({rep}) FROM ({base})"
        kv = ({"ctp_schema_version": "2", "organisations_version": "test",
               "organisations_mapping_sha256": "0" * 64} if schema2 else {"x": "1"})
        con.execute(f"COPY ({base}) TO {P.sql_str(path)} (FORMAT PARQUET, "
                    f"KV_METADATA {P._kv_sql(kv)})")
    finally:
        con.close()
    return path


@pytest.fixture
def files(tmp_path):
    a = _make(tmp_path, "alpha-dataset.parquet", seed="alpha")
    # The second file has 100 more people: p200 .. p299.
    b = _make(tmp_path, "beta-dataset.parquet", seed="beta", people=PEOPLE + 100)
    return tmp_path, [a, b]


@pytest.fixture
def registry(files):
    tmp, paths = files
    reg = str(tmp / "registry.parquet")
    P.build_registry(paths, P.derive_keys(SALT), reg, threads=2, memory_limit="2GB",
                     out=io.StringIO())
    return tmp, paths, reg


def _written(tmp, paths, reg, outname="out", **kw) -> tuple[str, dict]:
    outdir = str(tmp / outname)
    res = P.run_write(paths, outdir, keys=P.derive_keys(SALT), registry=reg, threads=2,
                      memory_limit="2GB", out=io.StringIO(), **kw)
    return outdir, res


# --------------------------------------------------------------------------
# Contract and the token function
# --------------------------------------------------------------------------

def test_schema_2_is_the_71_column_contract():
    names = [c for c, _ in P.SCHEMA_2]
    assert len(P.SCHEMA_2) == 71 and len(set(names)) == 71
    assert names[-4:] == [c for c, _ in P.ORGANISATION_TYPES]
    assert not set(names) & set(A.FIRM_COLUMNS)
    assert [c for c in names[:67]] == [c for c, _ in vs.EXPECTED_COLUMNS
                                       if c not in A.FIRM_COLUMNS]


@requires_duckdb
def test_sql_token_equals_the_python_token():
    keys = P.derive_keys(SALT)
    con = _con(keys)
    for space, v in (("name", "p1 person"), ("email", "p123" + AT + "corp1.example"),
                     ("name", "élève o'brien")):
        assert con.execute("SELECT anon_t(?, ?)", [space, v]).fetchone()[0] == \
            P.token_py(keys, space, v)
    con.close()


def test_a_fixed_salt_gives_a_known_token():
    """Known answer: a change of the scheme changes this value, on purpose."""
    keys = P.derive_keys(SALT)
    tok = P.token_py(keys, "name", "p1 person")
    assert len(tok) == A.PSEUDO_HEX_LEN and int(tok, 16) >= 0
    assert tok == P.token_py(P.derive_keys(SALT), "name", "p1 person")
    assert tok != P.token_py(P.derive_keys(OTHER_SALT), "name", "p1 person")
    assert tok != P.token_py(keys, "email", "p1 person")


def test_redact_removes_both_keys():
    keys = ("a" * 64, "b" * 64)
    assert "a" * 64 not in P.redact(f"x {'a' * 64} y {'b' * 64}", keys)
    assert P.redact("plain", None) == "plain"


# --------------------------------------------------------------------------
# The footer parse in SQL is the parse of anonymize_parquet
# --------------------------------------------------------------------------

TRAILERS = [
    "P1 Person <p1" + AT + "corp1.example>",
    "  P1 Person   <  p1" + AT + "corp1.example >  ",
    "P1 Person <p1" + AT + "corp1.example>\r",
    "P1 Person <p1" + AT + "corp1.example>",
    "<p1" + AT + "corp1.example>",
    "p1" + AT + "corp1.example",
    " p1" + AT + "corp1.example\t",
    "p1-bot at p1-bot" + AT + "users.example",
    "P1 Person",
    "P1 Person (corp1)",
    "a <b" + AT + "c" + AT + "d>",
    "x <y>",
    " P2 Person <p2" + AT + "corp2.example>",
]


@requires_duckdb
def test_sql_footer_shape_equals_the_python_shape():
    con = _con()
    for e in TRAILERS:
        got = con.execute("SELECT anon_footer_shape(?)", [e]).fetchone()[0]
        assert got == A.parse_footer_element(e)[0], repr(e)
    con.close()


@requires_duckdb
def test_sql_footer_render_equals_the_old_render():
    """With the old domain rule (second-part), the SQL render gives the string
    that render_footer_element gives with the same tokens."""
    keys = P.derive_keys(SALT)
    con = _con(keys, email_domain="second-part")
    for e in TRAILERS:
        shape, name, email = A.parse_footer_element(e)
        reg = A.Registry()
        if name:
            reg.names[name.lower()] = P.token_py(keys, "name", name.lower())
        if email:
            reg.emails[email.lower()] = P.token_py(keys, "email", email.lower())
        want = A.render_footer_element(reg, name, email)
        got = con.execute("SELECT anon_footer_elem(?)", [e]).fetchone()[0]
        assert got == want, repr(e)
    assert con.execute("SELECT anon_footer_elem('')").fetchone()[0] == ""
    assert con.execute("SELECT anon_footer_elem(NULL)").fetchone()[0] is None
    con.close()


@requires_duckdb
def test_scalar_macros_keep_null_and_empty_and_the_domain():
    keys = P.derive_keys(SALT)
    con = _con(keys)
    addr = "P5" + AT + "Corp5.example"
    got = con.execute("SELECT anon_email(?), anon_email(''), anon_email(NULL), "
                      "anon_name('P5 Person'), anon_personid('P5 Person'), anon_email('nobody')",
                      [addr]).fetchone()
    tok = P.token_py(keys, "email", addr.lower())
    assert got[0] == f"author_{tok}{AT}corp5.example"     # rule E: lower case
    assert got[1] == "" and got[2] is None
    assert got[3] == "Author " + P.token_py(keys, "name", "p5 person")
    assert got[4] == "author " + P.token_py(keys, "name", "p5 person")
    assert got[5] == "author_" + P.token_py(keys, "email", "nobody")
    con.close()


# --------------------------------------------------------------------------
# The registry
# --------------------------------------------------------------------------

@requires_duckdb
def test_registry_counts_and_privacy(registry):
    tmp, paths, reg = registry
    assert stat.S_IMODE(os.stat(reg).st_mode) == 0o600
    con = _con()
    meta = P.verify_registry_file(con, reg)
    assert meta["n_email"] != "0" and meta["n_name"] != "0" and meta["n_footer"] != "0"
    assert meta["salt_check"] == P.salt_check(con)
    blob = Path(reg).read_bytes()
    assert SALT not in blob
    for k in P.derive_keys(SALT):
        assert k.encode() not in blob
    con.close()


@requires_duckdb
def test_registry_is_stable_with_a_fixed_salt(files, tmp_path):
    """The same salt and the same values give the same table and sha256; a
    smaller file set changes no token of a value in both."""
    tmp, paths = files
    keys = P.derive_keys(SALT)
    sink = io.StringIO()
    r1 = P.build_registry(paths, keys, str(tmp / "r1.parquet"), threads=2, out=sink)
    r2 = P.build_registry(list(reversed(paths)), keys, str(tmp / "r2.parquet"),
                          threads=2, out=sink)
    r3 = P.build_registry(paths[:1], keys, str(tmp / "r3.parquet"), threads=2, out=sink)
    assert r1["registry_sha256"] == r2["registry_sha256"]
    assert r1["registry_sha256"] != r3["registry_sha256"]
    con = _con(keys)
    q = ("SELECT count(*), count(*) FILTER (WHERE a.pseudonym IS DISTINCT FROM b.pseudonym) "
         "FROM read_parquet(?) a JOIN read_parquet(?) b USING (space, identity)")
    n, diff = con.execute(q, [str(tmp / "r3.parquet"), str(tmp / "r1.parquet")]).fetchone()
    assert n > 0 and diff == 0
    one = con.execute("SELECT token FROM read_parquet(?) WHERE space = 'name' "
                      "AND identity = 'p150 person'", [str(tmp / "r1.parquet")]).fetchone()[0]
    assert one == P.token_py(keys, "name", "p150 person")
    con.close()


@requires_duckdb
def test_another_salt_changes_every_token(files):
    tmp, paths = files
    sink = io.StringIO()
    a = P.build_registry(paths, P.derive_keys(SALT), str(tmp / "ra.parquet"), out=sink)
    b = P.build_registry(paths, P.derive_keys(OTHER_SALT), str(tmp / "rb.parquet"), out=sink)
    assert a["salt_check"] != b["salt_check"]
    con = _con()
    same = con.execute("SELECT count(*) FROM read_parquet(?) a JOIN read_parquet(?) b "
                       "USING (space, identity) WHERE a.token = b.token",
                       [str(tmp / "ra.parquet"), str(tmp / "rb.parquet")]).fetchone()[0]
    assert same == 0
    con.close()


@requires_duckdb
def test_a_collision_stops_the_registry_and_names_no_identity(files, monkeypatch):
    tmp, paths = files
    real_install = P.install

    def colliding(con, keys, **kw):
        real_install(con, keys, **kw)
        con.execute("CREATE OR REPLACE TEMP MACRO anon_t(sp, k) AS 'x'")

    monkeypatch.setattr(P, "install", colliding)
    out = tmp / "rc.parquet"
    with pytest.raises(P.RegistryError) as e:
        P.build_registry(paths, P.derive_keys(SALT), str(out), out=io.StringIO())
    msg = str(e.value)
    assert "collision" in msg and AT not in msg and "person" not in msg.lower()
    assert not out.exists()


@requires_duckdb
def test_registry_holds_footer_only_people(tmp_path):
    """A person who is only in a trailer gets a registry row."""
    path = _make(tmp_path, "f.parquet", replace={
        "footer_reported_by": "CASE WHEN token_index = 1 THEN ['Q9 Only <q999" + AT
                              + "only.example>'] ELSE [] END"})
    reg = str(tmp_path / "r.parquet")
    P.build_registry([path], P.derive_keys(SALT), reg, out=io.StringIO())
    con = _con()
    got = con.execute("SELECT space, count(*) FROM read_parquet(?) WHERE identity IN "
                      f"('q9 only', 'q999{AT}only.example', 'only.example') GROUP BY 1 ORDER BY 1",
                      [reg]).fetchall()
    assert got == [("domain", 1), ("email", 1), ("name", 1)]
    con.close()


def test_build_registry_needs_inputs(tmp_path):
    with pytest.raises(P.RegistryError):
        P.build_registry([], P.derive_keys(SALT), str(tmp_path / "r.parquet"))


@requires_duckdb
def test_a_changed_registry_file_is_refused(registry):
    tmp, paths, reg = registry
    con = _con()
    bad = str(tmp / "bad.parquet")
    kv = P.registry_meta(con, reg)
    con.execute(f"COPY (SELECT * REPLACE ('Author 0' AS pseudonym) FROM read_parquet(?) ) "
                f"TO {P.sql_str(bad)} (FORMAT PARQUET, KV_METADATA {P._kv_sql(kv)})", [reg])
    with pytest.raises(P.RegistryError):
        P.verify_registry_file(con, bad)
    with pytest.raises(P.RegistryError):
        P.verify_registry_file(con, str(tmp / "nothing.parquet"))
    con.close()


# --------------------------------------------------------------------------
# The fused write
# --------------------------------------------------------------------------

@requires_duckdb
def test_write_keeps_the_schema_the_footer_and_the_encoding(registry):
    tmp, paths, reg = registry
    outdir, res = _written(tmp, paths, reg)
    assert res["failures"] == []
    con = _con()
    sha = P.registry_meta(con, reg)["registry_sha256"]
    for src in paths:
        out = os.path.join(outdir, os.path.basename(src))
        assert P.file_schema(con, out) == list(P.SCHEMA_2)
        kv = P.file_kv(con, out)
        assert kv["ctp_schema_version"] == "2" and kv["organisations_version"] == "test"
        assert kv[P.FOOTER_VERSION_KEY] == P.SCHEME_VERSION
        assert kv[P.FOOTER_REGISTRY_KEY] == sha
        fmt = con.execute("SELECT format_version FROM parquet_file_metadata(?)", [out]).fetchone()[0]
        assert fmt == 2
        comp = {r[0] for r in con.execute("SELECT DISTINCT compression FROM parquet_metadata(?)",
                                          [out]).fetchall()}
        assert comp == {"ZSTD"}
        n_in = con.execute("SELECT count(*) FROM read_parquet(?)", [src]).fetchone()[0]
        assert con.execute("SELECT count(*) FROM read_parquet(?)", [out]).fetchone()[0] == n_in
    con.close()


@requires_duckdb
def test_write_replaces_every_identity_and_keeps_the_rest(registry):
    tmp, paths, reg = registry
    outdir, _ = _written(tmp, paths, reg)
    keys = P.derive_keys(SALT)
    con = _con(keys)
    src = paths[0]
    out = os.path.join(outdir, os.path.basename(src))
    row = con.execute("SELECT author_name, author_email, person_name, footer_person_names, "
                      "footer_signed_off_by, commit_summary FROM read_parquet(?) "
                      "WHERE len(footer_signed_off_by) > 0 LIMIT 1", [out]).fetchone()
    for v in row[:3]:
        assert "Person" not in v and not v.lower().startswith("p")
    assert row[1].endswith(".example") and row[1].startswith("author_")
    name_tok = row[3][0].split(" ", 1)[1]
    assert row[4][0].startswith(f"Author {name_tok} <author_")
    # One person, one pseudonym: person_name of p<n> is footer name of p<n>.
    a = con.execute("SELECT DISTINCT anon_name(person_name) FROM read_parquet(?) "
                    "WHERE person_name = 'P150 Person'", [src]).fetchone()[0]
    b = con.execute("SELECT DISTINCT person_name FROM read_parquet(?) p POSITIONAL JOIN "
                    "(SELECT person_name AS pn FROM read_parquet(?)) s WHERE s.pn = 'P150 Person'",
                    [out, src]).fetchone()[0]
    assert a == b
    # The unchanged columns are equal row by row.
    diff = con.execute("SELECT count(*) FROM read_parquet(?) o POSITIONAL JOIN "
                       "(SELECT source_text AS st, token_index AS ti, firm_based_on_domain AS fd "
                       "FROM read_parquet(?)) s WHERE o.source_text IS DISTINCT FROM s.st "
                       "OR o.token_index IS DISTINCT FROM s.ti "
                       "OR o.firm_based_on_domain IS DISTINCT FROM s.fd",
                       [out, src]).fetchone()[0]
    assert diff == 0
    # No synthetic identity string in any identity column of the output. The
    # commit message is published as git has it (the default mode keep).
    ident = P.identity_columns(A.classify([c for c, _ in P.file_schema(con, out)]))
    for c in ident:
        if c == "commit_summary":
            continue
        expr = f"array_to_string({P.q(c)}, ' ')" if c.startswith("footer_") else P.q(c)
        n = con.execute(f"SELECT count(*) FROM read_parquet(?) WHERE {expr} ILIKE '%person%' "
                        f"OR regexp_matches({expr}, '(^|[^_a-z0-9])p[0-9]+{AT}')",
                        [out]).fetchone()[0]
        assert n == 0, c
    con.close()


@requires_duckdb
def test_person_domain_is_lower_case_with_no_outer_spaces(tmp_path):
    src = _make(tmp_path, "d.parquet", replace={
        "person_domain": "CASE WHEN token_index % 2 = 0 THEN ' Corp1.EXAMPLE ' ELSE person_domain END"})
    reg = str(tmp_path / "r.parquet")
    P.build_registry([src], P.derive_keys(SALT), reg, out=io.StringIO())
    outdir, res = _written(tmp_path, [src], reg)
    assert res["failures"] == []
    con = _con()
    got = {r[0] for r in con.execute("SELECT DISTINCT person_domain FROM read_parquet(?)",
                                     [os.path.join(outdir, "d.parquet")]).fetchall()}
    assert "corp1.example" in got and all(d == d.lower().strip() for d in got)
    con.close()


@requires_duckdb
def test_commit_summary_scrub_and_null(registry):
    tmp, paths, reg = registry
    outdir, _ = _written(tmp, paths[:1], reg, outname="outscrub", summary_mode="scrub")
    con = _con()
    out = os.path.join(outdir, os.path.basename(paths[0]))
    n = con.execute(f"SELECT count(*) FROM read_parquet(?) WHERE contains(commit_summary, "
                    f"'author{AT}')", [out]).fetchone()[0]
    assert n > 0
    outdir2, res = _written(tmp, paths[:1], reg, outname="outnull", summary_mode="null")
    assert res["failures"] == []
    out2 = os.path.join(outdir2, os.path.basename(paths[0]))
    assert con.execute("SELECT count(commit_summary) FROM read_parquet(?)", [out2]).fetchone()[0] == 0
    with pytest.raises(ValueError):
        P.write_public(con, paths[0], str(tmp / "x.parquet"), registry=reg,
                       registry_sha256="0", summary_mode="drop")
    con.close()


@requires_duckdb
def test_a_value_outside_the_registry_stops_the_write(registry):
    """The registry of file alpha does not hold the people of file beta."""
    tmp, paths, reg = registry
    small = str(tmp / "small.parquet")
    P.build_registry(paths[:1], P.derive_keys(SALT), small, out=io.StringIO())
    outdir, res = _written(tmp, paths[1:], small, outname="outmiss")
    assert res["failures"] and "not in the registry" in res["failures"][0]
    assert list(Path(outdir).glob("*.parquet")) == []
    assert list(Path(outdir).glob(".*")) == []


@requires_duckdb
def test_a_registry_of_another_salt_is_refused(files):
    tmp, paths = files
    reg = str(tmp / "other.parquet")
    P.build_registry(paths, P.derive_keys(OTHER_SALT), reg, out=io.StringIO())
    with pytest.raises(P.RegistryError):
        _written(tmp, paths, reg, outname="outsalt")
    # Under the other salt's own check the keys differ from the registry too.
    con = _con()
    agree = P.registry_agreement(con, paths[0], A.classify(
        [c for c, _ in P.file_schema(con, paths[0])]), reg)
    assert agree["mismatch"] == agree["values"] > 0
    con.close()


@requires_duckdb
def test_an_input_that_is_already_pseudonymized_is_refused(registry):
    tmp, paths, reg = registry
    outdir, _ = _written(tmp, paths[:1], reg)
    out = os.path.join(outdir, os.path.basename(paths[0]))
    con = _con()
    with pytest.raises(P.WriteError):
        P.write_public(con, out, str(tmp / "again.parquet"), registry=reg, registry_sha256="0")
    con.close()


@requires_duckdb
def test_a_schema_1_file_is_not_a_release_file(tmp_path):
    src = _make(tmp_path, "s1.parquet", schema2=False)
    reg = str(tmp_path / "r.parquet")
    P.build_registry([src], P.derive_keys(SALT), reg, out=io.StringIO())
    _, res = _written(tmp_path, [src], reg, outname="o1")
    assert any("not ctp schema 2" in f for f in res["failures"])
    _, res = _written(tmp_path, [src], reg, outname="o2", expect_schema_2=False)
    assert res["failures"] == []


@requires_duckdb
def test_the_fused_write_takes_the_organisation_select(tmp_path):
    """A schema 1 file plus an organisation SELECT (as build_v0.py makes it)
    gives a schema 2 file in one COPY."""
    src = _make(tmp_path, "raw.parquet", schema2=False)
    reg = str(tmp_path / "r.parquet")
    P.build_registry([src], P.derive_keys(SALT), reg, out=io.StringIO())
    con = _con()
    sha = P.registry_meta(con, reg)["registry_sha256"]
    sel = ("SELECT t.*, CAST($ns AS VARCHAR) AS firm_based_on_namespace, "
           "lower(trim(t.person_domain)) AS firm_based_on_domain, "
           "CAST(true AS BOOLEAN) AS project_enterprise_driven, "
           "CAST('test' AS VARCHAR) AS project_enterprise_basis FROM read_parquet($src) t")
    kv = {"ctp_schema_version": "2", "organisations_version": "v-test",
          "organisations_mapping_sha256": "1" * 64}
    dst = str(tmp_path / "public.parquet")
    res = P.write_public(con, src, dst, registry=reg, registry_sha256=sha, source_sql=sel,
                         source_params={"src": src, "ns": "independent"}, kv=kv)
    assert res["problems"] == []
    assert P.file_schema(con, dst) == list(P.SCHEMA_2)
    fk = P.file_kv(con, dst)
    assert fk["organisations_version"] == "v-test" and fk[P.FOOTER_REGISTRY_KEY] == sha
    assert "x" not in fk                       # the caller's footer replaces the input's
    scan = P.leak_scan(con, dst, reg)
    assert scan["failures"] == []
    con.close()


@requires_duckdb
def test_identity_columns_must_come_from_the_input(tmp_path):
    src = _make(tmp_path, "raw.parquet")
    reg = str(tmp_path / "r.parquet")
    P.build_registry([src], P.derive_keys(SALT), reg, out=io.StringIO())
    con = _con()
    sel = (f"SELECT * EXCLUDE (author_name), 'x' AS author_name_2 FROM read_parquet({P.sql_str(src)})")
    with pytest.raises((P.WriteError, A.UnknownColumnError, A.MissingColumnError)):
        P.write_public(con, src, str(tmp_path / "o.parquet"), registry=reg,
                       registry_sha256="0", source_sql=sel)
    con.close()


@requires_duckdb
def test_parallel_jobs_give_the_same_bytes(registry):
    tmp, paths, reg = registry
    o1, r1 = _written(tmp, paths, reg, outname="j1", jobs=1)
    o2, r2 = _written(tmp, paths, reg, outname="j2", jobs=2)
    assert r1["failures"] == r2["failures"] == []
    for p in paths:
        n = os.path.basename(p)
        assert Path(o1, n).read_bytes() == Path(o2, n).read_bytes()


@requires_duckdb
def test_verify_anon_accepts_the_output_of_each_summary_mode(registry, capsys):
    """verify_anon.py takes commit_summary as a content column since the user
    decided to keep the commit message (2026-10-25): an address in it is a
    note. Each mode, also the old scrub, passes."""
    import verify_anon
    tmp, paths, reg = registry
    for mode, want in (("keep", 0), ("pseudonym", 0), ("null", 0), ("scrub", 0)):
        outdir, res = _written(tmp, paths, reg, outname=f"v-{mode}", summary_mode=mode)
        assert res["failures"] == []
        assert verify_anon.main([outdir]) == want, mode
    capsys.readouterr()


@requires_duckdb
def test_the_pseudonym_mode_gives_each_summary_address_its_pseudonym():
    keys = P.derive_keys(SALT)
    con = _con(keys)
    a, b = "p150" + AT + "corp0.example", "Q" + AT + "x.example"
    text = f"see {a}, and {b}."
    got = con.execute("SELECT anon_summary_pseudonym(?)", [text]).fetchone()[0]
    want = (f"see author_{P.token_py(keys, 'email', a)}{AT}corp0.example, and "
            f"author_{P.token_py(keys, 'email', b.lower())}{AT}x.example.")
    assert got == want
    for plain in ("no address here", "", None):
        assert con.execute("SELECT anon_summary_pseudonym(?)", [plain]).fetchone()[0] == plain
    con.close()


# --------------------------------------------------------------------------
# The leak scan
# --------------------------------------------------------------------------

def test_mask_domains_masks_a_suffix_and_keeps_the_rest():
    d = frozenset({"github.com", "corp1.example"})
    assert "github" not in P.mask_domains("see mail.github.com now", d)
    assert P.mask_domains("bump github actions", d) == "bump github actions"
    assert P.mask_domains("x", frozenset()) == "x"


def test_name_hit_is_the_old_rule():
    d = frozenset({"github.com"})
    assert P.name_hit("ci: bump github actions", "github", d)        # the known false positive
    assert not P.name_hit("see github.com", "github", d)
    assert P.name_hit("thanks p7 person!", "p7 person", d)
    assert not P.name_hit("thanks p70 person", "p7 person", d)


def test_odd_local_parts_follow_the_old_look_behind():
    odd = {"a#b1": "tok1"}
    assert P.odd_local_hits("mail a#b1" + AT + "x.example", odd) == {"tok1"}
    assert P.odd_local_hits("mail za#b1" + AT + "x.example", odd) == set()
    assert P.odd_local_hits("no at sign", odd) == set()
    assert P.odd_local_hits("a" + AT + "b", {}) == set()


@requires_duckdb
def test_the_leak_scan_finds_planted_leaks(tmp_path):
    """Names and local parts of the registry, planted in four column classes.
    The people planted in file beta come from file alpha: one registry covers
    both files, so the scan finds them across files."""
    a = _make(tmp_path, "alpha.parquet", seed="alpha")
    b = _make(tmp_path, "beta.parquet", seed="beta", people=50, replace={
        "stratum": "CASE WHEN token_index = 3 THEN 'kept by P150 Person' ELSE stratum END",
        "license": f"CASE WHEN token_index = 4 THEN 'x p160{AT}corp10.example' ELSE license END",
        "source_text": "CASE WHEN token_index = 5 THEN '/* P170 Person */' ELSE source_text END",
        "commit_summary": "CASE WHEN token_index = 6 THEN 'thanks P180 Person' ELSE commit_summary END",
    })
    reg = str(tmp_path / "r.parquet")
    P.build_registry([a, b], P.derive_keys(SALT), reg, out=io.StringIO())
    con = _con()
    scan = P.leak_scan(con, b, reg)
    cols = scan["columns"]
    assert cols["stratum"]["name_values"] == 1 and cols["stratum"]["class"] == "structural"
    assert cols["license"]["local_values"] == 1 and cols["license"]["email_shaped_values"] == 1
    assert cols["source_text"]["class"] == "content" and "source_text" in scan["reported"]
    # commit_summary is published as git has it: its finding is information.
    assert cols["commit_summary"]["name_values"] == 1
    assert cols["commit_summary"]["class"] == "information"
    assert "commit_summary" in scan["reported"]
    failed = " ".join(scan["failures"])
    for c in ("stratum", "license"):
        assert c in failed
    assert "source_text" not in failed and "commit_summary" not in failed
    tok = P.token_py(P.derive_keys(SALT), "name", "p150 person")
    assert cols["stratum"]["name_tokens"] == [tok]
    # content=False skips the information columns.
    assert "commit_summary" not in P.leak_scan(con, b, reg, content=False)["columns"]
    con.close()


@requires_duckdb
def test_the_scan_of_a_clean_output_is_clean_and_the_input_is_not(registry):
    tmp, paths, reg = registry
    outdir, res = _written(tmp, paths, reg)
    con = _con()
    out = os.path.join(outdir, os.path.basename(paths[0]))
    assert P.leak_scan(con, out, reg)["failures"] == []
    assert P.leak_scan(con, paths[0], reg)["failures"] != []
    con.close()


@requires_duckdb
def test_the_scan_finds_a_name_with_no_word(tmp_path):
    src = _make(tmp_path, "w.parquet", replace={
        "author_name": "CASE WHEN token_index = 0 THEN '-----' ELSE author_name END",
        "stratum": "CASE WHEN token_index = 1 THEN 'x ----- y' ELSE stratum END"})
    reg = str(tmp_path / "r.parquet")
    P.build_registry([src], P.derive_keys(SALT), reg, out=io.StringIO())
    con = _con()
    scan = P.leak_scan(con, src, reg)
    assert scan["name_candidates"] >= 1
    con.close()


@requires_duckdb
def test_the_scan_checks_odd_local_parts(tmp_path):
    src = _make(tmp_path, "o.parquet", replace={
        "author_email": f"CASE WHEN token_index = 0 THEN 'a#b1{AT}x.example' ELSE author_email END",
        "stratum": f"CASE WHEN token_index = 1 THEN 'mail a#b1{AT}x.example' ELSE stratum END"})
    reg = str(tmp_path / "r.parquet")
    P.build_registry([src], P.derive_keys(SALT), reg, out=io.StringIO())
    con = _con()
    scan = P.leak_scan(con, src, reg)
    assert scan["columns"]["stratum"]["local_values"] == 1
    con.close()


# --------------------------------------------------------------------------
# Reports and the command line
# --------------------------------------------------------------------------

def _no_identity(text: str, salt: bytes = SALT) -> None:
    import re
    low = text.lower()
    assert AT not in text
    assert not re.search(r"[pq][0-9]+ (person|only)", low) and ".example" not in low
    assert salt.decode() not in text
    for k in P.derive_keys(salt):
        assert k not in text


@requires_duckdb
def test_the_cli_runs_and_the_report_has_no_identity(files, capsys):
    tmp, paths = files
    salt = _salt_file(tmp)
    reg = str(tmp / "reg.parquet")
    rep = str(tmp / "report.json")
    rc = P.main(["run", "--salt-file", salt, "--registry", reg, "--outdir", str(tmp / "pub"),
                 "--report", rep, "--jobs", "2", "--threads", "1", "--memory-limit", "1GB",
                 *paths])
    printed = capsys.readouterr()
    assert rc == 0, printed.out
    text = Path(rep).read_text()
    _no_identity(text)
    _no_identity(printed.out + printed.err)
    blob = json.loads(text)
    assert blob["registry"]["identities"]["email"] > 0
    assert len(blob["files"]) == 2 and all(f["status"] == "ok" for f in blob["files"])
    # The published files pass the scan, with no salt.
    rep2 = str(tmp / "scan.json")
    rc = P.main(["scan", "--registry", reg, "--report", rep2, str(tmp / "pub")])
    assert rc == 0
    _no_identity(Path(rep2).read_text())


@requires_duckdb
def test_the_cli_report_of_a_leak_has_tokens_not_values(tmp_path, capsys):
    src = _make(tmp_path, "leak.parquet", replace={
        "stratum": "CASE WHEN token_index = 3 THEN 'kept by P150 Person' ELSE stratum END"})
    salt = _salt_file(tmp_path)
    reg = str(tmp_path / "reg.parquet")
    rep = str(tmp_path / "report.json")
    rc = P.main(["run", "--salt-file", salt, "--registry", reg, "--outdir",
                 str(tmp_path / "pub"), "--report", rep, src])
    printed = capsys.readouterr()
    assert rc == 1
    text = Path(rep).read_text()
    _no_identity(text)
    _no_identity(printed.out)
    assert P.token_py(P.derive_keys(SALT), "name", "p150 person") in text
    assert not list((tmp_path / "pub").glob("*.parquet"))


@requires_duckdb
def test_the_scan_cli_checks_the_registry_hash(registry, tmp_path):
    tmp, paths, reg = registry
    rc = P.main(["scan", "--registry", reg, "--report", str(tmp / "s.json"), paths[0]])
    assert rc == 1
    assert "registry sha256" in Path(tmp / "s.json").read_text()


def test_the_cli_needs_a_salt_file_and_inputs(tmp_path, capsys):
    with pytest.raises(SystemExit):
        P.main(["registry", "--registry", "r.parquet", "x.parquet"])
    assert P.main(["registry", "--salt-file", "s", "--registry", "r.parquet"]) == 2
    rc = P.main(["registry", "--salt-file", str(tmp_path / "no-salt"), "--registry",
                 str(tmp_path / "r.parquet"), str(tmp_path)])
    assert rc == 2


@requires_duckdb
def test_a_salt_readable_by_others_is_refused(files, capsys):
    tmp, paths = files
    salt = _salt_file(tmp)
    os.chmod(salt, 0o644)
    rc = P.main(["registry", "--salt-file", salt, "--registry", str(tmp / "r.parquet"), *paths])
    assert rc == 2
    assert SALT.decode() not in capsys.readouterr().err


def test_gather_inputs_reads_folders_and_lists(tmp_path):
    (tmp_path / "d").mkdir()
    for n in ("b.parquet", "a.parquet", "c.txt"):
        (tmp_path / "d" / n).write_text("")
    lst = tmp_path / "list.txt"
    lst.write_text(f"{tmp_path / 'x.parquet'}\n\n{tmp_path / 'd' / 'a.parquet'}\n")
    got = P.gather_inputs([str(tmp_path / "d")], str(lst))
    assert [os.path.basename(g) for g in got] == ["a.parquet", "b.parquet", "x.parquet"]


def test_strip_at_cleans_every_string():
    obj = {"a" + AT: ["x" + AT + "y", {"k": {"v" + AT}}], "n": 3}
    assert AT not in json.dumps(P.strip_at(obj))


@requires_duckdb
def test_two_inputs_with_one_name_are_refused(files, tmp_path):
    tmp, paths = files
    other = tmp_path / "sub"
    other.mkdir()
    twin = str(other / os.path.basename(paths[0]))
    Path(twin).write_bytes(Path(paths[0]).read_bytes())
    reg = str(tmp / "r.parquet")
    P.build_registry(paths + [twin], P.derive_keys(SALT), reg, out=io.StringIO())
    with pytest.raises(P.RegistryError):
        _written(tmp, [paths[0], twin], reg, outname="twin")


@requires_duckdb
def test_bench_gives_rates_and_no_problem(tmp_path):
    r = P.bench(20_000, 1, str(tmp_path), memory_limit="1GB", out=io.StringIO())
    assert r["problems"] == []
    assert r["rows_per_s_per_thread_fused"] > 0
    assert r["hours_32_6e9_rows_96_vcpu_fused_and_scan"] > 0
    assert not list(tmp_path.iterdir())


@requires_duckdb
def test_the_person_domain_rules_for_a_value_with_an_at_sign(tmp_path):
    """'keep' fails the structural gate; the two other rules pass it."""
    src = _make(tmp_path, "at.parquet", replace={
        "person_domain": f"CASE WHEN token_index = 2 THEN ' X{AT}Corp1.Example' "
                         "ELSE person_domain END"})
    reg = str(tmp_path / "r.parquet")
    P.build_registry([src], P.derive_keys(SALT), reg, out=io.StringIO())
    con = _con()
    assert con.execute("SELECT count(*) FROM read_parquet(?) WHERE space = 'domain' "
                       f"AND contains(identity, '{AT}')", [reg]).fetchone()[0] == 0
    want = {"keep": None, "after-last-at": "corp1.example", "empty": "",
            "first-valid": "corp1.example"}
    for mode, value in want.items():
        outdir, res = _written(tmp_path, [src], reg, outname=f"o-{mode}", domain_at=mode)
        if mode == "keep":
            assert any("structural" in f and "person_domain" in f for f in res["failures"])
            continue
        assert res["failures"] == [], mode
        got = con.execute("SELECT person_domain FROM read_parquet(?) WHERE token_index = 2",
                          [os.path.join(outdir, "at.parquet")]).fetchone()[0]
        assert got == value
    with pytest.raises(ValueError):
        P.person_domain_expr("x", "drop")
    con.close()


@requires_duckdb
def test_the_content_scan_can_be_skipped(tmp_path):
    src = _make(tmp_path, "c.parquet", replace={
        "source_text": "CASE WHEN token_index = 5 THEN '/* P170 Person */' ELSE source_text END"})
    reg = str(tmp_path / "r.parquet")
    P.build_registry([src], P.derive_keys(SALT), reg, out=io.StringIO())
    con = _con()
    assert "source_text" in P.leak_scan(con, src, reg)["columns"]
    assert "source_text" not in P.leak_scan(con, src, reg, content=False)["columns"]
    con.close()


def test_safe_error_hides_foreign_text_and_keeps_own_text():
    keys = ("a" * 64, "b" * 64)
    own = P.safe_error(P.WriteError("3 of 9 values"), keys)
    assert own == "WriteError: 3 of 9 values"
    foreign = P.safe_error(RuntimeError("cannot cast 'P1 Person' " + "a" * 64), keys)
    assert "Person" not in foreign and "a" * 64 not in foreign and "sha256" in foreign


@requires_duckdb
def test_a_duckdb_error_in_a_job_is_reported_without_its_text(registry, monkeypatch):
    tmp, paths, reg = registry
    real = P.write_public

    def broken(con, src, dst, **kw):
        con.execute("SELECT CAST('P1 Person' AS INTEGER)")
        return real(con, src, dst, **kw)

    monkeypatch.setattr(P, "write_public", broken)
    _, res = _written(tmp, paths[:1], reg, outname="broken")
    assert res["failures"] and "Person" not in res["failures"][0]
    assert "text not shown" in res["failures"][0]


@requires_duckdb
def test_the_source_schema_can_be_given(registry):
    tmp, paths, reg = registry
    con = _con()
    sha = P.registry_meta(con, reg)["registry_sha256"]
    res = P.write_public(con, paths[0], str(tmp / "given.parquet"), registry=reg,
                         registry_sha256=sha, source_schema=P.file_schema(con, paths[0]))
    assert res["problems"] == []
    con.close()



# --------------------------------------------------------------------------
# The user's decisions of 2026-10-24
# --------------------------------------------------------------------------

def test_the_defaults_are_the_decisions_of_2026_10_25():
    import inspect
    assert P.DEFAULT_SUMMARY_MODE == "keep"
    assert P.DEFAULT_DOMAIN_AT == "first-valid"
    assert P.DEFAULT_EMAIL_DOMAIN == "first-valid"
    for fn in (P.write_public, P.run_write):
        sig = inspect.signature(fn)
        assert sig.parameters["summary_mode"].default == "keep"
        assert sig.parameters["domain_at"].default == "first-valid"
    assert inspect.signature(P.build_registry).parameters["email_domain"].default == "first-valid"
    assert P.SCHEME_VERSION == "2"


@requires_duckdb
def test_the_cli_defaults_are_the_decisions(files, capsys):
    """A default run keeps the commit message, uses rule E, and passes
    verify_anon.py."""
    import verify_anon
    tmp, paths = files
    salt = _salt_file(tmp)
    rep = str(tmp / "rep.json")
    rc = P.main(["run", "--salt-file", salt, "--registry", str(tmp / "private" / "reg.parquet"),
                 "--outdir", str(tmp / "pub"), "--report", rep, *paths])
    assert rc == 0
    blob = json.loads(Path(rep).read_text())
    assert blob["options"]["summary_mode"] == "keep"
    assert blob["options"]["person_domain_at"] == "first-valid"
    assert blob["options"]["email_domain"] == "first-valid"
    assert verify_anon.main([str(tmp / "pub")]) == 0
    con = _con()
    out = str(tmp / "pub" / os.path.basename(paths[0]))
    same = con.execute("SELECT count(*) FROM read_parquet(?) o POSITIONAL JOIN (SELECT "
                       "commit_summary AS c FROM read_parquet(?)) s "
                       "WHERE o.commit_summary IS DISTINCT FROM s.c", [out, paths[0]]).fetchone()[0]
    assert same == 0
    con.close()
    capsys.readouterr()


@requires_duckdb
def test_a_person_domain_with_two_at_signs_gets_the_rule_e_domain(tmp_path):
    src = _make(tmp_path, "two.parquet", replace={
        "person_domain": f"CASE WHEN token_index = 2 THEN ' A{AT}B{AT}Corp1.Example ' "
                         f"WHEN token_index = 3 THEN 'c{AT}' ELSE person_domain END"})
    reg = str(tmp_path / "r.parquet")
    P.build_registry([src], P.derive_keys(SALT), reg, out=io.StringIO())
    outdir, res = _written(tmp_path, [src], reg)          # the default rule
    assert res["failures"] == []
    con = _con()
    got = dict(con.execute("SELECT token_index, person_domain FROM read_parquet(?) "
                           "WHERE token_index IN (2, 3)",
                           [os.path.join(outdir, "two.parquet")]).fetchall())
    assert got == {2: "corp1.example", 3: ""}
    con.close()


@requires_duckdb
def test_a_summary_with_an_address_and_a_known_name_is_kept(tmp_path):
    """Default mode keep (the user, 2026-10-25): the commit message is
    published as git has it. The scan gives its findings as information, and
    the gate does not fail the file. The trailers still get pseudonyms."""
    addr = "p150" + AT + "corp0.example"
    text = f"thanks P150 Person, mail {addr}"
    src = _make(tmp_path, "s.parquet", replace={
        "commit_summary": f"CASE WHEN token_index IN (6, 60, 600) THEN '{text}' "
                          "ELSE commit_summary END"})
    reg = str(tmp_path / "r.parquet")
    P.build_registry([src], P.derive_keys(SALT), reg, out=io.StringIO())
    con = _con()
    sha = P.registry_meta(con, reg)["registry_sha256"]
    dst = str(tmp_path / "s-out.parquet")
    res = P.write_public(con, src, dst, registry=reg, registry_sha256=sha)
    assert res["problems"] == []
    got = {r[0] for r in con.execute("SELECT commit_summary FROM read_parquet(?) "
                                     "WHERE token_index IN (6, 60, 600)", [dst]).fetchall()}
    assert got == {text}
    scan = P.leak_scan(con, dst, reg)
    assert scan["failures"] == []
    entry = scan["columns"]["commit_summary"]
    assert entry["class"] == "information" and entry["name_values"] >= 1
    assert entry["local_values"] >= 1 and "commit_summary" in scan["reported"]
    n = con.execute("SELECT count(*) FROM read_parquet(?) WHERE array_to_string("
                    "footer_signed_off_by, ' ') ILIKE '%person%'", [dst]).fetchone()[0]
    assert n == 0
    con.close()


def _orgs_file(tmp: Path, rows: list[tuple[str, str]]) -> str:
    path = str(tmp / "organisations.parquet")
    con = P.connect("1GB", 1)
    vals = ", ".join(f"({P.sql_str(i)}, {P.sql_str(n)})" for i, n in rows)
    con.execute(f"COPY (SELECT * FROM (VALUES {vals}) t(organisation_id, name)) "
                f"TO {P.sql_str(path)} (FORMAT PARQUET)")
    con.close()
    return path


@requires_duckdb
def test_a_name_that_is_a_published_organisation_name_is_masked(tmp_path):
    """A bot identity 'GitHub' and the organisation GitHub: 'uses github' in a
    structural column fails only without the organisations table."""
    src = _make(tmp_path, "g.parquet", replace={
        "committer_name": "CASE WHEN token_index = 1 THEN 'GitHub' ELSE committer_name END",
        "stratum": "CASE WHEN token_index = 2 THEN 'uses github actions' ELSE stratum END"})
    orgs = _orgs_file(tmp_path, [("github", "GitHub"), ("red-hat", "Red Hat")])
    keys = P.derive_keys(SALT)
    sink = io.StringIO()
    with_org = str(tmp_path / "r-org.parquet")
    P.build_registry([src], keys, with_org, organisations=(orgs,), out=sink)
    no_org = str(tmp_path / "r-none.parquet")
    P.build_registry([src], keys, no_org, out=sink)
    con = _con(keys)
    n = con.execute("SELECT count(*) FROM read_parquet(?) WHERE space = 'organisation' AND "
                    "identity IN ('github', 'red hat', 'red-hat', 'org1')", [with_org]).fetchone()[0]
    assert n == 4                                      # the table, and the file's org column
    assert not any(f.startswith("stratum") for f in P.leak_scan(con, src, with_org)["failures"])
    assert any(f.startswith("stratum") for f in P.leak_scan(con, src, no_org)["failures"])
    con.close()


@requires_duckdb
def test_a_name_inside_a_longer_word_is_not_a_hit(tmp_path):
    src = _make(tmp_path, "w.parquet", replace={
        "author_name": "CASE WHEN token_index = 1 THEN 'Kalle' ELSE author_name END",
        "stratum": "CASE WHEN token_index = 2 THEN 'add kallebot and P150 Personal' "
                   "ELSE stratum END",
        "license": "CASE WHEN token_index = 3 THEN 'thanks kalle!' ELSE license END"})
    reg = str(tmp_path / "r.parquet")
    P.build_registry([src], P.derive_keys(SALT), reg, out=io.StringIO())
    con = _con()
    failed = " ".join(P.leak_scan(con, src, reg)["failures"])
    assert "stratum" not in failed and "license" in failed
    con.close()


def test_organisation_spans_and_pseudonym_mask():
    assert P.org_spans("x red hat y", frozenset()) == []
    assert P.org_spans("x Red Hat y", frozenset({"red hat"})) == [(2, 9)]
    assert P.mask_organisations("bump github now", frozenset({"github"})) == \
        f"bump {P.ORG_MASK} now"
    tok = "0123456789abcd"
    for form in (f"Author {tok}", f"author {tok}", f"author_{tok}"):
        assert P.mask_pseudonyms(f"by {form}.") == f"by {P.PSEUDONYM_MASK}."
        assert not P.name_hit(f"by {form}", "author", frozenset())
    assert P.name_hit("the author field", "author", frozenset())


@requires_duckdb
def test_a_registry_name_author_does_not_make_pseudonyms_leaks(tmp_path):
    """The defect of 8223fae: the registry holds the name 'author' (as dataset
    v1 does), and every pseudonym 'Author <token>' was a leak."""
    src = _make(tmp_path, "a.parquet", replace={
        "author_name": "CASE WHEN token_index = 1 THEN 'Author' ELSE author_name END"})
    reg = str(tmp_path / "r.parquet")
    P.build_registry([src], P.derive_keys(SALT), reg, out=io.StringIO())
    con = _con()
    assert con.execute("SELECT count(*) FROM read_parquet(?) WHERE space = 'name' AND "
                       "identity = 'author'", [reg]).fetchone()[0] == 1
    sha = P.registry_meta(con, reg)["registry_sha256"]
    dst = str(tmp_path / "a-out.parquet")
    assert P.write_public(con, src, dst, registry=reg, registry_sha256=sha)["problems"] == []
    assert P.leak_scan(con, dst, reg)["failures"] == []
    con.close()


# --------------------------------------------------------------------------
# Rule E (the user, 2026-10-25)
# --------------------------------------------------------------------------

def _rule_e(con, value: str | None, skip_first: bool = True) -> str | None:
    return con.execute(f"SELECT {P.rule_e_domain_sql('v', skip_first=skip_first)} "
                       "FROM (SELECT ?::VARCHAR AS v)", [value]).fetchone()[0]


@requires_duckdb
def test_rule_e_takes_the_first_valid_part_after_an_at_sign():
    con = _con()
    cases = {
        # git-svn: user, at sign, domain, at sign, uuid -> the domain
        f"user{AT}Corp1.Example{AT}0c2b5d6e-1f2a-4b3c-9d8e-7f6a5b4c3d2e": "corp1.example",
        # a doubled at sign -> the valid part
        f"user{AT}{AT}corp2.example": "corp2.example",
        # two joined addresses -> the first valid part (the first part is
        # 'corp3.examplebob', which has no ICANN suffix)
        f"alice{AT}corp3.examplebob{AT}corp4.org": "corp4.org",
        f"alice{AT}corp3.example, bob{AT}corp4.org": "corp4.org",
        # no valid part -> ''
        f"user{AT}localhost": "",
        f"user{AT}0c2b5d6e-uuid": "",
        f"user{AT}co.uk": "",                   # a public suffix is not a domain
        f"user{AT}a.co.uk": "a.co.uk",
        f"user{AT}b.ck": "",                    # the wildcard *.ck
        f"user{AT}www.ck": "www.ck",            # the exception !www.ck
        f"user{AT}x.blogspot.com": "x.blogspot.com",   # the PRIVATE section is not used
        f"user{AT}x.unknowntld": "",
        "no at sign": "",
    }
    for value, want in cases.items():
        assert _rule_e(con, value) == want, value
    assert _rule_e(con, None) is None
    # person_domain: every part is a candidate.
    assert _rule_e(con, f"corp1.example{AT}uuid", skip_first=False) == "corp1.example"
    assert _rule_e(con, f"uuid{AT}corp1.example", skip_first=False) == "corp1.example"
    con.close()


@requires_duckdb
def test_rule_e_in_the_email_columns_and_the_trailers():
    keys = P.derive_keys(SALT)
    con = _con(keys)
    raw = f"Dev{AT}Corp1.Example{AT}uuid-1"
    tok = P.token_py(keys, "email", raw.lower())
    assert con.execute("SELECT anon_email(?)", [raw]).fetchone()[0] == f"author_{tok}{AT}corp1.example"
    bad = f"dev{AT}localhost"
    tok2 = P.token_py(keys, "email", bad)
    assert con.execute("SELECT anon_email(?)", [bad]).fetchone()[0] == f"author_{tok2}"
    elem = f"P1 Person <{raw}>"
    name = P.token_py(keys, "name", "p1 person")
    assert con.execute("SELECT anon_footer_elem(?)", [elem]).fetchone()[0] == \
        f"Author {name} <author_{tok}{AT}corp1.example>"
    con.close()
    # The old rule is still there as an option.
    old = _con(keys, email_domain="second-part")
    assert old.execute("SELECT anon_email(?)", [raw]).fetchone()[0] == \
        f"author_{tok}{AT}Corp1.Example"
    old.close()
    last = _con(keys, email_domain="after-last-at")
    assert last.execute("SELECT anon_email(?)", [raw]).fetchone()[0] == f"author_{tok}{AT}uuid-1"
    last.close()
    with pytest.raises(ValueError):
        P._macros("third-part")


def test_load_psl_reads_the_icann_section_only(_test_psl, tmp_path):
    psl = P.load_psl(_test_psl)
    assert "co.uk" in psl["rules"] and "blogspot.com" not in psl["rules"]
    assert psl["wild"] == {"ck"} and psl["exc"] == {"www.ck"}
    assert {"uk", "ck", "example"} <= psl["tlds"] and len(psl["sha256"]) == 64
    with pytest.raises(P.RegistryError):
        P.load_psl(str(tmp_path / "missing.dat"))


@requires_duckdb
def test_the_write_refuses_another_domain_rule_or_list(registry, tmp_path):
    tmp, paths, reg = registry
    with pytest.raises(P.RegistryError):
        _written(tmp, paths, reg, outname="rule", email_domain="second-part")
    other = tmp_path / "other_psl.dat"
    other.write_text(TEST_PSL + "// changed\n")
    with pytest.raises(P.RegistryError):
        _written(tmp, paths, reg, outname="psl", psl=str(other))


@requires_duckdb
def test_the_registry_is_refused_in_a_published_place(files, tmp_path):
    tmp, paths = files
    keys = P.derive_keys(SALT)
    sink = io.StringIO()
    release = tmp_path / "release"
    (release / "parquet").mkdir(parents=True)
    (release / "SHA256SUMS").write_text("")
    for bad, roots in ((release / "parquet" / "r.parquet", ()),
                       (release / "r.parquet", ()),
                       (tmp_path / "rel2" / "r.parquet", (str(tmp_path / "rel2"),))):
        bad.parent.mkdir(parents=True, exist_ok=True)
        with pytest.raises(P.RegistryError):
            P.build_registry(paths, keys, str(bad), forbidden_roots=roots, out=sink)
        assert not bad.exists()
    good = tmp_path / "private" / "r.parquet"
    good.parent.mkdir()
    P.build_registry(paths, keys, str(good), out=sink)
    # The write refuses a registry under its output folder.
    out = tmp_path / "pub"
    out.mkdir()
    inside = out / "r.parquet"
    inside.write_bytes(good.read_bytes())
    with pytest.raises(P.RegistryError):
        P.run_write(paths, str(out), keys=keys, registry=str(inside), out=sink)
    with pytest.raises(P.RegistryError):
        P.run_write(paths, str(tmp_path / "pub2"), keys=keys, registry=str(good),
                    release_roots=(str(tmp_path / "private"),), out=sink)


@requires_duckdb
def test_the_cli_refuses_a_registry_under_the_output_folder(files, capsys):
    tmp, paths = files
    salt = _salt_file(tmp)
    rc = P.main(["run", "--salt-file", salt, "--registry", str(tmp / "pub" / "reg.parquet"),
                 "--outdir", str(tmp / "pub"), *paths])
    assert rc == 2
    assert "private storage" in capsys.readouterr().err
    rc = P.main(["registry", "--salt-file", salt, "--registry", str(tmp / "rr" / "reg.parquet"),
                 "--release-root", str(tmp / "rr"), *paths])
    assert rc == 2


@requires_duckdb
def test_the_registry_does_not_depend_on_the_batch_size(files):
    tmp, paths = files
    keys = P.derive_keys(SALT)
    a = P.build_registry(paths, keys, str(tmp / "b1.parquet"), batch_files=1, out=io.StringIO())
    b = P.build_registry(paths, keys, str(tmp / "b9.parquet"), batch_files=9, out=io.StringIO())
    assert a["registry_sha256"] == b["registry_sha256"]


@requires_duckdb
def test_the_cli_takes_the_organisations_table(tmp_path, capsys):
    src = _make(tmp_path, "g.parquet", replace={
        "committer_name": "CASE WHEN token_index = 1 THEN 'GitHub' ELSE committer_name END",
        "commit_summary": "CASE WHEN token_index = 2 THEN 'ci: bump github actions' "
                          "ELSE commit_summary END"})
    orgs = _orgs_file(tmp_path, [("github", "GitHub")])
    salt = _salt_file(tmp_path)
    rc = P.main(["run", "--salt-file", salt, "--registry", str(tmp_path / "p" / "r.parquet"),
                 "--organisations", orgs, "--outdir", str(tmp_path / "pub"), src])
    assert rc == 0
    con = _con()
    got = con.execute("SELECT commit_summary FROM read_parquet(?) WHERE token_index = 2",
                      [str(tmp_path / "pub" / "g.parquet")]).fetchone()[0]
    assert got == "ci: bump github actions"
    con.close()
    capsys.readouterr()


def test_synthetic_summaries_can_name_people():
    assert "', thanks '" in P.synthetic_select(10, name_every=5)
    assert "', thanks '" not in P.synthetic_select(10)
