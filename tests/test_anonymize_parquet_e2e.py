"""End-to-end tests for anonymize_parquet.py over real Parquet files.

tests/test_anonymize_parquet.py covers the pure logic: classification, footer
parsing, the registry, SQL generation and the leak scanner. None of it writes a
Parquet, so until now the six release claims were enforced only by assertions
inside anonymize_parquet.run() at release time. A claim that is checked only when
someone runs the tool by hand is not a tested claim.

This file states each claim as a test over a written and re-read Parquet:

  1. one person, one pseudonym, through author_email, through person_email and
     through a Co-authored-by footer, and across two files of one invocation
  2. the multiset of e-mail domains is unchanged -- firm attribution needs it
  3. rows, columns and the 70-column schema contract survive
  4. a footer trailer stays a well-formed trailer, not mangled text and not NULL
  5. two runs produce byte-identical Parquet files
  6. failure is closed: an unknown column, a too-narrow input and identity text
     in commit_summary each stop the run instead of publishing

These write and read actual Parquet, so they need the real duckdb from
`devenv shell` and skip without it.
"""
from __future__ import annotations

import io
import re
import sys
from pathlib import Path

import pytest

import anonymize_parquet as A
import validate_schema as vs

CONTRACT = vs.EXPECTED_COLUMNS

try:                                          # pragma: no cover - import plumbing
    import duckdb as _duckdb
except ModuleNotFoundError:                   # pragma: no cover - import plumbing
    _duckdb = None

# An import check alone is NOT enough. tests/test_consolidate.py installs a stub
# `duckdb` module into sys.modules, and it sorts before this file, so by the time
# this module is imported the name resolves to a stub whose every call raises.
# Detect the stub the way tests/test_consolidate.py labels it.
_STUBBED = getattr(sys.modules.get("duckdb"), "__doc__", "") or ""
requires_duckdb = pytest.mark.skipif(
    _duckdb is None or _STUBBED.startswith("Test stub"),
    reason="needs real duckdb (devenv shell) to write parquet")


# --------------------------------------------------------------------------
# Fixture construction
# --------------------------------------------------------------------------

def _lit(value, typ: str) -> str:
    """One SQL literal, cast to the contract type.

    Casting rather than letting duckdb infer is what makes the fixture a
    legitimate stand-in for a published project: validate_schema has to accept
    the result, so a typo fails here instead of quietly narrowing the tests.
    """
    if value is None:
        return f"cast(null as {typ})"
    if typ == "VARCHAR[]":
        inner = ", ".join("'" + str(v).replace("'", "''") + "'" for v in value)
        return f"cast([{inner}] as VARCHAR[])"
    if typ == "BIGINT":
        return f"cast({int(value)} as BIGINT)"
    return "cast('" + str(value).replace("'", "''") + f"' as {typ})"


def write_dataset(path: Path, rows: list[dict], columns=CONTRACT) -> Path:
    """A Parquet with `columns` in contract order, one row per dict."""
    selects = []
    for row in rows:
        parts = [f'{_lit(row.get(name), typ)} as "{name}"'
                 for name, typ in columns]
        selects.append("select " + ", ".join(parts))
    _duckdb.sql(f"copy ({' union all '.join(selects)}) "
                f"to '{path}' (format parquet)")
    return path


# Per-project provenance and the token stream. Deliberately free of any identity
# name: these columns are audited as structural pass-through, and a name in one
# of them is a release FAILURE, so the fixture must not smuggle one in.
BASE = {
    "repo_name": "acme__widget", "clone_url": "https://example.test/acme/widget",
    "provenance_status": "candidates.csv", "source": "roster", "stratum": "a",
    "fact": "control", "contested": "no", "label_date": "2026-01-01",
    "owner": "acme", "repo": "widget", "roster_name": "rosterA",
    "roster_lang": "C", "language": "C", "commits": "10", "size_class": "sm",
    "size_kb": "100", "stars": "5", "pushed_at": "2026-01-01", "license": "MIT",
    "owner_type": "Organization", "archived": "false", "fork": "false",
    "history_cluster": "", "history_shared_with": "", "history_relation": "",
    "history_includes": "", "history_first": "", "history_created": "",
    "manifest_category": "control", "file_mask": r"\.c$",
    "file_path": "src/main.c", "token_index": 1, "source_line": 1,
    "source_col": 1, "source_text": "int argc", "token_type": "name",
    "token_value": "argc", "is_structural": 0,
    "cregit_commit_sha": "a" * 40, "original_commit_sha": "b" * 40,
    "author_date": "2026-01-01", "committer_date": "2026-01-01",
    "repo_tag": "v1",
}

ALICE_MAIL = "alice@redhat.com"
BOB_MAIL = "bob@intel.com"
CAROL_MAIL = "carol@vendor.example"
DAVE_MAIL = "dave@acme.test"

# Route 1 and 2: Alice as author_email AND person_email in file A.
ROW_ALICE = dict(BASE, **{
    "author_name": "Alice Smith", "author_email": ALICE_MAIL,
    "committer_name": "Alice Smith", "committer_email": ALICE_MAIL,
    "commit_summary": "add the parser", "personid": "alice smith",
    "person_name": "Alice Smith", "person_email": ALICE_MAIL,
    "person_domain": "redhat.com", "firm_raw": "Red Hat", "firm": "Red Hat",
    "firm_source": "gitdm",
    "footer_signed_off_by": [f"Alice Smith <{ALICE_MAIL}>"],
    "footer_co_authored_by": [f"Bob Jones <{BOB_MAIL}>"],
    "footer_person_names": ["Alice Smith", "Bob Jones"],
    "footer_personids": ["alice smith", "bob jones"],
})

# Carol appears ONLY inside a footer here. The reference implementation built its
# registry from four scalar columns and would have had no entry for her.
ROW_BOB = dict(BASE, **{
    "token_index": 2,
    "author_name": "Bob Jones", "author_email": BOB_MAIL,
    "committer_name": "Bob Jones", "committer_email": BOB_MAIL,
    "commit_summary": "tidy the build", "personid": "bob jones",
    "person_name": "Bob Jones", "person_email": BOB_MAIL,
    "person_domain": "intel.com", "firm_raw": "Intel", "firm": "Intel",
    "firm_source": "gitdm",
    "footer_reviewed_by": [f"Carol Danvers <{CAROL_MAIL}>"],
    # A shape the parser cannot read as a trailer. Must be counted, and must not
    # survive into the output as it stands.
    "footer_thanks_to": ["Some Cryptic Trailer"],
})

# Route 3: Alice reached through a Co-authored-by footer only, in a SECOND file.
ROW_DAVE = dict(BASE, **{
    "repo_name": "acme__gadget", "repo": "gadget", "token_index": 3,
    "author_name": "Dave Lee", "author_email": DAVE_MAIL,
    "committer_name": "Dave Lee", "committer_email": DAVE_MAIL,
    "commit_summary": "fix a leak", "personid": "dave lee",
    "person_name": "Dave Lee", "person_email": DAVE_MAIL,
    "person_domain": "acme.test", "firm_raw": "", "firm": "", "firm_source": "",
    "footer_co_authored_by": [f"Alice Smith <{ALICE_MAIL}>"],
})


@pytest.fixture
def corpus(tmp_path: Path) -> tuple[Path, list[str]]:
    """Two input Parquets plus an empty output directory."""
    src = tmp_path / "in"
    src.mkdir()
    a = write_dataset(src / "acme__widget-dataset.parquet",
                      [ROW_ALICE, ROW_BOB])
    b = write_dataset(src / "acme__gadget-dataset.parquet", [ROW_DAVE])
    return tmp_path, [str(a), str(b)]


def anonymize(tmp_path: Path, paths: list[str], outname: str = "out",
              **kw) -> tuple[dict, Path]:
    """Run the release path, returning (report, outdir)."""
    outdir = tmp_path / outname
    report = A.run(str(outdir), paths, out=io.StringIO(), **kw)
    return report, outdir


def describe(path: Path) -> list[tuple[str, str]]:
    return [(r[0], r[1]) for r in
            _duckdb.sql(f"describe select * from read_parquet('{path}')").fetchall()]


def scalars(path: Path, col: str) -> list[str]:
    return [v for (v,) in _duckdb.sql(
        f"select {col} from read_parquet('{path}') where {col} is not null "
        f"and {col} <> ''").fetchall()]


def elements(path: Path, col: str) -> list[str]:
    return [v for (v,) in _duckdb.sql(
        f"select unnest({col}) from read_parquet('{path}') "
        f"where {col} is not null").fetchall() if v]


def domain_multiset(path: Path) -> dict[str, int]:
    """Every domain of every address in the identity columns, with counts.

    Requirement 2 is about the domains the firm join reads, so this counts them
    wherever an address lives: the three scalar e-mail columns and inside every
    trailer-text element.
    """
    counts: dict[str, int] = {}
    for col in ("author_email", "committer_email", "person_email"):
        for v in scalars(path, col):
            if "@" in v:
                d = v.split("@", 1)[1].lower()
                counts[d] = counts.get(d, 0) + 1
    for col in A.FOOTER_TEXT_COLUMNS:
        for v in elements(path, col):
            for m in re.findall(A.EMAILISH, v):
                d = m.split("@", 1)[1].lower()
                counts[d] = counts.get(d, 0) + 1
    return counts


# --------------------------------------------------------------------------
# Requirement 1 -- one person, one pseudonym, everywhere
# --------------------------------------------------------------------------

@requires_duckdb
def test_the_run_of_the_fixture_corpus_succeeds(corpus):
    """Control for every other test here: the clean fixture must pass."""
    tmp, paths = corpus
    report, _ = anonymize(tmp, paths)
    assert report["failures"] == []


@requires_duckdb
def test_one_person_one_pseudonym_through_all_three_routes(corpus):
    """author_email, person_email and a Co-authored-by footer agree.

    The three routes reach Alice by different code paths: two scalar macros over
    file A and a list macro over file B. A registry that were rebuilt per file,
    or per column, would give her two or three different local parts.
    """
    tmp, paths = corpus
    _, outdir = anonymize(tmp, paths)
    a = outdir / "acme__widget-dataset.parquet"
    b = outdir / "acme__gadget-dataset.parquet"

    via_author = set(scalars(a, "author_email"))
    via_person = set(scalars(a, "person_email"))
    # Alice's pseudonymous address, as it appears inside file B's footer.
    footer = elements(b, "footer_co_authored_by")
    assert len(footer) == 1
    via_footer = re.search(A.EMAILISH, footer[0]).group(0)

    # Alice is the only redhat.com address in the fixture, so the domain picks
    # her pseudonym out of each route without knowing the number in advance.
    alice = {v for v in via_author if v.endswith("@redhat.com")}
    assert len(alice) == 1
    pseudo = alice.pop()
    assert pseudo in via_person
    assert via_footer == pseudo
    assert re.fullmatch(r"author_\d+@redhat\.com", pseudo)
    # ... and the real local part is gone from all three.
    assert "alice" not in " ".join(via_author | via_person | {via_footer})


@requires_duckdb
def test_pseudonyms_are_consistent_across_files_of_one_invocation(corpus):
    """The registry spans the invocation, so no conflict may be reported."""
    tmp, paths = corpus
    report, _ = anonymize(tmp, paths)
    assert report["cross_file_consistency"]["conflicts"] == []
    assert report["cross_file_consistency"]["checked"] > 0


@requires_duckdb
def test_a_footer_only_person_is_pseudonymized_not_passed_through(corpus):
    """Carol never authored a commit; she exists only in footer_reviewed_by."""
    tmp, paths = corpus
    _, outdir = anonymize(tmp, paths)
    got = elements(outdir / "acme__widget-dataset.parquet",
                   "footer_reviewed_by")
    assert len(got) == 1
    assert "Carol Danvers" not in got[0]
    assert "carol@" not in got[0]
    assert got[0].endswith("@vendor.example>")
    assert A.MISSING_MARKER not in got[0]


# --------------------------------------------------------------------------
# Requirement 2 -- domains survive
# --------------------------------------------------------------------------

@requires_duckdb
def test_domain_multiset_is_unchanged(corpus):
    """Firm attribution joins on the domain, so it must be bit-for-bit intact.

    A multiset, not a set: if anonymization changed how many rows carry
    redhat.com the firm-level row counts would move even though the domain list
    looked right.
    """
    tmp, paths = corpus
    _, outdir = anonymize(tmp, paths)
    for p in paths:
        before = domain_multiset(Path(p))
        after = domain_multiset(outdir / Path(p).name)
        assert before == after, Path(p).name
    # Positive control: the fixture really does carry the domains under test.
    a = domain_multiset(outdir / "acme__widget-dataset.parquet")
    assert a["redhat.com"] > 0 and a["intel.com"] > 0


@requires_duckdb
def test_person_domain_column_is_untouched(corpus):
    tmp, paths = corpus
    _, outdir = anonymize(tmp, paths)
    a = outdir / "acme__widget-dataset.parquet"
    assert sorted(scalars(a, "person_domain")) == ["intel.com", "redhat.com"]


# --------------------------------------------------------------------------
# Requirement 3 -- rows, columns and the schema contract
# --------------------------------------------------------------------------

@requires_duckdb
def test_rows_and_columns_are_unchanged_and_the_schema_still_validates(corpus):
    tmp, paths = corpus
    report, outdir = anonymize(tmp, paths)
    for p in paths:
        entry = report["files"][p]
        assert entry["rows_in"] == entry["rows_out"]
        assert entry["columns_in"] == entry["columns_out"] == 70
        # The authority is validate_schema, not the count alone: names, types
        # and order all have to survive.
        drifts = vs.compare_schema(describe(outdir / Path(p).name))
        assert drifts == [], [str(d) for d in drifts]


@requires_duckdb
def test_no_footer_element_is_lost(corpus):
    """Element counts are data. Pseudonymizing must not drop a trailer line."""
    tmp, paths = corpus
    report, _ = anonymize(tmp, paths)
    for p in paths:
        assert report["files"][p]["footer_elements_lost"] == {}
    a = report["files"][paths[0]]["footer_elements"]
    assert a["footer_signed_off_by"] == 1
    assert a["footer_person_names"] == 2


# --------------------------------------------------------------------------
# Requirement 4 -- footer text keeps its structure
# --------------------------------------------------------------------------

TRAILER = re.compile(r"^Author \d+ <author_\d+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}>$")


@requires_duckdb
def test_a_trailer_comes_out_a_well_formed_trailer(corpus):
    """'Signed-off-by: Alice <alice@redhat.com>' -> pseudonym, real domain.

    Not mangled text and not NULL: the reference implementation NULLed the
    column that carried this text rather than parse it.
    """
    tmp, paths = corpus
    _, outdir = anonymize(tmp, paths)
    got = elements(outdir / "acme__widget-dataset.parquet",
                   "footer_signed_off_by")
    assert len(got) == 1
    assert TRAILER.match(got[0]), got[0]
    assert got[0].endswith("@redhat.com>")
    assert "Alice" not in got[0] and "alice@" not in got[0]


@requires_duckdb
def test_every_name_email_trailer_element_is_well_formed(corpus):
    tmp, paths = corpus
    _, outdir = anonymize(tmp, paths)
    seen = 0
    for p in paths:
        for col in ("footer_signed_off_by", "footer_co_authored_by",
                    "footer_reviewed_by"):
            for v in elements(outdir / Path(p).name, col):
                assert TRAILER.match(v), (col, v)
                seen += 1
    # 3 in file A (Alice signed-off, Bob co-authored, Carol reviewed) and
    # 1 in file B (Alice co-authored).
    assert seen == 4


@requires_duckdb
def test_footer_columns_are_not_nulled(corpus):
    """The partner's decision: pseudonymise, do not drop and do not NULL."""
    tmp, paths = corpus
    _, outdir = anonymize(tmp, paths)
    a = outdir / "acme__widget-dataset.parquet"
    assert elements(a, "footer_signed_off_by")
    assert elements(a, "footer_person_names")
    assert elements(a, "footer_personids")


@requires_duckdb
def test_an_unparseable_footer_shape_is_counted_and_replaced(corpus):
    """Requirement 6 applied to footers: counted, and never passed through.

    'Some Cryptic Trailer' is not 'Name <email>'. It is reported under its shape
    and mapped whole through the name registry, so the raw string cannot reach
    the release.
    """
    tmp, paths = corpus
    report, outdir = anonymize(tmp, paths)
    assert report["registry"]["footer_shapes"].get(A.SHAPE_OTHER) == 1
    got = elements(outdir / "acme__widget-dataset.parquet", "footer_thanks_to")
    assert got == [A.pseudo_name(int(re.search(r"\d+", got[0]).group(0)))]
    assert "Cryptic" not in got[0]


# --------------------------------------------------------------------------
# Requirement 5 -- idempotence
# --------------------------------------------------------------------------

@requires_duckdb
def test_two_runs_are_byte_identical(corpus):
    """Ids come from sorting the distinct values, so nothing may drift.

    Byte equality, not value equality: a release is archived and checksummed, so
    a reproducer has to get the same file, not merely the same table.
    """
    tmp, paths = corpus
    _, first = anonymize(tmp, paths, outname="out1")
    _, second = anonymize(tmp, paths, outname="out2")
    for p in paths:
        name = Path(p).name
        assert (first / name).read_bytes() == (second / name).read_bytes(), name


@requires_duckdb
def test_input_file_order_does_not_change_the_output(corpus):
    """The registry sorts, so presenting the same files reversed changes nothing."""
    tmp, paths = corpus
    _, first = anonymize(tmp, paths, outname="fwd")
    _, second = anonymize(tmp, list(reversed(paths)), outname="rev")
    for p in paths:
        name = Path(p).name
        assert (first / name).read_bytes() == (second / name).read_bytes(), name


# --------------------------------------------------------------------------
# Requirement 6 -- failure is closed, not silent
# --------------------------------------------------------------------------

@requires_duckdb
def test_an_unknown_column_stops_the_run_and_writes_nothing(corpus):
    """A widened schema must break the tool loudly, not shrink the release."""
    tmp, _ = corpus
    src = tmp / "wide"
    src.mkdir()
    wide = write_dataset(src / "acme__widget-dataset.parquet", [ROW_ALICE],
                         columns=CONTRACT + (("review_url", "VARCHAR"),))
    outdir = tmp / "outwide"
    with pytest.raises(A.UnknownColumnError) as e:
        A.run(str(outdir), [str(wide)], out=io.StringIO())
    assert "review_url" in str(e.value)
    assert not list(outdir.glob("*.parquet"))


@requires_duckdb
def test_a_too_narrow_input_is_refused(corpus):
    """Measured on the corpus: jq, libuv, tmux and zstd are 23-column files.

    Anonymizing one would publish a file that does not match the contract, and
    its invariance could not be checked, so it is refused rather than written.
    """
    tmp, _ = corpus
    src = tmp / "narrow"
    src.mkdir()
    narrow_cols = tuple((n, t) for n, t in CONTRACT if n != "firm")
    row = {k: v for k, v in ROW_ALICE.items() if k != "firm"}
    narrow = write_dataset(src / "acme__widget-dataset.parquet", [row],
                           columns=narrow_cols)
    outdir = tmp / "outnarrow"
    with pytest.raises(A.MissingColumnError) as e:
        A.run(str(outdir), [str(narrow)], out=io.StringIO())
    assert "firm" in str(e.value)
    assert not list(outdir.glob("*.parquet"))


@requires_duckdb
def test_identity_text_in_commit_summary_fails_the_run(corpus):
    """Positive control on the residue gate.

    A scanner that never fires proves nothing, so this puts a real contributor
    name into a subject line and requires the run to report it. The output file
    is still written -- the exit status and the report are the gate -- so the
    assertion is on the reported failure, which is what main() turns into a
    non-zero exit.
    """
    tmp, _ = corpus
    src = tmp / "leak"
    src.mkdir()
    row = dict(ROW_ALICE, commit_summary="thanks to Carol Danvers for the fix",
               footer_reviewed_by=[f"Carol Danvers <{CAROL_MAIL}>"])
    leaky = write_dataset(src / "acme__widget-dataset.parquet", [row])
    report = A.run(str(tmp / "outleak"), [str(leaky)], out=io.StringIO())
    assert report["failures"], "a real name in commit_summary must be reported"
    assert any("commit_summary" in f for f in report["failures"])
    entry = report["files"][str(leaky)]
    assert entry["commit_summary"]["n_residue"] == 1


@requires_duckdb
def test_null_commit_summary_is_the_documented_escape(corpus):
    """The reference implementation's behaviour, still available on request."""
    tmp, _ = corpus
    src = tmp / "leak2"
    src.mkdir()
    row = dict(ROW_ALICE, commit_summary="thanks to Carol Danvers for the fix",
               footer_reviewed_by=[f"Carol Danvers <{CAROL_MAIL}>"])
    leaky = write_dataset(src / "acme__widget-dataset.parquet", [row])
    report, outdir = anonymize(tmp, [str(leaky)], outname="outnull",
                               null_commit_summary=True)
    assert report["failures"] == []
    assert scalars(outdir / "acme__widget-dataset.parquet",
                   "commit_summary") == []


@requires_duckdb
def test_the_missing_marker_never_reaches_the_output(corpus):
    """A registry miss must show as a marker, and a marker must fail the run."""
    tmp, paths = corpus
    report, outdir = anonymize(tmp, paths)
    for p in paths:
        assert report["files"][p]["leaks"]["missing_marker"] == {}
    for p in paths:
        path = outdir / Path(p).name
        for col, typ in describe(path):
            vals = (elements(path, col) if typ == "VARCHAR[]"
                    else scalars(path, col) if typ == "VARCHAR" else [])
            assert not [v for v in vals if A.MISSING_MARKER in v], col


@requires_duckdb
def test_no_identity_column_leaks_a_real_string(corpus):
    tmp, paths = corpus
    report, _ = anonymize(tmp, paths)
    for p in paths:
        assert report["files"][p]["leaks"]["identity"] == {}
        assert report["files"][p]["passthrough_audit"]["email_shape"] == {}
        assert report["files"][p]["passthrough_audit"]["identity_name"] == {}


@requires_duckdb
def test_no_real_address_survives_anywhere_in_the_release(corpus):
    """The independent check: every address in the output is a pseudonym.

    Deliberately not registry-aware, so it cannot be fooled by a registry bug.
    Mirrors verify_anon.py's rule over every string column.
    """
    tmp, paths = corpus
    _, outdir = anonymize(tmp, paths)
    real = re.compile(r"(?<![A-Za-z0-9._%+\-])(?!author_\d+@)"
                      r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
    for p in paths:
        path = outdir / Path(p).name
        for col, typ in describe(path):
            if col in A.CONTENT_COLUMNS:
                continue
            vals = (elements(path, col) if typ == "VARCHAR[]"
                    else scalars(path, col) if typ == "VARCHAR" else [])
            for v in vals:
                assert not real.search(v), (col, v)
