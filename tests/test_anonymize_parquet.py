"""Tests for anonymize_parquet.py.

Pure logic only: classification, footer parsing, the registry, rendering, SQL
generation and the leak scanner. duckdb lives in `devenv shell` and not in
.venv, so nothing here touches a parquet. The end-to-end verification (row
counts, column counts, analysis invariance, leak scan, cross-file pseudonym
consistency) runs inside anonymize_parquet.run() and gates its exit status;
these tests cover the parts that can go wrong silently.

The headline test is test_unknown_column_raises: a schema that grows must break
this tool loudly, which is the property the whole module exists to provide.
"""
from __future__ import annotations

import pytest

import anonymize_parquet as A
import validate_schema

CONTRACT = [c for c, _ in validate_schema.EXPECTED_COLUMNS]


# --------------------------------------------------------------------------
# Column classification -- fail closed
# --------------------------------------------------------------------------

def test_contract_is_seventy_columns():
    """Guards the arithmetic every other count in this file depends on."""
    assert len(CONTRACT) == 70


def test_every_contract_column_is_classified():
    plan = A.classify(CONTRACT)
    assert plan.n_in == 70
    assert plan.n_out == 70
    assert plan.dropped == ()
    # Each column accounted for by exactly one stated rule.
    rules = {c: plan.rule_of(c) for c in CONTRACT}
    assert len(rules) == 70
    assert sum(1 for r in rules.values() if r == "pass") == 29 + 3 + 15


def test_classification_partitions_the_schema():
    """No column may appear under two rules, and none may be missed."""
    plan = A.classify(CONTRACT)
    buckets = (list(plan.passthrough) + [c for c, _ in plan.scalar]
               + list(plan.footer_text) + [c for c, _ in plan.footer_derived]
               + list(plan.dropped))
    assert len(buckets) == len(set(buckets)) == 70
    assert set(buckets) == set(CONTRACT)


def test_unknown_column_raises_and_names_the_column():
    """The defect this module exists to prevent.

    The original enumerated its output columns by hand, so a widened schema was
    silently truncated. Here an unclassified column is a hard error.
    """
    with pytest.raises(A.UnknownColumnError) as e:
        A.classify(CONTRACT + ["review_url"])
    assert "review_url" in str(e.value)


def test_unknown_column_raises_even_when_it_looks_like_a_footer():
    with pytest.raises(A.UnknownColumnError) as e:
        A.classify(CONTRACT + ["footer_closes"])
    assert "footer_closes" in str(e.value)


def test_several_unknown_columns_are_all_named():
    with pytest.raises(A.UnknownColumnError) as e:
        A.classify(CONTRACT + ["zeta_col", "alpha_col"])
    msg = str(e.value)
    assert "alpha_col" in msg and "zeta_col" in msg


def test_missing_required_column_raises():
    """Fail closed both ways: an input too narrow to verify is refused."""
    narrow = [c for c in CONTRACT if c != "firm"]
    with pytest.raises(A.MissingColumnError) as e:
        A.classify(narrow)
    assert "firm" in str(e.value)


def test_drop_footers_yields_fifty_five_columns():
    plan = A.classify(CONTRACT, drop_footers=True)
    assert plan.n_in == 70
    assert plan.n_out == 55
    assert len(plan.dropped) == 15
    assert set(plan.dropped) == set(A.FOOTER_COLUMNS)


def test_firm_and_provenance_columns_pass_through():
    plan = A.classify(CONTRACT)
    for c in A.FIRM_COLUMNS + A.PROVENANCE_COLUMNS + ("person_domain",):
        assert plan.rule_of(c) == "pass", c


def test_identity_columns_are_transformed():
    plan = A.classify(CONTRACT)
    assert plan.rule_of("author_email") == "transform:email"
    assert plan.rule_of("person_name") == "transform:name"
    assert plan.rule_of("personid") == "transform:personid"
    assert plan.rule_of("footer_signed_off_by") == "transform:footer_text"
    assert plan.rule_of("footer_personids") == "transform:footer_personid"
    assert plan.rule_of("footer_person_names") == "transform:footer_name"


# --------------------------------------------------------------------------
# The SELECT list cannot end early
# --------------------------------------------------------------------------

def test_select_list_emits_one_item_per_output_column():
    """Regression guard on the original's actual failure.

    Run unchanged on this 70-column schema the original emitted 23 columns and
    exited 0. Here the SELECT list is generated from the plan, so its length is
    the output column count by construction.
    """
    plan = A.classify(CONTRACT)
    items = A.select_list(plan, null_commit_summary=False)
    assert len(items) == plan.n_out == 70


def test_select_list_covers_firm_footer_and_provenance():
    plan = A.classify(CONTRACT)
    text = " ".join(A.select_list(plan, null_commit_summary=False))
    for c in ("firm_raw", "firm", "firm_source", "clone_url", "file_mask",
              "footer_signed_off_by", "footer_thanks_to", "footer_personids"):
        assert c in text, c


def test_select_list_preserves_input_column_order():
    plan = A.classify(CONTRACT)
    emitted = [i.split(" AS ")[-1].strip() for i in
               A.select_list(plan, null_commit_summary=False)]
    assert emitted == CONTRACT


def test_select_list_under_drop_footers_omits_footers():
    plan = A.classify(CONTRACT, drop_footers=True)
    items = A.select_list(plan, null_commit_summary=False)
    assert len(items) == 55
    assert not any("footer_" in i for i in items)


def test_null_commit_summary_casts_null():
    plan = A.classify(CONTRACT)
    items = A.select_list(plan, null_commit_summary=True)
    assert "CAST(NULL AS VARCHAR) AS commit_summary" in items
    assert len(items) == 70


def test_commit_summary_is_preserved_by_default():
    plan = A.classify(CONTRACT)
    items = A.select_list(plan, null_commit_summary=False)
    assert "anon_summary(commit_summary) AS commit_summary" in items


# --------------------------------------------------------------------------
# Footer element parsing
# --------------------------------------------------------------------------

def test_parses_the_git_trailer_shape():
    shape, name, email = A.parse_footer_element(
        "Nora Quill <nquill@shipwell.example>")
    assert shape == A.SHAPE_NAME_EMAIL
    assert name == "Nora Quill"
    assert email == "nquill@shipwell.example"


def test_parses_a_bare_address():
    shape, name, email = A.parse_footer_element("bob@example.com")
    assert shape == A.SHAPE_BARE_EMAIL
    assert name is None
    assert email == "bob@example.com"


def test_cregit_personid_convention_is_not_mistaken_for_a_name():
    """'handle at local@domain' carries a real address without angle brackets.

    Measured in footer_personids and in person_name itself. Treated as one
    opaque string and replaced whole, which is why no address survives.
    """
    shape, name, email = A.parse_footer_element(
        "prql-bot at prql-bot@users.noreply.github.com")
    assert shape == A.SHAPE_NAME_AT_EMAIL
    assert name == "prql-bot at prql-bot@users.noreply.github.com"
    assert email is None


def test_unrecognised_shape_is_reported_not_passed_through():
    shape, name, email = A.parse_footer_element("Some Cryptic Trailer")
    assert shape == A.SHAPE_OTHER
    assert name == "Some Cryptic Trailer"
    assert shape in A.NONCONFORMING_SHAPES


def test_trailing_carriage_return_still_parses():
    shape, name, email = A.parse_footer_element("A B <a@b.com>\r")
    assert shape == A.SHAPE_NAME_EMAIL
    assert email == "a@b.com"


# --------------------------------------------------------------------------
# The registry
# --------------------------------------------------------------------------

def build(emails=(), names=(), footers=(), derived=None, domains=()):
    return A.build_registry(set(emails), set(names), set(footers),
                            derived or {}, extra_domains=set(domains))


def test_one_person_gets_one_pseudonym_in_footer_and_in_person_name():
    """One person keeps one pseudonym across the scalar and the footer columns."""
    reg = build(emails=["alice@intel.com"], names=["Alice Smith"],
                footers=["Alice Smith <alice@intel.com>"])
    assert reg.footer_elements["Alice Smith <alice@intel.com>"] == (
        f"{A.pseudo_name(reg.names['alice smith'])} "
        f"<{A.pseudo_email_local(reg.emails['alice@intel.com'])}@intel.com>")


def test_footer_only_person_still_gets_a_pseudonym():
    """The original's registry came from four scalar columns and would miss this.

    A reviewer who never authored a commit appears only in footer_reviewed_by.
    With no registry entry their real name would have had to pass through.
    """
    reg = build(emails=["a@x.com"], names=["A"],
                footers=["Reviewer Person <rev@vendor.com>"])
    rendered = reg.footer_elements["Reviewer Person <rev@vendor.com>"]
    assert "Reviewer Person" not in rendered
    assert "rev@" not in rendered
    assert rendered.endswith("@vendor.com>")
    assert A.MISSING_MARKER not in rendered


def test_derived_footer_values_enter_the_name_registry():
    """footer_personids can hold people absent from personid. Measured in prql."""
    reg = build(emails=[], names=["known person"],
                derived={"footer_personids":
                         {"prql-bot at prql-bot@users.noreply.github.com"}})
    assert "prql-bot at prql-bot@users.noreply.github.com" in reg.names
    assert "known person" in reg.names


def test_email_domain_is_preserved_and_local_part_is_not():
    reg = build(emails=["secret.person@intel.com"])
    n = reg.emails["secret.person@intel.com"]
    rendered = A.render_footer_element(reg, None, "secret.person@intel.com")
    assert rendered == f"{A.pseudo_email_local(n)}@intel.com"
    assert "secret.person" not in rendered
    assert "intel.com" in rendered


def test_registry_is_case_insensitive_so_joins_survive():
    reg = build(emails=["Alice@Intel.com", "alice@intel.com"])
    assert len(reg.emails) == 1


def test_registry_is_deterministic():
    a = build(emails=["b@x.com", "a@x.com"], names=["Zoe", "Amy"])
    b = build(emails=["a@x.com", "b@x.com"], names=["Amy", "Zoe"])
    assert a.emails == b.emails
    assert a.names == b.names


def test_pseudonym_formats_are_zero_padded_and_distinct_per_space():
    assert A.pseudo_email_local(42) == "author_0042"
    assert A.pseudo_name(42) == "Author 0042"
    assert A.pseudo_personid(42) == "author 0042"


def test_preserved_domains_include_person_domain_values():
    reg = build(emails=["a@x.com"], domains=["extra.example"])
    assert "x.com" in reg.domains
    assert "extra.example" in reg.domains


# --------------------------------------------------------------------------
# SQL generation
# --------------------------------------------------------------------------

def test_sql_string_escapes_apostrophes():
    """Names like O'Brien would otherwise break the MAP literal."""
    assert A.sql_str("O'Brien") == "'O''Brien'"


def test_sql_map_escapes_both_sides_and_is_sorted():
    out = A.sql_map({"o'b": "x", "a": "y"})
    assert out.startswith("MAP {")
    assert "'o''b': 'x'" in out
    assert out.index("'a'") < out.index("'o''b'")


def test_empty_sql_map_is_typed():
    """An untyped MAP {} cannot be bound by duckdb."""
    assert A.sql_map({}) == "MAP([]::VARCHAR[], []::VARCHAR[])"


def test_macros_map_to_the_missing_marker_not_to_the_input():
    """A value the registry never saw must become visible, never pass through."""
    reg = build(emails=["a@x.com"], names=["Someone"])
    sql = " ".join(A.registry_macros(reg))
    assert A.sql_str(A.MISSING_MARKER) in sql
    assert sql.count("coalesce") >= 4


def test_macros_cover_every_transform_kind():
    reg = build(emails=["a@x.com"], names=["N"])
    sql = " ".join(A.registry_macros(reg))
    for macro in ("anon_email", "anon_name", "anon_personid",
                  "anon_footer_text", "anon_footer_names",
                  "anon_footer_personids", "anon_summary"):
        assert f"MACRO {macro}(" in sql, macro


# --------------------------------------------------------------------------
# The leak scanner
# --------------------------------------------------------------------------

def test_scanner_catches_a_real_local_part():
    """Positive control: a scanner that never fires proves nothing."""
    reg = build(emails=["secretperson@intel.com"], names=["Secret Person"])
    hit = A.Scanner(reg).find("Signed-off-by: X <secretperson@intel.com>")
    assert hit == ("local_part", "secretperson")


def test_scanner_catches_a_real_name():
    reg = build(emails=["a@x.com"], names=["Secret Person"])
    assert A.Scanner(reg).find("thanks to Secret Person") == (
        "name", "secret person")


def test_scanner_passes_a_correctly_pseudonymized_value():
    reg = build(emails=["secretperson@intel.com"], names=["Secret Person"])
    assert A.Scanner(reg).find("Author 0001 <author_0001@intel.com>") is None


def test_preserved_domain_is_not_counted_as_a_leak():
    """The false positive that made every file report five leaks.

    'github' is a real identity NAME in this data -- the GitHub web-flow
    committer -- and users.noreply.github.com is a domain the tool preserves on
    purpose. A name found inside a published domain is not a leak.
    """
    reg = build(emails=["someone@users.noreply.github.com"], names=["github"])
    scanner = A.Scanner(reg)
    assert scanner.find("author_0001@users.noreply.github.com") is None
    # ... but the same name outside a published domain still fires.
    assert scanner.find("patch by github") == ("name", "github")


def test_short_probes_are_not_used():
    """A three letter name would match half of any C file."""
    reg = build(emails=["ab@x.com"], names=["Bob"])
    assert A.Scanner(reg).find("Bob wrote this") is None


def test_bare_local_part_in_prose_is_not_a_leak_but_an_address_is():
    reg = build(emails=["buildbot@x.com"], names=[])
    scanner = A.Scanner(reg)
    assert scanner.find("the buildbot failed") is None
    assert scanner.find("mail buildbot@elsewhere.org") == (
        "local_part", "buildbot")


def test_domain_masker_handles_an_empty_domain_set():
    """An empty alternation would otherwise match everywhere."""
    assert A.domain_masker(set())("anything") == "anything"


def test_domain_masker_prefers_the_longest_domain():
    mask = A.domain_masker({"co.uk", "example.co.uk"})
    assert mask("a@example.co.uk") == f"a@{A.DOMAIN_MASK}"


# --------------------------------------------------------------------------
# Footer fidelity reporting
# --------------------------------------------------------------------------

def test_footer_collapses_are_reported():
    """Two trailers differing only by a CR render alike. Said out loud, not hidden."""
    reg = build(emails=["p@driftwork.example"], names=["Priya Kestrel"],
                footers=["Priya Kestrel <p@driftwork.example>",
                         "Priya Kestrel <p@driftwork.example>\r"])
    collapses = A.footer_collapses(reg)
    assert len(collapses) == 1
    assert len(next(iter(collapses.values()))) == 2


def test_no_collapse_reported_when_mapping_is_injective():
    reg = build(emails=["a@x.com", "b@y.com"], names=["A A", "B B"],
                footers=["A A <a@x.com>", "B B <b@y.com>"])
    assert A.footer_collapses(reg) == {}


# --------------------------------------------------------------------------
# Agreement with the schema contract
# --------------------------------------------------------------------------

def test_no_column_is_both_passthrough_and_transformed():
    transformed = (set(A.SCALAR_TRANSFORMS) | set(A.FOOTER_TEXT_COLUMNS)
                   | set(A.FOOTER_DERIVED_COLUMNS))
    assert not (A.PASSTHROUGH_COLUMNS & transformed)


def test_the_rule_tables_cover_the_contract_exactly():
    """Drift guard: if validate_schema grows, this fails before a release does."""
    known = (A.PASSTHROUGH_COLUMNS | set(A.SCALAR_TRANSFORMS)
             | set(A.FOOTER_TEXT_COLUMNS) | set(A.FOOTER_DERIVED_COLUMNS))
    assert known == set(CONTRACT)


def test_fifteen_footer_columns():
    assert len(A.FOOTER_COLUMNS) == 15
    assert len(A.PROVENANCE_COLUMNS) == 29
    assert len(A.FIRM_COLUMNS) == 3
