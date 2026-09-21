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

import re

import pytest

import anonymize_parquet as A
import validate_schema

CONTRACT = [c for c, _ in validate_schema.EXPECTED_COLUMNS]

# A salt for the tests only. 40 bytes, over the 32-byte floor. It is a literal
# here on purpose: every test that produces a pseudonym has to say which key made
# it, or the test is not reproducible.
TEST_SALT = b"test-salt-not-the-release-salt-0123456789"


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
        "Victor Adossi <vadossi@cosmonic.com>")
    assert shape == A.SHAPE_NAME_EMAIL
    assert name == "Victor Adossi"
    assert email == "vadossi@cosmonic.com"


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

def build(emails=(), names=(), footers=(), derived=None, domains=(),
          salt=TEST_SALT):
    return A.build_registry(set(emails), set(names), set(footers),
                            derived or {}, extra_domains=set(domains),
                            salt=salt)


def test_one_person_gets_one_pseudonym_in_footer_and_in_person_name():
    """Claim 5: consistency across the scalar and the footer columns."""
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


def test_pseudonym_formats_are_distinct_per_space():
    tok = "0123456789abcd"
    assert A.pseudo_email_local(tok) == "author_0123456789abcd"
    assert A.pseudo_name(tok) == "Author 0123456789abcd"
    assert A.pseudo_personid(tok) == "author 0123456789abcd"


# --------------------------------------------------------------------------
# The salted stable hash. THE headline property: a pseudonym depends on the
# value and the salt, and on nothing else -- not on what else was in the run.
# --------------------------------------------------------------------------

def test_adding_values_to_the_set_renumbers_nobody():
    """The 98.0% renumbering of the superseded sequential scheme, gone.

    Measured on six corpus Parquets before this change: dropping one file from a
    six-file invocation moved 150 of the 153 addresses present in both runs --
    98.0%. The cause was that ids came from the ordinal position of a value in
    the sorted distinct set, so inserting one value shifted everything after it.
    Here the rate must be exactly zero, for any addition.
    """
    small_e = [f"m{i:03d}@mcorp.example" for i in range(40)]
    small_n = [f"M{i:03d} Person" for i in range(40)]
    # The worst case for the sequential scheme: every added value sorts BEFORE
    # every existing one, so every existing ordinal moves.
    added_e = [f"a{i:03d}@acorp.example" for i in range(40)]
    added_n = [f"A{i:03d} Person" for i in range(40)]

    small = build(emails=small_e, names=small_n)
    large = build(emails=small_e + added_e, names=small_n + added_n)

    for what, a, b in (("addresses", small.emails, large.emails),
                       ("names", small.names, large.names)):
        both = sorted(set(a) & set(b))
        assert len(both) == 40, what
        moved = [v for v in both if a[v] != b[v]]
        rate = 100.0 * len(moved) / len(both)
        assert moved == [], (
            f"{what}: {len(moved)} of {len(both)} renumbered ({rate:.1f}%)")


def test_dropping_values_from_the_set_renumbers_nobody():
    """The other direction: a release that drops a project, not adds one."""
    keep = [f"k{i:03d}@keep.example" for i in range(20)]
    goes = [f"a{i:03d}@gone.example" for i in range(20)]
    with_all = build(emails=keep + goes)
    without = build(emails=keep)
    assert [with_all.emails[v] for v in keep] == [without.emails[v] for v in keep]


def test_a_pseudonym_is_a_function_of_the_value_and_the_salt_only():
    one = build(emails=["alice@intel.com"])
    two = build(emails=["alice@intel.com", "bob@intel.com", "zoe@intel.com"])
    assert one.emails["alice@intel.com"] == two.emails["alice@intel.com"]


def test_a_different_salt_gives_a_different_pseudonym():
    """Otherwise the salt would be decoration and the map would be public."""
    a = build(emails=["alice@intel.com"], salt=TEST_SALT)
    b = build(emails=["alice@intel.com"], salt=b"a" * 32)
    assert a.emails["alice@intel.com"] != b.emails["alice@intel.com"]


def test_the_token_is_a_fixed_width_lowercase_hex_string():
    reg = build(emails=["alice@intel.com"], names=["Alice Smith"])
    for tok in list(reg.emails.values()) + list(reg.names.values()):
        assert re.fullmatch(r"[0-9a-f]{%d}" % A.PSEUDO_HEX_LEN, tok), tok


def test_the_two_id_spaces_are_domain_separated():
    """The same string in the name space and the e-mail space must not collide.

    They are separate id spaces by design (see Registry.__doc__), so the hash
    input carries the space name. Without that, a personid that happens to equal
    an address would render the same token in both, which silently links them.
    """
    key = A.derive_key(TEST_SALT)
    same = "alice@intel.com"
    assert A.pseudo_token(key, "email", same) != A.pseudo_token(key, "name", same)


def test_a_token_collision_fails_the_run(monkeypatch):
    """Truncation can in principle map two people to one pseudonym.

    Two distinct addresses sharing a pseudonym would merge two people, drop the
    distinct-person count and corrupt the headline metric. It must stop the run,
    not be papered over, so this forces the collision.
    """
    monkeypatch.setattr(A, "pseudo_token",
                        lambda key, space, value: "f" * A.PSEUDO_HEX_LEN)
    with pytest.raises(A.CollisionError) as e:
        build(emails=["alice@intel.com", "bob@intel.com"])
    assert "alice@intel.com" in str(e.value) or "bob@intel.com" in str(e.value)


def test_a_collision_inside_one_space_only_is_still_fatal(monkeypatch):
    """A name/e-mail pair sharing a token is fine; two names sharing one is not."""
    monkeypatch.setattr(
        A, "pseudo_token",
        lambda key, space, value: ("a" if space == "email" else "b")
        * A.PSEUDO_HEX_LEN)
    build(emails=["alice@intel.com"], names=["Alice Smith"])      # no collision
    with pytest.raises(A.CollisionError):
        build(names=["Alice Smith", "Bob Jones"])


# --------------------------------------------------------------------------
# Salt custody. The tool must fail loudly rather than invent a salt: a fresh
# salt per run is the worst possible outcome -- it renumbers EVERY pseudonym on
# EVERY run, silently.
# --------------------------------------------------------------------------

def test_a_missing_salt_fails_loudly(tmp_path, monkeypatch):
    monkeypatch.delenv(A.SALT_ENV, raising=False)
    monkeypatch.setenv(A.SALT_FILE_ENV, str(tmp_path / "absent"))
    with pytest.raises(A.SaltError) as e:
        A.load_salt()
    assert "absent" in str(e.value)


def test_a_named_but_missing_salt_file_does_not_fall_back(tmp_path, monkeypatch):
    """An explicitly named source that is absent must NOT reach the next one.

    Falling through would hash with a key the operator did not ask for. Every
    pseudonym in the release would change and the run would still exit 0 -- the
    silent instability this whole scheme exists to remove. The default location
    exists on a developer's machine, so the fall-through would be the normal case,
    not an edge one.
    """
    default = tmp_path / "default-salt"
    default.write_bytes(b"d" * 40)
    default.chmod(0o600)
    monkeypatch.setattr(A, "DEFAULT_SALT_FILE", str(default))
    monkeypatch.setenv(A.SALT_ENV, "e" * 40)
    monkeypatch.setenv(A.SALT_FILE_ENV, str(tmp_path / "absent"))
    with pytest.raises(A.SaltError) as e:
        A.load_salt()
    assert "does not exist" in str(e.value)
    # And the same for --salt-file, which outranks everything.
    with pytest.raises(A.SaltError):
        A.load_salt(str(tmp_path / "also-absent"))


def test_a_missing_salt_creates_nothing(tmp_path, monkeypatch):
    """Never generate. A generated salt is instability with no warning."""
    monkeypatch.delenv(A.SALT_ENV, raising=False)
    monkeypatch.delenv(A.SALT_FILE_ENV, raising=False)
    monkeypatch.setattr(A, "DEFAULT_SALT_FILE", str(tmp_path / "nope"))
    target = tmp_path / "absent"
    monkeypatch.setenv(A.SALT_FILE_ENV, str(target))
    with pytest.raises(A.SaltError):
        A.load_salt()
    assert not target.exists()
    assert list(tmp_path.iterdir()) == []


def test_no_salt_source_at_all_fails_and_names_the_remedy(tmp_path, monkeypatch):
    monkeypatch.delenv(A.SALT_ENV, raising=False)
    monkeypatch.delenv(A.SALT_FILE_ENV, raising=False)
    monkeypatch.setattr(A, "DEFAULT_SALT_FILE", str(tmp_path / "nope"))
    with pytest.raises(A.SaltError) as e:
        A.load_salt()
    msg = str(e.value)
    assert A.SALT_ENV in msg and A.SALT_FILE_ENV in msg
    assert "urandom" in msg                      # how to make one, spelled out


def test_a_short_salt_is_refused(tmp_path, monkeypatch):
    monkeypatch.setenv(A.SALT_ENV, "too-short")
    monkeypatch.delenv(A.SALT_FILE_ENV, raising=False)
    with pytest.raises(A.SaltError) as e:
        A.load_salt()
    assert str(A.MIN_SALT_BYTES) in str(e.value)


def test_an_empty_salt_file_is_refused(tmp_path, monkeypatch):
    p = tmp_path / "salt"
    p.write_text("")
    p.chmod(0o600)
    monkeypatch.delenv(A.SALT_ENV, raising=False)
    monkeypatch.setenv(A.SALT_FILE_ENV, str(p))
    with pytest.raises(A.SaltError):
        A.load_salt()


def test_a_salt_file_readable_by_others_is_refused(tmp_path, monkeypatch):
    p = tmp_path / "salt"
    p.write_bytes(b"x" * 40)
    p.chmod(0o644)
    monkeypatch.delenv(A.SALT_ENV, raising=False)
    monkeypatch.setenv(A.SALT_FILE_ENV, str(p))
    with pytest.raises(A.SaltError) as e:
        A.load_salt()
    assert "600" in str(e.value)


def test_a_salt_file_is_read_and_its_trailing_newline_ignored(tmp_path,
                                                              monkeypatch):
    """`printf ... > salt` and `echo ... > salt` must give the same key."""
    bare, nl = tmp_path / "a", tmp_path / "b"
    bare.write_bytes(b"x" * 40)
    nl.write_bytes(b"x" * 40 + b"\n")
    for p in (bare, nl):
        p.chmod(0o600)
    monkeypatch.delenv(A.SALT_ENV, raising=False)
    monkeypatch.setenv(A.SALT_FILE_ENV, str(bare))
    first = A.load_salt()
    monkeypatch.setenv(A.SALT_FILE_ENV, str(nl))
    assert A.load_salt() == first == b"x" * 40


def test_an_explicit_path_beats_the_environment(tmp_path, monkeypatch):
    chosen, ignored = tmp_path / "chosen", tmp_path / "ignored"
    chosen.write_bytes(b"c" * 40)
    ignored.write_bytes(b"i" * 40)
    for p in (chosen, ignored):
        p.chmod(0o600)
    monkeypatch.setenv(A.SALT_ENV, "e" * 40)
    monkeypatch.setenv(A.SALT_FILE_ENV, str(ignored))
    assert A.load_salt(str(chosen)) == b"c" * 40


def test_the_salt_file_beats_the_inline_environment_variable(tmp_path,
                                                             monkeypatch):
    p = tmp_path / "salt"
    p.write_bytes(b"f" * 40)
    p.chmod(0o600)
    monkeypatch.setenv(A.SALT_ENV, "e" * 40)
    monkeypatch.setenv(A.SALT_FILE_ENV, str(p))
    assert A.load_salt() == b"f" * 40


def test_the_inline_environment_variable_works(monkeypatch):
    monkeypatch.setenv(A.SALT_ENV, "e" * 40)
    monkeypatch.delenv(A.SALT_FILE_ENV, raising=False)
    assert A.load_salt() == b"e" * 40


def test_the_cli_exits_two_and_writes_nothing_when_the_salt_is_absent(
        tmp_path, monkeypatch, capsys):
    """Exit 2 is "misuse", distinct from the 1 that means "found residue".

    A release script has to be able to tell a missing secret from a dirty
    dataset, and it must not see a traceback where it expected a status.
    """
    monkeypatch.delenv(A.SALT_ENV, raising=False)
    monkeypatch.delenv(A.SALT_FILE_ENV, raising=False)
    monkeypatch.setattr(A, "DEFAULT_SALT_FILE", str(tmp_path / "nope"))
    outdir = tmp_path / "out"
    rc = A.main([str(outdir), str(tmp_path / "in.parquet")])
    assert rc == 2
    err = capsys.readouterr().err
    assert "no anonymization salt" in err
    assert "urandom" in err                      # the remedy, in the error
    assert not outdir.exists()                   # nothing created, nothing read


def test_a_long_salt_is_folded_rather_than_refused():
    """blake2b keys stop at 64 bytes; a 100-byte salt must still work."""
    long_salt = bytes(range(100)) * 2
    assert len(A.derive_key(long_salt)) == 64
    reg = build(emails=["alice@intel.com"], salt=long_salt)
    assert len(reg.emails["alice@intel.com"]) == A.PSEUDO_HEX_LEN


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
    reg = build(emails=["m@maxroos.com"], names=["Maximilian Roos"],
                footers=["Maximilian Roos <m@maxroos.com>",
                         "Maximilian Roos <m@maxroos.com>\r"])
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
