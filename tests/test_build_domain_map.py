"""Unit tests for build_domain_map.

The module turns person-grain grey literature (cncf/gitdm) plus a published
CC-BY dataset (Spinellis et al., MSR 2020) into one row per e-mail domain. Every
test here locks in either a research-correctness rule (who may attribute a
domain to a firm, and with what provenance) or a data-safety rule (no network,
no writes outside tmp_path).

No test touches the real `.corpus-cache/`, the real `data/` or the real curated
map: the `env` fixture repoints every module constant into tmp_path and asserts
it did so. There is no network: `urllib.request.urlopen` is monkeypatched.
"""

from __future__ import annotations

import argparse
import csv
import time
from collections import Counter
from pathlib import Path

import pytest

import build_domain_map as bdm

REAL_CACHE = bdm.CACHE
REAL_DATA = bdm.DATA
REAL_OUT = bdm.OUT
REAL_CURATED = bdm.CURATED
REAL_SPINELLIS = bdm.SPINELLIS_TSV

# The Spinellis TSV is a 29-column headerless file; only these indices are read.
SP_COLUMNS = 29


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """No test may sleep. A slow suite does not get run, and an unrun suite
    protects nothing."""
    monkeypatch.setattr(time, "sleep", lambda *a, **k: None)


class Env:
    """Every path the module writes to or reads from, inside tmp_path."""

    def __init__(self, tmp_path: Path):
        self.root = tmp_path
        self.cache = tmp_path / "cache" / "affil"
        self.data = tmp_path / "data"
        self.out = self.data / "affiliation.merged.csv"
        self.curated = tmp_path / "curated" / "affiliation.csv"
        self.spinellis = tmp_path / "sources" / "enterprise_projects.txt"
        self.cache.mkdir(parents=True)
        self.curated.parent.mkdir(parents=True)
        self.spinellis.parent.mkdir(parents=True)
        self._n = 0

    def gitdm(self, text: str) -> Path:
        """Write one more cached developers_affiliations file."""
        self._n += 1
        dest = self.cache / f"developers_affiliations{self._n}.txt"
        dest.write_text(text if text.endswith("\n") else text + "\n")
        return dest

    def curated_rows(self, rows: list[dict], header: list[str] | None = None) -> None:
        header = header or ["domain", "company", "kind", "source"]
        with self.curated.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=header)
            w.writeheader()
            w.writerows(rows)

    def spinellis_rows(self, rows: list[str]) -> None:
        self.spinellis.write_text("\n".join(rows) + "\n")

    def read_out(self) -> list[dict[str, str]]:
        with self.out.open() as f:
            return list(csv.DictReader(f))

    def out_header(self) -> str:
        return self.out.read_text().splitlines()[0]


@pytest.fixture
def env(tmp_path, monkeypatch):
    e = Env(tmp_path)
    monkeypatch.setattr(bdm, "CACHE", e.cache)
    monkeypatch.setattr(bdm, "DATA", e.data)
    monkeypatch.setattr(bdm, "OUT", e.out)
    monkeypatch.setattr(bdm, "CURATED", e.curated)
    monkeypatch.setattr(bdm, "SPINELLIS_TSV", e.spinellis)
    # Data safety: a mistake here would read or write the live corpus cache.
    for real, fake in ((REAL_CACHE, bdm.CACHE), (REAL_DATA, bdm.DATA),
                       (REAL_OUT, bdm.OUT), (REAL_CURATED, bdm.CURATED),
                       (REAL_SPINELLIS, bdm.SPINELLIS_TSV)):
        assert fake != real
        assert fake.is_relative_to(tmp_path)
    return e


def person(handle: str, emails: list[str], companies: list[str]) -> str:
    """One gitdm person block: a flush-left person line, TAB-indented
    affiliation lines."""
    return "\n".join([f"{handle}: " + ", ".join(emails)]
                     + [f"\t{c}" for c in companies])


def sp_row(domain: str, company: str, *, fg500: str = "",
           k10: str = "", f20: str = "", extra_cols: int = SP_COLUMNS) -> str:
    f = [""] * extra_cols
    if extra_cols > bdm.SP_DOMAIN:
        f[bdm.SP_DOMAIN] = domain
    if extra_cols > bdm.SP_COMPANY:
        f[bdm.SP_COMPANY] = company
    if extra_cols > bdm.SP_20F:
        f[bdm.SP_FG500], f[bdm.SP_10K], f[bdm.SP_20F] = fg500, k10, f20
    return "\t".join(f)


def build(min_persons: int = bdm.MIN_PERSONS, report: bool = False) -> int:
    return bdm.cmd_build(argparse.Namespace(min_persons=min_persons, report=report))


def rows_by_domain(env: Env) -> dict[str, dict[str, str]]:
    return {r["domain"]: r for r in env.read_out()}


# --------------------------------------------------------------------------
# norm_company — collapses two spellings of one company into one
# --------------------------------------------------------------------------

@pytest.mark.parametrize("raw, expected", [
    ("Foo Inc.", "Foo"),
    ("Foo Inc", "Foo"),
    ("Foo Ltd", "Foo"),
    ("Foo Ltd.", "Foo"),
    ("Foo Limited", "Foo"),
    ("Foo GmbH", "Foo"),
    ("Foo LLC", "Foo"),
    ("Foo Corp.", "Foo"),
    ("Foo Corporation", "Foo"),
    ("Axis Communications AB", "Axis Communications"),
    ("Red Hat, Inc.", "Red Hat"),
    ("Foo Inc. Ltd", "Foo"),          # strips repeatedly, not once
    ("Foo GmbH & Co. KG", "Foo GmbH &"),
])
def test_norm_company_strips_legal_form_suffixes(raw, expected):
    """Research correctness: 'Axis Communications AB' and 'Axis Communications'
    are one firm. If suffix stripping stops working, one firm splits into two
    and every per-firm ownership measure is understated."""
    assert bdm.norm_company(raw) == expected


@pytest.mark.parametrize("raw, expected", [
    ("Foo (Bar)", "Foo"),
    ("Foo (formerly Baz)", "Foo"),
    ("Foo   (Bar)  ", "Foo"),
])
def test_norm_company_removes_a_trailing_parenthetical(raw, expected):
    """A trailing note in parentheses is commentary, not part of the name."""
    assert bdm.norm_company(raw) == expected


def test_norm_company_keeps_a_parenthetical_that_is_not_last():
    """DOCUMENTS A GAP. The parenthetical is removed once, BEFORE the suffix
    loop, so a note followed by a legal form survives: 'Foo (Bar) Inc.' stays
    'Foo (Bar)' while 'Foo (Bar)' becomes 'Foo'. The two spellings of one firm
    therefore stay two firms. See the report."""
    assert bdm.norm_company("Foo (Bar) Inc.") == "Foo (Bar)"
    assert bdm.norm_company("Foo (Bar)") == "Foo"


@pytest.mark.parametrize("raw", [
    "independent", "Independent", "(Independent)", "unknown", "none", "n/a",
    "na", "N/A", "self", "freelance", "freelancer", "student", "retired",
    "individual", "private", "personal", "no company", "nocompany", "notfound",
    "?", "???",
])
def test_norm_company_rejects_a_non_firm(raw):
    """Research correctness: 'independent' is not an employer. Letting it
    through would invent a firm that owns code in many projects."""
    assert bdm.norm_company(raw) == ""


@pytest.mark.parametrize("raw", [
    "hosting company", "consulting company", "various", "Various",
    "multiple", "several", "unaffiliated", "not disclosed", "notdisclosed",
    "undisclosed", "Berlin based", "Foo, based in Berlin",
])
def test_norm_company_rejects_a_description(raw):
    """A description of an employment situation is not a firm name."""
    assert bdm.norm_company(raw) == ""


@pytest.mark.parametrize("raw", ["A", "X", "X Inc", " Q ", "Z Ltd."])
def test_norm_company_rejects_a_one_character_result(raw):
    """A single letter cannot identify a firm, before or after suffix
    stripping."""
    assert bdm.norm_company(raw) == ""


@pytest.mark.parametrize("raw, expected", [
    ("  Foo   Bar  ", "Foo Bar"),
    ("Foo,", "Foo"),
    ("Foo;", "Foo"),
    ("Foo , ", "Foo"),
    (";Foo Bar;", "Foo Bar"),
    ("Foo\tBar", "Foo Bar"),
])
def test_norm_company_collapses_whitespace_and_stray_punctuation(raw, expected):
    """'Foo,' and 'Foo' must not become two firms."""
    assert bdm.norm_company(raw) == expected


def test_norm_company_accepts_none_and_empty():
    """The upstream file has ragged lines; a missing value must not raise."""
    assert bdm.norm_company(None) == ""
    assert bdm.norm_company("") == ""
    assert bdm.norm_company("   ") == ""


# --------------------------------------------------------------------------
# domain_root
# --------------------------------------------------------------------------

@pytest.mark.parametrize("domain, expected", [
    ("opensource.cirrus.com", "cirrus"),
    ("cirrus.com", "cirrus"),
    ("OPENSOURCE.CIRRUS.COM", "cirrus"),
    ("a.b.c.d", "c"),
    ("localhost", "localhost"),       # single label, no crash
    ("", ""),                         # empty input, no crash
    (".", ""),
    ("..", ""),
])
def test_domain_root(domain, expected):
    """Single-label and empty input must not raise: the upstream file contains
    both."""
    assert bdm.domain_root(domain) == expected


# --------------------------------------------------------------------------
# parse_source — the load-bearing attribution rule
# --------------------------------------------------------------------------

def test_one_work_domain_and_one_company_contributes(env):
    """The only shape that may attribute a domain to a firm."""
    env.gitdm(person("alice", ["alice!redhat.com"], ["Red Hat, Inc."]))
    assert dict(bdm.parse_source()) == {"redhat.com": Counter({"Red Hat": 1})}


def test_two_work_domains_contribute_nothing(env):
    """Research correctness: with two work domains the person cannot say which
    domain their employer owns. A naive pass credits both, which is the
    dilution that made most domains fail the majority test."""
    env.gitdm(person("bob", ["bob!redhat.com", "bob!suse.com"], ["Red Hat"]))
    assert dict(bdm.parse_source()) == {}


def test_two_companies_contribute_nothing(env):
    """Research correctness: one domain but two employers is ambiguous, so the
    person contributes to neither."""
    env.gitdm(person("carol", ["carol!example.org"], ["Red Hat", "SUSE"]))
    assert dict(bdm.parse_source()) == {}


def test_the_same_company_listed_twice_is_one_company(env):
    """Two dated stints at one employer are one employer, so the person still
    contributes. Counting rows instead of distinct names would silently drop
    every long-tenured contributor."""
    env.gitdm(person("dave", ["dave!suse.com"],
                     ["SUSE until 2019-01-01", "SUSE"]))
    assert dict(bdm.parse_source()) == {"suse.com": Counter({"SUSE": 1})}


def test_a_free_provider_does_not_count_as_a_work_domain(env):
    """R1 at parse time: gmail.com is nobody's employer, so it must not make a
    person look ambiguous. Counting it would discard most contributors."""
    env.gitdm(person("erin", ["erin!gmail.com", "erin!igalia.com"], ["Igalia"]))
    assert dict(bdm.parse_source()) == {"igalia.com": Counter({"Igalia": 1})}


@pytest.mark.parametrize("line, expected", [
    ("Collabora until 2020-05-01", "Collabora"),
    ("Collabora from 2018-01-01", "Collabora"),
    ("Collabora   until   2020-05-01", "Collabora"),
])
def test_validity_ranges_are_stripped_from_the_company_name(env, line, expected):
    """A domain map carries no dates. 'Collabora until 2020-05-01' and
    'Collabora' must be the same firm."""
    env.gitdm(person("fred", ["fred!collabora.com"], [line]))
    assert dict(bdm.parse_source()) == {"collabora.com": Counter({expected: 1})}


def test_comments_and_blank_lines_are_ignored(env):
    """The upstream file is hand-maintained and carries banners and gaps."""
    env.gitdm("# a banner\n"
              "\n"
              "   \n"
              + person("gina", ["gina!igalia.com"], ["Igalia"]) + "\n"
              "\t# an indented comment\n"
              "\n")
    assert dict(bdm.parse_source()) == {"igalia.com": Counter({"Igalia": 1})}


def test_a_missing_cache_returns_empty_and_does_not_raise(env, capsys):
    """`build` before `fetch` must fail loudly but softly, not traceback."""
    env.cache.rmdir()
    assert not env.cache.exists()
    assert bdm.parse_source() == {}
    assert "no cached source" in capsys.readouterr().out


@pytest.mark.parametrize("line", [
    "handle-with-no-colon and no bang",
    "handle: plain.address@example.org",     # no '!' separator
    "handle: alice!nodots",                  # not a domain
    "handle: alice!",                        # empty domain
])
def test_a_person_line_yielding_no_domain_is_dropped(env, line):
    """A malformed person line must contribute nothing, and must not leak its
    affiliation lines onto the previous person."""
    env.gitdm(person("gina", ["gina!igalia.com"], ["Igalia"]) + "\n"
              + line + "\n\tShould Be Ignored Inc.\n")
    assert dict(bdm.parse_source()) == {"igalia.com": Counter({"Igalia": 1})}


def test_a_non_firm_affiliation_line_is_discarded_not_counted(env):
    """Research correctness: '(Independent)' next to a real employer must be
    dropped, not counted as a second company. Counting it would make every
    such person ambiguous and lose their domain."""
    env.gitdm(person("gwen", ["gwen!igalia.com"],
                     ["(Independent)", "Igalia", "various"]))
    assert dict(bdm.parse_source()) == {"igalia.com": Counter({"Igalia": 1})}


def test_a_person_with_only_non_firm_affiliations_contributes_nothing(env):
    """No usable company means no attribution."""
    env.gitdm(person("gus", ["gus!igalia.com"], ["unknown", "self"]))
    assert dict(bdm.parse_source()) == {}


def test_a_trailing_dot_and_case_are_normalised(env):
    """'Igalia.COM.' and 'igalia.com' are one domain."""
    env.gitdm(person("hank", ["hank!Igalia.COM."], ["Igalia"]))
    assert dict(bdm.parse_source()) == {"igalia.com": Counter({"Igalia": 1})}


def test_people_are_counted_across_several_cached_files(env):
    """The source is ten files; a person must not be merged across a file
    boundary."""
    env.gitdm(person("ivan", ["ivan!igalia.com"], ["Igalia"]))
    env.gitdm(person("jane", ["jane!igalia.com"], ["Igalia"]))
    assert dict(bdm.parse_source()) == {"igalia.com": Counter({"Igalia": 2})}


# --------------------------------------------------------------------------
# cmd_build — the four rules
# --------------------------------------------------------------------------

def test_r1_a_free_provider_becomes_independent(env, monkeypatch):
    """R1: a free provider is never a firm. Exercised through an injected
    parse_source because no real gitdm input can reach this branch — see
    test_r1_is_unreachable_from_the_real_source."""
    monkeypatch.setattr(bdm, "parse_source",
                        lambda: {"gmail.com": Counter({"Google": 3})})
    assert build() == 0
    row = rows_by_domain(env)["gmail.com"]
    assert row["company"] == "(Independent)"
    assert row["kind"] == "free_provider"
    assert row["source"] == "cncf-gitdm"


def test_r1_is_unreachable_from_the_real_source(env, capsys):
    """Documents a dead branch, not a wish. parse_source keys the map on
    non-free domains only, so cmd_build's R1 arm can never fire on real input:
    a person whose only domain is free has no work domain and contributes
    nothing. No `kind=free_provider` row can be imported."""
    env.gitdm(person("kate", ["kate!gmail.com"], ["Google"]))
    assert build() == 1                       # nothing parsed at all
    assert not env.out.exists()
    assert "R1-free-provider" not in capsys.readouterr().out


@pytest.mark.parametrize("n_people, min_persons, expected_source", [
    (1, 2, "cncf-gitdm-single"),
    (2, 2, "cncf-gitdm"),
    (3, 2, "cncf-gitdm"),
    (1, 1, "cncf-gitdm"),
    (2, 3, "cncf-gitdm-single"),
])
def test_r2_is_a_confidence_tier_not_a_filter(env, n_people, min_persons,
                                              expected_source):
    """Research correctness: a domain under the threshold is KEPT and flagged,
    never dropped. Excluding the single-person tier costs about 92% of the
    yield, so a consumer must be able to choose the tier, not have it chosen
    for them."""
    env.gitdm("\n".join(person(f"p{i}", [f"p{i}!igalia.com"], ["Igalia"])
                        for i in range(n_people)))
    assert build(min_persons=min_persons) == 0
    row = rows_by_domain(env)["igalia.com"]
    assert row["company"] == "Igalia"
    assert row["kind"] == "company"
    assert row["source"] == expected_source


def test_r3_drops_a_domain_with_no_majority_company(env, capsys):
    """R3: at exactly 50% no company wins, so the domain is dropped. A shared
    domain must not be attributed to one firm on a coin flip."""
    env.gitdm(person("liam", ["liam!shared.example"], ["Red Hat"]) + "\n"
              + person("mia", ["mia!shared.example"], ["SUSE"]))
    assert build() == 0
    assert rows_by_domain(env) == {}
    assert "R3-no-plurality" in capsys.readouterr().out


def test_r3_keeps_a_clear_majority_company(env):
    """Two of three people beat 50%, so the domain is attributed and the
    minority employer is discarded."""
    env.gitdm("\n".join([
        person("nina", ["nina!mixed.example"], ["Red Hat"]),
        person("omar", ["omar!mixed.example"], ["Red Hat"]),
        person("pia", ["pia!mixed.example"], ["SUSE"]),
    ]))
    assert build() == 0
    row = rows_by_domain(env)["mixed.example"]
    assert row["company"] == "Red Hat"
    assert row["source"] == "cncf-gitdm"


def test_r4_drops_a_company_that_is_only_a_domain_string(env, capsys):
    """R4: `systemli.org -> systemli.org` names no company."""
    env.gitdm(person("quinn", ["quinn!systemli.org"], ["systemli.org"]))
    assert build() == 0
    assert rows_by_domain(env) == {}
    assert "R4-company-is-a-domain" in capsys.readouterr().out


def test_r4_also_drops_a_real_company_whose_name_contains_a_dot(env):
    """DOCUMENTS A FALSE POSITIVE. R4 tests the shape of the string, not
    whether it names a firm, so a firm whose registered name IS a domain --
    Booking.com, Salesforce.com -- is dropped like a self-reference. The rule
    loses these rows in both sources. See the report."""
    env.gitdm(person("book", ["book!booking.com"], ["Booking.com"]))
    env.spinellis_rows([sp_row("bookings.example", "Booking.com", k10="t")])
    assert build() == 0
    assert rows_by_domain(env) == {}


def test_a_build_that_drops_every_domain_still_overwrites_the_artifact(env):
    """DOCUMENTS A DATA-SAFETY GAP. cmd_build aborts only when the source
    parses to nothing. A source that parses but loses every domain to R3/R4
    writes a header-only CSV over a good artifact. See the report."""
    env.data.mkdir()
    env.out.write_text("domain,company,kind,source\nkeep.example,Keep,company,gitdm\n")
    env.gitdm(person("quinn", ["quinn!systemli.org"], ["systemli.org"]))
    assert build() == 0
    assert env.out.read_text().splitlines() == ["domain,company,kind,source"]


@pytest.mark.parametrize("domain, company", [
    ("github.com", "GitHub"),
    ("mozilla.org", "Mozilla"),
    ("igalia.com", "Igalia"),
    ("suse.com", "SUSE"),
])
def test_r4_keeps_a_company_named_after_its_own_domain(env, domain, company):
    """REGRESSION LOCK. R4 once fired on any company whose name resembled its
    domain and silently deleted hundreds of correct rows. `github.com ->
    GitHub` and `mozilla.org -> Mozilla` are correct facts and MUST survive."""
    env.gitdm(person("rita", [f"rita!{domain}"], [company]))
    assert build() == 0
    assert rows_by_domain(env)[domain]["company"] == company


def test_the_curated_map_is_never_overwritten_by_the_import(env, capsys):
    """Research correctness: the curated map was hand-checked for the VEM paper
    and carries identity-level corrections this source cannot express."""
    env.curated_rows([{"domain": "redhat.com", "company": "Red Hat",
                       "kind": "company", "source": "gitdm"}])
    env.gitdm("\n".join([
        person("sam", ["sam!redhat.com"], ["Wrong Employer"]),
        person("tom", ["tom!redhat.com"], ["Wrong Employer"]),
    ]))
    assert build() == 0
    row = rows_by_domain(env)["redhat.com"]
    assert (row["company"], row["source"]) == ("Red Hat", "gitdm")
    assert "curated-wins" in capsys.readouterr().out


# --------------------------------------------------------------------------
# Provenance precedence, one test per adjacent pair
# --------------------------------------------------------------------------

def test_precedence_single_tier_is_weaker_than_the_corroborated_tier(env):
    """Adjacent pair 1: cncf-gitdm-single < cncf-gitdm. The two tiers are
    assigned by head-count, so one domain carries exactly one of them: the
    corroborated label is reserved for domains at or above the threshold."""
    env.gitdm("\n".join([
        person("u1", ["u1!one.example"], ["One Firm"]),
        person("u2", ["u2!two.example"], ["Two Firm"]),
        person("u3", ["u3!two.example"], ["Two Firm"]),
    ]))
    assert build(min_persons=2) == 0
    rows = rows_by_domain(env)
    assert rows["one.example"]["source"] == "cncf-gitdm-single"
    assert rows["two.example"]["source"] == "cncf-gitdm"


@pytest.mark.parametrize("n_people, cncf_source", [
    (1, "cncf-gitdm-single"),
    (2, "cncf-gitdm"),
])
def test_precedence_plain_spinellis_only_fills_a_gap(env, n_people, cncf_source):
    """Adjacent pair 2: plain `spinellis` never overwrites a CNCF row; it only
    supplies a domain nobody else has.

    NOTE: the module comment claims the ladder is
    `cncf-gitdm-single < cncf-gitdm < spinellis`, but the merge condition is
    `if src == "spinellis-sec" or d not in merged`, so a one-person,
    uncorroborated CNCF row also beats a published CC-BY row. The test locks in
    the code as written; the comment is wrong. See the report."""
    env.gitdm("\n".join(person(f"v{i}", [f"v{i}!held.example"], ["CNCF Firm"])
                        for i in range(n_people)))
    env.spinellis_rows([sp_row("held.example", "Spinellis Firm"),
                        sp_row("gap.example", "Gap Firm")])
    assert build() == 0
    rows = rows_by_domain(env)
    assert (rows["held.example"]["company"],
            rows["held.example"]["source"]) == ("CNCF Firm", cncf_source)
    assert (rows["gap.example"]["company"],
            rows["gap.example"]["source"]) == ("Gap Firm", "spinellis")


def test_precedence_spinellis_sec_overwrites_a_plain_spinellis_row(env):
    """Adjacent pair 3: spinellis < spinellis-sec. Within the published dataset
    an SEC/Fortune-matched row wins over an unmatched one for the same
    domain."""
    env.spinellis_rows([sp_row("dual.example", "Plain Name"),
                        sp_row("dual.example", "Filer Name", k10="t")])
    got = bdm.parse_spinellis_domains()
    assert got["dual.example"] == ("Filer Name", "spinellis-sec")


@pytest.mark.parametrize("n_people, cncf_source", [
    (1, "cncf-gitdm-single"),
    (2, "cncf-gitdm"),
])
def test_precedence_spinellis_sec_overwrites_a_cncf_row(env, n_people,
                                                       cncf_source):
    """Adjacent pair 3, in the merge: an externally verifiable pairing beats
    both CNCF tiers, because an SEC filing is the best provenance in the
    map."""
    env.gitdm("\n".join(person(f"w{i}", [f"w{i}!filer.example"], ["CNCF Firm"])
                        for i in range(n_people)))
    env.spinellis_rows([sp_row("filer.example", "Filer Firm", f20="t")])
    assert build() == 0
    row = rows_by_domain(env)["filer.example"]
    assert (row["company"], row["source"]) == ("Filer Firm", "spinellis-sec")
    assert row["company"] != "CNCF Firm", f"{cncf_source} must lose to spinellis-sec"


def test_precedence_curated_overwrites_spinellis_sec(env):
    """Adjacent pair 4: curated < nothing. The hand-checked map is final, even
    against an SEC-verified row."""
    env.curated_rows([{"domain": "filer.example", "company": "Curated Firm",
                       "kind": "company", "source": "patch"}])
    env.spinellis_rows([sp_row("filer.example", "Filer Firm", fg500="t")])
    env.gitdm(person("x1", ["x1!other.example"], ["Other Firm"]))
    assert build() == 0
    row = rows_by_domain(env)["filer.example"]
    assert (row["company"], row["source"]) == ("Curated Firm", "patch")


# --------------------------------------------------------------------------
# parse_spinellis_domains
# --------------------------------------------------------------------------

def test_spinellis_drops_an_ambiguous_domain(env):
    """Two firms named for one domain is not a company fact."""
    env.spinellis_rows([sp_row("shared.example", "Firm One"),
                        sp_row("shared.example", "Firm Two")])
    assert "shared.example" not in bdm.parse_spinellis_domains()


def test_spinellis_drops_an_ambiguous_verified_domain(env):
    """Being an SEC filer does not excuse ambiguity: two verified firms for one
    domain is still not a company fact."""
    env.spinellis_rows([sp_row("shared.example", "Filer One", k10="t"),
                        sp_row("shared.example", "Filer Two", f20="t")])
    assert bdm.parse_spinellis_domains() == {}


def test_a_verified_ambiguity_falls_back_to_the_plain_row(env):
    """An ambiguous verified bucket leaves an unambiguous plain row standing,
    so the domain is still mapped, at the weaker provenance."""
    env.spinellis_rows([sp_row("mixed.example", "Plain Firm"),
                        sp_row("mixed.example", "Filer One", k10="t"),
                        sp_row("mixed.example", "Filer Two", k10="t")])
    assert bdm.parse_spinellis_domains() == {
        "mixed.example": ("Plain Firm", "spinellis")}


@pytest.mark.parametrize("kwargs", [
    {"fg500": "t"}, {"k10": "t"}, {"f20": "t"}, {"fg500": "T"},
    {"k10": " t "}, {"fg500": "t", "k10": "t", "f20": "t"},
])
def test_spinellis_sec_or_fortune_flag_yields_the_verified_source(env, kwargs):
    """A Fortune Global 500 or SEC 10-K/20-F match is externally verifiable, so
    it earns the strongest imported provenance."""
    env.spinellis_rows([sp_row("filer.example", "Filer Firm", **kwargs)])
    assert bdm.parse_spinellis_domains() == {
        "filer.example": ("Filer Firm", "spinellis-sec")}


@pytest.mark.parametrize("kwargs", [{}, {"fg500": "f"}, {"k10": "false"},
                                    {"f20": "0"}])
def test_spinellis_without_a_flag_yields_the_plain_source(env, kwargs):
    """Anything that is not a 't' is not a verification."""
    env.spinellis_rows([sp_row("plain.example", "Plain Firm", **kwargs)])
    assert bdm.parse_spinellis_domains() == {
        "plain.example": ("Plain Firm", "spinellis")}


@pytest.mark.parametrize("domain", ["gmail.com", "hotmail.com", "qq.com",
                                    "users.noreply.github.com", "example.com"])
def test_spinellis_never_turns_a_free_provider_into_a_firm(env, domain):
    """R1 applies to the published dataset too."""
    env.spinellis_rows([sp_row(domain, "Some Firm", k10="t")])
    assert bdm.parse_spinellis_domains() == {}


@pytest.mark.parametrize("company", ["systemli.org", "foo.co.uk", "bar.io"])
def test_spinellis_drops_a_company_that_is_a_domain(env, company):
    """R4 applies to the published dataset too."""
    env.spinellis_rows([sp_row("some.example", company)])
    assert bdm.parse_spinellis_domains() == {}


def test_spinellis_normalises_the_company_name(env):
    """One firm, one spelling, whichever source it came from."""
    env.spinellis_rows([sp_row("axis.example", "Axis Communications AB"),
                        sp_row("axis.example", "Axis Communications")])
    assert bdm.parse_spinellis_domains() == {
        "axis.example": ("Axis Communications", "spinellis")}


@pytest.mark.parametrize("bad_row", [
    "",                                       # blank line
    "\t".join([""] * 5),                      # too few columns
    "\t".join([""] * bdm.SP_COMPANY),         # one column short
])
def test_spinellis_ignores_a_short_or_blank_row(env, bad_row):
    """The file is headerless and ragged; a short row must not raise."""
    env.spinellis_rows([bad_row, sp_row("good.example", "Good Firm")])
    assert bdm.parse_spinellis_domains() == {
        "good.example": ("Good Firm", "spinellis")}


@pytest.mark.parametrize("domain, company", [
    ("", "Firm"), ("nodots", "Firm"), ("good.example", ""),
    ("good.example", "independent"), ("good.example", "various"),
])
def test_spinellis_ignores_an_unusable_pair(env, domain, company):
    """A missing domain, a single label, or a non-firm value yields nothing."""
    env.spinellis_rows([sp_row(domain, company)])
    assert bdm.parse_spinellis_domains() == {}


def test_spinellis_missing_file_returns_empty_and_does_not_raise(env, capsys):
    """The dataset is optional; without it the build must still run."""
    assert not env.spinellis.exists()
    assert bdm.parse_spinellis_domains() == {}
    assert "spinellis" in capsys.readouterr().out


def test_spinellis_sec_row_hides_a_conflicting_plain_row(env):
    """DOCUMENTS A GAP, not a desired behaviour. Ambiguity is tested inside the
    verified and the plain bucket separately, so a domain whose SEC-flagged row
    and unflagged row name DIFFERENT firms is not reported as ambiguous: the
    verified name wins silently. See the report."""
    env.spinellis_rows([sp_row("conflict.example", "Plain Firm"),
                        sp_row("conflict.example", "Filer Firm", k10="t")])
    assert bdm.parse_spinellis_domains() == {
        "conflict.example": ("Filer Firm", "spinellis-sec")}


# --------------------------------------------------------------------------
# load_curated
# --------------------------------------------------------------------------

def test_load_curated_missing_file_warns_and_returns_empty(env, capsys):
    """A missing curated map must be visible in the log, not silent: the whole
    output would otherwise lose its authoritative rows without a word."""
    assert bdm.load_curated() == {}
    out = capsys.readouterr().out
    assert "WARNING" in out and "curated map not found" in out


def test_load_curated_reads_kind_and_source(env):
    """Curated provenance is carried through, not re-labelled."""
    env.curated_rows([
        {"domain": "gmail.com", "company": "(Independent)",
         "kind": "free_provider", "source": "gitdm"},
        {"domain": "REDHAT.COM", "company": "Red Hat",
         "kind": "company", "source": "patch"},
    ])
    assert bdm.load_curated() == {
        "gmail.com": ("(Independent)", "free_provider", "gitdm"),
        "redhat.com": ("Red Hat", "company", "patch"),
    }


def test_load_curated_defaults_blank_kind_and_source(env):
    """A row with only a domain and a company still gets a usable kind and a
    provenance label."""
    env.curated_rows([{"domain": "redhat.com", "company": "Red Hat",
                       "kind": "", "source": ""},
                      {"domain": " ", "company": "Ignored",
                       "kind": "", "source": ""}])
    assert bdm.load_curated() == {"redhat.com": ("Red Hat", "company", "gitdm")}


# --------------------------------------------------------------------------
# cmd_build output contract
# --------------------------------------------------------------------------

def test_output_header_is_exact_and_rows_are_sorted_by_domain(env):
    """The schema is consumed by signals.py; a renamed or reordered column
    breaks it silently."""
    env.gitdm("\n".join([
        person("y1", ["y1!zeta.example"], ["Zeta Firm"]),
        person("y2", ["y2!alpha.example"], ["Alpha Firm"]),
        person("y3", ["y3!mid.example"], ["Mid Firm"]),
    ]))
    env.curated_rows([{"domain": "beta.example", "company": "Beta Firm",
                       "kind": "company", "source": "gitdm"}])
    assert build() == 0
    assert env.out_header() == "domain,company,kind,source"
    domains = [r["domain"] for r in env.read_out()]
    assert domains == sorted(domains)
    assert domains == ["alpha.example", "beta.example", "mid.example",
                       "zeta.example"]


def test_report_does_not_change_the_data(env):
    """--report is a log flag. If it changed a byte of the artifact, a printed
    summary would not describe the shipped file."""
    env.gitdm("\n".join([
        person("z1", ["z1!igalia.com"], ["Igalia"]),
        person("z2", ["z2!igalia.com"], ["Igalia"]),
        person("z3", ["z3!suse.com"], ["SUSE"]),
    ]))
    env.spinellis_rows([sp_row("filer.example", "Filer Firm", k10="t")])
    assert build(report=False) == 0
    plain = env.out.read_bytes()
    assert build(report=True) == 0
    assert env.out.read_bytes() == plain


def test_report_lists_the_largest_imported_companies(env, capsys):
    """The report counts imported rows only, so a curated-only firm must not
    appear in it."""
    env.gitdm("\n".join([
        person("r1", ["r1!a.example"], ["Big Firm"]),
        person("r2", ["r2!b.example"], ["Big Firm"]),
    ]))
    env.curated_rows([{"domain": "c.example", "company": "Curated Only",
                       "kind": "company", "source": "gitdm"}])
    assert build(report=True) == 0
    out = capsys.readouterr().out
    assert "largest imported companies" in out
    assert "Big Firm" in out
    assert "Curated Only" not in out


def test_an_empty_source_aborts_without_writing(env):
    """Data safety: a failed parse must not overwrite a good artifact with an
    empty one."""
    env.data.mkdir()
    env.out.write_text("domain,company,kind,source\nkeep.example,Keep,company,gitdm\n")
    env.gitdm("# nothing but a comment\n")
    assert build() == 1
    assert "keep.example" in env.out.read_text()


def test_build_writes_even_when_only_the_import_has_rows(env):
    """The curated map is optional at build time."""
    env.gitdm(person("s1", ["s1!igalia.com"], ["Igalia"]))
    assert build() == 0
    assert [r["domain"] for r in env.read_out()] == ["igalia.com"]


# --------------------------------------------------------------------------
# cmd_fetch — no network
# --------------------------------------------------------------------------

class FakeResponse:
    def __init__(self, payload: bytes):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self) -> bytes:
        return self.payload


@pytest.fixture
def fake_urlopen(monkeypatch):
    """No test may open a socket."""
    calls: list[str] = []

    def _urlopen(req, timeout=None):
        calls.append(req.full_url)
        return FakeResponse(b"payload for " + req.full_url.encode())

    monkeypatch.setattr(bdm.urllib.request, "urlopen", _urlopen)
    return calls


def test_fetch_caches_each_missing_file(env, monkeypatch, fake_urlopen):
    """The cache directory is created and one file is written per source."""
    monkeypatch.setattr(bdm, "N_FILES", 2)
    assert bdm.cmd_fetch(argparse.Namespace()) == 0
    assert len(fake_urlopen) == 2
    for i in (1, 2):
        dest = env.cache / f"developers_affiliations{i}.txt"
        assert dest.read_bytes().startswith(b"payload for https://")


def test_fetch_skips_a_file_already_cached(env, monkeypatch, fake_urlopen):
    """A long fetch must be resumable. Re-downloading a cached file wastes the
    upstream's bandwidth and ours."""
    monkeypatch.setattr(bdm, "N_FILES", 2)
    (env.cache / "developers_affiliations1.txt").write_bytes(b"already here")
    assert bdm.cmd_fetch(argparse.Namespace()) == 0
    assert len(fake_urlopen) == 1                        # only file 2
    assert (env.cache / "developers_affiliations1.txt").read_bytes() == b"already here"


def test_fetch_refetches_an_empty_cached_file(env, monkeypatch, fake_urlopen):
    """A zero-byte file is a failed download, not a cache hit."""
    monkeypatch.setattr(bdm, "N_FILES", 1)
    (env.cache / "developers_affiliations1.txt").write_bytes(b"")
    assert bdm.cmd_fetch(argparse.Namespace()) == 0
    assert len(fake_urlopen) == 1
    assert (env.cache / "developers_affiliations1.txt").stat().st_size > 0


def test_fetch_survives_a_network_error(env, monkeypatch, capsys):
    """One unreachable file must not abort the other nine."""
    monkeypatch.setattr(bdm, "N_FILES", 2)

    def _boom(req, timeout=None):
        if "affiliations1" in req.full_url:
            raise OSError("connection reset")
        return FakeResponse(b"ok")

    monkeypatch.setattr(bdm.urllib.request, "urlopen", _boom)
    assert bdm.cmd_fetch(argparse.Namespace()) == 0
    out = capsys.readouterr().out
    assert "FAILED: connection reset" in out
    assert not (env.cache / "developers_affiliations1.txt").exists()
    assert (env.cache / "developers_affiliations2.txt").read_bytes() == b"ok"


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------

def test_main_build_uses_the_default_threshold(env, monkeypatch):
    """The CLI default must be the documented MIN_PERSONS, so a plain
    `build` and the paper's numbers agree."""
    env.gitdm(person("t1", ["t1!igalia.com"], ["Igalia"]))
    monkeypatch.setattr("sys.argv", ["build_domain_map.py", "build"])
    assert bdm.main() == 0
    assert rows_by_domain(env)["igalia.com"]["source"] == "cncf-gitdm-single"


def test_main_build_accepts_min_persons_and_report(env, monkeypatch):
    """The flags reach cmd_build."""
    env.gitdm(person("t2", ["t2!igalia.com"], ["Igalia"]))
    monkeypatch.setattr("sys.argv", ["build_domain_map.py", "build",
                                     "--min-persons", "1", "--report"])
    assert bdm.main() == 0
    assert rows_by_domain(env)["igalia.com"]["source"] == "cncf-gitdm"


def test_main_fetch_dispatches_to_cmd_fetch(env, monkeypatch, fake_urlopen):
    """`fetch` must not need the build flags."""
    monkeypatch.setattr(bdm, "N_FILES", 1)
    monkeypatch.setattr("sys.argv", ["build_domain_map.py", "fetch"])
    assert bdm.main() == 0
    assert len(fake_urlopen) == 1


def test_main_requires_a_subcommand(monkeypatch):
    """A bare invocation must not silently do nothing."""
    monkeypatch.setattr("sys.argv", ["build_domain_map.py"])
    with pytest.raises(SystemExit) as exc:
        bdm.main()
    assert exc.value.code == 2


def test_say_prints_a_timestamp(capsys):
    """Every log line is timestamped, so a long run can be audited."""
    bdm.say("hello")
    out = capsys.readouterr().out
    assert out.endswith("hello\n")
    assert out.startswith("[") and "]" in out
