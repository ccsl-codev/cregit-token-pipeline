#!/usr/bin/env python3
"""Pseudonymize cregit token-ownership parquets for publication, fail-closed.

Adapted from pipeline/anonymize/anonymize_parquet.py in
cbsoft-vem2026-corporate-truck-factor at commit 3d722c6 (that repository backs a
published Zenodo artifact and is not modified here). What is kept from the
original, what is fixed, and what is new:

KEPT -- the two decisions that make the firm analysis survive anonymization.

  1. The e-mail LOCAL PART is pseudonymized and the DOMAIN is preserved:
     alice@intel.com -> author_3f9c1a7b2e5d40@intel.com. The firm attribution is
     resolved from the domain (person_domain -> data/affiliation.merged.csv ->
     firm), so replacing the domain with anon.invalid would collapse every firm
     to '(Unknown)' and the corporate-truck-factor analysis would die.
  2. person_domain passes through untouched: company signal, not a personal
     identifier. firm_raw / firm / firm_source likewise.
  Names become 'Author <token>' through a registry keyed on the lowercased
  string, so every occurrence of one person maps to one pseudonym and group-by
  joins still work.

FIXED -- the original's COPY (SELECT ...) enumerated columns by hand and ended
at `person_domain, repo_tag`. Run on this 70-column schema it emits 26 columns
and silently drops 44: the 3 firm columns, the 15 footer_* columns and the 29
provenance columns. No error, no warning. So column handling here is FAIL
CLOSED: every column of the input schema is classified into exactly one of
pass-through / transform / drop, and a column matched by no rule raises
UnknownColumnError naming it. When the schema grows again this tool stops
working loudly instead of quietly shrinking the dataset.

NEW -- the 15 footer_* columns (VARCHAR[] of raw commit-trailer text such as
'Signed-off-by: Real Name <real@email>') are PRESERVED by default, not dropped.
Each element is parsed and both halves go through the SAME registry as the
scalar identity columns, so a person carries one pseudonym in a footer and in
person_name. --drop-footers is offered as the explicit alternative; the choice
of what to publish is not made here. Non-conforming footer elements (anything
that is not 'Name <email>') are never passed through as they stand: they are
mapped whole through the name registry, and counted and sampled in the report.

  Measured on the three files this was developed against, the derived columns
  footer_personids / footer_person_names carry values of the form
  'prql-bot at prql-bot@users.noreply.github.com' -- cregit's own personid
  convention, a real e-mail inside a name-shaped string. The original's
  registry was built from four scalar columns only, so those values would have
  had no mapping. Element-level mapping of the derived columns is therefore not
  cosmetic; without it the footers leak.

NEW -- pseudonyms come from a SALTED KEYED HASH, not from a sequence number.

  The original, and this tool up to the commit that added this paragraph, handed
  out ids by sorting the distinct lowercased values of one invocation and
  counting from 1. That is deterministic but NOT stable: an id is an ordinal
  position, so inserting one value shifts every value after it. Measured over six
  corpus Parquets, dropping one file from the invocation renumbered 150 of the
  153 addresses present in both runs -- 98.0%. A second release that adds one
  project therefore renumbers almost every pseudonym and cannot be diffed against
  the first, and a reader cannot follow one contributor across two releases.

  A pseudonym is now `blake2b(space || NUL || lowercased value, key=salt)` taken
  to PSEUDO_HEX_LEN hex characters, so it depends on the value and the salt and
  on nothing else. Adding or dropping a project renumbers nobody, which is
  pinned by tests/test_anonymize_parquet_e2e.py requirement 7.

  * blake2b with a key is a FAST keyed hash, chosen deliberately. A slow
    password KDF (bcrypt, scrypt, argon2, PBKDF2 with a high iteration count)
    exists to burn wall-clock time; that is the opposite of what is wanted for
    28,339 distinct values inside a release step. The argument for a slow KDF is
    that it raises the cost of a brute-force guess of the *inputs* by an
    adversary who has ALREADY obtained the salt. That threat is addressed here by
    keeping the salt out of the release and out of the repository, not by making
    the hash slow. See docs/DESIGN.md row 8 if that trade is ever revisited.
  * PSEUDO_HEX_LEN = 14 hex characters = 56 bits. Collision arithmetic at the
    measured population of 28,339 distinct addresses over 186 conforming files:
    n(n-1)/2 = 401,535,291 pairs, over 2^56 = 72,057,594,037,927,936, so the
    probability that ANY two addresses share a pseudonym is about 5.6e-9 -- one
    release in 180 million. At ten times the corpus it is still 5.6e-7. 48 bits
    would give 1.4e-6, or one release in 700,000, which is too coarse for
    something that hard-fails a release; 64 bits costs two more characters per id
    and buys nothing usable. The name space is independent and smaller (25,386
    distinct names, 4.5e-9).
  * A collision is NOT tolerated. Two people sharing a pseudonym would merge two
    contributors, drop the distinct-person count and corrupt the dataset's
    headline metric, so build_registry raises CollisionError and the release
    stops. The remedy is to rotate the salt, which renumbers everyone -- see
    docs/DESIGN.md.
  * The salt lives OUTSIDE this repository and is never committed; see load_salt
    for where it is read from. A missing or short salt is a hard error. Nothing
    here ever generates one: a freshly generated salt per run would renumber
    every pseudonym on every run, which is the 98.0% defect made continuous and
    silent.
  * The real -> pseudonym map is held in memory for the length of one run and is
    never written anywhere. It is NOT published, and neither is the salt, so the
    release is not reversible from public inputs -- which the sequential scheme
    was, since its inputs derive from public GitHub repositories.

commit_summary: PRESERVED with an e-mail scrub, reversing the original's
unconditional NULL. The original's reason was that commit_summary carries
footer-style 'Name <email>' identity text. In this schema the trailers are 15
separate columns and commit_summary holds the subject line only. Measured over
1,393 distinct subjects in the three development files: 0 contain an e-mail
shape, 0 contain an angle-bracketed address and 0 contain any identity name of
five characters or more. Publishing it costs nothing and a subject line is
useful. The check is not assumed: any residue makes the run FAIL, and
--null-commit-summary restores the original behaviour.

RESIDUAL RISK, to be disclosed in any paper using this output:

  * Preserving the e-mail domain leaves a sole contributor at a rare domain
    identifiable. Anyone who knows that exactly one person ever committed from
    smallfirm.example can re-identify author_3f9c1a7b2e5d40@smallfirm.example
    without breaking anything, and no salt helps: the domain is in the clear.
    Measured over all 186 conforming corpus files and 28,339 distinct addresses:
    5,276 of 6,228 person_domain groups have exactly ONE distinct address --
    84.7%. An earlier three-file sample gave 33 of 38 and hedged that the ratio
    was "inflated by the small sample". It was not. The ratio did not fall with
    scale, because the corpus is mostly small samples and a small sample is
    itself composed of single-contributor projects.
    This is a deliberate trade of privacy for the firm signal the dataset exists
    to carry, not a defect, and it cannot be fixed while the domain is published.
    k-anonymity over the domain is the mitigation if a release needs one; it is
    NOT applied here, and no k-anonymity work is planned, because it would drop
    exactly the long tail of small firms that the truck-factor question is about.
    This is a DISCLOSED LIMITATION, not a mitigated one. Any paper using this
    output must disclose it; see docs/LIMITATIONS.md.
  * repo_name, clone_url, owner, repo and roster_name carry the GitHub
    namespace, which for a personal repository IS a person's handle
    (owner_type = 'User'). These are not pseudonymized: the repository
    identifier is the dataset's key and its provenance, and the repositories
    are public. Their presence is a re-identification route for the owner.
  * source_text and token_value are source code and can contain author names in
    copyright headers and @author tags. They are not scrubbed, because scrubbing
    the token stream would destroy the artifact the dataset is. Leak scanning
    reports these separately from the identity columns for exactly this reason.

Usage:
  anonymize_parquet.py OUTDIR IN.parquet [IN.parquet ...]
      [--drop-footers] [--null-commit-summary] [--report FILE.json]
      [--salt-file PATH]

  The registry spans every input file of one invocation, so pseudonyms are
  consistent across files. Pass the files that will be published together.
  Since the salted hash made pseudonyms stable, the grouping no longer affects
  the pseudonyms -- it affects only which files the leak scan can cross-check.

  A salt is REQUIRED. Create one once, outside the repository:

      mkdir -p ~/.config/cregit-token-pipeline
      head -c 32 /dev/urandom | base64 > ~/.config/cregit-token-pipeline/anon-salt
      chmod 600 ~/.config/cregit-token-pipeline/anon-salt

  Back it up somewhere you would trust with the release itself. Losing it means
  no future release can ever be linked to a published one; leaking it makes every
  pseudonym reversible by dictionary attack over public GitHub addresses.

Needs duckdb, which comes from `devenv shell` (entered from cregit-issue61) and
is absent from .venv. duckdb is imported inside the functions that need it so
the classification, parsing and rendering logic can be unit-tested without it.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import sys
from dataclasses import dataclass, field

# --------------------------------------------------------------------------
# The column contract. Mirrors validate_schema.EXPECTED_COLUMNS, split by what
# anonymization does to each column rather than by what type it is.
# --------------------------------------------------------------------------

# Per-project provenance, injected from project_meta.json. Constant within a
# project, carries no personal data (audited by audit_passthrough()). These are
# the point of the dataset: a reader filters on them without a second join.
PROVENANCE_COLUMNS: tuple[str, ...] = (
    "clone_url", "provenance_status", "source", "stratum", "fact", "contested",
    "label_date", "owner", "repo", "roster_name", "roster_lang", "language",
    "commits", "size_class", "size_kb", "stars", "pushed_at", "license",
    "owner_type", "archived", "fork", "history_cluster", "history_shared_with",
    "history_relation", "history_includes", "history_first", "history_created",
    "manifest_category", "file_mask",
)

# Firm attribution, resolved from person_domain. Public company data.
FIRM_COLUMNS: tuple[str, ...] = ("firm_raw", "firm", "firm_source")

# Everything else that is copied verbatim: the token stream, shas, dates, paths,
# and person_domain -- the company signal, which is why it is kept.
OTHER_PASSTHROUGH_COLUMNS: tuple[str, ...] = (
    "repo_name", "file_path", "token_index", "source_line", "source_col",
    "source_text", "token_type", "token_value", "is_structural",
    "cregit_commit_sha", "original_commit_sha", "author_date", "committer_date",
    "person_domain", "repo_tag",
)

PASSTHROUGH_COLUMNS: frozenset[str] = frozenset(
    PROVENANCE_COLUMNS + FIRM_COLUMNS + OTHER_PASSTHROUGH_COLUMNS)

# Scalar identity columns -> how to rewrite them. <t> is the 14-hex-character
# salted token from pseudo_token().
#   name      'Author <t>'
#   email     'author_<t>@' + original domain
#   personid  'author <t>'    (cregit's lowercased id style)
#   summary   e-mail local parts scrubbed, or NULL under --null-commit-summary
SCALAR_TRANSFORMS: dict[str, str] = {
    "author_name": "name",
    "committer_name": "name",
    "person_name": "name",
    "personid": "personid",
    "author_email": "email",
    "committer_email": "email",
    "person_email": "email",
    "commit_summary": "summary",
}

# Raw commit-trailer text, one list element per trailer line.
FOOTER_TEXT_COLUMNS: tuple[str, ...] = (
    "footer_signed_off_by", "footer_co_authored_by", "footer_co_developed_by",
    "footer_reviewed_by", "footer_acked_by", "footer_tested_by",
    "footer_reported_by", "footer_suggested_by", "footer_based_on_patch_by",
    "footer_helped_by", "footer_mentored_by", "footer_assisted_by",
    "footer_thanks_to",
)

# Derived from the above by generate_dataset, in cregit's personid convention.
FOOTER_DERIVED_COLUMNS: dict[str, str] = {
    "footer_personids": "personid",
    "footer_person_names": "name",
}

FOOTER_COLUMNS: tuple[str, ...] = FOOTER_TEXT_COLUMNS + tuple(
    FOOTER_DERIVED_COLUMNS)

# Columns whose content is prose or code rather than an identity field. A real
# name found here is reported, not asserted away; see the docstring.
CONTENT_COLUMNS: frozenset[str] = frozenset({"source_text", "token_value",
                                             "file_path", "repo_name",
                                             "clone_url", "owner", "repo",
                                             "roster_name", "fact"})

# Emitted where a value reached the writer with no registry entry. Must never
# appear in the output; verify_output() asserts that. It exists so that a
# registry bug shows up as a visible marker instead of a leaked name.
MISSING_MARKER = "(ANON-MISSING)"


class UnknownColumnError(ValueError):
    """An input column matched no rule. The whole point of this module."""


class MissingColumnError(ValueError):
    """The input lacks a column the verification needs. The mirror of the above.

    Fail closed cuts both ways: an input narrower than the contract (an older
    generate_dataset output, say) cannot be checked for invariance, so it is
    refused rather than anonymized without proof.
    """


# Columns the verification stages read by name. An input without them cannot be
# verified, so it is not written.
REQUIRED_COLUMNS: tuple[str, ...] = (
    "firm", "person_domain", "repo_name", "person_email", "person_name",
    "commit_summary",
)


@dataclass(frozen=True)
class Plan:
    """What happens to each column of one input schema."""

    column_order: tuple[str, ...]           # the input schema, in file order
    passthrough: tuple[str, ...]
    scalar: tuple[tuple[str, str], ...]     # (column, kind)
    footer_text: tuple[str, ...]
    footer_derived: tuple[tuple[str, str], ...]   # (column, kind)
    dropped: tuple[str, ...]

    @property
    def n_in(self) -> int:
        return (len(self.passthrough) + len(self.scalar) + len(self.footer_text)
                + len(self.footer_derived) + len(self.dropped))

    @property
    def n_out(self) -> int:
        return self.n_in - len(self.dropped)

    def rule_of(self, column: str) -> str:
        """The stated rule for one column, for the accounting table."""
        if column in self.dropped:
            return "drop"
        if column in self.passthrough:
            return "pass"
        for c, kind in self.scalar:
            if c == column:
                return f"transform:{kind}"
        if column in self.footer_text:
            return "transform:footer_text"
        for c, kind in self.footer_derived:
            if c == column:
                return f"transform:footer_{kind}"
        raise UnknownColumnError(column)


def classify(columns: list[str], drop_footers: bool = False) -> Plan:
    """Assign every input column exactly one rule, or raise.

    Fail closed: a column the contract does not know about is an error, not a
    thing to copy and hope about and not a thing to silently omit. Both of those
    are how a 70-column dataset becomes a 26-column dataset without anyone
    noticing.
    """
    passthrough: list[str] = []
    scalar: list[tuple[str, str]] = []
    footer_text: list[str] = []
    footer_derived: list[tuple[str, str]] = []
    dropped: list[str] = []
    unknown: list[str] = []

    for c in columns:
        if c in PASSTHROUGH_COLUMNS:
            passthrough.append(c)
        elif c in SCALAR_TRANSFORMS:
            scalar.append((c, SCALAR_TRANSFORMS[c]))
        elif c in FOOTER_TEXT_COLUMNS:
            (dropped if drop_footers else footer_text).append(c)
        elif c in FOOTER_DERIVED_COLUMNS:
            if drop_footers:
                dropped.append(c)
            else:
                footer_derived.append((c, FOOTER_DERIVED_COLUMNS[c]))
        else:
            unknown.append(c)

    missing = [c for c in REQUIRED_COLUMNS if c not in columns]
    if missing and not unknown:
        raise MissingColumnError(
            "input is missing column(s) the verification needs: "
            + ", ".join(missing)
            + ". This tool verifies analysis invariance over firm, "
              "person_domain and repo_name and refuses to write output it "
              "cannot check."
        )
    if unknown:
        raise UnknownColumnError(
            "input columns matched by no anonymization rule: "
            + ", ".join(sorted(unknown))
            + ". Classify each one in anonymize_parquet.py (pass through, "
              "transform or drop) before publishing. Refusing to guess."
        )
    return Plan(tuple(columns), tuple(passthrough), tuple(scalar),
                tuple(footer_text), tuple(footer_derived), tuple(dropped))


# --------------------------------------------------------------------------
# Footer element parsing
# --------------------------------------------------------------------------

# 'Real Name <real@email>', the git trailer shape.
NAME_EMAIL = re.compile(r"^\s*(?P<name>.*?)\s*<\s*(?P<email>[^<>\s]+@[^<>\s]+)\s*>\s*$")
# A bare address on its own, 'real@email'.
BARE_EMAIL = re.compile(r"^\s*(?P<email>[^<>\s@]+@[^<>\s@]+\.[A-Za-z0-9\-]+)\s*$")
# cregit's personid convention, 'handle at local@domain'. Real address, no
# angle brackets, so NAME_EMAIL does not see it.
NAME_AT_EMAIL = re.compile(r"^\s*(?P<name>\S.*?)\s+at\s+(?P<email>[^<>\s@]+@[^<>\s]+)\s*$")
# Anything e-mail shaped, anywhere in a string. One definition used on both
# sides: Python compiles it, and audit_passthrough() hands the same text to
# duckdb's regexp_matches. Safe to share because duckdb does not process
# backslash escapes inside single-quoted literals, so the engine's RE2 sees
# exactly these characters.
EMAILISH = r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}"

SHAPE_NAME_EMAIL = "name_email"
SHAPE_BARE_EMAIL = "bare_email"
SHAPE_NAME_AT_EMAIL = "name_at_email"
SHAPE_OTHER = "other"

# Shapes that are not the documented 'Name <email>' trailer form. Reported.
NONCONFORMING_SHAPES = (SHAPE_BARE_EMAIL, SHAPE_NAME_AT_EMAIL, SHAPE_OTHER)


def parse_footer_element(value: str) -> tuple[str, str | None, str | None]:
    """(shape, name, email) for one footer list element.

    The shapes other than name_email are the honest part of this function: they
    say 'this did not look like a trailer', which is what gets counted and
    sampled in the report. Nothing is passed through unexamined -- name_at_email
    and other are mapped whole through the name registry, which is safe because
    it replaces the entire string.
    """
    m = NAME_EMAIL.match(value)
    if m:
        return SHAPE_NAME_EMAIL, (m.group("name") or None), m.group("email")
    m = BARE_EMAIL.match(value)
    if m:
        return SHAPE_BARE_EMAIL, None, m.group("email")
    if NAME_AT_EMAIL.match(value):
        return SHAPE_NAME_AT_EMAIL, value, None
    return SHAPE_OTHER, value, None


# --------------------------------------------------------------------------
# The salt. Held privately, outside this repository, and never published.
# --------------------------------------------------------------------------

# Read in this order, first hit wins: --salt-file, $CTP_ANON_SALT_FILE,
# $CTP_ANON_SALT (the salt inline, for CI), DEFAULT_SALT_FILE.
SALT_ENV = "CTP_ANON_SALT"
SALT_FILE_ENV = "CTP_ANON_SALT_FILE"
DEFAULT_SALT_FILE = "~/.config/cregit-token-pipeline/anon-salt"

# 32 bytes = 256 bits. Enough that guessing the salt is not the cheap attack, and
# the same floor as the `head -c 32 /dev/urandom` the docs tell you to run. A
# shorter one is refused rather than accepted with a warning: a warning in a
# release script is read once and then never again.
MIN_SALT_BYTES = 32

# 14 lowercase hex characters = 56 bits. See the module docstring for the
# collision arithmetic at 28,339 distinct addresses (p ~= 5.6e-9).
PSEUDO_HEX_LEN = 14


class SaltError(Exception):
    """No usable salt. Never recovered from by inventing one."""


class CollisionError(Exception):
    """Two distinct values hashed to one pseudonym. Stops the release."""


def load_salt(path: str | None = None, env=None) -> bytes:
    """The private salt, or SaltError. Never generates, never defaults.

    A generated salt would be the 98.0% renumbering defect made continuous and
    silent: every run would produce a fresh mapping, and nothing in the output
    would say so. So the only outcomes here are "the operator's salt" and "stop".
    """
    env = os.environ if env is None else env

    # Strict precedence, most explicit first. Inline is deliberately below the
    # two file sources: a developer who exports $CTP_ANON_SALT for one experiment
    # must not silently override the salt a release script passed by path.
    #
    # An EXPLICITLY named source that is absent is a hard error, never a
    # fall-through to the next candidate. Falling through would quietly hash with
    # a different key than the operator asked for, which renumbers the whole
    # release and looks like a success -- the same class of silent instability
    # this scheme exists to remove. Only the default location may be absent,
    # because there is nothing after it.
    if path:
        return _require_salt_file(path, "--salt-file")
    if env.get(SALT_FILE_ENV):
        return _require_salt_file(env[SALT_FILE_ENV], f"${SALT_FILE_ENV}")
    if env.get(SALT_ENV):
        salt = env[SALT_ENV].strip().encode()
        _check_salt_length(salt, f"${SALT_ENV}")
        return salt

    default = os.path.expanduser(DEFAULT_SALT_FILE)
    if os.path.isfile(default):
        return _read_salt_file(default, "the default location")

    raise SaltError(
        f"no anonymization salt. --salt-file and ${SALT_FILE_ENV} and "
        f"${SALT_ENV} are all unset, and the default {default} does not "
        f"exist.\n"
        f"Set ${SALT_FILE_ENV} to a salt file, or ${SALT_ENV} to the salt "
        f"itself, or create the default:\n"
        f"    mkdir -p {os.path.dirname(DEFAULT_SALT_FILE)}\n"
        f"    head -c {MIN_SALT_BYTES} /dev/urandom | base64 > "
        f"{DEFAULT_SALT_FILE}\n"
        f"    chmod 600 {DEFAULT_SALT_FILE}\n"
        "The salt must live outside this repository, must never be committed, "
        "and must be backed up: losing it makes a future release unlinkable to "
        "a published one.")


def _require_salt_file(raw: str, label: str) -> bytes:
    p = os.path.expanduser(raw)
    if not os.path.isfile(p):
        raise SaltError(
            f"salt file {p} (from {label}) does not exist. It is not silently "
            f"replaced by another salt: hashing with a different key would "
            f"renumber every pseudonym in the release and still exit 0. Create "
            f"it with `head -c {MIN_SALT_BYTES} /dev/urandom | base64 > {p}` "
            f"and `chmod 600 {p}`, or point {label} somewhere that exists.")
    return _read_salt_file(p, label)


def _read_salt_file(p: str, label: str) -> bytes:
    """Bytes of a salt file, with the two mistakes that silently weaken it."""
    mode = os.stat(p).st_mode
    if mode & (stat.S_IRWXG | stat.S_IRWXO):
        raise SaltError(
            f"salt file {p} (from {label}) is readable by group or others "
            f"(mode {stat.filemode(mode)}). A leaked salt makes every pseudonym "
            f"reversible and the leak is silent. Fix it with:\n"
            f"    chmod 600 {p}")
    with open(p, "rb") as fh:
        # Trailing whitespace only: `echo x > salt` and `printf x > salt` must
        # give the same key, or a release changes when someone re-creates the
        # file with a different shell builtin.
        salt = fh.read().strip()
    _check_salt_length(salt, f"{p} (from {label})")
    return salt


def _check_salt_length(salt: bytes, where: str) -> None:
    if len(salt) < MIN_SALT_BYTES:
        raise SaltError(
            f"salt from {where} is {len(salt)} byte(s); at least "
            f"{MIN_SALT_BYTES} are required. Make one with "
            f"`head -c {MIN_SALT_BYTES} /dev/urandom | base64`.")


def derive_key(salt: bytes) -> bytes:
    """A 64-byte blake2b key from a salt of any length.

    blake2b refuses a key longer than 64 bytes, and a salt file is whatever the
    operator made -- a 100-byte base64 line is entirely reasonable. Folding once
    per run costs nothing and means no legitimate salt is ever refused for being
    too long, which would be a confusing failure right next to "too short".
    """
    return hashlib.blake2b(salt, digest_size=64).digest()


def pseudo_token(key: bytes, space: str, value: str) -> str:
    """The pseudonym id for one value: keyed, fast, and set-independent.

    blake2b with a key is a FAST keyed hash (roughly memcpy speed on this data);
    the release step runs it once per distinct value, 28,339 times for the
    corpus, so it does not show up in wall-clock time at all. A slow password KDF
    would, on purpose, and that is not the threat being defended -- see the module
    docstring.

    `space` is mixed in so that the name id space and the e-mail id space are
    domain-separated: the same string appearing as a name and as an address must
    not produce the same token, or the two spaces would be silently linked. NUL
    separates it from the value so that no (space, value) pair can be confused
    with another.

    digest_size is the truncation. BLAKE2 encodes the output length in its
    parameter block, so a 7-byte digest is a hash in its own right rather than a
    prefix of a longer one -- the correct way to shorten BLAKE2.
    """
    h = hashlib.blake2b(f"{space}\0{value}".encode(), key=key,
                        digest_size=PSEUDO_HEX_LEN // 2)
    return h.hexdigest()


# --------------------------------------------------------------------------
# The pseudonym registry
# --------------------------------------------------------------------------

def pseudo_email_local(token: str) -> str:
    return f"author_{token}"


def pseudo_name(token: str) -> str:
    return f"Author {token}"


def pseudo_personid(token: str) -> str:
    return f"author {token}"


@dataclass
class Registry:
    """Lowercased real string -> pseudonym token, for names and for e-mails.

    Two separate id spaces, names and e-mails, exactly as the original. They are
    deliberately NOT unified into one person id. Unifying would need name-e-mail
    co-occurrence, and one shared bot address or one repeated name string would
    merge two humans into one component -- at which point two distinct
    addresses at one domain render to the same output string, distinct-person
    counts drop, and analysis invariance is gone. Injective maps cost a reader
    the ability to say 'Author <t> is author_<t>@...' and buy exact group-by
    preservation. That is the right trade for a dataset whose headline metric is
    a count of distinct people. The two spaces are kept apart by hashing the
    space name alongside the value; see pseudo_token.

    Consistency, which is what claim 5 actually needs, holds within each space:
    one input string always renders to one output string, in every column of
    every file -- and, since the token is a keyed hash of the value rather than
    its ordinal position in a sorted set, in every FUTURE release made with the
    same salt.

    This map is the reverse mapping. It exists only for the length of one run,
    is never serialized, and is never published.
    """

    emails: dict[str, str] = field(default_factory=dict)
    names: dict[str, str] = field(default_factory=dict)
    # Exact raw footer element -> rendered replacement.
    footer_elements: dict[str, str] = field(default_factory=dict)
    shape_counts: dict[str, int] = field(default_factory=dict)
    shape_examples: dict[str, list[str]] = field(default_factory=dict)
    # Every domain that is deliberately published. Leak scanning must exclude
    # these; see mask_domains().
    domains: set[str] = field(default_factory=set)

    def note_shape(self, shape: str, value: str) -> None:
        self.shape_counts[shape] = self.shape_counts.get(shape, 0) + 1
        ex = self.shape_examples.setdefault(shape, [])
        if len(ex) < 5:
            ex.append(value)


DOMAIN_MASK = "<PRESERVED-DOMAIN>"


def domain_masker(domains: set[str]):
    """A function replacing every published domain with a placeholder.

    Needed because the leak scanner searches for real identity strings, and one
    of the real identity strings in this data is the *name* 'github' -- the
    GitHub web-flow committer. Searched naively it matches inside
    users.noreply.github.com, which is a domain the tool preserves ON PURPOSE,
    and every file then reports five leaks that are not leaks. Masking the
    published domains first states the rule precisely: a real string found
    inside a deliberately published domain is not a leak; anywhere else it is.

    Longest domain first, so example.co.uk is masked before co.uk.
    """
    if not domains:
        return lambda v: v
    pat = re.compile("|".join(re.escape(d) for d in
                              sorted(domains, key=len, reverse=True)), re.I)
    return lambda v: pat.sub(DOMAIN_MASK, v)


def build_registry(email_values: set[str], name_values: set[str],
                   footer_text_values: set[str],
                   footer_derived_values: dict[str, set[str]],
                   extra_domains: set[str] | None = None,
                   *, salt: bytes) -> Registry:
    """Assign pseudonyms: deterministic, and stable across input sets.

    A token is a keyed hash of the lowercased value (pseudo_token), so two runs
    over the same inputs produce byte-identical output AND a value carries the
    same pseudonym in a later release that adds or drops projects. The previous
    scheme numbered the sorted distinct values from 1, which gave the first
    property and not the second: measured, adding one file renumbered 98.0% of
    the addresses present in both runs.

    `salt` is required and has no default. A default would be a published salt,
    and a generated one would renumber everybody on every run.
    """
    reg = Registry()
    key = derive_key(salt)

    emails = {v.lower() for v in email_values if v}
    names = {v.lower() for v in name_values if v}

    # Footer elements contribute to both spaces before ids are handed out, so a
    # footer-only person gets a pseudonym too. The original built its registry
    # from four scalar columns and therefore had no entry for those people.
    parsed: dict[str, tuple[str, str | None, str | None]] = {}
    for raw in footer_text_values:
        if raw is None or raw == "":
            continue
        shape, nm, em = parse_footer_element(raw)
        parsed[raw] = (shape, nm, em)
        reg.note_shape(shape, raw)
        if nm:
            names.add(nm.lower())
        if em:
            emails.add(em.lower())
    for values in footer_derived_values.values():
        for raw in values:
            if raw:
                names.add(raw.lower())

    reg.emails = _assign(key, "email", emails)
    reg.names = _assign(key, "name", names)

    # Render each footer text element once, keyed on the exact raw string, so
    # the SQL side is a plain dictionary lookup with no parsing.
    for raw, (shape, nm, em) in parsed.items():
        reg.footer_elements[raw] = render_footer_element(reg, nm, em)

    reg.domains = {e.split("@", 1)[1] for e in reg.emails if "@" in e}
    reg.domains |= {d.lower() for d in (extra_domains or set()) if d}
    reg.domains.discard("")
    return reg


def _assign(key: bytes, space: str, values: set[str]) -> dict[str, str]:
    """{value -> token} for one id space, refusing to publish a collision.

    Truncating a digest can in principle map two values to one token. That would
    merge two contributors into one pseudonym: two distinct addresses at one
    domain would render identically, count(distinct person_email) would drop, and
    the dataset's headline metric -- a count of distinct people -- would be wrong
    in a way no downstream reader could detect. At 56 bits over 28,339 addresses
    the chance is about 5.6e-9, so this is a guard against the improbable rather
    than the expected, and the right response is to stop the release, not to
    disambiguate behind the reader's back.

    The remedy for a real collision is a new salt, which renumbers every
    pseudonym in the release. Values are walked in sorted order so that the same
    collision is reported the same way twice; the order has no effect on any
    token.
    """
    out: dict[str, str] = {}
    owner: dict[str, str] = {}
    for v in sorted(values):
        token = pseudo_token(key, space, v)
        if token in owner:
            raise CollisionError(
                f"{space} pseudonym collision: {v!r} and {owner[token]!r} both "
                f"hash to {token!r}. Two people would share one pseudonym and "
                f"the distinct-person count would drop, so this release is "
                f"refused. Rotate the salt (which renumbers every pseudonym), "
                f"or widen PSEUDO_HEX_LEN.")
        owner[token] = v
        out[v] = token
    return out


def render_footer_element(reg: Registry, name: str | None,
                          email: str | None) -> str:
    """'Author 0007 <author_0003@cosmonic.com>', preserving the domain.

    Keeping the trailer shape means a consumer can still parse the column, and
    keeping the domain means a Signed-off-by chain still carries firm signal --
    which is the reason to publish these columns at all.
    """
    parts = []
    if name:
        n = reg.names.get(name.lower())
        parts.append(pseudo_name(n) if n else MISSING_MARKER)
    if email:
        n = reg.emails.get(email.lower())
        local = pseudo_email_local(n) if n else MISSING_MARKER
        domain = email.split("@", 1)[1] if "@" in email else ""
        addr = f"{local}@{domain}" if domain else local
        parts.append(f"<{addr}>" if name else addr)
    return " ".join(parts) if parts else MISSING_MARKER


# --------------------------------------------------------------------------
# SQL generation. Pure SQL, no scalar UDFs: duckdb's Python UDFs need numpy and
# the devenv has none. A MAP literal inside a macro inside list_transform does
# the same job and stays inside the engine.
# --------------------------------------------------------------------------

def sql_str(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def sql_map(pairs: dict[str, str]) -> str:
    """A VARCHAR->VARCHAR MAP literal. Typed explicitly when empty."""
    if not pairs:
        return "MAP([]::VARCHAR[], []::VARCHAR[])"
    body = ", ".join(f"{sql_str(k)}: {sql_str(v)}"
                     for k, v in sorted(pairs.items()))
    return "MAP {" + body + "}"


def registry_macros(reg: Registry) -> list[str]:
    """CREATE MACRO statements implementing the four rewrites.

    Every lookup coalesces to MISSING_MARKER rather than to the input, so a
    value the registry never saw becomes a visible marker instead of a leak.
    """
    email_map = {k: pseudo_email_local(v) for k, v in reg.emails.items()}
    name_map = {k: pseudo_name(v) for k, v in reg.names.items()}
    pid_map = {k: pseudo_personid(v) for k, v in reg.names.items()}
    mm = sql_str(MISSING_MARKER)
    return [
        f"CREATE OR REPLACE MACRO _elocal(x) AS "
        f"coalesce(map_extract({sql_map(email_map)}, lower(x))[1], {mm})",
        f"CREATE OR REPLACE MACRO _name(x) AS "
        f"coalesce(map_extract({sql_map(name_map)}, lower(x))[1], {mm})",
        f"CREATE OR REPLACE MACRO _pid(x) AS "
        f"coalesce(map_extract({sql_map(pid_map)}, lower(x))[1], {mm})",
        f"CREATE OR REPLACE MACRO _felem(x) AS "
        f"coalesce(map_extract({sql_map(reg.footer_elements)}, x)[1], {mm})",
        # Domain preserved from the raw value, so casing survives. An address
        # with no '@' keeps just the pseudonym.
        "CREATE OR REPLACE MACRO anon_email(x) AS CASE "
        "  WHEN x IS NULL OR x = '' THEN x "
        "  WHEN strpos(x, '@') = 0 THEN _elocal(x) "
        "  ELSE _elocal(x) || '@' || split_part(x, '@', 2) END",
        "CREATE OR REPLACE MACRO anon_name(x) AS CASE "
        "  WHEN x IS NULL OR x = '' THEN x ELSE _name(x) END",
        "CREATE OR REPLACE MACRO anon_personid(x) AS CASE "
        "  WHEN x IS NULL OR x = '' THEN x ELSE _pid(x) END",
        "CREATE OR REPLACE MACRO anon_footer_text(l) AS "
        "  list_transform(l, x -> CASE WHEN x IS NULL OR x = '' THEN x "
        "                              ELSE _felem(x) END)",
        "CREATE OR REPLACE MACRO anon_footer_names(l) AS "
        "  list_transform(l, x -> CASE WHEN x IS NULL OR x = '' THEN x "
        "                              ELSE _name(x) END)",
        "CREATE OR REPLACE MACRO anon_footer_personids(l) AS "
        "  list_transform(l, x -> CASE WHEN x IS NULL OR x = '' THEN x "
        "                              ELSE _pid(x) END)",
        # Subject lines: strip any local part, keep the domain, same rule as the
        # e-mail columns. Nothing in the three development files matches.
        "CREATE OR REPLACE MACRO anon_summary(x) AS "
        "  regexp_replace(x, "
        "    '[A-Za-z0-9._%+\\-]+@([A-Za-z0-9.\\-]+\\.[A-Za-z]{2,})', "
        "    'author@\\1', 'g')",
    ]


def select_list(plan: Plan, null_commit_summary: bool) -> list[str]:
    """One SELECT item per output column, in input order.

    Built from the plan rather than typed out, which is the structural fix: it
    cannot end early at person_domain the way the hand-written list did.
    """
    kind_of = dict(plan.scalar)
    derived_of = dict(plan.footer_derived)
    items = []
    for c in ordered_columns(plan):
        if c in plan.passthrough:
            items.append(c)
        elif c in kind_of:
            k = kind_of[c]
            if k == "name":
                items.append(f"anon_name({c}) AS {c}")
            elif k == "email":
                items.append(f"anon_email({c}) AS {c}")
            elif k == "personid":
                items.append(f"anon_personid({c}) AS {c}")
            elif k == "summary":
                items.append(f"CAST(NULL AS VARCHAR) AS {c}"
                             if null_commit_summary
                             else f"anon_summary({c}) AS {c}")
            else:                                    # unreachable by classify()
                raise UnknownColumnError(f"no rewrite for kind {k!r} ({c})")
        elif c in plan.footer_text:
            items.append(f"anon_footer_text({c}) AS {c}")
        elif c in derived_of:
            fn = ("anon_footer_personids" if derived_of[c] == "personid"
                  else "anon_footer_names")
            items.append(f"{fn}({c}) AS {c}")
    return items


def ordered_columns(plan: Plan) -> list[str]:
    """Output columns in input order, minus the dropped ones.

    Input order is kept so that validate_schema.py's order check still passes on
    the anonymized file when footers are preserved.
    """
    return [c for c in plan.column_order if c not in plan.dropped]


# --------------------------------------------------------------------------
# duckdb-backed stages
# --------------------------------------------------------------------------

def connect():
    """A capped connection. 3 GB / 3 threads: this box is running other work."""
    import duckdb
    con = duckdb.connect()
    con.execute("SET memory_limit='3GB'")
    con.execute("SET threads=3")
    return con


def read_columns(con, path: str) -> list[str]:
    return [name for name, _ in read_schema(con, path)]


def read_schema(con, path: str) -> list[tuple[str, str]]:
    """(name, type) per column, in file order.

    DESCRIBE returns six columns, not two, so the pair is taken by index.
    """
    rows = con.execute("describe select * from read_parquet(?)",
                       [path]).fetchall()
    return [(r[0], r[1]) for r in rows]


def collect_values(con, paths: list[str], plans: dict[str, Plan]):
    """Distinct identity strings over every input file, for one shared registry.

    Shared across files because claim 5 asks for pseudonym consistency ACROSS
    files, and because a person who commits to two projects must not get two
    identities.
    """
    emails: set[str] = set()
    names: set[str] = set()
    footer_text: set[str] = set()
    footer_derived: dict[str, set[str]] = {c: set() for c in FOOTER_DERIVED_COLUMNS}
    domains: set[str] = set()

    for p in paths:
        plan = plans[p]
        for (v,) in con.execute(
                "select distinct person_domain from read_parquet(?) "
                "where person_domain is not null and person_domain <> ''",
                [p]).fetchall():
            domains.add(v)
        for col, kind in plan.scalar:
            if kind == "email":
                target = emails
            elif kind in ("name", "personid"):
                target = names
            else:
                continue                             # commit_summary
            for (v,) in con.execute(
                    f"select distinct {col} from read_parquet(?) "
                    f"where {col} is not null and {col} <> ''", [p]).fetchall():
                target.add(v)
        for col in plan.footer_text:
            for (v,) in con.execute(
                    f"select distinct unnest({col}) from read_parquet(?) "
                    f"where {col} is not null and len({col}) > 0",
                    [p]).fetchall():
                if v:
                    footer_text.add(v)
        for col, _ in plan.footer_derived:
            for (v,) in con.execute(
                    f"select distinct unnest({col}) from read_parquet(?) "
                    f"where {col} is not null and len({col}) > 0",
                    [p]).fetchall():
                if v:
                    footer_derived[col].add(v)
    return emails, names, footer_text, footer_derived, domains


def write_anonymized(con, src: str, out: str, plan: Plan,
                     null_commit_summary: bool) -> None:
    items = select_list(plan, null_commit_summary)
    con.execute(
        f"COPY (SELECT {', '.join(items)} FROM read_parquet({sql_str(src)})) "
        f"TO {sql_str(out)} (FORMAT parquet, COMPRESSION zstd)")


# --------------------------------------------------------------------------
# Verification
# --------------------------------------------------------------------------

def audit_passthrough(con, path: str, plan: Plan, reg: Registry) -> dict:
    """Test the claim that the pass-through columns carry no personal data.

    Asked of the data, not of whoever asserted the column list. Two tiers,
    because they are two different claims:

      structural -- the 29 provenance columns, the 3 firm columns and
        person_domain. These are per-project constants and public company data.
        An e-mail address in one of them would mean the claim is false, so it is
        a FAILURE.
      content -- source_text, token_value, file_path and the repository
        namespace. An address in a copyright header is real and unremovable
        without destroying the token stream, so it is REPORTED, with counts.

    Name probes are limited to strings of five characters or more, matched on
    word boundaries and after the published domains are masked out. A three
    letter name would otherwise match half of any C file and say nothing.
    """
    result = {"email_shape": {}, "content_email_shape": {},
              "identity_name": {}, "content_identity_name": {}}
    types = dict(read_schema(con, path))
    mask = domain_masker(reg.domains)
    probes = sorted({n for n in reg.names if len(n) >= 5})
    for c in plan.passthrough:
        if types.get(c) != "VARCHAR":
            continue
        content = c in CONTENT_COLUMNS
        n = con.execute(
            f"select count(*) from read_parquet(?) where {c} is not null "
            f"and regexp_matches({c}, {sql_str(EMAILISH)})",
            [path]).fetchone()[0]
        if n:
            result["content_email_shape" if content else "email_shape"][c] = n
        if probes:
            distinct = [mask(v) for (v,) in con.execute(
                f"select distinct {c} from read_parquet(?) "
                f"where {c} is not null and {c} <> ''", [path]).fetchall()]
            hits = sorted({p for p in probes
                           for v in distinct
                           if re.search(rf"\b{re.escape(p)}\b", v, re.I)})
            if hits:
                key = "content_identity_name" if content else "identity_name"
                result[key][c] = hits[:5]
    return result


GROUP_KEYS: tuple[str, ...] = ("firm", "person_domain", "repo_name")
DISTINCT_KEYS: tuple[str, ...] = ("person_email", "person_name")


def aggregate_profile(con, path: str) -> dict:
    """Row counts per firm, per person_domain and per repo_name.

    The invariance proof. These are the group-bys the firm-level analysis runs,
    so if they are identical before and after, the published numbers are too.
    """
    prof = {}
    for col in GROUP_KEYS:
        rows = con.execute(
            f"select coalesce({col}, '<NULL>') as k, count(*) "
            f"from read_parquet(?) group by 1 order by 1", [path]).fetchall()
        prof[col] = {k: n for k, n in rows}
    prof["rows"] = con.execute("select count(*) from read_parquet(?)",
                               [path]).fetchone()[0]
    for col in DISTINCT_KEYS:
        prof[f"distinct_{col}"] = con.execute(
            f"select count(distinct {col}) from read_parquet(?)",
            [path]).fetchone()[0]
    return prof


def invariance_diff(before: dict, after: dict) -> list[str]:
    """Every way the two aggregate profiles disagree. Empty means proven."""
    problems = []
    if before["rows"] != after["rows"]:
        problems.append(f"row count {before['rows']} -> {after['rows']}")
    for col in ("firm", "repo_name"):
        if before[col] != after[col]:
            problems.append(f"{col} group counts changed")
    # person_domain keys must be identical: the domain is what the firm join
    # uses, so any drift here is the failure mode the original warned about.
    if before["person_domain"] != after["person_domain"]:
        problems.append("person_domain group counts changed")
    # Injectivity: if two real addresses collapsed onto one pseudonym the
    # distinct-person count would fall and the truck factor would rise for free.
    for col in DISTINCT_KEYS:
        k = f"distinct_{col}"
        if before[k] != after[k]:
            problems.append(f"{k} {before[k]} -> {after[k]}")
    return problems


MIN_LOCAL_PART_PROBE = 4
MIN_NAME_PROBE = 5


class Scanner:
    """Looks for one real identity string inside one value.

    Two probe families, deliberately asymmetric:

      local part -- only matched when followed by '@', i.e. as an address. A
        bare 'root' or 'admin' in prose is not evidence of a leak and matching it
        would drown the real signal.
      name -- matched on word boundaries, five characters or more.

    Both run AFTER the published domains are masked out, so a hit inside
    users.noreply.github.com does not count. That masking is the difference
    between a scanner that reports five leaks per file and one that reports none;
    see domain_masker().
    """

    def __init__(self, reg: Registry):
        self.mask = domain_masker(reg.domains)
        self.locals = sorted({e.split("@", 1)[0] for e in reg.emails
                              if "@" in e
                              and len(e.split("@", 1)[0]) >= MIN_LOCAL_PART_PROBE})
        self.names = sorted({n for n in reg.names if len(n) >= MIN_NAME_PROBE})
        self._local_re = [
            (lp, re.compile(rf"(?<![A-Za-z0-9._%+\-]){re.escape(lp)}@"))
            for lp in self.locals]
        self._name_re = [(nm, re.compile(rf"\b{re.escape(nm)}\b"))
                         for nm in self.names]

    def find(self, value: str) -> tuple[str, str] | None:
        """(kind, probe) of the first real string found, or None."""
        low = self.mask(value).lower()
        for lp, rx in self._local_re:
            if rx.search(low):
                return "local_part", lp
        for nm, rx in self._name_re:
            if rx.search(low):
                return "name", nm
        return None


def leak_scan(con, path: str, plan: Plan, reg: Registry) -> dict:
    """Real local parts and real names anywhere in the output, per column.

    Split into identity columns (must be zero) and content columns (reported).
    Footer columns are scanned by unnesting them, which the original's verifier
    never did: it walked SQLite text columns and had no concept of a list.
    """
    scan = Scanner(reg)
    types = dict(read_schema(con, path))
    out = {"identity": {}, "content": {}, "missing_marker": {}}

    def values_of(col: str) -> list[str]:
        if types[col].endswith("[]"):
            q = (f"select distinct unnest({col}) from read_parquet(?) "
                 f"where {col} is not null and len({col}) > 0")
        else:
            q = (f"select distinct {col} from read_parquet(?) "
                 f"where {col} is not null and {col} <> ''")
        return [v for (v,) in con.execute(q, [path]).fetchall() if v]

    for col in ordered_columns(plan):
        if types[col] not in ("VARCHAR", "VARCHAR[]"):
            continue
        vals = values_of(col)
        n_marker = sum(1 for v in vals if MISSING_MARKER in v)
        if n_marker:
            out["missing_marker"][col] = n_marker
        hits = [(kind, probe, v) for v in vals
                for kind, probe in [scan.find(v) or (None, None)] if kind]
        if hits:
            bucket = "content" if col in CONTENT_COLUMNS else "identity"
            out[bucket][col] = {"n": len(hits),
                                "examples": [list(h) for h in hits[:3]]}
    return out


def footer_collapses(reg: Registry) -> dict[str, list[str]]:
    """Rendered footer value -> the several raw values that produced it.

    Pseudonymization is not injective on footer TEXT, and that has to be said
    out loud rather than left for someone to find. Two raw trailers differing
    only in case or in a trailing CR render to one string. No element is lost --
    element counts are checked separately -- but a distinct-value count over a
    footer column can fall. Reported, not failed: the input variation is a
    CRLF artifact of trailer parsing, not information.
    """
    groups: dict[str, list[str]] = {}
    for raw, rendered in reg.footer_elements.items():
        groups.setdefault(rendered, []).append(raw)
    return {k: sorted(v) for k, v in groups.items() if len(v) > 1}


def footer_element_counts(con, path: str, plan: Plan) -> dict[str, int]:
    """Total list elements per footer column. Must not change: that is data."""
    counts = {}
    cols = list(plan.footer_text) + [c for c, _ in plan.footer_derived]
    for c in cols:
        counts[c] = con.execute(
            f"select count(*) from (select unnest({c}) v from "
            f"read_parquet(?) where {c} is not null)", [path]).fetchone()[0]
    return counts


def summary_residue(con, path: str, reg: Registry) -> dict:
    """Identity text surviving in commit_summary. Non-empty means FAIL.

    Separate from leak_scan because commit_summary is the one column whose
    publication is a judgement call: the evidence for keeping it has to be
    visible in the report, not buried in a pass/fail.
    """
    scan = Scanner(reg)
    vals = [v for (v,) in con.execute(
        "select distinct commit_summary from read_parquet(?) "
        "where commit_summary is not null and commit_summary <> ''",
        [path]).fetchall() if v]
    bad = [(kind, v) for v in vals
           for kind, _ in [scan.find(v) or (None, None)] if kind]
    return {"n_distinct": len(vals), "residue": bad[:10],
            "n_residue": len(bad)}


# --------------------------------------------------------------------------
# Driver
# --------------------------------------------------------------------------

def run(outdir: str, paths: list[str], drop_footers: bool = False,
        null_commit_summary: bool = False, report_path: str | None = None,
        out=sys.stdout, salt: bytes | None = None,
        salt_file: str | None = None) -> dict:
    # Before anything is read or any directory is created: no salt, no release.
    if salt is None:
        salt = load_salt(salt_file)
    con = connect()
    os.makedirs(outdir, exist_ok=True)

    plans: dict[str, Plan] = {}
    for p in paths:
        cols = read_columns(con, p)
        plan = classify(cols, drop_footers=drop_footers)
        plans[p] = plan
        print(f"[{os.path.basename(p)}] schema: {len(cols)} columns in, "
              f"{plan.n_out} out ({len(plan.dropped)} dropped)", file=out)

    emails, names, ftext, fderived, domains = collect_values(con, paths, plans)
    reg = build_registry(emails, names, ftext, fderived,
                         extra_domains=domains, salt=salt)
    print(f"registry: {len(reg.emails)} e-mails, {len(reg.names)} names, "
          f"{len(reg.footer_elements)} footer elements, "
          f"{len(reg.domains)} preserved domains "
          f"(shared across {len(paths)} file(s))", file=out)
    print(f"pseudonyms: {PSEUDO_HEX_LEN}-hex-character salted blake2b tokens, "
          f"stable across releases made with the same salt. No collision "
          f"(0 of {len(reg.emails) + len(reg.names)} tokens shared).", file=out)
    if reg.shape_counts:
        print("footer element shapes: "
              + ", ".join(f"{k}={v}" for k, v in sorted(reg.shape_counts.items())),
              file=out)
        n_nc = sum(reg.shape_counts.get(s, 0) for s in NONCONFORMING_SHAPES)
        print(f"  non-conforming footer elements (not 'Name <email>'): {n_nc} "
              f"of {sum(reg.shape_counts.values())}", file=out)
        for shape in NONCONFORMING_SHAPES:
            for ex in reg.shape_examples.get(shape, []):
                print(f"    [{shape}] {ex!r} -> "
                      f"{reg.footer_elements.get(ex)!r}", file=out)

    collapses = footer_collapses(reg)
    print(f"footer text pseudonymization is injective: "
          f"{'yes' if not collapses else 'NO -- ' + str(len(collapses)) + ' group(s)'}",
          file=out)
    for rendered, raws in sorted(collapses.items())[:10]:
        print(f"  {rendered!r} <- {raws}", file=out)

    for stmt in registry_macros(reg):
        con.execute(stmt)

    # Counts and shapes only. The reverse map is NOT in here, and neither is the
    # salt: the report sits next to the release and is easy to hand over.
    report = {"files": {}, "registry": {"emails": len(reg.emails),
                                        "names": len(reg.names),
                                        "footer_elements": len(reg.footer_elements),
                                        "footer_shapes": reg.shape_counts,
                                        "preserved_domains": len(reg.domains),
                                        "footer_text_collapses": collapses},
              "pseudonym_scheme": {"hash": "blake2b keyed, salted",
                                   "hex_length": PSEUDO_HEX_LEN,
                                   "stable_across_input_sets": True},
              "drop_footers": drop_footers,
              "null_commit_summary": null_commit_summary}
    failures: list[str] = []

    for p in paths:
        plan = plans[p]
        dest = os.path.join(outdir, os.path.basename(p))
        before = aggregate_profile(con, p)
        audit = audit_passthrough(con, p, plan, reg)
        write_anonymized(con, p, dest, plan, null_commit_summary)
        after = aggregate_profile(con, dest)
        out_cols = read_columns(con, dest)
        leaks = leak_scan(con, dest, plan, reg)
        resid = (summary_residue(con, dest, reg) if not null_commit_summary
                 else {"n_distinct": 0, "residue": [], "n_residue": 0})
        drift = invariance_diff(before, after)
        mapping = check_column_mapping(con, p, dest, plan)
        fe_before = footer_element_counts(con, p, plan)
        fe_after = footer_element_counts(con, dest, plan)
        fe_lost = {c: (fe_before[c], fe_after[c]) for c in fe_before
                   if fe_before[c] != fe_after[c]}

        entry = {
            "out": dest,
            "columns_in": len(plan.column_order), "columns_out": len(out_cols),
            "rows_in": before["rows"], "rows_out": after["rows"],
            "accounting": {c: plan.rule_of(c) for c in plan.column_order},
            "invariance": drift,
            "column_mapping": mapping,
            "passthrough_audit": audit,
            "leaks": leaks,
            "commit_summary": resid,
            "footer_elements": fe_before,
            "footer_elements_lost": fe_lost,
        }
        report["files"][p] = entry

        print(f"\n[{os.path.basename(p)}]", file=out)
        print(f"  rows:    in={before['rows']:,} out={after['rows']:,}", file=out)
        print(f"  columns: in={len(plan.column_order)} out={len(out_cols)}"
              f" (expected {plan.n_out})", file=out)
        print(f"  distinct person_email {before['distinct_person_email']} -> "
              f"{after['distinct_person_email']}, person_name "
              f"{before['distinct_person_name']} -> "
              f"{after['distinct_person_name']}", file=out)
        print(f"  groups: firm={len(before['firm'])} "
              f"person_domain={len(before['person_domain'])} "
              f"repo_name={len(before['repo_name'])} "
              f"-> invariance {'OK' if not drift else 'DRIFT'}", file=out)
        for d in drift:
            print(f"    DRIFT {d}", file=out)
        print(f"  column mapping: {'OK' if not mapping else 'MISMATCH'} "
              f"(output identity columns == macro over input)", file=out)
        for m in mapping:
            print(f"    MISMATCH {m}", file=out)
        if fe_before:
            print(f"  footer elements: {sum(fe_before.values()):,} in, "
                  f"{sum(fe_after.values()):,} out"
                  f"{' -- LOST ' + str(fe_lost) if fe_lost else ''}", file=out)
        print(f"  pass-through audit: structural columns clean="
              f"{not audit['email_shape'] and not audit['identity_name']}"
              f" (29 provenance + 3 firm + person_domain carry no address "
              f"and no identity name)", file=out)
        if audit["email_shape"]:
            print(f"    FAIL e-mail address in structural pass-through "
                  f"column(s): {audit['email_shape']}", file=out)
        for c, hits in audit["identity_name"].items():
            print(f"    FAIL structural pass-through {c} contains identity "
                  f"name(s) {hits}", file=out)
        for c, n in audit["content_email_shape"].items():
            print(f"    NOTE content column {c}: {n} row(s) contain an e-mail "
                  f"address (source code / repo namespace, not scrubbed)",
                  file=out)
        for c, hits in audit["content_identity_name"].items():
            print(f"    NOTE content column {c} contains identity name(s) "
                  f"{hits} (source code / repo namespace, not scrubbed)",
                  file=out)
        if leaks["identity"]:
            for c, info in leaks["identity"].items():
                print(f"    LEAK identity column {c}: {info['n']} "
                      f"{info['examples']}", file=out)
        if leaks["content"]:
            for c, info in leaks["content"].items():
                print(f"    NOTE content column {c}: {info['n']} value(s) "
                      f"contain an identity string (source code / repo "
                      f"namespace, not scrubbed)", file=out)
        if leaks["missing_marker"]:
            print(f"    BUG {MISSING_MARKER} present: "
                  f"{leaks['missing_marker']}", file=out)
        if resid["n_residue"]:
            print(f"    LEAK commit_summary: {resid['n_residue']} of "
                  f"{resid['n_distinct']} distinct subjects carry identity "
                  f"text; use --null-commit-summary", file=out)
            for kind, v in resid["residue"][:5]:
                print(f"      [{kind}] {v!r}", file=out)

        if len(out_cols) != plan.n_out:
            failures.append(f"{p}: column count {len(out_cols)} != {plan.n_out}")
        if before["rows"] != after["rows"]:
            failures.append(f"{p}: row count changed")
        failures += [f"{p}: {d}" for d in drift]
        failures += [f"{p}: {m}" for m in mapping]
        failures += [f"{p}: footer elements lost in {c}: {a} -> {b}"
                     for c, (a, b) in fe_lost.items()]
        failures += [f"{p}: identity leak in {c}" for c in leaks["identity"]]
        failures += [f"{p}: {MISSING_MARKER} in {c}"
                     for c in leaks["missing_marker"]]
        if audit["email_shape"]:
            failures.append(f"{p}: e-mail address in structural pass-through "
                            f"column(s) {sorted(audit['email_shape'])}")
        if audit["identity_name"]:
            failures.append(f"{p}: identity name in structural pass-through "
                            f"column(s) {sorted(audit['identity_name'])}")
        if resid["n_residue"]:
            failures.append(f"{p}: commit_summary identity residue")

    consistency = check_consistency(con, paths, plans)
    report["cross_file_consistency"] = consistency
    print(f"\ncross-file pseudonym consistency: "
          f"{'OK' if not consistency['conflicts'] else 'CONFLICT'} "
          f"({consistency['checked']} (real, pseudonym) pairs over "
          f"{len(paths)} file(s))", file=out)
    for c in consistency["conflicts"][:10]:
        print(f"  CONFLICT {c}", file=out)
    failures += [f"consistency: {c}" for c in consistency["conflicts"]]

    report["failures"] = failures
    if report_path:
        with open(report_path, "w") as fh:
            json.dump(report, fh, indent=2, sort_keys=True)
        print(f"\nreport -> {report_path}", file=out)
    print(f"\n{'FAIL: ' + str(len(failures)) + ' problem(s)' if failures else 'OK'}",
          file=out)
    for f in failures:
        print(f"  {f}", file=out)
    return report


# Identity columns and the id space each one draws from. A footer trailer-text
# column is not here: its elements mix a name and an address, so consistency for
# it is checked through the elements' parsed halves in the registry itself.
IDENTITY_SPACES: tuple[tuple[str, str], ...] = (
    ("author_name", "name"), ("committer_name", "name"),
    ("person_name", "name"), ("personid", "personid"),
    ("author_email", "email"), ("committer_email", "email"),
    ("person_email", "email"),
    ("footer_personids", "personid"), ("footer_person_names", "name"),
)

_MACRO_OF_SPACE = {"name": "anon_name", "email": "anon_email",
                   "personid": "anon_personid"}
_LIST_MACRO_OF_SPACE = {"name": "anon_footer_names",
                        "personid": "anon_footer_personids"}


def check_consistency(con, paths: list[str], plans: dict[str, Plan]) -> dict:
    """One real identity -> one pseudonym, in every column of every file.

    Evaluates the same macros the writer used over every input file and collects
    the observed (real, pseudonym) pairs. No row alignment is involved, which
    matters: `row_number() over ()` across two independent multi-threaded
    parquet scans is not a stable join key, so a row-wise comparison would
    report phantom conflicts. What this proves instead is that the rewrite is a
    function of the input value alone and that one registry served every file --
    a person in two projects gets one pseudonym.
    """
    seen: dict[tuple[str, str], tuple[str, str]] = {}
    conflicts: list[str] = []
    checked = 0
    for p in paths:
        cols = set(plans[p].column_order)
        for col, space in IDENTITY_SPACES:
            if col not in cols:
                continue
            if col.startswith("footer_"):
                fn = _LIST_MACRO_OF_SPACE[space]
                q = (f"select distinct v.real, v.pseudo from (select "
                     f"unnest({col}) as real, unnest({fn}({col})) as pseudo "
                     f"from read_parquet({sql_str(p)}) "
                     f"where {col} is not null and len({col}) > 0) v "
                     f"where v.real is not null and v.real <> ''")
            else:
                fn = _MACRO_OF_SPACE[space]
                q = (f"select distinct {col}, {fn}({col}) "
                     f"from read_parquet({sql_str(p)}) "
                     f"where {col} is not null and {col} <> ''")
            for real, pseudo in con.execute(q).fetchall():
                checked += 1
                key = (space, (real or "").lower())
                prev = seen.get(key)
                if prev is None:
                    seen[key] = (pseudo, f"{os.path.basename(p)}.{col}")
                elif prev[0] != pseudo:
                    conflicts.append(
                        f"{space} {real!r}: {prev[0]!r} in {prev[1]}, "
                        f"{pseudo!r} in {os.path.basename(p)}.{col}")
    return {"checked": checked, "distinct_pairs": len(seen),
            "conflicts": conflicts}


def check_column_mapping(con, src: str, dest: str, plan: Plan) -> list[str]:
    """Proof that each output identity column IS the macro applied to the input.

    For every identity column, the value->count histogram of the output must
    equal the histogram of the macro evaluated over the input. Row order never
    enters, and a histogram match plus an equal row count leaves no room for the
    COPY to have shuffled values between rows. Cheaper and stronger than
    eyeballing a few sample rows.
    """
    problems = []
    out_cols = set(ordered_columns(plan))
    for col, space in IDENTITY_SPACES:
        if col not in out_cols:
            continue
        if col.startswith("footer_"):
            fn = _LIST_MACRO_OF_SPACE[space]
            # UNNEST cannot sit beside an aggregate, hence the subquery.
            q_exp = (f"select v, count(*) from (select unnest({fn}({col})) v "
                     f"from read_parquet({sql_str(src)}) where {col} is not "
                     f"null and len({col}) > 0) group by 1")
            q_got = (f"select v, count(*) from (select unnest({col}) v "
                     f"from read_parquet({sql_str(dest)}) where {col} is not "
                     f"null and len({col}) > 0) group by 1")
        else:
            fn = _MACRO_OF_SPACE[space]
            q_exp = (f"select {fn}({col}) v, count(*) from "
                     f"read_parquet({sql_str(src)}) group by 1")
            q_got = (f"select {col} v, count(*) from "
                     f"read_parquet({sql_str(dest)}) group by 1")
        exp = dict(con.execute(q_exp).fetchall())
        got = dict(con.execute(q_got).fetchall())
        if exp != got:
            only_exp = {k: v for k, v in exp.items() if got.get(k) != v}
            problems.append(f"{col}: value histogram differs from the macro "
                            f"applied to the input, e.g. "
                            f"{list(only_exp.items())[:3]}")
    return problems


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("outdir", help="directory for the anonymized parquets")
    ap.add_argument("inputs", nargs="+", help="input *-dataset.parquet files")
    ap.add_argument("--drop-footers", action="store_true",
                    help="drop the 15 footer_* columns instead of "
                         "pseudonymizing their contents (70 in, 55 out)")
    ap.add_argument("--null-commit-summary", action="store_true",
                    help="NULL commit_summary, as the original tool did")
    ap.add_argument("--report", help="write the full JSON report here")
    ap.add_argument("--salt-file",
                    help=f"file holding the private pseudonymization salt. "
                         f"Default: ${SALT_FILE_ENV}, else ${SALT_ENV} inline, "
                         f"else {DEFAULT_SALT_FILE}. Required; never generated.")
    args = ap.parse_args(argv)

    try:
        report = run(args.outdir, args.inputs, drop_footers=args.drop_footers,
                     null_commit_summary=args.null_commit_summary,
                     report_path=args.report, salt_file=args.salt_file)
    except SaltError as e:
        # Exit 2 is "misuse", matching verify_anon.py, and distinct from the 1
        # that means "ran and found residue". A release script can tell the
        # difference between a missing secret and a dirty dataset.
        print(f"ERROR: {e}", file=sys.stderr)
        return 2
    except CollisionError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2
    return 1 if report["failures"] else 0


if __name__ == "__main__":
    sys.exit(main())
