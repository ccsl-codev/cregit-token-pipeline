#!/usr/bin/env python3
"""Pseudonymize the released ctp schema 2 Parquet files in one pass per file.

This is the release tool of the dataset (release plan step 7, x1k row X5). It
replaces the release path of anonymize_parquet.py. anonymize_parquet.py stays as
the library of rules (column classes, footer shapes, salt file) and as the
record of the measurements in its docstring.

WHAT CHANGED, AND WHY

1. One registry TABLE, not SQL MAP literals.
   `registry` reads every input file in ONE invocation (dataset v1 and the
   extension together) and writes one private Parquet table:

       space | identity | token | pseudonym | shape | anchor | local

   space is 'email', 'name', 'footer' or 'domain'. identity is the lower-case
   address or name (for 'footer': the raw trailer element). The collision check
   runs on this table, so it covers all files. The leak scan reads its probes
   from this table, so a person of file A is also looked for in file B.
   The table holds real identities. It is as private as the salt: mode 0600,
   never in the release, never in git. Its sha256 (over the sorted rows) goes
   into the footer of each written file.

2. The pseudonym is a keyed hash that DuckDB computes per row.
   A join with the registry does not keep the row order of a Parquet file
   (measured: DuckDB 1.5.5 reorders the rows of a hash join, also with one
   thread). A constant list or MAP lookup copies the list for each vector of
   2,048 rows (measured: 0.8 ms per vector for 44,000 entries). Thus the write
   computes

       token = substr(sha256(K2 || sha256(K1 || space || ':' || lower(value))), 1, 14)

   in SQL. K1 and K2 are the two halves of blake2b(salt, 64 bytes), as hex.
   The nested hash stops a length-extension attack, and the result has
   PSEUDO_HEX_LEN = 14 hex characters (56 bits), as before. DuckDB runs a
   scalar function once per dictionary entry of a Parquet column, so the cost
   on the scalar columns is near zero (measured: 4.4 M rows/s per thread with
   two hashed columns, the same as a plain copy).
   Before each write, the tool compares the SQL pseudonym of each distinct
   value of the file with the registry. A value that is not in the registry,
   or a registry from a different salt, stops the write. Thus the registry is
   the reference, and the per-row SQL is only the fast path to it.
   The keys go into DuckDB as variables (SET VARIABLE), not as SQL text, and
   the tool removes them from every error message.

3. A linear leak scan.
   The old scan tried each probe regex on each distinct value: O(values x
   probes), about 62 days for the full dataset. The new scan:
     * e-mail local parts: DuckDB extracts the run of address characters before
       each at sign and joins it with the registry local parts (a hash join).
     * names: DuckDB splits each value into words and joins the words with the
       ANCHOR word of each registry name (the word of the name that the fewest
       names share). Only the joined pairs go to Python, which applies the old
       rule: the whole name between word boundaries, after the published
       domains are masked.
   The probe limits are the old ones (local part 4 characters or more, name 5
   characters or more).

4. Reports without identities.
   The report and the standard output give counts, column names, file names
   and registry tokens (salted hashes). They never give a value, a name, an
   address or a domain. The report writer also removes each at sign.

5. --memory-limit, --threads (per job) and --jobs (parallel files), as in
   build_v0.py and run_v0.py. --temp-dir sets the spill folder.

6. One fused write (decision D1).
   write_public() takes a source relation (default: the input file) and writes
   the pseudonymized file in one COPY, with the v1 encoding (ZSTD 19,
   ROW_GROUP_SIZE 1000000, PARQUET_VERSION V2). It keeps the footer metadata
   and adds pseudonymization_version and pseudonymization_registry_sha256.
   build_v0.py can give its organisation SELECT as the source relation, so the
   organisation columns and the pseudonyms come from one read and one ZSTD 19
   encode. See write_public() for the call.

7. The decided column rules of docs/ANON-OPEN-QUESTIONS.md and of the release
   plan:
     * person_domain is written as lower(trim(person_domain)) (the key of
       build_links.py; JOIN_PROTOTYPE.md recommendation 4). Measured on
       dataset v1: this changes 2 of 70,737 distinct values.
     * commit_summary: the rule is still OPEN (question 2). The tool keeps the
       two documented modes, --commit-summary scrub (the default: the local
       part of each address becomes 'author') and --commit-summary null, and
       adds --commit-summary pseudonym (each address gets the pseudonym of the
       e-mail columns). verify_anon.py refuses the scrub form, so the release
       needs pseudonym or null (question 2b). The residue gate stays strict.
       --summary-residue report is option 4 of question 2 (an address residue
       fails, a name residue is counted); it is off until the user selects it.
     * person_domain values with an at sign: OPEN (question 5). The default
       ('keep') writes them, and the structural gate fails the file.
       --person-domain-at after-last-at or empty are the two other rules.

8. Speed. One file per job and one DuckDB thread per job (--jobs N
   --threads 1) is the fast form: an ordered ZSTD 19 write with more threads
   was slower on this host (2 M rows: 9.5 s with 1 thread, 23.2 s with 2,
   20.8 s with 4). `bench` measures the rates on synthetic data.

Usage (each step reads the salt from a file; the tool never prints it):

  pseudonymize.py registry --salt-file S --registry R.parquet INPUT...
  pseudonymize.py write    --salt-file S --registry R.parquet --outdir D INPUT...
  pseudonymize.py run      --salt-file S --registry R.parquet --outdir D INPUT...
  pseudonymize.py scan     --registry R.parquet FILE...
  pseudonymize.py bench    --rows 2000000 --threads 1

INPUT is a Parquet file or a folder of Parquet files; --inputs-from FILE reads
one path per line. Exit status: 0 good, 1 a check failed, 2 wrong use, salt
error or registry collision.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import shutil
import sys
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context
from pathlib import Path

import anonymize_parquet as A
import validate_schema as vs

AT = chr(64)
SCHEME_VERSION = "2"
FOOTER_VERSION_KEY = "pseudonymization_version"
FOOTER_REGISTRY_KEY = "pseudonymization_registry_sha256"
ORG_FOOTER_KEYS = ("ctp_schema_version", "organisations_version",
                   "organisations_mapping_sha256")
ENCODING = ("FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 19, "
            "ROW_GROUP_SIZE 1000000, PARQUET_VERSION V2")
ORGANISATION_TYPES = (("firm_based_on_namespace", "VARCHAR"),
                      ("firm_based_on_domain", "VARCHAR"),
                      ("project_enterprise_driven", "BOOLEAN"),
                      ("project_enterprise_basis", "VARCHAR"))
# The release contract: the 67 kept cregit columns, then the 4 organisation
# columns. It is equal to SCHEMA_2 of release-staging/tools/fill_release.py.
SCHEMA_2 = tuple((c, t) for c, t in vs.EXPECTED_COLUMNS
                 if c not in A.FIRM_COLUMNS) + ORGANISATION_TYPES

SPACE_EMAIL, SPACE_NAME, SPACE_FOOTER, SPACE_DOMAIN = (
    "email", "name", "footer", "domain")
SALT_CHECK_VALUE = "ctp-anon-salt-check"
REGISTRY_COLUMNS = ("space", "identity", "token", "pseudonym", "shape",
                    "anchor", "local")
SUMMARY_MODES = ("scrub", "pseudonym", "null")
RESIDUE_MODES = ("fail", "report")
# person_domain values with an at sign (measured on dataset v1: 199 distinct
# values in 176 files, 70 of them address-shaped in 89 files). 'keep' writes
# them as they are, and the structural gate then fails those files. The other
# two rules are offered for the user's decision.
DOMAIN_AT_MODES = ("keep", "after-last-at", "empty")
MAX_TOKENS_IN_REPORT = 5

# Python's str '\s' as an RE2 class, so that DuckDB parses a trailer element
# as anonymize_parquet.parse_footer_element() does. '\S' is its negation.
_WS_CHARS = r"\t\n\x{0B}\x{0C}\r\x{1C}-\x{1F}\x{85}\p{Z}"
WS = f"[{_WS_CHARS}]"
NWS = f"[^{_WS_CHARS}]"
# The three footer shapes of anonymize_parquet, in RE2 syntax.
RE_NAME_EMAIL = (f"^{WS}*(.*?){WS}*<{WS}*([^<>{_WS_CHARS}]+{AT}[^<>{_WS_CHARS}]+)"
                 f"{WS}*>{WS}*$")
RE_BARE_EMAIL = (f"^{WS}*([^<>{_WS_CHARS}{AT}]+{AT}[^<>{_WS_CHARS}{AT}]+"
                 rf"\.[A-Za-z0-9\-]+){WS}*$")
RE_NAME_AT_EMAIL = (f"^{WS}*({NWS}.*?){WS}+at{WS}+([^<>{_WS_CHARS}{AT}]+{AT}"
                    f"[^<>{_WS_CHARS}]+){WS}*$")
# The word split of the leak scan: Python's '\w' is letters, digits and '_'.
RE_NON_WORD = r"[^\p{L}\p{N}_]+"
# The characters of an address local part (the old scanner's look-behind set).
LOCAL_CHARS = r"a-z0-9._%+\-"
RE_LOCAL_BEFORE_AT = f"([{LOCAL_CHARS}]+){AT}"
RE_SUMMARY_ADDRESS = rf"[A-Za-z0-9._%+\-]+{AT}([A-Za-z0-9.\-]+\.[A-Za-z]{{2,}})"


class RegistryError(Exception):
    """The registry is missing, from another salt, or has a collision."""


class WriteError(Exception):
    """A check of the fused write failed. The output is not kept."""


# --------------------------------------------------------------------------
# Keys and the pseudonym function
# --------------------------------------------------------------------------

def derive_keys(salt: bytes) -> tuple[str, str]:
    """The two hex keys of the SQL pseudonym, from the salt.

    They come from anonymize_parquet.derive_key (blake2b, 64 bytes). The salt
    itself does not go into DuckDB.
    """
    k = A.derive_key(salt)
    return k[:32].hex(), k[32:].hex()


def token_py(keys: tuple[str, str], space: str, key: str) -> str:
    """The Python form of the SQL macro anon_t. `key` is already lower case."""
    k1, k2 = keys
    inner = hashlib.sha256(f"{k1}{space}:{key}".encode()).hexdigest()
    return hashlib.sha256(f"{k2}{inner}".encode()).hexdigest()[:A.PSEUDO_HEX_LEN]


def redact(text: str, keys: tuple[str, str] | None) -> str:
    """Remove the keys from a message before the tool shows it."""
    if keys:
        for k in keys:
            text = text.replace(k, "<key>")
    return text


OWN_ERRORS = (WriteError, RegistryError, A.SaltError, A.UnknownColumnError,
              A.MissingColumnError, ValueError)


def safe_error(e: BaseException, keys: tuple[str, str] | None) -> str:
    """The text of an error for a report or the terminal.

    The tool's own errors carry counts, column names and tokens only, so their
    text is kept (without the keys). Another error (DuckDB, the OS) can quote a
    data value, so only its class and a hash of its text are shown. Run the
    file alone in Python to see the full text.
    """
    text = redact(str(e), keys)
    if isinstance(e, OWN_ERRORS) and type(e).__module__ != "_duckdb":
        return f"{type(e).__name__}: {text}"
    digest = hashlib.sha256(text.encode()).hexdigest()[:12]
    return f"{type(e).__name__}: text not shown (sha256 {digest})"


def sql_str(s: str) -> str:
    return A.sql_str(s)


def q(col: str) -> str:
    return '"' + col.replace('"', '""') + '"'


MACROS = (
    # The token: a nested keyed sha256. The argument is already lower case.
    "CREATE OR REPLACE TEMP MACRO anon_t(sp, k) AS "
    "substr(sha256(getvariable('anon_k2') || sha256(getvariable('anon_k1') "
    f"|| sp || ':' || k)), 1, {A.PSEUDO_HEX_LEN})",
    f"CREATE OR REPLACE TEMP MACRO anon_local(x) AS 'author_' || anon_t('{SPACE_EMAIL}', lower(x))",
    f"CREATE OR REPLACE TEMP MACRO anon_name_of(x) AS 'Author ' || anon_t('{SPACE_NAME}', lower(x))",
    f"CREATE OR REPLACE TEMP MACRO anon_pid_of(x) AS 'author ' || anon_t('{SPACE_NAME}', lower(x))",
    # The e-mail columns: the old anon_email rule (domain = second part).
    "CREATE OR REPLACE TEMP MACRO anon_email(x) AS CASE WHEN x IS NULL OR x = '' THEN x "
    f"WHEN strpos(x, '{AT}') = 0 THEN anon_local(x) "
    f"ELSE anon_local(x) || '{AT}' || split_part(x, '{AT}', 2) END",
    "CREATE OR REPLACE TEMP MACRO anon_name(x) AS CASE WHEN x IS NULL OR x = '' THEN x "
    "ELSE anon_name_of(x) END",
    "CREATE OR REPLACE TEMP MACRO anon_personid(x) AS CASE WHEN x IS NULL OR x = '' THEN x "
    "ELSE anon_pid_of(x) END",
    # A trailer address: render_footer_element (domain = all after the first at).
    "CREATE OR REPLACE TEMP MACRO anon_addr(m) AS anon_local(m) || CASE "
    f"WHEN strpos(m, '{AT}') > 0 AND substr(m, strpos(m, '{AT}') + 1) <> '' "
    f"THEN '{AT}' || substr(m, strpos(m, '{AT}') + 1) ELSE '' END",
    "CREATE OR REPLACE TEMP MACRO anon_render(n, m) AS CASE WHEN n = '' THEN anon_addr(m) "
    "ELSE anon_name_of(n) || ' <' || anon_addr(m) || '>' END",
    # One trailer element. The one-element list binds the parse result to a
    # lambda parameter, so that each regex runs once per element (a macro
    # argument is copied, and DuckDB would run the regex again for each copy).
    # The address group of the name_email shape is never empty on a match.
    "CREATE OR REPLACE TEMP MACRO anon_footer_elem(e) AS CASE WHEN e IS NULL OR e = '' THEN e "
    f"ELSE list_transform([regexp_extract(e, {sql_str(RE_NAME_EMAIL)}, ['n', 'm'])], s -> "
    "CASE WHEN s.m <> '' THEN anon_render(s.n, s.m) "
    f"ELSE list_transform([regexp_extract(e, {sql_str(RE_BARE_EMAIL)}, 1)], b -> "
    "CASE WHEN b <> '' THEN anon_addr(b) ELSE anon_name_of(e) END)[1] END)[1] END",
    "CREATE OR REPLACE TEMP MACRO anon_footer_shape(e) AS CASE "
    f"WHEN regexp_matches(e, {sql_str(RE_NAME_EMAIL)}) THEN '{A.SHAPE_NAME_EMAIL}' "
    f"WHEN regexp_matches(e, {sql_str(RE_BARE_EMAIL)}) THEN '{A.SHAPE_BARE_EMAIL}' "
    f"WHEN regexp_matches(e, {sql_str(RE_NAME_AT_EMAIL)}) THEN '{A.SHAPE_NAME_AT_EMAIL}' "
    f"ELSE '{A.SHAPE_OTHER}' END",
    "CREATE OR REPLACE TEMP MACRO anon_footer_text(l) AS list_transform(l, e -> anon_footer_elem(e))",
    "CREATE OR REPLACE TEMP MACRO anon_footer_names(l) AS list_transform(l, e -> anon_name(e))",
    "CREATE OR REPLACE TEMP MACRO anon_footer_personids(l) AS list_transform(l, e -> anon_personid(e))",
    # commit_summary, mode scrub: the old rule. The local part of each
    # address becomes the word 'author'. verify_anon.py refuses this form.
    "CREATE OR REPLACE TEMP MACRO anon_summary(x) AS regexp_replace(x, "
    f"{sql_str(RE_SUMMARY_ADDRESS)}, 'author{AT}\\1', 'g')",
    # commit_summary, mode pseudonym: each address gets the pseudonym of the
    # e-mail columns (author_<token>, the same domain). The text between the
    # addresses stays. The lambdas bind the split and the matches once.
    "CREATE OR REPLACE TEMP MACRO anon_summary_pseudonym(x) AS CASE "
    f"WHEN x IS NULL OR NOT regexp_matches(x, {sql_str(RE_SUMMARY_ADDRESS)}) THEN x "
    f"ELSE list_transform([[regexp_split_to_array(x, {sql_str(RE_SUMMARY_ADDRESS)}), "
    f"regexp_extract_all(x, {sql_str(RE_SUMMARY_ADDRESS)}, 0)]], pm -> array_to_string("
    "list_transform(range(1, len(pm[1]) + 1), k -> pm[1][k] || CASE WHEN k <= len(pm[2]) "
    "THEN anon_email(pm[2][k]) ELSE '' END), ''))[1] END",
)


def connect(memory_limit: str = "4GB", threads: int = 2,
            temp_dir: str | None = None):
    import duckdb
    cfg = {"memory_limit": memory_limit, "threads": threads,
           "preserve_insertion_order": True}
    if temp_dir:
        cfg["temp_directory"] = temp_dir
    con = duckdb.connect(config=cfg)
    con.execute("SET parquet_metadata_cache = true")
    con.execute("SET enable_progress_bar = false")
    return con


def install(con, keys: tuple[str, str] | None) -> None:
    """Put the keys into DuckDB variables and create the macros."""
    if keys is not None:
        con.execute("SET VARIABLE anon_k1 = ?", [keys[0]])
        con.execute("SET VARIABLE anon_k2 = ?", [keys[1]])
    for m in MACROS:
        con.execute(m)


def salt_check(con) -> str:
    """A salted hash of a constant. It shows which salt made a registry and
    tells nothing about the salt."""
    return con.execute("SELECT anon_t('check', ?)", [SALT_CHECK_VALUE]).fetchone()[0]


# --------------------------------------------------------------------------
# Columns
# --------------------------------------------------------------------------

def describe(con, sql: str, params=None) -> list[tuple[str, str]]:
    """(column, type) of a relation. A relation with parameters cannot go
    into DESCRIBE, so it is run with LIMIT 0."""
    if params:
        cur = con.execute(f"SELECT * FROM ({sql}) LIMIT 0", params)
        return [(d[0], str(d[1])) for d in cur.description]
    return [(r[0], r[1]) for r in con.execute(f"DESCRIBE {sql}").fetchall()]


def file_schema(con, path: str) -> list[tuple[str, str]]:
    return describe(con, f"SELECT * FROM read_parquet({sql_str(path)})")


def person_domain_expr(src: str, domain_at: str = "keep") -> str:
    """The person_domain rule: lower(trim(value)), and the rule for a value
    with an at sign."""
    if domain_at not in DOMAIN_AT_MODES:
        raise ValueError(f"domain_at must be one of {DOMAIN_AT_MODES}")
    norm = f"lower(trim({src}))"
    if domain_at == "keep":
        return norm
    other = (f"regexp_extract({norm}, '[^{AT}]*$')" if domain_at == "after-last-at" else "''")
    return f"CASE WHEN strpos({src}, '{AT}') > 0 THEN {other} ELSE {norm} END"


def output_expr(plan: A.Plan, col: str, alias: str, summary_mode: str,
                domain_at: str = "keep") -> str | None:
    """The SELECT item of one output column, or None for a dropped column."""
    rule = plan.rule_of(col)
    src = f"{alias}.{q(col)}"
    if rule == "drop":
        return None
    if rule == "pass":
        if col == "person_domain":
            return f"{person_domain_expr(src, domain_at)} AS {q(col)}"
        return f"{src} AS {q(col)}"
    kind = rule.split(":", 1)[1]
    fn = {"email": "anon_email", "name": "anon_name", "personid": "anon_personid",
          "footer_text": "anon_footer_text", "footer_name": "anon_footer_names",
          "footer_personid": "anon_footer_personids"}.get(kind)
    if kind == "summary":
        if summary_mode == "null":
            return f"CAST(NULL AS VARCHAR) AS {q(col)}"
        if summary_mode == "pseudonym":
            return f"anon_summary_pseudonym({src}) AS {q(col)}"
        return f"anon_summary({src}) AS {q(col)}"
    if fn is None:                                    # pragma: no cover - guard
        raise ValueError(f"no rule for {col}: {rule}")
    return f"{fn}({src}) AS {q(col)}"


def output_columns(plan: A.Plan) -> list[str]:
    return [c for c in plan.column_order if c not in plan.dropped]


def identity_columns(plan: A.Plan) -> list[str]:
    """The columns that the write changes (not the person_domain rule)."""
    return ([c for c, _ in plan.scalar] + list(plan.footer_text)
            + [c for c, _ in plan.footer_derived])


def _value_sources(plan: A.Plan) -> dict[str, list[tuple[str, bool]]]:
    """For each registry space: (column, is_list) pairs that feed it."""
    out: dict[str, list[tuple[str, bool]]] = {SPACE_EMAIL: [], SPACE_NAME: [],
                                              SPACE_FOOTER: []}
    for c, kind in plan.scalar:
        if kind == "email":
            out[SPACE_EMAIL].append((c, False))
        elif kind in ("name", "personid"):
            out[SPACE_NAME].append((c, False))
    for c in plan.footer_text:
        out[SPACE_FOOTER].append((c, True))
    for c, _ in plan.footer_derived:
        out[SPACE_NAME].append((c, True))
    return out


def _values_sql(rel: str, col: str, is_list: bool) -> str:
    v = f"unnest({q(col)})" if is_list else q(col)
    return f"SELECT {v} AS v FROM {rel}"


# --------------------------------------------------------------------------
# The registry
# --------------------------------------------------------------------------

def gather_inputs(items: list[str], inputs_from: str | None = None) -> list[str]:
    paths: list[str] = []
    if inputs_from:
        with open(inputs_from) as fh:
            items = list(items) + [ln.strip() for ln in fh if ln.strip()]
    for it in items:
        p = Path(it)
        if p.is_dir():
            paths += sorted(str(x) for x in p.glob("*.parquet"))
        else:
            paths.append(str(p))
    seen: set[str] = set()
    out = []
    for p in paths:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def classify_all(con, paths: list[str], drop_footers: bool) -> dict[str, A.Plan]:
    return {p: A.classify([c for c, _ in file_schema(con, p)], drop_footers=drop_footers)
            for p in paths}


def build_registry(paths: list[str], keys: tuple[str, str], registry_out: str, *,
                   memory_limit: str = "8GB", threads: int = 4,
                   temp_dir: str | None = None, drop_footers: bool = False,
                   out=sys.stdout) -> dict:
    """Read all inputs once and write the one registry table.

    Returns a summary with counts only. Raises RegistryError on a collision.
    """
    if not paths:
        raise RegistryError("no input files")
    con = connect(memory_limit, threads, temp_dir)
    try:
        install(con, keys)
        plans = classify_all(con, paths, drop_footers)
        sources: dict[str, set[tuple[str, bool]]] = {
            SPACE_EMAIL: set(), SPACE_NAME: set(), SPACE_FOOTER: set()}
        for plan in plans.values():
            for space, cols in _value_sources(plan).items():
                sources[space].update(cols)
        rel = f"read_parquet({_sql_list(paths)}, union_by_name = true)"
        con.execute("CREATE TEMP TABLE anon_raw (space VARCHAR, v VARCHAR)")
        for space, cols in sources.items():
            for col, is_list in sorted(cols):
                key = "v" if space == SPACE_FOOTER else "lower(v)"
                con.execute(f"INSERT INTO anon_raw SELECT DISTINCT '{space}', {key} FROM "
                            f"({_values_sql(rel, col, is_list)}) WHERE v IS NOT NULL AND v <> ''")
        # Footer elements: the parsed name and address enter the two spaces.
        con.execute(
            "CREATE TEMP TABLE anon_fe AS SELECT e, anon_footer_shape(e) AS shape, "
            f"CASE WHEN regexp_matches(e, {sql_str(RE_NAME_EMAIL)}) THEN "
            f"nullif(regexp_extract(e, {sql_str(RE_NAME_EMAIL)}, 1), '') "
            f"WHEN regexp_matches(e, {sql_str(RE_BARE_EMAIL)}) THEN NULL ELSE e END AS fname, "
            f"CASE WHEN regexp_matches(e, {sql_str(RE_NAME_EMAIL)}) THEN "
            f"regexp_extract(e, {sql_str(RE_NAME_EMAIL)}, 2) "
            f"WHEN regexp_matches(e, {sql_str(RE_BARE_EMAIL)}) THEN "
            f"regexp_extract(e, {sql_str(RE_BARE_EMAIL)}, 1) END AS femail "
            f"FROM (SELECT DISTINCT v AS e FROM anon_raw WHERE space = '{SPACE_FOOTER}')")
        con.execute(f"INSERT INTO anon_raw SELECT DISTINCT '{SPACE_EMAIL}', lower(femail) "
                    "FROM anon_fe WHERE femail IS NOT NULL AND femail <> ''")
        con.execute(f"INSERT INTO anon_raw SELECT DISTINCT '{SPACE_NAME}', lower(fname) "
                    "FROM anon_fe WHERE fname IS NOT NULL AND fname <> ''")
        # Published domains: masked before the name scan, as in the old tool.
        has_domain = any("person_domain" in p.column_order for p in plans.values())
        if has_domain:
            for expr in ("lower(person_domain)", "lower(trim(person_domain))"):
                con.execute(f"INSERT INTO anon_raw SELECT DISTINCT '{SPACE_DOMAIN}', {expr} "
                            f"FROM {rel} WHERE person_domain IS NOT NULL")
        con.execute(f"INSERT INTO anon_raw SELECT DISTINCT '{SPACE_DOMAIN}', "
                    f"substr(v, strpos(v, '{AT}') + 1) FROM anon_raw "
                    f"WHERE space = '{SPACE_EMAIL}' AND strpos(v, '{AT}') > 0")
        con.execute(_REGISTRY_SQL)
        _add_anchors(con)
        collisions = con.execute(
            "SELECT space, token, count(*) FROM anon_reg WHERE token IS NOT NULL "
            "GROUP BY ALL HAVING count(*) > 1 ORDER BY ALL").fetchall()
        if collisions:
            toks = ", ".join(f"{s}:{t}" for s, t, _ in collisions[:MAX_TOKENS_IN_REPORT])
            raise RegistryError(
                f"{len(collisions)} pseudonym collision(s) ({toks}). Two identities "
                "would share one pseudonym, so the registry is refused. Use a new "
                "salt (this changes every pseudonym), or widen PSEUDO_HEX_LEN.")
        digest = registry_digest(con, "anon_reg")
        counts = dict(con.execute("SELECT space, count(*) FROM anon_reg GROUP BY 1").fetchall())
        shapes = dict(con.execute(
            f"SELECT shape, count(*) FROM anon_reg WHERE space = '{SPACE_FOOTER}' "
            "GROUP BY 1").fetchall())
        collapses = con.execute(
            f"SELECT count(*) FROM (SELECT pseudonym FROM anon_reg WHERE space = '{SPACE_FOOTER}' "
            "GROUP BY 1 HAVING count(*) > 1)").fetchone()[0]
        check = salt_check(con)
        kv = {"pseudonymization_version": SCHEME_VERSION, "registry_sha256": digest,
              "salt_check": check, "files": str(len(paths))}
        for s in (SPACE_EMAIL, SPACE_NAME, SPACE_FOOTER, SPACE_DOMAIN):
            kv[f"n_{s}"] = str(counts.get(s, 0))
        tmp = registry_out + ".tmp"
        old = os.umask(0o077)
        try:
            con.execute(f"COPY (SELECT {', '.join(REGISTRY_COLUMNS)} FROM anon_reg "
                        f"ORDER BY space, identity) TO {sql_str(tmp)} (FORMAT PARQUET, "
                        f"COMPRESSION ZSTD, KV_METADATA {_kv_sql(kv)})")
        finally:
            os.umask(old)
        os.chmod(tmp, 0o600)
        os.replace(tmp, registry_out)
    except RegistryError:
        raise
    except Exception as e:
        raise RegistryError(safe_error(e, keys)) from None
    finally:
        con.close()
    summary = {"files": len(paths), "registry_sha256": digest, "salt_check": check,
               "identities": {s: counts.get(s, 0) for s in
                              (SPACE_EMAIL, SPACE_NAME, SPACE_FOOTER, SPACE_DOMAIN)},
               "footer_shapes": shapes, "footer_collapses": collapses, "collisions": 0}
    print(f"registry: {len(paths)} file(s); {summary['identities']}; "
          f"footer shapes {shapes}; collapses {collapses}; sha256 {digest[:16]}; "
          f"salt check {check}", file=out)
    return summary


_REGISTRY_SQL = f"""
CREATE TEMP TABLE anon_reg AS
SELECT '{SPACE_EMAIL}' AS space, v AS identity, anon_t('{SPACE_EMAIL}', v) AS token,
       'author_' || anon_t('{SPACE_EMAIL}', v) AS pseudonym, NULL::VARCHAR AS shape,
       NULL::VARCHAR AS anchor,
       CASE WHEN length(split_part(v, '{AT}', 1)) >= {A.MIN_LOCAL_PART_PROBE}
            THEN split_part(v, '{AT}', 1) END AS local
FROM (SELECT DISTINCT v FROM anon_raw WHERE space = '{SPACE_EMAIL}')
UNION ALL
SELECT '{SPACE_NAME}', v, anon_t('{SPACE_NAME}', v), 'Author ' || anon_t('{SPACE_NAME}', v),
       NULL, NULL, NULL
FROM (SELECT DISTINCT v FROM anon_raw WHERE space = '{SPACE_NAME}')
UNION ALL
SELECT '{SPACE_FOOTER}', e, NULL, anon_footer_elem(e), shape, NULL, NULL FROM anon_fe
UNION ALL
SELECT '{SPACE_DOMAIN}', v, NULL, NULL, NULL, NULL, NULL
FROM (SELECT DISTINCT v FROM anon_raw WHERE space = '{SPACE_DOMAIN}' AND v <> ''
      AND strpos(v, '{AT}') = 0)
"""


def _add_anchors(con) -> None:
    """Give each name probe its anchor: the word of the name with the fewest
    names that share it (then the longest, then the first)."""
    con.execute(f"""
        CREATE TEMP TABLE anon_nw AS
        SELECT DISTINCT identity, w FROM (
            SELECT identity, unnest(string_split_regex(identity, {sql_str(RE_NON_WORD)})) AS w
            FROM anon_reg WHERE space = '{SPACE_NAME}' AND length(identity) >= {A.MIN_NAME_PROBE})
        WHERE w <> ''""")
    con.execute("""
        CREATE TEMP TABLE anon_anchor AS
        SELECT nw.identity, first(nw.w ORDER BY df.n, length(nw.w) DESC, nw.w) AS anchor
        FROM anon_nw nw JOIN (SELECT w, count(*) AS n FROM anon_nw GROUP BY w) df USING (w)
        GROUP BY nw.identity""")
    # A name of five characters or more with no word gets the empty anchor:
    # the scan then uses a substring test for it.
    con.execute(f"""
        UPDATE anon_reg SET anchor = coalesce(
            (SELECT a.anchor FROM anon_anchor a WHERE a.identity = anon_reg.identity), '')
        WHERE space = '{SPACE_NAME}' AND length(identity) >= {A.MIN_NAME_PROBE}""")


def registry_digest(con, table_sql: str) -> str:
    """sha256 over the sorted (space, identity, pseudonym) rows, one per line."""
    h = hashlib.sha256()
    cur = con.execute(f"SELECT space, identity, coalesce(pseudonym, '') FROM {table_sql} "
                      "ORDER BY space, identity")
    while True:
        rows = cur.fetchmany(100_000)
        if not rows:
            break
        for s, i, p in rows:
            h.update(f"{s}\t{i}\t{p}\n".encode())
    return h.hexdigest()


def registry_meta(con, registry: str) -> dict[str, str]:
    if not os.path.isfile(registry):
        raise RegistryError(f"no registry file at {registry}")
    return dict(con.execute("SELECT decode(key), decode(value) FROM parquet_kv_metadata(?)",
                            [registry]).fetchall())


def verify_registry_file(con, registry: str) -> dict[str, str]:
    """Check the registry file against its own footer, and return the footer."""
    meta = registry_meta(con, registry)
    got = registry_digest(con, f"read_parquet({sql_str(registry)})")
    if meta.get("registry_sha256") != got:
        raise RegistryError("the registry file does not match its own sha256")
    if meta.get("pseudonymization_version") != SCHEME_VERSION:
        raise RegistryError(f"registry scheme {meta.get('pseudonymization_version')} "
                            f"is not {SCHEME_VERSION}")
    return meta


def _sql_list(items: list[str]) -> str:
    return "[" + ", ".join(sql_str(i) for i in items) + "]"


def _kv_sql(kv: dict[str, str]) -> str:
    return "{" + ", ".join(f"{q(k)}: {sql_str(str(v))}" for k, v in kv.items()) + "}"


# --------------------------------------------------------------------------
# The fused write
# --------------------------------------------------------------------------

def registry_agreement(con, src: str, plan: A.Plan, registry: str) -> dict[str, int]:
    """Compare the SQL pseudonym of each distinct value of `src` with the
    registry. Returns counts: values, missing (not in the registry) and
    mismatch (the registry has another pseudonym: another salt)."""
    rel = f"read_parquet({sql_str(src)})"
    parts = []
    for space, cols in _value_sources(plan).items():
        for col, is_list in cols:
            if space == SPACE_FOOTER:
                parts.append(f"SELECT '{space}' AS space, v AS identity, anon_footer_elem(v) AS p "
                             f"FROM ({_values_sql(rel, col, is_list)}) WHERE v IS NOT NULL AND v <> ''")
            else:
                pre = "author_" if space == SPACE_EMAIL else "Author "
                parts.append(f"SELECT '{space}' AS space, lower(v) AS identity, "
                             f"'{pre}' || anon_t('{space}', lower(v)) AS p "
                             f"FROM ({_values_sql(rel, col, is_list)}) WHERE v IS NOT NULL AND v <> ''")
    if not parts:
        return {"values": 0, "missing": 0, "mismatch": 0}
    sql = (f"WITH k AS (SELECT DISTINCT * FROM ({' UNION ALL '.join(parts)})) "
           "SELECT count(*), count(*) FILTER (WHERE r.identity IS NULL), "
           "count(*) FILTER (WHERE r.identity IS NOT NULL AND r.pseudonym IS DISTINCT FROM k.p) "
           f"FROM k LEFT JOIN (SELECT space, identity, pseudonym FROM read_parquet({sql_str(registry)}) "
           f"WHERE space <> '{SPACE_DOMAIN}') r ON r.space = k.space AND r.identity = k.identity")
    n, missing, mismatch = con.execute(sql).fetchone()
    return {"values": n, "missing": missing, "mismatch": mismatch}


def order_hash(con, path: str, exprs: list[str]) -> tuple[int, int]:
    """Row count and an order-sensitive hash of the given expressions."""
    if not exprs:
        return con.execute(f"SELECT count(*), 0 FROM read_parquet({sql_str(path)})").fetchone()
    cols = ", ".join(exprs)
    return con.execute(
        f"SELECT count(*), coalesce(sum(hash(file_row_number, {cols})::HUGEINT), 0) "
        f"FROM read_parquet({sql_str(path)}, file_row_number = true)").fetchone()


def file_kv(con, path: str) -> dict[str, str]:
    return dict(con.execute("SELECT decode(key), decode(value) FROM parquet_kv_metadata(?)",
                            [path]).fetchall())


def write_public(con, src: str, dst: str, *, registry: str, registry_sha256: str,
                 source_sql: str | None = None, source_params: dict | None = None,
                 kv: dict[str, str] | None = None, summary_mode: str = "scrub",
                 drop_footers: bool = False, expect_schema_2: bool = True,
                 domain_at: str = "keep",
                 source_schema: list[tuple[str, str]] | None = None) -> dict:
    """Write the pseudonymized file `dst` from `src` in one COPY.

    con: a DuckDB connection with install() done (the keys are set).
    source_sql: the relation to read, in the row order of `src`. Default:
        read_parquet(src). The organisation write gives its own SELECT here,
        for example build_v0.py:

            sel = "SELECT t.*, <org columns> FROM read_parquet($src) t POSITIONAL JOIN (...) n"
            P.install(con, keys)
            P.write_public(con, str(src), str(tmp), registry=reg, registry_sha256=sha,
                           source_sql=sel, source_params=params, kv=kv)

        The identity columns must come unchanged from `src`.
    kv: the footer metadata. Default: the footer metadata of `src`.
    source_schema: the (column, type) list of source_sql. Give it when a
        LIMIT 0 run of source_sql is not cheap (build_v0.py knows its columns).

    The function writes `dst`, then checks it: columns (SCHEMA_2 if
    expect_schema_2), row count, an order hash of every column that passes
    unchanged, and the footer. It does not delete `dst` on a failure; the
    caller does (run_file() writes to a temporary name).
    """
    if summary_mode not in SUMMARY_MODES:
        raise ValueError(f"summary_mode must be one of {SUMMARY_MODES}")
    params = dict(source_params or {})
    if source_sql is None:
        source_sql = f"SELECT * FROM read_parquet({sql_str(src)})"
    src_cols = [c for c, _ in file_schema(con, src)]
    rel_schema = (list(source_schema) if source_schema is not None
                  else describe(con, source_sql, params or None))
    plan = A.classify([c for c, _ in rel_schema], drop_footers=drop_footers)
    missing_identity = [c for c in identity_columns(plan) if c not in src_cols]
    if missing_identity:
        raise WriteError(f"identity columns not in the input file: {missing_identity}")
    base_kv = file_kv(con, src) if kv is None else dict(kv)
    if FOOTER_VERSION_KEY in base_kv or FOOTER_VERSION_KEY in file_kv(con, src):
        raise WriteError("the input is already pseudonymized")
    agree = registry_agreement(con, src, plan, registry)
    if agree["missing"]:
        raise WriteError(f"{agree['missing']} of {agree['values']} identity values are not "
                         "in the registry (build the registry over all files)")
    if agree["mismatch"]:
        raise WriteError(f"{agree['mismatch']} of {agree['values']} pseudonyms differ from "
                         "the registry (another salt or another registry)")
    items = [e for e in (output_expr(plan, c, "s", summary_mode, domain_at)
                         for c in plan.column_order)
             if e is not None]
    out_kv = dict(base_kv)
    out_kv[FOOTER_VERSION_KEY] = SCHEME_VERSION
    out_kv[FOOTER_REGISTRY_KEY] = registry_sha256
    t0 = time.time()
    con.execute(f"COPY (SELECT {', '.join(items)} FROM ({source_sql}) s) TO {sql_str(dst)} "
                f"({ENCODING}, KV_METADATA {_kv_sql(out_kv)})", params or None)
    t_write = time.time() - t0
    # Checks of the written file.
    problems: list[str] = []
    types = dict(rel_schema)
    want = [(c, types[c]) for c in output_columns(plan)]
    got = file_schema(con, dst)
    if got != want:
        problems.append(f"columns differ from the input rules ({len(got)} vs {len(want)})")
    if expect_schema_2 and got != list(SCHEMA_2):
        problems.append(f"not ctp schema 2 ({len(got)} columns)")
    fk = file_kv(con, dst)
    for k, v in out_kv.items():
        if fk.get(k) != v:
            problems.append(f"footer key {k} is not kept")
    if expect_schema_2:
        for k in ORG_FOOTER_KEYS:
            if k not in fk:
                problems.append(f"footer has no {k}")
    same = [c for c in plan.passthrough if c in src_cols]
    src_exprs = [person_domain_expr(q(c), domain_at) if c == "person_domain" else q(c)
                 for c in same]
    a = order_hash(con, src, src_exprs)
    b = order_hash(con, dst, [q(c) for c in same])
    if a[0] != b[0]:
        problems.append(f"row count changed: {a[0]} -> {b[0]}")
    elif a[1] != b[1]:
        problems.append(f"the {len(same)} unchanged columns differ in value or order")
    return {"rows": b[0], "columns": len(got), "identity_values": agree["values"],
            "seconds_write": round(t_write, 3), "problems": problems, "plan": plan}


# --------------------------------------------------------------------------
# The leak scan
# --------------------------------------------------------------------------

_DOMAINS_CACHE: dict[str, frozenset[str]] = {}
_ODD_CACHE: dict[str, dict[str, str]] = {}


def _domains(con, registry: str) -> frozenset[str]:
    if registry not in _DOMAINS_CACHE:
        _DOMAINS_CACHE[registry] = frozenset(r[0] for r in con.execute(
            f"SELECT identity FROM read_parquet(?) WHERE space = '{SPACE_DOMAIN}'",
            [registry]).fetchall())
    return _DOMAINS_CACHE[registry]


def _odd_locals(con, registry: str) -> dict[str, str]:
    """Registry local parts with a character outside the address set. The SQL
    join cannot see them, so Python checks them (they are few)."""
    if registry not in _ODD_CACHE:
        _ODD_CACHE[registry] = dict(con.execute(
            f"SELECT local, min(token) FROM read_parquet(?) WHERE space = '{SPACE_EMAIL}' "
            f"AND local IS NOT NULL AND NOT regexp_full_match(local, '[{LOCAL_CHARS}]+') "
            "GROUP BY local", [registry]).fetchall())
    return _ODD_CACHE[registry]


_CHUNK_SPLIT = re.compile(r"[^\w.\-]+")


def mask_domains(value: str, domains: frozenset[str]) -> str:
    """Mask each published domain in `value` (lower case), as the old scanner.

    A chunk is a run of word characters, dots and hyphens. Each dot suffix of
    a chunk that is a published domain is masked with the old masker.
    """
    found = set()
    for chunk in _CHUNK_SPLIT.split(value):
        chunk = chunk.strip(".-")
        if not chunk:
            continue
        labels = chunk.split(".")
        for i in range(len(labels)):
            s = ".".join(labels[i:])
            if s in domains:
                found.add(s)
                break
    return A.domain_masker(found)(value) if found else value


def name_hit(value: str, name: str, domains: frozenset[str]) -> bool:
    """The old rule: the whole name between word boundaries, after masking."""
    return re.search(rf"\b{re.escape(name)}\b", mask_domains(value, domains), re.I) is not None


def odd_local_hits(value: str, odd: dict[str, str]) -> set[str]:
    """Tokens of odd local parts that stand before an at sign in `value`,
    with no address character just before them (the old look-behind)."""
    hits = set()
    if not odd:
        return hits
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._%+-")
    longest = max(len(k) for k in odd)
    i = value.find(AT)
    while i >= 0:
        for j in range(max(0, i - longest), i):
            if (j == 0 or value[j - 1] not in allowed) and value[j:i] in odd:
                hits.add(odd[value[j:i]])
        i = value.find(AT, i + 1)
    return hits


def leak_scan(con, path: str, registry: str, *, plan: A.Plan | None = None,
              summary_residue: str = "fail", content: bool = True) -> dict:
    """Scan every string column of `path` for registry identities.

    Returns counts and registry tokens only. 'failures' lists the columns that
    fail the release; 'reported' lists content columns (source code, project
    namespace) where an identity is real text of the project.
    """
    schema = file_schema(con, path)
    if plan is None:
        plan = A.classify([c for c, _ in schema])
    ident = set(identity_columns(plan))
    rel = f"read_parquet({sql_str(path)})"
    con.execute("CREATE OR REPLACE TEMP TABLE anon_vals (col VARCHAR, v VARCHAR)")
    for c, t in schema:
        if t not in ("VARCHAR", "VARCHAR[]") or (not content and c in A.CONTENT_COLUMNS):
            continue
        con.execute(f"INSERT INTO anon_vals SELECT DISTINCT '{c}', lower(v) FROM "
                    f"({_values_sql(rel, c, t == 'VARCHAR[]')}) WHERE v IS NOT NULL AND v <> ''")
    reg = f"read_parquet({sql_str(registry)})"
    found: dict[str, dict] = {}

    def note(col: str, kind: str, value: str, token: str) -> None:
        d = found.setdefault(col, {"name": set(), "local": set(), "name_tokens": set(),
                                   "local_tokens": set()})
        d[kind].add(value)
        d[f"{kind}_tokens"].add(token)

    for col, v, tok in con.execute(
            f"SELECT x.col, x.v, r.token FROM (SELECT col, v, unnest(regexp_extract_all("
            f"v, {sql_str(RE_LOCAL_BEFORE_AT)}, 1)) AS lp FROM anon_vals WHERE strpos(v, '{AT}') > 0) x "
            f"JOIN (SELECT local, token FROM {reg} WHERE space = '{SPACE_EMAIL}' "
            "AND local IS NOT NULL) r ON r.local = x.lp").fetchall():
        note(col, "local", v, tok)
    odd = _odd_locals(con, registry)
    if odd:
        for col, v in con.execute(f"SELECT col, v FROM anon_vals WHERE strpos(v, '{AT}') > 0").fetchall():
            for tok in odd_local_hits(v, odd):
                note(col, "local", v, tok)
    domains = _domains(con, registry)
    cand = con.execute(
        f"SELECT x.col, x.v, r.identity, r.token FROM (SELECT DISTINCT col, v, unnest("
        f"string_split_regex(v, {sql_str(RE_NON_WORD)})) AS w FROM anon_vals) x "
        f"JOIN (SELECT identity, token, anchor FROM {reg} WHERE space = '{SPACE_NAME}' "
        "AND anchor IS NOT NULL AND anchor <> '') r ON r.anchor = x.w "
        "UNION ALL SELECT a.col, a.v, r.identity, r.token FROM anon_vals a JOIN "
        f"(SELECT identity, token FROM {reg} WHERE space = '{SPACE_NAME}' AND anchor = '') r "
        "ON contains(a.v, r.identity)").fetchall()
    n_candidates = len(cand)
    for col, v, name, tok in cand:
        if name_hit(v, name, domains):
            note(col, "name", v, tok)
    email_shape = dict(con.execute(
        f"SELECT col, count(*) FROM anon_vals WHERE regexp_matches(v, {sql_str(A.EMAILISH)}) "
        "GROUP BY col").fetchall())
    marker = dict(con.execute(
        "SELECT col, count(*) FROM anon_vals WHERE contains(v, ?) GROUP BY col",
        [A.MISSING_MARKER.lower()]).fetchall())
    con.execute("DROP TABLE anon_vals")

    failures, reported, columns = [], [], {}
    for col in sorted(set(found) | set(email_shape) | set(marker)):
        d = found.get(col, {"name": set(), "local": set(), "name_tokens": set(),
                            "local_tokens": set()})
        content = col in A.CONTENT_COLUMNS
        kind = "identity" if col in ident else ("content" if content else "structural")
        entry = {"class": kind, "name_values": len(d["name"]), "local_values": len(d["local"]),
                 "email_shaped_values": email_shape.get(col, 0),
                 "missing_marker_values": marker.get(col, 0),
                 "name_tokens": sorted(d["name_tokens"])[:MAX_TOKENS_IN_REPORT],
                 "local_tokens": sorted(d["local_tokens"])[:MAX_TOKENS_IN_REPORT]}
        columns[col] = entry
        if entry["missing_marker_values"]:
            failures.append(f"{col}: {entry['missing_marker_values']} value(s) with the missing marker")
        if kind == "content":
            if entry["name_values"] or entry["local_values"] or entry["email_shaped_values"]:
                reported.append(col)
            continue
        if kind == "structural" and entry["email_shaped_values"]:
            failures.append(f"{col}: {entry['email_shaped_values']} address-shaped value(s) "
                            "in a structural column")
        if entry["local_values"]:
            failures.append(f"{col}: {entry['local_values']} value(s) with a real local part")
        if entry["name_values"]:
            if col == "commit_summary" and summary_residue == "report":
                reported.append(col)
            else:
                failures.append(f"{col}: {entry['name_values']} value(s) with a real name")
    return {"columns": columns, "failures": failures, "reported": reported,
            "name_candidates": n_candidates}


# --------------------------------------------------------------------------
# One file, many files
# --------------------------------------------------------------------------

def run_file(job: dict) -> dict:
    """Write and check one file. Used by the process pool; returns counts only."""
    src, outdir = job["src"], job["outdir"]
    name = os.path.basename(src)
    dst = os.path.join(outdir, name)
    tmp = os.path.join(outdir, f".{name}.anon-tmp")
    spill = tempfile.mkdtemp(prefix=".anon-spill-", dir=job.get("temp_dir") or outdir)
    keys = job["keys"]
    entry = {"file": name, "status": "fail", "problems": []}
    t0 = time.time()
    con = None
    try:
        con = connect(job["memory_limit"], job["threads"], spill)
        install(con, keys)
        res = write_public(con, src, tmp, registry=job["registry"],
                           registry_sha256=job["registry_sha256"],
                           summary_mode=job["summary_mode"], drop_footers=job["drop_footers"],
                           expect_schema_2=job["expect_schema_2"], domain_at=job["domain_at"])
        entry.update({k: res[k] for k in ("rows", "columns", "identity_values", "seconds_write")})
        entry["problems"] += res["problems"]
        if job["scan"]:
            t1 = time.time()
            scan = leak_scan(con, tmp, job["registry"], plan=None,
                             summary_residue=job["summary_residue"],
                             content=job["content_scan"])
            entry["seconds_scan"] = round(time.time() - t1, 3)
            entry["leaks"] = scan["columns"]
            entry["reported"] = scan["reported"]
            entry["problems"] += scan["failures"]
        if not entry["problems"]:
            os.replace(tmp, dst)
            entry["status"] = "ok"
    except Exception as e:                    # the report holds a safe text only
        entry["problems"].append(safe_error(e, keys))
    finally:
        if con is not None:
            con.close()
        if os.path.exists(tmp):
            os.unlink(tmp)
        shutil.rmtree(spill, ignore_errors=True)
    entry["seconds"] = round(time.time() - t0, 3)
    return entry


def run_write(paths: list[str], outdir: str, *, keys: tuple[str, str], registry: str,
              jobs: int = 1, threads: int = 1, memory_limit: str = "4GB",
              temp_dir: str | None = None, summary_mode: str = "scrub",
              summary_residue: str = "fail", scan: bool = True, drop_footers: bool = False,
              expect_schema_2: bool = True, domain_at: str = "keep",
              content_scan: bool = True, out=sys.stdout) -> dict:
    os.makedirs(outdir, exist_ok=True)
    con = connect("1GB", 1)
    try:
        install(con, keys)
        meta = verify_registry_file(con, registry)
        if meta.get("salt_check") != salt_check(con):
            raise RegistryError("the registry was made with another salt")
    finally:
        con.close()
    names = [os.path.basename(p) for p in paths]
    if len(set(names)) != len(names):
        raise RegistryError("two inputs have the same file name")
    base = {"outdir": outdir, "keys": keys, "registry": registry,
            "registry_sha256": meta["registry_sha256"], "threads": threads,
            "memory_limit": memory_limit, "temp_dir": temp_dir, "summary_mode": summary_mode,
            "summary_residue": summary_residue, "scan": scan, "drop_footers": drop_footers,
            "expect_schema_2": expect_schema_2, "domain_at": domain_at,
            "content_scan": content_scan}
    jobs_list = [dict(base, src=p) for p in paths]
    entries = []
    if jobs <= 1:
        it = map(run_file, jobs_list)
        for e in it:
            entries.append(e)
            _print_entry(e, out)
    else:
        with ProcessPoolExecutor(max_workers=jobs, mp_context=get_context("spawn")) as ex:
            for e in ex.map(run_file, jobs_list):
                entries.append(e)
                _print_entry(e, out)
    failures = [f"{e['file']}: {p}" for e in entries for p in e["problems"]]
    rows = sum(e.get("rows", 0) for e in entries)
    secs = sum(e.get("seconds", 0) for e in entries)
    return {"files": entries, "failures": failures, "registry_sha256": meta["registry_sha256"],
            "salt_check": meta["salt_check"], "rows": rows,
            "rows_per_second_per_thread": round(rows / secs / threads, 1) if secs else None,
            "options": {"jobs": jobs, "threads": threads, "memory_limit": memory_limit,
                        "summary_mode": summary_mode, "summary_residue": summary_residue,
                        "scan": scan, "content_scan": content_scan,
                        "drop_footers": drop_footers, "person_domain_at": domain_at}}


def _print_entry(e: dict, out) -> None:
    extra = "" if e["status"] == "ok" else f" ({len(e['problems'])} problem(s))"
    print(f"{e['status']:4} {e['file']} rows={e.get('rows', '-')} s={e.get('seconds')}{extra}",
          file=out)
    for p in e["problems"][:5]:
        print(f"     {strip_at(p)}", file=out)


def strip_at(obj):
    """Remove each at sign from every string of a report object."""
    if isinstance(obj, str):
        return obj.replace(AT, "")
    if isinstance(obj, dict):
        return {strip_at(k): strip_at(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [strip_at(v) for v in (sorted(obj) if isinstance(obj, set) else obj)]
    return obj


def write_report(path: str, report: dict) -> None:
    clean = strip_at(report)
    with open(path, "w") as fh:
        json.dump(clean, fh, indent=2, sort_keys=True, default=str)
        fh.write("\n")


# --------------------------------------------------------------------------
# Synthetic data and the throughput measure
# --------------------------------------------------------------------------

def synthetic_select(rows: int, *, people: int = 2000, commits: int = 20000,
                     schema2: bool = True, seed_text: str = "bench",
                     rows_per_commit: int = 37) -> str:
    """A SELECT of `rows` synthetic token rows with the contract types.

    The people are 'P<n> Person' at '<n>.example' domains. No value is person
    data. About one commit in three has a Signed-off-by trailer, one in ten a
    Co-authored-by trailer, one in 97 an address in its summary.
    """
    def person(e: str) -> dict:
        return {
            "name": f"'P' || ({e})::VARCHAR || ' Person'",
            "email": f"'p' || ({e})::VARCHAR || '{AT}corp' || (({e}) % 50)::VARCHAR || '.example'",
        }
    a = person("c % " + str(people))
    m = person("(c * 7) % " + str(people))
    so = person("(c * 13) % " + str(people))
    ca = person("(c * 17) % " + str(people))
    vals = {
        "repo_name": f"'{seed_text}-project'", "clone_url": f"'https://example.org/{seed_text}.git'",
        "file_path": "'src/f' || (i // 5000)::VARCHAR || '.c'",
        "token_index": "i", "source_line": "i // 10", "source_col": "i % 80",
        "source_text": "'tok' || (hash(i) % 50000)::VARCHAR",
        "token_type": "'name'", "token_value": "'tok' || (hash(i) % 50000)::VARCHAR",
        "is_structural": "i % 2",
        "cregit_commit_sha": "md5(c::VARCHAR)", "original_commit_sha": "md5((c + 1)::VARCHAR)",
        "author_name": a["name"], "author_email": a["email"],
        "author_date": "'2020-01-01T00:00:00'", "committer_name": m["name"],
        "committer_email": m["email"], "committer_date": "'2020-01-02T00:00:00'",
        "commit_summary": ("'fix: change ' || c::VARCHAR || CASE WHEN c % 97 = 0 THEN ' see ' || "
                           + a["email"] + " ELSE '' END"),
        "personid": a["name"], "person_name": a["name"], "person_email": a["email"],
        "person_domain": f"'corp' || ((c % {people}) % 50)::VARCHAR || '.example'",
        "repo_tag": "'v1'",
        "footer_signed_off_by": f"CASE WHEN c % 3 = 0 THEN [{so['name']} || ' <' || {so['email']} || '>'] ELSE [] END",
        "footer_co_authored_by": f"CASE WHEN c % 10 = 0 THEN [{ca['name']} || ' <' || {ca['email']} || '>'] ELSE [] END",
        "footer_personids": f"CASE WHEN c % 3 = 0 THEN [{so['name']}] ELSE [] END",
        "footer_person_names": f"CASE WHEN c % 3 = 0 THEN [{so['name']}] ELSE [] END",
        "firm_raw": "'org' || ((c % 50))::VARCHAR", "firm": "'org' || ((c % 50))::VARCHAR",
        "firm_source": "'domain'",
        "firm_based_on_namespace": "'independent'",
        "firm_based_on_domain": "'org' || ((c % 50))::VARCHAR",
        "project_enterprise_driven": "false", "project_enterprise_basis": "'none'",
    }
    contract = SCHEMA_2 if schema2 else vs.EXPECTED_COLUMNS
    items = []
    for col, typ in contract:
        if col in vals:
            items.append(f"CAST({vals[col]} AS {typ}) AS {q(col)}")
        elif typ == "VARCHAR[]":
            items.append(f"CAST([] AS VARCHAR[]) AS {q(col)}")
        elif typ == "BIGINT":
            items.append(f"CAST(0 AS BIGINT) AS {q(col)}")
        else:
            items.append(f"CAST('x' AS {typ}) AS {q(col)}")
    return (f"SELECT {', '.join(items)} FROM (SELECT i, (i // {rows_per_commit}) % {commits} AS c "
            f"FROM range({rows}) t(i))")


def make_synthetic(con, path: str, rows: int, *, schema2: bool = True,
                   kv: dict[str, str] | None = None, **kw) -> None:
    if kv is None:
        kv = ({"ctp_schema_version": "2", "organisations_version": "test",
               "organisations_mapping_sha256": "0" * 64} if schema2 else {})
    opts = f", KV_METADATA {_kv_sql(kv)}" if kv else ""
    con.execute(f"COPY ({synthetic_select(rows, schema2=schema2, **kw)}) TO {sql_str(path)} "
                f"(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000{opts})")


def bench(rows: int, threads: int, workdir: str | None = None, *,
          memory_limit: str = "4GB", out=sys.stdout) -> dict:
    """Measure the fused write on synthetic data with a throw-away salt."""
    work = tempfile.mkdtemp(prefix="anon-bench-", dir=workdir)
    try:
        keys = derive_keys(os.urandom(32))
        src = os.path.join(work, "bench.parquet")
        con = connect(memory_limit, threads, os.path.join(work, "spill"))
        install(con, keys)
        make_synthetic(con, src, rows)
        con.close()
        reg = os.path.join(work, "registry.parquet")
        t0 = time.time()
        build_registry([src], keys, reg, memory_limit=memory_limit, threads=threads,
                       out=io.StringIO())
        t_reg = time.time() - t0
        con = connect(memory_limit, threads, os.path.join(work, "spill"))
        install(con, keys)
        sha = registry_meta(con, reg)["registry_sha256"]
        t0 = time.time()
        con.execute(f"COPY (SELECT * FROM read_parquet({sql_str(src)})) TO "
                    f"{sql_str(os.path.join(work, 'plain.parquet'))} ({ENCODING})")
        t_plain = time.time() - t0
        t0 = time.time()
        res = write_public(con, src, os.path.join(work, "out.parquet"), registry=reg,
                           registry_sha256=sha)
        t_fused = time.time() - t0
        t0 = time.time()
        scan = leak_scan(con, os.path.join(work, "out.parquet"), reg)
        t_scan = time.time() - t0
        con.close()
    finally:
        shutil.rmtree(work, ignore_errors=True)
    r = {"rows": rows, "threads": threads, "seconds_registry": round(t_reg, 2),
         "seconds_plain_zstd19_copy": round(t_plain, 2),
         "seconds_fused_write_and_checks": round(t_fused, 2),
         "seconds_copy_part": res["seconds_write"], "seconds_scan": round(t_scan, 2),
         "problems": res["problems"] + scan["failures"]}
    total = t_fused + t_scan
    r["rows_per_s_per_thread_plain"] = round(rows / t_plain / threads)
    r["rows_per_s_per_thread_fused"] = round(rows / t_fused / threads)
    r["rows_per_s_per_thread_fused_and_scan"] = round(rows / total / threads)
    for label, rate in (("plain", r["rows_per_s_per_thread_plain"]),
                        ("fused_and_scan", r["rows_per_s_per_thread_fused_and_scan"])):
        r[f"hours_32_6e9_rows_96_vcpu_{label}"] = round(32.6e9 / (rate * 96) / 3600, 2)
    print(json.dumps(r, indent=2), file=out)
    return r


# --------------------------------------------------------------------------
# Command line
# --------------------------------------------------------------------------

def _add_run_opts(ap: argparse.ArgumentParser, jobs: bool = True) -> None:
    ap.add_argument("--memory-limit", default="4GB", help="DuckDB memory limit per job")
    ap.add_argument("--threads", type=int, default=1, help="DuckDB threads per job")
    if jobs:
        ap.add_argument("--jobs", type=int, default=1, help="files written at the same time")
    ap.add_argument("--temp-dir", default=None, help="folder for DuckDB spill files")


def _add_inputs(ap: argparse.ArgumentParser) -> None:
    ap.add_argument("inputs", nargs="*", help="Parquet files or folders")
    ap.add_argument("--inputs-from", default=None, help="a file with one input path per line")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Pseudonymize ctp schema 2 Parquet files.")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("registry", help="build the one registry over all inputs")
    r.add_argument("--salt-file", required=True)
    r.add_argument("--registry", required=True, help="output path (private)")
    r.add_argument("--drop-footers", action="store_true")
    r.add_argument("--report", default=None)
    _add_run_opts(r, jobs=False)
    _add_inputs(r)
    for name in ("write", "run"):
        w = sub.add_parser(name, help=("write the pseudonymized files" if name == "write"
                                       else "build the registry, then write"))
        w.add_argument("--salt-file", required=True)
        w.add_argument("--registry", required=True)
        w.add_argument("--outdir", required=True)
        w.add_argument("--commit-summary", choices=SUMMARY_MODES, default="scrub")
        w.add_argument("--summary-residue", choices=RESIDUE_MODES, default="fail",
                       help="'report' is option 4 of open question 2 (not decided)")
        w.add_argument("--no-scan", action="store_true", help="skip the leak scan")
        w.add_argument("--no-content-scan", action="store_true",
                       help="do not scan the content columns (their findings never fail)")
        w.add_argument("--person-domain-at", choices=DOMAIN_AT_MODES, default="keep",
                       help="rule for a person_domain value with an at sign (not decided)")
        w.add_argument("--drop-footers", action="store_true")
        w.add_argument("--allow-schema-1", action="store_true",
                       help="do not require the output to be ctp schema 2")
        w.add_argument("--report", default=None)
        _add_run_opts(w)
        _add_inputs(w)
    s = sub.add_parser("scan", help="leak scan of written files (no salt needed)")
    s.add_argument("--registry", required=True)
    s.add_argument("--summary-residue", choices=RESIDUE_MODES, default="fail")
    s.add_argument("--report", default=None)
    _add_run_opts(s, jobs=False)
    _add_inputs(s)
    b = sub.add_parser("bench", help="throughput on synthetic data")
    b.add_argument("--rows", type=int, default=2_000_000)
    b.add_argument("--threads", type=int, default=1)
    b.add_argument("--memory-limit", default="4GB")
    b.add_argument("--workdir", default=None)
    a = ap.parse_args(argv)

    if a.cmd == "bench":
        bench(a.rows, a.threads, a.workdir, memory_limit=a.memory_limit)
        return 0
    paths = gather_inputs(a.inputs, a.inputs_from)
    if not paths:
        print("no input files", file=sys.stderr)
        return 2
    keys = None
    report: dict = {"tool": "pseudonymize.py", "scheme_version": SCHEME_VERSION,
                    "command": a.cmd, "inputs": len(paths)}
    try:
        if a.cmd in ("registry", "write", "run"):
            keys = derive_keys(A.load_salt(path=a.salt_file, env={}))
        if a.cmd in ("registry", "run"):
            report["registry"] = build_registry(
                paths, keys, a.registry, memory_limit=a.memory_limit, threads=a.threads,
                temp_dir=a.temp_dir, drop_footers=a.drop_footers)
            report["failures"] = []
        if a.cmd in ("write", "run"):
            res = run_write(paths, a.outdir, keys=keys, registry=a.registry, jobs=a.jobs,
                            threads=a.threads, memory_limit=a.memory_limit,
                            temp_dir=a.temp_dir, summary_mode=a.commit_summary,
                            summary_residue=a.summary_residue, scan=not a.no_scan,
                            drop_footers=a.drop_footers,
                            expect_schema_2=not a.allow_schema_1,
                            domain_at=a.person_domain_at,
                            content_scan=not a.no_content_scan)
            report.update(res)
        if a.cmd == "scan":
            con = connect(a.memory_limit, a.threads, a.temp_dir)
            try:
                meta = verify_registry_file(con, a.registry)
                files, failures = [], []
                for p in paths:
                    sc = leak_scan(con, p, a.registry, summary_residue=a.summary_residue)
                    kv = file_kv(con, p)
                    if kv.get(FOOTER_REGISTRY_KEY) != meta["registry_sha256"]:
                        sc["failures"].append("footer registry sha256 is not this registry")
                    files.append({"file": os.path.basename(p), "leaks": sc["columns"],
                                  "reported": sc["reported"], "problems": sc["failures"]})
                    failures += [f"{os.path.basename(p)}: {f}" for f in sc["failures"]]
                    print(f"{'ok' if not sc['failures'] else 'fail':4} {os.path.basename(p)}")
            finally:
                con.close()
            report.update({"files": files, "failures": failures,
                           "registry_sha256": meta["registry_sha256"]})
    except (A.SaltError, RegistryError, A.UnknownColumnError, A.MissingColumnError) as e:
        print(f"error: {safe_error(e, keys)}", file=sys.stderr)
        return 2
    if a.report:
        write_report(a.report, report)
    fails = report.get("failures", [])
    print(f"{'FAIL: ' + str(len(fails)) + ' problem(s)' if fails else 'OK'}")
    for f in fails[:20]:
        print(f"  {strip_at(f)}")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
