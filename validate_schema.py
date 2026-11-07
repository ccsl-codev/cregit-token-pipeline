#!/usr/bin/env python3
"""Schema gate: every project parquet must carry the same columns and types.

    ./validate_schema.py <dataset.parquet> [<dataset.parquet> ...]
    ./validate_schema.py --schema 1 <dataset.parquet> ...    # an older schema
    ./validate_schema.py --emit-contract <dataset.parquet>   # print, do not check

The default contract is ctp schema 3 (EXPECTED_COLUMNS_3, 61 columns). Schema 1
(EXPECTED_COLUMNS, 70 columns) and schema 2 (71 columns) stay available.

Why a separate gate from validate.py: that one asks "did this project produce a
non-empty parquet". This one asks "do all projects agree". A dataset is unusable
if one project has 38 columns and another 23, or if `token_index` is BIGINT in
one and VARCHAR in another. A consumer would union them and get silent nulls.

Exit status is the whole interface:

  0  every file matches the contract
  1  at least one drifted. Every drift is printed, not just the first
  2  wrong invocation

duckdb is imported inside read_schema, not at module scope, so the pure
comparison logic can be unit-tested without duckdb installed. duckdb comes from
`devenv shell` and is absent from .venv.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass

USAGE = ("usage: validate_schema.py [--schema 1|2|3] <dataset.parquet> ...\n"
         "       validate_schema.py --emit-contract <dataset.parquet>")
EXIT_DRIFT = 1
EXIT_USAGE = 2

# The contract, measured from the updated cregit-issue61 output on 2026-09-13,
# widened 2026-09-19 and again 2026-09-20.
#
# 70 columns. It was 38 (and 23 before that; the 15 footer columns are
# commit-trailer evidence, which matters to this research because Signed-off-by,
# Reviewed-by and Co-authored-by carry attribution that authorship alone does
# not).
#
# The 29 after repo_name are per-project provenance, injected from the sidecar
# given with --project-meta: which roster or search found the project, its
# labels and their evidence, its shared-history cluster, and the regex mask it
# was tokenized with. They repeat per row by design: a reader
# can filter without a second join, and a column is cheap to drop at publish
# time but expensive to add later.
#
# clone_url is among them because repo_name is a lossy slug. provenance_status
# lets a reader separate the selected projects from development fixtures that
# share the output directory. file_mask is there because a mask can change
# between runs, and without it nobody can tell which rows came from which mask.
#
# These 29 names and their order must match META_FIELDS below and
# PROJECT_META_FIELDS in cregit-issue61/generate_dataset/generate_dataset.py.
# tests/test_meta_field_drift.py checks all three against each other.
#
# The three after person_domain are the firm attribution, added 2026-09-20. They
# sit there because they are RESOLVED FROM person_domain — the key and its answer
# belong together — and before repo_tag so the identity block stays contiguous.
#
# Unlike the 29 provenance columns they are not per-project constants: they come
# from a per-row join against data/affiliation.merged.csv, so they are the only
# columns in this contract whose value can differ between two rows of one
# project.
#
#   firm_raw     the map's `company` string, unaltered
#   firm         the canonical name, from the reviewed data/firm_canonical.csv
#   firm_source  the map's `source`, so a reader can tell a hand-curated
#                attribution (patch, gitdm, rich, correction) from a
#                single-person inference (cncf-gitdm-single, 2,771 of 4,049 rows)
#
# All three are '' when person_domain is not in the map, so an empty firm_source
# means "no attribution" and is the column to filter on.
#
# Still absent: person_email is published as it stands. That is a release
# decision, not a schema defect, and it is not settled here.
EXPECTED_COLUMNS: tuple[tuple[str, str], ...] = (
    ("repo_name", "VARCHAR"),
    ("clone_url", "VARCHAR"),
    ("provenance_status", "VARCHAR"),
    ("source", "VARCHAR"),
    ("stratum", "VARCHAR"),
    ("fact", "VARCHAR"),
    ("contested", "VARCHAR"),
    ("label_date", "VARCHAR"),
    ("owner", "VARCHAR"),
    ("repo", "VARCHAR"),
    ("roster_name", "VARCHAR"),
    ("roster_lang", "VARCHAR"),
    ("language", "VARCHAR"),
    ("commits", "VARCHAR"),
    ("size_class", "VARCHAR"),
    ("size_kb", "VARCHAR"),
    ("stars", "VARCHAR"),
    ("pushed_at", "VARCHAR"),
    ("license", "VARCHAR"),
    ("owner_type", "VARCHAR"),
    ("archived", "VARCHAR"),
    ("fork", "VARCHAR"),
    ("history_cluster", "VARCHAR"),
    ("history_shared_with", "VARCHAR"),
    ("history_relation", "VARCHAR"),
    ("history_includes", "VARCHAR"),
    ("history_first", "VARCHAR"),
    ("history_created", "VARCHAR"),
    ("manifest_category", "VARCHAR"),
    ("file_mask", "VARCHAR"),
    ("file_path", "VARCHAR"),
    ("token_index", "BIGINT"),
    ("source_line", "BIGINT"),
    ("source_col", "BIGINT"),
    ("source_text", "VARCHAR"),
    ("token_type", "VARCHAR"),
    ("token_value", "VARCHAR"),
    ("is_structural", "BIGINT"),
    ("cregit_commit_sha", "VARCHAR"),
    ("original_commit_sha", "VARCHAR"),
    ("author_name", "VARCHAR"),
    ("author_email", "VARCHAR"),
    ("author_date", "VARCHAR"),
    ("committer_name", "VARCHAR"),
    ("committer_email", "VARCHAR"),
    ("committer_date", "VARCHAR"),
    ("commit_summary", "VARCHAR"),
    ("personid", "VARCHAR"),
    ("person_name", "VARCHAR"),
    ("person_email", "VARCHAR"),
    ("person_domain", "VARCHAR"),
    ("firm_raw", "VARCHAR"),
    ("firm", "VARCHAR"),
    ("firm_source", "VARCHAR"),
    ("repo_tag", "VARCHAR"),
    ("footer_signed_off_by", "VARCHAR[]"),
    ("footer_co_authored_by", "VARCHAR[]"),
    ("footer_co_developed_by", "VARCHAR[]"),
    ("footer_reviewed_by", "VARCHAR[]"),
    ("footer_acked_by", "VARCHAR[]"),
    ("footer_tested_by", "VARCHAR[]"),
    ("footer_reported_by", "VARCHAR[]"),
    ("footer_suggested_by", "VARCHAR[]"),
    ("footer_based_on_patch_by", "VARCHAR[]"),
    ("footer_helped_by", "VARCHAR[]"),
    ("footer_mentored_by", "VARCHAR[]"),
    ("footer_assisted_by", "VARCHAR[]"),
    ("footer_thanks_to", "VARCHAR[]"),
    ("footer_personids", "VARCHAR[]"),
    ("footer_person_names", "VARCHAR[]"),
)

# The 29 per-project sidecar fields, in column order. The sidecar is a JSON
# object keyed by manifest name, and each value carries these keys. The cregit
# generator owns the same list (PROJECT_META_FIELDS), so a change here needs a
# change there. Every value is a string, and '' means "not known".
META_FIELDS: tuple[str, ...] = (
    "clone_url", "provenance_status",
    "source", "stratum", "fact", "contested", "label_date",
    "owner", "repo", "roster_name", "roster_lang",
    "language", "commits", "size_class", "size_kb", "stars", "pushed_at",
    "license", "owner_type", "archived", "fork",
    "history_cluster", "history_shared_with", "history_relation",
    "history_includes", "history_first", "history_created",
    "manifest_category", "file_mask",
)

# ctp schema 2: the release write removed the 3 firm columns and added the 4
# organization columns at the end. pseudonymize.SCHEMA_2 is the same list.
FIRM_COLUMNS: tuple[str, ...] = ("firm_raw", "firm", "firm_source")
ORGANIZATION_COLUMNS: tuple[tuple[str, str], ...] = (
    ("firm_based_on_namespace", "VARCHAR"),
    ("firm_based_on_domain", "VARCHAR"),
    ("project_enterprise_driven", "BOOLEAN"),
    ("project_enterprise_basis", "VARCHAR"),
)
EXPECTED_COLUMNS_2: tuple[tuple[str, str], ...] = tuple(
    (c, t) for c, t in EXPECTED_COLUMNS if c not in FIRM_COLUMNS
) + ORGANIZATION_COLUMNS

# ctp schema 3, the schema of the "Token on Me" release: schema 2 without 10
# per-project columns, and provenance_status named audit_frame_file. The 10
# columns are constant per project, so they are cheap to keep in a side table:
# `fact` goes to project_facts (schema3.py), and the other 9 stay in the
# sidecar. The cregit generator writes the 19 kept fields with
# --ctp-schema 3 (PROJECT_META_COLUMNS_3 there; tests/test_meta_field_drift.py
# compares the two lists). schema3.py adds the 4 organization columns and the
# footer key-values below.
SCHEMA_3_DROPPED: tuple[str, ...] = (
    "size_kb", "file_mask", "manifest_category", "fact",
    "history_cluster", "history_shared_with", "history_relation",
    "history_includes", "history_first", "history_created",
)
SCHEMA_3_RENAMED: dict[str, str] = {"provenance_status": "audit_frame_file"}
META_FIELDS_3: tuple[str, ...] = tuple(
    SCHEMA_3_RENAMED.get(f, f) for f in META_FIELDS if f not in SCHEMA_3_DROPPED)
EXPECTED_COLUMNS_3: tuple[tuple[str, str], ...] = tuple(
    (SCHEMA_3_RENAMED.get(c, c), t) for c, t in EXPECTED_COLUMNS_2
    if c not in SCHEMA_3_DROPPED)

# The footer key-value that names the schema of a file.
SCHEMA_KEY = "ctp_schema_version"
CONTRACTS: dict[str, tuple[tuple[str, str], ...]] = {
    "1": EXPECTED_COLUMNS, "2": EXPECTED_COLUMNS_2, "3": EXPECTED_COLUMNS_3,
}
DEFAULT_SCHEMA = "3"


def contract(schema: str) -> tuple[tuple[str, str], ...]:
    """The (name, type) contract of a ctp schema version."""
    if schema not in CONTRACTS:
        raise ValueError(f"unknown ctp schema {schema!r}; use one of {sorted(CONTRACTS)}")
    return CONTRACTS[schema]


@dataclass(frozen=True)
class Drift:
    """One disagreement between a file and the contract."""

    kind: str      # missing | unexpected | type | order
    column: str
    detail: str

    def __str__(self) -> str:
        return f"{self.kind:<10} {self.column:<26} {self.detail}"


def compare_schema(
    actual: list[tuple[str, str]],
    expected: tuple[tuple[str, str], ...] = EXPECTED_COLUMNS,
) -> list[Drift]:
    """Every way `actual` disagrees with the contract, in a stable order.

    Reports all drifts rather than the first, because a schema change usually
    moves several columns at once and fixing them one run at a time is slow.

    Column order is checked too. Parquet is read by name, so order does not break
    a consumer, but a reordering means the generator changed and that is worth a
    human looking at it.
    """
    drifts: list[Drift] = []
    actual_types = dict(actual)
    expected_types = dict(expected)

    for name, want in expected:
        if name not in actual_types:
            drifts.append(Drift("missing", name, f"contract expects {want}"))
        elif actual_types[name] != want:
            drifts.append(
                Drift("type", name, f"expected {want}, found {actual_types[name]}"))

    for name, found in actual:
        if name not in expected_types:
            drifts.append(Drift("unexpected", name, f"found {found}, not in contract"))

    # Order is only meaningful when the two column sets already agree.
    if not drifts and [n for n, _ in actual] != [n for n, _ in expected]:
        drifts.append(Drift("order", "-", "same columns, different order"))
    return drifts


def read_schema(path: str) -> list[tuple[str, str]]:
    """(name, type) per column, in file order. Needs duckdb."""
    import duckdb                          # devenv-only; see the module docstring

    rows = duckdb.sql("describe select * from read_parquet(?)",
                      params=[path]).fetchall()
    return [(r[0], r[1]) for r in rows]


def emit_contract(path: str) -> None:
    """Print a file's schema as a paste-ready EXPECTED_COLUMNS block.

    For when the generator legitimately changes: read the new schema, review the
    diff by eye, then paste. Better than hand-typing 70 rows.
    """
    for name, typ in read_schema(path):
        print(f'    ("{name}", "{typ}"),')


def check(paths: list[str],
           expected: tuple[tuple[str, str], ...] = EXPECTED_COLUMNS) -> int:
    """Report every file's drift. Returns the process exit status."""
    worst = 0
    for path in paths:
        try:
            actual = read_schema(path)
        except Exception as e:                       # unreadable is a drift too
            print(f"FAIL {path}\n  unreadable   {type(e).__name__}: {e}",
                  file=sys.stderr)
            worst = EXIT_DRIFT
            continue
        drifts = compare_schema(actual, expected)
        if drifts:
            worst = EXIT_DRIFT
            print(f"FAIL {path}  ({len(actual)} columns, "
                  f"{len(drifts)} drift{'s' if len(drifts) > 1 else ''})",
                  file=sys.stderr)
            for d in drifts:
                print(f"  {d}", file=sys.stderr)
        else:
            print(f"OK   {path}  ({len(actual)} columns)")
    return worst


def main() -> None:
    args = sys.argv[1:]
    schema = DEFAULT_SCHEMA
    if args[:1] == ["--schema"]:
        if len(args) < 2 or args[1] not in CONTRACTS:
            print(USAGE, file=sys.stderr)
            sys.exit(EXIT_USAGE)
        schema, args = args[1], args[2:]
    if not args:
        print(USAGE, file=sys.stderr)
        sys.exit(EXIT_USAGE)
    if args[0] == "--emit-contract":
        if len(args) != 2:
            print(USAGE, file=sys.stderr)
            sys.exit(EXIT_USAGE)
        emit_contract(args[1])
        return
    sys.exit(check(args, contract(schema)))


if __name__ == "__main__":
    main()
