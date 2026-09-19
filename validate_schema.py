#!/usr/bin/env python3
"""Schema gate: every project parquet must carry the same columns and types.

    ./validate_schema.py <dataset.parquet> [<dataset.parquet> ...]
    ./validate_schema.py --emit-contract <dataset.parquet>   # print, do not check

Discharges EXECUTION-STATE.md D20 ("the schema is not validated yet").

Why a separate gate from validate.py: that one asks "did this project produce a
non-empty parquet". This one asks "do all projects agree". A corpus is unusable
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

USAGE = "usage: validate_schema.py [--emit-contract] <dataset.parquet> ..."
EXIT_DRIFT = 1
EXIT_USAGE = 2

# The contract, measured from the updated cregit-issue61 output on 2026-09-13,
# widened 2026-09-19.
#
# 67 columns. It was 38 (and 23 before that; the 15 footer columns are
# commit-trailer evidence, which matters to this research because Signed-off-by,
# Reviewed-by and Co-authored-by carry attribution that authorship alone does
# not).
#
# The 29 after repo_name are per-project provenance, injected from
# project_meta.json: which roster or search found the project, which stratum it
# was assigned and on what evidence, its shared-history cluster, and the regex
# mask it was tokenized with. They repeat per row by design — a reader can filter
# without a second join against candidates.csv, and a column is cheap to drop at
# publish time but expensive to add later.
#
# clone_url is among them because repo_name is a lossy slug. provenance_status
# separates the 200 corpus projects from the 10 development fixtures that share
# the output directory: select the corpus with
#   where provenance_status = 'candidates.csv'
# file_mask is there because a mask widening is planned, and without it nobody
# can tell which rows came from which mask.
#
# These 29 names and their order must match META_FIELDS in project_meta.py and
# PROJECT_META_FIELDS in cregit-issue61/generate_dataset/generate_dataset.py.
# tests/test_meta_field_drift.py checks all three against each other.
#
# Still absent: a firm column. Resolving person_domain to an employer is the
# contribution this dataset is being built for, so its absence is expected here
# and must not be read as agreement that the schema is finished.
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
    diff by eye, then paste. Better than hand-typing 67 rows.
    """
    for name, typ in read_schema(path):
        print(f'    ("{name}", "{typ}"),')


def check(paths: list[str]) -> int:
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
        drifts = compare_schema(actual)
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
    if not args:
        print(USAGE, file=sys.stderr)
        sys.exit(EXIT_USAGE)
    if args[0] == "--emit-contract":
        if len(args) != 2:
            print(USAGE, file=sys.stderr)
            sys.exit(EXIT_USAGE)
        emit_contract(args[1])
        return
    sys.exit(check(args))


if __name__ == "__main__":
    main()
