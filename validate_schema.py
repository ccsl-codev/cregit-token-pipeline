#!/usr/bin/env python3
"""Schema gate: every parquet must carry the 70 columns of EXPECTED_COLUMNS.
Usage: validate_schema.py <parquet>...  |  --emit-contract <parquet> (print, no check).
Exit 0 all match, 1 any drift (all drifts printed), 2 usage. duckdb is imported lazily."""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass

USAGE = "usage: validate_schema.py [--emit-contract] <dataset.parquet> ..."
EXIT_DRIFT = 1
EXIT_USAGE = 2

# --project-meta sidecar: a JSON object keyed by project name; each value maps the 29
# provenance fields (clone_url..file_mask) to strings, a missing key giving ''. Names
# and order must match PROJECT_META_FIELDS in cregit's generate_dataset.py.
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
    # A per-row join of person_domain against the caller's --firm-map CSV, so these
    # can differ within one project. All three are '' when the domain is not mapped.
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
    """Every drift, not just the first, in a stable order. Order is checked too: it
    breaks no by-name reader, but it means the generator changed."""
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

    if not drifts and [n for n, _ in actual] != [n for n, _ in expected]:
        drifts.append(Drift("order", "-", "same columns, different order"))
    return drifts


def read_schema(path: str) -> list[tuple[str, str]]:
    """(name, type) per column, in file order. Needs duckdb."""
    import duckdb

    rows = duckdb.sql("describe select * from read_parquet(?)",
                      params=[path]).fetchall()
    return [(r[0], r[1]) for r in rows]


def emit_contract(path: str) -> None:
    """Print a file's schema as a paste-ready EXPECTED_COLUMNS block."""
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


def build_parser() -> argparse.ArgumentParser:
    """No --help: the usage and error text in main() is the contract."""
    ap = argparse.ArgumentParser(add_help=False)
    ap.add_argument("--emit-contract", action="store_true",
                    help="print one file's schema as a paste-ready contract block")
    ap.add_argument("paths", nargs="*", help="parquet files to check")
    return ap


def main() -> None:
    args = build_parser().parse_args()
    if not args.paths:
        print(USAGE, file=sys.stderr)
        sys.exit(EXIT_USAGE)
    if args.emit_contract:
        if len(args.paths) != 1:
            print(USAGE, file=sys.stderr)
            sys.exit(EXIT_USAGE)
        emit_contract(args.paths[0])
        return
    sys.exit(check(args.paths))


if __name__ == "__main__":
    raise SystemExit(main())
