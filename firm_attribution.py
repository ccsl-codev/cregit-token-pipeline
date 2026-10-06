#!/usr/bin/env python3
"""Add firm_raw, firm and firm_source to one project Parquet, in place.
Usage: firm_attribution.py <dataset.parquet> [--memory-limit SIZE] [--threads N]
Exit 0 written, 1 refused (wrong input columns, or a repeated lookup key), 2 usage."""
from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path

from validate_schema import (CREGIT_COLUMNS, EXPECTED_COLUMNS, compare_schema,
                             read_schema)

CORPUS = Path(__file__).resolve().parent
FIRM_MAP = CORPUS / "data" / "affiliation.merged.csv"
FIRM_CANONICAL = CORPUS / "data" / "firm_canonical.csv"
EXIT_REFUSED = 1

WRONG_INPUT = ("FAIL {path}: {n} columns, neither cregit's {cregit} nor ctp's {final}. "
               "First drift: {drift}")
NO_KEY_COLUMN = "FAIL {path}: no `{column}` column"
REPEATED_KEYS = ("FAIL {path}: {n} repeated key(s) in `{column}`: {keys}. "
                 "A repeated key multiplies token rows through the LEFT JOIN.")
WRITTEN = "OK {path}: firm columns added from {map}"

# The join of cregit feat/firm-attribution e215e16 (firm_sql), keyed on person_domain.
# all_varchar: a company spelled like a number ('360') must stay a string.
FIRM_SQL = """
COPY (
    SELECT {columns},
           coalesce(fm.company, '') AS firm_raw,
           coalesce(nullif(fc.firm, ''), fm.company, '') AS firm,
           coalesce(fm.source, '') AS firm_source
    FROM read_parquet($src, file_row_number = true) t
    LEFT JOIN (SELECT lower(domain) AS domain, company, source
               FROM read_csv_auto($firm_map, header = true, all_varchar = true)) fm
           ON fm.domain = lower(t.person_domain)
    LEFT JOIN (SELECT lower(trim(firm_raw)) AS firm_raw, firm
               FROM read_csv_auto($firm_canonical, header = true, all_varchar = true)) fc
           ON fc.firm_raw = lower(trim(fm.company))
    ORDER BY t.file_row_number
) TO $dst (FORMAT PARQUET, COMPRESSION ZSTD)
"""


def input_refusal(parquet: Path) -> str | None:
    """None when the file is cregit's form, or ctp's form from an earlier attribution."""
    actual = read_schema(str(parquet))
    if not compare_schema(actual, EXPECTED_COLUMNS):
        return None
    drifts = compare_schema(actual, CREGIT_COLUMNS)
    if not drifts:
        return None
    return WRONG_INPUT.format(path=parquet, n=len(actual), cregit=len(CREGIT_COLUMNS),
                              final=len(EXPECTED_COLUMNS), drift=drifts[0])


def repeated_keys(path: Path, column: str) -> list[str] | None:
    """Keys seen more than once, in first-repeat order; None when the column is absent."""
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if column not in (reader.fieldnames or []):
            return None
        seen: set[str] = set()
        repeats: dict[str, None] = {}
        for row in reader:
            key = (row[column] or "").strip().lower()
            if key in seen:
                repeats[key] = None
            seen.add(key)
    return list(repeats)


def table_refusal(path: Path, column: str) -> str | None:
    repeats = repeated_keys(path, column)
    if repeats is None:
        return NO_KEY_COLUMN.format(path=path, column=column)
    if repeats:
        return REPEATED_KEYS.format(path=path, n=len(repeats), column=column,
                                    keys=", ".join(repeats[:5]))
    return None


def connect(parquet: Path, memory_limit: str, threads: int):
    """Spill beside the Parquet: the ORDER BY sorts every token of the project."""
    import duckdb

    config = {"temp_directory": str(parquet.parent / ".firm-spill")}
    if memory_limit:
        config["memory_limit"] = memory_limit
    if threads:
        config["threads"] = threads
    return duckdb.connect(config=config)


def add_firm_columns(parquet: Path, firm_map: Path, firm_canonical: Path,
                     memory_limit: str = "", threads: int = 0) -> None:
    """Rewrite parquet as cregit's 67 columns then FIRM_COLUMNS, rows in file order."""
    tmp = parquet.with_name(f".{parquet.name}.firm-tmp")
    columns = ", ".join(f't."{name}"' for name, _ in CREGIT_COLUMNS)
    con = connect(parquet, memory_limit, threads)
    try:
        con.execute(FIRM_SQL.format(columns=columns), {
            "src": str(parquet), "dst": str(tmp),
            "firm_map": str(firm_map), "firm_canonical": str(firm_canonical)})
    except Exception:
        tmp.unlink(missing_ok=True)
        raise
    finally:
        con.close()
    os.replace(tmp, parquet)


def refusal(parquet: Path, firm_map: Path, firm_canonical: Path) -> str | None:
    for path, column in ((firm_map, "domain"), (firm_canonical, "firm_raw")):
        reason = table_refusal(path, column)
        if reason:
            return reason
    return input_refusal(parquet)


def parse_args(argv: list[str]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Add the firm columns to one Parquet.")
    ap.add_argument("parquet", type=Path)
    ap.add_argument("--memory-limit", default="", metavar="SIZE",
                    help="DuckDB memory_limit, e.g. 4GB. Default: DuckDB's own.")
    ap.add_argument("--threads", type=int, default=0, metavar="N",
                    help="DuckDB threads. Default: DuckDB's own.")
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    reason = refusal(args.parquet, FIRM_MAP, FIRM_CANONICAL)
    if reason:
        print(reason, file=sys.stderr)
        return EXIT_REFUSED
    add_firm_columns(args.parquet, FIRM_MAP, FIRM_CANONICAL,
                     args.memory_limit, args.threads)
    print(WRITTEN.format(path=args.parquet, map=FIRM_MAP.name))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
