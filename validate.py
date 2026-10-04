#!/usr/bin/env python3
"""Post-run gate. Usage: validate.py <dataset.parquet> <stamp-file>
Exit 0 writes the stamp (ctp skips the project), 1 rejects, 2 is a usage error.
Plain `if`s, not `assert` (gone under `python -O`); the path is a bound SQL parameter."""
import os
import sys

import duckdb

from validate_schema import compare_schema, read_schema

USAGE = "usage: validate.py <dataset.parquet> <stamp-file>"
MIN_BYTES = 10_000
EXIT_REJECTED = 1
EXIT_USAGE = 2

COUNT_SQL = "select count(*) from read_parquet(?)"


def reject(reason: str) -> None:
    print(f"FAIL {reason}", file=sys.stderr)
    sys.exit(EXIT_REJECTED)


def main() -> None:
    if len(sys.argv) != 3:
        print(USAGE, file=sys.stderr)
        sys.exit(EXIT_USAGE)
    parquet, stamp = sys.argv[1], sys.argv[2]

    size = os.path.getsize(parquet)
    if size <= MIN_BYTES:
        reject(f"parquet suspiciously small: {size} bytes")

    n = duckdb.sql(COUNT_SQL, params=[parquet]).fetchone()[0]
    if n <= 0:
        reject("parquet has zero rows")

    drifts = compare_schema(read_schema(parquet))
    if drifts:
        reject(f"schema drift ({len(drifts)}): " + "; ".join(map(str, drifts[:3])))
    print(f"OK rows={n} bytes={size}")

    with open(stamp, "w") as f:
        f.write(f"rows={n}\nbytes={size}\n")


if __name__ == "__main__":
    raise SystemExit(main())
