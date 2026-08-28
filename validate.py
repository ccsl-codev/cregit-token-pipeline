#!/usr/bin/env python3
"""Post-run gate: a project is only DONE if its parquet passes these checks.

Usage: validate.py <dataset.parquet> <stamp-file>
Run inside `devenv shell` (needs duckdb).
"""
import os
import sys

import duckdb


def main() -> None:
    parquet, stamp = sys.argv[1], sys.argv[2]

    size = os.path.getsize(parquet)
    assert size > 10_000, f"parquet suspiciously small: {size} bytes"

    n = duckdb.sql(f"select count(*) from '{parquet}'").fetchone()[0]
    assert n > 0, "parquet has zero rows"

    cols = [r[0] for r in duckdb.sql(f"describe select * from '{parquet}'").fetchall()]
    print(f"OK rows={n} bytes={size}")
    print(f"cols={cols}")

    with open(stamp, "w") as f:
        f.write(f"rows={n}\nbytes={size}\n")


if __name__ == "__main__":
    main()
