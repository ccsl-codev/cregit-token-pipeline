#!/usr/bin/env python3
"""Post-run gate: a project is only DONE if its parquet passes these checks.

Usage: validate.py <dataset.parquet> <stamp-file>
Run inside `devenv shell` (needs duckdb).

Exit status is the whole interface ctp.py sees:

  0  accepted. The stamp is written, so the next ctp pass skips the project.
  1  rejected. No stamp, so the next ctp pass retries the project.
  2  wrong invocation. Usage is printed and nothing else is touched.

Two positional arguments, no flags: that is the whole CLI, and it is a
published contract, not an oversight. sys.argv is parsed by hand on purpose --
argparse would add a --help and an error-formatting surface this script does
not need, for two required paths.

The checks are plain `if` statements on purpose. `assert` vanishes under
`python -O`, and a gate that an interpreter flag can delete is not a gate: any
parquet would then be stamped DONE.

The parquet path is bound as a query parameter and is never pasted into the SQL
text. Project names come from the manifest, so a name holding a single quote
would otherwise unbalance the string literal and break the gate, and a crafted
name could inject SQL.
"""
import os
import sys

import duckdb

USAGE = "usage: validate.py <dataset.parquet> <stamp-file>"
MIN_BYTES = 10_000
EXIT_REJECTED = 1
EXIT_USAGE = 2

COUNT_SQL = "select count(*) from read_parquet(?)"
SCHEMA_SQL = "describe select * from read_parquet(?)"


def reject(reason: str) -> None:
    """Report a rejected parquet and exit. Writes no stamp."""
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

    cols = [r[0] for r in duckdb.sql(SCHEMA_SQL, params=[parquet]).fetchall()]
    print(f"OK rows={n} bytes={size}")
    print(f"cols={cols}")

    with open(stamp, "w") as f:
        f.write(f"rows={n}\nbytes={size}\n")


if __name__ == "__main__":
    raise SystemExit(main())
