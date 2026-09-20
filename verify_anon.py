#!/usr/bin/env python3
"""Verify anonymized parquets WITHOUT access to the originals.

Adapted from pipeline/anonymize/verify.py in
cbsoft-vem2026-corporate-truck-factor at commit 3d722c6. Three changes:

  * parquet, not SQLite: it walks every VARCHAR and VARCHAR[] column of every
    *.parquet in a directory. The original walked SQLite text columns and had no
    concept of a list, so it could not have inspected a footer_* column.
  * shape-based, not allowlist-based. The original asked "is this address at a
    known mailing-list domain, and if not is it anon.invalid". Here every domain
    is published on purpose, so the domain says nothing. What is checked instead
    is that every LOCAL PART is a pseudonym: author_NNNN and nothing else.
  * no secrets required. anonymize_parquet.py's own leak scan needs the registry
    of real names to search for, so only whoever ran it can repeat it. This
    checks the published files alone, which means a reviewer, a co-author or a
    Zenodo depositor can run it -- and it is the check worth putting in a
    release script, because it cannot be passed by forgetting to anonymize.

It is a necessary, not a sufficient, condition: a real name that happens to look
like 'Author 0042' would pass. Run it together with anonymize_parquet.py, whose
report covers the other direction.

Usage:
  verify_anon.py DIR [DIR ...]          # exit 0 clean, 1 residue, 2 misuse

Needs duckdb, from `devenv shell`.
"""
from __future__ import annotations

import argparse
import os
import re
import sys

# The pseudonym shapes. Any run of digits, not the four anonymize_parquet.py
# zero-pads to: the padding is cosmetic and a check that rejected 'Author 10'
# would report a formatting preference as though it were a leaked name, which is
# the one thing this script must not do.
PSEUDO_LOCAL = r"author_\d+"
PSEUDO_NAME = r"Author \d+"
PSEUDO_ID = r"author \d+"

# Any address, anywhere, whose local part is NOT a pseudonym. This is the whole
# check: one regex over every string in the release.
REAL_ADDRESS = re.compile(
    r"(?<![A-Za-z0-9._%+\-])(?!" + PSEUDO_LOCAL + r"@)"
    r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")

# Emitted by anonymize_parquet.py where a value had no registry entry. Its
# presence means that tool hit a bug, so it is checked here too.
MISSING_MARKER = "(ANON-MISSING)"

# Columns whose every value must be exactly a pseudonym, not merely free of
# addresses. A real name carries no '@' and would otherwise pass unnoticed.
EXACT_SHAPE = {
    "author_name": PSEUDO_NAME,
    "committer_name": PSEUDO_NAME,
    "person_name": PSEUDO_NAME,
    "footer_person_names": PSEUDO_NAME,
    "personid": PSEUDO_ID,
    "footer_personids": PSEUDO_ID,
    "author_email": PSEUDO_LOCAL + r"@.+",
    "committer_email": PSEUDO_LOCAL + r"@.+",
    "person_email": PSEUDO_LOCAL + r"@.+",
}

# Source code and repository namespace. An address in a copyright header is real
# and is not removable without destroying the token stream, so these are counted
# and shown but do not fail the run. anonymize_parquet.py's docstring discloses
# this as residual risk; the point of listing it here is that a release cannot
# pretend it is not there.
CONTENT_COLUMNS = frozenset({"source_text", "token_value", "file_path",
                             "repo_name", "clone_url", "owner", "repo",
                             "roster_name", "fact", "file_mask"})


def connect():
    import duckdb
    con = duckdb.connect()
    con.execute("SET memory_limit='3GB'")
    con.execute("SET threads=3")
    return con


def string_columns(con, path: str) -> list[tuple[str, bool]]:
    """(column, is_list) for every VARCHAR / VARCHAR[] column."""
    rows = con.execute("describe select * from read_parquet(?)",
                       [path]).fetchall()
    return [(r[0], r[1] == "VARCHAR[]") for r in rows
            if r[1] in ("VARCHAR", "VARCHAR[]")]


def distinct_values(con, path: str, col: str, is_list: bool) -> list[str]:
    """Distinct non-empty values, unnested when the column is a list.

    Distinct rather than every row: the check is on the set of strings, and a
    200,000 row file has a few hundred distinct identity values.
    """
    if is_list:
        q = (f"select distinct v from (select unnest({col}) v from "
             f"read_parquet(?) where {col} is not null)")
    else:
        q = (f"select distinct {col} from read_parquet(?) "
             f"where {col} is not null and {col} <> ''")
    return [v for (v,) in con.execute(q, [path]).fetchall() if v]


def check_file(con, path: str, verbose: bool = False) -> tuple[int, int, list]:
    """(values checked, failures, notes) for one parquet."""
    checked = 0
    failures: list[tuple[str, str, str]] = []
    notes: list[tuple[str, str, str]] = []

    for col, is_list in string_columns(con, path):
        exact = EXACT_SHAPE.get(col)
        exact_re = re.compile(rf"^{exact}$") if exact else None
        content = col in CONTENT_COLUMNS
        for v in distinct_values(con, path, col, is_list):
            checked += 1
            bucket = notes if content else failures
            if MISSING_MARKER in v:
                failures.append((col, "anonymizer bug marker", v))
                continue
            if exact_re and not exact_re.match(v):
                failures.append((col, "not a pseudonym", v))
                continue
            m = REAL_ADDRESS.search(v)
            if m:
                bucket.append((col, f"address {m.group(0)!r}", v))

    if verbose or failures:
        for col, why, v in failures[:20]:
            print(f"    FAIL {col}: {why} in {v[:120]!r}")
    for col, why, v in notes[:5]:
        print(f"    note {col}: {why} (source code / repo namespace)")
    if len(notes) > 5:
        print(f"    note ... and {len(notes) - 5} more in content columns")
    return checked, len(failures), notes


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("dirs", nargs="+", help="directories of anonymized parquets")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args(argv)

    con = connect()
    total_checked = total_failed = total_notes = 0
    n_files = 0
    for d in args.dirs:
        if not os.path.isdir(d):
            print(f"ERROR: not a directory: {d}", file=sys.stderr)
            return 2
        files = sorted(f for f in os.listdir(d) if f.endswith(".parquet"))
        if not files:
            print(f"ERROR: no parquet files in {d}", file=sys.stderr)
            return 2
        for f in files:
            p = os.path.join(d, f)
            print(f"[{f}]")
            checked, failed, notes = check_file(con, p, args.verbose)
            print(f"  {checked:,} distinct strings checked, {failed} failure(s),"
                  f" {len(notes)} content-column note(s)")
            total_checked += checked
            total_failed += failed
            total_notes += len(notes)
            n_files += 1

    print(f"\n{n_files} file(s), {total_checked:,} distinct strings checked.")
    print(f"content-column addresses (disclosed residual risk): {total_notes}")
    if total_failed:
        print(f"FAIL: {total_failed} value(s) are not pseudonymized.")
        return 1
    print("OK: every identity column holds a pseudonym and no identity column "
          "carries a non-pseudonymous address.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
