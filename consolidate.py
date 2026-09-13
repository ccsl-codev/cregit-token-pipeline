#!/usr/bin/env python3
"""Build ctp.duckdb: project tracking table + unified token view.

Derived index, NOT authority — ground truth stays manifest.tsv + validated
stamps + metrics.tsv. Safe to rerun any time. Run inside devenv (needs duckdb).

state = 'DONE' means the pipeline finished for that project. It does NOT mean
the data is present. A DONE project whose parquet is gone keeps a null
parquet_path and stays out of the tokens view, which is right for a data view
but leaves the index disagreeing with itself. Such a project is therefore
flagged parquet_missing = true, counted, and named in the printed summary, so
`select name from projects where parquet_missing` finds it.

One unusable stamp never aborts the rebuild. A stamp whose rows= value is not
an integer leaves token_rows null, is flagged rows_unreadable = true, is
reported by name and by value, and makes the exit status non-zero once every
other project is indexed.

Exit status:
  0  index rebuilt, every stamp readable
  1  index rebuilt, but at least one stamp was malformed (see the summary)
"""
import configparser
import fcntl
import sys
from pathlib import Path

import duckdb

CORPUS = Path(__file__).resolve().parent
_cfg = configparser.ConfigParser()
_cfg.read(CORPUS / "pipeline.cfg")
OUT = (CORPUS / Path(_cfg.get("paths", "output_dir",
       fallback="../cregit-workspace/corpus-files")).expanduser()).resolve()
DB = CORPUS / "ctp.duckdb"

PROJECTS_DDL = (
    "create or replace table projects (name text primary key, url text, "
    "category text, size_class text, state text, token_rows bigint, "
    "parquet_path text, rows_unreadable boolean, parquet_missing boolean)")
PROJECTS_INSERT = "insert into projects values (?, ?, ?, ?, ?, ?, ?, ?, ?)"


def lock_held(lockfile: Path) -> bool:
    if not lockfile.exists():
        return False
    with lockfile.open("w") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return False
        except BlockingIOError:
            return True


def sql_literal(path: str) -> str:
    """Quote `path` as a DuckDB string literal, doubling embedded quotes.

    A view definition cannot take bound parameters, so the tokens view has to
    interpolate its file list. Doubling the quotes keeps the literal balanced,
    so a project name holding a quote can neither break the SQL nor inject it.
    """
    return "'" + str(path).replace("'", "''") + "'"


def project_rows() -> list:
    """One tuple per manifest row, shaped like the projects table:

    (name, url, category, size_class, state, token_rows, parquet_path,
     rows_unreadable, parquet_missing)

    The two flags carry the two ways a project can disagree with itself. Both
    are reported rather than raised, because one broken project must not cost
    the index for all the others.
    """
    rows = []
    for line in (CORPUS / "manifest.tsv").read_text().splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        name, url, category, file_filter, size_class = line.split("\t")
        workdir = OUT / name
        stamp = workdir / f"{name}.validated"
        parquet = workdir / f"{name}-dataset.parquet"
        validated = stamp.exists()
        if validated:
            state = "DONE"
        elif lock_held(workdir / ".lock"):
            state = "RUNNING"
        elif workdir.exists():
            state = "FAILED"
        else:
            state = "QUEUED"
        n_rows = None
        rows_unreadable = False
        if validated:
            kv = dict(l.split("=") for l in stamp.read_text().splitlines() if "=" in l)
            raw = kv.get("rows", 0)
            try:
                n_rows = int(raw)
            except ValueError:
                # Report and carry on: 1,423 projects must not lose their index
                # because one stamp says rows=many.
                print(f"  ! {name}: stamp rows={raw!r} is not an integer, "
                      "token_rows left null", file=sys.stderr)
                rows_unreadable = True
        has_parquet = parquet.exists()
        rows.append((name, url, category, size_class, state, n_rows,
                     str(parquet) if has_parquet else None,
                     rows_unreadable, validated and not has_parquet))
    return rows


def main() -> None:
    projects = project_rows()
    con = duckdb.connect(str(DB))

    con.execute(PROJECTS_DDL)
    con.executemany(PROJECTS_INSERT, projects)

    con.execute(f"""create or replace table phase_metrics as
        select * from read_csv('{CORPUS / 'metrics.tsv'}', delim='\t', header=true)""")

    validated = [p[6] for p in projects if p[4] == "DONE" and p[6]]
    if validated:
        files = ", ".join(sql_literal(f) for f in validated)
        con.execute(f"""create or replace view tokens as
            select p.category, p.size_class, t.*
            from read_parquet([{files}]) t
            join projects p on t.repo_name = p.name""")
        total = con.sql("select count(*) from tokens").fetchone()[0]
    else:
        total = 0

    print(f"ctp.duckdb rebuilt: {DB}")
    for state in ("DONE", "RUNNING", "FAILED", "QUEUED"):
        n = sum(1 for p in projects if p[4] == state)
        if n:
            print(f"  {state}: {n}")
    print(f"  tokens view: {total:,} rows across {len(validated)} projects")

    no_parquet = [p[0] for p in projects if p[8]]
    if no_parquet:
        print(f"  DONE but parquet missing: {len(no_parquet)} "
              f"({', '.join(no_parquet)}): flagged parquet_missing, "
              "left out of the tokens view")
    unreadable = [p[0] for p in projects if p[7]]
    if unreadable:
        print(f"  unreadable rows= in stamp: {len(unreadable)} "
              f"({', '.join(unreadable)}): flagged rows_unreadable, "
              "token_rows is null")
    con.close()
    if unreadable:
        sys.exit(1)


if __name__ == "__main__":
    main()
