#!/usr/bin/env python3
"""Build ctp.duckdb: project tracking table + unified token view.

Derived index, NOT authority — ground truth stays manifest.tsv + validated
stamps + metrics.tsv. Safe to rerun any time. Run inside devenv (needs duckdb).
"""
import configparser
import fcntl
from pathlib import Path

import duckdb

CORPUS = Path(__file__).resolve().parent
_cfg = configparser.ConfigParser()
_cfg.read(CORPUS / "pipeline.cfg")
OUT = (CORPUS / Path(_cfg.get("paths", "output_dir",
       fallback="../cregit-workspace/corpus-files")).expanduser()).resolve()
DB = CORPUS / "ctp.duckdb"


def lock_held(lockfile: Path) -> bool:
    if not lockfile.exists():
        return False
    with lockfile.open("w") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return False
        except BlockingIOError:
            return True


def project_rows():
    rows = []
    for line in (CORPUS / "manifest.tsv").read_text().splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        name, url, category, file_filter, size_class = line.split("\t")
        workdir = OUT / name
        stamp = workdir / f"{name}.validated"
        parquet = workdir / f"{name}-dataset.parquet"
        if stamp.exists():
            state = "DONE"
        elif lock_held(workdir / ".lock"):
            state = "RUNNING"
        elif workdir.exists():
            state = "FAILED"
        else:
            state = "QUEUED"
        n_rows = None
        if stamp.exists():
            kv = dict(l.split("=") for l in stamp.read_text().splitlines() if "=" in l)
            n_rows = int(kv.get("rows", 0))
        rows.append((name, url, category, size_class, state, n_rows,
                     str(parquet) if parquet.exists() else None))
    return rows


def main() -> None:
    projects = project_rows()
    con = duckdb.connect(str(DB))

    con.execute("create or replace table projects (name text primary key, url text, "
                "category text, size_class text, state text, token_rows bigint, parquet_path text)")
    con.executemany("insert into projects values (?, ?, ?, ?, ?, ?, ?)", projects)

    con.execute(f"""create or replace table phase_metrics as
        select * from read_csv('{CORPUS / 'metrics.tsv'}', delim='\t', header=true)""")

    validated = [p[6] for p in projects if p[4] == "DONE" and p[6]]
    if validated:
        files = ", ".join(f"'{f}'" for f in validated)
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
    con.close()


if __name__ == "__main__":
    main()
