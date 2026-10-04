#!/usr/bin/env python3
"""Build ctp.duckdb, a derived index over the manifests, stamps and metrics.tsv; rerunnable.
Usage: consolidate.py [--manifest PATH]... (default manifest.tsv). The tokens view takes
only parquets that match EXPECTED_COLUMNS. Exit 1 when a stamp's rows= is unreadable."""
from __future__ import annotations

import argparse
import configparser
import sys
from collections.abc import Sequence
from dataclasses import astuple, dataclass
from pathlib import Path

import duckdb

from retain import project_state
from validate_schema import EXPECTED_COLUMNS, compare_schema, read_schema

CORPUS = Path(__file__).resolve().parent
_cfg = configparser.ConfigParser()
_cfg.read(CORPUS / "pipeline.cfg")
OUT = (CORPUS / Path(_cfg.get("paths", "output_dir",
       fallback="../cregit-workspace/corpus-files")).expanduser()).resolve()
DB = CORPUS / "ctp.duckdb"

DEFAULT_MANIFEST = "manifest.tsv"

PROJECTS_DDL = (
    "create or replace table projects (name text primary key, url text, "
    "category text, size_class text, state text, token_rows bigint, "
    "parquet_path text, rows_unreadable boolean, parquet_missing boolean, "
    "excluded_because text)")
PROJECTS_INSERT = "insert into projects values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"


@dataclass
class ProjectRow:
    """One `projects` row by name. Fields are declared in PROJECTS_DDL column order,
    which as_tuple() relies on for executemany."""
    name: str
    url: str
    category: str
    size_class: str
    state: str
    token_rows: int | None
    parquet_path: str | None
    rows_unreadable: bool
    parquet_missing: bool
    excluded_because: str | None

    def as_tuple(self) -> tuple:
        return astuple(self)

    def __getitem__(self, index: int):
        """Positional access, for callers written against the old tuple shape."""
        return self.as_tuple()[index]


# Validated but deliberately not published (e.g. a near-duplicate fork): name -> reason.
# The row stays in `projects` with excluded_because; the parquet stays out of the view.
# Ships empty; a deployer adds their own entries.
PUBLICATION_EXCLUSIONS: dict[str, str] = {}


def sql_literal(path: str) -> str:
    """A view cannot take bound parameters, so the file list is interpolated; doubling
    quotes keeps a project name holding a quote from breaking or injecting SQL."""
    return "'" + str(path).replace("'", "''") + "'"


def resolve_manifest(value: str) -> Path:
    """Relative to this repo, not the cwd: ctp.py runs this script from the cregit dir."""
    path = Path(value).expanduser()
    return path if path.is_absolute() else CORPUS / path


def default_manifests() -> list[Path]:
    return [CORPUS / DEFAULT_MANIFEST]


def manifest_entries(manifests: Sequence[Path]):
    """(name, url, category, size_class) per row, the first row wins for a repeated name."""
    seen: set[str] = set()
    for manifest in manifests:
        for line in Path(manifest).read_text().splitlines():
            if not line.strip() or line.startswith("#"):
                continue
            name, url, category, _file_filter, size_class = line.split("\t")
            if name in seen:
                print(f"  ! {name} is named twice, "
                      f"the later row in {Path(manifest).name} is ignored",
                      file=sys.stderr)
                continue
            seen.add(name)
            yield name, url, category, size_class


def stamp_rows(name: str, stamp: Path) -> tuple[int | None, bool]:
    """(token_rows, rows_unreadable) from a validated stamp."""
    kv = dict(l.split("=", 1) for l in stamp.read_text().splitlines() if "=" in l)
    raw = kv.get("rows", 0)
    try:
        return int(raw), False
    except ValueError:
        print(f"  ! {name}: stamp rows={raw!r} is not an integer, "
              "token_rows left null", file=sys.stderr)
        return None, True


def project_row(name: str, url: str, category: str, size_class: str) -> ProjectRow:
    workdir = OUT / name
    stamp = workdir / f"{name}.validated"
    parquet = workdir / f"{name}-dataset.parquet"
    validated = stamp.exists()
    n_rows, rows_unreadable = stamp_rows(name, stamp) if validated else (None, False)
    has_parquet = parquet.exists()
    return ProjectRow(
        name=name, url=url, category=category, size_class=size_class,
        state=project_state(name, workdir), token_rows=n_rows,
        parquet_path=str(parquet) if has_parquet else None,
        rows_unreadable=rows_unreadable,
        parquet_missing=validated and not has_parquet,
        excluded_because=PUBLICATION_EXCLUSIONS.get(name))


def project_rows(manifests: Sequence[Path] | None = None) -> list[ProjectRow]:
    """None means default_manifests(). A project named twice is indexed once, from the
    first manifest. Broken projects are flagged, not raised, so the rest stay indexed."""
    if manifests is None:
        manifests = default_manifests()
    return [project_row(*entry) for entry in manifest_entries(manifests)]


def schema_split(paths: Sequence[str]) -> tuple[list[str], list, list]:
    """(usable, drifted, unread). read_parquet([...]) binds one schema, so one drifted
    file would abort the whole view: it is left out. An unreadable file cannot be shown
    to drift, so it stays usable and is reported."""
    usable, drifted, unread = [], [], []
    for path in paths:
        try:
            actual = read_schema(str(path))
        except Exception as exc:                     # not this gate's defect
            unread.append((path, f"{type(exc).__name__}: {exc}"))
            usable.append(path)
            continue
        drifts = compare_schema(actual)
        if drifts:
            drifted.append((path, len(actual), drifts))
        else:
            usable.append(path)
    return usable, drifted, unread


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rebuild ctp.duckdb over one or more manifests.")
    parser.add_argument(
        "--manifest", action="append", metavar="PATH",
        help="manifest to index, repeatable. A relative path is resolved "
             f"against this repo. Default: {DEFAULT_MANIFEST}.")
    return parser.parse_args(list(argv))


def create_tokens_view(con, usable: list[str]) -> int:
    """Rows in the new tokens view; 0, and no view, when no parquet is usable."""
    if not usable:
        return 0
    files = ", ".join(sql_literal(f) for f in usable)
    con.execute(f"""create or replace view tokens as
        select p.category, p.size_class, t.*
        from read_parquet([{files}]) t
        join projects p on t.repo_name = p.name""")
    return con.sql("select count(*) from tokens").fetchone()[0]


def print_schema_problems(drifted: list, unread: list, n_validated: int) -> None:
    if drifted:
        named = ", ".join(f"{Path(p).name} ({n} columns)" for p, n, _ in drifted)
        print(f"  schema mismatch, left out of the tokens view: "
              f"{len(drifted)} of {n_validated} "
              f"({named}): the contract is "
              f"{len(EXPECTED_COLUMNS)} columns, see validate_schema.py")
    if unread:
        named = ", ".join(f"{Path(p).name}: {why}" for p, why in unread)
        print(f"  ! schema not read for {len(unread)} of {n_validated} "
              f"parquet(s) ({named}): left in the tokens view, "
              "run validate_schema.py on them", file=sys.stderr)


FLAG_NOTES = (
    ("parquet_missing", "DONE but parquet missing",
     "flagged parquet_missing, left out of the tokens view"),
    ("rows_unreadable", "unreadable rows= in stamp",
     "flagged rows_unreadable, token_rows is null"),
)


def print_project_flags(projects: list[ProjectRow]) -> None:
    excluded = [(p.name, p.excluded_because) for p in projects if p.excluded_because]
    if excluded:
        print(f"  publication exclusions: {len(excluded)} "
              "(row kept in projects, left out of the tokens view)")
        for name, why in excluded:
            print(f"    - {name}: {why}")
    for attr, label, note in FLAG_NOTES:
        names = [p.name for p in projects if getattr(p, attr)]
        if names:
            print(f"  {label}: {len(names)} ({', '.join(names)}): {note}")


def print_state_counts(projects: list[ProjectRow]) -> None:
    for state in ("DONE", "RUNNING", "FAILED", "QUEUED"):
        n = sum(1 for p in projects if p.state == state)
        if n:
            print(f"  {state}: {n}")


def publishable_parquets(projects: list[ProjectRow]) -> list[str]:
    return [p.parquet_path for p in projects
            if p.state == "DONE" and p.parquet_path and not p.excluded_because]


def main(argv: Sequence[str] = ()) -> None:
    args = parse_args(argv)
    manifests = ([resolve_manifest(v) for v in args.manifest]
                 if args.manifest else default_manifests())
    projects = project_rows(manifests)
    con = duckdb.connect(str(DB))

    con.execute(PROJECTS_DDL)
    con.executemany(PROJECTS_INSERT, [p.as_tuple() for p in projects])

    con.execute(f"""create or replace table phase_metrics as
        select * from read_csv('{CORPUS / 'metrics.tsv'}', delim='\t', header=true)""")

    validated = publishable_parquets(projects)
    usable, drifted, unread = schema_split(validated)
    total = create_tokens_view(con, usable)

    print(f"ctp.duckdb rebuilt: {DB}")
    print(f"  manifests: {', '.join(Path(m).name for m in manifests)}")
    print_state_counts(projects)
    print(f"  tokens view: {total:,} rows across {len(usable)} projects")
    print_schema_problems(drifted, unread, len(validated))
    print_project_flags(projects)
    con.close()
    if any(p.rows_unreadable for p in projects):
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
