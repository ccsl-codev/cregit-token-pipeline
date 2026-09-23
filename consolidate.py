#!/usr/bin/env python3
"""Build ctp.duckdb: project tracking table + unified token view.

    ./consolidate.py                                    # manifest.tsv (default)
    ./consolidate.py --manifest a.tsv --manifest b.tsv  # repeatable

Derived index, NOT authority — ground truth stays the manifests + validated
stamps + metrics.tsv. Safe to rerun any time. Run inside devenv (needs duckdb).

WHICH MANIFESTS. manifest.tsv is the only manifest this repository carries: 4
small public pilot projects (jq, zstd, libuv, tmux). It is the default when
--manifest is not given, so ctp.py's `db` command — which passes no arguments —
always has something to describe. A relative --manifest resolves against this
repo, not the caller's cwd, because ctp.py runs this script from the cregit
directory. A project named by two manifests is indexed once, from the first
manifest that names it.

SCHEMA GATE. read_parquet([...]) binds one schema for the whole list, so a
single file at the old 23 or 38 columns aborts the tokens view for every
project. The view is therefore built only from parquets whose schema matches the
current dataset contract, and the contract is validate_schema.EXPECTED_COLUMNS
rather than a column count repeated here. Every file left out is named and
counted in the printed summary: a silent exclusion is worse than the crash it
replaces, because the row count then looks plausible.

A parquet whose schema cannot be read at all is a different defect — that is
validate.py's gate, not this one — and it cannot be shown to disagree with the
contract, so it is reported on stderr and left in the list. Dropping it would be
exactly the silent exclusion above.

state = 'DONE' means the pipeline finished for that project. It does NOT mean
the data is present. A DONE project whose parquet is gone keeps a null
parquet_path and stays out of the tokens view, which is right for a data view
but leaves the index disagreeing with itself. Such a project is therefore
flagged parquet_missing = true, counted, and named in the printed summary, so
`select name from projects where parquet_missing` finds it.

PUBLICATION EXCLUSIONS. A project can be validly drawn, run, and validated, and
still not belong in the published dataset. The near-duplicate fork pair is the
case that forced this: two Tencent repos share 505,056 of their ~507,000 source
blobs and 134,715 commits, so publishing both counts the same authorship twice.
Deleting the manifest row would hide the decision and break the sampling record,
so the row stays and the project is named in PUBLICATION_EXCLUSIONS with its
reason. The effect is the schema gate's: the parquet is left out of the tokens
view, the project keeps its row in `projects`, and the reason is recorded in
`excluded_because` and printed. `select name, excluded_because from projects
where excluded_because is not null` is the audit query.

One unusable stamp never aborts the rebuild. A stamp whose rows= value is not
an integer leaves token_rows null, is flagged rows_unreadable = true, is
reported by name and by value, and makes the exit status non-zero once every
other project is indexed.

Exit status:
  0  index rebuilt, every stamp readable
  1  index rebuilt, but at least one stamp was malformed (see the summary)
"""
from __future__ import annotations

import argparse
import configparser
import fcntl
import sys
from collections.abc import Sequence
from dataclasses import astuple, dataclass
from pathlib import Path

import duckdb

from validate_schema import EXPECTED_COLUMNS, compare_schema, read_schema

CORPUS = Path(__file__).resolve().parent
_cfg = configparser.ConfigParser()
_cfg.read(CORPUS / "pipeline.cfg")
OUT = (CORPUS / Path(_cfg.get("paths", "output_dir",
       fallback="../cregit-workspace/corpus-files")).expanduser()).resolve()
DB = CORPUS / "ctp.duckdb"
# ctp.py's own directory per project, holding the lock and the logs. It is
# deliberately NOT inside the work directory, because run_pipeline_process.sh
# deletes the work directory at FROM_STEP=1 and again from its EXIT trap, which
# would take the lock with it. This must stay in step with ctp.py's state_dir().
STATE = CORPUS / "state"

# The one manifest this repository carries, and therefore the default run set.
DEFAULT_MANIFEST = "manifest.tsv"

PROJECTS_DDL = (
    "create or replace table projects (name text primary key, url text, "
    "category text, size_class text, state text, token_rows bigint, "
    "parquet_path text, rows_unreadable boolean, parquet_missing boolean, "
    "excluded_because text)")
PROJECTS_INSERT = "insert into projects values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"


@dataclass
class ProjectRow:
    """One project, shaped like the `projects` table, by name rather than by
    position — so a reader never has to count columns to know what a field
    means, and inserting one does not silently shift every other reader.

    as_tuple() exists for the one place that still needs positions:
    con.executemany(PROJECTS_INSERT, ...) takes a plain sequence per row, in
    PROJECTS_DDL's column order — the order these fields are declared in.
    """
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
        """Positional access, kept only for callers written against the row's
        pre-dataclass tuple shape."""
        return self.as_tuple()[index]


# Projects run and validated, but deliberately not published. Name -> reason.
# See PUBLICATION EXCLUSIONS in the module docstring, and docs/LIMITATIONS.md
# for the measurements behind each entry.
PUBLICATION_EXCLUSIONS = {
    "tencent__tendbcluster-tendb":
        "near-duplicate of tencent__tendbcluster-tdbctl: 505,056 shared source "
        "blobs (99.66% of its own) and 134,715 shared commits (99.91%). tdbctl "
        "is kept because it holds 4.9x more first-party authorship once "
        "vendored directories are excluded (202,331 vs 41,571 tokens) and has "
        "30 unique first-party files at HEAD against 0 for tendb.",
}


def lock_held(lockfile: Path) -> bool:
    """True when another process holds this project's flock.

    Opened "r", not "w". A probe must not write to the thing it observes, and
    "w" truncates on open — so this function used to truncate the lock file of a
    RUNNING job every time the index was rebuilt. Harmless in practice only
    because ctp.py also opens the lock "w" and never writes a byte to it, so
    there was nothing to lose. flock works on a read-only descriptor.

    `retain.py` and `ctp.py`'s `_lock_held` carry the same predicate, opened the
    same "r" way, so all three now agree. Duplicated on purpose rather than
    shared: retain.py is stdlib-only so `ctp.py run --drop-memo` can call it
    without pulling in duckdb, and importing across the three would break that
    independence to save eight lines.

    An unreadable lock file returns True: refusing to guess is the safe answer
    when the question is "is a run in flight".
    """
    if not lockfile.exists():
        return False
    try:
        f = lockfile.open("r")
    except OSError:
        return True
    with f:
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


def resolve_manifest(value: str) -> Path:
    """One --manifest value as a path.

    A relative value is resolved against this repo, never against the cwd,
    because ctp.py runs this script with cwd set to the cregit directory.
    """
    path = Path(value).expanduser()
    return path if path.is_absolute() else CORPUS / path


def default_manifests() -> list[Path]:
    """The manifest indexed when --manifest is not given: DEFAULT_MANIFEST."""
    return [CORPUS / DEFAULT_MANIFEST]


def project_rows(manifests: Sequence[Path] | None = None) -> list[ProjectRow]:
    """One ProjectRow per manifest line. See ProjectRow for the fields.

    `manifests` is the list to index, so a caller — main(), or a test — decides
    which manifests are ground truth instead of this function deciding for it.
    None means default_manifests().

    A project named by two manifests is indexed once, from the first manifest
    that names it, and the repeat is named on stderr.

    The two flags carry the two ways a project can disagree with itself. Both
    are reported rather than raised, because one broken project must not cost
    the index for all the others.
    """
    if manifests is None:
        manifests = default_manifests()
    rows = []
    seen: set[str] = set()
    for manifest in manifests:
        for line in Path(manifest).read_text().splitlines():
            if not line.strip() or line.startswith("#"):
                continue
            name, url, category, file_filter, size_class = line.split("\t")
            if name in seen:
                print(f"  ! {name} is named twice, "
                      f"the later row in {Path(manifest).name} is ignored",
                      file=sys.stderr)
                continue
            seen.add(name)
            workdir = OUT / name
            stamp = workdir / f"{name}.validated"
            parquet = workdir / f"{name}-dataset.parquet"
            validated = stamp.exists()
            if validated:
                state = "DONE"
            elif lock_held(STATE / name / ".lock"):
                state = "RUNNING"
            elif workdir.exists():
                state = "FAILED"
            else:
                state = "QUEUED"
            n_rows = None
            rows_unreadable = False
            if validated:
                kv = dict(l.split("=") for l in stamp.read_text().splitlines()
                          if "=" in l)
                raw = kv.get("rows", 0)
                try:
                    n_rows = int(raw)
                except ValueError:
                    # Report and carry on: the rest of the corpus must not lose
                    # its index because one stamp says rows=many.
                    print(f"  ! {name}: stamp rows={raw!r} is not an integer, "
                          "token_rows left null", file=sys.stderr)
                    rows_unreadable = True
            has_parquet = parquet.exists()
            rows.append(ProjectRow(
                name=name, url=url, category=category, size_class=size_class,
                state=state, token_rows=n_rows,
                parquet_path=str(parquet) if has_parquet else None,
                rows_unreadable=rows_unreadable,
                parquet_missing=validated and not has_parquet,
                excluded_because=PUBLICATION_EXCLUSIONS.get(name)))
    return rows


def schema_split(paths: Sequence[str]) -> tuple[list[str], list, list]:
    """Split validated parquets by whether they match the dataset contract.

    Returns (usable, drifted, unread):

      usable   goes into read_parquet([...])
      drifted  (path, n_columns, drifts) — schema disagrees with the contract,
               so the file is left out. One of these in the list would abort the
               whole view, which is how 4 legacy 23-column files used to poison
               it.
      unread   (path, reason) — the schema could not be read at all. Such a file
               cannot be shown to disagree, so it stays in `usable` and is
               reported instead of dropped.

    The contract is validate_schema.EXPECTED_COLUMNS, so this gate follows a
    schema widening automatically instead of pinning a column count.
    """
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

    # A deliberate publication exclusion keeps its projects row but
    # contributes no tokens.
    validated = [p.parquet_path for p in projects
                 if p.state == "DONE" and p.parquet_path and not p.excluded_because]
    usable, drifted, unread = schema_split(validated)
    if usable:
        files = ", ".join(sql_literal(f) for f in usable)
        con.execute(f"""create or replace view tokens as
            select p.category, p.size_class, t.*
            from read_parquet([{files}]) t
            join projects p on t.repo_name = p.name""")
        total = con.sql("select count(*) from tokens").fetchone()[0]
    else:
        total = 0

    print(f"ctp.duckdb rebuilt: {DB}")
    print(f"  manifests: {', '.join(Path(m).name for m in manifests)}")
    for state in ("DONE", "RUNNING", "FAILED", "QUEUED"):
        n = sum(1 for p in projects if p.state == state)
        if n:
            print(f"  {state}: {n}")
    print(f"  tokens view: {total:,} rows across {len(usable)} projects")

    if drifted:
        named = ", ".join(f"{Path(p).name} ({n} columns)" for p, n, _ in drifted)
        print(f"  schema mismatch, left out of the tokens view: "
              f"{len(drifted)} of {len(validated)} "
              f"({named}): the contract is "
              f"{len(EXPECTED_COLUMNS)} columns, see validate_schema.py")
    if unread:
        named = ", ".join(f"{Path(p).name}: {why}" for p, why in unread)
        print(f"  ! schema not read for {len(unread)} of {len(validated)} "
              f"parquet(s) ({named}): left in the tokens view, "
              "run validate_schema.py on them", file=sys.stderr)

    excluded = [(p.name, p.excluded_because) for p in projects if p.excluded_because]
    if excluded:
        print(f"  publication exclusions: {len(excluded)} "
              "(row kept in projects, left out of the tokens view)")
        for name, why in excluded:
            print(f"    - {name}: {why}")

    no_parquet = [p.name for p in projects if p.parquet_missing]
    if no_parquet:
        print(f"  DONE but parquet missing: {len(no_parquet)} "
              f"({', '.join(no_parquet)}): flagged parquet_missing, "
              "left out of the tokens view")
    unreadable = [p.name for p in projects if p.rows_unreadable]
    if unreadable:
        print(f"  unreadable rows= in stamp: {len(unreadable)} "
              f"({', '.join(unreadable)}): flagged rows_unreadable, "
              "token_rows is null")
    con.close()
    if unreadable:
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
