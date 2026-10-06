"""Unit tests for consolidate.py, the DuckDB index builder. Each test patches
consolidate.duckdb.connect with a recorder, so no database is opened; only the
schema-gate tests that write a real Parquet need duckdb itself."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import consolidate
import retain

DUCKDB_IS_STUBBED = getattr(sys.modules["duckdb"], "__doc__", "").startswith("Test stub")


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #

class FakeResult:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row


class FakeCon:
    """Records every statement consolidate.py sends. Opens no database."""

    def __init__(self, count=0):
        self.statements: list[str] = []
        self.batches: list[tuple[str, list]] = []
        self.count = count
        self.closed = False

    def execute(self, sql, *args):
        self.statements.append(sql)

    def executemany(self, sql, rows):
        self.batches.append((sql, list(rows)))

    def sql(self, query):
        self.statements.append(query)
        return FakeResult((self.count,))

    def close(self):
        self.closed = True

    def find(self, needle: str) -> str:
        matches = [s for s in self.statements if needle in s]
        assert matches, f"no statement contains {needle!r}: {self.statements}"
        return matches[0]


def write_manifest(root: Path, *lines: str, name: str = "manifest.tsv") -> Path:
    path = root / name
    path.write_text("".join(f"{line}\n" for line in lines))
    return path


def make_project(out: Path, name: str, *, stamp: str | None = None,
                 parquet: bool = False) -> Path:
    workdir = out / name
    workdir.mkdir(parents=True, exist_ok=True)
    if stamp is not None:
        (workdir / f"{name}.validated").write_text(stamp)
    if parquet:
        (workdir / f"{name}-dataset.parquet").write_bytes(b"PAR1" + b"\0" * 64)
    return workdir


ROW = "jq\thttps://github.com/jqlang/jq.git\tcommunity\t\\.[ch]$\tS"


# --------------------------------------------------------------------------- #
# fixtures
# --------------------------------------------------------------------------- #

@pytest.fixture(autouse=True)
def sandbox(tmp_path, monkeypatch):
    """Point consolidate at tmp_path so the live corpus and ctp.duckdb are safe."""
    out = tmp_path / "corpus-files"
    out.mkdir()
    monkeypatch.setattr(consolidate, "CORPUS", tmp_path)
    monkeypatch.setattr(consolidate, "OUT", out)
    monkeypatch.setattr(consolidate, "DB", tmp_path / "ctp.duckdb")
    monkeypatch.setattr(retain, "STATE", tmp_path / "state")
    return SimpleNamespace(root=tmp_path, out=out)


@pytest.fixture
def con(monkeypatch):
    """Install a recording connection and return it."""
    fake = FakeCon(count=1_234_567)
    monkeypatch.setattr(consolidate.duckdb, "connect",
                        lambda path: fake, raising=False)
    return fake


# --------------------------------------------------------------------------- #
# project_rows
# --------------------------------------------------------------------------- #

def test_project_rows_classifies_all_four_states(sandbox, held_lock):
    """The index is the dashboard. A wrong state misreports corpus progress."""
    write_manifest(
        sandbox.root,
        "done\thttps://d.git\tcommunity\t\\.[ch]$\tS",
        "running\thttps://r.git\tcommunity\t\\.[ch]$\tM",
        "failed\thttps://f.git\tenterprise\t\\.[ch]$\tL",
        "wiped\thttps://w.git\tenterprise\t\\.[ch]$\tL",
        "queued\thttps://q.git\tcommunity\t\\.[ch]$\tS")
    make_project(sandbox.out, "done", stamp="rows=7\nbytes=8\n", parquet=True)
    make_project(sandbox.out, "failed")
    make_project(sandbox.out, "running")
    # The runner deleted this workdir on failure; only ctp's state dir is left.
    retain.state_dir("wiped").mkdir(parents=True)
    retain.state_dir("running").mkdir(parents=True)

    with held_lock(retain.lock_path("running")):
        states = {r[0]: r[4] for r in consolidate.project_rows()}

    assert states == dict(done="DONE", running="RUNNING", failed="FAILED",
                          wiped="FAILED", queued="QUEUED")


def test_project_rows_ignores_a_stale_lock_left_in_the_work_directory(sandbox, held_lock):
    """The runner deletes the workdir, so ctp locks state/<name>/.lock. A lock
    in the workdir is someone else's, and believing it would hide a FAILED project."""
    write_manifest(sandbox.root, "proj\thttps://p.git\tcommunity\t\\.[ch]$\tS")
    workdir = make_project(sandbox.out, "proj")

    with held_lock(workdir / ".lock"):
        states = {r[0]: r[4] for r in consolidate.project_rows()}

    assert states == {"proj": "FAILED"}


def test_project_rows_skips_comments_and_blank_lines(sandbox):
    """The manifest header is a comment; treating it as data adds a fake project."""
    write_manifest(sandbox.root, "# name  url  category  filter  class", "", "  ", ROW)
    assert [r[0] for r in consolidate.project_rows()] == ["jq"]


def test_project_rows_rejects_a_malformed_row(sandbox):
    """Five fields is the contract, exactly as in ctp.read_manifest."""
    write_manifest(sandbox.root, "jq\thttps://x.git\tcommunity\t\\.[ch]$")
    with pytest.raises(ValueError):
        consolidate.project_rows()


def test_project_rows_without_a_manifest_raises(sandbox):
    """The manifest is ground truth; a missing one must stop the rebuild."""
    with pytest.raises(FileNotFoundError):
        consolidate.project_rows()


def test_project_rows_reports_no_parquet_path_when_the_file_is_absent(sandbox):
    """A stamp without a parquet must not put a dead path in the tokens view,
    and parquet_missing makes the disagreement queryable."""
    write_manifest(sandbox.root, ROW)
    make_project(sandbox.out, "jq", stamp="rows=9\nbytes=9\n", parquet=False)
    (name, _url, _cat, _cls, state, n_rows, parquet,
     rows_unreadable, parquet_missing, excluded) = consolidate.project_rows()[0]
    assert (name, state, n_rows, parquet) == ("jq", "DONE", 9, None)
    assert (rows_unreadable, parquet_missing, excluded) == (False, True, None)


def test_project_rows_does_not_flag_a_queued_project_as_missing_data(sandbox):
    """parquet_missing means "DONE but no data", so a project that never ran
    must not be flagged. Otherwise the flag would name every queued project."""
    write_manifest(sandbox.root, ROW)
    assert consolidate.project_rows()[0][8] is False


@pytest.mark.parametrize("stamp, rows", [
    pytest.param("malformed stamp\n", 0, id="no-rows-key"),
    pytest.param("rows=5\nbytes=6\nnote=a=b\n", 5, id="extra-key-with-a-second-equals"),
])
def test_project_rows_tolerates_an_odd_stamp(sandbox, stamp, rows):
    """The stamp is a key=value bag; an old or odd stamp must not crash the rebuild."""
    write_manifest(sandbox.root, ROW)
    make_project(sandbox.out, "jq", stamp=stamp, parquet=True)
    assert consolidate.project_rows()[0][5] == rows


def test_project_rows_survives_a_non_numeric_rows_value(sandbox, capsys):
    """A bad rows= value is reported by name and value, flagged, and still indexed."""
    write_manifest(sandbox.root, ROW)
    make_project(sandbox.out, "jq", stamp="rows=many\nbytes=6\n", parquet=True)

    row = consolidate.project_rows()[0]

    assert row[5] is None
    assert row[7] is True
    assert row[4] == "DONE"
    err = capsys.readouterr().err
    assert "jq" in err
    assert "'many'" in err


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #

def test_main_replaces_the_projects_table_and_inserts_every_row(sandbox, con, capsys):
    """`create or replace` plus one insert batch is what makes a rebuild safe
    to rerun at any time."""
    write_manifest(sandbox.root, ROW)
    make_project(sandbox.out, "jq", stamp="rows=1234\nbytes=5678\n", parquet=True)

    consolidate.main()

    ddl = con.find("create or replace table projects")
    assert "name text primary key" in ddl
    assert "token_rows bigint" in ddl
    assert "rows_unreadable boolean" in ddl
    assert "parquet_missing boolean" in ddl
    assert "excluded_because text" in ddl
    sql, rows = con.batches[0]
    assert sql == "insert into projects values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    assert rows == [("jq", "https://github.com/jqlang/jq.git", "community", "S",
                     "DONE", 1234, str(sandbox.out / "jq" / "jq-dataset.parquet"),
                     False, False, None)]


def test_main_loads_the_metrics_ledger_as_a_tsv_with_a_header(sandbox, con):
    """metrics.tsv is tab separated with a header row. Wrong options here turn
    the benchmark ledger into one unusable column."""
    write_manifest(sandbox.root, ROW)
    (sandbox.root / "metrics.tsv").write_text("iso_start\tproject\n")

    consolidate.main()

    sql = con.find("create or replace table phase_metrics")
    assert str(sandbox.root / "metrics.tsv") in sql
    assert "delim='\t'" in sql
    assert "header=true" in sql


def test_main_builds_the_tokens_view_from_validated_parquets_only(sandbox, con, capsys):
    """The unified view must not read a parquet from a project that failed, or
    the corpus statistics include unvalidated tokens."""
    write_manifest(sandbox.root,
                   "done\thttps://d.git\tcommunity\t\\.[ch]$\tS",
                   "failed\thttps://f.git\tenterprise\t\\.[ch]$\tL")
    make_project(sandbox.out, "done", stamp="rows=10\nbytes=20\n", parquet=True)
    make_project(sandbox.out, "failed", parquet=True)

    consolidate.main()

    view = con.find("create or replace view tokens")
    assert str(sandbox.out / "done" / "done-dataset.parquet") in view
    assert "failed-dataset.parquet" not in view
    assert "join projects p on t.repo_name = p.name" in view
    assert "1,234,567 rows across 1 projects" in capsys.readouterr().out


def test_main_skips_the_tokens_view_when_nothing_is_validated(sandbox, con, capsys):
    """read_parquet([]) is invalid SQL, so the view must be skipped, and the
    reported total must be 0 rather than a stale number."""
    write_manifest(sandbox.root, ROW)
    make_project(sandbox.out, "jq", parquet=True)

    consolidate.main()

    assert not [s for s in con.statements if "view tokens" in s or "from tokens" in s]
    assert "tokens view: 0 rows across 0 projects" in capsys.readouterr().out


def test_main_prints_a_count_for_every_populated_state(sandbox, con, capsys):
    """The printed summary is the only feedback the operator gets."""
    write_manifest(sandbox.root,
                   "done\thttps://d.git\tcommunity\t\\.[ch]$\tS",
                   "failed\thttps://f.git\tenterprise\t\\.[ch]$\tL",
                   "queued\thttps://q.git\tcommunity\t\\.[ch]$\tS")
    make_project(sandbox.out, "done", stamp="rows=1\nbytes=2\n", parquet=True)
    make_project(sandbox.out, "failed")

    consolidate.main()

    out = capsys.readouterr().out
    assert "DONE: 1" in out
    assert "FAILED: 1" in out
    assert "QUEUED: 1" in out
    assert "RUNNING" not in out


def test_main_connects_to_the_configured_database_and_closes_it(
        sandbox, monkeypatch, capsys):
    """A leaked connection leaves a write-ahead file next to ctp.duckdb."""
    write_manifest(sandbox.root)
    fake = FakeCon()
    seen = {}

    def fake_connect(path):
        seen["path"] = path
        return fake

    monkeypatch.setattr(consolidate.duckdb, "connect", fake_connect, raising=False)

    consolidate.main()

    assert seen["path"] == str(sandbox.root / "ctp.duckdb")
    assert fake.closed is True
    assert "ctp.duckdb rebuilt" in capsys.readouterr().out


def test_main_reports_a_malformed_stamp_and_still_indexes_the_rest(
        sandbox, con, capsys):
    """One bad stamp costs one project: the rebuild finishes, names and counts
    it, and exits non-zero."""
    write_manifest(sandbox.root,
                   "bad\thttps://b.git\tcommunity\t\\.[ch]$\tS",
                   "good\thttps://g.git\tenterprise\t\\.[ch]$\tL")
    make_project(sandbox.out, "bad", stamp="rows=many\nbytes=6\n", parquet=True)
    make_project(sandbox.out, "good", stamp="rows=77\nbytes=8\n", parquet=True)

    with pytest.raises(SystemExit) as exc:
        consolidate.main()

    assert exc.value.code == 1
    captured = capsys.readouterr()
    assert "unreadable rows= in stamp: 1 (bad)" in captured.out
    assert "'many'" in captured.err
    _sql, rows = con.batches[0]
    assert [r[0] for r in rows] == ["bad", "good"]
    assert [r[5] for r in rows] == [None, 77]
    assert con.closed is True
    assert "good-dataset.parquet" in con.find("create or replace view tokens")


def test_main_counts_and_names_a_done_project_whose_parquet_is_gone(
        sandbox, con, capsys):
    """Flagged, counted and named, and the view still leaves the dead file out."""
    write_manifest(sandbox.root,
                   "gone\thttps://x.git\tcommunity\t\\.[ch]$\tS",
                   "here\thttps://y.git\tenterprise\t\\.[ch]$\tL")
    make_project(sandbox.out, "gone", stamp="rows=5\nbytes=6\n", parquet=False)
    make_project(sandbox.out, "here", stamp="rows=7\nbytes=8\n", parquet=True)

    consolidate.main()

    out = capsys.readouterr().out
    assert "DONE but parquet missing: 1 (gone)" in out
    assert "DONE: 2" in out
    _sql, rows = con.batches[0]
    assert {r[0]: r[8] for r in rows} == {"gone": True, "here": False}
    view = con.find("create or replace view tokens")
    assert "gone-dataset.parquet" not in view
    assert "here-dataset.parquet" in view


# --------------------------------------------------------------------------- #
# publication exclusions
# --------------------------------------------------------------------------- #

def test_an_excluded_project_keeps_its_row_and_reason_but_not_its_tokens(
        sandbox, con, monkeypatch, capsys):
    """Deleting the manifest row would drop the project from the record, and
    publishing it would count the same authorship twice. The summary names it."""
    monkeypatch.setattr(consolidate, "PUBLICATION_EXCLUSIONS",
                        {"twin": "near-duplicate of other"})
    write_manifest(sandbox.root,
                   "twin\thttps://t.git\tcompany-owned\t\\.[ch]$\tM",
                   "other\thttps://o.git\tcompany-owned\t\\.[ch]$\tM")
    make_project(sandbox.out, "twin", stamp="rows=5\nbytes=6\n", parquet=True)
    make_project(sandbox.out, "other", stamp="rows=7\nbytes=8\n", parquet=True)

    consolidate.main()

    _sql, rows = con.batches[0]
    assert {r[0]: (r[4], r[9]) for r in rows} == {
        "twin": ("DONE", "near-duplicate of other"), "other": ("DONE", None)}
    view = con.find("create or replace view tokens")
    assert "twin-dataset.parquet" not in view
    assert "other-dataset.parquet" in view
    out = capsys.readouterr().out
    assert "publication exclusions: 1" in out
    assert "- twin: near-duplicate of other" in out
    assert "tokens view: 1,234,567 rows across 1 projects" in out


def test_the_shipped_exclusion_list_is_empty(sandbox):
    """Nothing ships pre-excluded; a deployer adds their own entries."""
    assert consolidate.PUBLICATION_EXCLUSIONS == {}


def test_main_says_nothing_about_flags_when_every_project_is_healthy(
        sandbox, con, capsys):
    """The summary must stay quiet on a clean corpus, or the operator learns to
    ignore it."""
    write_manifest(sandbox.root, ROW)
    make_project(sandbox.out, "jq", stamp="rows=3\nbytes=4\n", parquet=True)

    consolidate.main()

    out = capsys.readouterr().out
    assert "parquet missing" not in out
    assert "unreadable" not in out
    assert "publication exclusions" not in out


def test_main_doubles_a_quote_in_a_project_path_in_the_tokens_view(sandbox, con):
    """A view definition cannot bind parameters, so its file list is escaped
    instead. A project name with a quote must not unbalance the SQL."""
    write_manifest(sandbox.root, "we'ird\thttps://w.git\tcommunity\t\\.[ch]$\tS")
    make_project(sandbox.out, "we'ird", stamp="rows=2\nbytes=3\n", parquet=True)

    consolidate.main()

    view = con.find("create or replace view tokens")
    escaped = str(sandbox.out / "we'ird" / "we'ird-dataset.parquet").replace("'", "''")
    assert escaped in view
    assert view.count("'") % 2 == 0


def test_main_is_safe_to_rerun(sandbox, con, capsys):
    """Documented promise: safe to rerun any time. Two runs must emit the same
    statements, all of them `create or replace`."""
    write_manifest(sandbox.root, ROW)
    make_project(sandbox.out, "jq", stamp="rows=3\nbytes=4\n", parquet=True)

    consolidate.main()
    first = list(con.statements)
    con.statements.clear()
    consolidate.main()

    assert con.statements == first
    assert all(s.strip().startswith(("create or replace", "select"))
               for s in con.statements)


# --------------------------------------------------------------------------- #
# which manifests are indexed
# --------------------------------------------------------------------------- #

SM_ROW = "dpdk__dpdk\thttps://x.git/dpdk\tfoundation\t\\.[ch]$\tL"
LINUX_ROW = "torvalds__linux\t/staging/linux.git\tfoundation\t\\.[ch]$\tL"


def test_main_indexes_manifest_tsv_by_default(sandbox, con, capsys):
    """`ctp.py db` passes no arguments, so manifest.tsv is the default."""
    write_manifest(sandbox.root, ROW)
    make_project(sandbox.out, "jq", stamp="rows=5\n", parquet=True)

    consolidate.main()

    _sql, rows = con.batches[0]
    assert [r[0] for r in rows] == ["jq"]
    out = capsys.readouterr().out
    assert "manifests: manifest.tsv" in out
    assert "DONE: 1" in out


def test_manifest_given_explicitly_indexes_only_that_manifest(sandbox, con):
    """A named manifest is read on its own; the default sitting on disk is not
    mixed in."""
    write_manifest(sandbox.root, ROW)                  # would be the default
    write_manifest(sandbox.root, SM_ROW, name="other.tsv")

    consolidate.main(["--manifest", "other.tsv"])

    _sql, rows = con.batches[0]
    assert [r[0] for r in rows] == ["dpdk__dpdk"]


def test_manifest_given_twice_indexes_both_manifests(sandbox, con, capsys):
    """--manifest is repeatable, and the union is indexed in the order given."""
    write_manifest(sandbox.root, SM_ROW, name="a.tsv")
    write_manifest(sandbox.root, LINUX_ROW, name="b.tsv")

    consolidate.main(["--manifest", "b.tsv", "--manifest", "a.tsv"])

    _sql, rows = con.batches[0]
    assert [r[0] for r in rows] == ["torvalds__linux", "dpdk__dpdk"]
    assert "manifests: b.tsv, a.tsv" in capsys.readouterr().out


def test_a_project_named_by_two_manifests_is_indexed_once(sandbox, con, capsys):
    """The projects table has name as its primary key, so a repeat would abort
    the insert batch for every project. The repeat is named, not silent."""
    write_manifest(sandbox.root, SM_ROW, LINUX_ROW, name="a.tsv")
    write_manifest(sandbox.root, LINUX_ROW, name="b.tsv")

    consolidate.main(["--manifest", "a.tsv", "--manifest", "b.tsv"])

    _sql, rows = con.batches[0]
    assert [r[0] for r in rows] == ["dpdk__dpdk", "torvalds__linux"]
    assert "torvalds__linux is named twice" in capsys.readouterr().err


def test_a_duplicate_inside_one_manifest_is_also_indexed_once(sandbox):
    """De-duplication is by project name, so it holds within a manifest too."""
    one = write_manifest(sandbox.root, LINUX_ROW, LINUX_ROW, name="a.tsv")
    assert [r[0] for r in consolidate.project_rows([one])] == ["torvalds__linux"]


def test_an_empty_manifest_indexes_nothing_and_builds_no_view(sandbox, con,
                                                              capsys):
    """An empty run set is a valid state. read_parquet([]) is invalid SQL, so
    the view must be skipped rather than written empty."""
    empty = write_manifest(sandbox.root, name="empty.tsv")

    assert consolidate.project_rows([empty]) == []
    consolidate.main(["--manifest", "empty.tsv"])

    _sql, rows = con.batches[0]
    assert rows == []
    assert not [s for s in con.statements if "view tokens" in s]
    assert "tokens view: 0 rows across 0 projects" in capsys.readouterr().out


@pytest.mark.parametrize("value", ["manifest.tsv", "sub/manifest.tsv"])
def test_a_relative_manifest_resolves_against_the_repo(sandbox, value):
    """ctp.py runs this script with cwd set to the cregit directory, so a
    relative value must not be read against the cwd."""
    assert consolidate.resolve_manifest(value) == sandbox.root / value


def test_an_absolute_manifest_is_used_as_given(sandbox, tmp_path):
    """An absolute path is the escape hatch for a manifest outside the repo."""
    elsewhere = tmp_path / "elsewhere" / "manifest.tsv"
    assert consolidate.resolve_manifest(str(elsewhere)) == elsewhere


# --------------------------------------------------------------------------- #
# the schema gate
# --------------------------------------------------------------------------- #

LEGACY_23_COLUMNS = consolidate.EXPECTED_COLUMNS[:22] + (("repo_tag", "VARCHAR"),)
RETYPED_COLUMNS = tuple((name, "VARCHAR" if name == "token_index" else typ)
                        for name, typ in consolidate.EXPECTED_COLUMNS)


@pytest.fixture
def schemas(monkeypatch):
    """Map a parquet file name to the schema consolidate.read_schema reports for it."""
    table: dict[str, tuple] = {}
    monkeypatch.setattr(consolidate, "read_schema", lambda path: list(table[Path(path).name]))
    return table


def schema_project(out: Path, schemas: dict, name: str, columns) -> Path:
    """A DONE project whose parquet reports `columns`."""
    make_project(out, name, stamp="rows=1\nbytes=2\n", parquet=True)
    schemas[f"{name}-dataset.parquet"] = columns
    return out / name / f"{name}-dataset.parquet"


def test_the_schema_contract_comes_from_validate_schema(sandbox):
    """The gate must follow a schema widening, so the contract is imported, not copied."""
    import validate_schema
    assert consolidate.EXPECTED_COLUMNS is validate_schema.EXPECTED_COLUMNS


@pytest.mark.parametrize("columns, drifted_width", [
    pytest.param(consolidate.EXPECTED_COLUMNS, None, id="matches"),
    pytest.param(LEGACY_23_COLUMNS, 23, id="legacy-23-columns"),
    # Same names, token_index as VARCHAR: it would union into silent nulls.
    pytest.param(RETYPED_COLUMNS, len(consolidate.EXPECTED_COLUMNS), id="wrong-type"),
])
def test_schema_split_keeps_only_a_parquet_that_matches_the_contract(
        sandbox, schemas, columns, drifted_width):
    path = str(schema_project(sandbox.out, schemas, "jq", columns))
    usable, drifted, unread = consolidate.schema_split([path])
    assert unread == []
    if drifted_width is None:
        assert (usable, drifted) == ([path], [])
    else:
        assert usable == []
        assert [(d[0], d[1], bool(d[2])) for d in drifted] == [(path, drifted_width, True)]


def test_every_mismatched_parquet_is_named_and_counted(sandbox, con, schemas, capsys):
    """Reporting only the first would hide the second."""
    write_manifest(sandbox.root, SM_ROW, ROW, LINUX_ROW, name="a.tsv")
    schema_project(sandbox.out, schemas, "dpdk__dpdk", consolidate.EXPECTED_COLUMNS)
    schema_project(sandbox.out, schemas, "jq", LEGACY_23_COLUMNS)
    schema_project(sandbox.out, schemas, "torvalds__linux", consolidate.EXPECTED_COLUMNS[:38])

    consolidate.main(["--manifest", "a.tsv"])

    assert "dpdk__dpdk-dataset.parquet" in con.find("create or replace view tokens")
    out = capsys.readouterr().out
    assert "schema mismatch, left out of the tokens view: 2 of 3" in out
    assert "jq-dataset.parquet (23 columns)" in out
    assert "torvalds__linux-dataset.parquet (38 columns)" in out
    assert f"the contract is {len(consolidate.EXPECTED_COLUMNS)} columns" in out
    assert "tokens view: 1,234,567 rows across 1 projects" in out


def write_parquet(path: Path, columns) -> Path:
    """A one-row parquet with exactly `columns`, (name, duckdb type) pairs."""
    import duckdb as real_duckdb

    select = ", ".join(
        (f"'{path.parent.name}' as {name}" if name == "repo_name"
         else f'cast(null as {typ}) as "{name}"')
        for name, typ in columns)
    real_duckdb.sql(f"copy (select {select}) to '{path}' (format parquet)")
    return path


@pytest.mark.skipif(DUCKDB_IS_STUBBED, reason="needs real duckdb to write parquet")
def test_main_leaves_a_real_mismatched_parquet_out_of_the_tokens_view(sandbox, con, capsys):
    """End to end through the real read_schema. One legacy file would otherwise
    abort read_parquet([...]) for every project."""
    write_manifest(sandbox.root, SM_ROW, ROW, name="a.tsv")
    for name, columns in (("dpdk__dpdk", consolidate.EXPECTED_COLUMNS), ("jq", LEGACY_23_COLUMNS)):
        workdir = make_project(sandbox.out, name, stamp="rows=1\nbytes=2\n")
        write_parquet(workdir / f"{name}-dataset.parquet", columns)

    consolidate.main(["--manifest", "a.tsv"])

    view = con.find("create or replace view tokens")
    assert "dpdk__dpdk-dataset.parquet" in view
    assert "jq-dataset.parquet" not in view
    assert "schema mismatch, left out of the tokens view: 1 of 2" in capsys.readouterr().out


def test_a_parquet_whose_schema_cannot_be_read_stays_in_and_is_reported(
        sandbox, con, capsys):
    """A file that cannot be read cannot be shown to disagree, so it stays in
    the view and is reported on stderr."""
    write_manifest(sandbox.root, ROW)
    make_project(sandbox.out, "jq", stamp="rows=1\nbytes=2\n", parquet=True)

    consolidate.main(["--manifest", "manifest.tsv"])

    assert "jq-dataset.parquet" in con.find("create or replace view tokens")
    captured = capsys.readouterr()
    assert "schema not read for 1 of 1 parquet(s)" in captured.err
    assert "jq-dataset.parquet" in captured.err
    assert "schema mismatch" not in captured.out


def test_project_rows_reads_a_pinned_six_column_row(sandbox):
    write_manifest(sandbox.root, ROW + "\t" + "a" * 40)
    assert [r[0] for r in consolidate.project_rows()] == ["jq"]


def test_project_rows_rejects_seven_fields(sandbox):
    write_manifest(sandbox.root, ROW + "\t" + "a" * 40 + "\textra")
    with pytest.raises(ValueError, match="5 or 6"):
        consolidate.project_rows()
