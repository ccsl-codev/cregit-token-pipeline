"""Unit tests for consolidate.py, the DuckDB index builder.

consolidate.py imports duckdb at module scope, and duckdb is not installed in
.venv (it comes from `devenv shell`). To keep the suite runnable and offline,
this file installs a stub `duckdb` module in sys.modules before the import when
the real one is absent. Every test then patches `consolidate.duckdb.connect`
with a recorder, so no database file is ever opened. The repo's own ctp.duckdb
is never touched.

The stub only makes the import succeed. Nothing here asserts DuckDB semantics;
the tests assert the SQL consolidate.py emits and the row tuples it derives.
"""

from __future__ import annotations

import fcntl
import sys
import types
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

if "duckdb" not in sys.modules:  # pragma: no cover - import plumbing
    try:
        import duckdb  # noqa: F401
    except ModuleNotFoundError:
        def _unpatched(*args, **kwargs):
            raise AssertionError(
                "stub duckdb was called: patch consolidate.duckdb.connect "
                "or validate.duckdb.sql in the test")

        _stub = types.ModuleType("duckdb")
        _stub.__doc__ = "Test stub. Real duckdb lives in the devenv shell."
        _stub.connect = _unpatched
        _stub.sql = _unpatched
        sys.modules["duckdb"] = _stub

import consolidate  # noqa: E402

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


@contextmanager
def held_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    fh = path.open("w")
    fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        yield
    finally:
        fcntl.flock(fh, fcntl.LOCK_UN)
        fh.close()


def write_manifest(root: Path, *lines: str) -> Path:
    path = root / "manifest.tsv"
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
    return SimpleNamespace(root=tmp_path, out=out)


@pytest.fixture
def con(monkeypatch):
    """Install a recording connection and return it."""
    fake = FakeCon(count=1_234_567)
    monkeypatch.setattr(consolidate.duckdb, "connect",
                        lambda path: fake, raising=False)
    return fake


# --------------------------------------------------------------------------- #
# lock_held
# --------------------------------------------------------------------------- #

def test_lock_held_is_false_without_a_lockfile(sandbox):
    """A project that never started must read QUEUED, not RUNNING."""
    assert consolidate.lock_held(sandbox.out / "jq" / ".lock") is False


def test_lock_held_is_false_for_a_stale_lockfile(sandbox):
    """A lockfile left behind by a killed run holds no flock."""
    lockfile = make_project(sandbox.out, "jq") / ".lock"
    lockfile.touch()
    assert consolidate.lock_held(lockfile) is False


def test_lock_held_is_true_while_another_process_holds_it(sandbox):
    """The index must show a live tokenizer as RUNNING, never as FAILED."""
    lockfile = make_project(sandbox.out, "jq") / ".lock"
    with held_lock(lockfile):
        assert consolidate.lock_held(lockfile) is True


# --------------------------------------------------------------------------- #
# project_rows
# --------------------------------------------------------------------------- #

def test_project_rows_builds_one_seven_column_tuple_per_project(sandbox):
    """The tuple shape must match the projects table, or executemany fails."""
    write_manifest(sandbox.root, ROW)
    make_project(sandbox.out, "jq", stamp="rows=1234\nbytes=5678\n", parquet=True)

    rows = consolidate.project_rows()
    assert rows == [("jq", "https://github.com/jqlang/jq.git", "community", "S",
                     "DONE", 1234, str(sandbox.out / "jq" / "jq-dataset.parquet"))]


def test_project_rows_classifies_all_four_states(sandbox):
    """The index is the dashboard. A wrong state misreports corpus progress."""
    write_manifest(
        sandbox.root,
        "done\thttps://d.git\tcommunity\t\\.[ch]$\tS",
        "running\thttps://r.git\tcommunity\t\\.[ch]$\tM",
        "failed\thttps://f.git\tenterprise\t\\.[ch]$\tL",
        "queued\thttps://q.git\tcommunity\t\\.[ch]$\tS")
    make_project(sandbox.out, "done", stamp="rows=7\nbytes=8\n", parquet=True)
    make_project(sandbox.out, "failed")
    lockfile = make_project(sandbox.out, "running") / ".lock"

    with held_lock(lockfile):
        states = {r[0]: r[4] for r in consolidate.project_rows()}

    assert states == dict(done="DONE", running="RUNNING",
                          failed="FAILED", queued="QUEUED")


def test_project_rows_skips_comments_and_blank_lines(sandbox):
    """The manifest header is a comment; treating it as data adds a fake project."""
    write_manifest(sandbox.root, "# name  url  category  filter  class", "", "  ", ROW)
    assert [r[0] for r in consolidate.project_rows()] == ["jq"]


def test_project_rows_on_an_empty_manifest_returns_nothing(sandbox):
    """An empty corpus is a valid state, not a crash."""
    write_manifest(sandbox.root)
    assert consolidate.project_rows() == []


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
    """A stamp without a parquet must not put a dead path in the tokens view."""
    write_manifest(sandbox.root, ROW)
    make_project(sandbox.out, "jq", stamp="rows=9\nbytes=9\n", parquet=False)
    (name, _url, _cat, _cls, state, n_rows, parquet) = consolidate.project_rows()[0]
    assert (name, state, n_rows, parquet) == ("jq", "DONE", 9, None)


def test_project_rows_counts_zero_for_a_stamp_without_a_rows_key(sandbox):
    """An old or truncated stamp must not crash the rebuild."""
    write_manifest(sandbox.root, ROW)
    make_project(sandbox.out, "jq", stamp="malformed stamp\n", parquet=True)
    assert consolidate.project_rows()[0][5] == 0


def test_project_rows_ignores_extra_stamp_keys(sandbox):
    """The stamp is a key=value bag; unknown keys are tolerated."""
    write_manifest(sandbox.root, ROW)
    make_project(sandbox.out, "jq",
                 stamp="rows=5\nbytes=6\nvalidated_by=ctp\n", parquet=True)
    assert consolidate.project_rows()[0][5] == 5


@pytest.mark.xfail(strict=True, reason=(
    "Defect: int(kv.get('rows', 0)) is unguarded, so a stamp whose rows value "
    "is not an integer raises ValueError and aborts the whole rebuild instead "
    "of reporting that one project as unusable."))
def test_project_rows_survives_a_non_numeric_rows_value(sandbox):
    """One bad stamp must not stop the derived index for 1,423 projects."""
    write_manifest(sandbox.root, ROW)
    make_project(sandbox.out, "jq", stamp="rows=many\nbytes=6\n", parquet=True)
    assert consolidate.project_rows()[0][5] in (0, None)


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
    sql, rows = con.batches[0]
    assert sql == "insert into projects values (?, ?, ?, ?, ?, ?, ?)"
    assert rows == [("jq", "https://github.com/jqlang/jq.git", "community", "S",
                     "DONE", 1234, str(sandbox.out / "jq" / "jq-dataset.parquet"))]


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
