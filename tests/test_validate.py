"""Unit tests for validate.py, the post-run gate.

validate.py imports duckdb at module scope and duckdb is not installed in
.venv (it comes from `devenv shell`), so this file installs a stub `duckdb`
module in sys.modules before the import when the real one is absent. Each test
then patches `validate.duckdb.sql` with a recorder, so no parquet is ever read
and no query ever runs. The fixture parquet is a few bytes of padding under
tmp_path; only its size matters to the code under test.

The gate is what makes ctp idempotent: it writes the completion stamp, and ctp
skips any project that has one. So "no stamp on rejection" is a contract, not
an implementation detail.

Three tests run validate.py in a subprocess under `python -O`, with a tiny
stub duckdb module on PYTHONPATH. That is the only way to prove the checks
survive with assertions compiled out. Those runs read no parquet and open no
database either.
"""

from __future__ import annotations

import os
import subprocess
import sys
import types
from pathlib import Path

import pytest

if "duckdb" not in sys.modules:  # pragma: no cover - import plumbing
    try:
        import duckdb  # noqa: F401
    except ModuleNotFoundError:
        def _unpatched(*args, **kwargs):
            raise AssertionError(
                "stub duckdb was called: patch validate.duckdb.sql in the test")

        _stub = types.ModuleType("duckdb")
        _stub.__doc__ = "Test stub. Real duckdb lives in the devenv shell."
        _stub.connect = _unpatched
        _stub.sql = _unpatched
        sys.modules["duckdb"] = _stub

import validate  # noqa: E402


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #

COLUMNS = ("repo_name", "commit_id", "token", "token_type", "author")

SCRIPT = Path(validate.__file__).resolve()

# Enough duckdb for validate.py to import and run in a subprocess. It reports
# zero rows, which is the rejection the -O tests are after.
DUCKDB_STUB = '''
"""Test stub. Reads no parquet, opens no database."""


class _Result:
    def fetchone(self):
        return (0,)

    def fetchall(self):
        return []


def sql(query, params=None):
    return _Result()
'''


class FakeResult:
    def __init__(self, row=None, rows=None):
        self._row = row
        self._rows = rows or []

    def fetchone(self):
        return self._row

    def fetchall(self):
        return self._rows


class FakeSql:
    """Recording stand-in for duckdb.sql. Reads nothing."""

    def __init__(self, count=4321, columns=COLUMNS):
        self.queries: list[str] = []
        self.params: list[list] = []
        self.count = count
        self.columns = columns

    def __call__(self, query, params=None):
        self.queries.append(query)
        self.params.append(params)
        if query.startswith("select count(*)"):
            return FakeResult(row=(self.count,))
        if query.startswith("describe"):
            return FakeResult(rows=[(c, "VARCHAR", None) for c in self.columns])
        raise AssertionError(f"unexpected query: {query}")


def run_in_subprocess(tmp_path, *argv, flags=("-O",)):
    """Run validate.py for real, with a stub duckdb ahead of any real one."""
    stubdir = tmp_path / "stub"
    stubdir.mkdir(exist_ok=True)
    (stubdir / "duckdb.py").write_text(DUCKDB_STUB)
    return subprocess.run(
        [sys.executable, *flags, str(SCRIPT), *map(str, argv)],
        capture_output=True, text=True,
        env={**os.environ, "PYTHONPATH": str(stubdir)})


@pytest.fixture
def parquet(tmp_path):
    """A file that is big enough to pass the size floor."""
    path = tmp_path / "jq-dataset.parquet"
    path.write_bytes(b"PAR1" + b"\0" * 20_000)
    return path


@pytest.fixture
def stamp(tmp_path):
    return tmp_path / "jq.validated"


@pytest.fixture
def invoke(monkeypatch):
    """Run validate.main() with a chosen argv and a chosen duckdb.sql double."""
    def _invoke(*argv, sql=None):
        fake = sql if sql is not None else FakeSql()
        monkeypatch.setattr(validate.duckdb, "sql", fake, raising=False)
        monkeypatch.setattr(validate.sys, "argv", ["validate.py", *map(str, argv)])
        validate.main()
        return fake
    return _invoke


# --------------------------------------------------------------------------- #
# the happy path
# --------------------------------------------------------------------------- #

def test_a_good_parquet_writes_the_completion_stamp(invoke, parquet, stamp):
    """Idempotence contract: the stamp is the only thing that makes ctp skip a
    finished project, and it carries the row and byte counts the ledger uses."""
    invoke(parquet, stamp, sql=FakeSql(count=4321))
    assert stamp.read_text() == f"rows=4321\nbytes={parquet.stat().st_size}\n"


def test_the_gate_reports_rows_bytes_and_columns(invoke, parquet, stamp, capsys):
    """The phase log is the record of what was accepted, so it must name the
    row count and the schema."""
    invoke(parquet, stamp)
    out = capsys.readouterr().out
    assert f"OK rows=4321 bytes={parquet.stat().st_size}" in out
    assert "cols=['repo_name', 'commit_id', 'token', 'token_type', 'author']" in out


def test_the_gate_counts_rows_and_describes_the_named_parquet(invoke, parquet, stamp):
    """Both queries must target the file given on the command line; querying
    anything else would validate the wrong project. The file arrives as a bound
    parameter, so the SQL text is the same for every project."""
    sql = invoke(parquet, stamp)
    assert sql.queries == [
        "select count(*) from read_parquet(?)",
        "describe select * from read_parquet(?)",
    ]
    assert sql.params == [[str(parquet)], [str(parquet)]]


def test_the_stamp_is_overwritten_not_appended(invoke, parquet, stamp):
    """A re-validated project must end with one row= line, not a growing file."""
    stamp.write_text("rows=1\nbytes=1\nstale=yes\n")
    invoke(parquet, stamp)
    assert stamp.read_text().splitlines() == ["rows=4321", f"bytes={parquet.stat().st_size}"]


# --------------------------------------------------------------------------- #
# rejection paths
# --------------------------------------------------------------------------- #

def test_a_suspiciously_small_parquet_is_rejected(invoke, tmp_path, stamp, capsys):
    """A truncated parquet means the tokenizer died late. Accepting it would
    stamp a broken project DONE and retain.py would then delete its memo/.

    This used to expect an AssertionError. Before the fix the check was a bare
    `assert`, so `python -O` removed it; now it is an explicit check that
    prints the reason and exits with the rejection code.
    """
    small = tmp_path / "jq-dataset.parquet"
    small.write_bytes(b"PAR1")
    with pytest.raises(SystemExit) as exc:
        invoke(small, stamp)
    assert exc.value.code == validate.EXIT_REJECTED
    assert "FAIL parquet suspiciously small: 4 bytes" in capsys.readouterr().err
    assert not stamp.exists()


def test_an_empty_parquet_is_rejected(invoke, parquet, stamp, capsys):
    """A parquet with a valid header and no rows passes the size floor, so the
    row count is a separate check.

    This used to expect an AssertionError; the check is now explicit so that
    `python -O` cannot remove it.
    """
    with pytest.raises(SystemExit) as exc:
        invoke(parquet, stamp, sql=FakeSql(count=0))
    assert exc.value.code == validate.EXIT_REJECTED
    assert "FAIL parquet has zero rows" in capsys.readouterr().err
    assert not stamp.exists()


def test_the_size_floor_is_exclusive(invoke, tmp_path, stamp, capsys):
    """Documents the boundary: exactly 10,000 bytes is rejected."""
    edge = tmp_path / "jq-dataset.parquet"
    edge.write_bytes(b"\0" * 10_000)
    with pytest.raises(SystemExit) as exc:
        invoke(edge, stamp)
    assert exc.value.code == validate.EXIT_REJECTED
    assert "10000 bytes" in capsys.readouterr().err


def test_a_missing_parquet_raises_before_any_query(invoke, tmp_path, stamp):
    """A project whose pipeline produced no dataset must fail the gate, and
    must not leave a stamp behind."""
    sql = FakeSql()
    with pytest.raises(FileNotFoundError):
        invoke(tmp_path / "absent.parquet", stamp, sql=sql)
    assert sql.queries == []
    assert not stamp.exists()


def test_a_rejected_parquet_leaves_no_stamp_so_ctp_retries(invoke, parquet, stamp):
    """Locks in the resume contract: rejection must be indistinguishable from
    'never validated', so the next run retries the project."""
    with pytest.raises(SystemExit):
        invoke(parquet, stamp, sql=FakeSql(count=0))
    assert list(stamp.parent.glob("*.validated")) == []


@pytest.mark.parametrize("argv", [
    pytest.param((), id="no-arguments"),
    pytest.param(("only-the-parquet",), id="missing-stamp-path"),
    pytest.param(("parquet", "stamp", "extra"), id="extra-arguments"),
])
def test_a_wrong_invocation_prints_usage_and_exits_two(monkeypatch, capsys, argv):
    """Before the fix, validate.py read sys.argv[1] blind and a call with no
    arguments died with a bare IndexError. It now prints usage and exits with a
    code of its own, distinct from a rejected parquet.

    This test used to assert the IndexError.
    """
    monkeypatch.setattr(validate.sys, "argv", ["validate.py", *argv])
    with pytest.raises(SystemExit) as exc:
        validate.main()
    assert exc.value.code == validate.EXIT_USAGE
    assert exc.value.code != validate.EXIT_REJECTED
    assert "usage: validate.py <dataset.parquet> <stamp-file>" in capsys.readouterr().err


# --------------------------------------------------------------------------- #
# path handling
# --------------------------------------------------------------------------- #

def test_the_parquet_path_is_bound_not_interpolated(invoke, parquet, stamp):
    """Before the fix the path was pasted into the query text. It is now a
    bound parameter, so no project name reaches the SQL parser."""
    sql = invoke(parquet, stamp)
    assert all(str(parquet) not in query for query in sql.queries)
    assert sql.params[0] == [str(parquet)]


def test_a_quote_in_the_path_does_not_break_the_sql(invoke, tmp_path):
    """A path with a quote must still produce valid SQL and validate the file
    that was named.

    This was an expected failure. Before the fix, validate.py built SQL with
    f"...from '{parquet}'", so a single quote in the path produced an
    unbalanced string literal and the gate crashed instead of validating.
    Project names come from the manifest, so one manifest row was enough.
    """
    odd = tmp_path / "we'ird-dataset.parquet"
    odd.write_bytes(b"PAR1" + b"\0" * 20_000)
    stamp = tmp_path / "we'ird.validated"

    sql = invoke(odd, stamp)

    assert all(query.count("'") % 2 == 0 for query in sql.queries)
    assert all("we'ird" not in query for query in sql.queries)
    assert sql.params == [[str(odd)], [str(odd)]]
    assert stamp.read_text().startswith("rows=4321\n")


def test_a_sql_fragment_in_the_path_is_read_as_a_literal_path(invoke, tmp_path):
    """A crafted project name must be data, never SQL. Before the fix the
    fragment landed in the query text and would have run."""
    evil = tmp_path / "'; drop table x; --"
    evil.mkdir()
    crafted = evil / "jq-dataset.parquet"
    crafted.write_bytes(b"PAR1" + b"\0" * 20_000)
    stamp = tmp_path / "jq.validated"

    sql = invoke(crafted, stamp)

    assert all("drop table" not in query for query in sql.queries)
    assert sql.queries == [
        "select count(*) from read_parquet(?)",
        "describe select * from read_parquet(?)",
    ]
    assert sql.params == [[str(crafted)], [str(crafted)]]
    assert stamp.exists()


# --------------------------------------------------------------------------- #
# with the assertions compiled out
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("size, reason", [
    pytest.param(4, "parquet suspiciously small: 4 bytes", id="too-small"),
    pytest.param(20_000, "parquet has zero rows", id="zero-rows"),
])
def test_a_bad_parquet_still_fails_under_o(tmp_path, size, reason):
    """Before the fix both checks were bare `assert` statements, so under
    `python -O` they vanished and any parquet was stamped DONE. The stub duckdb
    reports zero rows, so both rejections are reachable with -O on."""
    bad = tmp_path / "jq-dataset.parquet"
    bad.write_bytes(b"\0" * size)
    stamp = tmp_path / "jq.validated"

    done = run_in_subprocess(tmp_path, bad, stamp)

    assert done.returncode == 1
    assert reason in done.stderr
    assert "AssertionError" not in done.stderr + done.stdout
    assert not stamp.exists()


def test_no_arguments_under_o_prints_usage_without_a_traceback(tmp_path):
    """The operator must get one line of usage, not an IndexError traceback."""
    done = run_in_subprocess(tmp_path)

    assert done.returncode == validate.EXIT_USAGE
    assert done.stderr.strip() == "usage: validate.py <dataset.parquet> <stamp-file>"
    assert "Traceback" not in done.stderr
