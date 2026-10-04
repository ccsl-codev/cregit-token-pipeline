"""Unit tests for validate.py, the post-run gate. Each test patches duckdb.sql
with a recorder, so no parquet is read. The stamp is what makes ctp skip a
project, so "no stamp on rejection" is a contract."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

import validate
from validate_schema import EXPECTED_COLUMNS


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #


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

    def __init__(self, count=4321, columns=EXPECTED_COLUMNS):
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
            return FakeResult(rows=[(c, t, None) for c, t in self.columns])
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


def test_the_gate_reports_rows_and_bytes(invoke, parquet, stamp, capsys):
    """The phase log is the record of what was accepted."""
    invoke(parquet, stamp)
    assert f"OK rows=4321 bytes={parquet.stat().st_size}" in capsys.readouterr().out


def test_a_parquet_that_drifts_from_the_contract_is_rejected(invoke, parquet, stamp, capsys):
    """A stamp lets --drop-memo delete memo/, so a drifted file must not get one."""
    drifted = (*EXPECTED_COLUMNS[:-1], (EXPECTED_COLUMNS[-1][0], "BLOB"))
    with pytest.raises(SystemExit) as exc:
        invoke(parquet, stamp, sql=FakeSql(columns=drifted))
    assert exc.value.code == validate.EXIT_REJECTED
    assert "schema drift (1)" in capsys.readouterr().err
    assert not stamp.exists()


def test_the_stamp_is_overwritten_not_appended(invoke, parquet, stamp):
    """A re-validated project must end with one row= line, not a growing file."""
    stamp.write_text("rows=1\nbytes=1\nstale=yes\n")
    invoke(parquet, stamp)
    assert stamp.read_text().splitlines() == ["rows=4321", f"bytes={parquet.stat().st_size}"]


# --------------------------------------------------------------------------- #
# rejection paths
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("size, count, reason", [
    # A truncated parquet means the tokenizer died late; retain.py would delete memo/.
    pytest.param(4, 4321, "FAIL parquet suspiciously small: 4 bytes", id="too-small"),
    pytest.param(10_000, 4321, "FAIL parquet suspiciously small: 10000 bytes",
                 id="floor-is-exclusive"),
    pytest.param(20_000, 0, "FAIL parquet has zero rows", id="zero-rows"),
])
def test_a_bad_parquet_is_rejected_and_left_unstamped(
        invoke, tmp_path, stamp, capsys, size, count, reason):
    """No stamp means the next ctp pass retries the project."""
    bad = tmp_path / "jq-dataset.parquet"
    bad.write_bytes(b"\0" * size)
    with pytest.raises(SystemExit) as exc:
        invoke(bad, stamp, sql=FakeSql(count=count))
    assert exc.value.code == validate.EXIT_REJECTED
    assert reason in capsys.readouterr().err
    assert not stamp.exists()


def test_a_missing_parquet_raises_before_any_query(invoke, tmp_path, stamp):
    """A project whose pipeline produced no dataset must fail the gate, and
    must not leave a stamp behind."""
    sql = FakeSql()
    with pytest.raises(FileNotFoundError):
        invoke(tmp_path / "absent.parquet", stamp, sql=sql)
    assert sql.queries == []
    assert not stamp.exists()


@pytest.mark.parametrize("argv", [
    pytest.param((), id="no-arguments"),
    pytest.param(("only-the-parquet",), id="missing-stamp-path"),
    pytest.param(("parquet", "stamp", "extra"), id="extra-arguments"),
])
def test_a_wrong_invocation_prints_usage_and_exits_two(monkeypatch, capsys, argv):
    """Usage, and an exit code distinct from a rejected parquet."""
    monkeypatch.setattr(validate.sys, "argv", ["validate.py", *argv])
    with pytest.raises(SystemExit) as exc:
        validate.main()
    assert exc.value.code == validate.EXIT_USAGE
    assert exc.value.code != validate.EXIT_REJECTED
    assert "usage: validate.py <dataset.parquet> <stamp-file>" in capsys.readouterr().err


# --------------------------------------------------------------------------- #
# path handling
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("folder", ["plain", "we'ird", "'; drop table x; --"])
def test_the_parquet_path_is_bound_not_interpolated(invoke, tmp_path, folder):
    """Project names come from the manifest, so the path must be a bound
    parameter: the SQL text is the same for every project."""
    (tmp_path / folder).mkdir()
    parquet = tmp_path / folder / "jq-dataset.parquet"
    parquet.write_bytes(b"PAR1" + b"\0" * 20_000)
    stamp = tmp_path / "jq.validated"

    sql = invoke(parquet, stamp)

    assert sql.queries == [
        "select count(*) from read_parquet(?)",
        "describe select * from read_parquet(?)",
    ]
    assert sql.params == [[str(parquet)], [str(parquet)]]
    assert stamp.read_text().startswith("rows=4321\n")


# --------------------------------------------------------------------------- #
# with the assertions compiled out
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("size, reason", [
    pytest.param(4, "parquet suspiciously small: 4 bytes", id="too-small"),
    pytest.param(20_000, "parquet has zero rows", id="zero-rows"),
])
def test_a_bad_parquet_still_fails_under_o(tmp_path, size, reason):
    """`python -O` must not remove the checks. The stub duckdb reports zero
    rows, so both rejections are reachable."""
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
