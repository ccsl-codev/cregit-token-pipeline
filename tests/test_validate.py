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
"""

from __future__ import annotations

import sys
import types

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
        self.count = count
        self.columns = columns

    def __call__(self, query):
        self.queries.append(query)
        if query.startswith("select count(*)"):
            return FakeResult(row=(self.count,))
        if query.startswith("describe"):
            return FakeResult(rows=[(c, "VARCHAR", None) for c in self.columns])
        raise AssertionError(f"unexpected query: {query}")


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
    anything else would validate the wrong project."""
    sql = invoke(parquet, stamp)
    assert sql.queries == [
        f"select count(*) from '{parquet}'",
        f"describe select * from '{parquet}'",
    ]


def test_the_stamp_is_overwritten_not_appended(invoke, parquet, stamp):
    """A re-validated project must end with one row= line, not a growing file."""
    stamp.write_text("rows=1\nbytes=1\nstale=yes\n")
    invoke(parquet, stamp)
    assert stamp.read_text().splitlines() == ["rows=4321", f"bytes={parquet.stat().st_size}"]


# --------------------------------------------------------------------------- #
# rejection paths
# --------------------------------------------------------------------------- #

def test_a_suspiciously_small_parquet_is_rejected(invoke, tmp_path, stamp):
    """A truncated parquet means the tokenizer died late. Accepting it would
    stamp a broken project DONE and retain.py would then delete its memo/."""
    small = tmp_path / "jq-dataset.parquet"
    small.write_bytes(b"PAR1")
    with pytest.raises(AssertionError, match="parquet suspiciously small: 4 bytes"):
        invoke(small, stamp)
    assert not stamp.exists()


def test_an_empty_parquet_is_rejected(invoke, parquet, stamp):
    """A parquet with a valid header and no rows passes the size floor, so the
    row count is a separate check."""
    with pytest.raises(AssertionError, match="parquet has zero rows"):
        invoke(parquet, stamp, sql=FakeSql(count=0))
    assert not stamp.exists()


def test_the_size_floor_is_exclusive(invoke, tmp_path, stamp):
    """Documents the boundary: exactly 10,000 bytes is rejected."""
    edge = tmp_path / "jq-dataset.parquet"
    edge.write_bytes(b"\0" * 10_000)
    with pytest.raises(AssertionError, match="10000 bytes"):
        invoke(edge, stamp)


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
    with pytest.raises(AssertionError):
        invoke(parquet, stamp, sql=FakeSql(count=0))
    assert list(stamp.parent.glob("*.validated")) == []


@pytest.mark.parametrize("argv", [
    pytest.param((), id="no-arguments"),
    pytest.param(("only-the-parquet",), id="missing-stamp-path"),
])
def test_missing_arguments_raise_indexerror(monkeypatch, argv):
    """Documents current behaviour: validate.py reads sys.argv positionally and
    offers no usage message, so a wrong invocation shows a bare IndexError."""
    monkeypatch.setattr(validate.sys, "argv", ["validate.py", *argv])
    with pytest.raises(IndexError):
        validate.main()


# --------------------------------------------------------------------------- #
# path handling
# --------------------------------------------------------------------------- #

def test_the_parquet_path_is_interpolated_into_the_sql_verbatim(invoke, parquet, stamp):
    """Documents current behaviour: the path is pasted into the query with no
    escaping and no parameter binding."""
    sql = invoke(parquet, stamp)
    assert str(parquet) in sql.queries[0]


@pytest.mark.xfail(strict=True, reason=(
    "Defect: validate.py builds SQL with f\"...from '{parquet}'\", so a path "
    "containing a single quote produces an unbalanced string literal. Project "
    "names come from the manifest, so a manifest row is enough to break the "
    "gate; the same hole would let a crafted path inject SQL. Needs a bound "
    "parameter or a doubled-quote escape."))
def test_a_quote_in_the_path_does_not_break_the_sql(monkeypatch, tmp_path):
    """A path with a quote must still produce a balanced SQL string literal."""
    odd = tmp_path / "we'ird-dataset.parquet"
    odd.write_bytes(b"PAR1" + b"\0" * 20_000)
    sql = FakeSql()
    monkeypatch.setattr(validate.duckdb, "sql", sql, raising=False)
    monkeypatch.setattr(validate.sys, "argv",
                        ["validate.py", str(odd), str(tmp_path / "s.validated")])
    validate.main()
    assert sql.queries[0].count("'") % 2 == 0
