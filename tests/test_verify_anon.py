"""Tests for verify_anon.py, the release gate for anonymized parquets.

verify_anon.py defers `import duckdb` into connect(), so importing the module
never needs duckdb. Everything driven through check_file()/main() below runs
with a FakeCon: a stand-in for a duckdb connection that answers the exact two
query shapes string_columns()/distinct_values() send, keyed by the literal
path string, and opens no real file. That is not a weaker test of check_file
or main -- both are the real functions from verify_anon.py, only the
connection object is fake -- and it means these tests run in .venv, which has
no duckdb, same as tests/test_consolidate.py's non-@requires_duckdb tests.

A small @requires_duckdb section at the end proves the real SQL against a real
parquet: that connect() opens a working connection, and that string_columns()
and distinct_values() read what check_file() assumes they read. It is skipped
here and runs only in the devenv shell that has duckdb.

Task 3's smell: `bucket = notes if content else failures` is computed but the
MISSING_MARKER check and the exact-shape check both ignore it and append to
`failures` directly; only the trailing REAL_ADDRESS check honors `bucket`. In
the shipped constants EXACT_SHAPE and CONTENT_COLUMNS never share a column, so
the exact-shape-in-a-content-column branch is only reachable by construction;
one test below forces that overlap with monkeypatch to pin what the code
actually does there, not what today's data happens to hide.
"""
from __future__ import annotations

import re
import sys

import pytest

import verify_anon as V

# tests/test_consolidate.py installs a stub `duckdb` module in sys.modules when
# the real package is absent, so it can import consolidate.py. That stub can
# already be in sys.modules by the time this file loads (pytest collects
# test_consolidate.py first, alphabetically), so a plain `import duckdb` here
# would find the stub and wrongly conclude duckdb is available. Detect the
# stub the same way test_consolidate.py's own DUCKDB_IS_STUBBED flag does.
if "duckdb" in sys.modules:
    DUCKDB_AVAILABLE = not getattr(
        sys.modules["duckdb"], "__doc__", "").startswith("Test stub")
else:
    try:
        import duckdb as _duckdb  # noqa: F401
        DUCKDB_AVAILABLE = True
    except ModuleNotFoundError:
        DUCKDB_AVAILABLE = False

requires_duckdb = pytest.mark.skipif(
    not DUCKDB_AVAILABLE, reason="needs real duckdb (devenv shell) to write parquet")


# --------------------------------------------------------------------------- #
# FakeCon -- answers string_columns()/distinct_values() without a database
# --------------------------------------------------------------------------- #

class FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class FakeCon:
    """One fake parquet per path: a column list and the distinct values in it.

    Opens no file. execute() reads the query text only to tell a DESCRIBE from
    a value query, and to find the column name a value query names -- it does
    not run SQL.
    """

    def __init__(self):
        self.files: dict[str, dict] = {}

    def add_file(self, path, columns, values):
        """columns: [(name, is_list), ...]. values: {name: [str, ...]}."""
        self.files[str(path)] = {"columns": columns, "values": values}

    def execute(self, query, params=None):
        path = str(params[0]) if params else None
        record = self.files[path]
        if query.strip().lower().startswith("describe"):
            rows = [(name, "VARCHAR[]" if is_list else "VARCHAR")
                    for name, is_list in record["columns"]]
            return FakeResult(rows)
        m = re.search(r"unnest\((\w+)\)", query)
        if m is None:
            m = re.search(r"distinct (\w+) from", query)
        col = m.group(1)
        return FakeResult([(v,) for v in record["values"].get(col, [])])


@pytest.fixture
def con(monkeypatch):
    """Install a FakeCon in place of verify_anon.connect(). Opens no database."""
    fake = FakeCon()
    monkeypatch.setattr(V, "connect", lambda: fake)
    return fake


def touch_parquet(con, tmp_path, name, columns, values):
    """Create an empty file so os.listdir finds it, and register it with con.

    check_file() never reads the file's bytes -- FakeCon answers by path -- so
    an empty file is enough to satisfy main()'s directory walk.
    """
    path = tmp_path / f"{name}.parquet"
    path.write_bytes(b"")
    con.add_file(str(path), columns, values)
    return path


# --------------------------------------------------------------------------- #
# Pseudonym shapes -- pure regexes, no file, no database
# --------------------------------------------------------------------------- #

def test_pseudo_local_accepts_the_shape_it_defines():
    assert re.fullmatch(V.PSEUDO_LOCAL, "author_1")
    assert re.fullmatch(V.PSEUDO_LOCAL, "author_0042")


def test_pseudo_local_rejects_the_right_shape_with_the_wrong_prefix():
    assert re.fullmatch(V.PSEUDO_LOCAL, "person_0042") is None


def test_pseudo_local_rejects_the_right_prefix_with_no_digits():
    assert re.fullmatch(V.PSEUDO_LOCAL, "author_") is None


def test_pseudo_local_rejects_an_empty_string():
    assert re.fullmatch(V.PSEUDO_LOCAL, "") is None


def test_pseudo_name_accepts_the_shape_it_defines():
    assert re.fullmatch(V.PSEUDO_NAME, "Author 1")
    assert re.fullmatch(V.PSEUDO_NAME, "Author 0042")


def test_pseudo_name_rejects_the_right_shape_with_the_wrong_case_prefix():
    """'author 0042' is PSEUDO_ID's shape, not PSEUDO_NAME's. The two pseudonym
    spaces must stay distinguishable by case alone."""
    assert re.fullmatch(V.PSEUDO_NAME, "author 0042") is None


def test_pseudo_name_rejects_the_right_prefix_with_no_digits():
    assert re.fullmatch(V.PSEUDO_NAME, "Author ") is None


def test_pseudo_name_rejects_an_empty_string():
    assert re.fullmatch(V.PSEUDO_NAME, "") is None


def test_pseudo_id_accepts_the_shape_it_defines():
    assert re.fullmatch(V.PSEUDO_ID, "author 1")
    assert re.fullmatch(V.PSEUDO_ID, "author 0042")


def test_pseudo_id_rejects_the_right_shape_with_the_wrong_case_prefix():
    assert re.fullmatch(V.PSEUDO_ID, "Author 0042") is None


def test_pseudo_id_rejects_the_right_prefix_with_no_digits():
    assert re.fullmatch(V.PSEUDO_ID, "author ") is None


def test_pseudo_id_rejects_an_empty_string():
    assert re.fullmatch(V.PSEUDO_ID, "") is None


# --------------------------------------------------------------------------- #
# REAL_ADDRESS, MISSING_MARKER, CONTENT_COLUMNS -- pure constants
# --------------------------------------------------------------------------- #

def test_real_address_matches_an_ordinary_email():
    assert V.REAL_ADDRESS.search("contact bob@example.com now") is not None


def test_real_address_does_not_match_a_pseudonym_email():
    assert V.REAL_ADDRESS.search("author_0001@example.com") is None


def test_real_address_does_not_match_plain_text():
    assert V.REAL_ADDRESS.search("no address here") is None


def test_missing_marker_is_the_documented_literal():
    assert V.MISSING_MARKER == "(ANON-MISSING)"


def test_content_columns_covers_source_and_repo_namespace():
    for col in ("source_text", "token_value", "file_path", "repo_name",
                "clone_url", "owner", "repo", "roster_name", "fact",
                "file_mask"):
        assert col in V.CONTENT_COLUMNS


def test_content_columns_and_exact_shape_never_share_a_column():
    """A column in both sets would make the exact-shape check's ignored-bucket
    smell (see check_file tests below) reachable with real data, not only by
    construction. Today the sets are disjoint; this pins that fact."""
    assert not (V.CONTENT_COLUMNS & set(V.EXACT_SHAPE))


# --------------------------------------------------------------------------- #
# check_file -- Task 1: the contract
# --------------------------------------------------------------------------- #

def test_check_file_passes_a_file_whose_identity_columns_are_all_pseudonyms(con):
    con.add_file("clean.parquet", [
        ("author_name", False), ("author_email", False), ("personid", False),
    ], {
        "author_name": ["Author 0001", "Author 0002"],
        "author_email": ["author_0001@example.com", "author_0002@example.com"],
        "personid": ["author 0001", "author 0002"],
    })

    checked, failed, notes = V.check_file(con, "clean.parquet")

    assert checked == 6
    assert failed == 0
    assert notes == []


def test_check_file_fails_a_real_email_in_an_identity_column(con):
    con.add_file("leaky.parquet", [("author_email", False)], {
        "author_email": ["bob@example.com"],
    })

    _checked, failed, notes = V.check_file(con, "leaky.parquet")

    assert failed == 1
    assert notes == []


def test_check_file_notes_but_does_not_fail_a_real_email_in_a_content_column(con):
    assert "source_text" in V.CONTENT_COLUMNS
    con.add_file("content.parquet", [("source_text", False)], {
        "source_text": ["// contact bob@example.com for details"],
    })

    _checked, failed, notes = V.check_file(con, "content.parquet")

    assert failed == 0
    assert len(notes) == 1
    assert notes[0][0] == "source_text"


def test_check_file_fails_a_real_address_in_a_column_with_no_exact_shape(con):
    """Not every identity-bearing column is in EXACT_SHAPE. The fallback is the
    REAL_ADDRESS scan, whose bucket is `failures` for any non-content column."""
    assert "reviewer_email" not in V.CONTENT_COLUMNS
    assert "reviewer_email" not in V.EXACT_SHAPE
    con.add_file("generic.parquet", [("reviewer_email", False)], {
        "reviewer_email": ["carol@example.com"],
    })

    _checked, failed, notes = V.check_file(con, "generic.parquet")

    assert failed == 1
    assert notes == []


def test_check_file_fails_on_the_missing_marker_in_an_identity_column(con):
    con.add_file("bug.parquet", [("author_email", False)], {
        "author_email": [f"{V.MISSING_MARKER}@example.com"],
    })

    _checked, failed, notes = V.check_file(con, "bug.parquet")

    assert failed == 1
    assert notes == []


def test_check_file_handles_list_columns_the_same_as_scalar_columns(con):
    con.add_file("lists.parquet", [("footer_person_names", True)], {
        "footer_person_names": ["Author 0001", "Real Person"],
    })

    checked, failed, notes = V.check_file(con, "lists.parquet")

    assert checked == 2
    assert failed == 1  # "Real Person" does not match PSEUDO_NAME
    assert notes == []


def test_check_file_reports_a_count_of_additional_notes_beyond_five(con, capsys):
    con.add_file("many.parquet", [("source_text", False)], {
        "source_text": [f"user{i}@example.com" for i in range(7)],
    })

    _checked, failed, notes = V.check_file(con, "many.parquet")

    assert failed == 0
    assert len(notes) == 7
    assert "and 2 more in content columns" in capsys.readouterr().out


# --------------------------------------------------------------------------- #
# check_file -- Task 3: pin the bucket smell, both ignored paths and the one
# path that honors bucket
# --------------------------------------------------------------------------- #

def test_missing_marker_fails_even_in_a_content_column_bucket_is_ignored(con):
    """THE SMELL, path 1. A reader of `bucket = notes if content else failures`
    could conclude a content column never fails. The MISSING_MARKER check at
    verify_anon.py's check_file (~line 126) appends to `failures` directly and
    never looks at `bucket`, so this must fail even though source_text is a
    content column."""
    assert "source_text" in V.CONTENT_COLUMNS
    con.add_file("bug_content.parquet", [("source_text", False)], {
        "source_text": [f"{V.MISSING_MARKER} leaked into source"],
    })

    _checked, failed, notes = V.check_file(con, "bug_content.parquet")

    assert failed == 1
    assert notes == []


def test_exact_shape_violation_fails_even_in_a_content_column_bucket_is_ignored(
        con, monkeypatch):
    """THE SMELL, path 2. EXACT_SHAPE and CONTENT_COLUMNS never overlap today
    (see test_content_columns_and_exact_shape_never_share_a_column), so this
    branch is unreachable with the shipped constants. Forcing the overlap here
    pins the actual behaviour of the exact-shape check (~line 129): it appends
    to `failures` directly too, ignoring `bucket`, exactly like the
    MISSING_MARKER check above."""
    monkeypatch.setitem(V.EXACT_SHAPE, "source_text", V.PSEUDO_NAME)
    con.add_file("shape_content.parquet", [("source_text", False)], {
        "source_text": ["not a pseudonym shape"],
    })

    _checked, failed, notes = V.check_file(con, "shape_content.parquet")

    assert failed == 1
    assert notes == []


def test_real_address_in_a_content_column_is_the_one_path_that_uses_bucket(con):
    """THE PATH THAT WORKS. Only the trailing REAL_ADDRESS check (~line 132)
    reads `bucket`, so this is the only way a content column value lands in
    `notes` instead of `failures`."""
    con.add_file("address_content.parquet", [("source_text", False)], {
        "source_text": ["copyright bob@example.com"],
    })

    _checked, failed, notes = V.check_file(con, "address_content.parquet")

    assert failed == 0
    assert len(notes) == 1


# --------------------------------------------------------------------------- #
# main -- Task 2: the exit status is the interface
# --------------------------------------------------------------------------- #

def test_main_exits_zero_when_every_file_passes(con, tmp_path, capsys):
    touch_parquet(con, tmp_path, "clean", [("author_email", False)], {
        "author_email": ["author_0001@example.com"],
    })

    code = V.main([str(tmp_path)])

    assert code == 0
    assert "OK:" in capsys.readouterr().out


def test_main_exits_one_when_a_file_fails(con, tmp_path, capsys):
    touch_parquet(con, tmp_path, "leaky", [("author_email", False)], {
        "author_email": ["bob@example.com"],
    })

    code = V.main([str(tmp_path)])

    assert code == 1
    assert "FAIL:" in capsys.readouterr().out


def test_main_exits_two_for_a_directory_that_does_not_exist(con, tmp_path, capsys):
    missing = tmp_path / "does-not-exist"

    code = V.main([str(missing)])

    assert code == 2
    assert "not a directory" in capsys.readouterr().err


def test_main_exits_two_for_a_directory_with_no_parquet_files(con, tmp_path, capsys):
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()

    code = V.main([str(empty_dir)])

    assert code == 2
    assert "no parquet files" in capsys.readouterr().err


def test_main_verbose_still_exits_zero_with_nothing_to_report(con, tmp_path):
    """Exercises the `if verbose or failures:` branch by way of verbose alone,
    with an empty failures list, distinct from the plain pass/fail cases above."""
    touch_parquet(con, tmp_path, "clean", [("author_email", False)], {
        "author_email": ["author_0001@example.com"],
    })

    code = V.main([str(tmp_path), "--verbose"])

    assert code == 0


# --------------------------------------------------------------------------- #
# The real thing -- needs duckdb (devenv shell), skipped in .venv
# --------------------------------------------------------------------------- #

def sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def write_real_parquet(path, columns, rows):
    """A real parquet with exactly `columns` -- (name, duckdb type) pairs.

    rows: a list of {name: value} dicts, one per output row. A VARCHAR[] value
    is a list of str; any other type's value is used as a raw SQL literal
    (e.g. "1" for an INTEGER column); a missing key casts to NULL.
    """
    import duckdb as real_duckdb

    def cell(name, typ, row):
        v = row.get(name)
        if v is None:
            return f"cast(null as {typ})"
        if typ == "VARCHAR[]":
            return "[" + ", ".join(sql_str(x) for x in v) + "]"
        if typ == "VARCHAR":
            return sql_str(v)
        return str(v)

    selects = []
    for row in rows:
        cells = ", ".join(f'{cell(name, typ, row)} as "{name}"'
                          for name, typ in columns)
        selects.append(f"select {cells}")
    query = " union all ".join(selects)
    real_duckdb.sql(f"copy ({query}) to '{path}' (format parquet)")
    return path


@pytest.fixture
def real_con():
    return V.connect()


@requires_duckdb
def test_connect_returns_a_working_duckdb_connection(real_con):
    assert real_con.execute("select 1").fetchone() == (1,)


@requires_duckdb
def test_string_columns_finds_varchar_and_varchar_list_columns(tmp_path, real_con):
    path = write_real_parquet(
        tmp_path / "real.parquet",
        [("author_email", "VARCHAR"), ("footer_person_names", "VARCHAR[]"),
         ("token_index", "INTEGER")],
        [{"author_email": "author_0001@example.com",
          "footer_person_names": ["Author 0001", "Author 0002"],
          "token_index": "1"}],
    )

    cols = V.string_columns(real_con, str(path))

    assert ("author_email", False) in cols
    assert ("footer_person_names", True) in cols
    assert all(name != "token_index" for name, _ in cols)


@requires_duckdb
def test_distinct_values_unnests_a_list_column(tmp_path, real_con):
    path = write_real_parquet(
        tmp_path / "list.parquet",
        [("footer_person_names", "VARCHAR[]")],
        [{"footer_person_names": ["Author 0001", "Author 0002"]},
         {"footer_person_names": ["Author 0001"]}],
    )

    values = V.distinct_values(real_con, str(path), "footer_person_names", True)

    assert sorted(values) == ["Author 0001", "Author 0002"]


@requires_duckdb
def test_check_file_end_to_end_against_a_real_parquet(tmp_path, real_con):
    """The FakeCon tests above simulate this; this proves the simulation
    matches the real SQL, on the exact scenario Task 1 asks for: an identity
    column fails on a real address, a content column only notes it."""
    path = write_real_parquet(
        tmp_path / "e2e.parquet",
        [("author_email", "VARCHAR"), ("source_text", "VARCHAR")],
        [{"author_email": "author_0001@example.com",
          "source_text": "contact bob@example.com"},
         {"author_email": "bob@example.com",
          "source_text": "no address here"}],
    )

    _checked, failed, notes = V.check_file(real_con, str(path))

    assert failed == 1
    assert len(notes) == 1
