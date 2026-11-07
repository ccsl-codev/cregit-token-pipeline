"""Tests for verify_anon.py, the reviewer-facing release gate.

It is the check that a co-author or a depositor runs, and it needs only the
published files. It "cannot be passed by forgetting to anonymize", so it is the
worst place in the release path to have no coverage.

The headline test is test_a_domainless_pseudonym_is_accepted: a git author
field can hold a bare user name, and anonymize_parquet.py replaces the whole
value with a pseudonym that has no domain. A check that required a domain
rejected correct output.

The shape tests and the fake-connection tests need no duckdb: a fake connection
replaces the query layer. The end-to-end tests write a parquet and skip without
the real duckdb from `devenv shell`.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

import verify_anon as V

try:                                          # pragma: no cover - import plumbing
    import duckdb as _duckdb
except ModuleNotFoundError:                   # pragma: no cover - import plumbing
    _duckdb = None

# tests/test_consolidate.py installs a stub duckdb into sys.modules and sorts
# before this file, so an ImportError check alone would not skip correctly.
_STUBBED = getattr(sys.modules.get("duckdb"), "__doc__", "") or ""
requires_duckdb = pytest.mark.skipif(
    _duckdb is None or _STUBBED.startswith("Test stub"),
    reason="needs real duckdb (devenv shell) to write parquet")


def exact(col: str, value: str) -> bool:
    """Does `value` satisfy the exact-shape rule for `col`?"""
    return re.match(rf"^{V.EXACT_SHAPE[col]}$", value) is not None


# --------------------------------------------------------------------------
# The e-mail column shape
# --------------------------------------------------------------------------

@pytest.mark.parametrize("col", ["author_email", "committer_email",
                                "person_email"])
def test_a_pseudonym_with_a_preserved_domain_is_accepted(col):
    assert exact(col, "author_0042@redhat.com")


@pytest.mark.parametrize("col", ["author_email", "committer_email",
                                "person_email"])
def test_a_domainless_pseudonym_is_accepted(col):
    """The defect this change fixes.

    A git author field can hold a bare user name in an e-mail column. That is
    personal data, and anonymize_parquet.py replaces the whole string, which
    leaves a pseudonym with no domain to re-attach. Rejecting it made the two
    gates of one release path contradict each other.
    """
    assert exact(col, "author_0109")


@pytest.mark.parametrize("bad", [
    "alice@redhat.com",        # a real address
    "janedoe",                 # a bare user name, un-anonymized
    "Author 0042",             # right pseudonym space, wrong column
    "author_0042 <x@y.com>",   # a pseudonym with a real address appended
    "xauthor_0042",            # a prefix that only looks like a pseudonym
])
def test_a_real_value_is_still_rejected(bad):
    """The relaxation must not have opened a hole."""
    assert not exact("person_email", bad)


def test_the_name_columns_still_require_the_name_pseudonym():
    assert exact("person_name", "Author 0042")
    assert not exact("person_name", "Alice Smith")
    # A local-part pseudonym in a name column is still wrong.
    assert not exact("person_name", "author_0042")


def test_the_personid_columns_require_the_lowercase_id_shape():
    assert exact("personid", "author 0042")
    assert not exact("personid", "Author 0042")
    assert not exact("personid", "alice smith")


def test_padding_is_not_enforced():
    """'Author 10' is a formatting difference, not a leaked name."""
    assert exact("person_name", "Author 10")
    assert exact("person_email", "author_7@x.com")


# --------------------------------------------------------------------------
# The address regex
# --------------------------------------------------------------------------

def test_real_address_regex_ignores_a_pseudonymous_address():
    assert V.REAL_ADDRESS.search("author_0001@intel.com") is None


def test_real_address_regex_catches_a_real_address_in_free_text():
    m = V.REAL_ADDRESS.search("Signed-off-by: X <alice@redhat.com>")
    assert m and m.group(0) == "alice@redhat.com"


def test_real_address_regex_catches_a_real_address_beside_a_pseudonym():
    """A value may hold both. The real one must still be found."""
    assert V.REAL_ADDRESS.search("author_0001@x.com and bob@intel.com")


def test_every_exact_shape_column_is_an_identity_column():
    """Guard against a content column drifting into the strict table."""
    assert not (set(V.EXACT_SHAPE) & V.CONTENT_COLUMNS)


# --------------------------------------------------------------------------
# End to end over a parquet
# --------------------------------------------------------------------------

def write(path: Path, rows: list[dict]) -> Path:
    cols = sorted({k for r in rows for k in r})
    selects = []
    for r in rows:
        parts = []
        for c in cols:
            v = r.get(c)
            if isinstance(v, list):
                inner = ", ".join("'" + x.replace("'", "''") + "'" for x in v)
                parts.append(f'cast([{inner}] as VARCHAR[]) as "{c}"')
            elif v is None:
                parts.append(f'cast(null as VARCHAR) as "{c}"')
            else:
                parts.append("cast('" + v.replace("'", "''")
                             + f"' as VARCHAR) as \"{c}\"")
        selects.append("select " + ", ".join(parts))
    _duckdb.sql(f"copy ({' union all '.join(selects)}) to '{path}' "
                f"(format parquet)")
    return path


@requires_duckdb
def test_a_clean_release_directory_exits_zero(tmp_path, capsys):
    d = tmp_path / "rel"
    d.mkdir()
    write(d / "a-dataset.parquet", [{
        "person_email": "author_0001@redhat.com",
        "author_email": "author_0002",           # the domainless case
        "person_name": "Author 0001",
        "personid": "author 0001",
        "footer_signed_off_by": ["Author 0001 <author_0001@redhat.com>"],
    }])
    assert V.main([str(d)]) == 0
    assert "OK:" in capsys.readouterr().out


@requires_duckdb
def test_a_real_address_in_a_footer_fails(tmp_path, capsys):
    d = tmp_path / "rel"
    d.mkdir()
    write(d / "a-dataset.parquet", [{
        "person_email": "author_0001@redhat.com",
        "footer_signed_off_by": ["Alice Smith <alice@redhat.com>"],
    }])
    assert V.main([str(d)]) == 1
    assert "FAIL" in capsys.readouterr().out


@requires_duckdb
def test_an_unanonymized_name_column_fails(tmp_path):
    d = tmp_path / "rel"
    d.mkdir()
    write(d / "a-dataset.parquet", [{"person_name": "Alice Smith"}])
    assert V.main([str(d)]) == 1


@requires_duckdb
def test_the_anonymizer_bug_marker_fails(tmp_path):
    """A registry miss must not be shrugged off by the independent check."""
    d = tmp_path / "rel"
    d.mkdir()
    write(d / "a-dataset.parquet",
          [{"person_name": V.MISSING_MARKER}])
    assert V.main([str(d)]) == 1


@requires_duckdb
def test_an_address_in_a_content_column_is_noted_not_failed(tmp_path, capsys):
    """A copyright header is real and unremovable. Disclosed, not a failure."""
    d = tmp_path / "rel"
    d.mkdir()
    write(d / "a-dataset.parquet", [{
        "person_email": "author_0001@redhat.com",
        "source_text": "/* Copyright alice@redhat.com */",
    }])
    assert V.main([str(d)]) == 0
    assert "note" in capsys.readouterr().out


def test_a_missing_directory_is_a_usage_error(tmp_path):
    assert V.main([str(tmp_path / "nope")]) == 2


@requires_duckdb
def test_a_directory_without_parquet_is_a_usage_error(tmp_path):
    """An empty directory must not read as 'nothing wrong found'."""
    d = tmp_path / "empty"
    d.mkdir()
    assert V.main([str(d)]) == 2


# --------------------------------------------------------------------------
# check_file and main over a fake connection
# --------------------------------------------------------------------------

class FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class FakeCon:
    """Answers the two queries verify_anon sends: the schema and the values.

    `files` maps a path to {column: (duckdb_type, [values])}.
    """

    def __init__(self, files: dict[str, dict[str, tuple[str, list]]]):
        self.files = files

    def execute(self, query: str, params=None):
        cols = self.files[params[0]]
        if query.startswith("describe"):
            return FakeResult([(c, t) for c, (t, _) in cols.items()])
        for col, (_t, values) in cols.items():
            if f"unnest({col})" in query or f"select distinct {col} from" in query:
                return FakeResult([(v,) for v in values])
        raise AssertionError(f"unexpected query: {query}")


CLEAN = {
    "author_name": ("VARCHAR", ["Author 1", "Author 0042"]),
    "author_email": ("VARCHAR", ["author_1@example.org"]),
    "footer_person_names": ("VARCHAR[]", ["Author 2"]),
    "token_index": ("BIGINT", [1, 2, 3]),
}


def check(cols: dict, verbose: bool = False):
    return V.check_file(FakeCon({"f.parquet": cols}), "f.parquet", verbose)


def test_a_pseudonymous_address_is_not_a_real_address():
    assert V.REAL_ADDRESS.search("author_12@example.org") is None
    assert V.REAL_ADDRESS.search("x bob@example.com y").group(0) == \
        "bob@example.com"


def test_a_clean_file_has_no_failures_and_skips_non_string_columns():
    checked, failed, notes = check(CLEAN)
    assert (checked, failed, notes) == (4, 0, [])


def test_a_real_name_in_an_identity_column_fails(capsys):
    checked, failed, _ = check({"author_name": ("VARCHAR", ["Alice Example"])})
    assert (checked, failed) == (1, 1)
    assert "not a pseudonym" in capsys.readouterr().out


def test_the_anonymizer_bug_marker_fails_in_any_column():
    _, failed, _ = check({"committer_name":
                          ("VARCHAR", [V.MISSING_MARKER])})
    assert failed == 1


def test_a_real_address_outside_the_content_columns_fails(capsys):
    _, failed, notes = check({"footer_signed_off_by":
                              ("VARCHAR[]", ["Alice <alice@example.com>"])})
    assert (failed, notes) == (1, [])
    assert "FAIL footer_signed_off_by" in capsys.readouterr().out


def test_an_address_in_source_text_is_a_note_not_a_failure(capsys):
    values = [f"(c) dev{i}@example.com" for i in range(7)]
    _, failed, notes = check({"source_text": ("VARCHAR", values)})
    assert failed == 0
    assert len(notes) == 7
    out = capsys.readouterr().out
    assert out.count("note source_text") == 5
    assert "and 2 more in content columns" in out


def test_verbose_on_a_clean_file_prints_no_failure(capsys):
    check(CLEAN, verbose=True)
    assert "FAIL" not in capsys.readouterr().out


def fake_connect(monkeypatch, files: dict):
    monkeypatch.setattr(V, "connect", lambda: FakeCon(files))


def test_main_refuses_a_path_that_is_not_a_directory(tmp_path, monkeypatch,
                                                     capsys):
    fake_connect(monkeypatch, {})
    assert V.main([str(tmp_path / "absent")]) == 2
    assert "not a directory" in capsys.readouterr().err


def test_main_refuses_a_directory_with_no_parquet(tmp_path, monkeypatch, capsys):
    fake_connect(monkeypatch, {})
    assert V.main([str(tmp_path)]) == 2
    assert "no parquet files" in capsys.readouterr().err


def test_main_passes_a_clean_directory(tmp_path, monkeypatch, capsys):
    path = tmp_path / "a.parquet"
    path.touch()
    fake_connect(monkeypatch, {str(path): CLEAN})
    assert V.main([str(tmp_path)]) == 0
    out = capsys.readouterr().out
    assert "1 file(s), 4 distinct strings checked." in out
    assert out.rstrip().endswith("carries a non-pseudonymous address.")


def test_main_fails_a_directory_with_one_real_name(tmp_path, monkeypatch,
                                                   capsys):
    good, bad = tmp_path / "a.parquet", tmp_path / "b.parquet"
    good.touch()
    bad.touch()
    fake_connect(monkeypatch, {
        str(good): CLEAN,
        str(bad): {"person_name": ("VARCHAR", ["Alice Example"])},
    })
    assert V.main([str(tmp_path)]) == 1
    assert "FAIL: 1 value(s) are not pseudonymized." in capsys.readouterr().out


@requires_duckdb
def test_main_reads_a_real_parquet(tmp_path, capsys):
    path = tmp_path / "a.parquet"
    con = _duckdb.connect()
    con.execute(
        "copy (select 'Author 1' as author_name, "
        "['Author 2'] as footer_person_names, 7 as token_index) "
        f"to '{path}' (format parquet)")
    con.close()
    assert V.main([str(tmp_path)]) == 0
    assert "2 distinct strings checked" in capsys.readouterr().out
