"""Unit tests for verify_anon.py, the check that needs only the published files.

The query layer is replaced by a fake connection, so the shape rules run
without duckdb. One end-to-end test writes a real Parquet and skips when duckdb
is absent, the same gate tests/test_consolidate.py uses.
"""
from __future__ import annotations

import sys

import pytest

import verify_anon as va


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
    return va.check_file(FakeCon({"f.parquet": cols}), "f.parquet", verbose)


def test_a_pseudonymous_address_is_not_a_real_address():
    assert va.REAL_ADDRESS.search("author_12@example.org") is None
    assert va.REAL_ADDRESS.search("x bob@example.com y").group(0) == \
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
                          ("VARCHAR", [va.MISSING_MARKER])})
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
    monkeypatch.setattr(va, "connect", lambda: FakeCon(files))


def test_main_refuses_a_path_that_is_not_a_directory(tmp_path, monkeypatch,
                                                     capsys):
    fake_connect(monkeypatch, {})
    assert va.main([str(tmp_path / "absent")]) == 2
    assert "not a directory" in capsys.readouterr().err


def test_main_refuses_a_directory_with_no_parquet(tmp_path, monkeypatch, capsys):
    fake_connect(monkeypatch, {})
    assert va.main([str(tmp_path)]) == 2
    assert "no parquet files" in capsys.readouterr().err


def test_main_passes_a_clean_directory(tmp_path, monkeypatch, capsys):
    path = tmp_path / "a.parquet"
    path.touch()
    fake_connect(monkeypatch, {str(path): CLEAN})
    assert va.main([str(tmp_path)]) == 0
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
    assert va.main([str(tmp_path)]) == 1
    assert "FAIL: 1 value(s) are not pseudonymized." in capsys.readouterr().out


try:                                          # pragma: no cover - import plumbing
    import duckdb as _duckdb
except ModuleNotFoundError:                   # pragma: no cover - import plumbing
    _duckdb = None

# tests/test_consolidate.py can put a stub `duckdb` into sys.modules first, so
# an import check alone is not enough. Detect the stub by its label, as
# tests/test_measure_dataset_gaps.py does.
_STUBBED = getattr(sys.modules.get("duckdb"), "__doc__", "") or ""


@pytest.mark.skipif(_duckdb is None or _STUBBED.startswith("Test stub"),
                    reason="needs real duckdb (devenv shell) to write parquet")
def test_main_reads_a_real_parquet(tmp_path, capsys):
    path = tmp_path / "a.parquet"
    con = _duckdb.connect()
    con.execute(
        "copy (select 'Author 1' as author_name, "
        "['Author 2'] as footer_person_names, 7 as token_index) "
        f"to '{path}' (format parquet)")
    con.close()
    assert va.main([str(tmp_path)]) == 0
    assert "2 distinct strings checked" in capsys.readouterr().out
