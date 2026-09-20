"""Tests for verify_anon.py, the reviewer-facing release gate.

This script had no tests. It is the check a co-author or a Zenodo depositor is
meant to run, and the one that "cannot be passed by forgetting to anonymize", so
it is the worst place in the release path to have no coverage.

The headline test is test_a_domainless_pseudonym_is_accepted: requiring a domain
made this script reject output that anonymize_parquet.py had correctly produced,
on 36 of the 196 corpus parquets.

The shape tests need no duckdb. The end-to-end tests write a parquet and skip
without the real duckdb from `devenv shell`.
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

    Measured on the corpus: 36 of 196 parquets hold a bare user name in an
    e-mail column ('robertmartin', 'nashjain'), 1,332,252 rows in total. Those
    are personal data and anonymize_parquet.py replaces the whole string, which
    leaves a pseudonym with no domain to re-attach. Rejecting it made the two
    gates of one release path contradict each other.
    """
    assert exact(col, "author_0109")


@pytest.mark.parametrize("bad", [
    "alice@redhat.com",        # a real address
    "robertmartin",            # a real user name, un-anonymized
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
