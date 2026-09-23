"""Unit tests for validate_schema.py, the corpus schema gate.

The gate answers a different question from validate.py. That one asks whether one
project produced a non-empty parquet. This one asks whether every project agrees,
because a corpus where one file has 38 columns and another 23 is unusable: a
consumer unions them and gets silent nulls.

No test needs duckdb. compare_schema is pure, and read_schema imports duckdb
inside the function precisely so this file can be collected without it.
"""
from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

import validate_schema as vs


CONTRACT = vs.EXPECTED_COLUMNS
GOOD = list(CONTRACT)


def kinds(drifts) -> list[str]:
    return [d.kind for d in drifts]


# --------------------------------------------------------------------------- #
# compare_schema
# --------------------------------------------------------------------------- #

def test_a_matching_schema_has_no_drift():
    assert vs.compare_schema(GOOD) == []


def test_a_missing_column_is_reported():
    actual = [c for c in GOOD if c[0] != "person_domain"]
    drifts = vs.compare_schema(actual)
    assert kinds(drifts) == ["missing"]
    assert drifts[0].column == "person_domain"


def test_an_extra_column_is_reported():
    """An unexpected column is not harmless: it means the generator changed and
    nobody recorded why."""
    # The sentinel used to be "firm". It became a real contract column on
    # 2026-09-20, so this test needed a name the contract does not hold.
    drifts = vs.compare_schema(GOOD + [("employer", "VARCHAR")])
    assert kinds(drifts) == ["unexpected"]
    assert drifts[0].column == "employer"


def test_a_changed_type_is_reported():
    """The quiet killer. token_index as VARCHAR in one file and BIGINT in another
    makes any ordering or arithmetic across the corpus wrong."""
    actual = [("token_index", "VARCHAR") if n == "token_index" else (n, t)
              for n, t in GOOD]
    drifts = vs.compare_schema(actual)
    assert kinds(drifts) == ["type"]
    assert "expected BIGINT, found VARCHAR" in drifts[0].detail


def test_a_footer_column_losing_its_array_type_is_reported():
    """VARCHAR[] to VARCHAR would silently flatten a multi-trailer commit to one
    string, and Signed-off-by often carries several names."""
    actual = [("footer_signed_off_by", "VARCHAR") if n == "footer_signed_off_by"
              else (n, t) for n, t in GOOD]
    drifts = vs.compare_schema(actual)
    assert kinds(drifts) == ["type"]
    assert drifts[0].column == "footer_signed_off_by"


def test_every_drift_is_reported_not_just_the_first():
    """A schema change usually moves several columns at once. Fixing them one run
    at a time is slow, so the gate reports all of them."""
    actual = [c for c in GOOD if c[0] not in ("repo_tag", "personid")]
    actual = [("source_line", "VARCHAR") if n == "source_line" else (n, t)
              for n, t in actual]
    actual.append(("stray", "INTEGER"))
    drifts = vs.compare_schema(actual)
    assert sorted(kinds(drifts)) == ["missing", "missing", "type", "unexpected"]


def test_reordered_columns_are_reported_as_order_not_as_missing():
    """Parquet is read by name, so order breaks no consumer. It still means the
    generator changed, so it is surfaced as its own kind rather than hidden."""
    actual = list(reversed(GOOD))
    drifts = vs.compare_schema(actual)
    assert kinds(drifts) == ["order"]


def test_order_is_not_reported_while_columns_still_disagree():
    """Reporting order on top of a missing column would be noise: fix the set
    first, then the order question becomes meaningful."""
    actual = list(reversed([c for c in GOOD if c[0] != "repo_tag"]))
    assert "order" not in kinds(vs.compare_schema(actual))


def test_an_empty_schema_reports_every_column_missing():
    drifts = vs.compare_schema([])
    assert len(drifts) == len(CONTRACT)
    assert set(kinds(drifts)) == {"missing"}


def test_a_caller_may_pass_its_own_contract():
    """The contract is a default, not a global. A future all-token dataset will
    have its own, and this gate should be reusable for it."""
    mine = (("a", "BIGINT"),)
    assert vs.compare_schema([("a", "BIGINT")], mine) == []
    assert kinds(vs.compare_schema([("a", "VARCHAR")], mine)) == ["type"]


# --------------------------------------------------------------------------- #
# the contract itself
# --------------------------------------------------------------------------- #

def test_the_contract_has_no_duplicate_columns():
    names = [n for n, _ in CONTRACT]
    assert len(names) == len(set(names))


def test_the_contract_records_the_measured_column_count():
    """70: the 38 measured from the updated cregit on 2026-09-13, plus the 29
    per-project provenance columns injected from project_meta.json on 2026-09-19,
    plus the 3 firm columns joined from a domain-to-firm CSV the caller supplies,
    on 2026-09-20.
    The earlier kernel snapshot had 23; 15 of the additions to that are
    commit-trailer footers."""
    assert len(CONTRACT) == 70
    assert sum(1 for n, _ in CONTRACT if n.startswith("footer_")) == 15


def test_the_firm_columns_sit_directly_after_the_key_they_are_resolved_from():
    """Replaces test_the_contract_still_has_no_firm_column, whose docstring asked
    for exactly this the day a firm column landed (2026-09-20).

    Position is the claim: firm is resolved FROM person_domain, so the key and its
    three answers are adjacent and a reader filtering on one finds the others in
    the next columns. They are also the only per-ROW columns in the contract —
    everything before file_path is a per-project constant.
    """
    names = [n for n, _ in CONTRACT]
    i = names.index("person_domain")
    assert names[i:i + 4] == ["person_domain", "firm_raw", "firm", "firm_source"]
    assert names[i + 4] == "repo_tag"
    assert {t for n, t in CONTRACT
            if n in ("firm_raw", "firm", "firm_source")} == {"VARCHAR"}


# --------------------------------------------------------------------------- #
# check(): the process-level contract
# --------------------------------------------------------------------------- #

def test_check_returns_zero_when_every_file_matches(monkeypatch, capsys):
    monkeypatch.setattr(vs, "read_schema", lambda p: GOOD)
    assert vs.check(["a.parquet", "b.parquet"]) == 0
    out = capsys.readouterr().out
    assert out.count("OK") == 2


def test_check_returns_one_when_any_file_drifts(monkeypatch, capsys):
    def schema(path):
        return GOOD if path == "good.parquet" else GOOD[:-1]
    monkeypatch.setattr(vs, "read_schema", schema)
    assert vs.check(["good.parquet", "bad.parquet"]) == vs.EXIT_DRIFT
    err = capsys.readouterr().err
    assert "bad.parquet" in err and "missing" in err


def test_check_keeps_going_after_the_first_bad_file(monkeypatch, capsys):
    """One unreadable file must not hide the state of the rest. A corpus run
    produces hundreds, and re-running the gate per file is wasteful."""
    def schema(path):
        if path == "boom.parquet":
            raise OSError("no such file")
        return GOOD
    monkeypatch.setattr(vs, "read_schema", schema)
    assert vs.check(["boom.parquet", "fine.parquet"]) == vs.EXIT_DRIFT
    captured = capsys.readouterr()
    assert "unreadable" in captured.err
    assert "OK" in captured.out, "the readable file was not reported"


def test_an_unreadable_file_counts_as_drift(monkeypatch, capsys):
    """Silently passing a file the gate could not read would be worse than
    failing: the corpus would look validated."""
    monkeypatch.setattr(vs, "read_schema",
                        lambda p: (_ for _ in ()).throw(ValueError("not parquet")))
    assert vs.check(["x.parquet"]) == vs.EXIT_DRIFT
    assert "ValueError" in capsys.readouterr().err


# --------------------------------------------------------------------------- #
# main(): argument handling
# --------------------------------------------------------------------------- #

def test_main_with_no_arguments_exits_two(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["validate_schema.py"])
    with pytest.raises(SystemExit) as exc:
        vs.main()
    assert exc.value.code == vs.EXIT_USAGE
    assert vs.USAGE in capsys.readouterr().err


def test_main_checks_the_files_it_is_given(monkeypatch):
    seen = {}
    monkeypatch.setattr(sys, "argv", ["validate_schema.py", "a.parquet", "b.parquet"])
    monkeypatch.setattr(vs, "check", lambda paths: seen.setdefault("paths", paths) and 0)
    with pytest.raises(SystemExit) as exc:
        vs.main()
    assert seen["paths"] == ["a.parquet", "b.parquet"]
    assert exc.value.code == 0


def test_emit_contract_prints_paste_ready_rows(monkeypatch, capsys):
    """When the generator legitimately changes, hand-typing 38 rows invites a
    typo. This prints them."""
    monkeypatch.setattr(vs, "read_schema",
                        lambda p: [("a", "BIGINT"), ("b", "VARCHAR[]")])
    monkeypatch.setattr(sys, "argv", ["validate_schema.py", "--emit-contract", "x.parquet"])
    vs.main()
    out = capsys.readouterr().out
    assert '    ("a", "BIGINT"),' in out
    assert '    ("b", "VARCHAR[]"),' in out


def test_emit_contract_needs_exactly_one_file(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["validate_schema.py", "--emit-contract"])
    with pytest.raises(SystemExit) as exc:
        vs.main()
    assert exc.value.code == vs.EXIT_USAGE


def test_read_schema_binds_the_path_as_a_parameter(monkeypatch):
    """A project name holding a quote must not be able to break or inject SQL.
    Same rule as validate.py: the path is bound, never pasted into the text."""
    calls = {}

    def fake_sql(text, params=None):
        calls["text"], calls["params"] = text, params
        return SimpleNamespace(fetchall=lambda: [("a", "BIGINT")])

    monkeypatch.setitem(sys.modules, "duckdb", SimpleNamespace(sql=fake_sql))
    assert vs.read_schema("it's.parquet") == [("a", "BIGINT")]
    assert calls["params"] == ["it's.parquet"]
    assert "it's" not in calls["text"], "the path was pasted into the SQL text"


def test_drift_renders_as_one_readable_line():
    """The gate's output is read by a human at 3am, so it must be scannable."""
    line = str(vs.Drift("type", "token_index", "expected BIGINT, found VARCHAR"))
    assert "type" in line and "token_index" in line and "BIGINT" in line
