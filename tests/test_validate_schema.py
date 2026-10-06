"""Unit tests for validate_schema.py, the corpus schema gate. No test needs
duckdb: compare_schema is pure, and read_schema imports duckdb lazily."""
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


def retyped(column: str, typ: str) -> list:
    return [(n, typ) if n == column else (n, t) for n, t in GOOD]


@pytest.mark.parametrize("actual, kind, column", [
    pytest.param([c for c in GOOD if c[0] != "person_domain"], "missing",
                 "person_domain", id="missing"),
    # An unexpected column means the generator changed and nobody recorded why.
    pytest.param(GOOD + [("employer", "VARCHAR")], "unexpected", "employer", id="unexpected"),
    pytest.param(retyped("token_index", "VARCHAR"), "type", "token_index", id="type"),
    # Parquet is read by name, so order breaks no consumer, but it is still surfaced.
    pytest.param(list(reversed(GOOD)), "order", "-", id="order"),
])
def test_each_kind_of_drift_is_reported(actual, kind, column):
    drifts = vs.compare_schema(actual)
    assert [(d.kind, d.column) for d in drifts] == [(kind, column)]


def test_a_changed_type_names_both_types():
    detail = vs.compare_schema(retyped("token_index", "VARCHAR"))[0].detail
    assert "expected BIGINT, found VARCHAR" in detail


def test_every_drift_is_reported_not_just_the_first():
    """A schema change usually moves several columns at once."""
    actual = [c for c in retyped("source_line", "VARCHAR")
              if c[0] not in ("repo_tag", "personid")]
    actual.append(("stray", "INTEGER"))
    drifts = vs.compare_schema(actual)
    assert sorted(kinds(drifts)) == ["missing", "missing", "type", "unexpected"]


def test_order_is_not_reported_while_columns_still_disagree():
    """Order on top of a missing column would be noise: fix the set first."""
    actual = list(reversed([c for c in GOOD if c[0] != "repo_tag"]))
    assert "order" not in kinds(vs.compare_schema(actual))


def test_an_empty_schema_reports_every_column_missing():
    drifts = vs.compare_schema([])
    assert len(drifts) == len(CONTRACT)
    assert set(kinds(drifts)) == {"missing"}


def test_a_caller_may_pass_its_own_contract():
    """The contract is a default, not a global."""
    mine = (("a", "BIGINT"),)
    assert vs.compare_schema([("a", "BIGINT")], mine) == []
    assert kinds(vs.compare_schema([("a", "VARCHAR")], mine)) == ["type"]


# --------------------------------------------------------------------------- #
# the contract itself
# --------------------------------------------------------------------------- #

def test_the_contract_has_70_unique_columns():
    names = [n for n, _ in CONTRACT]
    assert len(set(names)) == len(names) == 70


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


def test_check_reports_an_unreadable_file_as_drift_and_keeps_going(monkeypatch, capsys):
    """Passing a file the gate could not read would make the corpus look
    validated, and one bad file must not hide the state of the rest."""
    def schema(path):
        if path == "boom.parquet":
            raise ValueError("not parquet")
        return GOOD
    monkeypatch.setattr(vs, "read_schema", schema)
    assert vs.check(["boom.parquet", "fine.parquet"]) == vs.EXIT_DRIFT
    captured = capsys.readouterr()
    assert "unreadable" in captured.err and "ValueError" in captured.err
    assert "OK" in captured.out, "the readable file was not reported"


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
    """Hand-typing the contract after a generator change invites a typo."""
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
    """A project name holding a quote must not break or inject SQL."""
    calls = {}

    def fake_sql(text, params=None):
        calls["text"], calls["params"] = text, params
        return SimpleNamespace(fetchall=lambda: [("a", "BIGINT")])

    monkeypatch.setitem(sys.modules, "duckdb", SimpleNamespace(sql=fake_sql))
    assert vs.read_schema("it's.parquet") == [("a", "BIGINT")]
    assert calls["params"] == ["it's.parquet"]
    assert "it's" not in calls["text"], "the path was pasted into the SQL text"


def test_drift_renders_as_one_readable_line():
    line = str(vs.Drift("type", "token_index", "expected BIGINT, found VARCHAR"))
    assert "type" in line and "token_index" in line and "BIGINT" in line
