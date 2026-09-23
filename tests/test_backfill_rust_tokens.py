"""Unit tests for backfill_rust_tokens.py, the Rust token-column repair pass.

The script recovers `token_type`, `source_line`, `source_col` and
`is_structural` on the rows the Rust tokenizer corrupted, by reconstructing the
tokenizer line the consumer should have seen and re-running the consumer's own
classifier over it. See the module docstring there for the defect and
`docs/LIMITATIONS.md` for its measured scale.

Two halves. Everything above the `requires_duckdb` marker is pure: SQL-string
builders, planning, and `verify` with fabricated aggregates. `backfill_rust_tokens`
imports duckdb inside `main` only, following `validate_schema.py`, so this file is
collected and most of it runs without duckdb installed.

Below the marker the tests write real 70-column Parquet files and drive the whole
pass end to end, so they skip when duckdb is absent, exactly like the five in
tests/test_consolidate.py.

The fixtures encode measured facts, not invention. Every "correct" shape asserted
here -- `begin_unit` carrying `begin_unit|revision:...` in `token_value`,
`end_unit` carrying `end_unit`, `is_structural = 1` implying `source_text = ''`
-- was read off C and Java projects in the same corpus.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

import backfill_rust_tokens as bf
import validate_schema as vs

CONTRACT = vs.EXPECTED_COLUMNS
TAB = "\t"


# --------------------------------------------------------------------------- #
# the position grammar
#
# N:N ordinary token, N:- a DECL line, -:- a unit marker. The script's SQL and
# these tests share one constant, so a change to the grammar cannot pass here and
# fail there.
# --------------------------------------------------------------------------- #

ANCHORED = re.compile("^" + bf.POSITION_RE + TAB)


@pytest.mark.parametrize("position", ["1:1", "12:5", "999:1", "7:-", "-:-"])
def test_the_position_grammar_accepts_all_three_shapes(position):
    assert ANCHORED.match(f"{position}{TAB}keyword")


@pytest.mark.parametrize("text", [
    "keyword",                    # no position at all: a correct token line
    "1:1|comment",                # a pipe, not a tab: already the correct form
    f"a:b{TAB}keyword",           # letters are not a position
    f"1{TAB}keyword",             # no colon
    f"x1:1{TAB}keyword",          # not anchored at the start
    f" 1:1{TAB}keyword",          # nor after a space
])
def test_the_position_grammar_rejects_everything_else(text):
    assert ANCHORED.match(text) is None


# --------------------------------------------------------------------------- #
# the corruption detector, which is also the idempotency detector
# --------------------------------------------------------------------------- #

def test_the_detector_is_anchored_and_never_a_bare_tab_search():
    """A bare `strpos(token_type, chr(9)) > 0` would fire on any TAB anywhere and
    would strip through it. Anchoring to the position grammar is what makes a
    second run a no-op instead of a second split."""
    assert "^" in bf.DETECTOR_SQL
    assert bf.POSITION_RE in bf.DETECTOR_SQL
    assert "strpos" not in bf.DETECTOR_SQL


def test_the_detector_covers_both_wrecks_the_pipe_split_produced():
    """The regex matched for most rows, leaving the prefix in token_type. For the
    `end_unit` row it did not match at all, so token_type is the literal
    'unknown' and the whole line sits in token_value."""
    assert "token_type" in bf.DETECTOR_SQL
    assert "'unknown'" in bf.DETECTOR_SQL
    assert "token_value" in bf.DETECTOR_SQL


def test_a_tab_is_always_chr_9_never_a_backslash_escape():
    """DuckDB string literals do not interpret backslash escapes, so '\\t' would
    match a backslash followed by a t."""
    for sql in (bf.DETECTOR_SQL, bf.repair_sql("x.parquet"), bf.STATS_SQL):
        assert "chr(9)" in sql
        assert "\\t" not in sql


# --------------------------------------------------------------------------- #
# the branch chain, mirrored from generate_dataset.classify_and_skip
# --------------------------------------------------------------------------- #

def test_the_branch_numbers_are_the_consumer_s_source_order():
    """classify_and_skip is an ordered if/elif chain and the FIRST match wins. If
    these were reordered, `begin_unit|revision:...` would fall through to the
    content branch and be typed `begin_unit` with is_structural 0."""
    assert [entry[0] for entry in bf._BRANCHES] == [1, 2, 3, 4, 5, 6, 7]
    assert bf._CONTENT_BRANCH == 8


def test_the_branch_predicates_are_in_the_documented_order():
    predicates = [entry[1] for entry in bf._BRANCHES]
    assert "begin_unit" in predicates[0]
    assert "begin_function" in predicates[1]
    assert "DECL" in predicates[2]
    assert predicates[3] == "ctp_line = '|'"
    assert predicates[4] == "ctp_line = ''"
    assert "(begin|end)_[a-z_]+" in predicates[5]
    assert predicates[6].startswith("NOT regexp_matches")


def test_only_the_content_branch_is_non_structural():
    """is_structural is not an expression in the consumer: every structural branch
    hard-codes 1 and only the final content return hard-codes 0."""
    sql = bf.repaired_columns_sql()["is_structural"]
    assert f"WHEN ctp_branch = {bf._CONTENT_BRANCH} THEN 0" in sql
    assert "ELSE 1" in sql


def test_the_pipe_split_regex_is_the_consumer_s_own():
    assert bf.PIPE_SPLIT_RE == r"^(.+?)\|(.+)$"
    # Non-greedy first group: only the FIRST pipe splits.
    m = re.match(bf.PIPE_SPLIT_RE, "comment|/* a | b */")
    assert m.group(1) == "comment"
    assert m.group(2) == "/* a | b */"


# --------------------------------------------------------------------------- #
# the rewritten columns
# --------------------------------------------------------------------------- #

def test_exactly_six_columns_may_be_rewritten():
    assert set(bf.repaired_columns_sql()) == set(bf.REPAIRED_COLUMNS)


def test_source_text_is_only_blanked_on_structural_rows():
    """The content rows keep their wrong source_text. Blanking those would be a
    sentinel, and the script does not claim to repair the column."""
    sql = bf.repaired_columns_sql()["source_text"]
    assert f"ctp_branch IN (0, {bf._CONTENT_BRANCH}) THEN source_text" in sql
    assert "ELSE ''" in sql


def test_the_position_is_backfilled_only_when_both_components_are_numeric():
    """`-:-` gives neither and `N:-` gives no column, and try_cast turns '-' into
    NULL rather than the empty string regexp_extract would return."""
    for column in ("source_line", "source_col"):
        sql = bf.repaired_columns_sql()[column]
        assert "ctp_pos_line IS NOT NULL AND ctp_pos_col IS NOT NULL" in sql
        assert "try_cast" not in sql          # the cast happens in parsed_sql
    assert "try_cast" in bf.parsed_sql("x.parquet")
    assert "regexp_extract" not in bf.parsed_sql("x.parquet")


# --------------------------------------------------------------------------- #
# the 70-column contract
# --------------------------------------------------------------------------- #

def test_the_contract_is_imported_never_copied():
    """A schema widening must reach this script without anyone editing it."""
    assert bf.contract_columns() == tuple(n for n, _ in vs.EXPECTED_COLUMNS)
    assert len(bf.contract_columns()) == 70


def test_the_repair_projects_every_contract_column_in_order():
    sql = bf.repair_sql("x.parquet")
    aliases = [name for name in bf.contract_columns()
               if re.search(rf"\b{name}\b", sql)]
    assert aliases == list(bf.contract_columns())


def test_the_repair_aliases_each_rewritten_column_to_its_own_name():
    """A CASE that landed under the wrong alias would silently swap two columns."""
    sql = bf.repair_sql("x.parquet")
    for name in bf.REPAIRED_COLUMNS:
        assert f"AS {name}" in sql


def test_a_narrowed_column_list_is_honoured():
    sql = bf.repair_sql("x.parquet", columns=("file_path", "token_type"))
    assert "AS token_type" in sql
    assert "repo_name" not in sql


def test_the_path_reaches_read_parquet_as_a_quoted_literal():
    assert "read_parquet('x.parquet')" in bf.parsed_sql("x.parquet")


def test_a_quote_in_a_path_is_doubled_not_injected():
    assert bf.sql_string("o'brien.parquet") == "'o''brien.parquet'"
    assert "read_parquet('o''brien.parquet')" in bf.parsed_sql("o'brien.parquet")


def test_the_stats_battery_is_one_query_text_for_both_sides():
    """Prediction and actual must not be able to drift apart: any change to the
    checks changes both sides at once."""
    predicted = bf.stats_sql(bf.repair_sql("in.parquet"))
    actual = bf.stats_sql("SELECT * FROM read_parquet('out.parquet')")
    body = bf.STATS_SQL.split("FROM {relation}")[0]
    assert body in predicted
    assert body in actual


# --------------------------------------------------------------------------- #
# planning
# --------------------------------------------------------------------------- #

def test_a_corpus_directory_is_globbed_one_level_down(tmp_path):
    """The corpus layout is <project>/<project>-dataset.parquet, and the same
    directory holds each project's git repositories -- so this is a targeted glob
    and never a recursive walk."""
    for name in ("b__b", "a__a"):
        d = tmp_path / name
        d.mkdir()
        (d / f"{name}-dataset.parquet").touch()
        (d / f"{name}-cregit.db").touch()
    found = bf.expand_inputs([str(tmp_path)])
    assert [p.name for p in found] == ["a__a-dataset.parquet", "b__b-dataset.parquet"]


def test_a_project_directory_also_works(tmp_path):
    d = tmp_path / "a__a"
    d.mkdir()
    (d / "a__a-dataset.parquet").touch()
    assert bf.expand_inputs([str(d)]) == [d / "a__a-dataset.parquet"]


def test_an_explicit_file_is_taken_as_given(tmp_path):
    f = tmp_path / "anything.parquet"
    f.touch()
    assert bf.expand_inputs([str(f)]) == [f]


def test_a_file_named_twice_is_planned_once(tmp_path):
    f = tmp_path / "x-dataset.parquet"
    f.touch()
    assert bf.expand_inputs([str(f), str(f)]) == [f]


def test_a_destination_keeps_the_input_basename(tmp_path):
    src = tmp_path / "corpus" / "a__a" / "a__a-dataset.parquet"
    src.parent.mkdir(parents=True)
    src.touch()
    jobs = bf.plan([str(src)], str(tmp_path / "out"))
    assert jobs[0].dst == tmp_path / "out" / "a__a-dataset.parquet"


def test_writing_over_the_input_is_refused(tmp_path):
    """Never overwrite an input in place -- the published corpus is the only copy
    of source_text there is."""
    src = tmp_path / "a__a-dataset.parquet"
    src.touch()
    with pytest.raises(ValueError, match="over its own input"):
        bf.plan([str(src)], str(tmp_path))


def test_writing_over_the_input_is_refused_through_a_relative_path(tmp_path,
                                                                  monkeypatch):
    src = tmp_path / "a__a-dataset.parquet"
    src.touch()
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError, match="over its own input"):
        bf.plan(["a__a-dataset.parquet"], ".")


def test_a_dry_run_plans_no_destination_at_all(tmp_path):
    """Nothing to collide with, and nothing to accidentally write."""
    src = tmp_path / "a__a-dataset.parquet"
    src.touch()
    jobs = bf.plan([str(src)], None)
    assert jobs[0].dst is None


# --------------------------------------------------------------------------- #
# verify(), driven with fabricated aggregates so every failure path is covered
# without duckdb
# --------------------------------------------------------------------------- #

CLEAN_AFTER = {
    "rows": 10,
    "tab_in_token_type": 0,
    "corrupt": 0,
    "distinct_token_type": 4,
    "structural": 2,
    "structural_with_text": 0,
    "h_token_type": 1, "h_token_value": 2, "h_source_text": 3,
    "h_source_line": 4, "h_source_col": 5,
}
CLEAN_CHANGES = {"chg_value_on_content": 0, "chg_text_on_content": 0}


@pytest.fixture
def no_drift(monkeypatch):
    monkeypatch.setattr(bf, "schema_drift", lambda path: [])


@pytest.fixture
def job(tmp_path):
    return bf.Job(src=tmp_path / "in.parquet", dst=tmp_path / "out.parquet")


def test_verify_passes_when_every_check_holds(no_drift, job):
    bf.verify(job, 10, CLEAN_CHANGES, dict(CLEAN_AFTER), dict(CLEAN_AFTER), "w")


def test_verify_rejects_a_drifted_output(monkeypatch, job):
    monkeypatch.setattr(bf, "schema_drift",
                        lambda path: [vs.Drift("missing", "token_type", "gone")])
    with pytest.raises(bf.VerificationError, match="70-column contract"):
        bf.verify(job, 10, CLEAN_CHANGES, dict(CLEAN_AFTER), dict(CLEAN_AFTER), "w")


def test_verify_rejects_a_changed_row_count(no_drift, job):
    """A correct re-run emits one more row per .rs file. This script must never
    produce that row, so the count is an invariant, not a target."""
    with pytest.raises(bf.VerificationError, match="row count changed"):
        bf.verify(job, 11, CLEAN_CHANGES, dict(CLEAN_AFTER), dict(CLEAN_AFTER), "w")


def test_verify_rejects_a_surviving_tab_in_token_type(no_drift, job):
    after = dict(CLEAN_AFTER, tab_in_token_type=3)
    with pytest.raises(bf.VerificationError, match="TAB in token_type"):
        bf.verify(job, 10, CLEAN_CHANGES, after, after, "w")


def test_verify_rejects_a_surviving_corrupt_row(no_drift, job):
    after = dict(CLEAN_AFTER, corrupt=1)
    with pytest.raises(bf.VerificationError, match="corruption detector"):
        bf.verify(job, 10, CLEAN_CHANGES, after, after, "w")


def test_verify_rejects_a_structural_row_that_kept_its_source_text(no_drift, job):
    """is_structural = 1 => source_text = '' holds in correct data with 0
    violations over 190,683 measured rows. Repairing is_structural while leaving
    a stale source fragment would break it."""
    after = dict(CLEAN_AFTER, structural_with_text=2)
    with pytest.raises(bf.VerificationError, match="non-empty source_text"):
        bf.verify(job, 10, CLEAN_CHANGES, after, after, "w")


def test_verify_rejects_a_token_value_change_on_a_content_row(no_drift, job):
    """token_value is correct on every content row and this is what proves it."""
    changes = dict(CLEAN_CHANGES, chg_value_on_content=5)
    with pytest.raises(bf.VerificationError, match="token_value changed on 5"):
        bf.verify(job, 10, changes, dict(CLEAN_AFTER), dict(CLEAN_AFTER), "w")


def test_verify_rejects_a_source_text_change_on_a_content_row(no_drift, job):
    changes = dict(CLEAN_CHANGES, chg_text_on_content=1)
    with pytest.raises(bf.VerificationError, match="does not repair source_text"):
        bf.verify(job, 10, changes, dict(CLEAN_AFTER), dict(CLEAN_AFTER), "w")


@pytest.mark.parametrize("key", [
    "distinct_token_type", "structural", "h_token_type", "h_token_value",
    "h_source_text", "h_source_line", "h_source_col",
])
def test_verify_rejects_a_written_file_that_disagrees_with_the_plan(no_drift, job,
                                                                   key):
    """The hash sums are what prove the COPY round-tripped the values, rather than
    the plan merely being right on paper."""
    after = dict(CLEAN_AFTER)
    after[key] = CLEAN_AFTER[key] + 1
    with pytest.raises(bf.VerificationError, match=f"disagrees .* on {key}"):
        bf.verify(job, 10, CLEAN_CHANGES, dict(CLEAN_AFTER), after, "w")


# --------------------------------------------------------------------------- #
# reporting and the CLI surface
# --------------------------------------------------------------------------- #

def test_a_skip_prints_the_reason(tmp_path, capsys):
    bf.report(bf.Outcome(tmp_path / "jq-dataset.parquet", "SKIP", "not 70"))
    err = capsys.readouterr().err
    assert "SKIP" in err and "not 70" in err


def test_a_clean_file_reports_on_stdout(tmp_path, capsys):
    bf.report(bf.Outcome(tmp_path / "a.parquet", "CLEAN", "0 corrupt"))
    out = capsys.readouterr()
    assert "CLEAN" in out.out
    assert out.err == ""


BEFORE_STATS = dict(CLEAN_AFTER, distinct_token_type=11_824,
                    tab_in_token_type=12_271, structural=7, rows=12_278)
AFTER_STATS = dict(CLEAN_AFTER, distinct_token_type=8, tab_in_token_type=0,
                   structural=14, rows=12_278)
REPORT_CHANGES = dict(CLEAN_CHANGES, position_agree_before=0,
                      position_checkable=12_264, position_unrecoverable=14,
                      decl_shaped=0)


def test_a_repair_reports_before_and_after_for_every_measured_column(tmp_path,
                                                                    capsys):
    """These are the numbers a reviewer reads, so the formatting is covered too.
    The values are the real ones measured on cloudflare__wildcard."""
    bf.report(bf.Outcome(tmp_path / "cloudflare__wildcard-dataset.parquet",
                         "REPAIRED", "12278 of 12278 rows",
                         before=BEFORE_STATS, after=AFTER_STATS,
                         changes=REPORT_CHANGES))
    out = capsys.readouterr().out
    assert "11,824 ->            8" in out
    assert "12,271 ->            0" in out
    assert "7 ->           14" in out
    assert "0 of 12,264" in out
    # The 14 unit-marker rows whose embedded position is -:-, named rather than
    # quietly left behind.
    assert "position kept as-is" in out
    assert "14 rows" in out
    assert "N:-" not in out


def test_a_decl_shaped_position_is_named_in_the_report(tmp_path, capsys):
    bf.report(bf.Outcome(tmp_path / "a.parquet", "REPAIRED", "",
                         before=BEFORE_STATS, after=AFTER_STATS,
                         changes=dict(REPORT_CHANGES, decl_shaped=3)))
    assert "3 of them N:-" in capsys.readouterr().out


def test_a_dry_run_reports_before_only(tmp_path, capsys):
    bf.report(bf.Outcome(tmp_path / "a.parquet", "WOULD-REPAIR", "",
                         before=BEFORE_STATS, changes=REPORT_CHANGES))
    out = capsys.readouterr().out
    assert "11,824" in out
    assert "->" not in out


def test_out_is_required_unless_dry_run():
    with pytest.raises(SystemExit) as e:
        bf.main(["some.parquet"])
    assert e.value.code == bf.EXIT_USAGE


def test_no_parquet_found_is_a_usage_error(tmp_path, capsys):
    assert bf.main([str(tmp_path), "--out", str(tmp_path / "o")]) == bf.EXIT_USAGE
    assert "no Parquet files found" in capsys.readouterr().err


def test_writing_over_the_input_is_a_usage_error(tmp_path, capsys):
    src = tmp_path / "a-dataset.parquet"
    src.touch()
    assert bf.main([str(src), "--out", str(tmp_path)]) == bf.EXIT_USAGE
    assert "over its own input" in capsys.readouterr().err


# --------------------------------------------------------------------------- #
# end to end over real Parquet files
#
# These write and read actual Parquet, so they need the real duckdb from
# `devenv shell` and skip without it -- the same gate tests/test_consolidate.py
# uses for its five parquet tests.
# --------------------------------------------------------------------------- #

try:                                          # pragma: no cover - import plumbing
    import duckdb as _duckdb
except ModuleNotFoundError:                   # pragma: no cover - import plumbing
    _duckdb = None

requires_duckdb = pytest.mark.skipif(
    _duckdb is None, reason="needs real duckdb (devenv shell) to write parquet")


def _literal(value) -> str:
    if value is None:
        return "null"
    if isinstance(value, int):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def write_dataset(path: Path, rows: list[dict], columns=CONTRACT) -> Path:
    """A Parquet file with `columns`, one row per dict; absent columns are NULL.

    Casting every value to the contract type is what makes the file a legitimate
    stand-in for a published project: the script's own schema gate has to accept
    it, so a typo in the fixture fails loudly here rather than silently widening
    what the tests cover.
    """
    selects = []
    for row in rows:
        parts = [f'cast({_literal(row.get(name))} as {typ}) as "{name}"'
                 for name, typ in columns]
        selects.append("select " + ", ".join(parts))
    _duckdb.sql(f"copy ({' union all '.join(selects)}) "
                f"to '{path}' (format parquet)")
    return path


def content_row(index: int, line: int, col: int, kind: str, value: str,
                text: str = "stale") -> dict:
    """A corrupt ordinary token: `line:col<TAB>kind` ended up in token_type."""
    return {
        "repo_name": "cloudflare__wildcard", "file_path": "src/lib.rs",
        "token_index": index,
        "token_type": f"{line}:{col}{TAB}{kind}", "token_value": value,
        # Desynchronised: the walked cursor ran ahead of the true position.
        "source_line": line + 2, "source_col": col + 20,
        "source_text": text, "is_structural": 0,
    }


BEGIN_UNIT_ROW = {
    "repo_name": "cloudflare__wildcard", "file_path": "src/lib.rs",
    "token_index": 0,
    "token_type": f"-:-{TAB}begin_unit",
    "token_value": "revision:0.0.1;language:Rust;cregit-version:0.0.1",
    "source_line": 1, "source_col": 1,
    "source_text": "// Copyright 2024 Cloudflare, Inc.", "is_structural": 0,
}

END_UNIT_ROW = {
    "repo_name": "cloudflare__wildcard", "file_path": "src/lib.rs",
    "token_index": 99,
    # No pipe in the line at all, so the consumer's regex never matched.
    "token_type": "unknown", "token_value": f"-:-{TAB}end_unit",
    "source_line": 40, "source_col": 1, "source_text": "", "is_structural": 1,
}

CORRECT_C_ROW = {
    "repo_name": "suse__pam-config", "file_path": "src/load_config.c",
    "token_index": 1, "token_type": "name", "token_value": "argc",
    "source_line": 12, "source_col": 7, "source_text": "argc, ",
    "is_structural": 0,
}


@pytest.fixture
def con():
    return _duckdb.connect()


def read_rows(path: Path, columns: str) -> list[tuple]:
    return _duckdb.sql(
        f"select {columns} from read_parquet('{path}') order by token_index"
    ).fetchall()


@requires_duckdb
def test_a_correct_file_is_clean_and_is_not_written(con, tmp_path):
    """Idempotence by a property of the data: nothing to repair means nothing is
    written, so a second pass over a repaired corpus spends no disk."""
    src = write_dataset(tmp_path / "suse__pam-config-dataset.parquet",
                        [CORRECT_C_ROW])
    job = bf.Job(src=src, dst=tmp_path / "out" / src.name)

    outcome = bf.repair_file(con, job)

    assert outcome.status == "CLEAN"
    assert not job.dst.exists()


@requires_duckdb
def test_a_content_row_gets_its_type_and_true_position_back(con, tmp_path):
    src = write_dataset(tmp_path / "a-dataset.parquet",
                        [content_row(1, 3, 1, "comment", "// hi")])
    job = bf.Job(src=src, dst=tmp_path / "out" / src.name)

    outcome = bf.repair_file(con, job)

    assert outcome.status == "REPAIRED"
    assert read_rows(job.dst, "token_type, source_line, source_col, is_structural") \
        == [("comment", 3, 1, 0)]


@requires_duckdb
def test_a_content_row_keeps_its_token_value_and_its_wrong_source_text(con,
                                                                      tmp_path):
    """token_value was never corrupted on a content row, and source_text cannot be
    recovered -- so neither is touched."""
    src = write_dataset(tmp_path / "a-dataset.parquet",
                        [content_row(1, 3, 1, "comment", "// hi", text="stale")])
    job = bf.Job(src=src, dst=tmp_path / "out" / src.name)

    bf.repair_file(con, job)

    assert read_rows(job.dst, "token_value, source_text") == [("// hi", "stale")]


@requires_duckdb
def test_the_begin_unit_row_becomes_what_a_correct_project_carries(con, tmp_path):
    """Measured on C and Java projects: token_type 'begin_unit', token_value the
    whole line, is_structural 1, source_text ''."""
    src = write_dataset(tmp_path / "a-dataset.parquet", [BEGIN_UNIT_ROW])
    job = bf.Job(src=src, dst=tmp_path / "out" / src.name)

    bf.repair_file(con, job)

    assert read_rows(job.dst,
                     "token_type, token_value, is_structural, source_text") == [
        ("begin_unit",
         "begin_unit|revision:0.0.1;language:Rust;cregit-version:0.0.1",
         1, "")]


@requires_duckdb
def test_the_begin_unit_row_keeps_its_position_because_minus_is_not_a_number(
        con, tmp_path):
    """`-:-` recovers neither line nor column. The published values happen to be
    correct here -- the walking cursor has consumed nothing yet -- so nulling them
    would destroy good data."""
    src = write_dataset(tmp_path / "a-dataset.parquet", [BEGIN_UNIT_ROW])
    job = bf.Job(src=src, dst=tmp_path / "out" / src.name)

    outcome = bf.repair_file(con, job)

    assert read_rows(job.dst, "source_line, source_col") == [(1, 1)]
    assert outcome.changes["position_unrecoverable"] == 1


@requires_duckdb
def test_the_end_unit_row_is_recovered_from_token_value(con, tmp_path):
    """This row is the one the brief and LIMITATIONS.md both miss: the consumer's
    regex found no pipe, so token_type became 'unknown' and the tab-delimited
    string landed in token_value. Leaving it would leave tab-corrupted text in a
    file this script had just declared clean."""
    src = write_dataset(tmp_path / "a-dataset.parquet", [END_UNIT_ROW])
    job = bf.Job(src=src, dst=tmp_path / "out" / src.name)

    bf.repair_file(con, job)

    assert read_rows(job.dst,
                     "token_type, token_value, is_structural, source_text") == [
        ("end_unit", "end_unit", 1, "")]


@requires_duckdb
def test_a_decl_shaped_position_gives_up_both_components(con, tmp_path):
    """`N:-` has a line but no column, and a correct run writes the walking
    cursor's position there anyway, not the tokenizer's line. Guessing would be
    worse than leaving it, so the row is left alone and counted."""
    row = dict(content_row(1, 9, 1, "x", "y"),
               token_type=f"9:-{TAB}DECL|variable|helptext",
               token_value="", source_line=40, source_col=3)
    src = write_dataset(tmp_path / "a-dataset.parquet", [row])
    job = bf.Job(src=src, dst=tmp_path / "out" / src.name)

    outcome = bf.repair_file(con, job)

    assert read_rows(job.dst, "token_type, source_line, source_col, is_structural") \
        == [("DECL", 40, 3, 1)]
    assert outcome.changes["decl_shaped"] == 1
    assert outcome.changes["position_unrecoverable"] == 1


@requires_duckdb
def test_a_legitimate_tab_inside_a_token_value_is_not_corruption(con, tmp_path):
    """102 rows of the control projects hold a TAB in token_value. None of them
    starts with a position, so none is mistaken for a wreck."""
    rows = [
        dict(CORRECT_C_ROW, token_index=1, token_type="literal",
             token_value=f'"a{TAB}b"'),
        dict(CORRECT_C_ROW, token_index=2, token_type="unknown",
             token_value=f"not-a-position{TAB}end_unit"),
    ]
    src = write_dataset(tmp_path / "a-dataset.parquet", rows)
    job = bf.Job(src=src, dst=tmp_path / "out" / src.name)

    assert bf.repair_file(con, job).status == "CLEAN"


@requires_duckdb
def test_a_repaired_file_is_clean_on_a_second_pass(con, tmp_path):
    """The whole idempotency claim, end to end."""
    src = write_dataset(tmp_path / "a-dataset.parquet",
                        [BEGIN_UNIT_ROW,
                         content_row(1, 3, 1, "comment", "// hi"),
                         END_UNIT_ROW])
    first = bf.Job(src=src, dst=tmp_path / "out" / src.name)
    assert bf.repair_file(con, first).status == "REPAIRED"

    second = bf.Job(src=first.dst, dst=tmp_path / "out2" / src.name)
    outcome = bf.repair_file(con, second)

    assert outcome.status == "CLEAN"
    assert not second.dst.exists()


@requires_duckdb
def test_the_row_count_never_changes(con, tmp_path):
    rows = [BEGIN_UNIT_ROW] + [content_row(i, i, 1, "op", "+") for i in range(1, 6)] \
        + [END_UNIT_ROW]
    src = write_dataset(tmp_path / "a-dataset.parquet", rows)
    job = bf.Job(src=src, dst=tmp_path / "out" / src.name)

    outcome = bf.repair_file(con, job)

    assert outcome.before["rows"] == outcome.after["rows"] == len(rows)


@requires_duckdb
def test_the_repair_restores_the_structural_invariant_on_a_whole_file(con,
                                                                     tmp_path):
    src = write_dataset(tmp_path / "a-dataset.parquet",
                        [BEGIN_UNIT_ROW,
                         content_row(1, 3, 1, "comment", "// hi"),
                         END_UNIT_ROW])
    job = bf.Job(src=src, dst=tmp_path / "out" / src.name)

    outcome = bf.repair_file(con, job)

    # Two of the three rows carry the prefix in token_type: the content row and
    # the begin_unit row. The end_unit row carries it in token_value instead,
    # which is exactly why `corrupt` below is the check that matters and a count
    # of tabs in token_type alone would have missed it.
    assert outcome.before["tab_in_token_type"] == 2
    assert outcome.after["tab_in_token_type"] == 0
    assert outcome.after["corrupt"] == 0
    assert outcome.after["structural"] == 2
    assert outcome.after["structural_with_text"] == 0
    assert outcome.changes["position_agree_before"] == 0
    assert outcome.changes["position_checkable"] == 1


@requires_duckdb
def test_a_legacy_38_column_file_is_refused_even_when_it_is_corrupt(con, tmp_path):
    """The dangerous case: `rustlings` in the corpus is both 38-column and
    tab-bearing. Repairing it would write a file that is neither the old schema
    nor the contract."""
    legacy = CONTRACT[:30] + CONTRACT[30:38]
    src = write_dataset(tmp_path / "rustlings-dataset.parquet",
                        [content_row(1, 3, 1, "comment", "// hi")],
                        columns=legacy)
    job = bf.Job(src=src, dst=tmp_path / "out" / src.name)

    outcome = bf.repair_file(con, job)

    assert outcome.status == "SKIP"
    assert "70-column contract" in outcome.detail
    assert not job.dst.exists()


@requires_duckdb
def test_a_dry_run_measures_and_writes_nothing(con, tmp_path):
    src = write_dataset(tmp_path / "a-dataset.parquet",
                        [content_row(1, 3, 1, "comment", "// hi")])
    job = bf.Job(src=src, dst=None)

    outcome = bf.repair_file(con, job, dry_run=True)

    assert outcome.status == "WOULD-REPAIR"
    assert outcome.before["tab_in_token_type"] == 1
    assert not (tmp_path / "out").exists()


@requires_duckdb
def test_a_failed_verification_leaves_no_file_behind(con, tmp_path, monkeypatch):
    """Fail closed: the write lands on <name>.partial and is renamed only after
    every check passes, so a failure cannot leave a file that looks repaired."""
    src = write_dataset(tmp_path / "a-dataset.parquet",
                        [content_row(1, 3, 1, "comment", "// hi")])
    job = bf.Job(src=src, dst=tmp_path / "out" / src.name)

    def boom(*args, **kwargs):
        raise bf.VerificationError("planted")

    monkeypatch.setattr(bf, "verify", boom)

    with pytest.raises(bf.VerificationError, match="planted"):
        bf.repair_file(con, job)

    assert not job.dst.exists()
    assert not job.dst.with_name(job.dst.name + ".partial").exists()


@requires_duckdb
def test_the_output_satisfies_the_schema_gate(con, tmp_path):
    """The repaired file must be indexable alongside every other project."""
    src = write_dataset(tmp_path / "a-dataset.parquet",
                        [BEGIN_UNIT_ROW,
                         content_row(1, 3, 1, "comment", "// hi"),
                         END_UNIT_ROW])
    job = bf.Job(src=src, dst=tmp_path / "out" / src.name)

    bf.repair_file(con, job)

    assert vs.compare_schema(vs.read_schema(str(job.dst))) == []


@requires_duckdb
def test_main_reports_a_repair_and_exits_zero(tmp_path, capsys):
    src = write_dataset(tmp_path / "a-dataset.parquet",
                        [content_row(1, 3, 1, "comment", "// hi")])

    code = bf.main([str(src), "--out", str(tmp_path / "out")])

    assert code == bf.EXIT_OK
    out = capsys.readouterr().out
    assert "REPAIRED" in out
    assert "1 REPAIRED" in out


@requires_duckdb
def test_main_exits_non_zero_when_a_file_was_skipped(tmp_path, capsys):
    legacy = CONTRACT[:30] + CONTRACT[30:38]
    src = write_dataset(tmp_path / "jq-dataset.parquet", [CORRECT_C_ROW],
                        columns=legacy)

    code = bf.main([str(src), "--out", str(tmp_path / "out")])

    assert code == bf.EXIT_SKIPPED
    assert "SKIP" in capsys.readouterr().err
