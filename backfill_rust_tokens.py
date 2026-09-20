#!/usr/bin/env python3
"""Backfill the four columns the Rust tokenizer corrupted, in published Parquet.

    ./backfill_rust_tokens.py --out REPAIRED/ CORPUS/*/*-dataset.parquet
    ./backfill_rust_tokens.py --out REPAIRED/ CORPUS/            # globs the corpus
    ./backfill_rust_tokens.py --dry-run CORPUS/                  # measure, write nothing

WHY THIS EXISTS, AND WHAT IT CANNOT DO
======================================
`docs/LIMITATIONS.md` ("On every `.rs` row, four columns are wrong") documents the
defect: the Rust tokenizer separated a token's position from its type with a TAB,
and emitted that position prefix even though the pipeline never passes
`--position`. The consumer splits a token line on the first PIPE:

    generate_dataset/generate_dataset.py:243   m = re.match(r"^(.+?)\\|(.+)$", token_content)

`(.+?)` is non-greedy, so for a Rust line `12:5<TAB>keyword|fn` the whole
`12:5<TAB>keyword` landed in `token_type` and every later field shifted.

This script recovers what is recoverable and refuses to pretend about the rest.
It does NOT re-run the pipeline, and a backfilled file is NOT byte-identical to a
re-run -- see "NOT REPAIRED" below.

THE METHOD: UNDO THE SPLIT, STRIP THE PREFIX, RE-APPLY THE CONSUMER'S PARSE
===========================================================================
Rather than patch each column with its own ad-hoc rule, this script reconstructs
the tokenizer line the consumer *should* have seen and runs the consumer's own
classifier over it, in SQL. That is one rule instead of five, so the marker rows
fall out correctly instead of needing special cases.

Step 1, reconstruct the corrupt line. The split was lossless (`token_type` is
group 1, `token_value` is group 2, verbatim -- generate_dataset.py:255-256), so:

    token_type || '|' || token_value          when token_type carries the prefix
    token_value                               when the regex found no pipe at all,
                                              which is the `end_unit` row: it has
                                              no value, so token_type became
                                              'unknown' and the whole
                                              `-:-<TAB>end_unit` string went into
                                              token_value (generate_dataset.py:244)

Step 2, strip the leading `pos<TAB>`, where the position grammar is

    N:N     an ordinary token          tokenizeSrcMl.pl:136   print "$line:$col|"
    N:-     a DECL line                tokenizeSrcMl.pl:128   print "$line:-|"
    -:-     a unit marker              tokenizeSrcMl.pl:141   print "-:-|"

Step 3, re-apply `generate_dataset.classify_and_skip` (generate_dataset.py:171).
It is an ordered if/elif chain and the FIRST match wins; `is_structural` is not
an expression but a literal per branch. The branches, in source order, are
mirrored by `_BRANCH_SQL` below. Getting the order wrong would, for example,
classify `begin_unit|revision:...` as an ordinary `begin_unit`-typed content
token instead of a structural marker.

REPAIRED
--------
`token_type`     from the text after the position prefix, re-classified.
`source_line`    from the embedded position, when BOTH components are numeric.
`source_col`     likewise.
`is_structural`  the literal the matching branch assigns.
`token_value`    on structural marker rows ONLY -- see the note below.
`source_text`    set to '' on structural marker rows ONLY -- see the note below.

NOT REPAIRED, AND NOT FAKED
---------------------------
`source_text` on content rows. It desynchronised from the first row onward,
because the tokenizer misread `begin_unit` as an ordinary token and the reader
consumed source past it. It is not derivable from the Parquet. No sentinel is
written: the column keeps its wrong value, because a column that is wrong
everywhere is easier to document and harder to misuse than one that is wrong
almost everywhere and blanked in 0.06% of rows.

`source_line`/`source_col` on rows whose embedded position is not fully numeric.
`-:-` (unit markers) yields neither; `N:-` (DECL) yields a line but no column,
and a correct run does not write the tokenizer's line there anyway -- it writes
the position of a cursor walking the source (generate_dataset.py:174,
`reader.location()`), which for a DECL row sits wherever the previous token left
it. Guessing would be worse than leaving it, so these rows are left alone and
counted as `position_unrecoverable`.

Row alignment with a re-run. The FIXED tokenizer emits a bare marker line after
`end_unit` (tokenize/rustTokenizer/src/main.rs, `marker("")`, mirroring
tokenizeSrcMl.pl:143), which the consumer classifies as `blank`. The published
Rust data has no such row, so a correct re-run emits exactly ONE MORE row per
`.rs` file than this script can produce. This script never invents that row: row
count is an invariant it checks, not a thing it changes.

WHY `token_value` AND `source_text` ARE TOUCHED AT ALL
=====================================================
`docs/LIMITATIONS.md` says "`token_value` is correct". That is true for every
content row and this script proves it row by row (`chg_value_on_content` must be
0). It is NOT true for the two structural marker rows in each `.rs` file, and
both are repaired because leaving them would leave tab-corrupted text in the
dataset while this script's own success criterion -- no TAB in `token_type` --
reported all clear:

  `end_unit` row   before: token_type 'unknown', token_value '-:-<TAB>end_unit'
                   after:  token_type 'end_unit', token_value 'end_unit'
  `begin_unit` row before: token_type '-:-<TAB>begin_unit',
                           token_value 'revision:...'
                   after:  token_type 'begin_unit',
                           token_value 'begin_unit|revision:...'

Both "after" forms are exactly what a correct project already carries; they were
read off C and Java projects in the same corpus, not invented here.

`source_text` is set to '' on those same rows for the same reason. Every
structural branch of `classify_and_skip` sets `"source_text": ""`, so
`is_structural = 1 => source_text = ''` is an invariant of correct data (0
violations measured over 190,683 rows of three non-Rust projects). Repairing
`is_structural` to 1 while leaving a stale 40-character source fragment behind
would break that invariant and introduce a new inconsistency of this script's own
making. So the repair restores it. This is not a sentinel and not a guess: '' is
the value a re-run writes.

FAIL-CLOSED PROPERTIES
======================
* A file whose schema is not the 70-column contract (`validate_schema.py`,
  `EXPECTED_COLUMNS`) is refused and named on stderr. Eleven Parquet files in the
  corpus are still at the older 38- and 23-column schemas; they are skipped
  loudly and the process exits non-zero.
* An input is never written to. The output goes to `--out`, and an output path
  that resolves to its own input is refused.
* The write lands on `<name>.partial` and is renamed only after every check
  passes, so a failed verification never leaves behind a file that looks
  repaired.
* Verification raises `VerificationError`. It does not warn.
* Idempotent by a property of the data, not a marker file: a file with no
  tab-delimited position prefix has nothing to repair and is reported CLEAN and
  not written. Running this script over its own output is therefore a no-op, and
  it cannot double-split. See `DETECTOR_SQL`.

duckdb is imported inside the functions that need it, following
`validate_schema.py`, so every SQL-string builder and the planning logic are
unit-testable without duckdb installed.
"""
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

import validate_schema

EXIT_OK = 0
EXIT_SKIPPED = 1
EXIT_USAGE = 2

DATASET_GLOB = "*-dataset.parquet"

# The position grammar, as a regex fragment. `N` is one or more digits and `-`
# means "this component does not exist". tokenizeSrcMl.pl:119 parses the same
# shape on the way in:
#     $line =~ /^([0-9]+|-):([0-9]+|-)\s+(.+)$/
POSITION_RE = "([0-9]+|-):([0-9]+|-)"

# The consumer's own split, reproduced verbatim from generate_dataset.py:243.
# A raw string: DuckDB string literals do not interpret backslash escapes, so the
# regex engine receives `\|` and reads it as a literal pipe.
PIPE_SPLIT_RE = r"^(.+?)\|(.+)$"

# DuckDB has no escape for TAB inside a string literal, so every place that needs
# one concatenates chr(9). Writing '\t' would silently match a backslash and a t.
TAB = "chr(9)"

# Anchored: the prefix must be at the START of the value. This is what makes the
# detector and the strip safe. A bare `strpos(token_type, chr(9)) > 0` would fire
# on any TAB anywhere and would strip through it; this cannot.
_PREFIX = f"'^{POSITION_RE}' || {TAB}"

# The idempotency detector, and the definition of "this row is corrupt".
#
# Two shapes, because the consumer's pipe-split produced two different wrecks:
#   1. the regex matched, so the prefix sits at the front of token_type
#   2. the regex did not match (the line had no pipe -- the `end_unit` row), so
#      token_type is the literal 'unknown' and the whole line sits in token_value
#
# It cannot be fooled into a double-split, because a repaired row has no
# tab-delimited position prefix left in either place, so neither arm matches. It
# cannot fire on correct data either: measured 0 matches over 190,683 rows of
# three non-Rust projects, and 12,278 of 12,278 on a Rust one. And because it is
# anchored to the position grammar rather than to "contains a TAB", a legitimate
# TAB inside a token value does not trigger it -- 102 such rows exist in the
# control projects and none match.
DETECTOR_SQL = (
    f"regexp_matches(token_type, {_PREFIX}) "
    f"OR (token_type = 'unknown' AND regexp_matches(token_value, {_PREFIX}))"
)

# generate_dataset.classify_and_skip (generate_dataset.py:171-276), branch by
# branch, in source order. Branch 0 is this script's own addition: "not corrupt,
# do not touch". Branch 8 is the only one that yields is_structural = 0.
#
# (branch, predicate on the corrected token line, token_type, token_value)
_BRANCHES: tuple[tuple[int, str, str, str], ...] = (
    (1, "ctp_line LIKE 'begin_unit%'", "'begin_unit'", "ctp_line"),
    (2, "ctp_line IN ('begin_function', 'end_function')", "ctp_line", "ctp_line"),
    (3, "ctp_line LIKE 'DECL|%'", "'DECL'", "ctp_line"),
    (4, "ctp_line = '|'", "''", "''"),
    (5, "ctp_line = ''", "'blank'", "''"),
    (6, "regexp_full_match(ctp_line, '(begin|end)_[a-z_]+')", "ctp_line", "ctp_line"),
    (7, f"NOT regexp_matches(ctp_line, '{PIPE_SPLIT_RE}')", "'unknown'", "ctp_line"),
)
_CONTENT_BRANCH = 8
_CONTENT_TYPE = f"regexp_extract(ctp_line, '{PIPE_SPLIT_RE}', 1)"
_CONTENT_VALUE = f"regexp_extract(ctp_line, '{PIPE_SPLIT_RE}', 2)"

# Columns this script may rewrite. Everything else in the 70 is selected by name
# and passes through untouched; selecting by name is also what preserves the
# LIST(VARCHAR) footer columns.
REPAIRED_COLUMNS = (
    "token_type", "token_value", "source_text", "source_line", "source_col",
    "is_structural",
)


class VerificationError(RuntimeError):
    """A post-write check failed. The output was not kept."""


# --------------------------------------------------------------------------- #
# SQL builders. Pure string functions -- no duckdb, no filesystem.
# --------------------------------------------------------------------------- #

def branch_sql() -> str:
    """The CASE that assigns each row its classify_and_skip branch number."""
    arms = "\n".join(
        f"      WHEN {pred} THEN {n}" for n, pred, _, _ in _BRANCHES)
    return (
        "    CASE\n"
        "      WHEN ctp_line IS NULL THEN 0\n"
        f"{arms}\n"
        f"      ELSE {_CONTENT_BRANCH}\n"
        "    END AS ctp_branch"
    )


def _branch_case(untouched: str, content: str, which: int) -> str:
    """A CASE over ctp_branch picking the value classify_and_skip would assign.

    `which` selects the 0-based slot in each _BRANCHES entry's value pair:
    0 for token_type, 1 for token_value.
    """
    arms = "\n".join(
        f"           WHEN {entry[0]} THEN {entry[2 + which]}" for entry in _BRANCHES)
    return (f"CASE ctp_branch\n"
            f"           WHEN 0 THEN {untouched}\n"
            f"{arms}\n"
            f"           ELSE {content}\n"
            f"      END")


def repaired_columns_sql() -> dict[str, str]:
    """The bare expression for each rewritten column, keyed by column name.

    No `AS name` alias: callers add their own, because `changes_sql` needs the
    same expressions under `new_*` names alongside the originals.
    """
    return {
        "token_type": _branch_case("token_type", _CONTENT_TYPE, which=0),
        "token_value": _branch_case("token_value", _CONTENT_VALUE, which=1),
        # Literal per branch, never an expression -- see generate_dataset.py:183,
        # 194, 206, 217, 228, 239, 251 (all 1) and :274 (0).
        "is_structural": (
            "CAST(CASE WHEN ctp_branch = 0 THEN is_structural\n"
            f"                WHEN ctp_branch = {_CONTENT_BRANCH} THEN 0\n"
            "                ELSE 1 END AS BIGINT)"),
        # Only when BOTH components are numeric. try_cast turns '-' into NULL,
        # which is why this is not built on regexp_extract: that returns '' and
        # '' would need a coalesce() chain that silently joins empty strings.
        "source_line": (
            "CAST(CASE WHEN ctp_pos_line IS NOT NULL AND ctp_pos_col IS NOT NULL\n"
            "                THEN ctp_pos_line ELSE source_line END AS BIGINT)"),
        "source_col": (
            "CAST(CASE WHEN ctp_pos_line IS NOT NULL AND ctp_pos_col IS NOT NULL\n"
            "                THEN ctp_pos_col ELSE source_col END AS BIGINT)"),
        # '' on structural rows restores the is_structural = 1 => source_text = ''
        # invariant. Content rows (branch 8) and untouched rows (branch 0) keep
        # their value: on a corrupt content row it is wrong and unrecoverable.
        "source_text": (
            f"CAST(CASE WHEN ctp_branch IN (0, {_CONTENT_BRANCH}) THEN source_text\n"
            "                ELSE '' END AS VARCHAR)"),
    }


def contract_columns() -> tuple[str, ...]:
    """The 70 column names, in contract order, from validate_schema."""
    return tuple(name for name, _ in validate_schema.EXPECTED_COLUMNS)


def repair_sql(path: str, columns: tuple[str, ...] | None = None) -> str:
    """The SELECT that produces the repaired 70 columns for one Parquet file.

    `columns` defaults to the 70-column contract, so the output column set, order
    and types are the contract by construction rather than by hand.
    """
    names = columns if columns is not None else contract_columns()
    repaired = repaired_columns_sql()
    projected = ",\n".join(
        f"    {repaired[name]} AS {name}" if name in repaired else f"    {name}"
        for name in names)
    return f"""SELECT
{projected}
FROM ({parsed_sql(path)})"""


def parsed_sql(path: str) -> str:
    """Every input column plus the derived ctp_* helpers, for one Parquet file.

    ctp_raw       the tokenizer line as it was before the consumer split it, or
                  NULL when the row is not corrupt
    ctp_line      that line with the position prefix stripped -- what the
                  consumer should have been handed
    ctp_pos_line  the position's line component as BIGINT, NULL for '-'
    ctp_pos_col   the position's column component as BIGINT, NULL for '-'
    ctp_branch    which classify_and_skip branch that line takes
    """
    literal = sql_string(path)
    return f"""  SELECT *,
{branch_sql()}
  FROM (
    SELECT *,
      CASE WHEN ctp_raw IS NULL THEN NULL
           ELSE regexp_replace(ctp_raw, '^{POSITION_RE}' || {TAB}, '') END AS ctp_line,
      CASE WHEN ctp_raw IS NULL THEN NULL
           ELSE try_cast(split_part(split_part(ctp_raw, {TAB}, 1), ':', 1) AS BIGINT)
           END AS ctp_pos_line,
      CASE WHEN ctp_raw IS NULL THEN NULL
           ELSE try_cast(split_part(split_part(ctp_raw, {TAB}, 1), ':', 2) AS BIGINT)
           END AS ctp_pos_col
    FROM (
      SELECT *,
        CASE
          WHEN regexp_matches(token_type, {_PREFIX}) THEN token_type || '|' || token_value
          WHEN token_type = 'unknown' AND regexp_matches(token_value, {_PREFIX})
            THEN token_value
        END AS ctp_raw
      FROM read_parquet({literal})
    )
  )"""


def detect_sql(path: str) -> str:
    """Count the corrupt rows in one file without rewriting anything."""
    return (f"SELECT count(*) FILTER (WHERE {DETECTOR_SQL}), count(*) "
            f"FROM read_parquet({sql_string(path)})")


# The aggregate battery. Deliberately ONE query text, applied to the repaired
# expression tree and to the written file, so a prediction and its actual cannot
# drift apart: any change to the checks changes both sides at once.
STATS_SQL = f"""SELECT
  count(*)                                                              AS rows,
  count(*) FILTER (WHERE strpos(token_type, {TAB}) > 0)                 AS tab_in_token_type,
  count(*) FILTER (WHERE {DETECTOR_SQL})                                AS corrupt,
  count(DISTINCT token_type)                                            AS distinct_token_type,
  sum(is_structural)                                                    AS structural,
  count(*) FILTER (WHERE is_structural = 1 AND source_text <> '')        AS structural_with_text,
  sum(hash(token_type))                                                 AS h_token_type,
  sum(hash(token_value))                                                AS h_token_value,
  sum(hash(source_text))                                                AS h_source_text,
  sum(hash(source_line))                                                AS h_source_line,
  sum(hash(source_col))                                                 AS h_source_col
FROM {{relation}}"""


def stats_sql(relation: str) -> str:
    return STATS_SQL.format(relation=f"({relation})")


def changes_sql(path: str) -> str:
    """Cell-level change counts, measured on the input against the repair.

    This is the only place that can see an original and its replacement side by
    side, so the two "must be zero" guarantees live here:
    `chg_value_on_content` and `chg_text_on_content`.
    """
    repaired = repaired_columns_sql()
    # Re-project the repair expressions next to the originals, renamed so both
    # are visible in one row. Same expressions, so the counts below describe the
    # repair that will actually be written.
    new = ",\n".join(
        f"    {repaired[c]} AS new_{c}" for c in REPAIRED_COLUMNS)
    return f"""SELECT
  count(*) FILTER (WHERE ctp_branch <> 0)                       AS repaired_rows,
  count(*) FILTER (WHERE new_token_type IS DISTINCT FROM token_type)   AS chg_token_type,
  count(*) FILTER (WHERE new_token_value IS DISTINCT FROM token_value) AS chg_token_value,
  count(*) FILTER (WHERE new_token_value IS DISTINCT FROM token_value
                     AND new_is_structural <> 1)                AS chg_value_on_content,
  count(*) FILTER (WHERE new_source_text IS DISTINCT FROM source_text) AS chg_source_text,
  count(*) FILTER (WHERE new_source_text IS DISTINCT FROM source_text
                     AND new_is_structural <> 1)                AS chg_text_on_content,
  count(*) FILTER (WHERE new_source_line IS DISTINCT FROM source_line) AS chg_source_line,
  count(*) FILTER (WHERE new_source_col IS DISTINCT FROM source_col)   AS chg_source_col,
  count(*) FILTER (WHERE ctp_branch <> 0
                     AND (ctp_pos_line IS NULL OR ctp_pos_col IS NULL))
                                                                AS position_unrecoverable,
  count(*) FILTER (WHERE ctp_pos_line IS NOT NULL AND ctp_pos_col IS NULL)
                                                                AS decl_shaped,
  count(*) FILTER (WHERE ctp_pos_line IS NOT NULL AND ctp_pos_col IS NOT NULL)
                                                                AS position_checkable,
  count(*) FILTER (WHERE ctp_pos_line IS NOT NULL AND ctp_pos_col IS NOT NULL
                     AND source_line = ctp_pos_line
                     AND source_col = ctp_pos_col)              AS position_agree_before
FROM (
  SELECT *,
{new}
  FROM ({parsed_sql(path)})
)"""


def copy_sql(path: str, out: str) -> str:
    """COPY the repaired projection to a Parquet file."""
    return (f"COPY ({repair_sql(path)}) TO {sql_string(out)} "
            f"(FORMAT PARQUET)")


def sql_string(value: str) -> str:
    """A single-quoted SQL literal. Doubles any embedded quote."""
    return "'" + str(value).replace("'", "''") + "'"


# --------------------------------------------------------------------------- #
# Planning. Pure -- no duckdb.
# --------------------------------------------------------------------------- #

@dataclass
class Job:
    src: Path
    dst: Path | None            # None under --dry-run: nothing will be written


def expand_inputs(inputs: list[str]) -> list[Path]:
    """Resolve each argument to Parquet files, deterministically ordered.

    A directory is searched one level down (`<project>/<project>-dataset.parquet`,
    which is the corpus layout) and also directly, so both a corpus root and a
    single project directory work. Deliberately a targeted glob and not a
    recursive walk: the corpus directory holds the per-project git repositories
    too, and walking it is expensive.
    """
    found: list[Path] = []
    for raw in inputs:
        p = Path(raw)
        if p.is_dir():
            found.extend(sorted(p.glob(f"*/{DATASET_GLOB}")))
            found.extend(sorted(p.glob(DATASET_GLOB)))
        else:
            found.append(p)
    seen: set[Path] = set()
    unique: list[Path] = []
    for p in found:
        if p not in seen:
            seen.add(p)
            unique.append(p)
    return unique


def plan(inputs: list[str], out_dir: str | None) -> list[Job]:
    """One Job per input. Raises if an output would land on its own input.

    `out_dir` is None under --dry-run, and then no destination is computed at all
    -- there is nothing to collide with, and nothing to accidentally write.
    """
    out = Path(out_dir) if out_dir is not None else None
    jobs = []
    for src in expand_inputs(inputs):
        if out is None:
            jobs.append(Job(src=src, dst=None))
            continue
        dst = out / src.name
        if _same_path(src, dst):
            raise ValueError(
                f"refusing to write {dst} over its own input. --out must be a "
                f"different directory")
        jobs.append(Job(src=src, dst=dst))
    return jobs


def _same_path(a: Path, b: Path) -> bool:
    """True when two paths name the same location, existing or not."""
    try:
        if a.exists() and b.exists():
            return a.samefile(b)
    except OSError:                                    # pragma: no cover
        pass
    return os.path.normpath(os.path.abspath(a)) == os.path.normpath(os.path.abspath(b))


# --------------------------------------------------------------------------- #
# Outcomes
# --------------------------------------------------------------------------- #

@dataclass
class Outcome:
    src: Path
    status: str                      # REPAIRED | CLEAN | SKIP | WOULD-REPAIR
    detail: str = ""
    before: dict = field(default_factory=dict)
    after: dict = field(default_factory=dict)
    changes: dict = field(default_factory=dict)

    def __str__(self) -> str:
        return f"{self.status:<13} {self.src}  {self.detail}".rstrip()


def schema_drift(path: str) -> list:
    """The 70-column contract, checked through validate_schema. [] means OK."""
    return validate_schema.compare_schema(validate_schema.read_schema(path))


# --------------------------------------------------------------------------- #
# duckdb-backed stages
# --------------------------------------------------------------------------- #

def _row(con, sql: str) -> dict:
    cur = con.sql(sql)
    names = cur.columns
    return dict(zip(names, cur.fetchone()))


def repair_file(con, job: Job, dry_run: bool = False) -> Outcome:
    """Repair one file, verify the result, then publish it under its final name.

    The write goes to `<dst>.partial` and is renamed only once every check has
    passed, so a failed verification cannot leave a file that looks repaired.
    """
    src = str(job.src)

    drifts = schema_drift(src)
    if drifts:
        return Outcome(job.src, "SKIP",
                       f"not the 70-column contract ({len(drifts)} drift"
                       f"{'s' if len(drifts) > 1 else ''}: "
                       f"{'; '.join(str(d) for d in drifts[:3])})")

    corrupt, total = con.sql(detect_sql(src)).fetchone()
    if corrupt == 0:
        # Idempotent no-op. Nothing is written: the file is already correct, so a
        # copy would only spend disk. This is also what makes a second run over
        # this script's own output a no-op.
        return Outcome(job.src, "CLEAN", f"{total} rows, 0 corrupt")

    changes = _row(con, changes_sql(src))
    before = _row(con, stats_sql(f"SELECT * FROM read_parquet({sql_string(src)})"))
    if dry_run:
        return Outcome(job.src, "WOULD-REPAIR",
                       f"{corrupt} of {total} rows", before=before,
                       changes=changes)

    assert job.dst is not None, "a non-dry run must have a destination"
    predicted = _row(con, stats_sql(repair_sql(src)))
    job.dst.parent.mkdir(parents=True, exist_ok=True)
    partial = job.dst.with_name(job.dst.name + ".partial")
    con.execute(copy_sql(src, str(partial)))
    try:
        after = _row(con, stats_sql(
            f"SELECT * FROM read_parquet({sql_string(str(partial))})"))
        verify(job, total, changes, predicted, after, str(partial))
    except BaseException:
        partial.unlink(missing_ok=True)
        raise
    os.replace(partial, job.dst)
    return Outcome(job.src, "REPAIRED",
                   f"{corrupt} of {total} rows -> {job.dst}",
                   before=before, after=after, changes=changes)


def verify(job: Job, in_rows: int, changes: dict, predicted: dict,
           after: dict, written: str) -> None:
    """Every check. Raises VerificationError on the first failure."""
    def fail(msg: str) -> None:
        raise VerificationError(f"{job.src}: {msg}")

    drifts = schema_drift(written)
    if drifts:
        fail(f"output is not the 70-column contract: "
             f"{'; '.join(str(d) for d in drifts)}")

    if after["rows"] != in_rows:
        fail(f"row count changed: {in_rows} in, {after['rows']} out")

    if after["tab_in_token_type"] != 0:
        fail(f"{after['tab_in_token_type']} rows still carry a TAB in token_type")

    if after["corrupt"] != 0:
        fail(f"{after['corrupt']} rows still match the corruption detector")

    if after["structural_with_text"] != 0:
        fail(f"{after['structural_with_text']} rows have is_structural = 1 and a "
             f"non-empty source_text, which correct data never does")

    if changes["chg_value_on_content"] != 0:
        fail(f"token_value changed on {changes['chg_value_on_content']} content "
             f"rows; it may only change on structural marker rows")

    if changes["chg_text_on_content"] != 0:
        fail(f"source_text changed on {changes['chg_text_on_content']} content "
             f"rows; this script does not repair source_text")

    # The written file must equal the repair expression on every aggregate,
    # including the hash sums of all four rewritten columns. This is what proves
    # the COPY round-tripped the values rather than the plan merely being right.
    for key, want in predicted.items():
        if after[key] != want:
            fail(f"written file disagrees with the repair plan on {key}: "
                 f"expected {want}, found {after[key]}")


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #

def report(outcome: Outcome) -> None:
    stream = sys.stderr if outcome.status == "SKIP" else sys.stdout
    print(outcome, file=stream)
    b, a, c = outcome.before, outcome.after, outcome.changes
    if not b:
        return
    print(f"                 distinct token_type   {b['distinct_token_type']:>12,}"
          + (f" -> {a['distinct_token_type']:>12,}" if a else ""))
    print(f"                 tab in token_type     {b['tab_in_token_type']:>12,}"
          + (f" -> {a['tab_in_token_type']:>12,}" if a else ""))
    print(f"                 is_structural = 1     {int(b['structural'] or 0):>12,}"
          + (f" -> {int(a['structural'] or 0):>12,}" if a else ""))
    print(f"                 position agreement    "
          f"{c['position_agree_before']:>12,} of {c['position_checkable']:,}"
          + (f" -> {c['position_checkable']:>12,} of {c['position_checkable']:,}"
             if a else ""))
    if c["position_unrecoverable"]:
        print(f"                 position kept as-is   "
              f"{c['position_unrecoverable']:>12,} rows "
              f"(non-numeric embedded position"
              + (f", {c['decl_shaped']:,} of them N:-" if c["decl_shaped"] else "")
              + ")")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="backfill_rust_tokens.py",
        description="Repair the Rust-tokenizer damage in published Parquet files.")
    ap.add_argument("inputs", nargs="+", metavar="INPUT",
                    help="Parquet files, or directories holding them")
    ap.add_argument("--out", metavar="DIR",
                    help="write repaired files here (required unless --dry-run)")
    ap.add_argument("--dry-run", action="store_true",
                    help="measure and report; write nothing")
    args = ap.parse_args(argv)

    if not args.dry_run and not args.out:
        ap.error("--out is required unless --dry-run is given")

    try:
        jobs = plan(args.inputs, None if args.dry_run else args.out)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return EXIT_USAGE
    if not jobs:
        print("error: no Parquet files found in the given inputs", file=sys.stderr)
        return EXIT_USAGE

    import duckdb                        # devenv-only; see the module docstring
    con = duckdb.connect()
    try:
        outcomes = [repair_file(con, job, dry_run=args.dry_run) for job in jobs]
    finally:
        con.close()

    for outcome in outcomes:
        report(outcome)

    counts: dict[str, int] = {}
    for o in outcomes:
        counts[o.status] = counts.get(o.status, 0) + 1
    print("\n" + ", ".join(f"{n} {s}" for s, n in sorted(counts.items())))
    return EXIT_SKIPPED if counts.get("SKIP") else EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
