"""The per-project metadata field list exists in three places. Keep them equal.

  1. project_meta.META_FIELDS            — writes project_meta.json (this repo)
  2. generate_dataset.PROJECT_META_FIELDS — lays out the Parquet columns
                                            (cregit-issue61, a different repo)
  3. validate_schema.EXPECTED_COLUMNS    — gates the corpus (this repo)

Nothing else keeps them in step, and the two repositories are versioned
separately. A divergence does not crash: it produces a Parquet whose columns are
named for one list and filled from another, which is the worst kind of defect
because every later reader inherits it silently.

generate_dataset.py is read with `ast`, not imported: it needs duckdb, it lives
in the other checkout, and its module body is not the thing under test here. The
CREGIT-side test is a mirror of this one asserting length and endpoints, so a
machine with only this repository checked out still gets a signal.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

import ctp                                  # only for the configured CREGIT path
import project_meta
import validate_schema as vs

GENERATE_DATASET = ctp.CREGIT / "generate_dataset" / "generate_dataset.py"

# repo_name comes first and file_path follows the metadata block, so the
# metadata columns are the span between them.
FIRST_AFTER_META = "file_path"


def cregit_field_list() -> tuple[str, ...]:
    """PROJECT_META_FIELDS as the other repository's source actually spells it."""
    tree = ast.parse(GENERATE_DATASET.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == "PROJECT_META_FIELDS"
                for t in node.targets):
            return tuple(ast.literal_eval(node.value))
    pytest.fail(f"no PROJECT_META_FIELDS assignment in {GENERATE_DATASET}")


def contract_meta_columns() -> list[tuple[str, str]]:
    names = [n for n, _ in vs.EXPECTED_COLUMNS]
    return list(vs.EXPECTED_COLUMNS)[1:names.index(FIRST_AFTER_META)]


def test_the_generator_and_the_sidecar_agree_on_every_field_and_its_position():
    """Same names, same order. Order matters: the columns are positional in the
    SELECT, and the sidecar JSON is written sort_keys=True, so the generator must
    iterate its own tuple rather than the JSON."""
    if not GENERATE_DATASET.exists():
        pytest.skip(f"{GENERATE_DATASET} is not checked out on this machine; "
                    "the drift check needs both repositories")
    assert cregit_field_list() == tuple(project_meta.META_FIELDS)


def test_the_contract_carries_the_same_metadata_columns_in_the_same_order():
    """validate_schema.py is the third copy. It gates every project, so a
    disagreement here fails the whole corpus rather than one file."""
    assert [n for n, _ in contract_meta_columns()] == list(project_meta.META_FIELDS)


def test_every_metadata_column_is_a_varchar():
    """They are injected as SQL string literals, so nothing else is possible —
    and a numeric-looking one (commits, stars, size_kb) must not be typed as a
    number in one project's file and a string in another's."""
    assert {t for _, t in contract_meta_columns()} == {"VARCHAR"}


def test_the_metadata_block_sits_between_repo_name_and_file_path():
    names = [n for n, _ in vs.EXPECTED_COLUMNS]
    assert names[0] == "repo_name"
    assert names[1:1 + len(project_meta.META_FIELDS)] == list(project_meta.META_FIELDS)
    assert names[1 + len(project_meta.META_FIELDS)] == FIRST_AFTER_META


def test_the_configured_cregit_checkout_is_the_one_the_pipeline_runs():
    """If pipeline.cfg pointed somewhere else, the drift test above would be
    checking a file no run uses. Cheap assertion, and it names the failure."""
    assert isinstance(ctp.CREGIT, Path)
    assert (ctp.CREGIT / "run_pipeline_process.sh").exists(), (
        f"{ctp.CREGIT} does not look like a cregit checkout; pipeline.cfg is stale")
