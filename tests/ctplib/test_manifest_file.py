"""Tests for ctplib/manifest_file.py: reading the manifest."""

from __future__ import annotations

import pytest

from ctplib import (
    manifest_file,
)

from fakes import VALID_ROW, write_manifest

# --------------------------------------------------------------------------- #
# manifest parsing
# --------------------------------------------------------------------------- #


def test_manifest_parses_a_valid_row(tmp_path):
    projects = manifest_file.read_manifest(write_manifest(tmp_path, VALID_ROW), None)
    assert projects == [
        dict(
            name="jq",
            url="https://github.com/jqlang/jq.git",
            category="community",
            file_filter=r"\.[ch]$",
            size_class="S",
        )
    ]


def test_manifest_skips_comment_lines(tmp_path):
    path = write_manifest(tmp_path, "# name  url  category  filter  class", VALID_ROW)
    assert [p["name"] for p in manifest_file.read_manifest(path, None)] == ["jq"]


def test_manifest_skips_blank_and_whitespace_only_lines(tmp_path):
    path = write_manifest(tmp_path, "", "   ", VALID_ROW, "\t", "")
    assert len(manifest_file.read_manifest(path, None)) == 1


@pytest.mark.parametrize(
    "row, fields",
    [
        pytest.param("jq\thttps://x.git\tcommunity\t\\.[ch]$", 4, id="four-fields"),
        pytest.param("jq\thttps://x.git\tcommunity\t\\.[ch]$\tS\textra", 6, id="six-fields"),
    ],
)
def test_manifest_rejects_a_row_without_exactly_five_fields(tmp_path, row, fields):
    """A short or long row is a corrupt manifest that would mislabel projects."""
    with pytest.raises(ValueError):
        manifest_file.read_manifest(write_manifest(tmp_path, row), None)


def test_manifest_error_names_the_offending_line(tmp_path):
    path = write_manifest(tmp_path, VALID_ROW, "zstd\thttps://z.git\tenterprise\t\\.[ch]$")
    with pytest.raises(ValueError) as exc:
        manifest_file.read_manifest(path, None)
    assert "zstd" in str(exc.value)


def test_manifest_rejects_a_bad_size_class(tmp_path):
    row = "jq\thttps://x.git\tcommunity\t\\.[ch]$\tXL"
    with pytest.raises(ValueError):
        manifest_file.read_manifest(write_manifest(tmp_path, row), None)


def test_manifest_empty_returns_no_projects(tmp_path):
    assert manifest_file.read_manifest(write_manifest(tmp_path), None) == []


def test_manifest_only_filter_keeps_just_the_named_projects(tmp_path):
    path = write_manifest(
        tmp_path,
        VALID_ROW,
        "zstd\thttps://github.com/facebook/zstd.git\tenterprise\t\\.[ch]$\tS",
        "tmux\thttps://github.com/tmux/tmux.git\tcommunity\t\\.[ch]$\tS",
    )
    assert [p["name"] for p in manifest_file.read_manifest(path, {"zstd"})] == ["zstd"]
