"""Tests for build_programming_table, on a small synthetic languages.yml and a stub tokei."""

from __future__ import annotations

import sys

import pytest

import build_programming_table as bpt

YML = """\
# Defines all languages known to GitHub.
---
C:
  type: programming
  extensions:
  - ".c"
JSON:
  type: data
  aliases:
  - geojson
Shell:
  type: programming
"Makefile":
  type: programming
Markdown:
  type: prose
Lex:
  type: programming
"""

LISTING = """\
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
┃ Language                              Extensions                     ┃
┃━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┃
┃ C                                     c, ec, pgc                     ┃
┃ C Header                              h                              ┃
┃ JSON                                  json                           ┃
┃ shell                                 sh                             ┃
┃ Alex                                  x                              ┃
┃ Gherkin (Cucumber)                    feature                        ┃
┃ Brand New                             bn                             ┃
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""


@pytest.fixture
def stub_tokei(tmp_path):
    path = tmp_path / "tokei"
    path.write_text(f"#!{sys.executable}\nimport sys\nsys.stdout.write({LISTING!r})\n")
    path.chmod(0o755)
    return str(path)


def test_linguist_reads_the_type_of_each_top_level_language():
    assert bpt.linguist(YML) == {"C": "programming", "JSON": "data", "Shell": "programming",
                                 "Makefile": "programming", "Markdown": "prose",
                                 "Lex": "programming"}


def test_tokei_languages_reads_the_name_column(stub_tokei):
    assert bpt.tokei_languages(stub_tokei) == ["C", "C Header", "JSON", "shell", "Alex",
                                                "Gherkin (Cucumber)", "Brand New"]


def test_build_decides_by_name_then_by_hand():
    rows, unmapped = bpt.build(["C", "JSON", "shell", "Alex", "Brand New"],
                               bpt.linguist(YML))
    assert rows == [
        ("C", "yes", "name", "C (programming)"),
        ("JSON", "no", "name", "JSON (data)"),
        ("shell", "yes", "name", "Shell (programming)"),
        ("Alex", "yes", "hand", "Lex (programming)"),
        ("Brand New", "no", "unmapped", ""),
    ]
    assert unmapped == ["Brand New"]


def test_a_hand_entry_without_a_linguist_language_keeps_its_reason():
    rows, _ = bpt.build(["Gherkin (Cucumber)", "Arturo"], {})
    assert rows == [("Gherkin (Cucumber)", "yes", "hand", "Gherkin"),
                    ("Arturo", "yes", "hand", "none: general-purpose language")]


def test_main_writes_the_table_and_fails_on_an_unmapped_language(tmp_path, stub_tokei, capsys):
    yml = tmp_path / "languages.yml"
    yml.write_text(YML)
    out = tmp_path / "t.tsv"
    assert bpt.main(["--languages-yml", str(yml), "--tokei", stub_tokei, "--out", str(out)]) == 1
    lines = out.read_text().splitlines()
    assert lines[0] == "tokei_language\tprogramming\tdecided_by\tlinguist"
    assert lines[2] == "C Header\tyes\thand\tC (programming)"
    assert lines[-1] == "Brand New\tno\tunmapped\t"
    assert "unmapped, add a HAND entry: Brand New" in capsys.readouterr().err


def test_main_succeeds_when_every_language_is_decided(tmp_path, capsys):
    tokei = tmp_path / "tokei"
    listing = LISTING.replace("┃ Brand New                             bn                             ┃\n", "")
    tokei.write_text(f"#!{sys.executable}\nimport sys\nsys.stdout.write({listing!r})\n")
    tokei.chmod(0o755)
    yml = tmp_path / "languages.yml"
    yml.write_text(YML)
    out = tmp_path / "t.tsv"
    assert bpt.main(["--languages-yml", str(yml), "--tokei", str(tokei), "--out", str(out)]) == 0
    err = capsys.readouterr().err
    assert "6 tokei languages" in err
    assert "HAND entries that are not tokei names" in err
