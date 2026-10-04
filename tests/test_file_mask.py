"""The universal mask. Each property guards a failure that would not announce
itself: srcML exits 0 on an unknown extension, and blobExec compares the mask
string character for character, so a reorder forces a full rebuild."""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from file_mask import TOKENIZABLE_EXTENSIONS, UNIVERSAL_MASK, build_mask

CORPUS = Path(__file__).resolve().parent.parent


def test_the_mask_selects_every_tokenizable_extension():
    for ext in TOKENIZABLE_EXTENSIONS:
        assert re.search(UNIVERSAL_MASK, f"file.{ext}"), ext
        assert re.search(UNIVERSAL_MASK, f"deep/path/to/file.{ext}"), ext


def test_the_mask_is_case_insensitive():
    """.C and .H are real files, and a missed one is copied through untokenized."""
    assert UNIVERSAL_MASK.startswith("(?i)")
    for ext in TOKENIZABLE_EXTENSIONS:
        assert re.search(UNIVERSAL_MASK, f"file.{ext.upper()}"), ext
    assert re.search(UNIVERSAL_MASK, "Widget.CPP")
    assert re.search(UNIVERSAL_MASK, "Widget.Hxx")


@pytest.mark.parametrize("name", [
    # No parser at all.
    "main.go", "README.md", "ci.yaml", "ci.yml",
    # srcML 1.1.0 ignores -l C++ for these and emits an empty token file, exit 0.
    "mod.ixx", "impl.inl", "mod.cppm", "mod.cxxm", "detail.ipp",
    # M4 is routed by the tokenizer but excluded from the mask: m4.py's lexer
    # mis-lexes real autotools quoting.
    "Makefile.am", "configure.ac",
    # Never tokenizable.
    "setup.py", "notes.txt", "App.cs", "lib.rb", "index.ts", "data.json",
])
def test_the_mask_rejects_what_the_tokenizer_cannot_parse(name):
    assert not re.search(UNIVERSAL_MASK, name), name


@pytest.mark.parametrize("name", [
    "x.cpp.orig", "x.cpp.bak", "x.h.in", "x.java.tmpl", "x.cppp", "x.c++x",
])
def test_the_mask_is_anchored_at_the_end(name):
    """`x.h.in` is autoconf input, not a header."""
    assert not re.search(UNIVERSAL_MASK, name), name


def test_the_extension_list_is_sorted_and_has_no_duplicates():
    """blobExec compares the mask string on every resume, so its bytes are a contract."""
    assert list(TOKENIZABLE_EXTENSIONS) == sorted(TOKENIZABLE_EXTENSIONS)
    assert len(set(TOKENIZABLE_EXTENSIONS)) == len(TOKENIZABLE_EXTENSIONS)


def test_no_extension_carries_a_dot_or_uppercase():
    """A leading dot would produce `\\.\\.c$`, and an uppercase entry would be
    unreachable: both perl gates lowercase the extension before the lookup."""
    for ext in TOKENIZABLE_EXTENSIONS:
        assert not ext.startswith(".")
        assert ext == ext.lower()


def test_the_regex_metacharacters_in_c_plus_plus_are_escaped():
    """`c++` unescaped is `c+` repeated, which matches `x.ccc`."""
    assert r"c\+\+" in UNIVERSAL_MASK
    assert not re.search(UNIVERSAL_MASK, "x.ccc")
    assert re.search(UNIVERSAL_MASK, "x.c++")


def test_an_empty_extension_list_is_refused():
    """A mask that selects nothing produces an empty repository and no error."""
    with pytest.raises(ValueError):
        build_mask(())


@pytest.mark.parametrize("name", ["main.c", "Widget.cpp", "App.java", "lib.rs"])
def test_the_mask_selects_c_cpp_java_and_rust_sources(name):
    assert re.search(UNIVERSAL_MASK, name), name


def test_every_manifest_row_carries_the_universal_mask():
    """The runner records the manifest's mask in the Parquet's file_mask column,
    so a stale row would misstate how those tokens were produced."""
    manifests = sorted(CORPUS.glob("manifest*.tsv"))
    assert manifests, "no manifests found; the glob or the layout changed"
    for path in manifests:
        for n, line in enumerate(path.read_text().splitlines(), start=1):
            if not line.strip() or line.startswith("#"):
                continue
            fields = line.split("\t")
            assert len(fields) == 5, f"{path.name}:{n} is not a 5-column row"
            assert fields[3] == UNIVERSAL_MASK, f"{path.name}:{n} carries {fields[3]!r}"
