"""The universal mask, and the properties a wrong one would violate quietly.

Every assertion here is about a failure that would not announce itself:

  * an extension in the mask that the tokenizer cannot parse — srcML exits 0 and
    emits nothing for an extension it does not know, so the blob is tokenized to
    an empty file and the run reports success;
  * an extension the tokenizer knows that the mask forgets — that source is
    copied through untokenized and nothing in the output says so;
  * a start-anchored or unanchored mask — it would match `x.cpp.orig` or a path
    component, and the corpus would gain files nobody chose;
  * a reordered alternation — the mask string is recorded in blobExec's meta
    table and compared character for character, so a reorder reads as a mask
    change and forces a full rebuild of every project.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

import file_mask
from file_mask import TOKENIZABLE_EXTENSIONS, UNIVERSAL_MASK, build_mask

CORPUS = Path(__file__).resolve().parent.parent


def test_the_mask_is_built_from_the_extension_list_not_typed_out():
    """The one property that keeps the two from drifting."""
    assert UNIVERSAL_MASK == build_mask(TOKENIZABLE_EXTENSIONS)


def test_the_mask_selects_every_tokenizable_extension():
    for ext in TOKENIZABLE_EXTENSIONS:
        assert re.search(UNIVERSAL_MASK, f"file.{ext}"), ext
        assert re.search(UNIVERSAL_MASK, f"deep/path/to/file.{ext}"), ext


def test_the_mask_is_case_insensitive():
    """.C and .H are real files. Every per-language mask this replaces missed
    them, so they were copied through untokenized."""
    assert UNIVERSAL_MASK.startswith("(?i)")
    for ext in TOKENIZABLE_EXTENSIONS:
        assert re.search(UNIVERSAL_MASK, f"file.{ext.upper()}"), ext
    assert re.search(UNIVERSAL_MASK, "Widget.CPP")
    assert re.search(UNIVERSAL_MASK, "Widget.Hxx")


@pytest.mark.parametrize("name", [
    # No parser at all: tokenBySha.pl used to map these three and tokenize.pl had
    # no parser for any, so the blob passed the first gate and died at the second.
    "main.go", "README.md", "ci.yaml", "ci.yml",
    # srcML 1.1.0 does not know these; it ignores -l C++ and emits no <unit>, so
    # the token file comes out EMPTY with exit 0. Probed one by one, 2026-09-19.
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
    """`x.h.in` is autoconf input, not a header. An unanchored mask would take it
    and the tokenizer would try to parse a template."""
    assert not re.search(UNIVERSAL_MASK, name), name


def test_the_mask_is_not_anchored_at_the_start():
    """Consumers match a repository-relative path, not a basename."""
    assert re.search(UNIVERSAL_MASK, "src/lib/deep/thing.cpp")


def test_the_extension_list_is_sorted_and_has_no_duplicates():
    """The mask string is compared character for character by blobExec on every
    resume, so its byte form is part of the contract."""
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


def test_the_mask_covers_c_cpp_java_and_rust():
    """The decision behind the widening, stated as an assertion."""
    for ext, name in (("c", "C"), ("cpp", "C++"), ("java", "Java"), ("rs", "Rust")):
        assert ext in TOKENIZABLE_EXTENSIONS, name
    assert file_mask.TOKENIZABLE_LANGUAGES == ("C", "C++", "Java", "Rust")


def test_every_manifest_row_carries_the_universal_mask():
    """The manifests are the pipeline's input, and the runner records the mask it
    used in the Parquet's file_mask column, so a stale row makes the recorded
    mask a lie about how those tokens were produced."""
    manifests = sorted(CORPUS.glob("manifest*.tsv"))
    assert manifests, "no manifests found; the glob or the layout changed"
    for path in manifests:
        for n, line in enumerate(path.read_text().splitlines(), start=1):
            if not line.strip() or line.startswith("#"):
                continue
            fields = line.split("\t")
            assert len(fields) == 5, f"{path.name}:{n} is not a 5-column row"
            assert fields[3] == UNIVERSAL_MASK, f"{path.name}:{n} carries {fields[3]!r}"
