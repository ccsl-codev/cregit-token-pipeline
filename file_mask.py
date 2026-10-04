"""The universal file mask: one regex every project is tokenized with.

The authority is cregit's tokenize/CregitLanguages.pm; tests/test_mask_drift.py holds
the two equal. Left out: .am .ac (broken m4 lexer), .go .md .yaml (no parser), and
.ixx .inl .cppm .cxxm .ipp (srcML 1.1.0 writes an empty token file and exits 0).
"""
from __future__ import annotations

import re

# Lowercase, no leading dot, sorted: blobExec records the mask string and compares it
# character for character on resume, so a reordering would force a rebuild.
TOKENIZABLE_EXTENSIONS: tuple[str, ...] = (
    "c",
    "c++",
    "cc",
    "cp",
    "cpp",
    "cxx",
    "h",
    "h++",
    "hh",
    "hpp",
    "hxx",      # verified against srcML 1.1.0
    "java",
    "rs",
    "tcc",      # verified against srcML 1.1.0
)

# For a reader; the mask does not branch on language.
TOKENIZABLE_LANGUAGES: tuple[str, ...] = ("C", "C++", "Java", "Rust")


def build_mask(extensions: tuple[str, ...] | list[str]) -> str:
    """Case-insensitive, because `.C` and `.H` are real files. End-anchored only: every
    consumer searches rather than full-matches (blobExec's findFirstIn, Perl's m//)."""
    if not extensions:
        raise ValueError("an empty extension list would select nothing")
    return "(?i)\\.(" + "|".join(re.escape(e) for e in sorted(extensions)) + ")$"


UNIVERSAL_MASK: str = build_mask(TOKENIZABLE_EXTENSIONS)
