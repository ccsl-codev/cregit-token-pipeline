"""The universal file mask: the one regex every project is tokenized with.

Defined once, here, and derived from TOKENIZABLE_EXTENSIONS rather than typed
out — a mask and an extension list maintained separately drift, and both
directions are silent. An extension the mask names but the tokenizer cannot
parse either kills the run part-way through (no parser: "Unknown parser for
extension") or, worse, produces an empty token file and exits 0 (srcML does this
for an extension it does not recognise). An extension the tokenizer knows but no
mask names is source left untokenized with nothing in the output to show it.

The mask used to be per-language, from GitHub's primary-language field:

    C     \\.[ch]$
    C++   \\.(c|cc|cp|cpp|cxx|h|hh|hpp)$
    Java  \\.java$
    Rust  \\.rs$

Measured over 188 bare clones, that selected 545,957 files where a universal mask
selects 570,201 — 63 projects gain files, 125 gain nothing, and none lose a file.
`data/mask-impact.csv` is the authority on this split; it carries one row per
project, so the counts above are derived from it rather than maintained by hand. The dataset is a tokenized set of projects, not a set of language
exemplars, so a polyglot project's C++ must not be dropped because GitHub calls
it a Java project. A union mask selects nothing a project does not have, so there
is nothing left for a per-project mask to do.

THE OTHER COPY OF THIS LIST lives in the tokenizer's own repository, at
cregit-issue61/tokenize/CregitLanguages.pm, which is the authority: it is what
routes an extension to a parser, and this list only has to agree with it.
tests/test_mask_drift.py compares the two by running `perl tokenize/fileMask.pl`
in the configured checkout, the same way tests/test_meta_field_drift.py compares
the metadata field lists. Two repositories cannot share one file; they can be
held equal by a test.

Not here, deliberately:

  .am .ac   routed to m4Tokenizer/m4.py, which is not fit for real autotools
            input: its lexer's end_quote is a backtick rather than an apostrophe,
            so `x' swallows text to the next backtick, and 2 of 38 real
            configure.ac/Makefile.am files on this machine die outright. (It was
            also Python 2 and did not run at all until 2026-09-19.) 1.4 MB in 22
            projects, deferred until the lexer is fixed.
  .go .md   cregit-issue61's tokenBySha.pl used to map these to Go/Markdown/Yaml
  .yaml     while tokenize.pl had no parser for any of them, so such a blob
            passed the first gate and died at the second. The entries are gone.
  .ixx .inl srcML 1.1.0 does not know these extensions and then ignores
  .cppm     `-l C++`: it emits an XML declaration with no <unit> and exits 0, so
  .cxxm     the chain writes an EMPTY token file and reports success. Probed one
  .ipp      by one on 2026-09-19; .hxx and .tcc passed and are in, these five
            failed and are out.
"""
from __future__ import annotations

import re

# Lowercase, no leading dot. Sorted, because the mask string is recorded in
# blobExec's meta table and compared character for character on every resume: a
# reordering would read as a mask change and force a rebuild.
TOKENIZABLE_EXTENSIONS: tuple[str, ...] = (
    "c",        # C
    "c++",      # C++ from here
    "cc",
    "cp",
    "cpp",
    "cxx",
    "h",        # C
    "h++",      # C++
    "hh",
    "hpp",
    "hxx",      # verified against srcML 1.1.0, 2026-09-19
    "java",
    "rs",       # Rust
    "tcc",      # verified against srcML 1.1.0, 2026-09-19
)

# The languages those extensions belong to. Provenance for a reader of this file;
# the mask does not branch on language any more.
TOKENIZABLE_LANGUAGES: tuple[str, ...] = ("C", "C++", "Java", "Rust")


def build_mask(extensions: tuple[str, ...] | list[str]) -> str:
    """One case-insensitive, end-anchored alternation over `extensions`.

    Case-insensitive because `.C` and `.H` are real files that every per-language
    mask missed. End-anchored only, never start-anchored: each consumer matches
    against a repository-relative path, and all of them search rather than
    full-match (Scala's Regex.findFirstIn in blobExec, Perl's m// in
    blameRepoFiles.pl and prettyPrintFiles.pl).
    """
    if not extensions:
        raise ValueError("an empty extension list would select nothing")
    return "(?i)\\.(" + "|".join(re.escape(e) for e in sorted(extensions)) + ")$"


UNIVERSAL_MASK: str = build_mask(TOKENIZABLE_EXTENSIONS)
