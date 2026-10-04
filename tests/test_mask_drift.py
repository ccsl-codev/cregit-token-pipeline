"""The mask exists twice: file_mask.UNIVERSAL_MASK here, and CregitLanguages.pm in
the cregit checkout, which routes extensions to parsers and is the authority.
Nothing else keeps them in step. The perl side is read by running it."""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

import ctp
from file_mask import TOKENIZABLE_EXTENSIONS, UNIVERSAL_MASK

TOKENIZE_DIR = ctp.CREGIT / "tokenize"
FILE_MASK_PL = TOKENIZE_DIR / "fileMask.pl"
LANGUAGES_PM = TOKENIZE_DIR / "CregitLanguages.pm"


pytestmark = [
    pytest.mark.skipif(not LANGUAGES_PM.exists(), reason=(
        f"{LANGUAGES_PM} is not checked out on this machine; "
        "the drift check needs both repositories")),
    pytest.mark.skipif(shutil.which("perl") is None, reason="no perl on PATH"),
]


def _perl(expression: str) -> str:
    """Run one perl expression with CregitLanguages loaded, return its stdout."""
    out = subprocess.run(
        ["perl", f"-I{TOKENIZE_DIR}", "-MCregitLanguages", "-e", expression],
        capture_output=True, text=True, check=True)
    return out.stdout


def cregit_mask() -> str:
    return subprocess.run(["perl", str(FILE_MASK_PL)],
                          capture_output=True, text=True, check=True).stdout.strip()


def cregit_masked_extensions() -> list[str]:
    return _perl('print join("\\n", CregitLanguages::masked_extensions_sorted())'
                 ).split()


def cregit_routed_extensions() -> dict[str, str]:
    raw = _perl('my %e = %CregitLanguages::EXT_LANG;'
                'print "$_\\t$e{$_}\\n" for sort keys %e')
    return dict(line.split("\t") for line in raw.splitlines() if line)


def cregit_parsers() -> dict[str, str]:
    raw = _perl(f'my %p = CregitLanguages::parsers("{TOKENIZE_DIR}");'
                'print "$_\\t$p{$_}\\n" for sort keys %p')
    return dict(line.split("\t") for line in raw.splitlines() if line)


def test_the_two_repositories_agree_on_the_mask_byte_for_byte():
    """blobExec compares the recorded mask as a string: a different spelling of
    the same regex forces every project to rebuild."""
    assert cregit_mask() == UNIVERSAL_MASK


def test_the_two_repositories_agree_on_the_extension_list():
    """As lists, so an ordering difference shows as a readable diff."""
    assert cregit_masked_extensions() == sorted(TOKENIZABLE_EXTENSIONS)


def test_every_extension_this_repo_masks_is_routed_to_a_parser_by_the_tokenizer():
    """A masked extension with no parser dies part-way through the run."""
    routed = cregit_routed_extensions()
    parsers = cregit_parsers()
    for ext in TOKENIZABLE_EXTENSIONS:
        assert ext in routed, f".{ext} is in the mask but the tokenizer does not route it"
        assert routed[ext] in parsers, (
            f".{ext} routes to language {routed[ext]!r}, which has no parser")


def test_every_parser_the_mask_can_reach_exists_and_is_executable():
    """A parser that is missing or mode 644 fails per blob, deep inside step 2,
    after step 1 has already cloned and walked."""
    routed = cregit_routed_extensions()
    parsers = cregit_parsers()
    reachable = {routed[ext] for ext in TOKENIZABLE_EXTENSIONS}
    assert reachable, "the mask reaches no language at all"
    for lang in sorted(reachable):
        p = Path(parsers[lang])
        assert p.exists(), f"parser for {lang} is missing: {p}"
        assert p.stat().st_mode & 0o111, f"parser for {lang} is not executable: {p}"


def test_the_tokenizer_no_longer_claims_languages_it_cannot_parse():
    """Every routed extension must reach a parser, or a future widening here can
    select an extension that was never going to work."""
    routed = cregit_routed_extensions()
    parsers = cregit_parsers()
    for ext, lang in sorted(routed.items()):
        assert lang in parsers, f".{ext} routes to {lang!r}, which has no parser"
    for dead in ("go", "md", "yaml"):
        assert dead not in routed, (
            f"{dead} is back in the tokenizer's extension table; there is still no "
            f"parser for it, so a .{dead} blob would die at the second gate")


def test_m4_is_routed_but_not_masked():
    """m4.py's lexer ends a quote on a backtick, so selecting .am/.ac would fail
    whole projects at step 2. Delete this when the lexer is fixed."""
    routed = cregit_routed_extensions()
    assert routed.get("am") == "M4" and routed.get("ac") == "M4"
    assert "am" not in TOKENIZABLE_EXTENSIONS
    assert "ac" not in TOKENIZABLE_EXTENSIONS
    assert "M4" not in {routed[e] for e in TOKENIZABLE_EXTENSIONS}


def test_the_configured_cregit_checkout_is_the_one_the_pipeline_runs():
    """If pipeline.cfg pointed somewhere else, everything above would be checking
    a tokenizer no run uses."""
    assert (ctp.CREGIT / "run_pipeline_process.sh").exists(), (
        f"{ctp.CREGIT} does not look like a cregit checkout; pipeline.cfg is stale")


def test_the_runner_takes_its_default_mask_from_the_same_table():
    """A project run without --mask must get the mask the manifest records."""
    runner = (ctp.CREGIT / "run_pipeline_process.sh").read_text()
    assert "tokenize/fileMask.pl" in runner
    assert r"MASK='\.[ch]$'" not in runner
