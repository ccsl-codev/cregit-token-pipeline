"""The mask exists in two repositories. Keep them equal, and keep both honest.

  1. file_mask.UNIVERSAL_MASK                         — what ctp.py sends and what
                                                        every manifest records
                                                        (this repo)
  2. cregit-issue61/tokenize/CregitLanguages.pm       — what routes an extension
                                                        to a parser, and what
                                                        tokenize/fileMask.pl
                                                        prints (the other repo)

(2) is the authority: it decides whether a selected blob can actually be
tokenized. (1) only has to agree with it. They are versioned separately and
nothing else keeps them in step, so a drift test is the only thing that catches
a mismatch before a run does.

A divergence does not crash. The mask in this repo selects a file, the tokenizer
in the other repo has no parser for it, and the run dies part-way through with
"Unknown parser for extension" after step 1 has already done its work. Or worse:
srcML exits 0 and emits nothing for an extension it does not recognise, so the
blob is tokenized to an empty file and the run reports success.

That second failure mode is why the parser checks below exist at all. "The parser
file is present and executable" is not the same claim as "the parser can parse
this extension", and the M4 tokenizer is the proof: m4Tokenizer/m4.py was present
and executable while being Python 2 and unable to run at all.

The perl side is read by RUNNING it (`perl tokenize/fileMask.pl`) rather than by
parsing the .pm, because the mask is derived there too and a hand-parse would
only compare the inputs, not the result. Skipped when that checkout is absent.
"""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

import ctp                                  # only for the configured CREGIT path
from file_mask import TOKENIZABLE_EXTENSIONS, UNIVERSAL_MASK

TOKENIZE_DIR = ctp.CREGIT / "tokenize"
FILE_MASK_PL = TOKENIZE_DIR / "fileMask.pl"
LANGUAGES_PM = TOKENIZE_DIR / "CregitLanguages.pm"


def _need_cregit() -> None:
    if not LANGUAGES_PM.exists():
        pytest.skip(f"{LANGUAGES_PM} is not checked out on this machine; "
                    "the drift check needs both repositories")
    if shutil.which("perl") is None:
        pytest.skip("no perl on PATH")


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
    """blobExec compares the recorded mask as a string, so "equivalent" is not
    good enough: a different spelling of the same regex forces every project to
    rebuild."""
    _need_cregit()
    assert cregit_mask() == UNIVERSAL_MASK


def test_the_two_repositories_agree_on_the_extension_list():
    """Compared as lists, so an ordering difference fails here with a readable
    diff rather than as an opaque mask mismatch."""
    _need_cregit()
    assert cregit_masked_extensions() == sorted(TOKENIZABLE_EXTENSIONS)


def test_every_extension_this_repo_masks_is_routed_to_a_parser_by_the_tokenizer():
    """The go/md/yaml defect, as an assertion. tokenBySha.pl mapped `go`, `md` and
    `yaml`; tokenize.pl had no parser for any of them, so such a blob cleared the
    first gate and died at the second."""
    _need_cregit()
    routed = cregit_routed_extensions()
    parsers = cregit_parsers()
    for ext in TOKENIZABLE_EXTENSIONS:
        assert ext in routed, f".{ext} is in the mask but the tokenizer does not route it"
        assert routed[ext] in parsers, (
            f".{ext} routes to language {routed[ext]!r}, which has no parser")


def test_every_parser_the_mask_can_reach_exists_and_is_executable():
    """A parser that is missing or mode 644 fails per blob, deep inside step 2,
    after step 1 has already cloned and walked."""
    _need_cregit()
    routed = cregit_routed_extensions()
    parsers = cregit_parsers()
    reachable = {routed[ext] for ext in TOKENIZABLE_EXTENSIONS}
    assert reachable, "the mask reaches no language at all"
    for lang in sorted(reachable):
        p = Path(parsers[lang])
        assert p.exists(), f"parser for {lang} is missing: {p}"
        assert p.stat().st_mode & 0o111, f"parser for {lang} is not executable: {p}"


def test_the_tokenizer_no_longer_claims_languages_it_cannot_parse():
    """Every extension the tokenizer routes must reach a parser, not only the ones
    this repo masks. Otherwise a future widening here can select an extension that
    was never going to work."""
    _need_cregit()
    routed = cregit_routed_extensions()
    parsers = cregit_parsers()
    for ext, lang in sorted(routed.items()):
        assert lang in parsers, f".{ext} routes to {lang!r}, which has no parser"
    for dead in ("go", "md", "yaml"):
        assert dead not in routed, (
            f"{dead} is back in the tokenizer's extension table; there is still no "
            f"parser for it, so a .{dead} blob would die at the second gate")


def test_m4_is_routed_but_not_masked():
    """m4Tokenizer/m4.py is present and executable and still cannot be trusted
    with real autotools input: its lexer ends a quote on a backtick instead of an
    apostrophe. Selecting .am/.ac would fail whole projects at step 2. This
    assertion is the record of that decision — delete it when the lexer is
    fixed, not before."""
    _need_cregit()
    routed = cregit_routed_extensions()
    assert routed.get("am") == "M4" and routed.get("ac") == "M4"
    assert "am" not in TOKENIZABLE_EXTENSIONS
    assert "ac" not in TOKENIZABLE_EXTENSIONS
    assert "M4" not in {routed[e] for e in TOKENIZABLE_EXTENSIONS}


def test_the_configured_cregit_checkout_is_the_one_the_pipeline_runs():
    """If pipeline.cfg pointed somewhere else, everything above would be checking
    a tokenizer no run uses."""
    _need_cregit()
    assert (ctp.CREGIT / "run_pipeline_process.sh").exists(), (
        f"{ctp.CREGIT} does not look like a cregit checkout; pipeline.cfg is stale")


def test_the_runner_takes_its_default_mask_from_the_same_table():
    """Not typed beside the table: run_pipeline_process.sh resolves its default
    from tokenize/fileMask.pl, so a project run without --mask gets exactly the
    mask this repo records in the manifest."""
    _need_cregit()
    runner = (ctp.CREGIT / "run_pipeline_process.sh").read_text()
    assert "tokenize/fileMask.pl" in runner
    # And no literal per-language mask left behind as a default.
    assert r"MASK='\.[ch]$'" not in runner
