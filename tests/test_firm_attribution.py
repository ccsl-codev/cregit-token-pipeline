"""Firm attribution: the corrections overlay, the canonical-name table, and the
two committed data files themselves.

Three things are under test here and they fail for different reasons.

1. `build_domain_map.load_corrections` — the overlay mechanism. It must win over
   every source including `curated`, because `curated` lives in a third checkout
   this repository does not version, so a fix made there is invisible here and a
   fix made by hand in `data/affiliation.merged.csv` is erased by the next build.

2. `data/affiliation.corrections.csv` and `data/affiliation.merged.csv` — the
   artifacts. The named regression is `qti.qualcomm.com`: a single-person
   inference attributed the domain, and every token under it, to the wrong
   company.

3. `data/firm_canonical.csv` — the reviewed table that fills the `firm` column.
   It is data, not code, so what can be checked is its shape: unique keys, no
   chains, no dead rows, and rejections recorded rather than dropped. The
   judgement in each row is a human's and no test can confirm it; that is the
   price of not merging names automatically.

These read the LIVE data files on purpose. They are committed artifacts that a
corpus-wide regeneration reads, so a test against a fixture would prove nothing
about what gets published.
"""
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path

import pytest

import build_domain_map as bdm
import ctp

# The sandbox fixture below replaces ctp.run_project for the cmd_run tests, so
# the two argv tests keep a reference to the real one.
REAL_RUN_PROJECT = ctp.run_project

ROOT = Path(__file__).resolve().parent.parent
MERGED = ROOT / "data" / "affiliation.merged.csv"
CORRECTIONS = ROOT / "data" / "affiliation.corrections.csv"
CANONICAL = ROOT / "data" / "firm_canonical.csv"


def rows(path: Path) -> list[dict[str, str]]:
    with path.open() as fh:
        return list(csv.DictReader(fh))


# --------------------------------------------------------------------------- #
# the named regression. Defect 1.
# --------------------------------------------------------------------------- #

def test_qti_qualcomm_com_resolves_to_qualcomm_not_cern():
    """THE regression. `qti.qualcomm.com` is Qualcomm Technologies, Inc.

    A single-person gitdm inference once named it CERN instead. A naive join
    on that row would attribute every token under the domain to the wrong
    company, which is the kind of wrong number this dataset must not publish.
    """
    row = next(r for r in rows(MERGED) if r["domain"] == "qti.qualcomm.com")
    assert row["company"] == "Qualcomm"
    assert row["source"] == "correction"


def test_the_other_qualcomm_domains_were_already_right_and_still_are():
    """The fix must be the one bad row, not a rule that rewrote a family. These
    four were correct before the correction and are untouched by it."""
    by_domain = {r["domain"]: r for r in rows(MERGED)}
    for domain in ("oss.qualcomm.com", "qca.qualcomm.com", "quicinc.com",
                   "codeaurora.org"):
        assert by_domain[domain]["company"] == "Qualcomm"
        assert by_domain[domain]["source"] == "patch"


def test_the_correction_is_in_the_overlay_so_a_rebuild_keeps_it():
    """A hand edit of the merged file is erased by the next `build`. The point of
    the overlay is that the fix survives, so the fix has to live in it."""
    by_domain = {r["domain"]: r for r in rows(CORRECTIONS)}
    assert set(by_domain) == {"qti.qualcomm.com", "collibra.com"}
    assert by_domain["qti.qualcomm.com"]["company"] == "Qualcomm"
    assert "CERN" in by_domain["qti.qualcomm.com"]["reason"], \
        "the row must say what it corrects"


# --------------------------------------------------------------------------- #
# the second named regression, same class of error as the first.
# --------------------------------------------------------------------------- #

def test_collibra_com_resolves_to_collibra_not_medidata():
    """THE second regression, the same shape as the first. `collibra.com` is
    Collibra NV, the data-governance software company. It is not Medidata: a
    single-person inference had attached a contributor's employer to a domain
    that firm does not own. Medidata Solutions owns mdsol.com, which the map
    already carries.
    """
    row = {r["domain"]: r for r in rows(CORRECTIONS)}["collibra.com"]
    assert row["company"] == "Collibra"
    assert row["kind"] == "company"
    assert row["source"] == "correction"
    assert "Medidata" in row["reason"], "the row must say what it corrects"


def test_the_overlay_row_actually_overrides_the_bad_source_row(tmp_path,
                                                              monkeypatch):
    """The overlay row proved against the real merge code, not just read back.

    The source is fed the inference that produced the defect — one person on
    collibra.com saying `Medidata` — and the LIVE overlay file is the only other
    input. If precedence ever regressed, the build would emit Medidata again.
    """
    monkeypatch.setattr(bdm, "DATA", tmp_path)
    monkeypatch.setattr(bdm, "OUT", tmp_path / "merged.csv")
    monkeypatch.setattr(bdm, "CURATED", tmp_path / "curated.csv")
    monkeypatch.setattr(bdm, "CACHE", tmp_path / "cache")
    monkeypatch.setattr(bdm, "SPINELLIS_TSV", tmp_path / "absent.tsv")
    (tmp_path / "cache").mkdir()
    (tmp_path / "cache" / "developers_affiliations1.txt").write_text(
        "someone: someone!collibra.com\n\tMedidata\n")
    (tmp_path / "curated.csv").write_text("domain,company,kind,source\n")
    (tmp_path / bdm.CORRECTIONS_NAME).write_text(CORRECTIONS.read_text())

    assert bdm.cmd_build(argparse.Namespace(min_persons=2, report=False,
                                            curated=None, no_curated=False)) == 0
    out = {r["domain"]: r for r in rows(tmp_path / "merged.csv")}
    assert out["collibra.com"]["company"] == "Collibra"
    assert out["collibra.com"]["source"] == "correction"


def test_the_committed_artifact_agrees_with_every_overlay_row():
    """The overlay is only authoritative once the artifact is rebuilt from it.

    A row in the overlay that the merged map contradicts means the artifact is
    stale, which is the one failure mode the overlay cannot prevent by itself.
    """
    merged = {r["domain"]: r for r in rows(MERGED)}
    for r in rows(CORRECTIONS):
        assert merged[r["domain"]]["company"] == r["company"], r["domain"]


# --------------------------------------------------------------------------- #
# the overlay mechanism
# --------------------------------------------------------------------------- #

def test_no_corrections_file_is_not_an_error(tmp_path, monkeypatch):
    """The overlay is additive. A checkout without one builds the same map."""
    monkeypatch.setattr(bdm, "DATA", tmp_path)
    assert bdm.load_corrections() == {}


def test_a_correction_row_is_read_with_its_defaults(tmp_path, monkeypatch):
    monkeypatch.setattr(bdm, "DATA", tmp_path)
    (tmp_path / bdm.CORRECTIONS_NAME).write_text(
        "domain,company,kind,source,reason\n"
        "QTI.Example.COM,Example Inc,,,because\n")
    # The domain is lower-cased (the map's keys are), and the two empty columns
    # fall back rather than writing blanks into the artifact.
    assert bdm.load_corrections() == {
        "qti.example.com": ("Example Inc", "company", "correction")}


def test_a_row_with_no_domain_is_skipped(tmp_path, monkeypatch):
    """A blank line or a `#` note in a hand-edited overlay must not become a
    domain called '' that then matches nothing, or worse, matches a NULL."""
    monkeypatch.setattr(bdm, "DATA", tmp_path)
    (tmp_path / bdm.CORRECTIONS_NAME).write_text(
        "domain,company,kind,source,reason\n"
        ",Nobody,company,correction,blank domain\n"
        "# a note,Nobody,company,correction,commented out\n"
        "real.example,Somebody,company,correction,kept\n")
    assert list(bdm.load_corrections()) == ["real.example"]


def test_a_correction_beats_the_curated_map(tmp_path, monkeypatch, capsys):
    """Precedence is the whole mechanism. `curated` already wins over every
    import, so an overlay that did not outrank it could not fix a curated row."""
    monkeypatch.setattr(bdm, "DATA", tmp_path)
    monkeypatch.setattr(bdm, "OUT", tmp_path / "merged.csv")
    monkeypatch.setattr(bdm, "CURATED", tmp_path / "curated.csv")
    monkeypatch.setattr(bdm, "CACHE", tmp_path / "cache")
    monkeypatch.setattr(bdm, "SPINELLIS_TSV", tmp_path / "absent.tsv")
    (tmp_path / "cache").mkdir()
    (tmp_path / "cache" / "developers_affiliations1.txt").write_text(
        "dev: d!widget.example\n\tWidget Co\n")
    (tmp_path / "curated.csv").write_text(
        "domain,company,kind,source\nwidget.example,Wrong Name,company,patch\n")
    (tmp_path / bdm.CORRECTIONS_NAME).write_text(
        "domain,company,kind,source,reason\n"
        "widget.example,Right Name,company,correction,reviewed\n")

    assert bdm.cmd_build(argparse.Namespace(min_persons=2, report=False,
                                            curated=None, no_curated=False)) == 0
    out = {r["domain"]: r for r in rows(tmp_path / "merged.csv")}
    assert out["widget.example"]["company"] == "Right Name"
    assert out["widget.example"]["source"] == "correction"
    assert "corrections overlay: 1 domains" in capsys.readouterr().out


# --------------------------------------------------------------------------- #
# the reviewed canonical-name table. Defect 2.
# --------------------------------------------------------------------------- #

def canonical_rows():
    return rows(CANONICAL)


def test_every_row_declares_a_decision_and_a_reason():
    """A row with no note is an unreviewed row. The whole reason this is a data
    file rather than a rule is that a reviewer can read why."""
    for r in canonical_rows():
        assert r["decision"] in ("merge", "keep"), r
        assert len(r["note"].strip()) > 20, r


def test_a_merge_changes_the_name_and_a_keep_does_not():
    for r in canonical_rows():
        if r["decision"] == "merge":
            assert r["firm"] != r["firm_raw"], r
        else:
            assert r["firm"] == r["firm_raw"], r


def test_the_key_is_unique():
    """A repeated firm_raw would make the join ambiguous, and because it is a LEFT
    JOIN against token_map it would silently DUPLICATE token rows. The generator
    refuses such a file; this catches it a commit earlier."""
    keys = [r["firm_raw"] for r in canonical_rows()]
    assert len(keys) == len(set(keys))


def test_no_merge_target_is_itself_merged_away():
    """One hop only. A chain (A->B, B->C) would leave `firm` holding B for some
    rows and C for others, which is the split defect wearing a canonical name."""
    merged_away = {r["firm_raw"] for r in canonical_rows()
                   if r["decision"] == "merge"}
    targets = {r["firm"] for r in canonical_rows() if r["decision"] == "merge"}
    assert targets & merged_away == set()


def test_every_name_in_the_table_actually_appears_in_the_map():
    """A dead row is worse than no row: it reads as a reviewed decision about a
    firm this corpus does not contain, and it hides a typo in the name."""
    companies = {r["company"] for r in rows(MERGED)}
    for r in canonical_rows():
        assert r["firm_raw"] in companies, f"firm_raw not in the map: {r['firm_raw']}"
        assert r["firm"] in companies, f"canonical not in the map: {r['firm']}"


def test_the_rejections_are_recorded_rather_than_dropped():
    """A rejection is a result. `keep` rows are how a reviewer sees which merges
    were considered and refused, and can disagree with one line."""
    keeps = [r for r in canonical_rows() if r["decision"] == "keep"]
    assert keeps, "expected at least one considered-and-rejected merge"
    for r in keeps:
        assert "REJECTED" in r["note"], r


def test_some_canonical_firms_absorb_more_than_one_raw_spelling():
    """The table's whole purpose is collapsing split spellings into one name.
    If no canonical firm ever absorbed more than one raw spelling, the table
    would do nothing a plain rename could not."""
    merges = [r for r in canonical_rows() if r["decision"] == "merge"]
    by_target: dict[str, set[str]] = defaultdict(set)
    for r in merges:
        by_target[r["firm"]].add(r["firm_raw"])
    multi = {firm: raws for firm, raws in by_target.items() if len(raws) > 1}
    assert multi, "expected at least one canonical firm absorbing more than " \
                  "one raw spelling"


def test_case_only_spellings_resolve_to_one_canonical_name():
    """norm_company does not case-fold, so two case variants of one company
    name are two different strings in data/affiliation.merged.csv's `company`
    column — the all-caps spellings come from the Spinellis SEC/Fortune
    source, whose filing names are upper case. Resolving every company string
    through the canonical table (falling back to the string itself where the
    table is silent) must still collapse case variants to one name."""
    canon = {r["firm_raw"]: r["firm"] for r in canonical_rows()}
    companies = {r["company"] for r in rows(MERGED)}
    by_fold: dict[str, set[str]] = defaultdict(set)
    for co in companies:
        by_fold[co.casefold()].add(canon.get(co, co))
    for fold, resolved in by_fold.items():
        assert len(resolved) == 1, (fold, resolved)


def test_the_table_holds_both_merges_and_keeps():
    """The table must hold both outcomes to have done its job: at least one
    raw spelling actually merged into another name, and at least one
    candidate merge considered and rejected. A table with only one kind of
    row would mean the review for the other kind never ran."""
    decisions = {r["decision"] for r in canonical_rows()}
    assert decisions == {"merge", "keep"}


# --------------------------------------------------------------------------- #
# ctp.py's preflight. Same rule as --project-meta: refuse before the run, not
# 185 times during it.
# --------------------------------------------------------------------------- #

def run_args(**over):
    """allow_empty_provenance defaults to True here and only here, as it does in
    tests/test_ctp.py's run_args. On the real CLI it is False, and cmd_run then
    refuses any step-10 run that leaves --project-meta out. These tests are about
    the firm flags and never pass a sidecar (the stand-in runner in the fixture
    below does not even advertise --project-meta), so without the hatch every one
    of them would exit on a refusal about a different flag. The guard itself is
    tested in tests/test_ctp.py, including its real default."""
    base = dict(manifest="manifest.tsv", only=None, jobs=1, retries=0,
                skip_html=False, drop_memo=False, memo_dir="",
                shards=0, shard_classes="L", from_step=1, gc=None,
                blame_jobs=0, memory_limit=None, duckdb_threads=0,
                project_meta="", mask="", firm_map="", firm_canonical="",
                allow_empty_provenance=True, reblame=False,
                mask_widened=False, retokenize="")
    base.update(over)
    return argparse.Namespace(**base)


@pytest.fixture(autouse=True)
def sandbox(tmp_path, monkeypatch):
    """Keep cmd_run away from the live corpus. Mirrors tests/test_ctp.py."""
    out = tmp_path / "corpus-files"
    out.mkdir()
    cregit = tmp_path / "cregit"
    cregit.mkdir()
    (cregit / "run_pipeline_process.sh").write_text(
        "#!/bin/sh\n# --repo-url --repo-name --work --mask --firm-map "
        "--firm-canonical\n")
    (tmp_path / "manifest.tsv").write_text(
        "jq\thttps://github.com/jqlang/jq.git\tcommunity\t\\.[ch]$\tS\n")
    monkeypatch.setattr(ctp, "CORPUS", tmp_path)
    monkeypatch.setattr(ctp, "OUT", out)
    monkeypatch.setattr(ctp, "CREGIT", cregit)
    monkeypatch.setattr(ctp, "STATE", tmp_path / "state")
    monkeypatch.setattr(ctp, "METRICS", tmp_path / "metrics.tsv")
    monkeypatch.setattr(ctp, "RUNS_LOG", tmp_path / "runs.log")
    monkeypatch.setattr(ctp, "capture_devenv_env", lambda: {"PATH": "/x"})
    monkeypatch.setattr(ctp, "run_project", lambda p: ctp.RunOutcome.DONE)
    for shared in (ctp._OPTS, ctp._ENV, ctp._live):
        shared.clear()
    yield tmp_path
    for shared in (ctp._OPTS, ctp._ENV, ctp._live):
        shared.clear()


def test_a_firm_map_that_is_not_a_file_stops_the_run(sandbox):
    with pytest.raises(SystemExit, match="is not a file"):
        ctp.cmd_run(run_args(firm_map=str(sandbox / "absent.csv")))


def test_a_canonical_table_without_a_map_stops_the_run(sandbox):
    """There is no firm_raw to canonicalise without a map to read it from, and
    accepting the pair silently would write `firm` from nothing."""
    with pytest.raises(SystemExit, match="no firm_raw"):
        ctp.cmd_run(run_args(firm_canonical=str(CANONICAL)))


def test_a_canonical_table_that_is_not_a_file_stops_the_run(sandbox):
    (sandbox / "map.csv").write_text("domain,company,kind,source\n")
    with pytest.raises(SystemExit, match="firm-canonical"):
        ctp.cmd_run(run_args(firm_map=str(sandbox / "map.csv"),
                             firm_canonical=str(sandbox / "absent.csv")))


def test_a_runner_that_cannot_take_the_flag_stops_the_run(sandbox):
    """Otherwise the flag is dropped and 185 Parquets carry blank firm columns —
    a silent wrong answer, which is worse than a loud refusal."""
    (sandbox / "cregit" / "run_pipeline_process.sh").write_text(
        "#!/bin/sh\n# --repo-url --repo-name --work --mask\n")
    (sandbox / "map.csv").write_text("domain,company,kind,source\n")
    with pytest.raises(SystemExit, match="blank firm columns"):
        ctp.cmd_run(run_args(firm_map=str(sandbox / "map.csv")))


def test_both_paths_are_resolved_absolute_for_the_runner(sandbox, monkeypatch):
    """The runner is started with cwd=CREGIT while these paths are typed relative
    to this repository. A whole corpus pass was lost to exactly this."""
    monkeypatch.chdir(sandbox)
    (sandbox / "map.csv").write_text("domain,company,kind,source\n")
    (sandbox / "canon.csv").write_text("firm_raw,firm\n")
    assert ctp.cmd_run(run_args(firm_map="map.csv",
                                firm_canonical="canon.csv")) == 0
    assert ctp._OPTS["firm_map"] == str(sandbox / "map.csv")
    assert ctp._OPTS["firm_canonical"] == str(sandbox / "canon.csv")


def test_a_map_without_a_canonical_table_is_accepted(sandbox):
    """Not every caller has a reviewed table. Without one `firm` repeats
    `firm_raw`, which is the unnormalised truth rather than a guess."""
    (sandbox / "map.csv").write_text("domain,company,kind,source\n")
    assert ctp.cmd_run(run_args(firm_map=str(sandbox / "map.csv"))) == 0
    assert ctp._OPTS["firm_canonical"] == ""


def test_a_run_without_a_firm_map_is_refused_not_merely_noted(sandbox):
    """Blank firm columns are the expensive silent outcome of this whole task, so
    the run that would produce them has to be stopped.

    This test used to assert a `note: no --firm-map` line on stdout. A note is not
    enough in this position: it covered 3 of the 32 columns at stake (omitting
    --project-meta printed nothing at all), and it is line 1 of a log whose other
    105 lines are 30-second heartbeats. It is now a refusal with an explicit
    escape hatch, so the assertion changes from "it says so" to "it stops"."""
    with pytest.raises(SystemExit, match="--firm-map absent"):
        ctp.cmd_run(run_args(allow_empty_provenance=False))


def test_the_runner_argv_carries_both_flags(sandbox, monkeypatch):
    """run_project sends them only when _OPTS holds them, which is what keeps
    test_ctp.py's REQUIRED_RUNNER_FLAGS assertion true: a default run still sends
    exactly the four flags that constant lists."""
    sent: list[list[str]] = []
    monkeypatch.setattr(ctp, "run_phase",
                        lambda project, phase, args: sent.append(args) or 0)
    monkeypatch.setattr(ctp, "DISK_FLOOR_GB", 0)
    project = dict(name="jq", url="u", category="community",
                   file_filter=r"\.[ch]$", size_class="S")

    ctp._OPTS.update(firm_map="/m.csv", firm_canonical="/c.csv")
    REAL_RUN_PROJECT(project)
    assert sent[0][sent[0].index("--firm-map") + 1] == "/m.csv"
    assert sent[0][sent[0].index("--firm-canonical") + 1] == "/c.csv"

    # …and nothing is sent when the map is absent, so the argv of an ordinary run
    # is unchanged.
    sent.clear()
    ctp._OPTS.clear()
    (sandbox / "corpus-files" / "jq" / "jq.validated").unlink(missing_ok=True)
    REAL_RUN_PROJECT(project)
    assert "--firm-map" not in sent[0]

    # A map with no canonical table is a legitimate run: `firm` then repeats
    # `firm_raw` and the split spellings stay split, which is honest.
    sent.clear()
    ctp._OPTS.update(firm_map="/m.csv")
    (sandbox / "corpus-files" / "jq" / "jq.validated").unlink(missing_ok=True)
    REAL_RUN_PROJECT(project)
    assert "--firm-map" in sent[0] and "--firm-canonical" not in sent[0]


def test_a_canonical_table_alone_reaches_no_runner(sandbox, monkeypatch):
    """Belt and braces on the pairing: even if _OPTS were set by hand, the
    canonical path is only sent alongside the map it keys into."""
    sent: list[list[str]] = []
    monkeypatch.setattr(ctp, "run_phase",
                        lambda project, phase, args: sent.append(args) or 0)
    monkeypatch.setattr(ctp, "DISK_FLOOR_GB", 0)
    ctp._OPTS.update(firm_canonical="/c.csv")
    REAL_RUN_PROJECT(dict(name="jq", url="u", category="community",
                          file_filter=r"\.[ch]$", size_class="S"))
    assert "--firm-canonical" not in sent[0]
