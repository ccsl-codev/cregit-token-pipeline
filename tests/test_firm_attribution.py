"""Firm attribution: the corrections overlay, the canonical-name table, and the
two committed data files themselves.

Three things are under test here and they fail for different reasons.

1. `build_domain_map.load_corrections` — the overlay mechanism. It must win over
   every source including `curated`, because `curated` lives in a third checkout
   this repository does not version, so a fix made there is invisible here and a
   fix made by hand in `data/affiliation.merged.csv` is erased by the next build.

2. `data/affiliation.corrections.csv` and `data/affiliation.merged.csv` — the
   artifacts. The named regression is `qti.qualcomm.com`: it read `CERN` from a
   single-person inference, which attributed all 180,971 tokens of
   qualcomm__qcom-embedded-power-measurement to CERN.

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

    It read `CERN,company,cncf-gitdm-single` at data/affiliation.merged.csv:2866
    until 2026-09-20. A naive join on that row attributed every one of the
    180,971 tokens of qualcomm__qcom-embedded-power-measurement to CERN, which is
    the single most visible wrong number this dataset could publish.
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
    assert {r["domain"] for r in rows(CORRECTIONS)} == {"qti.qualcomm.com"}
    row = rows(CORRECTIONS)[0]
    assert row["company"] == "Qualcomm"
    assert "CERN" in row["reason"], "the row must say what it corrects"


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

    assert bdm.cmd_build(argparse.Namespace(min_persons=2, report=False)) == 0
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
    keeps = {r["firm_raw"] for r in canonical_rows() if r["decision"] == "keep"}
    assert {"Independent", "AWS", "Azure", "Samsung SDS", "Yahoo! Japan",
            "Hewlett", "China Mobile International"} <= keeps
    for r in canonical_rows():
        if r["decision"] == "keep":
            assert "REJECTED" in r["note"], r


def test_the_two_splits_the_measurement_named_are_both_merged():
    """IBM is the largest split in the map (44 rows spell it out in full against
    6 that say IBM) and Salesforce the one no suffix rule can find, because `.com`
    is part of the string. Both are hand additions; if either is dropped the
    table's headline claim is wrong."""
    canon = {r["firm_raw"]: r["firm"] for r in canonical_rows()}
    assert canon["International Business Machines"] == "IBM"
    assert canon["Salesforce.com"] == "Salesforce"
    assert canon["SalesForce"] == "Salesforce"


def test_case_only_spellings_resolve_to_one_name():
    """The gap that let these through: norm_company does not case-fold, and the
    all-caps spellings come from the Spinellis SEC/Fortune source, whose filing
    names are upper case. An automatic rule would have canonicalised to the
    SHOUTING form, because that is the higher-confidence source."""
    canon = {r["firm_raw"]: r["firm"] for r in canonical_rows()}
    for shouted, proper in [("NETFLIX", "Netflix"), ("TWITTER", "Twitter"),
                            ("YANDEX", "Yandex"), ("ADOBE", "Adobe"),
                            ("YELP", "Yelp"), ("RAPID7", "Rapid7"),
                            ("NIKE", "Nike"), ("NEW RELIC", "New Relic"),
                            ("F5 NETWORKS", "F5 Networks")]:
        assert canon[shouted] == proper


def test_the_table_collapses_forty_eight_firms_out_of_a_hundred_strings():
    """The measurement this task was given: 44 groups over 89 strings under the
    controller's conservative key. Reviewing by hand found more, not fewer — 48
    groups over 100 strings — and rejected 11 candidate merges outright.
    """
    merges = [r for r in canonical_rows() if r["decision"] == "merge"]
    targets = {r["firm"] for r in merges}
    assert len(targets) == 48
    assert len(targets | {r["firm_raw"] for r in merges}) == 100
    assert sum(1 for r in canonical_rows() if r["decision"] == "keep") == 11


# --------------------------------------------------------------------------- #
# ctp.py's preflight. Same rule as --project-meta: refuse before the run, not
# 185 times during it.
# --------------------------------------------------------------------------- #

def run_args(**over):
    base = dict(manifest="manifest.tsv", only=None, jobs=1, retries=0,
                skip_html=False, drop_memo=False, memo_dir="",
                shards=0, shard_classes="L", from_step=1, gc=None,
                blame_jobs=0, memory_limit=None, duckdb_threads=0,
                project_meta="", mask="", firm_map="", firm_canonical="")
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
    monkeypatch.setattr(ctp, "run_project", lambda p: "done")
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
    to this repository. Task 7 lost a whole corpus pass to exactly this."""
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


def test_a_run_without_a_firm_map_says_so(sandbox, capsys):
    """Blank firm columns are the expensive silent outcome of this whole task, so
    the run that would produce them has to announce it."""
    assert ctp.cmd_run(run_args()) == 0
    assert "no --firm-map" in capsys.readouterr().out


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
