"""Unit tests for validate_spinellis.py.

This script audits the corpus labels, so a bug in it is worse than a crash: it
reports "all clear" about a corpus it failed to join. Two rules carry the weight.

  1. norm() is the join key for both datasets. If it normalises one side
     differently from the other, every lookup misses, every project looks
     `absent`, and section 4 reports zero defects. A silent pass.
  2. strong_tier_eligible() must mirror select_corpus.parse_spinellis. Section 4
     claims a defect against *our own* rule, so if the mirror drifts the claim
     is unfounded in either direction.
"""

from __future__ import annotations

import pytest

import select_corpus as sc
import validate_spinellis as vs


# ---------------------------------------------------------------- helpers


def ent_row(**over) -> dict:
    """A Spinellis enterprise record that passes every strong-tier filter."""
    r = dict.fromkeys(sc.SPINELLIS_COLS, "")
    r.update(url="https://github.com/acme/widget", sec10k="t",
             company_name="ACME CORP", lines=str(sc.SP_MIN_LINES),
             commit_count=str(sc.SP_MIN_COMMITS),
             most_recent_commit=f"{sc.SP_MIN_YEAR}-01-01 00:00:00")
    r.update(over)
    return r


def cand(**over) -> dict:
    r = {"source": "ghsearch", "stratum": "community", "fact": "F1_residual:none",
         "owner": "acme", "repo": "widget", "included": "True",
         "clone_url": "https://github.com/acme/widget.git"}
    r.update(over)
    return r


# ---------------------------------------------------------------- norm


@pytest.mark.parametrize("raw", [
    "https://github.com/Acme/Widget",
    "http://github.com/Acme/Widget",
    "https://github.com/acme/widget.git",
    "git@github.com:Acme/Widget.git",
    "github.com/acme/widget/",
    "  https://github.com/ACME/WIDGET  ",
])
def test_norm_collapses_every_url_shape_to_one_key(raw):
    assert vs.norm(raw) == "acme/widget"


@pytest.mark.parametrize("raw", ["", "   ", None])
def test_norm_returns_none_for_no_url(raw):
    assert vs.norm(raw) is None


def test_norm_joins_the_two_sides_of_the_real_comparison():
    """The enterprise file has no .git; candidates.csv clone_url does."""
    assert vs.norm("https://github.com/acme/widget") == vs.norm(cand()["clone_url"])


def test_norm_keeps_a_dot_inside_the_repo_name():
    assert vs.norm("https://github.com/acme/widget.js") == "acme/widget.js"


# ---------------------------------------------------------------- tier


def test_tier_precedence_matches_parse_spinellis():
    assert vs.tier(ent_row(fg500="t", sec10k="t", sec20f="t")) == "fg500"
    assert vs.tier(ent_row(fg500="f", sec10k="t", sec20f="t")) == "sec10k"
    assert vs.tier(ent_row(fg500="f", sec10k="f", sec20f="t")) == "sec20f"


def test_tier_is_none_without_a_registry_flag():
    assert vs.tier(ent_row(sec10k="f")) is None


def test_flag_only_accepts_the_files_true_token():
    assert vs.flag("t") and vs.flag(" T ")
    assert not vs.flag("f")
    assert not vs.flag("true")
    assert not vs.flag("")


def test_num_treats_junk_as_zero_not_as_a_crash():
    assert vs.num("12") == 12
    assert vs.num("") == 0
    assert vs.num("not-a-number") == 0


# ------------------------------------------------- strong_tier_eligible


def test_strong_tier_eligible_accepts_a_fully_qualifying_row():
    assert vs.strong_tier_eligible(ent_row())


@pytest.mark.parametrize("over, why", [
    ({"sec10k": "f"}, "no registry flag"),
    ({"company_name": ""}, "no registered company"),
    ({"company_name": "   "}, "blank company"),
    ({"most_recent_commit": "2017-12-31 00:00:00"}, "older than SP_MIN_YEAR"),
    ({"lines": str(sc.SP_MIN_LINES - 1)}, "under SP_MIN_LINES"),
    ({"commit_count": str(sc.SP_MIN_COMMITS - 1)}, "under SP_MIN_COMMITS"),
])
def test_strong_tier_eligible_rejects_each_filter_boundary(over, why):
    assert not vs.strong_tier_eligible(ent_row(**over)), why


def test_strong_tier_eligible_ignores_the_per_company_cap():
    """The cap is what the audit measures, so the mirror must not apply it."""
    rows = [ent_row(url=f"https://github.com/acme/w{i}") for i in range(sc.SP_MAX_PER_COMPANY + 5)]
    assert all(vs.strong_tier_eligible(r) for r in rows)


def test_strong_tier_eligible_tracks_select_corpus_thresholds(monkeypatch):
    """Thresholds import from the selector, so a bump there must bind here."""
    monkeypatch.setattr(sc, "SP_MIN_COMMITS", 10_000)
    assert not vs.strong_tier_eligible(ent_row(commit_count="500"))


# ---------------------------------------------------------------- loaders


def test_load_enterprise_pads_short_lines_and_keys_on_url(tmp_path, monkeypatch):
    f = tmp_path / "enterprise_projects.txt"
    f.write_text("https://github.com/Acme/Widget\t42\tt\n")
    monkeypatch.setattr(sc, "SPINELLIS_TSV", f)
    ent = vs.load_enterprise()
    assert set(ent) == {"acme/widget"}
    assert ent["acme/widget"]["project_id"] == "42"
    assert ent["acme/widget"]["license"] == ""


def test_load_enterprise_takes_a_full_width_row_unpadded(tmp_path, monkeypatch):
    f = tmp_path / "enterprise_projects.txt"
    cells = [""] * len(sc.SPINELLIS_COLS)
    cells[0] = "https://github.com/acme/widget"
    cells[-1] = "Apache-2.0"
    f.write_text("\t".join(cells) + "\n")
    monkeypatch.setattr(sc, "SPINELLIS_TSV", f)
    assert vs.load_enterprise()["acme/widget"]["license"] == "Apache-2.0"


def test_load_enterprise_drops_a_line_with_no_url(tmp_path, monkeypatch):
    """A blank trailing line must not become a record keyed on None."""
    f = tmp_path / "enterprise_projects.txt"
    f.write_text("https://github.com/acme/widget\t1\n\n\t\t\n")
    monkeypatch.setattr(sc, "SPINELLIS_TSV", f)
    assert set(vs.load_enterprise()) == {"acme/widget"}


def test_load_cohort_reads_the_first_column_only(tmp_path, monkeypatch):
    f = tmp_path / "cohort_project_details.txt"
    f.write_text("https://github.com/a/b\t1\t2\t3\nhttps://github.com/c/d\t4\t5\t6\n\n")
    monkeypatch.setattr(sc, "COHORT_TSV", f)
    assert vs.load_cohort() == {"a/b", "c/d"}


@pytest.mark.parametrize("name", ["SPINELLIS_TSV", "COHORT_TSV"])
def test_loaders_exit_rather_than_report_on_a_missing_source(tmp_path, monkeypatch, name):
    monkeypatch.setattr(sc, name, tmp_path / "gone.txt")
    loader = vs.load_enterprise if name == "SPINELLIS_TSV" else vs.load_cohort
    monkeypatch.setattr(sc, "SPINELLIS_TSV", tmp_path / "gone.txt", raising=False)
    with pytest.raises(SystemExit):
        loader()


# ---------------------------------------------------------------- build


def test_build_flags_a_registry_row_we_labelled_community():
    ent = {"acme/widget": ent_row()}
    report = "\n".join(vs.build(ent, set(), [cand()]))
    assert "## 4." in report
    assert "`acme/widget`" in report
    assert "1 do not, and are listed here." in report


def test_build_does_not_flag_the_same_row_when_we_labelled_it_company_owned():
    ent = {"acme/widget": ent_row()}
    rows = [cand(stratum="company-owned", source="spinellis",
                 fact="F1_pool:spinellis-sec10k")]
    report = "\n".join(vs.build(ent, set(), rows))
    assert "0 do not, and are listed here." in report


def test_build_ignores_rows_the_eligibility_filter_excluded():
    ent = {"acme/widget": ent_row()}
    rows = [cand(included="False", excluded_because="lang=Python")]
    report = "\n".join(vs.build(ent, set(), rows))
    assert "0 such rows reached our eligible frame." in report


def test_build_counts_a_cohort_member_as_their_non_enterprise_verdict():
    rows = [cand(stratum="company-owned", source="company-org", fact="F2_org:acme")]
    report = "\n".join(vs.build({}, {"acme/widget"}, rows))
    assert "## 5." in report
    assert "1 rows we label `company-owned` sit in their non-enterprise cohort." in report


def test_build_reports_a_row_in_neither_file_as_absent():
    report = "\n".join(vs.build({}, set(), [cand()]))
    assert "| absent | 1 | 100.0%" in report


def test_build_falls_back_to_owner_repo_when_clone_url_is_empty():
    ent = {"acme/widget": ent_row()}
    report = "\n".join(vs.build(ent, set(), [cand(clone_url="")]))
    assert "`acme/widget`" in report


def test_build_omits_the_foundation_caveat_when_no_foundation_row_is_flagged():
    ent = {"acme/widget": ent_row()}
    report = "\n".join(vs.build(ent, set(), [cand()]))
    assert "A `foundation` row above" not in report
    assert "A `community` row with `F1_residual:none`" in report


def test_build_adds_the_foundation_caveat_when_one_is_flagged():
    ent = {"acme/widget": ent_row()}
    rows = [cand(stratum="foundation", source="eclipse", fact="F1_roster:eclipse")]
    report = "\n".join(vs.build(ent, set(), rows))
    assert "A `foundation` row above" in report


def test_build_never_divides_by_zero_on_an_empty_frame():
    report = "\n".join(vs.build({}, set(), []))
    assert "## 1." in report


def test_build_emits_every_stratum_row_even_at_zero():
    report = "\n".join(vs.build({}, set(), [cand()]))
    for s in vs.STRATA:
        assert f"| `{s}` |" in report


# ---------------------------------------------------------------- main


def test_main_writes_the_markdown_file(tmp_path, monkeypatch, capsys):
    ef = tmp_path / "e.txt"
    ef.write_text("https://github.com/acme/widget\t1\n")
    cf = tmp_path / "c.txt"
    cf.write_text("https://github.com/x/y\t1\t2\t3\n")
    cc = tmp_path / "candidates.csv"
    cc.write_text("source,stratum,fact,owner,repo,clone_url,included\n"
                  "ghsearch,community,F1_residual:none,acme,widget,"
                  "https://github.com/acme/widget.git,True\n")
    monkeypatch.setattr(sc, "SPINELLIS_TSV", ef)
    monkeypatch.setattr(sc, "COHORT_TSV", cf)
    monkeypatch.setattr(vs, "CANDIDATES", cc)
    out = tmp_path / "report.md"
    monkeypatch.setattr("sys.argv", ["validate_spinellis.py", "-o", str(out)])
    assert vs.main() == 0
    assert out.read_text().startswith("# Validation against Spinellis")
    assert f"wrote {out}" in capsys.readouterr().out


def test_main_prints_to_stdout_without_o(tmp_path, monkeypatch, capsys):
    ef = tmp_path / "e.txt"
    ef.write_text("https://github.com/acme/widget\t1\n")
    cf = tmp_path / "c.txt"
    cf.write_text("")
    cc = tmp_path / "candidates.csv"
    cc.write_text("source,stratum,fact,owner,repo,clone_url,included\n")
    monkeypatch.setattr(sc, "SPINELLIS_TSV", ef)
    monkeypatch.setattr(sc, "COHORT_TSV", cf)
    monkeypatch.setattr(vs, "CANDIDATES", cc)
    monkeypatch.setattr("sys.argv", ["validate_spinellis.py"])
    assert vs.main() == 0
    assert "# Validation against Spinellis" in capsys.readouterr().out


def test_main_exits_when_candidates_csv_is_absent(tmp_path, monkeypatch):
    monkeypatch.setattr(vs, "CANDIDATES", tmp_path / "gone.csv")
    monkeypatch.setattr("sys.argv", ["validate_spinellis.py"])
    with pytest.raises(SystemExit):
        vs.main()
