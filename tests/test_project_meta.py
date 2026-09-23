import json
import sys
from pathlib import Path

import project_meta
from file_mask import UNIVERSAL_MASK

CANDIDATES = (
    "source,stratum,fact,contested,label_date,owner,repo,roster_name,roster_lang,"
    "language,commits,size_class,size_kb,stars,pushed_at,license,owner_type,"
    "archived,fork,clone_url,history_cluster,history_shared_with,history_relation,"
    "history_includes,history_first,history_created,included,excluded_because\n"
    "spinellis,company-owned,F1_roster:spinellis,False,2026-09-13,Tencent,"
    "TencentKona-21,,,Java,77678,M,900000,300,2026-08-01,GPL-2.0,Organization,"
    "False,False,https://github.com/Tencent/TencentKona-21.git,abc123,"
    "openjdk/jdk21u,diverged,,openjdk/jdk21u,2023-01-01T00:00:00Z,True,\n"
)

MANIFEST = (
    "# Corpus manifest (TSV)\n"
    "tencent__tencentkona-21\thttps://github.com/Tencent/TencentKona-21.git\t"
    "company-owned\t\\.java$\tM\n"
)


def test_meta_is_keyed_by_manifest_name_and_joined_on_clone_url(tmp_path):
    """The manifest name is a slug; only clone_url joins reliably."""
    cands = tmp_path / "candidates.csv"
    cands.write_text(CANDIDATES)
    man = tmp_path / "manifest.tsv"
    man.write_text(MANIFEST)

    meta = project_meta.build_meta(cands, man)

    assert set(meta) == {"tencent__tencentkona-21"}
    row = meta["tencent__tencentkona-21"]
    assert row["source"] == "spinellis"
    assert row["stratum"] == "company-owned"
    assert row["history_relation"] == "diverged"
    assert row["history_first"] == "openjdk/jdk21u"
    assert row["commits"] == "77678"
    # clone_url is carried, because repo_name is a lossy slug.
    assert row["clone_url"] == "https://github.com/Tencent/TencentKona-21.git"
    assert row["provenance_status"] == project_meta.PROVENANCE_OK
    # The manifest's own two fields, which no later reader can recover.
    assert row["manifest_category"] == "company-owned"
    assert row["file_mask"] == r"\.java$"
    # Absent values are empty strings, never None: they go straight into SQL.
    assert row["roster_name"] == ""
    assert all(isinstance(v, str) for v in row.values())
    # The bookkeeping columns are not dataset columns.
    assert "included" not in row
    assert "excluded_because" not in row


def test_a_fixture_gets_a_flagged_placeholder_not_blank_metadata(tmp_path):
    """The 10 development fixtures are regenerated so one schema covers every
    file. They must be filterable, not silently blank."""
    cands = tmp_path / "candidates.csv"
    cands.write_text(CANDIDATES)
    man = tmp_path / "manifest.tsv"
    man.write_text(MANIFEST)
    fix = tmp_path / "manifest.mvp5.tsv"
    # An older corpus manifest doubles as a fixture manifest, so it can name a
    # project that is still in the corpus. The corpus row must win.
    fix.write_text("jq\thttps://github.com/jqlang/jq.git\tcommunity\t\\.c$\tS\n"
                   + MANIFEST.splitlines()[1] + "\n")

    meta = project_meta.build_meta(cands, man, fixture_manifests=(fix,))

    assert meta["jq"]["provenance_status"] == project_meta.PROVENANCE_FIXTURE
    assert meta["jq"]["clone_url"] == "https://github.com/jqlang/jq.git"
    assert meta["jq"]["source"] == ""
    assert meta["jq"]["stratum"] == ""
    # A fixture has no provenance, but it does have a real mask.
    assert meta["jq"]["file_mask"] == r"\.c$"
    assert meta["jq"]["manifest_category"] == "community"
    # A corpus member is never flagged as a fixture.
    assert meta["tencent__tencentkona-21"]["provenance_status"] == \
        project_meta.PROVENANCE_OK


def test_a_manifest_project_missing_from_candidates_is_an_error(tmp_path):
    """Silently emitting empty metadata would put blank columns in the paper."""
    cands = tmp_path / "candidates.csv"
    cands.write_text(CANDIDATES)
    man = tmp_path / "manifest.tsv"
    man.write_text(MANIFEST + "ghost\thttps://example.invalid/ghost.git\t"
                              "community\t\\.c$\tS\n")

    try:
        project_meta.build_meta(cands, man)
    except SystemExit as exc:
        assert "ghost" in str(exc)
    else:
        raise AssertionError("a missing project must stop the build")


def test_a_duplicate_clone_url_resolves_to_the_row_the_manifest_was_drawn_from(
        tmp_path):
    """candidates.csv keeps every provenance row, so one repository can appear
    twice under one clone_url -- 197 do, and 5 of those pairs disagree on the
    stratum. The sidecar must pick the same row select_corpus.py picked, or the
    stratum it reports contradicts the manifest category beside it."""
    cands = tmp_path / "candidates.csv"
    stale = (
        "cohort,company-owned,F1_pool:spinellis-cohort,False,2026-09-13,"
        "OldOwner,incubator-doris,,,Java,32053,L,1401325,300,2026-08-01,"
        "Apache-2.0,Organization,False,False,https://github.com/apache/doris.git,"
        ",,,,,,True,\n"
    )
    current = (
        "asf,foundation,F1_roster:asf,False,2026-09-13,apache,doris,"
        "Apache Doris,\"C++,Java\",Java,32052,L,1401625,300,2026-08-01,"
        "Apache-2.0,Organization,False,False,https://github.com/apache/doris.git,"
        ",,,,,,True,\n"
    )
    cands.write_text(CANDIDATES + stale + current)
    man = tmp_path / "manifest.tsv"
    man.write_text(
        "apache__doris\thttps://github.com/apache/doris.git\tfoundation\t"
        "\\.java$\tL\n"
    )

    row = project_meta.build_meta(cands, man)["apache__doris"]

    assert row["stratum"] == row["manifest_category"] == "foundation"
    assert row["source"] == "asf"
    assert row["owner"] == "apache"


def test_the_cli_writes_a_json_sidecar(tmp_path, monkeypatch, capsys):
    """The sidecar is generated and committed, so the CLI is the product."""
    cands = tmp_path / "candidates.csv"
    cands.write_text(CANDIDATES)
    man = tmp_path / "manifest.tsv"
    man.write_text(MANIFEST)
    fix = tmp_path / "manifest.mvp5.tsv"
    fix.write_text("jq\thttps://github.com/jqlang/jq.git\tcommunity\t\\.c$\tS\n")
    out = tmp_path / "project_meta.json"

    monkeypatch.setattr(sys, "argv", [
        "project_meta.py", "--candidates", str(cands), "--manifest", str(man),
        "--fixture-manifest", str(fix), "--out", str(out),
    ])
    assert project_meta.main() == 0

    written = json.loads(out.read_text())
    assert set(written) == {"tencent__tencentkona-21", "jq"}
    assert len(written["jq"]) == len(project_meta.META_FIELDS)
    assert "2 projects" in capsys.readouterr().out


def test_meta_fields_ends_with_the_two_manifest_fields():
    """file_mask is the regex a project was actually tokenized with. The C mask
    is a strict subset of the C++ mask, so ~19 projects will be re-run with a
    wider one: without this column nobody can tell which rows came from which
    mask, and the corpus stops being comparable across projects."""
    assert len(project_meta.META_FIELDS) == 29
    assert project_meta.META_FIELDS[:2] == ("clone_url", "provenance_status")
    assert project_meta.META_FIELDS[-2:] == ("manifest_category", "file_mask")
    assert "included" not in project_meta.META_FIELDS
    assert "excluded_because" not in project_meta.META_FIELDS


def test_the_committed_sidecar_covers_the_corpus_and_the_fixtures():
    """The generated artefact is committed, so it is under test too."""
    root = Path(__file__).resolve().parent.parent
    meta = json.loads((root / "project_meta.json").read_text())

    assert len(meta) == 210
    statuses = [v["provenance_status"] for v in meta.values()]
    assert statuses.count(project_meta.PROVENANCE_OK) == 200
    assert statuses.count(project_meta.PROVENANCE_FIXTURE) == 10
    assert all(set(v) == set(project_meta.META_FIELDS) for v in meta.values())
    # Every row carries the mask it was tokenized with, fixtures included.
    assert all(v["file_mask"] for v in meta.values())


def test_the_committed_sidecar_records_the_universal_mask_for_every_project():
    """The sidecar is a generated artefact, and it goes stale silently.

    file_mask is copied out of the manifest when the sidecar is built, and it is
    injected into all 185 Parquets as a column claiming to say how those tokens
    were produced. On 2026-09-19 the corpus moved to one universal mask, so a
    sidecar left over from the four per-language masks would make every Parquet
    record a mask it was not built with — and nothing downstream reads the
    manifest again to notice. Regenerate with:

        ./project_meta.py --candidates candidates.csv \\
            --manifest manifest.sample.tsv \\
            --fixture-manifest manifest.mvp5.tsv \\
            --fixture-manifest manifest.tsv \\
            --fixture-manifest manifest.shardtest.tsv \\
            --out project_meta.json
    """
    root = Path(__file__).resolve().parent.parent
    meta = json.loads((root / "project_meta.json").read_text())

    stale = {name: row["file_mask"] for name, row in meta.items()
             if row["file_mask"] != UNIVERSAL_MASK}
    assert not stale, (
        f"{len(stale)} project(s) in project_meta.json carry a mask that is not the "
        f"universal one: {sorted(set(stale.values()))}. Regenerate the sidecar."
    )
    # One mask for the whole corpus is the decision, stated as an assertion: the
    # dataset is a tokenized set of projects, not a set of language exemplars.
    assert len({row["file_mask"] for row in meta.values()}) == 1
