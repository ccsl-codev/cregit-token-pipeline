"""Tests for build_manifest.py: the frame contract, u order, and the two outputs."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import pytest

import build_manifest as bm
import ctp
import validate_schema
from file_mask import UNIVERSAL_MASK

FIXTURE = Path(__file__).parent / "fixtures" / "frame.sample.csv"


def write_frame(tmp_path: Path, rows: list[dict], name: str = "frame.csv") -> Path:
    path = tmp_path / name
    with path.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    return path


def frame_row(slug: str = "jqlang/jq", **over) -> dict:
    row = dict(slug=slug, host="github.com", repo_url=f"https://github.com/{slug}.git",
               head_oid="a" * 40, u=repr(bm.u_of(slug)), size_class="S", labels="community")
    row.update(over)
    return row


def build_files(tmp_path: Path, frame: Path, *extra: str) -> tuple[Path, Path, Path]:
    manifest, meta = tmp_path / "m.tsv", tmp_path / "meta.json"
    assert bm.main([str(frame), "--manifest", str(manifest),
                    "--project-meta", str(meta), *extra]) == 0
    return manifest, meta, manifest.with_suffix(".order.tsv")


def test_u_is_the_first_53_bits_of_the_salted_sha256():
    digest = hashlib.sha256(b"20261110:jqlang/jq").digest()
    assert bm.u_of("jqlang/jq") == (int.from_bytes(digest[:8], "big") >> 11) / 2**53


def test_u_is_always_below_one():
    assert all(0 <= bm.u_of(f"o/r{i}") < 1 for i in range(2000))


def test_the_fixture_builds_a_manifest_in_u_order(tmp_path):
    manifest, _, _ = build_files(tmp_path, FIXTURE)
    projects = ctp.read_manifest(manifest, None)
    assert [p["name"] for p in projects] == [
        "zserge__jsmn", "DaveGamble__cJSON",
        "gitlab.com__example-group__example-lib", "antirez__kilo"]
    us = [bm.u_of(s) for s in ("zserge/jsmn", "DaveGamble/cJSON",
                               "example-group/example-lib", "antirez/kilo")]
    assert us == sorted(us)


def test_every_manifest_row_is_pinned_to_head_oid(tmp_path):
    manifest, _, _ = build_files(tmp_path, FIXTURE)
    projects = {p["name"]: p for p in ctp.read_manifest(manifest, None)}
    assert projects["antirez__kilo"]["commit"] == "0099562d0e79aea0c6deedfa1ee0ef4a3a8883b7"
    assert all(len(p["commit"]) == 40 for p in projects.values())


def test_a_row_not_included_is_dropped(tmp_path):
    manifest, meta, _ = build_files(tmp_path, FIXTURE)
    assert "example-org__excluded" not in manifest.read_text()
    assert "example-org__excluded" not in json.loads(meta.read_text())


def test_labels_become_the_category_and_the_manifest_category(tmp_path):
    manifest, meta, _ = build_files(tmp_path, FIXTURE)
    gitlab = "gitlab.com__example-group__example-lib"
    [row] = [p for p in ctp.read_manifest(manifest, None) if p["name"] == gitlab]
    assert row["category"] == "company;Example Corp"
    assert json.loads(meta.read_text())[gitlab]["manifest_category"] == "company;Example Corp"


def test_a_category_column_wins_over_labels(tmp_path):
    frame = write_frame(tmp_path, [frame_row(category="foundation")])
    manifest, meta, _ = build_files(tmp_path, frame)
    assert ctp.read_manifest(manifest, None)[0]["category"] == "foundation"


def test_project_meta_holds_the_29_fields_the_generator_reads(tmp_path):
    _, meta, _ = build_files(tmp_path, FIXTURE)
    rec = json.loads(meta.read_text())["antirez__kilo"]
    assert tuple(rec) == bm.META_FIELDS
    assert rec["clone_url"] == "https://github.com/antirez/kilo.git"
    assert rec["owner"] == "antirez" and rec["repo"] == "kilo"
    assert rec["language"] == "C" and rec["commits"] == "20"
    assert rec["size_class"] == "S"
    assert rec["file_mask"] == UNIVERSAL_MASK
    assert rec["provenance_status"].startswith("frame:frame.sample.csv@sha256:")


def test_meta_fields_match_the_schema_contract():
    """Columns 2..30 of the dataset are the sidecar fields, in order."""
    assert bm.META_FIELDS == tuple(c for c, _ in validate_schema.EXPECTED_COLUMNS[1:30])


def test_the_order_file_maps_queue_position_to_slug_and_u(tmp_path):
    _, _, order = build_files(tmp_path, FIXTURE)
    rows = list(csv.DictReader(order.open(), delimiter="\t"))
    assert [r["queue_pos"] for r in rows] == ["1", "2", "3", "4"]
    assert rows[0]["slug"] == "zserge/jsmn"
    assert float(rows[0]["u"]) == bm.u_of("zserge/jsmn")


def test_limit_keeps_the_lowest_u_rows(tmp_path):
    manifest, meta, _ = build_files(tmp_path, FIXTURE, "--limit", "2")
    assert [p["name"] for p in ctp.read_manifest(manifest, None)] == [
        "zserge__jsmn", "DaveGamble__cJSON"]
    assert len(json.loads(meta.read_text())) == 2


def test_name_with_owner_is_accepted_as_the_slug_column(tmp_path):
    row = frame_row()
    row["name_with_owner"] = row.pop("slug")
    manifest, _, _ = build_files(tmp_path, write_frame(tmp_path, [row]))
    assert ctp.read_manifest(manifest, None)[0]["name"] == "jqlang__jq"


def test_the_manifest_header_names_the_frame_and_its_hash(tmp_path):
    manifest, _, _ = build_files(tmp_path, FIXTURE)
    digest = hashlib.sha256(FIXTURE.read_bytes()).hexdigest()
    assert f"sha256 {digest}" in manifest.read_text().splitlines()[0]


@pytest.mark.parametrize("over, message", [
    pytest.param(dict(head_oid="abc"), "head_oid", id="short-sha"),
    pytest.param(dict(head_oid="A" * 40), "head_oid", id="upper-sha"),
    pytest.param(dict(u="0.5"), "does not match", id="wrong-u"),
    pytest.param(dict(u="x"), "not a number", id="u-not-a-number"),
    pytest.param(dict(u="1.0"), "outside", id="u-one"),
    pytest.param(dict(size_class="XL"), "size_class", id="bad-size-class"),
    pytest.param(dict(repo_url=""), "repo_url is empty", id="no-url"),
    pytest.param(dict(labels="a\tb"), "tab or a newline", id="tab-in-labels"),
    pytest.param(dict(slug="jq"), "not owner/repo", id="slug-without-owner"),
])
def test_a_broken_row_stops_the_build_and_names_the_line(tmp_path, capsys, over, message):
    slug = over.get("slug", "jqlang/jq")
    row = frame_row(slug, **{k: v for k, v in over.items() if k != "slug"})
    frame = write_frame(tmp_path, [row])
    assert bm.main([str(frame), "--manifest", str(tmp_path / "m.tsv"),
                    "--project-meta", str(tmp_path / "p.json")]) == 1
    err = capsys.readouterr().err
    assert message in err and "frame.csv:2" in err
    assert not (tmp_path / "m.tsv").exists()


def test_a_missing_column_is_named(tmp_path, capsys):
    row = frame_row()
    del row["head_oid"]
    assert bm.main([str(write_frame(tmp_path, [row])), "--manifest", str(tmp_path / "m"),
                    "--project-meta", str(tmp_path / "p")]) == 1
    assert "missing column(s): head_oid" in capsys.readouterr().err


def test_a_frame_without_a_slug_column_is_refused(tmp_path, capsys):
    row = frame_row()
    del row["slug"]
    assert bm.main([str(write_frame(tmp_path, [row])), "--manifest", str(tmp_path / "m"),
                    "--project-meta", str(tmp_path / "p")]) == 1
    assert "slug or name_with_owner" in capsys.readouterr().err


def test_two_rows_with_one_name_are_refused(tmp_path):
    frame = write_frame(tmp_path, [frame_row("a/b"), frame_row("A/B")])
    with pytest.raises(bm.FrameError, match="repeats"):
        bm.build(frame)


def test_a_slug_that_is_not_a_plain_name_is_refused():
    with pytest.raises(bm.FrameError, match="plain directory name"):
        bm.project_name("github.com", "own er/repo")
    with pytest.raises(bm.FrameError, match="plain directory name"):
        bm.project_name("github.com", "owner/re..po")


def test_a_non_github_host_prefixes_the_name():
    assert bm.project_name("gitlab.com", "g/sub/p") == "gitlab.com__g__sub__p"
    assert bm.project_name("github.com", "o/r") == "o__r"


def test_a_repository_at_the_root_of_a_non_github_host_is_named_after_the_host():
    assert bm.project_name("git.libreoffice.org", "libvisio") == "git.libreoffice.org__libvisio"
    with pytest.raises(bm.FrameError, match="not owner/repo"):
        bm.project_name("github.com", "libvisio")


def test_limit_below_one_is_a_usage_error(tmp_path):
    with pytest.raises(SystemExit):
        bm.main([str(FIXTURE), "--manifest", str(tmp_path / "m"),
                 "--project-meta", str(tmp_path / "p"), "--limit", "0"])


def test_a_missing_frame_fails_cleanly(tmp_path, capsys):
    assert bm.main([str(tmp_path / "absent.csv"), "--manifest", str(tmp_path / "m"),
                    "--project-meta", str(tmp_path / "p")]) == 1
    assert "FAIL" in capsys.readouterr().err
