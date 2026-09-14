"""Unit tests for shared_history.py.

The clustering is what decides which projects enter the corpus, so it is tested
against the real shapes the probe measured on 2026-09-14:

  * Linux carries four roots, not one.
  * freebsd/freebsd and freebsd/freebsd-src carry identical 215-root sets.
  * MariaDB/server shares two of mysql/mysql-server's three roots.
  * FreeBSD, NetBSD and OpenBSD share none with each other, so the test must
    not merge projects that only share ancestry in the 1990s.

Nothing here touches the network. The one function that clones is exercised
through a fake subprocess, because a test that needs GitHub is a test that fails
on a train.
"""
from __future__ import annotations

import json
import subprocess

import pytest

import shared_history as sh

LINUX_ROOTS = ["1da177e4c3f41524e886b7f1b8a0c1fc7321cac2",
               "a101ad945113be3d7f283a181810d76897f0a0d6",
               "be0e5c097fc206b863ce9fe6b3cfd6974b0110f4",
               "cd26f1bd6bf3c73cc5afe848677b430ab342a909"]
FREEBSD_ROOTS = ["eb3b1302382b1d0cbe37eeebabfcdd546aa2fc4e",
                 "6294b6ab217a2d5f1d2bc23a64505a228294c508"]


def row(name: str, url: str = "", stratum: str = "community",
        size_class: str = "S") -> dict:
    return dict(name=name, url=url or f"https://github.com/{name.replace('__', '/')}.git",
                stratum=stratum, mask=r"\.[ch]$", size_class=size_class)


@pytest.fixture(autouse=True)
def sandbox(tmp_path, monkeypatch):
    """Every test writes its cache under tmp_path, never the real one."""
    monkeypatch.setattr(sh, "CACHE_DIR", tmp_path)
    monkeypatch.setattr(sh, "CACHE", tmp_path / "roots.json")
    monkeypatch.setattr(sh, "SCRATCH", tmp_path / "scratch")
    return tmp_path


# --------------------------------------------------------------------------- #
# the cache
# --------------------------------------------------------------------------- #

def test_a_missing_cache_reads_as_empty():
    assert sh.load_cache() == {"roots": {}, "errors": {}, "clusters": {},
                               "projects": {}, "ancestry": {}}


def test_a_corrupt_cache_reads_as_empty_rather_than_stopping(sandbox):
    """The cache is derived. Refusing to start because a derived file is broken
    would block the corpus on something one command rebuilds."""
    (sandbox / "roots.json").write_text("{not json")
    assert sh.load_cache()["roots"] == {}


def test_the_cache_round_trips(sandbox):
    sh.save_cache({"roots": {"u": ["r"]}, "errors": {}, "excluded": {"a/b": "x"}})
    assert sh.load_cache()["roots"] == {"u": ["r"]}
    assert sh.load_cache()["excluded"] == {"a/b": "x"}


def test_saving_the_cache_does_not_leave_the_temporary_file(sandbox):
    sh.save_cache({"roots": {}, "errors": {}, "excluded": {}})
    assert [p.name for p in sandbox.iterdir()] == ["roots.json"]


# --------------------------------------------------------------------------- #
# the manifest
# --------------------------------------------------------------------------- #

def test_read_manifest_skips_comments_and_blank_lines(sandbox):
    p = sandbox / "m.tsv"
    p.write_text("# header\n\nacme__widget\thttps://x/y.git\tcommunity\t.c\tS\n")
    rows = sh.read_manifest(p)
    assert [r["name"] for r in rows] == ["acme__widget"]
    assert rows[0]["size_class"] == "S"


def test_read_manifest_refuses_a_short_row(sandbox):
    """ctp hard-requires five fields, so a four-field row must fail here rather
    than after the scan has reported success."""
    p = sandbox / "m.tsv"
    p.write_text("acme__widget\thttps://x/y.git\tcommunity\t.c\n")
    with pytest.raises(SystemExit, match="5 tab-separated"):
        sh.read_manifest(p)


# --------------------------------------------------------------------------- #
# grouping. Union-find, because sharing is transitive through a root.
# --------------------------------------------------------------------------- #

def test_identical_root_sets_form_one_cluster():
    """freebsd/freebsd and freebsd/freebsd-src, measured: same 215 roots."""
    groups = sh.group_by_root({"fb": FREEBSD_ROOTS, "fb-src": FREEBSD_ROOTS})
    assert groups == [["fb", "fb-src"]]


def test_a_single_shared_root_is_enough():
    """MariaDB shares two of MySQL's three roots. One would do."""
    groups = sh.group_by_root({"mysql": ["a", "b", "c"], "mariadb": ["c", "d"]})
    assert groups == [["mariadb", "mysql"]]


def test_projects_that_share_nothing_stay_apart():
    """FreeBSD, NetBSD and OpenBSD descend from the same 1990s code and share
    zero roots, because each was converted to git separately."""
    groups = sh.group_by_root({"fb": ["a"], "nb": ["b"], "ob": ["c"]})
    assert sorted(groups) == [["fb"], ["nb"], ["ob"]]


def test_sharing_is_transitive_through_a_root():
    """A tree with two roots ties together two projects that share neither root
    with each other. A plain group-by would miss that."""
    groups = sh.group_by_root({"a": ["r1"], "bridge": ["r1", "r2"], "c": ["r2"]})
    assert groups == [["a", "bridge", "c"]]


def test_a_project_with_no_roots_is_left_out_of_every_cluster():
    """No roots means the clone failed. Unknown is not the same as unrelated,
    and it is certainly not the same as a copy."""
    assert sh.group_by_root({"a": [], "b": ["r"]}) == [["b"]]


# --------------------------------------------------------------------------- #
# clustering and annotation. Nothing is excluded: the relationship is recorded,
# and the record says which member came first.
# --------------------------------------------------------------------------- #

def cache_of(rows, roots_by_name, ancestry=None):
    """A cache holding the given roots and projects, keyed by clone URL.

    clusters_of() and annotate() read the cache alone, never a manifest, so the
    fixture has to populate `projects` as a real scan would.
    """
    by_name = {r["name"]: r["url"] for r in rows}
    return {"roots": {by_name[n]: rs for n, rs in roots_by_name.items()},
            "projects": {r["url"]: {k: r[k] for k in
                                    ("name", "stratum", "size_class")}
                         for r in rows},
            "errors": {}, "clusters": {}, "ancestry": ancestry or {}}


def test_a_cluster_lists_every_member_by_name():
    rows = [row("torvalds__linux"), row("SUSE__kernel")]
    cache = cache_of(rows, {"torvalds__linux": ["r1"], "SUSE__kernel": ["r1"]})
    cluster, = sh.clusters_of(cache)
    assert cluster["id"] == "r1"
    assert sorted(cluster["members"]) == ["suse/kernel", "torvalds/linux"]


def test_a_lone_project_is_not_a_cluster():
    rows = [row("torvalds__linux")]
    assert sh.clusters_of(cache_of(rows, {"torvalds__linux": ["r1"]})) == []


def test_annotate_records_every_member_and_excludes_nobody():
    """The decision this file implements: a copy stays, and the record says so."""
    rows = [row("torvalds__linux"), row("SUSE__kernel")]
    cache = cache_of(rows, {"torvalds__linux": ["r1"], "SUSE__kernel": ["r1"]})
    out = sh.annotate(cache)
    assert sorted(out) == ["suse/kernel", "torvalds/linux"]
    assert out["torvalds/linux"]["shared_with"] == ["suse/kernel"]
    assert "excluded" not in out["torvalds/linux"]


def test_annotate_says_unmeasured_before_the_ancestry_step():
    """Absence of a measurement must not read as a measured tie."""
    rows = [row("a__one"), row("b__two")]
    cache = cache_of(rows, {"a__one": ["r1"], "b__two": ["r1"]})
    out = sh.annotate(cache)
    assert {a["relation"] for a in out.values()} == {"unmeasured"}
    assert all(a["includes"] == [] and a["included_in"] == []
               for a in out.values())


def test_annotate_marks_inclusion_in_both_directions():
    rows = [row("torvalds__linux"), row("SUSE__kernel")]
    cache = cache_of(rows, {"torvalds__linux": ["r1"], "SUSE__kernel": ["r1"]},
                     ancestry={"r1": {"first": "torvalds/linux", "evidence": [
                         {"a": "suse/kernel", "b": "torvalds/linux",
                          "relation": "torvalds/linux inside suse/kernel"}]}})
    out = sh.annotate(cache)
    assert out["torvalds/linux"]["relation"] == "included_in"
    assert out["torvalds/linux"]["included_in"] == ["suse/kernel"]
    assert out["suse/kernel"]["relation"] == "includes"
    assert out["suse/kernel"]["includes"] == ["torvalds/linux"]


def test_annotate_marks_a_diverged_member_even_inside_a_measured_cluster():
    """A cluster can hold a mirror and a lagging fork at once. Claiming inclusion
    for the fork would assert something the graph did not prove for that pair."""
    rows = [row("torvalds__linux"), row("SUSE__kernel"), row("raspberrypi__linux")]
    cache = cache_of(rows, dict.fromkeys(
        ["torvalds__linux", "SUSE__kernel", "raspberrypi__linux"], ["r1"]),
        ancestry={"r1": {"first": "torvalds/linux", "evidence": [
            {"a": "suse/kernel", "b": "torvalds/linux",
             "relation": "torvalds/linux inside suse/kernel"},
            {"a": "raspberrypi/linux", "b": "torvalds/linux",
             "relation": "diverged"}]}})
    out = sh.annotate(cache)
    assert out["suse/kernel"]["relation"] == "includes"
    assert out["raspberrypi/linux"]["relation"] == "diverged"


def test_annotate_names_nobody_first_in_a_diverged_cluster():
    """MariaDB against Percona: both descend from MySQL, which is absent."""
    rows = [row("mariadb__server"), row("percona__percona-xtrabackup")]
    cache = cache_of(rows, {"mariadb__server": ["r1"],
                            "percona__percona-xtrabackup": ["r1"]},
                     ancestry={"r1": {"evidence": [
                         {"a": "mariadb/server", "b": "percona/percona-xtrabackup",
                          "relation": "diverged"}]}})
    out = sh.annotate(cache)
    assert {a["relation"] for a in out.values()} == {"diverged"}
    assert out["mariadb/server"]["diverged_from"] == ["percona/percona-xtrabackup"]
    assert all(a["includes"] == [] and a["included_in"] == []
               for a in out.values())


def test_annotate_carries_the_cluster_note():
    rows = [row("a__one"), row("b__two")]
    cache = cache_of(rows, {"a__one": ["known"], "b__two": ["known"]})
    monkey = {"known": "a note for the reader"}
    original, sh.CLUSTER_NOTES = sh.CLUSTER_NOTES, monkey
    try:
        out = sh.annotate(cache)
    finally:
        sh.CLUSTER_NOTES = original
    assert out["a/one"]["note"] == "a note for the reader"


# --------------------------------------------------------------------------- #
# direction. What the commit graph can and cannot order, measured 2026-09-14.
# --------------------------------------------------------------------------- #

def test_an_identical_pair_is_reported_as_identical():
    """freebsd/freebsd and freebsd/freebsd-src: same commits, two names."""
    evidence = sh.direction({"a": {1, 2, 3}, "b": {1, 2, 3}})
    assert evidence[0]["relation"] == "identical"


def test_strict_containment_names_the_contained_member_first():
    """A subtree merge: rust carries rust-analyzer, so rust-analyzer came first."""
    evidence = sh.direction({"rust": {1, 2, 3, 4}, "analyzer": {1, 2}})
    assert evidence[0]["relation"] == "analyzer inside rust"
    assert evidence[0]["relation"].startswith("analyzer inside")


def test_a_lagging_mirror_still_counts_as_inside():
    """Measured: SUSE/kernel holds 1,482,841 of torvalds/linux's 1,482,901
    commits and misses 60, the commits Linux made after SUSE's last merge.
    Strict containment would call that a divergence and lose the fact."""
    linux = set(range(1_482_901))
    suse = set(range(1_482_841)) | set(range(2_000_000, 2_084_519))
    evidence = sh.direction({"linux": linux, "suse": suse})
    assert evidence[0]["relation"] == "linux inside suse"
    assert evidence[0]["relation"].startswith("linux inside")


def test_the_lag_tolerance_is_a_fraction_not_a_count():
    """A small project must not inherit a large project's allowance."""
    a, b = set(range(100)), set(range(90)) | set(range(500, 600))
    evidence = sh.direction({"a": a, "b": b})
    assert evidence[0]["relation"] == "diverged"


def test_a_lagging_fork_against_its_upstream_gets_no_order():
    """The important limit. Measured on torvalds/linux against
    raspberrypi/linux: 1,398,773 shared, 84,128 only in Linux, 15,892 only in
    rpi. rpi is MORE contained in Linux than Linux is in rpi, yet Linux is the
    upstream, so any containment rule would answer backwards. The honest answer
    is that the graph gives no order."""
    linux = set(range(1_398_773)) | set(range(3_000_000, 3_084_128))
    rpi = set(range(1_398_773)) | set(range(9_000_000, 9_015_892))
    evidence = sh.direction({"linux": linux, "rpi": rpi})
    assert evidence[0]["relation"] == "diverged"
    e = evidence[0]
    assert e["shared"] == 1_398_773


def test_direction_reports_the_counts_as_evidence():
    e = sh.direction({"a": {1, 2, 3}, "b": {3, 4}})[0]
    assert (e["shared"], e["only_a"], e["only_b"]) == (1, 2, 1)


def test_two_containments_in_one_cluster_stay_independent():
    """Measured on the MySQL cluster: tencent/tendbcluster-tendb is a near-mirror
    of tendbcluster-tdbctl, and it is nobody's ancestor. A cluster-wide vote gave
    it the cluster, so there is no vote -- every pair keeps its own answer."""
    evidence = sh.direction({"a": {1}, "b": {1, 2}, "c": {3}, "d": {3, 4}})
    inside = sorted(e["relation"] for e in evidence if "inside" in e["relation"])
    assert inside == ["a inside b", "c inside d"]
    assert sum(1 for e in evidence if e["relation"] == "diverged") == 4


# --------------------------------------------------------------------------- #
# reading the roots. The clone is faked; the argument list is the contract.
# --------------------------------------------------------------------------- #

def fake_run(results):
    """Return a subprocess.run stand-in that answers from `results` in order."""
    calls = []

    def run(cmd, **kw):
        calls.append(cmd)
        rc, out, err = results[len(calls) - 1]
        return subprocess.CompletedProcess(cmd, rc, out, err)

    run.calls = calls
    return run


def test_root_commits_asks_for_the_commit_graph_only(monkeypatch):
    """--filter=tree:0 is what makes this affordable: 819 MB for Linux instead
    of a full clone, and 856 KB for jq."""
    run = fake_run([(0, "", ""), (0, "r2\nr1\nr1\n", "")])
    monkeypatch.setattr(sh.subprocess, "run", run)
    assert sh.root_commits("https://x/y.git", "x__y") == ["r1", "r2"]
    assert "--filter=tree:0" in run.calls[0]
    assert "--bare" in run.calls[0] and "--single-branch" in run.calls[0]
    assert run.calls[1][-3:] == ["rev-list", "--max-parents=0", "HEAD"]


def test_root_commits_removes_a_leftover_clone_before_starting(monkeypatch, sandbox):
    """A scan killed mid-clone leaves the directory behind, and git clone
    refuses a target that exists. The next scan must clear it, not fail."""
    stale = sandbox / "scratch" / "x__y.git"
    stale.mkdir(parents=True)
    (stale / "junk").write_text("from the killed scan")
    run = fake_run([(0, "", ""), (0, "r1\n", "")])
    monkeypatch.setattr(sh.subprocess, "run", run)
    assert sh.root_commits("https://x/y.git", "x__y") == ["r1"]
    assert not stale.exists()


def test_root_commits_reports_a_failed_clone(monkeypatch):
    monkeypatch.setattr(sh.subprocess, "run",
                        fake_run([(128, "", "fatal: repository not found")]))
    with pytest.raises(RuntimeError, match="not found"):
        sh.root_commits("https://x/gone.git", "x__gone")


def test_root_commits_reports_a_failed_rev_list(monkeypatch):
    monkeypatch.setattr(sh.subprocess, "run",
                        fake_run([(0, "", ""), (128, "", "fatal: bad revision")]))
    with pytest.raises(RuntimeError, match="bad revision"):
        sh.root_commits("https://x/y.git", "x__y")


def test_root_commits_reports_a_timeout_rather_than_hanging(monkeypatch):
    """raspberrypi/linux ran 1,122 s and still failed. A ceiling is required,
    and tripping it must name the project, not kill the scan."""
    def boom(cmd, **kw):
        raise subprocess.TimeoutExpired(cmd, sh.CLONE_TIMEOUT_S)
    monkeypatch.setattr(sh.subprocess, "run", boom)
    with pytest.raises(RuntimeError, match="timed out"):
        sh.root_commits("https://x/slow.git", "x__slow")


def test_root_commits_deletes_the_clone_even_when_it_fails(monkeypatch, sandbox):
    def boom(cmd, **kw):
        (sandbox / "scratch" / "x__y.git").mkdir(parents=True, exist_ok=True)
        raise subprocess.TimeoutExpired(cmd, 1)
    monkeypatch.setattr(sh.subprocess, "run", boom)
    with pytest.raises(RuntimeError):
        sh.root_commits("https://x/y.git", "x__y")
    assert not (sandbox / "scratch" / "x__y.git").exists()


# --------------------------------------------------------------------------- #
# the scan
# --------------------------------------------------------------------------- #

def test_scan_skips_a_project_whose_roots_are_cached(monkeypatch):
    monkeypatch.setattr(sh, "root_commits",
                        lambda url, name: pytest.fail("should not clone"))
    rows = [row("a__b")]
    cache = {"roots": {rows[0]["url"]: ["r1"]}, "errors": {}, "excluded": {}}
    assert sh.scan(rows, cache)["roots"][rows[0]["url"]] == ["r1"]


def test_scan_force_re_reads_a_cached_project(monkeypatch):
    monkeypatch.setattr(sh, "root_commits", lambda url, name: ["fresh"])
    rows = [row("a__b")]
    cache = {"roots": {rows[0]["url"]: ["stale"]}, "errors": {}, "excluded": {}}
    assert sh.scan(rows, cache, force=True)["roots"][rows[0]["url"]] == ["fresh"]


def test_scan_records_a_failure_and_carries_on(monkeypatch):
    """One unreachable repository must not hide the clusters among the others."""
    def flaky(url, name):
        if name == "bad__one":
            raise RuntimeError("gone")
        return ["r1"]
    monkeypatch.setattr(sh, "root_commits", flaky)
    rows = [row("bad__one"), row("good__two")]
    cache = sh.scan(rows, sh.load_cache())
    assert cache["errors"][rows[0]["url"]] == "gone"
    assert cache["roots"][rows[1]["url"]] == ["r1"]


def test_scan_clears_a_stale_error_once_the_clone_works(monkeypatch):
    monkeypatch.setattr(sh, "root_commits", lambda url, name: ["r1"])
    rows = [row("a__b")]
    cache = {"roots": {}, "errors": {rows[0]["url"]: "old failure"}, "excluded": {}}
    cache = sh.scan(rows, cache)
    assert rows[0]["url"] not in cache["errors"]


def test_scan_drops_stale_roots_when_a_re_read_fails(monkeypatch):
    """Keeping roots that no longer verify would cluster on evidence the scan
    just failed to confirm."""
    def boom(url, name):
        raise RuntimeError("gone")
    monkeypatch.setattr(sh, "root_commits", boom)
    rows = [row("a__b")]
    cache = {"roots": {rows[0]["url"]: ["r1"]}, "errors": {}, "excluded": {}}
    cache = sh.scan(rows, cache, force=True)
    assert rows[0]["url"] not in cache["roots"]


def test_scan_saves_after_every_project(monkeypatch, sandbox):
    """A scan of 200 projects must not lose everything to an interruption."""
    seen = []

    def one_then_stop(url, name):
        if seen:
            raise KeyboardInterrupt
        seen.append(name)
        return ["r1"]
    monkeypatch.setattr(sh, "root_commits", one_then_stop)
    rows = [row("a__b"), row("c__d")]
    with pytest.raises(KeyboardInterrupt):
        sh.scan(rows, sh.load_cache())
    assert sh.load_cache()["roots"][rows[0]["url"]] == ["r1"]


# --------------------------------------------------------------------------- #
# the command line
# --------------------------------------------------------------------------- #

def write_manifest(sandbox, rows) -> str:
    p = sandbox / "m.tsv"
    p.write_text("".join(f"{r['name']}\t{r['url']}\t{r['stratum']}\t"
                         f"{r['mask']}\t{r['size_class']}\n" for r in rows))
    return str(p)


def args_for(sandbox, rows, **over):
    import argparse
    base = dict(manifest=write_manifest(sandbox, rows), force=False)
    base.update(over)
    return argparse.Namespace(**base)


def test_scan_command_records_every_cluster_member(monkeypatch, sandbox):
    monkeypatch.setattr(sh, "root_commits", lambda url, name: ["r1"])
    rows = [row("torvalds__linux"), row("SUSE__kernel")]
    # Non-zero, because no direction is measured yet.
    assert sh.cmd_scan(args_for(sandbox, rows)) == 1
    cache = json.loads((sandbox / "roots.json").read_text())
    assert sorted(cache["clusters"]) == ["suse/kernel", "torvalds/linux"]
    assert cache["clusters"]["suse/kernel"]["relation"] == "unmeasured"


def test_scan_command_excludes_nothing(monkeypatch, sandbox):
    """The record must not grow an exclusion map again by accident."""
    monkeypatch.setattr(sh, "root_commits", lambda url, name: ["r1"])
    rows = [row("torvalds__linux"), row("SUSE__kernel")]
    sh.cmd_scan(args_for(sandbox, rows))
    assert "excluded" not in json.loads((sandbox / "roots.json").read_text())


def test_scan_command_drops_a_stale_exclusion_map(monkeypatch, sandbox):
    """A cache from the exclusion era must not keep a verdict nothing writes."""
    monkeypatch.setattr(sh, "root_commits", lambda url, name: ["r1"])
    sh.save_cache({"roots": {}, "errors": {}, "clusters": {}, "projects": {},
                   "ancestry": {}, "excluded": {"old/verdict": "gone"}})
    sh.cmd_scan(args_for(sandbox, [row("a__one")]))
    assert "excluded" not in json.loads((sandbox / "roots.json").read_text())


def test_scan_command_asks_for_the_ancestry_step(monkeypatch, sandbox, capsys):
    monkeypatch.setattr(sh, "root_commits", lambda url, name: ["r1"])
    rows = [row("mysql__mysql-server"), row("mariadb__server")]
    assert sh.cmd_scan(args_for(sandbox, rows)) == 1
    out = capsys.readouterr().out
    assert "no measured direction" in out
    assert "shared_history.py ancestry" in out


def test_a_cluster_survives_the_project_leaving_the_manifest(monkeypatch, sandbox):
    """The bug this guards against, measured on the real frame on 2026-09-14:
    a verdict scoped to the current manifest vanished when the draw dropped the
    project, and the draw took it straight back. The corpus oscillated between
    intel/mOS and SUSE/kernel. Roots are cached for ever, so membership is
    permanent knowledge."""
    monkeypatch.setattr(sh, "root_commits", lambda url, name: ["r1"])
    sh.cmd_scan(args_for(sandbox, [row("torvalds__linux"), row("intel__mos")]))
    # The draw drops intel/mos and picks suse/kernel instead.
    sh.cmd_scan(args_for(sandbox, [row("torvalds__linux"), row("suse__kernel")]))
    clusters = json.loads((sandbox / "roots.json").read_text())["clusters"]
    assert sorted(clusters) == ["intel/mos", "suse/kernel", "torvalds/linux"]


def test_a_cluster_survives_a_cache_written_before_names_were_recorded(monkeypatch, sandbox):
    """Roots cached by an older version carry no name. Skipping them would
    narrow resolution to the latest manifest again."""
    monkeypatch.setattr(sh, "root_commits", lambda url, name: ["r1"])
    sh.save_cache({"roots": {"https://github.com/intel/mOS.git": ["r1"]},
                   "errors": {}, "clusters": {}, "projects": {}, "ancestry": {}})
    sh.cmd_scan(args_for(sandbox, [row("torvalds__linux")]))
    clusters = json.loads((sandbox / "roots.json").read_text())["clusters"]
    assert sorted(clusters) == ["intel/mos", "torvalds/linux"]


def test_scan_command_refuses_a_missing_manifest(sandbox, capsys):
    import argparse
    assert sh.cmd_scan(argparse.Namespace(manifest=str(sandbox / "nope.tsv"),
                                          force=False)) == 1
    assert "run `select_corpus.py sample` first" in capsys.readouterr().out


def test_report_command_needs_no_network(monkeypatch, sandbox):
    monkeypatch.setattr(sh, "root_commits",
                        lambda url, name: pytest.fail("report must not clone"))
    rows = [row("torvalds__linux"), row("SUSE__kernel")]
    sh.save_cache({"roots": {r["url"]: ["r1"] for r in rows}, "errors": {},
                   "clusters": {}, "projects": {}, "ancestry": {}})
    import argparse
    assert sh.cmd_report(argparse.Namespace(
        manifest=write_manifest(sandbox, rows))) == 1


def test_report_command_returns_zero_once_every_cluster_is_measured(monkeypatch, sandbox):
    rows = [row("torvalds__linux"), row("SUSE__kernel")]
    sh.save_cache({"roots": {r["url"]: ["r1"] for r in rows}, "errors": {},
                   "clusters": {}, "projects": {},
                   "ancestry": {"r1": {"first": "torvalds/linux", "evidence": [
                       {"a": "suse/kernel", "b": "torvalds/linux",
                        "relation": "torvalds/linux inside suse/kernel"}]}}})
    import argparse
    assert sh.cmd_report(argparse.Namespace(
        manifest=write_manifest(sandbox, rows))) == 0


def test_the_report_prints_the_cluster_note(monkeypatch, sandbox, capsys):
    """The note is how a reader learns that a cluster is a subtree merge rather
    than a fork, so it has to reach the report."""
    monkeypatch.setattr(sh, "CLUSTER_NOTES", {"r1": "a subtree merge, not a fork"})
    rows = [row("rust-lang__rust"), row("rust-lang__rust-analyzer")]
    sh.save_cache({"roots": {r["url"]: ["r1"] for r in rows}, "errors": {},
                   "clusters": {}, "projects": {}, "ancestry": {}})
    import argparse
    sh.cmd_report(argparse.Namespace(manifest=write_manifest(sandbox, rows)))
    assert "a subtree merge, not a fork" in capsys.readouterr().out


def test_report_command_refuses_a_missing_manifest(sandbox, capsys):
    import argparse
    assert sh.cmd_report(argparse.Namespace(manifest=str(sandbox / "nope.tsv"))) == 1


def test_the_report_names_a_project_it_could_not_read(monkeypatch, sandbox, capsys):
    rows = [row("slow__one")]
    sh.save_cache({"roots": {}, "errors": {rows[0]["url"]: "timed out"},
                   "clusters": {}, "projects": {}, "ancestry": {}})
    import argparse
    sh.cmd_report(argparse.Namespace(manifest=write_manifest(sandbox, rows)))
    assert "could not be read" in capsys.readouterr().out


# --------------------------------------------------------------------------- #
# the ancestry command. head_commits is faked; the pairing is the contract.
# --------------------------------------------------------------------------- #

def test_ancestry_measures_a_cluster_and_records_the_direction(monkeypatch, sandbox):
    sets = {"torvalds__linux": set(range(100)),
            "suse__kernel": set(range(100)) | set(range(500, 600))}
    monkeypatch.setattr(sh, "head_commits", lambda url, name: sets[name])
    monkeypatch.setattr(sh, "repo_created", lambda name: {
        "torvalds/linux": "2011-09-04T00:00:00Z",
        "suse/kernel": "2013-01-01T00:00:00Z"}[name])
    rows = [row("torvalds__linux"), row("suse__kernel")]
    sh.save_cache({"roots": {r["url"]: ["r1"] for r in rows}, "errors": {},
                   "clusters": {}, "projects": {}, "ancestry": {}})
    assert sh.cmd_ancestry(args_for(sandbox, rows)) == 0
    cache = json.loads((sandbox / "roots.json").read_text())
    assert cache["ancestry"]["r1"]["evidence"][0]["relation"] == (
        "torvalds/linux inside suse/kernel")
    assert cache["clusters"]["suse/kernel"]["relation"] == "includes"
    assert cache["clusters"]["suse/kernel"]["includes"] == ["torvalds/linux"]
    # Origin is the older repository, and it is a separate claim from inclusion.
    assert cache["clusters"]["suse/kernel"]["first"] == "torvalds/linux"


def test_ancestry_skips_a_cluster_it_already_measured(monkeypatch, sandbox):
    monkeypatch.setattr(sh, "head_commits",
                        lambda url, name: pytest.fail("should not clone"))
    monkeypatch.setattr(sh, "repo_created",
                        lambda name: pytest.fail("should not call GitHub"))
    rows = [row("a__one"), row("b__two")]
    sh.save_cache({"roots": {r["url"]: ["r1"] for r in rows}, "errors": {},
                   "clusters": {}, "projects": {},
                   "ancestry": {"r1": {"evidence": [{"a": "a/one", "b": "b/two",
                                                     "relation": "diverged"}],
                                       "created": {"a/one": "2020-01-01T00:00:00Z",
                                                   "b/two": "2021-01-01T00:00:00Z"}}}})
    assert sh.cmd_ancestry(args_for(sandbox, rows)) == 0


def test_ancestry_backfills_dates_without_cloning(monkeypatch, sandbox):
    """Inclusion costs minutes per member and the dates cost seconds, so a
    cluster measured before the dates existed must not pay for the clones again."""
    monkeypatch.setattr(sh, "head_commits",
                        lambda url, name: pytest.fail("should not clone"))
    monkeypatch.setattr(sh, "repo_created", lambda name: "2019-05-05T00:00:00Z")
    rows = [row("a__one"), row("b__two")]
    sh.save_cache({"roots": {r["url"]: ["r1"] for r in rows}, "errors": {},
                   "clusters": {}, "projects": {},
                   "ancestry": {"r1": {"evidence": [{"a": "a/one", "b": "b/two",
                                                     "relation": "diverged"}]}}})
    assert sh.cmd_ancestry(args_for(sandbox, rows)) == 0
    cache = json.loads((sandbox / "roots.json").read_text())
    assert cache["ancestry"]["r1"]["created"]["a/one"] == "2019-05-05T00:00:00Z"
    assert cache["ancestry"]["r1"]["evidence"][0]["relation"] == "diverged"


def test_ancestry_force_re_measures(monkeypatch, sandbox):
    seen = []
    monkeypatch.setattr(sh, "head_commits",
                        lambda url, name: seen.append(name) or {1, 2})
    monkeypatch.setattr(sh, "repo_created", lambda name: "")
    rows = [row("a__one"), row("b__two")]
    sh.save_cache({"roots": {r["url"]: ["r1"] for r in rows}, "errors": {},
                   "clusters": {}, "projects": {},
                   "ancestry": {"r1": {"evidence": []}}})
    sh.cmd_ancestry(args_for(sandbox, rows, force=True))
    assert sorted(seen) == ["a__one", "b__two"]


def test_ancestry_carries_on_when_one_member_will_not_clone(monkeypatch, sandbox):
    """One unreachable repository must not cost the whole cluster its direction."""
    def flaky(url, name):
        if name == "bad__one":
            raise RuntimeError("gone")
        return {1, 2}
    monkeypatch.setattr(sh, "head_commits", flaky)
    monkeypatch.setattr(sh, "repo_created", lambda name: "")
    rows = [row("bad__one"), row("good__two"), row("good__three")]
    sh.save_cache({"roots": {r["url"]: ["r1"] for r in rows}, "errors": {},
                   "clusters": {}, "projects": {}, "ancestry": {}})
    sh.cmd_ancestry(args_for(sandbox, rows))
    cache = json.loads((sandbox / "roots.json").read_text())
    assert cache["ancestry"]["r1"]["unreadable"] == ["bad/one"]


def test_ancestry_records_no_inclusion_when_only_one_member_is_readable(monkeypatch, sandbox, capsys):
    def flaky(url, name):
        if name == "good__two":
            return {1, 2}
        raise RuntimeError("gone")
    monkeypatch.setattr(sh, "head_commits", flaky)
    monkeypatch.setattr(sh, "repo_created", lambda name: "2020-01-01T00:00:00Z")
    rows = [row("bad__one"), row("good__two")]
    sh.save_cache({"roots": {r["url"]: ["r1"] for r in rows}, "errors": {},
                   "clusters": {}, "projects": {}, "ancestry": {}})
    sh.cmd_ancestry(args_for(sandbox, rows))
    cache = json.loads((sandbox / "roots.json").read_text())
    # The dates are still recorded: they cost nothing and they answer origin.
    assert "evidence" not in cache["ancestry"]["r1"]
    assert cache["ancestry"]["r1"]["created"]["good/two"]
    assert "fewer than two members readable" in capsys.readouterr().out


def test_ancestry_refuses_a_missing_manifest(sandbox):
    import argparse
    assert sh.cmd_ancestry(argparse.Namespace(
        manifest=str(sandbox / "nope.tsv"), force=False)) == 1


def test_head_commits_asks_for_the_commit_graph_only(monkeypatch):
    """--filter=tree:0 is what makes this affordable: 819 MB for Linux."""
    graph = "1da177e4c3f41524aaaa\nbe0e5c097fc206b8bbbb\n"
    run = fake_run([(0, "", ""), (0, graph, "")])
    monkeypatch.setattr(sh.subprocess, "run", run)
    out = sh.head_commits("https://x/y.git", "x__y")
    assert out == {int("1da177e4c3f41524", 16), int("be0e5c097fc206b8", 16)}
    assert "--filter=tree:0" in run.calls[0]
    assert run.calls[1][-2:] == ["rev-list", "HEAD"]


def test_ancestry_measures_inclusion_without_re_fetching_the_dates(monkeypatch, sandbox):
    """The mirror of the backfill: dates already known, inclusion still missing.
    Each half is cached on its own, so neither pays for the other."""
    monkeypatch.setattr(sh, "repo_created",
                        lambda name: pytest.fail("dates are already cached"))
    monkeypatch.setattr(sh, "head_commits", lambda url, name: {1, 2})
    rows = [row("a__one"), row("b__two")]
    sh.save_cache({"roots": {r["url"]: ["r1"] for r in rows}, "errors": {},
                   "clusters": {}, "projects": {},
                   "ancestry": {"r1": {"created": {"a/one": "2020-01-01T00:00:00Z",
                                                   "b/two": "2021-01-01T00:00:00Z"}}}})
    assert sh.cmd_ancestry(args_for(sandbox, rows)) == 0
    cache = json.loads((sandbox / "roots.json").read_text())
    assert cache["ancestry"]["r1"]["evidence"][0]["relation"] == "identical"
    assert cache["clusters"]["a/one"]["first"] == "a/one"


def test_repo_created_returns_the_date_github_reports(monkeypatch):
    """Origin comes from here, because the commit graph proves inclusion only."""
    run = fake_run([(0, "2011-09-04T22:48:12Z\n", "")])
    monkeypatch.setattr(sh.subprocess, "run", run)
    assert sh.repo_created("torvalds__linux") == "2011-09-04T22:48:12Z"
    assert run.calls[0][:4] == ["gh", "repo", "view", "torvalds/linux"]


def test_repo_created_accepts_a_slug_as_well_as_a_project_name(monkeypatch):
    run = fake_run([(0, "2016-01-01T00:00:00Z\n", "")])
    monkeypatch.setattr(sh.subprocess, "run", run)
    assert sh.repo_created("zephyrproject-rtos/zephyr") == "2016-01-01T00:00:00Z"
    assert run.calls[0][3] == "zephyrproject-rtos/zephyr"


def test_repo_created_is_empty_when_github_refuses(monkeypatch):
    """An unknown date must not read as the epoch, which would win every
    comparison and name the wrong project as the oldest."""
    monkeypatch.setattr(sh.subprocess, "run",
                        fake_run([(1, "", "could not resolve to a Repository")]))
    assert sh.repo_created("gone__away") == ""


@pytest.mark.parametrize("boom", [OSError("no gh on PATH"),
                                  subprocess.TimeoutExpired("gh", 120)])
def test_repo_created_survives_a_missing_or_hung_gh(monkeypatch, boom):
    def raise_it(cmd, **kw):
        raise boom
    monkeypatch.setattr(sh.subprocess, "run", raise_it)
    assert sh.repo_created("a__b") == ""


def test_the_oldest_repository_is_named_first_even_when_inclusion_disagrees():
    """The two claims are independent and can point opposite ways.

    Built from the real Zephyr numbers. Measured, that pair reads `diverged`: its
    lag is 0.32% against a 0.1% tolerance. Loosen the tolerance and it would read
    `simplelink-zephyr inside zephyr`, and the subset would be the FORK, while the
    older repository is Zephyr. That is why inclusion must not decide origin."""
    ti_name = "texasinstruments/simplelink-zephyr"
    rows = [row("texasinstruments__simplelink-zephyr"),
            row("zephyrproject-rtos__zephyr")]
    cache = cache_of(rows, {"texasinstruments__simplelink-zephyr": ["r1"],
                            "zephyrproject-rtos__zephyr": ["r1"]},
                     ancestry={"r1": {
                         "evidence": [{"a": ti_name,
                                       "b": "zephyrproject-rtos/zephyr",
                                       "relation": f"{ti_name} inside "
                                                   "zephyrproject-rtos/zephyr"}],
                         "created": {ti_name: "2021-01-01T00:00:00Z",
                                     "zephyrproject-rtos/zephyr": "2016-01-01T00:00:00Z"}}})
    out = sh.annotate(cache)
    assert out[ti_name]["relation"] == "included_in"
    assert out[ti_name]["first"] == "zephyrproject-rtos/zephyr"


def test_an_unknown_date_never_wins_the_oldest_comparison():
    rows = [row("a__one"), row("b__two")]
    cache = cache_of(rows, {"a__one": ["r1"], "b__two": ["r1"]},
                     ancestry={"r1": {"evidence": [],
                                      "created": {"a/one": "",
                                                  "b/two": "2020-01-01T00:00:00Z"}}})
    assert sh.annotate(cache)["a/one"]["first"] == "b/two"


def test_head_commits_reports_a_failed_clone(monkeypatch):
    monkeypatch.setattr(sh.subprocess, "run",
                        fake_run([(128, "", "fatal: repository not found")]))
    with pytest.raises(RuntimeError, match="not found"):
        sh.head_commits("https://x/gone.git", "x__gone")


def test_head_commits_reports_a_failed_rev_list(monkeypatch):
    monkeypatch.setattr(sh.subprocess, "run",
                        fake_run([(0, "", ""), (128, "", "fatal: bad revision")]))
    with pytest.raises(RuntimeError, match="bad revision"):
        sh.head_commits("https://x/y.git", "x__y")


def test_head_commits_reports_a_timeout(monkeypatch):
    def boom(cmd, **kw):
        raise subprocess.TimeoutExpired(cmd, sh.CLONE_TIMEOUT_S)
    monkeypatch.setattr(sh.subprocess, "run", boom)
    with pytest.raises(RuntimeError, match="timed out"):
        sh.head_commits("https://x/slow.git", "x__slow")


def test_head_commits_removes_a_leftover_clone(monkeypatch, sandbox):
    stale = sandbox / "scratch" / "x__y.git"
    stale.mkdir(parents=True)
    run = fake_run([(0, "", ""), (0, "1da177e4c3f41524aaaa\n", "")])
    monkeypatch.setattr(sh.subprocess, "run", run)
    sh.head_commits("https://x/y.git", "x__y")
    assert not stale.exists()


def test_main_dispatches_to_the_named_subcommand(monkeypatch, sandbox):
    monkeypatch.setattr(sh.sys, "argv",
                        ["shared_history.py", "report", "--manifest",
                         str(sandbox / "nope.tsv")])
    assert sh.main() == 1
