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
    assert sh.load_cache() == {"roots": {}, "errors": {}, "excluded": {},
                               "clusters": {}, "projects": {}}


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
# resolution. Detection is exact; naming the survivor is a research decision.
# --------------------------------------------------------------------------- #

def cache_of(rows, roots_by_name):
    """A cache holding the given roots and projects, keyed by clone URL.

    resolve() reads the cache alone, never a manifest, so the fixture has to
    populate `projects` as a real scan would.
    """
    by_name = {r["name"]: r["url"] for r in rows}
    return {"roots": {by_name[n]: rs for n, rs in roots_by_name.items()},
            "projects": {r["url"]: {k: r[k] for k in
                                    ("name", "stratum", "size_class")}
                         for r in rows},
            "errors": {}, "excluded": {}, "clusters": {}}


def test_resolve_excludes_every_member_except_the_named_upstream(monkeypatch):
    monkeypatch.setattr(sh, "UPSTREAM", {"r1": "torvalds/linux"})
    rows = [row("torvalds__linux"), row("microsoft__WSL2-Linux-Kernel")]
    cache = cache_of(rows, {"torvalds__linux": ["r1"],
                            "microsoft__WSL2-Linux-Kernel": ["r1"]})
    excluded, annotations, unresolved = sh.resolve(cache)
    assert excluded == {"microsoft/wsl2-linux-kernel": "shared-history=torvalds/linux"}
    assert unresolved == []
    # The survivor is annotated too: it shares the history, it just keeps its place.
    assert set(annotations) == {"torvalds/linux", "microsoft/wsl2-linux-kernel"}


def test_resolve_leaves_a_cluster_of_one_alone(monkeypatch):
    monkeypatch.setattr(sh, "UPSTREAM", {"r1": "torvalds/linux"})
    rows = [row("torvalds__linux")]
    cache = cache_of(rows, {"torvalds__linux": ["r1"]})
    assert sh.resolve(cache) == ({}, {}, [])


def test_resolve_reports_a_cluster_with_no_verdict(monkeypatch):
    """Resolving by guesswork would put the wrong project in the corpus and the
    wrong stratum with it."""
    monkeypatch.setattr(sh, "UPSTREAM", {})
    monkeypatch.setattr(sh, "DISTINCT_HISTORY", {})
    rows = [row("mysql__mysql-server"), row("mariadb__server")]
    cache = cache_of(rows, {"mysql__mysql-server": ["r1"], "mariadb__server": ["r1"]})
    excluded, annotations, unresolved = sh.resolve(cache)
    assert excluded == {}
    assert len(unresolved) == 1 and len(unresolved[0]) == 2
    # Annotated even without a verdict: the relationship is a measurement.
    assert len(annotations) == 2


def test_resolve_reports_a_cluster_that_names_two_upstreams(monkeypatch):
    """Two names for one cluster is a contradiction in the table, not a verdict
    to apply. It must be reported, not resolved by dict order."""
    monkeypatch.setattr(sh, "UPSTREAM", {"r1": "a/one", "r2": "b/two"})
    monkeypatch.setattr(sh, "DISTINCT_HISTORY", {})
    rows = [row("a__one"), row("b__two")]
    cache = cache_of(rows, {"a__one": ["r1"], "b__two": ["r1", "r2"]})
    excluded, _annotations, unresolved = sh.resolve(cache)
    assert excluded == {} and len(unresolved) == 1


def test_resolve_matches_the_upstream_whatever_the_case(monkeypatch):
    """The manifest lowercases; UPSTREAM is written the way GitHub spells it."""
    monkeypatch.setattr(sh, "UPSTREAM", {"r1": "FreeBSD/FreeBSD-src"})
    rows = [row("freebsd__freebsd-src"), row("freebsd__freebsd")]
    cache = cache_of(rows, {"freebsd__freebsd-src": ["r1"], "freebsd__freebsd": ["r1"]})
    excluded, _a, _u = sh.resolve(cache)
    assert excluded == {"freebsd/freebsd": "shared-history=FreeBSD/FreeBSD-src"}


def test_the_upstream_table_names_no_project_it_also_excludes():
    """A self-inconsistent table would exclude the project it protects."""
    for root, name in sh.UPSTREAM.items():
        assert name.count("/") == 1, f"{root} names {name!r}, not owner/repo"


def test_the_two_tables_never_claim_the_same_cluster():
    """A cluster cannot both keep one member and keep them all."""
    assert not set(sh.UPSTREAM) & set(sh.DISTINCT_HISTORY)


# --------------------------------------------------------------------------- #
# keeping a cluster whole. Author decision 2026-09-14: a derivative with its own
# governance is a project, not a duplicate, so record the relationship instead.
# --------------------------------------------------------------------------- #

def test_a_distinct_history_cluster_excludes_nobody(monkeypatch):
    monkeypatch.setattr(sh, "UPSTREAM", {})
    monkeypatch.setattr(sh, "DISTINCT_HISTORY", {"r1": "separate governance"})
    rows = [row("mariadb__server"), row("percona__percona-xtrabackup")]
    cache = cache_of(rows, {"mariadb__server": ["r1", "r2"],
                            "percona__percona-xtrabackup": ["r1"]})
    excluded, annotations, unresolved = sh.resolve(cache)
    assert excluded == {} and unresolved == []
    assert len(annotations) == 2


def test_a_kept_cluster_still_records_who_it_shares_with(monkeypatch):
    """This is the whole point of the decision: the overlap must be visible."""
    monkeypatch.setattr(sh, "UPSTREAM", {})
    monkeypatch.setattr(sh, "DISTINCT_HISTORY", {"r1": "separate governance"})
    rows = [row("mariadb__server"), row("percona__percona-xtrabackup"),
            row("tencent__tendbcluster-tendb")]
    cache = cache_of(rows, dict.fromkeys(
        ["mariadb__server", "percona__percona-xtrabackup",
         "tencent__tendbcluster-tendb"], ["r1"]))
    _e, annotations, _u = sh.resolve(cache)
    assert annotations["mariadb/server"]["shared_with"] == [
        "percona/percona-xtrabackup", "tencent/tendbcluster-tendb"]
    assert annotations["mariadb/server"]["cluster"] == "r1"


def test_the_cluster_id_is_the_smallest_root_in_the_cluster(monkeypatch):
    """Content-addressed, so every member reports the same id and no registry
    is needed."""
    monkeypatch.setattr(sh, "UPSTREAM", {})
    monkeypatch.setattr(sh, "DISTINCT_HISTORY", {"aaa": "kept"})
    rows = [row("a__one"), row("b__two")]
    cache = cache_of(rows, {"a__one": ["ccc", "aaa"], "b__two": ["aaa", "bbb"]})
    _e, annotations, _u = sh.resolve(cache)
    assert {a["cluster"] for a in annotations.values()} == {"aaa"}


@pytest.mark.parametrize("roots,expected", [
    (["b", "a", "c"], "a"),
    (["only"], "only"),
    (LINUX_ROOTS, "1da177e4c3f41524e886b7f1b8a0c1fc7321cac2"),
])
def test_cluster_id_picks_the_smallest_root(roots, expected):
    assert sh.cluster_id(roots) == expected


def test_the_real_linux_cluster_id_is_the_2_6_12_import():
    """The id doubles as documentation when it is a commit a reader recognises."""
    assert sh.cluster_id(LINUX_ROOTS) in sh.UPSTREAM


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


def test_scan_command_writes_the_verdict_into_the_cache(monkeypatch, sandbox, capsys):
    monkeypatch.setattr(sh, "UPSTREAM", {"r1": "torvalds/linux"})
    monkeypatch.setattr(sh, "root_commits", lambda url, name: ["r1"])
    rows = [row("torvalds__linux"), row("microsoft__wsl2-linux-kernel")]
    import argparse as _a
    args = _a.Namespace(manifest=write_manifest(sandbox, rows), force=False)
    assert sh.cmd_scan(args) == 0
    assert json.loads((sandbox / "roots.json").read_text())["excluded"] == {
        "microsoft/wsl2-linux-kernel": "shared-history=torvalds/linux"}


def test_scan_command_exits_non_zero_on_an_unnamed_cluster(monkeypatch, sandbox, capsys):
    monkeypatch.setattr(sh, "UPSTREAM", {})
    monkeypatch.setattr(sh, "DISTINCT_HISTORY", {})
    monkeypatch.setattr(sh, "root_commits", lambda url, name: ["r1"])
    rows = [row("mysql__mysql-server"), row("mariadb__server")]
    import argparse as _a
    args = _a.Namespace(manifest=write_manifest(sandbox, rows), force=False)
    assert sh.cmd_scan(args) == 1
    out = capsys.readouterr().out
    assert "no verdict" in out
    # The report has to name the members, or the author cannot decide.
    assert "mariadb__server" in out and "mysql__mysql-server" in out


def test_a_verdict_survives_the_project_leaving_the_manifest(monkeypatch, sandbox):
    """The bug this guards against, measured on the real frame on 2026-09-14:

    exclude a kernel copy -> the draw refills the cell with the next kernel copy
    -> the excluded one leaves the manifest -> its exclusion disappears -> the
    draw takes it back. The corpus oscillated between intel/mOS and SUSE/kernel.

    Roots are cached for ever, so cluster membership is permanent knowledge.
    Resolution therefore runs over the cache, not over one manifest.
    """
    monkeypatch.setattr(sh, "UPSTREAM", {"r1": "torvalds/linux"})
    monkeypatch.setattr(sh, "root_commits", lambda url, name: ["r1"])
    import argparse as _a

    first = [row("torvalds__linux"), row("intel__mos")]
    sh.cmd_scan(_a.Namespace(manifest=write_manifest(sandbox, first), force=False))
    assert "intel/mos" in json.loads((sandbox / "roots.json").read_text())["excluded"]

    # The draw drops intel/mos and picks suse/kernel instead.
    second = [row("torvalds__linux"), row("suse__kernel")]
    sh.cmd_scan(_a.Namespace(manifest=write_manifest(sandbox, second), force=False))
    excluded = json.loads((sandbox / "roots.json").read_text())["excluded"]
    assert excluded == {"intel/mos": "shared-history=torvalds/linux",
                        "suse/kernel": "shared-history=torvalds/linux"}


@pytest.mark.parametrize("url,expected", [
    ("https://github.com/Intel/mOS.git", "intel__mos"),
    ("https://github.com/torvalds/linux", "torvalds__linux"),
    ("https://github.com/SUSE/kernel.git/", "suse__kernel"),
    ("nonsense", "nonsense"),
])
def test_name_from_url_rebuilds_ctps_project_name(url, expected):
    assert sh.name_from_url(url) == expected


def test_a_verdict_survives_a_cache_written_before_names_were_recorded(monkeypatch, sandbox):
    """Roots cached by an older version carry no name. Skipping them would
    narrow resolution to the latest manifest again, which is the oscillation."""
    monkeypatch.setattr(sh, "UPSTREAM", {"r1": "torvalds/linux"})
    monkeypatch.setattr(sh, "root_commits", lambda url, name: ["r1"])
    sh.save_cache({"roots": {"https://github.com/intel/mOS.git": ["r1"]},
                   "errors": {}, "excluded": {}, "clusters": {}, "projects": {}})
    import argparse as _a
    rows = [row("torvalds__linux")]
    sh.cmd_scan(_a.Namespace(manifest=write_manifest(sandbox, rows), force=False))
    excluded = json.loads((sandbox / "roots.json").read_text())["excluded"]
    assert excluded == {"intel/mos": "shared-history=torvalds/linux"}


def test_the_scan_records_a_kept_cluster_in_the_cache(monkeypatch, sandbox):
    """select_corpus reads `clusters`, so a kept cluster must land there even
    though nothing is excluded."""
    monkeypatch.setattr(sh, "UPSTREAM", {})
    monkeypatch.setattr(sh, "DISTINCT_HISTORY", {"r1": "separate governance"})
    monkeypatch.setattr(sh, "root_commits", lambda url, name: ["r1"])
    rows = [row("mariadb__server"), row("percona__percona-xtrabackup")]
    import argparse as _a
    assert sh.cmd_scan(_a.Namespace(manifest=write_manifest(sandbox, rows),
                                    force=False)) == 0
    cache = json.loads((sandbox / "roots.json").read_text())
    assert cache["excluded"] == {}
    assert cache["clusters"]["mariadb/server"]["shared_with"] == [
        "percona/percona-xtrabackup"]


def test_scan_command_refuses_a_missing_manifest(sandbox, capsys):
    import argparse as _a
    assert sh.cmd_scan(_a.Namespace(manifest=str(sandbox / "nope.tsv"),
                                    force=False)) == 1
    assert "run `select_corpus.py sample` first" in capsys.readouterr().out


def test_report_command_needs_no_network(monkeypatch, sandbox):
    monkeypatch.setattr(sh, "root_commits",
                        lambda url, name: pytest.fail("report must not clone"))
    monkeypatch.setattr(sh, "UPSTREAM", {"r1": "torvalds/linux"})
    rows = [row("torvalds__linux"), row("microsoft__wsl2-linux-kernel")]
    sh.save_cache({"roots": {r["url"]: ["r1"] for r in rows},
                   "errors": {}, "excluded": {}})
    import argparse as _a
    assert sh.cmd_report(_a.Namespace(manifest=write_manifest(sandbox, rows))) == 0


def test_report_command_refuses_a_missing_manifest(sandbox, capsys):
    import argparse as _a
    assert sh.cmd_report(_a.Namespace(manifest=str(sandbox / "nope.tsv"))) == 1


def test_the_report_names_a_project_it_could_not_read(monkeypatch, sandbox, capsys):
    monkeypatch.setattr(sh, "UPSTREAM", {})
    rows = [row("slow__one")]
    sh.save_cache({"roots": {}, "errors": {rows[0]["url"]: "timed out"},
                   "excluded": {}})
    import argparse as _a
    sh.cmd_report(_a.Namespace(manifest=write_manifest(sandbox, rows)))
    assert "could not be read" in capsys.readouterr().out


def test_main_dispatches_to_the_named_subcommand(monkeypatch, sandbox):
    monkeypatch.setattr(sh.sys, "argv",
                        ["shared_history.py", "report", "--manifest",
                         str(sandbox / "nope.tsv")])
    assert sh.main() == 1
