#!/usr/bin/env python3
"""shared_history — find projects that carry another project's history.

    ./shared_history.py scan                    # scan manifest.sample.tsv
    ./shared_history.py scan --manifest M        # scan another manifest
    ./shared_history.py scan --force             # re-read cached roots
    ./shared_history.py report                   # read the cache, no network

Why. GitHub marks a repository as a fork only when it was made with the fork
button. A tree pushed as an independent repository carries the upstream's whole
history and is not marked: among 3,840 eligible rows, `fork=True` counts **zero**,
and the L class still holds four Linux kernels, eight MySQL descendants and two
copies of FreeBSD. A copy is not an independent observation. Its tokens are the
upstream's tokens, and the copies sit in different strata, so a cross-stratum
comparison would partly compare a project with itself.

Naming the copies by hand does not converge. Two kernel trees were excluded by
name on 2026-09-14, and the next draw replaced them with `intel/mOS` (a kernel)
and `TexasInstruments/mesa` (a Mesa tree), which is 1,366,279 commits of copied
history back in the sample after one iteration.

How. Two repositories share history when they share a root commit — a commit
with no parent. The test is exact and needs no name list. It needs the commit
graph but not the content, so a `--filter=tree:0 --bare --single-branch` clone
answers it. Measured: Linux 60 s and 819 MB, jq 1 s and 856 KB. The clone is
deleted as soon as the roots are read.

A repository can hold several roots. Linux holds four, of which
`1da177e4c3f4` is the 2.6.12-rc2 import, and `freebsd/freebsd-src` holds 215
because its CVS conversion imported many disconnected trees. Two repositories
are therefore in one cluster when their root SETS intersect, not when they are
equal, and the grouping is a union-find over roots so that A-B and B-C put A
and C together. Measured on the real clusters:

    freebsd/freebsd     313,060 commits, 215 roots  \\ identical root sets
    freebsd/freebsd-src 313,060 commits, 215 roots  /  same repository
    mysql/mysql-server  191,048 commits,   3 roots  \\ share 2 of 3
    MariaDB/server      206,015 commits,  23 roots  /

Does the test over-merge? It was checked against the case most likely to fail.
FreeBSD, NetBSD and OpenBSD descend from the same 1990s code, and they share
**zero** roots with each other, because each was converted to git separately.
Identical root SHAs mean identical commit objects, which is the granularity
token duplication needs.

What this file does NOT decide. Detection is exact; choosing which member
represents a cluster is a research decision. Commit count cannot decide it:
`SUSE/kernel` holds 1,566,626 commits against `torvalds/linux`'s 1,482,779
because it adds SUSE's patches, and `raspberrypi/linux` holds fewer because it
lags. So UPSTREAM names one representative per cluster, one line each, and a
cluster with no entry makes the scan exit non-zero and name it. That is one
decision per cluster, not one per copy.

Output. The cache is the only thing this script writes. `select_corpus.py` reads
two maps from it, so both land in `candidates.csv` and therefore in the PRISMA
flow. Nothing here edits the frame directly.

    roots      every project ever scanned, keyed by clone URL
    projects   the name, stratum and size class of each, so a later scan can
               resolve without being handed the same manifest again
    excluded   one member of the cluster survives; the rest carry a reason
    clusters   EVERY member, with its cluster id and the projects it shares a
               history with, whether or not it was excluded

Resolution runs over the whole cache, never over one manifest. Scoping it to a
manifest oscillates: exclude a kernel copy, the draw refills the cell with the
next kernel copy, the first copy leaves the manifest, its exclusion vanishes, and
the draw takes it back. Measured on the real frame before the fix.

The second map exists because a shared history is a property of the corpus, not
only a reason to drop something. A reader who wants one project per history can
filter on the cluster id; a reader who keeps them all can state the overlap.

    ./shared_history.py scan          # 1. find the clusters
    ./select_corpus.py emit           # 2. write the exclusions into the record
    ./select_corpus.py sample         # 3. re-draw; freed slots refill per cell

Repeat until the draw stops changing. Each pass scans only what the last draw
added, because roots are cached by clone URL.

Scope. `--single-branch` follows HEAD, so a root reachable only from another
branch is not seen. The pipeline's dataset covers the default branch's history,
so that is the right basis. Stated, not hidden.

Stdlib only.
"""
from __future__ import annotations

import argparse
import configparser
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

CORPUS = Path(__file__).resolve().parent
CACHE_DIR = CORPUS / ".corpus-cache"
CACHE = CACHE_DIR / "roots.json"
SCRATCH = CACHE_DIR / "roots-scratch"
DEFAULT_MANIFEST = CORPUS / "manifest.sample.tsv"

# A commits-only clone of Linux took 60 s, FreeBSD 19 s, jq 1 s. But
# raspberrypi/linux ran 1,122 s and still failed, so the ceiling is not a
# multiple of the fastest case. 45 minutes distinguishes a slow transfer from a
# hung one, and a project that trips it is recorded, not excluded: a repository
# we could not read is unknown, not innocent.
CLONE_TIMEOUT_S = 2_700

# One representative per cluster, keyed by any root commit in that cluster.
#
# This is the only manual decision in this file, and it is a research decision:
# the algorithm finds the cluster, a person names the project the cluster is
# about. Cite the reason in the comment beside each entry.
UPSTREAM: dict[str, str] = {
    # Linux: 1da177e4 is the 2.6.12-rc2 import that starts the git history.
    # torvalds/linux is the tree the others copied, and it is the tree the
    # kernel results in the paper already describe.
    "1da177e4c3f41524e886b7f1b8a0c1fc7321cac2": "torvalds/linux",

    # FreeBSD: freebsd/freebsd and freebsd/freebsd-src are one repository under
    # two names -- identical 215-root sets, 313,060 commits each. The project
    # renamed the canonical repository to freebsd-src, so that name survives.
    # This entry is bookkeeping, not a research judgement.
    "eb3b1302382b1d0cbe37eeebabfcdd546aa2fc4e": "freebsd/freebsd-src",
}

# Clusters where every member stays in the corpus, and the shared history is
# recorded rather than resolved. Author decision, 2026-09-14.
#
# The reasoning is that a derivative with its own governance is a project, not a
# duplicate: MariaDB has had separate governance since 2009, and a vendor JDK is
# a shipped product. Dropping them would answer a question the dataset should
# instead let its reader ask. The cost is real -- the same tokens appear under
# more than one `repo_name` -- so the annotation has to reach the data, not just
# this file. See EXECUTION-STATE.md D26.
#
# Every cluster listed here sits inside ONE stratum, so the duplication costs
# compute and inflates a within-stratum count. It does not contaminate a
# comparison ACROSS strata, which is what the excluded kernel copies did.
DISTINCT_HISTORY: dict[str, str] = {
    # MySQL descendants, all company-owned: MariaDB/server,
    # percona/percona-xtrabackup, Tencent/TenDBCluster-Tdbctl,
    # Tencent/TenDBCluster-TenDB. The upstream, mysql/mysql-server, is not in
    # the sample, so no member is the cluster's obvious representative.
    "0175860925a8dc08e831cf54220cc0e7d7387213":
        "MySQL descendants with separate governance; upstream absent from the sample",

    # OpenJDK descendants, both company-owned: SAP/SapMachine and
    # Tencent/TencentKona-21. Upstream openjdk/jdk is not in the sample.
    "29e77aaf0b4ec026f49a6027f045b2429e7e3177":
        "vendor JDK builds, each a shipped product; upstream absent from the sample",

    # rust-lang/rust and rust-lang/rust-analyzer, both foundation, same owner.
    # This is a SUBTREE MERGE, not a fork: rust carries rust-analyzer under
    # src/tools/rust-analyzer, so it holds those tokens as well as its own. The
    # mechanism differs from a fork and the annotation should say so.
    "37226273a7a5b2119daaab06d253f93b6813b881":
        "subtree merge: rust-lang/rust contains rust-analyzer under src/tools",
}


def name_from_url(url: str) -> str:
    """`https://github.com/Owner/Repo.git` -> `owner__repo`, ctp's project name.

    Needed to backfill a project whose roots were cached before the cache
    recorded names. The roots are the expensive part and they are already there,
    so the entry is reconstructed instead of re-cloned. Without the backfill,
    resolution silently narrows to the latest manifest again, which is the
    oscillation this module exists to avoid.
    """
    parts = url.rstrip("/").removesuffix(".git").split("/")
    return f"{parts[-2]}__{parts[-1]}".lower() if len(parts) >= 2 else url.lower()


def cluster_id(roots: list[str]) -> str:
    """A cluster's identifier: the lexicographically smallest root it holds.

    Content-addressed, so it needs no registry and it is the same for every
    member. It is a snapshot value, like `label_date`: the minimum is taken over
    the roots of the members that were scanned, so a later scan that adds a
    member holding a smaller root would lower it. Recompute on a re-scan, and
    read the value in `candidates.csv` as belonging to that frame.
    """
    return sorted(roots)[0]


def say(*a) -> None:
    print(f"[{time.strftime('%H:%M:%S')}]", *a, flush=True)


def load_cache() -> dict:
    """The cache holds roots per clone URL plus the resolved exclusions.

    A missing or unreadable cache is an empty cache, never an error: the scan
    rebuilds it, and refusing to start because a derived file is corrupt would
    block the corpus on something that costs one command to regenerate.
    """
    empty = {"roots": {}, "errors": {}, "excluded": {}, "clusters": {},
             "projects": {}}
    try:
        data = json.loads(CACHE.read_text())
    except (OSError, ValueError):
        return empty
    for key in empty:
        data.setdefault(key, {})
    return data


def save_cache(data: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    tmp = CACHE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, indent=1, sort_keys=True))
    tmp.replace(CACHE)


def read_manifest(path: Path) -> list[dict]:
    """Parse ctp's 5-field manifest. A short row is an error, not a warning.

    ctp.py hard-requires exactly five fields, so accepting a four-field row here
    would hand the runner something it refuses later, after the scan reported
    success.
    """
    rows = []
    for n, line in enumerate(path.read_text().splitlines(), 1):
        if not line.strip() or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) != 5:
            sys.exit(f"{path.name}:{n}: expected 5 tab-separated fields, got {len(parts)}")
        rows.append(dict(zip(("name", "url", "stratum", "mask", "size_class"), parts)))
    return rows


def root_commits(url: str, name: str) -> list[str]:
    """Clone the commit graph only, read every parentless commit, delete it.

    Raises RuntimeError with the git output on failure. The caller records that
    against the project rather than stopping: one unreachable repository must not
    hide the clusters among the others.
    """
    SCRATCH.mkdir(parents=True, exist_ok=True)
    # Named after the project so a leftover directory says which scan died.
    dst = SCRATCH / f"{name}.git"
    if dst.exists():
        shutil.rmtree(dst)
    try:
        clone = subprocess.run(
            ["git", "clone", "--filter=tree:0", "--bare", "--single-branch",
             "--quiet", url, str(dst)],
            capture_output=True, text=True, timeout=CLONE_TIMEOUT_S)
        if clone.returncode != 0:
            raise RuntimeError((clone.stderr or clone.stdout).strip()[:300])
        rev = subprocess.run(
            ["git", "--git-dir", str(dst), "rev-list", "--max-parents=0", "HEAD"],
            capture_output=True, text=True, timeout=CLONE_TIMEOUT_S)
        if rev.returncode != 0:
            raise RuntimeError((rev.stderr or rev.stdout).strip()[:300])
        return sorted(set(rev.stdout.split()))
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"timed out after {CLONE_TIMEOUT_S}s")
    finally:
        shutil.rmtree(dst, ignore_errors=True)


def scan(rows: list[dict], cache: dict, force: bool = False) -> dict:
    """Fill the cache with the roots of every project that has none yet."""
    todo = [r for r in rows if force or r["url"] not in cache["roots"]]
    say(f"{len(rows)} projects, {len(todo)} to read "
        f"({len(rows) - len(todo)} already cached)")
    for i, r in enumerate(todo, 1):
        try:
            roots = root_commits(r["url"], r["name"])
        except RuntimeError as e:
            cache["errors"][r["url"]] = str(e)
            cache["roots"].pop(r["url"], None)
            say(f"  {i}/{len(todo)} {r['name']} — FAILED: {e}")
            continue
        cache["roots"][r["url"]] = roots
        cache["errors"].pop(r["url"], None)
        say(f"  {i}/{len(todo)} {r['name']} — {len(roots)} root(s)")
        # Written every time, so an interrupted scan keeps what it read.
        save_cache(cache)
    return cache


def group_by_root(roots_by_url: dict[str, list[str]]) -> list[list[str]]:
    """Union-find over root commits. Returns clusters of clone URLs.

    Union-find, not a plain group-by, because sharing is transitive through a
    root: a tree with two roots can tie together two projects that share neither
    root with each other.
    """
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for url, roots in roots_by_url.items():
        if not roots:
            continue
        find(url)
        for root in roots:
            union(url, f"root:{root}")

    groups: dict[str, list[str]] = {}
    for url in roots_by_url:
        if roots_by_url[url]:
            groups.setdefault(find(url), []).append(url)
    return [sorted(g) for g in groups.values()]


def resolve(cache: dict) -> tuple[dict[str, str], dict[str, dict], list[list[str]]]:
    """Return (exclusions, annotations, clusters with no verdict).

    Resolves over **everything the cache knows**, not over one manifest. That
    distinction is load-bearing. A verdict scoped to the current manifest
    oscillates: exclude a kernel copy, the draw refills the cell with the next
    kernel copy, the first one leaves the manifest, its exclusion disappears, and
    the draw takes it back. Roots are cached for ever, so cluster membership is
    permanent knowledge and the manifest is only the work queue.

    Every member of every cluster is annotated, whether or not it is excluded.
    The shared history is a property of the corpus, so it belongs in the record
    even where the project stays: a reader who wants one project per history can
    then filter, and a reader who wants all of them can state the overlap.

    Only UPSTREAM turns a cluster into an exclusion. DISTINCT_HISTORY keeps every
    member and relies on the annotation instead. A cluster in neither is reported
    and left alone: resolving it by guesswork would put the wrong project in the
    corpus, and the wrong stratum with it.
    """
    by_url = cache["projects"]
    excluded: dict[str, str] = {}
    annotations: dict[str, dict] = {}
    unresolved: list[list[str]] = []

    for cluster in group_by_root({u: cache["roots"].get(u, [])
                                  for u in by_url if u in cache["roots"]}):
        if len(cluster) < 2:
            continue
        roots = sorted({root for url in cluster
                        for root in cache["roots"].get(url, [])})
        cid = cluster_id(roots)
        # ctp's project name is owner__repo; the frame keys on owner/repo.
        names = {url: by_url[url]["name"].replace("__", "/", 1).lower()
                 for url in cluster}
        for url in cluster:
            annotations[names[url]] = {
                "cluster": cid,
                "shared_with": sorted(n for u, n in names.items() if u != url),
            }

        if cid in DISTINCT_HISTORY:
            continue

        named = {UPSTREAM[root] for root in roots if root in UPSTREAM}
        if len(named) != 1:
            unresolved.append(cluster)
            continue
        upstream = named.pop()
        for url in cluster:
            if names[url] != upstream.lower():
                excluded[names[url]] = f"shared-history={upstream}"
    return excluded, annotations, unresolved


def cmd_scan(args: argparse.Namespace) -> int:
    manifest = Path(args.manifest)
    if not manifest.exists():
        say(f"no {manifest} — run `select_corpus.py sample` first")
        return 1
    rows = read_manifest(manifest)
    cache = scan(rows, load_cache(), force=args.force)
    return finish(rows, cache)


def cmd_report(args: argparse.Namespace) -> int:
    manifest = Path(args.manifest)
    if not manifest.exists():
        say(f"no {manifest}")
        return 1
    return finish(read_manifest(manifest), load_cache())


def finish(rows: list[dict], cache: dict) -> int:
    """Record what this manifest taught the cache, then resolve over all of it."""
    for r in rows:
        cache["projects"][r["url"]] = {k: r[k] for k in
                                       ("name", "stratum", "size_class")}
    # Anything with cached roots but no recorded name predates the projects map.
    # Its verdict must still hold, so reconstruct the entry from the URL.
    for url in cache["roots"]:
        cache["projects"].setdefault(url, {"name": name_from_url(url),
                                           "stratum": "?", "size_class": "?"})
    excluded, annotations, unresolved = resolve(cache)
    cache["excluded"] = excluded
    cache["clusters"] = annotations
    save_cache(cache)

    by_name = {p["name"].replace("__", "/", 1).lower(): p
               for p in cache["projects"].values()}

    def describe(name: str) -> str:
        r = by_name.get(name, {})
        return f"{name:44s} {r.get('stratum', '?'):14s} {r.get('size_class', '?')}"

    kept = {n: a for n, a in annotations.items() if n not in excluded}
    say(f"--- {len(annotations)} projects share history: "
        f"{len(kept)} kept and flagged, {len(excluded)} excluded ---")
    for cid, note in sorted(DISTINCT_HISTORY.items()):
        members = sorted(n for n, a in kept.items() if a["cluster"] == cid)
        if not members:
            continue
        say(f"  cluster {cid[:12]} — {note}")
        for name in members:
            say(f"      {describe(name)}")
    for name, reason in sorted(excluded.items()):
        say(f"  {describe(name)}  {reason}")

    failed = {u: e for u, e in cache["errors"].items() if u in {r["url"] for r in rows}}
    if failed:
        say(f"--- {len(failed)} projects could not be read ---")
        for url, err in sorted(failed.items()):
            say(f"  {url} — {err}")

    if unresolved:
        by_url = cache["projects"]
        say(f"--- {len(unresolved)} clusters have no verdict ---")
        for cluster in unresolved:
            roots = sorted({root for url in cluster
                            for root in cache["roots"].get(url, [])})
            say(f"  cluster {cluster_id(roots)} ({len(roots)} roots)")
            for url in cluster:
                r = by_url[url]
                say(f"      {r['name']:44s} {r['stratum']:14s} {r['size_class']}")
        say("Each cluster needs one line in shared_history.py: an UPSTREAM entry "
            "to keep one member, or a DISTINCT_HISTORY entry to keep them all "
            "and rely on the annotation.")
        say("Naming a representative is a research decision. Commit count cannot "
            "decide it, because a copy may hold more commits than the upstream "
            "or fewer.")
        return 1

    say("next: ./select_corpus.py emit && ./select_corpus.py sample")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("scan", help="read the roots of every project in a manifest")
    s.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    s.add_argument("--force", action="store_true",
                   help="re-read roots that are already cached")
    s.set_defaults(fn=cmd_scan)

    r = sub.add_parser("report", help="resolve clusters from the cache, no network")
    r.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    r.set_defaults(fn=cmd_report)

    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
