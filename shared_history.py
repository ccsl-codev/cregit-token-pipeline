#!/usr/bin/env python3
"""shared_history — find projects that carry another project's history.

    ./shared_history.py scan            # 1. find the clusters (one clone each)
    ./shared_history.py ancestry        # 2. measure who holds whose history
    ./shared_history.py report          # read the cache, no network
    ./select_corpus.py emit             # 3. write the flags into the record
    ./select_corpus.py sample           # 4. re-draw

Why. GitHub marks a repository as a fork only when it was made with the fork
button. A tree pushed as an independent repository carries the upstream's whole
history and is not marked: among the eligible rows, `fork=True` counts **zero**,
and the sample still draws five Linux kernels, four MySQL descendants and two
vendor JDKs. Those tokens appear more than once, under more than one
`repo_name`, and a reader who does not know cannot correct for it.

**Nothing is excluded.** Author decision, 2026-09-14: a derivative with its own
governance is a project, not a duplicate, so the relationship is recorded and the
record says which project came first. A consumer who wants one project per
history filters on `history_cluster`.

Naming copies by hand was tried first and abandoned. Two kernel trees were
excluded by name, and the next draw replaced them with `intel/mOS` (a kernel) and
`TexasInstruments/mesa` (a Mesa tree): 1,366,279 commits of copied history back
in the sample after one iteration.

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

Which came first. `ancestry` compares the commit SETS of the members of a
cluster. If B holds every commit of A, then B holds A's whole history and A came
first. That is exact, and it is reported per PAIR, never for the cluster as a
whole -- see `direction` for the measurement that proved a cluster-wide vote
wrong. The graph cannot order every pair, and `diverged` says so rather than
guessing.

Output. The cache is the only thing this script writes. `select_corpus.py` reads
`clusters` from it, so the flags land in `candidates.csv` and therefore in the
PRISMA flow. Nothing here edits the frame directly.

    roots      every project ever scanned, keyed by clone URL
    projects   the name, stratum and size class of each, so a later run can
               resolve without being handed the same manifest again
    ancestry   the pairwise evidence per cluster: shared, unique and lag counts
    clusters   every member, with its cluster id, the projects it shares a
               history with, and which of them it came after or before

Resolution runs over the whole cache, never over one manifest. Scoping it to a
manifest oscillated: a verdict on a project vanished when the draw dropped that
project, and the draw took it straight back. Measured on the real frame.

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

# Nothing is excluded for sharing a history. Author decision, 2026-09-14:
# flagging is enough, and the flag must say which project came first.
#
# The reasoning is that a derivative with its own governance is a project, not a
# duplicate: MariaDB has had separate governance since 2009, and a vendor JDK is
# a shipped product. Dropping one would answer a question the dataset should let
# its reader ask instead. The cost is real -- the same tokens appear under more
# than one `repo_name` -- so the annotation has to reach the data, and a consumer
# who wants one project per history filters on `history_cluster`.
#
# A human-readable note per cluster, keyed by cluster id. Documentation only: no
# entry here changes what enters the corpus, and a cluster with no note is still
# annotated. Nothing is excluded for sharing a history; it is recorded as data.
CLUSTER_NOTES: dict[str, str] = {
    "1da177e4c3f41524e886b7f1b8a0c1fc7321cac2":
        "Linux kernel trees; 1da177e4 is the 2.6.12-rc2 import",
    "eb3b1302382b1d0cbe37eeebabfcdd546aa2fc4e":
        "freebsd/freebsd and freebsd/freebsd-src are one repository under two names",
    "0175860925a8dc08e831cf54220cc0e7d7387213":
        "MySQL descendants; upstream mysql/mysql-server absent from the sample",
    "29e77aaf0b4ec026f49a6027f045b2429e7e3177":
        "vendor JDK builds; upstream openjdk/jdk absent from the sample",
    "37226273a7a5b2119daaab06d253f93b6813b881":
        "subtree merge: rust-lang/rust contains rust-analyzer under src/tools",
    "8ddf82cf70dc6f951ab477f325dee0efde3ec589":
        "Zephyr and TexasInstruments/simplelink-zephyr, a vendor fork",
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
    empty = {"roots": {}, "errors": {}, "clusters": {}, "projects": {},
             "ancestry": {}}
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


def head_commits(url: str, name: str) -> set[int]:
    """Every commit reachable from HEAD, as the 64-bit prefix of its SHA.

    A prefix, not the whole hash, so a kernel-sized history costs tens of
    megabytes instead of hundreds. Across 1.5 M commits the chance of a 64-bit
    collision is about 1.5M**2 / 2**65, which is far below the chance of a disk
    error, and a single collision would move one commit between the shared and
    unique counts rather than change a direction.
    """
    SCRATCH.mkdir(parents=True, exist_ok=True)
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
        rev = subprocess.run(["git", "--git-dir", str(dst), "rev-list", "HEAD"],
                             capture_output=True, text=True,
                             timeout=CLONE_TIMEOUT_S)
        if rev.returncode != 0:
            raise RuntimeError((rev.stderr or rev.stdout).strip()[:300])
        return {int(line[:16], 16) for line in rev.stdout.split()}
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"timed out after {CLONE_TIMEOUT_S}s")
    finally:
        shutil.rmtree(dst, ignore_errors=True)


# A tree that mirrors another still lags it. SUSE/kernel holds 1,482,841 of
# torvalds/linux's 1,482,901 commits and misses 60 -- the commits Linux made after
# SUSE's last merge. Strict containment would call that a divergence and lose a
# fact the numbers make obvious, so "inside" allows the lagging tail: A is inside
# B when the commits of A that B lacks are under this fraction of A.
LAG_TOLERANCE = 0.001


def repo_created(name: str) -> str:
    """The repository's creation date on GitHub, or "" when it cannot be read.

    Origin cannot come from the commit graph -- see `direction` -- so it comes
    from here. One call per cluster member, about fifteen calls, not one per
    candidate.

    **What this date is.** When the repository appeared on GitHub, not when the
    project began. `torvalds/linux` is a 2011 mirror of a history that starts in
    2005, for work that began in 1991. Within a cluster the comparison is still
    informative, because a fork's repository is created after the repository it
    forked from. The absolute date is not a birthday, and the paper must say so.
    """
    slug = name if "/" in name else name.replace("__", "/", 1)
    try:
        out = subprocess.run(
            ["gh", "repo", "view", slug, "--json", "createdAt",
             "-q", ".createdAt"],
            capture_output=True, text=True, timeout=120)
    except (OSError, subprocess.TimeoutExpired):
        return ""
    return out.stdout.strip() if out.returncode == 0 else ""


def direction(commits: dict[str, set[int]],
              tolerance: float = LAG_TOLERANCE) -> list[dict]:
    """Return the pairwise evidence: for each pair, whose history holds whose.

    **Pairwise, never cluster-wide.** An earlier version also picked the member
    that came first for the cluster as a whole, by counting how many others held
    its history. Measured on the MySQL cluster, that answered
    `tencent/tendbcluster-tendb`, which is nobody's ancestor: it is a near-mirror
    of `tencent/tendbcluster-tdbctl` (112 unique commits of 134,827), so it won
    the only containment in a cluster where every other pair had diverged. A
    near-mirror pair hijacks any cluster-wide vote, so there is no vote.

    **What the commit graph proves: inclusion, not origin.** A cluster shares
    root commits, and a shared root is the same object in both repositories, so
    no date on it orders the members. Containment is exact -- if B holds every
    commit of A, then A's whole history is part of B's -- but it does **not** say
    which project came first. Measured on two real pairs of the same shape:

        torvalds/linux vs SUSE/kernel          lag 0.004%  inside; subset = upstream
        simplelink-zephyr vs zephyr/zephyr     lag 0.32%   diverged, but the
                                                           subset is the FORK

    The first pair is inside the tolerance and reads `torvalds/linux inside
    SUSE/kernel`: SUSE keeps merging upstream and adding patches, so the upstream
    is the subset. The second pair sits just outside it and reads `diverged`, but
    it shows what inclusion would have claimed: TI's fork lags Zephyr and adds
    almost nothing, so the FORK is the subset. Same topology as the first pair,
    opposite origin. Every asymmetry that looks promising turns out symmetric:
    both trees are "the other, truncated, plus their own commits", and both
    sides' unique commits are newer than the last commit they share.

    So origin is not taken from here. It comes from `repo_created`, and this
    function reports inclusion, which is what it can prove.

    Each row reads: shared, only_a, only_b, the two lag fractions, and one of
    `identical`, `<x> inside <y>` or `diverged`.
    """
    names = sorted(commits)
    evidence = []
    for i, a in enumerate(names):
        for b in names[i + 1:]:
            shared = len(commits[a] & commits[b])
            only_a, only_b = len(commits[a]) - shared, len(commits[b]) - shared
            lag_a = only_a / max(len(commits[a]), 1)
            lag_b = only_b / max(len(commits[b]), 1)
            a_inside = lag_a <= tolerance
            b_inside = lag_b <= tolerance
            if a_inside and b_inside:
                rel = "identical"
            elif a_inside:
                rel = f"{a} inside {b}"
            elif b_inside:
                rel = f"{b} inside {a}"
            else:
                rel = "diverged"
            evidence.append({"a": a, "b": b, "shared": shared,
                             "only_a": only_a, "only_b": only_b,
                             "lag_a": round(lag_a, 6), "lag_b": round(lag_b, 6),
                             "relation": rel})
    return evidence


def clusters_of(cache: dict) -> list[dict]:
    """Every cluster the cache knows, as {id, note, members: {name: url}}.

    Built over **everything the cache knows**, not over one manifest. That
    distinction is load-bearing. Scoping it to the current manifest oscillated:
    a verdict on a project vanished when the draw dropped that project, and the
    draw took it straight back. Roots are cached for ever, so cluster membership
    is permanent knowledge and the manifest is only the work queue.
    """
    by_url = cache["projects"]
    out = []
    for group in group_by_root({u: cache["roots"].get(u, [])
                                for u in by_url if u in cache["roots"]}):
        if len(group) < 2:
            continue
        roots = sorted({root for url in group
                        for root in cache["roots"].get(url, [])})
        cid = cluster_id(roots)
        # ctp's project name is owner__repo; the frame keys on owner/repo.
        out.append({"id": cid, "note": CLUSTER_NOTES.get(cid, ""),
                    "members": {by_url[url]["name"].replace("__", "/", 1).lower(): url
                                for url in group}})
    return sorted(out, key=lambda c: c["id"])


def annotate(cache: dict) -> dict[str, dict]:
    """One record per project that shares a history with another.

    Nothing is excluded. The relationship is the finding, so it is recorded for
    every member: which cluster, which projects it shares with, and which of
    them came first.

    `first` and `relation` come from `cache["ancestry"]`, which the `ancestry`
    command fills. Without it the cluster is still recorded, and `relation` reads
    `unmeasured` rather than guessing a direction.
    """
    ancestry = cache.get("ancestry", {})
    out: dict[str, dict] = {}
    for cluster in clusters_of(cache):
        names = sorted(cluster["members"])
        measured = ancestry.get(cluster["id"], {})
        evidence = measured.get("evidence")
        created = measured.get("created", {})
        # The earliest repository in the cluster. This is the origin signal, and
        # it is external: the commit graph proves inclusion, not who came first.
        dated = {n: d for n, d in created.items() if d}
        first = min(dated, key=lambda n: dated[n]) if dated else ""
        for name in names:
            record = {
                "cluster": cluster["id"],
                "shared_with": [n for n in names if n != name],
                "note": cluster["note"],
                # `includes` names the projects whose whole history is part of
                # this one. `included_in` is the mirror of that. Neither claims
                # origin: `first` does, from the repository creation date.
                "includes": [], "included_in": [], "mirror_of": [],
                "diverged_from": [], "relation": "unmeasured",
                "first": first, "created": created.get(name, ""),
            }
            if evidence is not None:
                for e in evidence:
                    if name not in (e["a"], e["b"]):
                        continue
                    other = e["b"] if e["a"] == name else e["a"]
                    if e["relation"] == "identical":
                        record["mirror_of"].append(other)
                    elif e["relation"] == f"{other} inside {name}":
                        record["includes"].append(other)
                    elif e["relation"] == f"{name} inside {other}":
                        record["included_in"].append(other)
                    else:
                        record["diverged_from"].append(other)
                record["relation"] = (
                    "includes" if record["includes"] else
                    "included_in" if record["included_in"] else
                    "mirror" if record["mirror_of"] else "diverged")
            out[name] = record
    return out


def measure_ancestry(cache: dict, force: bool = False) -> dict:
    """Fill `cache["ancestry"]`: inclusion from the graph, origin from GitHub.

    Two independent measurements per cluster, and they are cached apart because
    they cost different amounts. `evidence` needs one commits-only clone per
    member, which is minutes. `created` needs one API call per member, which is
    seconds. A cluster that already has evidence but no dates therefore backfills
    the dates without cloning anything again.
    """
    cache.setdefault("ancestry", {})
    all_clusters = clusters_of(cache)
    todo = [c for c in all_clusters
            if force
            or not cache["ancestry"].get(c["id"], {}).get("evidence")
            or not cache["ancestry"].get(c["id"], {}).get("created")]
    say(f"{len(all_clusters)} clusters, {len(todo)} to measure")
    for cluster in todo:
        entry = dict(cache["ancestry"].get(cluster["id"], {}))
        members = sorted(cluster["members"])
        say(f"  cluster {cluster['id'][:12]} — {len(members)} members"
            f"{': ' + cluster['note'] if cluster['note'] else ''}")

        if force or not entry.get("created"):
            # Origin comes from outside the graph. One call per member.
            entry["created"] = {name: repo_created(name) for name in members}
            for name, when in entry["created"].items():
                say(f"      {name:44s} created {when or 'unknown'}")

        if force or not entry.get("evidence"):
            commits: dict[str, set[int]] = {}
            failed = []
            for name in members:
                try:
                    commits[name] = head_commits(cluster["members"][name],
                                                 name.replace("/", "__"))
                    say(f"      {name:44s} {len(commits[name]):>9,} commits")
                except RuntimeError as e:
                    failed.append(name)
                    say(f"      {name:44s} FAILED: {e}")
            if len(commits) < 2:
                say("      fewer than two members readable; no inclusion recorded")
                cache["ancestry"][cluster["id"]] = entry
                save_cache(cache)
                continue
            entry["evidence"] = direction(commits)
            entry["unreadable"] = failed
            for e in entry["evidence"]:
                say(f"      {e['relation']}: shared {e['shared']:,}, "
                    f"only {e['a']} {e['only_a']:,}, only {e['b']} {e['only_b']:,}")
            commits.clear()

        cache["ancestry"][cluster["id"]] = entry
        save_cache(cache)
    return cache


def cmd_ancestry(args: argparse.Namespace) -> int:
    manifest = Path(args.manifest)
    if not manifest.exists():
        say(f"no {manifest}")
        return 1
    cache = load_cache()
    for r in read_manifest(manifest):
        cache["projects"][r["url"]] = {k: r[k] for k in
                                       ("name", "stratum", "size_class")}
    for url in cache["roots"]:
        cache["projects"].setdefault(url, {"name": name_from_url(url),
                                           "stratum": "?", "size_class": "?"})
    cache = measure_ancestry(cache, force=args.force)
    return finish(read_manifest(manifest), cache)


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
    annotations = annotate(cache)
    cache["clusters"] = annotations
    # An older cache carries an `excluded` map. Nothing writes it now, and a
    # stale verdict left in a published record is worse than no verdict.
    cache.pop("excluded", None)
    save_cache(cache)

    by_name = {p["name"].replace("__", "/", 1).lower(): p
               for p in cache["projects"].values()}

    def describe(name: str) -> str:
        r = by_name.get(name, {})
        return f"{name:44s} {r.get('stratum', '?'):14s} {r.get('size_class', '?')}"

    unmeasured = [c for c in clusters_of(cache)
                  if c["id"] not in cache.get("ancestry", {})]
    say(f"--- {len(clusters_of(cache))} clusters, "
        f"{len(annotations)} projects share a history. Nothing is excluded ---")
    for cluster in clusters_of(cache):
        head = f"  cluster {cluster['id'][:12]}"
        if cluster["note"]:
            head += f" — {cluster['note']}"
        say(head)
        for name in sorted(cluster["members"]):
            a = annotations[name]
            detail = {"includes": "includes " + " ".join(a["includes"]),
                      "included_in": "included in " + " ".join(a["included_in"]),
                      "mirror": "mirror of " + " ".join(a["mirror_of"]),
                      "diverged": "diverged",
                      "unmeasured": "inclusion unmeasured"}[a["relation"]]
            oldest = "  <- oldest repository" if a["first"] == name else ""
            say(f"      {describe(name)}  {detail}{oldest}")

    failed = {u: e for u, e in cache["errors"].items() if u in {r["url"] for r in rows}}
    if failed:
        say(f"--- {len(failed)} projects could not be read ---")
        for url, err in sorted(failed.items()):
            say(f"  {url} — {err}")

    if unmeasured:
        say(f"--- {len(unmeasured)} clusters have no measured direction ---")
        say("Run `./shared_history.py ancestry` to fill it. It clones the commit "
            "graph of each cluster member, so it costs one clone per member and "
            "nothing per candidate.")
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

    a = sub.add_parser("ancestry",
                       help="measure which member of each cluster came first")
    a.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    a.add_argument("--force", action="store_true",
                   help="re-measure clusters that already have a direction")
    a.set_defaults(fn=cmd_ancestry)

    r = sub.add_parser("report", help="read the cache and report, no network")
    r.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    r.set_defaults(fn=cmd_report)

    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
