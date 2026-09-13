#!/usr/bin/env python3
"""select_corpus — build a stratified corpus manifest from control facts.

    ./select_corpus.py rosters                 # fetch + cache every roster
    ./select_corpus.py enrich [--limit N]      # resolve repos, add gh metadata
    ./select_corpus.py emit [--per-stratum N]  # candidates.csv + manifest.tsv
    ./select_corpus.py all --per-stratum 170

Stratum labels come from control facts (who controls the project), never from
contribution composition — labelling a project "enterprise" because this
pipeline measured high firm concentration would make the finding true by
construction. See CORPUS-HEURISTICS-PLAN.md "Locked decisions".

Three strata: company-owned / foundation / community. Collapsible to two in
analysis. Every row records which fact assigned the label, and the date.

The stratum is named `company-owned`, not `single-vendor`, because F2 proves
only that one company owns the namespace. True single-vendor control needs F3,
the CLA, which assigns copyright to one counterparty. F3 is not implemented, so
`google/leveldb` (released code, community patches) and a product repository
governed by a CLA both label `company-owned` today. Rename the stratum back
only after F3 runs.

Facts, in precedence order:
  F1 trademark owner   -> foundation roster membership (ASF/Eclipse/CNCF hold
                          the marks for their projects); community roster
                          membership (SFC/SPI hold assets in trust)
  F2 namespace owner   -> the GitHub org that owns the repository
  F3 CLA/DCO           -> --deep only; CLA implies a single counterparty
  F4 maintainer appt.  -> --deep only; GOVERNANCE.md / MAINTAINERS present

Language scope is what cregit tokenizes today: C, C++, Java, Rust (and m4,
which is never a primary language so it is not a selection key). Verified in
cregit-issue61/tokenize/tokenize.pl %parsers.

Caution — language confounds stratum. ASF is 217/378 Java; the mailing-list
community world is overwhelmingly C. If foundation comes out mostly Java and
community mostly C, a stratum difference may be a language effect. `emit`
balances languages within a stratum when --balance-lang is set, and
candidates.csv always carries the language so it can enter a model as a
covariate.

Stdlib only. Needs an authenticated `gh` on PATH (5000 req/h; 60 without).
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import sys
import time
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

CORPUS = Path(__file__).resolve().parent
CACHE = CORPUS / ".corpus-cache"
CANDIDATES = CORPUS / "candidates.csv"
MANIFEST_OUT = CORPUS / "manifest.generated.tsv"

# Only languages cregit can tokenize. m4 is omitted on purpose: it appears as
# .am/.ac build files, never as a project's primary language.
LANG_FILTER = {
    "C": r"\.[ch]$",
    "C++": r"\.(c|cc|cp|cpp|cxx|h|hh|hpp)$",
    "Java": r"\.java$",
    "Rust": r"\.rs$",
}

# manifest.tsv header: size_class: S < 30k commits | M 30k-150k | L > 150k
SIZE_S, SIZE_M = 30_000, 150_000

# Selection constraints (CORPUS-HEURISTICS-PLAN.md P0).
MIN_SIZE_KB = 2_000       # excludes doc-only and toy repos
MAX_STALE_DAYS = 550      # "alive in 2026"

ROSTERS = {
    "asf": ("foundation", "https://projects.apache.org/json/foundation/projects.json"),
    "cncf": ("foundation", "https://raw.githubusercontent.com/cncf/landscape/master/landscape.yml"),
    "sfc": ("community", "https://sfconservancy.org/projects/current/"),
    "spi": ("community", "https://www.spi-inc.org/projects/"),
}

# Eclipse caps pagesize at 100 and ignores anything larger, so it is paged.
ECLIPSE_API = "https://projects.eclipse.org/api/projects?page={}&pagesize=100"

# GitHub search supplies volume. Rosters supply positive labels; whatever a
# roster does not claim is classified by F2 (namespace owner). This is what
# lets the corpus reach 500+ without hand curation.
GH_SEARCH_STARS = ">400"
GH_SEARCH_PER_LANG = 260

# GitHub orgs held by a company, i.e. F2 namespace ownership. Hand-seeded, so it
# is a supplement only: the citable pool is the Spinellis strong tier below,
# whose membership rests on SEC filings and the Fortune Global 500 rather than on
# anyone's judgement. `enrich` also reads org metadata to catch orgs missing here.
COMPANY_ORGS = {
    "google", "googleapis", "GoogleCloudPlatform", "microsoft", "Azure", "dotnet",
    "facebook", "facebookincubator", "meta-llama", "apple", "aws", "awslabs",
    "amzn", "netflix", "uber", "linkedin", "twitter", "airbnb", "spotify",
    "alibaba", "tencent", "bytedance", "baidu", "huawei", "oracle", "IBM",
    "RedHatOfficial", "intel", "NVIDIA", "AMD", "arm", "qualcomm", "samsung",
    "elastic", "mongodb", "hashicorp", "confluentinc", "databricks", "grafana",
    "DataDog", "cloudflare", "canonical", "SUSE", "JetBrains", "SonarSource",
    "redis", "influxdata", "timescale", "yugabyte", "cockroachdb", "MariaDB",
    "percona", "vmware", "dell", "hpe", "cisco", "juniper", "broadcom",
    "mediatek", "realtek", "st-micro", "TexasInstruments", "nxp", "renesas",
}

# Foundation-owned namespaces for the residual ghsearch pool — a project the
# rosters miss but whose namespace is held by a foundation is still F2-foundation.
FOUNDATION_ORGS = {
    "apache", "eclipse", "eclipse-ee4j", "eclipse-vertx", "cncf", "kubernetes",
    "kubernetes-sigs", "opencontainers", "containerd", "etcd-io", "prometheus",
    "envoyproxy", "openjs-foundation", "nodejs", "libuv", "python", "rust-lang",
    "golang", "llvm", "gnome", "kde", "freedesktop", "videolan", "mozilla",
    "openssl", "torvalds", "git", "gitlabhq", "OpenPrinting", "systemd",
    "linuxfoundation", "openstack", "ceph", "hyperledger", "jenkinsci",
    "eclipse-openj9", "TheAlgorithms", "openembedded", "yoctoproject",
}

# Community projects that no roster carries but that anchor the stratum:
# mailing-list culture, no corporate namespace, no foundation trademark.
# CORPUS-HEURISTICS-PLAN.md:66 names these as the Gate-2 fallback stratum.
COMMUNITY_SEED = [
    "postgres/postgres", "qemu/qemu", "git/git", "openbsd/src", "freebsd/freebsd-src",
    "NetBSD/src", "curl/curl", "vim/vim", "neovim/neovim", "tmux/tmux",
    "jqlang/jq", "libuv/libuv", "htop-dev/htop", "wireshark/wireshark",
    "openssl/openssl", "libressl/portable", "util-linux/util-linux",
    "systemd/systemd", "bminor/glibc", "gcc-mirror/gcc", "irssi/irssi",
    "weechat/weechat", "mpv-player/mpv", "FFmpeg/FFmpeg", "videolan/vlc",
    "GNOME/glib", "KDE/kdelibs", "inkscape/inkscape", "gimp-mirror/gimp",
    "zsh-users/zsh", "fish-shell/fish-shell", "rakudo/rakudo",
]

SLUG_RE = re.compile(r"github\.com[:/]+([A-Za-z0-9._-]+)/([A-Za-z0-9._-]+?)(?:\.git)?/?$")

# Superseded stratum names, mapped forward. See the module docstring for why
# `single-vendor` became `company-owned`.
LEGACY_STRATUM = {"single-vendor": "company-owned"}


def say(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


# ---------------------------------------------------------------- rosters


def fetch(url: str, dest: Path) -> Path:
    """Download url to dest once. Cached files are never re-fetched."""
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(url, headers={"User-Agent": "cregit-token-pipeline"})
    with urllib.request.urlopen(req, timeout=60) as r:
        dest.write_bytes(r.read())
    return dest


def fetch_eclipse() -> Path:
    """Page the Eclipse API into one JSON array."""
    dest = CACHE / "roster-eclipse.raw"
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    allp, page = [], 0
    while page < 40:
        tmp = CACHE / f".ecl-{page}"
        try:
            fetch(ECLIPSE_API.format(page), tmp)
            got = json.loads(tmp.read_text())
        except Exception:
            break
        got = got if isinstance(got, list) else list(got.values())
        if not got:
            break
        allp += got
        page += 1
    dest.write_text(json.dumps(allp))
    for t in CACHE.glob(".ecl-*"):
        t.unlink()
    return dest


def fetch_subpages(name: str) -> Path:
    """SFC and SPI index pages link to per-project pages; concatenate them all."""
    dest = CACHE / f"roster-{name}-pages.raw"
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    index = (CACHE / f"roster-{name}.raw").read_text(errors="replace")
    base = ROSTERS[name][1]
    hrefs = set(re.findall(r'href="([^"#?:]+)"', index))
    hrefs = {h for h in hrefs
             if not h.startswith(("/", "..")) and not h.endswith((".css", ".ico", ".png", ".js"))}
    blob = [index]
    for h in sorted(hrefs)[:200]:
        tmp = CACHE / f".sub-{name}-{re.sub('[^A-Za-z0-9]', '_', h)}"
        try:
            fetch(base.rstrip("/") + "/" + h.strip("/"), tmp)
            blob.append(tmp.read_text(errors="replace"))
        except Exception:
            continue
    dest.write_text("\n".join(blob))
    for t in CACHE.glob(f".sub-{name}-*"):
        t.unlink()
    return dest


def cmd_rosters(args: argparse.Namespace) -> int:
    for name, (stratum, url) in ROSTERS.items():
        try:
            p = fetch(url, CACHE / f"roster-{name}.raw")
            say(f"{name:8s} {stratum:13s} {p.stat().st_size:>9,} bytes")
        except Exception as e:  # a single unreachable roster must not stop the rest
            say(f"{name:8s} FAILED: {e}")
    try:
        p = fetch_eclipse()
        n = len(json.loads(p.read_text()))
        say(f"{'eclipse':8s} {'foundation':13s} {p.stat().st_size:>9,} bytes ({n} projects)")
    except Exception as e:
        say(f"eclipse  FAILED: {e}")
    for name in ("sfc", "spi"):
        try:
            p = fetch_subpages(name)
            say(f"{name + '-pages':14s} {p.stat().st_size:>9,} bytes")
        except Exception as e:
            say(f"{name}-pages FAILED: {e}")
    return 0


def slug(text: str) -> tuple[str, str] | None:
    m = SLUG_RE.search((text or "").strip())
    return (m.group(1), m.group(2)) if m else None


def lang_tokens(text: str) -> set[str]:
    """Split a roster language string into exact tokens ('Scala' must not match 'C')."""
    return {t.strip() for t in re.split(r"[,;/|]| and ", text or "") if t.strip()}


def parse_asf() -> list[dict]:
    """ASF publishes gitbox/svn URLs, not GitHub. Every ASF project mirrors to
    github.com/apache/<repo>, so the slug is derived from the gitbox basename.
    The roster's own programming-language field pre-filters the pool, which
    saves ~140 wasted gh calls."""
    d = json.loads((CACHE / "roster-asf.raw").read_text())
    out = []
    for key, v in d.items():
        raw = v.get("programming-language") or ""
        langs = raw if isinstance(raw, list) else [raw]
        langs = "; ".join(str(x) for x in langs)
        if not (lang_tokens(langs) & set(LANG_FILTER)):
            continue
        repos = v.get("repository") or []
        repos = repos if isinstance(repos, list) else [repos]
        for r in repos:
            url = r.get("url", "") if isinstance(r, dict) else str(r)
            s = slug(url)
            if not s:
                base = re.sub(r"\.git$", "", url.rstrip("/")).rsplit("/", 1)[-1]
                if not base or base in {"asf", "repos"}:
                    continue
                s = ("apache", base)
            out.append(dict(source="asf", stratum="foundation", fact="F1_roster:asf",
                            owner=s[0], repo=s[1], roster_lang=langs,
                            roster_name=v.get("name") or key))
            break
    return out


def parse_eclipse() -> list[dict]:
    d = json.loads((CACHE / "roster-eclipse.raw").read_text())
    d = d if isinstance(d, list) else list(d.values())
    out = []
    for v in d:
        if (v.get("state") or "").lower() == "archived":
            continue
        best = None
        for r in (v.get("github_repos") or []):
            s = slug(r.get("url") if isinstance(r, dict) else r)
            # skip website/docs mirrors; prefer a repo that is not *.io / *.net
            if s and not re.search(r"\.(net|io|org|com)$|^(www|blog|docs)", s[1]):
                best = s
                break
            best = best or s
        if best:
            out.append(dict(source="eclipse", stratum="foundation",
                            fact="F1_roster:eclipse", owner=best[0], repo=best[1],
                            roster_lang="", roster_name=v.get("name") or ""))
    return out


# Spinellis et al., "A Dataset of Enterprise-Driven Open Source Software", MSR
# 2020. Zenodo 10.5281/zenodo.3742962, CC-BY-4.0. Used as a CANDIDATE POOL only,
# never as a label: its own manual evaluation reports 89% precision with recall
# unevaluated and Cohen's kappa 0.29, and 41 of MongoDB's 42 repositories sit in
# its NON-enterprise cohort. Our own control facts assign the stratum, and our
# own filters below decide eligibility.
SPINELLIS_TSV = CORPUS / "sources" / "enterprise_projects.txt"
SPINELLIS_COLS = (
    "url project_id sdtc mcpc mcve star_number commit_count files lines "
    "pull_requests github_repo_creation earliest_commit most_recent_commit "
    "committer_count author_count dominant_domain dominant_domain_committer_commits "
    "dominant_domain_author_commits dominant_domain_committers dominant_domain_authors "
    "cik fg500 sec10k sec20f project_name owner_login company_name owner_company license"
).split()

# Our filters over their fields, applied before any API call.
SP_MIN_YEAR = "2018"     # their snapshot is 2020; gh pushed_at re-checks for 2026
SP_MIN_LINES = 20_000    # excludes doc and sample repos
SP_MIN_COMMITS = 200
# Their strong tier is 60% Microsoft plus Alphabet. Uncapped, the stratum would
# measure those two firms rather than company-owned OSS, and firm concentration
# would be high by construction.
SP_MAX_PER_COMPANY = 12


def parse_spinellis() -> list[dict]:
    if not SPINELLIS_TSV.exists():
        say("spinellis: sources/enterprise_projects.txt absent, skipped")
        return []

    def txt(v: str) -> str:
        return (v or "").strip()

    def flag(v: str) -> bool:
        return txt(v).lower() == "t"

    def num(v: str) -> int:
        try:
            return int(txt(v) or 0)
        except ValueError:
            return 0

    kept, per_company = [], Counter()
    rows = []
    for line in SPINELLIS_TSV.read_text(errors="replace").splitlines():
        f = line.split("\t")
        if len(f) < len(SPINELLIS_COLS):
            f += [""] * (len(SPINELLIS_COLS) - len(f))
        rows.append(dict(zip(SPINELLIS_COLS, f)))

    # Strongest provenance first, so the per-company cap keeps the best-attested
    # projects: Fortune Global 500, then 10-K filers, then 20-F filers.
    def tier(r: dict) -> tuple[int, str]:
        if flag(r["fg500"]):
            return 0, "fg500"
        if flag(r["sec10k"]):
            return 1, "sec10k"
        return 2, "sec20f"

    strong = [r for r in rows
              if (flag(r["fg500"]) or flag(r["sec10k"]) or flag(r["sec20f"]))
              and txt(r["company_name"])]                      # + org-registered
    strong.sort(key=lambda r: (tier(r)[0], -num(r["lines"])))

    for r in strong:
        if txt(r["most_recent_commit"])[:4] < SP_MIN_YEAR:
            continue
        if num(r["lines"]) < SP_MIN_LINES or num(r["commit_count"]) < SP_MIN_COMMITS:
            continue
        co = txt(r["company_name"])
        if per_company[co] >= SP_MAX_PER_COMPANY:
            continue
        s = slug(txt(r["url"]))
        if not s:
            continue
        per_company[co] += 1
        kept.append(dict(source="spinellis", stratum="company-owned",
                         fact=f"F1_pool:spinellis-{tier(r)[1]}",
                         owner=s[0], repo=s[1], roster_lang="",
                         roster_name=txt(r["project_name"]) or s[1]))
    say(f"  spinellis: {len(strong):,} strong+org-registered -> {len(kept):,} after "
        f"our filters (>={SP_MIN_YEAR}, >={SP_MIN_LINES:,} lines, "
        f">={SP_MIN_COMMITS} commits, <={SP_MAX_PER_COMPANY}/company); "
        f"{len(per_company)} companies")
    return kept


# The same authors' comparison cohort: 311,223 projects their heuristics did NOT
# match to an enterprise, selected to have comparable quality attributes.
COHORT_TSV = CORPUS / "sources" / "cohort_project_details.txt"
COHORT_COLS = "url project_id stars commit_count".split()
# Match GH_SEARCH_STARS exactly (strictly greater than 400) so a project does not
# become eligible or ineligible according to which source found it.
COHORT_MIN_STARS = 400


def parse_spinellis_cohort() -> list[dict]:
    """Community candidate POOL, from Spinellis et al.'s non-enterprise cohort.

    Be exact about what the fact is. Membership means their three heuristics
    found no enterprise signal, so this is an ABSENCE of a corporate signal, not
    a positive community control fact. Community therefore stays a negatively
    defined stratum. What changes is that the negation is now published,
    versioned and reproducible, instead of being whatever our own GitHub search
    happened to leave over.

    The label stays open. judge() relabels any row whose namespace turns out to
    belong to a company (F2) or a foundation, so `weak=True` marks these rows as
    eligible for that relabelling in the same way as the residual pool.

    Their snapshot is GHTorrent circa 2020. Many rows are now dead, renamed or
    private; `unreachable` and MAX_STALE_DAYS remove those at emit, which is why
    the pool is much larger than the eventual stratum.

    Thresholds match the other sources on purpose: > 400 stars as in the starred
    search, >= 200 commits as in the strong tier. The file carries no language,
    so LANG_FILTER can only apply after enrichment.
    """
    if not COHORT_TSV.exists():
        say("cohort: sources/cohort_project_details.txt absent, skipped")
        return []
    kept, seen, malformed = [], 0, 0
    for line in COHORT_TSV.read_text(errors="replace").splitlines():
        f = line.split("\t")
        if len(f) != len(COHORT_COLS):
            malformed += 1
            continue
        r = dict(zip(COHORT_COLS, f))
        seen += 1
        try:
            stars, commits = int(r["stars"]), int(r["commit_count"])
        except ValueError:
            malformed += 1
            continue
        if stars <= COHORT_MIN_STARS or commits < SP_MIN_COMMITS:
            continue
        s = slug(r["url"].strip())
        if not s:
            continue
        kept.append(dict(source="cohort", stratum="community",
                         fact="F1_pool:spinellis-cohort", weak=True,
                         owner=s[0], repo=s[1], roster_lang="", roster_name=s[1],
                         cohort_stars=stars, cohort_commits=commits))
    # Deterministic order, best-attested first, so a --per-stratum cut is stable.
    kept.sort(key=lambda r: (-r["cohort_stars"], r["owner"].lower(), r["repo"].lower()))
    say(f"  cohort: {seen:,} rows -> {len(kept):,} after our filters "
        f"(>{COHORT_MIN_STARS} stars, >={SP_MIN_COMMITS} commits)"
        + (f"; {malformed:,} malformed" if malformed else ""))
    return kept


def parse_company_orgs() -> list[dict]:
    """Enumerate repositories directly from company namespaces.

    Needed because the starred-search pool skews community and foundation: it
    produced only 59 company-owned candidates against 687 community. A company
    org's repository list is the F2 control fact stated at its strongest — the
    company owns the namespace — so this source is labelled company-owned and
    the language filter does the rest.
    """
    out = []
    for org in sorted(COMPANY_ORGS):
        try:
            d = gh(f"orgs/{org}/repos", "--paginate", "-X", "GET",
                   "-f", "per_page=100", "-f", "sort=updated", "-f", "type=public")
        except GhFailed as e:
            say(f"  company-org {org}: skipped, {e}")
            continue
        if not isinstance(d, list):
            continue
        for r in d:
            if r.get("fork") or r.get("archived"):
                continue
            if (r.get("language") or "") not in LANG_FILTER:
                continue
            out.append(dict(source="company-org", stratum="company-owned",
                            fact=f"F2_org:{org}", owner=org,
                            repo=(r.get("name") or ""), roster_lang=r.get("language") or "",
                            roster_name=r.get("name") or ""))
    return out


def parse_ghsearch() -> list[dict]:
    """Volume source. Unlabelled on purpose — F1 rosters and F2 namespace
    ownership assign the stratum in judge(); anything neither claims is
    community by residual, which mirrors the MAINTAINERS F:* catch-all."""
    out = []
    for lang in ("c", "cpp", "java", "rust"):
        p = subprocess.run(
            ["gh", "search", "repos", f"--language={lang}", f"--stars={GH_SEARCH_STARS}",
             "--sort=stars", f"--limit={GH_SEARCH_PER_LANG}",
             "--json", "fullName,stargazersCount"],
            capture_output=True, text=True, timeout=180)
        if p.returncode != 0:
            say(f"gh search {lang} failed: {p.stderr.strip()[:120]}")
            continue
        for r in json.loads(p.stdout or "[]"):
            owner, _, repo = r["fullName"].partition("/")
            out.append(dict(source="ghsearch", stratum="community",
                            fact="F1_residual:none", owner=owner, repo=repo,
                            roster_lang=lang, roster_name=repo))
    return out


# CNCF hosting levels. Only an entry carrying one of these has donated its
# trademark to the Linux Foundation, which is what makes F1 a control fact.
CNCF_HOSTED = {"graduated", "incubating", "sandbox", "archived"}


def parse_cncf() -> list[dict]:
    """landscape.yml without a yaml dep: buffer each item, then judge it.

    The distinction this function exists to make: **the CNCF landscape is not
    the CNCF.** The file catalogues the whole cloud-native ecosystem, and only an
    entry with a `project:` key is actually CNCF-hosted. 256 of them are, against
    988 repository URLs in the file.

    Reading every URL as a foundation roster put `postgres/postgres` and
    `redis/redis` in the foundation stratum. PostgreSQL is the canonical
    mailing-list community project, and Redis Ltd relicensed Redis in 2024. Both
    labels were wrong, and neither error was visible in the counts.

    So an item splits two ways:
      hosted     -> foundation, fact F1_roster:cncf-<level>. A real control fact.
      catalogued -> an OPEN label. Good candidate pool, no control fact, so F2
                    decides and community is the residual.

    Buffering matters because `project:` and `repo_url:` sit in the same item in
    either order, so a line-at-a-time pass cannot pair them.
    """
    text = (CACHE / "roster-cncf.raw").read_text(errors="replace")
    out: list[dict] = []

    def flush(item: dict) -> None:
        s = slug(item.get("repo_url", ""))
        if not s:
            return
        level = item.get("project", "").strip().strip("'\"").lower()
        if level in CNCF_HOSTED:
            out.append(dict(source="cncf", stratum="foundation",
                            fact=f"F1_roster:cncf-{level}", owner=s[0], repo=s[1],
                            roster_lang="", roster_name=item.get("name", s[1])))
        else:
            out.append(dict(source="cncf-landscape", stratum="community",
                            fact="F1_pool:cncf-landscape", weak=True,
                            owner=s[0], repo=s[1], roster_lang="",
                            roster_name=item.get("name", s[1])))

    item: dict = {}
    for line in text.splitlines():
        st = line.strip()
        if st.startswith("- item:") or st.startswith("- name:"):
            flush(item)
            item = {}
            if ":" in st and st.startswith("- name:"):
                item["name"] = st.split(":", 1)[1].strip().strip("'\"")
            continue
        for key in ("name", "repo_url", "project"):
            if st.startswith(f"{key}:") and key not in item:
                item[key] = st.split(":", 1)[1].strip().strip("'\"")
                break
    flush(item)

    hosted = sum(1 for r in out if r["stratum"] == "foundation")
    say(f"  cncf: {hosted} hosted (F1 control fact), "
        f"{len(out) - hosted} catalogued only (pool, label stays open)")
    return out


def parse_html_roster(name: str, stratum: str) -> list[dict]:
    """SFC and SPI index pages link to project homepages, not repositories, so
    the per-project subpages are fetched too and scanned for github slugs."""
    paths = [CACHE / f"roster-{name}-pages.raw", CACHE / f"roster-{name}.raw"]
    text = next((p.read_text(errors="replace") for p in paths if p.exists()), "")
    skip = {"sfconservancy", "spi-inc", "giveupgithub", "outreachy", "sponsors"}
    seen, out = set(), []
    for m in re.finditer(r"github\.com/([A-Za-z0-9._-]+)/([A-Za-z0-9._-]+)", text):
        key = (m.group(1), re.sub(r"\.git$", "", m.group(2)))
        if key in seen or key[0].lower() in skip or key[1].lower() in {"", "issues"}:
            continue
        seen.add(key)
        out.append(dict(source=name, stratum=stratum, fact=f"F1_roster:{name}",
                        owner=key[0], repo=key[1], roster_lang="", roster_name=key[1]))
    return out


def collect_candidates() -> list[dict]:
    rows: list[dict] = []
    # Order matters: rosters carry POSITIVE control facts and are read first, so
    # dedup lets a roster label win over the residual ghsearch pool.
    parsers = {"asf": parse_asf, "eclipse": parse_eclipse, "cncf": parse_cncf,
               "sfc": lambda: parse_html_roster("sfc", "community"),
               "spi": lambda: parse_html_roster("spi", "community")}
    for name, fn in parsers.items():
        if not (CACHE / f"roster-{name}.raw").exists():
            say(f"{name}: no cache, skipped (run `rosters` first)")
            continue
        try:
            got = fn()
            rows += got
            say(f"{name:8s} {len(got):>4} candidates")
        except Exception as e:
            say(f"{name:8s} parse FAILED: {e}")

    for s in COMMUNITY_SEED:
        owner, repo = s.split("/")
        rows.append(dict(source="seed", stratum="community", fact="F1_roster:seed",
                         owner=owner, repo=repo, roster_lang="", roster_name=repo))
    say(f"seed     {len(COMMUNITY_SEED):>4} candidates")

    # Spinellis strong tier. Read before the volume sources so its citable
    # SEC/Fortune provenance wins on dedup over a bare namespace match.
    sp = parse_spinellis()
    rows += sp
    say(f"spinellis{len(sp):>5} candidates")

    # Volume sources, read last so roster labels win on dedup. Both cached
    # because they cost gh calls.
    for label, cache_name, fn in (
            ("company-org", "company-orgs.json", parse_company_orgs),
            # The cohort sits between the two on purpose. Its fact is weaker than
            # a company namespace (F2), and stronger than ghsearch, which carries
            # no fact at all, so this position is its dedup precedence.
            ("cohort", None, parse_spinellis_cohort),
            ("ghsearch", "ghsearch.json", parse_ghsearch)):
        if cache_name is None:
            got = fn()                  # local file: no API cost, nothing to cache
        else:
            c = CACHE / cache_name
            if c.exists():
                got = json.loads(c.read_text())
            else:
                got = fn()
                c.write_text(json.dumps(got))
        rows += got
        say(f"{label:9s}{len(got):>5} candidates")

    # The JSON caches were written before a stratum was renamed, and they are
    # replayed verbatim, so a stale cache can reintroduce a dead label. Normalise
    # here rather than deleting the caches, which cost thousands of gh calls.
    for r in rows:
        r["stratum"] = LEGACY_STRATUM.get(r["stratum"], r["stratum"])

    # De-duplicate on owner/repo; the first roster to claim a project wins, and
    # foundation/community rosters are read before the company-owned extras.
    uniq: dict[tuple[str, str], dict] = {}
    for r in rows:
        uniq.setdefault((r["owner"].lower(), r["repo"].lower()), r)
    say(f"total    {len(uniq):>4} unique candidates ({len(rows)} before dedup)")
    return list(uniq.values())


# ---------------------------------------------------------------- enrich


class RateLimited(Exception):
    """A gh call failed for a transient reason: quota, 5xx or the network.

    This aborts the enrich pass. Waiting is the only cure, and continuing would
    cache live repositories as dead.
    """


class GhFailed(Exception):
    """A gh call failed for a reason this code does not recognise.

    One strange repository must not stop a 22,000 repository pass, so the caller
    records the detail and carries on. `--retry-errors` picks these up again, and
    the recorded detail is what tells you whether the classifier needs a new rule.
    """


# States that are permanent. Caching them as gone is correct and saves the quota.
#   404 / not found            deleted, renamed or private
#   could not resolve          how GraphQL reports a missing repository
#   451 / access blocked       DMCA takedown, for example compozed/deployadactyl
#   410 / gone                 removed
#   repository is empty        no commits, so nothing to tokenise. GitHub words
#                              this "Git Repository is empty." with HTTP 409, so
#                              the substring is NOT "empty repository".
GONE_PATTERNS = ("404", "not found", "could not resolve", "does not exist",
                 "451", "access blocked", "410", "gone",
                 # both word orders: GitHub has used "Git Repository is empty."
                 # and "Empty Repository", both with HTTP 409
                 "repository is empty", "empty repository")
# Quota exhausted. The cure is to wait for the window to reset, which GitHub
# reports, so sleep exactly that long instead of guessing.
QUOTA_PATTERNS = ("rate limit", "403", "abuse", "secondary")
# The server or the network failed. The cure is a short exponential backoff. Do
# NOT use the quota timer here: a 502 does not mean the window is exhausted.
SERVER_PATTERNS = ("502", "503", "504", "500", "bad gateway", "timeout",
                   "timed out", "no such host", "connection reset",
                   "unexpected eof", "tls handshake")


def rate_limit_wait(default: int = 120) -> int:
    """Seconds until the exhausted quota resets, read from GitHub itself.

    `gh api rate_limit` does not consume quota, so this is free to call.
    """
    try:
        p = subprocess.run(["gh", "api", "rate_limit"], capture_output=True,
                           text=True, timeout=30)
        res = json.loads(p.stdout)["resources"]
        empty = [r for r in (res.get("core"), res.get("graphql")) if r and not r["remaining"]]
        if not empty:
            return default
        return max(default, int(min(r["reset"] for r in empty) - time.time()) + 5)
    except Exception:
        return default


def gh(*args: str, retries: int = 6) -> dict | list | None:
    """One `gh api` call.

    Three outcomes, and the difference between them is load-bearing:

      body   the call worked
      None   the resource is permanently gone. Safe to cache as dead.
      raise  RateLimited for a transient failure, GhFailed for anything else.

    cmd_enrich caches this result, so one throttled call cached as "unreachable"
    removes a live project from the corpus permanently and silently. An earlier
    run did exactly that: 2,734 of 5,803 cached rows, 47%, were marked
    unreachable, which no plausible rate of dead repositories explains.

    A 404 is a fact worth caching. A 403 is a queue. An unknown state is neither,
    so it raises GhFailed and the caller records the detail and carries on: one
    strange repository must not stop a 22,000 repository pass. That happened too.
    A DMCA-blocked repository answers 451, which no rule matched, so the pass
    burned six retries and aborted after 154 repositories.
    """
    detail, transient = "", False
    for attempt in range(retries):
        try:
            p = subprocess.run(["gh", "api", *args], capture_output=True,
                               text=True, timeout=60)
        except Exception as e:
            # gh missing, timeout, or the network down. All transient.
            detail, transient = f"subprocess: {e}", True
            time.sleep(2 ** attempt)
            continue
        if p.returncode == 0 and p.stdout.strip():
            try:
                return json.loads(p.stdout)
            except json.JSONDecodeError:
                return None
        err = ((p.stderr or "") + (p.stdout or "")).lower()
        detail = (p.stderr or p.stdout or f"rc={p.returncode}").strip()[:200]
        if any(pat in err for pat in GONE_PATTERNS):
            return None                       # permanent: safe to cache as dead
        if any(pat in err for pat in QUOTA_PATTERNS):
            transient = True
            wait = rate_limit_wait()
            say(f"throttled, sleeping {wait}s ({' '.join(args)[:60]})")
            time.sleep(wait)
            continue
        if any(pat in err for pat in SERVER_PATTERNS):
            transient = True
        time.sleep(2 ** attempt)              # backoff for server and unknown alike
    if transient or not detail:
        raise RateLimited(f"{' '.join(args)}: {detail}")
    raise GhFailed(f"{' '.join(args)}: {detail}")


def commit_count(owner: str, repo: str) -> int | None:
    q = ('{repository(owner:"%s",name:"%s"){defaultBranchRef{target{'
         '... on Commit{history{totalCount}}}}}}' % (owner, repo))
    try:
        d = gh("graphql", "-f", f"query={q}")
    except GhFailed:
        # An unknown GraphQL state costs this project its exact commit count, and
        # emit then excludes it as `no-commit-count`. RateLimited still propagates.
        return None
    try:
        return d["data"]["repository"]["defaultBranchRef"]["target"]["history"]["totalCount"]
    except Exception:
        return None


def org_is_company(login: str, cache: dict) -> tuple[bool, str]:
    """F2: does the namespace owner look like a company?"""
    if login in COMPANY_ORGS:
        return True, f"F2_org:{login}"
    if login in cache:
        return cache[login]
    try:
        d = gh(f"users/{login}") or {}
    except GhFailed:
        d = {}          # unknown namespace state: claim nothing. RateLimited propagates.
    # A verified org that names a company, or a User account (a personal
    # namespace), are both single-counterparty namespaces.
    verdict = (False, "")
    if d.get("type") == "Organization" and d.get("is_verified") and d.get("company"):
        verdict = (True, f"F2_org_verified:{login}")
    cache[login] = verdict
    return verdict


def save_json(path: Path, obj: object) -> None:
    """Write through a temporary file, so an interrupted run cannot truncate a
    cache that cost thousands of API calls."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj))
    tmp.replace(path)


def cmd_enrich(args: argparse.Namespace) -> int:
    cands = collect_candidates()
    if args.limit:
        cands = cands[: args.limit]
    cache_path = CACHE / "repo-meta.json"
    org_path = CACHE / "org-meta.json"
    meta: dict = json.loads(cache_path.read_text()) if cache_path.exists() else {}
    # Persisted, because the corpus draws on far more namespaces than projects and
    # an in-memory cache re-paid for every one of them on each restart.
    org_cache: dict = {k: tuple(v) for k, v in
                       json.loads(org_path.read_text()).items()} if org_path.exists() else {}

    if args.retry_errors:
        dropped = [k for k, v in meta.items() if v.get("error")]
        for k in dropped:
            del meta[k]
        say(f"retrying {len(dropped):,} previously failed repos")

    # Case-insensitive, so a repo already enriched under a different spelling does
    # not cost a second pair of API calls.
    seen_keys = {k.lower() for k in meta}
    done = unclassified = 0
    try:
        for i, c in enumerate(cands, 1):
            key = f"{c['owner']}/{c['repo']}"
            if key.lower() in seen_keys:
                continue
            seen_keys.add(key.lower())
            try:
                d = gh(f"repos/{key}")
            except GhFailed as e:
                # Unknown state. Record why, so the classifier can gain a rule, and
                # carry on. --retry-errors picks these up again later.
                meta[key] = {"error": "unclassified", "detail": str(e)[:200]}
                unclassified += 1
                done += 1
                continue
            if d is None:
                # Only a genuine 404 reaches this branch. gh() raises on a
                # transient failure, so a throttled call is never cached as dead.
                meta[key] = {"error": "unreachable"}
            else:
                company, fact = org_is_company(d["owner"]["login"], org_cache)
                meta[key] = {
                    "full_name": d.get("full_name"), "language": d.get("language"),
                    "size_kb": d.get("size"), "pushed_at": d.get("pushed_at"),
                    "archived": d.get("archived"), "fork": d.get("fork"),
                    "stars": d.get("stargazers_count"),
                    "license": (d.get("license") or {}).get("spdx_id"),
                    "owner_type": d["owner"].get("type"),
                    "clone_url": d.get("clone_url"),
                    "owner_is_company": company, "company_fact": fact,
                    "commits": commit_count(c["owner"], c["repo"]),
                }
            done += 1
            if done % 25 == 0:
                save_json(cache_path, meta)
                save_json(org_path, org_cache)
                say(f"enriched {done:,} new; {i:,}/{len(cands):,} scanned")
    except (RateLimited, KeyboardInterrupt) as e:
        save_json(cache_path, meta)
        save_json(org_path, org_cache)
        say(f"STOPPED after {done:,} new repos: {type(e).__name__} {e}")
        say("nothing was cached as dead on a transient failure. Re-run to resume.")
        return 2

    save_json(cache_path, meta)
    save_json(org_path, org_cache)
    gone = sum(1 for v in meta.values() if v.get("error") == "unreachable")
    unclear = sum(1 for v in meta.values() if v.get("error") == "unclassified")
    say(f"enriched {len(meta):,} repos: {gone:,} gone, {unclear:,} unclassified "
        f"-> {cache_path}")
    if unclear:
        say("unclassified rows carry a `detail` field. A large count means gh() "
            "needs a new rule; --retry-errors fetches them again.")
    return 0


# ---------------------------------------------------------------- emit


def size_class(commits: int | None) -> str:
    if commits is None:
        return "?"
    return "S" if commits < SIZE_S else ("M" if commits <= SIZE_M else "L")


def stale_days(pushed: str | None) -> int | None:
    if not pushed:
        return None
    d = datetime.fromisoformat(pushed.replace("Z", "+00:00"))
    return (datetime.now(timezone.utc) - d).days


def judge(c: dict, m: dict) -> dict:
    """Apply the label and the filters. Never drops a row — records why."""
    row = dict(c)
    row.update({k: m.get(k) for k in
                ("language", "size_kb", "pushed_at", "archived", "fork", "stars",
                 "license", "owner_type", "clone_url", "commits")})
    row["label_date"] = today()

    # An OPEN label may be reassigned by F2. ghsearch carries no fact at all. The
    # Spinellis cohort carries only the ABSENCE of an enterprise signal, so a
    # foundation or company namespace outranks it. A positive F1 roster does not.
    residual = c["fact"] == "F1_residual:none"
    open_label = residual or bool(c.get("weak"))
    owner = (m.get("full_name") or "/").split("/")[0]

    # F2 on the open pool: a namespace held by a foundation is foundation.
    if open_label and owner.lower() in {o.lower() for o in FOUNDATION_ORGS}:
        row["stratum"] = "foundation"
        row["fact"] = f"F2_org_foundation:{owner}"

    # F2 overrides a foundation F1 only when the namespace is a company: a
    # company-owned repo inside a foundation roster is a contested case, kept
    # and flagged rather than silently resolved (CORPUS-HEURISTICS-PLAN.md:16).
    if m.get("owner_is_company"):
        if c["stratum"] == "foundation" and not open_label:
            row["contested"] = f"{c['fact']} vs {m.get('company_fact')}"
        else:
            row["stratum"] = "company-owned"
            row["fact"] = m.get("company_fact") or "F2_org"
    row.setdefault("contested", "")

    reasons = []
    if m.get("error"):
        # Report WHICH error. "not-enriched" means the enrich pass has not reached
        # this row yet; "unreachable" means GitHub says it is gone; "unclassified"
        # means gh() met a state it does not know. Collapsing the three into one
        # word made 21,512 pending rows read as dead repositories in the review.
        reasons.append(str(m["error"]))
    if m.get("archived"):
        reasons.append("archived")
    if m.get("fork"):
        reasons.append("fork")
    if (m.get("language") or "") not in LANG_FILTER:
        reasons.append(f"lang={m.get('language')}")
    if (m.get("size_kb") or 0) < MIN_SIZE_KB:
        reasons.append(f"size={m.get('size_kb')}kb")
    sd = stale_days(m.get("pushed_at"))
    if sd is not None and sd > MAX_STALE_DAYS:
        reasons.append(f"stale={sd}d")
    if m.get("commits") is None:
        reasons.append("no-commit-count")

    row["size_class"] = size_class(m.get("commits"))
    row["excluded_because"] = "; ".join(reasons)
    row["included"] = not reasons
    return row


def cmd_emit(args: argparse.Namespace) -> int:
    cache_path = CACHE / "repo-meta.json"
    if not cache_path.exists():
        say("no repo-meta.json — run `enrich` first")
        return 1
    meta = json.loads(cache_path.read_text())
    # GitHub owner/repo names are case-insensitive and the sources disagree on
    # case: the Spinellis cohort carries 2020 GHTorrent spelling. A case-sensitive
    # lookup reports an already enriched project as "not-enriched" and drops it.
    by_key = {k.lower(): v for k, v in meta.items()}
    rows = [judge(c, by_key.get(f"{c['owner']}/{c['repo']}".lower(),
                                {"error": "not-enriched"}))
            for c in collect_candidates()]

    cols = ["source", "stratum", "fact", "contested", "label_date", "owner", "repo",
            "roster_name", "roster_lang", "language", "commits", "size_class",
            "size_kb", "stars", "pushed_at", "license", "owner_type", "archived",
            "fork", "clone_url", "included", "excluded_because"]
    with CANDIDATES.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    say(f"wrote {CANDIDATES} ({len(rows)} rows)")

    kept = [r for r in rows if r["included"]]
    by_stratum: dict[str, list] = {}
    for r in kept:
        by_stratum.setdefault(r["stratum"], []).append(r)

    say("--- eligible by stratum x language ---")
    for s in sorted(by_stratum):
        langs: dict[str, int] = {}
        for r in by_stratum[s]:
            langs[r["language"]] = langs.get(r["language"], 0) + 1
        say(f"  {s:14s} {len(by_stratum[s]):>4}  {dict(sorted(langs.items()))}")

    picked = []
    for s in sorted(by_stratum):
        pool = by_stratum[s]
        if args.balance_lang:
            # round-robin across languages so a stratum is not one language
            buckets: dict[str, list] = {}
            for r in pool:
                buckets.setdefault(r["language"], []).append(r)
            for b in buckets.values():
                b.sort(key=lambda r: -(r["stars"] or 0))
            pool, i = [], 0
            while len(pool) < len(kept) and any(buckets.values()):
                for lang in sorted(buckets):
                    if buckets[lang]:
                        pool.append(buckets[lang].pop(0))
                i += 1
        else:
            pool.sort(key=lambda r: -(r["stars"] or 0))
        picked += pool[: args.per_stratum] if args.per_stratum else pool

    with MANIFEST_OUT.open("w") as f:
        f.write("# Corpus manifest (TSV): name\turl\tcategory\tfile_filter\tsize_class\n")
        f.write(f"# generated {today()} by select_corpus.py — "
                f"{len(picked)} projects; labels from control facts\n")
        f.write("# size_class: S < 30k commits | M 30k-150k | L > 150k\n")
        for r in picked:
            name = r["repo"].lower().replace(".", "-")
            f.write(f"{name}\t{r['clone_url']}\t{r['stratum']}\t"
                    f"{LANG_FILTER[r['language']]}\t{r['size_class']}\n")
    say(f"wrote {MANIFEST_OUT} ({len(picked)} projects)")

    counts: dict[tuple[str, str], int] = {}
    for r in picked:
        counts[(r["stratum"], r["size_class"])] = counts.get((r["stratum"], r["size_class"]), 0) + 1
    say("--- selected: stratum x size_class ---")
    for k in sorted(counts):
        say(f"  {k[0]:14s} {k[1]}  {counts[k]:>4}")
    contested = [r for r in picked if r["contested"]]
    say(f"contested cases kept and flagged: {len(contested)}")
    return 0


# ---------------------------------------------------------------- review


REVIEW_OUT = CORPUS / "docs" / "CORPUS-REVIEW.md"
REVIEW_SAMPLE = 25


def cmd_review(args: argparse.Namespace) -> int:
    """Write a human-reviewable digest of candidates.csv.

    24,405 rows are not reviewable by reading. This picks out the parts where a
    human can actually catch an error: the exclusion histogram, which is the
    PRISMA flow in table form; every contested case; the largest projects per
    stratum with the fact that labelled each one; and a sample of the rows whose
    label rests on the weakest fact. Reads candidates.csv, so it does not race a
    running `enrich`.
    """
    if not CANDIDATES.exists():
        say(f"no {CANDIDATES} — run `emit` first")
        return 1
    with CANDIDATES.open() as f:
        rows = list(csv.DictReader(f))
    kept = [r for r in rows if r["included"] == "True"]

    def yes(r: dict) -> bool:
        return r["included"] == "True"

    reasons = Counter()
    for r in rows:
        if r["excluded_because"]:
            # one row can carry several reasons; count each
            for part in r["excluded_because"].split("; "):
                reasons[re.sub(r"=.*", "=...", part)] += 1

    pending = sum(1 for r in rows if "not-enriched" in r["excluded_because"])
    out = [f"# Corpus review — {today()}", "",
           "Generated by `./select_corpus.py review`. Source of truth is",
           "`candidates.csv`, one row per candidate with its exclusion reason.", "",
           f"- candidates: **{len(rows):,}**",
           f"- eligible: **{len(kept):,}**",
           f"- excluded: **{len(rows) - len(kept):,}**", ""]
    if pending:
        out += [f"> **This is a partial snapshot.** {pending:,} candidates "
                f"({100 * pending / len(rows):.0f}%) are still waiting for the",
                "> `enrich` pass, so they count as excluded for now. Re-run `emit`",
                "> then `review` when enrichment finishes. Do not freeze the corpus",
                "> from this file.", ""]

    out += ["## 1. Eligible by stratum, language and size", "",
            "| Stratum | Total | C | C++ | Java | Rust | S | M | L |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
    langs = ("C", "C++", "Java", "Rust")
    for s in sorted({r["stratum"] for r in kept}):
        g = [r for r in kept if r["stratum"] == s]
        lc = [sum(1 for r in g if r["language"] == x) for x in langs]
        sc = [sum(1 for r in g if r["size_class"] == x) for x in "SML"]
        out.append(f"| {s} | {len(g)} | " + " | ".join(str(n) for n in lc + sc) + " |")

    out += ["", "## 2. Why candidates were excluded", "",
            "**This table is the PRISMA flow.** A row can carry more than one",
            "reason, so the counts overlap.", "",
            "| Reason | Candidates |", "| --- | --- |"]
    for reason, n in reasons.most_common():
        out.append(f"| `{reason}` | {n:,} |")

    out += ["", "## 3. Where each label came from", "",
            "| Fact | Eligible | Meaning |", "| --- | --- | --- |"]
    fact_help = {
        "F1_roster": "the project appears on a foundation or trust roster",
        "F1_pool:spinellis-cohort": "**weakest.** Published cohort with no enterprise "
                                    "signal found. An ABSENCE, not a positive fact",
        "F1_pool:spinellis": "published enterprise dataset, SEC or Fortune matched",
        "F2_org": "a company GitHub namespace owns the repository",
        "F2_org_verified": "a verified company namespace that names a company",
        "F2_org_foundation": "a foundation namespace owns the repository",
        "F1_residual:none": "**no fact at all.** Community by residual",
    }
    fact_counts = Counter(r["fact"].split(":")[0] if r["fact"].startswith("F2")
                          else r["fact"].rsplit(":", 1)[0] if r["fact"].count(":") > 1
                          else r["fact"] for r in kept)
    for fact, n in fact_counts.most_common():
        # Longest match wins, so F2_org_foundation does not read as F2_org.
        keys = sorted((k for k in fact_help if fact.startswith(k)), key=len)
        out.append(f"| `{fact}` | {n:,} | {fact_help.get(keys[-1], '') if keys else ''} |")

    contested = [r for r in rows if r["contested"]]
    out += ["", f"## 4. Contested cases — {len(contested)}", "",
            "Two facts disagree. These are kept and flagged, never resolved",
            "silently. **Review every one.**", "",
            "| Project | Stratum | Conflict | Eligible |", "| --- | --- | --- | --- |"]
    for r in sorted(contested, key=lambda r: r["owner"].lower()):
        out.append(f"| `{r['owner']}/{r['repo']}` | {r['stratum']} | "
                   f"{r['contested']} | {'yes' if yes(r) else 'no'} |")

    out += ["", f"## 5. Largest {REVIEW_SAMPLE} per stratum", "",
            "Sorted by stars. A mislabel here costs the most, because these",
            "projects carry the most tokens.", ""]
    for s in sorted({r["stratum"] for r in kept}):
        g = sorted((r for r in kept if r["stratum"] == s),
                   key=lambda r: -int(r["stars"] or 0))[:REVIEW_SAMPLE]
        out += [f"### {s}", "",
                "| Project | Stars | Lang | Commits | Labelled by |",
                "| --- | --- | --- | --- | --- |"]
        for r in g:
            out.append(f"| `{r['owner']}/{r['repo']}` | {int(r['stars'] or 0):,} | "
                       f"{r['language']} | {int(r['commits'] or 0):,} | `{r['fact']}` |")
        out.append("")

    weak = sorted((r for r in kept if r["source"] == "cohort"),
                  key=lambda r: -int(r["stars"] or 0))[:REVIEW_SAMPLE]
    out += [f"## 6. Weakest evidence — {REVIEW_SAMPLE} of "
            f"{sum(1 for r in kept if r['source'] == 'cohort')} cohort rows", "",
            "These are labelled `community` because a published heuristic found",
            "**no enterprise signal**. That is an absence of evidence. If any of",
            "these looks company-run to you, the stratum needs a positive fact.", "",
            "| Project | Stars | Lang | Labelled by |", "| --- | --- | --- | --- |"]
    for r in weak:
        out.append(f"| `{r['owner']}/{r['repo']}` | {int(r['stars'] or 0):,} | "
                   f"{r['language']} | `{r['fact']}` |")

    out += ["", "## 7. Slicing the raw data yourself", "",
            "```sh",
            "# everything a stratum holds",
            "awk -F, '$2==\"foundation\" && $21==\"True\"' candidates.csv | less",
            "",
            "# one project's full row, all 23 columns",
            "head -1 candidates.csv | tr ',' '\\n' | nl   # column numbers",
            "grep -n '^.*,linux,' candidates.csv",
            "",
            "# every project excluded for one reason",
            "grep 'stale=' candidates.csv | wc -l",
            "```", ""]

    REVIEW_OUT.parent.mkdir(parents=True, exist_ok=True)
    REVIEW_OUT.write_text("\n".join(out) + "\n")
    say(f"wrote {REVIEW_OUT} ({len(out)} lines)")
    say(f"  {len(kept):,} eligible of {len(rows):,}; {len(contested)} contested")
    return 0


def cmd_all(args: argparse.Namespace) -> int:
    return cmd_rosters(args) or cmd_enrich(args) or cmd_emit(args)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("rosters").set_defaults(fn=cmd_rosters)
    sub.add_parser("review", help="write docs/CORPUS-REVIEW.md for a human to read"
                   ).set_defaults(fn=cmd_review)
    e = sub.add_parser("enrich")
    e.add_argument("--limit", type=int, default=0)
    e.add_argument("--retry-errors", action="store_true",
                   help="drop cached failures and fetch them again")
    e.set_defaults(fn=cmd_enrich)
    for name, fn in (("emit", cmd_emit), ("all", cmd_all)):
        p = sub.add_parser(name)
        p.add_argument("--per-stratum", type=int, default=0,
                       help="cap projects per stratum (0 = keep all eligible)")
        p.add_argument("--balance-lang", action="store_true",
                       help="round-robin languages within a stratum")
        p.add_argument("--limit", type=int, default=0)
        p.add_argument("--retry-errors", action="store_true",
                       help="enrich stage: drop cached failures and fetch again")
        p.set_defaults(fn=fn)
    args = ap.parse_args()
    CACHE.mkdir(parents=True, exist_ok=True)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
