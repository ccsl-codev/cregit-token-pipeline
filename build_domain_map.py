#!/usr/bin/env python3
"""build_domain_map — widen the domain->firm map from public affiliation data.

    ./build_domain_map.py fetch          # cache the cncf/gitdm affiliation files
    ./build_domain_map.py build          # emit data/affiliation.merged.csv
    ./build_domain_map.py build --min-persons 3 --report

Source: cncf/gitdm developers_affiliations{1..10}.txt (grey literature; the CNCF
DevStats affiliation list). We read it for one thing only: which e-mail DOMAIN
belongs to which company. No person, name or e-mail address is carried into the
output, so the upstream per-person opt-out does not reach this artifact.

The upstream file is person-grain, so projecting it onto domains is lossy: in
file 1 of 10, 638 of 1116 domains (57%) name more than one company, because a
contributor's employer gets attached to their personal domain. Four rules clean
that up:

  R1 free providers never become a firm     -> (Independent), kind=free_provider
  R2 a company domain is shared            -> require >= MIN_PERSONS distinct people
  R3 one company must clearly win          -> top company > 50% of the domain's people
  R4 the company must look like a company  -> drop descriptions and self-references

The curated kernel map always wins on conflict — it was hand-checked for the
VEM paper and carries identity-level corrections this source cannot express.

Output matches the existing schema exactly: domain,company,kind,source
`source` is `gitdm` (curated, pre-existing), `patch` (curated) or `cncf-gitdm`
(imported here), so provenance stays visible per row.

Stdlib only.
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

CORPUS = Path(__file__).resolve().parent
CACHE = CORPUS / ".corpus-cache" / "affil"
DATA = CORPUS / "data"
OUT = DATA / "affiliation.merged.csv"

# The curated map from the VEM paper. Authoritative on any conflict.
CURATED = Path("/local/home/ellianco/Projects/cregit-workspace/"
               "cbsoft-vem2026-corporate-truck-factor/pipeline/data/affiliation.csv")

SRC_URL = ("https://raw.githubusercontent.com/cncf/gitdm/master/"
           "developers_affiliations{}.txt")
N_FILES = 10

MIN_PERSONS = 2          # R2
PLURALITY = 0.50         # R3

# R1. A free provider is never a firm; signals.py must keep these Independent.
FREE_PROVIDERS = {
    "gmail.com", "googlemail.com", "outlook.com", "hotmail.com", "hotmail.co.uk",
    "yahoo.com", "yahoo.co.jp", "yahoo.co.uk", "ymail.com", "aol.com", "gmx.de",
    "gmx.net", "gmx.com", "web.de", "mail.ru", "yandex.ru", "yandex.com",
    "qq.com", "163.com", "126.com", "sina.com", "sina.cn", "foxmail.com",
    "live.com", "live.co.uk", "msn.com", "icloud.com", "me.com", "mac.com",
    "protonmail.com", "protonmail.ch", "proton.me", "pm.me", "tutanota.com",
    "fastmail.com", "fastmail.fm", "zoho.com", "posteo.de", "posteo.net",
    # only the noreply forms are free; @github.com is a GitHub employee address
    "users.noreply.github.com", "noreply.github.com",
    "example.com", "localhost", "localhost.localdomain", "none", "(none)",
}

# R4. Values that are descriptions, placeholders or non-firms, not company names.
NOT_A_FIRM = re.compile(
    r"^\(?\s*(independent|unknown|none|n/?a|\?+|self|freelance|freelancer|"
    r"student|retired|individual|private|personal|no ?company|notfound)\s*\)?$",
    re.I)
DESCRIPTIVE = re.compile(
    r"\b(based|hosting company|consulting company|various|multiple|several|"
    r"unaffiliated|not ?disclosed|undisclosed)\b", re.I)

# Legal-form suffixes stripped before comparing two spellings of one company.
SUFFIX = re.compile(
    r"[,\s]+(inc|inc\.|llc|l\.l\.c\.|ltd|ltd\.|limited|gmbh|ag|s\.?a\.?|"
    r"b\.?v\.?|n\.?v\.?|plc|co|co\.|corp|corp\.|corporation|company|"
    r"pty|pte|oy|ab|as|aps|srl|s\.r\.l\.|kg|kgaa|sas|sarl)$", re.I)


def say(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def norm_company(name: str) -> str:
    """Collapse spellings so 'Axis Communications AB' and 'Axis Communications'
    are one company. Returns '' when the value is not a company at all."""
    c = re.sub(r"\s+", " ", (name or "").strip().strip(",;"))
    c = re.sub(r"\s*\([^)]*\)\s*$", "", c)          # trailing parenthetical
    if not c or NOT_A_FIRM.match(c) or DESCRIPTIVE.search(c):
        return ""
    prev = None
    while prev != c:                                 # 'Foo Inc. Ltd' -> 'Foo'
        prev = c
        c = SUFFIX.sub("", c).strip().strip(",")
    return c if len(c) > 1 else ""


def domain_root(d: str) -> str:
    """'opensource.cirrus.com' -> 'cirrus'; used only to spot self-references."""
    parts = [p for p in d.lower().split(".") if p]
    return parts[-2] if len(parts) >= 2 else (parts[0] if parts else "")


def cmd_fetch(args: argparse.Namespace) -> int:
    CACHE.mkdir(parents=True, exist_ok=True)
    total = 0
    for i in range(1, N_FILES + 1):
        dest = CACHE / f"developers_affiliations{i}.txt"
        if dest.exists() and dest.stat().st_size > 0:
            total += dest.stat().st_size
            continue
        try:
            req = urllib.request.Request(SRC_URL.format(i),
                                         headers={"User-Agent": "cregit-token-pipeline"})
            with urllib.request.urlopen(req, timeout=120) as r:
                dest.write_bytes(r.read())
            total += dest.stat().st_size
            say(f"developers_affiliations{i}.txt  {dest.stat().st_size:>9,} bytes")
        except Exception as e:
            say(f"developers_affiliations{i}.txt  FAILED: {e}")
    say(f"cached {total:,} bytes in {CACHE}")
    return 0


def parse_source() -> dict[str, Counter]:
    """domain -> Counter(company -> people attributing that company to it).

    Format: a person line 'handle: a!dom.com, b!dom2.com', then TAB-indented
    affiliation lines, each optionally '... until DATE' / '... from DATE'.
    Only the domain and the company survive; the handle and address do not.

    Attribution rule (this is the load-bearing choice). A naive pass credits
    every employer a person ever had to every domain they ever used, and 54% of
    people here have more than one employer — that dilution made 5,426 of 10,118
    domains fail a majority test. So a person contributes to the map only when
    the mapping is unambiguous for them: exactly one non-free domain, and
    exactly one company. 64% of people list only free providers and contribute
    nothing either way.
    """
    people: list[tuple[set[str], list[str]]] = []
    files = sorted(CACHE.glob("developers_affiliations*.txt"))
    if not files:
        say("no cached source — run `fetch` first")
        return {}
    for f in files:
        domains: set[str] = set()
        companies: list[str] = []
        for line in f.read_text(errors="replace").splitlines():
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            if not line[0].isspace():
                if domains:
                    people.append((domains, companies))
                domains, companies = set(), []
                for e in line.split(":", 1)[1].split(",") if ":" in line else []:
                    e = e.strip()
                    if "!" in e:
                        d = e.split("!", 1)[1].strip().lower().rstrip(".")
                        if d and "." in d:
                            domains.add(d)
            elif domains:
                # strip the validity range; we keep no dates in a domain map
                co = norm_company(re.split(r"\s+(?:until|from)\s+", line.strip())[0])
                if co:
                    companies.append(co)
        if domains:
            people.append((domains, companies))

    per_domain: dict[str, Counter] = defaultdict(Counter)
    used = 0
    for domains, companies in people:
        work = [d for d in domains if d not in FREE_PROVIDERS]
        if len(work) != 1 or len(set(companies)) != 1:
            continue
        per_domain[work[0]][companies[0]] += 1
        used += 1
    say(f"parsed {len(files)} files, {len(people):,} persons; "
        f"{used:,} unambiguous ({100 * used / max(len(people), 1):.0f}%) "
        f"-> {len(per_domain):,} domains")
    return per_domain


SPINELLIS_TSV = CORPUS / "sources" / "enterprise_projects.txt"
# field index -> name, for the 29-column headerless TSV
SP_DOMAIN, SP_FG500, SP_10K, SP_20F, SP_COMPANY = 15, 21, 22, 23, 26


def parse_spinellis_domains() -> dict[str, tuple[str, str]]:
    """domain -> (company, source) from Spinellis et al. MSR 2020.

    Zenodo 10.5281/zenodo.3742962, CC-BY-4.0 — unlike the CNCF source this is
    licensed for redistribution with attribution. A row's `dominant_domain` plus
    its SEC/Fortune-matched `company_name` is a company fact, and where the
    company is an SEC filer or Fortune Global 500 member the pairing is
    externally verifiable, which is the best provenance in this map.

    Our own filters still apply: the company name is normalised, free providers
    never become a firm, and a "company" that is itself a domain is dropped.
    """
    if not SPINELLIS_TSV.exists():
        say("spinellis: sources/enterprise_projects.txt absent, skipped")
        return {}
    verified: dict[str, Counter] = defaultdict(Counter)
    plain: dict[str, Counter] = defaultdict(Counter)
    for line in SPINELLIS_TSV.read_text(errors="replace").splitlines():
        f = line.split("\t")
        if len(f) <= SP_COMPANY:
            continue
        d = f[SP_DOMAIN].strip().lower()
        co = norm_company(f[SP_COMPANY])
        if not d or "." not in d or not co or d in FREE_PROVIDERS:
            continue
        if re.fullmatch(r"[a-z0-9.-]+\.[a-z]{2,}", co.lower()):
            continue
        sec = any(f[i].strip().lower() == "t" for i in (SP_FG500, SP_10K, SP_20F))
        (verified if sec else plain)[d][co] += 1
    out: dict[str, tuple[str, str]] = {}
    for d, c in plain.items():
        if len(c) == 1:
            out[d] = (c.most_common(1)[0][0], "spinellis")
    for d, c in verified.items():          # verified wins over plain
        if len(c) == 1:
            out[d] = (c.most_common(1)[0][0], "spinellis-sec")
    n_sec = sum(1 for v in out.values() if v[1] == "spinellis-sec")
    say(f"spinellis: {len(out)} unambiguous domains ({n_sec} SEC/Fortune-verified)")
    return out


def load_curated() -> dict[str, tuple[str, str, str]]:
    if not CURATED.exists():
        say(f"WARNING curated map not found at {CURATED}")
        return {}
    out = {}
    with CURATED.open() as f:
        for row in csv.DictReader(f):
            d = (row.get("domain") or "").strip().lower()
            if d:
                out[d] = (row.get("company") or "", row.get("kind") or "company",
                          row.get("source") or "gitdm")
    say(f"curated map: {len(out)} domains (authoritative on conflict)")
    return out


def cmd_build(args: argparse.Namespace) -> int:
    per_domain = parse_source()
    if not per_domain:
        return 1
    curated = load_curated()

    kept: dict[str, tuple[str, str, str]] = {}
    drop = Counter()
    for d, counter in per_domain.items():
        if d in curated:
            drop["curated-wins"] += 1
            continue
        if d in FREE_PROVIDERS:
            kept[d] = ("(Independent)", "free_provider", "cncf-gitdm")
            drop["R1-free-provider"] += 1
            continue
        total = sum(counter.values())
        company, n = counter.most_common(1)[0]
        if n / total <= PLURALITY:
            drop["R3-no-plurality"] += 1
            continue
        # R4 fires only when the "company" is itself a domain string, e.g.
        # `systemli.org -> systemli.org`, which names no company. It must NOT
        # fire on a company legitimately named after its domain: `github.com ->
        # GitHub` and `mozilla.org -> Mozilla` are correct and must survive.
        if re.fullmatch(r"[a-z0-9.-]+\.[a-z]{2,}", company.strip().lower()):
            drop["R4-company-is-a-domain"] += 1
            continue
        # R2 is a confidence tier, not a filter. Most company domains here are
        # attested by exactly one person, so excluding them costs ~92% of the
        # yield; but a one-person domain may be that person's personal domain
        # carrying their employer. Both tiers ship, distinguished by `source`,
        # so a consumer can restrict to the corroborated tier.
        if total >= args.min_persons:
            kept[d] = (company, "company", "cncf-gitdm")
        else:
            kept[d] = (company, "company", "cncf-gitdm-single")
            drop["R2-single-person (kept, flagged)"] += 1

    # Precedence, strongest provenance last so it wins:
    #   cncf-gitdm* (single/corroborated) < spinellis (CC-BY) < spinellis-sec
    #   (externally verifiable) < curated (hand-checked for the VEM paper)
    merged = dict(kept)
    for d, (co, src) in parse_spinellis_domains().items():
        if src == "spinellis-sec" or d not in merged:
            merged[d] = (co, "company", src)
    merged.update(curated)

    DATA.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["domain", "company", "kind", "source"])
        for d in sorted(merged):
            co, kind, src = merged[d]
            w.writerow([d, co, kind, src])

    by_src = Counter(v[2] for v in merged.values())
    say("--- rules applied ---")
    for k in sorted(drop):
        say(f"  {k:34s} {drop[k]:>6,}")
    say("--- result by source ---")
    for src in sorted(by_src):
        say(f"  {src:34s} {by_src[src]:>6,}")
    say(f"  {'TOTAL':34s} {len(merged):>6,}"
        f"   ({len(merged) / max(len(curated), 1):.1f}x the curated map)")
    corroborated = len(curated) + by_src.get("cncf-gitdm", 0)
    say(f"  {'of which corroborated':34s} {corroborated:>6,}")
    say(f"wrote {OUT}")

    if args.report:
        say("--- 15 largest imported companies by domain count ---")
        by_co = Counter(v[0] for v in merged.values()
                        if v[2].startswith("cncf-gitdm"))
        for co, n in by_co.most_common(15):
            say(f"  {n:>4}  {co}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("fetch").set_defaults(fn=cmd_fetch)
    b = sub.add_parser("build")
    b.add_argument("--min-persons", type=int, default=MIN_PERSONS,
                   help="R2: distinct people needed to call a domain a company")
    b.add_argument("--report", action="store_true")
    b.set_defaults(fn=cmd_build)
    return ap.parse_args().fn(ap.parse_args())


if __name__ == "__main__":
    sys.exit(main())
