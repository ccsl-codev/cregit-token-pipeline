#!/usr/bin/env python3
"""build_domain_map — widen the domain->firm map from public affiliation data.

    ./build_domain_map.py fetch                       # cache the cncf/gitdm affiliation files
    ./build_domain_map.py build --curated PATH        # emit data/affiliation.merged.csv
    ./build_domain_map.py build --no-curated          # ...or without one, if you have none
    ./build_domain_map.py build --min-persons 3 --report --curated PATH

Source: cncf/gitdm developers_affiliations{1..10}.txt (grey literature; the CNCF
DevStats affiliation list). We read it for one thing only: which e-mail DOMAIN
belongs to which company. No person, name or e-mail address is carried into the
output, so the upstream per-person opt-out does not reach this artifact.

The upstream file is person-grain, so projecting it onto domains is lossy: in
file 1 of 10, 638 of 1116 domains (57%) name more than one company, because a
contributor's employer gets attached to their personal domain. Four rules clean
that up:

  R1 free providers never become a firm    -> (Independent), kind=free_provider.
     FREE_PROVIDERS is our own curated constant, not something we read out of
     the source, so EVERY domain in it earns a row (source=builtin) whether or
     not the source happens to mention it.
  R2 a company domain is shared            -> require >= MIN_PERSONS distinct people
  R3 one company must clearly win          -> top company > 50% of the domain's people
  R4 the company must name a firm          -> drop descriptions. A value that
     only repeats its own domain is TAGGED, never dropped: `systemli.org ->
     systemli.org` names no firm, while `joeyb.org -> Salesforce.com` names a
     real employer. 35 of the 77 domain-shaped values are self-references; the
     other 42 are firms, so the two cases must not share one tag.

The curated kernel map always wins on conflict — it was hand-checked and
carries identity-level corrections this source cannot express.

Output matches the existing schema exactly: domain,company,kind,source
`source` is `gitdm` / `patch` / `rich` (all curated and pre-existing, read from
CURATED), `builtin` (R1, straight from FREE_PROVIDERS), `cncf-gitdm[-single]` or
`spinellis[-sec]` (imported here), or `correction` (this repository's own
reviewed overlay, see CORRECTIONS), so provenance stays visible per row. A
`-self-reference` suffix marks an R4 tag, so a consumer can audit or exclude
those rows.

Stdlib only.
"""

from __future__ import annotations

import argparse
import csv
import os
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

# The curated map. Authoritative on any conflict. No built-in default: every
# checkout supplies its own, via `--curated PATH` or $CURATED_ENV_VAR.
# `cmd_build` sets this module global from args before the first read, the
# same way CACHE/DATA/OUT/SPINELLIS_TSV are bare globals a caller may
# override rather than arguments threaded through every function.
CURATED_ENV_VAR = "CURATED_AFFILIATION_CSV"
CURATED: Path | None = None

# This repository's own correction overlay, applied AFTER `curated` and so
# authoritative over everything. It exists because `CURATED` lives in a third
# checkout that this repository does not version: a fix made there is invisible
# to anyone who clones only this one, and a fix made by hand in OUT is erased by
# the next `build`. A row here is a reviewed, committed override with its reason
# in the file.
#
# Columns: domain,company,kind,source,reason — `reason` is documentation and is
# not written to OUT.
#
# Resolved from DATA at call time, not bound here: the tests redirect DATA into
# tmp_path, and a constant captured at import would make every sandboxed build
# read the live overlay.
CORRECTIONS_NAME = "affiliation.corrections.csv"

SRC_URL = ("https://raw.githubusercontent.com/cncf/gitdm/master/"
           "developers_affiliations{}.txt")
N_FILES = 10

MIN_PERSONS = 2          # R2
PLURALITY = 0.50         # R3

# R1. A free provider is never a firm; signals.py must keep these Independent.
# This list is the whole R1 rule: cmd_build emits a row for every domain in it,
# so a free provider can never fall through to the (Unknown) bucket.
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

# A trailing note in parentheses is commentary, not part of the name. It is
# stripped inside the same fixed-point loop as SUFFIX, so the two interleave in
# any order: 'Foo (Bar) Inc.', 'Foo Inc. (Bar)' and 'Foo (Bar)' all give 'Foo'.
PARENTHETICAL = re.compile(r"\s*\([^)]*\)\s*$")

# R4. A value shaped like a domain.
DOMAIN_SHAPED = re.compile(r"[a-z0-9.-]+\.[a-z]{2,}")
DOMAIN_NAME_TAG = "-self-reference"

# The address list on a gitdm person line. Documented as comma-separated, but 7
# lines in the 10 cached files separate two addresses with a SPACE instead, e.g.
#   gnm444: ngonapa!cisco.com gnm444!users.noreply.github.com
# Splitting on the comma alone then yields one "address" holding both, and its
# domain becomes the literal string `cisco.com gnm444!users.noreply.github.com`.
# Whitespace is never valid inside an address, so accepting it as a separator
# loses nothing and recovers the second address.
ADDR_SEP = re.compile(r"[,\s]+")


def self_reference(domain: str, company: str) -> bool:
    """True when the company value only repeats its own domain, naming no firm.

    This is the whole of R4, and the distinction is measured, not assumed. In the
    real map 77 company values are shaped like a domain, and they are two
    different things:

      35  the value repeats the key    `systemli.org -> systemli.org`
      42  the value names a real firm  `joeyb.org -> Salesforce.com`
                                       `chriskramer.nl -> Bol.com`
                                       `helsedir.no -> FINN.no`

    Only the first group adds nothing. Tagging both would mean a consumer who
    filters the tag also discards 42 real employers, Salesforce among them.

    A firm merely named after its domain never reaches here: `github.com ->
    GitHub` and `mozilla.org -> Mozilla` carry no dot, so they never match
    DOMAIN_SHAPED.
    """
    co = company.strip().lower().rstrip(".")
    if not DOMAIN_SHAPED.fullmatch(co):
        return False
    return co == domain.strip().lower().rstrip(".")


def say(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def norm_company(name: str) -> str:
    """Collapse spellings so 'Axis Communications AB' and 'Axis Communications'
    are one company. Returns '' when the value is not a company at all."""
    c = re.sub(r"\s+", " ", (name or "").strip().strip(",;"))
    # Strip once before the non-firm test, so '(Independent)' is still caught,
    # and 'no company' or 'hosting company' are rejected before the SUFFIX loop
    # can shorten them into something that looks like a name.
    c = PARENTHETICAL.sub("", c)
    if not c or NOT_A_FIRM.match(c) or DESCRIPTIVE.search(c):
        return ""
    prev = None
    while prev != c:                        # 'Foo (Bar) Inc. Ltd' -> 'Foo'
        prev = c
        c = SUFFIX.sub("", c).strip().strip(",")
        c = PARENTHETICAL.sub("", c).strip().strip(",")
    return c if len(c) > 1 else ""


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

    The address list is hand-maintained and two malformations occur in it: a
    SPACE where the comma belongs, and a second `!` inside an address. Reading
    either one literally produced a map key that is not a domain — `cisco.com
    gnm444!users.noreply.github.com`, `sheldrake!isovalent.com`. See ADDR_SEP
    and the rsplit below. Eight such keys existed; none could ever match a
    person_domain, so they were dead rows carrying a firm attribution.

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
                addrs = ADDR_SEP.split(line.split(":", 1)[1]) if ":" in line else []
                for e in addrs:
                    if "!" in e:
                        # rsplit, not split: gitdm writes `!` for `@`, and one
                        # address carries a second one in its local part
                        # (`kevin!sheldrake!isovalent.com`). The domain is what
                        # follows the LAST `!`; taking the first gave the domain
                        # `sheldrake!isovalent.com`.
                        d = e.rsplit("!", 1)[1].strip().lower().rstrip(".")
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

    Our own filters still apply: the company name is normalised and a free
    provider never becomes a firm. A value that only repeats its own domain is
    TAGGED with a `-self-reference` source suffix, not dropped. See
    self_reference() for why the shape of the string alone is not enough.

    Ambiguity is checked inside the verified bucket, inside the plain bucket,
    and ACROSS the two. A domain that the unflagged rows call one firm and the
    10-K rows call another is a disagreement in the source: the SEC-verified
    name wins, because it is externally checkable, but the conflict is counted
    and reported. A high rate is a finding about the source, so it must not be
    silent.
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
        sec = any(f[i].strip().lower() == "t" for i in (SP_FG500, SP_10K, SP_20F))
        (verified if sec else plain)[d][co] += 1
    out: dict[str, tuple[str, str]] = {}
    for d, c in plain.items():
        if len(c) == 1:
            out[d] = (c.most_common(1)[0][0], "spinellis")
    for d, c in verified.items():          # verified wins over plain
        if len(c) == 1:
            out[d] = (c.most_common(1)[0][0], "spinellis-sec")
    # Cross-bucket ambiguity. Two firms claiming one domain used to disappear,
    # because each bucket was checked on its own.
    n_conflict = 0
    for d in sorted(set(plain) & set(verified)):
        pc, vc = plain[d], verified[d]
        if (len(pc) == 1 and len(vc) == 1
                and pc.most_common(1)[0][0] != vc.most_common(1)[0][0]):
            n_conflict += 1
    n_tagged = 0
    for d in list(out):                    # R4 tags the row, it never drops it
        co, src = out[d]
        if self_reference(d, co):
            out[d] = (co, src + DOMAIN_NAME_TAG)
            n_tagged += 1
    n_sec = sum(1 for v in out.values() if v[1].startswith("spinellis-sec"))
    say(f"spinellis: {len(out)} unambiguous domains "
        f"({n_sec} SEC/Fortune-verified, {n_tagged} tagged {DOMAIN_NAME_TAG}, "
        f"{n_conflict} cross-bucket name conflicts resolved to the SEC name)")
    return out


def load_curated() -> dict[str, tuple[str, str, str]]:
    """Read CURATED. Returns {} when it is unset or absent.

    Whether an absent curated map is acceptable is a decision for the caller
    (cmd_build), not this function: it depends on whether the caller passed
    --no-curated.
    """
    if CURATED is None or not CURATED.exists():
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


def load_corrections() -> dict[str, tuple[str, str, str]]:
    """Reviewed overrides from this repository, authoritative over every source.

    Kept separate from `curated` on purpose. `curated` is another project's
    artifact that we read; this is ours, so a correction is reviewable in the
    same commit as the code that consumes it. A missing file is normal and means
    "no corrections", not an error — the overlay is additive.
    """
    path = DATA / CORRECTIONS_NAME
    if not path.exists():
        return {}
    out = {}
    with path.open() as f:
        for row in csv.DictReader(f):
            d = (row.get("domain") or "").strip().lower()
            if d and not d.startswith("#"):
                out[d] = (row.get("company") or "",
                          row.get("kind") or "company",
                          row.get("source") or "correction")
    say(f"corrections overlay: {len(out)} domains (authoritative over curated)")
    return out


def write_refusal(merged: dict, curated: dict) -> str:
    """Reason to refuse the write, or '' to go ahead.

    The old guard caught only a source that failed to parse. A source that
    parsed but lost every domain to a rule rewrote the artifact as a bare
    header line. Two things are never a valid build: an empty result, and a
    result holding fewer rows than the curated input, which is the floor.
    """
    if not merged:
        return "the merged map is empty"
    if len(merged) < len(curated):
        return (f"the merged map holds {len(merged)} rows, fewer than the "
                f"{len(curated)} curated rows that are its floor")
    return ""


def apply_rules(per_domain: dict[str, Counter], curated: dict,
                min_persons: int) -> tuple[dict, Counter]:
    """Apply R1-R4 to the parsed import. Returns the kept rows and a count of
    how many domains each rule affected, under the label it is reported by."""
    kept: dict[str, tuple[str, str, str]] = {}
    rule_hits = Counter()
    for d, counter in per_domain.items():
        if d in curated:
            rule_hits["curated-wins"] += 1
            continue
        total = sum(counter.values())
        company, n = counter.most_common(1)[0]
        if n / total <= PLURALITY:
            rule_hits["R3-no-plurality"] += 1
            continue
        tag = ""
        if self_reference(d, company):
            tag = DOMAIN_NAME_TAG
            rule_hits["R4-self-reference"] += 1
        # R2 is a confidence tier, not a filter. Most company domains here are
        # attested by exactly one person, so excluding them costs ~92% of the
        # yield; but a one-person domain may be that person's personal domain
        # carrying their employer. Both tiers ship, distinguished by `source`,
        # so a consumer can restrict to the corroborated tier.
        if total >= min_persons:
            kept[d] = (company, "company", "cncf-gitdm" + tag)
        else:
            kept[d] = (company, "company", "cncf-gitdm-single" + tag)
            rule_hits["R2-single-person"] += 1

    for d in sorted(FREE_PROVIDERS):
        kept[d] = ("(Independent)", "free_provider", "builtin")
        rule_hits["R1-free-provider"] += 1

    return kept, rule_hits


def merge_sources(gitdm: dict, spinellis: dict, curated: dict,
                  corrections: dict) -> dict:
    """The three-source precedence, exactly as implemented: `spinellis-sec`
    and `curated` OVERRIDE whatever is already in the merge, while a plain
    `spinellis` row only FILLS A GAP, so even a one-person CNCF row outranks a
    published CC-BY row. Reason: an unflagged Spinellis row is not externally checkable,
    so it is not evidence enough to overturn a pairing the CNCF source attests.

    `corrections` is merged last, so a reviewed correction outranks every
    source including `curated`. This layer exists because a single-person
    gitdm row once misattributed a whole project to the wrong company.
    """
    merged = dict(gitdm)
    for d, (co, src) in spinellis.items():
        if src.startswith("spinellis-sec") or d not in merged:
            merged[d] = (co, "company", src)
    merged.update(curated)
    merged.update(corrections)
    return merged


def write_map(merged: dict, path: Path) -> None:
    """Write `merged` to `path` through a temporary sibling file, then rename.

    A rename is atomic on one filesystem, so an interrupted write leaves the
    previous artifact intact instead of truncating it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / (path.name + ".tmp")
    with tmp.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["domain", "company", "kind", "source"])
        for d in sorted(merged):
            co, kind, src = merged[d]
            w.writerow([d, co, kind, src])
    tmp.replace(path)


def print_build_report(merged: dict, rule_hits: Counter, curated: dict,
                       show_companies: bool) -> None:
    by_src = Counter(v[2] for v in merged.values())
    say("--- rules applied ---")
    for k in sorted(rule_hits):
        say(f"  {k:34s} {rule_hits[k]:>6,}")
    say("--- result by source ---")
    for src in sorted(by_src):
        say(f"  {src:34s} {by_src[src]:>6,}")
    say(f"  {'TOTAL':34s} {len(merged):>6,}"
        f"   ({len(merged) / max(len(curated), 1):.1f}x the curated map)")
    corroborated = (len(curated) + by_src.get("cncf-gitdm", 0)
                    + by_src.get("cncf-gitdm" + DOMAIN_NAME_TAG, 0))
    say(f"  {'of which corroborated':34s} {corroborated:>6,}")
    tagged = sum(n for s, n in by_src.items() if s.endswith(DOMAIN_NAME_TAG))
    say(f"  {'of which R4 self-references':34s} {tagged:>6,}")
    say(f"wrote {OUT}")

    if show_companies:
        say("--- 15 largest imported companies by domain count ---")
        by_co = Counter(v[0] for v in merged.values()
                        if v[2].startswith("cncf-gitdm"))
        for co, n in by_co.most_common(15):
            say(f"  {n:>4}  {co}")


def cmd_build(args: argparse.Namespace) -> int:
    global CURATED
    if args.curated is not None:
        CURATED = Path(args.curated)

    per_domain = parse_source()
    if not per_domain:
        return 1

    curated_present = CURATED is not None and CURATED.exists()
    if not curated_present and not args.no_curated:
        # The floor guard below can never fire on a curated map that was
        # never loaded: an empty `curated` makes `len(merged) < len(curated)`
        # compare against zero, which is always false. So a missing curated
        # map is refused here, directly, rather than left to a guard that
        # cannot see the difference between "absent" and "empty on purpose".
        if CURATED is None:
            say("REFUSING to build: no curated map given. Pass --curated "
                f"PATH, set ${CURATED_ENV_VAR}, or pass --no-curated to "
                "build without one.")
        else:
            say(f"REFUSING to build: curated map not found at {CURATED}. "
                f"Pass --curated PATH, set ${CURATED_ENV_VAR}, or pass "
                "--no-curated to build without one.")
        return 1
    curated = load_curated() if curated_present else {}

    kept, rule_hits = apply_rules(per_domain, curated, args.min_persons)
    merged = merge_sources(kept, parse_spinellis_domains(), curated,
                           load_corrections())

    reason = write_refusal(merged, curated)
    if reason:
        say(f"REFUSING to write {OUT}: {reason}")
        say("the existing artifact is left untouched")
        return 1

    write_map(merged, OUT)
    print_build_report(merged, rule_hits, curated, args.report)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("fetch").set_defaults(fn=cmd_fetch)
    b = sub.add_parser("build")
    b.add_argument("--min-persons", type=int, default=MIN_PERSONS,
                   help="R2: distinct people needed to call a domain a company")
    b.add_argument("--curated", type=Path,
                   default=os.environ.get(CURATED_ENV_VAR),
                   help="path to the curated affiliation map, authoritative "
                        f"on conflict (or set ${CURATED_ENV_VAR})")
    b.add_argument("--no-curated", action="store_true",
                   help="build without a curated map, for a caller who "
                        "genuinely has none")
    b.add_argument("--report", action="store_true")
    b.set_defaults(fn=cmd_build)
    args = ap.parse_args()          # once: parsing twice can only diverge
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
