# Codebook v1 — control facts and strata

Discharges `CORPUS-HEURISTICS-PLAN.md` P2, which was planned and never written.

This document defines how a project gets its stratum. It is the design decision
the dataset rests on, because the stratum defines the population under study. If
this document is wrong, every result is wrong in the same direction.

A second rater must be able to label a project from this file alone.

---

## 1. The rule the whole design turns on

**A stratum label comes from who *controls* a project. It never comes from who
*contributes* to it.**

The pipeline measures contribution composition, per firm, per token. If a
project were labelled "enterprise" because the pipeline measured a high firm
share, the finding would be true by construction. So the label must come from
governance evidence that exists independently of any commit.

Consequence, and it is intended: a project can sit off the diagonal. The Linux
kernel is the most corporate-funded system in the corpus and labels **community**
by control, because no company owns its trademark, its namespace, or its
maintainer appointments. That off-diagonal is a **result**, not an error.

## 2. Unit of labelling

One **repository**, identified by `owner/repo` on GitHub. Not an organisation,
not a product, not a foundation.

A company can own one repository and not another in the same namespace. An
organisation label would hide that.

## 3. The three strata

| Stratum | Operational definition |
| --- | --- |
| **`foundation`** | A non-profit foundation or trust holds the project's trademark or assets, and appoints or ratifies its maintainers. |
| **`company-owned`** | A single for-profit company owns the namespace, the trademark, or the copyright, and can change the licence or the roadmap without asking anyone. |
| **`community`** | Neither of the above. No single legal entity controls the project. Maintainership is earned inside the project. |

`community` is a **residual**. Say so in the paper. Section 9 explains why that
is a limitation and what it costs.

> **Naming.** The third stratum is `company-owned`, not `single-vendor`. F2 proves
> only that one company owns the namespace. True single-vendor control needs F3,
> the CLA, which is not implemented. `google/leveldb`, which takes outside
> patches under Apache-2.0, and a CLA-governed product repository both label
> `company-owned` today. Rename only after F3 runs.

## 4. The five control facts

### F1 — trademark owner *(strongest, automated)*

The project appears on the roster of an organisation that holds its mark.

| Source | What counts | Fact recorded |
| --- | --- | --- |
| Apache Software Foundation | a Top-Level Project on `projects.apache.org` | `F1_roster:asf` |
| Eclipse Foundation | a project in the Eclipse projects API | `F1_roster:eclipse` |
| CNCF | an entry in `landscape.yml` **carrying a `project:` key** — graduated, incubating, sandbox or archived | `F1_roster:cncf-<level>` |
| Software Freedom Conservancy, SPI | a member project | `F1_roster:sfc`, `F1_roster:spi` |
| mailing-list culture | a hand-listed anchor, see `COMMUNITY_SEED` | `F1_roster:seed` |

**What does NOT count.** An entry in the CNCF landscape *without* a `project:`
key. The landscape catalogues the whole cloud-native ecosystem: 988 repository
URLs, of which only 256 are CNCF-hosted. Reading all 988 as a roster put
`postgres/postgres` and `redis/redis` in the foundation stratum. PostgreSQL is
the canonical mailing-list community project. Redis Ltd relicensed Redis in
2024. Catalogue membership is a fine **candidate pool** and no control fact at
all.

### F2 — namespace owner *(automated)*

The GitHub organisation that owns the repository.

| Condition | Fact recorded |
| --- | --- |
| the login is in the hand-seeded `COMPANY_ORGS` | `F2_org:<login>` |
| the login is a **verified** GitHub Organization whose profile names a company | `F2_org_verified:<login>` |
| the login is in `FOUNDATION_ORGS` | `F2_org_foundation:<login>` |

**What does NOT count.** An unverified organisation. A personal user account —
see the gap in section 9.

### F3 — CLA or DCO *(NOT IMPLEMENTED)*

A Contributor Licence Agreement assigning copyright to one named company is the
strongest evidence of single-vendor control, because it makes relicensing
unilateral. A DCO is the opposite signal: it keeps copyright with the author.

Look for `CLA.md`, a CLA bot in pull requests, or a `Contributor License
Agreement` link in `CONTRIBUTING.md`.

**A human rater must apply F3 by hand.** The pipeline does not.

### F4 — maintainer appointment *(NOT IMPLEMENTED)*

Who may add a committer. Read `GOVERNANCE.md`, `MAINTAINERS`, or the charter.

| Evidence | Points to |
| --- | --- |
| a company employee or role appoints committers | `company-owned` |
| a foundation board or PMC ratifies | `foundation` |
| existing committers vote | `community` |

### F5 — maintainer employment *(tie-breaker only)*

The share of listed maintainers employed by one firm.

**Use F5 last and only to break a tie.** It is the closest fact to contribution
composition, so leaning on it reintroduces the circularity that section 1
forbids. Never let F5 override F1 or F2.

## 5. Decision procedure

Apply in order. Stop at the first fact that decides.

1. **F1 with a positive roster fact** → that roster's stratum. Record the fact.
2. **No F1 fact**, or only a *pool* fact (`F1_pool:*`) → the label is **open**.
   Continue.
3. **F2 foundation namespace** and the label is open → `foundation`.
4. **F2 company namespace** and the label is open → `company-owned`.
5. **F2 company namespace** and a positive F1 foundation fact → **contested**.
   Keep the F1 label, set the `contested` column, resolve by hand. See section 6.
6. **Nothing decided** → `community` by residual. Record `F1_residual:none`.
7. A human rater then applies **F3**, then **F4**, then **F5** as a tie-breaker.

Every row records the fact that decided it, and the date. Re-labelling later is
allowed; overwriting the date is not.

### Pool facts are not control facts

| Pool | Meaning | Label effect |
| --- | --- | --- |
| `F1_pool:spinellis-fg500`, `-sec10k`, `-sec20f` | a published heuristic matched the project to an SEC filer or Fortune Global 500 firm | **none.** Candidate only |
| `F1_pool:spinellis-cohort` | the same authors' heuristics found **no** enterprise signal | **none.** An absence of evidence |
| `F1_pool:cncf-landscape` | catalogued in the CNCF landscape, not CNCF-hosted | **none** |

Rows carrying a pool fact are marked `weak=True`, which means F2 may relabel
them freely.

Why pools are never labels: their own labels come from commit composition, which
is circular with what we measure. The published enterprise dataset reports
**κ = 0.29** and precision only, with recall unevaluated. Empirically, **41 of
MongoDB's 42 repositories sit in its non-enterprise cohort**, while MongoDB Inc.
is an SEC filer. A pool that misplaces MongoDB cannot be a label.

## 6. Contested-case protocol

Two facts disagree.

1. Keep the stronger fact's label. F1 outranks F2.
2. Write both facts into the `contested` column.
3. Never resolve it silently.
4. A human reads `docs/CORPUS-REVIEW.md` section 4 and decides.
5. Contested projects go into the κ pilot on purpose. They are where raters
   disagree, so excluding them would inflate agreement.

Example: `alibaba/nacos` was `F1_roster:cncf vs F2_org:alibaba`. Once the CNCF
landscape stopped counting as a roster, the conflict dissolved and F2 decided:
`company-owned`.

## 7. Relicense time-boxing

A project that changed licence changed its control regime. Terraform in 2023,
Redis in 2024, MongoDB earlier.

**Rule.** Label the regime that held during the window under analysis, and record
the relicense date. Do not label a project `company-owned` for its whole history
because of a change in its last year.

**Not implemented.** No time-boxing exists in the pipeline today. A rater must
apply it by hand, and the paper must state the gap.

## 8. Worked examples

Real rows from `candidates.csv`.

| Project | Stratum | Fact | Why |
| --- | --- | --- | --- |
| `kubernetes/kubernetes` | foundation | `F1_roster:cncf-graduated` | CNCF holds the mark. Excluded from the corpus for language: Go |
| `postgres/postgres` | community | `F1_pool:cncf-landscape` | catalogued, not hosted. No entity controls it |
| `redis/redis` | company-owned | `F2_org:redis` | Redis Ltd owns the namespace and relicensed in 2024 |
| `alibaba/nacos` | company-owned | `F2_org:alibaba` | company namespace, no roster fact |
| `curl/curl` | community | `F1_roster:seed` | mailing-list culture anchor |
| `tensorflow/tensorflow` | community | `F1_pool:spinellis-cohort` | **WRONG.** See section 9 |

## 9. Known gaps

Each one is a limitation the paper must state.

| # | Gap | How it bites |
| --- | --- | --- |
| G1 | **F3 is not implemented** | Google controls TensorFlow through a CLA, but the `tensorflow` namespace names no company, so F2 cannot see it and the cohort pool leaves it `community`. Only F3 catches this class. |
| G2 | **F4 and F5 are not implemented** | Projects that no namespace fact reaches stay in the residual. |
| G3 | **A personal namespace has no rule** | `tporadowski/redis`, a Windows port in a personal account, labels `community` by residual. One person controls it, which is neither foundation nor company nor community. The code comment claims personal namespaces count as single-counterparty; the code does not implement it. Options: a fourth `individual` stratum, or exclusion. **Open.** |
| G4 | **`community` is a residual** | It is defined by the absence of evidence, so weak instrument coverage inflates it. The published cohort makes the absence citable and reproducible. It does not make it positive. |
| G5 | **`COMPANY_ORGS` is hand-seeded** | Coverage of company namespaces is not systematic, so F2 recall is unknown. |
| G6 | **No relicense time-boxing** | Section 7. |
| G7 | **Unverified organisations are invisible to F2** | GitHub verification is opt-in, so a real company that never verified its namespace is missed. |

## 10. Procedure for the second rater

Gate: **Cohen's κ ≥ 0.7** (`CORPUS-HEURISTICS-PLAN.md:57`).

1. Draw 30 projects: 10 per stratum, **including every contested case** that
   falls in the sample.
2. The rater receives `owner/repo` and this codebook. **Not our labels.**
3. The rater assigns one of `foundation`, `company-owned`, `community`, plus the
   fact number that decided it, plus `unsure` where the evidence is absent.
4. Compute κ on the stratum only. Report the fact-number agreement separately: a
   pair can agree on the label and disagree on the reason, which is a codebook
   problem worth knowing about.
5. **κ ≥ 0.7** → freeze codebook v2 and label the corpus.
   **κ < 0.7** → tighten the rules that produced the disagreement, then repeat
   with a fresh sample. Do not re-rate the same sample.
6. Report κ, the sample size, the rater's role, and every disagreement.

Benchmark to beat: the published enterprise dataset reports **κ = 0.29**.

## 11. Deviations from `CORPUS-HEURISTICS-PLAN.md`

| Planned | Actual | Reason |
| --- | --- | --- |
| languages C/C++/Java/C#/Go, Rust if tree-sitter lands | **C, C++, Java, M4, Rust** | verified in `cregit-issue61/tokenize/tokenize.pl`. Rust works through `ra-ap-rustc_lexer`, not tree-sitter. C# is absent. Go has a tokenizer file but no extension mapping, so it is unreachable. |
| stratum `single-vendor` | **`company-owned`** | F2 proves namespace ownership, not single-vendor control. Section 3. |
| community rosters SFC and SPI | **3 rows total** | SFC links to homepages, not repositories. Replaced by the published non-enterprise cohort as a pool. |
| ~101 projects including Linux | **1,808 and rising** | the cost model showed compute is not the constraint. Disk was, and `retain.py` removed it. |
| resolvability bar, at most 40% unknown | **not yet measured** | needs Gate 2, the pipeline pilot. |
| Avelino truck-factor corpus intersection | **not done** | comparability check still open. |

## 12. Change history

| Version | Date | Change |
| --- | --- | --- |
| v1 | 2026-09-13 | First written version. F1 and F2 automated; F3, F4, F5 manual. CNCF landscape demoted from roster to pool, which moved 734 candidates out of `foundation`. |
