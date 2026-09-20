# Codebook — control facts and strata

How a project gets the `stratum` value carried in every dataset row. This is the
design decision the dataset rests on, because the stratum defines the population
under study: if this document is wrong, every result built on the strata is wrong
in the same direction.

A second rater must be able to label a project from this file alone. The
implementation is `select_corpus.judge()`; the per-project result and the fact
that decided it are in `candidates.csv`.

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

`community` is a **residual**: it is defined by the absence of evidence, not by
positive evidence of community control. Section 9 explains what that costs and
must be stated by any analysis using the strata.

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

**Not implemented.** No time-boxing exists in the pipeline. A rater must apply it
by hand, and any analysis must state the gap: a project that changed licence or
owner mid-history carries one label for all of it. `label_date` records when the
label was assigned.

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

Every one of these is a limitation an analysis using the strata must state.

| # | Gap | How it bites |
| --- | --- | --- |
| G1 | **F3 is not implemented** | Google controls TensorFlow through a CLA, but the `tensorflow` namespace names no company, so F2 cannot see it and the cohort pool leaves it `community`. Only F3 catches this class. |
| G2 | **F4 and F5 are not implemented** | Projects that no namespace fact reaches stay in the residual. |
| G3 | **A personal namespace has no rule** | `tporadowski/redis`, a Windows port in a personal account, labels `community` by residual. One person controls it, which is neither foundation nor company nor community. The code comment claims personal namespaces count as single-counterparty; the code does not implement it. Options: a fourth `individual` stratum, or exclusion. **Open.** |
| G4 | **`community` is a residual** | It is defined by the absence of evidence, so weak instrument coverage inflates it. The published cohort makes the absence citable and reproducible. It does not make it positive. |
| G5 | **`COMPANY_ORGS` is hand-seeded** | Coverage of company namespaces is not systematic, so F2 recall is unknown. |
| G6 | **No relicense time-boxing** | Section 7. |
| G7 | **Unverified organisations are invisible to F2** | GitHub verification is opt-in, so a real company that never verified its namespace is missed. |
| G8 | **`SP_MAX_PER_COMPANY` leaks from sampling into labelling** | The cap stops one firm dominating the draw, which is correct for sampling. But a capped-out project does not leave the pipeline. It re-enters through a weaker source and takes that source's stratum, so a project our own strong-tier rule attests to a company can label `community`. Seven such rows sit in the eligible frame. `validate_spinellis.py` measures them and `docs/SPINELLIS-VALIDATION.md` section 4 lists them. The cap must bound how many rows *enter*; the registry fact must still attach wherever a row enters. One constant now serves both concerns. **Open.** |

## 10. Procedure for the second rater

Agreement gate: **Cohen's κ ≥ 0.7.**

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

## 11. Scope note: language is not a stratum, but it confounds one

Eligibility is restricted to what the tokenizer can parse: **C, C++, Java and
Rust** (m4 is also parsed but is never a primary language, so it is not a
selection key). The authority is the tokenizer's own language table in the
configured cregit checkout, and `tests/test_mask_drift.py` holds `file_mask.py`'s
extension list equal to it. Rust is handled by a Rust lexer, not tree-sitter. C#
is absent. Go has a tokenizer file but no extension mapping, so it is
unreachable.

**Language confounds stratum.** The ASF roster is majority Java, while the
mailing-list community world is overwhelmingly C. If `foundation` comes out mostly
Java and `community` mostly C, a difference between strata may be a language
effect. `candidates.csv` and the Parquet both carry `language` so it can enter a
model as a covariate, and the draw is stratified by language for the same reason.

The community rosters (SFC, SPI) yielded **3 rows in total**, because SFC links to
project homepages rather than repositories. The published non-enterprise cohort
replaced them as a candidate pool — see section 5.
