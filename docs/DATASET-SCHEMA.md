# The cregit token dataset

Reference for the published artefact. Four questions, in order: what the dataset
is (§1), how to regenerate it (§2), what every column means (§3), and what is
known to be wrong with it (§5).

**`validate_schema.py` is the authority on the schema, not this document.**
`EXPECTED_COLUMNS` there is the contract that every file is gated against; §3.2
below is a description of it. If the two disagree, the code is right. Regenerate
the contract from a real file with `./validate_schema.py --emit-contract F.parquet`.

---

## 1. What the dataset is

One Parquet file per project, all with the same 70 columns.

**The grain is one row per token occurrence per file**, at the blamed revision of
the tokenized working tree — not one row per token per commit that touched it.
`git blame` attributes each token to exactly one commit, so `(file_path,
token_index)` is unique within a project and each row carries exactly one
last-touching commit. The dataset holds no history of an individual token.

Each row also carries 29 per-project provenance columns (how the project was
found, how it was labelled, which mask tokenized it) repeated verbatim on every
row, so a consumer can filter a corpus without a second join. A column is cheap
to drop at publish time and expensive to add later.

### 1.1 The corpus

| | Value | Source |
| --- | --- | --- |
| Languages tokenized | C, C++, Java, Rust | `file_mask.TOKENIZABLE_LANGUAGES` |
| File mask (identical for every project) | `(?i)\.(c\|c\+\+\|cc\|cp\|cpp\|cxx\|h\|h\+\+\|hh\|hpp\|hxx\|java\|rs\|tcc)$` | `file_mask.UNIVERSAL_MASK` |
| Candidate rows considered | 24,405 (28 fields) | `candidates.csv` |
| Eligible rows | 3,948 | `candidates.csv`, `included == True` |
| Sampling frame (distinct repositories) | 3,842 | eligible rows collapsed by `clone_url` |
| Projects drawn | 200 — 156 S, 31 M, 13 L | `manifest.sample.tsv` |
| Strata drawn | 103 community, 69 company-owned, 28 foundation | `manifest.sample.tsv` field 3 |
| Sampling seed | `20261110` | `select_corpus.SAMPLE_SEED` |
| Cell floor | 2 per `(stratum, language, size_class)` cell | `select_corpus.SAMPLE_FLOOR` |
| Size classes | S < 30,000 commits; M 30,000–150,000; L > 150,000 | `select_corpus.SIZE_S`, `SIZE_M` |

The draw is stratified by `(stratum, language, size_class)`: a floor of 2 from
every cell first, then the remainder by largest remainder, and a cell is never
asked for more than it holds. `docs/CORPUS-SAMPLE.md` is the per-cell allocation.
One `random.Random` is seeded per cell from `f"{seed}:{cell_key}"`, so adding a
cell does not re-draw the others.

**The run set is smaller than the draw.** `manifest.phase1-sm.tsv` holds the 187
S- and M-class projects and `manifest.linux.tsv` holds one L-class project: 188
of the 200. The other 12 L-class projects are held back on disk grounds (one
L-class working directory measured 435 GB). Run them from
`manifest.sample.tsv` once retention has a Parquet-only level.

How a project gets its `stratum` is `docs/CODEBOOK.md`. It is a claim about who
*controls* a project and never about who contributes to it.

### 1.2 Selecting the corpus out of the output directory

The output directory also holds development fixtures. One predicate separates
them:

```sql
CREATE VIEW tokens AS
SELECT * FROM read_parquet('<output_dir>/*/*-dataset.parquet')
WHERE provenance_status = 'candidates.csv';   -- drops the fixtures
```

`provenance_status` is `candidates.csv` for a corpus project and
`fixture-needs-rework` for a fixture. `project_meta.json` holds 210 entries of 29
fields: 200 corpus projects and 10 fixtures. A fixture has only four real fields
— `provenance_status`, `clone_url`, `manifest_category`, `file_mask` — and the
other **25 are empty strings**. It was never selected, so it has no stratum, no
draw and no history cluster, and empty strings read as findings rather than as
absences. `provenance_status` is deliberately the second column, because it
qualifies everything after it.

A corpus project that is *not* found in `candidates.csv` is a hard error rather
than a silent blank row.

`./ctp.py db` builds the same view (plus a `projects` tracking table and a
`phase_metrics` table) into `ctp.duckdb`, over the run set rather than over the
glob. It is a derived index; the files are the authority.

---

## 2. Regenerating it

`duckdb`, `srcml`, `ctags`, `java` and `perl` come from the cregit checkout's
`devenv shell`; the orchestrator resolves that environment once per run and
passes it to every subprocess. Stdlib Python otherwise. Point `pipeline.cfg` at
a cregit checkout that has `rustTokenizer` and at an output directory.

| Step | Command | Writes |
| --- | --- | --- |
| 1. Fetch rosters | `./select_corpus.py rosters` | `.corpus-cache/` |
| 2. Resolve repositories | `./select_corpus.py enrich` | `.corpus-cache/repo-meta.json` |
| 3. Find shared histories | `./shared_history.py scan` then `ancestry` | history cluster cache |
| 4. Emit candidates | `./select_corpus.py emit` | `candidates.csv`, `manifest.tsv` |
| 5. Draw the sample | `./select_corpus.py sample` | `manifest.sample.tsv`, `docs/CORPUS-SAMPLE.md` |
| 6. Build the provenance sidecar | `./project_meta.py --candidates candidates.csv --manifest manifest.sample.tsv --fixture-manifest manifest.tsv --fixture-manifest manifest.mvp5.tsv --fixture-manifest manifest.shardtest.tsv --out project_meta.json` | `project_meta.json` (200 + 10) |
| 7. Build the domain→firm map | `./build_domain_map.py fetch` then `build` | `data/affiliation.merged.csv` |
| 8. Run the corpus | `./ctp.py run --manifest manifest.phase1-sm.tsv --jobs N --skip-html --drop-memo --project-meta project_meta.json --firm-map data/affiliation.merged.csv --firm-canonical data/firm_canonical.csv` | one Parquet per project, plus a `.validated` stamp |
| 9. Gate the schema | `./validate_schema.py <out>/*/*-dataset.parquet` | exit 1 on any drift |
| 10. Build the index | `./ctp.py db` | `ctp.duckdb` |
| 11. Prune | `./retain.py --apply` | deletes `memo/` and `html/` only |
| 12. Pseudonymize for release | `./anonymize_parquet.py OUTDIR <in>.parquet ...` | anonymized Parquets + JSON report |
| 13. Verify the release | `./verify_anon.py OUTDIR` | exit 1 on any residue |

**`--manifest` defaults to `manifest.tsv`, which is four legacy pilot projects.**
Name the manifest you mean on every `ctp.py` subcommand that takes one.

Runs are idempotent. A validated project is skipped, an interrupted one resumes
through the tokenizer's incremental blob map, and a failed one is retried on the
next pass. Resume is sensitive to two strings: the work directory path and the
mask. Both are recorded in the blob map's meta table and compared character for
character, so paths are canonicalized before the runner invokes anything, and a
mask change forces a rebuild unless `--mask-widened` is passed with
`--from-step 2`.

`./ctp.py status` and `./ctp.py progress` are the progress views. Per-attempt
logs live under `state/<project>/logs/`, beside `metrics.tsv` and `runs.log` and
**not** in the project work directory, because a from-scratch run deletes that
directory.

`./run_tests.sh` runs the suite with an 80% coverage floor; tests needing network
or an authenticated `gh` are deselected.

### 2.1 Two gates, and they check different things

| Gate | Asks | Checks |
| --- | --- | --- |
| `validate.py <parquet> <stamp>` | did this project produce data? | file larger than 10,000 bytes, row count > 0. Writes the `.validated` stamp on success. **It does not check columns.** |
| `validate_schema.py <parquet> ...` | do all projects agree? | every column name, type and position against `EXPECTED_COLUMNS`. Reports every drift, not the first. |

---

## 3. The columns

### 3.1 Blocks

70 columns in five blocks. Boundaries are 1-based and inclusive;
`30 + 8 + 9 + 8 + 15 = 70`.

| Columns | Block | Varies by |
| --- | --- | --- |
| 1–30 | project identity and provenance | project only — constant on every row |
| 31–38 | token grain | row |
| 39–47 | commit | commit |
| 48–55 | resolved identity and firm | row |
| 56–70 | commit trailers, `VARCHAR[]` | commit |

The firm trio (52–54) is the only part of the identity block that is not a
per-project constant: it comes from a per-row join on `person_domain`.

### 3.2 The 70 columns

| # | Column | Type | Comes from | Note |
| ---: | --- | --- | --- | --- |
| 1 | `repo_name` | VARCHAR | `--repo-name` | the manifest name; a lossy slug, see §3.5 |
| 2 | `clone_url` | VARCHAR | sidecar | **the identity. Join on this.** |
| 3 | `provenance_status` | VARCHAR | sidecar | `candidates.csv` \| `fixture-needs-rework` |
| 4 | `source` | VARCHAR | sidecar | which roster or search found the project |
| 5 | `stratum` | VARCHAR | sidecar | `community` \| `company-owned` \| `foundation` |
| 6 | `fact` | VARCHAR | sidecar | the control fact that assigned the stratum |
| 7 | `contested` | VARCHAR | sidecar | non-empty when two facts disagreed |
| 8 | `label_date` | VARCHAR | sidecar | when the stratum was assigned |
| 9 | `owner` | VARCHAR | sidecar | the owner the roster recorded |
| 10 | `repo` | VARCHAR | sidecar | |
| 11 | `roster_name` | VARCHAR | sidecar | the name the roster used |
| 12 | `roster_lang` | VARCHAR | sidecar | the language the roster claimed |
| 13 | `language` | VARCHAR | sidecar | GitHub's primary language |
| 14 | `commits` | VARCHAR | sidecar | at selection time |
| 15 | `size_class` | VARCHAR | sidecar | `S` \| `M` \| `L`, from `commits` |
| 16 | `size_kb` | VARCHAR | sidecar | |
| 17 | `stars` | VARCHAR | sidecar | at selection time |
| 18 | `pushed_at` | VARCHAR | sidecar | |
| 19 | `license` | VARCHAR | sidecar | SPDX identifier |
| 20 | `owner_type` | VARCHAR | sidecar | `User` \| `Organization` |
| 21 | `archived` | VARCHAR | sidecar | |
| 22 | `fork` | VARCHAR | sidecar | GitHub's flag only; see §5 |
| 23 | `history_cluster` | VARCHAR | sidecar | projects sharing one commit history |
| 24 | `history_shared_with` | VARCHAR | sidecar | space-separated cluster members |
| 25 | `history_relation` | VARCHAR | sidecar | `includes` \| `included_in` \| `mirror` \| `diverged` — about **inclusion**, not origin |
| 26 | `history_includes` | VARCHAR | sidecar | projects whose whole history is inside this one |
| 27 | `history_first` | VARCHAR | sidecar | the cluster's oldest repository — the **origin** signal |
| 28 | `history_created` | VARCHAR | sidecar | |
| 29 | `manifest_category` | VARCHAR | **manifest**, not the sidecar | need not equal `stratum` |
| 30 | `file_mask` | VARCHAR | **manifest**, not the sidecar | which mask tokenized this Parquet |
| 31 | `file_path` | VARCHAR | blame | repository-relative |
| 32 | `token_index` | BIGINT | blame | 0-based, per file |
| 33 | `source_line` | BIGINT | source tree | 1-based |
| 34 | `source_col` | BIGINT | source tree | 1-based |
| 35 | `source_text` | VARCHAR | source tree | raw characters, trailing whitespace included |
| 36 | `token_type` | VARCHAR | tokenizer | |
| 37 | `token_value` | VARCHAR | tokenizer | whitespace-stripped |
| 38 | `is_structural` | BIGINT | computed | 1 = boundary marker, 0 = real code |
| 39 | `cregit_commit_sha` | VARCHAR | blame | commit in the **tokenized** repository |
| 40 | `original_commit_sha` | VARCHAR | `commitmap` | `coalesce(m.originalcid, t.commit_sha)` |
| 41 | `author_name` | VARCHAR | `commits.autname` | raw git string, not resolved |
| 42 | `author_email` | VARCHAR | `commits.autemail` | |
| 43 | `author_date` | VARCHAR | `commits.autdate` | **string, not a timestamp** |
| 44 | `committer_name` | VARCHAR | `commits.comname` | |
| 45 | `committer_email` | VARCHAR | `commits.comemail` | |
| 46 | `committer_date` | VARCHAR | `commits.comdate` | **string, not a timestamp** |
| 47 | `commit_summary` | VARCHAR | `commits.summary` | subject line only |
| 48 | `personid` | VARCHAR | `emails.personid` | the identity-merged person. LEFT join — may be NULL |
| 49 | `person_name` | VARCHAR | `coalesce(persons.personname, emails.personid)` | |
| 50 | `person_email` | VARCHAR | `emails.emailaddr` | **a real address unless the file is anonymized** |
| 51 | `person_domain` | VARCHAR | `emails.domain` | the key the firm join uses |
| 52 | `firm_raw` | VARCHAR | `affiliation.merged.csv.company` | the map's string, unaltered. `''` = domain not in the map |
| 53 | `firm` | VARCHAR | `firm_canonical.csv.firm` | canonical name; equals `firm_raw` unless the reviewed table renames it |
| 54 | `firm_source` | VARCHAR | `affiliation.merged.csv.source` | **the confidence tier. `''` = no attribution.** See §3.4 |
| 55 | `repo_tag` | VARCHAR | `commitmap.repo` | `''` for a single-repository project |
| 56 | `footer_signed_off_by` | VARCHAR[] | `footers` | |
| 57 | `footer_co_authored_by` | VARCHAR[] | `footers` | |
| 58 | `footer_co_developed_by` | VARCHAR[] | `footers` | |
| 59 | `footer_reviewed_by` | VARCHAR[] | `footers` | |
| 60 | `footer_acked_by` | VARCHAR[] | `footers` | |
| 61 | `footer_tested_by` | VARCHAR[] | `footers` | |
| 62 | `footer_reported_by` | VARCHAR[] | `footers` | |
| 63 | `footer_suggested_by` | VARCHAR[] | `footers` | |
| 64 | `footer_based_on_patch_by` | VARCHAR[] | `footers` | |
| 65 | `footer_helped_by` | VARCHAR[] | `footers` | |
| 66 | `footer_mentored_by` | VARCHAR[] | `footers` | |
| 67 | `footer_assisted_by` | VARCHAR[] | `footers` | |
| 68 | `footer_thanks_to` | VARCHAR[] | `footers` | |
| 69 | `footer_personids` | VARCHAR[] | `footers` → `emails` | resolved **set**: DISTINCT and sorted |
| 70 | `footer_person_names` | VARCHAR[] | `footers` → `emails` → `persons` | resolved **set**: DISTINCT and sorted |

Columns 41–46 are the raw git strings on the commit; 48–51 are the same person
after cregit's identity merge across addresses. Use `personid` to count people
and `author_name` only to see what the commit actually said.

Trailer keys are matched case-insensitively and each typed list (56–68) is
ordered by the trailer's own index, so element *i* of one list does not
correspond to element *i* of another. Columns 69 and 70 are DISTINCT and sorted,
so they are sets and are **not** positionally aligned with the 13 typed lists.

### 3.3 How the blocks are joined

```sql
FROM token_map t                                       -- one row per token
JOIN      commits c   ON t.commit_sha = c.cid          -- INNER: no commit, no row
LEFT JOIN commitmap m ON c.cid = m.cid
LEFT JOIN emails e    ON (c.autname = e.emailname AND c.autemail = e.emailaddr)
LEFT JOIN persons p   ON e.personid = p.personid
LEFT JOIN ( ... footers f LEFT JOIN emails fe ... GROUP BY f.cid ) ftr
                      ON c.cid = ftr.cid
ORDER BY t.file_path, t.token_index
```

Two consequences to plan for:

* `JOIN commits` is an **inner** join. A blamed commit missing from the commit
  table drops its tokens silently, and nothing downstream reports the loss.
* `emails` is matched on the exact **pair** `(emailname, emailaddr)`. A commit
  whose author spelling differs from every `emails` row yields
  `personid = NULL`, and columns 48–51 are then all NULL for that token. Measure
  the miss rate on your own slice; it varies with how many distinct people a
  project has.

The 29 provenance columns are injected as SQL literals from `project_meta.json`,
with no join at all. A missing sidecar key is a hard error, so a typo cannot
silently blank 29 columns.

### 3.4 Firm attribution: always read `firm_source`

`firm_raw`/`firm`/`firm_source` resolve `person_domain` through
`data/affiliation.merged.csv` (4,049 domains, 2,996 distinct company strings),
canonicalizing the name through the reviewed `data/firm_canonical.csv`. All three
are `''` when the domain is not in the map, so **an empty `firm_source` means "no
attribution"** and is the column to filter on.

The map is built from person-grain public affiliation data projected onto
domains, which is lossy: a contributor's employer gets attached to their personal
domain. Single-person attestations are kept deliberately — dropping them costs
most of the yield — and tagged so a consumer can restrict to the corroborated
tier. Counted from `data/affiliation.merged.csv`:

| `source` | rows | Confidence |
| --- | ---: | --- |
| `cncf-gitdm-single` (+ `-self-reference`) | 2,770 + 28 = **2,798 of 4,049 (69%)** | **one person only** |
| `gitdm` | 768 | curated, hand-checked |
| `cncf-gitdm` (+ `-self-reference`) | 204 + 5 = 209 | several people agree |
| `spinellis-sec` (+ `-self-reference`) | 111 + 2 = 113 | published source |
| `patch` | 72 | curated |
| `rich` | 55 | curated |
| `builtin` | 33 | definitional |
| `correction` | 1 | reviewed overlay, `data/affiliation.corrections.csv` |

```sql
-- Restrict to the corroborated tier.
SELECT firm, COUNT(*) FROM tokens
WHERE firm <> '' AND firm_source NOT LIKE 'cncf-gitdm-single%'
GROUP BY firm;
```

`data/firm_canonical.csv` is a **reviewed table, not a rule**: 62 rows, of which
51 merge a raw spelling into a canonical name and 11 record a candidate merge
that was examined and **rejected** (parent rollups such as `AWS`→`Amazon`;
genuinely separate companies such as `Samsung SDS`; `Hewlett` against `HP`, which
split in 2015). An automatic rule would eventually merge two different firms and
nobody would notice. Note also that an automatic rule keyed on source confidence
would canonicalize *towards* the all-caps SEC filing names, because the
higher-confidence source is the one that spells firms in upper case.
`firm_raw` is never overwritten, so every merge is reversible by a reader.

### 3.5 Join on `clone_url`, never on `repo_name`

`repo_name` is a **filesystem key**: the runner locks and names work directories
with it, so it must stay filesystem-safe. It is built as
`slug(owner) + "__" + slug(repo)`, where `slug` lowercases and replaces every run
of characters outside `[a-z0-9-]` with a hyphen. `_` and `.` both become `-`, so
the transform is **not invertible**. The owner recorded is also the owner the
roster used, which may no longer own the repository.

Measured over the 200 drawn projects: 125 names equal `owner__repo` verbatim, 60
need slugging to match the URL, and **15 do not derive from the clone URL at
all** because the repository was renamed or transferred upstream. Any analysis
that joins back to a roster on `repo_name` silently loses at least those 15.
Reproduce the split with `select_corpus.project_name(owner, repo)` over
`manifest.sample.tsv`.

`clone_url` is the identity. `candidates.csv` keeps **one row per provenance
fact**, not one per project, so a repository that two rosters name appears twice:
24,405 rows cover 23,707 distinct non-empty `clone_url`s, 196 of which carry more
than one row. `select_corpus.dedupe_by_clone_url` is the resolution rule — group
by `clone_url`, and the row whose owner matches the URL wins — and anything
reading `candidates.csv` must apply it. `project_meta.py` does.

---

## 4. Intermediate artefacts

Per project the pipeline writes four SQLite databases, two bare repositories, two
working clones, a `blame/` tree and one Parquet. **Only the Parquet is
published.** A project is re-derivable from `clone_url` plus the recorded
`file_mask`, which is why `file_mask` is a column.

| Artefact | Read to build the Parquet? | Kept because |
| --- | --- | --- |
| `<name>-dataset.parquet` | — | **it is the product** |
| `<name>-cregit.db` | yes — commits, commitmap, footers | audits, re-derivation |
| `<name>-persons.db` | yes — persons, emails | audits. **Holds raw e-mail addresses** |
| `<name>-original/` working clone | yes — the source text | re-derivable from the bare repo |
| `blame/**/*.blame` | yes | consumed, re-derivable |
| `<name>-original.db` | **no** | audits and re-derivation |
| `<name>-blobmap.db` | **no** | the incremental-resume ledger |
| `<name>-original.git`, `<name>-cregit.git` | no | cheap incremental re-runs |
| `memo/` | no | nothing — deleted by `retain.py` |
| `html/` | no | nothing — never written under `--skip-html` |
| `sync_*.db` (`token_map`) | yes, then deleted | nothing — transient |

`retain.py` deletes `memo/` and `html/` and nothing else. It is dry-run by
default, requires `--apply`, and refuses a project whose keepers are missing.
Dropping `memo/` costs the tokenizer's blob-to-token cache, so a later
incremental re-run of that project re-tokenizes from cold; that is accepted
because a validated project is not re-run.

Three naming traps, all easy to get wrong:

* The tokenized→original commit mapping is **`commitmap`** in `<name>-cregit.db`.
  `commit_map` (with the underscore) is a *different* table, in
  `<name>-blobmap.db`, with different columns, mapping original→rewritten
  commits produced by the rewrite rather than the tokenizer's provenance.
* `persons.db :: persons` has **only two columns**, `personid` and `personname`.
  Every address, domain and counter lives in `emails`, so `person_email` and
  `person_domain` come from `emails`.
* There is **no persisted token table**. It is created in a temporary `sync_*.db`,
  indexed, read once, and unlinked. At token grain the data exists on disk only
  as `blame/**/*.blame` plus the working tree, and after that only in the Parquet.

The temporary token table computes one column the Parquet does not carry:
`func_name`, populated for declaration tokens only. Function attribution is
therefore not available in the published data.

---

## 5. Known limitations

Ordered by how likely it is to change a result.

**Publishing the Parquet as it stands publishes contributors' e-mail addresses.**
`person_email` (column 50) is a real address. `anonymize_parquet.py` is the
release path: it rewrites the e-mail local part to `author_NNNN` and names to
`Author N`, running every identity string — including each element of the 15
trailer arrays — through one registry, so a person carries one pseudonym
everywhere. It classifies **every** input column into pass-through, transform or
drop and raises on a column it does not recognise, so a schema change stops it
loudly instead of silently shrinking the release. It also checks its own output:
row count, `firm`/`repo_name` group counts and every `person_domain` must be
unchanged, and `count(distinct person_email)` and `count(distinct person_name)`
must not drop — two real people collapsing onto one pseudonym would lower a
distinct-contributor count for free.

Two properties to plan a release around. There is **no salt and no key**: ids are
assigned by sorting the distinct lowercased values, so two runs over the same
inputs are byte-identical and two releases diff cleanly, but the only thing
protecting the mapping is not publishing the registry. And the registry spans
**one invocation**, so pseudonyms are not stable across runs with different input
sets — pass every file that will be published together in a single command.

Run `verify_anon.py` over the output directory as an independent check: it tests
the published files alone for any e-mail local part that is not a pseudonym, needs
no secrets, and so can be run by a reviewer or a depositor. It is a necessary,
not a sufficient, condition. Two residual risks survive
anonymization by design and must be disclosed by any analysis: the e-mail
**domain is preserved on purpose** (it is the firm signal), so a sole contributor
at a rare or vanity domain is re-identifiable; and `owner`/`repo_name`/
`clone_url` carry the GitHub namespace, which for a personal repository is a
person's handle. `source_text` and `token_value` are source code and are not
scrubbed, so copyright headers and `@author` tags pass through.

**63 of the 188 run projects were tokenized with a narrower mask than the one now
recorded.** The mask used to be chosen from GitHub's primary-language field,
which dropped a polyglot project's other languages. `data/mask-impact.csv`
(188 rows, re-derivable with `./mask_impact.py`) measures the change: 545,957 →
570,201 files and 7,043.0 → 7,329.7 MiB selected. 63 projects are `gainer` — the
universal mask selects files at HEAD that their recorded mask did not, so their
tokenized repository is incomplete and they must be re-tokenized from step 2. The
other 125 select the same HEAD paths and need only a step-10 regeneration. 17 of
those 125 gain files only in history, on paths deleted or renamed before HEAD;
those rows never reach the Parquet, so they are not a Parquet defect. The mask
never narrows: `delta_files < 0` occurs for no project. Filter on `file_mask`
(column 30) to tell which rows came from which mask.

`./ctp.py run --mask REGEX` overrides the manifest for a whole run but does
**not** update `project_meta.json`, so the published `file_mask` will not describe
what actually ran. The runner warns; nothing enforces it.

**The firm columns are empty for most of the corpus, and an empty value is
ambiguous.** A project only carries firm attribution if it was generated with
`--firm-map`; otherwise all three columns are empty strings, which is also what
"this domain is not in the map" looks like. From columns 52–54 alone a consumer
**cannot distinguish** "no attribution for this domain" from "this project was
never regenerated with the map". Cross-check against the project rather than the
row.

**Some tokenizable source is deliberately excluded, and nothing in the output
says so.** `.ixx`, `.inl`, `.cppm`, `.cxxm` and `.ipp` are left out of the mask
because srcML 1.1.0 does not recognise them, ignores `-l C++`, and then writes an
**empty token file and exits 0** — a silent success. Autotools input (`.am`,
`.ac`, 1.4 MB across 22 projects) is left out because the m4 tokenizer's lexer
uses a backtick as its closing quote, so an apostrophe swallows text to the next
backtick. C++ module and inline-implementation files and autotools source are
therefore absent from the dataset with nothing in a Parquet to indicate the
absence. `tests/test_mask_drift.py` holds the extension list to the tokenizer's
own language table, so adding an extension without probing it is caught.

**Runs are not pinned to a commit.** The manifest carries exactly five fields and
three parsers plus four tests assert their positions, so the planned
`pinned_commit` sixth field does not exist. A re-run analyzes whatever HEAD is at
clone time, so the dataset is reproducible in method but not byte-for-byte, and a
published Parquet cannot cite the revision it was built from.

**`data/affiliation.merged.csv` is a build artefact and currently predates one of
its corrections.** `data/affiliation.corrections.csv` holds two reviewed rows but
only one is present in the merged map: `collibra.com` still resolves to
`Medidata` rather than `Collibra`. Rerun `./build_domain_map.py build` to apply
the overlay — it is applied last, so a rebuild keeps it and a hand edit of the
merged file would be lost. Two further single-person inferences are wrong on
their face and are **not** corrected: `central-intelligence.agency`→`Microsoft`
and `intellisys.info`→`Takeaway.com`. A row-by-row audit of 2,770 single-person
inferences has not been done.

**`community` is a residual stratum, and `contested` is empty for every row.**
Three of the five control facts in `docs/CODEBOOK.md` are not implemented, so a
project that no namespace fact reaches stays `community` by default.
`contested` (column 7) is non-empty in **0 of 24,405** candidate rows, so the
contested-case protocol in `docs/CODEBOOK.md` §6 has not flagged anything and the
column cannot currently be used to find disputed labels. `docs/CODEBOOK.md` §9 is
the full gap list with a worked example per gap;
`docs/SPINELLIS-VALIDATION.md` §4 names seven eligible rows that an independent
published registry attests to a company and that this pipeline nonetheless labels
`community`.

**GitHub's `fork` flag finds nothing.** Among eligible rows `fork = True` counts
zero, because a tree pushed as an independent repository is not marked. Projects
that carry another project's history are therefore **kept and flagged**, not
excluded: filter on `history_cluster` for one project per history, and read
`history_first` for origin — `history_relation` is about inclusion, and the same
topology occurs with the origin on either side.

**A stratum is a label at a date.** Namespaces get donated, so two rows for one
repository can disagree: of the 196 duplicate `clone_url` groups in
`candidates.csv`, **28 disagree on `stratum`** — **5** of them within the 106
groups that fall inside the eligible frame. `label_date` (column 8) records when
the label was assigned, and there is no relicensing time-boxing anywhere in the
pipeline (`docs/CODEBOOK.md` §7), so a project that changed licence or owner
mid-history carries one label for all of it.

**`manifest_category` (column 29) has a fourth value outside the stratum
vocabulary.** The four legacy pilot projects in `manifest.tsv` carry
`enterprise`, which is not one of `community`/`company-owned`/`foundation`. Those
four are also still at an older 23-column schema, so `consolidate.py`'s schema
gate excludes them from the `tokens` view and names them in its summary. Group on
`stratum` (column 5), not on `manifest_category`.

**A `.validated` stamp does not certify the schema.** `validate.py` is the only
gate the run invokes, and it checks size and row count only (§2.1). Run
`validate_schema.py` yourself over the output. Likewise `state = 'DONE'` in
`ctp.duckdb` means the pipeline finished, not that the data is present: such a
project is flagged `parquet_missing` and stays out of the `tokens` view, so
`select name from projects where parquet_missing` finds it.

**Dates are strings.** `author_date` and `committer_date` (43, 46) are raw git
strings, so a consumer must cast before any temporal query.

**Corpus-level firm and org rollups do not exist.** The firm columns are per
token; there is no aggregated firm table, no package step and no dataset card
generator. `docs/DESIGN.md` §7 is the intended shape of those.

**Reproducibility depends on an unpinned sibling checkout.** The per-project
pipeline, the tokenizer and their flag names live in the cregit checkout that
`pipeline.cfg` names, at whatever revision it happens to be. A flag rename there
has already broken a run mid-corpus. Record that checkout's revision alongside
any published Parquet.

---

## 6. Invariants a change must not break

| Invariant | Enforced by |
| --- | --- |
| every Parquet has the same 70 columns, types and order | `validate_schema.py` — exit 1 on any drift |
| the 29 provenance names agree across three files in two repositories | `tests/test_meta_field_drift.py` — `project_meta.META_FIELDS`, `validate_schema.EXPECTED_COLUMNS[1:30]`, and the generator's own list |
| the mask names only extensions with a working parser | `tests/test_mask_drift.py`, against the tokenizer's own language table |
| the mask string is byte-stable | `file_mask.build_mask` sorts the extension list; the blob map compares the mask character for character on every resume |
| a missing sidecar key fails loudly rather than blanking 29 columns | the generator raises on load |
| duplicate `clone_url`s resolve the same way everywhere | `select_corpus.dedupe_by_clone_url`, called by `project_meta.build_meta` |
| anonymization never silently shrinks the schema | `anonymize_parquet.py` classifies every input column and raises on an unknown one |

If you change `EXPECTED_COLUMNS`, change §3.2 in the same commit.
