# The cregit token dataset

Reference for the published artefact. Four questions, in order: what the dataset
is (§1), how to regenerate it (§2), what every column means (§3), and what is
known to be wrong with it — [`LIMITATIONS.md`](LIMITATIONS.md), which you should
read before analysing the data.

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

### 1.1 The shape of a corpus

| | Value | Source |
| --- | --- | --- |
| Languages tokenized | C, C++, Java, Rust | `file_mask.TOKENIZABLE_LANGUAGES` |
| File mask (identical for every project in one run) | `(?i)\.(c\|c\+\+\|cc\|cp\|cpp\|cxx\|h\|h\+\+\|hh\|hpp\|hxx\|java\|rs\|tcc)$` | `file_mask.UNIVERSAL_MASK` |

Which repositories go into a corpus, how many, and what stratum, size class or
history relationship each one carries, is entirely up to whoever writes the
manifest and the optional provenance sidecar. Nothing in this repository drafts
a manifest, draws a sample, or assigns a stratum: it runs cregit over the
manifest it is given and records whatever the sidecar says about each project.
`size_class` (`S`/`M`/`L`) is likewise a free label you assign per project; this
repository does not enforce a threshold on it, though `--shard-classes` and the
`--jobs` mix you choose are worth basing on one, since a bigger project can need
sharding or a longer retry budget.

**A run can be smaller than the manifest, and a published set smaller than the
run.** A project can fail to complete — a disk-space hold-back, a parser crash,
a timeout — and stay in the manifest without a Parquet. A project that
completes can still be excluded from publication on purpose: `consolidate.py`'s
`PUBLICATION_EXCLUSIONS` names a project and a reason without deleting its row,
so the sampling and run record stays complete. Read published membership from
`projects.excluded_because`, never from the presence of a Parquet file.
[`LIMITATIONS.md`](LIMITATIONS.md) holds the measurements for the projects this
pipeline has published so far.

**Every project in one run is tokenized with the same mask**, so `file_mask`
(column 30) does not distinguish projects within a run — it distinguishes
*runs*, which is what it exists for. The mask used to be chosen from GitHub's
primary-language field, which dropped a polyglot project's other languages;
widening it to the union of every parseable extension is provably a superset —
no project loses a file. Note that `./ctp.py run --mask REGEX` overrides the
manifest for a run without updating the sidecar, so a run using it publishes a
`file_mask` that does not describe what happened. The runner warns; nothing
enforces it.

### 1.2 Selecting rows out of the index

`./ctp.py db` builds `ctp.duckdb` — a `projects` state table, a `phase_metrics`
table, and a `tokens` view — from the manifest or manifests you name it (default
`manifest.tsv`). Nothing is discovered by scanning the output directory: a
project has to be named in a manifest to appear in the index at all.

The `tokens` view is not simply every Parquet a manifest names. `read_parquet`
over a list binds **one** schema for the whole list, so a single file at a
different column count would abort the view outright. `consolidate.py` builds
the view only from files whose schema matches `validate_schema.EXPECTED_COLUMNS`,
and **names and counts every file it leaves out** in its printed summary — a
silent exclusion would be worse, because the row count would still look
plausible.

A project that ran and validated, but that you decide should not be published,
is excluded the same way without losing its row: name it and the reason in
`consolidate.PUBLICATION_EXCLUSIONS`, and it keeps its row in `projects`, with
the reason in `excluded_because`, while contributing no rows to `tokens`. Read
published membership from that column, never from the presence of a Parquet
file.

`provenance_status` (column 3) is a free string that your provenance sidecar
sets; this repository does not read or filter on its value anywhere. If you
need to separate one kind of row from another in the view — a real project from
a test fixture, say — filter on a column your own sidecar fills, not on
`provenance_status`.

---

## 2. Regenerating it

`duckdb`, `srcml`, `ctags`, `java` and `perl` come from the cregit checkout's
`devenv shell`; the orchestrator resolves that environment once per run and
passes it to every subprocess. Stdlib Python otherwise. Point `pipeline.cfg` at
a cregit checkout that has `rustTokenizer` and at an output directory.

| Step | Command | Writes |
| --- | --- | --- |
| 1. Write a manifest | a TSV file, one line per project: `name  url  category  file_filter  size_class` | `manifest.*.tsv` |
| 2. Point the pipeline at cregit | edit `pipeline.cfg`'s `[paths]` section: `cregit_dir`, `output_dir` | — |
| 3. Write a provenance sidecar (optional) | a JSON object, one key per project; format is `validate_schema.py`'s comment block above `EXPECTED_COLUMNS` | e.g. `project_meta.json` |
| 4. Run it | `./ctp.py run --manifest manifest.tsv --jobs N --skip-html --drop-memo --project-meta project_meta.json --firm-map data/affiliation.merged.csv --firm-canonical data/firm_canonical.csv` | one Parquet per project, plus a `.validated` stamp |
| 5. Gate the schema | `./validate_schema.py <out>/*/*-dataset.parquet` | exit 1 on any drift |
| 6. Build the index | `./ctp.py db` | `ctp.duckdb` |
| 7. Prune | `./retain.py --apply` | deletes `memo/` and `html/` only |
| 8. Pseudonymize for release | `./anonymize_parquet.py OUTDIR <in>.parquet ...` | anonymized Parquets + JSON report |
| 9. Verify the release | `./verify_anon.py OUTDIR` | exit 1 on any residue |

`data/affiliation.merged.csv` and `data/firm_canonical.csv` already ship in this
repository, built from public affiliation data plus a curated overlay. Rebuild
either only if you want a different map: `./build_domain_map.py fetch` then
`build`, and see [`LIMITATIONS.md`](LIMITATIONS.md) for what the shipped map
gets wrong.

**`--manifest` defaults to `manifest.tsv`.** That file is the only manifest in
this repository — 4 small public pilot projects — and is a worked example, not
a real corpus. Name the manifest you mean on every `ctp.py` subcommand that
takes one.

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
| 3 | `provenance_status` | VARCHAR | sidecar | a free string the sidecar sets; this repository does not interpret it |
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
`data/affiliation.merged.csv` (**4,041 domains, 2,995 distinct company strings**),
canonicalizing the name through the reviewed `data/firm_canonical.csv`. The join is
an **exact match on the domain string** — no subdomain fallback, no normalization.
All three columns are `''` when the domain is not in the map, so **an empty
`firm_source` means "no attribution"** and is the column to filter on.

The map is built from person-grain public affiliation data projected onto
domains, which is lossy: a contributor's employer gets attached to their personal
domain. Single-person attestations are kept deliberately — dropping them costs
most of the yield — and tagged so a consumer can restrict to the corroborated
tier. Counted from `data/affiliation.merged.csv` at this revision:

| `source` | domains | Confidence |
| --- | ---: | --- |
| `cncf-gitdm-single` (+ `-self-reference`) | 2,761 + 28 = **2,789 of 4,041 (69%)** | **one person only** |
| `gitdm` | 768 | curated, hand-checked |
| `cncf-gitdm` (+ `-self-reference`) | 204 + 5 = 209 | several people agree |
| `spinellis-sec` (+ `-self-reference`) | 111 + 2 = 113 | published source |
| `patch` | 72 | curated |
| `rich` | 55 | curated |
| `builtin` | 33 | definitional |
| `correction` | 2 | reviewed overlay, `data/affiliation.corrections.csv` |

`firm` is **not always a firm.** 58 domains carry the label `(Independent)` and 15
more carry `Independent` — the same category under two spellings, so any
`GROUP BY firm` that does not fold them invents a category. `(Independent)` is
what the free-provider rule emits, and it covers `gmail.com`,
`users.noreply.github.com` and the rest: a contributor using a forge no-reply
address, a contributor using consumer webmail, and a bot are all labelled
`(Independent)`, which conflates *unknowable* affiliation with *absent*
affiliation. Counting `firm <> ''` therefore over-counts real firm attribution.
Use `kind` from the map (`free_provider`, 52 domains) to separate them, and treat
`(Independent)` as missing data rather than as a firm.

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

Measured over the 200 drawn projects that produced this dataset: 125 names
equal `owner__repo` verbatim, 60 needed slugging to match the URL, and **15 do
not derive from the clone URL at all** because the repository was renamed or
transferred upstream. Any analysis that joins back to a roster on `repo_name`
silently loses at least those 15.

`clone_url` is the identity, never `repo_name`. The selection process that built
this dataset kept one row per provenance fact rather than one per project, so a
repository that two rosters both named appeared twice: 24,405 candidate rows
covered 23,707 distinct non-empty `clone_url`s, 196 of which carried more than
one row. Its resolution rule — group by `clone_url`, and the row whose recorded
owner matches the URL wins — is the one to apply if you build a similar
candidate table yourself; nothing in this repository does that for you any
more.

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

They are their own document: **[`LIMITATIONS.md`](LIMITATIONS.md)**. Read it before
analysing the data. The ones most likely to change a result:

| Limitation | Affects |
| --- | --- |
| the Rust tokenizer shifts four columns | every `.rs` row |
| `person_email` is a real address until `anonymize_parquet.py` runs | any release |
| `(Independent)` is a category, not a firm | any `GROUP BY firm` |
| token count is not a measure of human contribution | any ranking by row count |
| every `.h` file was parsed with the C grammar | every header |
| runs are not pinned to a commit | byte-for-byte reproduction |
| 2 of the 188 run-set projects have no Parquet | corpus totals |
| 185 of the 188 are published; read `excluded_because`, not file presence | corpus totals, per-org aggregates |
| a vendored dependency credits its whole library to the importing author | any per-org or per-author aggregate |

---

## 6. Invariants a change must not break

| Invariant | Enforced by |
| --- | --- |
| every Parquet has the same 70 columns, types and order | `validate_schema.py` — exit 1 on any drift |
| the 29 provenance names agree between the sidecar format and the external generator | **not tested in this repository.** `validate_schema.py`'s comment block above `EXPECTED_COLUMNS` is the description; check it by hand against the generator's `PROJECT_META_FIELDS` |
| the mask names only extensions with a working parser | `tests/test_mask_drift.py`, against the tokenizer's own language table |
| the mask string is byte-stable | `file_mask.build_mask` sorts the extension list; the blob map compares the mask character for character on every resume |
| a missing sidecar key fails loudly rather than blanking 29 columns | the generator raises on load |
| duplicate `clone_url`s resolve the same way everywhere, if you build a candidate list of your own | **not applicable here.** This repository does not build or de-duplicate a candidate list; that choice, and its consistency, is on whoever writes the manifest |
| anonymization never silently shrinks the schema | `anonymize_parquet.py` classifies every input column and raises on an unknown one |

If you change `EXPECTED_COLUMNS`, change §3.2 in the same commit.
