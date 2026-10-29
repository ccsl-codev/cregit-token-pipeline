# Anonymization: open decisions for the partner

Three questions the release needs answered. Each carries a measured consequence
rather than an opinion.

> **Status.** Questions 1 and 3 have since been ANSWERED and the answers are
> implemented; the text below is preserved as the record of the choice, so the
> measurements that drove it are not lost. Question 2 was answered on 2026-10-24.
>
> * **Question 1 — answered: Option B, the salted stable hash.** Implemented in
>   `anonymize_parquet.py`. The salt is held privately outside the repository and
>   the reverse map is never published. The 98.0% renumbering below is now the
>   measured behavior of the **superseded** design; re-run after the change, the
>   same experiment renumbers **0 of 153 — 0.0%**. The "correction to the brief"
>   section and the greps in it describe the state of the checkout *before* that
>   change: `docs/DESIGN.md` and `docs/LIMITATIONS.md` no longer say "no salt and
>   no key", and they now describe the hash, the private salt and the unpublished
>   reverse map. See `docs/DESIGN.md` §7 item 6.
> * **Question 3 — answered: publish with disclosure.** The 84.7% single-address
>   domain tail is stated in `docs/LIMITATIONS.md` with the project's reasoning,
>   and no k-anonymity work is planned. The docstring hedge has been corrected.
> * **Question 2 — answered on 2026-10-25: keep.** The commit message is
>   published as git has it. See the section "Status after the release
>   rework" below. The text of question 2 is kept as the record of the choice.

## Status after the release rework (2026-10-24)

The release tool is now `pseudonymize.py` (one registry table, a linear leak
scan, reports with counts and registry tokens only, `--jobs`/`--threads`/
`--memory-limit`, and a fused write for the organization step). Its module
docstring gives the design and the measurements. `anonymize_parquet.py` keeps
the rules and its old command line. Do not use that command line for the
release: its report and its standard output give examples of real values.

What the documents decided, and what the new tool does:

* `person_domain` is written as `lower(trim(person_domain))`, the key of
  `build_links.py` (JOIN_PROTOTYPE.md, recommendation 4). Measured on the v1
  files: 2 of 70,737 distinct values change.

Decided by the user on 2026-10-24 and 2026-10-25:

* **2 and 2b. `commit_summary`: decided 2026-10-25, keep.** The user: "We
  shouldn't change commit messages; if somebody committed their name in the
  history, we shouldn't do anything." The default is `--commit-summary keep`:
  the commit message is published as git has it, with no name and no address
  changed. The gate does not check `commit_summary`; the leak scan gives its
  counts as information only, and `verify_anon.py` takes it as a content
  column (a note, not a failure). The trailer columns (`footer_*`) are
  separate and get pseudonyms. The modes `pseudonym` (each address gets the
  pseudonym of the e-mail columns), `scrub` and `null` stay as options. The
  name replacement of 2026-10-24 is removed: measured on the v1 files,
  6,370,045 of 19,747,315 distinct summaries (3,529 of 3,544 files,
  9,010,968,089 of 23,692,479,892 rows) hold a registry name, most of them a
  single-word name that is also an ordinary word, so the replacement would
  have changed 38% of the summary rows.
* **5 and 10. The domain of an address and of `person_domain`: decided
  2026-10-25, rule E.** The domain is the first part after an at sign that is
  a valid domain: the STRICT_DOMAIN characters of `build_companies.py`, a
  dot, and an ICANN public suffix (the Public Suffix List, `--psl`; default:
  the newest list in the nix store). No valid part gives the empty domain.
  This is the default for the e-mail columns and the trailer addresses
  (`--email-domain first-valid`) and for a `person_domain` value with an at
  sign (`--person-domain-at first-valid`). The old rules stay as options
  (`--email-domain second-part`, `--person-domain-at after-last-at`).
  `pseudonymize.rule_e_domain_sql()` and `person_domain_expr()` give the
  rule as SQL for other tools; `build_v0.py` must use them for its domain key.
  Measured on the v1 files: 199 distinct `person_domain` values (15,278,362
  rows, 176 files) have an at sign; rule E changes 133 distinct values
  (14,726,554 rows) against the after-last-at rule of 2026-10-24, and gives
  the empty domain for 10 values (25,940 rows). Of the 471,906 registry
  addresses, rule E changes the published domain of 11,909 against the old
  rule (the second part, in lower case).
* **The pseudonym mask (defect of 8223fae).** The registry holds the name
  `author`. Without a mask, every pseudonym (`Author <token>`, `author
  <token>`, `author_<token>`) was a known name for the leak scan, so every
  file failed. The leak scan now masks the pseudonym forms.
* **6. The registry file: decided, kept private.** The registry is the
  reverse map (real identity to pseudonym). It is kept in the private bucket
  after the release and is never published, so that a later release with the
  same salt gives the same pseudonyms and can check the footer hash
  `pseudonymization_registry_sha256`. The tool writes it with mode 0600 in a
  folder with mode 0700. It refuses a registry path under `--outdir`, under a
  `--release-root`, or under a folder with the layout of a release root (a
  `parquet` folder with `organizations`, `MANIFEST.tsv` or `SHA256SUMS`).
* **9. The pseudonym scheme: decided, scheme 2.** The release scheme is the
  nested keyed sha256 that DuckDB computes (scheme 2), not the blake2b of
  `anonymize_parquet.py`. With one salt, the two tools give different tokens.
  Nothing was published with the old scheme.

Questions that still need the user:

* **7. The salt on the write machine.** The fused write computes the
  pseudonyms in DuckDB, so the salt file must be on the machine that writes
  the files (release plan step 7, "the salt transfer").
* **8. The `build_v0.py` fusion.** The fused write needs a change in the other
  repository (`s8-companies/build_v0.py`): its COPY becomes a call of
  `pseudonymize.write_public(...)` with its own SELECT as `source_sql`. Its own
  order hash must not compare the identity columns, because they change.
  The call is in the module docstring of `pseudonymize.py`
  (`write_public()`).
* **11. The name probes of the structural gate: decided by the main session
  on 2026-10-26 (a false-positive fix, open to the user's review).** With the
  defaults of 2026-10-25 the structural gate failed all 3,544 v1 files, only
  because of name probes: 366 distinct values, nearly all a single-word
  registry name that is also a word of a closed vocabulary. Failing files per
  column: `token_type` 3,544, `manifest_category` 3,544, `source` 324,
  `person_domain` 209, `language` 100, `license` 11, `stars` 1. Without the
  name probes no file failed (no address shape and no local part). The
  identity columns passed (a sample of 60 files: 0 failures).

  The rule (`NO_NAME_PROBE_COLUMNS` in `pseudonymize.py`): no name probes in
  (a) the closed-vocabulary columns, (b) `person_domain` (domains are
  published by design), and (c) the numeric, date and hash columns. (d) The
  address-shape and local-part probes stay on every column. The columns:

  * closed vocabularies (`VOCABULARY_COLUMNS`): `provenance_status`,
    `source`, `stratum`, `contested`, `roster_lang`, `language`,
    `size_class`, `license`, `owner_type`, `archived`, `fork`,
    `history_relation`, `manifest_category`, `file_mask`, `token_type`, and in
    schema 2 `firm_based_on_namespace`, `firm_based_on_domain`,
    `project_enterprise_basis` (ids and labels of the organizations step);
  * numbers, dates and hashes (`NUMERIC_COLUMNS`): `commits`, `size_kb`,
    `stars`, `label_date`, `pushed_at`, `history_created`, `author_date`,
    `committer_date`, `cregit_commit_sha`, `original_commit_sha`.

  How they were found: the source column of `docs/DATASET-SCHEMA.md` (the
  sidecar, the manifest, the tokenizer, blame and the organization step write
  these values from a fixed list or in a fixed format; for example `stratum`
  is `community` | `company-owned` | `foundation`, `license` is an SPDX id,
  `size_class` is `S` | `M` | `L`), and a count of the distinct values of each
  structural column over the 3,544 v1 files (read only, counts only):
  `provenance_status` 25, `source` 192, `stratum` 3, `language` 108,
  `size_class` 3, `license` 87, `owner_type` 2, `archived` 1, `fork` 1,
  `manifest_category` 1, `file_mask` 1, `token_type` 309, and 0 (empty in v1)
  for `contested`, `roster_lang`, `size_kb`, `history_relation` and
  `history_created`. All values of `commits` (3,186) and `stars` (2,550) are
  numbers; `label_date`, `pushed_at`, `author_date` and `committer_date` are
  dates; the two commit columns are 40-character hex. The free-text structural
  columns keep the name probes: `repo_tag` (a repository name; empty in v1)
  and `history_cluster`, `history_shared_with`, `history_includes`,
  `history_first` (project names; empty in v1).

  Measured again on the 3,544 v1 files with this rule (read only, counts
  only): 0 distinct structural values fail, so 0 files fail the structural
  gate (no name, no local part, no address shape).

## First, a correction to the brief

The task described a conflict: `docs/DESIGN.md` §7 item 2, around line 163, was
said to specify a **salted stable hash with a private salt and a local reverse
mapping**, conflicting with the unsalted sequential registry in the code.

**That conflict does not exist in this checkout.** Verified:

- `grep -rn -i salt docs/ *.py` returns exactly two hits. `docs/DESIGN.md:182`
  says "There is **no salt and no key** — ids come from sorting the distinct
  values". `docs/LIMITATIONS.md:141` says "There is **no salt and no key**".
  Both describe the shipped behaviour.
- `grep -rn -i hash docs/` returns nothing at all.
- `grep -rn -i "reverse map"` returns nothing.
- §7 item 2 is `shared_history.py`, not anonymization. The anonymization item is
  §7 item **6**. The cited lines 25, 38 and 143 are a table header, the pipeline
  diagram and a `validate.py` bullet.

So the documents and the code **agree**. There is nothing to reconcile, and no
design document was overridden by the implementation.

The underlying trade-off is still a real and unmade release decision, so it is
recorded below as question 1. The point of this section is that it is an open
choice, not a contradiction anyone needs to repair.

## Question 1 — keep the unsalted sequential registry, or move to a salted hash?

### Option A: unsalted sequential registry (what ships today)

Ids are assigned by sorting the distinct lowercased values and counting from 1.
`alice@redhat.com` becomes `author_0042@redhat.com`.

Measured properties:

- **Byte-reproducible.** Two independent runs over the same six files produced
  byte-identical Parquet files, matching md5 for all 6 of 6.
- **Input order does not matter.** Presenting the same files reversed produced
  byte-identical output. Pinned by a test.
- **Verification needs no secret.** `verify_anon.py` checked 158,145 distinct
  strings over the six files and exited 0. A co-author or a Zenodo depositor can
  run it. This is the property that makes the gate worth putting in a release
  script.
- **Reversible by anyone who can rebuild the inputs.** The registry is a pure
  function of the input values, and the inputs derive from **public** GitHub
  repositories. An adversary needs no salt, no key and no leaked file — only the
  same file set and the same command. The mapping to invert is 28,339 distinct
  addresses and 25,386 distinct names over the 186 conforming corpus files.
- **Pseudonyms are NOT stable across releases with different file sets.** This is
  the cost that is easy to miss. Adding **one** file to the invocation renumbered
  **150 of 153** addresses that were present in both runs — **98.0%**.

That last number contradicts a claim now in two documents. `DESIGN.md:182` and
`LIMITATIONS.md:141` both say the scheme means "two releases diff cleanly".
Measured, that holds only while the input set is byte-identical. A second release
that adds or drops a single project renumbers almost everyone, and a diff of the
two releases is then unreadable. Whoever answers this question should also decide
whether those two lines need correcting.

### Option B: salted stable hash

`author_` plus a truncated keyed hash of the lowercased value, with the salt held
privately and a reverse mapping kept locally and never published.

Consequences, reasoned from the code rather than measured, because it is not
implemented:

- **Not reversible from public inputs.** This is the whole point, and it closes
  the exposure in Option A.
- **Stable across releases.** The pseudonym depends on the value and the salt,
  not on the set of files in the invocation, so the 98.0% renumbering disappears
  and releases really do diff cleanly.
- **Needs salt custody.** A lost salt makes a future release unlinkable to a
  published one. A leaked salt makes the whole scheme worthless, and unlike
  Option A the leak is silent. This is operational work, not a code change.
- **Reproducibility becomes conditional.** An independent party can no longer
  reproduce the release from the inputs, which weakens the artifact claim a paper
  would make. Option A's byte-reproducibility is a real research asset.
- **Small code cost.** `verify_anon.py` matches `author_\d+`. A hex hash needs
  that shape widened; the rest of the release path is shape-agnostic.
- **Collision risk.** Truncation can map two people to one pseudonym. The tool
  currently fails a run when `count(distinct person_email)` drops, so a collision
  would be caught rather than published — but it would block the release, and the
  truncation length has to be chosen against 28,339 addresses.

### What is not in dispute

Both options preserve the e-mail domain, and both keep one pseudonym per person
per invocation. The partner's two settled decisions do not depend on this choice.

## Question 2 — `commit_summary`: one false positive currently fails the release

Measured on `tomaka__glutin`:

```
LEAK identity column commit_summary: 1 [['name', 'github', 'ci: bump github actions']]
```

cregit records the GitHub web-flow committer as a person named `GitHub`, so
`github` enters the name registry and becomes a scanner probe. The subject line
`ci: bump github actions` matches it. The value is not personal data, and the
tool already publishes `github.com` and `users.noreply.github.com` in the clear.

The gate is fail-closed, so this one word turns the whole run non-zero. The only
remedy the tool offers is `--null-commit-summary`, which discards all 1,168
distinct subject lines to suppress one token.

This was **deliberately not fixed** by loosening the scanner. A residue gate
tuned until it goes green stops being a gate, and this project has already been
bitten three times by checks that passed when they should not have.

Options, for the partner:

1. **Accept and ship with `--null-commit-summary`.** Safe, and throws away a
   useful column. This is what the reference implementation did.
2. **Exempt a registry name that is also a published domain label.** `github` is
   the registrable label of a domain the tool publishes on purpose, so finding
   the token reveals nothing new. Narrow and defensible, but it would also exempt
   a surname that matches a vanity domain — a contributor named `Gutwin` at
   `gutwin.org` would stop being probed for. That is a real hole.
3. **Mark non-human identities in the registry.** `GitHub`, `web-flow` and
   similar bot identities are not natural persons. Pseudonymize them as now but
   do not use them as leak probes. Most correct, and needs a curated list plus a
   rule for what counts as a bot.
4. **Split the gate.** Fail on an address-shaped residue; report a name-shaped
   residue as a reviewable count without failing. Keeps the strong half strict
   and makes the weak half honest about being advisory.

## Question 3 — the single-contributor domain tail is much larger than documented

Preserving the domain leaves a sole contributor at a rare domain identifiable.
`anonymize_parquet.py`'s docstring reports this from a three-file sample: 33 of 38
`person_domain` groups had exactly one contributor, and adds that "the ratio is
inflated by the small sample".

Measured over all 186 conforming corpus files: **5,276 of 6,228 domains have
exactly one distinct address — 84.7%.** The ratio did not fall with scale.

So the re-identification tail is a large majority of domains, not an artifact of a
small sample. The decision — publish with disclosure, or apply k-anonymity over
the domain and lose the long tail of small firms the truck-factor question is
about — is unchanged, but it should be taken against 84.7%, and the docstring's
"inflated by the small sample" hedge should be corrected either way.

## How to reproduce every number here

duckdb lives only in the devenv shell, entered from the cregit checkout:

```sh
cd /local/home/ellianco/Projects/cregit-workspace/cregit-issue61
devenv shell -- bash -c 'cd <this repo> && python -m pytest tests/test_anonymize_parquet_e2e.py'
```

The 98.0% renumbering, the 28,339 addresses and the 84.7% figure come from
ad-hoc scripts over copies of the corpus Parquets, not from committed code. They
are reproducible but not yet pinned by a test; see `docs/ANON-RESIDUE.md` for the
commands.
