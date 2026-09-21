# Design

Why the pipeline is built the way it is. For what it produces, read
[`DATASET-SCHEMA.md`](DATASET-SCHEMA.md); for how a project is labelled, read
[`CODEBOOK.md`](CODEBOOK.md).

**Goal.** Run cregit end-to-end over a stratified corpus of FLOSS projects and
produce per-project token-authorship datasets that share one schema, on a single
workstation (16 cores, 30 GB RAM, one NVMe).

## 1. Two layers

This repository is an **orchestrator around cregit's existing per-project
pipeline**, not a reimplementation of it. cregit internals are untouched except
for forwarded flags (§4).

1. **Per-project layer** — cregit's `run_pipeline_process.sh`, one work directory
   per project, embarrassingly parallel across projects.
2. **Corpus layer** — selection, provenance, validation, retention and
   anonymization. Pure functions over committed inputs or finished artefacts, so
   each is cheap and re-runnable at any time.

What already existed and is reused rather than rebuilt:

| Asset | Role |
| --- | --- |
| `run_pipeline_process.sh` | parameterized per-project pipeline, incremental and resumable |
| the blob-map incremental engine | source repo → tokenized repo, with a resume frontier |
| `tokenize.pl` | dispatches by extension (srcML for C-family, a Rust lexer for `.rs`, ctags) |
| `generate_dataset.py` | blame + SQLite → one Parquet |

```
manifest.*.tsv ──► ctp.py run ──► run_pipeline_process.sh ──► <name>-dataset.parquet
                                                                    │
                        validate.py ──► .validated stamp ◄──────────┤
                        retain.py   ──► delete memo/ and html/  ◄────┤
                        consolidate.py ──► ctp.duckdb (derived index)
                        anonymize_parquet.py ──► verify_anon.py ──► release
```

## 2. The manifest is a five-field contract

```
name	url	category	file_mask	size_class
```

Three parsers unpack exactly those five positions and four tests assert them.
That rigidity has two consequences:

- The 29 per-project provenance fields could not be added as manifest columns.
  They live in `project_meta.json`, a **sidecar keyed by manifest name and joined
  on `clone_url`**, passed to the generator with `--project-meta`.
- A sixth `pinned_commit` field cannot be added either, so runs are not pinned to
  a revision. This is a known limitation, not a decision; see
  `DATASET-SCHEMA.md` §5.

`file_mask` is the **same universal mask for every project**: the union of every
extension the tokenizer can parse, derived in `file_mask.py` from one extension
list rather than typed out, because a mask and an extension list maintained
separately drift in both directions and both directions are silent. It used to
come from a per-language matrix keyed on GitHub's primary language, which dropped
a polyglot project's other languages; `data/mask-impact.csv` measures what that
cost. The column stays because it records which mask a Parquet was built with.
Projects whose primary language has no tokenizer are still excluded at selection
time.

Selection heuristics are a separate concern: `select_corpus.py` emits candidate
rows for curation, and the runner only consumes a curated manifest.

## 3. Per-project pipeline: the delta

Three flags forwarded to `run_pipeline_process.sh`:

| Flag | What it really does |
| --- | --- |
| `--skip-html` | **Prevention.** The HTML view step is guarded, so `html/` (94–255 MB per project) is never created. Nothing downstream reads it. |
| `--drop-memo` | **Cleanup, not prevention.** The tokenizer dies without a memo directory, so `memo/` is always written, then deleted once the project validates. `--no-memo` is an alias and warns about this. |
| `--mask-widened` | Lets a resume accept a wider mask instead of rebuilding. Needs `--from-step 2`, because a step-1 run deletes the work directory first. |

Blame **cannot** be skipped: the generator consumes `--blame-dir`.

## 4. Runner: files are the ground truth

Stdlib Python, no SQLite state authority. Snakemake was evaluated and rejected as
not a tool this research community reads; GNU Parallel was built, validated, then
superseded by Python for transparency.

State lives in files:

| File | Role |
| --- | --- |
| `manifest.*.tsv` | one row per project |
| `<out>/<name>/<name>.validated` | written only after the validation gate passes |
| `metrics.tsv` | append-only, one row per phase attempt. Seven fields, asserted by a test |
| `resources.tsv` | append-only resource samples, every 15 s. A second ledger on purpose, so the seven-field contract above stays fixed |
| `runs.log` | one start/end row per runner invocation |
| `ctp.duckdb` | **derived index**, rebuilt on demand by `./ctp.py db`. The runner never dual-writes state |

Scheduling is a `ThreadPoolExecutor` with a global `--jobs N`, plus retry passes
over failures. Four things are load-bearing:

- **flock per project**, held for the whole job, so two runner invocations never
  collide.
- **A disk floor**: below the threshold a project is deferred rather than started.
- **Environment capture**: the cregit environment is resolved *once* per run and
  passed to every subprocess. Concurrent `devenv shell` invocations race on a
  shared Nix GC root and kill each other.
- **Path canonicalization**: every path is resolved before the tokenizer is
  invoked, because the blob map stores its invocation string in a meta table and
  refuses resume when that string differs. Home-directory symlink aliasing has
  caused a refusal in practice.

Logs live under `state/<project>/logs/`, beside `metrics.tsv` — **not** in the
project work directory, which a from-scratch run deletes.

## 5. Disk lifecycle

`memo/` plus `html/` is 68–96% of a work directory. Measured on four pilots,
`memo/` alone was 45–88%. Keeping both across a large corpus does not fit on the
disk; dropping both is what makes the corpus feasible.

| Artefact | Keep? | Why |
| --- | --- | --- |
| `<name>-dataset.parquet` | yes, forever | the product |
| `<name>-original.db`, `-cregit.db`, `-persons.db` | yes | cheap, needed for audits and re-derivation |
| `<name>-original.git`, `<name>-cregit.git` (bare) | yes, until publication | re-derivation and review responses |
| `<name>-blobmap.db` | yes, until publication | cheap resume and incremental re-runs |
| `memo/` | **no** | proven negligible value; `retain.py` deletes it |
| `html/` | **no** | never generated under `--skip-html` |
| working clones, `blame/` | re-derivable | not deleted by `retain.py` today |

`retain.py` implements exactly the two deletions: `memo/` and `html/`, and
nothing else. It is dry-run by default, needs `--apply`, refuses a project whose
keepers are missing or empty, and refuses a subtree that holds a protected name.
The cost of dropping `memo/` is that a later incremental re-run of that project
re-tokenizes from cold — accepted, because the Parquet is the product and a
validated project is not re-run.

## 6. Validation

Two gates, deliberately separate:

- `validate.py` asks *did this project produce data* — file size and row count.
  It writes the `.validated` stamp, and it does **not** check columns.
- `validate_schema.py` asks *do all projects agree* — every column name, type and
  position against `EXPECTED_COLUMNS`, reporting every drift rather than the
  first. A corpus is unusable if one project has 38 columns and another 70, or if
  `token_index` is BIGINT in one file and VARCHAR in another: a consumer would
  union them and get silent nulls.

The run invokes only the first, so the schema gate has to be run over the corpus
explicitly. Both keep the parquet path as a bound query parameter rather than
pasting it into SQL, because project names come from a manifest.

The checks are plain `if` statements rather than `assert`, because `assert`
vanishes under `python -O` and a gate an interpreter flag can delete is not a
gate.

## 7. Corpus-level stages

Built:

1. **`select_corpus.py`** — rosters and searches → control facts → `candidates.csv`
   → a stratified `manifest.sample.tsv`. See `CODEBOOK.md`.
2. **`shared_history.py`** — finds projects that carry another project's history,
   which GitHub's fork flag does not. They are flagged, not excluded.
3. **`project_meta.py`** — the 29-field provenance sidecar.
4. **`build_domain_map.py`** — the domain→firm map, from public affiliation data
   plus a curated overlay applied last, so a rebuild keeps the corrections.
5. **`consolidate.py`** — `ctp.duckdb`: a `projects` state table, `phase_metrics`,
   and a `tokens` view over every Parquet whose schema matches the contract. The
   manifests indexed are selectable and default to the corpus run set; a
   non-conforming file is excluded **by name, counted in the summary**, because a
   silent exclusion is worse than the crash it replaces — the row count then
   looks plausible.
6. **`anonymize_parquet.py`** / **`verify_anon.py`** — the release path. The
   e-mail local part becomes `author_<token>` and names become `Author <token>`,
   through a single registry so one person has one pseudonym everywhere,
   including inside the trailer arrays. **The e-mail domain is preserved on
   purpose**: firm attribution resolves from the domain, so replacing it would
   collapse every firm to unknown and destroy the analysis the dataset exists to
   support. Column handling is fail-closed.

   **A pseudonym is a salted keyed hash, and it is STABLE across releases.**
   `token = blake2b(space || NUL || lowercased value, key = salt)` taken to 14
   lowercase hex characters, so it depends on the value and the salt and on
   nothing else. Two releases whose file sets differ therefore diff cleanly, and
   a reader can follow one contributor from one release to the next.

   - **The salt is private, held outside this repository, and never published.**
     It is read from `--salt-file`, `$CTP_ANON_SALT_FILE`, `$CTP_ANON_SALT` or
     `~/.config/cregit-token-pipeline/anon-salt`, in that order; `.gitignore`
     carries patterns for it as a second line of defence. A missing or
     under-32-byte salt exits **2** with the remedy printed. Nothing ever
     generates one, because a fresh salt per run would renumber every pseudonym
     on every run and say nothing.
   - **The reverse map — real value → pseudonym — is never published and never
     written to disk.** It lives in memory for the length of one run. The
     `--report` JSON carries counts and shapes only. So unlike the superseded
     scheme, the release is **not** reversible from public inputs.
   - **Rotating the salt renumbers everybody.** That is the cost, and it is the
     whole cost: a rotation makes the next release unlinkable to every release
     before it. Rotate only on a suspected salt leak or a pseudonym collision.
     Losing the salt has the same effect as rotating it, without the choice, so
     back it up wherever the release itself is backed up.
   - **A collision fails the run.** 14 hex characters is 56 bits. At the measured
     28,339 distinct addresses over 186 conforming files there are
     n(n−1)/2 = 401,535,291 pairs against 2^56 = 72,057,594,037,927,936, so the
     chance that any two share a pseudonym is about **5.6 × 10⁻⁹** — one release
     in 180 million, and 5.6 × 10⁻⁷ even at ten times the corpus. 48 bits would
     be 1.4 × 10⁻⁶, too coarse for something that blocks a release; 64 bits costs
     two characters per id and buys nothing usable. If it ever happens,
     `build_registry` raises `CollisionError` and the release stops rather than
     merging two contributors and quietly lowering the distinct-person count.
   - **The hash is fast on purpose.** Keyed blake2b costs 26 ms for 28,339
     values, and the whole six-file release path moved from 76 s to 79 s. A
     deliberately slow password KDF (bcrypt, scrypt, argon2, high-iteration
     PBKDF2) would raise the cost of a brute-force guess of the inputs *by an
     adversary who already has the salt* — a threat addressed here by keeping the
     salt out of the release, not by burning wall-clock time. If that trade is
     ever revisited, this is the paragraph to argue with.

   **Why this replaced sequential ids, measured.** Until this change an id was
   the ordinal position of a value in the sorted distinct set of one invocation.
   That is deterministic but not stable: inserting one value shifts every value
   after it. Over six corpus Parquets, dropping one file from the invocation
   renumbered **150 of the 153** addresses present in both runs — **98.0%**. The
   same experiment after the change renumbers **0 of 153 — 0.0%**, and the five
   shared output Parquets are byte-identical between a five-file and a six-file
   release. That 98.0% describes the **superseded** design; it is kept here as
   the reason the change was made. `tests/test_anonymize_parquet_e2e.py`
   requirement 7 pins the 0.0%.

   `verify_anon.py` checks the published files alone, **needs no salt and no
   secret**, and is therefore still the check worth putting in a release script.
   Moving to a hash cost it one widening: `author_\d+` became
   `author_[0-9a-f]+`. See `LIMITATIONS.md` and `ANON-OPEN-QUESTIONS.md`
   question 1, whose Option B is what is now implemented.

Not built. Each is a gap, not a plan:

- **A corpus-level firm or organisation rollup.** Attribution is per token only.
- **A per-project metadata table / dataset card** — name, category, URL, pinned
  sha, commit count, token rows, file count, languages, run duration, and the
  cregit and tokenizer versions.
- **A packaging step** emitting per-stratum trees with checksums and a schema
  document.
- **Provenance pinning.** No `provenance.json` per run, so a published Parquet
  cannot cite the cregit revision, tool versions and pinned commit it was built
  from. This is the single largest reproducibility gap.

## 8. Failure modes and how they are handled

| Failure | Handling |
| --- | --- |
| Crash mid-tokenize | blob-map frontier resume; the runner relaunches with the same canonical command |
| Disk full | disk-floor guard defers launches; `memo/` and `html/` dropped |
| One project poisons the run | that project fails and is retried; the corpus continues |
| Tokenizer fix lands mid-corpus | the tokenized repo and blob map are retained, so only affected blobs are re-tokenized and the Parquet is regenerated |
| Resume refused on a meta mismatch | canonical paths enforced at the runner boundary; `--mask-widened` for a deliberate mask change |
| Machine reboot | idempotent tick loop plus flock; the runner is restartable |
| A malformed stamp or a missing Parquet | indexed anyway, flagged (`rows_unreadable`, `parquet_missing`), named in the summary, and the exit status goes non-zero |
