# Design

Why the pipeline is built the way it is. For what it produces, read
[`../README.md`](../README.md); the schema itself is authoritative in
`validate_schema.py`.

**Goal.** Run cregit end-to-end over a corpus of FLOSS projects and produce
per-project token-authorship datasets that share one schema, on one machine.

## 1. Two layers

This repository is an **orchestrator around cregit's existing per-project
pipeline**, not a reimplementation of it. cregit internals are untouched except
for forwarded flags (§4).

1. **Per-project layer** — cregit's `run_pipeline_process.sh`, one work directory
   per project, embarrassingly parallel across projects.
2. **Corpus layer** — the provenance guard, validation, retention and the index. Pure
   functions over committed inputs or finished artefacts, so each is cheap and
   re-runnable at any time.

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
```

## 2. The manifest is a five-field contract

```
name	url	category	file_filter	size_class
```

`ctp.py` refuses a row without exactly those five fields, naming its line, and
requires `size_class` to be S, M or L. That rigidity has two consequences:

- The 29 per-project provenance fields could not be added as manifest columns.
  They live in `project_meta.json`, a **sidecar keyed by manifest name and joined
  on `clone_url`**, passed to the generator with `--project-meta`.
- A sixth `pinned_commit` field cannot be added either, so runs are not pinned to
  a revision. This is a known limitation, not a decision: nothing in this
  repository records which commit a run cloned, beyond what git itself keeps in
  the per-project clone.

`file_mask` is the **same universal mask for every project**: the union of every
extension the tokenizer can parse, derived in `file_mask.py` from one extension
list rather than typed out, because a mask and an extension list maintained
separately drift in both directions and both directions are silent. The
column stays because it records which mask a Parquet was built with. A project
whose language has no tokenizer still fails to produce useful rows; nothing in
this repository screens for that before a run.

Choosing which repositories to run is a separate concern, and out of scope for
this tool: it consumes a manifest you write; it does not draft, sample or
curate one.

## 3. Per-project pipeline: the delta

Three flags forwarded to `run_pipeline_process.sh`:

| Flag | What it really does |
| --- | --- |
| `--skip-html` | **Prevention.** The HTML view step is guarded, so `html/` is never created. Nothing downstream reads it. |
| `--drop-memo` | **Cleanup, not prevention.** The tokenizer dies without a memo directory, so `memo/` is always written, then deleted once the project validates. `--no-memo` is an alias and warns about this. |
| `--mask-widened` | Lets a resume accept a wider mask instead of rebuilding. Needs `--from-step 2`, because a step-1 run deletes the work directory first. |

Blame **cannot** be skipped: the generator consumes `--blame-dir`.

## 4. Runner: files are the ground truth

Stdlib Python, no SQLite state authority.

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

`memo/` plus `html/` is most of a work directory. Keeping both across a large
corpus does not fit on the disk; dropping both is what makes the corpus feasible.

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

- `validate.py` is the per-project gate the run invokes: file size, row count,
  and every column name, type and position against `EXPECTED_COLUMNS`. It writes
  the `.validated` stamp only when all of them pass, so `--drop-memo` never
  deletes the `memo/` of a drifted file.
- `validate.py` runs after `firm_attribution.py`, so it checks the final form:
  cregit's 67 columns (`CREGIT_COLUMNS`) then the 3 firm columns
  (`FIRM_COLUMNS`). `firm_attribution.py` accepts only the 67-column form, or
  the 70-column form from an earlier run, and refuses a map with a repeated key,
  because a repeated key multiplies token rows through the join.
- `validate_schema.py` runs the same contract check over many files at once and
  reports every drift rather than the first. A corpus is unusable if one project
  has 38 columns and another 67, or if `token_index` is BIGINT in one file and
  VARCHAR in another: a consumer would union them and get silent nulls.
  `consolidate.py` applies it again when it builds the `tokens` view.

Both keep the parquet path as a bound query parameter rather than pasting it
into SQL, because project names come from a manifest.

The checks are plain `if` statements rather than `assert`, because `assert`
vanishes under `python -O` and a gate an interpreter flag can delete is not a
gate.

## 7. The provenance guard

`ctp.py run` refuses to reach the Parquet-writing step without `--project-meta`
unless `--allow-empty-provenance` says otherwise.

It refuses rather than warns. Nothing downstream can tell a blank column from
provenance that is genuinely unknown: the file still carries all 70 columns in
the right order, so both validation gates pass it, and a long run can finish and
publish a Parquet that is silently inconsistent with the rest of a corpus. A
warning is easy to miss at the end of a long log; a refusal is not.

The escape hatch takes no default, so it can only arrive by being typed, and it
prints exactly which columns it gave up. It is refused in turn when nothing
would actually be blank, so it cannot sit unused in a launcher script and
silence a real gap on a later run that does have one.

## 8. Corpus-level stages

Built:

1. **`consolidate.py`** — `ctp.duckdb`: a `projects` state table, `phase_metrics`,
   and a `tokens` view over every Parquet whose schema matches the contract. The
   manifests indexed are selectable and default to `manifest.tsv`; a
   non-conforming file is excluded **by name, counted in the summary**, because a
   silent exclusion is worse than the crash it replaces — the row count then
   looks plausible.

Not built. Each is a gap, not a plan:

- **Choosing which repositories to run.** No script here drafts a manifest,
  draws a sample, or assigns a project's stratum or history cluster. Those
  choices, and the 29 provenance columns that record them, are entirely on the
  person who writes the manifest and the optional sidecar.
- **Firm attribution.** cregit master emits no firm columns, so `ctp.py run`
  forwards no firm flags.
- **A per-project metadata table / dataset card** — name, category, URL, pinned
  sha, commit count, token rows, file count, languages, run duration, and the
  cregit and tokenizer versions.
- **A packaging step** emitting per-stratum trees with checksums and a schema
  document.
- **Anonymizing or pseudonymizing the output.** Names and e-mail addresses are
  published as cregit writes them; anonymizing them, if wanted, happens outside
  this repository.
- **Provenance pinning.** No `provenance.json` per run, so a published Parquet
  cannot cite the cregit revision, tool versions and pinned commit it was built
  from. This is the single largest reproducibility gap.

## 9. Failure modes and how they are handled

| Failure | Handling |
| --- | --- |
| Crash mid-tokenize | blob-map frontier resume; the runner relaunches with the same canonical command |
| Disk full | disk-floor guard defers launches; `memo/` and `html/` dropped |
| One project poisons the run | that project fails and is retried; the corpus continues |
| Tokenizer fix lands mid-corpus | the tokenized repo and blob map are retained, so only affected blobs are re-tokenized and the Parquet is regenerated |
| Resume refused on a meta mismatch | canonical paths enforced at the runner boundary; `--mask-widened` for a deliberate mask change |
| Machine reboot | idempotent tick loop plus flock; the runner is restartable |
| A malformed stamp or a missing Parquet | indexed anyway, flagged (`rows_unreadable`, `parquet_missing`), named in the summary, and the exit status goes non-zero |
