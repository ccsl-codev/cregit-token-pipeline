# Change log

One entry per change, newest first. Each entry states what changed, why, and the
evidence that it works. A claim without evidence is not finished.

Read this with `docs/DESIGN.md`, which states the intended design, and with
`candidates.csv`, which is the corpus decision record.

---

## 2026-09-18

### 1. External validation of the strata against Spinellis et al. MSR'20

| | |
| --- | --- |
| Files | `validate_spinellis.py`, `tests/test_validate_spinellis.py`, `docs/SPINELLIS-VALIDATION.md`, `docs/CODEBOOK.md` (G8) |
| Why | The strata had no external check. Spinellis et al. published labels for the same population, and we already read both of their files, so the check costs nothing but was never run. |

The two label sets do not measure the same thing. They label **contribution**: a
project is enterprise if several committers share one enterprise email domain.
We label **control** (`CODEBOOK.md` section 1). So most of the report screens for
leads and cannot correct a label. Section 4 is the exception, and it is a hard
check: their registry flags are already a control fact here, emitted by
`parse_spinellis` as `F1_pool:spinellis-*`.

What it found, and it is a real defect:

`SP_MAX_PER_COMPANY = 12` caps how many strong-tier projects one company
contributes. 1,322 rows in their file pass our own strong-tier rule; the cap
admitted 400. The cap is right for sampling. But a capped-out project does not
leave the pipeline — it re-enters through `ghsearch` or `cncf-landscape` and
takes that source's stratum. So a cap on *sampling* became an error in
*labelling*. Seven rows in the eligible frame carry a registry-attested company
and label `community`, four of them with `F1_residual:none`, which is no control
fact at all:

`abseil/abseil-cpp`, `awsdocs/aws-doc-sdk-examples`,
`firecracker-microvm/firecracker`, `googlecontainertools/jib`, `grpc/grpc-java`,
`spinnaker/spinnaker`, `z3prover/z3`.

Recorded as gap **G8**. Not fixed here: the fix changes strata, so it must be one
deliberate change with a re-draw, not a side effect of adding a measure.

The reverse direction is not our defect. 138 rows we label `company-owned` sit in
their non-enterprise cohort, under `google`, `alibaba`, `facebook`, `intel` and
`oracle`. We hold a namespace fact. Their heuristics need several committers on
one enterprise domain, so staff who commit from personal addresses defeat them.
That measures their recall, and it is a result worth reporting.

Evidence:

```sh
./validate_spinellis.py -o docs/SPINELLIS-VALIDATION.md   # regenerates the report
./run_tests.sh                                            # 969 passed, validate_spinellis.py 100.0%
```

The script imports its thresholds from `select_corpus.py` rather than restating
them, so the audit cannot drift from the selector it audits. A test asserts that
binding. The first test run found a division by zero on an empty frame, now fixed.

## 2026-09-13

### 1. Test harness, and an 80% coverage floor

| | |
| --- | --- |
| Files | `pytest.ini`, `.coveragerc`, `conftest.py`, `run_tests.sh`, `.gitignore` |
| Why | The repository had **zero tests** while it produced research data. |

Run everything with one command:

```sh
./run_tests.sh              # whole suite, then the coverage report
./run_tests.sh -k domain    # one subset
```

`.coveragerc` sets `fail_under = 80`, so a change that drops coverage below the
floor fails the run. Tests that need a live network or an authenticated `gh`
carry the `network` marker and `run_tests.sh` deselects them, so the result does
not depend on the internet or on a rate-limit window.

Create the environment once:

```sh
python3 -m venv .venv && .venv/bin/pip install pytest coverage
```

### 2. `gh()` now separates a dead repository from a throttled call

| | |
| --- | --- |
| File | `select_corpus.py` |
| Why | **A previous run cached 2,734 live repositories as dead.** |

The old `gh()` returned `None` on every failure, and `cmd_enrich` cached that as
`{"error": "unreachable"}` forever. A 404 and a rate-limit 403 became the same
fact. The cache held 5,803 entries of which 2,734, or **47%**, were marked
unreachable. No plausible rate of dead repositories explains 47%, so the corpus
of 1,423 projects was an undercount.

Now:

- `gh()` returns `None` only for a genuine 404, and raises `RateLimited` on a
  transient failure. A 404 is a fact worth caching. A 403 is a queue.
- GraphQL reports a missing repository as `Could not resolve to a Repository`
  with exit code 1, not as a 404. Both spellings count as gone.
- `rate_limit_wait()` reads the reset time from GitHub and sleeps until then.
  `gh api rate_limit` costs no quota.
- `cmd_enrich` catches `RateLimited`, saves what it has, and exits 2. It never
  records a transient failure as dead.
- `--retry-errors` drops cached failures so they are fetched again.
- `save_json()` writes through a temporary file and renames, so an interrupted
  run cannot truncate a cache that cost thousands of API calls.
- The namespace cache is persisted to `.corpus-cache/org-meta.json`. It used to
  live in memory only and was re-paid on every restart.

Evidence:

```
live  : dict          # repos/torvalds/linux
dead  : None          # repos/ellianco-no-such-org-xyz/nope
gqlbad: None          # commit_count on a missing repo, no exception
gqlok : 12135         # tmux/tmux, matches the previously verified count
```

### 3. Metadata lookup is case-insensitive

| | |
| --- | --- |
| File | `select_corpus.py`, `cmd_emit` and `cmd_enrich` |
| Why | Sources disagree on the case of `owner/repo`. |

GitHub treats `owner/repo` as case-insensitive. The Spinellis cohort carries
2020 GHTorrent spelling. A case-sensitive lookup reported an already enriched
project as `not-enriched` and dropped it. It also paid twice for the same
repository under two spellings.

Evidence: eligible projects went 1,419 → **1,423** after the fix, recovering the
four projects the cohort had shadowed.

### 4. Stratum renamed `single-vendor` → `company-owned`

| | |
| --- | --- |
| File | `select_corpus.py` |
| Why | F2 proves only that one company owns the namespace. |

True single-vendor control needs F3, the CLA, which assigns copyright to one
counterparty. F3 is not implemented. So `google/leveldb` and a CLA-governed
product repository both label `company-owned` today. Rename back only after F3
runs.

`LEGACY_STRATUM` maps the old label forward on load. The JSON caches were
written before the rename and are replayed verbatim, so without the map a stale
cache reintroduced the dead label: the first run after the rename produced a
residual `single-vendor` stratum of 20 projects. Deleting the caches would have
worked too, and would have cost thousands of `gh` calls.

### 5. Community stratum gets a published candidate pool

| | |
| --- | --- |
| File | `select_corpus.py`, `parse_spinellis_cohort()` |
| Why | Community was a residual with no citable definition. SFC and SPI yielded 3 rows. |

Source: the 311,223-project comparison cohort from Spinellis et al., MSR 2020,
Zenodo `10.5281/zenodo.3742962`, CC-BY-4.0.

Be exact about the fact. Cohort membership means their heuristics found **no
enterprise signal**, so it is an *absence* of a corporate signal, not a positive
community control fact. Community stays negatively defined. What improves is that
the negation is now published, versioned and reproducible instead of being
whatever our own GitHub search left over.

Thresholds match the other sources, so eligibility does not vary by stratum:
more than 400 stars as in the starred search, at least 200 commits as in the
strong tier. Result: **19,375 candidates** from 311,223 rows.

These rows carry `weak=True`. `judge()` relabels any of them whose namespace
belongs to a company (F2) or a foundation, so the label stays open.

### 6. `pipeline.cfg` points at `cregit-issue61`

| | |
| --- | --- |
| File | `pipeline.cfg` |
| Why | It is the only checkout with `rustTokenizer`. |

The plain `cregit` checkout cannot tokenise the 315 Rust projects in the corpus.

**This change alone is not enough. It opened a defect, recorded below.**

### 7. `retain.py`, `--skip-html`, `--drop-memo`

| | |
| --- | --- |
| Files | `retain.py` (new), `ctp.py` |
| Why | At 1,423 projects, keeping `memo/` and `html/` needs 1.4-3.5 TB against 1.2 TB free. |

| Flag | What it really does |
| --- | --- |
| `--skip-html` | **Prevents the work.** Step 9 of `run_pipeline_process.sh` is guarded, so `prettyPrintFiles.pl` never runs and `html/` is never created. Step 10 does not read it. |
| `--drop-memo` | **Cleanup only.** `tokenBySha.pl` dies without `BFG_MEMO_DIR`, so `memo/` is always written, then deleted after the project validates. |
| `--no-memo` | Alias for `--drop-memo`. It warns that it does not prevent the write. |

`retain.py` is dry-run by default and needs `--apply` to delete. It refuses to
prune a project whose keepers are missing or empty, and refuses when a protected
entry sits inside a subtree it would remove.

Measured on the four pilot projects: 5.6 GiB reclaimable of the 5.9 GiB those
workdirs hold. Keepers are 58-200 MB per project, so 1,423 projects need about
**164 GB** instead of 1.4-3.5 TB.

The `run_pipeline_process.sh` change lives in the `cregit-issue61` repository and
is **uncommitted there**.

---

## Open defects

| # | Defect | Impact |
| --- | --- | --- |
| D1 | `ctp.py:192-195` builds `--work-dir` and `--file-filter`. The configured `cregit-issue61/run_pipeline_process.sh` accepts `--work` and `--mask`, and exits 2 on an unknown argument. | **`ctp.py run` cannot start.** Blocks the corpus run. |
| D2 | `cregit-issue61/run_pipeline_process.sh` runs `rm -rf "$WORK"` when `FROM_STEP=1`. `ctp.py` opens its log directory inside `$WORK` first. | Deletes the logs mid-run and breaks the resume contract. |
| D3 | `ctp.py` hard-requires exactly five manifest fields, so `pinned_commit` cannot be added as a sixth. | No commit pinning, so a run is not reproducible. |
| D4 | The corpus is emitted but **not frozen**. No PRISMA-style flow diagram yet. | `candidates.csv` records every exclusion reason, so the flow is derivable. |
