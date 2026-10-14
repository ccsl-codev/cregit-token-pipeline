# Baseline and identity gate of refactor/readability

Base commit: `ca113e3` (feat/e2e-runner, "feat(ctp): pass --tokenizer-worker to
the runner"). Work tree: `/local/home/ellianco/Projects/ctp-readability`, branch
`refactor/readability`. Rule: identical output, no behavior change.

## Test environment

- `.venv` in the work tree, made with
  `python3 -m venv --system-site-packages` from the Nix interpreter
  `/nix/store/5gnikzfb2hkysq9d45mjl74h7vj4c5ad-python3-3.14.7-env`. That
  interpreter has duckdb 1.5.5 and pytest 9.1.1. pip added coverage 7.16.0 and
  ruff 0.17.0.
- `./run_tests.sh` runs the whole suite under coverage. `.coveragerc` fails the
  run below 80%.
- With duckdb present, no test skips. The cregit checkout that `pipeline.cfg`
  names (`../cregit-workspace/cregit-issue61`) exists on this host, so the
  tests in `tests/test_mask_drift.py` run too.

## Test result

| | Base `ca113e3` | Head of refactor/readability |
| --- | --- | --- |
| Passed | 735 | 737 |
| Failed, errors | 0 | 0 |
| Skipped, xfail | 0 | 0 |
| Coverage | 99.2% | 99.3% |

The two new tests are in `tests/test_entry_point.py`: `ctp.main is cli.main`,
and `ctp.py --help` runs. The 344 tests of `tests/test_ctp.py` moved to
`tests/ctplib/`; the set of collected test ids is the same. Four pairs of
copied tests became four parametrized tests with two ids each, so the count
stays the same.

Collected tests per file at the base: test_build_manifest 30,
test_consolidate 55, test_ctp 344, test_file_mask 34, test_firm_attribution 36,
test_ledger 13, test_mask_drift 8, test_pin 18, test_retain 157,
test_validate 17, test_validate_schema 23.

## The end-to-end gate

The gate runs the tool twice on one real project and hashes every output.
Its scripts are in `refactor-notes/tools/` (copies) and in
`/local/home/ellianco/Projects/cregit-workspace/refactor-gate/scripts/`.

- Project: `libexpat__libexpat`, pinned at
  `a76b1174c1ea52abb639a4c69d52b630a8a106a2`, with the batch 1 provenance
  sidecar. This is the usual cregit gate project.
- cregit: a `cp -a` copy of `census-local-c/cregit` (commit `57551d7`) in
  `refactor-gate/cregit`. The gate only reads it.
- Python of the run: a separate venv with Python 3.12.13 and duckdb 1.5.5, as
  the live runs use.
- Each gate run clones the repository at one commit into fixed paths under
  `refactor-gate/run/`, then moves the result to `refactor-gate/results/LABEL/`.
  Fixed paths keep the path strings in the ledger the same between runs.

### What the gate runs

1. `ctp.py --help` and the `--help` of every subcommand.
2. `ctp.py census` with `--workers 1 --retries 0 --skip-html --tokenizer-worker
   --blame-jobs 2 --memory-limit 2GB --min-free-mem-gb 10 --project-meta ...
   --anonymize anon_stub.py`. Steps: clone (pin.py), pipeline (all runner
   steps), firm, validate, schema, anonymize (a stub that copies the Parquet),
   join, cleanup.
3. Then `status`, `progress`, `audit` (a known and an unknown project), `db`,
   `validate_schema.py`, `validate_schema.py --emit-contract`, the usage error of
   `validate.py`, the dry run of `retain.py`, and a second `census` that finds
   the project done.
4. `ctp.py run --jobs 1 --retries 0 --join --drop-memo` with the same project
   flags, in a second clone and output directory. Steps: pipeline (the runner
   clones the remote), firm, validate, schema, join, cleanup (drop memo/ only).
5. Then `status`, and a second `run` that finds the project done.

Not covered by the gate: `--latest` (it reads the remote head, so the result
depends on the day), `--drop-html` and the HTML views, `--shards`,
`--retokenize`, `--mask-widened`, `--reblame`, the failure and retry paths and
the signal handlers. The unit tests cover those paths.

### How the gate compares

`normalize.py` writes one sha256 line per output:

- Every file under `out-census/` and `out-run/` hashes byte for byte, except:
  - SQLite files (`*.db`): the hash of the sorted SQL dump, with timestamps
    masked. Two runs of the base gave different bytes and row orders.
  - Git directories: the hash of the refs, of every reachable object id and,
    for a work tree, of the index entries and status. Pack files differ
    between runs.
  - `pipeline.log` of the run stage: the hash with all digits masked.
- `ledger.jsonl`, `metrics.tsv`, `runs.log`, `state/*/*.json`, the tables of
  `ctp.duckdb`, stdout, stderr, exit codes and the report output: the hash of
  a masked copy in `norm/`. The masks remove times, durations, pids, CPU and
  memory samples, free disk space, and `ctp_commit`.
- Ledger keys dropped before the hash: `run_id`, `start_utc`, `end_utc`,
  `duration_s`, `elapsed_s`, `ctp_commit`, and the byte counts `bytes_freed`
  and `removed_bytes` of the cleanup row (directory sizes change between
  runs). Every other ledger field is compared: the steps and their order,
  `status`, `exit_code`, `argv`, `log` (time masked), `pin`, `resume`,
  `parquet` (path, bytes, sha256, rows), `anonymized`, `cleanup` (removed,
  kept, errors), `tools` (except `ctp_commit`), `flags`, `manifest_row`,
  `pinned_sha`, `checked_out_sha`, `file_mask_sha256`, `state`, `outcomes`.

Two runs of the base (`base-1`, `base-2`) gave the same 590 lines. That shows
the masks remove all run-to-run noise.

### Key hashes (base and every gate run)

| File | sha256 | Bytes |
| --- | --- | --- |
| `out-census/.../libexpat__libexpat-dataset.parquet` | `14eb5d728ca6fa50623cb89088ab59348e646f1205ed3cf9f48bff0142eab799` | 2,196,248 |
| `out-census/.../anon/libexpat__libexpat-dataset-anon.parquet` | `14eb5d728ca6fa50623cb89088ab59348e646f1205ed3cf9f48bff0142eab799` | 2,196,248 |
| `out-run/.../libexpat__libexpat-dataset.parquet` | `14eb5d728ca6fa50623cb89088ab59348e646f1205ed3cf9f48bff0142eab799` | 2,196,248 |
| `out-*/.../libexpat__libexpat.validated` | `feb12fbb8fc87a24400dcbd065eeeddffe3d6259955b35b7030117be7bde1c22` | 26 |
| `out-*/.../libexpat__libexpat-persons.xls` | `289b148538d19dabedd25a13966787bfef510156c92bb86e209db654a8cfc7e8` | 64,000 |

The Parquet hash is the known cregit gate hash of `57551d7`. The full list of
590 lines is `refactor-notes/gate-sha256.txt`.

### Gate runs

| Label | Commit | After | Result |
| --- | --- | --- | --- |
| base-1, base-2 | `ca113e3` | the base | the reference |
| g1-ae6ddff | `ae6ddff` | format, lint, config, runtime, console, manifest_file | identical |
| g2-29b8b89 | `29b8b89` | all moves up to project_steps | identical |
| g3-f5a15ca | `f5a15ca` | all moves; ctp.py is only the entry point | identical |
| g4-7b20aa3 | `7b20aa3` | schedule, build_steps, run_project split; comment pass | identical |
| g5-bad7a31 | `bad7a31` | test split, cleanup_step, cmd_census, cli, consolidate, pin | identical |
| g6-ab0cf6d | `ab0cf6d` | shared refusal and runs.log writer; README | identical |

Each gate run compares its `SHA256SUMS` with `base-1/SHA256SUMS`
(`compare.sh LABEL`).

## Before and after

| | Base `ca113e3` | Head |
| --- | --- | --- |
| `ctp.py` | 2,017 lines, 98 top-level functions and classes | 10 lines: the entry point |
| `ctplib/` | none | 22 modules and `__init__.py`, 19 to 409 lines |
| Tool code, all modules | 3,890 lines (4,510 after `ruff format` alone) | 4,868 lines |
| Largest ruff complexity | `build_steps` 18, `schedule` 16, `run_project` 11 | 9 (`ledger.histories`, `retain.clean_workdir`) |
| Longest function with branches | `schedule`, 81 lines | `cmd_census`, 56 lines |
| `ruff check .` (the final config) | 41 findings | all checks passed |
| `ruff format --check .` | 22 files to reformat | clean |
| Test files | `tests/test_ctp.py`, 3,006 lines | `tests/ctplib/`, 20 files, 43 to 440 lines |

Most of the added lines come from the black-style layout of `ruff format`, the
module docstrings and import blocks of the 22 modules, and the extracted
functions.

## The tools in refactor-notes/tools

- `gate.sh`, `normalize.py`, `compare.sh`, `anon_stub.py`: the gate above.
- `mover.py`: moves named top-level definitions of `ctp.py` into a `ctplib`
  module, and points every reference (and the tests) at the new place. It is
  scope-aware, so a local variable with the same name stays. `step.sh` runs
  one move, ruff, the tests and the commit; it calls a name-clash check that
  was in `/tmp/clash.py`.
- `split_tests.py`: split `tests/test_ctp.py` into `tests/ctplib/`.
- `astcmp_nodoc.py`: checks that a change touches only comments and
  docstrings, and none that the program prints.
- `comments_dump.py`: lists every comment and docstring of a file.
