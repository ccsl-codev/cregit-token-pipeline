# End-to-end runs

This page covers a long run over a projects dataset: from the selection frame
to one joined, cleaned Parquet per project, with an audit record for each step.

## 1. From the frame to a manifest

`build_manifest.py` reads the selection frame and writes three files: the
manifest, the `project_meta.json` sidecar, and an order file.

```sh
./build_manifest.py frame.csv --manifest manifest.census.tsv \
    --project-meta project_meta.census.json
```

Do not build a manifest for a new run from `candidates.csv`. The frame is the
only input.

### The frame contract

The frame is a UTF-8 CSV file with a header row.

| Column | Required | Meaning |
|---|---|---|
| `slug` or `name_with_owner` | yes | `owner/repo` on its host. It seeds `u`. |
| `host` | yes | For example `github.com`. Blank means `github.com`. |
| `repo_url` | yes | The clone URL. |
| `head_oid` | yes | The commit to analyse, 40 lowercase hex digits. |
| `u` | yes | The permanent random number of the row. See below. |
| `size_class` | yes | `S`, `M` or `L`. |
| `labels` | yes | Free text. It becomes the manifest category. |
| `category` | no | Overrides `labels` as the manifest category. |
| `file_filter` | no | The file mask. Blank means the universal mask. |
| `included` | no | When present, only rows with `1`, `true` or `yes` are kept. |
| any of the 29 provenance fields | no | Fills that field of the sidecar. |

`u` is `sha256("20261110:" + slug)`, read as a fraction: the first 53 bits
divided by 2^53. The value is exact in a float and always below 1. The builder
computes `u` itself and stops when the frame's value differs, because a
different salt or formula would change the run order.

The tests use `tests/fixtures/frame.sample.csv` as a small example.

### What the builder writes

- The manifest lists the projects in `u` order, lowest first. So the projects
  finished at any date are a random sample of the frame. The sixth column pins
  each project to `head_oid`.
- The project name is `owner__repo`. A host other than GitHub is a prefix:
  `gitlab.com__group__repo`. The builder refuses two rows with one name.
- `project_meta.json` holds the 29 provenance fields per project.
  `clone_url`, `owner`, `repo`, `size_class`, `manifest_category` and
  `file_mask` come from the row. `provenance_status` names the frame file and
  the start of its sha256.
- The order file maps each queue position to `slug`, `host`, `u`, `head_oid`
  and `labels`.
- `--limit N` keeps the first N projects in `u` order, for an early batch.

## 2. One command for the whole run

```sh
export CTP_CONFIG=/path/to/census.cfg      # optional: cregit_dir and output_dir
./ctp.py census --manifest manifest.census.tsv \
    --project-meta project_meta.census.json \
    --workers 3 --retries 1 --skip-html
```

For each project, in manifest order, `census` runs these steps:

1. `clone`: `pin.py` fetches the pinned commit into
   `<output_dir>/.ctp-pinned/<name>.git` and keeps only that commit's history.
   It fetches by SHA first. When the host refuses that, it fetches every
   branch and tag. A commit that is on neither stops with exit 3.
2. `pipeline`: cregit's `run_pipeline_process.sh`, cloning the staging clone.
   Then ctp reads HEAD of the runner's working clone. A checkout that is not
   the pinned commit fails the project.
3. `validate`: `validate.py`, which writes the `.validated` stamp.
4. `schema`: `validate_schema.py` on the Parquet.
5. `anonymize`: only with `--anonymize SCRIPT`. See below.
6. `join`: `consolidate.py --join`, which adds the project to `ctp.duckdb`.
7. `cleanup`: deletes the clones, `memo/`, `blame/` and the work databases.
   It keeps the Parquet, the stamp, the two small persons files, `html/` and
   `anon/`. The logs in `state/` and the ledger are outside the workdir.

A five-column manifest still works. Such a project runs the remote HEAD on the
day of its clone, and the ledger says `"pinned_sha": "unpinned"`.

`CTP_CONFIG` names a config file to use instead of the tracked
`pipeline.cfg`, so a census can use another cregit checkout or output
directory without editing it.

### The newest commit: `--latest`

With `--latest`, a project does not run at its manifest commit. When its first
attempt starts, the census reads the head of the default branch with
`git ls-remote` (300 s timeout) and pins that commit. So each project runs at the
newest commit on the day it runs.

- The ledger records both commits: `pinned_sha` is the commit that ran, and
  `snapshot_sha` is the manifest commit. A `resolve` row before the clone
  records the branch, the time of the read, and whether it was reused.
- `state/<name>/latest.json` keeps the choice. A later attempt of the project
  reuses it, so a resumed run keeps its finished work at one commit. To read
  the head again, delete that file.
- A remote that does not answer fails the project at the `resolve` step.

When the workdir holds a clone of another commit than the pinned one (for
example after the pin changed), the runner gets `--force-clean`: the old work
is for another commit, so the step-1 wipe is allowed even with a large memo. The
pipeline row records it as `resume.stale_workdir`.

### Gates, stop and resume

- A project starts only with `--disk-floor-gb` free disk (default 150) and
  `--min-free-mem-gb` of MemAvailable (default 8). While a gate is closed, the
  census waits and says why.
- To stop cleanly, `touch STOP` in this repository (or the `--stop-file`
  path). The running projects finish, and no new one starts. The first
  SIGINT or SIGTERM does the same. A second one terminates the running
  projects. The census refuses to start while the stop file exists.
- To resume, run the same command again. A project whose last state is
  `done` is skipped. A validated project that is not `done` runs only the
  steps after validation. A project that crashed mid-pipeline restarts the
  runner at step 2 when its clone and blob map survive, so its
  tokenizations are kept.
- A failed project runs at most `1 + --retries` times, counted across
  restarts. An attempt that died mid-step, with its machine, counts too, so a
  project that kills its run is not retried for ever.
- Each phase holds the project's lock, and so does every process it starts.
  If ctp dies with `kill -9`, its runner tree keeps the lock, and a new
  census waits for it rather than racing it.
- Exit codes: 0 when every project succeeded, 3 when a stop left the run
  incomplete without a failure, 1 otherwise.

### Open decisions: HTML and anonymization

Both are off by default, until the decision is made.

- `--drop-html` deletes `html/` in the cleanup. `--skip-html` does not write
  it at all.
- `--anonymize SCRIPT` runs `python3 SCRIPT <workdir>/anon <parquet>` after the
  schema check, the interface of `anonymize_parquet.py`. The ledger records the
  sha256 of each output. The raw Parquet is still the one joined into
  `ctp.duckdb`. No anonymizer for the 67-column schema ships here.

## 3. Audit a project

Every step appends rows to `ledger.jsonl` in this repository. The file is only
appended to: each row is written whole under a lock and fsynced.

```sh
./ctp.py audit DaveGamble__cJSON          # one line per step
./ctp.py audit DaveGamble__cJSON --json   # every row, whole
```

Each step writes a `started` row before it runs and a row when it ends.
`audit` hides a started row once its step has ended, and shows one that never
ended as `interrupted`. The last row of an attempt has `"step": "project"` and
a `state`:

| State | Meaning |
|---|---|
| `done` | Validated, joined and cleaned. |
| `done-dirty` | The Parquet is valid, but the cleanup did not finish. The next census retries the steps after validation. |
| `failed` | `failed_step` names the step. |
| `deferred` | Another process held the project's lock, or the disk was below the floor. |

Every row carries `run_id`, `queue_pos`, `queue_total`, `attempt`, the
`manifest_row`, `pinned_sha`, `checked_out_sha`, `file_mask_sha256`, `tools`
and `flags`. A step row adds `start_utc`, `end_utc`, `duration_s`,
`exit_code`, `argv` and `log`. Some steps add more:

- `clone`: `pin`, with the fetch method, the remote's default branch and its
  HEAD at clone time.
- `pipeline`: `resume`, when the census restarted the runner at step 2.
- `validate` and the final row: `parquet`, with path, bytes, sha256 and rows.
- `anonymize`: `anonymized`, with path, bytes and sha256 per output.
- `cleanup`: `cleanup`, with what was removed, the bytes freed, what was kept,
  and the errors.

`tools` holds the ctp and cregit commits (with `-dirty` when a tracked file
differs), the srcML version line and binary path, the blobExec jar sha256, and
the git version. `flags` holds every option as typed and as checked, the
manifest and config paths with their sha256, and the cregit and output
directories. The run-start and run-end rows bracket each run.

To check one project by hand:

```sh
jq -c 'select(.project == "antirez__kilo" and .step == "project")' ledger.jsonl
sha256sum <output_dir>/antirez__kilo/antirez__kilo-dataset.parquet   # equals parquet.sha256
./validate_schema.py <output_dir>/antirez__kilo/antirez__kilo-dataset.parquet
```

## 4. One census per checkout

The ledger, `state/` (logs and locks), `metrics.tsv`, `runs.log` and
`ctp.duckdb` live in this checkout, whatever `CTP_CONFIG` says. Two censuses
from one checkout share them, so run one census per checkout. A second census
on the same output directory is safe, because of the locks, but each would
count the other's running attempts as interrupted.

## 5. ctp.duckdb during a census

Each join holds `ctp.duckdb.lock` and writes `ctp.duckdb`. DuckDB lets one
process write, so do not keep the file open in another process during a
census: a join retries for a minute, then fails the project. Query a copy, or
the Parquets. `./ctp.py db --manifest manifest.census.tsv` rebuilds the whole
file from the stamps, and gives the same tables.
