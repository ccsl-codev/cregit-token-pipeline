# Rename candidates: identifiers with "census" or "corpus"

The refactor keeps every existing identifier, file name and printed text,
because the output must stay identical. A later rename pass can change them.
New prose in this branch says "dataset" or "dataset run". New identifiers of
this branch do not contain either word.

The counts are at the head of refactor/readability.

## Code identifiers

| Identifier | Where | Uses | Note |
| --- | --- | --- | --- |
| `CORPUS` | `ctplib/config.py`, `consolidate.py`, `retain.py`, `firm_attribution.py` | 39 | The repository root. Proposal: `REPO_ROOT`. |
| `cmd_census` | `ctplib/scheduler.py`, `ctplib/cli.py` | 12 | Proposal: `cmd_dataset`, with the subcommand. |
| `census_args`, `census_ready` | `tests/ctplib/test_scheduler.py` | 29 | Test helpers. |
| 19 test function names with `census` | `tests/ctplib/test_scheduler.py` | 19 | For example `test_census_runs_in_manifest_order_and_records_the_queue_position`, `test_cmd_census_exits_3_when_stopped_cleanly`, `test_main_dispatches_census`. |
| 3 test function names with `corpus` | `tests/test_retain.py:350,904,1214` | 3 | `test_a_whole_corpus_dry_run_is_clean_when_a_stamp_is_empty`, `test_the_main_dry_run_total_spans_a_partly_blocked_corpus`, `test_a_whole_corpus_sweep_skips_the_live_project_and_prunes_the_rest`. |

## Names that users, scripts or records see

A rename of these changes the output or the interface, so it needs a
decision and a note in the docs.

| Name | Where | Kind |
| --- | --- | --- |
| `census` | `ctplib/cli.py` | The subcommand `ctp.py census`; also the `command` field of the run-start ledger row. |
| `census-start`, `census-end` | `ctplib/scheduler.py` | Event names in `runs.log`. |
| `census: N projects, ...`, `census finished rc=...` | `ctplib/scheduler.py` | Log lines. |
| `... so the census would stop at once` | `ctplib/scheduler.py` (`stop_file_of`) | Error message. |
| `run the corpus` | `ctplib/cli.py` | Help text of `ctp.py run`. |
| `corpus  [bar]` | `ctplib/report.py` | The first word of the `progress` output. |
| `corpus` in a message | `ctplib/workdir_state.py` (`check_memo_dir`) | Log line. |
| `../cregit-workspace/corpus-files` | `ctplib/config.py`, `retain.py`, `consolidate.py` | Default `output_dir`; also in the tracked `pipeline.cfg:9`. |
| `corpus-*.log` | `.gitignore:11` | Ignore pattern. |
| `# Corpus manifest (TSV)` | `manifest.tsv:1` | Header comment of the example manifest. |
| `ctp-census`, `out-census`, `census.cfg` | `refactor-notes/tools/gate.sh` | Paths of the gate (tool of this branch, follows the subcommand name). |

## Documents

The existing prose uses the words in README.md (8 lines), docs/DESIGN.md
(8 lines) and docs/E2E.md (19 lines). The branch did not rewrite that prose;
its new sentences avoid the words.
