# Defects seen during the refactor (not fixed)

The refactor keeps the output identical, so it fixes no defect that changes
behavior. Each item gives the place at the head of refactor/readability, what
is wrong, and a one-line fix proposal. The user decides later.

## From the earlier reviews

1. **Tracked pipeline.cfg and a config read at import.** `pipeline.cfg:8-9` is
   tracked with the paths of the owner's layout
   (`../cregit-workspace/cregit-issue61`, `../cregit-workspace/corpus-files`).
   `retain.py:39`, `consolidate.py:25` and `ctplib/config.py:16` read it when
   the module is imported. The import-time mkdir of the old `ctp.py:71` is gone
   at `ca113e3` (`ctplib/config.py` creates no directory).
   Fix: ship `pipeline.cfg.example`, ignore `pipeline.cfg`, and read the config
   in a function on first use.
2. **An OSError before the project lock escapes `ctp.py run`.**
   `ctplib/project_steps.py:310-313` (`workdir.mkdir`, `state_dir(...).mkdir`,
   `lock_path(...).open`) is outside the guard that turns an exception into a
   failed final row. `ctplib/passes.py:56` gets the result through `pool.map`,
   so the exception ends the whole run: no run-end ledger row, no `run-end`
   line in runs.log. Reproduced: an `output_dir` under a regular file gives
   `NotADirectoryError` out of `run_passes`. `ctp.py census` is safe:
   `scheduler.finished_outcome()` catches it.
   Fix: move the three calls into the guarded block of `run_project`, or catch
   per project in `run_passes` as `finished_outcome` does.
3. **The validate gate skips the column check.** Not reproduced at `ca113e3`.
   `validate.py:40-42` rejects every drift that
   `validate_schema.compare_schema` reports: missing, unexpected, retyped and
   reordered columns. Note: `ctp.py run` without `--join` does not run the
   separate `schema` step (`ctplib/project_steps.py:133-137`); `validate.py`
   still checks the columns. Fix: none needed; close the item.
4. **status crashes without output_dir.** Not reproduced at `ca113e3`.
   `ctplib/report.py:33-36` measures the free disk of the nearest existing
   parent. `ctp.py status` and `ctp.py progress` with a missing `output_dir`
   exit 0. Fix: none needed; close the item.
5. **Three copies of lock_held.** At `ca113e3` there is one copy,
   `retain.py:176`. Nothing to deduplicate.

## Found during this refactor

6. **The steps run `python3` from the devenv PATH, not the ctp interpreter.**
   `ctplib/pinning.py:32`, `ctplib/runner_args.py:106,112` and
   `ctplib/project_steps.py:136,155,172` start `python3`, which resolves in the
   captured devenv environment. The firm step writes the published Parquet with
   the duckdb of that interpreter. The ledger records `ctp_python`, not the
   Python and duckdb versions that wrote the Parquet.
   Fix: record `python3 --version` and its duckdb version in `tools`, or start
   the steps with `sys.executable`.
7. **The status table breaks for names longer than 16 characters.**
   `ctplib/report.py:24,30` pad the name to 16 columns; `libexpat__libexpat`
   (18) pushes the other columns out of line. Fix: pad to the longest name.
8. **`config.DISK_FLOOR_GB` is global state that cmd_census changes.**
   `ctplib/scheduler.py:288` assigns it; `ctplib/project_steps.py` and
   `scheduler.gate_reason` read it. `ctplib/cli.py:227` reads it as the
   `--disk-floor-gb` default when the parser is built, so a second `main()` in
   one process shows the last value. Fix: carry the floor in `runtime.opts`.
9. **The shared run state is module-level.** `ctplib/runtime.py` holds `opts`,
   `env`, `run_info`, `live`, `lock_fds`, `procs` and `stop`, because
   `run_project` gets only the project from `pool.map`. Fix: pass one
   `RunContext` object with `functools.partial` to `run_project` and the
   scheduler.
10. **`heartbeat` lives in `ctplib/passes.py:26` but the scheduler uses it
    too** (`ctplib/scheduler.py:222`). Fix: move it to its own small module.
11. **Documentation gaps.** docs/E2E.md lists the final states done,
    done-dirty, failed and deferred, but `ledger.py` also names `skipped`. The
    exit codes 1 and 2 of `pin.py` and the exit codes of `consolidate.py` are
    only in their docstrings. A torn last line of `ledger.jsonl` after a crash
    is skipped by `ledger.read` with a note; no document says so.
    Fix: add these facts to docs/E2E.md.
12. **The help text of `run` says "run the corpus"** (`ctplib/cli.py:164`).
    It is user-visible text, so the refactor keeps it. Fix: "run every project
    of the manifest" in the rename pass.

## Outside ctp (seen in the gate)

13. **cregit outputs that are not byte-reproducible.** Two runs of the same
    commit give different bytes for `*-persons.db` (a generation timestamp
    column), `*-blobmap.db` (a timestamp column and the row order) and the pack
    files of the generated repositories. The Parquet is identical. Fix: in
    cregit, if byte-reproducible side outputs matter.
