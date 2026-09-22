# The copy-detection re-blame

Why every project's attribution was rebuilt in September 2026, what it cost, and
what it changed. [`LIMITATIONS.md`](LIMITATIONS.md) states the defect as a limitation
of the data; this file is the engineering record.

Read this before you compare any number in the paper against an earlier draft. The
correction moves up to a quarter of a project's tokens to a different author.

---

## 1. The defect

`blameRepo/formatBlame.pl` ran a plain `git blame`. Upstream cregit's `-C100` copy
detection was commented out, and no commit in this repository did that, so it arrived
that way from upstream.

Plain `git blame` follows a whole-file rename. It does not follow content moved
*between* files. So a header split, a refactor, or a vendored-code drop re-credited
every token it touched to whoever moved it.

The defect was found by cross-checking an independent cregit implementation, not by
reading the code. See [`SPINELLIS-VALIDATION.md`](SPINELLIS-VALIDATION.md) and the
`power_supply.h` case in [`LIMITATIONS.md`](LIMITATIONS.md).

## 2. The decision

Enable `-C100` and re-blame everything. The alternative — ship the gap as documented —
was rejected because the defect changes a project's single largest author, which is a
headline result rather than a footnote.

## 3. The code changes

| commit | repo | change |
| --- | --- | --- |
| `e2f7de6` | `cregit-issue61` | enable `-C100` at `formatBlame.pl:73` |
| `931d616` | `cregit-issue61` | add `--reblame` to the runner, plus 15 tests |
| `4e7d341` | `cregit-token-pipeline` | forward `--reblame`, and refuse it where it cannot act |
| `1f78a54` | `cregit-token-pipeline` | add `reblame_wave.sh`, the dispatcher |

`-C100` is live on branch `deploy/reblame-c100`. `322bfcf` stays reachable, so a
checkout back to it reverts the deployment.

## 4. The trap: a re-blame that changes nothing and reports success

**This is the most important lesson in this file.**

`blameRepoFiles.pl:99` is:

```perl
if (!$overwrite && -f $outputFile) { $alreadyDone++; next; }
```

Step 7 never passed `--overwrite`. That skip is what makes an interrupted run cheap
to resume. It also makes a deliberate re-blame a silent no-op: change a `git blame`
flag, resume at step 7, and every file is reported as already done.

The first pilot run changed **0 of 15,036,195 tokens**. It had every marker of
success — identical schema, identical row counts, `rc=0`, all three projects
validated. It was caught only by disbelieving a zero, then reading the POD and the
blame directory's mtime, which was still six days old.

Two further facts about this trap:

* **A unit test for `--overwrite` already existed and passed.** The defect was in the
  wiring: nothing asserted that the runner *passes* the flag. When a leaf option looks
  tested, check that something drives it end to end.
  `cregit-issue61/test_reblame_passthrough.sh` now does, in 15 cases.
* **The POD documented the flag under the wrong name** — `--override`, not
  `--overwrite`. Corrected in `931d616`.

`--reblame` refuses above `FROM_STEP=7`, in both the runner and `ctp.py`. Above step 7
the flag would be silently skipped, step 10 would rebuild the Parquet from the old
blame, and the run would exit 0 — the same hole, one step later.

**How to tell a real re-blame from a no-op.** Read step 7's own summary line:

```
Newly processed [10] Already done [0] files Error [0]
```

`Already done [0]` is the proof. A run without `--reblame` prints the file count in
the second bracket instead.

## 5. The snapshot, which is also a result

Step 10 rewrites `<slug>-dataset.parquet` in place, so a re-blame destroys the corpus
that the current numbers came from.

All **187** Parquets were copied to `parquet-backups/pre-reblame/` first: **6.1 GiB**,
sha256-verified after the rename, with `MANIFEST.sha256`.

Keep it. It is not only a rollback path — the difference between the two attributions
is the measurement in §7, and it cannot be recomputed once the old files are gone.

The `.validated` stamps are also backed up, because `ctp.py` skips a project that has
one and the dispatcher must remove them. Restore from
`parquet-backups/stamps-reblame-<timestamp>/`.

## 6. Cost, and why project size does not predict it

**Measured, not estimated.** Two earlier estimates in this project were wrong, both
because they measured the wrong thing.

| estimate | basis | verdict |
| --- | --- | --- |
| 1.14x | `git blame -C100` run serially over 200 warm Linux files | **wrong** — does not measure the pipeline |
| 8.1x | three small projects, old step 7 against new | order of magnitude only; spread was 4.8x-13.5x |
| 7.9 s per MiB of Parquet | median of 15 projects in the wave | **wrong** — see below |

The size-based rate failed badly. Observed cost per MiB of the project's dataset
Parquet, in the wave of 2026-09-22:

| project | dataset | step 7 + 8 + 10 | s / MiB |
| --- | ---: | ---: | ---: |
| `util-linux__util-linux` | 12.5 MiB | 65 s | 5.2 |
| `dolphin-emu__dolphin` | 41.6 MiB | 329 s | 7.9 |
| `dpdk__dpdk` | 111.0 MiB | 1,438 s | 13.0 |
| `sumatrapdfreader__sumatrapdf` | 73.2 MiB | 3,488 s | 47.6 |
| `moarvm__moarvm` | 18.3 MiB | >4,177 s | **>228** |

That is a **44x** spread, and the worst case is one of the smaller projects.
`moarvm__moarvm` spent about 10 seconds per file on only 407 files.

**Cost tracks the individual file, not the project.** Do not size a wave from dataset
size, row count, or size class. This is the same trap recorded for step 2 memory, where
the largest consumer was a class-M project.

Blame throughput ranged from **0.1 to 14 files per second** per project.

### One file can hold a slot for hours

`moarvm__moarvm` looked hung: its log went silent for 72 minutes after it dispatched
the last of its 407 files, and no `.blame` file had been written for 10 minutes. It was
not hung. A single `git blame -C100` was running at 99.8% CPU with **72 minutes of CPU
time in 72 minutes of wall time** — real work on one file:

```
src/strings/unicode_db.c   23 MB   1,666,007 lines   104 revisions
```

A generated Unicode property table. `-C100` searches other files for the origin of
every line at every commit boundary, so its cost grows with lines × revisions. Plain
`git blame` handled the same file in the earlier run without trouble.

`kicad__kicad-source-mirror` showed the same shape at the same moment:
`common/gal/opengl/bitmap_font_img.c`, a generated bitmap-font array, at 10 minutes of
CPU on one file and still running.

**How to tell this from a hang.** A silent log proves nothing. Read the CPU time of the
`git blame` child:

```bash
ps -eo pid,etime,time,pcpu,args | grep 'blame -C100'
```

If `TIME` tracks `ELAPSED` at near 100% CPU, it is working. If `TIME` is flat, it is
stuck. The same reasoning as the step-2 rule that a frozen log is not a dead run.

**Consequence for planning.** A project's cost has a long tail set by its single worst
file, so no per-project estimate is safe. `torvalds__linux` has 64,508 files and
certainly contains files of this class. Expect the wave's finish time to be decided by
a handful of generated files, not by the project count.

**Open question, not yet decided.** Whether to cap `-C100` per file — a timeout that
falls back to plain blame, or a denylist of generated paths. A cap would trade a small
attribution error on a few generated files for a predictable run time. Nothing
implements this today.

## 7. Effect on the data

See the token-level table in [`LIMITATIONS.md`](LIMITATIONS.md). In summary, joining
old against new on `(file_path, token_index)`:

* **0% to 25% of a project's tokens change author.** Row counts and schema are
  identical in every case measured, so the difference is attribution alone.
* **A project's single largest author can change.** It did for `dpdk__dpdk` and for
  `buchen__portfolio`.
* **Firm attribution moves less than author attribution**, 0% to 15%, because many
  moves stay inside one organisation.

**The correction introduces one new defect.** `dpdk__dpdk`'s top-1 author became
`intel at intel.com`, a corporate address rather than a person. An identity-hygiene
pass is needed before any per-author result is published from the corrected data.

## 8. How to run it

```bash
JOBS=3 BLAME_JOBS=4 MEMORY_LIMIT=2GB bash ./reblame_wave.sh <slugs-file>
```

`reblame_wave.sh` runs `ctp.py run --from-step 7 --reblame --skip-html`. It is under
version control on purpose; the earlier scratch dispatcher was not.

It refuses, before it changes any state, when:

1. the python on `PATH` lacks `duckdb` — step 10 would be skipped silently;
2. the checkout named by `pipeline.cfg` does not pass `-C100`;
3. that checkout's runner has no `--reblame`;
4. a member cannot resume at step 7, or has no Parquet snapshot;
5. a member holds a tokenized `.rs` blob, so it needs step 2 as well.

**Steps 7-10 only, and that matters.** The crash gate lives inside step 2, and
`run_step` returns early below `FROM_STEP`. So a re-blame needs neither the tokenizer
fix nor the crash denylist, and it skips step 1's `rm -rf "$WORK"`. Step 9 is
skippable because nothing downstream reads `$WORK/html`.

**Order the slugs file largest-first.** `ctp.py` dispatches alphabetically, so the
wave of 2026-09-22 put `torvalds__linux` at line 128 of 136 and the longest job started
last. Do not fix this by launching the giant in a second concurrent run: `ctp.py` holds
a per-project lock and the wave would mark the project failed.

## 9. Status

| date | event |
| --- | --- |
| 2026-09-22 | 3-project pilot; `-C100` deployed; `--reblame` added after the no-op was found |
| 2026-09-22 17:19 | wave launched over 136 blame-only projects, `--jobs 3 --blame-jobs 4` |
| — | **47 Rust members still pending.** They need step 2 as well, which waits on the `fix/tokenizer-correctness` review |

Until the 47 are done, the corpus carries two attributions. Do not mix them in one
analysis. Detect which one a project has by comparing it against
`parquet-backups/pre-reblame/`: if the two files are identical, it still carries plain
blame.
