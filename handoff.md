# Handoff: re-blame `googleapis__google-cloud-java` on a second machine

**For:** kirocrew, on a machine other than `dev-dsk-ellianco-1e-5a4f0103`.
**Task:** run pipeline **step 7 only** — `git blame -C100` per file — for
**`googleapis__google-cloud-java`**, then return the `blame/` directory.
**Date written:** 2026-09-22. **Author:** Claude Code session on the primary host.

**Honest scope.** This is one project, and it does not unblock anything. The primary host
will reach it on its own within a day. What you buy is a shorter tail on a 24-36 hour run,
and insurance against a file that blames pathologically slowly. Do not treat it as
critical-path work. If it is inconvenient, say so and nothing is lost.

Read §1 to §4 before you start. §5 is a rehearsal you must pass first.

---

## 1. Why this work exists

The cregit token pipeline blamed every file with a plain `git blame`. Upstream cregit had
`-C100` copy detection commented out. Plain `git blame` follows a whole-file rename. It
does not follow content moved *between* files. So a header split or a refactor credited
every token it touched to whoever moved it, not to the author.

`-C100` is now enabled. Every project must be re-blamed. Measured effect: **0% to 25% of
a project's tokens change author**, and a project's largest author can change.

Full background: `docs/REBLAME-C100.md` in the `cregit-token-pipeline` repository.

## 2. Scope: step 7 only

The pipeline has ten steps. You run **only step 7**. This matters, because step 7 is the
only step that is portable.

| step | what it needs | you run it? |
| ---: | --- | --- |
| 1-6 | srcML 1.1.0 via nix, scala/java jars | **no** — already done, output is on disk |
| **7** | **git and core perl** | **yes** |
| 8 | java, a jar built with sbt | no — runs on the primary host |
| 9 | skipped entirely (`--skip-html`) | no |
| 10 | python3 with duckdb, provenance sidecars | no — runs on the primary host |

So you need **git and perl only**. No nix, no java, no python, no duckdb, no CPAN module.

**Do not run `run_pipeline_process.sh`.** It needs the whole toolchain, and a full run
starts by deleting the work directory. Call `blameRepoFiles.pl` directly, as §6 shows.

## 3. Hard rules

1. **Do not touch these three projects.** They are being blamed on the primary host right
   now: `torvalds__linux`, `kicad__kicad-source-mirror`, `moarvm__moarvm`.
2. **Do not delete or overwrite anything outside your own working copy.** You receive a
   copy of each repository. Your only output is a new `blame/` directory.
3. **Do not run `git gc`, `git repack` or `git prune`** in the repositories you receive.
   Leave the object store as delivered.
4. **Do not push anything anywhere.**
5. **Report a file you cannot blame. Do not skip it silently.** A missing `.blame` file
   becomes a silent gap in published data.

## 4. Prerequisites on your machine

| requirement | primary host has | note |
| --- | --- | --- |
| `git` | **2.55.0** | 2.30 or later is enough. `-C100` behaviour is stable across these. |
| `perl` | **5.32.1** | core modules only: `Errno`, `File::Basename`, `File::Copy`, `File::Path`, `File::Temp`, `FindBin`, `Getopt::Long`, `Pod::Usage`, `POSIX`. Nothing from CPAN. |
| CPU | 16 cores | more cores is the whole point; blame is one process per file |
| RAM | 30 GiB | step 7 is light. Peak seen is under 1 GiB per blame process. |
| disk | — | budget **3x** the pack size per project, for the clone plus the blame output |

Check both versions before you start:

```bash
git --version
perl -e 'use Errno; use File::Path; use File::Temp; use Getopt::Long; use Pod::Usage; print "perl deps ok\n"'
```

## 5. Phase 0 — the rehearsal. Do this first

Do not start on a large project. Prove the procedure on a small one whose output nobody
depends on.

Use **`scrcpy`**. It is from an earlier MVP set, it is not in the published corpus, and
it has no published Parquet. If you damage its output, nothing is lost.

| project | pack | files in clone |
| --- | ---: | ---: |
| `scrcpy` | **7.16 MiB** | 371 |

Steps:

1. Receive `scrcpy-cregit` and the two perl scripts (§6.1).
2. Run the blame command (§6.2) with `scrcpy-cregit` as the repository.
3. Confirm every check in §7 passes.
4. Send the result back and wait for confirmation before you start Phase 1.

The four other MVP projects are available if you want a second rehearsal: `rustlings`
(6.56 MiB, 293 files), `dubbo` (41.13 MiB, 4,847 files), `redis` (80.17 MiB, 1,860
files), `terminal` (119.75 MiB, 3,680 files).

## 6. Phase 1 — the real target

### 6.1 What you receive

Two things:

1. **The non-bare cregit clone**, named `<slug>-cregit`. This is a tokenized checkout: the
   working tree holds token streams, not original source. Step 7 blames those.
2. **Two perl scripts** from the `cregit-issue61` checkout:
   - `blameRepo/blameRepoFiles.pl` — the driver, with the `--overwrite` flag
   - `blameRepo/formatBlame.pl` — the formatter, which holds `-C100` at line 73

Confirm the copy detection is really present before you spend hours:

```bash
grep -n 'blame -C100' formatBlame.pl
# must print:  73:open(IN, "git -C '$repo' blame -C100 --line-porcelain '$file'|" ) ...
```

**If that line says `git blame` without `-C100`, stop.** The whole job would cost the same
hours and change nothing.

### 6.2 The command

```bash
perl blameRepoFiles.pl \
  --jobs=<CORES> \
  --formatBlame=$(pwd)/formatBlame.pl \
  --overwrite \
  <slug>-cregit \
  <slug>-blame-out \
  '(?i)\.(c|c\+\+|cc|cp|cpp|cxx|h|h\+\+|hh|hpp|hxx|java|rs|tcc)$'
```

Four things about this command:

* **`--overwrite` is mandatory.** Without it the script skips every file whose `.blame`
  output already exists, reports them all as already done, and exits 0. See §8.1.
* **Set `--jobs` to your core count.** One `git blame` is single-threaded, so throughput is
  linear in jobs until you run out of cores or disk bandwidth.
* **Quote the mask.** The shell will otherwise eat the backslashes and the `$`.
* **The mask is identical for every project in this handoff.** It is the universal mask.
  Do not narrow it.

Write the output to a **new** directory, `<slug>-blame-out`. Do not write into a directory
you received.

### 6.3 The target: one project

**Take `googleapis__google-cloud-java`. That is the job.**

| | |
| --- | --- |
| clone to transfer | `googleapis__google-cloud-java-cregit`, **1.21 GiB** packed |
| files in the clone | **139,624** — second only to Linux in the whole corpus |
| dataset it produces | **991 MB** of Parquet |
| oversized generated files | 7, but only 0.8% of its tokens, so no pathological tail expected |

One project, because the arithmetic says so. At the time of writing, 50 projects remain
on the primary host, worth 3,047 MB of dataset. `torvalds__linux` is 1,501 MB of that and
is already running there on separate cores. Of the 1,546 MB left,
**`googleapis__google-cloud-java` alone is 991 MB — 64%.**

The next largest pending projects are `google__nearby` (76 MB), `grpc__grpc` (48 MB),
`kde__krita` (47 MB) and `gnome__gtk` (46 MB). Together they are less than a fifth of the
one project above. Handing them over would cost more coordination than it saves, so do not
take them unless the primary host asks.

**Ask before you take anything not named here.** The primary host dispatches in
`manifest.sample.tsv` order, monotonically (`ctp.py:1021` maps over the manifest). It had
reached position 85 of 135 when this was written; `googleapis__google-cloud-java` sits at
position 101. So you have a real head start, but it is a head start, not a reservation —
confirm before you begin.

## 7. Verification. Every check must pass

Run all five before you send anything back.

> **This procedure was tested before it was sent to you.** On the primary host, on 2026-09-22,
> the §6.2 command ran against `scrcpy` with `--jobs=2` and finished in **18 seconds**:
>
> ```
> Newly processed [278] Already done [0] files Error [0]
> ```
>
> All five checks below passed: 278 mask-selected files against 278 `.blame` files written,
> zero empty files, correct format, and 13 distinct shas in the sample file.
>
> The trap in §8.1 was also reproduced deliberately. The same command **without**
> `--overwrite` printed `Newly processed [0] Already done [278] files Error [0]` and
> **exited 0**. That is what a silent no-op looks like. Check 1 below is what catches it.

**1. The driver reported real work, not a skip.** Its last line must read:

```
Newly processed [N] Already done [0] files Error [0]
```

`Already done [0]` is the proof that `--overwrite` took effect. **Any non-zero value in
the second bracket means the run did nothing.** Any non-zero `Error` count must be
reported, with the file names.

**2. The output file count matches the mask-selected file count.**

```bash
git -C <slug>-cregit ls-files \
  | grep -iE '\.(c|c\+\+|cc|cp|cpp|cxx|h|h\+\+|hh|hpp|hxx|java|rs|tcc)$' | wc -l
find <slug>-blame-out -name '*.blame' | wc -l
```

The two numbers must be equal. If they differ, say which files are missing.

**3. No `.blame` file is empty.**

```bash
find <slug>-blame-out -name '*.blame' -empty
```

This must print nothing. An empty file is a silent gap.

**4. The format is right.** Each line is `<sha>;;<TAB><token>`. Check one file:

```bash
head -3 <slug>-blame-out/<some file>.blame
```

Expected shape:

```
4440a0718386edaf1c00a9c66ac876d96680668c;;	begin_unit|revision:1.0.0;language:C;cregit-version:0.0.1
4440a0718386edaf1c00a9c66ac876d96680668c;;	begin_define
4440a0718386edaf1c00a9c66ac876d96680668c;;	DECL|macro|_GNU_SOURCE
```

**5. More than one distinct sha appears.** Copy detection should spread attribution. A
file with exactly one sha throughout is possible but unusual:

```bash
cut -d';' -f1 <slug>-blame-out/<some file>.blame | sort -u | wc -l
```

## 8. Troubleshooting

### 8.1 The silent no-op. Read this even if nothing goes wrong

`blameRepoFiles.pl:99` is:

```perl
if (!$overwrite && -f $outputFile) { $alreadyDone++; next; }
```

Without `--overwrite`, the script skips every file that already has output. It then exits
**0**. On the primary host this produced a run that changed **0 of 15,036,195 tokens**,
with identical row counts and identical schema — indistinguishable from success.

This is why check §7.1 exists. Do not skip it.

### 8.2 A file that runs for hours is normal. Prove it is working

`-C100` searches other files for the origin of every line at every commit boundary. Its
cost grows with lines × revisions. A generated table is the worst case.

Measured on the primary host: `moarvm__moarvm/src/strings/unicode_db.c` — 23 MB,
1,666,007 lines, 104 revisions — consumed **over 3.5 hours of CPU on one file** and
produced **zero output** while doing so. `git blame` emits nothing until it finishes.

**A silent log proves nothing. Check CPU time instead:**

```bash
ps -eo pid,etime,time,pcpu,args | grep 'blame -C100'
```

* `TIME` tracking `ELAPSED` at near 100% CPU → it is working. Wait.
* `TIME` flat while `ELAPSED` grows → it is stuck. Report it.

Do not use `pgrep -f` or `pkill -f` for this. Those patterns match your own command line.

Expect at most a mild version of this in `googleapis__google-cloud-java`: it has 7 oversized
generated files, but they are only 0.8% of its tokens. **Do not kill these processes.** The
`-C100` runs on every file with no cap, so that the method is one reproducible sentence.

### 8.3 Throughput you should expect

Measured on the primary host: **0.1 to 14 files per second** per project, with `--jobs 4`.
The spread is 44x and it is driven by individual files, not by project size. Do not
conclude a job is broken because it is slower than another.

### 8.4 Disk fills

Blame output is larger than the source: about 25 to 60 bytes per token. Budget 3x the pack
size. If disk runs short, stop cleanly and report. Do not delete a repository to make room.

## 9. What to send back

Per project, one archive:

```bash
tar -czf <slug>-blame.tar.gz -C <slug>-blame-out .
sha256sum <slug>-blame.tar.gz > <slug>-blame.tar.gz.sha256
```

Send with it:

1. The driver's final summary line (§7.1).
2. The two file counts from §7.2.
3. The wall time, and the `--jobs` value you used.
4. Any file that errored, timed out, or that you skipped, with the reason.
5. Your `git --version` and `perl` version.

## 10. What happens after you return it

On the primary host, per project:

1. The archive is verified against its sha256 and unpacked over the project's `blame/`.
2. **Step 8** rebuilds the commit map with java.
3. **Step 10** rebuilds `<slug>-dataset.parquet` with duckdb, joining the blame against the
   persons and firm sidecars. 70 columns.
4. The new Parquet is compared against the pre-correction snapshot in
   `parquet-backups/pre-reblame/`, joined on `(file_path, token_index)`. Row counts and
   schema must be identical; only attribution may differ. That difference is a published
   result, not only a check.
5. The project is re-validated and gets its `.validated` stamp back.

Every original Parquet is already snapshotted, sha256-verified, so your work cannot lose
the current numbers.

## 11. Coordination, and why a race is safe

The primary host is running the same corpus. Duplicated work is possible. It cannot
corrupt anything, because of two guards in `ctp.py`:

* A project with a `.validated` stamp returns **`skipped`** (`ctp.py:473-475`). The check
  happens when that project is dispatched, not up front.
* A project whose lock is held returns **`deferred`**, not `failed` (`ctp.py:484-487`).

So whichever finishes first wins, and the other is discarded. The §6.3 ordering exists to
make that unlikely, not to make it safe — it is already safe.

**Tell the primary host when you start each project**, so it can skip it if the wave gets
close. That is the only coordination needed.

## 12. Context you may want

| file, in `cregit-token-pipeline` | what it holds |
| --- | --- |
| `docs/REBLAME-C100.md` | the full method, cost and effect of this correction |
| `docs/LIMITATIONS.md` | what is wrong with the dataset; read the copy-detection entry |
| `docs/DATASET-SCHEMA.md` | all 70 columns |
| `reblame_wave.sh` | the dispatcher the primary host uses, with its five refusals |

Current state of the primary run, at the time of writing: **77 of 136 projects re-blamed,
zero failures**, with `torvalds__linux` started separately on the idle cores.
