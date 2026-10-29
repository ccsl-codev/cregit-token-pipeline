# Known limitations

What is wrong with the dataset, ordered by how likely it is to change a result.
Read this before analyzing the data. [`DATASET-SCHEMA.md`](DATASET-SCHEMA.md) is
the column reference; [`CODEBOOK.md`](CODEBOOK.md) §9 holds the gaps specific to
the stratum labels.

Every entry states the defect, its measured magnitude where one exists, how to
detect it in the data, and the workaround if there is one. An entry with no
magnitude says so rather than guessing.

---

**Pilot run only (stock srcML): one drawn project is excluded: `tencent__tencentkona-21`.** It was
selected by the sampling procedure and it is not published. Two independent reasons,
both measured:

* **53 historical revisions of `src/hotspot/share/runtime/continuationFreezeThaw.cpp`
  do not terminate under the parser.** Re-confirmed at a 120-second budget: zero
  bytes of output, resident memory flat at 9.7 MB, one core saturated. Minimal
  reproducer, 41 bytes, from HotSpot's unexpanded macro idiom — an unexpanded
  function-like macro standing where a declaration is expected, inside a template.
  This is ordinary production source, not a stress test.
* **7 further blobs crash the parser outright.** A crash is not a timeout, so the
  exclusion list never sees it, and the file is written as a zero-byte tokenization
  that no counter records. So even with 53 exclusions the project would publish with
  seven production files silently contributing nothing.

The project is excluded rather than published with gaps it cannot report. The row stays
in the manifest with its reason so the sampling record remains complete.

This exclusion is only for the pilot run, which used stock srcML. Dataset v1 used srcML
with the patch of the authors against non-termination, and the release has a file for
`Tencent__TencentKona-21` (5,000+ layer, cregit `57551d7`; it is in the plain-blame list
below).

**A second project is excluded as a near-duplicate: `tencent__tendbcluster-tendb`.**
The sampling frame drew both halves of one fork pair. The two repos share **505,056
source blobs** — 99.66% of tendb's own and 99.54% of tdbctl's — and **134,715 commits**,
99.91% of tendb's. Publishing both would count the same authorship twice.

`tencent__tendbcluster-tdbctl` is kept, and the file count argues the other way. tendb
has 31,139 files at HEAD against tdbctl's 7,212, but **every one of its 24,611 unique
paths is third-party code**: 11,321 files of Oracle's `mysql-test/suite`, 8,326 of
Oracle's `storage/ndb`, 1,616 of Facebook's RocksDB. Exclude the vendored directories
and the test suite, and tendb has **zero** unique first-party files while tdbctl has
**30** — the `sql/tc_*.cc` cluster-routing, DDL, monitoring and XA-repair sources its
README advertises. On tokens, with vendored directories excluded, tdbctl carries
**202,331** Tencent-authored tokens (2.20%) against tendb's **41,571** (0.43%): 4.9x
more first-party authorship in a tree 40% smaller. tdbctl also still receives commits
(HEAD 2026-05) and already contains 134,715 of tendb's 134,827 commits.

The exclusion is declared in `consolidate.py`'s `PUBLICATION_EXCLUSIONS`, so the row
stays in `projects` with its reason in `excluded_because` and only the tokens are
withheld. Audit it with
`select name, excluded_because from projects where excluded_because is not null`.

With both exclusions the corpus is **186 published of 186 in scope**: 185 S- and M-class
projects plus `torvalds__linux`, which published 2026-09-21 with 199,678,137 rows. Of the
188 run-set rows, `tencent__tencentkona-21` has no Parquet and `tencent__tendbcluster-tendb`
has one that is withheld.

**Dataset v1 (census 2026-10): `kitao__pyxel` is pending, because GitHub suspended its
owner's account.** The project is in the v1 frame. Its frame commit is `50f9bd77`, from
the frame snapshot of about 2026-10-05. The census failed to resolve it 4 times; the last
attempt was 2026-10-12 08:07 UTC (`git ls-remote` exit 128). On 2026-10-13 GitHub
answered "Repository not found" for the repository and listed no public repositories for
the owner.

* **Cause (reported, not verified by us):** GitHub suspended the owner's account for a
  time, after an attacker used the account in a supply chain attack. The attacker pushed
  a malicious GitHub Actions workflow (`security-audit.yml`) to 27 of the owner's
  repositories.
* **No other v1 project has this owner.**
* **Status:** pending (user decision 2026-10-13). If the account returns, the project
  runs at the frame commit `50f9bd77`, not at the newest HEAD. So no commit after the
  snapshot, and none of the attacker's commits after it, enters the dataset. If the
  account does not return before the v1 freeze, or the commit is gone, v1 excludes the
  project and keeps the row with this reason.
* **Effect on the data:** the census reads the history and does not run workflows. A
  workflow file is also outside the language mask (C, C++, Java, Rust). Until a rerun
  finishes, the project has no Parquet: count it in the frame, not in the token tables.

**Dataset v1: three more frame rows have no Parquet.** The v1 frame has 3,548 projects
(2026-10-14). The rows below stay in the frame with their reason. Count them in the
frame, not in the token tables. In the "Dataset v1" entries, the `s2-pool/…` and
`s7-run/…` paths are in the dataset selection records
(`2026.2-estudos-pesquisa-sistemas/dataset-selection/` of the ellians-master
repository). The dataset paper repository copies part of them under `pipeline/`.

| project | frame commit | reason | status (2026-10-14) |
| --- | --- | --- | --- |
| `googleapis__discovery-artifact-manager` | `9b6a724` | No in-scope source at the run commit. The run uses the newest head (`--latest`). Head `bb221e1` (2026-10-10) has 1,284 files and 0 C, C++, Java or Rust files, because the project deleted its Java code on 2026-10-07. The frame commit has 559 such files. Blame finds 0 files, and the pipeline stops with "no tokens processed" (4 attempts). | excluded (user decision 2026-10-11) |
| `signalwire__freeswitch` | `3dee68e` | blobExec stops in step 2 with a `NullPointerException` at `Walker.scala:866` (`CommitBuilder.getCommitter()` is null): it reads a commit with no committer. All 4 attempts gave the same error. A scan of the 40,145 commits found no committer line that breaks git's format, so the cause in blobExec is not known. | excluded (user decision 2026-10-11); tool defect, not fixed |
| `BRL-CAD__brlcad` | `27ba3c4` | The repository is gone. `git ls-remote` failed first on 2026-10-11 05:16 UTC, and it gives "Repository not found" on 2026-10-14 17:49 UTC. The user wrote to the owners (2026-10-12). | waits for the owners' answer; without the repository before the v1 freeze, v1 excludes the project |

`discovery-artifact-manager` shows a property of the `--latest` rule. The selection
facts (commit count, languages, license file) come from the frame commit, but the tokens
come from the newest head at the first run attempt. A project can thus pass selection
and have no tokens. `kitao__pyxel` (entry above) is the fourth frame row with no Parquet.

**Dataset v1: the 51 GitLab rows (batches 25 and 26) have a later snapshot and other
selection rules than the GitHub rows.** Find these rows by name in
`s7-run/manifest.batch25.tsv` and `s7-run/manifest.batch26.tsv` of the selection
records. Do not use the host prefix of the name: some earlier rows are on GitLab hosts
too (for example `gitlab.eclipse.org__…`).

* **Snapshot.** The GitHub rows use the frame snapshot of 2026-10-05. For the GitLab
  rows, the selection read the head of the default branch on 2026-10-14: 01:23-01:27 UTC
  for the 50 rows of batch 25, and 15:48 UTC for `muttmua/mutt`, batch 26
  (`s2-pool/gitlab/measure.jsonl`). The manifests were built on 2026-10-14 at 01:37 UTC
  (batch 25) and 16:37 UTC (batch 26). Thus the selection facts of a GitLab row are 9
  days younger than those of a GitHub row. Both kinds run at the newest head of their
  first run attempt (`--latest`).
* **Fork rule.** On GitHub, only the fork flag removes a row, and it finds nothing (see
  "GitHub's fork flag finds nothing" below). So GitHub rows that carry the history of
  another project are kept and flagged. On GitLab, the copy rules of
  `s2-pool/gitlab/decide.py` remove more rows: copy namespaces (rule R1, 43 rows), copies
  of a pool project (R2, 2 rows) and forks that share history with a pool project (R3,
  73 rows, 21 of them Linux trees). The 73 R3 rows stay out of v1 (user decision
  2026-10-14). An upstream whose root commit only its forks share stays in (R4, 6 rows,
  for example `mesa/mesa`). Thus, by rule, the GitLab part has no fork of a pool
  project, but the GitHub part can have one.
* **License rule.** The rule is now the same. The DP8L v4 near-miss classes C, D and E
  (2026-10-11) passed GitHub rows only. Since 2026-10-14 about 16:40 UTC they also pass
  GitLab rows (`s2-pool/gitlab/near_miss_gitlab.py`). `muttmua/mutt` (class C: root
  `COPYRIGHT` and `GPL`) entered v1 as batch 26 with license `NOASSERTION`.
  `mesa/demos` (class H: no license evidence) stays out.
* **Namespaces and stars.** The listing of the 11 GitLab hosts and of the 22 gitlab.com
  roster groups takes group namespaces only. It removed 20,812 projects in personal
  namespaces, and it has no stars rule, as the rosters. The gitlab.com-wide search uses
  stars > 400, the GitHub search rule, and it takes group and personal namespaces (the
  personal ones since 2026-10-14 about 15:35 UTC), as the GitHub search has no owner
  rule. That search found 75 projects. The personal namespaces add 1 row, `muttmua/mutt`.
* **Labels.** The stratum labels of the GitLab rows have no review by hand yet (batch 25:
  foundation 4, fiscal-sponsor 5, unknown 41; `s7-run/labels.batch25.csv`).

**Dataset v1: some files have blame without copy detection.** v1 uses
`git blame -C100` (see the re-blame entry below). For the files of this entry, a plain
`git blame` ran. Plain blame follows a whole-file rename, but it does not follow content
that moved between files. So the tokens of these files can go to the commit that moved
them. The effect on these files is not measured. Three mechanisms caused it:

1. **Time limit** (cregit `57551d7`, `9ccead6` and `23e4ccb`, and the running copies
   that got their `formatBlame.pl`; see the next entry). After 3600 s of `-C100`, the
   script stops git and runs plain blame. The pipeline log gets the line
   `PLAIN-BLAME <file> after 3600 s of -C100` (sometimes `3601 s`).
2. **git exit 128** (cregit `23e4ccb`, `gitlab.inria.fr__cedar__connectionlens` only).
   `git blame -C100` stops with "unable to generate diff" when a commit moves a blob
   over 1 GiB (the xdiff limit). The script then runs plain blame and logs
   `PLAIN-BLAME <file> after git exit 128 of -C100`.
3. **Manual swap** (2026-10-11, before the time limit existed). For a `-C100` blame that
   ran for hours, the operator made a plain blame apart, stopped the slow git, and put
   the plain `.blame` file in its place. **No log line marks these files.** The table
   at the end of this entry lists them.

The time limit is wall time. Whether a file falls back depends on the machine: its load,
the number of parallel jobs and the restarts of the run. The only record is the
`PLAIN-BLAME` line of the pipeline log, and the release does not hold the logs. No option
of the tools makes plain blame for a given list of files. Thus a rebuild on another
machine can give `-C100` blame where the release has plain blame, or the reverse (tool
review of 2026-10-28). The counts of this entry and of the entry of the 1,000-4,999 commit
layer (201 + 367 files) are lower bounds.

Count on 2026-10-14 17:45 UTC, from the pipeline logs on the analysis machine. It has
3,532 finished projects, and 3,517 of them have their logs there:

| mechanism | projects | files |
| --- | ---: | ---: |
| time limit (`PLAIN-BLAME` line) | 81 | 186 |
| git exit 128 (`PLAIN-BLAME` line) | 1 | 2 |
| manual swap (no log line) | 11 | 13 |
| **total** | **93** | **201** |

This count does not include:

* projects with no finished output on that date. For example, `alibaba__AliSQL` runs
  again (status of 2026-10-14 13:24 UTC), so the manual swap of its 3 files is in no
  finished Parquet;
* 15 finished projects whose logs are not on the analysis machine yet: the L7-20b run
  (`aftermathlabs__llvm-msvc`, `alpinelinux__aports`, `BrowserWorks__waterfox`,
  `flang-compiler__classic-flang-llvm-project`, `intel__llvm`,
  `MotorolaMobilityLLC__kernel-msm`, `mozilla__enterprise-firefox`), the L23 run
  (`archlinuxcn__repo`, `checkedc__checkedc-clang`, `openbsd__ports`, `openbsd__src`,
  `openucx__ODOS`, `WebKit__WebKit`, `Xilinx__hls-llvm-project`) and the L1r run
  (`torvalds__linux`).

Find the logged files with `grep -h '^PLAIN-BLAME' state/*/logs/pipeline-*.log` in the
ctp state of each run part. Projects with 2 or more logged files:

| project | files |
| --- | ---: |
| `arangodb__arangodb` | 23 |
| `EiffelSoftware__EiffelStudio` | 10 |
| `mamedev__mame` | 8 |
| `openLuat__LuatOS` | 7 |
| `wireshark__wireshark` | 7 |
| `z88dk__z88dk` | 7 |
| `BlitterStudio__amiberry` | 6 |
| `googleapis__google-api-java-client-services` | 6 |
| `IfcOpenShell__IfcOpenShell` | 6 |
| `vpinball__vpinball` | 6 |
| `mongodb__mongo` | 5 |
| `oceanbase__oceanbase` | 5 |
| `chapel-lang__chapel` | 4 |
| `KiCad__kicad-source-mirror` | 4 |
| `openjdk__jfx` | 4 |
| `illumos__illumos-gate` | 3 |
| `gitlab.inria.fr__cedar__connectionlens` (git exit 128) | 2 |
| `joncampbell123__dosbox-x` | 2 |
| `Kitware__VTK` | 2 |
| `miranda-ng__miranda-ng` | 2 |
| `picolibc__picolibc` | 2 |
| `praat__praat.github.io` | 2 |
| `qt__qtbase` | 2 |
| `SakuraEngine__SakuraEngine` | 2 |
| `TelegramMessenger__Telegram-iOS` | 2 |
| `Weather-OS__WineGDK` | 2 |
| `yandex__perforator` | 2 |

Projects with 1 logged file (55): `adoptium__jdk`, `adoptium__jdk21u`,
`Azure__azure-sdk-for-java`, `BeamMW__beam`, `catboost__catboost`, `citizenfx__fivem`,
`cocos2d__cocos2d-x`, `corretto__corretto-21`, `corretto__corretto-26`,
`corretto__corretto-27`, `DrTimothyAldenDavis__GraphBLAS`, `FarGroup__FarManager`,
`ferrocene__ferrocene`, `gitlab.eclipse.org__eclipse__titan__titan.EclipsePlug-ins`,
`jeandle__jeandle-jdk`, `JetBrains__JetBrainsRuntime`, `libretro__parallel-n64`,
`llnl__rose`, `logicalclocks__rondb`, `maplibre__maplibre-native`,
`microsoft__openjdk-jdk25u`, `mlflow__mlflow`, `mudita__MuditaOS`,
`mysql__mysql-server`, `netxms__netxms`, `NVIDIA__cuda-gdb`, `omnetpp__omnetpp`,
`openjdk__jdk`, `OpenRCT2__OpenRCT2`, `pawelsalawa__letos`, `percona__percona-server`,
`percona__percona-xtrabackup`, `pi-hole__FTL`, `polardb__polardbx-engine`,
`prusa3d__PrusaSlicer`, `Pzqqt__android_kernel_xiaomi_marble`, `qt-creator__qt-creator`,
`rbfx__rbfx`, `RosettaCommons__rosetta`, `rust-lang__rust`,
`schrodinger__pymol-open-source`, `sleuthkit__sleuthkit`, `SoarGroup__Soar`,
`sourceware.org__git__binutils-gdb`, `stellar__stellar-core`, `Tencent__TencentKona-21`,
`Tencent__wcdb`, `tnodir__fort`, `tutao__tutanota`, `ugeneunipro__ugene`,
`UPBGE__upbge`, `wolfpld__tracy`, `wolfSSL__wolfssl`, `ytsaurus__ytsaurus`,
`zeek__zeek`.

The files of the manual swap (records: `s7-run/plain-swap-spot-a-20261011T1852Z.txt`,
`s7-run/plain-swap-spot-b-20261011T2003Z.txt`, and the runs log update of 2026-10-11
04:30 UTC for OpenVSP):

| project | file | `-C100` time before the swap |
| --- | --- | ---: |
| `OpenVSP__OpenVSP` | `src/geom_core/humanshape/FemaleGeomData.h` | 38.5 h in step 7 (both files) |
| `OpenVSP__OpenVSP` | `src/geom_core/humanshape/MaleGeomData.h` | 38.5 h in step 7 (both files) |
| `avrdudes__avrdude` | `src/urbootlist.c` | 681 min |
| `hortonworks__cloudbreak` | `usage-collection/src/generated/main/java/com/cloudera/thunderhead/service/common/usage/UsageProto.java` | 650 min |
| `apache__hive` | `standalone-metastore/metastore-common/src/gen/thrift/gen-javabean/org/apache/hadoop/hive/metastore/api/ThriftHiveMetastore.java` | 636 min |
| `phalcon__cphalcon` | `build/phalcon/phalcon.zep.c` | 623 min |
| `Wilfred__difftastic` | `vendored_parsers/tree-sitter-kotlin/src/parser.c` | 434 min |
| `MoarVM__MoarVM` | `src/strings/unicode_db.c` | 726 min |
| `KratosMultiphysics__Kratos` | `applications/ContactStructuralMechanicsApplication/custom_conditions/ALM_frictional_mortar_contact_condition.cpp` | 313 min |
| `OpenMPT__openmpt` | `include/opus/dnn/fargan_data.c` | 243 min |
| `OpenMPT__openmpt` | `include/opus/dnn/nolace_data.c` | 243 min |
| `vsg-dev__VulkanSceneGraph` | `src/vsg/utils/shaders/pbr_ShaderSet.cpp` | 232 min |
| `u3d-community__U3D` | `Source/ThirdParty/SQLite/src/sqlite3.c` | 117 min |

The ledger of each of these 11 projects shows that its finished Parquet comes from the
run with the swap. The two OpenVSP files have 1 commit each, so plain blame gives every
line to that commit, as `-C100` would, unless `-C100` finds a copy from another file in
that commit.

**Dataset v1: for some runs, the ledger names an older cregit commit than the blame code
that ran.** On 2026-10-11 the blame time limit was put into running cregit copies in
place. The operator replaced `blameRepo/formatBlame.pl` (old sha256 prefix `bcb80679`)
with the file of cregit `57551d7` (sha256 `788acb9a…0b435`; `9ccead6` has the same
file). The ctp ledger records the cregit commit when a run starts. So the ledger of
these runs still names `95cdc88` or `1f17663`.

| time (UTC) | copies changed |
| --- | --- |
| 2026-10-11 20:08:45-20:08:50 | 20: spot a `/w/{batch1resta2, batch10a1-a5, batch6a1, batch9a1}`; spot b `/w/{batch1restb2, batch11b1, batch15b4, batch16b3, batch17b1, batch17b4, batch17b5, batch18b1}`; big EC2 `/b5L`, `/bL7`, `/bL7b`, `/bL22` |
| 2026-10-11 20:57:16-20:57:19 | 5: spot a `/w/batch11a1`, `/w/batch11a2`; spot b `/w/batch18b2`, `/w/batch18b3`, `/w/batch18b4` |
| 2026-10-12 01:29 | 1: `/w/batch1resta2` on the new spot a, after a spot relaunch |
| 2026-10-12 06:31:13 | 1: `/w/batch1restb2` on the new spot b, after a spot relaunch |

Spot a and spot b are the two EC2 spot instances. The big EC2 is a third instance, which
runs the L runs. On 2026-10-11, 21 changed copies were at `1f17663` and 4 at `95cdc88`.
The two copies of 2026-10-12 were at `95cdc88`. So there were 27 changes in 25 copy paths. The
template copies `/data/cregit-ec2` kept the old file. On spot a, the batch 3 part
(`brave-core`) runs in its template, so its files have no time limit. Records:
`s7-run/blame-time-limit-deploy-20261011.txt` (the command output),
`s7-run/blame-time-limit.md` (the plan) and `s7-run/runs-20261009.md`.

* **What changed.** `blameRepoFiles.pl` starts one `formatBlame.pl` process for each
  file, and Perl reads the script when the process starts. So a file whose blame started
  after the swap time used the new code. A file whose blame ran at that time kept the
  old code.
* **Effect.** The new code changes the output in two cases only. (1) `-C100` runs for
  more than 3600 s: plain blame follows, with a `PLAIN-BLAME` line (previous entry).
  (2) git fails: the new code writes no `.blame` file and step 7 fails, so the project
  fails and runs again. The old code wrote an empty or a partial `.blame` file and exited
  0. In all
  other cases, the old and the new script give the same `.blame` bytes (measured on 116
  real files).
* **Detect.** In these runs, a file differs from the output of the ledger commit only if
  the log has a `PLAIN-BLAME` line for it (the manual swap files of the previous entry
  are a separate case). On 2026-10-14 this is true for 39 files in 9 projects:
  `arangodb__arangodb` 23 and `chapel-lang__chapel` 4 (ledger `95cdc88`);
  `IfcOpenShell__IfcOpenShell` 6, `catboost__catboost` 1, `maplibre__maplibre-native` 1,
  `mudita__MuditaOS` 1, `pi-hole__FTL` 1 and `zeek__zeek` 1 (ledger `1f17663`); and
  `Pzqqt__android_kernel_xiaomi_marble` 1 (the L7-20 run, copy `/bL7` at `1f17663`).
  The logs of the L7-20b run (copy `/bL7b`) are not on the analysis machine yet.

**Both commit layers: seven cregit commits wrote the Parquets, and one equality is not
measured.** Source: `tools.lock` of the dataset paper repository (checked 2026-10-14; the
rows of `365a47a` and `14b8db4` and the branches: tool review of 2026-10-28).
All seven commits start from `95cdc88`. Each machine made the libexpat gate Parquet
(libexpat at `a76b1174`, fixed flags) and compared it before its runs started.

| cregit | what it is | runs | libexpat gate |
| --- | --- | --- | --- |
| `95cdc88` | base | batch1rest, batch2, batch3, batch3L, batch4, batch5, batch5L, batch6, batch6L | `40511e0d…`, 2,184,432 B |
| `9ccead6` | `95cdc88` plus the 3600 s `-C100` limit | batch6, 1 project (`mongodb__mongo`) | `40511e0d…`, 2,184,432 B |
| `1f17663` | `95cdc88` plus the trailer fix: the `footer_*` columns come from the original log DB, one entry for each person | batch7-batch10, part of batch11-batch18, L7-20, L7-20b, L22, local-a | `14eb5d72…`, 2,196,248 B |
| `57551d7` | `1f17663` plus the 3600 s `-C100` limit | part of batch11-batch18, batch19-batch26, L23, L1r, local-b, local-c | `14eb5d72…`, 2,196,248 B |
| `23e4ccb` | `57551d7` plus plain blame after a `-C100` git exit 128 | local-d (`gitlab.inria.fr__cedar__connectionlens` only) | `14eb5d72…`, 2,196,248 B |
| `365a47a` | `57551d7` plus a lock for each output file and a skip of a finished `.blame` | the blame step of 4 running copies, changed in place on 2026-10-15: batch11a1 (`googleapis__google-cloud-java`) and batch19b1 (`leanprover__lean4`) from 12:08 UTC, batch13a1 (`RMerl__asuswrt-merlin.ng`) from 12:25 UTC, bL7b (`liferay__liferay-portal`) from 14:03 UTC. The ledgers name `57551d7` (`liferay__liferay-portal`: `1f17663`) | `14eb5d72…`, 2,196,248 B |
| `14b8db4` | `57551d7` plus the fix of the path defects of step 7 (entry below) | the path-fix rerun (35 projects of both layers) and `BRL-CAD__brlcad` | `14eb5d72…`, 2,196,248 B |

Public branches of `github.com/ccsl-codev/cregit`: `95cdc88` the branch of the batch 2
cloud run (its head);
`9ccead6` `fix/blame-time-limit-95cdc88`; `1f17663` and `57551d7`
`fix/trailers-original-db` (`57551d7` is the head); `23e4ccb` `fix/blame-exit128-fallback`;
`365a47a` `fix/blame-skip-lock`; `14b8db4` `fix/path-quoting`. `95cdc88` is the merge of
the branches of pull request #110 (`perf/step2-serial-path`, open) and pull request #109
(`fix/srcml-nontermination`, merged as one squash commit).

A sixth commit, `157cf8b`, wrote the first batch 1 outputs on the local machine. The
local-c run (49 projects) and the L1r run (`torvalds__linux`, `SAP__project-foxhound`)
run them again, so v1 has no `157cf8b` Parquet when L1r ends. On 2026-10-14 17:45 UTC
the analysis machine has no L1r output for `SAP__project-foxhound` yet.

* **Proven equal on the gate.** `95cdc88` and `9ccead6` give the same gate file.
  `1f17663`, `57551d7` and `23e4ccb` give the same gate file. The gate run has 0
  `PLAIN-BLAME` lines, so it does not test the fallback paths. The equality holds for
  files whose `-C100` blame ends within 3600 s with git exit 0.
* **Different on the gate, by design.** `95cdc88` and `1f17663` differ only in the 15
  `footer_*` columns. `95cdc88` fills them wrongly: blobExec appends a
  `Former-commit-id` paragraph to each message, so JGit finds no other trailer.
* **Not measured.** After the run, `rewrite_footer_columns.py` rewrote the `footer_*`
  columns of the `95cdc88` Parquets (931 Parquets, status ok in
  `census-cloud-fixed/manifest.tsv` of the build workspace, 2026-10-14). The tool uses the SQL of the
  `1f17663` branch, and a test fails if that SQL changes. But no record compares a
  rewritten `95cdc88` gate Parquet with the `1f17663` gate `14eb5d72…`. So the
  equality of `95cdc88` plus the rewrite and `1f17663` is not measured. For the 584
  spot-run projects, the rewrite also built the persons DB again from a pinned clone.
  This was measured equal on 1 project (`Activiti__Activiti`), except the
  `emails.dateadded` time.
* **Fixed on 2026-10-16: `mongodb__mongo`.** Its ledger names `9ccead6`, which has the
  trailer code of `95cdc88`. The first rewrite (`s7-run/footer_fix_batches.py`) took only
  Parquets whose ledger names `95cdc88`, so it skipped this file. On 2026-10-16 at 19:44
  UTC the rewrite ran on it (study commit `206f134`; status ok in the `manifest.tsv` of
  the previous item, output sha256 `3fdd5ff4…`). The merge of the 5,000+
  layer took this output (`merge_input_sha256` in `data/REBUILD-TARGETS.tsv` of the paper
  repository, merge input `post-fix`). The `pending.txt` of the same folder still names
  the project: that file is stale.

**Dataset v1: Python and TypeScript are not tokenized, so the dataset is incomplete in two
ways.** The tokenizers read C, C++, Java and Rust only (srcML and the Rust lexer). This is a
liability of the dataset, not of one run. Source: `s2-pool/pool.csv` (Linguist bytes),
`s4-filter/frame_status.v4.csv` and `frame.v4.csv` of the dataset selection, 2026-10-16.

* **Projects outside the dataset.** The language step removes a GitHub row only if Linguist
  lists no C, C++, Java or Rust. It removed 42,061 active rows with Python or TypeScript:
  19,929 with Python as the primary language, 11,111 with TypeScript, and 11,021 with
  another primary language. The 5% rule (DP3) removed 1,750 more rows with Python or
  TypeScript as the primary language; 290 of them have 5,000 commits or more (195 Python,
  95 TypeScript). The commit counts at the pinned commit (`what-if-py-ts/`, 2026-10-16; 52
  rows have no count) give these rows with a GitHub license, for the two groups together:

  | Primary language | ≥ 5,000 commits | ≥ 1,000 | ≥ 100 |
  |---|---:|---:|---:|
  | Python | 866 | 3,945 | 11,619 |
  | TypeScript | 710 | 3,167 | 8,431 |
  | another language | 1,250 | 4,246 | 9,077 |
  | all | 2,826 | 11,358 | 29,127 |

  The v1 rule (5,000 commits or more) gives 3,497 GitHub rows. So a Python and TypeScript
  tokenizer would add up to 2,826 rows, 1,576 of them with Python or TypeScript as the
  primary language. These are upper bounds: the rows did not go through the measure step or
  the 5% rule for the new languages, and the near misses of DP8L v4 are not counted.
* **Tokens missing inside v1 projects.** Of the 3,427 v1 GitHub projects with Linguist data,
  2,661 contain Python or TypeScript. These two languages are 5.7% of their code bytes. In
  362 projects they are 25% of the bytes or more, and in 201 projects 50% or more (for
  example `PostHog/posthog`, `microsoft/vscode`, `pytorch/pytorch`, `sgl-project/sglang`).
  In these projects, the Parquet holds only the C, C++, Java or Rust part.
* **Effect on results.** A person or a firm that writes only Python or TypeScript in a v1
  project has no tokens there. So author and firm shares describe the tokenized languages
  only, not the project. The machine learning and web ecosystems are under-represented.
* **Detect.** Compare the `file_path` extensions of a Parquet with the Linguist languages of
  its pool row. No column records the untokenized share.
* **Workaround.** None in v1. A Python or TypeScript tokenizer is future work.

**Both commit layers: the release is one dataset in two commit layers.** The frame has
10,763 projects: 3,548 with 5,000 or more commits and 7,215 with 1,000 to 4,999 commits.
The dataset has 10,754 project files: 3,545 in the 5,000+ layer and 7,209 in the
1,000-4,999 commit layer. The other 9 frame rows have no file (see "9 frame rows have no
project file" below). The final write (2026-10-27) holds 10,753 files with
32,626,508,161 token rows: 23,692,959,193 in the 5,000+ layer and 8,933,548,968 in the
1,000-4,999 commit layer. The separate file of `BRL-CAD__brlcad` (see "`BRL-CAD__brlcad`
ran from its new host" below) adds 8,666,457 rows. Thus the 10,754 release files hold
32,635,174,618 token rows, 23,701,625,650 of them in the 5,000+ layer. The older count
32,626,028,860 (5,000+ layer: 23,692,479,892) is the input check of the links run of
2026-10-24. It is before the 17 path-fix files of the 5,000+ layer, which added 479,301 rows.
The final error model still uses the older count (0.0015% fewer rows). Each size names its stage:

* **Run output:** 241.3 GB. That is 186.2 GB for the 5,000+ layer (before ZSTD 19) and
  55.09 GB for the 1,000-4,999 commit layer (the input of its merge).
* **Release files before the final write (ZSTD 19):** 162.6 GB. That is 125.83 GB for the
  5,000+ layer with the organization columns of 2026-10-21 and 36.81 GB for the 1,000-4,999
  commit layer before its organization columns.
* **Release folder of the final write (ZSTD 19, organization columns and pseudonyms of both
  layers):** 162,429,007,486 bytes (162.4 GB): the 10,753 Parquet files and 6 metadata files
  (`aws s3 ls --summarize`, 2026-10-27).
* **Release folder of `BRL-CAD__brlcad` (ZSTD 19):** 45,737,481 bytes. The Parquet file is
  45,229,873 bytes. With it, the 10,754 Parquet files are 162,466,431,130 bytes (162.5 GB).

The "Dataset v1" entries above are about the 5,000+ layer. Sources: `STATUS.md` section 0
of the dataset selection records (the token counts are the input check of the links run of
2026-10-24); `RELEASE_PLAN.md`, decision "One dataset" (2026-10-23).

**The 1,000-4,999 commit layer ran with the code of the 5,000+ layer, on two kinds of
machine.** The frame of this layer has 7,215 projects: the rows of `frame.v4` with 1,000 to
4,999 commits (`x1k/frame.x1k.csv` of the dataset selection; user decision 2026-10-19: the
rows join the released dataset). No row of this layer is in a manifest of the 5,000+ layer.
The run used the code and flags of the 5,000+ layer: ctp `ca113e3`, cregit `57551d7`,
libexpat gate Parquet `14eb5d72`, the flags of the fed parts with `--latest`.

* Two machines ran 7,204 rows (2026-10-19 17:24 UTC to 2026-10-20 17:05 UTC).
* A third machine with EBS volumes (the long-row machine) ran the 11 longest rows. A spot
  reclaim stopped it on 2026-10-20 at 00:48 UTC. It started again at 12:28 UTC (later on an
  on-demand machine). It resumed each row at its last step and kept the finished `.blame`
  files. Its last row ended on 2026-10-23 at 16:53 UTC.
* The 18 rows of the path defect ran again with cregit `14b8db4` (see "blame dropped
  in-scope files with a special path name" below).
* The merge (metadata rewrite, ZSTD 19) holds 7,209 of 7,209 projects, 0 failed. Its
  second pass took the 4 slowest rows and ended on 2026-10-23 at 19:31 UTC.
* The stratum labels of this layer come from the same rules and rosters as those of the
  5,000+ layer (2026-10-20, `x1k/project_meta.x1k.final.json`). They have no review by hand.

Sources: `~/s3-work/dataset-x1k/README.md`; `STATUS.md` sections 0 and 1.

**Both commit layers: 9 frame rows have no project file.** The frame has 10,763 rows and
the dataset has 10,754 files. The rows below stay in the frame with their reason. Count them
in the frame, not in the token tables. The entries "`kitao__pyxel` is pending" and "three
more frame rows have no Parquet" above give the details of the rows of the 5,000+ layer.
Their status there is of 2026-10-13 and 2026-10-14. One of these rows, `BRL-CAD__brlcad`,
has a file now (see "`BRL-CAD__brlcad` ran from its new host" below). This table gives the
status of the release.

| project | layer | frame commit | run commit | reason | status |
| --- | --- | --- | --- | --- | --- |
| `kitao__pyxel` | 5,000+ | `50f9bd77` | - | The repository is gone: GitHub disabled the owner's account. | no file in the release |
| `googleapis__discovery-artifact-manager` | 5,000+ | `9b6a724` | `bb221e1` | Not a failure. The project deleted its Java code on 2026-10-07, after the frame measurement: the frame commit has 559 C, C++, Java or Rust files, the run commit has 0. | code removed after the frame measurement (user decision 2026-10-11) |
| `signalwire__freeswitch` | 5,000+ | `3dee68e` | - | The only pipeline failure of the 9 rows. blobExec stops in step 2 on a commit with no committer. Tool defect, not fixed. | excluded (user decision 2026-10-11); not fixed in this release (user decision 2026-10-26) |
| `Librelancer__Librelancer` | 1,000-4,999 | `dcbcf80` | `c113747` | Not a failure. After the frame measurement, the project moved its C and C++ code to git submodules: the frame commit has 44 such files, the run commit has 0. The run commit has 4 files (`.gitmodules`, a README, `.gitignore`, `.editorconfig`). The source is in git submodules, and the pipeline does not follow submodules. History: 122 in-scope paths. 4 attempts. | code removed after the frame measurement (user decision 2026-10-20) |
| `vllm-project__semantic-router` | 1,000-4,999 | `41c0903` | `c9b65d5` | Not a failure. After the frame measurement, the project deleted its C, C++, Java and Rust code: the frame commit has 244 such files, the run commit has 0. 11,665 files (Go, Python, TypeScript), 0 in scope. History: 253 in-scope paths. 4 attempts. | code removed after the frame measurement (user decision 2026-10-20) |
| `visgl__loaders.gl` | 1,000-4,999 | `81ad3e5` | `6773abc` | Not a failure. After the frame measurement, the project deleted its C, C++, Java and Rust code: the frame commit has 335 such files, the run commit has 0. 6,822 files (TypeScript, JavaScript), 0 in scope. History: 670 in-scope paths. 4 attempts. | code removed after the frame measurement (user decision 2026-10-20) |
| `greta-dev__greta` | 1,000-4,999 | `179021a` | `c656bd5` | Not a failure. After the frame measurement, the project deleted its C, C++, Java and Rust code: the frame commit has 449 such files, the run commit has 0. 426 files (R), 0 in scope. History: 477 in-scope paths. 4 attempts. | code removed after the frame measurement (user decision 2026-10-20) |
| `notnotype__neuro-book` | 1,000-4,999 | `6faecf8` | `532ebb8` | Not a failure. After the frame measurement, the project deleted its C, C++, Java and Rust code: the frame commit has 2 such files, the run commit has 0. 8,976 files (TypeScript, Vue, Markdown), 0 in scope. History: 4 in-scope paths. 11 attempts. | code removed after the frame measurement (user decision 2026-10-20) |
| `pur1fying__blue_archive_auto_script` | 1,000-4,999 | `8a7ff4c` | `0d57f47` | Not a failure. After the frame measurement, the project deleted its C, C++, Java and Rust code: the frame commit has 46 such files, the run commit has 0. 3,788 files (PNG, Python), 0 in scope. History: 46 in-scope paths. 11 attempts. | code removed after the frame measurement (user decision 2026-10-20) |

Only 1 of the 9 rows is a pipeline failure (`signalwire__freeswitch`). 1 repository is gone. The other 7
rows are not failures: each project had C, C++, Java or Rust files at its frame commit (the commit that the
selection measured, 2026-09-23 to 2026-10-05), and it deleted them, or moved them to git submodules, before its
run commit (the newest commit on the run date, 2026-10-10 to 2026-10-20). The file counts come from the GitHub
trees of both commits (2026-10-25). The code-size rule was right for the commit that it measured.

The 6 rows of the 1,000-4,999 commit layer have the case of
`googleapis__discovery-artifact-manager`. The run uses the newest head at the first attempt
(`--latest`). At that commit the repository has no C, C++, Java or Rust file, but its
history has such files. Blame finds 0 files, and step 10 stops with "no .blame files found"
and "no tokens processed" at each attempt. A rerun gives the same result. A run at the
frame commit can give tokens, but it breaks the `--latest` rule of the dataset.

Sources: `V1_NOT_IN_DATASET` in `report/build_status_html.py` of the dataset selection
records (the rows of the 5,000+ layer); `x1k/manifest.x1k.tsv` (frame commits) and
`~/s3-work/dataset-x1k/README.md` (run commits, file counts, attempts) for the other 6;
`STATUS.md` section 2 (decision of 2026-10-20).

**5,000+ layer: `BRL-CAD__brlcad` ran from its new host, `git.brlcad.org`.** GitHub hides
the repository of the project: GitHub answers "Repository not found" since 2026-10-11. The
project moved its repositories to `https://git.brlcad.org/BRL-CAD/brlcad` (the owners told
the user on 2026-10-26). The final write of the other 10,753 files had no file for it. A
separate run made its file after the final write (user decision 2026-10-26).

* **The history is the same.** The frame commit `27ba3c48` is on the new host with the same
  commit id. Its commit count is 85,051 on both hosts (the forge API on 2026-10-26, and
  `git rev-list --count` on the run machine). The run machine also checked that the frame
  commit is an ancestor of the run commit, and that the tree of the frame commit gives the
  frame measurement again. The select check passed on 2026-10-26 at 23:00 UTC. The clone
  needed retries, because the forge answered HTTP 503. `select.json` gives: frame commit
  `27ba3c48` present on the new host, with 85,051 commits (the frame count); the run head
  `ed0b1e09` on `main` has 85,110 commits, and 85,051 of them are before the frame date; the
  tree of the frame commit gives the frame measurement again (8,165 blobs, 328,062,026 bytes,
  3,535 tokenizable files with 47,887,256 bytes, 917,970 vendored bytes, 65,815,855 code
  bytes); the code-size share is 0.7136 (rule: at least 0.05). Thus the selection facts of the
  row do not change.
* **The run.** ctp `ca113e3` and cregit `14b8db4` (the cregit of the path-fix rows), with the
  flags of the 5,000+ layer. ctp ran for 59 minutes and ended on 2026-10-27 at 00:01 UTC. The
  file has 8,666,457 token rows. 0 files fell back to plain blame. The run commit is the head
  of `main` at the first attempt (`--latest`). `ctp-summary.json` gives the run commit
  `ed0b1e098a8e5bbf32e7baf15573e017eda74626`, resolved on 2026-10-26 at 23:02:59 UTC (2
  attempts; the pipeline step took 3,441 s), and the Parquet sha256 `83cbc17f41ce`. On
  2026-10-26 at about 21:30 UTC the head of `main` was `ed0b1e09`, with 85,110 commits
  (`evidence/ls-remote.20261026.txt` and the forge API). The run commit is not on GitHub.
* **The `clone_url` column keeps the GitHub URL.** The 29 provenance columns come from the
  v1 sidecar, as for every other file of the layer. Thus `clone_url` is
  `https://github.com/BRL-CAD/brlcad.git`, but the run fetched from `git.brlcad.org`. The
  ledger of the run records the URL that the run used.
* **Organization columns and pseudonyms.** The file has the same database (v4.9) and the
  same salt as the other 10,753 files, so a person has the same pseudonym in all files. The
  footer of this file names registry B: the registry of the final write plus the identities
  of this file. The other files name the registry of the final write. Each pseudonym has
  the same value in both registries. Registry B (`56369303e6e2`) is registry A
  (`ca284a4e1b9f`) plus the identities of BRL-CAD: 0 collisions, 0 pseudonyms changed.
* **One mapping hash for all 10,754 files.** The links of the write are the links of the
  final write (mapping `7b52a5155e6f`). One `build_links.py` run over the caches of all
  10,754 files gives 0 new domain rows, so the second link set is equal to the first, and
  there was no choice to make. The project row of BRL-CAD gets its enterprise columns from
  its own cache. These columns are not in the mapping hash.
* **The database has no row for `army.mil`.** The audits of the database used the domains
  of the other 10,753 files, and BRL-CAD was not one of them. Thus a domain that only
  BRL-CAD authors use has no review. A real domain with no row gives `independent`.
  `links-merge.txt` gives the BRL-CAD tokens by class: 8 real domains with no row hold
  17,847 tokens (0.2059% of its 8,666,457 tokens; they get `independent`), 12 domains of the
  final write table hold 8,648,592 tokens (99.7939%), and 1 value with no real domain holds
  18 tokens (0.0002%; it gets `unknown`). The database has no new domain row for BRL-CAD, so
  the link tables with and without BRL-CAD give the same values (0 tokens change).
* **The write.** The write ended in an alert only because of the row-group defect of the
  check-out of the final write (DuckDB writes row groups of up to 1,001,472 rows, and the
  check-out refused more than 1,000,000). The file passed the leak scan. The fixed check-out
  (`final_tools.py` `83ef5cf204bd` -> `45f36393fc4a`) gives 1 file, 8,666,457 rows, 0 files
  with problems. The release folder `dataset-brlcad/release/dataset-brlcad-1/` was uploaded
  on 2026-10-27 at 01:05 UTC. Then the machine powered off.
* **Detect.** The footer key `pseudonymization_registry_sha256` of this file is not the
  value of the other files.
* **Status:** done on 2026-10-27 (run `dataset-brlcad-1`).

Sources: `~/s3-work/dataset-brlcad/README.md` (the kit, the evidence, the design) and
`fix-20261027/README.md` (the check-out fix);
`~/Projects/cregit-workspace/agent-runs-20261023/brlcad/evidence/` and `PROGRESS.md` (the
remote refs and the forge facts of 2026-10-26, no person data); `STATUS.md` section 0 of the
dataset selection records (entries of 2026-10-27); the status files and the object list of
S3 `dataset-brlcad/work/dataset-brlcad-1/` and `dataset-brlcad/release/dataset-brlcad-1/`.

**Both commit layers: blame dropped in-scope files with a special path name. cregit
`14b8db4` fixes this, and 35 projects ran again.** Step 7 (blame) of cregit `57551d7` and
of all earlier commits of the fork has these defects:

1. `blameRepoFiles.pl` read `git ls-files` without `-z`. git quotes a path that has a byte
   above 0x7F, a `"`, a `\` or a control byte (`core.quotePath` is not set on the run
   machines). The quoted name ends in `"`, so the mask (`\.(c|...)$`) does not match it.
   The file has no `.blame` file and no row in the Parquet. No log line names it.
2. `blameRepoFiles.pl` gave the path to `formatBlame.pl`, and `formatBlame.pl` gave it to
   `git blame`, with no `--` before it. `GetOptions` reads a path that starts with `-` as
   an option ("Error in command line arguments"). Step 7 then exits 1 and the project
   fails. A later path segment that starts with `-` (`a/-b.c`) is not affected.
3. Only in cregit `95cdc88` and `1f17663` (and the commits before them): `formatBlame.pl`
   ran git through a shell string. A `'` in the path breaks the command. The script then
   wrote an empty `.blame` file and exited 0, so the file has no rows and no error.
   The old script does not check the exit status of git at all. So any git failure
   (not only a `'`) gives an empty or a partial `.blame` file and exit 0, and the file
   then has no rows or only a part of its rows (test of the tool review of 2026-10-28:
   a git that stops after 1 of 2 records). This applies to all runs of `95cdc88` and
   `1f17663` code (batch1rest to batch 10, part of batches 11-18, L7-20, L7-20b, L22,
   local-a). Only the `'` class and the check of 61 Parquets below are measured.
   cregit `57551d7`, `9ccead6` and `23e4ccb` run git with an argument list and do not have
   this defect.

* **Measured (scan of 2026-10-20, the tree of each project, the run mask):** 5,000+ layer:
  1,709 files (2,523,350 bytes) in 19 projects. Defect 1: 1,661 files in 16 projects.
  Defect 3: 48 files in 4 projects (the 2 files of `github__codeql` are empty). Defect 2: 0
  (a project with such a path failed and is not in this layer). 1,000-4,999 commit layer:
  3,097 files (16,694,820 bytes) in 18 projects. Defect 1: 3,042 files in 17 projects.
  Defect 2: 55 files in 1 project (`saleha-muzammil__Academic-Time-Machine`, which failed).
  `huaweicloud__ModelArts-Lab` lost all its 8 in-scope files and failed.
* **Check:** in 61 local Parquets of the 5,000+ layer (the affected ones and 40 at random),
  the in-scope files with no row are exactly the files of the scan.
* **Fix:** cregit `14b8db4` (`57551d7` with `git ls-files -z` and `--` before the paths in
  both scripts). The libexpat gate Parquet does not change (`14eb5d72`, 2,196,248 bytes).
* **Rerun:** 35 projects (17 of the 5,000+ layer, 18 of the 1,000-4,999 commit layer), with
  the full pipeline, ctp `ca113e3`, cregit `14b8db4`, the flags of the fed parts, at the
  commit of the first run. The rerun ended on 2026-10-20 at 23:25 UTC: 35 of 35 OK.
  Not run again: `github__codeql` (its 2 lost files are empty, so no tokens are lost),
  `JetBrains__intellij-plugins` and `KDE__labplot` (their runs used `57551d7`, so their files
  with a `'` have rows), and `alibaba__AliSQL` (next item).
* **`alibaba__AliSQL` (5,000+ layer) keeps the defect (user decision 2026-10-20).** Its
  Parquet (cregit `57551d7`, commit `9e121f27`) has no rows for 1 in-scope file (defect 1):
  458 bytes, 10 lines, a Boost.Wave test file in a folder with a non-ASCII name. That is 1 of
  50,284 blamed files of the project. It did not run again, because its first run took 66 h
  (63 h of blame).
* **Not measured:**
  * The scan of the 1,000-4,999 commit layer read the tree at the manifest commit, not at
    the run commit (the read of the run commits was denied). The runs started 2.7 to 3.6
    days after the manifest. A file with a special name that came in between is not in the
    scan, and its project did not run again.
  * `git.savannah.gnu.org__denemo` (5,000+ layer): the scan could not list its tree.
  * Old file names in the history: a `;` in the old name of a renamed file breaks the
    record split in step 10. The scan found 0 paths with `;` in the trees, but it did not
    read the old names.
  * The rerun files have no recorded check (the rows of the other files equal in all
    columns, the new rows exactly the lost files).
* **Detect:** compare the in-scope paths of the run commit (`git ls-tree -r -z`) with the
  `file_path` values of the Parquet.

Sources: `~/Projects/cregit-workspace/path-defect/affected.tsv` and `README-scan.txt`
(scan); `~/s3-work/dataset-pathfix/README.md` (rows, decision "35 rows, no AliSQL");
`STATUS.md` section 2 (rerun result).

**The 1,000-4,999 commit layer: some files have blame without copy detection.** The
mechanism is the time limit of cregit `57551d7` (see the entry "Dataset v1: some files have
blame without copy detection"). cregit `14b8db4` has the same limit. After 3600 s of
`-C100`, the script stops git, runs plain blame and logs a `PLAIN-BLAME` line. The effect on
these files is not measured.

Count of 2026-10-24, from the pipeline logs of all 7,209 projects (0 projects with no log).
A file that falls back in two attempts counts once.

| rows | logs read | projects | with `PLAIN-BLAME` | files |
| --- | --- | ---: | ---: | ---: |
| long-row machine (11 rows less the 2 rows with no tokens) | all logs of each project: a resume at step 7 keeps the `.blame` files of the attempts before it | 9 | 6 | 229 |
| the two other machines | the newest log | 7,182 | 92 | 138 |
| path-fix rerun | the newest log of the rerun | 18 | 0 | 0 |
| extra machines (`awslabs__aws-sdk-rust` 5 parts, `aws__aws-sdk-java` 2 parts) | their own `plainblame.log` | - | 0 | 0 |
| **total** | | **7,209** | **98** | **367** |

* The limit is wall time. From 2026-10-20 the slow rows of the long-row machine got more
  blame jobs (more parallel git processes), which can push more files over the limit. A
  file can fall back on one machine and not on another.
* The count of the 5,000+ layer (201 files, see the entry above) is of 2026-10-14. It was
  not made again for the 17 projects of that layer that ran again with the path fix.

Sources: `plainblame.x1k.json` of the links run
(`~/Projects/cregit-workspace/companies/x1k-links/dataset-x1k-links-1/`); `RELEASE_PLAN.md`,
decision "Links of both layers" (the count rules).

**The 1,000-4,999 commit layer: a retry limit stopped `awslabs__aws-sdk-rust` once.** On
2026-10-23 at 14:19 UTC, ctp stopped the row with "not retried".

* **Cause:** two timers of the long-row machine restart a slow row in step 7. The
  rebalance timer gives it more blame jobs. The pull timers drop the files that extra
  machines blamed (next entry). Each restart stops the row with SIGTERM, and ctp counts
  each such stop as a failed "interrupted" attempt. After 11 attempts the count was more
  than `--retries 10`.
* **Fix (14:30 UTC):** the retries went from 10 to 30, and the row started again at step 7
  with the finished `.blame` files kept. The row ended on 2026-10-23 at 16:53:50 UTC with
  293,153,594 token rows.
* **Effect on the data:** none known. A test resume at step 7 gave Parquets that are
  byte-equal to the gate Parquet `14eb5d72` (0 differing rows). The restarts can change
  which files fall back to plain blame (the limit is wall time).
* **Lesson:** raise `--retries` before you use a restart scheme.

Sources: `STATUS.md` section 0; `~/s3-work/dataset-x1k/README.md` (resume test).

**The 1,000-4,999 commit layer: extra machines blamed part of the files of
`awslabs__aws-sdk-rust` and `aws__aws-sdk-java`.** These two rows are generated SDK code.
On the long-row machine alone, their step 7 needed days to weeks. The files that were left
went into parts. Each extra machine ran the cregit `57551d7` blame script with the same git
path as the long-row machine (the setup refuses another git). Before its part, each extra
machine blamed 30 files that the long-row machine already had, and compared them byte for
byte. The long-row machine pulled the finished `.blame` files and restarted the row, so its
file list skipped them.

* `awslabs__aws-sdk-rust`: 5 parts on spot machines, with 7 replacements (2026-10-21
  20:37 UTC to 2026-10-22 00:00 UTC). Spot reclaims stopped all of them. The byte check
  gave 30 of 30 equal on every part that reached it. The row uses the pulled files, but
  their exact count is not measured.
* `aws__aws-sdk-java`: 2 on-demand machines (c7a.48xlarge), done 2026-10-23 12:25 UTC. The
  byte check gave 30 of 30 equal on both. 65,087 files were pulled (0 bad, 0 refused). The
  row restarted once (12:40 UTC) with 0 files left in step 7.
* `PLAIN-BLAME` on the extra machines: 0 files for both rows.
* **Detect:** nothing in the Parquet tells which machine blamed a file.

Sources: `~/s3-work/dataset-x1k/rust-split/README.md` and `java-split/README.md`;
`STATUS.md` section 2 (rows "Extra spot machines for aws-sdk-rust" and "On-demand extra
machines for aws-sdk-java"); `plainblame.x1k.json` (extra machines).

**Both commit layers: the code-size rule of the selection counts bytes by extension. A
line counter gives a slightly different frame.** The selection keeps a project when its own
C, C++, Java and Rust bytes (not vendored) are at least 5% of its code bytes. Code bytes are
the bytes of the extensions that every claiming Linguist language marks as "programming",
plus the language mask. The release keeps this byte rule (user decision 2026-10-23).
tokei 15.0.0 is a check. A stratified sample of 100 projects (84 rows of the two layers in 4
share bands, 16 rows that the Linguist gate removed) gives this estimate:

| layer | rows at the rule | tokei drops (95% interval) | tokei adds (95% interval) | net |
| --- | ---: | ---: | ---: | ---: |
| 5,000+ | 3,497 | 99 (86-115) | 34 (21-53) | -65 (-1.9%) |
| 1,000-4,999 | 7,215 | 201 (172-237) | 81 (49-125) | -121 (-1.7%) |

The intervals show only the sampling error of the ratio, not the error of the model. The
3,497 rows of the 5,000+ layer do not include its 51 GitLab rows. The causes (from the
whole frame, exact):

* `.ts` and `.tsx` (and `.mm`, `.pm`, `.inc`) are not code bytes, because Linguist also
  gives these extensions to a data language. If `.ts` and `.tsx` count as code, 67 rows of
  the 5,000+ layer and 135 rows of the other layer fail the rule. This is most of the
  tokei drop.
* Vendored bytes are in the code total but not in the share. If they also leave the
  total, 25 rows of the 5,000+ layer and 48 rows of the other layer pass.
* `measure_clones` reads `git ls-tree` without `-z`, so a path with non-ASCII characters
  gets a wrong extension. This affects only the rows measured from a clone (other hosts,
  truncated trees). In the sample it changed the bytes of 2 rows and no result.
* sloccount 2.26 is not used: it does not know Rust.

Sources: `~/Projects/cregit-workspace/agent-runs-20261023/loc-tools/SUMMARY.md`;
`RELEASE_PLAN.md`, decision "Code-size rule" (2026-10-23).

**Both commit layers: the tokens differ from stock srcML `d382719`.** The C, C++ and
Java tokens come from srcML `d382719` with the patch `nix/srcml-nontermination.patch` of
cregit (commits `50a63193` and `543fed6a`, public branch `fix/cregit-nontermination` of
`github.com/EllianCarlos/srcML`). The patch changes the tokens of some files: two
adjacent string literals (`"a""b"`) are two tokens (stock srcML can run a quote swap to the
end of the file); files on which stock srcML does not finish are present, with the
recovery markup of the patched parser; some runs of quotes in code that the preprocessor
removes (for example in `#if 0`) give other tokens. The number of such files is small and
not measured (fuzz and hand cases of the tool review of 2026-10-28). Exception: the
superseded batch 1 run of `157cf8b`, which used stock srcML and is not in the release.

**Both commit layers: an older srcML defect stays.** A string on a preprocessor line ends
at a backslash-newline. In `sqlite3.c` an `#error` line has such a string, and the
tokens after it go wrong in both builds (stock and patched). The number of such files is
not measured.

**Both commit layers: `person_name` is always equal to `personid`, and the merge rule of
the person ids has limits.** `persons.personname` is never filled, so
`coalesce(personname, personid)` gives the person id (a scan of 199 files and a test with
the persons jar of the dataset). There is no display name. `unifyPersons.scala`
(`157cf8b`) makes the key from the name in lower case with no accents; a name of one word
gets `at` and its address added. Two persons with the same full name get one id. An address
that two names share joins them into one id, and such joins chain. When two keys tie, the
order of a hash set decides. The lower-case step (`toLowerCase` with no locale) uses the
default locale of the JVM. False merges are not counted. In the release, `person_name` and
`personid` differ only in the first letter of the pseudonym (`Author`, `author`).

**Both commit layers: the trailer columns follow JGit 4.6, not git.** The `footer_*`
lists come from `getFooterLines` of JGit 4.6.0 (slickgitlog) on the original message:
every `Key: value` line of the last paragraph is a trailer, with no 25% rule and no blank
line necessary, and a continuation line is dropped. So a `Reported-by:` line in a last
paragraph of prose gives 1 trailer, and git 2.56 gives 0 (a probe repository of the tool
review of 2026-10-28). Each of the 13 typed lists holds one entry for each person (the
entry with the lowest trailer index); two values are one person when their addresses
give the same `personid` or the same address in lower case. The rewrite of the `95cdc88`
Parquets uses the same SQL, so the rule holds for all files.

**Both commit layers: some run-time inputs are not pinned.** (1) Step 10
(`generate_dataset.py`) runs `INSTALL sqlite_scanner`, so DuckDB downloads the extension
from the network. The build of the extension on the run machines is not recorded; the
local machine has build `f79b1db` for DuckDB 1.5.5 (2026-10-28). (2) Rule E of
`pseudonymize.py` (`find_psl`) takes the newest Public Suffix List in `/nix/store`. The
release used the list with sha256 prefix `65365c4c9a4a`; another list stops the write.
(3) formatBlame, the runner and ctp do not set `GIT_CONFIG_NOSYSTEM` or
`GIT_CONFIG_GLOBAL`, so the git configuration of the machine can change the blame. (4)
The time zone (the dates entry below).

**Both commit layers: a torn ledger line loses the next row too.** `ledger.py`
(`ca113e3`) appends one JSON line for each step. If a crash cuts a line, the next append
joins it, and `read()` drops both rows. The docstring ("a crash loses at most the row
being written") is wrong. Count on 2026-10-28: in the raw ledgers of the release work
(`release-work/ledgers-20261028/`: 12,291 ledger files, 870,561 lines that are not
empty), **0** lines fail `json.loads`. So this defect lost no row of the release ledgers.

**Both commit layers: identities get pseudonyms, but the pseudonymizer does not change
`commit_summary`.**

* `commit_summary` is the subject line of the commit log of the cregit run, and the
  pseudonymizer does not change it. A name or an address that a person wrote into a
  message stays (user decision 2026-10-25; it replaces the choice of 2026-10-24). A check
  of this column against the original commit messages is in progress.
* The trailer columns (`footer_*`) get pseudonyms. The release used `pseudonymize.py` of
  ctp-anon `4be3800`, then the hotfix `a4b511b` (public branch
  `feat/pseudonymise-identity-columns`; scheme 2, a nested sha256 with keys from a salt),
  not `anonymize_parquet.py` of the entries below and not `f3fdd7a`. The salt was deleted
  after the write, by design, so nobody can compute these pseudonyms again. The registry
  (real identity to pseudonym) stays private. It is never published.
* An e-mail value is published as a pseudonym, an at sign and a domain. Some values have
  two or more at signs. In the 5,000+ layer: 680 distinct values in 178 files, on
  15,282,753 token rows (0.0645% of the tokens of the layer). 96% of these token rows hold
  git-svn ids: an svn user name that is an address, an at sign, then the repository UUID.
* For these values, the domain is the first part after an at sign that is a valid domain
  (strict characters, a dot, an ICANN public suffix), else empty. `person_domain` uses the
  same rule, and the organization columns use the same domain. Thus the release publishes
  no local part and no UUID. Of the 677 distinct `person_email` values, 668 give a valid
  domain and 9 give an empty domain (user decision 2026-10-25; it replaces the rule "after
  the last at sign" of 2026-10-24).
* **Not measured:** these values in the 1,000-4,999 commit layer. The analysis read the
  3,544 files of the 5,000+ layer only.

Sources: `RELEASE_PLAN.md`, decisions "Pseudonymization", "E-mail values with two or more at
signs" and "Commit messages"; `~/Projects/cregit-workspace/agent-runs-20261023/two-at/PROGRESS.md`
(counts only).

**The 1,000-4,999 commit layer: the organization columns of the new domains rest on a
review of 99% of their tokens.** The links of both layers exist since 2026-10-24 (label
v4.6). In this layer, 22,276 new domains (not in the 5,000+ layer) had no row in the
database and showed `independent`. They hold 9.17% of the tokens of the layer and 2.51% of
all tokens.

* Review round 10 (2026-10-25, database v4.7) judged the new domains with the most tokens,
  up to 99% of their tokens (3,175 domains), with fetched web evidence and DNS records. It
  judged the new namespaces at the same level (3,528 of 4,706), and a seeded 10% sample
  (93 of 921) of the new domains that already link by a rule.
* The other new domains (0.025% of all tokens) keep `independent` by the default rule: a
  real domain with no row maps to `independent`.
* **Open:** the user check of the undecided items and of the seeded sample; the links and
  the organization columns of this layer from database v4.7.
* **Open: the error model of this layer.** The audit of its strata and its error numbers
  follow. Until then, no error number covers this layer.

Sources: `RELEASE_PLAN.md`, decision "Review of the new domains" (2026-10-25);
`~/Projects/cregit-workspace/companies/review/ai/r10/PROGRESS.md`; `STATUS.md` section 0
(links of both layers).

**Both commit layers: the organization database gives the current parent only, with no
dates of acquisitions.** `parent_company_id` names the owner of an organization at the time
of the database (October 2026), not the owner in the years of the commits. The database has
no history of owners and no date of a purchase or a sale. Example: Jeppesen has the parent
Thoma Bravo, which bought it in 2025; from 2000 to 2025 the owner was The Boeing Company,
and the commits of those years are not marked as Boeing work. An analysis that groups
organizations by parent therefore uses the ownership of today for all years. A history of
owners with dates is not a goal of this dataset (the user, 2026-10-25). The columns of the
project files are not affected: they name the organization of the domain or of the
account, not its parent.

Sources: the v4.9 audit page (2026-10-25), question on the history of owners;
`manual/v49_fields.csv` (the reason of the Jeppesen row) in `dataset-selection/s8-companies/`.

**A vendored dependency attributes its whole library to the engineer who imported
it.** cregit credits the commit that introduced a line, so a wholesale vendor drop
credits the importing author with every token of third-party code. This is a
property of token-level blame, not a pipeline defect, and it is large enough to
invert a reading of the data. Measured on the fork pair above: **98.3%** of
`tendb`'s 2,446,834 Tencent-attributed tokens sit in `storage/rocksdb/`, which is
95.33% Tencent-attributed, and its single largest file is
`third-party/gtest-1.7.0/fused-src/gtest/gtest.h` at **148,729 tokens** — Google's
code. The published `tdbctl` carries the same artifact on a smaller scale:
**336,501** of its 574,465 Tencent tokens are the `extra/ncurses-5.7/` drop, which
is 100% Tencent-attributed. `storage/ndb`, 4.22 M tokens, is 0.00% Tencent.

Any per-organization or per-author aggregate over a project that vendors its
dependencies is therefore an upper bound, not a measurement. Restrict such
aggregates to non-vendored paths. No column in the dataset marks a path as
vendored, so the consumer must supply that list.

**Content moved between files was credited to whoever moved it, not to its author.
This is now corrected, and the correction is itself a large change to the data.**
Read [`REBLAME-C100.md`](REBLAME-C100.md) for the method, the cost and the full
measurement. This entry states the defect and its magnitude.

`blameRepo/formatBlame.pl` ran a plain `git blame` with upstream cregit's `-C100`
copy detection **commented out**. Plain blame follows a whole-file rename but not
content moved *between* files, so a header split or a refactor re-credited every
token it touched. No commit in this repository commented that line out, so it
arrived that way from upstream rather than by a local choice.

`-C100` is **enabled** from 2026-09-22 (`formatBlame.pl:73`). Every project is being
re-blamed. **Status: in progress — do not mix the two attributions in one analysis.**

| projects | state |
| ---: | --- |
| 3 | re-blamed, pilot |
| 136 | re-blamed by the wave of 2026-09-22 |
| 47 | **not yet re-blamed** — they also need Rust re-tokenization, which waits on a separate fix |

Detect which one a file carries by comparing it against the snapshot at
`parquet-backups/pre-reblame/`, which holds the pre-correction attribution for all
187 projects.

**Magnitude, measured at token level.** Old attribution against new, joined on
`(file_path, token_index)`, on the first five projects of the wave. Row counts and
schema were identical in every case, so the difference is attribution alone:

| project | tokens | author changed | firm changed | top-1 author |
| --- | ---: | ---: | ---: | --- |
| `citusdata__citus` | 947,699 | **25.011%** | 14.942% | unchanged |
| `dpdk__dpdk` | 15,200,540 | **16.576%** | 6.253% | unchanged |
| `open-mpi__ompi` | 2,580,411 | 12.981% | 8.039% | unchanged |
| `kamailio__kamailio` | 5,276,523 | 12.809% | 7.641% | unchanged |
| `dolphin-emu__dolphin` | 5,758,507 | 10.877% | 3.080% | unchanged |
| `buchen__portfolio` (pilot) | — | **23.183%** | 21.3% | **changed** |
| `google__obr` (pilot) | — | 4.701% | 4.7% | unchanged |
| `renesas__rz-fsp-examples` (pilot) | — | 0.000% | 0.0% | unchanged |

So between **0% and 25%** of a project's tokens change author, and a project's
single largest author can change. `buchen__portfolio` moved from Alexander Ott to
Andreas Buchen.

**A corporate address can win an author ranking.** `dpdk__dpdk`'s top-1 author is
**`intel at intel.com`** before and after the re-blame (494k, then 691k tokens). It is
not a person. The re-blame did not cause this; the defect was already in the data. Any
per-author result needs an identity-hygiene pass first. Related: the vendoring artifact above.

**Earlier line-level exposure figures in this file were wrong and are withdrawn.**
They were measured with `git blame -C -C`, not the `-C100` the pipeline runs. Under
`-C100` the same seeded 100-file sample of the 54,075 comparable Linux files gives
**40.0%** of files with at least one line re-attributed, **12.0%** gaining an author,
and **3.0%** changing their top-1 author — not the 41% / 20% / 7% previously stated.
Use the token-level table above in preference: it measures the dataset, not a
line-level proxy for it.

The independent cross-check that first exposed the defect still stands.
`cregit.linuxsources.org` release 7.2, on `drivers/power/supply/power_supply.h` —
byte-identical content, identical history. Token totals agreed **exactly**, 357 =
357, but **114 of 357 tokens (31.9%) carried a different author**, and our top-1
author was wrong: ours Thomas Weißschuh at 61.6%, theirs Anton Vorontsov at 56.0%.
Reproduced locally on the same blob:

    git blame       : Weißschuh 52, Vorontsov 33, … 7 authors
    git blame -C -C : Weißschuh 42, Vorontsov 33, Smirnov 9, Kozlowski 1, … 9 authors

Copy detection moved 10 lines off the most recent author and introduced two authors
we omitted entirely. The mover was `44fcc479a574`, "power: supply: hwmon: move
interface to private header".

* **Consequence, for the 47 projects not yet re-blamed**: every per-author,
  per-organization and truck-factor result over them **systematically over-credits
  refactorers, file-movers and header-splitters** and under-credits original authors.
  This is a separate mechanism from the vendoring artifact above, and it moves
  attribution in the same direction.
* **Detect**: compare the project's Parquet against `parquet-backups/pre-reblame/`.
  If the two are identical, the project still carries plain blame.
* **Work around**: re-blame it. `reblame_wave.sh` does this, and it refuses any
  project that needs re-tokenization first.

**The same crash mechanism reaches published data, and the corpus-wide count is
35 files in 19 of the 186 published projects.** Re-measured 2026-09-22 with
`measure_dataset_gaps.py`, after `torvalds__linux` published and after the
near-duplicate exclusion, so the published set is now 185 S/M-class projects plus
Linux. **504,117** mask-selected regular files exist at HEAD and **35** of them
have no row, which is **0.007%**.

The arithmetic, since the script takes one manifest at a time and does not know
about `PUBLICATION_EXCLUSIONS`:

| | regular files | missing |
| --- | ---: | ---: |
| `manifest.phase1-sm.tsv`, 186 with a Parquet | 446,060 | 36 |
| less `tencent__tendbcluster-tendb`, withheld | −6,954 | −2 |
| plus `torvalds__linux` | +65,011 | +1 |
| **published** | **504,117** | **35** |

Every one of the 35 is a parser crash. No other cause contributes a single file, so
the residual is zero. The largest single project loss is
`sumatrapdfreader__sumatrapdf` with 7, including one on a 1,263-byte file, so it is
not a size limit. `elfmz__far2l`, `texasinstruments__simplelink-zephyr` (6) and
`util-linux__util-linux` (3) follow.

**Linux contributes exactly one, and it is diagnosed.**
`tools/testing/selftests/mm/protection_keys.c` (46,717 bytes, blob
`ae6e1530b354`) holds two `__attribute__` occurrences. srcML 1.1.0 exits 0 on it
without `--position` and **crashes with 139 when `--position` is given**, which is
what `tokenizeSrcMl.pl` always passes. The result is a **zero-byte `.blame`** that
no counter records — the file is not on the denylist and the run reported
`blobsParserCrashed=0`. Measured against a source-tip build, it parses cleanly
both ways, so this file is in the recoverable class.

The 36 blobs from the earlier sweep are on the blob denylist by sha, so a re-run
reports them instead of losing them. This Linux blob is **not** yet listed.

**A file absent from the dataset is not, by itself, a gap.** Of the paths that a
naive detector reports as missing, **354 are symlinks or submodule gitlinks**, not
source. `powerdns__pdns` alone carries **331** masked symlinks in
`pdns/dnsdistdist/`; counting them makes its Parquet look 29% incomplete when it
is complete, because the Parquet holds exactly its 806 regular files.
`nvidia__opensma` (10) and `facebookincubator__qemu-wearables` (4) are the same
artifact. Over `manifest.phase1-sm.tsv` the naive count is 390 and the true count is
36; over the published set it is **417 naive and 35 true**, because Linux adds 28
more masked symlinks and gitlinks and one real gap.

* **Detect**: a path that exists at HEAD, matches the file mask, **and is mode
  100644 or 100755**, and has no row in the Parquet. Read the mode: use
  `git ls-tree -r HEAD` and drop 120000 (symlink) and 160000 (gitlink). Do **not**
  use `ls-tree -r --name-only`, which cannot distinguish them.
* **The 35 are HEAD only.** A historical revision that crashed is not visible to
  this measurement, because the detector compares against HEAD. The denylist
  covers **197** historical blobs of these same 36 paths, so history is worse than
  HEAD by at least that much. No corpus-wide historical count exists.
* **A silently empty tokenization contributes nothing at HEAD.** The separate
  mechanism below — a mask-selected file that tokenizes to an empty result with a
  zero exit status — accounts for **0** of the 35. Linux's one gap is a signaled crash, not this mechanism. It is unmeasured for historical
  revisions.
* **Work around**: none from the data. The file's tokens do not exist in the dataset.

---

**Status, 2026-10-03: fixed in the Parquets, except the attribution (next entry).**
cregit PR #97 (`759f8be`) removes the prefix in step 10, before the source walk, and
`ctp.py run --from-step 10` rebuilt the 45 corpus projects with Rust files on
2026-10-03. It removed **42,132,664** prefixes; that count also holds the `-:-`
unit-marker rows. The text below describes the Parquets before that rerun.
`backfill_rust_tokens.py` is no longer needed for them. The pilot `rustlings`,
outside the corpus, was not rebuilt.

**On every `.rs` row, four columns are wrong.** Two faults in the Rust tokenizer
combine. First, it separates a token's position from its type with a **tab**, while
`generate_dataset.py:243` splits on a **pipe**. Second, it discarded the
`--position` flag and then emitted the position prefix anyway, so it prefixed every
line even though the pipeline never asks for positions. The whole
`line:col<TAB>type` string therefore lands in `token_type` and every later field
shifts. Consequences, measured on `intel__tsffs` (201,030 rows, 145,125 of them
`.rs`):

| | `.rs` rows | other rows |
| --- | ---: | ---: |
| `is_structural = 1` | 64 (**0.044%**) | 32,993 (59.0%) |
| `token_value` found inside `source_text` | **2.3%** | 40.2% |
| position inside `token_type` agrees with `source_line`/`source_col` | **0 of 144,997** | n/a |

So `token_type` is malformed, `source_line`, `source_col` and `source_text` are
wrong, and `is_structural` is effectively dead for Rust. This is a property of the
tokenizer, not of one project, so it applies to **every `.rs` row in the corpus** —
including `.rs` files inside projects whose primary language is not Rust.

`source_line` and `source_col` are wrong by a **different mechanism**, and the
distinction matters when you repair them. They are not shifted fields: nothing ever
parses a `line:col` string into them. Both come from a cursor walking the original
source, read at `generate_dataset.py:174` through `reader.location()`, and they are
never NULL. They are wrong because that cursor desynchronized — the token consumer
skipped the length of `line:col<TAB>type` instead of the length of the token. So a
repair substitutes the tokenizer's own embedded position for a corrupted cursor
reading, rather than un-shifting a field.

* **Detect**: `file_path LIKE '%.rs'`. Do **not** use
  `token_type LIKE '%' || chr(9) || '%'` alone. It misses the end-of-unit rows: the
  line `-:-<TAB>end_unit` holds no pipe, so it fails the parse at
  `generate_dataset.py:243`, lands in the `unknown` branch, and the tab ends up in
  **`token_value`** while `token_type` reads a clean `unknown`. Test both columns.
* **Work around**: `split_part(token_type, chr(9), 1)` recovers the true
  `line:col` and `split_part(token_type, chr(9), 2)` the real token type; skip the
  `-:-` end-of-unit marker. **`token_value` is correct** on content rows.
  `source_text` is not recoverable from the Parquet.
* **`backfill_rust_tokens.py` does this repair**, fails closed on a non-contract
  schema, and is idempotent. It leaves `source_text` wrong on content rows on
  purpose, and sets it to `''` on structural rows only, because every structural
  branch of `classify_and_skip` writes `''` there — so `is_structural = 1` implies
  `source_text = ''` in correct data, and repairing one without the other would
  create a new inconsistency.
* **`is_structural` becomes correct, not useful.** After the repair a Rust file has
  about 3 structural rows, against roughly 59% of rows in a C file, because the Rust
  tokenizer emits no `begin_*`/`end_*` tag markers, no `blank` and no `DECL`. A
  correct re-run gives the same small number. Do not read "repaired" as "structural
  analysis now works for Rust".
* **A repair in place cannot match a re-run.** `token_type`, `source_line` and
  `source_col` are backfillable by the rule above, and `is_structural` follows from
  the repaired `token_type`. Two things are not. `source_text` desynchronizes from
  the first row onward, because the tokenizer misread `begin_unit` as an ordinary
  token and consumed source past it. And a correct tokenization emits **one more row
  per `.rs` file** — the trailing end-of-unit marker, which every C file already
  had. So the published rows are not row-aligned with a correct run, and only a
  re-run of the Rust-bearing projects produces a comparable file.
* Anyone grouping on `token_type` or filtering on `is_structural` without this
  gets silently wrong answers for all of Rust.
* **Corpus-wide magnitude, measured 2026-10-02:** **42,147,426** tokens (4.2% of
  1,003,142,740) in **46** projects hold the `line:col<TAB>` prefix in
  `token_type` in the Parquets before the 2026-10-03 rerun. Example:
  `ankitects__anki`, 528,676 of 529,083 `.rs` rows. Per-project counts:
  `ellians-master/.../mojibake-census/data/token_type_prefixed.tsv`.

**Status, 2026-10-04: fixed in the 45 dataset projects with Rust files.**
`ctp.py run --from-step 2 --retokenize rs` tokenized their `.rs` blobs again with
the current tokenizer, folded the repositories again, and blamed every file again
with `-C100` (cregit `759f8be`, `c9e4d74` and `e411e7a`). The run also covered
`elastic__elasticsearch` and `oceanbase__oceanbase`, whose `.rs` files exist only in
history. After it, no `.rs` file at HEAD of the 47 tokenized repositories holds a
`line:col` prefix. 22 of the projects had blob maps built with the old mask `\.rs$`,
so a `--mask-widened` run moved them to the universal mask first. Until then their
`file_mask` column named the universal mask, but their tokens came from `\.rs$`; the
column is now true. The text below describes the Parquets before these runs.

**The author of a `.rs` token is often the commit that moved it, not the commit that
wrote it.** This is open, and the step-10 rerun does not fix it. The 45 corpus
projects with Rust files were tokenized before cregit `729643e` (2026-09-20), which
removed the prefix from the tokenizer. So the tokenized file that `git blame` reads
holds the `line:col` of every token. An edit above a token changes its line number,
so its token line changes, and blame credits the commit that made the edit. The
current tokenizer binary writes no prefix.

| commit in `intel__tsffs` `simics-fuzz/src/fuzzer/mod.rs` | source lines | token lines |
| --- | ---: | ---: |
| `0477d35` | +13 / -3 | +3,420 / -3,336 |
| `0d618e6` | +5 / -4 | +3,408 / -3,394 |
| `b5badd4` | +39 / -10 | +5,058 / -4,901 |

* **Affected columns**: the commit, person and firm columns of the `.rs` rows in the
  45 projects (42.1 M tokens). Rows of other files in those projects are correct.
* **Detect**: `file_path LIKE '%.rs'` in a project with a row in
  `token_type_prefixed.tsv`. For one commit, compare the numstat of the tokenized
  repository with the numstat of the original (`commitmap` in `<name>-cregit.db`).
* **Work around**: none in the Parquet. Re-tokenize the `.rs` files with the current
  binary: `ctp.py run --from-step 2 --retokenize rs`. That refolds the project and
  blames it again. None of the 45 projects has the copy-detection re-blame yet, so
  this run also gives them `-C100`.

**Status, 2026-10-03: fixed.** cregit PR #97 reads the source as `utf-8-sig`, and
the step-10 rerun of 2026-10-03 rebuilt the 25 projects: **587** files with a BOM.
The census below counted 579. The text below describes the Parquets before that
rerun.

**Every file that starts with a byte-order mark has wrong positions after the first
token.** `generate_dataset.py:302` opens the source with `encoding="utf-8"`, not
`"utf-8-sig"`. So the byte-order mark (BOM, U+FEFF) stays in the text as one
character, but the tokenizer drops it. The source cursor therefore runs one
character ahead, from the first token to the end of the file.

| | files | tokens | projects |
| --- | ---: | ---: | ---: |
| files that start with a BOM, measured 2026-10-02 | 579 | 811,391 | 25 |

`zlmediakit__zlmediakit` holds most of it: 419 of its 444 files start with a BOM,
460,325 tokens. In those files, **0.0%** of name and literal rows have a
`source_text` that starts with the token itself, against 16.2% in the project's
other files. Example, `3rdpart/assert.h`: the `#` of `#ifndef` is stored at line 9,
column 3, with `source_text` `'/\n\n'`.

* **Detect**: a file whose first content row has `source_text` that starts with
  `chr(65279)`.
* **Affected columns**: `source_line`, `source_col`, `source_text`. `token_value`,
  `token_type` and the attribution columns are correct.
* **Work around**: none in a Parquet from before 2026-10-03. Re-run step 10 with
  cregit PR #97. That needs no re-tokenization and no re-blame.
* Per-project counts: `ellians-master/.../mojibake-census/data/bom_per_project.tsv`.

**Status, 2026-10-03: fixed in the 165 corpus projects.** cregit PR #97 repairs the
token text in step 10, before the source walk, with the skip rule below, and the
step-10 rerun of 2026-10-03 rebuilt the 136 affected projects of the 165. It
repaired **422,789** tokens. The census below counts 422,806 tokens that pass the
skip rule in those 165 projects. The census also covers 9 pilot and backup folders
outside the corpus, which were not rebuilt. The text below describes the Parquets
before that rerun.

**Non-ASCII text in C, C++ and Java tokens is double-encoded (mojibake).** The
prebuilt srcML 1.1.0 binary that tokenized the corpus reads UTF-8 bytes as
Latin-1. So the source text `Högskolan` becomes the token text `HÃ¶gskolan`. This
is not the parser warning only: the stored text is wrong. Rust files are not
affected, because their tokenizer writes correct UTF-8.

| | all 197 projects | the 139 re-blamed projects |
| --- | ---: | ---: |
| projects with at least one bad token | 144 | 116 |
| tokens with wrong text | 424,866 (0.042%) | 91,084 (0.012%) |
| files with at least one bad token | 22,613 (4.36%) | 13,852 (3.71%) |
| tokens with **wrong positions** | **61,720,850 (6.15%)** | **33,210,328 (4.40%)** |

The wrong text is rare, but each bad character also moves the source cursor one to
three characters ahead. So every later token in the file gets a wrong
`source_line`, `source_col` and `source_text`, up to the end of the file. That is
why the position count is 145 times the text count. One project,
`oceanbase__oceanbase`, holds 300,362 of the 424,866 tokens with wrong text (71%).
The bad tokens are string literals (75.2%) and comments (24.7%). Only 400 are
identifiers (0.09%).

The rule is exact. On `jq`, the regular expression `[\x{C2}-\x{F4}][\x{80}-\x{BF}]`
matches exactly the 41 tokenized blobs with a non-ASCII byte, with no miss and no
extra. Over the corpus, 29 matches are false positives: the repository itself holds
mojibake there (22 in `fastled__fastled` files with a BOM, 7 in Rust files).

* **Detect**: `regexp_matches(token_value, '[\x{C2}-\x{F4}][\x{80}-\x{BF}]')` for
  the text. For the positions, every row after the first such token in the same
  `file_path`.
* **Affected columns**: `token_value` on the matched rows. `source_line`,
  `source_col` and `source_text` on every later row of the file. Attribution
  should not change, because the repair maps each line one to one. This is not
  tested: copy detection weighs bytes, so a score near its threshold can move.
* **Work around, text only**: `token_value.encode("latin-1").decode("utf-8")`
  repairs 424,845 of the 424,866 tokens. The 21 failures are 12 tokens with
  characters above U+00FF and 9 from sources that are not UTF-8. This does not fix
  the positions.
* **Work around, text and positions**: repair the token text inside step 10,
  before the source walk, and skip `.rs` files, files with a BOM, and files that
  are not strict UTF-8. A replay of the step 10 reader on 80 files from 69 projects
  gave correct text and positions for every token outside `.rs` files, and 290,969
  mismatch warnings fell to 0. Over the corpus, 424,821 tokens pass the skip rule and all repair
  cleanly. `ctp.py run --from-step 10` re-uses the blame on disk, so it needs no
  re-blame. cregit PR #97 implements this repair, and it ran on 2026-10-03 (see the
  status above).
* **The cause is gone on `master`.** cregit PR #93 builds srcML from source
  (revision `8a3a629d`), and that build writes correct UTF-8. So a run from
  `master` does not reproduce the published tokens, and it changes every file with a
  non-ASCII byte.
* Method, scripts and per-project counts:
  `ellians-master/.../sep-25-presentation/mojibake-census/`.

**Publishing the Parquet as it stands publishes contributors' e-mail addresses.**
`person_email` (column 50) is a real address. `anonymize_parquet.py` was the
release path of the pilot run (the release of both commit layers used `pseudonymize.py`
with a salt; see the `commit_summary` entry above): it rewrites the e-mail local part to `author_<token>` and names to
`Author <token>`, running every identity string — including each element of the 15
trailer arrays — through one registry, so a person carries one pseudonym
everywhere. It classifies **every** input column into pass-through, transform or
drop and raises on a column it does not recognize, so a schema change stops it
loudly instead of silently shrinking the release. It also checks its own output:
row count, `firm`/`repo_name` group counts and every `person_domain` must be
unchanged, and `count(distinct person_email)` and `count(distinct person_name)`
must not drop — two real people collapsing onto one pseudonym would lower a
distinct-contributor count for free.

**A pseudonym of `anonymize_parquet.py` (not of the release tool) is a salted
keyed hash, and the salt is a secret you now have to keep.** The token is `blake2b(space || NUL || lowercased value, key = salt)` taken
to 14 lowercase hex characters. Four consequences to plan a release around:

* **Pseudonyms are stable across releases.** The token is a function of the value
  and the salt, not of the set of files in the invocation, so a release that adds
  or drops a project renumbers nobody and two releases diff cleanly. A reader can
  follow one contributor from one release to the next. It remains true that
  passing every file that will be published together in one command is the right
  habit — but now only because it lets the leak scan cross-check them, not
  because the pseudonyms depend on it.
* **The salt must exist, must be kept, and must never be committed.** It is read
  from `--salt-file`, `$CTP_ANON_SALT_FILE`, `$CTP_ANON_SALT` or
  `~/.config/cregit-token-pipeline/anon-salt`, in that order. A missing salt, or
  one under 32 bytes, exits **2** and prints the remedy; nothing ever generates
  one, because a salt generated per run would renumber everybody on every run and
  nothing in the output would say so. `.gitignore` carries salt patterns as a
  second line of defense. **Losing the salt is unrecoverable**: no future release
  can be linked to a published one. Back it up wherever the release is backed up.
* **Rotating the salt renumbers everybody.** That is the cost and it is the whole
  cost. Rotate only on a suspected leak or a pseudonym collision, and expect the
  next release to be unlinkable to every release before it.
* **A collision fails the run.** 14 hex characters is 56 bits. At 28,339 distinct
  addresses that is n(n−1)/2 = 401,535,291 pairs over 2^56, a probability of
  about **5.6 × 10⁻⁹**. If it ever happens the run raises `CollisionError` and
  stops, rather than merging two contributors and silently lowering the
  distinct-person count.

**The reverse mapping is never published.** Real value → pseudonym exists in
memory for the length of one run and is written nowhere; the `--report` JSON
carries counts and shapes only. So the release is **not** reversible from public
inputs. That is a change of kind, not of degree: under the superseded sequential
scheme the mapping was a pure function of the input values, and the inputs derive
from *public* GitHub repositories, so anyone who could rebuild the same file set
could rebuild the whole mapping with no salt, no key and no leaked file. The
honest reading of the salted scheme is narrower than "anonymous": an adversary who
obtains the salt can still rebuild the mapping by hashing candidate addresses, and
that leak would be silent. The salt, not the hash, is the protection.

**Why this replaced sequential ids, and what the 98.0% figure now means.** Until
this change an id was the ordinal position of a value in the sorted distinct set
of one invocation. Determinism and stability are not the same property, and that
scheme had only the first: output was deterministic for a fixed input set and was
not stable when the set changed. Measured over six corpus Parquets, dropping one
file from the invocation renumbered **150 of the 153** addresses that appeared in
both runs — **98.0%**. A release that added one project renumbered almost every
pseudonym, and a reader could not track one contributor across two such releases.
That is the measured reason the salted hash was adopted. **It describes the
superseded design, not the current one**: the same experiment now renumbers
**0 of 153 — 0.0%**, and the five shared output Parquets come out byte-identical
between a five-file and a six-file release.

**Nothing runs the anonymizer automatically, and the Parquets on disk are not
anonymized.** `anonymize_parquet.py` has **no caller anywhere in the pipeline**:
it appears in no step of `run_pipeline_process.sh`, and neither `ctp.py` nor
`consolidate.py` invokes it. Step 10 writes
`<work>/<repo>-dataset.parquet` through `generate_dataset.py` and stops there. So
every per-project Parquet in the corpus holds real addresses and real names, and
the release is a separate manual command. Two consequences. A step-10 re-run
**cannot** undo pseudonymization, because the two never touch the same file — a
re-tokenization wave needs no anonymizer precaution. And `OUTDIR` is a mandatory
positional with **no same-file guard**, so naming a project's own work directory
as the destination would overwrite the raw Parquet with the pseudonymized one and
destroy the only copy of the identities. Always write to a fresh directory.

Run `verify_anon.py` over the output directory as an independent check: it tests
the published files alone for any e-mail local part that is not a pseudonym,
**needs no salt and no secret**, and so can be run by a reviewer or a depositor.
Moving to a hash cost it one widening, `author_\d+` to `author_[0-9a-f]+`; a
release made under either scheme still passes it. It is a necessary, not a
sufficient, condition. Two residual risks survive
anonymization by design and must be disclosed by any analysis: the e-mail
**domain is preserved on purpose** (it is the firm signal), so a sole contributor
at a rare or vanity domain is re-identifiable — see the next paragraph for how
large that tail is; and `owner`/`repo_name`/`clone_url` carry the GitHub
namespace, which for a personal repository is a
person's handle. `source_text` and `token_value` are source code and are not
scrubbed, so copyright headers and Javadoc author tags pass through.

**84.7% of the corpus's `person_domain` values carry exactly one contributor, and
the project publishes anyway.** Combined with the preserved domain above, a
single-contributor domain narrows a pseudonym to one identifiable person whenever
the domain is rare or personal. This is stated as the project's position, not as a
mitigation: the corpus is drawn largely from small projects, and small projects are
by their nature composed of single-contributor domains, so a threshold that
suppressed them would remove the population the dataset exists to describe. The
share is therefore disclosed rather than reduced. Any analysis that reports per-domain
results must treat a single-contributor domain as identifying, and any re-publication
that cannot accept that should aggregate domains before release.

**84.7% of published domains have exactly one contributor, and this release ships
anyway.** Measured over all 186 conforming corpus files and 28,339 distinct
addresses: **5,276 of 6,228** `person_domain` groups hold exactly one distinct
address. An earlier three-file sample gave 33 of 38 and hedged that the ratio was
"inflated by the small sample". It was not — the ratio did not fall with scale.
The project's position, stated plainly so a reader is not left to infer it: the
corpus is mostly **small samples**, and a small sample is itself composed of
single-contributor projects, so a large single-contributor tail is what this
corpus is, not an artifact to be corrected. The domain is published because firm
attribution resolves from it and because a domain is not personal data; the
pseudonym protects the local part and nothing protects the pairing of a rare
domain with a repository. **No k-anonymity work is being done**, now or planned:
suppressing or generalizing domains below a threshold would remove exactly the
long tail of small firms the corporate-truck-factor question is about. This is
therefore a **disclosed limitation, not a mitigation**. Any paper using this
output must carry the 84.7% figure and say that a sole contributor at a rare or
vanity domain (`gutwin.org`, `guerra.sh`, `haamer.ee`, `lukapeschke.com`,
`push-f.com`) is re-identifiable from the published file alone.

**Every `.h` file in the corpus was parsed with the C grammar.** The tokenizer's
language table maps `h` to `C`, so C++ declared in a `.h` header — extremely
common — is tokenized as C. The table also has only lowercase keys and both gates
lowercase the extension, so `.C` and `.H` resolve to C as well, although `.C` is
conventionally a C++ source file. This decides the grammar every header in the
dataset was tokenized with, and it is not visible in any column.

**Two files with identical content share one tokenizer cache slot, even in
different languages — and the measured effect is confined to C against C++.**
`tokenizeByBlobId/tokenBySha.pl` keys the memo on `sha1_hex($contents)` alone. It
derives the extension afterwards and uses it only to pick `--language` and to name
the temporary input file, and it validates nothing when it reads a cached entry
back. So whichever of two identical-content files is tokenized first decides the
grammar for both. With parallel workers, which file is first depends on the schedule, so
the grammar of such a file can change from one run to the next, and a rerun is not
byte-identical for it (test of the tool review of 2026-10-28). Dataset v1 is not measured.

Measured over all 188 projects, from `blob_map`, whose key is `(orig_blob, path)`:
identical content means an identical blob sha, so an affected blob is exactly one
whose paths span more than one language. Identity rows are excluded, since a blob
that was never tokenized never reached the memo.

| | count |
| --- | ---: |
| tokenized distinct blobs | 11,159,105 |
| blobs whose paths span two languages | **1,395 (0.0125%)** |
| projects affected | 33 of 188 |
| language pairs: C with C++ | 1,393 |
| language pairs: anything else | **2, and both are the empty file** |

The two exceptions are `e69de29bb2d1…`, git's empty blob, which is not a
meaningful token stream in any grammar. So **every case involving real content is
C against C++** — a `foo.c`/`foo.cpp` or `Bar.h`/`Bar.hpp` twin — which is the
same language-family confusion the `.h` entry above already describes. Worst
affected project by rate is `cgdb__cgdb`, 66 of 3,404 tokenized blobs (1.9%);
worst by count is `grpc__grpc` at 616.

The defect is real and no test covers it, so a future corpus with a wider language
mask could be hurt much more. In this corpus it changes no result that the `.h`
entry does not already qualify. Reproduce with
`/local/home/ellianco/tmp/memo-collision-corpus.py`.

**A file can be excluded from a project with no row and no marker in the
Parquet.** Three mechanisms remove a blob from the tokenized repository entirely,
so it yields no blame line and therefore no dataset row:

| Mechanism | Blocks publication? |
| --- | --- |
| blob denylist, keyed by **git blob sha** — a shipped data file in the cregit repo, so an exclusion can be cited | no |
| blob over JGit's 50 MiB stream-file threshold | no |
| tokenizer timeout (the parser fails to terminate on a construct) | in the pilot run, **yes** — the project could not complete and got no Parquet. In the cregit commits of dataset v1 (`157cf8b` and later): no, blobExec excludes the blob after 600 s of wall time |

Only the third is visible from outside, as a missing project. The counters count
`(sha, path)` pairs rather than files, so a count of 4 can be one file at two
paths across two revisions. Separately, a mask-selected file can tokenize to an
**empty** result with a zero exit status and produce zero rows silently. At HEAD
that mechanism accounts for **0 files**: all 36 HEAD files absent from the 186
published Parquets are parser crashes, measured 2026-09-20. Its count over
historical revisions is still unmeasured.

**An excluded blob also moves the credit of the history.** The text above is true only
for the head. blobExec (`Walker.scala`, `assemble`) drops an excluded blob from its
commit in the tokenized history. The next commit that has the file adds the whole file
again, so `git blame -C100` gives every line of that revision to that commit. A test of
the tool review of 2026-10-28 (one revision of a file with 31 lines times out) gave all
31 lines to the next commit. The causes are the denylist, a blob over 50 MiB, a parser
crash, a tokenizer error and the tokenizer time limit. Only `pipeline.log` records an
exclusion (the `EXCLUDED` lines and the counters); the release has no list. The counters
are 0 in 71 local logs; the logs of the cloud machines were not read. The time limit is
600 s of wall time with one worker for each processor, so an exclusion can depend on the
load of the machine.

**The denylist excludes 12 blobs that the patched srcML can tokenize.**
`BlobDenylistEntries.scala` (the same file from `157cf8b` to `14b8db4`) lists 8 OpenJDK
langtools test files (Java, srcML issue 2361) and 4 `.h` files with C++ code (one is
`eden/fs/utils/StatTimes.h`). They do not terminate with srcML 1.1.0. The patched srcML
of dataset v1 finishes on all 12 (test of the tool review of 2026-10-28), but blobExec
still excludes them, with the effect of the previous entry. Projects of the JDK family
(about 35, not counted) and probably `facebook__sapling` hold these blobs.

**A fourth mechanism removes a path with no row, and it is not a defect.** A
symlink or a submodule gitlink whose name matches the mask is not source and is
never tokenized. There are **354** of them at HEAD, **331** in `powerdns__pdns`
alone. Any completeness check must read the git mode, not the file name.

**One of the 188 run-set projects has no Parquet:** `tencent__tencentkona-21`.
So 187 Parquets exist, all conforming to the 70-column contract, and **186 of them
are published** — `tencent__tendbcluster-tendb` has a conforming Parquet that is
withheld as a near-duplicate (§ above). Verify with `validate_schema.py` over the
output directory rather than assuming a project is present, and read published
membership from `projects.excluded_because` rather than from the presence of a file.
`consolidate.py` reports both numbers: `DONE: 187` with `tokens view: ... across 186
projects` and the exclusion named beneath.

**`firm` is empty on rows with no attribution and ambiguous on the rest.** A
project only carries firm attribution if it was generated with `--firm-map`;
otherwise all three columns are empty strings, which is also what "this domain is
not in the map" looks like. From columns 52–54 alone a consumer **cannot
distinguish** the two. And `(Independent)` is a category, not a firm ([`DATASET-SCHEMA.md`](DATASET-SCHEMA.md) §3.4), so
`firm <> ''` is not a count of firm-attributed rows.

**Nothing in a Parquet records which revision of the firm map produced its firm
columns.** The map and the canonical table are versioned in git and have been
revised since the corpus was generated, so the values in columns 52–54 may not be
what a rebuild from the current `data/` would produce. Record the revision of
`data/affiliation.merged.csv` and `data/firm_canonical.csv` alongside any
published Parquet.

**The firm map has structural defects that survive the reviewed corrections.**
All are visible in `data/affiliation.merged.csv`:

* **No subdomain fallback.** `renesas.com` maps to Renesas; `dm.renesas.com` is
  absent from the map and therefore unattributed.
* **Sibling domains of one provider are treated inconsistently.** `163.com` maps
  to a named firm but `126.com` to `(Independent)`; `sina.com` to a named firm but
  `sina.cn` to `(Independent)`.
* **`hp.com` and `hpe.com` both map to `HP`**, although `data/firm_canonical.csv`
  deliberately refuses to merge `HP` and `Hewlett` because HP Inc. and HPE split
  in 2015.
* **Two single-person inferences are wrong on their face and are not corrected:**
  `central-intelligence.agency`→`Microsoft` and
  `intellisys.info`→`Takeaway.com`. A row-by-row audit of the 2,761
  single-person inferences has not been done.
* **The canonical join ignores the `decision` column.** It is safe at this
  revision — no `keep` row has `firm <> firm_raw` — but editing a rejected row's
  `firm` would silently apply the merge the review refused.

**Token count is not a measure of human contribution.** The mask retains
generated C/C++ sources — bitmap-font arrays, constant tables, embedded binary
blobs rendered as arrays — and a single generated file can contribute millions of
rows attributed to whoever committed it. Such files also have almost no
`is_structural` tokens, because they are flat literal arrays with little syntax.
Any firm or person ranking by raw row count is partly a ranking of generated
data. Weight by commits or people, or exclude outsized files, before drawing a
conclusion.

**Measured, the worst case found so far.** In `moarvm__moarvm`,
`src/strings/unicode_db.c` — a generated Unicode property table, 23 MB and
1,666,007 lines — contributes **1,666,007 of the project's 3,048,097 rows, 54.7%**.
One generated file is the majority of that project's dataset. Detect the pattern
with a per-file row count, and check it before reading any project-level author or
firm share:

```sql
SELECT file_path, COUNT(*) n
FROM read_parquet('<slug>-dataset.parquet')
GROUP BY 1 ORDER BY n DESC LIMIT 10;
```

**Seven columns carry no information and five more are constant.** `repo_tag` is
**structurally** empty, not accidentally: it comes from the multi-repository
import tag, which only cregit's Linux-specific import populates. `contested` is
empty for every row, as the stratum entry below explains. Five trailer arrays —
`footer_suggested_by`, `footer_based_on_patch_by`, `footer_helped_by`,
`footer_mentored_by`, `footer_thanks_to` — are empty throughout, and the trailer
block as a whole is dominated by `footer_co_authored_by`: the
`Signed-off-by`/`Reviewed-by`/`Acked-by` culture is largely absent from a
GitHub-workflow corpus, so trailers cannot serve as a second attribution channel
for most rows. Check for a non-empty value before building on any of these.

**`footer_personids` and `footer_person_names` are not reconstructible from the
13 typed trailer columns.** They resolve people across *every* trailer key in the
commit, including keys that are not published as columns (`Source-Author`,
`Committer`), so a row can carry a resolved person id with all 13 typed lists
empty. Trailer values also sometimes carry a stray carriage return, which will
split otherwise-identical values in a `GROUP BY`.

**Some tokenizable source is deliberately excluded, and nothing in the output
says so.** `.ixx`, `.inl`, `.cppm`, `.cxxm` and `.ipp` are left out of the mask
because srcML 1.1.0 does not recognize them, ignores `-l C++`, and then writes an
**empty token file and exits 0** — a silent success. Autotools input (`.am`,
`.ac`, `.m4`) is left out for the measured reasons in the next paragraph. C++
module and inline-implementation files and autotools source are therefore absent
from the dataset with nothing in a Parquet to indicate the absence.
`tests/test_mask_drift.py` holds the extension list to the tokenizer's own
language table, so adding an extension without probing it is caught.

**M4 / autotools input stays out of the mask. Ratified, with the measurement.**
The decision was taken against numbers, not against a preference, and the numbers
are these. Driving `generate_dataset.py`'s own `SourceReader` and
`classify_and_skip` over the m4 token streams of all 525 routed m4 files in the
corpus, **98,265 of 279,031 tokens — 35.2% — reconstruct the wrong source text**,
against **0 of 10 for the srcML C baseline**. `source_line` and `source_col` are
wrong from the first bad token to the end of the file. Root cause, one line:
`m4Tokenizer/m4.py:192-194` strips the quote delimiters out of every `string`
token value where the srcML path keeps them, so the consumer consumes two
characters too few per string token and never re-aligns. This is the **same
corruption class as the shipped Rust defect**, arriving through a different door.

**State this plainly so the verdict is not mistaken for a framing failure: the
framing test PASSED.** Under the real pipeline flags the m4 route is pipe-framed
and agrees with the srcML path; the tab-framed branch in `m4.py:311` is dead, and
`--position` is not even a valid option there. The exclusion rests on the 35.2%
desynchronization, which is a stronger and different measurement.

Three further faults found by the same sweep, and one blocker:

* `m4.py:105-106` sets the closing quote to a backtick, so a backtick opens a
  string that runs to the next backtick anywhere in the file. 749 multi-line
  records in 6 files across 5 projects are rejected outright by the consumer's
  regex. Setting it to an apostrophe is not the fix — shell backticks in
  `Makefile.am` then never close.
* `m4.py:183` has a bare `next()` inside a generator, which raises
  `RuntimeError: generator raised StopIteration` on an unterminated string at
  EOF. **2 of 525 files die outright** (`erikd__libsndfile`,
  `java-native-access__jna`), taking their projects down at step 2.
* `.m4` has **no dispatch route in the mask at all** — it is absent from
  `%EXT_LANG`, and `tokenizeByBlobId/tokenBySha.pl:96-98` dies with
  `unknown file extension` on the first `.m4` blob. Widening the mask to
  `\.m4$` without also adding `'m4' => 'M4'` would kill every affected project.
  This matters because `.m4` is the *largest* body of m4 in the corpus: 514 files
  and 4.38 MB, against 525 files and 1.64 MB for the routable `.ac`/`.am`.

One correction to the figure this document previously carried: the routable
autotools population is **1.64 MB across 525 files in 25 projects**, not 1.4 MB
across 22. The 22 is the `.ac`-only project count. Including `.m4` as well would
make it 1,039 files and 6.02 MB, and would cost 38,017 historical blobs of
re-tokenization, because a mask change invalidates the recorded mask string and
restarts each project rather than resuming it.

Full evidence, including the per-project breakdown and every reproduction script:
`/local/home/ellianco/tmp/m4-probe/m4-decision.md`.

**Runs are not pinned to a commit.** The manifest carries exactly five fields and
three parsers plus four tests assert their positions, so the planned
`pinned_commit` sixth field does not exist. A re-run analyzes whatever HEAD is at
clone time, so the dataset is reproducible in method but not byte-for-byte, and a
published Parquet cannot cite the revision it was built from.

**`data/affiliation.merged.csv` is a build artifact.** Never hand-edit it: put a
fix in `data/affiliation.corrections.csv`, which `build_domain_map.py` applies
last, and rerun `./build_domain_map.py build`. A hand edit is lost on the next
rebuild; a correction survives it.

**`community` is a residual stratum, and `contested` is empty for every row.**
Three of the five control facts in `docs/CODEBOOK.md` are not implemented, so a
project that no namespace fact reaches stays `community` by default.
`contested` (column 7) is non-empty in **0 of 24,405** candidate rows, so the
contested-case protocol in `docs/CODEBOOK.md` §6 has not flagged anything and the
column cannot currently be used to find disputed labels. `docs/CODEBOOK.md` §9 is
the full gap list with a worked example per gap;
`docs/SPINELLIS-VALIDATION.md` §4 names seven eligible rows that an independent
published registry attests to a company and that this pipeline nonetheless labels
`community`.

**GitHub's `fork` flag finds nothing.** Among eligible rows `fork = True` counts
zero, because a tree pushed as an independent repository is not marked. Projects
that carry another project's history are therefore **kept and flagged**, not
excluded: filter on `history_cluster` for one project per history, and read
`history_first` for origin — `history_relation` is about inclusion, and the same
topology occurs with the origin on either side.

**A stratum is a label at a date.** Namespaces get donated, so two rows for one
repository can disagree: of the 196 duplicate `clone_url` groups in
`candidates.csv`, **28 disagree on `stratum`** — **5** of them within the 106
groups that fall inside the eligible frame. `label_date` (column 8) records when
the label was assigned, and there is no relicensing time-boxing anywhere in the
pipeline (`docs/CODEBOOK.md` §7), so a project that changed license or owner
mid-history carries one label for all of it.

**`manifest_category` (column 29) has a fourth value outside the stratum
vocabulary.** The four legacy pilot projects in `manifest.tsv` carry
`enterprise`, which is not one of `community`/`company-owned`/`foundation`. Those
four are also still at an older 23-column schema, so `consolidate.py`'s schema
gate excludes them from the `tokens` view and names them in its summary. Group on
`stratum` (column 5), not on `manifest_category`.

**A `.validated` stamp does not certify the schema.** `validate.py` is the only
gate the run invokes, and it checks size and row count only ([`DATASET-SCHEMA.md`](DATASET-SCHEMA.md) §2.1). Run
`validate_schema.py` yourself over the output. Likewise `state = 'DONE'` in
`ctp.duckdb` means the pipeline finished, not that the data is present: such a
project is flagged `parquet_missing` and stays out of the `tokens` view, so
`select name from projects where parquet_missing` finds it.

**Dates are strings in UTC with no offset.** `author_date` and `committer_date` (43,
46) are not git strings. slickgitlog (`gitLogToDb.scala`, line 159 at `57551d7`) formats
them with `SimpleDateFormat("yyyy-MM-dd HH:mm:ss")` and no time zone: the wall time in the
default time zone of the JVM, with the UTC offset lost. Neither devenv nor the runner sets
`TZ`. The run machines used UTC, so the values are UTC. A rebuild on a machine in another
time zone gives other values: set `TZ=UTC`. A consumer must cast before any temporal
query.

**Corpus-level firm and org rollups do not exist.** The firm columns are per
token; there is no aggregated firm table, no package step and no dataset card
generator. `docs/DESIGN.md` §7 is the intended shape of those.

**Reproducibility depends on an unpinned sibling checkout.** The per-project
pipeline, the tokenizer and their flag names live in the cregit checkout that
`pipeline.cfg` names, at whatever revision it happens to be. A flag rename there
has already broken a run mid-corpus. Record that checkout's revision alongside
any published Parquet.
