# Known limitations

What is wrong with the dataset, ordered by how likely it is to change a result.
Read this before analysing the data. [`DATASET-SCHEMA.md`](DATASET-SCHEMA.md) is
the column reference.

Every entry states the defect, its measured magnitude where one exists, how to
detect it in the data, and the workaround if there is one. An entry with no
magnitude says so rather than guessing.

---

**One drawn project is excluded from the dataset: `tencent__tencentkona-21`.** It was
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

Any per-organisation or per-author aggregate over a project that vendors its
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

`-C100` is now **enabled** (`formatBlame.pl:73`), and a project is re-blamed by
running it through `reblame_wave.sh` (see [`REBLAME-C100.md`](REBLAME-C100.md)
§8). A corpus can hold projects re-blamed at different times, so **two
attributions can coexist. Do not mix them in one analysis.** A project needing
Rust re-tokenization needs that fix first, since a re-blame alone cannot reach
it.

Detect which one a file carries by comparing it against the snapshot at
`parquet-backups/pre-reblame/`, which holds the pre-correction attribution for all
187 projects.

**Magnitude, measured at token level.** Old attribution against new, joined on
`(file_path, token_index)`, on the first five projects of the wave. Row counts and
schema were identical in every case, so the difference is attribution alone:

| project | tokens | author changed | firm changed | top-1 author |
| --- | ---: | ---: | ---: | --- |
| `citusdata__citus` | 947,699 | **25.011%** | 14.942% | unchanged |
| `dpdk__dpdk` | 15,200,540 | **16.576%** | 6.253% | **changed** |
| `open-mpi__ompi` | 2,580,411 | 12.981% | 8.039% | unchanged |
| `kamailio__kamailio` | 5,276,523 | 12.809% | 7.641% | unchanged |
| `dolphin-emu__dolphin` | 5,758,507 | 10.877% | 3.080% | unchanged |
| `buchen__portfolio` (pilot) | — | **23.183%** | 21.3% | **changed** |
| `google__obr` (pilot) | — | 4.701% | 4.7% | unchanged |
| `renesas__rz-fsp-examples` (pilot) | — | 0.000% | 0.0% | unchanged |

So between **0% and 25%** of a project's tokens change author, and a project's
single largest author can change. `buchen__portfolio` moved from Alexander Ott to
Andreas Buchen.

**The correction introduces a new defect: a corporate address can win an author
ranking.** `dpdk__dpdk`'s top-1 author became **`intel at intel.com`**, which is not
a person. Copy detection promoted it. Any per-author result over the corrected data
needs an identity-hygiene pass first. Related: the vendoring artefact above.

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

* **Consequence, for any project not yet re-blamed**: every per-author,
  per-organisation and truck-factor result over it **systematically over-credits
  refactorers, file-movers and header-splitters** and under-credits original authors.
  This is a separate mechanism from the vendoring artefact above, and it moves
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
| the 186 S- and M-class projects with a Parquet | 446,060 | 36 |
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
artefact. Over the 186 S- and M-class projects the naive count is 390 and the true
count is 36; over the published set it is **417 naive and 35 true**, because Linux adds 28
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
  zero exit status — accounts for **0** of the 35. Linux's one gap is a signalled crash, not this mechanism. It is unmeasured for historical
  revisions.
* **Work around**: none from the data. The file's tokens do not exist in the dataset.

---

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
* **The defect is not repaired in place, and no repair script ships in this
  repository.** A repair would need to leave `source_text` wrong on content rows
  on purpose, and set it to `''` on structural rows only, because every
  structural branch of `classify_and_skip` writes `''` there — so
  `is_structural = 1` implies `source_text = ''` in correct data, and repairing
  one without the other would create a new inconsistency. The only clean fix is
  a re-run of the Rust-bearing projects once the tokenizer itself is corrected.
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

**Publishing the Parquet as it stands publishes contributors' e-mail addresses.**
`person_email` (column 50) is a real address. `anonymize_parquet.py` is the
release path: it rewrites the e-mail local part to `author_NNNN` and names to
`Author N`, running every identity string — including each element of the 15
trailer arrays — through one registry, so a person carries one pseudonym
everywhere. It classifies **every** input column into pass-through, transform or
drop and raises on a column it does not recognise, so a schema change stops it
loudly instead of silently shrinking the release. It also checks its own output:
row count, `firm`/`repo_name` group counts and every `person_domain` must be
unchanged, and `count(distinct person_email)` and `count(distinct person_name)`
must not drop — two real people collapsing onto one pseudonym would lower a
distinct-contributor count for free.

Two properties to plan a release around. There is **no salt and no key**: ids are
assigned by sorting the distinct lowercased values, so two runs over the same
inputs are byte-identical and two releases diff cleanly, but the only thing
protecting the mapping is not publishing the registry. And the registry spans
**one invocation**, so pseudonyms are not stable across runs with different input
sets — pass every file that will be published together in a single command.

**Nothing runs the anonymiser automatically, and the Parquets on disk are not
anonymised.** `anonymize_parquet.py` has **no caller anywhere in the pipeline**:
it appears in no step of `run_pipeline_process.sh`, and neither `ctp.py` nor
`consolidate.py` invokes it. Step 10 writes
`<work>/<repo>-dataset.parquet` through `generate_dataset.py` and stops there. So
every per-project Parquet in the corpus holds real addresses and real names, and
the release is a separate manual command. Two consequences. A step-10 re-run
**cannot** undo pseudonymisation, because the two never touch the same file — a
re-tokenization wave needs no anonymiser precaution. And `OUTDIR` is a mandatory
positional with **no same-file guard**, so naming a project's own work directory
as the destination would overwrite the raw Parquet with the pseudonymised one and
destroy the only copy of the identities. Always write to a fresh directory.

Run `verify_anon.py` over the output directory as an independent check: it tests
the published files alone for any e-mail local part that is not a pseudonym, needs
no secrets, and so can be run by a reviewer or a depositor. It is a necessary,
not a sufficient, condition. Two residual risks survive
anonymization by design and must be disclosed by any analysis: the e-mail
**domain is preserved on purpose** (it is the firm signal), so a sole contributor
at a rare or vanity domain is re-identifiable; and `owner`/`repo_name`/
`clone_url` carry the GitHub namespace, which for a personal repository is a
person's handle. `source_text` and `token_value` are source code and are not
scrubbed, so copyright headers and `@author` tags pass through.

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
grammar for both.

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
entry does not already qualify.

**A file can be excluded from a project with no row and no marker in the
Parquet.** Three mechanisms remove a blob from the tokenized repository entirely,
so it yields no blame line and therefore no dataset row:

| Mechanism | Blocks publication? |
| --- | --- |
| blob denylist, keyed by **git blob sha** — a shipped data file in the cregit repo, so an exclusion can be cited | no |
| blob over JGit's 50 MiB stream-file threshold | no |
| tokenizer timeout (the parser fails to terminate on a construct) | **yes** — the project cannot complete and gets no Parquet |

Only the third is visible from outside, as a missing project. The counters count
`(sha, path)` pairs rather than files, so a count of 4 can be one file at two
paths across two revisions. Separately, a mask-selected file can tokenize to an
**empty** result with a zero exit status and produce zero rows silently. At HEAD
that mechanism accounts for **0 files**: all 36 HEAD files absent from the 186
published Parquets are parser crashes, measured 2026-09-20. Its count over
historical revisions is still unmeasured.

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

**`data/affiliation.merged.csv` weights heavily toward single-person inference,
and shipping it as it stands is a re-identification risk.** Of its 4,041
domain-to-company rows, **2,789 — 69%** — carry a `source` of
`cncf-gitdm-single` or `cncf-gitdm-single-self-reference`: one person's own
claim about their employer, with no second person to corroborate it. The
remaining 1,252 rows come from hand-curated or multi-person sources.

A personal domain attributed to an employer is not dangerous by itself. Joined
against the repository the domain's owner contributed to, it can identify a
specific person: a vanity domain plus a project is often enough on its own. At
least one domain in the map today has exactly this shape.

**A mitigation exists: read `firm_source`.** It is published on every row
(column 54), so any analysis can filter to the corroborated tier and drop the
single-person rows before publishing a result; an empty `firm_source` already
means no attribution at all, and is the column to check first.

**A further mitigation does not exist, on purpose.** k-anonymity over the
domain is the standard remedy for this kind of exposure, and it is not applied
here. It is not an oversight: it would suppress exactly the small-firm,
single-contributor tail that this dataset exists to measure, so applying it
would remove the population the analysis is for.

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
because srcML 1.1.0 does not recognise them, ignores `-l C++`, and then writes an
**empty token file and exits 0** — a silent success. Autotools input (`.am`,
`.ac`, 1.4 MB across 22 projects) is left out because the m4 tokenizer's lexer
uses a backtick as its closing quote, so an apostrophe swallows text to the next
backtick. C++ module and inline-implementation files and autotools source are
therefore absent from the dataset with nothing in a Parquet to indicate the
absence. `tests/test_mask_drift.py` holds the extension list to the tokenizer's
own language table, so adding an extension without probing it is caught.

**Runs are not pinned to a commit.** The manifest carries exactly five fields and
three parsers plus four tests assert their positions, so the planned
`pinned_commit` sixth field does not exist. A re-run analyzes whatever HEAD is at
clone time, so the dataset is reproducible in method but not byte-for-byte, and a
published Parquet cannot cite the revision it was built from.

**`data/affiliation.merged.csv` is a build artefact.** Never hand-edit it: put a
fix in `data/affiliation.corrections.csv`, which `build_domain_map.py` applies
last, and rerun `./build_domain_map.py build`. A hand edit is lost on the next
rebuild; a correction survives it.

**`community` is a residual stratum, and `contested` is empty for every row of
the corpus this pipeline built.** Three of the five control facts the selection
process used were not implemented, so a project that no namespace fact reached
stayed `community` by default. `contested` (column 7) was non-empty in **0 of
24,405** candidate rows, so whatever contested-case protocol the labelling
process defined never flagged anything, and the column could not be used to find
disputed labels. An independent published registry separately attested a
company for at least seven of the eligible rows that this pipeline nonetheless
labelled `community`. Both gaps belonged to the now-removed selection tooling; a
sidecar you write yourself can set `contested` to whatever your own process
finds.

**GitHub's `fork` flag finds nothing.** Among eligible rows `fork = True` counts
zero, because a tree pushed as an independent repository is not marked. Projects
that carry another project's history are therefore **kept and flagged**, not
excluded: filter on `history_cluster` for one project per history, and read
`history_first` for origin — `history_relation` is about inclusion, and the same
topology occurs with the origin on either side.

**A stratum is a label at a date.** Namespaces get donated, so two rows for one
repository can disagree: of the 196 duplicate `clone_url` groups the selection
process found, **28 disagreed on `stratum`** — **5** of them within the 106
groups that fell inside the eligible frame. `label_date` (column 8) records when
the label was assigned, and nothing in this pipeline time-boxes a label against a
relicensing or ownership change, so a project that changed licence or owner
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

**Dates are strings.** `author_date` and `committer_date` (43, 46) are raw git
strings, so a consumer must cast before any temporal query.

**Corpus-level firm and org rollups do not exist.** The firm columns are per
token; there is no aggregated firm table, no package step and no dataset card
generator. `docs/DESIGN.md` §8 is the intended shape of those.

**Reproducibility depends on an unpinned sibling checkout.** The per-project
pipeline, the tokenizer and their flag names live in the cregit checkout that
`pipeline.cfg` names, at whatever revision it happens to be. A flag rename there
has already broken a run mid-corpus. Record that checkout's revision alongside
any published Parquet.
