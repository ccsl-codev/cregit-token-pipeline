# Known limitations

What is wrong with the dataset, ordered by how likely it is to change a result.
Read this before analysing the data. [`DATASET-SCHEMA.md`](DATASET-SCHEMA.md) is
the column reference; [`CODEBOOK.md`](CODEBOOK.md) §9 holds the gaps specific to
the stratum labels.

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

The project is excluded rather than published with gaps it cannot report. The corpus
is therefore **186 published of 187 in scope**, and the row stays in the manifest with
its reason so the sampling record remains complete.

**The same crash mechanism reaches published data, and the corpus-wide count is
36 files in 19 of the 186 published projects.** Measured 2026-09-20 over every
published Parquet: **446,060** mask-selected regular files exist at HEAD and
**36** of them have no row, which is **0.008%**. Every one of the 36 is a parser
crash. No other cause contributes a single file, so the residual is zero. The
largest single project loss is `sumatrapdfreader__sumatrapdf` with 7, including
one on a 1,263-byte file, so it is not a size limit. `elfmz__far2l`,
`texasinstruments__simplelink-zephyr` (6) and `util-linux__util-linux` (3) follow.
All 36 blobs are now on the blob denylist by sha, so a re-run reports them instead
of losing them.

**A file absent from the dataset is not, by itself, a gap.** Of the paths that a
naive detector reports as missing, **354 are symlinks or submodule gitlinks**, not
source. `powerdns__pdns` alone carries **331** masked symlinks in
`pdns/dnsdistdist/`; counting them makes its Parquet look 29% incomplete when it
is complete, because the Parquet holds exactly its 806 regular files.
`nvidia__opensma` (10) and `facebookincubator__qemu-wearables` (4) are the same
artefact. So the naive count is 390 and the true count is 36.

* **Detect**: a path that exists at HEAD, matches the file mask, **and is mode
  100644 or 100755**, and has no row in the Parquet. Read the mode: use
  `git ls-tree -r HEAD` and drop 120000 (symlink) and 160000 (gitlink). Do **not**
  use `ls-tree -r --name-only`, which cannot distinguish them.
* **The 36 are HEAD only.** A historical revision that crashed is not visible to
  this measurement, because the detector compares against HEAD. The denylist
  covers **197** historical blobs of these same 36 paths, so history is worse than
  HEAD by at least that much. No corpus-wide historical count exists.
* **A silently empty tokenization contributes nothing at HEAD.** The separate
  mechanism below — a mask-selected file that tokenizes to an empty result with a
  zero exit status — accounts for **0** of the 36. It is unmeasured for historical
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

**Publishing the Parquet as it stands publishes contributors' e-mail addresses.**
`person_email` (column 50) is a real address. `anonymize_parquet.py` is the
release path: it rewrites the e-mail local part to `author_<token>` and names to
`Author <token>`, running every identity string — including each element of the 15
trailer arrays — through one registry, so a person carries one pseudonym
everywhere. It classifies **every** input column into pass-through, transform or
drop and raises on a column it does not recognise, so a schema change stops it
loudly instead of silently shrinking the release. It also checks its own output:
row count, `firm`/`repo_name` group counts and every `person_domain` must be
unchanged, and `count(distinct person_email)` and `count(distinct person_name)`
must not drop — two real people collapsing onto one pseudonym would lower a
distinct-contributor count for free.

**A pseudonym is a salted keyed hash, and the salt is a secret you now have to
keep.** The token is `blake2b(space || NUL || lowercased value, key = salt)` taken
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
  second line of defence. **Losing the salt is unrecoverable**: no future release
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
superseded design, not the shipped one**: the same experiment now renumbers
**0 of 153 — 0.0%**, and the five shared output Parquets come out byte-identical
between a five-file and a six-file release.

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
scrubbed, so copyright headers and `@author` tags pass through.

**84.7% of published domains have exactly one contributor, and this release ships
anyway.** Measured over all 186 conforming corpus files and 28,339 distinct
addresses: **5,276 of 6,228** `person_domain` groups hold exactly one distinct
address. An earlier three-file sample gave 33 of 38 and hedged that the ratio was
"inflated by the small sample". It was not — the ratio did not fall with scale.
The project's position, stated plainly so a reader is not left to infer it: the
corpus is mostly **small samples**, and a small sample is itself composed of
single-contributor projects, so a large single-contributor tail is what this
corpus is, not an artefact to be corrected. The domain is published because firm
attribution resolves from it and because a domain is not personal data; the
pseudonym protects the local part and nothing protects the pairing of a rare
domain with a repository. **No k-anonymity work is being done**, now or planned:
suppressing or generalising domains below a threshold would remove exactly the
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

**Two of the 188 run-set projects have no Parquet:** `tencent__tencentkona-21`
and `torvalds__linux`. 186 do, all conforming to the 70-column contract. Verify
with `validate_schema.py` over the output directory rather than assuming a
project is present.

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

**`data/affiliation.merged.csv` is a build artefact.** Never hand-edit it: put a
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
pipeline (`docs/CODEBOOK.md` §7), so a project that changed licence or owner
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
generator. `docs/DESIGN.md` §7 is the intended shape of those.

**Reproducibility depends on an unpinned sibling checkout.** The per-project
pipeline, the tokenizer and their flag names live in the cregit checkout that
`pipeline.cfg` names, at whatever revision it happens to be. A flag rename there
has already broken a run mid-corpus. Record that checkout's revision alongside
any published Parquet.
