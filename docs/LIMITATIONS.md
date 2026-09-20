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

**The same crash mechanism reaches published data.** In
`sumatrapdfreader__sumatrapdf`, which **is** published, all seven of its zero-byte
tokenizations are parser crashes — including one on a 1,263-byte file, so it is not a
size limit. A corpus-wide count of affected files is **not yet established**; it is
being measured, and this entry will carry the number when it is. Until then, treat a
file present in a repository but absent from the dataset as possibly a parser crash
rather than evidence that the file held no tokens.

* **Detect**: a path that exists at HEAD, matches the file mask, and has no row in
  the Parquet.
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
shifted and wrong, and `is_structural` is effectively dead for Rust. This is a
property of the tokenizer, not of one project, so it applies to **every `.rs` row
in the corpus** — including `.rs` files inside projects whose primary language is
not Rust.

* **Detect**: `token_type LIKE '%' || chr(9) || '%'`, equivalently
  `file_path LIKE '%.rs'`.
* **Work around**: `split_part(token_type, chr(9), 1)` recovers the true
  `line:col` and `split_part(token_type, chr(9), 2)` the real token type; skip the
  `-:-` end-of-unit marker. **`token_value` is correct.** `source_text` is not
  recoverable from the Parquet.
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
**empty** result with a zero exit status and produce zero rows silently; the
corpus-wide count of that has not been measured.

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
