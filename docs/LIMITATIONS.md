# Known limitations

These are the limits of the method. They apply to every run of the pipeline,
unless an entry names the cregit commit that removes the limit. Read this before
you analyze the data. [`DATASET-SCHEMA.md`](DATASET-SCHEMA.md) is the column
reference.

Each entry states the limit, how to find it in the data, and the workaround if
one exists. This file gives no counts. Measure the effect on your own run.

---

## 1. Tokenizer and parser

**Non-ASCII text in C, C++ and Java tokens can be double-encoded (mojibake).** The
prebuilt srcML 1.1.0 binary reads UTF-8 bytes as Latin-1. Each bad character also
moves the source cursor one to three characters ahead, so the later
`source_line`, `source_col` and `source_text` values of the file are wrong. Rust
files are not affected.

* **Detect**: `regexp_matches(token_value, '[\x{C2}-\x{F4}][\x{80}-\x{BF}]')` for
  the text. For the positions, every row after the first such token in the same
  `file_path`.
* **Work around**, text only: `token_value.encode("latin-1").decode("utf-8")`.
  This does not fix the positions.
* **Fix**: cregit PR #93 builds srcML from source and writes correct UTF-8. cregit
  PR #97 repairs the token text in step 10, before the source walk, so
  `ctp.py run --from-step 10` repairs an old run with the blame on disk.

**A file that starts with a byte-order mark has wrong positions after the first
token.** Before cregit PR #97, `generate_dataset.py` opens the source with
`encoding="utf-8"`, not `"utf-8-sig"`. The byte-order mark (U+FEFF) stays in the
text, but the tokenizer drops it. So the source cursor runs one character ahead,
from the first token to the end of the file.

* **Detect**: the first content row of the file has a `source_text` that starts
  with `chr(65279)`.
* **Affected columns**: `source_line`, `source_col`, `source_text`. `token_value`,
  `token_type` and the attribution columns are correct.
* **Work around**: run step 10 again with cregit PR #97. This needs no new
  tokenization and no new blame.

**Before cregit PR #97, four columns are wrong on every `.rs` row.** The Rust
tokenizer separates a token's position from its type with a tab, but
`generate_dataset.py` splits on a pipe. The tokenizer also writes the position
prefix that the pipeline does not ask for. So the whole `line:col<TAB>type`
string goes into `token_type`, and the source cursor desynchronizes. `token_type`,
`source_line`, `source_col`, `source_text` and `is_structural` are then wrong.
`token_value` is correct on content rows.

* **Detect**: `file_path LIKE '%.rs'`. Do not test only for a tab in
  `token_type`: on the end-of-unit row the tab goes into `token_value`.
* **Work around**: `split_part(token_type, chr(9), 1)` gives the true `line:col`,
  and `split_part(token_type, chr(9), 2)` gives the real token type. `source_text`
  cannot be recovered from the Parquet. Only a new run gives rows that align with
  a correct run, because a correct run writes one more end-of-unit row per file.

**`is_structural` is of little use for Rust, also in a correct run.** The Rust
tokenizer writes no `begin_*` or `end_*` markers, no `blank` and no `DECL`. A Rust
file has only a few structural rows.

**The author of a `.rs` token can be the commit that moved it.** A tokenizer
before cregit `729643e` writes the `line:col` of every token into the tokenized
file that `git blame` reads. An edit above a token changes its line number, so
blame credits the commit of the edit. The current tokenizer writes no prefix.
Only a new tokenization and a new blame remove this.

**Every `.h` file is parsed with the C grammar.** The tokenizer's language table
maps `h` to `C`, so C++ in a `.h` header is tokenized as C. The table has only
lowercase keys, and both gates lowercase the extension, so `.C` and `.H` also go
to C, although `.C` is usually C++.

**Two files with identical content share one tokenizer cache slot, also in two
languages.** `tokenizeByBlobId/tokenBySha.pl` keys the memo on
`sha1_hex($contents)` only. The extension selects only `--language` and the name
of the temporary file. The first file of the pair that is tokenized decides the
grammar for both. With parallel workers this order can change, so two runs can
differ for such a file.

**The patched srcML gives other tokens than stock srcML.** cregit builds srcML with
`nix/srcml-nontermination.patch`. With the patch, two adjacent string literals
(`"a""b"`) are two tokens, a file on which stock srcML does not finish is present
with the recovery markup of the parser, and some runs of quotes in code that the
preprocessor removes (for example in `#if 0`) give other tokens.

**An old srcML defect stays.** A string on a preprocessor line ends at a
backslash-newline, so the tokens after it are wrong. Stock and patched builds both
have this defect.

**srcML 1.1.0 can crash only when `--position` is given.** On some files srcML
exits 0 without `--position` and crashes with exit status 139 with it.
`tokenizeSrcMl.pl` always passes `--position`. The result is a zero-byte `.blame`
file that no counter records. A source-tip build of srcML parsed the known case
cleanly.

**Some tokenizable source is excluded on purpose, and the output does not say
so.** `.ixx`, `.inl`, `.cppm`, `.cxxm` and `.ipp` are not in the mask, because
srcML 1.1.0 does not know them, ignores `-l C++`, and writes an empty token file
with exit 0. Autotools input (`.am`, `.ac`) is not in the mask, because the
lexer of the m4 tokenizer uses a backtick as its closing quote.
`tests/test_mask_drift.py` holds the extension list to the tokenizer's own
language table.

## 2. Files with no row

**A file can leave a project with no row and no marker in the Parquet.** These
mechanisms remove a blob from the tokenized repository, so it has no blame line
and no row:

1. the blob denylist, keyed by git blob sha;
2. a blob over the 50 MiB stream-file threshold of JGit;
3. the tokenizer time limit: from cregit `157cf8b`, blobExec excludes a blob
   after 600 s of wall time;
4. a parser crash or a tokenizer error.

Also, a mask-selected file can tokenize to an empty result with exit status 0,
and then it gives zero rows with no error. The counters of the pipeline count
`(sha, path)` pairs, not files. Only `pipeline.log` records an exclusion, in the
`EXCLUDED` lines and the counters.

**An excluded blob also moves the credit of the history.** blobExec
(`Walker.scala`, `assemble`) drops the excluded blob from its commit in the
tokenized history. The next commit that has the file adds the whole file again,
so `git blame -C100` gives every line to that commit.

**The denylist excludes blobs that the patched srcML can tokenize.**
`BlobDenylistEntries.scala` lists files on which srcML 1.1.0 does not finish:
OpenJDK langtools test files (Java, srcML issue 2361) and `.h` files with C++
code. The patched srcML finishes on them, but blobExec still excludes them.

**A symlink or a submodule gitlink has no row, and that is correct.** It is not
source, so it is never tokenized.

* **Detect a real gap**: a path that exists at HEAD, matches the file mask, has
  mode 100644 or 100755, and has no row in the Parquet. Use `git ls-tree -r HEAD`
  and drop mode 120000 (symlink) and 160000 (gitlink). Do not use
  `ls-tree -r --name-only`, because it cannot show the mode.
  `./measure_dataset_gaps.py --manifest <run>.tsv` does this count.
* **This is a HEAD measurement.** A historical revision that crashed the parser is
  not visible to it, so history can be worse than HEAD.
* **Work around**: none from the data. The tokens of the file are not in the
  dataset.

## 3. Blame and attribution

**Without copy detection, content moved between files goes to the commit that
moved it.** Plain `git blame` follows a whole-file rename. It does not follow
content that moved between files, so a header split or a refactor credits every
token that it touched to the commit of the move. The `blameRepo/formatBlame.pl`
of upstream cregit has the `-C100` copy detection commented out. The fork runs
`git blame -C100`.

**Some files can still have blame without copy detection.** With cregit
`57551d7`, `9ccead6` and `23e4ccb`, `formatBlame.pl` stops git after 3600 s of
`-C100` and runs plain blame. The pipeline log then has the line
`PLAIN-BLAME <file> after 3600 s of -C100`. With cregit `23e4ccb`, a `-C100` that
stops with git exit 128 ("unable to generate diff", when a commit moves a blob
over 1 GiB) also falls back to plain blame. Find these files from the
`PLAIN-BLAME` lines of the pipeline log.

**Before cregit `14b8db4`, blame drops files with a special path name.** Step 7
(blame) has these defects:

1. `blameRepoFiles.pl` reads `git ls-files` without `-z`. git quotes a path that
   has a byte above 0x7F, a `"`, a `\` or a control byte, with the default
   `core.quotePath`. The quoted name ends in `"`, so the mask does not match it. The file
   has no `.blame` file and no row, and no log line names it.
2. The path goes to `git blame` with no `--` before it. A path that starts with
   `-` is then read as an option, and step 7 exits 1.
3. Only in cregit `95cdc88`, `1f17663` and the commits before them:
   `formatBlame.pl` runs git through a shell string. A `'` in the path breaks the
   command. The script does not check the exit status of git, so any git failure
   gives an empty or a partial `.blame` file and exit 0.

**A vendored dependency credits its whole library to the person who imported
it.** cregit credits the commit that added a line. So a vendor drop credits the
importing author with every token of the third-party code. This is a property of
token-level blame, not a defect of the pipeline. A per-organization or
per-author aggregate over a project that vendors its dependencies is an upper
bound, not a measurement. No column marks a path as vendored, so you must supply
that list.

**Token count is not a measure of human contribution.** The mask keeps generated
C and C++ sources, for example bitmap-font arrays, constant tables and binary
data written as arrays. One generated file can give millions of rows to the
person who committed it. Such files have almost no `is_structural` tokens. Look at
the row count per file before you read a share of a project:

```sql
SELECT file_path, COUNT(*) n
FROM read_parquet('<name>-dataset.parquet')
GROUP BY 1 ORDER BY n DESC LIMIT 10;
```

Weight by commits or by people, or drop outsized files, before you draw a
conclusion.

## 4. Identity and trailers

**`person_name` is always equal to `personid`, and the merge rule of the person
ids has limits.** `persons.personname` is never filled, so
`coalesce(personname, personid)` gives the person id. There is no display name.
`unifyPersons.scala` makes the key from the name in lower case with no accents.
A name of one word gets `at` and its address added. Two persons with the same
full name get one id. An address that two names share joins them into one id,
and such joins chain. When two keys tie, the order of a hash set decides. The
lower-case step (`toLowerCase` with no locale) uses the default locale of the
JVM. Nothing counts the false merges.

**The trailer columns follow JGit 4.6, not git.** The `footer_*` lists come from
`getFooterLines` of JGit 4.6.0 (slickgitlog). Every `Key: value` line of the last
paragraph is a trailer: there is no 25% rule, no blank line is necessary, and a
continuation line is dropped. So a `Reported-by:` line in a last paragraph of
prose gives one trailer, where git gives none.

**`footer_personids` and `footer_person_names` do not follow from the 13 typed
trailer columns.** They resolve people across every trailer key of the commit,
also keys that are not columns (`Source-Author`, `Committer`). A row can have a
resolved person id while all 13 typed lists are empty. A trailer value can carry
a stray carriage return, which splits identical values in a `GROUP BY`.

**`repo_tag` is empty by design.** It comes from the multi-repository import tag,
and only the Linux-specific import of cregit fills it. Check for a non-empty value
before you build on any column.

**Dates are strings with no UTC offset.** `author_date` and `committer_date`
(columns 43 and 46) are not git strings. slickgitlog (`gitLogToDb.scala`) formats
them with `SimpleDateFormat("yyyy-MM-dd HH:mm:ss")` and no time zone. The value
is the wall time in the default zone of the JVM, and the offset is lost. Neither
devenv nor the runner sets `TZ`. Set `TZ=UTC` for a run, and cast the strings
before a query on time.

## 5. Firm attribution

**`firm` is empty on rows with no attribution and ambiguous on the rest.** A
project has firm attribution only if the run has `--firm-map`. Without it,
columns 52-54 are empty strings, which is also what "this domain is not in the
map" looks like. From columns 52-54 alone you cannot tell the two cases apart.
`(Independent)` is a category, not a firm (`DATASET-SCHEMA.md` §3.4), so
`firm <> ''` is not a count of firm-attributed rows.

**Nothing in a Parquet records which revision of the firm map made its firm
columns.** The map and the canonical table are versioned in git. Record the
revision of `data/affiliation.merged.csv` and `data/firm_canonical.csv` with any
Parquet that you publish.

**`data/affiliation.merged.csv` is a build artifact.** A hand edit is lost on the
next build. Put a fix in `data/affiliation.corrections.csv`, which
`build_domain_map.py` applies last, and run `./build_domain_map.py build` again.

## 6. Privacy

**A Parquet as it stands holds contributors' e-mail addresses.** `person_email`
holds real addresses. `anonymize_parquet.py` changes the local part of an address
to `author_NNNN` and a name to `Author N`. It has no salt and no key: the ids come
from the sorted distinct values, and the registry covers one invocation, so the
pseudonyms are not stable across runs with other input. The e-mail domain stays
on purpose, because it is the firm signal. `owner`, `repo_name` and `clone_url`
carry the GitHub namespace. `source_text` and `token_value` are not changed.

* **Detect**: run `verify_anon.py` over the output directory. It checks the
  published files alone.

**Nothing runs the anonymizer automatically.** `run_pipeline_process.sh`,
`ctp.py` and `consolidate.py` do not call `anonymize_parquet.py`. Its `OUTDIR` is
a required argument with no same-file guard, so if you name the work directory,
it overwrites the raw Parquet. Always write to a new directory, and run
`verify_anon.py` as an independent check.

**A domain with one contributor can identify that person.** The domain stays, so
a rare or a personal domain can narrow a pseudonym to one person. An analysis
that reports results per domain must treat such a domain as identifying. A
release that cannot accept this must aggregate the domains first.

## 7. Reproducibility and validation

**Runs are not pinned to a commit.** The manifest has exactly five fields, and no
`pinned_commit` field exists. A new run analyzes the HEAD at the time of the
clone. So the dataset is reproducible in method, but not byte for byte.

**Reproducibility depends on an unpinned sibling checkout.** The per-project
pipeline, the tokenizer and their flag names are in the cregit checkout that
`pipeline.cfg` names, at whatever revision it has. Record the revision of that
checkout with any Parquet that you publish.

**Some run-time inputs are not pinned.** Step 10 (`generate_dataset.py`) runs
`INSTALL sqlite_scanner`, so DuckDB downloads the extension from the network.
`formatBlame.pl`, the runner and `ctp.py` do not set `GIT_CONFIG_NOSYSTEM` or
`GIT_CONFIG_GLOBAL`, so the git configuration of the machine can change the
blame. The time zone is the third input (section 4).

**A `.validated` stamp does not certify the schema.** `validate.py` checks only
the file size and the row count (`DATASET-SCHEMA.md` §2.1). Run
`validate_schema.py` over the output. Also, `state = 'DONE'` in `ctp.duckdb` means
that the pipeline finished, not that the data is present. Find such a project
with `select name from projects where parquet_missing`.

**No firm or organization rollup exists.** The firm columns are per token. There
is no aggregated firm table, no packaging step and no dataset card
(`DESIGN.md` §7).

## 8. Language filter

These apply to `language_filter.py` (README, "Language filter"), which can select
the manifest before a run.

**A line count is not a token count.** The rule reads tokei's code lines, not
cregit tokens. A language with long lines or dense syntax has more tokens per
line than another, so the target share is not the share of tokens that the run
will produce. Measure the token share on the output if your analysis needs it.

**tokei decides the language by the file name.** A file is counted under the
language that its extension or name maps to in tokei 14, not under what it
holds. The Qt rule below is the one correction. Another tokei major version can
map names in another way, so the step records `tokei_version` and warns when it
is not 14.

**Which languages count is a judgment.** `data/tokei14_programming.tsv` follows
Linguist's `type: programming` where a tokei name is a Linguist name, and a hand
decision otherwise (`build_programming_table.py`, `HAND`). Shell, Makefile, CMake
and Autoconf count; Protocol Buffers, Jupyter Notebooks and every data, markup
and prose language do not. Pass another table to change it.

**Qt `.ts` files are removed by their first bytes.** A `.ts` file that starts
with `<?xml`, `<!DOCTYPE TS` or `<TS`, after an optional UTF-8 byte order mark
and white space, is removed as a Qt translation file. A TypeScript file that
starts with a `<TS...>` type assertion would be removed too, which is rare. A Qt
file in UTF-16, or one that starts with a comment, is counted as TypeScript.

**Vendored code is found by directory name only.** A directory named like an
entry of `--vendored-dirs`, at any depth, is removed; a symbolic link with such
a name is kept. A vendored library under another name (`src/zlib/`, `contrib/`,
`node_modules/` by default) is counted, and a project's own `deps/` directory is
removed.

**Embedded code counts toward the language that holds it.** A language's lines
include the code in its `children`, as in tokei's total. So a Rust block in
Markdown counts as Markdown and a script in HTML as HTML, and neither is counted,
because neither of those languages is a programming language.

**C++ modules are their own language.** tokei counts `.cppm`, `.ixx` and the
like as `C++ Module`. It is a programming language in the table, so it counts,
but it is not merged into C++ and is not a default target. The tokenizer does not
parse those files either (`file_mask.py`).

**Submodules and Git LFS objects are not fetched.** The count covers the files
of the commit itself. A submodule is an empty directory, and an LFS file is its
pointer text.

**A host that refuses a fetch by commit gives no count.** Some hosts send only
the commits that a branch or tag names. Such a row is reported as `not measured:
fetch by commit refused` and dropped; the step does not fall back to another
commit. A commit that the host does not have reads the same, because git prints
`not our ref` for both. A failed fetch is not retried.
