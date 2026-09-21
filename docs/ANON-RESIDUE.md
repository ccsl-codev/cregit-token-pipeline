# Anonymization residue, measured

What is left in a published Parquet after `anonymize_parquet.py` runs. Every
number here is measured, not asserted. Reproduce with the commands at the end.

## What was measured

Six corpus Parquets, chosen because they carry commit trailers. A first sample of
five files had **zero** footer elements, so it exercised none of the trailer
code. Pick footer-bearing files or the measurement means nothing.

Row counts are `rows_in` from the run report, and equal `rows_out` in every case.
The footer column counts all 15 `footer_*` columns, including the two derived
ones.

| file | rows | footer elements |
| --- | --- | --- |
| `awslabs__aurora-dsql-connectors` | 39,930 | 65,263 |
| `bytecodealliance__componentizejs` | 25,029 | 196 |
| `containers__composefs` | 60,140 | 0 |
| `googlemaps__google-maps-services-java` | 82,124 | 6,220 |
| `ohler55__ox` | 76,862 | 8,226 |
| `tomaka__glutin` | 66,738 | 37,086 |
| **total** | **350,823** | **116,991** |

Of those 116,991, **47,001** sit in the 13 trailer-**text** columns; the rest are
in `footer_personids` and `footer_person_names`.

One registry over all six: 184 e-mails, 224 names, 17 distinct footer elements,
63 preserved domains. Probes used by the scanner: 222 names of five characters or
more, 170 local parts of four or more.

All 17 distinct footer elements parsed as the documented `Name <email>` trailer
shape. No `bare_email`, `name_at_email` or `other` shape occurred in these six
files, so the non-conforming paths are covered by unit tests rather than by this
measurement.

## Residue per column

139,749 distinct strings over the 70 columns. Counts are of **distinct values**,
after the deliberately published domains are masked out.

`real addr` is an address whose local part is not a pseudonym. `real local` is a
registry local part used as an address. `real name` is a registry name matched on
word boundaries.

| column | rule | distinct | real addr | real local | real name |
| --- | --- | --- | --- | --- | --- |
| `repo_name` | pass | 6 | 0 | 0 | 0 |
| `clone_url` | pass | 6 | 0 | 0 | 0 |
| `provenance_status` | pass | 1 | 0 | 0 | 0 |
| `source` | pass | 4 | 0 | 0 | 0 |
| `stratum` | pass | 3 | 0 | 0 | 0 |
| `fact` | pass | 4 | 0 | 0 | 0 |
| `contested` | pass | 0 | 0 | 0 | 0 |
| `label_date` | pass | 1 | 0 | 0 | 0 |
| `owner` | pass | 6 | 0 | 0 | 0 |
| `repo` | pass | 6 | 0 | 0 | 0 |
| `roster_name` | pass | 6 | 0 | 0 | 0 |
| `roster_lang` | pass | 1 | 0 | 0 | 0 |
| `language` | pass | 3 | 0 | 0 | 0 |
| `commits` | pass | 6 | 0 | 0 | 0 |
| `size_class` | pass | 1 | 0 | 0 | 0 |
| `size_kb` | pass | 6 | 0 | 0 | 0 |
| `stars` | pass | 6 | 0 | 0 | 0 |
| `pushed_at` | pass | 6 | 0 | 0 | 0 |
| `license` | pass | 3 | 0 | 0 | 0 |
| `owner_type` | pass | 2 | 0 | 0 | 0 |
| `archived` | pass | 1 | 0 | 0 | 0 |
| `fork` | pass | 1 | 0 | 0 | 0 |
| `history_cluster` | pass | 0 | 0 | 0 | 0 |
| `history_shared_with` | pass | 0 | 0 | 0 | 0 |
| `history_relation` | pass | 0 | 0 | 0 | 0 |
| `history_includes` | pass | 0 | 0 | 0 | 0 |
| `history_first` | pass | 0 | 0 | 0 | 0 |
| `history_created` | pass | 0 | 0 | 0 | 0 |
| `manifest_category` | pass | 3 | 0 | 0 | 0 |
| `file_mask` | pass | 1 | 0 | 0 | 0 |
| `file_path` | pass | 337 | 0 | 0 | 0 |
| `token_index` | pass | — | — | — | — |
| `source_line` | pass | — | — | — | — |
| `source_col` | pass | — | — | — | — |
| **`source_text`** | pass | 46,439 | **6** | **2** | **40** |
| `token_type` | pass | 64,768 | 0 | 0 | 0 |
| **`token_value`** | pass | 21,148 | **14** | **1** | **39** |
| `is_structural` | pass | — | — | — | — |
| `cregit_commit_sha` | pass | 1,188 | 0 | 0 | 0 |
| `original_commit_sha` | pass | 1,188 | 0 | 0 | 0 |
| `author_name` | transform:name | 165 | 0 | 0 | 0 |
| `author_email` | transform:email | 172 | 0 | 0 | 0 |
| `author_date` | pass | 1,188 | 0 | 0 | 0 |
| `committer_name` | transform:name | 86 | 0 | 0 | 0 |
| `committer_email` | transform:email | 90 | 0 | 0 | 0 |
| `committer_date` | pass | 1,104 | 0 | 0 | 0 |
| **`commit_summary`** | transform:summary | 1,168 | 0 | 0 | **1** |
| `personid` | transform:personid | 165 | 0 | 0 | 0 |
| `person_name` | transform:name | 165 | 0 | 0 | 0 |
| `person_email` | transform:email | 172 | 0 | 0 | 0 |
| `person_domain` | pass | 60 | 0 | 0 | 0 |
| `firm_raw` | pass | 11 | 0 | 0 | 0 |
| `firm` | pass | 11 | 0 | 0 | 0 |
| `firm_source` | pass | 7 | 0 | 0 | 0 |
| `repo_tag` | pass | 0 | 0 | 0 | 0 |
| `footer_signed_off_by` | transform:footer_text | 4 | 0 | 0 | 0 |
| `footer_co_authored_by` | transform:footer_text | 11 | 0 | 0 | 0 |
| `footer_co_developed_by` | transform:footer_text | 1 | 0 | 0 | 0 |
| `footer_reviewed_by` | transform:footer_text | 0 | 0 | 0 | 0 |
| `footer_acked_by` | transform:footer_text | 0 | 0 | 0 | 0 |
| `footer_tested_by` | transform:footer_text | 0 | 0 | 0 | 0 |
| `footer_reported_by` | transform:footer_text | 0 | 0 | 0 | 0 |
| `footer_suggested_by` | transform:footer_text | 0 | 0 | 0 | 0 |
| `footer_based_on_patch_by` | transform:footer_text | 0 | 0 | 0 | 0 |
| `footer_helped_by` | transform:footer_text | 0 | 0 | 0 | 0 |
| `footer_mentored_by` | transform:footer_text | 0 | 0 | 0 | 0 |
| `footer_assisted_by` | transform:footer_text | 0 | 0 | 0 | 0 |
| `footer_thanks_to` | transform:footer_text | 0 | 0 | 0 | 0 |
| `footer_personids` | transform:footer_personid | 9 | 0 | 0 | 0 |
| `footer_person_names` | transform:footer_name | 9 | 0 | 0 | 0 |

A `0` in a footer column means the column is empty in these six files, not that
it was skipped. `footer_signed_off_by` holds only 4 distinct values because the
same trailer repeats on every token row of a commit; 47,001 **elements** were
rewritten, and all 47,001 came out well formed.

### Totals

| bucket | real addr | real local | real name |
| --- | --- | --- | --- |
| identity and transformed columns | **0** | **0** | **1** |
| content columns (disclosed risk) | 20 | 3 | 79 |

`(ANON-MISSING)`, the marker for a value that reached the writer with no registry
entry, appears **0** times.

## The four residue classes, named

**1. An `@` in an unexpected column: 0.** Every address-bearing value sits in an
e-mail column, a footer column or a content column. `commit_summary` holds 7
values containing `@`, and all 7 are the scrubbed `author@domain` form that
`anon_summary` writes; none is a real address.

**2. A name-shaped string in a footer: 0.** All 47,001 footer text elements match
`Author NNNN <author_NNNN@realdomain>`.

**3. A name-shaped string in `commit_summary`: 1.** See below. It is a false
positive, and it currently fails the release.

**4. A URL carrying a username: 26 projects.** Measured over all 196 corpus
files: 26 are owned by a GitHub **User** and 160 by an Organization. For those 26
the `owner`, `repo_name`, `clone_url` and `roster_name` values are a person's
handle — `akarnokd`, `casey`, `djcb`, `elfmz`. These are not pseudonymized,
because the repository identifier is the dataset key and the repositories are
public. This is a re-identification route for the project owner and must be
disclosed by any paper using the output.

## The one identity-column residue, and why it blocks a release

`tomaka__glutin` fails, on one value:

```
LEAK identity column commit_summary: 1 [['name', 'github', 'ci: bump github actions']]
```

The registry holds a **name** whose value is the literal string `GitHub` — the
GitHub web-flow committer, which cregit records as a person. The scanner
therefore probes for the word `github`, and a commit subject reading
`ci: bump github actions` matches it.

This is not personal data. The tool already publishes `github.com` and
`users.noreply.github.com` in the clear as preserved domains, so the token
carries nothing the release does not state elsewhere. But the residue gate is
fail-closed by design, so this one false positive turns the whole run non-zero,
and the only remedy the tool offers is `--null-commit-summary` — which discards
1,168 useful subject lines to suppress one word.

**This is an open partner decision, not a defect fixed here.** See
`docs/ANON-OPEN-QUESTIONS.md`, question 2. It was deliberately not "fixed" by
loosening the gate: a scanner tuned until it goes green is not a scanner.

## Content columns are not scrubbed, on purpose

`source_text` and `token_value` are the source code the dataset exists to carry.
They hold 20 real addresses and 79 real-name hits in these six files —
`simon@josefsson.org`, `alexl@redhat.com`, `giuseppe@scrivano.org` — in copyright
headers and `@author` tags. Scrubbing them would destroy the token stream.
They are counted and reported separately for exactly this reason, and
`verify_anon.py` prints them as notes rather than failures.

## Reproducing

From the cregit checkout, because duckdb lives only in that shell:

```sh
cd /local/home/ellianco/Projects/cregit-workspace/cregit-issue61
devenv shell -- bash -c 'cd <this repo> &&
  python anonymize_parquet.py OUTDIR IN/*.parquet --report report.json &&
  python verify_anon.py OUTDIR'
```

Measured results of those two commands over the six files above: the anonymizer
exits 1 with the single `commit_summary` residue named; `verify_anon.py` exits 0
over 158,145 distinct strings with 0 failures and 20 content-column notes.
