# Anonymization: open decisions for the partner

Three questions the release needs answered. Each carries a measured consequence
rather than an opinion.

> **Status.** Questions 1 and 3 have since been ANSWERED and the answers are
> implemented; the text below is preserved as the record of the choice, so the
> measurements that drove it are not lost. Question 2 is still open.
>
> * **Question 1 — answered: Option B, the salted stable hash.** Implemented in
>   `anonymize_parquet.py`. The salt is held privately outside the repository and
>   the reverse map is never published. The 98.0% renumbering below is now the
>   measured behaviour of the **superseded** design; re-run after the change, the
>   same experiment renumbers **0 of 153 — 0.0%**. The "correction to the brief"
>   section and the greps in it describe the state of the checkout *before* that
>   change: `docs/DESIGN.md` and `docs/LIMITATIONS.md` no longer say "no salt and
>   no key", and they now describe the hash, the private salt and the unpublished
>   reverse map. See `docs/DESIGN.md` §7 item 6.
> * **Question 3 — answered: publish with disclosure.** The 84.7% single-address
>   domain tail is stated in `docs/LIMITATIONS.md` with the project's reasoning,
>   and no k-anonymity work is planned. The docstring hedge has been corrected.
> * **Question 2 — still open.** `commit_summary` still fails the release on the
>   single `github` false positive, and `--null-commit-summary` is still the only
>   remedy the tool offers.

## First, a correction to the brief

The task described a conflict: `docs/DESIGN.md` §7 item 2, around line 163, was
said to specify a **salted stable hash with a private salt and a local reverse
mapping**, conflicting with the unsalted sequential registry in the code.

**That conflict does not exist in this checkout.** Verified:

- `grep -rn -i salt docs/ *.py` returns exactly two hits. `docs/DESIGN.md:182`
  says "There is **no salt and no key** — ids come from sorting the distinct
  values". `docs/LIMITATIONS.md:141` says "There is **no salt and no key**".
  Both describe the shipped behaviour.
- `grep -rn -i hash docs/` returns nothing at all.
- `grep -rn -i "reverse map"` returns nothing.
- §7 item 2 is `shared_history.py`, not anonymization. The anonymization item is
  §7 item **6**. The cited lines 25, 38 and 143 are a table header, the pipeline
  diagram and a `validate.py` bullet.

So the documents and the code **agree**. There is nothing to reconcile, and no
design document was overridden by the implementation.

The underlying trade-off is still a real and unmade release decision, so it is
recorded below as question 1. The point of this section is that it is an open
choice, not a contradiction anyone needs to repair.

## Question 1 — keep the unsalted sequential registry, or move to a salted hash?

### Option A: unsalted sequential registry (what ships today)

Ids are assigned by sorting the distinct lowercased values and counting from 1.
`alice@redhat.com` becomes `author_0042@redhat.com`.

Measured properties:

- **Byte-reproducible.** Two independent runs over the same six files produced
  byte-identical Parquet files, matching md5 for all 6 of 6.
- **Input order does not matter.** Presenting the same files reversed produced
  byte-identical output. Pinned by a test.
- **Verification needs no secret.** `verify_anon.py` checked 158,145 distinct
  strings over the six files and exited 0. A co-author or a Zenodo depositor can
  run it. This is the property that makes the gate worth putting in a release
  script.
- **Reversible by anyone who can rebuild the inputs.** The registry is a pure
  function of the input values, and the inputs derive from **public** GitHub
  repositories. An adversary needs no salt, no key and no leaked file — only the
  same file set and the same command. The mapping to invert is 28,339 distinct
  addresses and 25,386 distinct names over the 186 conforming corpus files.
- **Pseudonyms are NOT stable across releases with different file sets.** This is
  the cost that is easy to miss. Adding **one** file to the invocation renumbered
  **150 of 153** addresses that were present in both runs — **98.0%**.

That last number contradicts a claim now in two documents. `DESIGN.md:182` and
`LIMITATIONS.md:141` both say the scheme means "two releases diff cleanly".
Measured, that holds only while the input set is byte-identical. A second release
that adds or drops a single project renumbers almost everyone, and a diff of the
two releases is then unreadable. Whoever answers this question should also decide
whether those two lines need correcting.

### Option B: salted stable hash

`author_` plus a truncated keyed hash of the lowercased value, with the salt held
privately and a reverse mapping kept locally and never published.

Consequences, reasoned from the code rather than measured, because it is not
implemented:

- **Not reversible from public inputs.** This is the whole point, and it closes
  the exposure in Option A.
- **Stable across releases.** The pseudonym depends on the value and the salt,
  not on the set of files in the invocation, so the 98.0% renumbering disappears
  and releases really do diff cleanly.
- **Needs salt custody.** A lost salt makes a future release unlinkable to a
  published one. A leaked salt makes the whole scheme worthless, and unlike
  Option A the leak is silent. This is operational work, not a code change.
- **Reproducibility becomes conditional.** An independent party can no longer
  reproduce the release from the inputs, which weakens the artifact claim a paper
  would make. Option A's byte-reproducibility is a real research asset.
- **Small code cost.** `verify_anon.py` matches `author_\d+`. A hex hash needs
  that shape widened; the rest of the release path is shape-agnostic.
- **Collision risk.** Truncation can map two people to one pseudonym. The tool
  currently fails a run when `count(distinct person_email)` drops, so a collision
  would be caught rather than published — but it would block the release, and the
  truncation length has to be chosen against 28,339 addresses.

### What is not in dispute

Both options preserve the e-mail domain, and both keep one pseudonym per person
per invocation. The partner's two settled decisions do not depend on this choice.

## Question 2 — `commit_summary`: one false positive currently fails the release

Measured on `tomaka__glutin`:

```
LEAK identity column commit_summary: 1 [['name', 'github', 'ci: bump github actions']]
```

cregit records the GitHub web-flow committer as a person named `GitHub`, so
`github` enters the name registry and becomes a scanner probe. The subject line
`ci: bump github actions` matches it. The value is not personal data, and the
tool already publishes `github.com` and `users.noreply.github.com` in the clear.

The gate is fail-closed, so this one word turns the whole run non-zero. The only
remedy the tool offers is `--null-commit-summary`, which discards all 1,168
distinct subject lines to suppress one token.

This was **deliberately not fixed** by loosening the scanner. A residue gate
tuned until it goes green stops being a gate, and this project has already been
bitten three times by checks that passed when they should not have.

Options, for the partner:

1. **Accept and ship with `--null-commit-summary`.** Safe, and throws away a
   useful column. This is what the reference implementation did.
2. **Exempt a registry name that is also a published domain label.** `github` is
   the registrable label of a domain the tool publishes on purpose, so finding
   the token reveals nothing new. Narrow and defensible, but it would also exempt
   a surname that matches a vanity domain — a contributor named `Gutwin` at
   `gutwin.org` would stop being probed for. That is a real hole.
3. **Mark non-human identities in the registry.** `GitHub`, `web-flow` and
   similar bot identities are not natural persons. Pseudonymize them as now but
   do not use them as leak probes. Most correct, and needs a curated list plus a
   rule for what counts as a bot.
4. **Split the gate.** Fail on an address-shaped residue; report a name-shaped
   residue as a reviewable count without failing. Keeps the strong half strict
   and makes the weak half honest about being advisory.

## Question 3 — the single-contributor domain tail is much larger than documented

Preserving the domain leaves a sole contributor at a rare domain identifiable.
`anonymize_parquet.py`'s docstring reports this from a three-file sample: 33 of 38
`person_domain` groups had exactly one contributor, and adds that "the ratio is
inflated by the small sample".

Measured over all 186 conforming corpus files: **5,276 of 6,228 domains have
exactly one distinct address — 84.7%.** The ratio did not fall with scale.

So the re-identification tail is a large majority of domains, not an artifact of a
small sample. The decision — publish with disclosure, or apply k-anonymity over
the domain and lose the long tail of small firms the truck-factor question is
about — is unchanged, but it should be taken against 84.7%, and the docstring's
"inflated by the small sample" hedge should be corrected either way.

## How to reproduce every number here

duckdb lives only in the devenv shell, entered from the cregit checkout:

```sh
cd /local/home/ellianco/Projects/cregit-workspace/cregit-issue61
devenv shell -- bash -c 'cd <this repo> && python -m pytest tests/test_anonymize_parquet_e2e.py'
```

The 98.0% renumbering, the 28,339 addresses and the 84.7% figure come from
ad-hoc scripts over copies of the corpus Parquets, not from committed code. They
are reproducible but not yet pinned by a test; see `docs/ANON-RESIDUE.md` for the
commands.
