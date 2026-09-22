#!/usr/bin/env bash
# Re-blame a list of already-finished projects with copy detection enabled.
#
# This runs steps 7-10 only. It does NOT run step 2, so it needs neither the
# tokenizer fix nor the crash denylist: the crash gate lives inside step 2, and
# run_step returns early below FROM_STEP. Projects that also need Rust
# re-tokenization must NOT go through this script — they need a step-2 pass, which
# is a different wave.
#
# Three things this script exists to get right, each of which cost a wasted run:
#
#   * --reblame must be passed. blameRepoFiles.pl skips any file whose .blame
#     output already exists, so step 7 without it reports every file as already
#     done and exits 0. Measured: 0 of 15,036,195 tokens changed.
#   * the provenance sidecars must be passed. ctp.py refuses without them, because
#     32 of 70 columns would be blank on every row and validate.py cannot tell a
#     blank column from genuinely unknown provenance.
#   * the .validated stamp must be removed, or ctp.py skips the project. Stamps are
#     copied out first, so a failed run can be rolled back rather than leaving
#     projects unpublished.
#
# Warning: this removes each project's .validated stamp before the run. A project
# whose run then fails stays unpublished until it is re-run or its stamp restored
# from $STAMPS.
set -uo pipefail

CTP=/local/home/ellianco/Projects/cregit-token-pipeline
CORPUS=/local/home/ellianco/Projects/cregit-workspace/corpus-files
BACKUPS=/local/home/ellianco/Projects/cregit-workspace/parquet-backups/pre-reblame
CREGIT=/local/home/ellianco/Projects/cregit-workspace/cregit-issue61

# Step 10 needs python3 with the duckdb module, which only the cregit devenv
# supplies. Without it run_pipeline_process.sh SKIPS step 10 and still exits 0, so
# a long run ends with no Parquet.
CREGIT_PY3=/nix/store/3n4qphl9s728sz8frmpqqrv9b1m87g68-python3-3.14.7/bin/python3
CREGIT_PY3_SITE=/nix/store/5gnikzfb2hkysq9d45mjl74h7vj4c5ad-python3-3.14.7-env/lib/python3.14/site-packages
export PATH="$(dirname "$CREGIT_PY3"):$PATH"
export PYTHONPATH="${CREGIT_PY3_SITE}${PYTHONPATH:+:$PYTHONPATH}"

SLUGS=${1:?usage: reblame_wave.sh <slugs-file> [extra ctp.py args]}
shift || true

JOBS=${JOBS:-3}
BLAME_JOBS=${BLAME_JOBS:-4}
MEMORY_LIMIT=${MEMORY_LIMIT:-2GB}
MANIFEST=${MANIFEST:-manifest.sample.tsv}

die() { echo "reblame_wave: $*" >&2; exit 1; }

command -v python3 >/dev/null || die "no python3 on PATH"
python3 -c 'import duckdb' 2>/dev/null \
    || die "the python3 on PATH lacks duckdb; step 10 would be skipped silently"

# The checkout the pipeline actually runs is named by pipeline.cfg, not by this
# script. Read it the way ctp.py does rather than trusting $CREGIT above.
RUN_CREGIT=$(cd "$CTP" && python3 - <<'PY'
import configparser, pathlib
c = configparser.ConfigParser(); c.read("pipeline.cfg")
print((pathlib.Path(".").resolve() /
       c.get("paths", "cregit_dir", fallback="../cregit-workspace/cregit")).resolve())
PY
)
[ -d "$RUN_CREGIT" ] || die "pipeline.cfg names a checkout that does not exist: $RUN_CREGIT"

# Refuse if the code that will run does not actually have copy detection on. A
# re-blame against plain git blame costs the same hours and changes nothing.
grep -q "blame -C100" "$RUN_CREGIT/blameRepo/formatBlame.pl" \
    || die "$RUN_CREGIT/blameRepo/formatBlame.pl does not pass -C100; nothing to re-blame for"
grep -q -- "--reblame" "$RUN_CREGIT/run_pipeline_process.sh" \
    || die "$RUN_CREGIT/run_pipeline_process.sh has no --reblame; step 7 would skip every file"

mapfile -t MEMBERS < <(grep -v '^[[:space:]]*$' "$SLUGS" | grep -v '^#')
[ "${#MEMBERS[@]}" -gt 0 ] || die "no members in $SLUGS"

# ---------------------------------------------------------------------------
# Preflight: every member must be resumable at step 7 AND have a Parquet snapshot,
# because the snapshot is the only copy of the attribution being replaced.
# ---------------------------------------------------------------------------
BAD=()
for s in "${MEMBERS[@]}"; do
    miss=""
    [ -d "$CORPUS/$s/$s-cregit" ]        || miss="$miss cregit-clone"
    [ -f "$CORPUS/$s/$s-cregit.db" ]     || miss="$miss cregit-db"
    [ -d "$CORPUS/$s/blame" ]            || miss="$miss blame-dir"
    [ -f "$CORPUS/$s/$s.validated" ]     || miss="$miss validated"
    [ -f "$BACKUPS/$s-dataset.parquet" ] || miss="$miss parquet-snapshot"
    [ -n "$miss" ] && BAD+=("$s:$miss")
done
if [ "${#BAD[@]}" -gt 0 ]; then
    printf 'reblame_wave: %s\n' "${BAD[@]}" >&2
    die "${#BAD[@]} member(s) are not ready; fix or drop them before starting"
fi

# A project holding a tokenized .rs blob needs step 2 again, and that pass runs
# steps 7-10 for it anyway. Blaming it here costs the full ~8x step-7 price twice.
# The test is a blob_map row whose path ends .rs and whose new_blob differs from
# orig_blob; an identity row was never tokenized, so it is not work.
RUSTY=$(python3 - "${MEMBERS[@]}" <<PY
import sqlite3, sys
from pathlib import Path
corpus = Path("$CORPUS")
out = []
for slug in sys.argv[1:]:
    db = corpus / slug / f"{slug}-blobmap.db"
    if not db.is_file():
        continue
    # immutable=1, not mode=ro: a read-only open of a WAL database still creates
    # the -shm and -wal sidecars, so the probe writes into what it inspects.
    con = sqlite3.connect(f"file:{db}?immutable=1", uri=True)
    try:
        if con.execute("SELECT 1 FROM blob_map WHERE new_blob <> orig_blob "
                       "AND lower(path) LIKE '%.rs' LIMIT 1").fetchone():
            out.append(slug)
    except sqlite3.Error:
        out.append(slug + "(unreadable)")
    finally:
        con.close()
print(" ".join(out))
PY
)
if [ -n "$RUSTY" ]; then
    echo "reblame_wave: these members need Rust re-tokenization (step 2):" >&2
    printf '  %s\n' $RUSTY >&2
    die "drop them; they belong in the combined step-2 wave, which re-blames them too"
fi
echo "preflight ok: ${#MEMBERS[@]} members resumable at step 7, all with a Parquet"
echo "              snapshot, and none needing Rust re-tokenization"

# ---------------------------------------------------------------------------
# Stamps out, then removed. Copied BEFORE any removal, so an interrupt between the
# two leaves every stamp either in place or backed up.
# ---------------------------------------------------------------------------
STAMPS=/local/home/ellianco/Projects/cregit-workspace/parquet-backups/stamps-reblame-$(date +%Y%m%dT%H%M%S)
mkdir -p "$STAMPS" || die "cannot create $STAMPS"
for s in "${MEMBERS[@]}"; do
    cp -p "$CORPUS/$s/$s.validated" "$STAMPS/$s.validated" || die "cannot back up $s.validated"
done
n=$(find "$STAMPS" -name '*.validated' | wc -l)
[ "$n" = "${#MEMBERS[@]}" ] || die "backed up $n stamps, expected ${#MEMBERS[@]}"
echo "stamps backed up: $n at $STAMPS"
for s in "${MEMBERS[@]}"; do rm -f "$CORPUS/$s/$s.validated"; done
echo "stamps removed; restore with: cp -p $STAMPS/*.validated into each project dir"

# ---------------------------------------------------------------------------
ONLY=$(IFS=,; echo "${MEMBERS[*]}")
echo "starting: jobs=$JOBS blame-jobs=$BLAME_JOBS memory-limit=$MEMORY_LIMIT"
cd "$CTP" || die "cannot cd $CTP"
python3 ./ctp.py run \
    --manifest "$MANIFEST" \
    --only "$ONLY" \
    --from-step 7 --reblame --skip-html \
    --jobs "$JOBS" --blame-jobs "$BLAME_JOBS" \
    --memory-limit "$MEMORY_LIMIT" \
    --project-meta project_meta.json \
    --firm-map data/affiliation.merged.csv \
    --firm-canonical data/firm_canonical.csv \
    "$@"
rc=$?
echo "ctp.py exited $rc"
echo "stamps backup kept at $STAMPS"
exit $rc
