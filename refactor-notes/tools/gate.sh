#!/usr/bin/env bash
# gate.sh REV LABEL: the identity gate of refactor/readability. One run of `ctp.py census` (clone, pipeline,
# firm, validate, schema, anonymize stub, join, cleanup), one `ctp.py run` (pipeline, firm, validate,
# schema, join, drop-memo), then the report commands. Fixed paths under run/, moved to results/LABEL.
set -uo pipefail
REV=$1; LABEL=$2
G=/local/home/ellianco/Projects/cregit-workspace/refactor-gate
R=$G/run
PY=$G/venv/bin/python
M=$G/input/manifest.libexpat.tsv
META=$G/input/project_meta.batch1.json
P=libexpat__libexpat
[ -e $R ] && { echo "run/ exists; move it away first"; exit 2; }
[ -e $G/results/$LABEL ] && { echo "results/$LABEL exists"; exit 2; }
mkdir -p $R
FLAGS="--skip-html --tokenizer-worker --blame-jobs 2 --memory-limit 2GB --project-meta $META"
for stage in census run; do
  C=$R/ctp-$stage
  git clone -q --no-checkout /local/home/ellianco/Projects/cregit-token-pipeline $C
  git -C $C checkout -q "$REV" || exit 1
  printf '[paths]\ncregit_dir = %s\noutput_dir = %s\n' $G/cregit $R/out-$stage > $R/$stage.cfg
  mkdir -p $R/out-$stage
done
H=$(git -C $R/ctp-census rev-parse HEAD)
echo "=== $(date -u +%FT%TZ) gate $LABEL start (ctp $H)"
cd $R/ctp-census
export CTP_CONFIG=$R/census.cfg
{
  for sub in "" run census status progress audit db; do echo "--- help $sub"; $PY ctp.py $sub --help; echo "rc=$?"; done
} > $R/help.txt 2>&1
$PY ctp.py census --manifest $M --workers 1 --retries 0 $FLAGS --min-free-mem-gb 10 \
  --anonymize $G/scripts/anon_stub.py > $R/census.stdout 2> $R/census.stderr
echo "census rc=$?" | tee $R/census.rc
{
  echo "--- status"; $PY ctp.py status --manifest $M; echo "rc=$?"
  echo "--- progress"; $PY ctp.py progress --manifest $M; echo "rc=$?"
  echo "--- audit"; $PY ctp.py audit $P; echo "rc=$?"
  echo "--- audit nosuch"; $PY ctp.py audit nosuch__project; echo "rc=$?"
  echo "--- db"; $PY ctp.py db --manifest $M; echo "rc=$?"
  echo "--- validate_schema"; $PY validate_schema.py $R/out-census/$P/$P-dataset.parquet; echo "rc=$?"
  echo "--- emit-contract"; $PY validate_schema.py --emit-contract $R/out-census/$P/$P-dataset.parquet; echo "rc=$?"
  echo "--- validate usage"; $PY validate.py; echo "rc=$?"
  echo "--- retain dry-run"; $PY retain.py $P; echo "rc=$?"
  echo "--- census again (all done)"; $PY ctp.py census --manifest $M --workers 1 --retries 0 $FLAGS --min-free-mem-gb 10; echo "rc=$?"
} > $R/census-reports.txt 2>&1
cd $R/ctp-run
export CTP_CONFIG=$R/run.cfg
$PY ctp.py run --manifest $M --jobs 1 --retries 0 $FLAGS --join --drop-memo > $R/run.stdout 2> $R/run.stderr
echo "run rc=$?" | tee $R/run.rc
{
  echo "--- status"; $PY ctp.py status --manifest $M; echo "rc=$?"
  echo "--- run again (all done)"; $PY ctp.py run --manifest $M --jobs 1 --retries 0 $FLAGS --join --drop-memo; echo "rc=$?"
} > $R/run-reports.txt 2>&1
echo "=== $(date -u +%FT%TZ) gate $LABEL end"
cd $G
$PY $G/scripts/normalize.py $R > $R/SHA256SUMS
mv $R $G/results/$LABEL
echo "results/$LABEL/SHA256SUMS:"; cat $G/results/$LABEL/SHA256SUMS
