#!/usr/bin/env bash
# step.sh MODULE "DOC" "COMMIT SUBJECT" NAME...: one mechanical move, tidy, test, commit.
set -uo pipefail
cd /local/home/ellianco/Projects/ctp-readability
MOD=$1; DOC=$2; MSG=$3; shift 3
.venv/bin/python /tmp/clash.py "$MOD" ctp.py tests/*.py ctplib/*.py | grep . && { echo "CLASH for $MOD"; exit 1; }
.venv/bin/python /local/home/ellianco/Projects/cregit-workspace/refactor-gate/scripts/mover.py ctp.py "$MOD" --doc "$DOC" \
  --tests tests/test_ctp.py tests/test_build_manifest.py tests/test_mask_drift.py -- "$@" || exit 1
.venv/bin/ruff check --fix --select I,F401,F811 ctp.py ctplib tests >/dev/null
.venv/bin/ruff format ctp.py ctplib tests >/dev/null
.venv/bin/ruff check --output-format concise . | grep -v C901 | grep -v 'ctp.py:4:101' | grep -v '^Found'
./run_tests.sh > /tmp/t.log 2>&1; rc=$?
grep -E 'passed|failed' /tmp/t.log
[ $rc = 0 ] || { grep -E '^(FAILED|E  )' /tmp/t.log | head -20; exit 1; }
git add -A ctplib ctp.py tests && git commit -qm "$MSG" && git -P log --oneline -1
