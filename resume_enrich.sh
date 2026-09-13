#!/usr/bin/env bash
# Resume `select_corpus.py enrich` until it completes.
#
# Why this exists: one enrich pass cannot finish inside GitHub's hourly budget.
# Each new repository costs about two calls (REST metadata + a GraphQL commit
# count), and the account allows 5,000 of each per hour. So a pass caches roughly
# 2,500 new repositories per hour and then exits on RateLimited. That exit is
# correct — it caches nothing as dead — but it needs a human to restart it.
#
# This loop restarts it. It sleeps until the reported reset instant rather than a
# fixed interval, so it neither wastes an hour nor hammers a closed window.
set -u
cd "$(dirname "$0")"

LOG=.corpus-cache/resume-enrich.log
MAX_PASSES=${MAX_PASSES:-40}

say() { echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] $*" | tee -a "$LOG"; }

# Wait until a real call succeeds, not until `rate_limit` says the budget is fine.
#
# Those are different questions. Under a secondary limit every real call returns
# 403 while `gh api rate_limit` still answers 5000/5000, because that endpoint is
# exempt. Observed directly: repos/torvalds/linux returned 403 in the same second
# that rate_limit reported core 5000/5000. A loop that trusted rate_limit would
# restart immediately into a closed door.
#
# So probe with a real, cheap call and back off until it answers.
wait_until_api_answers() {
    local wait=60 probed=0
    while [ "$probed" -lt 12 ]; do
        if gh api rate_limit >/dev/null 2>&1 && gh api repos/torvalds/linux >/dev/null 2>&1; then
            say "a real API call succeeded; resuming"
            return 0
        fi
        say "API still refusing real calls; sleeping ${wait}s"
        sleep "$wait"
        probed=$(( probed + 1 ))
        wait=$(( wait * 2 ))
        [ "$wait" -gt 1800 ] && wait=1800
    done
    say "API still refusing after 12 probes; giving this pass up"
    return 1
}

for pass in $(seq 1 "$MAX_PASSES"); do
    say "pass $pass starting"
    if python3 ./select_corpus.py enrich >>"$LOG" 2>&1; then
        say "pass $pass finished cleanly — enrichment complete"
        exit 0
    fi
    say "pass $pass exited non-zero (expected on a rate limit)"
    wait_until_api_answers || exit 1
done

say "gave up after $MAX_PASSES passes; enrichment is still incomplete"
exit 1
