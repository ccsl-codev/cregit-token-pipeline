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

# Seconds until the core limit resets, floored at 60 and capped at one hour.
sleep_until_reset() {
    local reset now wait
    reset=$(gh api rate_limit --jq '.resources.core.reset' 2>/dev/null) || reset=""
    now=$(date -u +%s)
    if [ -z "$reset" ]; then
        wait=300
    else
        wait=$(( reset - now + 15 ))
    fi
    [ "$wait" -lt 60 ] && wait=60
    [ "$wait" -gt 3600 ] && wait=3600
    say "waiting ${wait}s for the core limit to reset"
    sleep "$wait"
}

for pass in $(seq 1 "$MAX_PASSES"); do
    say "pass $pass starting"
    if python3 ./select_corpus.py enrich >>"$LOG" 2>&1; then
        say "pass $pass finished cleanly — enrichment complete"
        exit 0
    fi
    say "pass $pass exited non-zero (expected on a rate limit)"
    sleep_until_reset
done

say "gave up after $MAX_PASSES passes; enrichment is still incomplete"
exit 1
