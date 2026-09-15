# Shell helpers for watching a corpus run. Source this from ~/.zshrc:
#
#     source ~/Projects/cregit-token-pipeline/corpus-progress.sh
#
# Then:
#     cprog          progress bar, last 5 finished, ETA
#     cprog 15       progress bar, last 15 finished
#     cwatch         the same, refreshed every 30 s
#     ctail          follow the live run log
#
# CTP_MANIFEST and CTP_JOBS override the defaults. Set CTP_JOBS to the --jobs
# the run actually uses, or the ETA is wrong.

export CTP_DIR="${CTP_DIR:-$HOME/Projects/cregit-token-pipeline}"
export CTP_MANIFEST="${CTP_MANIFEST:-manifest.phase1-sm.tsv}"
export CTP_JOBS="${CTP_JOBS:-2}"

cprog() {
    ( cd "$CTP_DIR" && ./ctp.py progress \
        --manifest "$CTP_MANIFEST" \
        --jobs "$CTP_JOBS" \
        --last "${1:-5}" )
}

# Refresh every N seconds (default 30, matching the runner's heartbeat).
cwatch() {
    local every="${1:-30}"
    while true; do
        clear
        cprog 8
        printf '\n(refreshing every %ss — Ctrl-C to stop)\n' "$every"
        sleep "$every"
    done
}

ctail() {
    ( cd "$CTP_DIR" && tail -f corpus-*.log )
}
