#!/usr/bin/env sh
# Auditable test entry point. One command, deterministic, no network.
#
#   ./run_tests.sh            # whole suite + coverage report, fails under 80%
#   ./run_tests.sh -k domain  # same, one subset
#
# Tests that need a live network or an authenticated `gh` carry the `network`
# marker and are deselected here, so the result does not depend on the internet
# or on a rate-limit window.
set -eu
cd "$(dirname "$0")"

VENV=.venv/bin
if [ ! -x "$VENV/coverage" ]; then
    echo "no test environment. Create it once with:" >&2
    echo "  python3 -m venv .venv && .venv/bin/pip install pytest coverage" >&2
    exit 2
fi

"$VENV/coverage" erase
"$VENV/coverage" run -m pytest -m "not network" "$@"
echo
"$VENV/coverage" report
