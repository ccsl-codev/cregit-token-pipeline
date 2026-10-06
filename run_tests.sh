#!/usr/bin/env sh
# The test entry point CI runs: the whole suite under coverage, failing under 80%.
#   ./run_tests.sh            # whole suite
#   ./run_tests.sh -k domain  # one subset
set -eu
cd "$(dirname "$0")"

VENV=.venv/bin
if [ ! -x "$VENV/coverage" ]; then
    echo "no test environment. Create it once with:" >&2
    echo "  python3 -m venv .venv && .venv/bin/pip install -e '.[dev]'" >&2
    exit 2
fi

"$VENV/coverage" erase
"$VENV/coverage" run -m pytest "$@"
echo
"$VENV/coverage" report
