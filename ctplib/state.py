"""The shared state of one ctp process. cmd_run and cmd_census fill it once.
Worker threads read it, because run_project gets only the project from pool.map."""

from __future__ import annotations

import threading

# The devenv environment, captured once. Concurrent `devenv shell` calls race on
# the GC root in CREGIT ("Failed to remove existing GC root").
env: dict = {}

# The run options.
opts: dict = {}

# run_id, tools and flags, copied into every ledger row.
run_info: dict = {}

# name -> (phase, started_at), for the heartbeat. Worker threads change it.
live: dict = {}

# name -> the fd of the project's lock, so the process tree of each phase holds it.
lock_fds: dict = {}

# name -> the Popen of its running phase, for the second signal.
procs: dict = {}

# Set by a signal or a broken ledger: no new project starts, running ones finish.
stop = threading.Event()
