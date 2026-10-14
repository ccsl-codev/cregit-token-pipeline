"""Tests for ctplib/cli.py and report.cmd_db: the dispatch of the subcommands."""

from __future__ import annotations

import argparse
import subprocess
import sys
from types import SimpleNamespace

import pytest

from ctplib import (
    cli,
    console,
    passes,
    report,
    toolchain,
)

# --------------------------------------------------------------------------- #
# cmd_db and main
# --------------------------------------------------------------------------- #


def test_cmd_db_runs_consolidate_inside_the_captured_environment(sandbox, monkeypatch):
    """consolidate.py needs duckdb, which only the devenv environment provides."""
    monkeypatch.setattr(toolchain, "capture_devenv_env", lambda: {"PATH": "/nix/bin"})
    seen = {}

    def fake_run(args, **kwargs):
        seen.update(args=list(args), **kwargs)
        return SimpleNamespace(returncode=7)

    monkeypatch.setattr(subprocess, "run", fake_run)
    assert report.cmd_db(argparse.Namespace()) == 7
    assert seen["args"] == ["python3", str(sandbox.root / "consolidate.py")]
    assert seen["cwd"] == sandbox.cregit
    assert seen["env"] == {"PATH": "/nix/bin"}


@pytest.mark.parametrize(
    "argv, owner, target",
    [
        pytest.param(["ctp.py", "run"], passes, "cmd_run", id="run"),
        pytest.param(["ctp.py", "status"], report, "cmd_status", id="status"),
        pytest.param(["ctp.py", "db"], report, "cmd_db", id="db"),
    ],
)
def test_main_dispatches_each_subcommand(monkeypatch, argv, owner, target):
    """A broken dispatch table makes the CLI silently do the wrong thing."""
    called = []
    monkeypatch.setattr(sys, "argv", argv)
    monkeypatch.setattr(owner, target, lambda args: called.append(target) or 0)
    assert cli.main() == 0
    assert called == [target]


def test_main_requires_a_subcommand(monkeypatch):
    """`./ctp.py` with no verb must not fall through to a default action."""
    monkeypatch.setattr(sys, "argv", ["ctp.py"])
    with pytest.raises(SystemExit):
        cli.main()


def test_say_prefixes_every_line_with_a_timestamp(capsys, clock):
    """Log lines are read next to metrics.tsv rows, so they need a clock."""
    console.say("jq ▶ pipeline started")
    assert capsys.readouterr().out == "[03:04:05] jq ▶ pipeline started\n"


def test_now_iso_is_utc_with_second_resolution(clock):
    """metrics.tsv rows are compared across hosts, so the stamp must be UTC."""
    assert console.now_iso() == "2026-01-02T03:04:05+00:00"
