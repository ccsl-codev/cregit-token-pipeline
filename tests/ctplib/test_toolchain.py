"""Tests for ctplib/toolchain.py: the devenv capture."""

from __future__ import annotations

import subprocess
from types import SimpleNamespace

import pytest

from ctplib import (
    toolchain,
)

# --------------------------------------------------------------------------- #
# devenv capture
# --------------------------------------------------------------------------- #


def test_capture_devenv_env_returns_the_parsed_environment(monkeypatch):
    """Captured once and shared by every worker: concurrent `devenv shell`
    invocations race on the GC root in CREGIT."""

    def fake_run(args, **kwargs):
        assert args[0] == "bash"
        assert kwargs["capture_output"] and kwargs["text"]
        return SimpleNamespace(returncode=0, stdout='noise\n{"PATH": "/nix/bin"}\n', stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    assert toolchain.capture_devenv_env() == {"PATH": "/nix/bin"}


def test_capture_devenv_env_exits_when_devenv_fails(monkeypatch):
    """Without the environment nothing can run, so fail loudly and early."""
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *a, **k: SimpleNamespace(
            returncode=1, stdout="", stderr='error: not an absolute path: "nix"'
        ),
    )
    with pytest.raises(SystemExit) as exc:
        toolchain.capture_devenv_env()
    assert "devenv environment capture failed" in str(exc.value)
