"""Tests for ctplib/config.py: CTP_CONFIG."""

from __future__ import annotations

import hashlib

import retain
from ctplib import (
    run_ledger,
    runtime,
    toolchain,
)

from fakes import VALID_ROW, run_args, write_manifest

# --------------------------------------------------------------------------- #
# CTP_CONFIG
# --------------------------------------------------------------------------- #


def test_config_is_forwarded_to_the_steps_when_ctp_config_is_set(monkeypatch, sandbox):
    cfg = sandbox.root / "e2e.cfg"
    cfg.write_text("[paths]\n")
    monkeypatch.setenv("CTP_CONFIG", str(cfg))
    monkeypatch.setattr(retain, "CONFIG", cfg)
    toolchain.forward_config()
    assert runtime.env["CTP_CONFIG"] == str(cfg)


def test_config_is_not_forwarded_by_default(monkeypatch):
    monkeypatch.delenv("CTP_CONFIG", raising=False)
    toolchain.forward_config()
    assert "CTP_CONFIG" not in runtime.env


def test_run_flags_name_the_config_and_the_directories(sandbox, monkeypatch):
    cfg = sandbox.root / "e2e.cfg"
    cfg.write_text("[paths]\n")
    monkeypatch.setattr(retain, "CONFIG", cfg)
    flags = run_ledger.run_flags(run_args(), write_manifest(sandbox.root, VALID_ROW))
    assert flags["config_path"] == str(cfg)
    assert flags["config_sha256"] == hashlib.sha256(b"[paths]\n").hexdigest()
    assert flags["cregit_dir"] == str(sandbox.cregit) and flags["output_dir"] == str(sandbox.out)
    monkeypatch.setattr(retain, "CONFIG", sandbox.root / "absent.cfg")
    assert run_ledger.run_flags(run_args(), sandbox.root / "manifest.tsv")["config_sha256"] == ""
