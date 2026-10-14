"""ctp.py is only the entry point: the code is in ctplib."""

import subprocess
import sys
from pathlib import Path

import ctp
from ctplib import cli

ROOT = Path(__file__).resolve().parent.parent


def test_ctp_exposes_the_cli_main():
    assert ctp.main is cli.main


def test_the_script_prints_the_help_text():
    out = subprocess.run(
        [sys.executable, str(ROOT / "ctp.py"), "--help"], capture_output=True, text=True
    )
    assert out.returncode == 0
    assert out.stdout.startswith("usage: ctp.py [-h] {run,census,status,progress,audit,db}")
    assert "cregit-token-pipeline runner" in out.stdout
