"""Paths and settings read from pipeline.cfg, or from the file that CTP_CONFIG names."""

from __future__ import annotations

import configparser
from pathlib import Path

import retain

# .resolve() canonicalizes to /local/home form — blobExec's meta table refuses
# resume if the command path string drifts (/home vs /local/home symlink alias).
CORPUS = Path(__file__).resolve().parent.parent
_cfg = configparser.ConfigParser()
# pipeline.cfg in the repository root, or the file CTP_CONFIG names.
# retain and consolidate read the same file.
_cfg.read(retain.CONFIG)


def _cfg_path(key: str, default: str) -> Path:
    raw = _cfg.get("paths", key, fallback=default)
    return (CORPUS / Path(raw).expanduser()).resolve()


CREGIT = _cfg_path("cregit_dir", "../cregit-workspace/cregit")
OUT = _cfg_path("output_dir", "../cregit-workspace/corpus-files")
# Not created at import: cmd_run and cmd_db create it when they need it.
METRICS = CORPUS / "metrics.tsv"
RUNS_LOG = CORPUS / "runs.log"
# The audit ledger: one JSON row per project and per step, append-only.
LEDGER = CORPUS / "ledger.jsonl"
# A separate ledger, because metrics.tsv's seven-field row is a published contract.
RESOURCES = CORPUS / "resources.tsv"

DISK_FLOOR_GB = 150
