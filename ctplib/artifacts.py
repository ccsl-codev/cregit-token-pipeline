"""Hashes and ledger records of the files a project run writes."""

from __future__ import annotations

import hashlib
from pathlib import Path

import retain
from ctplib import config


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def parquet_info(name: str) -> dict:
    """Path, size, sha256 and validated row count of a project's Parquet."""
    workdir = config.OUT / name
    parquet = workdir / f"{name}-dataset.parquet"
    stamp = workdir / f"{name}.validated"
    info: dict = {"path": str(parquet)}
    try:
        info["bytes"] = parquet.stat().st_size
        info["sha256"] = sha256_file(parquet)
    except OSError as exc:
        info["error"] = f"{type(exc).__name__}: {exc}"
    try:
        kv = dict(line.split("=", 1) for line in stamp.read_text().splitlines() if "=" in line)
        info["rows"] = int(kv["rows"])
    except (OSError, KeyError, ValueError):
        info["rows"] = None
    return info


def anon_outputs(name: str) -> list[dict]:
    anon = config.OUT / name / retain.ANON_DIR
    if not anon.is_dir():
        return []
    return [
        {"path": str(f), "bytes": f.stat().st_size, "sha256": sha256_file(f)}
        for f in sorted(anon.glob("*.parquet"))
    ]
