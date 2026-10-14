"""metrics.tsv: one row per phase attempt. Its seven fields are a published contract."""

from __future__ import annotations

import threading
from pathlib import Path

from ctplib import config, console

# Worker threads append to metrics.tsv and resources.tsv under this lock.
TSV_LOCK = threading.Lock()


def record_metric(project: str, cls: str, phase: str, duration: int, rc: int, log: Path) -> None:
    with TSV_LOCK:
        if not config.METRICS.exists():
            config.METRICS.write_text("iso_start\tproject\tclass\tphase\tduration_s\trc\tlog\n")
        with config.METRICS.open("a") as f:
            f.write(f"{console.now_iso()}\t{project}\t{cls}\t{phase}\t{duration}\t{rc}\t{log}\n")
