"""One attempt at one project: its ledger context, its outcome, and run_step."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import StrEnum

import ledger
from ctplib import artifacts, config, console, manifest_file, phases, runtime


class RunOutcome(StrEnum):
    """The result of one run_project call. Values match the strings already
    written to metrics.tsv and runs.log."""

    DONE = "done"
    DONE_DIRTY = "done-dirty"
    SKIPPED = "skipped"
    FAILED = "failed"
    DEFERRED = "deferred"

    @property
    def is_success(self) -> bool:
        """The Parquet is valid. done-dirty is a success that left work files behind."""
        return self in (RunOutcome.DONE, RunOutcome.DONE_DIRTY, RunOutcome.SKIPPED)


@dataclass
class ProjectRun:
    """One attempt at one project, and the context every ledger row carries."""

    project: dict
    queue_pos: int | None = None
    queue_total: int | None = None
    attempt: int = 1
    checked_out_sha: str = ""
    latest: str = ""  # --latest: the remote head this attempt pins, else ""
    parquet: dict | None = None
    failed_step: str = ""
    started: float = field(default_factory=time.time)

    @property
    def name(self) -> str:
        return self.project["name"]

    @property
    def pinned(self) -> str:
        return self.latest or self.project.get(manifest_file.PINNED_FIELD, "")

    def base(self) -> dict:
        mask = runtime.opts.get("mask") or self.project["file_filter"]
        return {
            "v": ledger.VERSION,
            "run_id": runtime.run_info.get("run_id", ""),
            "project": self.name,
            "queue_pos": self.queue_pos,
            "queue_total": self.queue_total,
            "attempt": self.attempt,
            "manifest_row": dict(self.project),
            "pinned_sha": self.pinned or manifest_file.UNPINNED,
            # The manifest commit, when --latest pinned another one.
            **(
                {"snapshot_sha": self.project.get(manifest_file.PINNED_FIELD, "")}
                if self.latest
                else {}
            ),
            "checked_out_sha": self.checked_out_sha,
            "file_mask_sha256": artifacts.sha256_text(mask),
            "tools": runtime.run_info.get("tools", {}),
            "flags": runtime.run_info.get("flags", {}),
        }

    def record(self, step: str, status: str, start: float, end: float, **extra) -> None:
        row = self.base()
        row.update(
            step=step,
            status=status,
            start_utc=console.utc(start),
            end_utc=console.utc(end),
            duration_s=round(end - start, 3),
        )
        row.update(extra)
        try:
            ledger.append(config.LEDGER, row)
        except OSError as exc:
            # The audit trail is the point of the run: stop handing out new projects.
            runtime.run_info["ledger_broken"] = f"{type(exc).__name__}: {exc}"
            console.say(f"{self.name} ✗ LEDGER WRITE FAILED ({exc}); the run stops dispatching")

    def finish(self, state: str, **extra) -> RunOutcome:
        if state == ledger.FAILED:
            extra.setdefault("failed_step", self.failed_step)
        if self.parquet is not None:
            extra.setdefault("parquet", self.parquet)
        self.record(ledger.FINAL_STEP, state, self.started, time.time(), state=state, **extra)
        return RunOutcome(state)


def run_step(ctx: ProjectRun, step: str, argv: list[str], after=None) -> int:
    """Run one phase and append its ledger rows: one when it starts, so a crash shows
    which step was running, and one when it ends. `after(rc)` may veto a success and
    add fields: it returns (rc, extra). Re-raises OSError after recording it."""
    start = time.time()
    ctx.record(step, "started", start, start, argv=argv)
    try:
        rc = phases.run_phase(ctx.project, step, argv, keep_fds=_held_fds(ctx.name))
    except OSError as exc:
        ctx.record(
            step,
            "failed",
            start,
            time.time(),
            exit_code=None,
            argv=argv,
            detail=f"could not start: {exc}",
        )
        ctx.failed_step = step
        raise
    extra: dict = {}
    if after is not None:
        rc, extra = after(rc)
    ctx.record(
        step,
        "ok" if rc == 0 else "failed",
        start,
        time.time(),
        exit_code=rc,
        argv=argv,
        log=phases.latest_log(ctx.name, step),
        **extra,
    )
    if rc != 0:
        ctx.failed_step = step
    return rc


def _held_fds(name: str) -> tuple:
    fd = runtime.lock_fds.get(name)
    return (fd,) if fd is not None else ()
