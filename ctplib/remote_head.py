"""--latest: pin a project to the remote head of its default branch, once per project."""

from __future__ import annotations

import json
import time
from pathlib import Path

import pin
from ctplib import console, manifest_file, project_run
from retain import state_dir

LATEST_TIMEOUT_S = 300


def latest_path(name: str) -> Path:
    return state_dir(name) / "latest.json"


def recorded_latest(name: str) -> str:
    """The commit an earlier --latest attempt chose for this project, or ""."""
    try:
        sha = json.loads(latest_path(name).read_text()).get("resolved_sha", "")
    except (OSError, ValueError, AttributeError):
        return ""
    return sha if pin.SHA_RE.match(sha) else ""


def resolve_latest(ctx: project_run.ProjectRun) -> bool:
    """--latest: pin this attempt to the remote head of the default branch.
    The first attempt keeps the choice in state/<name>/latest.json. Later attempts
    reuse it, so a resumed run stays at one commit. False on failure."""
    start = time.time()
    path = latest_path(ctx.name)
    reused = True
    try:
        rec = json.loads(path.read_text())
        if not pin.SHA_RE.match(rec.get("resolved_sha", "")):
            raise ValueError(f"no resolved_sha in {path}")
    except (OSError, ValueError):
        reused = False
        try:
            branch, sha = pin.remote_head(ctx.project["url"], timeout=LATEST_TIMEOUT_S)
        except pin.PinError as exc:
            sha, branch, error = "", "", str(exc)
        else:
            error = "" if pin.SHA_RE.match(sha) else "the remote gave no HEAD commit"
        if error:
            ctx.record("resolve", "failed", start, time.time(), detail=error)
            ctx.failed_step = "resolve"
            return False
        rec = {
            "url": ctx.project["url"],
            "snapshot_sha": ctx.project.get(manifest_file.PINNED_FIELD, ""),
            "resolved_sha": sha,
            "branch": branch,
            "resolved_at": console.now_iso(),
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(rec) + "\n")
        tmp.replace(path)
    ctx.latest = rec["resolved_sha"]
    console.say(
        f"{ctx.name} — latest: {'reusing' if reused else 'resolved'} {ctx.latest} "
        f"(snapshot {rec.get('snapshot_sha') or 'none'})"
    )
    ctx.record("resolve", "ok", start, time.time(), resolve={**rec, "reused": reused})
    return True
