"""The staging clone of a pinned project, and the pin.py call that makes it."""

from __future__ import annotations

from pathlib import Path

import pin
import retain
from ctplib import config, manifest_file
from retain import state_dir


def pinned_clone_path(name: str) -> Path:
    """Where the clone step stages a pinned project. Outside the workdir, because
    the runner deletes the workdir at step 1."""
    return config.OUT / pin.STAGING_DIR / f"{name}.git"


def pin_result_path(name: str) -> Path:
    return state_dir(name) / "pin.json"


def commit_url(url: str) -> str:
    """The runner's own default, from the real URL rather than the staging path."""
    return url.removesuffix(".git").rstrip("/") + "/commit/"


def pin_args(project: dict, commit: str = "") -> list[str]:
    """pin.py for this attempt: `commit` (the --latest choice) or the manifest commit."""
    name = project["name"]
    return [
        "python3",
        str(config.CORPUS / "pin.py"),
        "--url",
        project["url"],
        "--commit",
        commit or project[manifest_file.PINNED_FIELD],
        "--dest",
        str(pinned_clone_path(name)),
        "--result",
        str(pin_result_path(name)),
    ]


def drop_pinned_clone(name: str) -> list[str]:
    """Delete the staging clone. Returns the errors, empty when it is gone."""
    target = pinned_clone_path(name)
    if not target.exists() and not target.is_symlink():
        return []
    if target.is_symlink() or target.resolve().parent != (config.OUT / pin.STAGING_DIR).resolve():
        return [
            f"refusing to delete {target}: not a directory inside {config.OUT / pin.STAGING_DIR}"
        ]
    return retain.remove_tree(target)
