"""The external tools: the devenv environment of cregit, git, and the tool versions
that the ledger records."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pin
import retain
from ctplib import artifacts, config, runtime

DEVENV = Path.home() / ".nix-profile/bin/devenv"
# devenv needs the nix daemon env; a bare PATH prepend fails in non-login
# shells with: error: not an absolute path: "nix"
NIX_DAEMON_SH = "/nix/var/nix/profiles/default/etc/profile.d/nix-daemon.sh"


def capture_devenv_env() -> dict:
    """Resolve the devenv shell once and return its full environment."""
    out = subprocess.run(
        [
            "bash",
            "-c",
            f'. {NIX_DAEMON_SH}; cd "{config.CREGIT}"; exec "{DEVENV}" shell -- '
            'python3 -c "import os, json; print(json.dumps(dict(os.environ)))"',
        ],
        capture_output=True,
        text=True,
        timeout=300,
    )
    if out.returncode != 0:
        sys.exit(f"devenv environment capture failed:\n{out.stdout}\n{out.stderr}")
    return json.loads(out.stdout.splitlines()[-1])


def git_head(repo: Path) -> str:
    """HEAD of repo, or "" when repo is absent or git cannot read it."""
    if not repo.exists():
        return ""
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "HEAD^{commit}"],
            env=runtime.env or None,
            capture_output=True,
            text=True,
            timeout=120,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    out = (getattr(proc, "stdout", "") or "").strip() if proc.returncode == 0 else ""
    return out if pin.SHA_RE.match(out) else ""


def runner_checkout(name: str, workdir: Path) -> str:
    """The commit the runner checked out for blame (step 6), from its working clone."""
    return git_head(workdir / f"{name}-original")


BLOBEXEC_JAR = Path("blobExec/target/scala-2.13/blobExec-0.1.0-assembly.jar")


def _capture(cmd: list[str], cwd: Path | None = None) -> str:
    """stdout of cmd under the cregit environment, or "" when it fails."""
    try:
        proc = subprocess.run(
            cmd, cwd=cwd, env=runtime.env or None, capture_output=True, text=True, timeout=120
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    return (proc.stdout or "").strip() if proc.returncode == 0 else ""


def git_describe(repo: Path) -> str:
    """HEAD sha of repo, with -dirty when a tracked file differs from it."""
    head = _capture(["git", "-C", str(repo), "rev-parse", "HEAD"])
    if not head:
        return ""
    dirty = _capture(["git", "-C", str(repo), "status", "--porcelain", "--untracked-files=no"])
    return head + ("-dirty" if dirty else "")


def tool_versions() -> dict:
    """What produced this run's Parquets. Read once per run, after the devenv capture."""
    jar = config.CREGIT / BLOBEXEC_JAR
    srcml = (
        shutil.which("srcml", path=runtime.env.get("PATH"))
        if runtime.env
        else shutil.which("srcml")
    )
    srcml_out = _capture(["srcml", "--version"])
    return {
        "ctp_commit": git_describe(config.CORPUS),
        "cregit_commit": git_describe(config.CREGIT),
        # srcML builds from one release line print one version; the path names the build.
        "srcml_version": srcml_out.splitlines()[0] if srcml_out else "",
        "srcml_path": str(Path(srcml).resolve()) if srcml else "",
        "blobexec_jar_sha256": artifacts.sha256_file(jar) if jar.is_file() else "",
        "git_version": _capture(["git", "--version"]),
        "ctp_python": sys.version.split()[0],
    }


def forward_config() -> None:
    """A step that reads the config (the join, a rebuild) must read this run's file."""
    if os.environ.get("CTP_CONFIG"):
        runtime.env["CTP_CONFIG"] = str(retain.CONFIG)
