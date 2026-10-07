#!/usr/bin/env python3
"""Prepare a bare clone that holds exactly one pinned commit and its history.
Usage: pin.py --url URL --commit SHA --dest DIR.git --result FILE.json
ctp.py runs this as the `clone` step, then hands DIR.git to the runner as --repo-url.

The clone keeps one branch, named after the remote's default branch, pointing at the
pinned commit. Every other branch and tag is deleted, so the runner tokenizes and
blames the history of that commit and nothing else.

Exit 0 writes the result JSON. Exit 3 means the commit is not on the remote. Exit 1 is
any other git failure. Exit 2 is a usage error."""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

SHA_RE = re.compile(r"^[0-9a-f]{40}$")
# The staging area ctp.py uses, under its output_dir. pin.py deletes only inside it.
STAGING_DIR = ".ctp-pinned"
FALLBACK_BRANCH = "ctp-pinned"
EXIT_FAILED = 1
EXIT_USAGE = 2
EXIT_MISSING = 3

# No credential prompt: a deleted or private repository must fail, not wait for input.
# A stalled transfer below 1 KB/s for 10 minutes is aborted rather than hanging a worker.
GIT_ENV = {"GIT_TERMINAL_PROMPT": "0", "GIT_ASKPASS": "/bin/true"}
# These point git at another repository than cwd: inside a git hook or `git rebase
# --exec` they name the caller's repository, whose refs keep_only() would delete.
REPO_REDIRECTS = ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR",
                  "GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES",
                  "GIT_NAMESPACE", "GIT_PREFIX")
GIT_OPTS = ("-c", "http.lowSpeedLimit=1000", "-c", "http.lowSpeedTime=600")


class PinError(Exception):
    """A pinned clone could not be made. exit_code says why."""

    def __init__(self, message: str, exit_code: int = EXIT_FAILED):
        super().__init__(message)
        self.exit_code = exit_code


def log(msg: str) -> None:
    print(f"[pin {datetime.now(timezone.utc):%H:%M:%S}] {msg}", flush=True)


def git(args: list[str], cwd: Path | None = None, check: bool = True,
        timeout: float | None = None) -> subprocess.CompletedProcess:
    env = {k: v for k, v in os.environ.items() if k not in REPO_REDIRECTS}
    env.update(GIT_ENV)
    cmd = ["git", *GIT_OPTS, *args]
    log("$ " + " ".join(cmd))
    try:
        proc = subprocess.run(cmd, cwd=cwd, env=env, text=True, timeout=timeout,
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except subprocess.TimeoutExpired as exc:
        raise PinError(f"git {args[0]} did not end in {timeout:.0f} s") from exc
    if proc.stdout:
        print(proc.stdout, end="" if proc.stdout.endswith("\n") else "\n", flush=True)
    if check and proc.returncode != 0:
        raise PinError(f"git {args[0]} failed (exit {proc.returncode})")
    return proc


def remote_head(url: str, timeout: float | None = None) -> tuple[str, str]:
    """(default branch name, its sha) from `git ls-remote --symref`, or ("", "")."""
    proc = git(["ls-remote", "--symref", url, "HEAD"], check=False, timeout=timeout)
    if proc.returncode != 0:
        raise PinError(f"cannot reach {url}: git ls-remote exit {proc.returncode}")
    branch, sha = "", ""
    for line in proc.stdout.splitlines():
        m = re.match(r"^ref: refs/heads/(\S+)\tHEAD$", line)
        if m:
            branch = m.group(1)
        m = re.match(r"^([0-9a-f]{40})\tHEAD$", line)
        if m:
            sha = m.group(1)
    return branch, sha


def has_commit(repo: Path, sha: str) -> bool:
    return git(["cat-file", "-e", f"{sha}^{{commit}}"], cwd=repo, check=False).returncode == 0


def fetch_pinned(repo: Path, url: str, sha: str) -> str:
    """Fetch the commit into repo. Returns how: "fetch-by-sha" or "fetch-all-refs"."""
    if git(["fetch", "--no-tags", url, sha], cwd=repo, check=False).returncode == 0 \
            and has_commit(repo, sha):
        return "fetch-by-sha"
    # A host that refuses a request by SHA still serves its branches and tags.
    log("fetch by SHA refused or incomplete; fetching every branch and tag instead")
    git(["fetch", "--no-tags", url, "+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*"],
        cwd=repo)
    if not has_commit(repo, sha):
        raise PinError(f"commit {sha} is not on {url}: the host refused it by SHA, and no "
                       "branch or tag reaches it. The frame may be stale, or history was "
                       "rewritten.", EXIT_MISSING)
    return "fetch-all-refs"


def keep_only(repo: Path, branch: str, sha: str) -> None:
    """Point refs/heads/<branch> and HEAD at sha, and delete every other ref."""
    refs = git(["for-each-ref", "--format=%(refname)"], cwd=repo).stdout.split()
    for ref in refs:
        git(["update-ref", "-d", ref], cwd=repo)
    git(["update-ref", f"refs/heads/{branch}", sha], cwd=repo)
    git(["symbolic-ref", "HEAD", f"refs/heads/{branch}"], cwd=repo)
    (repo / "FETCH_HEAD").unlink(missing_ok=True)


def checked_out(repo: Path) -> tuple[str, list[str]]:
    """(HEAD sha, every ref name) of repo."""
    head = git(["rev-parse", "HEAD^{commit}"], cwd=repo).stdout.strip()
    refs = git(["for-each-ref", "--format=%(refname)"], cwd=repo).stdout.split()
    return head, refs


def check_dest(dest: Path) -> None:
    if dest.parent.name != STAGING_DIR or not dest.name.endswith(".git"):
        raise PinError(f"refusing --dest {dest}: it must be <output_dir>/{STAGING_DIR}/<name>.git",
                       EXIT_USAGE)


def reusable(dest: Path, sha: str) -> dict | None:
    """The recorded result when dest already holds exactly this pin, else None."""
    record = dest / "ctp-pin.json"
    try:
        result = json.loads(record.read_text())
        head, refs = checked_out(dest)
    except (OSError, ValueError, PinError):
        return None
    if result.get("pinned_sha") == sha and head == sha and len(refs) == 1:
        return result
    return None


def prepare(url: str, sha: str, dest: Path) -> dict:
    """Make dest a bare clone holding only sha's history. Returns the result record."""
    if not SHA_RE.match(sha):
        raise PinError(f"--commit {sha!r} is not a 40-character lowercase hex SHA", EXIT_USAGE)
    check_dest(dest)
    found = reusable(dest, sha)
    if found:
        log(f"reusing {dest}: it already holds {sha}")
        return {**found, "reused": True}

    branch, remote_sha = remote_head(url)
    tmp = dest.with_name(dest.name + ".tmp")
    for stale in (tmp, dest):
        if stale.exists():
            log(f"removing stale {stale}")
            shutil.rmtree(stale)
    tmp.parent.mkdir(parents=True, exist_ok=True)
    git(["init", "--quiet", "--bare", str(tmp)])
    method = fetch_pinned(tmp, url, sha)
    keep_only(tmp, branch or FALLBACK_BRANCH, sha)
    head, refs = checked_out(tmp)
    if head != sha or refs != [f"refs/heads/{branch or FALLBACK_BRANCH}"]:
        raise PinError(f"after pinning, HEAD is {head} with refs {refs}; expected {sha} alone")
    result = {
        "url": url,
        "pinned_sha": sha,
        "checked_out_sha": head,
        "branch": branch or FALLBACK_BRANCH,
        "remote_default_branch": branch,
        "remote_head_sha": remote_sha,
        "remote_head_moved": bool(remote_sha) and remote_sha != sha,
        "method": method,
        "dest": str(dest),
        "reused": False,
    }
    (tmp / "ctp-pin.json").write_text(json.dumps(result, indent=1) + "\n")
    tmp.rename(dest)
    log(f"pinned {url} at {sha} ({method}); remote HEAD is {remote_sha or 'unknown'}")
    return result


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--url", required=True)
    ap.add_argument("--commit", required=True)
    ap.add_argument("--dest", required=True, type=Path)
    ap.add_argument("--result", required=True, type=Path)
    args = ap.parse_args(argv)
    try:
        result = prepare(args.url, args.commit, args.dest.resolve())
    except PinError as exc:
        print(f"FAIL {exc}", file=sys.stderr, flush=True)
        return exc.exit_code
    args.result.write_text(json.dumps(result, indent=1) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
