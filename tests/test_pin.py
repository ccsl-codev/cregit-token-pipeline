"""Tests for pin.py against real git repositories built in tmp_path."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

import pin

pytestmark = pytest.mark.skipif(shutil.which("git") is None, reason="git is not installed")


@pytest.fixture(autouse=True)
def own_repositories(monkeypatch):
    """Under a git hook or `git rebase --exec`, GIT_DIR names the repository being
    rebased, and these tests would commit into it. Drop every such variable."""
    for var in pin.REPO_REDIRECTS:
        monkeypatch.delenv(var, raising=False)


def sh(*args: str, cwd: Path) -> str:
    return subprocess.run(["git", *args], cwd=cwd, check=True, text=True,
                          capture_output=True).stdout.strip()


@pytest.fixture
def source(tmp_path):
    """A repository: c1 - c2 - c3 on main, a side branch off c1, and a tag on c3."""
    src = tmp_path / "src"
    src.mkdir()
    sh("init", "-q", "-b", "main", cwd=src)
    sh("config", "user.email", "t@example.org", cwd=src)
    sh("config", "user.name", "T", cwd=src)
    shas = {}
    for i in (1, 2, 3):
        (src / "f.c").write_text(f"int x = {i};\n")
        sh("add", "f.c", cwd=src)
        sh("commit", "-q", "-m", f"c{i}", cwd=src)
        shas[f"c{i}"] = sh("rev-parse", "HEAD", cwd=src)
    sh("tag", "v3", cwd=src)
    sh("checkout", "-q", "-b", "side", shas["c1"], cwd=src)
    (src / "g.c").write_text("int y;\n")
    sh("add", "g.c", cwd=src)
    sh("commit", "-q", "-m", "side", cwd=src)
    shas["side"] = sh("rev-parse", "HEAD", cwd=src)
    sh("checkout", "-q", "main", cwd=src)
    return src, shas


@pytest.fixture
def dest(tmp_path):
    return tmp_path / "out" / pin.STAGING_DIR / "proj.git"


def history(repo: Path) -> list[str]:
    return sh("rev-list", "--all", cwd=repo).split()


def test_pins_a_commit_behind_the_remote_head(source, dest):
    src, shas = source
    result = pin.prepare(f"file://{src}", shas["c2"], dest)
    assert result["checked_out_sha"] == shas["c2"]
    assert result["branch"] == "main"
    assert result["remote_head_sha"] == shas["c3"]
    assert result["remote_head_moved"] is True
    assert sh("rev-parse", "HEAD", cwd=dest) == shas["c2"]
    # One ref, and no commit after the pin: not c3, not the side branch, no tag.
    assert sh("for-each-ref", "--format=%(refname)", cwd=dest) == "refs/heads/main"
    assert set(history(dest)) == {shas["c1"], shas["c2"]}


def test_pins_a_commit_only_a_side_branch_reaches(source, dest):
    src, shas = source
    result = pin.prepare(f"file://{src}", shas["side"], dest)
    assert result["checked_out_sha"] == shas["side"]
    assert set(history(dest)) == {shas["c1"], shas["side"]}


def test_falls_back_to_all_refs_when_the_host_refuses_a_sha(source, dest, monkeypatch):
    src, shas = source
    # Protocol v0 refuses a request for an unadvertised object, like some hosts do.
    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "protocol.version")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", "0")
    result = pin.prepare(f"file://{src}", shas["c2"], dest)
    assert result["method"] == "fetch-all-refs"
    assert sh("rev-parse", "HEAD", cwd=dest) == shas["c2"]
    assert sh("for-each-ref", "--format=%(refname)", cwd=dest) == "refs/heads/main"


def test_a_missing_commit_stops_with_exit_3(source, dest):
    src, _ = source
    with pytest.raises(pin.PinError) as err:
        pin.prepare(f"file://{src}", "0" * 40, dest)
    assert err.value.exit_code == pin.EXIT_MISSING
    assert "is not on" in str(err.value)
    assert not dest.exists()


def test_an_unreachable_url_fails(tmp_path, dest):
    with pytest.raises(pin.PinError, match="cannot reach"):
        pin.prepare(f"file://{tmp_path}/nothing-here", "a" * 40, dest)


@pytest.mark.parametrize("sha", ["abc", "A" * 40, "g" * 40, ""])
def test_a_malformed_sha_is_a_usage_error(source, dest, sha):
    src, _ = source
    with pytest.raises(pin.PinError) as err:
        pin.prepare(f"file://{src}", sha, dest)
    assert err.value.exit_code == pin.EXIT_USAGE


def test_refuses_a_dest_outside_the_staging_dir(source, tmp_path):
    src, shas = source
    with pytest.raises(pin.PinError, match="refusing --dest"):
        pin.prepare(f"file://{src}", shas["c2"], tmp_path / "elsewhere.git")


def test_a_second_call_reuses_a_matching_clone(source, dest):
    src, shas = source
    pin.prepare(f"file://{src}", shas["c2"], dest)
    again = pin.prepare(f"file://{src}", shas["c2"], dest)
    assert again["reused"] is True
    assert again["checked_out_sha"] == shas["c2"]


def test_a_different_pin_replaces_the_old_clone(source, dest):
    src, shas = source
    pin.prepare(f"file://{src}", shas["c2"], dest)
    result = pin.prepare(f"file://{src}", shas["c3"], dest)
    assert result["reused"] is False
    assert sh("rev-parse", "HEAD", cwd=dest) == shas["c3"]


def test_a_leftover_tmp_dir_is_replaced(source, dest):
    src, shas = source
    tmp = dest.with_name(dest.name + ".tmp")
    tmp.mkdir(parents=True)
    (tmp / "junk").write_text("x")
    pin.prepare(f"file://{src}", shas["c2"], dest)
    assert not tmp.exists()


def test_main_writes_the_result_file(source, dest, tmp_path):
    src, shas = source
    out = tmp_path / "pin.json"
    rc = pin.main(["--url", f"file://{src}", "--commit", shas["c1"],
                   "--dest", str(dest), "--result", str(out)])
    assert rc == 0
    assert json.loads(out.read_text())["checked_out_sha"] == shas["c1"]


def test_main_returns_the_error_exit_code(source, dest, tmp_path, capsys):
    src, _ = source
    rc = pin.main(["--url", f"file://{src}", "--commit", "1" * 40,
                   "--dest", str(dest), "--result", str(tmp_path / "r.json")])
    assert rc == pin.EXIT_MISSING
    assert "FAIL" in capsys.readouterr().err
    assert not (tmp_path / "r.json").exists()


def test_git_ignores_a_gitdir_that_points_elsewhere(source, dest, monkeypatch, tmp_path):
    """pin.py must act on the clone it builds, never on a repository GIT_DIR names."""
    src, shas = source
    victim = tmp_path / "victim.git"
    sh("init", "-q", "--bare", str(victim), cwd=tmp_path)
    monkeypatch.setenv("GIT_DIR", str(victim))
    pin.prepare(f"file://{src}", shas["c2"], dest)
    monkeypatch.delenv("GIT_DIR")
    assert sh("for-each-ref", cwd=victim) == ""
    assert sh("rev-parse", "HEAD", cwd=dest) == shas["c2"]


def test_a_remote_without_a_default_branch_uses_the_fallback_name(source, dest, monkeypatch):
    src, shas = source
    monkeypatch.setattr(pin, "remote_head", lambda url: ("", ""))
    result = pin.prepare(f"file://{src}", shas["c2"], dest)
    assert result["branch"] == pin.FALLBACK_BRANCH
    assert result["remote_head_moved"] is False


def test_a_git_call_that_does_not_end_is_a_pin_error(monkeypatch):
    def slow(*args, **kwargs):
        raise pin.subprocess.TimeoutExpired(cmd="git", timeout=kwargs["timeout"])
    monkeypatch.setattr(pin.subprocess, "run", slow)
    with pytest.raises(pin.PinError, match="did not end in 5 s"):
        pin.remote_head("https://example.invalid/r.git", timeout=5)
