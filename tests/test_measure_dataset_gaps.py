"""Tests for measure_dataset_gaps.py.

The defect this module exists to avoid is a false gap report, so the mode split
is the property under test. `git ls-tree -r --name-only` cannot tell a source
file from a symlink, and counting masked symlinks reported powerdns__pdns's
complete Parquet as 29% incomplete. Every test below builds a real git
repository, because the mode is a git fact and a mocked ls-tree would assert
nothing about git.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import measure_dataset_gaps as mdg


def git(repo: Path, *args: str) -> str:
    proc = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """A repository holding one regular file, one executable and one symlink."""
    work = tmp_path / "work"
    work.mkdir()
    git(work, "init", "-q", "-b", "main")
    git(work, "config", "user.email", "t@example.invalid")
    git(work, "config", "user.name", "Test")

    (work / "real.c").write_text("int main(void){return 0;}\n")
    (work / "script.c").write_text("int f(void){return 1;}\n")
    (work / "script.c").chmod(0o755)
    (work / "link.c").symlink_to("real.c")
    (work / "notsource.txt").write_text("not masked\n")

    git(work, "add", "-A")
    git(work, "commit", "-q", "-m", "one of each")
    return work


def test_symlink_is_not_a_regular_file(repo: Path) -> None:
    """The whole point: a masked symlink must not count as source."""
    regular, other = mdg.head_entries(repo / ".git")
    assert "link.c" in other
    assert "link.c" not in regular


def test_executable_source_counts_as_regular(repo: Path) -> None:
    """Mode 100755 is source. Dropping it would under-report the denominator."""
    regular, other = mdg.head_entries(repo / ".git")
    assert "script.c" in regular
    assert "script.c" not in other


def test_regular_and_other_partition_every_head_path(repo: Path) -> None:
    regular, other = mdg.head_entries(repo / ".git")
    listed = set(git(repo, "ls-tree", "-r", "HEAD", "--name-only").split())
    assert set(regular) | set(other) == listed
    assert not set(regular) & set(other)


def test_name_only_would_have_been_wrong(repo: Path) -> None:
    """Pin the bug this module was written against.

    A name-only listing puts the symlink in the same bucket as the source, so a
    difference against the Parquet reports it as a missing file.
    """
    name_only = [
        p for p in git(repo, "ls-tree", "-r", "HEAD", "--name-only").split()
        if p.endswith(".c")
    ]
    regular, _ = mdg.head_entries(repo / ".git")
    masked_regular = [p for p in regular if p.endswith(".c")]
    assert len(name_only) == 3
    assert len(masked_regular) == 2
    assert set(name_only) - set(masked_regular) == {"link.c"}


def test_missing_git_dir_returns_empty(tmp_path: Path) -> None:
    """A bad path must not raise; the caller records it as a status."""
    regular, other = mdg.head_entries(tmp_path / "absent.git")
    assert regular == []
    assert other == []


def test_gitlink_is_not_regular(repo: Path) -> None:
    """A submodule entry is mode 160000 and is not source.

    The gitlink is written straight into the index with `--cacheinfo`. That is
    the same tree entry `git submodule add` produces, without needing a second
    repository, a transport or `protocol.file.allow`.
    """
    commit = git(repo, "rev-parse", "HEAD").strip()
    git(repo, "update-index", "--add", "--cacheinfo", f"160000,{commit},sub.c")
    git(repo, "commit", "-q", "-m", "a gitlink under a masked name")

    regular, other = mdg.head_entries(repo / ".git")
    assert "sub.c" in other
    assert "sub.c" not in regular


def test_manifest_rows_skips_comments_and_short_rows(tmp_path: Path) -> None:
    m = tmp_path / "manifest.tsv"
    m.write_text(
        "# a comment\n"
        "\n"
        "alpha\thttps://x/a\tcommunity\t(?i)\\.c$\tS\n"
        "too\tshort\n"
        "beta\thttps://x/b\tfoundation\t(?i)\\.h$\tM\textra\n"
    )
    rows = list(mdg.manifest_rows(m))
    assert rows == [("alpha", "(?i)\\.c$"), ("beta", "(?i)\\.h$")]


def test_regular_modes_is_exactly_the_two_file_modes() -> None:
    """A guard: adding 120000 here would silently restore the false-gap bug."""
    assert mdg.REGULAR_MODES == frozenset({"100644", "100755"})


def test_cfg_path_resolves_a_relative_value(tmp_path: Path, monkeypatch) -> None:
    cfg = tmp_path / "pipeline.cfg"
    cfg.write_text("[paths]\noutput_dir = sub/dir\n")
    (tmp_path / "sub" / "dir").mkdir(parents=True)
    monkeypatch.setattr(mdg, "REPO", tmp_path)
    assert mdg.cfg_path("output_dir", "unused") == (tmp_path / "sub" / "dir").resolve()


def test_cfg_path_falls_back_when_the_key_is_absent(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / "pipeline.cfg").write_text("[paths]\n")
    (tmp_path / "fallback").mkdir()
    monkeypatch.setattr(mdg, "REPO", tmp_path)
    assert mdg.cfg_path("output_dir", "fallback") == (tmp_path / "fallback").resolve()


# --------------------------------------------------------------------------- #
# end to end over a real Parquet
#
# These write and read an actual Parquet, so they need the real duckdb from
# `devenv shell` and skip without it -- the same gate tests/test_consolidate.py
# and tests/test_backfill_rust_tokens.py use.
# --------------------------------------------------------------------------- #

try:                                          # pragma: no cover - import plumbing
    import duckdb as _duckdb
except ModuleNotFoundError:                   # pragma: no cover - import plumbing
    _duckdb = None

# An import check alone is NOT enough. tests/test_consolidate.py installs a stub
# `duckdb` module into sys.modules, and it sorts before this file, so by the time
# this module is imported the name resolves to a stub whose every call raises.
# Detect the stub the way tests/test_consolidate.py labels it.
_STUBBED = getattr(sys.modules.get("duckdb"), "__doc__", "") or ""
requires_duckdb = pytest.mark.skipif(
    _duckdb is None or _STUBBED.startswith("Test stub"),
    reason="needs real duckdb (devenv shell) to write parquet")


def _corpus(tmp_path: Path, present: list[str]) -> tuple[Path, Path]:
    """Build one project: a bare original repo and a Parquet holding `present`.

    The work tree carries `keep.c`, `gone.c`, `link.c` (a symlink) and
    `skip.txt`. Only the paths in `present` get a Parquet row, so the caller
    decides what counts as missing.
    """
    out = tmp_path / "corpus-files"
    slug = "acme__widget"
    proj = out / slug
    proj.mkdir(parents=True)

    work = tmp_path / "work"
    work.mkdir()
    git(work, "init", "-q", "-b", "main")
    git(work, "config", "user.email", "t@example.invalid")
    git(work, "config", "user.name", "Test")
    (work / "keep.c").write_text("int keep;\n")
    (work / "gone.c").write_text("int gone;\n")
    (work / "link.c").symlink_to("keep.c")
    (work / "skip.txt").write_text("unmasked\n")
    git(work, "add", "-A")
    git(work, "commit", "-q", "-m", "seed")
    git(work, "clone", "-q", "--bare", str(work), str(proj / f"{slug}-original.git"))

    con = _duckdb.connect()
    values = ", ".join(f"('{p}')" for p in present)
    con.execute(
        f"copy (select * from (values {values}) as t(file_path)) "
        f"to '{proj / f'{slug}-dataset.parquet'}' (format parquet)"
    )
    con.close()

    manifest = tmp_path / "manifest.tsv"
    manifest.write_text(
        f"# name\turl\tcategory\tfile_filter\tsize_class\n"
        f"{slug}\thttps://x/w\tcommunity\t(?i)\\.(c|h)$\tS\n"
    )
    return manifest, out


@requires_duckdb
def test_main_counts_one_missing_file(tmp_path: Path, capsys) -> None:
    manifest, out = _corpus(tmp_path, ["keep.c"])
    rc = mdg.main(["--manifest", str(manifest), "--out-dir", str(out)])
    text = capsys.readouterr().out
    assert rc == 0
    assert "mask-selected regular files   : 2" in text
    assert "mask-selected non-regular     : 1" in text
    assert "missing from the Parquet      : 1" in text
    assert "projects measured             : 1" in text


@requires_duckdb
def test_main_reports_no_gap_when_the_parquet_is_complete(tmp_path: Path, capsys) -> None:
    """The symlink must not be reported as missing. This is the whole defect."""
    manifest, out = _corpus(tmp_path, ["keep.c", "gone.c"])
    rc = mdg.main(["--manifest", str(manifest), "--out-dir", str(out)])
    text = capsys.readouterr().out
    assert rc == 0
    assert "missing from the Parquet      : 0" in text
    assert "missing share                 : 0.000%" in text


@requires_duckdb
def test_main_writes_the_missing_paths_to_json(tmp_path: Path) -> None:
    import json

    manifest, out = _corpus(tmp_path, ["keep.c"])
    detail = tmp_path / "gaps.json"
    assert mdg.main(
        ["--manifest", str(manifest), "--out-dir", str(out), "--json", str(detail)]
    ) == 0
    rows = json.loads(detail.read_text())
    assert len(rows) == 1
    assert rows[0]["status"] == "ok"
    assert rows[0]["missing_paths"] == ["gone.c"]


@requires_duckdb
def test_main_records_a_project_with_no_parquet(tmp_path: Path, capsys) -> None:
    """A missing Parquet must be named, never counted as zero gaps."""
    manifest, out = _corpus(tmp_path, ["keep.c"])
    (out / "acme__widget" / "acme__widget-dataset.parquet").unlink()
    rc = mdg.main(["--manifest", str(manifest), "--out-dir", str(out)])
    text = capsys.readouterr().out
    assert rc == 0
    assert "NO-PARQUET" in text
    assert "projects measured             : 0" in text
