"""Unit tests for retain.

retain.py DELETES FILES. It prunes a finished project workdir down to the
artifacts the research needs. Every test here builds its own fixture tree under
tmp_path and repoints `retain.OUT` at it, so no test can reach the real corpus.
Two invariants are asserted over and over, because a mistake in either one
destroys work that took days of compute:

  * dry run is the default -- without --apply the tree is byte-for-byte unchanged
  * only <output_dir>/<project>/{memo,html} is ever removed

The module was written by another author and is unreviewed, so the tests are
adversarial: they assume nothing and check the tree after every call.
"""

from __future__ import annotations

import configparser
import contextlib
import importlib.util
import os
import shutil
import time
from pathlib import Path

import pytest

import retain

REAL_OUT = retain.OUT


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """No test may sleep."""
    monkeypatch.setattr(time, "sleep", lambda *a, **k: None)


@pytest.fixture
def out(tmp_path, monkeypatch):
    """The output directory every test operates in. Never the real one."""
    d = (tmp_path / "corpus-files").resolve()
    d.mkdir()
    monkeypatch.setattr(retain, "OUT", d)
    # Data safety: if this ever equals the live corpus, --apply tests delete it.
    assert retain.OUT != REAL_OUT
    assert retain.OUT.is_relative_to(tmp_path.resolve())
    return d


def make_project(out: Path, name: str, *, parquet: bytes | None = b"PAR1data",
                 stamp: bytes | None = b"validated\n",
                 memo: bool = True, html: bool = True) -> Path:
    """A project workdir shaped like the pipeline leaves it.

    parquet=None or stamp=None omits that file; b"" writes it empty.
    """
    workdir = out / name
    workdir.mkdir(parents=True)
    if parquet is not None:
        (workdir / f"{name}-dataset.parquet").write_bytes(parquet)
    if stamp is not None:
        (workdir / f"{name}.validated").write_bytes(stamp)
    (workdir / "metrics.tsv").write_text("project\ttokens\n" + name + "\t42\n")
    (workdir / "runs.log").write_text("run ok\n")
    if memo:
        (workdir / "memo").mkdir()
        (workdir / "memo" / "blob0001").write_bytes(b"m" * 5000)
        (workdir / "memo" / "deep").mkdir()
        (workdir / "memo" / "deep" / "blob0002").write_bytes(b"n" * 9000)
    if html:
        (workdir / "html").mkdir()
        (workdir / "html" / "index.html").write_bytes(b"<html>" * 400)
    return workdir


def snapshot(root: Path) -> dict[str, tuple]:
    """Byte-for-byte state of a tree, without following symlinks."""
    state: dict[str, tuple] = {}
    stack = [root]
    while stack:
        current = stack.pop()
        with os.scandir(current) as it:
            for entry in it:
                rel = str(Path(entry.path).relative_to(root))
                if entry.is_symlink():
                    state[rel] = ("symlink", os.readlink(entry.path))
                elif entry.is_dir(follow_symlinks=False):
                    state[rel] = ("dir",)
                    stack.append(Path(entry.path))
                else:
                    state[rel] = ("file", Path(entry.path).read_bytes())
    return state


def disk_bytes(root: Path) -> int:
    """Independent oracle for the reclaimed total: st_blocks * 512 over the
    subtree, symlinks not followed. Deliberately not retain.scan()."""
    total = root.lstat().st_blocks * 512
    for parent, dirs, files in os.walk(root, followlinks=False):
        for name in dirs + files:
            total += Path(parent, name).lstat().st_blocks * 512
    return total


def run_main(monkeypatch, *argv: str) -> int:
    monkeypatch.setattr("sys.argv", ["retain.py", *argv])
    return retain.main()


# --------------------------------------------------------------------------
# Dry run is the default
# --------------------------------------------------------------------------

def test_prune_without_apply_deletes_nothing(out):
    """DATA SAFETY. Dry run is the default. If this fails, every accidental
    invocation destroys memo/ and html/ for a project."""
    make_project(out, "jq")
    before = snapshot(out)
    reclaimed, ok = retain.prune("jq")
    assert ok is True
    assert reclaimed > 0                      # it reports, it does not delete
    assert snapshot(out) == before


def test_main_without_apply_deletes_nothing(out, monkeypatch, capsys):
    """DATA SAFETY. Same guarantee through the CLI, which is how a human runs
    it."""
    make_project(out, "jq")
    make_project(out, "zstd")
    before = snapshot(out)
    assert run_main(monkeypatch) == 0
    assert snapshot(out) == before
    stdout = capsys.readouterr().out
    assert "DRY RUN — nothing is deleted" in stdout
    assert "would delete" in stdout
    assert "nothing was deleted" in stdout


def test_dry_run_reports_the_same_total_that_apply_reclaims(out):
    """A dry run is only useful if its number is the number you get."""
    make_project(out, "jq")
    predicted, ok = retain.prune("jq")
    assert ok
    reclaimed, ok = retain.prune("jq", apply=True)
    assert ok
    assert reclaimed == predicted


# --------------------------------------------------------------------------
# --apply
# --------------------------------------------------------------------------

def test_apply_deletes_the_disposable_subtrees_and_keeps_every_keeper(out):
    """The point of the script: memo/ and html/ go, the parquet, the stamp,
    metrics.tsv and runs.log stay."""
    workdir = make_project(out, "zstd")
    reclaimed, ok = retain.prune("zstd", apply=True)
    assert ok is True
    assert reclaimed > 0
    assert not (workdir / "memo").exists()
    assert not (workdir / "html").exists()
    assert snapshot(workdir) == {
        "zstd-dataset.parquet": ("file", b"PAR1data"),
        "zstd.validated": ("file", b"validated\n"),
        "metrics.tsv": ("file", b"project\ttokens\nzstd\t42\n"),
        "runs.log": ("file", b"run ok\n"),
    }


def test_apply_only_touches_the_named_project(out):
    """One project's prune must not reach into its neighbour."""
    make_project(out, "jq")
    other = make_project(out, "libuv")
    before = snapshot(other)
    assert retain.prune("jq", apply=True)[1] is True
    assert snapshot(other) == before


def test_apply_can_prune_one_subtree_only(out):
    """ctp.py --drop-memo calls prune(name, ("memo",)) — html/ must survive."""
    workdir = make_project(out, "tmux")
    reclaimed, ok = retain.prune("tmux", ("memo",), apply=True)
    assert ok is True
    assert not (workdir / "memo").exists()
    assert (workdir / "html" / "index.html").exists()
    assert reclaimed > 0


def test_an_absent_subtree_is_not_a_failure(out, capsys):
    """A project run with --skip-html has no html/. That is normal, not a
    skip."""
    make_project(out, "jq", html=False)
    reclaimed, ok = retain.prune("jq", apply=True)
    assert ok is True
    assert reclaimed > 0
    assert "html/ absent, nothing to do" in capsys.readouterr().out


# --------------------------------------------------------------------------
# The "finished" guard — one test per way a project can be unfinished
# --------------------------------------------------------------------------

@pytest.mark.parametrize("kwargs, expect", [
    ({"parquet": None}, "missing keeper"),
    ({"parquet": b""}, "empty keeper"),
    ({"stamp": None}, "missing or empty ctp stamp"),
    ({"stamp": b""}, "missing or empty ctp stamp"),
])
def test_an_unfinished_project_is_skipped_and_nothing_is_deleted(out, capsys,
                                                                kwargs, expect):
    """DATA SAFETY. An unfinished project still needs memo/ to resume. Deleting
    it costs a full re-tokenization of that project. --apply is passed here on
    purpose: the guard must hold in the dangerous mode."""
    make_project(out, "jq", **kwargs)
    before = snapshot(out)
    reclaimed, ok = retain.prune("jq", apply=True)
    assert (reclaimed, ok) == (0, False)
    assert snapshot(out) == before
    log = capsys.readouterr().out
    assert "SKIP: not finished" in log and expect in log


def test_a_missing_workdir_is_skipped_and_nothing_is_deleted(out, capsys):
    """DATA SAFETY. A typo in a project name must not do anything at all."""
    make_project(out, "jq")
    before = snapshot(out)
    reclaimed, ok = retain.prune("typo", apply=True)
    assert (reclaimed, ok) == (0, False)
    assert snapshot(out) == before
    assert "SKIP: not finished (no workdir)" in capsys.readouterr().out


def test_a_workdir_that_is_a_file_is_skipped(out):
    """`is_dir()` on a stray file must not be read as a project."""
    (out / "jq").write_text("not a directory")
    assert retain.prune("jq", apply=True) == (0, False)


@pytest.mark.parametrize("kwargs, ok", [
    ({}, True),
    ({"parquet": None}, False),
    ({"parquet": b""}, False),
    ({"stamp": None}, False),
    ({"stamp": b""}, False),
])
def test_finished_agrees_with_prune(out, kwargs, ok):
    """finished() is the single definition of done; prune must not have its
    own."""
    make_project(out, "jq", **kwargs)
    assert retain.finished("jq")[0] is ok


def test_finished_reports_the_kept_parquet_size(out):
    """The log line must name what is being kept, so a run can be audited."""
    make_project(out, "jq", parquet=b"x" * 2048)
    ok, why = retain.finished("jq")
    assert ok is True
    assert "jq-dataset.parquet" in why and "2.0 KiB" in why


# --------------------------------------------------------------------------
# Exit status
# --------------------------------------------------------------------------

def test_exit_status_is_zero_when_every_project_succeeds(out, monkeypatch):
    """A clean corpus-wide run must be scriptable."""
    make_project(out, "jq")
    make_project(out, "zstd")
    assert run_main(monkeypatch, "--apply") == 0


def test_exit_status_is_non_zero_when_any_project_is_skipped(out, monkeypatch,
                                                            capsys):
    """A skipped project must fail the run, or a cron job silently leaves the
    disk full."""
    make_project(out, "jq")
    make_project(out, "half", parquet=None)
    assert run_main(monkeypatch, "jq", "half") == 1
    log = capsys.readouterr().out
    assert "SKIPPED 1 project(s), not finished: half" in log


def test_a_skip_does_not_stop_the_other_projects(out, monkeypatch):
    """One bad project must not abandon the rest of the corpus."""
    good = make_project(out, "jq")
    make_project(out, "half", parquet=None)
    assert run_main(monkeypatch, "half", "jq", "--apply") == 1
    assert not (good / "memo").exists()
    assert (out / "half" / "memo").exists()


def test_main_with_no_finished_projects_returns_zero(out, monkeypatch, capsys):
    """An empty corpus is not an error."""
    make_project(out, "running", stamp=None)
    assert run_main(monkeypatch) == 0
    assert "no finished projects" in capsys.readouterr().out


def test_main_reports_a_missing_output_dir(out, monkeypatch, capsys):
    """A misconfigured output_dir must fail loudly, not prune nothing
    quietly."""
    monkeypatch.setattr(retain, "OUT", out / "does-not-exist")
    assert run_main(monkeypatch) == 1
    assert "output_dir does not exist" in capsys.readouterr().out


@pytest.mark.parametrize("bad", [Path("/"), Path("/tmp"), Path.home()])
def test_main_refuses_a_dangerous_output_dir(monkeypatch, bad):
    """DATA SAFETY. A root or home output_dir would walk the whole machine.
    Refuse before anything is read."""
    monkeypatch.setattr(retain, "OUT", bad)
    with pytest.raises(SystemExit) as exc:
        run_main(monkeypatch, "--apply")
    assert "refusing to operate on output_dir" in str(exc.value.code)


def test_finished_projects_lists_stamped_dirs_in_name_order(out):
    """The default project list comes from the stamps on disk."""
    make_project(out, "zstd")
    make_project(out, "jq")
    make_project(out, "libuv", stamp=None)
    (out / "loose-file.txt").write_text("ignored")
    assert retain.finished_projects() == ["jq", "zstd"]


def test_finished_projects_accepts_an_empty_stamp_that_prune_rejects(out):
    """DOCUMENTS AN INCONSISTENCY. finished_projects() only checks that the
    stamp file exists, while finished() also requires it to be non-empty. A
    zero-byte stamp therefore lands in the default project list and then fails
    the guard, so a whole-corpus dry run exits non-zero. See the report."""
    make_project(out, "jq", stamp=b"")
    assert retain.finished_projects() == ["jq"]
    assert retain.finished("jq")[0] is False


def test_missing_output_dir_yields_no_projects(out, monkeypatch):
    """finished_projects() must not raise when output_dir is absent."""
    monkeypatch.setattr(retain, "OUT", out / "gone")
    assert retain.finished_projects() == []


# --------------------------------------------------------------------------
# Protected paths
# --------------------------------------------------------------------------

@pytest.mark.parametrize("protected", [
    ".git", "metrics.tsv", "runs.log", "ctp.duckdb",
    "jq-dataset.parquet", "jq.validated",
])
def test_a_protected_entry_inside_a_subtree_refuses_the_delete(out, capsys,
                                                              protected):
    """DATA SAFETY. If a protected name turns up inside a subtree the script
    would remove, the whole project is refused. This is the backstop against a
    future DISPOSABLE entry being wrong."""
    workdir = make_project(out, "jq")
    if protected == ".git":
        (workdir / "memo" / protected).mkdir()
    else:
        (workdir / "memo" / protected).write_text("precious")
    before = snapshot(out)
    reclaimed, ok = retain.prune("jq", apply=True)
    assert (reclaimed, ok) == (0, False)
    assert snapshot(out) == before
    log = capsys.readouterr().out
    assert "protected entry inside subtree" in log and protected in log


def test_a_protected_entry_deep_inside_a_subtree_is_found(out):
    """The walk is recursive, so depth does not hide a protected file."""
    workdir = make_project(out, "jq")
    (workdir / "memo" / "deep" / "hand.validated").write_text("x")
    before = snapshot(out)
    assert retain.prune("jq", apply=True) == (0, False)
    assert snapshot(out) == before


def test_a_refusal_in_the_first_subtree_leaves_the_second_alone(out):
    """prune returns at the first refusal. memo/ blocked means html/ is not
    even scanned — fail closed, which is the right direction."""
    workdir = make_project(out, "jq")
    (workdir / "memo" / ".git").mkdir()
    assert retain.prune("jq", ("memo", "html"), apply=True) == (0, False)
    assert (workdir / "html" / "index.html").exists()


@pytest.mark.parametrize("name, expected", [
    (".git", True), ("metrics.tsv", True), ("runs.log", True),
    ("ctp.duckdb", True), ("x-dataset.parquet", True), ("jq.validated", True),
    ("blob0001", False), ("index.html", False), ("memo", False),
    ("git", False), ("parquet", False),
])
def test_is_protected(name, expected):
    """The protected-name test is exact: a substring match would refuse every
    prune, a loose one would delete a keeper."""
    assert retain.is_protected(name) is expected


# --------------------------------------------------------------------------
# Nothing outside the output directory
# --------------------------------------------------------------------------

def test_a_traversal_project_name_is_refused(out, tmp_path, capsys):
    """DATA SAFETY. `retain.py ../evil --apply` must not delete a tree outside
    output_dir. The refusal comes from check_target, which compares the RESOLVED
    path against output_dir."""
    outside = tmp_path / "evil"
    (outside / "memo").mkdir(parents=True)
    (outside / "memo" / "precious.txt").write_text("do not delete me")
    # Make the traversal look finished, so the guard under test is the path
    # check and not the finished() check.
    (tmp_path / "evil-dataset.parquet").write_bytes(b"PAR1")
    (tmp_path / "evil.validated").write_bytes(b"ok")
    before = snapshot(outside)

    assert retain.finished("../evil")[0] is True     # finished() does not object
    reclaimed, ok = retain.prune("../evil", apply=True)

    assert (reclaimed, ok) == (0, False)
    assert snapshot(outside) == before
    assert "is outside output_dir" in capsys.readouterr().out


def test_an_absolute_project_name_is_refused(out, tmp_path, capsys):
    """DATA SAFETY. `OUT / "/abs/path"` is "/abs/path": an absolute project name
    escapes output_dir entirely and must be refused."""
    outside = tmp_path / "abs_evil"
    (outside / "memo").mkdir(parents=True)
    (outside / "memo" / "precious.txt").write_text("do not delete me")
    (tmp_path / "abs_evil-dataset.parquet").write_bytes(b"PAR1")
    (tmp_path / "abs_evil.validated").write_bytes(b"ok")
    before = snapshot(outside)

    reclaimed, ok = retain.prune(str(outside), apply=True)

    assert (reclaimed, ok) == (0, False)
    assert snapshot(outside) == before
    assert "is outside output_dir" in capsys.readouterr().out


def test_a_nested_project_name_is_refused(out):
    """DATA SAFETY. Only <output_dir>/<project>/<subtree> may be removed, so a
    subtree two levels down is refused even though it stays inside
    output_dir."""
    nested = out / "group" / "jq"
    (nested / "memo").mkdir(parents=True)
    (nested / "memo" / "blob").write_text("x")
    with pytest.raises(ValueError, match="is not <output_dir>/<project>/memo"):
        retain.check_target(nested, "memo")
    assert (nested / "memo" / "blob").exists()


def test_a_disposable_subtree_that_is_a_symlink_is_refused(out, tmp_path):
    """DATA SAFETY. If memo/ is itself a symlink, rmtree would remove the link
    but scan would have measured the target — and a swapped link could point
    anywhere. Refuse."""
    workdir = make_project(out, "jq", memo=False)
    elsewhere = tmp_path / "real-memo"
    elsewhere.mkdir()
    (elsewhere / "blob").write_text("x")
    (workdir / "memo").symlink_to(elsewhere)
    before = snapshot(elsewhere)

    with pytest.raises(ValueError, match="is a symlink"):
        retain.check_target(workdir, "memo")
    assert retain.prune("jq", apply=True) == (0, False)
    assert (workdir / "memo").is_symlink()
    assert snapshot(elsewhere) == before


@pytest.mark.parametrize("subtree", ["", ".", "..", "/", "src", "data",
                                     "memo/deep", "../memo"])
def test_check_target_rejects_a_subtree_that_is_not_disposable(out, subtree):
    """DATA SAFETY. The subtree name is an allowlist, not a suggestion."""
    workdir = make_project(out, "jq")
    with pytest.raises(ValueError, match="is not one of the disposable"):
        retain.check_target(workdir, subtree)


def test_check_target_accepts_the_two_disposable_subtrees(out):
    """The allowlist must not be so tight that the script cannot work."""
    workdir = make_project(out, "jq")
    for subtree in retain.DISPOSABLE:
        assert retain.check_target(workdir, subtree) == workdir / subtree


def test_prune_refuses_a_subtree_outside_the_allowlist_without_deleting(out):
    """A caller that passes a wrong subtree gets a skip, not a delete."""
    workdir = make_project(out, "jq")
    before = snapshot(out)
    assert retain.prune("jq", ("src",), apply=True) == (0, False)
    assert snapshot(out) == before
    assert workdir.exists()


# --------------------------------------------------------------------------
# Symlinks inside a disposable subtree
# --------------------------------------------------------------------------

def test_a_symlink_inside_a_subtree_is_not_followed_out_of_the_tree(out,
                                                                    tmp_path):
    """DATA SAFETY. A symlink in memo/ must be removed as a link only. If the
    walk followed it, a link to the corpus root would delete the corpus."""
    workdir = make_project(out, "jq")
    outside = tmp_path / "outside"
    outside.mkdir()
    # Deliberately far bigger than memo/, so following the link would show up.
    (outside / "keep.txt").write_bytes(b"k" * 400_000)
    (outside / "sub").mkdir()
    (outside / "sub" / "also-keep.txt").write_text("k")
    (workdir / "memo" / "link").symlink_to(outside)
    before = snapshot(outside)

    size, entries, violations = retain.scan(workdir / "memo")
    assert violations == []
    assert entries == 4                       # blob, deep, deep/blob, link
    assert size == disk_bytes(workdir / "memo")
    assert size < disk_bytes(outside)         # the target was not counted

    reclaimed, ok = retain.prune("jq", ("memo",), apply=True)
    assert ok is True
    assert not (workdir / "memo").exists()
    assert snapshot(outside) == before        # the target survives untouched


def test_a_symlink_to_a_protected_file_outside_is_refused_by_name(out, tmp_path):
    """The name check fires on the link itself, so a link that looks like a
    keeper still aborts the prune."""
    workdir = make_project(out, "jq")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "real.parquet").write_text("x")
    (workdir / "memo" / "jq-dataset.parquet").symlink_to(outside / "real.parquet")
    assert retain.prune("jq", ("memo",), apply=True) == (0, False)
    assert (workdir / "memo").exists()


def test_a_broken_symlink_inside_a_subtree_does_not_block_the_prune(out):
    """A dangling link is measurable with lstat, so it must not be read as an
    unreadable entry."""
    workdir = make_project(out, "jq")
    (workdir / "memo" / "dangling").symlink_to(workdir / "memo" / "gone")
    size, entries, violations = retain.scan(workdir / "memo")
    assert violations == []
    assert entries == 4
    assert retain.prune("jq", ("memo",), apply=True)[1] is True
    assert not (workdir / "memo").exists()


# --------------------------------------------------------------------------
# Reclaimed bytes
# --------------------------------------------------------------------------

def test_the_reclaimed_total_is_the_real_size_of_what_was_removed(out):
    """The disk budget is the whole reason this script exists. The number it
    prints must be the space the filesystem gives back."""
    workdir = make_project(out, "zstd")
    expected = disk_bytes(workdir / "memo") + disk_bytes(workdir / "html")
    free_before = shutil.disk_usage(out).free

    reclaimed, ok = retain.prune("zstd", apply=True)

    assert ok is True
    assert reclaimed == expected
    assert reclaimed >= 5000 + 9000 + 6 * 400        # at least the file bytes
    assert shutil.disk_usage(out).free >= free_before


def test_the_main_total_is_the_sum_over_the_projects(out, monkeypatch, capsys):
    """The corpus-wide figure must add up, or the capacity plan is wrong."""
    make_project(out, "jq")
    make_project(out, "zstd")
    expected = sum(disk_bytes(out / n / s)
                   for n in ("jq", "zstd") for s in retain.DISPOSABLE)
    dry_jq, _ = retain.prune("jq")
    dry_zstd, _ = retain.prune("zstd")
    assert dry_jq + dry_zstd == expected

    assert run_main(monkeypatch, "--apply") == 0
    log = capsys.readouterr().out
    assert f"TOTAL reclaimed {retain.human(expected)} over 2 project(s)" in log
    assert "disk free" in log


def test_scan_counts_the_root_directory_itself(out):
    """An empty subtree still occupies an inode's worth of blocks, and the
    entry count must be honest about being zero."""
    workdir = make_project(out, "jq", memo=False, html=False)
    (workdir / "memo").mkdir()
    size, entries, violations = retain.scan(workdir / "memo")
    assert (entries, violations) == (0, [])
    assert size == disk_bytes(workdir / "memo")


def test_scan_reports_a_missing_root_as_a_violation(out):
    """A vanished subtree is a refusal, not a zero."""
    size, entries, violations = retain.scan(out / "nope")
    assert (size, entries) == (0, 0)
    assert len(violations) == 1 and "cannot stat" in violations[0]


def test_scan_reports_an_unreadable_directory_as_a_violation(out):
    """A stray FILE named memo/ cannot be walked, so it must be refused
    instead of removed blind."""
    workdir = make_project(out, "jq", memo=False)
    (workdir / "memo").write_text("not a directory")
    size, entries, violations = retain.scan(workdir / "memo")
    assert entries == 0
    assert len(violations) == 1 and "unreadable" in violations[0]
    assert retain.prune("jq", apply=True) == (0, False)
    assert (workdir / "memo").read_text() == "not a directory"


class VanishingEntry:
    """A directory entry that disappears between scandir and stat."""

    name = "blob0001"

    def __init__(self, path: str):
        self.path = path

    def stat(self, follow_symlinks: bool = True):
        raise OSError(2, "No such file or directory")

    def is_dir(self, follow_symlinks: bool = True) -> bool:
        return False


def test_scan_reports_an_entry_that_vanishes_mid_walk(out, monkeypatch):
    """DATA SAFETY. A file removed by another process while the walk runs must
    become a refusal, not a silent under-count of what is about to be
    deleted."""
    workdir = make_project(out, "jq")

    @contextlib.contextmanager
    def fake_scandir(path):
        yield [VanishingEntry(str(Path(path) / "blob0001"))]

    monkeypatch.setattr(retain.os, "scandir", fake_scandir)
    size, entries, violations = retain.scan(workdir / "memo")
    assert entries == 1
    assert len(violations) == 1 and "cannot stat" in violations[0]
    assert retain.prune("jq", ("memo",), apply=True) == (0, False)
    assert (workdir / "memo" / "blob0001").exists()


@pytest.mark.parametrize("n, expected", [
    (0, "0 B"), (512, "512 B"), (1023, "1023 B"), (1024, "1.0 KiB"),
    (1536, "1.5 KiB"), (1024 ** 2, "1.0 MiB"), (1024 ** 3, "1.0 GiB"),
    (1024 ** 4, "1.0 TiB"), (1024 ** 5, "1.0 PiB"), (1024 ** 6, "1024.0 PiB"),
])
def test_human(n, expected):
    """The log is the only record of how much disk a run reclaimed."""
    assert retain.human(n) == expected


def test_say_prints_a_timestamp(capsys):
    """Every deletion is timestamped, so a run can be audited afterwards."""
    retain.say("hello")
    out = capsys.readouterr().out
    assert out.endswith("hello\n") and out.startswith("[")


# --------------------------------------------------------------------------
# The output directory comes from pipeline.cfg
# --------------------------------------------------------------------------

def load_retain_copy(tmp_path: Path, cfg_body: str):
    """Import a copy of retain.py beside a tmp_path pipeline.cfg.

    The output dir is decided at import time from CORPUS/pipeline.cfg, so this
    is the only way to prove the module reads the config rather than a
    hardcoded path.
    """
    repo = tmp_path / "fake-repo"
    repo.mkdir()
    shutil.copy(Path(retain.__file__).resolve(), repo / "retain.py")
    (repo / "pipeline.cfg").write_text(cfg_body)
    spec = importlib.util.spec_from_file_location("retain_cfg_probe",
                                                 repo / "retain.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return repo, module


def test_output_dir_follows_an_absolute_path_in_pipeline_cfg(tmp_path):
    """The corpus lives on a different disk on every machine, so the path must
    be configuration, never a constant in the source."""
    target = tmp_path / "elsewhere" / "corpus-files"
    target.mkdir(parents=True)
    _, module = load_retain_copy(tmp_path,
                                 f"[paths]\noutput_dir = {target}\n")
    assert module.OUT == target.resolve()


def test_output_dir_follows_a_relative_path_in_pipeline_cfg(tmp_path):
    """A relative output_dir is resolved against the repo, as ctp.py does."""
    repo, module = load_retain_copy(tmp_path,
                                    "[paths]\noutput_dir = ../shared/files\n")
    assert module.OUT == (repo / ".." / "shared" / "files").resolve()


def test_output_dir_falls_back_only_when_the_config_is_silent(tmp_path):
    """The hardcoded string is a fallback, not the source of truth."""
    repo, module = load_retain_copy(tmp_path, "[paths]\ncregit_dir = /x\n")
    assert module.OUT == (repo / "../cregit-workspace/corpus-files").resolve()


def test_a_configured_output_dir_is_the_one_pruned(tmp_path, monkeypatch):
    """End to end: point the config at a tmp_path tree and the script prunes
    that tree."""
    target = tmp_path / "elsewhere" / "corpus-files"
    target.mkdir(parents=True)
    _, module = load_retain_copy(tmp_path, f"[paths]\noutput_dir = {target}\n")
    workdir = make_project(module.OUT, "jq")
    monkeypatch.setattr("sys.argv", ["retain.py", "--apply"])
    assert module.main() == 0
    assert not (workdir / "memo").exists()
    assert (workdir / "jq-dataset.parquet").exists()


@pytest.mark.parametrize("raw", ["~/corpus", "sub/dir", "/abs/corpus"])
def test_cfg_path_expands_and_resolves(tmp_path, monkeypatch, raw):
    """A ~ or a relative path in the config must become one canonical absolute
    path, because ctp.py and retain.py must agree on the string."""
    cfg = configparser.ConfigParser()
    cfg.read_string(f"[paths]\noutput_dir = {raw}\n")
    monkeypatch.setattr(retain, "CORPUS", tmp_path)
    monkeypatch.setattr(retain, "_cfg", cfg)
    expected = (tmp_path / Path(raw).expanduser()).resolve()
    assert retain._cfg_path("output_dir", "unused-default") == expected


def test_cfg_path_uses_the_default_for_an_unknown_key(tmp_path, monkeypatch):
    """A key the config does not carry falls back, it does not raise."""
    cfg = configparser.ConfigParser()
    cfg.read_string("[paths]\n")
    monkeypatch.setattr(retain, "CORPUS", tmp_path)
    monkeypatch.setattr(retain, "_cfg", cfg)
    assert retain._cfg_path("missing", "fallback/dir") == (
        tmp_path / "fallback" / "dir").resolve()
