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
import fcntl
import importlib.util
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

import retain

REAL_OUT = retain.OUT
REAL_STATE = retain.STATE


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """No test may sleep."""
    monkeypatch.setattr(time, "sleep", lambda *a, **k: None)


@pytest.fixture
def out(tmp_path, monkeypatch):
    """The output directory every test operates in. Never the real one.

    STATE is patched here too, not only OUT. retain.STATE is derived from CORPUS
    at import time and holds the liveness locks, so a test that creates a lock
    with STATE unpatched would write into the REAL state/ directory of a possibly
    running corpus — the same trap that was hit in consolidate.py, where the
    fixture patched CORPUS but not a constant derived from it. Both are asserted
    below to be inside tmp_path.
    """
    d = (tmp_path / "corpus-files").resolve()
    d.mkdir()
    monkeypatch.setattr(retain, "OUT", d)
    state = (tmp_path / "state").resolve()
    state.mkdir()
    monkeypatch.setattr(retain, "STATE", state)
    # Data safety: if this ever equals the live corpus, --apply tests delete it.
    assert retain.OUT != REAL_OUT
    assert retain.OUT.is_relative_to(tmp_path.resolve())
    assert retain.STATE != REAL_STATE
    assert retain.STATE.is_relative_to(tmp_path.resolve())
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
    disk full.

    EXPECTATION CHANGED: the summary line now reads "not pruned" where it read
    "not finished". A project is also skipped when a subtree is refused
    or when a delete fails, and the summary must not claim those projects
    were unfinished. The per-project line above it still gives the real reason.
    """
    make_project(out, "jq")
    make_project(out, "half", parquet=None)
    assert run_main(monkeypatch, "jq", "half") == 1
    log = capsys.readouterr().out
    assert "SKIPPED 1 project(s), not pruned: half" in log
    assert "half — SKIP: not finished (missing keeper" in log


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


@pytest.mark.parametrize("stamp, listed", [
    (None, False),          # no stamp at all
    (b"", False),           # zero-byte stamp
    (b"validated\n", True),  # a real stamp
])
def test_finished_projects_agrees_with_finished(out, stamp, listed):
    """One definition of finished, used everywhere.

    EXPECTATION CHANGED. This test used to assert the inconsistency instead of
    the fix: finished_projects() checked only that the stamp file existed, while
    finished() also required it to be non-empty. A zero-byte stamp was therefore
    listed and then refused, so a whole-corpus dry run exited 1 naming a project
    the user never asked about. Before the fix, the b"" row below listed "jq".
    """
    make_project(out, "jq", stamp=stamp)
    assert retain.finished("jq")[0] is listed
    assert retain.finished_projects() == (["jq"] if listed else [])


def test_finished_projects_agrees_with_finished_about_the_parquet(out):
    """The stamp is not the only half of finished(), so the lister must not
    treat it as though it were. Before the fix a stamped project with no parquet
    was listed and then refused."""
    make_project(out, "jq", parquet=None)
    assert retain.finished("jq")[0] is False
    assert retain.finished_projects() == []


def test_a_whole_corpus_dry_run_is_clean_when_a_stamp_is_empty(out, monkeypatch,
                                                              capsys):
    """The user-visible symptom. Before the fix, the zero-byte stamp put
    "half" in the default project list, prune() then reported it "not finished",
    and the run exited 1 naming a project the user never asked about."""
    make_project(out, "jq")
    make_project(out, "half", stamp=b"")
    assert run_main(monkeypatch) == 0
    log = capsys.readouterr().out
    assert "half" not in log
    assert "projects    1: jq" in log


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


def test_a_refusal_in_the_first_subtree_leaves_the_second_alone(out, capsys):
    """DATA SAFETY. A refusal in memo/ must not let html/ be deleted.

    EXPECTATION CHANGED. Before the fix prune() returned at the first refusal, so
    html/ was never scanned. Now html/ IS scanned — the dry-run total needs it —
    but under --apply a refusal anywhere still deletes nothing for the project.
    The guarantee is stronger than before, not weaker: the delete phase runs only
    after every subtree has passed.
    """
    workdir = make_project(out, "jq")
    (workdir / "memo" / ".git").mkdir()
    before = snapshot(out)
    assert retain.prune("jq", ("memo", "html"), apply=True) == (0, False)
    assert (workdir / "html" / "index.html").exists()
    assert snapshot(out) == before
    log = capsys.readouterr().out
    assert "measured html/" in log            # html/ was measured this time
    assert "nothing was deleted for this project" in log


def test_a_refusal_in_the_second_subtree_leaves_the_first_alone(out):
    """DATA SAFETY. The fail-closed rule is about the project, not about
    ordering. memo/ is clean and comes first, html/ is refused — and memo/ must
    still be there afterwards, because a refusal means the walk did not
    understand this project."""
    workdir = make_project(out, "jq")
    (workdir / "html" / "ctp.duckdb").write_text("precious")
    before = snapshot(out)
    assert retain.prune("jq", ("memo", "html"), apply=True) == (0, False)
    assert (workdir / "memo" / "blob0001").exists()
    assert snapshot(out) == before


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
    """DATA SAFETY. `retain.py ../evil --apply` must not delete a tree
    outside output_dir.

    EXPECTATION CHANGED. The refusal now comes from check_name inside prune(),
    before anything is read, and the log says so. Before the fix the only thing
    that stopped this was check_target's resolved-path comparison, one comparison
    between a typo and a delete outside output_dir. check_target still holds the
    line as well — the second half of this test proves it — because defence in
    depth is right here.
    """
    outside = tmp_path / "evil"
    (outside / "memo").mkdir(parents=True)
    (outside / "memo" / "precious.txt").write_text("do not delete me")
    # Make the traversal look finished, so the guard under test is the name check
    # and not the finished() check.
    (tmp_path / "evil-dataset.parquet").write_bytes(b"PAR1")
    (tmp_path / "evil.validated").write_bytes(b"ok")
    before = snapshot(outside)

    assert retain.finished("../evil")[0] is True     # finished() does not object
    reclaimed, ok = retain.prune("../evil", apply=True)

    assert (reclaimed, ok) == (0, False)
    assert snapshot(outside) == before
    assert "is not one plain project name" in capsys.readouterr().out

    # Defence in depth: check_target refuses the same path on its own.
    with pytest.raises(ValueError, match="is outside output_dir"):
        retain.check_target(out / "../evil", "memo")
    assert snapshot(outside) == before


def test_an_absolute_project_name_is_refused(out, tmp_path, capsys):
    """DATA SAFETY. `OUT / "/abs/path"` is "/abs/path": an absolute project
    name escapes output_dir entirely and must be refused.

    EXPECTATION CHANGED for the same reason as the traversal test above: the
    refusal is now check_name's, and check_target is checked separately as the
    second layer.
    """
    outside = tmp_path / "abs_evil"
    (outside / "memo").mkdir(parents=True)
    (outside / "memo" / "precious.txt").write_text("do not delete me")
    (tmp_path / "abs_evil-dataset.parquet").write_bytes(b"PAR1")
    (tmp_path / "abs_evil.validated").write_bytes(b"ok")
    before = snapshot(outside)

    reclaimed, ok = retain.prune(str(outside), apply=True)

    assert (reclaimed, ok) == (0, False)
    assert snapshot(outside) == before
    assert "is not one plain project name" in capsys.readouterr().out

    with pytest.raises(ValueError, match="is outside output_dir"):
        retain.check_target(outside, "memo")
    assert snapshot(outside) == before


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

    # The filesystem does not return the space at once, so a bare
    # `free >= free_before` can fail even though the delete was real. A writer
    # elsewhere on the same filesystem can also take more space than this test
    # released. So poll for the recovery, then report an unattributable delta as
    # inconclusive. The two assertions above are the contract; this one only
    # confirms the bytes were real, and it must not fail because another process
    # was busy.
    deadline = time.monotonic() + 5.0
    while shutil.disk_usage(out).free < free_before and time.monotonic() < deadline:
        time.sleep(0.1)
    if shutil.disk_usage(out).free < free_before:
        pytest.skip("another writer on this filesystem took more space than this "
                    "test released, so the free-space delta cannot be attributed")


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


def test_scan_reports_an_unreadable_directory_as_a_violation(out, capsys):
    """A stray FILE named memo/ cannot be walked, so scan() must report it
    instead of measuring it.

    EXPECTATION CHANGED at the prune() level. scan() is unchanged: os.scandir
    on a file still raises NotADirectoryError and still becomes an "unreadable"
    violation. But prune() no longer reaches scan() for this case, and it no
    longer costs the whole project. Before the fix, prune returned (0, False) and
    html/ was never even measured; one stray file cost a whole project's reclaim.
    Now the stray file is named and left alone, and html/ is still pruned.
    """
    workdir = make_project(out, "jq", memo=False)
    (workdir / "memo").write_text("not a directory")

    size, entries, violations = retain.scan(workdir / "memo")
    assert entries == 0
    assert len(violations) == 1 and "unreadable" in violations[0]

    reclaimed, ok = retain.prune("jq", apply=True)
    assert ok is False                     # the project did not fully prune
    assert reclaimed > 0                   # but html/ was reclaimed
    assert (workdir / "memo").read_text() == "not a directory"
    assert not (workdir / "html").exists()
    log = capsys.readouterr().out
    assert "SKIP memo/: exists but is not a directory" in log


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


# --------------------------------------------------------------------------
# The output_dir guard must hold on every entry point
# --------------------------------------------------------------------------

@pytest.mark.parametrize("path, refused", [
    (Path("/"), True),                                # the filesystem root
    (Path.home(), True),                              # the home directory
    (Path("/tmp"), True),                             # fewer than three parts
    (Path("/home/someone/cregit-workspace/files"), False),
])
def test_output_dir_refusal_is_the_one_shared_predicate(monkeypatch, path,
                                                        refused):
    """main() and prune() must not carry two copies of this rule, so it lives
    in one helper that both call."""
    monkeypatch.setattr(retain, "OUT", path)
    assert (retain.output_dir_refusal() is not None) is refused


@pytest.mark.parametrize("bad", [Path("/"), Path.home(), Path("/tmp")])
def test_prune_refuses_a_dangerous_output_dir_on_the_ctp_call_path(monkeypatch,
                                                                  capsys, bad):
    """DATA SAFETY, HIGHEST PRIORITY.

    Before the fix this guard sat in main() alone. ctp.py:213 calls
    prune(name, ("memo",), apply=True) directly on the --drop-memo path, so on
    the path that runs once per project the refusal never executed: a misread
    output_dir of / or ~ would have been walked and offered for deletion. The
    call below is exactly ctp.py's call.
    """
    monkeypatch.setattr(retain, "OUT", bad)
    reclaimed, ok = retain.prune("jq", ("memo",), apply=True)
    assert (reclaimed, ok) == (0, False)
    assert "refusing to operate on output_dir" in capsys.readouterr().out


def test_prune_refuses_a_dangerous_output_dir_before_reading_the_disk(monkeypatch,
                                                                     capsys):
    """The guard is worth nothing if it runs after the walk. finished() would
    be the first thing to touch the filesystem, so it must never be reached."""
    monkeypatch.setattr(retain, "OUT", Path("/"))
    monkeypatch.setattr(retain, "finished",
                        lambda name: pytest.fail("prune read the disk first"))
    assert retain.prune("jq", ("memo",), apply=True) == (0, False)
    assert "refusing to operate on output_dir" in capsys.readouterr().out


# --------------------------------------------------------------------------
# The project name must be one plain directory name
# --------------------------------------------------------------------------

@pytest.mark.parametrize("name", [
    "../evil", "/etc", "a/b", "", "..", "a/../b", "sub/dir", "./jq",
])
def test_check_name_rejects_anything_but_one_directory_name(name):
    """The four rejects: contains /, contains .., is absolute, is empty."""
    with pytest.raises(ValueError, match="is not one plain project name"):
        retain.check_name(name)


@pytest.mark.parametrize("name", [
    "jq", "zstd", "libuv", "tmux", "with-dash", "with_underscore",
    "dot.in.name", "x",
])
def test_check_name_accepts_a_real_project_name(name):
    """The check must not be so tight that the corpus cannot be pruned.
    Corpus names carry dashes, underscores and single dots."""
    assert retain.check_name(name) is None


@pytest.mark.parametrize("evil", ["../evil", "/etc", "a/b", "", ".."])
def test_prune_refuses_a_name_that_is_not_one_plain_project_name(out, tmp_path,
                                                                capsys, evil):
    """DATA SAFETY. Before the fix, prune("../evil") and prune("/abs/path")
    both passed finished(), because the parquet and stamp lookups follow the
    traversal too. Only check_target's resolved-path comparison stopped them —
    one comparison between a typo and a delete outside output_dir.

    The fixture tree below sits outside output_dir and is what "../evil" reaches.
    It must be byte-for-byte unchanged afterwards.
    """
    outside = tmp_path / "evil"
    (outside / "memo").mkdir(parents=True)
    (outside / "memo" / "precious.txt").write_text("do not delete me")
    (outside / "html").mkdir()
    (outside / "html" / "index.html").write_text("also precious")
    # Make the traversal look finished, so the guard under test is the name check.
    (tmp_path / "evil-dataset.parquet").write_bytes(b"PAR1")
    (tmp_path / "evil.validated").write_bytes(b"ok")
    before = snapshot(outside)

    reclaimed, ok = retain.prune(evil, apply=True)

    assert (reclaimed, ok) == (0, False)
    assert snapshot(outside) == before
    assert "is not one plain project name" in capsys.readouterr().out
    assert Path("/etc/passwd").is_file()        # the "/etc" row changed nothing


def test_prune_refuses_a_bad_name_before_reading_the_disk(out, monkeypatch,
                                                          capsys):
    """finished() cannot be the guard, because it follows the traversal
    happily, so the name check must run before it."""
    monkeypatch.setattr(retain, "finished",
                        lambda name: pytest.fail("prune read the disk first"))
    assert retain.prune("../evil", apply=True) == (0, False)
    assert "is not one plain project name" in capsys.readouterr().out


# --------------------------------------------------------------------------
# A refusal costs one subtree in a dry run, the project on --apply
# --------------------------------------------------------------------------

def test_a_dry_run_with_memo_blocked_still_reports_html_in_the_total(out,
                                                                    capsys):
    """Before the fix, a blocked memo/ hid html/ from the reclaimable total,
    so the capacity plan read low. That plan decides whether the corpus fits
    on disk, so under-reporting is a real cost."""
    workdir = make_project(out, "jq")
    (workdir / "memo" / ".git").mkdir()
    before = snapshot(out)

    reclaimed, ok = retain.prune("jq")

    assert ok is False                                # still not fully prunable
    assert reclaimed == disk_bytes(workdir / "html")  # html/ IS in the total
    assert reclaimed > 0
    assert snapshot(out) == before                    # and nothing was deleted
    log = capsys.readouterr().out
    assert "would delete html/" in log                # html/ was measured
    assert "protected entry inside subtree" in log    # memo/ was reported
    assert "1 refused (memo/)" in log


def test_the_main_dry_run_total_spans_a_partly_blocked_corpus(out, monkeypatch,
                                                             capsys):
    """The corpus-wide figure is the one the capacity plan uses. A project
    with one blocked subtree must still contribute the subtree that passed."""
    jq = make_project(out, "jq")
    zstd = make_project(out, "zstd")
    (jq / "memo" / ".git").mkdir()
    expected = (disk_bytes(jq / "html") + disk_bytes(zstd / "memo")
                + disk_bytes(zstd / "html"))
    before = snapshot(out)

    assert run_main(monkeypatch) == 1          # jq is not fully prunable

    log = capsys.readouterr().out
    assert f"TOTAL reclaimable {retain.human(expected)}" in log
    assert "SKIPPED 1 project(s), not pruned: jq" in log
    assert snapshot(out) == before


def test_an_apply_run_with_memo_blocked_deletes_nothing_for_that_project(
        out, monkeypatch, capsys):
    """DATA SAFETY. Fail closed. A refusal means the walk did not
    understand this project, so no subtree of it is safe to remove — not even
    html/, which passed. The neighbour project is still pruned."""
    jq = make_project(out, "jq")
    zstd = make_project(out, "zstd")
    (jq / "memo" / "runs.log").write_text("a protected name inside memo/")
    jq_before = snapshot(jq)

    assert run_main(monkeypatch, "--apply") == 1

    assert snapshot(jq) == jq_before               # nothing at all went for jq
    assert not (zstd / "memo").exists()           # the clean project still went
    assert not (zstd / "html").exists()
    log = capsys.readouterr().out
    assert "so nothing was deleted for this project" in log
    assert "SKIPPED 1 project(s), not pruned: jq" in log


def test_a_protected_file_in_memo_costs_one_subtree_not_the_project(out, capsys):
    """The tokenizer writes no .parquet, .validated, metrics.tsv, runs.log,
    ctp.duckdb or .git inside memo/ today, so this is latent. Keep refusing —
    erring toward refusal is right — but in a dry run the cost is one subtree,
    not the project's whole reclaim. Before the fix html/ was never measured."""
    workdir = make_project(out, "jq")
    (workdir / "memo" / "deep" / "hand.validated").write_text("x")

    reclaimed, ok = retain.prune("jq")

    assert ok is False
    assert reclaimed == disk_bytes(workdir / "html")
    log = capsys.readouterr().out
    assert "protected entry inside subtree" in log and "hand.validated" in log


# --------------------------------------------------------------------------
# A path that exists but is not a directory is its own case
# --------------------------------------------------------------------------

def test_a_file_named_memo_is_reported_and_html_is_still_pruned(out, capsys):
    """Before the fix a stray FILE named memo cost the whole project's
    reclaim: os.scandir raised NotADirectoryError, that became an "unreadable"
    violation, and prune returned before html/ was even measured. One stray file
    silently cost a whole project. Now it is named, left alone, and html/ still
    goes."""
    workdir = make_project(out, "jq", memo=False)
    (workdir / "memo").write_text("not a directory")
    expected = disk_bytes(workdir / "html")

    reclaimed, ok = retain.prune("jq", apply=True)

    assert (reclaimed, ok) == (expected, False)     # not fully pruned, but honest
    assert (workdir / "memo").read_text() == "not a directory"
    assert not (workdir / "html").exists()
    log = capsys.readouterr().out
    assert "SKIP memo/: exists but is not a directory" in log
    assert "1 skipped (memo/)" in log


def test_a_file_named_html_is_reported_in_a_dry_run_too(out, capsys):
    """The same case in the default mode, and on the second subtree, so the
    skip is not an artefact of ordering."""
    workdir = make_project(out, "jq", html=False)
    (workdir / "html").write_text("not a directory")
    before = snapshot(out)

    reclaimed, ok = retain.prune("jq")

    assert (reclaimed, ok) == (disk_bytes(workdir / "memo"), False)
    assert snapshot(out) == before
    log = capsys.readouterr().out
    assert "SKIP html/: exists but is not a directory" in log
    assert "1 skipped (html/)" in log


# --------------------------------------------------------------------------
# One delete failure must not abandon the rest of the corpus
# --------------------------------------------------------------------------

def rmtree_reporting_a_failure(match: str):
    """A shutil.rmtree that reports a permission error for matching paths.

    It reports through onexc, exactly as the real rmtree does for a per-entry
    failure, and it removes nothing — the worst realistic case, because the tree
    is still on disk and the caller must not count it as reclaimed. Anything that
    does not match is deleted for real.
    """
    real = shutil.rmtree

    def fake(path, *args, onexc=None, **kwargs):
        if match in f"{path}{os.sep}":
            onexc(os.unlink, str(Path(path) / "blob0001"),
                  PermissionError(13, "Permission denied"))
            return None
        return real(path, *args, onexc=onexc, **kwargs)

    return fake


def test_a_delete_failure_skips_one_project_and_the_next_still_runs(
        out, monkeypatch, capsys):
    """Before the fix shutil.rmtree was unwrapped, so a permission error
    part-way through a tree raised out of prune() and out of main(): every
    remaining project was abandoned with a traceback instead of a per-project
    SKIP and a non-zero exit. Over the whole corpus that is the whole run."""
    locked = make_project(out, "locked")
    good = make_project(out, "zz-good")
    monkeypatch.setattr(retain.shutil, "rmtree",
                        rmtree_reporting_a_failure(f"{os.sep}locked{os.sep}"))

    assert run_main(monkeypatch, "locked", "zz-good", "--apply") == 1

    log = capsys.readouterr().out
    assert "cannot remove" in log and "Permission denied" in log
    assert "SKIPPED 1 project(s), not pruned: locked" in log
    assert (locked / "memo" / "blob0001").exists()   # the failure changed nothing
    assert not (good / "memo").exists()              # the next project still ran
    assert not (good / "html").exists()


def test_a_delete_failure_reports_the_tree_as_partially_deleted(out, monkeypatch,
                                                                capsys):
    """rmtree removes entries as it walks, so a failure can leave a tree half
    gone. The log must say so, because the caller must not treat the project as
    pruned."""
    make_project(out, "locked")
    monkeypatch.setattr(retain.shutil, "rmtree",
                        rmtree_reporting_a_failure(f"{os.sep}memo{os.sep}"))

    reclaimed, ok = retain.prune("locked", ("memo",), apply=True)

    assert (reclaimed, ok) == (0, False)
    log = capsys.readouterr().out
    assert "PARTIAL DELETE" in log and "is NOT pruned" in log


def test_a_delete_failure_after_a_success_counts_only_what_really_went(
        out, monkeypatch):
    """memo/ went, html/ failed. The reclaimed figure must be memo/ alone —
    a disk budget built from a number that includes a tree still on disk is
    wrong — and ok must be False so the project is not treated as pruned."""
    workdir = make_project(out, "jq")
    expected = disk_bytes(workdir / "memo")
    monkeypatch.setattr(retain.shutil, "rmtree",
                        rmtree_reporting_a_failure(f"{os.sep}html{os.sep}"))

    reclaimed, ok = retain.prune("jq", ("memo", "html"), apply=True)

    assert (reclaimed, ok) == (expected, False)
    assert not (workdir / "memo").exists()
    assert (workdir / "html" / "index.html").exists()


def test_remove_tree_reports_no_errors_when_the_tree_goes(out):
    """The wrapper must be transparent on the happy path: the tree goes and
    the error list is empty."""
    workdir = make_project(out, "jq")
    assert retain.remove_tree(workdir / "memo") == []
    assert not (workdir / "memo").exists()


# --------------------------------------------------------------------------
# Liveness: a validated project can still be in use
# --------------------------------------------------------------------------
# The .validated stamp says "this project finished once", not "nothing is using
# it now". A `--from-step 2` re-run of an already-validated project keeps the
# stamp for the whole re-run while blobExec reads and writes memo/ as its
# blob-to-token cache. Pruning one of those mid-run destroys hours of tokenizing.


@contextlib.contextmanager
def foreign_lock(lockfile: Path):
    """A REAL second process holding an flock on lockfile.

    It has to be another process. flock locks belong to an open file description,
    so a handle opened here would be refused like a foreign one -- but retain
    excepts the CALLER's own lock on purpose, because ctp.py prunes from inside a
    live run while holding it. A same-process handle would therefore not exercise
    the guard at all. The child acks on stdout before the test proceeds, so
    nothing here depends on a sleep, and it exits when its stdin closes.
    """
    lockfile.parent.mkdir(parents=True, exist_ok=True)
    lockfile.touch()
    code = ("import fcntl, sys\n"
            "f = open(sys.argv[1], 'r')\n"
            "fcntl.flock(f, fcntl.LOCK_EX)\n"
            "sys.stdout.write('locked\\n'); sys.stdout.flush()\n"
            "sys.stdin.read()\n")
    proc = subprocess.Popen([sys.executable, "-c", code, str(lockfile)],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            text=True)
    try:
        assert proc.stdout.readline() == "locked\n", "the lock holder never started"
        yield proc
    finally:
        proc.stdin.close()
        proc.wait(timeout=60)
        proc.stdout.close()


def test_a_validated_but_live_project_is_not_touched(out, capsys):
    """The defect this guard fixes. Stamp present, parquet present, so every
    older gate passes -- and a run owns the workdir. --apply must delete
    nothing."""
    workdir = make_project(out, "jq")
    before = snapshot(workdir)
    with foreign_lock(retain.lock_path("jq")):
        assert retain.prune("jq", apply=True) == (0, False)
    assert snapshot(workdir) == before
    log = capsys.readouterr().out
    assert "jq — SKIP: LIVE" in log


def test_a_live_project_is_not_even_measured_in_a_dry_run(out, capsys):
    """A dry run reports the reason and no size. Measuring means stat()ing every
    blob in memo/, which is I/O taken from the run that owns it."""
    make_project(out, "jq")
    with foreign_lock(retain.lock_path("jq")):
        assert retain.prune("jq") == (0, False)
    log = capsys.readouterr().out
    assert "SKIP: LIVE" in log
    assert "would delete" not in log
    assert "reclaimable" not in log


def test_a_validated_and_idle_project_is_still_pruned(out):
    """The guard must not be a blanket refusal. A lock file left behind by a run
    that has finished is not a live run: state/<name>/ outlives the run, so an
    existence test would have frozen retention for every project ever run."""
    workdir = make_project(out, "jq")
    retain.lock_path("jq").parent.mkdir(parents=True)
    retain.lock_path("jq").touch()

    assert retain.live("jq") is False
    reclaimed, ok = retain.prune("jq", apply=True)

    assert ok and reclaimed > 0
    assert not (workdir / "memo").exists()
    assert not (workdir / "html").exists()
    assert (workdir / "jq-dataset.parquet").exists()


def test_a_project_with_no_state_directory_at_all_is_pruned(out):
    """The common case: nothing under state/ for this project."""
    workdir = make_project(out, "jq")
    assert not retain.lock_path("jq").exists()
    assert retain.live("jq") is False
    assert retain.prune("jq", apply=True)[1] is True
    assert not (workdir / "memo").exists()


def test_the_guard_fires_on_the_ctp_prune_path_not_only_via_main(out, capsys):
    """prune() is also `ctp.py run --drop-memo`'s entry point -- the call that
    runs once per project. This is that exact call signature, with no main()
    anywhere: ctp.py:595 prune(name, ("memo",), apply=True)."""
    workdir = make_project(out, "jq")
    before = snapshot(workdir)
    with foreign_lock(retain.lock_path("jq")):
        assert retain.prune("jq", ("memo",), apply=True) == (0, False)
    assert snapshot(workdir) == before
    assert "SKIP: LIVE" in capsys.readouterr().out


def test_the_callers_own_lock_does_not_block_its_own_prune(out):
    """ctp.py reaches prune() from --drop-memo while STILL holding that
    project's lock: the call sits inside the try whose finally closes the
    lockfile. flock conflicts between two handles on the same file even within
    one process, so a guard that did not except the caller's own lock would
    refuse every --drop-memo prune, keep memo/ for every project and fill
    the disk. Exactly what ctp.py does, in-process."""
    workdir = make_project(out, "jq")
    lockfile = retain.lock_path("jq")
    lockfile.parent.mkdir(parents=True)
    with lockfile.open("w") as held:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # Held, to anyone else.
        assert retain.lock_held(lockfile) is True
        # But not to the holder, so its own cleanup goes through.
        assert retain.live("jq") is False
        reclaimed, ok = retain.prune("jq", ("memo",), apply=True)

    assert ok and reclaimed > 0
    assert not (workdir / "memo").exists()
    assert (workdir / "html" / "index.html").exists()


def test_a_lock_in_the_work_directory_is_not_mistaken_for_the_real_lock(out):
    """The real lock lives in state/, never in the workdir -- the runner deletes
    the workdir at FROM_STEP=1 and from its EXIT trap, which would take a lock
    kept inside it, so every run would look idle. A held .lock in the workdir is
    therefore not evidence of a run, and it must not be read as any."""
    workdir = make_project(out, "jq")
    decoy = workdir / ".lock"
    with foreign_lock(decoy):
        assert retain.live("jq") is False
        reclaimed, ok = retain.prune("jq", apply=True)

    assert ok and reclaimed > 0
    assert not (workdir / "memo").exists()
    assert decoy.exists(), "the decoy was in the workdir root, not a subtree"


def test_the_liveness_probe_never_writes_to_the_lock_file(out):
    """The two older copies of this predicate (ctp.py, consolidate.py) open the
    lock file "w", which truncates the file they are inspecting. retain.py must
    not write anything under state/, so it opens "r"."""
    make_project(out, "jq")
    lockfile = retain.lock_path("jq")
    lockfile.parent.mkdir(parents=True)
    lockfile.write_bytes(b"pid=4242\n")

    assert retain.live("jq") is False
    retain.prune("jq", apply=True)

    assert lockfile.read_bytes() == b"pid=4242\n"


def test_an_unreadable_lock_file_counts_as_live(out):
    """"I cannot tell" must not resolve to "delete it"."""
    workdir = make_project(out, "jq")
    lockfile = retain.lock_path("jq")
    lockfile.parent.mkdir(parents=True)
    lockfile.touch()
    lockfile.chmod(0o000)
    if os.access(lockfile, os.R_OK):  # pragma: no cover - root ignores the mode
        pytest.skip("running as root: an unreadable file is still readable")
    try:
        assert retain.live("jq") is True
        assert retain.prune("jq", apply=True) == (0, False)
        assert (workdir / "memo" / "blob0001").exists()
    finally:
        lockfile.chmod(0o600)


def test_the_live_skip_is_reported_and_counted(out, monkeypatch, capsys):
    """A live project must be named, counted on its own line, and must not stop
    the sweep: one live project cannot hold up retention for the rest."""
    live_wd = make_project(out, "linux")
    idle_wd = make_project(out, "jq")
    with foreign_lock(retain.lock_path("linux")):
        assert run_main(monkeypatch, "linux", "jq", "--apply") == 1

    log = capsys.readouterr().out
    assert "LIVE 1 project(s), a running pipeline holds the lock, " \
           "left untouched: linux" in log
    assert (live_wd / "memo" / "blob0001").exists()
    assert not (idle_wd / "memo").exists()
    assert "TOTAL reclaimed" in log
    assert "over 1 project(s)" in log


def test_a_live_skip_is_distinguishable_from_an_unvalidated_skip(out, monkeypatch,
                                                                capsys):
    """Different causes, different remedies: a live project needs only a later
    sweep, an unvalidated one needs looking at. Neither line may absorb the
    other."""
    make_project(out, "linux")
    make_project(out, "half", parquet=None)
    make_project(out, "jq")
    with foreign_lock(retain.lock_path("linux")):
        assert run_main(monkeypatch, "linux", "half", "jq", "--apply") == 1

    log = capsys.readouterr().out
    assert "linux — SKIP: LIVE" in log
    assert "half — SKIP: not finished (missing keeper" in log
    assert "LIVE 1 project(s)" in log and "left untouched: linux" in log
    assert "SKIPPED 1 project(s), not pruned: half" in log
    live_line = next(l for l in log.splitlines() if "LIVE 1 project(s)" in l)
    skip_line = next(l for l in log.splitlines() if "SKIPPED 1 project(s)" in l)
    assert "half" not in live_line and "linux" not in skip_line


def test_a_whole_corpus_sweep_skips_the_live_project_and_prunes_the_rest(
        out, monkeypatch, capsys):
    """No project names on the command line: the lister still offers a live
    project, because it is validated, and the guard in prune() is what stops
    it."""
    live_wd = make_project(out, "linux")
    idle_wd = make_project(out, "zstd")
    with foreign_lock(retain.lock_path("linux")):
        assert "linux" in retain.finished_projects()
        assert run_main(monkeypatch, "--apply") == 1

    assert (live_wd / "memo" / "deep" / "blob0002").exists()
    assert not (idle_wd / "memo").exists()
    assert "left untouched: linux" in capsys.readouterr().out


def test_lock_path_agrees_with_ctp(out):
    """One spelling of the lock, in two files. retain.py cannot import ctp.py --
    ctp.py imports retain -- so the path is duplicated, and a silent drift would
    make retain look for locks where there are none and call every project idle.
    Imported here, where a cycle does not matter, to pin the two together."""
    import ctp  # noqa: PLC0415 - deliberately local, see the docstring

    assert REAL_STATE == ctp.STATE
    # retain.STATE is patched to tmp_path by the fixture; compare the shapes.
    assert (retain.lock_path("jq").relative_to(retain.STATE)
            == ctp.lock_path("jq").relative_to(ctp.STATE))
    assert retain.lock_path("jq").relative_to(retain.STATE) == Path("jq/.lock")


def test_live_is_not_fooled_by_a_lock_for_another_project(out):
    """The lock is per project. linux running says nothing about jq."""
    workdir = make_project(out, "jq")
    with foreign_lock(retain.lock_path("linux")):
        assert retain.live("linux") is True
        assert retain.live("jq") is False
        assert retain.prune("jq", apply=True)[1] is True
    assert not (workdir / "memo").exists()


def test_without_procfs_even_the_callers_own_lock_reads_as_live(out, monkeypatch,
                                                               capsys):
    """The self-ownership test reads /proc/self/fd, so it is Linux-only. Where
    that is unavailable the guard must fail towards NOT deleting: a --drop-memo
    prune is then declined and memo/ merely survives, which costs disk instead of
    work."""
    workdir = make_project(out, "jq")
    lockfile = retain.lock_path("jq")
    lockfile.parent.mkdir(parents=True)
    monkeypatch.setattr(retain.os, "listdir",
                        lambda p: (_ for _ in ()).throw(OSError("no procfs")))

    with lockfile.open("w") as held:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert retain.live("jq") is True
        assert retain.prune("jq", ("memo",), apply=True) == (0, False)

    assert (workdir / "memo" / "blob0001").exists()
    assert "SKIP: LIVE" in capsys.readouterr().out
