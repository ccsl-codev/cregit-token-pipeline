"""Unit tests for consolidate.py, the DuckDB index builder.

consolidate.py imports duckdb at module scope, and duckdb is not installed in
.venv (it comes from `devenv shell`). To keep the suite runnable and offline,
this file installs a stub `duckdb` module in sys.modules before the import when
the real one is absent. Every test then patches `consolidate.duckdb.connect`
with a recorder, so no database file is ever opened. The repo's own ctp.duckdb
is never touched.

The stub only makes the import succeed. Nothing here asserts DuckDB semantics;
the tests assert the SQL consolidate.py emits and the row tuples it derives.

A projects row carries two visibility flags at the end, rows_unreadable and
parquet_missing. They exist so that a project which disagrees with itself is
counted, named and queryable instead of silently wrong, and rows_unreadable
also drives the non-zero exit status.
"""

from __future__ import annotations

import fcntl
import sys
import types
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

if "duckdb" not in sys.modules:  # pragma: no cover - import plumbing
    try:
        import duckdb  # noqa: F401
    except ModuleNotFoundError:
        def _unpatched(*args, **kwargs):
            raise AssertionError(
                "stub duckdb was called: patch consolidate.duckdb.connect "
                "or validate.duckdb.sql in the test")

        _stub = types.ModuleType("duckdb")
        _stub.__doc__ = "Test stub. Real duckdb lives in the devenv shell."
        _stub.connect = _unpatched
        _stub.sql = _unpatched
        sys.modules["duckdb"] = _stub

import consolidate  # noqa: E402

DUCKDB_IS_STUBBED = getattr(sys.modules["duckdb"], "__doc__", "").startswith("Test stub")


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #

class FakeResult:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row


class FakeCon:
    """Records every statement consolidate.py sends. Opens no database."""

    def __init__(self, count=0):
        self.statements: list[str] = []
        self.batches: list[tuple[str, list]] = []
        self.count = count
        self.closed = False

    def execute(self, sql, *args):
        self.statements.append(sql)

    def executemany(self, sql, rows):
        self.batches.append((sql, list(rows)))

    def sql(self, query):
        self.statements.append(query)
        return FakeResult((self.count,))

    def close(self):
        self.closed = True

    def find(self, needle: str) -> str:
        matches = [s for s in self.statements if needle in s]
        assert matches, f"no statement contains {needle!r}: {self.statements}"
        return matches[0]


@contextmanager
def held_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    fh = path.open("w")
    fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        yield
    finally:
        fcntl.flock(fh, fcntl.LOCK_UN)
        fh.close()


def write_manifest(root: Path, *lines: str) -> Path:
    path = root / "manifest.tsv"
    path.write_text("".join(f"{line}\n" for line in lines))
    return path


def make_project(out: Path, name: str, *, stamp: str | None = None,
                 parquet: bool = False) -> Path:
    workdir = out / name
    workdir.mkdir(parents=True, exist_ok=True)
    if stamp is not None:
        (workdir / f"{name}.validated").write_text(stamp)
    if parquet:
        (workdir / f"{name}-dataset.parquet").write_bytes(b"PAR1" + b"\0" * 64)
    return workdir


ROW = "jq\thttps://github.com/jqlang/jq.git\tcommunity\t\\.[ch]$\tS"


# --------------------------------------------------------------------------- #
# fixtures
# --------------------------------------------------------------------------- #

@pytest.fixture(autouse=True)
def sandbox(tmp_path, monkeypatch):
    """Point consolidate at tmp_path so the live corpus and ctp.duckdb are safe."""
    out = tmp_path / "corpus-files"
    out.mkdir()
    monkeypatch.setattr(consolidate, "CORPUS", tmp_path)
    monkeypatch.setattr(consolidate, "OUT", out)
    monkeypatch.setattr(consolidate, "DB", tmp_path / "ctp.duckdb")
    # STATE is derived from CORPUS at import, so patching CORPUS alone leaves it
    # pointing at the real repo's state/ directory. A test that probes or creates
    # a lock there would touch a live run's bookkeeping.
    monkeypatch.setattr(consolidate, "STATE", tmp_path / "state")
    return SimpleNamespace(root=tmp_path, out=out)


@pytest.fixture
def con(monkeypatch):
    """Install a recording connection and return it."""
    fake = FakeCon(count=1_234_567)
    monkeypatch.setattr(consolidate.duckdb, "connect",
                        lambda path: fake, raising=False)
    return fake


# --------------------------------------------------------------------------- #
# lock_held
# --------------------------------------------------------------------------- #

def test_lock_held_is_false_without_a_lockfile(sandbox):
    """A project that never started must read QUEUED, not RUNNING."""
    assert consolidate.lock_held(sandbox.out / "jq" / ".lock") is False


def test_lock_held_is_false_for_a_stale_lockfile(sandbox):
    """A lockfile left behind by a killed run holds no flock."""
    lockfile = make_project(sandbox.out, "jq") / ".lock"
    lockfile.touch()
    assert consolidate.lock_held(lockfile) is False


def test_lock_held_is_true_while_another_process_holds_it(sandbox):
    """The index must show a live tokenizer as RUNNING, never as FAILED."""
    lockfile = make_project(sandbox.out, "jq") / ".lock"
    with held_lock(lockfile):
        assert consolidate.lock_held(lockfile) is True


# --------------------------------------------------------------------------- #
# project_rows
# --------------------------------------------------------------------------- #

def test_project_rows_builds_one_ten_column_tuple_per_project(sandbox):
    """The tuple shape must match the projects table, or executemany fails.

    The shape grew from seven columns to ten: rows_unreadable,
    parquet_missing and excluded_because now travel with every project. The
    first two are false for a healthy project, the third is None.
    """
    write_manifest(sandbox.root, ROW)
    make_project(sandbox.out, "jq", stamp="rows=1234\nbytes=5678\n", parquet=True)

    rows = consolidate.project_rows()
    assert rows == [("jq", "https://github.com/jqlang/jq.git", "community", "S",
                     "DONE", 1234, str(sandbox.out / "jq" / "jq-dataset.parquet"),
                     False, False, None)]


def test_project_rows_classifies_all_four_states(sandbox):
    """The index is the dashboard. A wrong state misreports corpus progress."""
    write_manifest(
        sandbox.root,
        "done\thttps://d.git\tcommunity\t\\.[ch]$\tS",
        "running\thttps://r.git\tcommunity\t\\.[ch]$\tM",
        "failed\thttps://f.git\tenterprise\t\\.[ch]$\tL",
        "queued\thttps://q.git\tcommunity\t\\.[ch]$\tS")
    make_project(sandbox.out, "done", stamp="rows=7\nbytes=8\n", parquet=True)
    make_project(sandbox.out, "failed")
    make_project(sandbox.out, "running")
    # The lock lives in ctp.py's state directory, NOT in the work directory.
    # This test used to create it inside the workdir, which made it agree with
    # the bug it was supposed to catch: consolidate.py probed the same wrong
    # path, so RUNNING never fired against a real run.
    lockdir = consolidate.STATE / "running"
    lockdir.mkdir(parents=True, exist_ok=True)

    with held_lock(lockdir / ".lock"):
        states = {r[0]: r[4] for r in consolidate.project_rows()}

    assert states == dict(done="DONE", running="RUNNING",
                          failed="FAILED", queued="QUEUED")


def test_project_rows_ignores_a_stale_lock_left_in_the_work_directory(sandbox):
    """A lock inside the workdir must not be believed.

    run_pipeline_process.sh deletes the work directory at FROM_STEP=1 and again
    from its EXIT trap, so a lock kept there is unreliable by construction. ctp.py
    therefore locks state/<name>/.lock. A workdir lock is either a leftover or a
    different tool's file, and reading it as RUNNING hides a FAILED project.
    """
    write_manifest(sandbox.root, "proj\thttps://p.git\tcommunity\t\\.[ch]$\tS")
    workdir = make_project(sandbox.out, "proj")

    with held_lock(workdir / ".lock"):
        states = {r[0]: r[4] for r in consolidate.project_rows()}

    assert states == {"proj": "FAILED"}


def test_project_rows_skips_comments_and_blank_lines(sandbox):
    """The manifest header is a comment; treating it as data adds a fake project."""
    write_manifest(sandbox.root, "# name  url  category  filter  class", "", "  ", ROW)
    assert [r[0] for r in consolidate.project_rows()] == ["jq"]


def test_project_rows_on_an_empty_manifest_returns_nothing(sandbox):
    """An empty corpus is a valid state, not a crash."""
    write_manifest(sandbox.root)
    assert consolidate.project_rows() == []


def test_project_rows_rejects_a_malformed_row(sandbox):
    """Five fields is the contract, exactly as in ctp.read_manifest."""
    write_manifest(sandbox.root, "jq\thttps://x.git\tcommunity\t\\.[ch]$")
    with pytest.raises(ValueError):
        consolidate.project_rows()


def test_project_rows_without_a_manifest_raises(sandbox):
    """The manifest is ground truth; a missing one must stop the rebuild."""
    with pytest.raises(FileNotFoundError):
        consolidate.project_rows()


def test_project_rows_reports_no_parquet_path_when_the_file_is_absent(sandbox):
    """A stamp without a parquet must not put a dead path in the tokens view.

    The row now also carries parquet_missing = True, so the disagreement
    between state DONE and an absent dataset is queryable.
    """
    write_manifest(sandbox.root, ROW)
    make_project(sandbox.out, "jq", stamp="rows=9\nbytes=9\n", parquet=False)
    (name, _url, _cat, _cls, state, n_rows, parquet,
     rows_unreadable, parquet_missing, excluded) = consolidate.project_rows()[0]
    assert (name, state, n_rows, parquet) == ("jq", "DONE", 9, None)
    assert (rows_unreadable, parquet_missing, excluded) == (False, True, None)


def test_project_rows_does_not_flag_a_queued_project_as_missing_data(sandbox):
    """parquet_missing means "DONE but no data", so a project that never ran
    must not be flagged. Otherwise the flag would name every queued project."""
    write_manifest(sandbox.root, ROW)
    assert consolidate.project_rows()[0][8] is False


def test_project_rows_counts_zero_for_a_stamp_without_a_rows_key(sandbox):
    """An old or truncated stamp must not crash the rebuild."""
    write_manifest(sandbox.root, ROW)
    make_project(sandbox.out, "jq", stamp="malformed stamp\n", parquet=True)
    assert consolidate.project_rows()[0][5] == 0


def test_project_rows_ignores_extra_stamp_keys(sandbox):
    """The stamp is a key=value bag; unknown keys are tolerated."""
    write_manifest(sandbox.root, ROW)
    make_project(sandbox.out, "jq",
                 stamp="rows=5\nbytes=6\nvalidated_by=ctp\n", parquet=True)
    assert consolidate.project_rows()[0][5] == 5


def test_project_rows_survives_a_non_numeric_rows_value(sandbox, capsys):
    """One bad stamp must not stop the derived index for 1,423 projects.

    This was an expected failure. Before the fix int(kv.get('rows', 0)) was
    unguarded, so a stamp reading rows=many raised ValueError and ended
    project_rows() for every project. The project is now reported by name and
    by value, flagged, and still indexed.
    """
    write_manifest(sandbox.root, ROW)
    make_project(sandbox.out, "jq", stamp="rows=many\nbytes=6\n", parquet=True)

    row = consolidate.project_rows()[0]

    assert row[5] is None
    assert row[7] is True
    assert row[4] == "DONE"
    err = capsys.readouterr().err
    assert "jq" in err
    assert "'many'" in err


def test_project_rows_keeps_indexing_the_projects_after_a_bad_stamp(sandbox):
    """The bad stamp must cost one project, not the tail of the manifest."""
    write_manifest(sandbox.root,
                   "bad\thttps://b.git\tcommunity\t\\.[ch]$\tS",
                   "good\thttps://g.git\tenterprise\t\\.[ch]$\tL")
    make_project(sandbox.out, "bad", stamp="rows=many\nbytes=6\n", parquet=True)
    make_project(sandbox.out, "good", stamp="rows=77\nbytes=8\n", parquet=True)

    rows = {r[0]: r for r in consolidate.project_rows()}

    assert [r[0] for r in consolidate.project_rows()] == ["bad", "good"]
    assert (rows["good"][5], rows["good"][7]) == (77, False)
    assert (rows["bad"][5], rows["bad"][7]) == (None, True)


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #

def test_main_replaces_the_projects_table_and_inserts_every_row(sandbox, con, capsys):
    """`create or replace` plus one insert batch is what makes a rebuild safe
    to rerun at any time."""
    write_manifest(sandbox.root, ROW)
    make_project(sandbox.out, "jq", stamp="rows=1234\nbytes=5678\n", parquet=True)

    consolidate.main()

    ddl = con.find("create or replace table projects")
    assert "name text primary key" in ddl
    assert "token_rows bigint" in ddl
    assert "rows_unreadable boolean" in ddl
    assert "parquet_missing boolean" in ddl
    assert "excluded_because text" in ddl
    sql, rows = con.batches[0]
    assert sql == "insert into projects values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    assert rows == [("jq", "https://github.com/jqlang/jq.git", "community", "S",
                     "DONE", 1234, str(sandbox.out / "jq" / "jq-dataset.parquet"),
                     False, False, None)]


def test_main_loads_the_metrics_ledger_as_a_tsv_with_a_header(sandbox, con):
    """metrics.tsv is tab separated with a header row. Wrong options here turn
    the benchmark ledger into one unusable column."""
    write_manifest(sandbox.root, ROW)
    (sandbox.root / "metrics.tsv").write_text("iso_start\tproject\n")

    consolidate.main()

    sql = con.find("create or replace table phase_metrics")
    assert str(sandbox.root / "metrics.tsv") in sql
    assert "delim='\t'" in sql
    assert "header=true" in sql


def test_main_builds_the_tokens_view_from_validated_parquets_only(sandbox, con, capsys):
    """The unified view must not read a parquet from a project that failed, or
    the corpus statistics include unvalidated tokens."""
    write_manifest(sandbox.root,
                   "done\thttps://d.git\tcommunity\t\\.[ch]$\tS",
                   "failed\thttps://f.git\tenterprise\t\\.[ch]$\tL")
    make_project(sandbox.out, "done", stamp="rows=10\nbytes=20\n", parquet=True)
    make_project(sandbox.out, "failed", parquet=True)

    consolidate.main()

    view = con.find("create or replace view tokens")
    assert str(sandbox.out / "done" / "done-dataset.parquet") in view
    assert "failed-dataset.parquet" not in view
    assert "join projects p on t.repo_name = p.name" in view
    assert "1,234,567 rows across 1 projects" in capsys.readouterr().out


def test_main_skips_the_tokens_view_when_nothing_is_validated(sandbox, con, capsys):
    """read_parquet([]) is invalid SQL, so the view must be skipped, and the
    reported total must be 0 rather than a stale number."""
    write_manifest(sandbox.root, ROW)
    make_project(sandbox.out, "jq", parquet=True)

    consolidate.main()

    assert not [s for s in con.statements if "view tokens" in s or "from tokens" in s]
    assert "tokens view: 0 rows across 0 projects" in capsys.readouterr().out


def test_main_prints_a_count_for_every_populated_state(sandbox, con, capsys):
    """The printed summary is the only feedback the operator gets."""
    write_manifest(sandbox.root,
                   "done\thttps://d.git\tcommunity\t\\.[ch]$\tS",
                   "failed\thttps://f.git\tenterprise\t\\.[ch]$\tL",
                   "queued\thttps://q.git\tcommunity\t\\.[ch]$\tS")
    make_project(sandbox.out, "done", stamp="rows=1\nbytes=2\n", parquet=True)
    make_project(sandbox.out, "failed")

    consolidate.main()

    out = capsys.readouterr().out
    assert "DONE: 1" in out
    assert "FAILED: 1" in out
    assert "QUEUED: 1" in out
    assert "RUNNING" not in out


def test_main_connects_to_the_configured_database_and_closes_it(
        sandbox, monkeypatch, capsys):
    """A leaked connection leaves a write-ahead file next to ctp.duckdb."""
    write_manifest(sandbox.root)
    fake = FakeCon()
    seen = {}

    def fake_connect(path):
        seen["path"] = path
        return fake

    monkeypatch.setattr(consolidate.duckdb, "connect", fake_connect, raising=False)

    consolidate.main()

    assert seen["path"] == str(sandbox.root / "ctp.duckdb")
    assert fake.closed is True
    assert "ctp.duckdb rebuilt" in capsys.readouterr().out


def test_main_reports_a_malformed_stamp_and_still_indexes_the_rest(
        sandbox, con, capsys):
    """Before the fix a stamp reading rows=many raised ValueError out of
    project_rows() and no index was built at all. The rebuild now finishes, the
    bad project is named and counted, and the exit status says something was
    wrong."""
    write_manifest(sandbox.root,
                   "bad\thttps://b.git\tcommunity\t\\.[ch]$\tS",
                   "good\thttps://g.git\tenterprise\t\\.[ch]$\tL")
    make_project(sandbox.out, "bad", stamp="rows=many\nbytes=6\n", parquet=True)
    make_project(sandbox.out, "good", stamp="rows=77\nbytes=8\n", parquet=True)

    with pytest.raises(SystemExit) as exc:
        consolidate.main()

    assert exc.value.code == 1
    captured = capsys.readouterr()
    assert "unreadable rows= in stamp: 1 (bad)" in captured.out
    assert "'many'" in captured.err
    _sql, rows = con.batches[0]
    assert [r[0] for r in rows] == ["bad", "good"]
    assert [r[5] for r in rows] == [None, 77]
    assert con.closed is True
    assert "good-dataset.parquet" in con.find("create or replace view tokens")


def test_main_counts_and_names_a_done_project_whose_parquet_is_gone(
        sandbox, con, capsys):
    """Before the fix such a project just vanished from the tokens view while
    the projects table still said DONE, and nothing reported it. The row is now
    flagged, counted and named, and the view still leaves the dead file out."""
    write_manifest(sandbox.root,
                   "gone\thttps://x.git\tcommunity\t\\.[ch]$\tS",
                   "here\thttps://y.git\tenterprise\t\\.[ch]$\tL")
    make_project(sandbox.out, "gone", stamp="rows=5\nbytes=6\n", parquet=False)
    make_project(sandbox.out, "here", stamp="rows=7\nbytes=8\n", parquet=True)

    consolidate.main()

    out = capsys.readouterr().out
    assert "DONE but parquet missing: 1 (gone)" in out
    assert "DONE: 2" in out
    _sql, rows = con.batches[0]
    assert {r[0]: r[8] for r in rows} == {"gone": True, "here": False}
    view = con.find("create or replace view tokens")
    assert "gone-dataset.parquet" not in view
    assert "here-dataset.parquet" in view


# --------------------------------------------------------------------------- #
# publication exclusions
# --------------------------------------------------------------------------- #

def test_project_rows_carries_the_reason_for_an_excluded_project(
        sandbox, monkeypatch):
    """The reason must reach the projects table. A bare boolean would record
    that a project was dropped without recording why, which is the thing the
    sampling record exists to prevent."""
    monkeypatch.setattr(consolidate, "PUBLICATION_EXCLUSIONS",
                        {"twin": "near-duplicate of other"})
    write_manifest(sandbox.root,
                   "twin\thttps://t.git\tcompany-owned\t\\.[ch]$\tM",
                   "other\thttps://o.git\tcompany-owned\t\\.[ch]$\tM")
    make_project(sandbox.out, "twin", stamp="rows=5\nbytes=6\n", parquet=True)
    make_project(sandbox.out, "other", stamp="rows=7\nbytes=8\n", parquet=True)

    reasons = {r[0]: r[9] for r in consolidate.project_rows()}
    assert reasons == {"twin": "near-duplicate of other", "other": None}


def test_main_keeps_an_excluded_project_in_projects_but_out_of_the_tokens_view(
        sandbox, con, monkeypatch, capsys):
    """This is the whole point of the mechanism. Deleting the manifest row would
    drop the project from the sampling record; publishing it would count the
    same authorship twice. The row stays, the tokens do not."""
    monkeypatch.setattr(consolidate, "PUBLICATION_EXCLUSIONS",
                        {"twin": "near-duplicate of other"})
    write_manifest(sandbox.root,
                   "twin\thttps://t.git\tcompany-owned\t\\.[ch]$\tM",
                   "other\thttps://o.git\tcompany-owned\t\\.[ch]$\tM")
    make_project(sandbox.out, "twin", stamp="rows=5\nbytes=6\n", parquet=True)
    make_project(sandbox.out, "other", stamp="rows=7\nbytes=8\n", parquet=True)

    consolidate.main()

    _sql, rows = con.batches[0]
    assert {r[0] for r in rows} == {"twin", "other"}, "the row must survive"
    assert {r[0]: r[4] for r in rows} == {"twin": "DONE", "other": "DONE"}
    view = con.find("create or replace view tokens")
    assert "twin-dataset.parquet" not in view
    assert "other-dataset.parquet" in view


def test_main_names_and_counts_a_publication_exclusion(
        sandbox, con, monkeypatch, capsys):
    """A silent exclusion is worse than the crash it replaces, so the summary
    prints the name and the reason."""
    monkeypatch.setattr(consolidate, "PUBLICATION_EXCLUSIONS",
                        {"twin": "near-duplicate of other"})
    write_manifest(sandbox.root,
                   "twin\thttps://t.git\tcompany-owned\t\\.[ch]$\tM",
                   "other\thttps://o.git\tcompany-owned\t\\.[ch]$\tM")
    make_project(sandbox.out, "twin", stamp="rows=5\nbytes=6\n", parquet=True)
    make_project(sandbox.out, "other", stamp="rows=7\nbytes=8\n", parquet=True)

    consolidate.main()

    out = capsys.readouterr().out
    assert "publication exclusions: 1" in out
    assert "- twin: near-duplicate of other" in out
    assert "tokens view: 1,234,567 rows across 1 projects" in out


def test_the_shipped_exclusion_list_drops_tendb_and_keeps_tdbctl(sandbox):
    """The fork pair decision, pinned. tendb has 4.3x the files but every extra
    path is upstream Oracle, Facebook or Percona code; tdbctl holds 4.9x more
    first-party authorship. Swapping these two silently would publish the wrong
    half of a near-duplicate pair."""
    assert "tencent__tendbcluster-tendb" in consolidate.PUBLICATION_EXCLUSIONS
    assert "tencent__tendbcluster-tdbctl" not in consolidate.PUBLICATION_EXCLUSIONS
    why = consolidate.PUBLICATION_EXCLUSIONS["tencent__tendbcluster-tendb"]
    assert "tencent__tendbcluster-tdbctl" in why, "the reason must name the twin"


def test_main_says_nothing_about_flags_when_every_project_is_healthy(
        sandbox, con, capsys):
    """The summary must stay quiet on a clean corpus, or the operator learns to
    ignore it."""
    write_manifest(sandbox.root, ROW)
    make_project(sandbox.out, "jq", stamp="rows=3\nbytes=4\n", parquet=True)

    consolidate.main()

    out = capsys.readouterr().out
    assert "parquet missing" not in out
    assert "unreadable" not in out
    assert "publication exclusions" not in out


def test_main_doubles_a_quote_in_a_project_path_in_the_tokens_view(sandbox, con):
    """A view definition cannot bind parameters, so its file list is escaped
    instead. A project name with a quote must not unbalance the SQL."""
    write_manifest(sandbox.root, "we'ird\thttps://w.git\tcommunity\t\\.[ch]$\tS")
    make_project(sandbox.out, "we'ird", stamp="rows=2\nbytes=3\n", parquet=True)

    consolidate.main()

    view = con.find("create or replace view tokens")
    escaped = str(sandbox.out / "we'ird" / "we'ird-dataset.parquet").replace("'", "''")
    assert escaped in view
    assert view.count("'") % 2 == 0


def test_main_is_safe_to_rerun(sandbox, con, capsys):
    """Documented promise: safe to rerun any time. Two runs must emit the same
    statements, all of them `create or replace`."""
    write_manifest(sandbox.root, ROW)
    make_project(sandbox.out, "jq", stamp="rows=3\nbytes=4\n", parquet=True)

    consolidate.main()
    first = list(con.statements)
    con.statements.clear()
    consolidate.main()

    assert con.statements == first
    assert all(s.strip().startswith(("create or replace", "select"))
               for s in con.statements)


# --------------------------------------------------------------------------- #
# which manifests are indexed
#
# The manifest used to be hardcoded to manifest.tsv, which holds 4 legacy pilot
# projects (jq, zstd, libuv, tmux) whose parquets are still at the old 23-column
# schema. ctp.py's `db` command passes no arguments, so ctp.duckdb could only
# ever describe those 4: no corpus-wide query was possible at all. The corpus is
# manifest.phase1-sm.tsv (187 projects) + manifest.linux.tsv (1).
# --------------------------------------------------------------------------- #

SM_ROW = "dpdk__dpdk\thttps://x.git/dpdk\tfoundation\t\\.[ch]$\tL"
LINUX_ROW = "torvalds__linux\t/staging/linux.git\tfoundation\t\\.[ch]$\tL"
CANDIDATE_ROW = "never__run\thttps://x.git/nr\tcommunity\t\\.[ch]$\tS"


def write_named_manifest(root: Path, filename: str, *lines: str) -> Path:
    path = root / filename
    path.write_text("".join(f"{line}\n" for line in lines))
    return path


def corpus_manifests(root: Path) -> tuple[Path, Path]:
    """The two manifests that make up the run set, as the repo carries them."""
    return (write_named_manifest(root, "manifest.phase1-sm.tsv", SM_ROW),
            write_named_manifest(root, "manifest.linux.tsv", LINUX_ROW))


def test_the_default_manifest_set_is_the_corpus_run_set(sandbox):
    """THE DEFECT. The default must be what was actually run, not the 4 legacy
    pilots, or no corpus-wide query is possible."""
    sm, linux = corpus_manifests(sandbox.root)
    write_manifest(sandbox.root, ROW)                  # the legacy pilots exist

    assert consolidate.default_manifests() == [sm, linux]


def test_the_default_set_never_includes_the_candidate_manifest(sandbox):
    """manifest.generated.tsv is 3,948 CANDIDATE rows that were never run.
    Indexing it would stat 3,948 absent workdirs and emit that many phantom
    QUEUED rows."""
    corpus_manifests(sandbox.root)
    write_named_manifest(sandbox.root, "manifest.generated.tsv", CANDIDATE_ROW)

    chosen = consolidate.default_manifests()

    assert all("generated" not in p.name for p in chosen)
    assert [r[0] for r in consolidate.project_rows(chosen)] == [
        "dpdk__dpdk", "torvalds__linux"]


def test_project_rows_by_default_indexes_the_corpus_not_the_pilots(sandbox):
    """project_rows() with no list must agree with default_manifests(), so the
    two entry points cannot drift apart."""
    corpus_manifests(sandbox.root)
    write_manifest(sandbox.root, ROW)

    names = [r[0] for r in consolidate.project_rows()]

    assert names == ["dpdk__dpdk", "torvalds__linux"]
    assert "jq" not in names


def test_project_rows_takes_the_manifest_list_as_a_parameter(sandbox):
    """The list is an argument, not a global, so a caller decides what is ground
    truth. The other manifests on disk must not leak in."""
    corpus_manifests(sandbox.root)
    picked = write_named_manifest(sandbox.root, "manifest.mine.tsv", ROW)

    assert [r[0] for r in consolidate.project_rows([picked])] == ["jq"]


def test_main_indexes_the_corpus_run_set_by_default(sandbox, con, capsys):
    """End to end through the CLI path ctp.py uses: no arguments at all."""
    corpus_manifests(sandbox.root)
    write_manifest(sandbox.root, ROW)
    make_project(sandbox.out, "dpdk__dpdk", stamp="rows=5\n", parquet=True)
    make_project(sandbox.out, "torvalds__linux", stamp="rows=6\n", parquet=True)

    consolidate.main()

    _sql, rows = con.batches[0]
    assert [r[0] for r in rows] == ["dpdk__dpdk", "torvalds__linux"]
    out = capsys.readouterr().out
    assert "manifests: manifest.phase1-sm.tsv, manifest.linux.tsv" in out
    assert "DONE: 2" in out


def test_manifest_given_once_indexes_only_that_manifest(sandbox, con):
    """Someone who wants the legacy pilots asks for them by name, and gets
    nothing else."""
    corpus_manifests(sandbox.root)
    write_manifest(sandbox.root, ROW)

    consolidate.main(["--manifest", "manifest.tsv"])

    _sql, rows = con.batches[0]
    assert [r[0] for r in rows] == ["jq"]


def test_manifest_given_twice_indexes_both_manifests(sandbox, con, capsys):
    """--manifest is repeatable, and the union is indexed in the order given."""
    write_named_manifest(sandbox.root, "a.tsv", SM_ROW)
    write_named_manifest(sandbox.root, "b.tsv", LINUX_ROW)

    consolidate.main(["--manifest", "b.tsv", "--manifest", "a.tsv"])

    _sql, rows = con.batches[0]
    assert [r[0] for r in rows] == ["torvalds__linux", "dpdk__dpdk"]
    assert "manifests: b.tsv, a.tsv" in capsys.readouterr().out


def test_a_project_named_by_two_manifests_is_indexed_once(sandbox, con, capsys):
    """The projects table has name as its primary key, so a repeat would abort
    the insert batch for every project. The repeat is named, not silent."""
    write_named_manifest(sandbox.root, "a.tsv", SM_ROW, LINUX_ROW)
    write_named_manifest(sandbox.root, "b.tsv", LINUX_ROW)

    consolidate.main(["--manifest", "a.tsv", "--manifest", "b.tsv"])

    _sql, rows = con.batches[0]
    assert [r[0] for r in rows] == ["dpdk__dpdk", "torvalds__linux"]
    assert "torvalds__linux is named twice" in capsys.readouterr().err


def test_a_duplicate_inside_one_manifest_is_also_indexed_once(sandbox):
    """De-duplication is by project name, so it holds within a manifest too."""
    one = write_named_manifest(sandbox.root, "a.tsv", LINUX_ROW, LINUX_ROW)
    assert [r[0] for r in consolidate.project_rows([one])] == ["torvalds__linux"]


def test_an_empty_manifest_indexes_nothing_and_builds_no_view(sandbox, con,
                                                              capsys):
    """An empty run set is a valid state. read_parquet([]) is invalid SQL, so
    the view must be skipped rather than written empty."""
    empty = write_named_manifest(sandbox.root, "empty.tsv")

    assert consolidate.project_rows([empty]) == []
    consolidate.main(["--manifest", "empty.tsv"])

    _sql, rows = con.batches[0]
    assert rows == []
    assert not [s for s in con.statements if "view tokens" in s]
    assert "tokens view: 0 rows across 0 projects" in capsys.readouterr().out


def test_an_absent_corpus_manifest_is_named_and_the_other_still_indexes(
        sandbox, capsys):
    """187 projects silently becoming 1 must be visible."""
    write_named_manifest(sandbox.root, "manifest.linux.tsv", LINUX_ROW)

    chosen = consolidate.default_manifests()

    assert [p.name for p in chosen] == ["manifest.linux.tsv"]
    assert "manifest.phase1-sm.tsv is absent" in capsys.readouterr().err


def test_the_run_set_falls_back_to_manifest_tsv_when_no_corpus_manifest_exists(
        sandbox):
    """A fixture tree or a checkout without the corpus manifests still has
    manifest.tsv, and that is the only manifest every checkout carries."""
    assert consolidate.default_manifests() == [sandbox.root / "manifest.tsv"]


@pytest.mark.parametrize("value", ["manifest.tsv", "sub/manifest.tsv"])
def test_a_relative_manifest_resolves_against_the_repo(sandbox, value):
    """ctp.py runs this script with cwd set to the cregit directory, so a
    relative value must not be read against the cwd."""
    assert consolidate.resolve_manifest(value) == sandbox.root / value


def test_an_absolute_manifest_is_used_as_given(sandbox, tmp_path):
    """An absolute path is the escape hatch for a manifest outside the repo."""
    elsewhere = tmp_path / "elsewhere" / "manifest.tsv"
    assert consolidate.resolve_manifest(str(elsewhere)) == elsewhere


# --------------------------------------------------------------------------- #
# the schema gate
#
# read_parquet([...]) binds one schema for the whole list, so one legacy file at
# 23 or 38 columns aborts the tokens view for every project. These tests need
# real parquet files, so they are skipped when duckdb is the stub.
# --------------------------------------------------------------------------- #

requires_duckdb = pytest.mark.skipif(
    DUCKDB_IS_STUBBED, reason="needs real duckdb (devenv shell) to write parquet")


def write_parquet(path: Path, columns) -> Path:
    """A one-row parquet with exactly `columns` — (name, duckdb type) pairs."""
    import duckdb as real_duckdb

    select = ", ".join(
        (f"'{path.parent.name}' as {name}" if name == "repo_name"
         else f'cast(null as {typ}) as "{name}"')
        for name, typ in columns)
    real_duckdb.sql(f"copy (select {select}) to '{path}' (format parquet)")
    return path


def make_parquet_project(out: Path, name: str, columns) -> Path:
    """A DONE project whose parquet really is a parquet with `columns`."""
    workdir = make_project(out, name, stamp="rows=1\nbytes=2\n")
    write_parquet(workdir / f"{name}-dataset.parquet", columns)
    return workdir


LEGACY_23_COLUMNS = consolidate.EXPECTED_COLUMNS[:22] + (("repo_tag", "VARCHAR"),)


def test_the_schema_contract_comes_from_validate_schema(sandbox):
    """The gate must follow a schema widening automatically, so the contract is
    imported, never a column count copied into consolidate.py."""
    import validate_schema

    assert consolidate.EXPECTED_COLUMNS is validate_schema.EXPECTED_COLUMNS


@requires_duckdb
def test_schema_split_keeps_a_parquet_that_matches_the_contract(sandbox):
    """The gate must not be so tight that the corpus cannot be indexed."""
    good = (make_parquet_project(sandbox.out, "dpdk__dpdk",
                                 consolidate.EXPECTED_COLUMNS)
            / "dpdk__dpdk-dataset.parquet")

    usable, drifted, unread = consolidate.schema_split([str(good)])

    assert (usable, drifted, unread) == ([str(good)], [], [])


@requires_duckdb
def test_schema_split_leaves_out_a_mismatched_parquet(sandbox):
    """A 23-column legacy file is the poison this gate exists for."""
    legacy = (make_parquet_project(sandbox.out, "jq", LEGACY_23_COLUMNS)
              / "jq-dataset.parquet")

    usable, drifted, unread = consolidate.schema_split([str(legacy)])

    assert usable == []
    assert unread == []
    assert len(drifted) == 1
    path, n_columns, drifts = drifted[0]
    assert path == str(legacy)
    assert n_columns == 23
    assert drifts


@requires_duckdb
def test_main_leaves_a_mismatched_parquet_out_of_the_tokens_view(sandbox, con,
                                                                capsys):
    """THE SECOND DEFECT. Before the gate, one legacy 23-column file aborted the
    whole view, so no corpus-wide query worked at all."""
    corpus_manifests(sandbox.root)
    write_manifest(sandbox.root, ROW)
    make_parquet_project(sandbox.out, "dpdk__dpdk", consolidate.EXPECTED_COLUMNS)
    make_parquet_project(sandbox.out, "jq", LEGACY_23_COLUMNS)

    consolidate.main(["--manifest", "manifest.phase1-sm.tsv",
                      "--manifest", "manifest.tsv"])

    view = con.find("create or replace view tokens")
    assert "dpdk__dpdk-dataset.parquet" in view
    assert "jq-dataset.parquet" not in view
    out = capsys.readouterr().out
    assert "tokens view: 1,234,567 rows across 1 projects" in out
    # named and counted, or the row count silently looks plausible
    assert "schema mismatch, left out of the tokens view: 1 of 2" in out
    assert "jq-dataset.parquet (23 columns)" in out
    assert f"the contract is {len(consolidate.EXPECTED_COLUMNS)} columns" in out


@requires_duckdb
def test_every_mismatched_parquet_is_named_and_counted(sandbox, con, capsys):
    """Reporting only the first would hide the second, and the caller cannot
    act on a file it is not told about."""
    write_named_manifest(sandbox.root, "a.tsv", SM_ROW, ROW, LINUX_ROW)
    make_parquet_project(sandbox.out, "dpdk__dpdk", consolidate.EXPECTED_COLUMNS)
    make_parquet_project(sandbox.out, "jq", LEGACY_23_COLUMNS)
    make_parquet_project(sandbox.out, "torvalds__linux",
                         consolidate.EXPECTED_COLUMNS[:38])

    consolidate.main(["--manifest", "a.tsv"])

    out = capsys.readouterr().out
    assert "schema mismatch, left out of the tokens view: 2 of 3" in out
    assert "jq-dataset.parquet (23 columns)" in out
    assert "torvalds__linux-dataset.parquet (38 columns)" in out
    assert "tokens view: 1,234,567 rows across 1 projects" in out


@requires_duckdb
def test_a_wrong_column_type_is_a_mismatch_too(sandbox):
    """Same columns with token_index as VARCHAR would union into silent nulls,
    which is worse than being left out."""
    retyped = tuple((name, "VARCHAR" if name == "token_index" else typ)
                    for name, typ in consolidate.EXPECTED_COLUMNS)
    path = (make_parquet_project(sandbox.out, "dpdk__dpdk", retyped)
            / "dpdk__dpdk-dataset.parquet")

    usable, drifted, _unread = consolidate.schema_split([str(path)])

    assert usable == []
    assert [d[1] for d in drifted] == [len(consolidate.EXPECTED_COLUMNS)]


def test_a_parquet_whose_schema_cannot_be_read_stays_in_and_is_reported(
        sandbox, con, capsys):
    """A file that is not readable at all cannot be shown to disagree with the
    contract, and a silent exclusion is worse than a loud failure: it is
    reported on stderr and left in the list."""
    write_manifest(sandbox.root, ROW)
    make_project(sandbox.out, "jq", stamp="rows=1\nbytes=2\n", parquet=True)

    consolidate.main(["--manifest", "manifest.tsv"])

    assert "jq-dataset.parquet" in con.find("create or replace view tokens")
    captured = capsys.readouterr()
    assert "schema not read for 1 of 1 parquet(s)" in captured.err
    assert "jq-dataset.parquet" in captured.err
    assert "schema mismatch" not in captured.out
