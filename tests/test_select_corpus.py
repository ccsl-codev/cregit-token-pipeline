"""Unit tests for select_corpus.py.

The corpus is a research instrument. A silent labelling or filtering regression
does not crash anything — it changes the population under study, and the paper
reports a finding about a corpus nobody can reconstruct. So the tests below are
weighted toward the rules that decide membership and stratum, not toward
plumbing.

Two rules carry most of the weight:

  1. gh() must tell "this repository is gone" apart from "GitHub said no for
     now". cmd_enrich caches the answer forever. A throttle cached as dead
     removes a live project from the corpus permanently and silently.
  2. A stratum label comes from a control fact — who controls the project —
     never from measured contribution composition. judge() must not quietly
     relabel a positive roster fact.

No test touches the network, the real `gh`, or the real .corpus-cache. The
`sandbox` fixture redirects every module path constant into tmp_path and makes
any unmocked subprocess or urlopen call fail the test.
"""

from __future__ import annotations

import json
import random
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import select_corpus as sc


# ---------------------------------------------------------------- fixtures


def _boom(*a, **k):
    """Fail the test loudly if production code reaches the outside world.

    pytest's Failed is a BaseException, so it escapes gh()'s `except
    Exception` retry loop instead of being swallowed as a transient error.
    """
    pytest.fail(f"unmocked outside call: {a!r} {k!r}")


@pytest.fixture(autouse=True)
def sandbox(tmp_path, monkeypatch):
    """Point every module path constant inside tmp_path; forbid real I/O.

    A long enrich job owns the real .corpus-cache while these tests run, so no
    test may read or write it. time.sleep becomes a recorder, so the retry
    paths are exercised without spending wall-clock time.
    """
    cache = tmp_path / ".corpus-cache"
    cache.mkdir()
    sources = tmp_path / "sources"

    monkeypatch.setattr(sc, "CORPUS", tmp_path)
    monkeypatch.setattr(sc, "CACHE", cache)
    monkeypatch.setattr(sc, "CANDIDATES", tmp_path / "candidates.csv")
    monkeypatch.setattr(sc, "MANIFEST_OUT", tmp_path / "manifest.generated.tsv")
    monkeypatch.setattr(sc, "SPINELLIS_TSV", sources / "enterprise_projects.txt")
    monkeypatch.setattr(sc, "COHORT_TSV", sources / "cohort_project_details.txt")
    # REVIEW_OUT is derived from CORPUS at import time, so patching CORPUS alone
    # would let cmd_review overwrite the real docs/CORPUS-REVIEW.md.
    monkeypatch.setattr(sc, "REVIEW_OUT", tmp_path / "docs" / "CORPUS-REVIEW.md")
    # Same reason for the sample outputs: both are derived from CORPUS at import
    # time, so patching CORPUS alone would let cmd_sample overwrite the real
    # manifest.sample.tsv and docs/CORPUS-SAMPLE.md.
    monkeypatch.setattr(sc, "SAMPLE_OUT", tmp_path / "manifest.sample.tsv")
    monkeypatch.setattr(sc, "SAMPLE_DOC", tmp_path / "docs" / "CORPUS-SAMPLE.md")

    slept: list[float] = []
    monkeypatch.setattr(sc.time, "sleep", lambda s: slept.append(s))
    # Pacing off by default, so a test that counts sleeps counts retry sleeps
    # only. The pacing tests switch it back on explicitly.
    monkeypatch.setattr(sc, "MIN_CALL_INTERVAL_S", 0)
    monkeypatch.setattr(sc, "BUDGET_FLOOR", 0)
    # Backoff is jittered in production. Tests want the ceiling, so the assertions
    # stay exact rather than ranged.
    monkeypatch.setattr(sc.random, "uniform", lambda lo, hi: hi)
    sc.reset_pacing()
    monkeypatch.setattr(sc.subprocess, "run", _boom)
    monkeypatch.setattr(sc.urllib.request, "urlopen", _boom)

    return SimpleNamespace(root=tmp_path, cache=cache, sources=sources, slept=slept)


def cp(rc: int, out: str = "", err: str = "") -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(args=["gh"], returncode=rc, stdout=out, stderr=err)


RATE_LIMIT_OK = json.dumps({"resources": {"core": {"remaining": 5000, "reset": 0},
                                          "graphql": {"remaining": 5000, "reset": 0}}})


def runner(rc: int = 0, out: str = "", err: str = "", raises: Exception | None = None):
    """A fake subprocess.run for gh().

    `gh api rate_limit` always answers "quota fine", because rate_limit_wait()
    is a separate unit under test and must not perturb the retry accounting.
    """
    calls: list[list[str]] = []

    def run(argv, **kw):
        calls.append(list(argv))
        if list(argv[:3]) == ["gh", "api", "rate_limit"]:
            return cp(0, RATE_LIMIT_OK)
        if raises is not None:
            raise raises
        return cp(rc, out, err)

    run.calls = calls
    return run


def api_calls(run) -> list[list[str]]:
    """gh calls that spend quota, i.e. everything but the free rate_limit probe."""
    return [c for c in run.calls if list(c[:3]) != ["gh", "api", "rate_limit"]]


# ---------------------------------------------------------------- builders


def fresh_pushed(days: int = 1) -> str:
    d = datetime.now(timezone.utc) - timedelta(days=days)
    return d.isoformat().replace("+00:00", "Z")


def cand(**over) -> dict:
    """A candidate row as a source parser emits it."""
    c = dict(source="ghsearch", stratum="community", fact="F1_residual:none",
             owner="acme", repo="widget", roster_lang="c", roster_name="widget")
    c.update(over)
    return c


def good_meta(**over) -> dict:
    """Enriched metadata that passes every eligibility filter."""
    m = dict(full_name="acme/widget", language="C", size_kb=5_000,
             pushed_at=fresh_pushed(), archived=False, fork=False, stars=100,
             license="MIT", owner_type="Organization", commits=1_000,
             clone_url="https://github.com/acme/widget.git",
             owner_is_company=False, company_fact="")
    m.update(over)
    return m


def repo_payload(**over) -> dict:
    """A `gh api repos/o/r` body."""
    d = dict(full_name="acme/widget", language="C", size=5_000,
             pushed_at=fresh_pushed(), archived=False, fork=False,
             stargazers_count=10, license={"spdx_id": "MIT"},
             clone_url="https://github.com/acme/widget.git",
             owner={"login": "acme", "type": "Organization"})
    d.update(over)
    return d


def sp_line(**over) -> str:
    """One tab-separated Spinellis enterprise-dataset row."""
    row = {c: "" for c in sc.SPINELLIS_COLS}
    row.update({k: str(v) for k, v in over.items()})
    return "\t".join(row[c] for c in sc.SPINELLIS_COLS)


def sp_ok(url: str, company: str = "Acme", tier: str = "fg500",
          lines: int = 50_000, commits: int = 1_000, year: str = "2020") -> str:
    """A Spinellis row that passes every filter, in the named provenance tier."""
    flags = {"fg500": "f", "sec10k": "f", "sec20f": "f"}
    flags[tier] = "t"
    return sp_line(url=url, company_name=company, lines=lines, commit_count=commits,
                   most_recent_commit=f"{year}-06-01 00:00:00",
                   project_name=url.rsplit("/", 1)[-1], **flags)


def cohort_line(url: str, stars, commits, pid: str = "1") -> str:
    return "\t".join([url, pid, str(stars), str(commits)])


def write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


# ================================================================ gh()
#
# The highest-value group. gh() returns the body, returns None only when the
# resource is genuinely gone, and raises RateLimited on anything transient.


def test_gh_returns_parsed_object_on_success(monkeypatch):
    """gh() decodes the body. Without this nothing downstream has data."""
    run = runner(0, out='{"full_name": "acme/widget"}')
    monkeypatch.setattr(sc.subprocess, "run", run)
    assert sc.gh("repos/acme/widget") == {"full_name": "acme/widget"}
    assert len(api_calls(run)) == 1


def test_gh_returns_none_on_unparseable_json(monkeypatch):
    """Exit 0 plus garbage is not retryable, so gh() reports "no body"."""
    monkeypatch.setattr(sc.subprocess, "run", runner(0, out="<html>nope</html>"))
    assert sc.gh("repos/acme/widget") is None


@pytest.mark.parametrize("stderr", [
    "gh: request failed with status 404",
    "gh: Not Found",
    "Could not resolve to a Repository with the name 'acme/widget'.",
    "GraphQL: Repository does not exist",
    "gh: Empty Repository (HTTP 409)",
])
def test_gh_returns_none_when_resource_is_gone(monkeypatch, stderr):
    """Each spelling GitHub uses for "gone" must map to None.

    Research correctness: None is the only answer cmd_enrich is allowed to
    cache as dead. `Could not resolve to a Repository` is how GraphQL reports a
    missing repo, with exit code 1 and no 404 anywhere in the message, so
    matching on "404" alone would misfile it as transient forever.
    """
    monkeypatch.setattr(sc.subprocess, "run", runner(1, err=stderr))
    assert sc.gh("repos/acme/widget", retries=2) is None


@pytest.mark.xfail(reason="BUG: the guard reads 'empty repository', but GitHub "
                          "says 'Git Repository is empty.' — the two words are "
                          "in the opposite order, so the branch never fires.")
def test_gh_recognises_githubs_own_empty_repository_wording(monkeypatch):
    """GitHub answers an empty repo with 409 "Git Repository is empty.".

    gh() looks for the substring "empty repository", which never appears in
    that message, so the intended "gone" branch is unreachable and the call
    retries until it raises RateLimited instead.
    """
    monkeypatch.setattr(sc.subprocess, "run",
                        runner(1, err="gh: Git Repository is empty. (HTTP 409)"))
    assert sc.gh("repos/acme/blank", retries=2) is None


def test_gh_gone_detection_reads_stdout_too(monkeypatch):
    """gh writes GraphQL errors to stdout, so both streams must be scanned."""
    monkeypatch.setattr(sc.subprocess, "run", runner(1, out="Could not resolve to a Repository"))
    assert sc.gh("graphql", "-f", "query={}") is None


def test_gh_gone_is_not_retried(monkeypatch):
    """A 404 is a fact. Retrying it burns quota for no new information."""
    run = runner(1, err="HTTP 404: Not Found")
    monkeypatch.setattr(sc.subprocess, "run", run)
    sc.gh("repos/acme/ghost", retries=6)
    assert len(api_calls(run)) == 1


def test_gh_rate_limit_retries_then_raises(monkeypatch, sandbox):
    """A quota message must raise, never return None.

    Research correctness: a throttled call cached as dead removes a live
    project from the corpus permanently. An earlier run marked 2,734 of 5,803
    rows unreachable this way.
    """
    run = runner(1, err="API rate limit exceeded for user ID 1.")
    monkeypatch.setattr(sc.subprocess, "run", run)
    with pytest.raises(sc.RateLimited):
        sc.gh("repos/acme/widget", retries=3)
    # A quota pause is a wait, not a failed request, so it spends its own budget
    # rather than `retries`. Sharing one budget is what let six 120s sleeps abort
    # a 24,405-row pass at row 7,894.
    assert len(api_calls(run)) == sc.MAX_QUOTA_PAUSES
    assert len(sandbox.slept) == sc.MAX_QUOTA_PAUSES


def test_gh_403_retries_then_raises(monkeypatch, sandbox):
    """403 is a queue, not a grave: it must retry and then raise."""
    run = runner(1, err="HTTP 403: Forbidden")
    monkeypatch.setattr(sc.subprocess, "run", run)
    with pytest.raises(sc.RateLimited):
        sc.gh("repos/acme/widget", retries=2)
    assert len(api_calls(run)) == sc.MAX_QUOTA_PAUSES
    assert sandbox.slept  # it really does back off


def test_a_quota_pause_does_not_spend_the_error_retry_budget(monkeypatch, sandbox):
    """The two budgets must stay separate, and the bound must be the quota one.

    With retries=1 a shared budget would give up after a single API call. The
    quota budget is larger on purpose, because waiting out a limit is the correct
    behaviour and abandoning the pass is not.
    """
    run = runner(1, err="API rate limit exceeded for user ID 1.")
    monkeypatch.setattr(sc.subprocess, "run", run)
    with pytest.raises(sc.RateLimited):
        sc.gh("repos/acme/widget", retries=1)
    assert len(api_calls(run)) == sc.MAX_QUOTA_PAUSES > 1


def test_a_server_error_still_spends_the_error_retry_budget(monkeypatch, sandbox):
    """The separation must not leak the other way: a 502 is a real failure and
    must stay bounded by `retries`, not by the larger quota budget."""
    run = runner(1, err="HTTP 502: Bad Gateway")
    monkeypatch.setattr(sc.subprocess, "run", run)
    with pytest.raises(sc.RateLimited):
        sc.gh("repos/acme/widget", retries=2)
    assert len(api_calls(run)) == 2


@pytest.mark.parametrize("stderr", [
    "HTTP 502: Bad Gateway",
    "HTTP 500: Internal Server Error",
    "dial tcp: lookup api.github.com: no such host",
])
def test_gh_unrecognised_failure_retries_with_backoff(monkeypatch, sandbox, stderr):
    """An unclassified failure must be treated as transient, not as gone."""
    run = runner(1, err=stderr)
    monkeypatch.setattr(sc.subprocess, "run", run)
    with pytest.raises(sc.RateLimited):
        sc.gh("repos/acme/widget", retries=4)
    assert len(api_calls(run)) == 4
    assert sandbox.slept == [1, 2, 4, 8]  # exponential 2 ** attempt


def test_gh_exit_zero_with_empty_body_is_transient(monkeypatch, sandbox):
    """An empty success body means nothing was returned, so retry."""
    run = runner(0, out="   ")
    monkeypatch.setattr(sc.subprocess, "run", run)
    with pytest.raises(sc.RateLimited):
        sc.gh("repos/acme/widget", retries=2)
    assert len(api_calls(run)) == 2


def test_gh_subprocess_exception_retries_then_raises(monkeypatch, sandbox):
    """Network down must raise RateLimited, not be cached as a dead repo."""
    run = runner(raises=OSError("network is unreachable"))
    monkeypatch.setattr(sc.subprocess, "run", run)
    with pytest.raises(sc.RateLimited):
        sc.gh("repos/acme/widget", retries=3)
    assert len(api_calls(run)) == 3
    assert sandbox.slept == [1, 2, 4]


def test_gh_raises_with_the_arguments_in_the_message(monkeypatch):
    """The message names the failed call, so a stopped run can be resumed."""
    monkeypatch.setattr(sc.subprocess, "run", runner(1, err="HTTP 502"))
    with pytest.raises(sc.RateLimited, match="repos/acme/widget"):
        sc.gh("repos/acme/widget", retries=1)


def test_gh_recovers_on_a_later_attempt(monkeypatch):
    """A transient failure followed by success must return the body."""
    seq = [cp(1, err="HTTP 502"), cp(0, out='{"ok": true}')]
    calls = []

    def run(argv, **kw):
        calls.append(list(argv))
        return seq[len(calls) - 1]

    monkeypatch.setattr(sc.subprocess, "run", run)
    assert sc.gh("repos/acme/widget", retries=4) == {"ok": True}
    assert len(calls) == 2


# ================================================================ rate_limit_wait()


def test_rate_limit_wait_returns_seconds_to_reset(monkeypatch):
    """An exhausted resource must be waited out, not polled."""
    reset = int(datetime.now(timezone.utc).timestamp()) + 10_000
    body = json.dumps({"resources": {"core": {"remaining": 0, "reset": reset},
                                     "graphql": {"remaining": 500, "reset": 0}}})
    monkeypatch.setattr(sc.subprocess, "run", lambda *a, **k: cp(0, body))
    wait, kind = sc.rate_limit_wait()
    assert wait >= 10_000
    assert kind == "primary", "an exhausted core bucket is a primary limit"


def test_rate_limit_wait_never_returns_less_than_the_default(monkeypatch):
    """A reset already in the past must not produce a zero or negative sleep."""
    past = int(datetime.now(timezone.utc).timestamp()) - 5_000
    body = json.dumps({"resources": {"graphql": {"remaining": 0, "reset": past}}})
    monkeypatch.setattr(sc.subprocess, "run", lambda *a, **k: cp(0, body))
    assert sc.rate_limit_wait(default=120) == (120, "primary")


def test_rate_limit_wait_reports_secondary_when_the_budget_looks_intact(monkeypatch):
    """The defect this replaced. A bucket that reports remaining budget while the
    real call was refused is the undocumented secondary limit, not a healthy
    quota. Observed directly: repos/torvalds/linux answered 403 in the same second
    that rate_limit reported core 5000/5000.

    Treating it as healthy returned a 120s wait, and six of those aborted a
    24,405-row pass at row 7,894."""
    monkeypatch.setattr(sc.subprocess, "run", lambda *a, **k: cp(0, RATE_LIMIT_OK))
    wait, kind = sc.rate_limit_wait(default=77)
    assert kind == "secondary"
    assert wait == sc.SECONDARY_BACKOFF[0], "a secondary limit needs a long wait"


def test_secondary_backoff_escalates_and_then_holds(monkeypatch):
    """A blind wait must grow, because there is no reset instant to read, and
    retrying a secondary limit too eagerly extends it."""
    monkeypatch.setattr(sc.subprocess, "run", lambda *a, **k: cp(0, RATE_LIMIT_OK))
    waits = [sc.rate_limit_wait(default=1, attempt=i)[0] for i in range(8)]
    assert waits[:5] == list(sc.SECONDARY_BACKOFF), waits
    assert waits[5:] == [sc.SECONDARY_BACKOFF[-1]] * 3, "must hold, not grow for ever"


@pytest.mark.parametrize("body", ["", "not json", '{"resources": {}}', '{"nope": 1}'])
def test_rate_limit_wait_survives_malformed_output(monkeypatch, body):
    """A broken probe must degrade to the default, never abort the run.

    '{"resources": {}}' is the subtle one: no bucket was reported at all. That is
    missing data, not a healthy budget, so it must NOT be read as a secondary
    limit and earn a 300s sleep."""
    monkeypatch.setattr(sc.subprocess, "run", lambda *a, **k: cp(0, body))
    assert sc.rate_limit_wait(default=42) == (42, "unknown")


def test_rate_limit_wait_survives_a_crash(monkeypatch):
    """gh missing from PATH must not turn a throttle into a traceback."""
    monkeypatch.setattr(sc.subprocess, "run",
                        lambda *a, **k: (_ for _ in ()).throw(FileNotFoundError("gh")))
    assert sc.rate_limit_wait(default=9) == (9, "unknown")


# ================================================================ save_json()


def test_save_json_writes_through_a_tmp_file_then_replaces(monkeypatch, tmp_path):
    """The cache is written atomically.

    Research correctness: repo-meta.json costs thousands of API calls. An
    interrupted run must not leave it truncated.
    """
    seen = []
    orig = Path.replace

    def spy(self, target):
        seen.append((str(self), str(target)))
        return orig(self, target)

    monkeypatch.setattr(Path, "replace", spy)
    dest = tmp_path / "sub" / "repo-meta.json"
    sc.save_json(dest, {"a/b": {"language": "C"}})

    assert json.loads(dest.read_text()) == {"a/b": {"language": "C"}}
    assert len(seen) == 1
    assert seen[0][0] == str(dest) + ".tmp"
    assert seen[0][1] == str(dest)
    assert not Path(seen[0][0]).exists()


def test_save_json_leaves_the_old_file_intact_when_serialising_fails(tmp_path):
    """A failed write must not destroy the previous cache."""
    dest = tmp_path / "repo-meta.json"
    dest.write_text('{"kept": 1}')
    with pytest.raises(TypeError):
        sc.save_json(dest, {"bad": {1, 2}})  # a set is not JSON serialisable
    assert json.loads(dest.read_text()) == {"kept": 1}


# ================================================================ cmd_enrich()


def enrich_args(limit: int = 0, retry_errors: bool = False) -> SimpleNamespace:
    return SimpleNamespace(limit=limit, retry_errors=retry_errors)


@pytest.fixture
def one_candidate(monkeypatch):
    monkeypatch.setattr(sc, "collect_candidates", lambda: [cand()])
    monkeypatch.setattr(sc, "commit_count", lambda o, r: 1_234)
    monkeypatch.setattr(sc, "org_is_company", lambda login, cache: (False, ""))


def test_enrich_caches_a_genuine_404_as_unreachable(sandbox, monkeypatch, one_candidate):
    """A repo GitHub says is gone is recorded as gone, so it is not re-fetched."""
    monkeypatch.setattr(sc, "gh", lambda *a, **k: None)
    assert sc.cmd_enrich(enrich_args()) == 0
    meta = json.loads((sandbox.cache / "repo-meta.json").read_text())
    assert meta == {"acme/widget": {"error": "unreachable"}}


def test_enrich_stores_the_full_metadata_record(sandbox, monkeypatch, one_candidate):
    """Every field judge() filters on must reach the cache."""
    monkeypatch.setattr(sc, "gh", lambda *a, **k: repo_payload())
    assert sc.cmd_enrich(enrich_args()) == 0
    rec = json.loads((sandbox.cache / "repo-meta.json").read_text())["acme/widget"]
    assert rec["language"] == "C"
    assert rec["size_kb"] == 5_000
    assert rec["license"] == "MIT"
    assert rec["owner_type"] == "Organization"
    assert rec["commits"] == 1_234
    assert rec["owner_is_company"] is False
    assert "error" not in rec


def test_enrich_ratelimited_writes_no_error_entry_and_exits_2(sandbox, monkeypatch):
    """A throttle mid-run must leave the failing repo absent, not dead.

    Research correctness: this is the regression that silently deleted 47% of
    the corpus. The repo that failed must be missing from the cache so the next
    run retries it; writing {"error": ...} would make the loss permanent.
    """
    monkeypatch.setattr(sc, "collect_candidates",
                        lambda: [cand(repo="widget"), cand(repo="ghost")])
    monkeypatch.setattr(sc, "commit_count", lambda o, r: 5)
    monkeypatch.setattr(sc, "org_is_company", lambda login, cache: (False, ""))

    def fake_gh(*args, **kw):
        if args[0].endswith("widget"):
            return repo_payload()
        raise sc.RateLimited(args[0])

    monkeypatch.setattr(sc, "gh", fake_gh)

    assert sc.cmd_enrich(enrich_args()) == 2
    meta = json.loads((sandbox.cache / "repo-meta.json").read_text())
    assert "acme/widget" in meta                      # progress was saved
    assert "acme/ghost" not in meta                   # and nothing was condemned
    assert not any(v.get("error") for v in meta.values())


def test_enrich_keyboardinterrupt_saves_progress_and_exits_2(sandbox, monkeypatch):
    """Ctrl-C must persist what was already paid for."""
    monkeypatch.setattr(sc, "collect_candidates",
                        lambda: [cand(repo="widget"), cand(repo="ghost")])
    monkeypatch.setattr(sc, "commit_count", lambda o, r: 5)
    monkeypatch.setattr(sc, "org_is_company", lambda login, cache: (False, ""))

    def fake_gh(*args, **kw):
        if args[0].endswith("widget"):
            return repo_payload()
        raise KeyboardInterrupt

    monkeypatch.setattr(sc, "gh", fake_gh)
    assert sc.cmd_enrich(enrich_args()) == 2
    assert "acme/widget" in json.loads((sandbox.cache / "repo-meta.json").read_text())


def test_enrich_retry_errors_drops_only_the_error_entries(sandbox, monkeypatch):
    """--retry-errors re-fetches failures and leaves good records untouched."""
    write(sandbox.cache / "repo-meta.json", json.dumps({
        "acme/dead": {"error": "unreachable"},
        "acme/good": {"language": "Java", "commits": 7},
    }))
    monkeypatch.setattr(sc, "collect_candidates",
                        lambda: [cand(repo="dead"), cand(repo="good")])
    monkeypatch.setattr(sc, "commit_count", lambda o, r: 99)
    monkeypatch.setattr(sc, "org_is_company", lambda login, cache: (False, ""))
    fetched = []

    def fake_gh(*args, **kw):
        fetched.append(args[0])
        return repo_payload()

    monkeypatch.setattr(sc, "gh", fake_gh)

    assert sc.cmd_enrich(enrich_args(retry_errors=True)) == 0
    meta = json.loads((sandbox.cache / "repo-meta.json").read_text())
    assert fetched == ["repos/acme/dead"]                     # only the failure
    assert "error" not in meta["acme/dead"]
    assert meta["acme/dead"]["commits"] == 99
    assert meta["acme/good"] == {"language": "Java", "commits": 7}


def test_enrich_already_enriched_check_is_case_insensitive(sandbox, monkeypatch):
    """`Owner/Repo` in the cache must satisfy a candidate spelled `owner/repo`.

    Research correctness: GitHub names are case-insensitive and the sources
    disagree on case. A case-sensitive check pays for a second pair of API
    calls per project and can double-count the corpus.
    """
    write(sandbox.cache / "repo-meta.json", json.dumps({"Acme/Widget": {"language": "C"}}))
    monkeypatch.setattr(sc, "collect_candidates", lambda: [cand(owner="acme", repo="widget")])
    monkeypatch.setattr(sc, "gh", _boom)          # any fetch at all is the bug

    assert sc.cmd_enrich(enrich_args()) == 0
    meta = json.loads((sandbox.cache / "repo-meta.json").read_text())
    assert list(meta) == ["Acme/Widget"]


def test_enrich_dedupes_candidates_that_differ_only_in_case(sandbox, monkeypatch):
    """Two spellings of one repo in the candidate list cost one fetch."""
    monkeypatch.setattr(sc, "collect_candidates",
                        lambda: [cand(owner="Acme", repo="Widget"),
                                 cand(owner="acme", repo="widget")])
    monkeypatch.setattr(sc, "commit_count", lambda o, r: 1)
    monkeypatch.setattr(sc, "org_is_company", lambda login, cache: (False, ""))
    fetched = []
    monkeypatch.setattr(sc, "gh", lambda *a, **k: fetched.append(a[0]) or repo_payload())

    sc.cmd_enrich(enrich_args())
    assert len(fetched) == 1


def test_enrich_honours_limit(sandbox, monkeypatch):
    """--limit truncates the candidate list, so a smoke run stays cheap."""
    monkeypatch.setattr(sc, "collect_candidates",
                        lambda: [cand(repo=f"r{i}") for i in range(5)])
    monkeypatch.setattr(sc, "commit_count", lambda o, r: 1)
    monkeypatch.setattr(sc, "org_is_company", lambda login, cache: (False, ""))
    monkeypatch.setattr(sc, "gh", lambda *a, **k: repo_payload())

    sc.cmd_enrich(enrich_args(limit=2))
    assert len(json.loads((sandbox.cache / "repo-meta.json").read_text())) == 2


def test_enrich_checkpoints_every_25_repos(sandbox, monkeypatch, capsys):
    """Periodic saves bound the loss if a long run dies."""
    monkeypatch.setattr(sc, "collect_candidates",
                        lambda: [cand(repo=f"r{i}") for i in range(26)])
    monkeypatch.setattr(sc, "commit_count", lambda o, r: 1)
    monkeypatch.setattr(sc, "org_is_company", lambda login, cache: (False, ""))
    monkeypatch.setattr(sc, "gh", lambda *a, **k: repo_payload())

    sc.cmd_enrich(enrich_args())
    assert "enriched 25 new" in capsys.readouterr().out


def test_enrich_persists_and_reloads_the_org_cache(sandbox, monkeypatch):
    """org-meta.json is reloaded as tuples, so namespaces are not re-priced."""
    write(sandbox.cache / "org-meta.json", json.dumps({"acme": [True, "F2_org:acme"]}))
    monkeypatch.setattr(sc, "collect_candidates", lambda: [cand()])
    monkeypatch.setattr(sc, "commit_count", lambda o, r: 1)
    monkeypatch.setattr(sc, "gh", lambda *a, **k: repo_payload())
    seen = {}

    def spy(login, cache):
        seen.update(cache)
        return cache[login]

    monkeypatch.setattr(sc, "org_is_company", spy)
    sc.cmd_enrich(enrich_args())
    assert seen["acme"] == (True, "F2_org:acme")
    rec = json.loads((sandbox.cache / "repo-meta.json").read_text())["acme/widget"]
    assert rec["owner_is_company"] is True


# ================================================================ org_is_company()


def test_org_is_company_hits_the_seeded_list_without_an_api_call(monkeypatch):
    """A known company namespace is F2 by definition; do not pay for it."""
    monkeypatch.setattr(sc, "gh", _boom)
    assert sc.org_is_company("google", {}) == (True, "F2_org:google")


def test_org_is_company_uses_the_cache(monkeypatch):
    """A namespace already judged is not re-fetched."""
    monkeypatch.setattr(sc, "gh", _boom)
    assert sc.org_is_company("someorg", {"someorg": (False, "")}) == (False, "")


def test_org_is_company_accepts_a_verified_org_that_names_a_company(monkeypatch):
    """A verified org with a company field is a single-counterparty namespace."""
    monkeypatch.setattr(sc, "gh", lambda *a, **k: {
        "type": "Organization", "is_verified": True, "company": "Acme Inc"})
    cache: dict = {}
    assert sc.org_is_company("acmeorg", cache) == (True, "F2_org_verified:acmeorg")
    assert cache["acmeorg"] == (True, "F2_org_verified:acmeorg")


@pytest.mark.parametrize("payload", [
    None,
    {"type": "Organization", "is_verified": False, "company": "Acme"},
    {"type": "Organization", "is_verified": True, "company": None},
    {"type": "User"},
])
def test_org_is_company_rejects_everything_else(monkeypatch, payload):
    """An unverified or anonymous namespace carries no company fact."""
    monkeypatch.setattr(sc, "gh", lambda *a, **k: payload)
    assert sc.org_is_company("someorg", {}) == (False, "")


# ================================================================ commit_count()


def test_commit_count_reads_the_graphql_total(monkeypatch):
    """size_class depends on this number."""
    monkeypatch.setattr(sc, "gh", lambda *a, **k: {"data": {"repository": {
        "defaultBranchRef": {"target": {"history": {"totalCount": 4_321}}}}}})
    assert sc.commit_count("acme", "widget") == 4_321


@pytest.mark.parametrize("payload", [None, {}, {"data": {"repository": None}}])
def test_commit_count_returns_none_on_a_shape_it_cannot_read(monkeypatch, payload):
    """An empty or missing default branch yields None, which judge() excludes."""
    monkeypatch.setattr(sc, "gh", lambda *a, **k: payload)
    assert sc.commit_count("acme", "widget") is None


# ================================================================ judge()
#
# Label and filter logic. judge() never drops a row; it records why.


def test_judge_open_residual_label_in_a_foundation_namespace(monkeypatch):
    """F1_residual:none carries no fact, so F2 may name the stratum."""
    row = sc.judge(cand(fact="F1_residual:none", stratum="community"),
                   good_meta(full_name="apache/kafka"))
    assert row["stratum"] == "foundation"
    assert row["fact"] == "F2_org_foundation:apache"


def test_judge_weak_candidate_in_a_foundation_namespace():
    """A weak=True cohort row is only an absence of a corporate signal, so a
    foundation namespace outranks it."""
    row = sc.judge(cand(fact="F1_pool:spinellis-cohort", stratum="community", weak=True),
                   good_meta(full_name="kubernetes/kubernetes"))
    assert row["stratum"] == "foundation"
    assert row["fact"] == "F2_org_foundation:kubernetes"


def test_judge_foundation_namespace_match_is_case_insensitive():
    """GitHub namespaces are case-insensitive, so the F2 test must be too."""
    row = sc.judge(cand(), good_meta(full_name="Apache/Kafka"))
    assert row["stratum"] == "foundation"


def test_judge_positive_f1_roster_survives_a_foundation_namespace():
    """A roster fact outranks a namespace guess.

    Research correctness: SFC holds the assets of its projects in trust, which
    is a positive community control fact. Letting a foundation namespace
    overwrite it would move projects between strata on a weaker fact.
    """
    row = sc.judge(cand(source="sfc", stratum="community", fact="F1_roster:sfc",
                        owner="apache", repo="thing"),
                   good_meta(full_name="apache/thing"))
    assert row["stratum"] == "community"
    assert row["fact"] == "F1_roster:sfc"


def test_judge_open_label_outside_a_foundation_namespace_is_left_alone():
    """No fact and no matching namespace means the residual label stands."""
    row = sc.judge(cand(), good_meta(full_name="randomuser/thing"))
    assert row["stratum"] == "community"
    assert row["fact"] == "F1_residual:none"


def test_judge_company_namespace_claims_an_open_label_row():
    """F2 namespace ownership names the stratum when nothing else does."""
    row = sc.judge(cand(), good_meta(owner_is_company=True, company_fact="F2_org:google",
                                     full_name="google/leveldb"))
    assert row["stratum"] == "company-owned"
    assert row["fact"] == "F2_org:google"
    assert row["contested"] == ""


def test_judge_company_namespace_without_a_fact_string_falls_back():
    """A company verdict with no fact string still records F2."""
    row = sc.judge(cand(), good_meta(owner_is_company=True, company_fact=""))
    assert row["stratum"] == "company-owned"
    assert row["fact"] == "F2_org"


def test_judge_flags_a_contested_case_instead_of_relabelling():
    """A company repo inside a foundation roster is kept and flagged.

    Research correctness: silently relabelling would erase the disagreement
    between two control facts. The row must survive with both facts recorded so
    a human can adjudicate.
    """
    row = sc.judge(cand(source="asf", stratum="foundation", fact="F1_roster:asf"),
                   good_meta(owner_is_company=True, company_fact="F2_org_verified:acme"))
    assert row["stratum"] == "foundation"
    assert row["fact"] == "F1_roster:asf"
    assert row["contested"] == "F1_roster:asf vs F2_org_verified:acme"


def test_judge_weak_foundation_row_is_not_contested():
    """A weak label has nothing to contest, so F2 simply wins."""
    row = sc.judge(cand(stratum="foundation", fact="F1_pool:spinellis-cohort", weak=True),
                   good_meta(owner_is_company=True, company_fact="F2_org:acme"))
    assert row["stratum"] == "company-owned"
    assert row["contested"] == ""


def test_judge_records_the_label_date():
    """Every row states when it was labelled, so a rerun is comparable."""
    assert sc.judge(cand(), good_meta())["label_date"] == sc.today()


def test_judge_includes_a_clean_row():
    """A row that passes every filter is eligible."""
    row = sc.judge(cand(), good_meta())
    assert row["included"] is True
    assert row["excluded_because"] == ""


@pytest.mark.parametrize("patch, expected", [
    # judge() reports WHICH error, because the three mean different things: gone,
    # not yet fetched, and a state gh() does not recognise. Collapsing them into
    # one word made 20,457 pending rows read as dead repositories in the review.
    ({"error": "unreachable"}, "unreachable"),
    ({"error": "not-enriched"}, "not-enriched"),
    ({"error": "unclassified"}, "unclassified"),
    ({"archived": True}, "archived"),
    ({"fork": True}, "fork"),
    ({"language": "Python"}, "lang=Python"),
    ({"language": None}, "lang=None"),
    ({"size_kb": 100}, "size=100kb"),
    ({"size_kb": None}, "size=Nonekb"),
    ({"commits": None}, "no-commit-count"),
])
def test_judge_every_exclusion_reason_fires(patch, expected):
    """Each documented filter must be able to exclude a row, and say so."""
    row = sc.judge(cand(), good_meta(**patch))
    assert expected in row["excluded_because"]
    assert row["included"] is False


def test_judge_excludes_a_stale_repository():
    """"Alive in 2026" is a selection criterion, enforced by pushed_at."""
    row = sc.judge(cand(), good_meta(pushed_at=fresh_pushed(days=sc.MAX_STALE_DAYS + 10)))
    assert row["excluded_because"].startswith("stale=")
    assert row["included"] is False


def test_judge_keeps_a_repository_just_inside_the_staleness_window():
    """The boundary is > MAX_STALE_DAYS, so a repo exactly at it stays."""
    row = sc.judge(cand(), good_meta(pushed_at=fresh_pushed(days=sc.MAX_STALE_DAYS)))
    assert "stale=" not in row["excluded_because"]


def test_judge_missing_pushed_at_is_not_a_staleness_reason():
    """An unknown push date cannot prove staleness, so it must not invent one."""
    row = sc.judge(cand(), good_meta(pushed_at=None))
    assert "stale=" not in row["excluded_because"]


def test_judge_collects_several_reasons_at_once():
    """The exclusion field is a full account, not the first thing found."""
    row = sc.judge(cand(), good_meta(archived=True, fork=True, language="Go", commits=None))
    reasons = row["excluded_because"].split("; ")
    assert reasons == ["archived", "fork", "lang=Go", "no-commit-count"]


def test_judge_keeps_the_data_of_an_excluded_row():
    """candidates.csv is the decision record. An excluded row keeps its
    evidence so the exclusion can be audited."""
    row = sc.judge(cand(), good_meta(archived=True, stars=4_242, commits=888))
    assert row["included"] is False
    assert row["stars"] == 4_242
    assert row["commits"] == 888
    assert row["language"] == "C"
    assert row["size_class"] == "S"
    assert row["owner"] == "acme"


# ---- shared history is recorded, never excluded. Author decision 2026-09-14:
# a derivative with its own governance is a project, not a duplicate, so the
# frame carries the relationship and the direction instead of dropping a row.
# shared_history.py owns the measurement; emit only reads the answer, because
# reading a root needs a clone and emit stays offline.


@pytest.fixture
def roots_cache(tmp_path, monkeypatch):
    """Point select_corpus at a scan cache under tmp_path."""
    path = tmp_path / "roots.json"
    monkeypatch.setattr(sc, "ROOTS_CACHE", path)
    sc._scan_verdict.cache_clear()
    yield path
    sc._scan_verdict.cache_clear()


def cluster_cache(**over) -> str:
    entry = dict(cluster="1da177e4", shared_with=["torvalds/linux"],
                 relation="includes", includes=["torvalds/linux"],
                 first="torvalds/linux", created="2013-01-01T00:00:00Z",
                 note="Linux trees")
    entry.update(over)
    return json.dumps({"clusters": {"acme/widget": entry}})


def test_sharing_a_history_never_excludes_a_row(roots_cache):
    """The whole point of the decision. A copy stays in the corpus."""
    roots_cache.write_text(cluster_cache())
    row = sc.judge(cand(), good_meta())
    assert row["included"] is True
    assert row["excluded_because"] == ""


def test_judge_records_the_cluster_and_the_direction(roots_cache):
    roots_cache.write_text(cluster_cache())
    row = sc.judge(cand(), good_meta())
    assert row["history_cluster"] == "1da177e4"
    assert row["history_shared_with"] == "torvalds/linux"
    assert row["history_includes"] == "torvalds/linux"
    assert row["history_relation"] == "includes"
    assert row["history_first"] == "torvalds/linux"
    assert row["history_created"] == "2013-01-01T00:00:00Z"


def test_judge_records_a_row_whose_history_is_inside_another(roots_cache):
    """The upstream of a vendor mirror reads `included_in`, and `includes` is
    empty: nothing in the cluster is part of it."""
    roots_cache.write_text(cluster_cache(relation="included_in", includes=[]))
    row = sc.judge(cand(), good_meta())
    assert row["history_includes"] == ""
    assert row["history_relation"] == "included_in"


def test_judge_records_a_diverged_pair_without_claiming_inclusion(roots_cache):
    """Measured on torvalds/linux against raspberrypi/linux: neither history is
    inside the other, so the inclusion column must stay empty."""
    roots_cache.write_text(cluster_cache(relation="diverged", includes=[]))
    row = sc.judge(cand(), good_meta())
    assert row["history_includes"] == ""
    assert row["history_relation"] == "diverged"
    assert row["included"] is True


def test_judge_joins_several_cluster_members_with_a_space(roots_cache):
    """The MySQL cluster holds four members, so the column is a list."""
    roots_cache.write_text(cluster_cache(shared_with=["a/one", "b/two", "c/three"]))
    assert sc.judge(cand(), good_meta())["history_shared_with"] == "a/one b/two c/three"


def test_judge_leaves_the_history_columns_empty_without_a_scan(roots_cache):
    assert not roots_cache.exists()
    row = sc.judge(cand(), good_meta())
    assert row["history_cluster"] == "" and row["history_shared_with"] == ""
    assert row["history_includes"] == "" and row["history_relation"] == ""
    assert row["history_first"] == "" and row["history_created"] == ""


def test_a_corrupt_scan_cache_annotates_nothing(roots_cache):
    roots_cache.write_text("{not json")
    assert sc.shared_history_clusters() == {}


def test_a_scan_cache_without_the_key_annotates_nothing(roots_cache):
    roots_cache.write_text(json.dumps({"roots": {"u": ["r"]}}))
    assert sc.shared_history_clusters() == {}


def test_a_scan_cache_that_is_not_a_mapping_annotates_nothing(roots_cache):
    """json.loads accepts a bare list. Indexing it would raise later, in emit,
    after the caller had already paid for enrichment."""
    roots_cache.write_text("[1, 2, 3]")
    assert sc.shared_history_clusters() == {}


def test_the_history_columns_reach_candidates_csv(sandbox, roots_cache, monkeypatch):
    """candidates.csv is the record a reviewer reads, so the columns have to
    reach the file and not only the row dict."""
    roots_cache.write_text(cluster_cache())
    write(sc.CACHE / "repo-meta.json", json.dumps({"acme/widget": good_meta()}))
    monkeypatch.setattr(sc, "collect_candidates", lambda: [cand()])

    assert sc.cmd_emit(SimpleNamespace(per_stratum=0, balance_lang=False)) == 0
    row, = read_csv_rows(sc.CANDIDATES)
    assert row["history_cluster"] == "1da177e4"
    assert row["history_includes"] == "torvalds/linux"
    assert row["history_relation"] == "includes"
    assert row["history_first"] == "torvalds/linux"
    assert row["history_created"] == "2013-01-01T00:00:00Z"
    assert row["included"] == "True"



# ================================================================ small pure functions


@pytest.mark.parametrize("text, expected", [
    ("https://github.com/postgres/postgres", ("postgres", "postgres")),
    ("https://github.com/postgres/postgres/", ("postgres", "postgres")),
    ("https://github.com/acme/widget.git", ("acme", "widget")),
    ("git@github.com:acme/widget.git", ("acme", "widget")),
    ("  https://github.com/eclipse/vert.x  ", ("eclipse", "vert.x")),
    ("http://github.com/a-b_c/d.e-f", ("a-b_c", "d.e-f")),
])
def test_slug_extracts_owner_and_repo(text, expected):
    """Every source states its repository as a URL; this is the only parser."""
    assert sc.slug(text) == expected


@pytest.mark.parametrize("text", [
    None, "", "https://gitlab.com/foo/bar", "https://example.com/x",
    "https://github.com/onlyowner", "not a url at all",
])
def test_slug_returns_none_for_a_non_github_slug(text):
    """A row we cannot resolve to owner/repo must be skipped, not guessed."""
    assert sc.slug(text) is None


def test_lang_tokens_splits_on_every_separator():
    """Roster language fields use commas, semicolons, slashes, pipes and "and"."""
    assert sc.lang_tokens("C, C++; Java / Rust | Go and Scala") == {
        "C", "C++", "Java", "Rust", "Go", "Scala"}


@pytest.mark.parametrize("text", ["Scala", "Clojure", "CoffeeScript", "C#"])
def test_lang_tokens_does_not_match_a_language_by_substring(text):
    """Exact tokens only: "Scala" must not be read as "C".

    Research correctness: substring matching would pull hundreds of untokenised
    projects into a pool cregit cannot process.
    """
    assert not (sc.lang_tokens(text) & set(sc.LANG_FILTER))


@pytest.mark.parametrize("text", ["C", "Java", "C, Python", "Rust and C++"])
def test_lang_tokens_matches_a_supported_language(text):
    assert sc.lang_tokens(text) & set(sc.LANG_FILTER)


def test_lang_tokens_of_nothing_is_empty():
    assert sc.lang_tokens(None) == set()
    assert sc.lang_tokens("  ") == set()


@pytest.mark.parametrize("commits, expected", [
    (None, "?"),
    (0, "S"),
    (sc.SIZE_S - 1, "S"),
    (sc.SIZE_S, "M"),
    (sc.SIZE_M, "M"),
    (sc.SIZE_M + 1, "L"),
])
def test_size_class_boundaries(commits, expected):
    """The manifest documents S < 30k | M 30k-150k | L > 150k."""
    assert sc.size_class(commits) == expected


def test_stale_days_counts_days_since_the_last_push():
    assert sc.stale_days(fresh_pushed(days=10)) in (9, 10)


def test_stale_days_handles_a_z_suffix_and_an_offset():
    """GitHub sends `...Z`; a cached value may already carry an offset."""
    assert sc.stale_days("2020-01-01T00:00:00Z") > 1_000
    assert sc.stale_days("2020-01-01T00:00:00+00:00") > 1_000


@pytest.mark.parametrize("pushed", [None, ""])
def test_stale_days_of_nothing_is_none(pushed):
    """An unknown date must stay unknown, not become "very stale"."""
    assert sc.stale_days(pushed) is None


def test_today_is_an_iso_utc_date():
    """label_date must be stable and timezone-free."""
    assert sc.today() == datetime.now(timezone.utc).date().isoformat()
    assert len(sc.today()) == 10


def test_say_prints_a_timestamped_line(capsys):
    """Progress output is the run log for a job that takes hours."""
    sc.say("hello")
    out = capsys.readouterr().out
    assert "hello" in out and out.startswith("[")


# ================================================================ parse_spinellis_cohort()


def test_cohort_star_threshold_is_strictly_greater_than(sandbox):
    """400 stars is rejected, 401 accepted.

    Research correctness: the threshold must match the starred GitHub search
    exactly, or a project becomes eligible according to which source found it.
    """
    write(sc.COHORT_TSV, "\n".join([
        cohort_line("https://github.com/a/exactly400", 400, 500),
        cohort_line("https://github.com/a/just401", 401, 500),
    ]))
    assert [r["repo"] for r in sc.parse_spinellis_cohort()] == ["just401"]


def test_cohort_rejects_too_few_commits(sandbox):
    """Below SP_MIN_COMMITS a repo has too little history to measure."""
    write(sc.COHORT_TSV, "\n".join([
        cohort_line("https://github.com/a/thin", 900, sc.SP_MIN_COMMITS - 1),
        cohort_line("https://github.com/a/thick", 900, sc.SP_MIN_COMMITS),
    ]))
    assert [r["repo"] for r in sc.parse_spinellis_cohort()] == ["thick"]


def test_cohort_counts_a_wrong_field_count_as_malformed(sandbox, capsys):
    """An 18 MB third-party file must not crash the run on one bad line."""
    write(sc.COHORT_TSV, "\n".join([
        "https://github.com/a/short\t1",                      # 2 fields
        "https://github.com/a/long\t1\t900\t500\textra",       # 5 fields
        cohort_line("https://github.com/a/ok", 900, 500),
    ]))
    assert [r["repo"] for r in sc.parse_spinellis_cohort()] == ["ok"]
    assert "2 malformed" in capsys.readouterr().out


@pytest.mark.parametrize("stars, commits", [("many", "500"), ("900", "lots"), ("", "")])
def test_cohort_counts_a_non_integer_as_malformed(sandbox, capsys, stars, commits):
    """A non-numeric count is reported, never raised."""
    write(sc.COHORT_TSV, cohort_line("https://github.com/a/bad", stars, commits))
    assert sc.parse_spinellis_cohort() == []
    assert "1 malformed" in capsys.readouterr().out


def test_cohort_skips_a_url_that_is_not_a_github_slug(sandbox):
    """Their 2020 snapshot carries non-GitHub and truncated URLs."""
    write(sc.COHORT_TSV, "\n".join([
        cohort_line("https://gitlab.com/a/elsewhere", 900, 500),
        cohort_line("https://github.com/a/here", 900, 500),
    ]))
    assert [r["repo"] for r in sc.parse_spinellis_cohort()] == ["here"]


def test_cohort_rows_carry_the_pool_fact_and_stay_relabellable(sandbox):
    """weak=True is what lets judge() reassign these rows by F2.

    Research correctness: cohort membership is the ABSENCE of an enterprise
    signal, not a positive community control fact. Marking it as a positive
    fact would freeze a company-owned repo into the community stratum.
    """
    write(sc.COHORT_TSV, cohort_line("https://github.com/Acme/Widget", 900, 500))
    row, = sc.parse_spinellis_cohort()
    assert row["weak"] is True
    assert row["stratum"] == "community"
    assert row["fact"] == "F1_pool:spinellis-cohort"
    assert row["source"] == "cohort"
    assert (row["owner"], row["repo"]) == ("Acme", "Widget")
    assert row["cohort_stars"] == 900
    assert row["cohort_commits"] == 500


def test_cohort_order_is_stars_desc_then_owner_then_repo(sandbox):
    """A --per-stratum cut must be reproducible run to run."""
    write(sc.COHORT_TSV, "\n".join([
        cohort_line("https://github.com/Beta/x", 900, 500),
        cohort_line("https://github.com/alpha/z", 900, 500),
        cohort_line("https://github.com/alpha/b", 900, 500),
        cohort_line("https://github.com/zed/top", 5_000, 500),
    ]))
    got = [(r["owner"], r["repo"]) for r in sc.parse_spinellis_cohort()]
    assert got == [("zed", "top"), ("alpha", "b"), ("alpha", "z"), ("Beta", "x")]


def test_cohort_missing_file_returns_empty(sandbox, capsys):
    """The 18 MB source is not in git, so its absence must be survivable."""
    assert not sc.COHORT_TSV.exists()
    assert sc.parse_spinellis_cohort() == []
    assert "absent, skipped" in capsys.readouterr().out


# ================================================================ parse_spinellis()


def test_spinellis_considers_only_fortune500_or_sec_filers(sandbox):
    """The strong tier rests on published filings, not on our judgement."""
    write(sc.SPINELLIS_TSV, "\n".join([
        sp_ok("https://github.com/a/fortune", company="A", tier="fg500"),
        sp_ok("https://github.com/b/tenk", company="B", tier="sec10k"),
        sp_ok("https://github.com/c/twentyf", company="C", tier="sec20f"),
        sp_line(url="https://github.com/d/noflag", company_name="D", lines=50_000,
                commit_count=1_000, most_recent_commit="2020-06-01",
                fg500="f", sec10k="f", sec20f="f"),
    ]))
    assert [r["repo"] for r in sc.parse_spinellis()] == ["fortune", "tenk", "twentyf"]


def test_spinellis_requires_a_company_name(sandbox):
    """A flagged row with no registered organisation carries no usable fact."""
    write(sc.SPINELLIS_TSV, "\n".join([
        sp_ok("https://github.com/a/named", company="Acme"),
        sp_ok("https://github.com/a/anon", company=""),
    ]))
    assert [r["repo"] for r in sc.parse_spinellis()] == ["named"]


def test_spinellis_fact_names_the_provenance_tier(sandbox):
    """The fact must say which filing supports the label."""
    write(sc.SPINELLIS_TSV, "\n".join([
        sp_ok("https://github.com/a/one", company="A", tier="fg500"),
        sp_ok("https://github.com/b/two", company="B", tier="sec10k"),
        sp_ok("https://github.com/c/three", company="C", tier="sec20f"),
    ]))
    rows = sc.parse_spinellis()
    assert [r["fact"] for r in rows] == ["F1_pool:spinellis-fg500",
                                        "F1_pool:spinellis-sec10k",
                                        "F1_pool:spinellis-sec20f"]
    assert {r["stratum"] for r in rows} == {"company-owned"}
    assert {r["source"] for r in rows} == {"spinellis"}


def test_spinellis_orders_by_tier_then_by_size(sandbox):
    """Fortune 500 first, then 10-K, then 20-F; largest first inside a tier."""
    write(sc.SPINELLIS_TSV, "\n".join([
        sp_ok("https://github.com/c/f20", company="C", tier="sec20f", lines=900_000),
        sp_ok("https://github.com/b/k10", company="B", tier="sec10k", lines=900_000),
        sp_ok("https://github.com/a/small500", company="A", tier="fg500", lines=30_000),
        sp_ok("https://github.com/a2/big500", company="A2", tier="fg500", lines=800_000),
    ]))
    assert [r["repo"] for r in sc.parse_spinellis()] == ["big500", "small500", "k10", "f20"]


def test_spinellis_per_company_cap_holds(sandbox):
    """Uncapped, the stratum would measure Microsoft and Alphabet.

    Research correctness: their strong tier is 60% Microsoft plus Alphabet, so
    firm concentration in the company-owned stratum would be high by
    construction.
    """
    write(sc.SPINELLIS_TSV, "\n".join(
        sp_ok(f"https://github.com/acme/r{i}", company="Acme") for i in range(20)))
    rows = sc.parse_spinellis()
    assert len(rows) == sc.SP_MAX_PER_COMPANY


def test_spinellis_cap_keeps_the_best_attested_rows(sandbox):
    """When the cap bites, provenance decides who survives."""
    lines = [sp_ok("https://github.com/acme/fortune", company="Acme",
                   tier="fg500", lines=20_000)]
    lines += [sp_ok(f"https://github.com/acme/f20-{i}", company="Acme",
                    tier="sec20f", lines=900_000 - i)
              for i in range(sc.SP_MAX_PER_COMPANY)]
    write(sc.SPINELLIS_TSV, "\n".join(lines))

    rows = sc.parse_spinellis()
    assert len(rows) == sc.SP_MAX_PER_COMPANY
    assert rows[0]["repo"] == "fortune"                     # the fg500 row survived
    weakest = f"f20-{sc.SP_MAX_PER_COMPANY - 1}"            # smallest 20-F dropped
    assert weakest not in [r["repo"] for r in rows]


def test_spinellis_cap_is_per_company_not_global(sandbox):
    """Two firms each keep their own allowance."""
    lines = [sp_ok(f"https://github.com/a/r{i}", company="A")
             for i in range(sc.SP_MAX_PER_COMPANY + 3)]
    lines += [sp_ok(f"https://github.com/b/r{i}", company="B")
              for i in range(sc.SP_MAX_PER_COMPANY + 3)]
    write(sc.SPINELLIS_TSV, "\n".join(lines))
    assert len(sc.parse_spinellis()) == 2 * sc.SP_MAX_PER_COMPANY


@pytest.mark.parametrize("over, kept", [
    ({"year": "2017"}, False),
    ({"year": str(int(sc.SP_MIN_YEAR))}, True),
    ({"lines": sc.SP_MIN_LINES - 1}, False),
    ({"lines": sc.SP_MIN_LINES}, True),
    ({"commits": sc.SP_MIN_COMMITS - 1}, False),
    ({"commits": sc.SP_MIN_COMMITS}, True),
])
def test_spinellis_each_threshold_rejects_a_row(sandbox, over, kept):
    """Year, size and history each gate the pool before any API call."""
    write(sc.SPINELLIS_TSV, sp_ok("https://github.com/a/probe", **over))
    assert bool(sc.parse_spinellis()) is kept


def test_spinellis_skips_a_row_with_an_unparseable_url(sandbox):
    """A row we cannot resolve is dropped, not guessed at."""
    write(sc.SPINELLIS_TSV, "\n".join([
        sp_ok("https://bitbucket.org/a/elsewhere"),
        sp_ok("https://github.com/a/here"),
    ]))
    assert [r["repo"] for r in sc.parse_spinellis()] == ["here"]


def test_spinellis_pads_a_short_line(sandbox):
    """A truncated line must be padded, not raise IndexError."""
    write(sc.SPINELLIS_TSV, "https://github.com/a/truncated\t1\t2")
    assert sc.parse_spinellis() == []


def test_spinellis_non_numeric_fields_count_as_zero(sandbox):
    """num() is defensive, so a dirty field excludes the row quietly."""
    write(sc.SPINELLIS_TSV, sp_line(
        url="https://github.com/a/dirty", company_name="A", fg500="t",
        lines="lots", commit_count="many", most_recent_commit="2020-06-01"))
    assert sc.parse_spinellis() == []


def test_spinellis_falls_back_to_the_repo_name_when_the_project_name_is_blank(sandbox):
    write(sc.SPINELLIS_TSV, sp_line(
        url="https://github.com/a/widget", company_name="A", fg500="t",
        lines=50_000, commit_count=1_000, most_recent_commit="2020-06-01",
        project_name=""))
    assert sc.parse_spinellis()[0]["roster_name"] == "widget"


def test_spinellis_missing_file_returns_empty(sandbox, capsys):
    """The 3.6 MB source is fetched by DOI, not committed."""
    assert sc.parse_spinellis() == []
    assert "absent, skipped" in capsys.readouterr().out


# ================================================================ roster parsers


def test_parse_asf_derives_the_github_slug_from_a_gitbox_url(sandbox):
    """ASF publishes gitbox URLs; every project mirrors to github.com/apache."""
    write(sc.CACHE / "roster-asf.raw", json.dumps({
        "kafka": {"name": "Apache Kafka", "programming-language": "Java",
                  "repository": [{"url": "https://gitbox.apache.org/repos/asf/kafka.git"}]},
    }))
    row, = sc.parse_asf()
    assert (row["owner"], row["repo"]) == ("apache", "kafka")
    assert row["stratum"] == "foundation"
    assert row["fact"] == "F1_roster:asf"
    assert row["roster_name"] == "Apache Kafka"


def test_parse_asf_prefilters_on_the_roster_language(sandbox):
    """The roster's own language field saves ~140 wasted gh calls."""
    write(sc.CACHE / "roster-asf.raw", json.dumps({
        "keep": {"programming-language": ["Java", "Scala"],
                 "repository": ["https://gitbox.apache.org/repos/asf/keep.git"]},
        "drop": {"programming-language": "Python",
                 "repository": ["https://gitbox.apache.org/repos/asf/drop.git"]},
        "none": {"repository": ["https://gitbox.apache.org/repos/asf/none.git"]},
    }))
    assert [r["repo"] for r in sc.parse_asf()] == ["keep"]


def test_parse_asf_uses_a_github_url_directly(sandbox):
    write(sc.CACHE / "roster-asf.raw", json.dumps({
        "x": {"programming-language": "C", "repository": "https://github.com/apache/arrow"},
    }))
    assert sc.parse_asf()[0]["repo"] == "arrow"


def test_parse_asf_skips_a_placeholder_basename(sandbox):
    """A bare `/repos/asf/` URL names no repository, so try the next one."""
    write(sc.CACHE / "roster-asf.raw", json.dumps({
        "x": {"programming-language": "C++", "repository": [
            "https://gitbox.apache.org/repos/asf/",
            "https://gitbox.apache.org/repos/asf/real.git"]},
    }))
    assert [r["repo"] for r in sc.parse_asf()] == ["real"]


def test_parse_asf_drops_a_project_whose_every_url_is_a_placeholder(sandbox):
    """A project we cannot resolve at all is skipped, not emitted as `apache/asf`."""
    write(sc.CACHE / "roster-asf.raw", json.dumps({
        "x": {"programming-language": "Java", "repository": [
            "https://gitbox.apache.org/repos/asf/",
            "https://gitbox.apache.org/repos/"]},
    }))
    assert sc.parse_asf() == []


def test_parse_asf_takes_only_the_first_repository_of_a_project(sandbox):
    """One row per project keeps the pool a project list, not a repo list."""
    write(sc.CACHE / "roster-asf.raw", json.dumps({
        "x": {"programming-language": "Java", "repository": [
            "https://gitbox.apache.org/repos/asf/first.git",
            "https://gitbox.apache.org/repos/asf/second.git"]},
    }))
    assert [r["repo"] for r in sc.parse_asf()] == ["first"]


def test_parse_eclipse_skips_archived_projects(sandbox):
    """An archived project is not alive in 2026."""
    write(sc.CACHE / "roster-eclipse.raw", json.dumps([
        {"name": "Dead", "state": "Archived",
         "github_repos": [{"url": "https://github.com/eclipse/dead"}]},
        {"name": "Alive", "state": "Regular",
         "github_repos": [{"url": "https://github.com/eclipse/alive"}]},
    ]))
    assert [r["repo"] for r in sc.parse_eclipse()] == ["alive"]


def test_parse_eclipse_prefers_a_code_repo_over_a_website_mirror(sandbox):
    """A *.io or docs repo is a website, not the project's source."""
    write(sc.CACHE / "roster-eclipse.raw", json.dumps([
        {"name": "Vert.x", "github_repos": [
            {"url": "https://github.com/eclipse/vertx.io"},
            {"url": "https://github.com/eclipse/vert.x"}]},
    ]))
    row, = sc.parse_eclipse()
    assert (row["owner"], row["repo"]) == ("eclipse", "vert.x")
    assert row["fact"] == "F1_roster:eclipse"


def test_parse_eclipse_falls_back_to_a_website_repo_when_it_is_the_only_one(sandbox):
    """Something beats nothing; emit filters decide eligibility later."""
    write(sc.CACHE / "roster-eclipse.raw", json.dumps([
        {"name": "OnlyDocs", "github_repos": ["https://github.com/eclipse/docs-thing"]},
    ]))
    assert sc.parse_eclipse()[0]["repo"] == "docs-thing"


def test_parse_eclipse_drops_a_project_with_no_resolvable_repo(sandbox):
    write(sc.CACHE / "roster-eclipse.raw", json.dumps([
        {"name": "None", "github_repos": []},
        {"name": "Bad", "github_repos": ["https://example.com/x"]},
    ]))
    assert sc.parse_eclipse() == []


def test_parse_eclipse_accepts_a_dict_keyed_payload(sandbox):
    """The API has returned both an array and an object; handle both."""
    write(sc.CACHE / "roster-eclipse.raw", json.dumps(
        {"p1": {"name": "P", "github_repos": ["https://github.com/eclipse/p"]}}))
    assert sc.parse_eclipse()[0]["repo"] == "p"


def test_parse_cncf_reads_repo_url_and_the_nearest_name(sandbox):
    """landscape.yml is parsed without a yaml dependency (stdlib only)."""
    write(sc.CACHE / "roster-cncf.raw", "\n".join([
        "landscape:",
        "  - category:",
        "    items:",
        "      - item:",
        "        name: Kubernetes",
        "        project: graduated",
        "        repo_url: https://github.com/kubernetes/kubernetes",
        "      - name: etcd",
        "        repo_url: 'https://github.com/etcd-io/etcd'",
        "        project: graduated",
        "      - item:",
        "        name: NoRepo",
    ]))
    rows = sc.parse_cncf()
    assert [(r["repo"], r["roster_name"]) for r in rows] == [
        ("kubernetes", "Kubernetes"), ("etcd", "etcd")]
    assert {r["stratum"] for r in rows} == {"foundation"}
    assert {r["fact"] for r in rows} == {"F1_roster:cncf-graduated"}


def test_parse_cncf_pairs_project_and_repo_url_in_either_order(sandbox):
    """`project:` may precede or follow `repo_url:` inside one item.

    A line-at-a-time pass cannot pair them, which is why the parser buffers.
    """
    write(sc.CACHE / "roster-cncf.raw", "\n".join([
        "      - item:",
        "        project: incubating",
        "        name: Before",
        "        repo_url: https://github.com/o/before",
        "      - item:",
        "        name: After",
        "        repo_url: https://github.com/o/after",
        "        project: sandbox",
    ]))
    facts = {r["repo"]: r["fact"] for r in sc.parse_cncf()}
    assert facts == {"before": "F1_roster:cncf-incubating",
                     "after": "F1_roster:cncf-sandbox"}


def test_parse_cncf_does_not_call_a_catalogued_project_a_foundation(sandbox):
    """**The CNCF landscape is not the CNCF.**

    The file catalogues the whole cloud-native ecosystem. Only an entry with a
    `project:` key donated its trademark, which is what makes F1 a control fact.
    Before this fix, 734 catalogued entries were labelled `foundation`, which put
    postgres/postgres and redis/redis in the foundation stratum. PostgreSQL is
    the canonical mailing-list community project, and Redis Ltd relicensed Redis
    in 2024. Neither error was visible in the counts.
    """
    write(sc.CACHE / "roster-cncf.raw", "\n".join([
        "      - item:",
        "        name: PostgreSQL",
        "        repo_url: https://github.com/postgres/postgres",
        "      - item:",
        "        name: Kubernetes",
        "        project: graduated",
        "        repo_url: https://github.com/kubernetes/kubernetes",
    ]))
    rows = {r["repo"]: r for r in sc.parse_cncf()}
    assert rows["postgres"]["stratum"] != "foundation"
    assert rows["postgres"]["fact"] == "F1_pool:cncf-landscape"
    assert rows["postgres"]["weak"] is True, "F2 must be free to relabel a pool row"
    assert rows["kubernetes"]["stratum"] == "foundation"


def test_parse_cncf_treats_an_archived_project_as_hosted(sandbox):
    """An archived CNCF project still donated its trademark, so F1 holds.

    The staleness filter removes it later if it is dead. That is a separate
    decision from who controls it.
    """
    write(sc.CACHE / "roster-cncf.raw", "\n".join([
        "      - item:",
        "        name: Old",
        "        project: archived",
        "        repo_url: https://github.com/o/old",
    ]))
    assert sc.parse_cncf()[0]["fact"] == "F1_roster:cncf-archived"


def test_parse_cncf_ignores_an_unknown_hosting_level(sandbox):
    """An unrecognised `project:` value is not evidence of hosting."""
    write(sc.CACHE / "roster-cncf.raw", "\n".join([
        "      - item:",
        "        name: Odd",
        "        project: rumoured",
        "        repo_url: https://github.com/o/odd",
    ]))
    assert sc.parse_cncf()[0]["fact"] == "F1_pool:cncf-landscape"


def test_parse_cncf_skips_an_unparseable_repo_url(sandbox):
    write(sc.CACHE / "roster-cncf.raw", "      - name: X\n        repo_url: https://example.com/x")
    assert sc.parse_cncf() == []


def test_parse_html_roster_scrapes_slugs_and_dedupes(sandbox):
    """SFC and SPI link to homepages, so the subpages are scanned for slugs."""
    write(sc.CACHE / "roster-sfc-pages.raw", " ".join([
        'href="https://github.com/inkscape/inkscape"',
        'href="https://github.com/inkscape/inkscape.git"',
        'href="https://github.com/sfconservancy/website"',
        'href="https://github.com/godot/issues"',
        'href="https://github.com/qemu/qemu"',
    ]))
    rows = sc.parse_html_roster("sfc", "community")
    assert [(r["owner"], r["repo"]) for r in rows] == [("inkscape", "inkscape"),
                                                      ("qemu", "qemu")]
    assert {r["stratum"] for r in rows} == {"community"}
    assert {r["fact"] for r in rows} == {"F1_roster:sfc"}


def test_parse_html_roster_falls_back_to_the_index_page(sandbox):
    """If the subpage crawl never ran, the index alone still yields slugs."""
    write(sc.CACHE / "roster-spi.raw", 'href="https://github.com/spi/thing"')
    assert sc.parse_html_roster("spi", "community")[0]["repo"] == "thing"


def test_parse_html_roster_with_no_cache_returns_empty(sandbox):
    assert sc.parse_html_roster("sfc", "community") == []


# ================================================================ volume sources


def test_parse_company_orgs_keeps_only_tokenisable_source_repos(sandbox, monkeypatch):
    """A company org's repo list is F2 stated at its strongest."""
    monkeypatch.setattr(sc, "COMPANY_ORGS", {"acme"})
    monkeypatch.setattr(sc, "gh", lambda *a, **k: [
        {"name": "engine", "language": "C++", "fork": False, "archived": False},
        {"name": "site", "language": "Python", "fork": False, "archived": False},
        {"name": "copy", "language": "C", "fork": True, "archived": False},
        {"name": "old", "language": "C", "fork": False, "archived": True},
        {"name": "nolang", "language": None, "fork": False, "archived": False},
    ])
    rows = sc.parse_company_orgs()
    assert [r["repo"] for r in rows] == ["engine"]
    assert rows[0]["stratum"] == "company-owned"
    assert rows[0]["fact"] == "F2_org:acme"
    assert rows[0]["owner"] == "acme"


def test_parse_company_orgs_skips_an_org_that_does_not_answer(sandbox, monkeypatch):
    """One dead namespace must not stop the other sixty."""
    monkeypatch.setattr(sc, "COMPANY_ORGS", {"acme", "ghost"})
    monkeypatch.setattr(sc, "gh", lambda *a, **k: (
        None if "ghost" in a[0] else
        [{"name": "engine", "language": "C", "fork": False, "archived": False}]))
    assert [r["repo"] for r in sc.parse_company_orgs()] == ["engine"]


def test_parse_ghsearch_emits_an_unlabelled_residual_pool(sandbox, monkeypatch):
    """ghsearch carries no control fact; judge() assigns the stratum.

    Research correctness: labelling the volume source would let a search
    ranking decide a stratum.
    """
    monkeypatch.setattr(sc.subprocess, "run", lambda *a, **k: cp(
        0, json.dumps([{"fullName": "acme/widget", "stargazersCount": 900}])))
    rows = sc.parse_ghsearch()
    assert len(rows) == 4                       # one per language searched
    assert {r["fact"] for r in rows} == {"F1_residual:none"}
    assert {r["stratum"] for r in rows} == {"community"}
    assert (rows[0]["owner"], rows[0]["repo"]) == ("acme", "widget")
    assert {r["roster_lang"] for r in rows} == {"c", "cpp", "java", "rust"}


def test_parse_ghsearch_survives_a_failed_search(sandbox, monkeypatch, capsys):
    """A failed language search is logged and skipped, not fatal."""
    monkeypatch.setattr(sc.subprocess, "run",
                        lambda *a, **k: cp(1, err="gh: search unavailable"))
    assert sc.parse_ghsearch() == []
    assert "gh search" in capsys.readouterr().out


# ================================================================ collect_candidates()


@pytest.fixture
def no_api_sources(sandbox):
    """Pre-seed the paid caches so collect_candidates() makes no gh call."""
    write(sandbox.cache / "company-orgs.json", "[]")
    write(sandbox.cache / "ghsearch.json", "[]")
    return sandbox


def by_slug(rows: list[dict]) -> dict[tuple[str, str], dict]:
    return {(r["owner"].lower(), r["repo"].lower()): r for r in rows}


def test_collect_seeds_the_community_anchors(no_api_sources, capsys):
    """COMMUNITY_SEED anchors the stratum that no roster carries."""
    rows = by_slug(sc.collect_candidates())
    assert rows[("postgres", "postgres")]["fact"] == "F1_roster:seed"
    assert rows[("qemu", "qemu")]["stratum"] == "community"
    assert len(rows) == len(sc.COMMUNITY_SEED)
    assert "no cache, skipped" in capsys.readouterr().out


def test_collect_dedup_is_case_insensitive_and_first_source_wins(no_api_sources):
    """A roster label must beat a later namespace match.

    Research correctness: dedup order is the fact precedence order. If a later
    source could overwrite an earlier one, a positive roster fact would lose to
    a bare namespace guess.
    """
    write(no_api_sources.cache / "company-orgs.json", json.dumps([
        dict(source="company-org", stratum="company-owned", fact="F2_org:Postgres",
             owner="Postgres", repo="Postgres", roster_lang="C", roster_name="Postgres"),
    ]))
    rows = by_slug(sc.collect_candidates())
    kept = rows[("postgres", "postgres")]
    assert kept["source"] == "seed"                 # the earlier source won
    assert kept["stratum"] == "community"
    assert sum(1 for k in rows if k == ("postgres", "postgres")) == 1


def test_collect_maps_a_legacy_stratum_name_forward(no_api_sources):
    """A cached `single-vendor` label becomes `company-owned`.

    Research correctness: the JSON caches were written before the rename and
    are replayed verbatim. Without this, a stale cache reintroduces a stratum
    name that no longer exists and the row is silently lost from analysis.
    """
    write(no_api_sources.cache / "company-orgs.json", json.dumps([
        dict(source="company-org", stratum="single-vendor", fact="F2_org:acme",
             owner="acme", repo="widget", roster_lang="C", roster_name="widget"),
    ]))
    assert sc.LEGACY_STRATUM["single-vendor"] == "company-owned"
    assert by_slug(sc.collect_candidates())[("acme", "widget")]["stratum"] == "company-owned"


def test_collect_runs_a_roster_parser_when_its_cache_exists(no_api_sources):
    """A cached roster is parsed; the stratum comes from the roster.

    The entry needs `project:` to be a CNCF-hosted project. Without it the
    landscape only catalogues it, and the label stays open for F2.
    """
    write(sc.CACHE / "roster-cncf.raw",
          "      - name: etcd\n        project: graduated\n"
          "        repo_url: https://github.com/etcd-io/etcd")
    row = by_slug(sc.collect_candidates())[("etcd-io", "etcd")]
    assert row["stratum"] == "foundation"
    assert row["fact"] == "F1_roster:cncf-graduated"


def test_collect_survives_a_broken_roster_cache(no_api_sources, capsys):
    """A corrupt 29 MB download must not lose the other sources."""
    write(sc.CACHE / "roster-asf.raw", "{ this is not json")
    rows = sc.collect_candidates()
    assert "parse FAILED" in capsys.readouterr().out
    assert len(rows) == len(sc.COMMUNITY_SEED)


def test_collect_writes_a_cache_for_a_paid_source(sandbox, monkeypatch):
    """company-orgs.json and ghsearch.json cost gh calls, so they are cached."""
    monkeypatch.setattr(sc, "parse_company_orgs", lambda: [
        dict(source="company-org", stratum="company-owned", fact="F2_org:acme",
             owner="acme", repo="widget", roster_lang="C", roster_name="widget")])
    monkeypatch.setattr(sc, "parse_ghsearch", lambda: [])
    sc.collect_candidates()
    cached = json.loads((sandbox.cache / "company-orgs.json").read_text())
    assert cached[0]["repo"] == "widget"
    assert json.loads((sandbox.cache / "ghsearch.json").read_text()) == []


def test_collect_reads_the_local_cohort_without_caching_it(no_api_sources):
    """The cohort is a local file: no API cost, so nothing to cache."""
    write(sc.COHORT_TSV, cohort_line("https://github.com/acme/cohorted", 900, 500))
    rows = by_slug(sc.collect_candidates())
    assert rows[("acme", "cohorted")]["weak"] is True
    assert not (no_api_sources.cache / "cohort.json").exists()


def test_collect_places_spinellis_before_the_volume_sources(no_api_sources):
    """SEC/Fortune provenance must win dedup over a bare namespace match."""
    write(sc.SPINELLIS_TSV, sp_ok("https://github.com/acme/widget"))
    write(no_api_sources.cache / "ghsearch.json", json.dumps([
        dict(source="ghsearch", stratum="community", fact="F1_residual:none",
             owner="acme", repo="widget", roster_lang="c", roster_name="widget"),
    ]))
    row = by_slug(sc.collect_candidates())[("acme", "widget")]
    assert row["source"] == "spinellis"
    assert row["fact"] == "F1_pool:spinellis-fg500"


# ================================================================ cmd_emit()


def emit_args(per_stratum: int = 0, balance_lang: bool = False) -> SimpleNamespace:
    return SimpleNamespace(per_stratum=per_stratum, balance_lang=balance_lang)


def read_csv_rows(path: Path) -> list[dict]:
    import csv
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def manifest_lines(path: Path) -> tuple[list[str], list[str]]:
    lines = path.read_text().splitlines()
    return ([l for l in lines if l.startswith("#")],
            [l for l in lines if l and not l.startswith("#")])


def test_emit_without_a_cache_returns_1(sandbox, capsys):
    """emit refuses to guess; it tells the user to run enrich."""
    assert sc.cmd_emit(emit_args()) == 1
    assert "run `enrich` first" in capsys.readouterr().out


def test_emit_metadata_lookup_is_case_insensitive(sandbox, monkeypatch):
    """A candidate spelled differently from the cache still finds its record.

    Research correctness: the Spinellis cohort carries 2020 GHTorrent spelling.
    A case-sensitive lookup reports an already enriched project as
    "not-enriched" and drops it from the corpus.
    """
    write(sc.CACHE / "repo-meta.json", json.dumps({"Acme/Widget": good_meta()}))
    monkeypatch.setattr(sc, "collect_candidates", lambda: [cand(owner="acme", repo="widget")])

    assert sc.cmd_emit(emit_args()) == 0
    row, = read_csv_rows(sc.CANDIDATES)
    assert row["excluded_because"] == ""
    assert row["included"] == "True"
    assert row["language"] == "C"


def test_emit_reports_a_candidate_that_was_never_enriched(sandbox, monkeypatch):
    """A missing record is an exclusion reason, not a crash.

    It must say `not-enriched`, not `unreachable`. A pending row and a dead
    repository are different facts, and the corpus review reports them apart.
    """
    write(sc.CACHE / "repo-meta.json", json.dumps({}))
    monkeypatch.setattr(sc, "collect_candidates", lambda: [cand()])
    sc.cmd_emit(emit_args())
    reason = read_csv_rows(sc.CANDIDATES)[0]["excluded_because"]
    assert "not-enriched" in reason
    assert "unreachable" not in reason


def test_emit_candidates_csv_holds_every_candidate(sandbox, monkeypatch):
    """candidates.csv is the decision record: excluded rows stay, with a reason."""
    write(sc.CACHE / "repo-meta.json", json.dumps({
        "acme/keep": good_meta(full_name="acme/keep", clone_url="https://x/keep.git"),
        "acme/drop": good_meta(full_name="acme/drop", archived=True),
    }))
    monkeypatch.setattr(sc, "collect_candidates",
                        lambda: [cand(repo="keep"), cand(repo="drop")])

    sc.cmd_emit(emit_args())
    rows = {r["repo"]: r for r in read_csv_rows(sc.CANDIDATES)}
    assert set(rows) == {"keep", "drop"}
    assert rows["keep"]["included"] == "True"
    assert rows["drop"]["included"] == "False"
    assert rows["drop"]["excluded_because"] == "archived"


def test_emit_manifest_has_the_documented_header_and_five_fields(sandbox, monkeypatch):
    """The manifest is consumed by the rest of the pipeline; its shape is an API."""
    write(sc.CACHE / "repo-meta.json", json.dumps({
        "acme/keep": good_meta(full_name="acme/keep", commits=200_000,
                               clone_url="https://github.com/acme/keep.git"),
        "acme/drop": good_meta(full_name="acme/drop", fork=True),
    }))
    monkeypatch.setattr(sc, "collect_candidates",
                        lambda: [cand(repo="keep"), cand(repo="drop")])

    sc.cmd_emit(emit_args())
    comments, data = manifest_lines(sc.MANIFEST_OUT)
    assert len(comments) == 3
    assert comments[0] == "# Corpus manifest (TSV): name\turl\tcategory\tfile_filter\tsize_class"
    assert "size_class: S < 30k commits" in comments[2]
    assert len(data) == 1                                  # only the eligible row
    fields = data[0].split("\t")
    assert len(fields) == 5
    assert fields[0] == "acme__keep", "the manifest key must carry the owner"
    assert fields[1] == "https://github.com/acme/keep.git"
    assert fields[2] == "community"
    assert fields[3] == sc.LANG_FILTER["C"]
    assert fields[4] == "L"


def test_emit_manifest_name_is_lowercased_with_dots_replaced(sandbox, monkeypatch):
    """The name becomes a directory, so `vert.x` must not create a suffix. The
    owner is kept, because the name must also be unique across owners."""
    write(sc.CACHE / "repo-meta.json", json.dumps({
        "eclipse/vert.x": good_meta(full_name="eclipse/Vert.X")}))
    monkeypatch.setattr(sc, "collect_candidates",
                        lambda: [cand(owner="eclipse", repo="Vert.X",
                                      fact="F1_roster:eclipse", stratum="foundation")])
    sc.cmd_emit(emit_args())
    assert manifest_lines(sc.MANIFEST_OUT)[1][0].split("\t")[0] == "eclipse__vert-x"


def test_emit_manifest_names_are_unique_across_every_row(sandbox, monkeypatch):
    """The property that matters, asserted on the artefact rather than on the
    helper: two repositories sharing a name would share a workdir, a lock and a
    completion stamp in ctp.py."""
    owners = ["apolloconfig", "ClassicOldSong", "ApolloAuto"]
    write(sc.CACHE / "repo-meta.json", json.dumps({
        f"{o}/apollo": good_meta(full_name=f"{o}/apollo",
                                 clone_url=f"https://github.com/{o}/apollo.git")
        for o in owners}))
    monkeypatch.setattr(sc, "collect_candidates",
                        lambda: [cand(owner=o, repo="apollo") for o in owners])

    sc.cmd_emit(emit_args())
    names = [line.split("\t")[0] for line in manifest_lines(sc.MANIFEST_OUT)[1]]
    assert len(names) == 3, f"expected all three rows, got {names}"
    assert len(set(names)) == 3, f"names still collide: {names}"


def test_emit_per_stratum_caps_each_stratum(sandbox, monkeypatch):
    """--per-stratum bounds the corpus and keeps the strata comparable."""
    meta = {f"acme/c{i}": good_meta(full_name=f"acme/c{i}", stars=100 - i)
            for i in range(4)}
    meta.update({f"apache/f{i}": good_meta(full_name=f"apache/f{i}", stars=50 - i)
                 for i in range(3)})
    write(sc.CACHE / "repo-meta.json", json.dumps(meta))
    monkeypatch.setattr(sc, "collect_candidates", lambda: (
        [cand(owner="acme", repo=f"c{i}") for i in range(4)]
        + [cand(owner="apache", repo=f"f{i}", stratum="foundation",
                fact="F1_roster:asf") for i in range(3)]))

    sc.cmd_emit(emit_args(per_stratum=2))
    data = manifest_lines(sc.MANIFEST_OUT)[1]
    strata = [l.split("\t")[2] for l in data]
    assert strata.count("community") == 2
    assert strata.count("foundation") == 2
    assert len(read_csv_rows(sc.CANDIDATES)) == 7      # candidates.csv keeps them all


def test_emit_without_balance_lang_sorts_by_stars(sandbox, monkeypatch):
    """The default pick is the most-starred projects of the stratum."""
    write(sc.CACHE / "repo-meta.json", json.dumps({
        "a/j1": good_meta(full_name="a/j1", language="Java", stars=100),
        "a/j2": good_meta(full_name="a/j2", language="Java", stars=90),
        "a/c1": good_meta(full_name="a/c1", language="C", stars=10),
        "a/c2": good_meta(full_name="a/c2", language="C", stars=5),
    }))
    monkeypatch.setattr(sc, "collect_candidates",
                        lambda: [cand(owner="a", repo=r) for r in ("j1", "j2", "c1", "c2")])

    sc.cmd_emit(emit_args(per_stratum=2))
    langs = [l.split("\t")[3] for l in manifest_lines(sc.MANIFEST_OUT)[1]]
    assert langs == [sc.LANG_FILTER["Java"], sc.LANG_FILTER["Java"]]


def test_emit_balance_lang_spreads_languages_within_a_stratum(sandbox, monkeypatch):
    """--balance-lang stops a stratum from being one language.

    Research correctness: ASF is 217/378 Java and the community world is
    overwhelmingly C, so an unbalanced stratum makes a language effect look
    like a stratum effect.
    """
    write(sc.CACHE / "repo-meta.json", json.dumps({
        "a/j1": good_meta(full_name="a/j1", language="Java", stars=100),
        "a/j2": good_meta(full_name="a/j2", language="Java", stars=90),
        "a/c1": good_meta(full_name="a/c1", language="C", stars=10),
        "a/c2": good_meta(full_name="a/c2", language="C", stars=5),
    }))
    monkeypatch.setattr(sc, "collect_candidates",
                        lambda: [cand(owner="a", repo=r) for r in ("j1", "j2", "c1", "c2")])

    sc.cmd_emit(emit_args(per_stratum=2, balance_lang=True))
    langs = [l.split("\t")[3] for l in manifest_lines(sc.MANIFEST_OUT)[1]]
    assert set(langs) == {sc.LANG_FILTER["C"], sc.LANG_FILTER["Java"]}


def test_emit_balance_lang_drains_every_bucket(sandbox, monkeypatch):
    """Round-robin must not lose rows when bucket sizes differ."""
    meta = {"a/c1": good_meta(full_name="a/c1", language="C", stars=10),
            "a/c2": good_meta(full_name="a/c2", language="C", stars=9),
            "a/c3": good_meta(full_name="a/c3", language="C", stars=8),
            "a/r1": good_meta(full_name="a/r1", language="Rust", stars=7)}
    write(sc.CACHE / "repo-meta.json", json.dumps(meta))
    monkeypatch.setattr(sc, "collect_candidates",
                        lambda: [cand(owner="a", repo=r) for r in ("c1", "c2", "c3", "r1")])

    sc.cmd_emit(emit_args(balance_lang=True))
    assert len(manifest_lines(sc.MANIFEST_OUT)[1]) == 4


def test_emit_reports_contested_rows(sandbox, monkeypatch, capsys):
    """A contested case is counted in the run log so it is never invisible."""
    write(sc.CACHE / "repo-meta.json", json.dumps({
        "apache/thing": good_meta(full_name="apache/thing", owner_is_company=True,
                                  company_fact="F2_org_verified:acme")}))
    monkeypatch.setattr(sc, "collect_candidates",
                        lambda: [cand(owner="apache", repo="thing",
                                      stratum="foundation", fact="F1_roster:asf")])
    sc.cmd_emit(emit_args())
    out = capsys.readouterr().out
    assert "contested cases kept and flagged: 1" in out
    assert read_csv_rows(sc.CANDIDATES)[0]["contested"] == "F1_roster:asf vs F2_org_verified:acme"


def test_emit_logs_the_stratum_by_language_and_size_tables(sandbox, monkeypatch, capsys):
    """The run log carries the corpus composition for the paper."""
    write(sc.CACHE / "repo-meta.json", json.dumps({"acme/widget": good_meta()}))
    monkeypatch.setattr(sc, "collect_candidates", lambda: [cand()])
    sc.cmd_emit(emit_args())
    out = capsys.readouterr().out
    assert "eligible by stratum x language" in out
    assert "selected: stratum x size_class" in out


# ================================================================ fetch / rosters


class FakeResponse:
    def __init__(self, data: bytes):
        self.data = data

    def read(self) -> bytes:
        return self.data

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_fetch_never_refetches_a_cached_file(sandbox):
    """Roster downloads are expensive and public, so a cache hit is final."""
    dest = write(sandbox.cache / "roster-asf.raw", "cached")
    assert sc.fetch("https://example.invalid/x", dest) is dest
    assert dest.read_text() == "cached"          # urlopen is _boom; it was not called


def test_fetch_downloads_when_the_cache_is_empty(sandbox, monkeypatch):
    """A zero-byte file is a failed download, so it must be retried."""
    monkeypatch.setattr(sc.urllib.request, "urlopen",
                        lambda req, timeout=None: FakeResponse(b'{"ok": 1}'))
    dest = write(sandbox.cache / "roster-asf.raw", "")
    assert sc.fetch("https://example.invalid/x", dest).read_text() == '{"ok": 1}'


def test_fetch_creates_the_parent_directory(sandbox, monkeypatch):
    monkeypatch.setattr(sc.urllib.request, "urlopen",
                        lambda req, timeout=None: FakeResponse(b"body"))
    dest = sandbox.cache / "deep" / "nested" / "file.raw"
    assert sc.fetch("https://example.invalid/x", dest).read_text() == "body"


def eclipse_page_fetcher(pages: dict[int, object]):
    def fake_fetch(url, dest):
        page = int(url.split("page=")[1].split("&")[0])
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(json.dumps(pages.get(page, [])))
        return dest
    return fake_fetch


def test_fetch_eclipse_pages_until_a_page_is_empty(sandbox, monkeypatch):
    """Eclipse caps pagesize at 100 and ignores anything larger."""
    monkeypatch.setattr(sc, "fetch", eclipse_page_fetcher({
        0: [{"name": "a"}, {"name": "b"}], 1: [{"name": "c"}], 2: []}))
    got = json.loads(sc.fetch_eclipse().read_text())
    assert [p["name"] for p in got] == ["a", "b", "c"]
    assert list(sandbox.cache.glob(".ecl-*")) == []     # scratch files cleaned up


def test_fetch_eclipse_stops_at_the_page_ceiling(sandbox, monkeypatch):
    """The crawl is bounded at 40 pages, so a paging bug cannot loop forever."""
    monkeypatch.setattr(sc, "fetch", eclipse_page_fetcher(
        {p: [{"name": f"p{p}"}] for p in range(50)}))
    assert len(json.loads(sc.fetch_eclipse().read_text())) == 40


def test_fetch_eclipse_flattens_a_dict_page(sandbox, monkeypatch):
    monkeypatch.setattr(sc, "fetch", eclipse_page_fetcher({0: {"k": {"name": "a"}}, 1: []}))
    assert json.loads(sc.fetch_eclipse().read_text()) == [{"name": "a"}]


def test_fetch_eclipse_stops_on_a_failure(sandbox, monkeypatch):
    """A mid-crawl failure keeps whatever was already paged."""
    def flaky(url, dest):
        if "page=0" in url:
            dest.write_text(json.dumps([{"name": "a"}]))
            return dest
        raise OSError("timeout")

    monkeypatch.setattr(sc, "fetch", flaky)
    assert json.loads(sc.fetch_eclipse().read_text()) == [{"name": "a"}]


def test_fetch_eclipse_returns_the_cached_file(sandbox, monkeypatch):
    monkeypatch.setattr(sc, "fetch", _boom)
    dest = write(sandbox.cache / "roster-eclipse.raw", "[]")
    assert sc.fetch_eclipse() == dest


def test_fetch_subpages_concatenates_the_index_and_its_children(sandbox, monkeypatch):
    """SFC and SPI index pages link to per-project pages, which hold the slugs."""
    write(sc.CACHE / "roster-sfc.raw", " ".join([
        'href="inkscape/"', 'href="/absolute"', 'href="../up"',
        'href="style.css"', 'href="logo.png"',
    ]))

    def fake_fetch(url, dest):
        dest.write_text(f"<page {url}> github.com/inkscape/inkscape")
        return dest

    monkeypatch.setattr(sc, "fetch", fake_fetch)
    text = sc.fetch_subpages("sfc").read_text()
    assert "github.com/inkscape/inkscape" in text
    assert "absolute" not in text.split("<page")[1]
    assert list(sandbox.cache.glob(".sub-sfc-*")) == []


def test_fetch_subpages_skips_a_child_that_fails(sandbox, monkeypatch):
    """One dead project page must not lose the whole roster."""
    write(sc.CACHE / "roster-sfc.raw", 'href="dead/" href="alive/"')

    def fake_fetch(url, dest):
        if "dead" in url:
            raise OSError("404")
        dest.write_text("github.com/alive/alive")
        return dest

    monkeypatch.setattr(sc, "fetch", fake_fetch)
    assert "github.com/alive/alive" in sc.fetch_subpages("sfc").read_text()


def test_fetch_subpages_returns_the_cached_file(sandbox, monkeypatch):
    monkeypatch.setattr(sc, "fetch", _boom)
    dest = write(sandbox.cache / "roster-sfc-pages.raw", "cached")
    assert sc.fetch_subpages("sfc") == dest


def test_cmd_rosters_reports_every_source(sandbox, monkeypatch, capsys):
    """`rosters` is the entry point for the whole cache; it must summarise."""
    def fake_fetch(url, dest):
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text("x" * 10)
        return dest

    monkeypatch.setattr(sc, "fetch", fake_fetch)
    monkeypatch.setattr(sc, "fetch_eclipse",
                        lambda: write(sc.CACHE / "roster-eclipse.raw", json.dumps([1, 2, 3])))
    monkeypatch.setattr(sc, "fetch_subpages",
                        lambda n: write(sc.CACHE / f"roster-{n}-pages.raw", "y"))

    assert sc.cmd_rosters(SimpleNamespace()) == 0
    out = capsys.readouterr().out
    for name in ("asf", "cncf", "sfc", "spi", "eclipse"):
        assert name in out
    assert "(3 projects)" in out


def test_cmd_rosters_survives_a_failure_in_every_stage(sandbox, monkeypatch, capsys):
    """A single unreachable roster must not stop the rest."""
    monkeypatch.setattr(sc, "fetch", lambda u, d: (_ for _ in ()).throw(OSError("no net")))
    monkeypatch.setattr(sc, "fetch_eclipse",
                        lambda: (_ for _ in ()).throw(OSError("no net")))
    monkeypatch.setattr(sc, "fetch_subpages",
                        lambda n: (_ for _ in ()).throw(OSError("no net")))

    assert sc.cmd_rosters(SimpleNamespace()) == 0
    out = capsys.readouterr().out
    assert out.count("FAILED") == len(sc.ROSTERS) + 1 + 2


# ================================================================ cmd_all / main


def test_cmd_all_runs_the_three_stages_in_order(sandbox, monkeypatch):
    seen = []
    monkeypatch.setattr(sc, "cmd_rosters", lambda a: seen.append("rosters") or 0)
    monkeypatch.setattr(sc, "cmd_enrich", lambda a: seen.append("enrich") or 0)
    monkeypatch.setattr(sc, "cmd_emit", lambda a: seen.append("emit") or 0)
    assert sc.cmd_all(SimpleNamespace()) == 0
    assert seen == ["rosters", "enrich", "emit"]


def test_cmd_all_stops_at_the_first_failing_stage(sandbox, monkeypatch):
    """A throttled enrich must not go on to emit a truncated manifest."""
    monkeypatch.setattr(sc, "cmd_rosters", lambda a: 0)
    monkeypatch.setattr(sc, "cmd_enrich", lambda a: 2)
    monkeypatch.setattr(sc, "cmd_emit", _boom)
    assert sc.cmd_all(SimpleNamespace()) == 2


def test_main_dispatches_rosters(sandbox, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["select_corpus.py", "rosters"])
    monkeypatch.setattr(sc, "cmd_rosters", lambda a: 7)
    assert sc.main() == 7


def test_main_parses_the_enrich_flags(sandbox, monkeypatch):
    monkeypatch.setattr(sys, "argv",
                        ["select_corpus.py", "enrich", "--limit", "5", "--retry-errors"])
    seen = {}
    monkeypatch.setattr(sc, "cmd_enrich", lambda a: seen.update(vars(a)) or 0)
    assert sc.main() == 0
    assert seen["limit"] == 5
    assert seen["retry_errors"] is True


@pytest.mark.parametrize("cmd", ["emit", "all"])
def test_main_parses_the_emit_flags(sandbox, monkeypatch, cmd):
    monkeypatch.setattr(sys, "argv",
                        ["select_corpus.py", cmd, "--per-stratum", "170", "--balance-lang"])
    seen = {}
    monkeypatch.setattr(sc, "cmd_emit", lambda a: seen.update(vars(a)) or 0)
    monkeypatch.setattr(sc, "cmd_all", lambda a: seen.update(vars(a)) or 0)
    assert sc.main() == 0
    assert seen["per_stratum"] == 170
    assert seen["balance_lang"] is True


def test_main_creates_the_cache_directory(sandbox, monkeypatch):
    """Every subcommand needs .corpus-cache to exist before it runs."""
    fresh = sandbox.root / "brand-new-cache"
    monkeypatch.setattr(sc, "CACHE", fresh)
    monkeypatch.setattr(sys, "argv", ["select_corpus.py", "rosters"])
    monkeypatch.setattr(sc, "cmd_rosters", lambda a: 0)
    sc.main()
    assert fresh.is_dir()


def test_main_requires_a_subcommand(sandbox, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["select_corpus.py"])
    with pytest.raises(SystemExit):
        sc.main()


# ================================================================ GhFailed paths
#
# gh() raises GhFailed when GitHub answers with a state the classifier does not
# know. A DMCA-blocked repository (HTTP 451) was exactly that, and it aborted a
# 24,405-candidate pass after 154 repositories. So every caller must degrade
# instead of propagating, and only RateLimited may stop a run.


UNKNOWN = "gh: teapot (HTTP 418)"


def test_gh_raises_ghfailed_for_an_unrecognised_state(sandbox, monkeypatch):
    """An unknown state is neither gone nor transient. It must be distinguishable."""
    monkeypatch.setattr(sc.subprocess, "run", runner(rc=1, err=UNKNOWN))
    with pytest.raises(sc.GhFailed):
        sc.gh("repos/acme/widget", retries=2)


def test_gh_raises_ratelimited_when_the_subprocess_itself_fails(sandbox, monkeypatch):
    """A network failure is transient, so it must raise RateLimited, not GhFailed.

    Before the fix this raised GhFailed, which made cmd_enrich record a live
    repository as permanently unclassified whenever the network blinked.
    """
    monkeypatch.setattr(sc.subprocess, "run",
                        runner(raises=OSError("network is unreachable")))
    with pytest.raises(sc.RateLimited):
        sc.gh("repos/acme/widget", retries=2)


@pytest.mark.parametrize("err", ["HTTP 502: Bad Gateway", "gh: 503 unavailable",
                                 "dial tcp: no such host"])
def test_gh_treats_a_server_failure_as_transient(sandbox, monkeypatch, err):
    """A 5xx is not a quota problem, but it is still transient."""
    monkeypatch.setattr(sc.subprocess, "run", runner(rc=1, err=err))
    with pytest.raises(sc.RateLimited):
        sc.gh("repos/acme/widget", retries=2)


@pytest.mark.parametrize("err", ["gh: Repository access blocked (HTTP 451)",
                                 "gh: Gone (HTTP 410)"])
def test_gh_treats_a_blocked_or_removed_repo_as_gone(sandbox, monkeypatch, err):
    """451 and 410 are permanent. Caching them as dead is correct and saves quota.

    A DMCA-blocked repository is what aborted the first full enrich pass.
    """
    monkeypatch.setattr(sc.subprocess, "run", runner(rc=1, err=err))
    assert sc.gh("repos/acme/widget", retries=2) is None


def test_commit_count_returns_none_on_ghfailed(sandbox, monkeypatch):
    """One odd repository costs its commit count, not the whole pass."""
    monkeypatch.setattr(sc, "gh", lambda *a, **k: (_ for _ in ()).throw(sc.GhFailed("x")))
    assert sc.commit_count("acme", "widget") is None


def test_commit_count_still_propagates_ratelimited(sandbox, monkeypatch):
    """Only RateLimited may stop a run, so it must not be swallowed here."""
    monkeypatch.setattr(sc, "gh",
                        lambda *a, **k: (_ for _ in ()).throw(sc.RateLimited("x")))
    with pytest.raises(sc.RateLimited):
        sc.commit_count("acme", "widget")


def test_org_is_company_claims_nothing_on_ghfailed(sandbox, monkeypatch):
    """An unreadable namespace must not become a company by accident."""
    monkeypatch.setattr(sc, "gh", lambda *a, **k: (_ for _ in ()).throw(sc.GhFailed("x")))
    cache: dict = {}
    assert sc.org_is_company("mystery", cache) == (False, "")


def test_parse_company_orgs_skips_an_org_that_fails(sandbox, monkeypatch):
    """One unreadable company namespace must not lose the other sixty."""
    calls = []

    def fake_gh(path, *a, **k):
        calls.append(path)
        if path.startswith("orgs/aws/"):
            raise sc.GhFailed("aws exploded")
        return [{"name": "thing", "language": "C", "fork": False, "archived": False}]

    monkeypatch.setattr(sc, "gh", fake_gh)
    monkeypatch.setattr(sc, "COMPANY_ORGS", {"aws", "google"})
    out = sc.parse_company_orgs()
    assert [r["owner"] for r in out] == ["google"]
    assert len(calls) == 2


def test_enrich_records_an_unclassified_repo_and_carries_on(sandbox, monkeypatch):
    """A single unknown state must not abort the pass; it must leave a detail.

    Before the fix, one DMCA-blocked repository ended a 24,405-candidate run.
    """
    monkeypatch.setattr(sc, "collect_candidates",
                        lambda: [cand(repo="blocked"), cand(repo="fine")])
    monkeypatch.setattr(sc, "commit_count", lambda o, r: 1_234)
    monkeypatch.setattr(sc, "org_is_company", lambda login, cache: (False, ""))

    def fake_gh(path, *a, **k):
        if path.endswith("blocked"):
            raise sc.GhFailed("repos/acme/blocked: teapot")
        return repo_payload(full_name="acme/fine")

    monkeypatch.setattr(sc, "gh", fake_gh)
    assert sc.cmd_enrich(SimpleNamespace(limit=0, retry_errors=False)) == 0
    meta = json.loads((sc.CACHE / "repo-meta.json").read_text())
    assert meta["acme/blocked"]["error"] == "unclassified"
    assert "teapot" in meta["acme/blocked"]["detail"]
    assert meta["acme/fine"]["language"] == "C"


# ================================================================ cmd_review()


def review_csv(rows: list[dict]) -> None:
    """Write a candidates.csv exactly as cmd_emit does."""
    import csv
    cols = ["source", "stratum", "fact", "contested", "label_date", "owner", "repo",
            "roster_name", "roster_lang", "language", "commits", "size_class",
            "size_kb", "stars", "pushed_at", "license", "owner_type", "archived",
            "fork", "clone_url", "included", "excluded_because"]
    sc.CANDIDATES.parent.mkdir(parents=True, exist_ok=True)
    with sc.CANDIDATES.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            base = {c: "" for c in cols}
            base.update(r)
            w.writerow(base)


def review_row(**over) -> dict:
    r = dict(source="ghsearch", stratum="community", fact="F1_residual:none",
             contested="", owner="acme", repo="widget", language="C", commits="1000",
             size_class="S", stars="100", included="True", excluded_because="")
    r.update(over)
    return r


def test_review_needs_candidates_csv_first(sandbox):
    """Without the decision record there is nothing to review."""
    assert sc.cmd_review(SimpleNamespace()) == 1


def test_review_reports_the_three_totals(sandbox):
    """The header must reconcile: candidates = eligible + excluded."""
    review_csv([review_row(),
                review_row(repo="dead", included="False",
                           excluded_because="unreachable")])
    assert sc.cmd_review(SimpleNamespace()) == 0
    text = sc.REVIEW_OUT.read_text()
    assert "- candidates: **2**" in text
    assert "- eligible: **1**" in text
    assert "- excluded: **1**" in text


def test_review_warns_when_enrichment_is_incomplete(sandbox):
    """A partial snapshot must say so, or someone freezes the corpus from it."""
    review_csv([review_row(repo="pending", included="False",
                           excluded_because="not-enriched")])
    sc.cmd_review(SimpleNamespace())
    assert "partial snapshot" in sc.REVIEW_OUT.read_text()


def test_review_is_silent_when_enrichment_is_complete(sandbox):
    """No false alarm once every candidate has been fetched."""
    review_csv([review_row()])
    sc.cmd_review(SimpleNamespace())
    assert "partial snapshot" not in sc.REVIEW_OUT.read_text()


def test_review_splits_one_row_into_several_reasons(sandbox):
    """A row can fail several filters, and the flow table counts each one."""
    review_csv([review_row(included="False",
                           excluded_because="lang=Python; size=10kb; archived")])
    sc.cmd_review(SimpleNamespace())
    text = sc.REVIEW_OUT.read_text()
    for reason in ("`lang=...`", "`size=...`", "`archived`"):
        assert reason in text


def test_review_lists_every_contested_case_including_excluded_ones(sandbox):
    """A contested label is a human decision, so none may be hidden."""
    review_csv([review_row(repo="a", contested="F1_roster:cncf vs F2_org:alibaba"),
                review_row(repo="b", contested="F1_roster:asf vs F2_org:aws",
                           included="False", excluded_because="archived")])
    sc.cmd_review(SimpleNamespace())
    text = sc.REVIEW_OUT.read_text()
    assert "## 4. Contested cases — 2" in text
    assert "acme/a" in text and "acme/b" in text


def test_review_names_the_foundation_namespace_fact_correctly(sandbox):
    """F2_org_foundation must not read as a company. Longest match wins."""
    review_csv([review_row(fact="F2_org_foundation:apache")])
    sc.cmd_review(SimpleNamespace())
    assert "a foundation namespace owns the repository" in sc.REVIEW_OUT.read_text()


def test_review_flags_the_weakest_evidence_separately(sandbox):
    """Cohort rows rest on an ABSENCE of evidence, so they get their own section."""
    review_csv([review_row(source="cohort", repo="weak",
                           fact="F1_pool:spinellis-cohort")])
    sc.cmd_review(SimpleNamespace())
    text = sc.REVIEW_OUT.read_text()
    assert "## 6. Weakest evidence" in text
    assert "acme/weak" in text


def test_review_sorts_the_largest_projects_first(sandbox):
    """A mislabel costs most on the biggest project, so those are listed first."""
    review_csv([review_row(repo="small", stars="1"),
                review_row(repo="huge", stars="90000")])
    sc.cmd_review(SimpleNamespace())
    text = sc.REVIEW_OUT.read_text()
    assert text.index("acme/huge") < text.index("acme/small")


def test_review_survives_blank_numeric_fields(sandbox):
    """An unenriched row has no stars and no commits. That must not crash."""
    review_csv([review_row(repo="bare", stars="", commits="", language="")])
    assert sc.cmd_review(SimpleNamespace()) == 0


# --------------------------------------------------------------------------- #
# project_name: the manifest key
# --------------------------------------------------------------------------- #

def test_project_name_includes_the_owner():
    """Repository names are not unique across owners. The name became the workdir,
    the lock and the stamp in ctp.py, so a collision made two projects share one
    directory and one stamped the other DONE."""
    assert sc.project_name("redis", "redis") == "redis__redis"
    assert sc.project_name("tporadowski", "redis") == "tporadowski__redis"
    assert sc.project_name("redis", "redis") != sc.project_name("tporadowski", "redis")


def test_project_name_separates_the_apollo_collision_across_strata():
    """The real case that made this a correctness defect rather than a nuisance:
    three apollo rows, two community and one company-owned. Sharing a name meant
    the published parquet could carry one repository's tokens under another
    repository's stratum, which is the study's independent variable."""
    names = {sc.project_name(o, "apollo")
             for o in ("apolloconfig", "ClassicOldSong", "ApolloAuto")}
    assert len(names) == 3, f"apollo rows still collide: {names}"


@pytest.mark.parametrize("owner, repo, expected", [
    ("Genymobile", "scrcpy", "genymobile__scrcpy"),
    ("apache", "commons-lang", "apache__commons-lang"),
    ("rust-lang", "rustlings", "rust-lang__rustlings"),
    ("foo", "bar.baz", "foo__bar-baz"),          # a dot is not path-friendly
    ("some_org", "under_score", "some-org__under-score"),
    ("Weird", "a  b", "weird__a-b"),             # runs collapse to one hyphen
    ("-lead-", "-trail-", "lead__trail"),        # no leading or trailing hyphen
])
def test_project_name_is_lowercase_and_path_safe(owner, repo, expected):
    """The name is used as a directory component and as a Parquet filename
    prefix, so only lowercase letters, digits and hyphens may survive."""
    assert sc.project_name(owner, repo) == expected


def test_project_name_never_contains_a_slash():
    """run_pipeline_process.sh exits 2 on a --repo-name holding '/', so
    owner/repo cannot be passed through even though it is the natural key."""
    assert "/" not in sc.project_name("a/b", "c/d")


# ================================================================ pacing
#
# Why this group exists: the pass that failed sustained 2.10 API calls/second,
# about 7,559 per hour against a 5,000/hour budget shared with every other tool
# using this token. Reactive backoff cannot fix that, because by the time a 403
# arrives the budget is already spent. These tests pin the proactive half.


def test_budget_remaining_reports_the_smallest_bucket(monkeypatch):
    """Either budget running out stops the pass, so the smaller one governs."""
    body = json.dumps({"resources": {"core": {"remaining": 4000, "reset": 0},
                                     "graphql": {"remaining": 120, "reset": 0}}})
    monkeypatch.setattr(sc.subprocess, "run", lambda *a, **k: cp(0, body))
    assert sc.budget_remaining() == 120


@pytest.mark.parametrize("body", ["", "not json", '{"resources": {}}', '{"nope": 1}'])
def test_budget_remaining_is_none_when_unreadable(monkeypatch, body):
    """Unknown must not read as zero. Zero would trigger an hour-long hold on
    every recheck and stall the pass for no reason."""
    monkeypatch.setattr(sc.subprocess, "run", lambda *a, **k: cp(0, body))
    assert sc.budget_remaining() is None


def test_pace_call_waits_out_the_minimum_interval(monkeypatch, sandbox):
    """The governor that keeps the pass inside the budget instead of 51% over."""
    monkeypatch.setattr(sc, "MIN_CALL_INTERVAL_S", 1.0)
    monkeypatch.setattr(sc.time, "time", lambda: 1000.0)
    sc.reset_pacing()

    sc.pace_call()                      # first call: no history, no wait
    assert sandbox.slept == []
    sc.pace_call()                      # immediately after: must wait
    assert len(sandbox.slept) == 1
    assert sandbox.slept[0] == pytest.approx(1.0 + sc.CALL_JITTER_S)


def test_pace_call_does_not_wait_when_the_gap_already_passed(monkeypatch, sandbox):
    """Pacing must not tax a pass that is already slow, for instance one whose
    calls are dominated by GraphQL latency."""
    monkeypatch.setattr(sc, "MIN_CALL_INTERVAL_S", 1.0)
    clock = {"t": 1000.0}
    monkeypatch.setattr(sc.time, "time", lambda: clock["t"])
    sc.reset_pacing()

    sc.pace_call()
    clock["t"] += 30.0                  # a slow call happened in between
    sc.pace_call()
    assert sandbox.slept == []


def test_pace_call_holds_when_the_budget_is_near_exhaustion(monkeypatch, sandbox):
    """Leaving a floor unspent is the point. The token is shared, so racing the
    other tools down to zero is what invites a block."""
    monkeypatch.setattr(sc, "MIN_CALL_INTERVAL_S", 0)
    monkeypatch.setattr(sc, "BUDGET_FLOOR", 500)
    monkeypatch.setattr(sc, "BUDGET_RECHECK_CALLS", 1)
    reset = int(datetime.now(timezone.utc).timestamp()) + 900
    body = json.dumps({"resources": {"core": {"remaining": 12, "reset": reset},
                                     "graphql": {"remaining": 4000, "reset": reset}}})
    monkeypatch.setattr(sc.subprocess, "run", lambda *a, **k: cp(0, body))
    sc.reset_pacing()

    sc.pace_call()
    assert sandbox.slept, "the pass kept spending below the floor"
    assert sandbox.slept[0] >= 120


def test_pace_call_does_not_hold_when_the_budget_is_healthy(monkeypatch, sandbox):
    """The floor must not throttle a pass that has plenty of budget left."""
    monkeypatch.setattr(sc, "MIN_CALL_INTERVAL_S", 0)
    monkeypatch.setattr(sc, "BUDGET_FLOOR", 500)
    monkeypatch.setattr(sc, "BUDGET_RECHECK_CALLS", 1)
    monkeypatch.setattr(sc.subprocess, "run", lambda *a, **k: cp(0, RATE_LIMIT_OK))
    sc.reset_pacing()

    sc.pace_call()
    assert sandbox.slept == []


def test_pace_call_checks_the_budget_only_periodically(monkeypatch, sandbox):
    """The probe is free of quota but not of time. Reading it on every call would
    double the wall time of a 24,405-row pass."""
    monkeypatch.setattr(sc, "MIN_CALL_INTERVAL_S", 0)
    monkeypatch.setattr(sc, "BUDGET_FLOOR", 500)
    monkeypatch.setattr(sc, "BUDGET_RECHECK_CALLS", 10)
    probes = []
    monkeypatch.setattr(sc.subprocess, "run",
                        lambda *a, **k: (probes.append(1), cp(0, RATE_LIMIT_OK))[1])
    sc.reset_pacing()

    for _ in range(25):
        sc.pace_call()
    assert len(probes) == 2, f"expected one probe per 10 calls, got {len(probes)}"


def test_backoff_is_exponential_with_jitter_and_a_cap(monkeypatch):
    """A bare 2**attempt lands every retry on the same instants, which is the
    pattern abuse detection looks for. Full jitter spreads them."""
    monkeypatch.setattr(sc.random, "uniform", lambda lo, hi: (lo, hi))
    assert sc.backoff(0) == (0.5, 1)
    assert sc.backoff(3) == (4, 8)
    assert sc.backoff(20) == (sc.BACKOFF_CAP_S / 2, sc.BACKOFF_CAP_S), "no cap"


def test_backoff_never_exceeds_the_cap(monkeypatch):
    """A late attempt must not sleep for hours; a quota wait is handled
    separately by rate_limit_wait."""
    monkeypatch.undo()
    for attempt in range(0, 30):
        assert 0 <= sc.backoff(attempt) <= sc.BACKOFF_CAP_S


def test_reset_pacing_forgets_the_history(monkeypatch, sandbox):
    """A fresh pass must not inherit the previous pass's last-call instant."""
    monkeypatch.setattr(sc, "MIN_CALL_INTERVAL_S", 1.0)
    monkeypatch.setattr(sc.time, "time", lambda: 1000.0)
    sc.reset_pacing()
    sc.pace_call()
    sc.reset_pacing()
    sandbox.slept.clear()
    sc.pace_call()
    assert sandbox.slept == [], "history survived the reset"


# ================================================================ cmd_sample()
#
# The sample is the population the dataset paper describes, so the draw has to
# be reproducible from the frame and the seed alone, and the allocation has to
# be checkable against the frame.


def sample_args(**over):
    base = dict(target=10, seed=sc.SAMPLE_SEED, floor=sc.SAMPLE_FLOOR)
    base.update(over)
    return SimpleNamespace(**base)


def frame_rows(n_per_cell: int, cells=(("community", "C", "S"),)) -> list[dict]:
    """A candidates.csv with n_per_cell eligible rows in each named cell."""
    rows = []
    for stratum, lang, size in cells:
        for i in range(n_per_cell):
            rows.append(review_row(
                stratum=stratum, language=lang, size_class=size,
                owner=f"{stratum[:3]}{lang[:1]}{size}", repo=f"r{i:03d}",
                clone_url=f"https://example.invalid/{stratum}/{lang}/{size}/r{i:03d}.git"))
    return rows


# ---- sample_frame: the frame is the eligible rows only


def test_sample_frame_keeps_only_included_rows():
    """An excluded row stays in candidates.csv as a decision record. It is not
    part of the frame, so it must never be drawn."""
    review_csv([review_row(repo="in", included="True"),
                review_row(repo="out", included="False", excluded_because="archived")])
    assert [r["repo"] for r in sc.sample_frame()] == ["in"]


# ---- one repository, one row. GitHub redirects a renamed repository, so two
# rosters can name the same project under two owners.


@pytest.mark.parametrize("url,owner", [
    ("https://github.com/apache/doris.git", "apache"),
    ("https://github.com/apache/doris", "apache"),
    ("https://github.com/apache/doris/", "apache"),
    ("https://github.com/Tencent/APIJSON.git", "tencent"),
    ("nonsense", ""),
])
def test_url_owner_reads_the_owner_segment(url, owner):
    assert sc.url_owner(url) == owner


def test_dedupe_keeps_the_row_whose_owner_matches_the_url():
    """apache/incubator-doris and apache/doris are one repository. The current
    name is the one the repository answers to."""
    url = "https://github.com/apache/doris.git"
    rows = [review_row(owner="apache", repo="incubator-doris", clone_url=url),
            review_row(owner="apache", repo="doris", clone_url=url)]
    kept = sc.dedupe_by_clone_url(rows)
    assert [(r["owner"], r["repo"]) for r in kept] == [("apache", "doris")]


def test_dedupe_survives_a_stratum_disagreement_by_taking_the_current_name():
    """Five real pairs disagree on the stratum, and every one is a donation.
    pingcap/tikv was community; tikv/tikv is foundation. The frame must not
    hold both, and the current namespace decides."""
    url = "https://github.com/tikv/tikv.git"
    rows = [review_row(owner="pingcap", repo="tikv", clone_url=url,
                       stratum="community"),
            review_row(owner="tikv", repo="tikv", clone_url=url,
                       stratum="foundation")]
    kept = sc.dedupe_by_clone_url(rows)
    assert [r["stratum"] for r in kept] == ["foundation"]


def test_dedupe_is_deterministic_when_no_owner_matches_the_url():
    url = "https://github.com/new-home/thing.git"
    rows = [review_row(owner="zeta", repo="thing", clone_url=url),
            review_row(owner="alpha", repo="thing", clone_url=url)]
    assert [r["owner"] for r in sc.dedupe_by_clone_url(rows)] == ["alpha"]


def test_dedupe_never_merges_rows_that_have_no_clone_url():
    """Grouping empty URLs together would collapse unrelated projects."""
    rows = [review_row(owner="a", repo="one", clone_url=""),
            review_row(owner="b", repo="two", clone_url="")]
    assert len(sc.dedupe_by_clone_url(rows)) == 2


def test_dedupe_leaves_distinct_repositories_alone():
    rows = [review_row(owner="a", repo="one",
                       clone_url="https://github.com/a/one.git"),
            review_row(owner="b", repo="two",
                       clone_url="https://github.com/b/two.git")]
    assert len(sc.dedupe_by_clone_url(rows)) == 2


def test_sample_frame_counts_a_redirected_repository_once():
    """The frame is what the paper reports as N, so a redirect pair must not
    inflate it."""
    url = "https://github.com/apache/doris.git"
    review_csv([review_row(owner="apache", repo="incubator-doris",
                           clone_url=url, included="True"),
                review_row(owner="apache", repo="doris",
                           clone_url=url, included="True")])
    assert [r["repo"] for r in sc.sample_frame()] == ["doris"]


# ---- cells_of: grouping, and an order the seed alone controls


def test_cells_of_groups_by_stratum_language_and_size():
    rows = frame_rows(2, cells=(("community", "C", "S"), ("foundation", "Rust", "L")))
    cells = sc.cells_of(rows)
    assert sorted(cells) == [("community", "C", "S"), ("foundation", "Rust", "L")]
    assert all(len(v) == 2 for v in cells.values())


def test_cells_of_sorts_rows_so_the_seed_alone_decides_the_draw():
    """Without this sort the draw would depend on the order candidates.csv
    happened to be written in, and the seed would not reproduce it."""
    rows = [review_row(owner="zeta", repo="b"), review_row(owner="Alpha", repo="a")]
    pool = sc.cells_of(rows)[("community", "C", "S")]
    assert [r["owner"] for r in pool] == ["Alpha", "zeta"]


# ---- allocate: floor, proportionality, and the two clamps


def test_allocate_gives_every_cell_the_floor_first():
    """Proportional allocation alone empties the thin cells, and a stratum that
    loses a language stops being comparable to the others."""
    take = sc.allocate({("a",): 100, ("b",): 3}, target=20, floor=2)
    assert take[("b",)] >= 2


def test_allocate_shares_the_rest_in_proportion():
    take = sc.allocate({("a",): 90, ("b",): 10}, target=20, floor=0)
    assert take == {("a",): 18, ("b",): 2}


def test_allocate_draws_exactly_the_target():
    sizes = {("a",): 40, ("b",): 30, ("c",): 7}
    for target in (13, 40, 77):
        assert sum(sc.allocate(sizes, target).values()) == target


def test_allocate_never_asks_a_cell_for_more_than_it_holds():
    """A thin cell caps the draw. Asking for more would fail in random.sample."""
    sizes = {("a",): 2, ("b",): 3}
    take = sc.allocate(sizes, target=99)
    assert take[("a",)] <= 2 and take[("b",)] <= 3


def test_allocate_cannot_exceed_the_whole_frame():
    sizes = {("a",): 2, ("b",): 3}
    assert sum(sc.allocate(sizes, target=1000).values()) == 5


def test_allocate_with_a_target_below_the_total_floor_returns_the_floor():
    """The floor wins. Cutting below it would empty cells, which is the thing
    the floor exists to prevent; the caller sees the larger count reported."""
    take = sc.allocate({("a",): 5, ("b",): 5}, target=1, floor=2)
    assert take == {("a",): 2, ("b",): 2}


def test_allocate_handles_a_frame_with_no_room_left_after_the_floor():
    """Every cell is at or below the floor, so there is nothing to share out."""
    take = sc.allocate({("a",): 2, ("b",): 1}, target=50, floor=2)
    assert take == {("a",): 2, ("b",): 1}


def test_allocate_breaks_remainder_ties_on_the_cell_key():
    """Equal cells have equal remainders. Breaking the tie on the key keeps the
    result independent of dict insertion order."""
    sizes = {("b",): 10, ("a",): 10}
    assert sc.allocate(sizes, target=3, floor=0) == {("a",): 2, ("b",): 1}


# ---- draw: reproducible, and isolated per cell


def test_draw_is_reproducible_for_the_same_seed():
    cells = sc.cells_of(frame_rows(20))
    take = {("community", "C", "S"): 5}
    first = [r["repo"] for r in sc.draw(cells, take, seed=7)]
    second = [r["repo"] for r in sc.draw(cells, take, seed=7)]
    assert first == second


def test_draw_changes_with_the_seed():
    cells = sc.cells_of(frame_rows(40))
    take = {("community", "C", "S"): 10}
    a = [r["repo"] for r in sc.draw(cells, take, seed=1)]
    b = [r["repo"] for r in sc.draw(cells, take, seed=2)]
    assert a != b


def test_draw_takes_each_project_at_most_once():
    cells = sc.cells_of(frame_rows(30))
    picked = sc.draw(cells, {("community", "C", "S"): 30}, seed=3)
    assert len(picked) == 30 == len({r["repo"] for r in picked})


def test_draw_seeds_each_cell_separately():
    """Cells are seeded from the run seed plus the cell key, so changing what
    one cell contributes cannot reshuffle another."""
    cells = sc.cells_of(frame_rows(20, cells=(("community", "C", "S"),
                                              ("foundation", "Rust", "L"))))
    key_a, key_b = ("community", "C", "S"), ("foundation", "Rust", "L")
    only_a = [r["repo"] for r in sc.draw(cells, {key_a: 4}, seed=9)]
    with_b = [r["repo"] for r in sc.draw(cells, {key_a: 4, key_b: 7}, seed=9)
              if r["stratum"] == "community"]
    assert only_a == with_b


def test_draw_skips_cells_allocated_nothing():
    cells = sc.cells_of(frame_rows(5, cells=(("community", "C", "S"),
                                             ("foundation", "Rust", "L"))))
    picked = sc.draw(cells, {("community", "C", "S"): 2}, seed=4)
    assert {r["stratum"] for r in picked} == {"community"}


# ---- draw: a phase extends the one before it
#
# The corpus runs in phases, so phase 2 must contain phase 1. A project costs
# hours, and a draw that reshuffled would throw that work away.


def test_draw_nests_when_a_cell_is_asked_for_more():
    cells = sc.cells_of(frame_rows(30))
    key = ("community", "C", "S")
    small = {r["repo"] for r in sc.draw(cells, {key: 5}, seed=13)}
    large = {r["repo"] for r in sc.draw(cells, {key: 11}, seed=13)}
    assert small <= large


def test_draw_nests_across_every_cell():
    # frame_rows repeats repo names across cells, so the row key is owner+repo.
    keys = (("community", "C", "S"), ("foundation", "Rust", "L"))
    cells = sc.cells_of(frame_rows(20, cells=keys))
    ident = lambda rows: {(r["owner"], r["repo"]) for r in rows}
    small = ident(sc.draw(cells, dict.fromkeys(keys, 3), seed=5))
    large = ident(sc.draw(cells, dict.fromkeys(keys, 9), seed=5))
    assert small <= large and len(small) == 6 and len(large) == 18


def test_draw_nests_where_random_sample_would_not():
    """random.sample picks its algorithm from k against the pool size, so
    growing k can drop a member. At pool 30 and seed 13 it loses one. The draw
    must not inherit that, because the earlier phase is already running."""
    pool = [f"p{i:03d}" for i in range(30)]
    lost = set(random.Random(13).sample(pool, 5)) - set(random.Random(13).sample(pool, 11))
    assert lost, "premise gone: random.sample now nests at this seed"

    rng = random.Random(13)
    order = list(pool)
    rng.shuffle(order)
    assert set(order[:5]) <= set(order[:11])


# ---- the written artefacts


def test_sample_manifest_carries_the_language_file_filter(sandbox):
    """ctp reads this column to build --mask, so a wrong filter tokenizes
    nothing."""
    picked = frame_rows(1, cells=(("foundation", "Rust", "L"),))
    sc.write_sample_manifest(picked, target=1, seed=5, path=sc.SAMPLE_OUT)
    body = sc.SAMPLE_OUT.read_text()
    assert f"\t{sc.LANG_FILTER['Rust']}\t" in body
    assert "target 1, seed 5" in body


def test_sample_doc_reports_the_frame_and_the_draw(sandbox):
    sizes = {("community", "C", "S"): 10, ("foundation", "Rust", "L"): 4}
    take = {("community", "C", "S"): 5, ("foundation", "Rust", "L"): 2}
    sc.write_sample_doc(sizes, take, target=7, seed=11, frame_n=14,
                        path=sc.SAMPLE_DOC)
    body = sc.SAMPLE_DOC.read_text()
    assert "Seed: **11**" in body
    assert "| community | C | S | 10 | 5 | 50% |" in body
    assert "**14**" in body and "**7**" in body


# ---- cmd_sample: the refusals, and the happy path


def test_cmd_sample_refuses_without_candidates(sandbox, capsys):
    assert sc.cmd_sample(sample_args()) == 1
    assert "run `emit` first" in capsys.readouterr().out


def test_cmd_sample_refuses_a_frame_with_no_eligible_rows(sandbox, capsys):
    review_csv([review_row(included="False", excluded_because="archived")])
    assert sc.cmd_sample(sample_args()) == 1
    assert "no eligible rows" in capsys.readouterr().out


def test_cmd_sample_refuses_a_target_below_one(sandbox, capsys):
    review_csv(frame_rows(3))
    assert sc.cmd_sample(sample_args(target=0)) == 1
    assert "--target must be 1 or greater" in capsys.readouterr().out


def test_cmd_sample_writes_both_artefacts(sandbox, capsys):
    review_csv(frame_rows(10, cells=(("community", "C", "S"),
                                     ("company-owned", "Java", "M"),
                                     ("foundation", "Rust", "L"))))
    assert sc.cmd_sample(sample_args(target=9)) == 0
    assert len([l for l in sc.SAMPLE_OUT.read_text().splitlines()
                if not l.startswith("#")]) == 9
    assert sc.SAMPLE_DOC.exists()
    out = capsys.readouterr().out
    assert "drew 9 of 30 eligible" in out
    for dim in ("stratum", "size_class", "language"):
        assert f"by {dim}:" in out


def test_cmd_sample_reports_cells_it_had_to_leave_empty(sandbox, capsys):
    """A target smaller than the cell count cannot reach every cell. Say so,
    rather than letting a stratum vanish silently."""
    review_csv(frame_rows(4, cells=(("community", "C", "S"),
                                    ("company-owned", "Java", "M"),
                                    ("foundation", "Rust", "L"))))
    assert sc.cmd_sample(sample_args(target=1, floor=0)) == 0
    assert "cells left empty" in capsys.readouterr().out


def test_allocate_never_overfills_any_cell_across_many_shapes():
    """Property check, standing in for the capacity guard that allocate does not
    need. The docstring there argues a cell with a fractional share always has
    room; this exercises the claim over many shapes rather than trusting it.

    It also protects the invariant random.sample depends on: asking a cell for
    more rows than it holds raises ValueError.
    """
    rng = random.Random(20261110)
    for _ in range(400):
        sizes = {(f"c{i}",): rng.randint(1, 40)
                 for i in range(rng.randint(1, 12))}
        target = rng.randint(1, sum(sizes.values()) + 20)
        floor = rng.randint(-1, 4)
        take = sc.allocate(sizes, target, floor)
        assert set(take) == set(sizes)
        for k, n in sizes.items():
            assert 0 <= take[k] <= n, (sizes, target, floor, take)
        assert sum(take.values()) <= sum(sizes.values())


def test_allocate_treats_a_negative_floor_as_zero():
    """--floor is a CLI integer. A negative one would make take[] negative and
    then random.sample would raise, far from the cause."""
    assert sc.allocate({("a",): 5}, target=3, floor=-2) == {("a",): 3}
