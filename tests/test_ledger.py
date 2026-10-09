"""Tests for ledger.py: whole rows, append-only, and the per-project history."""

from __future__ import annotations

import json
import threading

import pytest

import ledger


def final(name, state, **extra):
    return {"project": name, "step": ledger.FINAL_STEP, "state": state, **extra}


def test_append_writes_one_json_row_per_line(tmp_path):
    path = tmp_path / "sub" / "ledger.jsonl"
    ledger.append(path, {"project": "a", "step": "clone"})
    ledger.append(path, {"project": "a", "step": "pipeline", "note": "ünïcode"})
    lines = path.read_text(encoding="utf-8").splitlines()
    assert [json.loads(l)["step"] for l in lines] == ["clone", "pipeline"]
    assert "ünïcode" in lines[1]


def test_append_never_rewrites_what_is_there(tmp_path):
    path = tmp_path / "ledger.jsonl"
    ledger.append(path, {"n": 1})
    before, inode = path.read_bytes(), path.stat().st_ino
    ledger.append(path, {"n": 2})
    assert path.read_bytes().startswith(before) and path.stat().st_ino == inode


def test_concurrent_appends_never_interleave_a_row(tmp_path):
    path = tmp_path / "ledger.jsonl"
    big = "x" * 20000

    def worker(i):
        for j in range(20):
            ledger.append(path, {"worker": i, "j": j, "pad": big})

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    rows = ledger.read(path)
    assert len(rows) == 160 and all(r["pad"] == big for r in rows)


def test_read_of_a_missing_ledger_is_empty(tmp_path):
    assert ledger.read(tmp_path / "absent.jsonl") == []


def test_read_skips_a_torn_last_line(tmp_path, capsys):
    path = tmp_path / "ledger.jsonl"
    path.write_text('{"project": "a", "step": "clone"}\n\n{"project": "a", "st')
    assert ledger.read(path) == [{"project": "a", "step": "clone"}]
    assert ":3 is not a whole JSON row" in capsys.readouterr().err


def test_histories_take_the_last_final_state_and_count_failures():
    rows = [final("a", "failed", failed_step="clone"), {"project": "a", "step": "clone"},
            final("a", "failed", failed_step="pipeline"), final("a", "done"),
            final("b", "deferred"), {"step": "run-start"}]
    h = ledger.histories(rows)
    assert h["a"].state == "done" and h["a"].failures == 2 and h["a"].attempts == 3
    assert h["a"].last_failed_step == "pipeline" and len(h["a"].rows) == 4
    # deferred and skipped are not attempts: they say nothing about the project.
    assert h["b"].state == "" and h["b"].attempts == 0


def test_an_attempt_that_died_mid_step_counts_as_interrupted():
    rows = [{"project": "a", "run_id": "r1", "attempt": 1, "step": "clone", "status": "started"},
            {"project": "a", "run_id": "r1", "attempt": 1, "step": "clone", "status": "ok"},
            {"project": "a", "run_id": "r1", "attempt": 1, "step": "pipeline",
             "status": "started"},
            {"project": "a", "run_id": "r2", "attempt": 1, "step": "pipeline",
             "status": "started"},
            {"project": "a", "run_id": "r2", "attempt": 1, "step": "project", "state": "done"}]
    h = ledger.histories(rows)["a"]
    assert h.interrupted == 1 and h.attempts == 2 and h.failures == 0
    assert h.last_failed_step == "pipeline (interrupted)" and h.state == "done"


def test_project_rows_filters_by_name():
    rows = [final("a", "done"), final("b", "done"), {"project": "a", "step": "x"}]
    assert ledger.project_rows(rows, "a") == [rows[0], rows[2]]


@pytest.mark.parametrize("state", ledger.STATES)
def test_every_state_is_a_plain_string(state):
    assert state == state.strip().lower()
