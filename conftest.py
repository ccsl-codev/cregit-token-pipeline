"""Put the repository root on sys.path, so tests/ can `import ctp`. Keep this
file at the root: pytest takes the rootdir from the topmost conftest.py."""

import fcntl
import sys
import types
from contextlib import contextmanager
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    import duckdb  # noqa: F401
except ModuleNotFoundError:  # pragma: no cover - only without duckdb installed
    def _unpatched(*args, **kwargs):
        raise AssertionError("stub duckdb was called: patch duckdb.connect or duckdb.sql")

    _stub = types.ModuleType("duckdb")
    _stub.__doc__ = "Test stub. Install duckdb to run the real-Parquet tests."
    _stub.connect = _unpatched
    _stub.sql = _unpatched
    sys.modules["duckdb"] = _stub


@contextmanager
def _held_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)


@pytest.fixture
def held_lock():
    """`with held_lock(path):` holds an exclusive flock on path for the block."""
    return _held_lock
