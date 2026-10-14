"""normalize.py RUN_DIR: print a sha256 line for every gate output. Files under out-*/ hash as they are;
text records (ledger, metrics, runs.log, stdout, reports, ctp.duckdb tables) hash after the time, pid,
duration and host-load fields are masked. The masked text goes to RUN_DIR/norm/ for diffing."""
import hashlib
import json
import re
import sqlite3
import subprocess
import sys
from pathlib import Path

import duckdb

RUN = Path(sys.argv[1])
NORM = RUN / "norm"
DROP = {"run_id", "start_utc", "end_utc", "duration_s", "ctp_commit", "elapsed_s",
        "bytes_freed", "removed_bytes"}
TS = [
    (re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?(\+00:00|Z)?"), "<ISO>"),
    (re.compile(r"\d{8}T\d{6}Z?(-\d+)?"), "<TS>"),
    (re.compile(r"^\[\d\d:\d\d:\d\d\] ", re.M), "[<HMS>] "),
]
# Disk free, memory, elapsed times, freed bytes: host and time dependent in report text.
LOOSE = [
    (re.compile(r"\b\d+(\.\d+)? ?(B|KB|MB|GB|TB|KiB|MiB|GiB|TiB)\b"), "<SIZE>"),
    (re.compile(r"\b\d+(\.\d+)?s\b"), "<DUR>"),
    (re.compile(r"\b\d+h\d\dm\b|\b\d+m\d\ds\b|\b\d\d:\d\d:\d\d\b"), "<DUR>"),
    (re.compile(r"\b\d+ procs\b"), "<N> procs"),
    (re.compile(r"\b\d+(\.\d+)?% cpu\b"), "<P>% cpu"),
    (re.compile(r"\b\d+G (disk|free)\b"), r"<G> \1"),
    (re.compile(r"\b\d+\.\d+\b"), "<F>"),
]
SQL_TIME = [
    (re.compile(r"'\d{4}-\d\d-\d\d \d\d:\d\d:\d\d'"), "'<DT>'"),
    (re.compile(r"\b1[789]\d{8}\b"), "<EPOCH>"),
]


def mask(text: str, loose: bool) -> str:
    for rx, rep in TS + (LOOSE if loose else []):
        text = rx.sub(rep, text)
    return text


def strip(obj):
    if isinstance(obj, dict):
        return {k: strip(v) for k, v in obj.items() if k not in DROP}
    if isinstance(obj, list):
        return [strip(v) for v in obj]
    if isinstance(obj, str):
        return mask(obj, False)
    return obj


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def emit(name: str, text: str) -> None:
    out = NORM / name
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text)
    print(f"{sha(text.encode())}  norm/{name}")


def in_git_dir(rel: Path) -> bool:
    return any(part == ".git" or part.endswith(".git") for part in rel.parts[:-1])


def git_digest(repo: Path) -> str:
    """Refs, every reachable object id and, for a work tree, the index entries."""
    def out(*cmd):
        return subprocess.run(["git", "-C", str(repo), *cmd], capture_output=True, text=True).stdout
    parts = [out("for-each-ref", "--format=%(refname) %(objectname)"),
             "".join(sorted(out("rev-list", "--all", "--objects").splitlines(keepends=True)))]
    if (repo / ".git").exists():
        parts += [out("ls-files", "-s"), out("status", "--porcelain")]
    return sha("".join(parts).encode())


def sqlite_digest(db: Path) -> str:
    con = sqlite3.connect(f"file:{db}?immutable=1", uri=True)
    text = "\n".join(sorted(con.iterdump()))
    con.close()
    for rx, rep in SQL_TIME:
        text = rx.sub(rep, text)
    return sha(text.encode())


def main() -> None:
    repos = set()
    for f in sorted(RUN.glob("out-*/**/*")):
        rel = f.relative_to(RUN)
        if f.is_dir() and (f.name.endswith(".git") or (f / ".git").is_dir()):
            repos.add(f)
        if not f.is_file() or f.is_symlink() or in_git_dir(rel):
            continue
        if f.suffix == ".db":
            print(f"{sqlite_digest(f)}  sqlite:{rel}")
        elif f.name == "pipeline.log":
            print(f"{sha(re.sub(r'[0-9]+', 'N', f.read_text()).encode())}  digits-masked:{rel}")
        else:
            print(f"{sha(f.read_bytes())}  {rel}  {f.stat().st_size}")
    for repo in sorted(repos):
        print(f"{git_digest(repo)}  git:{repo.relative_to(RUN)}")
    for stage in ("census", "run"):
        c = RUN / f"ctp-{stage}"
        led = c / "ledger.jsonl"
        if led.exists():
            rows = [strip(json.loads(line)) for line in led.read_text().splitlines()]
            emit(f"{stage}/ledger.jsonl", "".join(json.dumps(r, sort_keys=True) + "\n" for r in rows))
        met = c / "metrics.tsv"
        if met.exists():
            lines = [l.split("\t") for l in met.read_text().splitlines()]
            emit(f"{stage}/metrics.tsv", "".join(
                "\t".join([f[1], f[2], f[3], f[5], mask(f[6], False)]) + "\n" for f in lines))
        runs = c / "runs.log"
        if runs.exists():
            emit(f"{stage}/runs.log", mask(re.sub(r"duration_s=\d+", "duration_s=<D>", runs.read_text()), False))
        for sf in sorted((c / "state").glob("*/*.json")):
            emit(f"{stage}/state/{sf.parent.name}/{sf.name}", mask(sf.read_text(), False))
        db = c / "ctp.duckdb"
        if db.exists():
            con = duckdb.connect(str(db), read_only=True)
            parts = []
            for (t, kind) in con.execute("select table_name, table_type from information_schema.tables order by 1").fetchall():
                cols = con.execute(f'describe "{t}"').fetchall()
                parts.append(f"## {t} {kind} " + ", ".join(f"{c[0]}:{c[1]}" for c in cols))
                keep = [c[0] for c in cols if not re.search(r"(time|_utc|iso|duration|started|finished|_at$)", c[0])]
                sel = ", ".join(f'"{k}"' for k in keep)
                q = f'select {sel} from "{t}" order by all' if t != "tokens" else \
                    f'select count(*), sum(hash(columns(*))) from (select {sel} from "{t}")'
                for row in con.execute(q).fetchall():
                    parts.append(mask("\t".join(map(str, row)), False))
            emit(f"{stage}/ctp.duckdb.txt", "\n".join(parts) + "\n")
    for name in ("help.txt", "census.stdout", "census.stderr", "census.rc", "census-reports.txt",
                 "run.stdout", "run.stderr", "run.rc", "run-reports.txt"):
        f = RUN / name
        if f.exists():
            text = f.read_text()
            text = "\n".join(l for l in text.splitlines() if "heartbeat" not in l and "♥" not in l) + "\n"
            emit(name, mask(text, True))


main()
