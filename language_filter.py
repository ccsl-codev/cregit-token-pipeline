#!/usr/bin/env python3
"""Language filter: keep the manifest rows whose pinned commit is written in the target languages.

    ./language_filter.py --manifest IN.tsv --out-dir DIR
    ./language_filter.py --manifest IN.tsv --out-dir DIR --mode share --threshold 0.30

For each row, fetch the files of the pinned commit alone (depth 1, never the
history) into a temporary folder and delete its .git. Remove the vendored
directories and the Qt translation files, count the code lines per language with
tokei, then delete the folder. Count only the programming languages of the table
that build_programming_table.py builds. Apply the rule and write three files to
DIR:

  languages.tsv  one row per manifest row: the counts, the rule inputs, the status
  kept.tsv       the manifest rows that the rule keeps, copied verbatim
  dropped.tsv    the rows that it drops, each with the reason

The input is a ctp manifest (docs/DESIGN.md §2) with a sixth field, the pinned
commit as 40 lowercase hex digits. A row with five fields is reported as not
measured and dropped.

Exit status: 0 every row was measured, 1 at least one row was not measured (the
three files are still written), 2 wrong invocation or no usable tokei.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

from file_mask import TOKENIZABLE_LANGUAGES

# The languages that the cregit tokenizer handles. tokei uses the same names.
TARGET_LANGUAGES: tuple[str, ...] = TOKENIZABLE_LANGUAGES

# tokei counts headers as their own languages. cregit tokenizes .h with the C
# grammar and .hpp with the C++ grammar, so a header counts toward its language.
HEADER_MERGE: dict[str, str] = {"C Header": "C", "C++ Header": "C++"}

# For each tokei 14 language, whether it is a programming language: Linguist's
# `type: programming`, or a hand decision. build_programming_table.py builds it.
# A language that the table does not list is not counted.
PROGRAMMING_TABLE = Path(__file__).resolve().parent / "data" / "tokei14_programming.tsv"

# A directory with one of these names, at any depth, is removed before the count.
# Compared without case. A symbolic link with such a name is kept.
VENDORED_DIRS: tuple[str, ...] = (
    "3rdparty", "deps", "external", "third-party", "third_party", "thirdparty",
    "vendor", "vendored",
)

# Run in an empty repository made with `git init`. {url} and {commit} are
# replaced in each argument; the command does not go through a shell.
FETCH_CMD = "git fetch --quiet --depth 1 --no-tags {url} {commit}"

TOKEI_MAJOR = 14
MODES = ("largest", "share")
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
MANIFEST_FIELDS = 5

# A Qt Linguist translation file: after an optional UTF-8 byte order mark and
# white space, the file starts with one of these. tokei counts every .ts file as
# TypeScript.
QT_TS_PREFIXES = (b"<?xml", b"<!DOCTYPE TS", b"<TS")
QT_TS_HEAD_BYTES = 8192
UTF8_BOM = b"\xef\xbb\xbf"

MEASURED = "measured"
NO_COMMIT = "not measured: no pinned commit"
FETCH_REFUSED = "not measured: fetch by commit refused"
FETCH_FAILED = "not measured: fetch failed"
TIMED_OUT = "not measured: timed out"
CHECKOUT_FAILED = "not measured: checkout failed"
TOKEI_FAILED = "not measured: tokei failed"

# What git prints when the host will not send a commit that no ref names. A
# commit that the host does not have reads the same: "not our ref". git runs with
# LC_ALL=C, so these are its English messages.
REFUSED_MARKERS = ("not our ref", "unadvertised object", "allowReachableSHA1InWant",
                   "allowAnySHA1InWant")

# Variables that would point git at another repository than the one it runs in.
GIT_REDIRECTS = ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_OBJECT_DIRECTORY",
                 "GIT_ALTERNATE_OBJECT_DIRECTORIES", "GIT_COMMON_DIR", "GIT_NAMESPACE")


@dataclass
class Row:
    name: str
    url: str
    commit: str
    line: str


@dataclass
class Measure:
    status: str
    counts: dict[str, int] = field(default_factory=dict)
    qt_ts_removed: int = 0
    vendored_dirs_removed: int = 0
    vendored_files_removed: int = 0
    detail: str = ""


@dataclass
class Counted:
    code: dict[str, int]
    nonprogramming: int
    unknown: list[str]


@dataclass
class Decision:
    kept: bool
    largest: str
    share: float | None
    reason: str


@dataclass
class Options:
    fetch_cmd: list[str]
    work_dir: Path | None
    tokei: str
    vendored: frozenset[str]
    timeout: int


def read_manifest(path: Path) -> list[Row]:
    rows = []
    for lineno, line in enumerate(path.read_text().splitlines(), 1):
        if not line.strip() or line.startswith("#"):
            continue
        fields = line.split("\t")
        if len(fields) not in (MANIFEST_FIELDS, MANIFEST_FIELDS + 1):
            raise ValueError(f"{path}:{lineno}: expected {MANIFEST_FIELDS} tab-separated fields,"
                             f" or {MANIFEST_FIELDS + 1} with a pinned commit, got {len(fields)}")
        commit = fields[MANIFEST_FIELDS] if len(fields) > MANIFEST_FIELDS else ""
        if commit and not SHA_RE.match(commit):
            raise ValueError(f"{path}:{lineno}: commit {commit!r} is not 40 lowercase hex digits")
        rows.append(Row(name=fields[0], url=fields[1], commit=commit, line=line))
    return rows


def read_programming_table(path: Path) -> dict[str, bool]:
    """tokei language -> True when it is a programming language."""
    with path.open(newline="") as fh:
        reader = csv.DictReader((x for x in fh if not x.startswith("#")), delimiter="\t")
        table = {r["tokei_language"]: r["programming"] == "yes" for r in reader}
    if not table:
        raise ValueError(f"{path}: no rows")
    return table


def git_env() -> dict[str, str]:
    """The environment, without the machine's git configuration or a redirect."""
    env = {k: v for k, v in os.environ.items() if k not in GIT_REDIRECTS}
    env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull, GIT_TERMINAL_PROMPT="0",
               GIT_LFS_SKIP_SMUDGE="1", LC_ALL="C")
    return env


def classify_fetch_error(output: str) -> str:
    return FETCH_REFUSED if any(m in output for m in REFUSED_MARKERS) else FETCH_FAILED


def first_line(text: str) -> str:
    for line in text.splitlines():
        if line.strip():
            return line.strip()
    return ""


def fetch_commit(url: str, commit: str, dest: Path, opts: Options) -> tuple[str, str]:
    """Check out `commit` alone into `dest`, then delete dest/.git. Returns (status, detail)."""
    env = git_env()
    steps = [
        (["git", "init", "--quiet", str(dest)], CHECKOUT_FAILED),
        ([a.replace("{url}", url).replace("{commit}", commit) for a in opts.fetch_cmd], None),
        (["git", "-c", "advice.detachedHead=false", "-c", "core.symlinks=true", "checkout",
          "--quiet", "--detach", commit], CHECKOUT_FAILED),
    ]
    for cmd, failure in steps:
        try:
            p = subprocess.run(cmd, cwd=dest, env=env, capture_output=True, text=True,
                               timeout=opts.timeout)
        except subprocess.TimeoutExpired:
            return TIMED_OUT, f"{cmd[0]} ran longer than {opts.timeout} s"
        if p.returncode != 0:
            out = p.stderr + p.stdout
            return failure or classify_fetch_error(out), first_line(out)
    shutil.rmtree(dest / ".git")
    return MEASURED, ""


def remove_vendored(root: Path, names: frozenset[str]) -> tuple[int, int]:
    """Delete every directory under `root` whose name is in `names`, but not a symbolic link.

    Returns how many directories, and how many files they held.
    """
    dirs_removed = files_removed = 0
    for top, dirs, _ in os.walk(root):
        keep = []
        for d in dirs:
            path = os.path.join(top, d)
            if d.lower() in names and not os.path.islink(path):
                files_removed += sum(len(fs) for _, _, fs in os.walk(path))
                shutil.rmtree(path, ignore_errors=True)
                dirs_removed += 1
            else:
                keep.append(d)
        dirs[:] = keep
    return dirs_removed, files_removed


def is_qt_ts(head: bytes) -> bool:
    """True when `head`, the first bytes of a .ts file, starts a Qt translation file."""
    if head.startswith(UTF8_BOM):
        head = head[len(UTF8_BOM):]
    return head.lstrip().startswith(QT_TS_PREFIXES)


def remove_qt_ts(root: Path) -> int:
    """Delete every .ts file under `root` that is a Qt translation file. Returns how many."""
    removed = 0
    for top, _, files in os.walk(root):
        for f in files:
            path = os.path.join(top, f)
            if not f.lower().endswith(".ts") or os.path.islink(path) or not os.path.isfile(path):
                continue
            with open(path, "rb") as fh:
                head = fh.read(QT_TS_HEAD_BYTES)
            if is_qt_ts(head):
                os.unlink(path)
                removed += 1
    return removed


def _code(stats: dict) -> int:
    return stats["code"] + sum(_code(b) for b in stats.get("blobs", {}).values())


def language_counts(report: dict) -> dict[str, int]:
    """Code lines per tokei language from `tokei -o json`, headers merged.

    A language's lines are its own `code` plus the code embedded in its files
    (its `children`: a Rust block in Markdown counts toward Markdown), as in
    tokei's own total.
    """
    counts: dict[str, int] = {}
    for lang, stats in report.items():
        if lang == "Total":
            continue
        n = stats["code"] + sum(_code(rep["stats"]) for reps in stats.get("children", {}).values()
                                for rep in reps)
        if n:
            lang = HEADER_MERGE.get(lang, lang)
            counts[lang] = counts.get(lang, 0) + n
    return counts


def count_programming(counts: dict[str, int], table: dict[str, bool]) -> Counted:
    return Counted(code={k: v for k, v in counts.items() if table.get(k, False)},
                   nonprogramming=sum(v for k, v in counts.items() if not table.get(k, False)),
                   unknown=sorted(k for k in counts if k not in table))


def decide(code: dict[str, int], targets: frozenset[str], mode: str,
           threshold: float | None) -> Decision:
    """Apply the rule to the programming-language code lines of one measured row.

    The largest language has the most code lines; a tie goes to the name that
    sorts first. The target share is the target-language code lines over all the
    programming-language code lines.
    """
    total = sum(code.values())
    if total == 0:
        return Decision(False, "", None, "no code lines in a programming language")
    largest = min(code, key=lambda lang: (-code[lang], lang))
    share = sum(n for lang, n in code.items() if lang in targets) / total
    if mode == "largest":
        kept = largest in targets
        reason = "" if kept else f"largest language is {largest}"
    else:
        kept = share >= threshold
        reason = "" if kept else f"target share {share:.6f} is below {threshold}"
    return Decision(kept, largest, share, reason)


def tokei_version(tokei: str) -> str:
    """The version that `tokei --version` prints, e.g. 14.0.0. Raises OSError if none."""
    try:
        p = subprocess.run([tokei, "--version"], capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.TimeoutExpired) as e:
        raise OSError(f"cannot run {tokei}: {e}") from e
    m = re.match(r"tokei\s+(\S+)", p.stdout.strip())
    if p.returncode != 0 or not m:
        raise OSError(f"{tokei} --version printed {p.stdout.strip()!r}, not a tokei version")
    return m.group(1)


def run_tokei(root: Path, opts: Options) -> dict:
    p = subprocess.run([opts.tokei, "--hidden", "--no-ignore", "--output", "json", str(root)],
                       cwd=root.parent, capture_output=True, text=True, timeout=opts.timeout)
    if p.returncode != 0:
        raise RuntimeError(first_line(p.stderr) or f"exit {p.returncode}")
    return json.loads(p.stdout)


def measure_row(row: Row, opts: Options) -> Measure:
    if not row.commit:
        return Measure(NO_COMMIT)
    if opts.work_dir:
        opts.work_dir.mkdir(parents=True, exist_ok=True)
    tmp = Path(tempfile.mkdtemp(prefix="ctp-lang-", dir=opts.work_dir))
    dest = tmp / "w"
    try:
        dest.mkdir()
        status, detail = fetch_commit(row.url, row.commit, dest, opts)
        if status != MEASURED:
            return Measure(status, detail=detail)
        vendored_dirs, vendored_files = remove_vendored(dest, opts.vendored)
        qt = remove_qt_ts(dest)
        try:
            report = run_tokei(dest, opts)
        except subprocess.TimeoutExpired:
            return Measure(TIMED_OUT, detail=f"tokei ran longer than {opts.timeout} s")
        except (OSError, RuntimeError, ValueError) as e:
            return Measure(TOKEI_FAILED, detail=str(e))
        return Measure(MEASURED, language_counts(report), qt, vendored_dirs, vendored_files)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def cell(value: object) -> str:
    return re.sub(r"[\t\r\n]+", " ", str(value))


def write_tsv(path: Path, header: list[str], rows: list[list[object]]) -> None:
    lines = ["\t".join(header)] + ["\t".join(cell(v) for v in r) for r in rows]
    path.write_text("\n".join(lines) + "\n")


def write_outputs(out_dir: Path, rows: list[Row], measures: list[Measure],
                  counted: list[Counted | None], decisions: list[Decision | None],
                  version: str, rule: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    langs = sorted({lang for c in counted if c for lang in c.code})
    header = ["name", "url", "commit", "status", "kept", "largest_language", "target_share",
              "programming_code", "nonprogramming_code", "unknown_languages", "qt_ts_removed",
              "vendored_dirs_removed", "vendored_files_removed", "tokei_version",
              "detail"] + [f"code:{lang}" for lang in langs]
    table, kept_lines, dropped = [], [], []
    for row, m, c, d in zip(rows, measures, counted, decisions, strict=True):
        share = "" if d is None or d.share is None else f"{d.share:.6f}"
        table.append([row.name, row.url, row.commit, m.status, "yes" if d and d.kept else "no",
                      d.largest if d else "", share,
                      sum(c.code.values()) if c else "", c.nonprogramming if c else "",
                      ",".join(c.unknown) if c else "", m.qt_ts_removed,
                      m.vendored_dirs_removed, m.vendored_files_removed, version, m.detail]
                     + [c.code.get(lang, 0) if c else 0 for lang in langs])
        if d and d.kept:
            kept_lines.append(row.line)
        else:
            reason = d.reason if d else m.status + (f" ({m.detail})" if m.detail else "")
            dropped.append([row.name, row.url, row.commit, reason])
    write_tsv(out_dir / "languages.tsv", header, table)
    write_tsv(out_dir / "dropped.tsv", ["name", "url", "commit", "reason"], dropped)
    comment = f"kept by language_filter.py, {rule}, tokei {version}"
    (out_dir / "kept.tsv").write_text(f"# {comment}\n" + "".join(f"{x}\n" for x in kept_lines))


def comma_list(text: str) -> list[str]:
    return [x.strip() for x in text.split(",") if x.strip()]


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--manifest", required=True, metavar="TSV",
                    help="ctp manifest with a sixth field, the pinned commit")
    ap.add_argument("--out-dir", required=True, metavar="DIR",
                    help="where languages.tsv, kept.tsv and dropped.tsv are written")
    ap.add_argument("--targets", default=",".join(TARGET_LANGUAGES), metavar="LANGS",
                    help="comma-separated tokei language names (default: %(default)s)")
    ap.add_argument("--mode", choices=MODES, default="largest",
                    help="largest: keep a row whose largest language is a target "
                         "(the default). share: keep a row whose target share is at "
                         "least --threshold")
    ap.add_argument("--threshold", type=float, default=None, metavar="X",
                    help="the share mode's threshold, in (0, 1], e.g. 0.30")
    ap.add_argument("--programming-table", default=str(PROGRAMMING_TABLE), metavar="TSV",
                    help="which tokei languages are programming languages "
                         "(default: data/tokei14_programming.tsv)")
    ap.add_argument("--vendored-dirs", default=",".join(VENDORED_DIRS), metavar="NAMES",
                    help="comma-separated directory names removed before the count, at "
                         "any depth (default: %(default)s). Empty removes none")
    ap.add_argument("--fetch-cmd", default=FETCH_CMD, metavar="CMD",
                    help="the fetch command, run in an empty repository; must contain "
                         "{url} and {commit} (default: %(default)s)")
    ap.add_argument("--work-dir", default=None, metavar="DIR",
                    help="parent of the temporary checkout folders (default: the system "
                         "temporary folder)")
    ap.add_argument("--tokei", default="tokei", metavar="PATH", help="the tokei executable")
    ap.add_argument("--timeout", type=int, default=3600, metavar="S",
                    help="limit for each git or tokei command (default: %(default)s)")
    ap.add_argument("--jobs", type=int, default=1, metavar="N", help="rows measured at once")
    args = ap.parse_args(argv)

    if args.mode == "share":
        if args.threshold is None or not 0 < args.threshold <= 1:
            ap.error("--mode share needs --threshold in (0, 1]")
    elif args.threshold is not None:
        ap.error("--threshold applies only to --mode share")
    args.targets = frozenset(comma_list(args.targets))
    if not args.targets:
        ap.error("--targets is empty")
    if bad := sorted(args.targets & set(HEADER_MERGE)):
        ap.error(f"a target is merged into another language before the rule: {', '.join(bad)}")
    args.fetch_cmd = shlex.split(args.fetch_cmd)
    if not any("{url}" in a for a in args.fetch_cmd) or \
            not any("{commit}" in a for a in args.fetch_cmd):
        ap.error("--fetch-cmd must contain {url} and {commit}")
    if args.jobs < 1 or args.timeout < 1:
        ap.error("--jobs and --timeout must be at least 1")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        rows = read_manifest(Path(args.manifest))
        table = read_programming_table(Path(args.programming_table))
        if bad := sorted(t for t in args.targets if not table.get(t, False)):
            raise ValueError(f"a target is not a programming language in "
                             f"{args.programming_table}: {', '.join(bad)}")
        version = tokei_version(args.tokei)
    except (OSError, ValueError) as e:
        print(f"language_filter: {e}", file=sys.stderr)
        return 2
    if not version.startswith(f"{TOKEI_MAJOR}."):
        print(f"language_filter: warning: tokei {version}, not {TOKEI_MAJOR}.x; "
              "its language detection may differ", file=sys.stderr)
    opts = Options(fetch_cmd=args.fetch_cmd,
                   work_dir=Path(args.work_dir).resolve() if args.work_dir else None,
                   tokei=args.tokei,
                   vendored=frozenset(n.lower() for n in comma_list(args.vendored_dirs)),
                   timeout=args.timeout)

    def one(i_row: tuple[int, Row]) -> Measure:
        i, row = i_row
        m = measure_row(row, opts)
        print(f"[{i}/{len(rows)}] {row.name}: {m.status}", file=sys.stderr, flush=True)
        return m

    with ThreadPoolExecutor(max_workers=args.jobs) as pool:
        measures = list(pool.map(one, enumerate(rows, 1)))
    counted = [count_programming(m.counts, table) if m.status == MEASURED else None
               for m in measures]
    decisions = [decide(c.code, args.targets, args.mode, args.threshold) if c else None
                 for c in counted]
    rule = ("mode largest" if args.mode == "largest"
            else f"mode share, threshold {args.threshold}")
    rule += f", targets {','.join(sorted(args.targets))}"
    write_outputs(Path(args.out_dir), rows, measures, counted, decisions, version, rule)

    kept = sum(1 for d in decisions if d and d.kept)
    unmeasured = sum(1 for m in measures if m.status != MEASURED)
    print(f"language_filter: {kept} kept, {len(rows) - kept} dropped "
          f"({unmeasured} not measured), {rule}, tokei {version}", file=sys.stderr)
    return 1 if unmeasured else 0


if __name__ == "__main__":
    sys.exit(main())
