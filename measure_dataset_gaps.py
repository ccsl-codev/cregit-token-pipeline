#!/usr/bin/env python3
"""Count mask-selected files present at HEAD that have no row in the Parquet.

This reproduces the number `docs/LIMITATIONS.md` publishes for the parser-crash
class. Run it after any corpus run, and update that entry if the count moves.

    ./measure_dataset_gaps.py                      # the frozen corpus, 187 rows
    ./measure_dataset_gaps.py --manifest manifest.tsv
    ./measure_dataset_gaps.py --json gaps.json     # keep the per-path detail

Two things make this measurement wrong if you skip them, and both cost real
accuracy rather than a rounding error.

**Read the git mode.** `git ls-tree -r --name-only` cannot tell a source file
from a symlink or a submodule gitlink. A symlink whose name matches the mask is
not source and is never tokenized. `powerdns__pdns` carries 331 masked symlinks
in `pdns/dnsdistdist/`; counting them reports its complete Parquet as 29%
incomplete. Corpus-wide the naive count is 390 and the true count is 36.

**This is a HEAD measurement.** A historical revision that crashed the parser is
invisible here, because the comparison is against HEAD. The blob denylist covers
197 historical blobs of the 36 HEAD paths, so history is worse than HEAD by at
least that much. Do not quote a HEAD number as a corpus-wide loss.

It does not attribute a cause. A missing path can be a parser crash, an
empty-but-successful tokenization, a denylisted blob, or a blob over JGit's
50 MiB stream-file threshold. Cross-reference the denylist to separate them.
"""

from __future__ import annotations

import argparse
import configparser
import json
import re
import subprocess
import sys
from pathlib import Path

from file_mask import UNIVERSAL_MASK

REPO = Path(__file__).resolve().parent

# `git ls-tree` modes that are real files. 120000 is a symlink and 160000 is a
# submodule gitlink; neither is source, and neither is ever tokenized.
REGULAR_MODES = frozenset({"100644", "100755"})


def cfg_path(key: str, default: str) -> Path:
    cfg = configparser.ConfigParser()
    cfg.read(REPO / "pipeline.cfg")
    raw = cfg.get("paths", key, fallback=default)
    p = Path(raw).expanduser()
    if not p.is_absolute():
        p = REPO / p
    return p.resolve()


def manifest_rows(manifest: Path):
    """Yield (name, file_filter) for every non-comment manifest row."""
    with manifest.open() as fh:
        for line in fh:
            if line.startswith("#") or not line.strip():
                continue
            fields = line.rstrip("\n").split("\t")
            if len(fields) >= 4:
                yield fields[0], fields[3]


def head_entries(git_dir: Path) -> tuple[list[str], list[str]]:
    """Return (regular-file paths at HEAD, non-regular paths at HEAD)."""
    proc = subprocess.run(
        ["git", f"--git-dir={git_dir}", "ls-tree", "-r", "HEAD"],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return [], []
    regular: list[str] = []
    other: list[str] = []
    for line in proc.stdout.splitlines():
        meta, _, path = line.partition("\t")
        if not path:
            continue
        (regular if meta.split()[0] in REGULAR_MODES else other).append(path)
    return regular, other


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--manifest", default="manifest.tsv")
    ap.add_argument("--out-dir", default=None, help="overrides pipeline.cfg output_dir")
    ap.add_argument("--json", default=None, help="write per-path detail here")
    ap.add_argument(
        "--mask",
        default=None,
        help="override every manifest file_filter with this regex",
    )
    args = ap.parse_args(argv)

    try:
        import duckdb
    except ImportError:
        print(
            "ERROR: duckdb is required. Run inside the cregit devenv shell, or:\n"
            "  uv pip install duckdb",
            file=sys.stderr,
        )
        return 2

    manifest = Path(args.manifest)
    if not manifest.is_absolute():
        manifest = REPO / manifest
    out_dir = Path(args.out_dir) if args.out_dir else cfg_path(
        "output_dir", "../cregit-workspace/corpus-files"
    )

    con = duckdb.connect()
    con.execute("set memory_limit='2GB'")
    con.execute("set threads=2")

    results = []
    for slug, file_filter in manifest_rows(manifest):
        parquet = out_dir / slug / f"{slug}-dataset.parquet"
        if not parquet.exists():
            results.append({"slug": slug, "status": "no-parquet"})
            print(f"{slug}\tNO-PARQUET", flush=True)
            continue

        mask = args.mask or file_filter or UNIVERSAL_MASK
        rx = re.compile(mask)
        regular, other = head_entries(out_dir / slug / f"{slug}-original.git")
        masked = [p for p in regular if rx.search(p)]
        masked_other = [p for p in other if rx.search(p)]
        if not masked:
            results.append({"slug": slug, "status": "no-masked-head-paths"})
            print(f"{slug}\tNO-MASKED-HEAD-PATHS", flush=True)
            continue

        try:
            have = {
                row[0]
                for row in con.execute(
                    "select distinct file_path from read_parquet(?)", [str(parquet)]
                ).fetchall()
            }
        except Exception as exc:  # a read failure must be loud, never a zero
            results.append({"slug": slug, "status": f"parquet-error: {exc}"})
            print(f"{slug}\tPARQUET-ERROR\t{exc}", flush=True)
            continue

        missing = sorted(set(masked) - have)
        results.append(
            {
                "slug": slug,
                "status": "ok",
                "head_masked": len(masked),
                "head_masked_nonregular": len(masked_other),
                "parquet_paths": len(have),
                "missing": len(missing),
                "missing_paths": missing,
            }
        )
        print(
            f"{slug}\t{len(masked)}\t{len(masked_other)}\t{len(have)}\t{len(missing)}",
            flush=True,
        )

    if args.json:
        Path(args.json).write_text(json.dumps(results, indent=1))

    ok = [r for r in results if r["status"] == "ok"]
    head = sum(r["head_masked"] for r in ok)
    nonreg = sum(r["head_masked_nonregular"] for r in ok)
    missing = sum(r["missing"] for r in ok)
    print("", flush=True)
    print(f"projects measured             : {len(ok)}")
    print(f"projects with >= 1 missing    : {sum(1 for r in ok if r['missing'])}")
    print(f"mask-selected regular files   : {head}")
    print(f"mask-selected non-regular     : {nonreg}  (symlink/gitlink, excluded)")
    print(f"missing from the Parquet      : {missing}")
    if head:
        print(f"missing share                 : {100 * missing / head:.3f}%")
    return 0


if __name__ == "__main__":
    sys.exit(main())
