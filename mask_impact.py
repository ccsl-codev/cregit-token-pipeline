#!/usr/bin/env python3
"""Re-derive, from the data, which projects the universal mask changes.

Writes data/mask-impact.csv. There was no machine-readable list of the re-run
set — only a prose count in mask-decision.md — and a prose count cannot be run
and cannot be audited. This script can be re-run at any time; it reads only
committed inputs plus each project's own blob map and bare clone.

Two populations, and the distinction is subtler than "the mask selects more":

  gainer     the universal mask selects files at HEAD that the project's
             RECORDED mask did not. Its tokenized repository is missing those
             files, so it must be re-tokenized from step 2.
  unchanged  the universal mask selects exactly the same HEAD paths. The
             Parquet is built from blame/*.blame, i.e. from the HEAD tree, so
             its token rows are already complete and a step-10 regeneration is
             enough.

HEAD is the right horizon for the Parquet and the wrong one for the tokenizer,
so both are reported. `hist_new_paths` counts blob_map IDENTITY rows (a path the
recorded mask did not select, passed through as raw bytes) whose path the
universal mask DOES select. A project can be unchanged at HEAD and still have
thousands of those, because the files existed earlier in history and were
deleted or renamed before HEAD. graknlabs__grakn is the extreme case: a Java
project rewritten in Rust, so HEAD is pure .rs and the .java is all history.
Those rows never reach the Parquet, but they are the reason the identity-row
purge in CREGIT dada585 is load-bearing, and they are not a Parquet defect.

The old mask comes from each blob map's own meta table — what actually ran —
never from a manifest or a document, both of which have already been rewritten
to the universal mask.
"""
from __future__ import annotations

import csv
import json
import re
import sqlite3
import subprocess
import sys
from collections import Counter
from pathlib import Path

from file_mask import UNIVERSAL_MASK

HERE = Path(__file__).resolve().parent


def read_cfg(cfg: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in cfg.read_text().splitlines():
        line = line.strip()
        if line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def selected(ls_tree: str, pattern: re.Pattern[str]) -> tuple[set[str], int]:
    """Paths and bytes the pattern selects, matched against the BASENAME the way
    Walker does (`fileMask.findFirstIn(tw.getNameString)`)."""
    paths: set[str] = set()
    total = 0
    for line in ls_tree.splitlines():
        head, _, path = line.partition("\t")
        f = head.split()
        if len(f) < 4 or f[1] != "blob":
            continue
        if pattern.search(path.rsplit("/", 1)[-1]):
            paths.add(path)
            total += int(f[3]) if f[3] != "-" else 0
    return paths, total


def row_for(slug: str, manifest_row: dict, out_dir: Path, meta: dict) -> dict:
    wd = out_dir / slug
    blobmap = wd / f"{slug}-blobmap.db"
    bare = wd / f"{slug}-original.git"
    if not blobmap.is_file():
        raise SystemExit(f"{slug}: no blob map at {blobmap}")
    if not bare.is_dir():
        raise SystemExit(f"{slug}: no bare clone at {bare}")

    con = sqlite3.connect(f"file:{blobmap}?mode=ro", uri=True)
    old_mask = dict(con.execute("select * from meta").fetchall()).get("mask", "")
    new_re = re.compile(UNIVERSAL_MASK)
    tokenized = identity = hist_new = 0
    narrowed = ""
    for orig, new, path in con.execute(
            "select orig_blob, new_blob, path from blob_map"):
        picked = new_re.search(path.rsplit("/", 1)[-1]) is not None
        if orig == new:
            identity += 1
            hist_new += picked
        else:
            tokenized += 1
            if not picked and not narrowed:
                narrowed = path
    con.close()

    ls = subprocess.run(["git", "--git-dir", str(bare), "ls-tree", "-r", "-l",
                         "HEAD"], capture_output=True, text=True,
                        check=True).stdout
    old_paths, old_bytes = selected(ls, re.compile(old_mask))
    new_paths, new_bytes = selected(ls, new_re)

    if new_paths == old_paths:
        population = "unchanged"
    elif old_paths < new_paths:
        population = "gainer"
    else:
        # Would break the whole plan: the universal mask is supposed to be a
        # strict superset of every recorded mask.
        population = "NOT-A-SUPERSET"

    return {
        "slug": slug,
        "stratum": meta.get(slug, {}).get("stratum", ""),
        "size_class": manifest_row["size_class"],
        "old_mask": old_mask,
        "old_files": len(old_paths),
        "new_files": len(new_paths),
        "delta_files": len(new_paths) - len(old_paths),
        "old_bytes": old_bytes,
        "new_bytes": new_bytes,
        "delta_bytes": new_bytes - old_bytes,
        "head_paths_are_a_superset": int(old_paths <= new_paths),
        "blob_map_tokenized": tokenized,
        "blob_map_identity": identity,
        "hist_new_paths": hist_new,
        "narrowed_path": narrowed,
        "population": population,
    }


COLUMNS = ("slug", "stratum", "size_class", "old_mask", "old_files",
           "new_files", "delta_files", "old_bytes", "new_bytes", "delta_bytes",
           "head_paths_are_a_superset", "blob_map_tokenized",
           "blob_map_identity", "hist_new_paths", "narrowed_path",
           "population")


def main(argv: list[str]) -> int:
    manifests = argv[1:] or ["manifest.phase1-sm.tsv", "manifest.linux.tsv"]
    cfg = read_cfg(HERE / "pipeline.cfg")
    out_dir = Path(cfg["output_dir"])
    meta = json.loads((HERE / "project_meta.json").read_text())

    seen: dict[str, dict] = {}
    for name in manifests:
        for line in (HERE / name).read_text().splitlines():
            if line.startswith("#") or not line.strip():
                continue
            f = line.split("\t")
            seen.setdefault(f[0], {"name": f[0], "url": f[1],
                                   "size_class": f[4]})

    rows = [row_for(slug, mr, out_dir, meta) for slug, mr in sorted(seen.items())]

    dest = HERE / "data" / "mask-impact.csv"
    with dest.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(COLUMNS))
        w.writeheader()
        w.writerows(rows)

    pop = Counter(r["population"] for r in rows)
    print(f"{dest.relative_to(HERE)}: {len(rows)} rows")
    for k, v in sorted(pop.items()):
        print(f"  {k:14s} {v}")
    print("  gainers by size_class:",
          dict(sorted(Counter(r["size_class"] for r in rows
                              if r["population"] == "gainer").items())))
    print("  unchanged at HEAD but gaining historically:",
          sum(1 for r in rows if r["population"] == "unchanged"
              and r["hist_new_paths"]))
    print("  recorded masks:",
          dict(Counter(r["old_mask"] for r in rows).most_common()))
    bad = [r["slug"] for r in rows if not r["head_paths_are_a_superset"]]
    if bad:
        print("  NOT A SUPERSET:", bad)
        return 1
    narrowed = [r["slug"] for r in rows if r["narrowed_path"]]
    if narrowed:
        print("  would be refused as a narrowing by blobExec:", narrowed)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
