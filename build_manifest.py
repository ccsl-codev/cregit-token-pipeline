#!/usr/bin/env python3
"""Build a pinned manifest and its project_meta.json from a selection frame.
Usage: build_manifest.py FRAME.csv --manifest OUT.tsv --project-meta OUT.json
       [--order OUT.order.tsv] [--limit N]

The frame is a CSV file with a header row. This script reads these columns:

  slug           owner/repo on its host, e.g. jqlang/jq. The column may be called
                 name_with_owner instead. It seeds the random number u.
  host           e.g. github.com
  repo_url       the clone URL
  head_oid       the commit to analyse: 40 lowercase hex digits
  u              the permanent random number of the row, a float in [0, 1).
                 It must equal u_of(slug), see below, or the build stops.
  size_class     S, M or L, as in the manifest
  labels         free text. It becomes the manifest category, unless the
                 frame also has a category column.

Optional columns: category, file_filter (blank means the universal mask), and
included. When included is present, only rows where it is 1, true or yes are
kept. Any column named like one of the 29 provenance fields fills that field.

u_of(slug) takes sha256 of "20261110:" + slug, and reads its first 53 bits as
a fraction of 2**53. So u is exact in a float, and always below 1.

The manifest lists the rows in u order, lowest first, so any finished prefix
of a run is a random sample of the frame. Its sixth column pins each project to
head_oid. The order file maps each queue position to slug, u and head_oid."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from file_mask import UNIVERSAL_MASK

SALT = "20261110:"
U_BITS = 53
U_TOLERANCE = 1e-12
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
NAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")
SIZE_CLASSES = ("S", "M", "L")
REQUIRED = ("host", "repo_url", "head_oid", "u", "size_class", "labels")
SLUG_COLUMNS = ("slug", "name_with_owner")
TRUE_WORDS = frozenset({"1", "true", "yes"})
DEFAULT_HOST = "github.com"

# The 29 provenance fields, in order. They must match PROJECT_META_FIELDS in
# cregit's generate_dataset.py; tests/test_build_manifest.py holds them equal
# to validate_schema.EXPECTED_COLUMNS.
META_FIELDS = (
    "clone_url", "provenance_status",
    "source", "stratum", "fact", "contested", "label_date",
    "owner", "repo", "roster_name", "roster_lang",
    "language", "commits", "size_class", "size_kb", "stars", "pushed_at",
    "license", "owner_type", "archived", "fork",
    "history_cluster", "history_shared_with", "history_relation",
    "history_includes", "history_first", "history_created",
    "manifest_category", "file_mask",
)
ORDER_FIELDS = ("queue_pos", "name", "slug", "host", "u", "head_oid", "labels")


class FrameError(ValueError):
    """The frame breaks the input contract. The message names the row."""


def u_of(slug: str) -> float:
    digest = hashlib.sha256((SALT + slug).encode("utf-8")).digest()
    return (int.from_bytes(digest[:8], "big") >> (64 - U_BITS)) / 2**U_BITS


def project_name(host: str, slug: str) -> str:
    """owner__repo for GitHub, host__owner__repo elsewhere. One plain directory name."""
    parts = [p for p in slug.strip("/").split("/") if p]
    if len(parts) < 2:
        raise FrameError(f"slug {slug!r} is not owner/repo")
    if host != DEFAULT_HOST:
        parts.insert(0, host)
    name = "__".join(parts)
    if not NAME_RE.match(name) or ".." in name:
        raise FrameError(f"slug {slug!r} on {host} gives the name {name!r}, which is not "
                         "one plain directory name")
    return name


def plain(value: str, what: str, where: str) -> str:
    """A value that can sit in one TSV field."""
    if any(c in value for c in "\t\r\n"):
        raise FrameError(f"{where}: {what} holds a tab or a newline: {value!r}")
    return value


def read_frame(path: Path) -> tuple[list[dict], int]:
    """(kept rows, rows dropped by `included`). Raises FrameError on a broken row."""
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        columns = set(reader.fieldnames or ())
        slug_col = next((c for c in SLUG_COLUMNS if c in columns), None)
        missing = [c for c in REQUIRED if c not in columns]
        if slug_col is None:
            missing.insert(0, " or ".join(SLUG_COLUMNS))
        if missing:
            raise FrameError(f"{path}: missing column(s): {', '.join(missing)}")
        rows, dropped = [], 0
        for lineno, row in enumerate(reader, 2):
            if "included" in columns and row["included"].strip().lower() not in TRUE_WORDS:
                dropped += 1
                continue
            rows.append(check_row(row, slug_col, f"{path}:{lineno}"))
    return rows, dropped


def check_row(row: dict, slug_col: str, where: str) -> dict:
    slug = row[slug_col].strip()
    host = row["host"].strip().lower() or DEFAULT_HOST
    sha = row["head_oid"].strip()
    if not SHA_RE.match(sha):
        raise FrameError(f"{where}: head_oid {sha!r} is not 40 lowercase hex digits")
    try:
        u = float(row["u"])
    except ValueError:
        raise FrameError(f"{where}: u {row['u']!r} is not a number") from None
    if not 0 <= u < 1:
        raise FrameError(f"{where}: u {u} is outside [0, 1)")
    expected = u_of(slug)
    if abs(u - expected) > U_TOLERANCE:
        raise FrameError(f"{where}: u {u!r} does not match u_of({slug!r}) = {expected!r}. "
                         "The frame used another salt or another formula.")
    size_class = row["size_class"].strip()
    if size_class not in SIZE_CLASSES:
        raise FrameError(f"{where}: size_class {size_class!r} is not one of S, M, L")
    url = plain(row["repo_url"].strip(), "repo_url", where)
    if not url:
        raise FrameError(f"{where}: repo_url is empty")
    category = (row.get("category") or row["labels"]).strip()
    try:
        name = project_name(host, slug)
    except FrameError as exc:
        raise FrameError(f"{where}: {exc}") from None
    return {
        "row": row, "where": where, "slug": slug, "host": host,
        "name": name, "url": url, "sha": sha, "u": expected,
        "size_class": size_class, "labels": row["labels"].strip(),
        "category": plain(category, "category", where),
        "file_filter": plain((row.get("file_filter") or "").strip(), "file_filter", where),
    }


def ordered(rows: list[dict]) -> list[dict]:
    """u order, slug breaking a tie. Refuses two rows with one project name."""
    seen: dict[str, str] = {}
    for r in rows:
        key = r["name"].lower()
        if key in seen:
            raise FrameError(f"{r['where']}: project name {r['name']!r} repeats {seen[key]}")
        seen[key] = r["where"]
    return sorted(rows, key=lambda r: (r["u"], r["slug"]))


def meta_record(r: dict, status: str) -> dict:
    source = r["row"]
    rec = {f: (source.get(f) or "").strip() for f in META_FIELDS}
    owner, _, repo = r["slug"].rpartition("/")
    rec["owner"] = rec["owner"] or owner
    rec["repo"] = rec["repo"] or repo
    rec["clone_url"] = r["url"]
    rec["provenance_status"] = status
    rec["size_class"] = r["size_class"]
    # From the manifest row: what the project is labelled and tokenized with.
    rec["manifest_category"] = r["category"]
    rec["file_mask"] = r["file_filter"] or UNIVERSAL_MASK
    return rec


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def write_atomic(path: Path, text: str) -> None:
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def build(frame: Path, limit: int | None = None) -> tuple[str, dict, str, dict]:
    """(manifest text, project_meta dict, order text, summary)."""
    rows, dropped = read_frame(frame)
    queue = ordered(rows)
    if limit is not None:
        queue = queue[:limit]
    digest = sha256_of(frame)
    status = f"frame:{frame.name}@sha256:{digest[:12]}"
    stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    header = (
        f"# Built by build_manifest.py from {frame.name} (sha256 {digest}) at {stamp}.\n"
        f"# {len(queue)} projects in u order; u = first {U_BITS} bits of "
        f"sha256({SALT!r} + slug).\n"
        "# Columns: name  url  category  file_filter  size_class  commit\n")
    lines = [f"{r['name']}\t{r['url']}\t{r['category']}\t{r['file_filter']}\t"
             f"{r['size_class']}\t{r['sha']}\n" for r in queue]
    meta = {r["name"]: meta_record(r, status) for r in queue}
    order = "\t".join(ORDER_FIELDS) + "\n" + "".join(
        f"{i}\t{r['name']}\t{r['slug']}\t{r['host']}\t{r['u']!r}\t{r['sha']}\t{r['labels']}\n"
        for i, r in enumerate(queue, 1))
    summary = dict(frame=str(frame), frame_sha256=digest, rows=len(rows),
                   dropped_by_included=dropped, written=len(queue))
    return header + "".join(lines), meta, order, summary


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("frame", type=Path)
    ap.add_argument("--manifest", required=True, type=Path)
    ap.add_argument("--project-meta", required=True, type=Path)
    ap.add_argument("--order", type=Path,
                    help="queue position, slug, u and head_oid per project "
                         "(default: the manifest path with .order.tsv)")
    ap.add_argument("--limit", type=int, metavar="N",
                    help="keep only the first N projects in u order")
    args = ap.parse_args(argv)
    if args.limit is not None and args.limit < 1:
        ap.error("--limit must be at least 1")
    try:
        manifest, meta, order, summary = build(args.frame, args.limit)
    except (FrameError, OSError) as exc:
        print(f"FAIL {exc}", file=sys.stderr)
        return 1
    order_path = args.order or args.manifest.with_suffix(".order.tsv")
    write_atomic(args.manifest, manifest)
    write_atomic(args.project_meta, json.dumps(meta, indent=1, ensure_ascii=False) + "\n")
    write_atomic(order_path, order)
    print(f"OK {summary['written']} projects from {summary['rows']} frame rows "
          f"({summary['dropped_by_included']} not included): {args.manifest}, "
          f"{args.project_meta}, {order_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
