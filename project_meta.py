#!/usr/bin/env python3
"""Per-project constants for the token-level Parquet, from candidates.csv.

The manifest cannot carry these: three parsers unpack exactly five positional
fields and four tests assert their positions. A sidecar keyed by the manifest
name, joined on clone_url, leaves all of that alone.

Every value is a string, and a missing one is "", because these go into a SQL
string literal and None has no representation there.
"""
import argparse
import csv
import json
import sys
from pathlib import Path

import select_corpus

# 29 per-project constants, in dataset order.
#
# clone_url leads because it is the only reliable identifier: repo_name is a
# slug that lowercases and maps _ and . to -, so 15 of the 200 projects do not
# round-trip by name.
#
# provenance_status is second because it qualifies everything after it. A
# development fixture carries "fixture-needs-rework" and empty metadata, so a
# reader filters it out instead of mistaking blanks for findings.
#
# manifest_category and file_mask come last, and they come from the manifest,
# not from candidates.csv. manifest_category is the manifest's own label and is
# not guaranteed to equal `stratum`. file_mask is the regex the project was
# actually tokenized with: the C mask is a strict subset of the C++ mask, so
# ~19 projects will be re-run with a wider one, and a corpus built with two
# masks is only comparable across projects if each row says which it used.
#
# included and excluded_because are absent on purpose: they describe the
# selection decision, and are constant across the published set.
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

PROVENANCE_OK = "candidates.csv"
PROVENANCE_FIXTURE = "fixture-needs-rework"


def build_meta(candidates_csv: Path, manifest: Path,
               fixture_manifests: tuple = ()) -> dict:
    """Per-project constants, keyed by manifest name, joined on clone_url.

    A project in `manifest` must be in candidates.csv. A project named only by
    a fixture manifest gets a placeholder row flagged PROVENANCE_FIXTURE, so the
    Parquet schema stays uniform across every file while a reader can still
    exclude non-corpus rows with one predicate.
    """
    with candidates_csv.open(newline="") as fh:
        rows = list(csv.DictReader(fh))

    # candidates.csv keeps one row per provenance fact, so one repository can
    # appear more than once under one clone_url: 197 do, and 5 of those pairs
    # disagree on the stratum. Resolve them exactly as select_corpus.py did when
    # it drew the manifest -- the row whose owner matches the URL wins -- or the
    # stratum reported here would contradict the manifest_category beside it.
    by_clone_url = {r["clone_url"]: r
                    for r in select_corpus.dedupe_by_clone_url(rows)}

    def rows_of(path: Path):
        for line in path.read_text().splitlines():
            if not line.strip() or line.startswith("#"):
                continue
            name, url, category, file_mask, _size_class = line.split("\t")
            yield name, url, category, file_mask

    def record(url: str, category: str, file_mask: str, row: dict | None,
               status: str) -> dict:
        source = row if row is not None else {}
        rec = {f: (source.get(f) or "") for f in META_FIELDS}
        rec["clone_url"] = url
        rec["provenance_status"] = status
        # From the manifest, never from candidates.csv. A fixture has no
        # provenance, but it was tokenized with a real mask, so these two stay.
        rec["manifest_category"] = category
        rec["file_mask"] = file_mask
        return rec

    meta = {}
    missing = []
    for name, url, category, file_mask in rows_of(manifest):
        row = by_clone_url.get(url)
        if row is None:
            missing.append(f"{name} ({url})")
            continue
        meta[name] = record(url, category, file_mask, row, PROVENANCE_OK)

    for path in fixture_manifests:
        for name, url, category, file_mask in rows_of(path):
            if name in meta:
                continue
            # A development fixture. It was never selected by select_corpus.py,
            # so whatever candidates.csv says about the repository does not
            # describe this clone: it has no stratum in the sample, no draw, no
            # history cluster. Say that in the data rather than emitting 25
            # borrowed strings and hoping nobody joins on them.
            meta[name] = record(url, category, file_mask, None,
                                PROVENANCE_FIXTURE)

    if missing:
        sys.exit(
            f"{len(missing)} manifest project(s) are not in {candidates_csv.name}, "
            "joined on clone_url: " + ", ".join(missing) + "\n"
            "Every corpus project must carry its provenance. Pass a fixture "
            "manifest with --fixture-manifest if these are development "
            "fixtures, not corpus members. Refusing to guess."
        )
    return meta


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--candidates", type=Path, required=True)
    ap.add_argument("--manifest", type=Path, required=True)
    ap.add_argument("--fixture-manifest", type=Path, action="append", default=[],
                    help="repeatable. Projects found only here get a placeholder "
                         "row flagged fixture-needs-rework.")
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    meta = build_meta(args.candidates, args.manifest,
                      tuple(args.fixture_manifest))
    args.out.write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n")
    print(f"wrote {args.out} — {len(meta)} projects, {len(META_FIELDS)} fields each")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
