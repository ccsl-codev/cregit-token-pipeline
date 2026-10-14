"""Read the project manifest: one tab-separated row per project."""

from __future__ import annotations

from pathlib import Path

import pin
from file_mask import UNIVERSAL_MASK

MANIFEST_FIELDS = ("name", "url", "category", "file_filter", "size_class")
# An optional sixth column pins the commit to analyse. A five-column row runs the
# remote's HEAD at clone time, and the record says "unpinned".
PINNED_FIELD = "commit"
UNPINNED = "unpinned"
SIZE_CLASSES = ("S", "M", "L")


def read_manifest(path: Path, only: set | None) -> list[dict]:
    projects = []
    for lineno, line in enumerate(path.read_text().splitlines(), 1):
        if not line.strip() or line.startswith("#"):
            continue
        fields = line.split("\t")
        if len(fields) not in (len(MANIFEST_FIELDS), len(MANIFEST_FIELDS) + 1):
            raise ValueError(
                f"{path}:{lineno}: expected {len(MANIFEST_FIELDS)} tab-separated"
                f" fields, or {len(MANIFEST_FIELDS) + 1} with a pinned commit,"
                f" got {len(fields)}: {line!r}"
            )
        row = dict(zip(MANIFEST_FIELDS, fields, strict=False))
        if len(fields) > len(MANIFEST_FIELDS):
            row[PINNED_FIELD] = fields[-1]
            if not pin.SHA_RE.match(row[PINNED_FIELD]):
                raise ValueError(
                    f"{path}:{lineno}: commit {row[PINNED_FIELD]!r} is not a"
                    " 40-character lowercase hex SHA"
                )
        if row["size_class"] not in SIZE_CLASSES:
            raise ValueError(
                f"{path}:{lineno}: size_class {row['size_class']!r}"
                f" is not one of {', '.join(SIZE_CLASSES)}"
            )
        if only and row["name"] not in only:
            continue
        # blobExec rejects an empty mask; a blank column means the universal one.
        row["file_filter"] = row["file_filter"] or UNIVERSAL_MASK
        projects.append(row)
    return projects
