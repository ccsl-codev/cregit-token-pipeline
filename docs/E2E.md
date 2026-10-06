# End-to-end runs

This page covers a long run over a projects dataset: from the selection frame
to one joined, cleaned Parquet per project, with an audit record for each step.

## 1. From the frame to a manifest

`build_manifest.py` reads the selection frame and writes three files: the
manifest, the `project_meta.json` sidecar, and an order file.

```sh
./build_manifest.py frame.csv --manifest manifest.census.tsv \
    --project-meta project_meta.census.json
```

Do not build a manifest for a new run from `candidates.csv`. The frame is the
only input.

### The frame contract

The frame is a UTF-8 CSV file with a header row.

| Column | Required | Meaning |
|---|---|---|
| `slug` or `name_with_owner` | yes | `owner/repo` on its host. It seeds `u`. |
| `host` | yes | For example `github.com`. Blank means `github.com`. |
| `repo_url` | yes | The clone URL. |
| `head_oid` | yes | The commit to analyse, 40 lowercase hex digits. |
| `u` | yes | The permanent random number of the row. See below. |
| `size_class` | yes | `S`, `M` or `L`. |
| `labels` | yes | Free text. It becomes the manifest category. |
| `category` | no | Overrides `labels` as the manifest category. |
| `file_filter` | no | The file mask. Blank means the universal mask. |
| `included` | no | When present, only rows with `1`, `true` or `yes` are kept. |
| any of the 29 provenance fields | no | Fills that field of the sidecar. |

`u` is `sha256("20261110:" + slug)`, read as a fraction: the first 53 bits
divided by 2^53. The value is exact in a float and always below 1. The builder
computes `u` itself and stops when the frame's value differs, because a
different salt or formula would change the run order.

The tests use `tests/fixtures/frame.sample.csv` as a small example.

### What the builder writes

- The manifest lists the projects in `u` order, lowest first. So the projects
  finished at any date are a random sample of the frame. The sixth column pins
  each project to `head_oid`.
- The project name is `owner__repo`. A host other than GitHub is a prefix:
  `gitlab.com__group__repo`. The builder refuses two rows with one name.
- `project_meta.json` holds the 29 provenance fields per project.
  `clone_url`, `owner`, `repo`, `size_class`, `manifest_category` and
  `file_mask` come from the row. `provenance_status` names the frame file and
  the start of its sha256.
- The order file maps each queue position to `slug`, `host`, `u`, `head_oid`
  and `labels`.
- `--limit N` keeps the first N projects in `u` order, for an early batch.
