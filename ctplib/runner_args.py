"""The argument lists of the cregit runner, run_pipeline_process.sh, and of the steps after it."""

from __future__ import annotations

import functools
import re
from pathlib import Path

from ctplib import config, manifest_file, pinning, runtime


@functools.cache
def _script_supports_cached(cregit: Path, flag: str) -> bool:
    try:
        text = (cregit / "run_pipeline_process.sh").read_text()
    except OSError:
        return False
    # A whole flag: --mask is not supported merely because --mask-widened appears.
    return re.search(rf"(?<![\w-]){re.escape(flag)}(?![\w-])", text) is not None


def script_supports(flag: str) -> bool:
    """True when the configured run_pipeline_process.sh advertises `flag`.
    The cache keys on CREGIT too, so it stays right when CREGIT is reassigned."""
    return _script_supports_cached(config.CREGIT, flag)


def shard_class(project: dict) -> bool:
    """True when --shards asks for sharding and this project's class is in --shard-classes.
    For S and M, concurrent projects already fill the machine; shards would only cost disk."""
    return runtime.opts.get("shards", 0) > 1 and project["size_class"] in runtime.opts.get(
        "shard_classes", ()
    )


_SWITCH_FLAGS = (
    ("skip_html", "--skip-html"),
    ("reblame", "--reblame"),
    ("mask_widened", "--mask-widened"),
    ("force_clean", "--force-clean"),
    ("tokenizer_worker", "--tokenizer-worker"),
)
# The runner calls blame_jobs --jobs; ctp's own --jobs means concurrent projects.
_VALUE_FLAGS = (
    ("gc", "--gc"),
    ("blame_jobs", "--jobs"),
    ("memory_limit", "--memory-limit"),
    ("duckdb_threads", "--duckdb-threads"),
)


def value_args(opts: dict, table: tuple) -> list[str]:
    """[flag, value] for every (option, flag) in table whose option is set."""
    return [arg for opt, flag in table if opts.get(opt) for arg in (flag, str(opts[opt]))]


def build_pipeline_args(project: dict, opts: dict, workdir: Path) -> list[str]:
    """The run_pipeline_process.sh argv for one project. Assumes check_memo_dir
    has already refused an unsafe --memo-dir."""
    name = project["name"]
    pinned = bool(project.get(manifest_file.PINNED_FIELD))
    pipeline_args = [
        "./run_pipeline_process.sh",
        "--repo-url",
        str(pinning.pinned_clone_path(name)) if pinned else project["url"],
        "--repo-name",
        name,
        "--work",
        str(workdir),
        "--mask",
        opts.get("mask") or project["file_filter"],
    ]
    if pinned:
        # The runner derives commit links from --repo-url, which is now a local path.
        pipeline_args += ["--commit-url", pinning.commit_url(project["url"])]
    pipeline_args += [flag for opt, flag in _SWITCH_FLAGS if opts.get(opt)]
    pipeline_args += value_args(opts, (("retokenize", "--retokenize"),))
    # One subdirectory per project: tokenBySha.pl keys the memo on the content
    # sha1 alone, so projects sharing one would serve each other's tokens.
    if opts.get("memo_dir"):
        pipeline_args += ["--memo-dir", str(Path(opts["memo_dir"]).resolve() / name)]
    if shard_class(project):
        pipeline_args += ["--mode", "sharded", "--shards", str(opts["shards"])]
    pipeline_args += value_args(opts, _VALUE_FLAGS)
    # The sidecar is keyed by manifest name; the generator refuses a key it cannot find.
    if opts.get("project_meta"):
        pipeline_args += ["--project-meta", str(opts["project_meta"]), "--project-key", name]
    # FROM_STEP is positional and must come last.
    from_step = opts.get("from_step", 1)
    if from_step > 1:
        pipeline_args.append(str(from_step))
    return pipeline_args


_FIRM_VALUE_FLAGS = (("memory_limit", "--memory-limit"), ("duckdb_threads", "--threads"))


def project_phases(project: dict, opts: dict, workdir: Path, stamp: Path) -> list[tuple]:
    """(phase, argv) in run order: cregit writes 67 columns, firm adds 3, validate gates 70."""
    parquet = str(workdir / f"{project['name']}-dataset.parquet")
    return [
        ("pipeline", build_pipeline_args(project, opts, workdir)),
        (
            "firm",
            [
                "python3",
                str(config.CORPUS / "firm_attribution.py"),
                parquet,
                *value_args(opts, _FIRM_VALUE_FLAGS),
            ],
        ),
        ("validate", ["python3", str(config.CORPUS / "validate.py"), parquet, str(stamp)]),
    ]
