#!/usr/bin/env python3
"""The license pass: one license value per project at a pinned commit, from ScanCode and a rule layer.

For each project of a list (TSV with the columns project, clone_url, run_commit and an optional
fetch_urls with more URLs separated by spaces):

1. fetch    a blobless depth-1 fetch of the pinned commit (git, no API, no token); the key files only
            (root and depth-2 legal files, root READMEs, LICENSES/, REUSE.toml, .reuse/dep5, package
            manifests, spec files, debian/copyright); symlinks resolved; staged into a folder
2. scan     ScanCode (-l -p --classify --summary --license-clarity-score --license-references
            --license-diagnostics) on the staged folder
3. template the SPDX template diff (SpdxTemplateDiff.java, spdx tools-java) on near text matches
4. headers  only when the -only/-or-later choice of a GNU license is still unknown: a capped, seeded
            sample of source files, the ones whose first 16 KB name a version choice, ScanCode on them
5. second   licensee (on the staged folder) and askalono (on the root license files)
6. decide   license_rules.decide: the SPDX expression, the evidence, a confidence class, the agreement

Each project writes <out>/projects/<project>/: result.json, scancode.json.gz, headers.json.gz (when
the header pass ran), second.json, templates.json, fetch.json and stage.tar.gz (the staged key files).
A project with a result.json is skipped (resume). `decide` runs the rule layer again on the stored
files; `collect` writes results.jsonl and results.tsv.

Usage:
  license_scan.py scan --list LIST.tsv --out-dir OUT --tools TOOLS.json [--jobs N] [--only P ...]
  license_scan.py decide --out-dir OUT [--only P ...]
  license_scan.py collect --out-dir OUT

TOOLS.json names the programs: {"scancode": path, "askalono": path, "ruby": path, "licensee": path to
the licensee script, "gem_home": dir, "java": path, "template_classpath": "dir:jar", "git": path}.
A missing program skips its step (and the result says so).

Stdlib Python only.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import os
import posixpath
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path

import license_rules as rules

MAX_FILE_BYTES = 2_000_000
MAX_LEGAL_FILES = 80
MAX_REUSE_FILES = 60
MAX_README = 6
MAX_MANIFESTS = 30
HEADER_FETCH = 150
HEADER_SCAN = 60
HEADER_BYTES = 16384
SOURCE_EXT = {".c", ".h", ".cc", ".cpp", ".cxx", ".hpp", ".hh", ".hxx", ".py", ".java", ".js", ".ts", ".go", ".rs",
              ".rb", ".pl", ".pm", ".sh", ".php", ".cs", ".kt", ".scala", ".m", ".mm", ".swift", ".el", ".lisp",
              ".scm", ".ml", ".hs", ".f90", ".f", ".vala", ".y", ".l", ".lua", ".tcl", ".qml", ".cmake", ".am",
              ".ac", ".in", ".texi", ".d", ".pas", ".adb", ".ads", ".asm", ".s", ".S", ".gd", ".gs", ".js"}
HEADER_RE = re.compile(rb"later version|or-later|-only\b|version [0-9.]+ only|only version|"
                       rb"spdx-license-identifier|\(at your option\)|any later|or later", re.I)
SCANCODE_OPTS = ["-l", "-p", "--classify", "--summary", "--license-clarity-score", "--license-references",
                 "--license-diagnostics", "--strip-root", "--quiet", "--timeout", "300"]
HEADER_OPTS = ["-l", "--license-text", "--license-references", "--strip-root", "--quiet", "--timeout", "120"]


# --- git -------------------------------------------------------------------------------------------

@dataclass
class Entry:
    mode: str
    type: str
    oid: str
    path: str


def git_env() -> dict:
    env = dict(os.environ)
    env.update({"GIT_TERMINAL_PROMPT": "0", "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_ASKPASS": "/bin/true", "SSH_ASKPASS": "/bin/true", "GIT_LFS_SKIP_SMUDGE": "1"})
    return env


class GitError(RuntimeError):
    pass


def git(repo: Path, *args, git_bin="git", timeout=900, input=None, env=None, check=True) -> bytes:
    cmd = [git_bin, "-C", str(repo), "-c", "credential.helper=", "-c", "protocol.version=2",
           "-c", "core.askPass=/bin/true", *args]
    try:
        p = subprocess.run(cmd, input=input, capture_output=True, timeout=timeout, env=env or git_env())
    except subprocess.TimeoutExpired:
        raise GitError(f"timeout after {timeout} s: git {args[0]}")
    if check and p.returncode != 0:
        msg = p.stderr.decode("utf-8", "replace").strip().splitlines()
        raise GitError(f"git {args[0]} rc={p.returncode}: {' | '.join(msg[-3:])[:400]}")
    return p.stdout


def ls_tree(repo: Path, commit: str, git_bin="git") -> list[Entry]:
    out = git(repo, "ls-tree", "-r", "-z", "--full-tree", commit, git_bin=git_bin)
    entries = []
    for rec in out.split(b"\0"):
        if not rec:
            continue
        meta, _, path = rec.partition(b"\t")
        mode, typ, oid = meta.decode().split(" ")
        entries.append(Entry(mode, typ, oid, path.decode("utf-8", "surrogateescape")))
    return entries


def missing_blobs(repo: Path, oids: list[str], git_bin="git") -> set[str]:
    if not oids:
        return set()
    env = git_env()
    env["GIT_NO_LAZY_FETCH"] = "1"
    out = git(repo, "cat-file", "--batch-check", git_bin=git_bin, input=("\n".join(oids) + "\n").encode(),
              env=env, check=False)
    return {line.split(" ")[0] for line in out.decode().splitlines() if line.endswith(" missing")}


def fetch_blobs(repo: Path, oids: list[str], git_bin="git", timeout=900) -> None:
    """Fetch blobs by id in batches (the promisor remote serves them); no-op for present blobs."""
    need = sorted(missing_blobs(repo, sorted(set(oids)), git_bin))
    for i in range(0, len(need), 400):
        git(repo, "fetch", "-q", "--no-tags", "--no-write-fetch-head", "--filter=blob:none", "origin",
            *need[i:i + 400], git_bin=git_bin, timeout=timeout)
    left = missing_blobs(repo, need, git_bin)
    if left:
        raise GitError(f"{len(left)} blobs still missing after the fetch")


def read_blobs(repo: Path, oids: list[str], git_bin="git", limit=None) -> dict[str, bytes]:
    """Blob contents by id (one cat-file --batch process); limit cuts each blob."""
    out = {}
    if not oids:
        return out
    env = git_env()
    env["GIT_NO_LAZY_FETCH"] = "1"
    raw = git(repo, "cat-file", "--batch", git_bin=git_bin, input=("\n".join(oids) + "\n").encode(), env=env,
              check=False)
    buf = io.BytesIO(raw)
    for oid in oids:
        head = buf.readline().decode().strip()
        parts = head.split(" ")
        if len(parts) < 3 or parts[1] == "missing":
            continue
        size = int(parts[2])
        data = buf.read(size)
        buf.read(1)
        out[oid] = data if limit is None else data[:limit]
    return out


def fetch_commit(repo: Path, urls: list[str], commit: str, git_bin="git", timeout=1800, log=None) -> str:
    """git init + a blobless depth-1 fetch of commit from the first URL that serves it. Returns the URL."""
    errors = []
    for url in urls:
        if repo.exists():
            shutil.rmtree(repo)
        repo.mkdir(parents=True)
        git(repo, "init", "-q", git_bin=git_bin)
        git(repo, "remote", "add", "origin", url, git_bin=git_bin)
        for attempt in (1, 2):
            try:
                git(repo, "fetch", "-q", "--depth", "1", "--filter=blob:none", "--no-tags", "origin", commit,
                    git_bin=git_bin, timeout=timeout)
                got = git(repo, "rev-parse", "--verify", "-q", f"{commit}^{{commit}}", git_bin=git_bin).decode().strip()
                if got != commit:
                    raise GitError(f"rev-parse gives {got}")
                return url
            except GitError as e:
                errors.append(f"{url} try {attempt}: {e}")
                if "unadvertised object" in str(e) or "not our ref" in str(e) or "403" in str(e) or \
                        "not found" in str(e).lower():
                    break
                time.sleep(5 * attempt)
    raise GitError("; ".join(errors)[-1200:])


# --- key files -------------------------------------------------------------------------------------

def _depth(path: str) -> int:
    return path.count("/")


def select_key_entries(entries: list[Entry]) -> tuple[list[Entry], dict]:
    """The key files of a tree. Returns the entries (blobs and symlinks) and the counts that were cut."""
    legal, reuse, readme, manifest, meta = [], [], [], [], []
    for e in entries:
        if e.type != "blob" or e.mode not in ("100644", "100755", "120000"):
            continue
        parts = e.path.split("/")
        name, depth = parts[-1], len(parts) - 1
        if e.path in ("REUSE.toml", ".reuse/dep5") or e.path.lower() == "debian/copyright":
            meta.append(e)
        elif depth == 1 and parts[0] in ("LICENSES", "LICENCES"):
            reuse.append(e)
        elif depth == 0 and rules.README_RE.match(name):
            readme.append(e)
        elif rules.is_manifest_name(name) and (depth == 0 or (depth <= 2 and name.lower().endswith(".spec"))):
            manifest.append(e)
        elif depth <= 2 and rules.is_legal_name(name, depth):
            legal.append(e)
        elif depth == 1 and parts[0].lower() in ("licenses", "licences"):
            legal.append(e)
    cut = {}

    def cap(lst, n, key):
        lst.sort(key=lambda e: (_depth(e.path), e.path.lower()))
        if len(lst) > n:
            cut[key] = len(lst) - n
        return lst[:n]

    readme.sort(key=lambda e: (not e.path.lower().startswith("readme."), len(e.path), e.path))
    out = cap(legal, MAX_LEGAL_FILES, "legal") + cap(reuse, MAX_REUSE_FILES, "reuse") + readme[:MAX_README] + \
        cap(manifest, MAX_MANIFESTS, "manifest") + meta
    if len(readme) > MAX_README:
        cut["readme"] = len(readme) - MAX_README
    return out, cut


def resolve_symlinks(selected: list[Entry], entries: list[Entry], targets: dict[str, bytes]) -> tuple[list, list]:
    """Turn symlink entries into (stage path, blob entry) pairs; targets: oid -> link text.

    Returns ([(stage_path, Entry, symlink_of)], [notes])."""
    by_path = {e.path: e for e in entries}
    out, notes = [], []
    for e in selected:
        if e.mode != "120000":
            out.append((e.path, e, None))
            continue
        cur, hops = e, 0
        while cur is not None and cur.mode == "120000" and hops < 6:
            link = targets.get(cur.oid)
            if link is None:
                notes.append(f"symlink-unread:{e.path}")
                cur = None
                break
            tgt = posixpath.normpath(posixpath.join(posixpath.dirname(cur.path), link.decode("utf-8", "replace")))
            if tgt.startswith("../") or tgt == ".." or tgt.startswith("/"):
                notes.append(f"symlink-outside:{e.path}->{tgt}")
                cur = None
                break
            cur = by_path.get(tgt)
            if cur is None:
                notes.append(f"symlink-not-a-file:{e.path}->{tgt}")
            hops += 1
        if cur is not None and cur.mode != "120000":
            out.append((e.path, cur, cur.path))
            notes.append(f"symlink:{e.path}->{cur.path}")
    return out, notes


def header_candidates(entries: list[Entry], seed: str, n: int = HEADER_FETCH) -> list[Entry]:
    """A seeded sample of source files outside bundled folders, for the header pass."""
    pool = []
    for e in entries:
        if e.type != "blob" or e.mode not in ("100644", "100755"):
            continue
        parts = e.path.lower().split("/")
        if any(p in rules.BUNDLED_DIRS for p in parts[:-1]):
            continue
        ext = posixpath.splitext(parts[-1])[1]
        if ext in SOURCE_EXT or ext.lower() in SOURCE_EXT:
            pool.append(e)
    pool.sort(key=lambda e: hashlib.sha256(f"{seed}:{e.path}".encode()).hexdigest())
    return pool[:n]


def _safe_stage_path(stage: Path, rel: str) -> Path:
    p = (stage / rel).resolve()
    if not str(p).startswith(str(stage.resolve()) + os.sep):
        raise ValueError(f"unsafe path {rel!r}")
    return p


def write_stage(stage: Path, staged: list, blobs: dict[str, bytes]) -> list[dict]:
    files = []
    for rel, e, link_of in staged:
        data = blobs.get(e.oid)
        rec = {"path": rel, "oid": e.oid, "role": rules.path_role(rel)}
        if link_of:
            rec["symlink_of"] = link_of
        if data is None:
            rec["skipped"] = "not-fetched"
        elif len(data) > MAX_FILE_BYTES:
            rec["skipped"] = f"size {len(data)}"
        else:
            p = _safe_stage_path(stage, rel)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(data)
            rec["bytes"] = len(data)
        files.append(rec)
    return files


# --- tools -----------------------------------------------------------------------------------------

@dataclass
class Tools:
    scancode: str | None = None
    askalono: str | None = None
    ruby: str | None = None
    licensee: str | None = None
    gem_home: str | None = None
    java: str | None = None
    template_classpath: str | None = None
    git: str = "git"
    scancode_processes: int = 0
    env: dict = field(default_factory=dict)

    @classmethod
    def load(cls, path: str | None) -> "Tools":
        if not path:
            return cls()
        d = json.loads(Path(path).read_text())
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})

    def run_env(self) -> dict:
        env = dict(os.environ)
        env.update(self.env)
        return env


def run_scancode(tools: Tools, target: Path, out_json: Path, opts: list[str], timeout=3600) -> dict:
    cmd = [tools.scancode, *opts, "-n", str(tools.scancode_processes), "--json", str(out_json), str(target)]
    p = subprocess.run(cmd, capture_output=True, timeout=timeout, env=tools.run_env())
    if p.returncode != 0 or not out_json.exists():
        raise RuntimeError(f"scancode rc={p.returncode}: {p.stderr.decode('utf-8', 'replace')[-400:]}")
    return json.loads(out_json.read_text())


def run_licensee(tools: Tools, stage: Path, timeout=300) -> dict:
    env = tools.run_env()
    env["GEM_HOME"] = env["GEM_PATH"] = tools.gem_home or ""
    p = subprocess.run([tools.ruby, tools.licensee, "detect", "--json", str(stage)], capture_output=True,
                       timeout=timeout, env=env)
    if p.returncode not in (0, 1):
        return {"error": f"rc={p.returncode}: {p.stderr.decode('utf-8', 'replace')[-300:]}"}
    try:
        d = json.loads(p.stdout)
    except json.JSONDecodeError:
        return {"error": "no json"}
    files = [{"file": m.get("filename"), "license": m.get("matched_license"),
              "matcher": (m.get("matcher") or {}).get("name"), "confidence": (m.get("matcher") or {}).get("confidence")}
             for m in d.get("matched_files") or []]
    ids = sorted({x.get("spdx_id") for x in d.get("licenses") or [] if x.get("spdx_id") not in (None, "NOASSERTION")})
    return {"licenses": ids, "files": files}


def run_askalono(tools: Tools, paths: list[Path], stage: Path, timeout=120) -> dict:
    out = {}
    for path in paths:
        try:
            p = subprocess.run([tools.askalono, "--format", "json", "identify", "--optimize", str(path)],
                               capture_output=True, timeout=timeout, env=tools.run_env())
            d = json.loads(p.stdout.decode("utf-8", "replace").strip().splitlines()[-1])
        except (subprocess.TimeoutExpired, json.JSONDecodeError, IndexError):
            continue
        res = d.get("result") or {}
        vals = []
        if res.get("license"):
            vals.append([res["license"]["name"], float(res.get("score") or 0)])
        for c in res.get("containing") or []:
            if c.get("license"):
                vals.append([c["license"]["name"], float(c.get("score") or 0)])
        rel = str(path.relative_to(stage))
        best = {}
        for name, score in vals:
            best[name] = max(best.get(name, 0), score)
        out[rel] = sorted(([n, s] for n, s in best.items()), key=lambda x: -x[1])
    return out


def run_templates(tools: Tools, stage: Path, requests: list, timeout=900) -> dict:
    """SpdxTemplateDiff on (path, [ids]) requests; returns path -> {id: result}."""
    lines = []
    for rel, ids in requests:
        p = stage / rel
        if p.exists():
            lines.append(f"{p}\t{','.join(ids)}")
    if not lines:
        return {}
    env = tools.run_env()
    env["SPDX_USE_ONLY_LOCAL_LICENSES"] = "true"
    p = subprocess.run([tools.java, "-Xmx1g", "-cp", tools.template_classpath, "SpdxTemplateDiff"],
                       input=("\n".join(lines) + "\n").encode(), capture_output=True, timeout=timeout, env=env)
    out = {}
    prefix = str(stage) + os.sep
    for line in p.stdout.decode("utf-8", "replace").splitlines():
        try:
            r = json.loads(line)
        except json.JSONDecodeError:
            continue
        rel = r.get("path", "")
        rel = rel[len(prefix):] if rel.startswith(prefix) else rel
        if r.get("id"):
            out.setdefault(rel, {})[r["id"]] = {k: r.get(k) for k in ("match", "within", "message", "error")}
    return out


# --- one project -----------------------------------------------------------------------------------

def _gz_json(path: Path, obj) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(obj, f)


def _load_gz_json(path: Path):
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return json.load(f)


def _tar_stage(stage: Path, out: Path) -> None:
    with tarfile.open(out, "w:gz") as t:
        for p in sorted(stage.rglob("*")):
            if p.is_file():
                t.add(p, arcname=str(p.relative_to(stage)))


def _second(tools: Tools, stage: Path, staged_files: list[dict]) -> dict:
    second = {}
    if tools.ruby and tools.licensee:
        second["licensee"] = run_licensee(tools, stage)
    if tools.askalono:
        paths = [stage / f["path"] for f in staged_files if "bytes" in f and f["role"] in ("primary", "alt", "named")]
        second["askalono"] = run_askalono(tools, paths[:12], stage)
    return second


TEXT_ROLES = ("readme", "notice", "primary", "alt", "named", "manifest")
TEXT_BYTES = 262_144
KERNEL_RE = re.compile(r"(?i)(^|__|[-_.])(linux|kernel|lustre|amdgpu)([-_.]|$)")


def is_kernel_project(name: str) -> bool:
    """An operating-system kernel or a kernel module tree (the syscall note of COPYING applies)."""
    return bool(KERNEL_RE.search(name))


def _root_text(name: str, data: bytes) -> str | None:
    if "/" in name or rules.path_role(name) not in TEXT_ROLES or b"\0" in data[:4096]:
        return None
    return data[:TEXT_BYTES].decode("utf-8", errors="replace")


def stage_texts(stage: Path) -> dict:
    """path -> text of the root key files of a staged folder (READMEs, notices, license files, manifests)."""
    out = {}
    for p in sorted(stage.iterdir()) if stage.exists() else []:
        if p.is_file() and not p.is_symlink():
            t = _root_text(p.name, p.read_bytes()[:TEXT_BYTES])
            if t is not None:
                out[p.name] = t
    return out


def stored_texts(stage_tar: Path) -> dict:
    """The same texts from a stored stage.tar.gz (no extraction to disk)."""
    out = {}
    if not stage_tar.exists():
        return out
    with tarfile.open(stage_tar, "r:gz") as t:
        for m in t.getmembers():
            if not m.isfile() or "/" in m.name or m.size > 4 * TEXT_BYTES:
                continue
            f = t.extractfile(m)
            if f is None:
                continue
            txt = _root_text(m.name, f.read(TEXT_BYTES))
            if txt is not None:
                out[m.name] = txt
    return out


def _second_for_rules(second: dict) -> dict:
    return {"licensee": (second.get("licensee") or {}).get("licenses") or [],
            "askalono": second.get("askalono") or {}}


def decide_stored(pdir: Path) -> dict:
    """The rule layer on the stored files of one project."""
    scan = _load_gz_json(pdir / "scancode.json.gz")
    headers = _load_gz_json(pdir / "headers.json.gz") if (pdir / "headers.json.gz").exists() else None
    templates = json.loads((pdir / "templates.json").read_text()) if (pdir / "templates.json").exists() else {}
    second = json.loads((pdir / "second.json").read_text()) if (pdir / "second.json").exists() else {}
    return rules.decide(scan, templates=templates, headers=headers, second=_second_for_rules(second),
                        texts=stored_texts(pdir / "stage.tar.gz"), kernel=is_kernel_project(pdir.name)).as_dict()


def scan_project(row: dict, out_dir: Path, tools: Tools, work: Path, keep_stage=False) -> dict:
    name = row["project"]
    pdir = out_dir / "projects" / name
    pdir.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    times = {}
    res = {"project": name, "clone_url": row.get("clone_url", ""), "commit": row["run_commit"]}
    fetch_info = {"urls": []}
    wdir = Path(tempfile.mkdtemp(prefix=f"{name[:40]}.", dir=work))
    repo, stage = wdir / "repo", wdir / "stage"
    try:
        urls = [row["clone_url"]] + [u for u in (row.get("fetch_urls") or "").split() if u]
        fetch_info["urls"] = urls
        try:
            used = fetch_commit(repo, urls, row["run_commit"], git_bin=tools.git)
        except GitError as e:
            res.update({"status": "fetch-error", "error": str(e)})
            return _finish(res, fetch_info, pdir, t0, times)
        fetch_info["url"] = used
        entries = ls_tree(repo, row["run_commit"], git_bin=tools.git)
        fetch_info["tree_entries"] = len(entries)
        fetch_info["submodules"] = sum(1 for e in entries if e.mode == "160000")
        selected, cut = select_key_entries(entries)
        fetch_info["cut"] = cut
        links = [e for e in selected if e.mode == "120000"]
        targets = {}
        if links:
            fetch_blobs(repo, [e.oid for e in links], git_bin=tools.git)
            targets = read_blobs(repo, [e.oid for e in links], git_bin=tools.git, limit=4096)
            more = []
            by_path = {e.path: e for e in entries}
            for e in links:  # fetch the link targets that are links again
                tgt = posixpath.normpath(posixpath.join(posixpath.dirname(e.path),
                                                        targets.get(e.oid, b"").decode("utf-8", "replace")))
                t = by_path.get(tgt)
                if t is not None and t.mode == "120000":
                    more.append(t.oid)
            if more:
                fetch_blobs(repo, more, git_bin=tools.git)
                targets.update(read_blobs(repo, more, git_bin=tools.git, limit=4096))
        staged, notes = resolve_symlinks(selected, entries, targets)
        fetch_info["notes"] = notes
        oids = sorted({e.oid for _, e, _ in staged})
        fetch_blobs(repo, oids, git_bin=tools.git)
        blobs = read_blobs(repo, oids, git_bin=tools.git)
        stage.mkdir()
        fetch_info["files"] = write_stage(stage, staged, blobs)
        times["fetch"] = round(time.time() - t0, 1)
        n_staged = sum(1 for f in fetch_info["files"] if "bytes" in f)
        if not n_staged:
            res.update({"status": "no-key-files"})
            d = rules.Decision()
            d.flags.append("no-key-files")
            res["decision"] = d.as_dict()
            return _finish(res, fetch_info, pdir, t0, times)
        if not tools.scancode:
            res.update({"status": "fetched"})
            return _finish(res, fetch_info, pdir, t0, times)
        t1 = time.time()
        scan = run_scancode(tools, stage, wdir / "scancode.json", SCANCODE_OPTS)
        _gz_json(pdir / "scancode.json.gz", scan)
        times["scancode"] = round(time.time() - t1, 1)
        t1 = time.time()
        second = _second(tools, stage, fetch_info["files"])
        (pdir / "second.json").write_text(json.dumps(second, indent=1))
        times["second"] = round(time.time() - t1, 1)
        texts, kernel = stage_texts(stage), is_kernel_project(name)
        d = rules.decide(scan, second=_second_for_rules(second), texts=texts, kernel=kernel)
        templates = {}
        if d.template_requests and tools.java and tools.template_classpath:
            t1 = time.time()
            templates = run_templates(tools, stage, d.template_requests)
            times["template"] = round(time.time() - t1, 1)
            d = rules.decide(scan, templates=templates, second=_second_for_rules(second), texts=texts,
                             kernel=kernel)
        (pdir / "templates.json").write_text(json.dumps(templates, indent=1))
        headers = None
        if d.needs_header_pass:
            t1 = time.time()
            headers, hinfo = header_pass(repo, entries, row["run_commit"], wdir, tools)
            fetch_info["header_pass"] = hinfo
            if headers is not None:
                _gz_json(pdir / "headers.json.gz", headers)
            times["headers"] = round(time.time() - t1, 1)
            d = rules.decide(scan, templates=templates, headers=headers or {"files": []},
                             second=_second_for_rules(second), texts=texts, kernel=kernel)
        res["status"] = "ok"
        res["decision"] = d.as_dict()
        res["scancode_summary"] = {k: (scan.get("summary") or {}).get(k) for k in
                                   ("declared_license_expression", "license_clarity_score")}
        _tar_stage(stage, pdir / "stage.tar.gz")
        return _finish(res, fetch_info, pdir, t0, times)
    except Exception as e:  # one project must not stop the run
        res.update({"status": "error", "error": f"{type(e).__name__}: {e}"[:1000]})
        return _finish(res, fetch_info, pdir, t0, times)
    finally:
        if keep_stage and stage.exists():
            dst = pdir / "stage"
            if dst.exists():
                shutil.rmtree(dst)
            shutil.copytree(stage, dst)
        shutil.rmtree(wdir, ignore_errors=True)


def header_pass(repo: Path, entries: list[Entry], commit: str, wdir: Path, tools: Tools):
    cands = header_candidates(entries, seed=commit)
    info = {"candidates": len(cands)}
    if not cands:
        return None, info
    fetch_blobs(repo, [e.oid for e in cands], git_bin=tools.git)
    blobs = read_blobs(repo, [e.oid for e in cands], git_bin=tools.git, limit=HEADER_BYTES)
    hits = [e for e in cands if e.oid in blobs and HEADER_RE.search(blobs[e.oid])]
    info["fetched"] = len(blobs)
    info["matched"] = len(hits)
    hits = hits[:HEADER_SCAN]
    if not hits:
        return None, info
    hdir = wdir / "headers"
    for e in hits:
        p = _safe_stage_path(hdir, e.path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(blobs[e.oid])
    info["scanned"] = len(hits)
    scan = run_scancode(tools, hdir, wdir / "headers.json", HEADER_OPTS)
    return scan, info


def _finish(res: dict, fetch_info: dict, pdir: Path, t0: float, times: dict) -> dict:
    times["total"] = round(time.time() - t0, 1)
    res["times"] = times
    res["finished"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    (pdir / "fetch.json").write_text(json.dumps(fetch_info, indent=1))
    tmp = pdir / "result.json.part"
    tmp.write_text(json.dumps(res, indent=1))
    tmp.replace(pdir / "result.json")
    return res


# --- commands --------------------------------------------------------------------------------------

def read_list(path: str) -> list[dict]:
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    for r in rows:
        for k in ("project", "clone_url", "run_commit"):
            if not r.get(k):
                raise SystemExit(f"{path}: a row without {k}: {r}")
        if not re.fullmatch(r"[0-9a-f]{40}", r["run_commit"]):
            raise SystemExit(f"{path}: {r['project']}: run_commit is not a full SHA-1")
        if "/" in r["project"] or r["project"].startswith("."):
            raise SystemExit(f"{path}: bad project name {r['project']!r}")
    return rows


def cmd_scan(args) -> int:
    rows = read_list(args.list)
    if args.only:
        rows = [r for r in rows if r["project"] in set(args.only)]
    out = Path(args.out_dir)
    (out / "projects").mkdir(parents=True, exist_ok=True)
    work = Path(args.work_dir or out / "work")
    work.mkdir(parents=True, exist_ok=True)
    tools = Tools.load(args.tools)
    redo = set(filter(None, (args.redo_status or "").split(",")))
    if redo:
        for r in rows:
            rp = out / "projects" / r["project"] / "result.json"
            if rp.exists() and json.loads(rp.read_text()).get("status") in redo:
                n_try = len(list(rp.parent.glob("result.json.try*"))) + 1
                rp.replace(rp.parent / f"result.json.try{n_try}")
    todo = [r for r in rows if not (out / "projects" / r["project"] / "result.json").exists()]
    print(f"[{time.strftime('%H:%M:%S')}] {len(rows)} projects, {len(rows) - len(todo)} done, {len(todo)} to do, "
          f"jobs {args.jobs}", flush=True)
    lock = threading.Lock()
    n = 0
    stop = Path(args.stop_file) if args.stop_file else None
    with ThreadPoolExecutor(max_workers=args.jobs) as pool:
        futs = {}
        it = iter(todo)

        def submit_next():
            if stop and stop.exists():
                return False
            r = next(it, None)
            if r is None:
                return False
            futs[pool.submit(scan_project, r, out, tools, work, args.keep_stage)] = r
            return True

        for _ in range(args.jobs * 2):
            if not submit_next():
                break
        while futs:
            done = next(as_completed(list(futs)))
            r = futs.pop(done)
            res = done.result()
            with lock:
                n += 1
                dec = res.get("decision") or {}
                print(f"[{time.strftime('%H:%M:%S')}] {n}/{len(todo)} {r['project']} {res.get('status')} "
                      f"{dec.get('license', '-')} {dec.get('confidence', '-')} {res['times'].get('total')}s",
                      flush=True)
            submit_next()
    if stop and stop.exists():
        print("STOP file found: no new projects were started", flush=True)
        return 3
    return 0


def cmd_decide(args) -> int:
    out = Path(args.out_dir)
    n = 0
    for pdir in sorted((out / "projects").iterdir()):
        if args.only and pdir.name not in args.only:
            continue
        rp = pdir / "result.json"
        if not rp.exists() or not (pdir / "scancode.json.gz").exists():
            continue
        res = json.loads(rp.read_text())
        res["decision"] = decide_stored(pdir)
        res["decided"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        rp.write_text(json.dumps(res, indent=1))
        n += 1
    print(f"decided {n} projects")
    return 0


TSV_COLS = ["project", "commit", "status", "license", "version_choice", "confidence", "evidence_kind",
            "evidence_files", "other_licenses", "bundled_licenses", "licensee", "licensee_agreement", "askalono",
            "askalono_agreement", "scancode_declared", "clarity_score", "flags", "error", "seconds"]


def result_row(res: dict) -> dict:
    d = res.get("decision") or {}
    so = d.get("second_opinions") or {}
    ss = res.get("scancode_summary") or {}
    clar = ss.get("license_clarity_score") or {}
    return {
        "project": res["project"], "commit": res.get("commit", ""), "status": res.get("status", ""),
        "license": d.get("license", "NOASSERTION"), "version_choice": d.get("version_choice", ""),
        "confidence": d.get("confidence", "none"), "evidence_kind": d.get("evidence_kind", ""),
        "evidence_files": ";".join(sorted({e.get("path", "") for e in d.get("evidence", [])})),
        "other_licenses": ";".join(d.get("other_licenses", [])),
        "bundled_licenses": ";".join(d.get("bundled_licenses", [])),
        "licensee": ";".join(so.get("licensee", [])), "licensee_agreement": so.get("licensee_agreement", ""),
        "askalono": ";".join(so.get("askalono", [])), "askalono_agreement": so.get("askalono_agreement", ""),
        "scancode_declared": ss.get("declared_license_expression") or "",
        "clarity_score": clar.get("score", "") if isinstance(clar, dict) else "",
        "flags": ";".join(d.get("flags", []))[:500], "error": (res.get("error") or "")[:300],
        "seconds": (res.get("times") or {}).get("total", ""),
    }


def cmd_collect(args) -> int:
    out = Path(args.out_dir)
    rows = []
    with open(out / "results.jsonl.part", "w") as fj:
        for pdir in sorted((out / "projects").iterdir()):
            rp = pdir / "result.json"
            if not rp.exists():
                continue
            res = json.loads(rp.read_text())
            fj.write(json.dumps(res, sort_keys=True) + "\n")
            rows.append(result_row(res))
    (out / "results.jsonl.part").replace(out / "results.jsonl")
    with open(out / "results.tsv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=TSV_COLS, delimiter="\t", lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow({k: str(v).replace("\t", " ").replace("\n", " ") for k, v in r.items()})
    counts = {}
    for r in rows:
        counts[(r["status"], r["confidence"])] = counts.get((r["status"], r["confidence"]), 0) + 1
    print(f"collected {len(rows)} projects: " + ", ".join(f"{s}/{c} {n}" for (s, c), n in sorted(counts.items())))
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("scan")
    s.add_argument("--list", required=True)
    s.add_argument("--out-dir", required=True)
    s.add_argument("--tools")
    s.add_argument("--work-dir")
    s.add_argument("--jobs", type=int, default=4)
    s.add_argument("--only", nargs="*")
    s.add_argument("--stop-file")
    s.add_argument("--keep-stage", action="store_true")
    s.add_argument("--redo-status", help="run again the projects whose result has one of these statuses "
                   "(comma list, e.g. fetch-error,error); the old result.json is kept as result.json.tryN")
    d = sub.add_parser("decide")
    d.add_argument("--out-dir", required=True)
    d.add_argument("--only", nargs="*")
    c = sub.add_parser("collect")
    c.add_argument("--out-dir", required=True)
    args = ap.parse_args(argv)
    return {"scan": cmd_scan, "decide": cmd_decide, "collect": cmd_collect}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
