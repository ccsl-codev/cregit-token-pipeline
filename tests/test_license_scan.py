"""Tests of license_scan: key-file selection, symlinks, the git fetch of a pinned commit, the per-project
flow with stand-in scanner programs, and the scan/decide/collect commands.

The git tests use a local repository served over file:// with partial-clone support switched on, so
they need git but no network. The scanners are small Python scripts that write canned output.
"""

import gzip
import json
import os
import subprocess
import sys
import tarfile
import textwrap
from pathlib import Path

import pytest

import license_scan as S

MIT_TEXT = "MIT License\n\nPermission is hereby granted, free of charge, to any person obtaining a copy\n"


def E(path, mode="100644", typ="blob", oid=None):
    return S.Entry(mode, typ, oid or ("%040x" % (abs(hash(path)) % (16 ** 40))), path)


# --- selection -------------------------------------------------------------------------------------

def test_select_key_entries_picks_key_files_only():
    entries = [E(p) for p in [
        "LICENSE", "COPYING.LESSER", "README.md", "README.de.md", "src/main.c", "docs/LICENSE.txt",
        "a/b/COPYING", "a/b/c/LICENSE", "LICENSES/MIT.txt", "LICENSES/sub/x.txt", "licenses/zlib.txt",
        "package.json", "sub/package.json", "rpm/foo.spec", "x/y/z/foo.spec", "debian/copyright",
        "REUSE.toml", ".reuse/dep5", "license.go", "GPL.txt", "lib/GPL.txt", "Cargo.toml"]]
    entries.append(E("ext", mode="160000", typ="commit"))
    entries.append(E("docs", typ="tree"))
    sel, cut = S.select_key_entries(entries)
    got = sorted(e.path for e in sel)
    assert got == sorted(["LICENSE", "COPYING.LESSER", "README.md", "README.de.md", "docs/LICENSE.txt",
                          "a/b/COPYING", "LICENSES/MIT.txt", "licenses/zlib.txt", "package.json", "rpm/foo.spec",
                          "debian/copyright", "REUSE.toml", ".reuse/dep5", "GPL.txt", "Cargo.toml"])
    assert cut == {}


def test_select_key_entries_caps():
    entries = [E(f"p{i:03d}/LICENSE") for i in range(S.MAX_LEGAL_FILES + 5)]
    entries += [E(f"README.{i}.md") for i in range(S.MAX_README + 2)]
    sel, cut = S.select_key_entries(entries)
    assert cut == {"legal": 5, "readme": 2}
    assert len(sel) == S.MAX_LEGAL_FILES + S.MAX_README


def test_resolve_symlinks():
    lic = E("docs/LICENSE.txt", oid="a" * 40)
    link = E("LICENSE", mode="120000", oid="b" * 40)
    link2 = E("COPYING", mode="120000", oid="c" * 40)       # -> LICENSE -> docs/LICENSE.txt
    out_link = E("NOTICE", mode="120000", oid="d" * 40)     # -> ../outside
    dir_link = E("LICENSES", mode="120000", oid="e" * 40)   # -> a folder
    lost = E("UNLICENSE", mode="120000", oid="f" * 40)      # its target was not read
    entries = [lic, link, link2, out_link, dir_link, lost, E("share/licenses/x", oid="1" * 40)]
    targets = {"b" * 40: b"docs/LICENSE.txt", "c" * 40: b"LICENSE", "d" * 40: b"../outside",
               "e" * 40: b"share/licenses"}
    staged, notes = S.resolve_symlinks([lic, link, link2, out_link, dir_link, lost], entries, targets)
    assert [(p, e.path, s) for p, e, s in staged] == [
        ("docs/LICENSE.txt", "docs/LICENSE.txt", None), ("LICENSE", "docs/LICENSE.txt", "docs/LICENSE.txt"),
        ("COPYING", "docs/LICENSE.txt", "docs/LICENSE.txt")]
    assert "symlink-outside:NOTICE->../outside" in notes
    assert "symlink-not-a-file:LICENSES->share/licenses" in notes
    assert "symlink-unread:UNLICENSE" in notes


def test_header_candidates_seeded_and_no_bundled():
    entries = [E(f"src/f{i}.c") for i in range(20)] + [E("third_party/x/a.c"), E("doc/a.png"), E("x.py")]
    a = S.header_candidates(entries, seed="s1", n=5)
    b = S.header_candidates(entries, seed="s1", n=5)
    c = S.header_candidates(entries, seed="s2", n=5)
    assert [e.path for e in a] == [e.path for e in b]
    assert [e.path for e in a] != [e.path for e in c]
    allc = S.header_candidates(entries, seed="s1", n=100)
    assert "third_party/x/a.c" not in [e.path for e in allc] and "doc/a.png" not in [e.path for e in allc]
    assert len(allc) == 21


def test_write_stage_rejects_unsafe_paths_and_big_files(tmp_path):
    big = E("README.md", oid="1" * 40)
    ok = E("LICENSE", oid="2" * 40)
    gone = E("COPYING", oid="3" * 40)
    files = S.write_stage(tmp_path, [("README.md", big, None), ("LICENSE", ok, None), ("COPYING", gone, None)],
                          {"1" * 40: b"x" * (S.MAX_FILE_BYTES + 1), "2" * 40: b"MIT"})
    assert files[0]["skipped"].startswith("size") and files[1]["bytes"] == 3 and files[2]["skipped"] == "not-fetched"
    with pytest.raises(ValueError):
        S.write_stage(tmp_path, [("../x", ok, None)], {"2" * 40: b"MIT"})


def test_read_list(tmp_path):
    p = tmp_path / "l.tsv"
    p.write_text("project\tclone_url\trun_commit\nx\thttps://h/x.git\t" + "a" * 40 + "\n")
    assert S.read_list(str(p))[0]["project"] == "x"
    for bad in ["x\thttps://h/x.git\tabc\n", "../x\thttps://h/x.git\t" + "a" * 40 + "\n", "x\t\t" + "a" * 40 + "\n"]:
        p.write_text("project\tclone_url\trun_commit\n" + bad)
        with pytest.raises(SystemExit):
            S.read_list(str(p))


# --- git fixtures ----------------------------------------------------------------------------------

def _git(cwd, *args):
    env = dict(os.environ, GIT_CONFIG_GLOBAL="/dev/null", GIT_CONFIG_NOSYSTEM="1", GIT_AUTHOR_NAME="t",
               GIT_AUTHOR_EMAIL="t.invalid", GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t.invalid")
    return subprocess.run(["git", "-C", str(cwd), *args], check=True, capture_output=True, env=env).stdout.decode()


@pytest.fixture
def origin(tmp_path):
    """A repository with key files, a symlinked LICENSE, a bundled license and GPL headers."""
    r = tmp_path / "origin"
    r.mkdir()
    _git(r, "init", "-q", "-b", "main")
    (r / "docs").mkdir()
    (r / "docs" / "LICENSE.txt").write_text(MIT_TEXT)
    os.symlink("docs/LICENSE.txt", r / "LICENSE")
    (r / "README.md").write_text("# x\n\nLicensed under the MIT license.\n")
    (r / "third_party" / "z").mkdir(parents=True)
    (r / "third_party" / "z" / "LICENSE").write_text("zlib license\n")
    (r / "src").mkdir()
    for i in range(3):
        (r / "src" / f"f{i}.c").write_text("/* either version 2 of the License, or (at your option) any later "
                                           "version. */\nint x;\n")
    (r / "package.json").write_text('{"name": "x", "license": "MIT"}\n')
    _git(r, "add", "-A")
    _git(r, "commit", "-q", "-m", "one")
    first = _git(r, "rev-parse", "HEAD").strip()
    (r / "README.md").write_text("# x\n\nchanged\n")
    _git(r, "commit", "-q", "-am", "two")
    _git(r, "config", "uploadpack.allowFilter", "true")
    _git(r, "config", "uploadpack.allowAnySHA1InWant", "true")
    return r, first


def test_fetch_commit_and_blobs(origin, tmp_path):
    r, first = origin
    repo = tmp_path / "w" / "repo"
    url = S.fetch_commit(repo, ["file:///nonexistent/x.git", f"file://{r}"], first)
    assert url == f"file://{r}"
    entries = S.ls_tree(repo, first)
    paths = {e.path: e for e in entries}
    assert paths["LICENSE"].mode == "120000"
    oid = paths["docs/LICENSE.txt"].oid
    assert S.missing_blobs(repo, [oid]) == {oid}
    S.fetch_blobs(repo, [oid])
    assert S.missing_blobs(repo, [oid]) == set()
    assert S.read_blobs(repo, [oid])[oid] == MIT_TEXT.encode()
    assert S.read_blobs(repo, [oid], limit=3)[oid] == b"MIT"
    assert S.read_blobs(repo, []) == {}
    S.fetch_blobs(repo, [])
    with pytest.raises(S.GitError):
        S.fetch_commit(tmp_path / "w2" / "repo", [f"file://{r}"], "0" * 40)


# --- stand-in tools --------------------------------------------------------------------------------

FAKE_SCANCODE = r'''
import json, os, sys
args = sys.argv[1:]
out = args[args.index("--json") + 1]
root = args[-1]
files, rules, lics = [], {}, {}
def add_lic(spdx, exc=False):
    lics[spdx] = {"key": spdx.lower(), "spdx_license_key": spdx, "is_exception": exc}
for dp, dn, fn in os.walk(root):
    for n in fn:
        p = os.path.join(dp, n)
        rel = os.path.relpath(p, root)
        text = open(p, errors="replace").read()
        dets, pkgs = [], []
        def det(spdx, rule, kind, matcher="2-aho", score=100.0):
            rules[rule] = {"identifier": rule, "is_license_" + kind: True}
            add_lic(spdx)
            dets.append({"license_expression_spdx": spdx, "matches": [{"license_expression_spdx": spdx,
                "license_expression": spdx.lower(), "matcher": matcher, "score": score, "match_coverage": score,
                "rule_identifier": rule, "start_line": 1, "end_line": 3, "matched_text": text[:200]}]})
        if "Permission is hereby granted" in text:
            det("MIT", "mit.LICENSE", "text", matcher=os.environ.get("FAKE_MATCHER", "2-aho"),
                score=float(os.environ.get("FAKE_SCORE", "100")))
        if "zlib license" in text:
            det("Zlib", "zlib.LICENSE", "text")
        if "Licensed under the MIT" in text:
            det("MIT", "mit_30.RULE", "reference")
        if "GNU GPL text" in text:
            det("GPL-2.0-only", "gpl-2.0.LICENSE", "text")
        if "any later" in text:
            det("GPL-2.0-or-later", "gpl-2.0-plus_1.RULE", "notice")
        if n == "package.json":
            pkgs.append({"declared_license_expression_spdx": "MIT", "extracted_license_statement": "MIT"})
        files.append({"path": rel, "type": "file", "license_detections": dets, "package_data": pkgs})
json.dump({"headers": [{"tool_version": "fake"}], "summary": {"declared_license_expression": "mit",
           "license_clarity_score": {"score": 90}}, "files": files,
           "license_rule_references": list(rules.values()), "license_references": list(lics.values())},
          open(out, "w"))
'''

FAKE_JAVA = r'''
import json, sys
for line in sys.stdin:
    path, ids = line.rstrip("\n").split("\t")
    for i in ids.split(","):
        print(json.dumps({"path": path, "id": i, "match": i == "MIT", "within": i == "MIT", "message": ""}))
'''

FAKE_ASKALONO = r'''
import json, sys
print(json.dumps({"path": sys.argv[-1], "result": {"score": 0.97, "license": {"name": "MIT"},
                  "containing": [{"score": 1.0, "license": {"name": "MIT"}}]}}))
'''

FAKE_LICENSEE = r'''
import json, sys
print(json.dumps({"licenses": [{"spdx_id": "MIT"}, {"spdx_id": "NOASSERTION"}],
                  "matched_files": [{"filename": "LICENSE", "matched_license": "MIT",
                                     "matcher": {"name": "exact", "confidence": 100}}]}))
'''


def _script(dirp: Path, name: str, body: str) -> str:
    p = dirp / name
    p.write_text(f"#!{sys.executable}\n" + textwrap.dedent(body))
    p.chmod(0o755)
    return str(p)


@pytest.fixture
def tools(tmp_path):
    b = tmp_path / "bin"
    b.mkdir()
    t = S.Tools(scancode=_script(b, "scancode", FAKE_SCANCODE), askalono=_script(b, "askalono", FAKE_ASKALONO),
                ruby=sys.executable, licensee=_script(b, "licensee", FAKE_LICENSEE), gem_home=str(b),
                java=_script(b, "java-runner", FAKE_JAVA), template_classpath="x")
    return t


def _fix_java(t: S.Tools) -> S.Tools:
    # run_templates calls: java -Xmx1g -cp CP SpdxTemplateDiff; the stand-in ignores its arguments
    return t


def _list(tmp_path, r, commit, name="p1"):
    lp = tmp_path / "list.tsv"
    lp.write_text("project\tclone_url\trun_commit\tfetch_urls\n" + f"{name}\tfile:///nonexistent\t{commit}\tfile://{r}\n")
    return lp


def test_scan_project_end_to_end(origin, tmp_path, tools):
    r, first = origin
    out = tmp_path / "out"
    row = {"project": "p1", "clone_url": f"file://{r}", "run_commit": first}
    res = S.scan_project(row, out, tools, tmp_path, keep_stage=True)
    assert res["status"] == "ok", res
    d = res["decision"]
    assert d["license"] == "MIT"
    assert d["confidence"] == "high"
    assert d["bundled_licenses"] == ["Zlib"]
    assert d["second_opinions"]["licensee"] == ["MIT"]
    assert d["second_opinions"]["askalono_agreement"] == "agree"
    pdir = out / "projects" / "p1"
    fetch = json.loads((pdir / "fetch.json").read_text())
    lic = [x for x in fetch["files"] if x["path"] == "LICENSE"][0]
    assert lic["symlink_of"] == "docs/LICENSE.txt"
    assert "symlink:LICENSE->docs/LICENSE.txt" in fetch["notes"]
    with tarfile.open(pdir / "stage.tar.gz") as t:
        names = set(t.getnames())
        assert t.extractfile("LICENSE").read().decode() == MIT_TEXT
    assert {"LICENSE", "README.md", "package.json", "docs/LICENSE.txt", "third_party/z/LICENSE"} <= names
    assert "src/f0.c" not in names
    assert (pdir / "stage" / "LICENSE").exists()
    with gzip.open(pdir / "scancode.json.gz", "rt") as g:
        assert json.load(g)["headers"][0]["tool_version"] == "fake"
    again = S.decide_stored(pdir)
    assert again["license"] == "MIT"


def test_scan_project_gpl_runs_header_pass_and_template(origin, tmp_path, tools, monkeypatch):
    r, first = origin
    (r / "docs" / "LICENSE.txt").write_text("GNU GPL text\n")
    _git(r, "commit", "-q", "-am", "gpl")
    commit = _git(r, "rev-parse", "HEAD").strip()
    res = S.scan_project({"project": "g", "clone_url": f"file://{r}", "run_commit": commit}, tmp_path / "o", tools,
                         tmp_path)
    assert res["status"] == "ok", res
    assert res["decision"]["license"] == "GPL-2.0-or-later"
    assert res["decision"]["version_choice_detail"]["GPL-2.0"]["level"] == "header"
    fetch = json.loads((tmp_path / "o" / "projects" / "g" / "fetch.json").read_text())
    assert fetch["header_pass"]["matched"] == 3 and fetch["header_pass"]["scanned"] == 3
    assert "headers" in res["times"]
    monkeypatch.setenv("FAKE_MATCHER", "3-seq")
    monkeypatch.setenv("FAKE_SCORE", "95")
    res = S.scan_project({"project": "m", "clone_url": f"file://{r}", "run_commit": first}, tmp_path / "o", tools,
                         tmp_path)
    assert res["decision"]["license"] == "MIT"
    assert res["decision"]["evidence"][0]["accepted_as"] == ["template-match"]
    tm = json.loads((tmp_path / "o" / "projects" / "m" / "templates.json").read_text())
    assert tm["LICENSE"]["MIT"]["match"] is True


def test_scan_project_fetch_error_and_no_scancode(origin, tmp_path):
    r, first = origin
    res = S.scan_project({"project": "x", "clone_url": "file:///nonexistent/x", "run_commit": first},
                         tmp_path / "o", S.Tools(), tmp_path)
    assert res["status"] == "fetch-error" and "nonexistent" in res["error"]
    res = S.scan_project({"project": "y", "clone_url": f"file://{r}", "run_commit": first}, tmp_path / "o",
                         S.Tools(), tmp_path)
    assert res["status"] == "fetched"


def test_scan_project_no_key_files(tmp_path, tools):
    r = tmp_path / "bare"
    r.mkdir()
    _git(r, "init", "-q")
    (r / "main.c").write_text("int main;\n")
    _git(r, "add", "-A")
    _git(r, "commit", "-q", "-m", "x")
    _git(r, "config", "uploadpack.allowFilter", "true")
    c = _git(r, "rev-parse", "HEAD").strip()
    res = S.scan_project({"project": "n", "clone_url": f"file://{r}", "run_commit": c}, tmp_path / "o", tools, tmp_path)
    assert res["status"] == "no-key-files"
    assert res["decision"]["license"] == "NOASSERTION"


def test_scan_project_tool_failure_is_an_error_row(origin, tmp_path):
    r, first = origin
    b = tmp_path / "b"
    b.mkdir()
    bad = S.Tools(scancode=_script(b, "sc", "import sys; sys.exit(2)\n"))
    res = S.scan_project({"project": "e", "clone_url": f"file://{r}", "run_commit": first}, tmp_path / "o", bad, tmp_path)
    assert res["status"] == "error" and "scancode rc=2" in res["error"]


def test_commands_scan_decide_collect(origin, tmp_path, tools, capsys):
    r, first = origin
    tj = tmp_path / "tools.json"
    tj.write_text(json.dumps({k: getattr(tools, k) for k in ("scancode", "askalono", "ruby", "licensee", "gem_home",
                                                              "java", "template_classpath")} | {"env": {"X": "1"}}))
    lp = _list(tmp_path, r, first)
    out = tmp_path / "out"
    assert S.main(["scan", "--list", str(lp), "--out-dir", str(out), "--tools", str(tj), "--jobs", "2"]) == 0
    assert (out / "projects" / "p1" / "result.json").exists()
    assert S.main(["scan", "--list", str(lp), "--out-dir", str(out), "--tools", str(tj)]) == 0
    assert "1 done, 0 to do" in capsys.readouterr().out
    assert S.main(["decide", "--out-dir", str(out)]) == 0
    assert S.main(["decide", "--out-dir", str(out), "--only", "zzz"]) == 0
    assert S.main(["collect", "--out-dir", str(out)]) == 0
    rows = (out / "results.tsv").read_text().splitlines()
    assert rows[0].split("\t") == S.TSV_COLS
    rec = dict(zip(S.TSV_COLS, rows[1].split("\t")))
    assert rec["license"] == "MIT" and rec["status"] == "ok" and rec["licensee_agreement"] == "agree"
    assert json.loads((out / "results.jsonl").read_text().splitlines()[0])["project"] == "p1"
    bad = tmp_path / "bad.tsv"
    bad.write_text("project\tclone_url\trun_commit\nz\tfile:///nonexistent\t" + "c" * 40 + "\n")
    assert S.main(["scan", "--list", str(bad), "--out-dir", str(out)]) == 0
    assert json.loads((out / "projects" / "z" / "result.json").read_text())["status"] == "fetch-error"
    assert S.main(["scan", "--list", str(bad), "--out-dir", str(out), "--redo-status", "fetch-error,error"]) == 0
    assert (out / "projects" / "z" / "result.json.try1").exists()
    assert (out / "projects" / "z" / "result.json").exists()
    stop = tmp_path / "STOP"
    stop.write_text("")
    lp2 = tmp_path / "l2.tsv"
    lp2.write_text("project\tclone_url\trun_commit\nq\tfile://x\t" + "b" * 40 + "\n")
    assert S.main(["scan", "--list", str(lp2), "--out-dir", str(out), "--stop-file", str(stop), "--only", "q"]) == 3


def test_tools_load_and_env(tmp_path):
    assert S.Tools.load(None).scancode is None
    p = tmp_path / "t.json"
    p.write_text(json.dumps({"scancode": "/x", "unknown_key": 1, "env": {"HOME": "/h"}}))
    t = S.Tools.load(str(p))
    assert t.scancode == "/x" and t.run_env()["HOME"] == "/h"


def test_run_licensee_and_askalono_errors(tmp_path):
    b = tmp_path / "b"
    b.mkdir()
    t = S.Tools(ruby=sys.executable, licensee=_script(b, "l", "import sys; sys.exit(5)\n"),
                askalono=_script(b, "a", "print('not json')\n"))
    assert "error" in S.run_licensee(t, tmp_path)
    t.licensee = _script(b, "l2", "print('no json')\n")
    assert S.run_licensee(t, tmp_path) == {"error": "no json"}
    f = tmp_path / "LICENSE"
    f.write_text("x")
    assert S.run_askalono(t, [f], tmp_path) == {}
    t.java = sys.executable
    assert S.run_templates(t, tmp_path, [("missing.txt", ["MIT"])]) == {}
