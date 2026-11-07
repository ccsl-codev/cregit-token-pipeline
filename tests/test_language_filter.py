"""Tests for language_filter.

Every repository here is synthetic: made with `git init` under tmp_path and
fetched by SHA, depth 1, through a file:// URL. The tests marked `needs_tokei`
run the real tokei and skip when it is not on PATH. The others use a stub tokei
that counts non-blank lines by extension, so the fetch, the removals and the
outputs are tested without it.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

import language_filter as lf

TOKEI = shutil.which("tokei")
needs_tokei = pytest.mark.skipif(TOKEI is None, reason="tokei is not on PATH")

QT_TS = ('<?xml version="1.0" encoding="utf-8"?>\n<!DOCTYPE TS>\n<TS version="2.1">\n'
         '<context>\n<name>Main</name>\n<message>\n<source>Open</source>\n'
         '<translation>Offnen</translation>\n</message>\n</context>\n</TS>\n')
TYPESCRIPT = "export function f(x: number): number {\n  return x + 1;\n}\n"
C_MAIN = "#include \"util.h\"\n\nint main(void)\n{\n    return util();\n}\n"
C_HEADER = "#pragma once\nint util(void);\n"
PYTHON = "def g():\n    return 1\n"
VENDORED_C = "".join(f"int v{i}(void) {{ return {i}; }}\n" for i in range(40))

STUB_TOKEI = """\
import json, os, sys
if "--version" in sys.argv:
    print("tokei 14.0.0 compiled with serialization support: json")
    sys.exit(0)
EXT = {".c": "C", ".h": "C Header", ".py": "Python", ".ts": "TypeScript",
       ".md": "Markdown", ".json": "JSON"}
out = {}
for top, _, files in os.walk("."):
    for f in files:
        lang = EXT.get(os.path.splitext(f)[1])
        if lang:
            with open(os.path.join(top, f), errors="ignore") as fh:
                n = sum(1 for line in fh if line.strip())
            out.setdefault(lang, {"code": 0, "children": {}})["code"] += n
out["Total"] = {"code": sum(v["code"] for v in out.values())}
print(json.dumps(out))
"""


def git(*args: str, cwd: Path) -> str:
    env = lf.git_env()
    env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull)
    p = subprocess.run(["git", "-c", "user.name=T", "-c", "user.email=t@example.org",
                        "-c", "init.defaultBranch=main", *args],
                       cwd=cwd, env=env, capture_output=True, text=True, check=True)
    return p.stdout.strip()


def write(root: Path, files: dict[str, str]) -> None:
    for rel, text in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)


@pytest.fixture
def repo(tmp_path):
    """A repository with two commits. The first is the pinned one; the second adds
    Python enough to change the largest language, so a fetch that silently took
    the tip would be seen."""
    src = tmp_path / "src"
    src.mkdir()
    git("init", "--quiet", cwd=src)
    write(src, {
        "src/main.c": C_MAIN,
        "src/util.h": C_HEADER,
        "i18n/app_de.ts": QT_TS,
        "web/app.ts": TYPESCRIPT,
        "tools/gen.py": PYTHON,
        "vendor/lib/big.c": VENDORED_C,
        "README.md": "# Example\n\nText.\n",
        "data.json": '{\n  "a": 1\n}\n',
    })
    git("add", ".", cwd=src)
    git("commit", "--quiet", "-m", "first", cwd=src)
    first = git("rev-parse", "HEAD", cwd=src)
    write(src, {"tools/more.py": "x = 1\n" * 200})
    git("add", ".", cwd=src)
    git("commit", "--quiet", "-m", "second", cwd=src)
    tip = git("rev-parse", "HEAD", cwd=src)
    return {"url": src.as_uri(), "first": first, "tip": tip, "path": src}


@pytest.fixture
def stub_tokei(tmp_path):
    path = tmp_path / "bin" / "tokei"
    path.parent.mkdir()
    path.write_text(f"#!{sys.executable}\n{STUB_TOKEI}")
    path.chmod(0o755)
    return str(path)


def manifest(tmp_path: Path, rows: list[tuple[str, str, str]]) -> Path:
    path = tmp_path / "in.tsv"
    lines = ["# name url category file_filter size_class commit"]
    for name, url, commit in rows:
        fields = [name, url, "community", "", "S"] + ([commit] if commit else [])
        lines.append("\t".join(fields))
    path.write_text("\n".join(lines) + "\n")
    return path


def read_tsv(path: Path) -> list[dict[str, str]]:
    lines = [x for x in path.read_text().splitlines() if not x.startswith("#")]
    header = lines[0].split("\t")
    return [dict(zip(header, x.split("\t"), strict=True)) for x in lines[1:]]


def options(**over) -> lf.Options:
    base = dict(fetch_cmd=lf.FETCH_CMD.split(), work_dir=None, tokei="tokei",
                vendored=frozenset(lf.VENDORED_DIRS), drop=frozenset(lf.DROP_LANGUAGES),
                timeout=600)
    base.update(over)
    return lf.Options(**base)


# --- the rule ---------------------------------------------------------------

TARGETS = frozenset(lf.TARGET_LANGUAGES)


def test_default_targets_are_the_tokenizer_languages():
    assert set(lf.TARGET_LANGUAGES) == {"C", "C++", "Java", "Rust"}


def test_largest_keeps_a_row_whose_largest_language_is_a_target():
    d = lf.decide({"C": 60, "Python": 40}, TARGETS, "largest", None)
    assert (d.kept, d.largest, d.share, d.reason) == (True, "C", 0.6, "")


def test_largest_drops_a_row_whose_largest_language_is_not_a_target():
    d = lf.decide({"C": 30, "C++": 30, "Python": 40}, TARGETS, "largest", None)
    assert (d.kept, d.largest, d.share) == (False, "Python", 0.6)
    assert d.reason == "largest language is Python"


def test_largest_breaks_a_tie_by_name():
    assert lf.decide({"Python": 50, "C": 50}, TARGETS, "largest", None).largest == "C"
    assert lf.decide({"Python": 50, "Zig": 50}, TARGETS, "largest", None).largest == "Python"


@pytest.mark.parametrize("threshold,kept", [(0.30, True), (0.60, True), (0.61, False)])
def test_share_compares_the_target_share_with_the_threshold(threshold, kept):
    d = lf.decide({"C": 30, "Rust": 30, "Python": 40}, TARGETS, "share", threshold)
    assert d.kept is kept
    assert d.share == pytest.approx(0.6)
    assert d.reason == ("" if kept else f"target share 0.6000 is below {threshold}")


def test_share_keeps_a_row_whose_largest_language_is_not_a_target():
    d = lf.decide({"C": 35, "Python": 65}, TARGETS, "share", 0.30)
    assert d.kept and d.largest == "Python"


def test_no_counted_code_is_dropped_with_a_reason():
    d = lf.decide({}, TARGETS, "largest", None)
    assert (d.kept, d.largest, d.share) == (False, "", None)
    assert d.reason == "no code lines in a counted language"


# --- the counts -------------------------------------------------------------

def test_headers_merge_into_their_language():
    report = {"C": {"code": 10}, "C Header": {"code": 5}, "C++": {"code": 7},
              "C++ Header": {"code": 3}, "Total": {"code": 25}}
    assert lf.code_lines(report, frozenset()) == {"C": 15, "C++": 10}


def test_a_header_alone_counts_as_its_language():
    assert lf.code_lines({"C Header": {"code": 4}}, frozenset()) == {"C": 4}


def test_dropped_languages_and_the_total_are_not_counted():
    report = {"Rust": {"code": 9}, "JSON": {"code": 500}, "Markdown": {"code": 80},
              "YAML": {"code": 20}, "Plain Text": {"code": 1}, "Total": {"code": 610}}
    assert lf.code_lines(report, frozenset(lf.DROP_LANGUAGES)) == {"Rust": 9}


def test_embedded_children_are_not_counted():
    report = {"Markdown": {"code": 0, "children": {"Rust": [{"stats": {"code": 30}}]}}}
    assert lf.code_lines(report, frozenset()) == {}


def test_the_drop_list_holds_the_named_formats():
    must = {"JSON", "YAML", "TOML", "XML", "Markdown", "Plain Text", "HTML", "CSS", "SVG",
            "ReStructuredText", "AsciiDoc", "INI"}
    assert must <= set(lf.DROP_LANGUAGES)
    assert not set(lf.DROP_LANGUAGES) & set(lf.TARGET_LANGUAGES)


# --- the Qt .ts rule --------------------------------------------------------

@pytest.mark.parametrize("head", [
    b'<?xml version="1.0" encoding="utf-8"?>\n<!DOCTYPE TS>\n<TS version="2.1">',
    b"<!DOCTYPE TS>\n<TS>",
    b'<TS version="2.0" language="de">',
    b"<TS>\n",
    b'\xef\xbb\xbf<?xml version="1.0"?>',
    b'\n\n  \t<?xml version="1.0"?>',
    b'\xef\xbb\xbf  \n<TS version="2.1">',
    b"\xff\xfe" + '<?xml version="1.0"?>'.encode("utf-16-le"),
    b"\xfe\xff" + '\n<TS version="2.1">'.encode("utf-16-be"),
])
def test_qt_translation_files_are_detected(head):
    assert lf.is_qt_ts(head)


@pytest.mark.parametrize("head", [
    b"export const x = 1;\n",
    b"import { a } from './a';\n<?xml",
    b"// <TS>\n",
    b"<TSomething>value;\n",
    b"",
    b"\xef\xbb\xbf/// <reference path='x.d.ts' />\n",
])
def test_typescript_is_not_taken_for_a_translation_file(head):
    assert not lf.is_qt_ts(head)


def test_remove_qt_ts_deletes_only_translation_files(tmp_path):
    write(tmp_path, {"a/app_de.ts": QT_TS, "a/app.ts": TYPESCRIPT, "b/UPPER.TS": QT_TS,
                     "b/notes.xml": QT_TS})
    (tmp_path / "link.ts").symlink_to(tmp_path / "a" / "app_de.ts")
    assert lf.remove_qt_ts(tmp_path) == 2
    left = sorted(str(p.relative_to(tmp_path)) for p in tmp_path.rglob("*") if p.is_file())
    assert left == ["a/app.ts", "b/notes.xml"]
    assert (tmp_path / "link.ts").is_symlink()


# --- the vendored-path rule -------------------------------------------------

def test_vendored_directories_are_removed_at_any_depth(tmp_path):
    write(tmp_path, {"vendor/a.c": "x", "src/third_party/b.c": "x", "src/Deps/c.c": "x",
                     "lib/node_modules/d/e.js": "x", "external/f.c": "x",
                     "third-party/g.c": "x", "src/keep.c": "x", "src/vendors/h.c": "x",
                     "src/vendor.c": "x"})
    (tmp_path / "src" / "real").mkdir()
    (tmp_path / "deps").symlink_to(tmp_path / "src" / "real")
    assert lf.remove_vendored(tmp_path, frozenset(lf.VENDORED_DIRS)) == 7
    left = sorted(str(p.relative_to(tmp_path)) for p in tmp_path.rglob("*") if p.is_file())
    assert left == ["src/keep.c", "src/vendor.c", "src/vendors/h.c"]
    assert (tmp_path / "src" / "real").is_dir()


def test_an_empty_vendored_list_removes_nothing(tmp_path):
    write(tmp_path, {"vendor/a.c": "x"})
    assert lf.remove_vendored(tmp_path, frozenset()) == 0
    assert (tmp_path / "vendor" / "a.c").exists()


# --- the manifest -----------------------------------------------------------

def test_read_manifest_takes_the_sixth_field_as_the_commit(tmp_path):
    sha = "a" * 40
    path = manifest(tmp_path, [("one", "https://h/one.git", sha), ("two", "https://h/two", "")])
    rows = lf.read_manifest(path)
    assert [(r.name, r.url, r.commit) for r in rows] == [
        ("one", "https://h/one.git", sha), ("two", "https://h/two", "")]
    assert rows[0].line == f"one\thttps://h/one.git\tcommunity\t\tS\t{sha}"


@pytest.mark.parametrize("line,msg", [
    ("a\tb\tc", "expected 5"),
    ("a\tb\tc\td\tS\t" + "A" * 40, "40 lowercase hex"),
    ("a\tb\tc\td\tS\tdeadbeef", "40 lowercase hex"),
])
def test_read_manifest_rejects_a_malformed_row(tmp_path, line, msg):
    path = tmp_path / "bad.tsv"
    path.write_text(line + "\n")
    with pytest.raises(ValueError, match=msg):
        lf.read_manifest(path)


# --- fetch ------------------------------------------------------------------

def test_the_default_fetch_takes_one_commit_with_depth_one():
    assert "--depth 1" in lf.FETCH_CMD and "{commit}" in lf.FETCH_CMD


def test_classify_fetch_error():
    assert lf.classify_fetch_error(
        "error: Server does not allow request for unadvertised object abc") == lf.FETCH_REFUSED
    assert lf.classify_fetch_error(
        "fatal: remote error: upload-pack: not our ref abc") == lf.FETCH_REFUSED
    assert lf.classify_fetch_error("fatal: Could not resolve host: h") == lf.FETCH_FAILED


def test_first_line_skips_blank_lines():
    assert lf.first_line("\n  \nfatal: x\nmore\n") == "fatal: x"
    assert lf.first_line("\n \n") == ""


def test_fetch_takes_the_pinned_commit_and_not_the_tip(repo, tmp_path):
    dest = tmp_path / "dest"
    dest.mkdir()
    assert lf.fetch_commit(repo["url"], repo["first"], dest, options()) == (lf.MEASURED, "")
    assert (dest / "src" / "main.c").read_text() == C_MAIN
    assert not (dest / "tools" / "more.py").exists()
    assert not (dest / ".git").exists()


def test_the_fetch_is_shallow(repo, tmp_path, monkeypatch):
    monkeypatch.setattr(lf.shutil, "rmtree", lambda path: None)
    dest = tmp_path / "dest"
    dest.mkdir()
    assert lf.fetch_commit(repo["url"], repo["tip"], dest, options())[0] == lf.MEASURED
    assert git("rev-parse", "--is-shallow-repository", cwd=dest) == "true"
    assert git("rev-list", "--all", "--count", cwd=dest) == "1"


def test_a_host_that_refuses_a_fetch_by_sha_is_reported(repo, tmp_path):
    dest = tmp_path / "dest"
    dest.mkdir()
    v0 = options(fetch_cmd="git -c protocol.version=0 fetch --depth 1 {url} {commit}".split())
    status, detail = lf.fetch_commit(repo["url"], repo["first"], dest, v0)
    assert status == lf.FETCH_REFUSED
    assert "unadvertised" in detail


def test_a_missing_repository_is_a_fetch_failure(tmp_path):
    dest = tmp_path / "dest"
    dest.mkdir()
    status, detail = lf.fetch_commit((tmp_path / "none").as_uri(), "1" * 40, dest, options())
    assert status == lf.FETCH_FAILED and detail


def test_a_fetch_that_brings_another_commit_fails_the_checkout(repo, tmp_path):
    dest = tmp_path / "dest"
    dest.mkdir()
    wrong = options(fetch_cmd=["git", "fetch", "--depth", "1", "{url}", repo["tip"]])
    assert lf.fetch_commit(repo["url"], repo["first"], dest, wrong)[0] == lf.CHECKOUT_FAILED


def test_a_fetch_that_runs_too_long_times_out(repo, tmp_path):
    dest = tmp_path / "dest"
    dest.mkdir()
    slow = options(fetch_cmd=[sys.executable, "-c", "import time; time.sleep(5)", "{url}",
                              "{commit}"], timeout=1)
    assert lf.fetch_commit(repo["url"], repo["first"], dest, slow)[0] == lf.TIMED_OUT


def test_git_env_drops_the_variables_that_redirect_git(monkeypatch):
    monkeypatch.setenv("GIT_DIR", "/elsewhere")
    monkeypatch.setenv("GIT_WORK_TREE", "/elsewhere")
    env = lf.git_env()
    assert "GIT_DIR" not in env and "GIT_WORK_TREE" not in env
    assert env["GIT_TERMINAL_PROMPT"] == "0"


# --- one row, with the stub tokei -------------------------------------------

def test_measure_row_counts_and_deletes_its_folder(repo, tmp_path, stub_tokei):
    work = tmp_path / "work"
    row = lf.Row("p", repo["url"], repo["first"], "")
    m = lf.measure_row(row, options(tokei=stub_tokei, work_dir=work))
    assert m.status == lf.MEASURED
    assert m.code == {"C": 7, "TypeScript": 3, "Python": 2}
    assert (m.qt_ts_removed, m.vendored_dirs_removed) == (1, 1)
    assert list(work.iterdir()) == []


def test_measure_row_without_a_commit_fetches_nothing(tmp_path):
    m = lf.measure_row(lf.Row("p", "file:///nowhere", "", ""), options(work_dir=tmp_path))
    assert m.status == lf.NO_COMMIT
    assert list(tmp_path.iterdir()) == []


def test_measure_row_reports_a_tokei_failure(repo, tmp_path):
    bad = tmp_path / "bad-tokei"
    bad.write_text("#!/bin/sh\necho broken >&2\nexit 3\n")
    bad.chmod(0o755)
    m = lf.measure_row(lf.Row("p", repo["url"], repo["first"], ""), options(tokei=str(bad)))
    assert (m.status, m.detail) == (lf.TOKEI_FAILED, "broken")


def test_measure_row_reports_tokei_output_that_is_not_json(repo, tmp_path):
    bad = tmp_path / "bad-tokei"
    bad.write_text("#!/bin/sh\necho not json\n")
    bad.chmod(0o755)
    m = lf.measure_row(lf.Row("p", repo["url"], repo["first"], ""), options(tokei=str(bad)))
    assert m.status == lf.TOKEI_FAILED


def test_measure_row_reports_a_tokei_that_runs_too_long(repo, tmp_path):
    slow = tmp_path / "slow-tokei"
    slow.write_text("#!/bin/sh\nsleep 5\n")
    slow.chmod(0o755)
    m = lf.measure_row(lf.Row("p", repo["url"], repo["first"], ""),
                       options(tokei=str(slow), timeout=1))
    assert m.status == lf.TIMED_OUT


# --- the command line -------------------------------------------------------

def run_main(tmp_path, rows, *extra):
    path = manifest(tmp_path, rows)
    out = tmp_path / "out"
    rc = lf.main(["--manifest", str(path), "--out-dir", str(out), *extra])
    return rc, out, path


def test_main_writes_the_three_files(repo, tmp_path, stub_tokei, capsys):
    rows = [("pinned", repo["url"], repo["first"]),
            ("unpinned", repo["url"], ""),
            ("gone", (tmp_path / "gone").as_uri(), "2" * 40)]
    rc, out, path = run_main(tmp_path, rows, "--tokei", stub_tokei,
                             "--work-dir", str(tmp_path / "work"), "--jobs", "2")
    assert rc == 1
    table = {r["name"]: r for r in read_tsv(out / "languages.tsv")}
    p = table["pinned"]
    assert (p["status"], p["kept"], p["largest_language"]) == (lf.MEASURED, "yes", "C")
    assert (p["code:C"], p["code:TypeScript"], p["code:Python"]) == ("7", "3", "2")
    assert (p["target_share"], p["total_code"]) == ("0.5833", "12")
    assert (p["qt_ts_removed"], p["vendored_dirs_removed"]) == ("1", "1")
    assert p["tokei_version"] == "14.0.0"
    assert table["unpinned"]["status"] == lf.NO_COMMIT
    assert table["gone"]["status"] == lf.FETCH_FAILED
    assert "code:Markdown" not in p and "code:JSON" not in p

    kept = (out / "kept.tsv").read_text().splitlines()
    assert kept[0].startswith("# kept by language_filter.py, mode largest")
    assert kept[1:] == [path.read_text().splitlines()[1]]
    dropped = {r["name"]: r["reason"] for r in read_tsv(out / "dropped.tsv")}
    assert dropped["unpinned"] == lf.NO_COMMIT
    assert dropped["gone"].startswith(lf.FETCH_FAILED + " (")
    assert list((tmp_path / "work").iterdir()) == []
    assert "1 kept, 2 dropped (2 not measured)" in capsys.readouterr().err


def test_main_share_mode(repo, tmp_path, stub_tokei):
    rows = [("pinned", repo["url"], repo["first"])]
    rc, out, _ = run_main(tmp_path, rows, "--tokei", stub_tokei, "--mode", "share",
                          "--threshold", "0.60")
    assert rc == 0
    assert (out / "kept.tsv").read_text().count("\n") == 1
    assert read_tsv(out / "dropped.tsv")[0]["reason"] == "target share 0.5833 is below 0.6"


def test_main_reports_a_refused_fetch_and_does_not_fall_back(repo, tmp_path, stub_tokei):
    rows = [("pinned", repo["url"], repo["first"])]
    rc, out, _ = run_main(tmp_path, rows, "--tokei", stub_tokei, "--fetch-cmd",
                          "git -c protocol.version=0 fetch --depth 1 {url} {commit}")
    assert rc == 1
    row = read_tsv(out / "languages.tsv")[0]
    assert (row["status"], row["kept"], row["total_code"]) == (lf.FETCH_REFUSED, "no", "")
    assert read_tsv(out / "dropped.tsv")[0]["reason"].startswith(lf.FETCH_REFUSED)


def test_main_vendored_dirs_and_drop_list_are_configurable(repo, tmp_path, stub_tokei):
    rows = [("pinned", repo["url"], repo["first"])]
    rc, out, _ = run_main(tmp_path, rows, "--tokei", stub_tokei, "--vendored-dirs", "",
                          "--drop-languages", "JSON")
    row = read_tsv(out / "languages.tsv")[0]
    assert rc == 0
    assert (row["code:C"], row["code:Markdown"], row["vendored_dirs_removed"]) == ("47", "2", "0")
    assert "code:JSON" not in row


def test_main_warns_on_another_tokei_major(repo, tmp_path, stub_tokei, capsys):
    other = Path(stub_tokei)
    other.write_text(other.read_text().replace("tokei 14.0.0", "tokei 15.0.0"))
    rc, _, _ = run_main(tmp_path, [("pinned", repo["url"], repo["first"])], "--tokei", str(other))
    assert rc == 0
    assert "warning: tokei 15.0.0, not 14.x" in capsys.readouterr().err


def test_main_exits_2_without_tokei(tmp_path, capsys):
    rc, _, _ = run_main(tmp_path, [], "--tokei", str(tmp_path / "no-such-tokei"))
    assert rc == 2
    assert "cannot run" in capsys.readouterr().err


def test_main_exits_2_on_a_program_that_is_not_tokei(tmp_path, capsys):
    rc, _, _ = run_main(tmp_path, [], "--tokei", "true")
    assert rc == 2
    assert "not a tokei version" in capsys.readouterr().err


def test_main_exits_2_on_a_malformed_manifest(tmp_path, stub_tokei):
    path = tmp_path / "bad.tsv"
    path.write_text("a\tb\n")
    assert lf.main(["--manifest", str(path), "--out-dir", str(tmp_path / "o"),
                    "--tokei", stub_tokei]) == 2


@pytest.mark.parametrize("extra,msg", [
    (["--mode", "share"], "needs --threshold"),
    (["--mode", "share", "--threshold", "0"], "needs --threshold"),
    (["--mode", "share", "--threshold", "1.5"], "needs --threshold"),
    (["--threshold", "0.3"], "applies only to --mode share"),
    (["--targets", ""], "--targets is empty"),
    (["--targets", "C,C Header"], "merged or dropped"),
    (["--targets", "C,JSON"], "merged or dropped"),
    (["--fetch-cmd", "git fetch {url}"], "must contain"),
    (["--jobs", "0"], "at least 1"),
])
def test_bad_options_are_usage_errors(tmp_path, capsys, extra, msg):
    with pytest.raises(SystemExit) as e:
        lf.parse_args(["--manifest", "m", "--out-dir", "o", *extra])
    assert e.value.code == 2
    assert msg in capsys.readouterr().err


def test_the_script_runs_from_the_command_line(tmp_path):
    script = Path(lf.__file__)
    assert os.access(script, os.X_OK)
    p = subprocess.run([sys.executable, str(script), "--help"], capture_output=True, text=True)
    assert p.returncode == 0 and "--fetch-cmd" in p.stdout


# --- the real tokei ---------------------------------------------------------

@needs_tokei
def test_real_tokei_is_version_14():
    assert lf.tokei_version(TOKEI).startswith("14.")


@needs_tokei
def test_every_configured_language_name_is_a_tokei_language():
    listing = subprocess.run([TOKEI, "--languages"], capture_output=True, text=True,
                             check=True).stdout
    names = set(lf.DROP_LANGUAGES) | set(lf.TARGET_LANGUAGES) | set(lf.HEADER_MERGE)
    missing = [n for n in names if not re.search(rf"^\W*{re.escape(n)}\s\s", listing, re.M)]
    assert missing == []


@needs_tokei
def test_real_tokei_counts_a_qt_translation_file_as_typescript(tmp_path):
    write(tmp_path, {"app_de.ts": QT_TS})
    report = json.loads(subprocess.run([TOKEI, "--output", "json", "."], cwd=tmp_path,
                                       capture_output=True, text=True, check=True).stdout)
    assert report["TypeScript"]["code"] > 0


@needs_tokei
def test_end_to_end_with_real_tokei(repo, tmp_path):
    rows = [("pinned", repo["url"], repo["first"]), ("tip", repo["url"], repo["tip"])]
    rc, out, path = run_main(tmp_path, rows, "--work-dir", str(tmp_path / "work"),
                             "--tokei", TOKEI)
    assert rc == 0
    table = {r["name"]: r for r in read_tsv(out / "languages.tsv")}
    pinned = table["pinned"]
    # main.c has 5 code lines, util.h 2; the vendored big.c is gone.
    assert (pinned["code:C"], pinned["code:TypeScript"], pinned["code:Python"]) == ("7", "3", "2")
    assert (pinned["largest_language"], pinned["kept"]) == ("C", "yes")
    assert (pinned["qt_ts_removed"], pinned["vendored_dirs_removed"]) == ("1", "1")
    assert pinned["tokei_version"].startswith("14.")
    assert not any(k in pinned for k in ("code:C Header", "code:Markdown", "code:JSON"))
    tip = table["tip"]
    assert (tip["largest_language"], tip["kept"]) == ("Python", "no")
    assert (out / "kept.tsv").read_text().splitlines()[1:] == [path.read_text().splitlines()[1]]
    assert read_tsv(out / "dropped.tsv") == [
        {"name": "tip", "url": repo["url"], "commit": repo["tip"],
         "reason": "largest language is Python"}]
    assert list((tmp_path / "work").iterdir()) == []
