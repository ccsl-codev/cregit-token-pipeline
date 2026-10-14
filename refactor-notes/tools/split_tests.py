"""split_tests.py: split tests/test_ctp.py into tests/ctplib/test_<module>.py by section.

Shared helpers go to tests/ctplib/fakes.py, shared fixtures to tests/ctplib/conftest.py.
A helper or fixture that only one target file uses stays in that file.
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from mover import global_refs, span  # noqa: E402

SRC = Path("tests/test_ctp.py")
DEST = Path("tests/ctplib")

TARGET = {
    "helpers": "fakes",
    "fixtures": "conftest",
    "manifest parsing": "test_manifest_file",
    "script_supports": "test_runner_args",
    "idempotence and locking": "test_project_steps",
    "flag handling": "test_runner_args",
    "--memo-dir": "test_workdir_state",
    "argument construction": "test_runner_args",
    "metrics ledger": "test_metrics",
    "log files and the -latest symlink": "test_phases",
    "return states": "test_project_steps",
    "devenv capture": "test_toolchain",
    "cmd_run": "test_passes",
    "refusals before the run starts": "test_preflight",
    "heartbeat": "test_passes",
    "cmd_status": "test_report",
    "cmd_db and main": "test_cli",
    "resource sampling": "test_resources",
    "sharding": "test_runner_args",
    "step 10 memory budget": "test_memory",
    "--project-meta": "test_preflight",
    "the provenance guard": "test_preflight",
    "cmd_progress": "test_report",
    "the universal mask and --mask": "test_runner_args",
    "pinned commit": "test_pinning",
    "audit ledger": "test_project_run",
    "schema check and join": "test_project_steps",
    "cleanup, --drop-html and --anonymize": "test_project_steps",
    "census: order, gates, stop, resume, retries": "test_scheduler",
    "CTP_CONFIG": "test_config",
    "census resumes an interrupted project at step 2": "test_workdir_state",
    "--latest": "test_remote_head",
    "--tokenizer-worker": "test_runner_args",
}

DOCS = {
    "fakes": "Fakes and constants that the ctplib tests share.",
    "test_manifest_file": "Tests for ctplib/manifest_file.py: reading the manifest.",
    "test_runner_args": "Tests for ctplib/runner_args.py: the arguments ctp gives the runner.",
    "test_project_steps": "Tests for ctplib/project_steps.py: the steps of one project, the lock, "
    "the outcomes, schema, join and cleanup.",
    "test_workdir_state": "Tests for ctplib/workdir_state.py: --memo-dir, and how an attempt "
    "resumes from the workdir an earlier attempt left.",
    "test_metrics": "Tests for ctplib/metrics.py: the metrics.tsv rows.",
    "test_phases": "Tests for ctplib/phases.py: the log files and the -latest symlink.",
    "test_toolchain": "Tests for ctplib/toolchain.py: the devenv capture.",
    "test_passes": "Tests for ctplib/passes.py: `ctp.py run` and the heartbeat.",
    "test_preflight": "Tests for ctplib/preflight.py: the refusals before a run and the "
    "provenance guard.",
    "test_report": "Tests for ctplib/report.py: status and progress.",
    "test_cli": "Tests for ctplib/cli.py and report.cmd_db: the dispatch of the subcommands.",
    "test_resources": "Tests for ctplib/resources.py: the resource sampler.",
    "test_memory": "Tests for ctplib/memory.py: the memory budget of step 10.",
    "test_pinning": "Tests for the pinned commit: ctplib/pinning.py and the clone step.",
    "test_project_run": "Tests for ctplib/project_run.py: the ledger rows of each step, and audit.",
    "test_scheduler": "Tests for ctplib/scheduler.py: order, gates, stop, resume and retries.",
    "test_config": "Tests for ctplib/config.py: CTP_CONFIG.",
    "test_remote_head": "Tests for ctplib/remote_head.py: --latest.",
}


def sections(lines: list[str]) -> list[tuple[int, str, tuple[int, int]]]:
    """(first line 1-based, title, header line span 0-based [s, e)) of each section."""
    out = []
    for i, line in enumerate(lines):
        if (
            re.match(r"^# -{20,}", line)
            and i + 2 < len(lines)
            and lines[i + 1].startswith("# ")
            and re.match(r"^# -{20,}", lines[i + 2])
        ):
            out.append((i + 1, lines[i + 1][2:].strip(), (i, i + 3)))
        elif line.startswith("# --latest:") or line.startswith("# --tokenizer-worker:"):
            title = line[2:].split(":")[0]
            out.append((i + 1, title, (i, i + 1)))
    return out


def defined(stmt) -> set[str]:
    if isinstance(stmt, (ast.FunctionDef, ast.ClassDef)):
        return {stmt.name}
    if isinstance(stmt, ast.Assign):
        return {n.id for t in stmt.targets for n in ast.walk(t) if isinstance(n, ast.Name)}
    if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
        return {stmt.target.id}
    return set()


def is_fixture(stmt) -> bool:
    if not isinstance(stmt, ast.FunctionDef):
        return False
    for d in stmt.decorator_list:
        txt = ast.unparse(d)
        if txt.startswith("pytest.fixture"):
            return True
    return False


def uses(stmt) -> set[str]:
    """Global names and fixture names a statement uses."""
    names = {n.id for n in global_refs(stmt)}
    if isinstance(stmt, ast.FunctionDef) and (stmt.name.startswith("test_") or is_fixture(stmt)):
        names |= {a.arg for a in stmt.args.args}
    return names


def main() -> None:
    text = SRC.read_text()
    lines = text.splitlines(keepends=True)
    tree = ast.parse(text)
    secs = sections([line.rstrip("\n") for line in lines])
    first_sec = secs[0][0]

    def section_of(lineno: int) -> str | None:
        found = None
        for start, title, _ in secs:
            if lineno >= start:
                found = title
        return found

    head, body = [], []
    for stmt in tree.body:
        (head if stmt.lineno < first_sec else body).append(stmt)
    imports = [s for s in head if isinstance(s, (ast.Import, ast.ImportFrom))]
    head_other = [s for s in head if not isinstance(s, (ast.Import, ast.ImportFrom, ast.Expr))]
    module_doc = ast.get_docstring(tree, clean=False)

    target = {}
    for s in head_other:
        target[id(s)] = "fakes"
    for s in body:
        t = section_of(s.lineno)
        target[id(s)] = TARGET[t]

    defs = {}
    for s in head_other + body:
        for name in defined(s):
            defs[name] = s

    # Move a definition that more than one file uses into fakes or conftest; repeat.
    changed = True
    while changed:
        changed = False
        users: dict[str, set[str]] = {}
        for s in head_other + body:
            for name in uses(s):
                if name in defs and defs[name] is not s:
                    users.setdefault(name, set()).add(target[id(s)])
        for name, files in users.items():
            s = defs[name]
            here = target[id(s)]
            if here in ("fakes", "conftest"):
                # Something shared needs it: it must be shared too.
                continue
            others = files - {here}
            if others:
                target[id(s)] = "conftest" if is_fixture(s) else "fakes"
                changed = True
        # A shared statement must not depend on a file-local one.
        for s in head_other + body:
            if target[id(s)] in ("fakes", "conftest"):
                for name in uses(s):
                    if name in defs:
                        d = defs[name]
                        want = "conftest" if is_fixture(d) else "fakes"
                        if target[id(d)] not in ("fakes", "conftest"):
                            target[id(d)] = want
                            changed = True
                        if target[id(s)] == "fakes" and target[id(d)] == "conftest":
                            sys.exit(f"fakes needs fixture {name}")

    files: dict[str, list] = {}
    for s in head_other + body:
        files.setdefault(target[id(s)], []).append(s)

    import_text = "".join(
        "".join(lines[s.lineno - 1 : s.end_lineno]) for s in imports
    )
    fakes_names = sorted(
        n for s in files.get("fakes", []) for n in defined(s) if not is_fixture(s)
    )

    DEST.mkdir(parents=True, exist_ok=True)
    for fname, stmts in files.items():
        seen_secs = set()
        chunks = []
        for s in stmts:
            title = section_of(s.lineno)
            if title and title not in seen_secs and fname not in ("fakes", "conftest"):
                seen_secs.add(title)
                for start, t, (hs, he) in secs:
                    if t == title:
                        chunks.append("".join(lines[hs:he]).rstrip("\n"))
            a, b = span(s, lines)
            chunks.append("".join(lines[a:b]).rstrip("\n"))
        used = set().union(*(uses(s) for s in stmts))
        from_fakes = sorted(n for n in fakes_names if n in used) if fname != "fakes" else []
        if fname == "conftest":
            doc = module_doc
        else:
            doc = DOCS[fname]
        out = f'"""{doc}"""\n\n{import_text}'
        if from_fakes:
            out += f"from fakes import {', '.join(from_fakes)}\n"
        out += "\n\n" + "\n\n\n".join(chunks) + "\n"
        (DEST / f"{fname}.py").write_text(out)
        print(f"{fname}.py: {len(stmts)} statements")


if __name__ == "__main__":
    main()
