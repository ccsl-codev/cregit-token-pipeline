"""mover.py: move top-level definitions from one module into a ctplib module, mechanically.

usage: mover.py SRC DEST_MODULE --doc DOC [--rename OLD=NEW ...] [--tests FILE ...] NAME ...

- Takes each named top-level statement of SRC (with decorators and the comment block
  directly above it) and appends it to ctplib/DEST_MODULE.py, in source order.
- In the moved code, imports it needs are copied from SRC. A reference to a name that
  stays in SRC is an error (it would make an import cycle).
- In what stays in SRC, every global reference to a moved name becomes DEST.NAME.
- In each test file, `ctp.NAME` and `setattr(ctp, "NAME"` point at DEST.
Scope-aware: a local variable or parameter with the same name is not touched.
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
from pathlib import Path

SCOPES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)
COMPS = (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)


def defined_names(stmt: ast.stmt) -> set[str]:
    if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return {stmt.name}
    if isinstance(stmt, ast.Assign):
        return {n.id for t in stmt.targets for n in ast.walk(t) if isinstance(n, ast.Name)}
    if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
        return {stmt.target.id}
    return set()


def _walk_no_scope(node):
    """Yield the nodes of node's body without entering nested function scopes."""
    for child in ast.iter_child_nodes(node):
        yield child
        if isinstance(child, SCOPES + (ast.ClassDef,)):
            continue
        yield from _walk_no_scope(child)


def scope_bound(fn) -> set[str]:
    a = fn.args
    bound = {x.arg for x in a.posonlyargs + a.args + a.kwonlyargs}
    bound |= {x.arg for x in (a.vararg, a.kwarg) if x}
    if isinstance(fn, ast.Lambda):
        return bound
    declared_global = set()
    for n in _walk_no_scope(fn):
        if isinstance(n, ast.Global):
            declared_global |= set(n.names)
        elif isinstance(n, ast.Name) and isinstance(n.ctx, (ast.Store, ast.Del)):
            bound.add(n.id)
        elif isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            bound.add(n.name)
        elif isinstance(n, (ast.Import, ast.ImportFrom)):
            bound |= {(al.asname or al.name).split(".")[0] for al in n.names}
        elif isinstance(n, ast.ExceptHandler) and n.name:
            bound.add(n.name)
    return bound - declared_global


def comp_bound(comp) -> set[str]:
    out = set()
    for g in comp.generators:
        out |= {n.id for n in ast.walk(g.target) if isinstance(n, ast.Name)}
    return out


def global_refs(node, bound_stack=()):
    """Yield every Name node under node that refers to a module-level name."""
    if isinstance(node, SCOPES):
        # Decorators, defaults and annotations evaluate in the enclosing scope.
        outer = []
        if not isinstance(node, ast.Lambda):
            outer += node.decorator_list
            if node.returns:
                outer.append(node.returns)
            for arg in node.args.posonlyargs + node.args.args + node.args.kwonlyargs + [
                node.args.vararg,
                node.args.kwarg,
            ]:
                if arg is not None and arg.annotation is not None:
                    outer.append(arg.annotation)
        outer += [d for d in node.args.defaults + node.args.kw_defaults if d is not None]
        for o in outer:
            yield from global_refs(o, bound_stack)
        inner = bound_stack + (scope_bound(node),)
        body = node.body if isinstance(node.body, list) else [node.body]
        for stmt in body:
            yield from global_refs(stmt, inner)
        return
    if isinstance(node, COMPS):
        inner = bound_stack + (comp_bound(node),)
        for child in ast.iter_child_nodes(node):
            yield from global_refs(child, inner)
        return
    if isinstance(node, ast.Name):
        if not any(node.id in b for b in bound_stack):
            yield node
        return
    for child in ast.iter_child_nodes(node):
        yield from global_refs(child, bound_stack)


def span(stmt: ast.stmt, lines: list[str]) -> tuple[int, int]:
    """0-based [start, end) line span, with decorators and the comment block above."""
    start = min([stmt.lineno] + [d.lineno for d in getattr(stmt, "decorator_list", [])]) - 1
    while start > 0 and lines[start - 1].lstrip().startswith("#"):
        start -= 1
    return start, stmt.end_lineno


def rewrite(text: str, edits: list[tuple[int, int, int, str]]) -> str:
    """edits: (lineno 1-based, col, end_col, replacement), all on one line each."""
    lines = text.splitlines(keepends=True)
    for lineno, col, end, rep in sorted(edits, reverse=True):
        line = lines[lineno - 1]
        b = line.encode()
        lines[lineno - 1] = (b[:col] + rep.encode() + b[end:]).decode()
    return "".join(lines)


def import_lines(tree: ast.Module, used: set[str]) -> list[str]:
    out = []
    for stmt in tree.body:
        if isinstance(stmt, ast.Import):
            for al in stmt.names:
                bound = (al.asname or al.name).split(".")[0]
                if bound in used:
                    out.append(f"import {al.name}" + (f" as {al.asname}" if al.asname else ""))
        elif isinstance(stmt, ast.ImportFrom):
            if stmt.module == "__future__":
                continue
            for al in stmt.names:
                bound = al.asname or al.name
                if bound in used:
                    mod = "." * stmt.level + (stmt.module or "")
                    out.append(
                        f"from {mod} import {al.name}" + (f" as {al.asname}" if al.asname else "")
                    )
    return out


def module_imported_names(tree: ast.Module) -> set[str]:
    names = set()
    for stmt in tree.body:
        if isinstance(stmt, ast.Import):
            names |= {(al.asname or al.name).split(".")[0] for al in stmt.names}
        elif isinstance(stmt, ast.ImportFrom):
            names |= {al.asname or al.name for al in stmt.names}
    return names


def add_import(text: str, line: str) -> str:
    """Insert line after the last top-level import; ruff sorts the block later."""
    if line in text.splitlines():
        return text
    tree = ast.parse(text)
    m = re.match(r"from (\S+) import (\w+)$", line)
    if m:
        for stmt in tree.body:
            if isinstance(stmt, ast.ImportFrom) and stmt.module == m.group(1):
                if any((al.asname or al.name) == m.group(2) for al in stmt.names):
                    return text
    last = 0
    for stmt in tree.body:
        if isinstance(stmt, (ast.Import, ast.ImportFrom)):
            last = stmt.end_lineno
    lines = text.splitlines(keepends=True)
    lines.insert(last, line + "\n")
    return "".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("src")
    ap.add_argument("dest")
    ap.add_argument("--doc", required=True)
    ap.add_argument("--rename", action="append", default=[])
    ap.add_argument("--tests", nargs="*", default=[])
    ap.add_argument("--pkg", default="ctplib")
    ap.add_argument("--test-module", default="ctp")
    ap.add_argument("--at", type=int, action="append", default=[], help="also move the statement at this line")
    ap.add_argument("names", nargs="+")
    args = ap.parse_args()

    src_path = Path(args.src)
    text = src_path.read_text()
    tree = ast.parse(text)
    lines = text.splitlines(keepends=True)
    names = set(args.names)
    renames = dict(r.split("=", 1) for r in args.rename)
    dest_mod = args.dest
    dest_path = Path(args.pkg) / f"{dest_mod}.py"

    for f in [args.src, *args.tests]:
        t = ast.parse(Path(f).read_text())
        clash = [
            n.lineno for n in ast.walk(t)
            if (isinstance(n, ast.Name) and n.id == dest_mod and not isinstance(n.ctx, ast.Load))
            or (isinstance(n, ast.arg) and n.arg == dest_mod)
        ]
        if clash:
            sys.exit(f"{f} already uses the name {dest_mod} at lines {clash[:5]}; pick another module name")
    moved, kept = [], []
    for stmt in tree.body:
        d = defined_names(stmt)
        if stmt.lineno in args.at:
            moved.append(stmt)
        elif d & names:
            if not d <= names:
                sys.exit(f"statement at line {stmt.lineno} defines {d}, only part is named")
            moved.append(stmt)
        else:
            kept.append(stmt)
    found = set().union(*(defined_names(s) for s in moved)) if moved else set()
    names |= found
    if not set(args.names) <= found:
        sys.exit(f"not found at top level: {sorted(set(args.names) - found)}")

    kept_defs = set().union(*(defined_names(s) for s in kept))
    imported = module_imported_names(tree)

    # Moved code: what it needs from SRC.
    moved_edits, used_imports = [], set()
    for stmt in moved:
        for n in global_refs(stmt):
            if n.id in kept_defs:
                sys.exit(f"moved code at line {n.lineno} uses {n.id}, which stays in {src_path}")
            if n.id in imported:
                used_imports.add(n.id)
            if n.id in renames:
                moved_edits.append((n.lineno, n.col_offset, n.end_col_offset, renames[n.id]))
        if isinstance(stmt, (ast.FunctionDef, ast.ClassDef)) and stmt.name in renames:
            # The def line: "def NAME" / "class NAME".
            ln = stmt.lineno
            line = lines[ln - 1]
            m = re.search(rf"\b(def|class)\s+{re.escape(stmt.name)}\b", line)
            col = len(line[: m.start()].encode()) + len(m.group(1)) + 1
            moved_edits.append((ln, col, col + len(stmt.name), renames[stmt.name]))
    for stmt in moved:
        if isinstance(stmt, (ast.Assign, ast.AnnAssign)):
            targets = stmt.targets if isinstance(stmt, ast.Assign) else [stmt.target]
            for t in targets:
                if isinstance(t, ast.Name) and t.id in renames:
                    moved_edits.append((t.lineno, t.col_offset, t.end_col_offset, renames[t.id]))
    # Deduplicate (assignment targets also show up as global refs).
    moved_edits = sorted(set(moved_edits))

    moved_text = rewrite(text, moved_edits)
    moved_lines = moved_text.splitlines(keepends=True)
    chunks = []
    prev = None
    for stmt in moved:
        s, e = span(stmt, lines)
        text_ = "".join(moved_lines[s:e]).rstrip("\n")
        is_def = isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
        if prev is not None:
            pe, pdef = prev
            gap = lines[pe:s]
            if all(not g.strip() for g in gap):
                sep = "\n" * (min(len(gap), 2) + 1)
            else:
                sep = "\n\n\n" if (is_def or pdef) else "\n\n"
            if is_def or pdef:
                sep = "\n\n\n"
            chunks.append(sep)
        chunks.append(text_)
        prev = (e, is_def)

    # What stays: point at DEST.
    kept_edits = []
    for stmt in kept:
        for n in global_refs(stmt):
            if n.id in names:
                if isinstance(n.ctx, ast.Store):
                    sys.exit(f"{src_path}:{n.lineno} assigns moved name {n.id}; fix by hand")
                kept_edits.append(
                    (n.lineno, n.col_offset, n.end_col_offset, f"{dest_mod}.{renames.get(n.id, n.id)}")
                )
        for g in ast.walk(stmt):
            if isinstance(g, ast.Global) and set(g.names) & names:
                sys.exit(f"{src_path}:{g.lineno} declares global {g.names}; fix by hand")
    new_src = rewrite(text, kept_edits)
    new_lines = new_src.splitlines(keepends=True)
    drop = set()
    for stmt in moved:
        s, e = span(stmt, lines)
        drop |= set(range(s, e))
    new_src = "".join(l for i, l in enumerate(new_lines) if i not in drop)
    if kept_edits:
        new_src = add_import(new_src, f"from {args.pkg} import {dest_mod}")
    src_path.write_text(new_src)

    header_imports = import_lines(tree, used_imports)
    if dest_path.exists():
        body = dest_path.read_text().rstrip("\n") + "\n\n\n" + "".join(chunks) + "\n"
        for line in header_imports:
            body = add_import(body, line)
    else:
        head = f'"""{args.doc}"""\n\nfrom __future__ import annotations\n\n'
        head += "\n".join(header_imports) + ("\n\n\n" if header_imports else "\n\n")
        body = head + "".join(chunks) + "\n"
    dest_path.parent.mkdir(exist_ok=True)
    dest_path.write_text(body)

    tm = args.test_module
    for t in args.tests:
        tp = Path(t)
        tt = tp.read_text()
        before = tt
        for n in sorted(names, key=len, reverse=True):
            new = renames.get(n, n)
            tt = re.sub(rf"\b{tm}\.{re.escape(n)}\b", f"{dest_mod}.{new}", tt)
            tt = re.sub(rf'setattr\((\s*){tm},(\s*)"{re.escape(n)}"', rf'setattr(\1{dest_mod},\2"{new}"', tt)
        if tt != before:
            tt = add_import(tt, f"from {args.pkg} import {dest_mod}")
            tp.write_text(tt)
    print(f"moved {len(moved)} statements to {dest_path}; {len(kept_edits)} references rewritten")


if __name__ == "__main__":
    main()
