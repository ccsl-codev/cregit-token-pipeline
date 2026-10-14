"""comments_dump.py FILE...: print every comment and docstring with its line number, and the
code line after each comment block, so a reviewer can judge them without reading whole files."""
import ast
import io
import sys
import tokenize

for f in sys.argv[1:]:
    src = open(f).read()
    lines = src.splitlines()
    print(f"===== {f} ({len(lines)} lines)")
    items = []
    for tok in tokenize.generate_tokens(io.StringIO(src).readline):
        if tok.type == tokenize.COMMENT:
            items.append((tok.start[0], "#", tok.string))
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            body = node.body
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) \
                    and isinstance(body[0].value.value, str):
                name = getattr(node, "name", "<module>")
                header = lines[node.lineno - 1].strip() if name != "<module>" else "<module>"
                items.append((body[0].lineno, "D", f"[{header}] lines {body[0].lineno}-{body[0].end_lineno}:\n"
                              + "\n".join(lines[body[0].lineno - 1: body[0].end_lineno])))
    items.sort()
    prev = None
    for ln, kind, text in items:
        if kind == "#":
            print(f"{ln:5d}: {lines[ln - 1]}")
            nxt = ln
            if not lines[ln - 1].lstrip().startswith("#"):
                continue  # trailing comment, shown with its code
            while nxt < len(lines) and lines[nxt].lstrip().startswith("#"):
                nxt += 1
            if nxt < len(lines) and (prev is None or prev != ln - 1):
                pass
            if nxt < len(lines) and not lines[ln].lstrip().startswith("#"):
                print(f"{nxt + 1:5d}|   {lines[nxt]}")
        else:
            print(f"{ln:5d}: DOCSTRING {text}")
        prev = ln
