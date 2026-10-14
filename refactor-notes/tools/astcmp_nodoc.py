"""astcmp_nodoc.py REV FILE...: check that FILE differs from REV only in comments and docstrings.
Exit 1 when the code differs. Docstrings that the program prints are compared too:
the whole module docstring of retain.py, and the first line of those of build_manifest.py and pin.py."""
import ast
import subprocess
import sys

PRINTED_WHOLE = {"retain.py"}
PRINTED_FIRST_LINE = {"build_manifest.py", "pin.py"}


def strip_docstrings(tree):
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            body = node.body
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) \
                    and isinstance(body[0].value.value, str):
                body[0].value.value = "<doc>"
    return tree


rev, files = sys.argv[1], sys.argv[2:]
bad = 0
for f in files:
    old = subprocess.run(["git", "show", f"{rev}:{f}"], capture_output=True, text=True, check=True).stdout
    new = open(f).read()
    a, b = ast.parse(old), ast.parse(new)
    da, db = ast.get_docstring(a, clean=False) or "", ast.get_docstring(b, clean=False) or ""
    if f in PRINTED_WHOLE and da != db:
        print(f"{f}: the module docstring is --help text and changed"); bad += 1
    if f in PRINTED_FIRST_LINE and da.splitlines()[:1] != db.splitlines()[:1]:
        print(f"{f}: the first docstring line is --help text and changed"); bad += 1
    if ast.dump(strip_docstrings(a)) != ast.dump(strip_docstrings(b)):
        print(f"{f}: CODE DIFFERS (not only comments and docstrings)"); bad += 1
print(f"{len(files)} files checked, {bad} problems")
sys.exit(1 if bad else 0)
