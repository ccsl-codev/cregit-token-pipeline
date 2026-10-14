"""retarget.py MODULE FILE[,FILE...] NAME[=NEW] ...: point ctp.NAME in test files at ctplib.MODULE."""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from mover import add_import  # noqa: E402

mod, files, specs = sys.argv[1], sys.argv[2].split(","), sys.argv[3:]
pairs = [s.split("=", 1) if "=" in s else (s, s) for s in specs]
for f in files:
    p = Path(f)
    t = before = p.read_text()
    for old, new in sorted(pairs, key=lambda x: -len(x[0])):
        t = re.sub(rf"\bctp\.{re.escape(old)}\b", f"{mod}.{new}", t)
        t = re.sub(rf'setattr\((\s*)ctp,(\s*)"{re.escape(old)}"', rf'setattr(\1{mod},\2"{new}"', t)
    if t != before:
        p.write_text(add_import(t, f"from ctplib import {mod}"))
        print(f"{f}: retargeted")
