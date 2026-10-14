"""Gate stub anonymizer: copy each input Parquet into OUTDIR unchanged, so the gate
exercises ctp's anonymize step with a deterministic output."""
import shutil
import sys
from pathlib import Path

out = Path(sys.argv[1])
out.mkdir(parents=True, exist_ok=True)
for src in sys.argv[2:]:
    shutil.copyfile(src, out / (Path(src).stem + "-anon.parquet"))
