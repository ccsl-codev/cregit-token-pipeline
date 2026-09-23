"""Put the repository root on sys.path.

The scripts live at the repository root and form no package, so a test file
inside tests/ cannot `import ctp` without this. Keep this file at the
root: pytest uses the directory of the topmost conftest.py as the rootdir.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
