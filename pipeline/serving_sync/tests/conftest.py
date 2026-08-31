"""Test configuration for serving_sync tests."""
import sys
from pathlib import Path

# Add parent directories to PYTHONPATH for config and other imports
repo_root = Path(__file__).parent.parent.parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

# Pipeline modules import `common.*` from the src source root.
src_dir = repo_root / "src"
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

# Legacy tests import serving jobs as top-level modules (e.g. build_mart_*).
serving_dir = src_dir / "serving"
if str(serving_dir) not in sys.path:
    sys.path.insert(0, str(serving_dir))
