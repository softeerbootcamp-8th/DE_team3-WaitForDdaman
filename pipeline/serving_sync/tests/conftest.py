"""Test configuration for serving_sync tests."""
import sys
from pathlib import Path

# Add parent directories to PYTHONPATH for config and other imports
repo_root = Path(__file__).parent.parent.parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))
