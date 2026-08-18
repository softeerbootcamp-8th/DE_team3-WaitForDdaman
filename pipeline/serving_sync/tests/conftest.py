"""Test configuration for serving_sync tests."""
import sys
from pathlib import Path

# Add parent directories to PYTHONPATH for config and other imports
repo_root = Path(__file__).parent.parent.parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

# jobs import `common.*` (s3_utils/spark_session) which lives under ingestion/common -
# real runs get this via PYTHONPATH=<ingestion_dir>:... (see staging/README.md), so
# tests need the same path added.
ingestion_dir = repo_root / "ingestion"
if str(ingestion_dir) not in sys.path:
    sys.path.insert(0, str(ingestion_dir))
