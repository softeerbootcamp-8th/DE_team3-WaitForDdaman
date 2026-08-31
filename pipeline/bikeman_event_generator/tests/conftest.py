"""레거시 테스트의 bare import를 새 src/bronze 레이어에 연결한다."""

import sys
from pathlib import Path


BRONZE_DIR = Path(__file__).resolve().parents[3] / "src" / "bronze"
if str(BRONZE_DIR) not in sys.path:
    sys.path.insert(0, str(BRONZE_DIR))
