"""db.py는 DATABASE_URL이 없으면 import 시점에 즉시 RuntimeError를 낸다(prod
fail-fast, db.py 참고). 테스트는 실제 커넥션을 맺지 않고 엔진 생성만 필요하므로
더미 값으로 채워 그 검증을 통과시킨다.
"""
import os

os.environ.setdefault("DATABASE_URL", "postgresql+psycopg2://test:test@localhost/test")
