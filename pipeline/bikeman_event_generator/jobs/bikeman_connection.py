"""
bikeman 쓰기용 DB(BIKEMAN_WRITER_DB_*) 접속 생성.

deploy_returned_bikes.py/generate_collect_events.py가 공유한다. bikeman_db.py는
"connection 객체를 인자로 받기만 한다"는 원칙(파일 상단 docstring 참고)이라 접속
생성 코드를 거기 두지 않고 이 파일로 분리한다.
"""
import os

import psycopg2


def connect():
    return psycopg2.connect(
        host=os.environ["BIKEMAN_WRITER_DB_HOST"],
        port=os.environ.get("BIKEMAN_WRITER_DB_PORT", "5432"),
        dbname=os.environ["BIKEMAN_WRITER_DB_NAME"],
        user=os.environ["BIKEMAN_WRITER_DB_USER"],
        password=os.environ["BIKEMAN_WRITER_DB_PASSWORD"],
        connect_timeout=10,
    )
