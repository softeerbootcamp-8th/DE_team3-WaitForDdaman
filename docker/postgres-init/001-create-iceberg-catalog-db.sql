-- Iceberg JDBC 카탈로그(메타데이터 포인터 저장용) 전용 DB.
-- POSTGRES_DB(Airflow 메타데이터 DB)와 분리한다 - 같은 DB를 쓰면 Iceberg의
-- JdbcCatalog가 만드는 iceberg_tables 등 스키마가 Airflow 메타데이터 테이블과
-- 뒤섞여서 둘 중 하나를 초기화/복구할 때 서로 영향을 줄 수 있다.
--
-- postgres 이미지는 이 디렉터리(/docker-entrypoint-initdb.d/)의 스크립트를
-- 데이터 디렉터리가 "비어있을 때"(최초 기동)만 실행한다 - 이미 볼륨이 있는
-- 환경에는 자동으로 반영되지 않으므로, 기존 환경에서는 수동으로
-- `CREATE DATABASE iceberg_catalog;`를 한 번 실행해야 한다.
CREATE DATABASE iceberg_catalog;
