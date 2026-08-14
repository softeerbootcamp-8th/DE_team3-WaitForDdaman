"""
대여소정보(OA-13252) - 파일(xlsx) S3 저장 + API 스냅샷 비교

전체 SCD Type 2 파이프라인이 아니라 지금 단계에서 필요한 가벼운 확인 작업이다:
    1) 로컬 xlsx 파일을 S3 raw zone에 원본 그대로 업로드 (lineage 보존)
    2) tbCycleStationInfo API를 페이징 호출해 현재 전체 대여소 스냅샷을 가져옴
    3) 파일 쪽 대여소번호 목록과 API 쪽을 비교해서 건수/차이를 보고

⚠️ tbCycleStationInfo의 실제 응답 필드명은 아직 확인된 샘플이 없다. STATION_ID_CANDIDATES에
   가능성 있는 필드명을 여러 개 등록해뒀고, 하나도 안 맞으면 실제 키 목록을 에러 메시지로
   보여준다 - 그 로그를 보고 정확한 필드명을 알려주면 다음 실행 때 반영하면 된다.

사용법:
    STATION_FILE_PATH="../data/station_master/공공자전거 대여소 정보(26.6월 기준).xlsx" \
        python -m jobs.compare_station_master
"""
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import config
from common.api_client import fetch_station_info
from common.s3_utils import ensure_bucket, put_json, put_text, upload_file

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# source_data 실측 기준 (2026년 1~6월 파일, 2,789행 x 10열):
# skiprows=5, header=None 후 이 순서로 수동 지정해야 함 (병합 헤더라 자동 인식 불가)
FILE_COLUMN_NAMES = [
    "대여소번호",
    "대여소명",
    "자치구",
    "상세주소",
    "위도",
    "경도",
    "설치시기",
    "LCD거치대수",
    "QR거치대수",
    "운영방식",
]
FILE_STATION_ID_COLUMN = "대여소번호"

# API 응답에서 대여소 식별자로 쓰일 가능성이 있는 필드명 후보
# (tbCycleStationInfo 실제 스펙 미확인 - 방어적으로 여러 개 시도)
STATION_ID_CANDIDATES = ["RENT_NO", "RENT_ID", "rentNo", "rentId", "stationId", "station_id", "대여소번호"]


class StationIdFieldNotFoundError(Exception):
    """API 응답에서 대여소 식별자 필드를 찾지 못함 - STATION_ID_CANDIDATES 보강 필요."""


def _normalize_station_id(raw_id: str) -> str:
    """
    실측 확인(2026-08-11): API(tbCycleStationInfo)는 대여소번호를 5자리 zero-padding으로
    준다(예: '00102'). 파일(xlsx)은 zero-padding 없이 준다(예: '102'). 같은 대여소인데
    문자열이 달라 비교가 전부 어긋나는 문제가 실제로 발생했다 - 비교 전에 반드시 정규화한다.
    """
    stripped = raw_id.strip()
    if stripped.isdigit():
        return str(int(stripped))  # 앞자리 0 제거: '00102' -> '102'
    return stripped


def _read_file_station_ids(xlsx_path: Path) -> set[str]:
    import pandas as pd

    df = pd.read_excel(xlsx_path, skiprows=5, header=None, dtype=str)
    if df.shape[1] != len(FILE_COLUMN_NAMES):
        raise ValueError(
            f"예상 컬럼 수({len(FILE_COLUMN_NAMES)})와 실제 컬럼 수({df.shape[1]})가 다름 - "
            "파일 구조가 바뀐 것으로 보임 (skiprows/컬럼 순서 재확인 필요)."
        )
    df.columns = FILE_COLUMN_NAMES

    station_ids = {_normalize_station_id(v) for v in df[FILE_STATION_ID_COLUMN].dropna().astype(str)}
    logger.info("파일 기준 대여소 수: %d개", len(station_ids))
    return station_ids


def _find_station_id_field(sample_row: dict) -> str:
    for candidate in STATION_ID_CANDIDATES:
        if candidate in sample_row:
            return candidate
    raise StationIdFieldNotFoundError(
        f"API 응답에서 대여소 식별자 필드를 못 찾음. 실제 키 목록: {list(sample_row.keys())} - "
        "이 목록을 보고 STATION_ID_CANDIDATES에 정확한 필드명을 추가해야 함."
    )


def _fetch_api_rows() -> list[dict]:
    """API에서 전체 대여소 스냅샷 원본 행을 그대로 가져온다 (정규화/비교 이전 원본)."""
    rows = list(fetch_station_info())
    logger.info("API 원본 응답: %d행", len(rows))
    if rows:
        logger.info("API 응답 필드 목록: %s", list(rows[0].keys()))
        logger.info("API 응답 샘플 3건: %s", rows[:3])
    return rows


def _extract_station_ids(rows: list[dict]) -> set[str]:
    if not rows:
        logger.warning("API 응답이 비어있음 (0건)")
        return set()

    id_field = _find_station_id_field(rows[0])
    logger.info("API 응답에서 대여소 식별자 필드로 '%s' 사용", id_field)

    station_ids = {
        _normalize_station_id(str(r[id_field])) for r in rows if r.get(id_field) is not None
    }
    logger.info("API 기준 대여소 수: %d개", len(station_ids))
    return station_ids


def _build_detail_lookup(rows: list[dict], id_field: str) -> dict:
    """정규화된 대여소번호 -> 원본 행 전체(이름/주소 등 모든 컬럼) 매핑."""
    lookup = {}
    for r in rows:
        if r.get(id_field) is not None:
            lookup[_normalize_station_id(str(r[id_field]))] = r
    return lookup


def run(xlsx_path_str: str) -> None:
    xlsx_path = Path(xlsx_path_str)
    if not xlsx_path.exists():
        logger.error("파일이 없습니다: %s", xlsx_path)
        sys.exit(1)

    ensure_bucket(config.SETTINGS.raw_bucket)

    # 1) 원본 xlsx를 S3 raw zone에 그대로 보존 (lineage)
    landing_date = datetime.utcnow().strftime("%Y-%m-%d")
    upload_file(
        xlsx_path,
        config.SETTINGS.raw_bucket,
        f"raw/station_master/_landing/{landing_date}/{xlsx_path.name}",
    )

    # 2) 파일 / API 각각에서 대여소번호 집합 추출
    file_ids = _read_file_station_ids(xlsx_path)

    api_rows = _fetch_api_rows()
    # API 원본을 raw zone에 그대로 보존 - 실제로 뭘 받았는지 눈으로 확인하고 싶을 때 여기서 봄
    put_json(
        config.SETTINGS.raw_bucket,
        f"raw/station_master/api/{landing_date}/payload.json",
        {"row_count": len(api_rows), "rows": api_rows},
    )
    api_ids = _extract_station_ids(api_rows)

    # 3) 비교
    only_in_file = file_ids - api_ids
    only_in_api = api_ids - file_ids
    common_ids = file_ids & api_ids

    # 신설 후보(API에만 있는 대여소)의 상세 정보(이름/주소 등)를 CSV로 뽑아둔다.
    # ID 목록만으로는 "이게 무슨 대여소인지" 알 수 없어서 다운로드해서 바로 확인 가능하게 함.
    new_station_rows: list[dict] = []
    if api_rows and only_in_api:
        id_field = _find_station_id_field(api_rows[0])
        detail_lookup = _build_detail_lookup(api_rows, id_field)
        new_station_rows = [detail_lookup[sid] for sid in sorted(only_in_api) if sid in detail_lookup]

    new_stations_key = f"raw/station_master/_compare/{landing_date}/new_stations.csv"
    if new_station_rows:
        import pandas as pd

        csv_text = pd.DataFrame(new_station_rows).to_csv(index=False)
        put_text(config.SETTINGS.raw_bucket, new_stations_key, csv_text)
        logger.info(
            "신설 후보 상세정보 CSV 저장: s3://%s/%s (%d건)",
            config.SETTINGS.raw_bucket, new_stations_key, len(new_station_rows),
        )

    report = {
        "file_count": len(file_ids),
        "api_count": len(api_ids),
        "common_count": len(common_ids),
        "only_in_file_count": len(only_in_file),
        "only_in_api_count": len(only_in_api),
        "only_in_file_ids": sorted(only_in_file),  # 전체 목록 (요약용 _sample과 별개)
        "only_in_api_ids": sorted(only_in_api),
        "only_in_file_sample": sorted(only_in_file)[:20],
        "only_in_api_sample": sorted(only_in_api)[:20],
        "new_stations_csv_key": new_stations_key if new_station_rows else None,
        "compared_at": datetime.utcnow().isoformat(),
    }

    logger.info("=== 비교 결과 ===")
    logger.info("파일 기준: %d개 / API 기준: %d개 / 공통: %d개", len(file_ids), len(api_ids), len(common_ids))
    logger.info("파일에만 있음(폐쇄 후보): %d개 %s", len(only_in_file), report["only_in_file_sample"])
    logger.info("API에만 있음(신설 후보): %d개 %s", len(only_in_api), report["only_in_api_sample"])

    put_json(
        config.SETTINGS.raw_bucket,
        f"raw/station_master/_compare/{landing_date}/report.json",
        report,
    )


if __name__ == "__main__":
    xlsx_path_str = os.getenv("STATION_FILE_PATH")
    if not xlsx_path_str:
        print("사용법: STATION_FILE_PATH=<xlsx경로> python -m jobs.compare_station_master")
        sys.exit(1)
    run(xlsx_path_str)