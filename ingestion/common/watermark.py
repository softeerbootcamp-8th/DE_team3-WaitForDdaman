"""
증분 적재 워터마크 관리

- 저장 위치: S3 (_meta/watermark/*.json) - 볼륨이 작아 별도 DB 없이 충분
- 워터마크는 "성공적으로 커밋된 마지막 날짜" 만 기록한다.
  부분 실패 시에는 절대 갱신하지 않는다 (그래야 재실행 시 누락 없이 이어서 처리 가능).
- 데이터셋마다 별도 워터마크가 필요하므로 watermark_key를 파라미터로 받는다
  (안 넘기면 config.SETTINGS.watermark_key 기본값 사용 - 대여이력 등 단일 워터마크만
  쓰는 잡은 인자 없이 그대로 호출 가능).
"""
import logging
from datetime import date, datetime, timezone

import config
from common.s3_utils import get_json, put_json

logger = logging.getLogger(__name__)

DATE_FMT = "%Y-%m-%d"


def read_watermark(default_start: str = None, watermark_key: str = None) -> date:
    """
    저장된 워터마크(마지막으로 성공 처리된 날짜)를 읽는다.
    없으면 backfill_start_date(설정) 또는 default_start를 반환한다.

    "last_processed_date"가 표준 필드명이지만, 이전에 "last_rent_dt"로 저장된
    워터마크(대여이력 초기 버전)도 계속 읽을 수 있게 하위호환을 유지한다.
    """
    settings = config.SETTINGS
    key = watermark_key or settings.watermark_key
    payload = get_json(settings.raw_bucket, key)
    if payload:
        date_str = payload.get("last_processed_date") or payload.get("last_rent_dt")
        if date_str:
            return datetime.strptime(date_str, DATE_FMT).date()

    fallback = default_start or settings.backfill_start_date
    logger.warning("워터마크 없음(%s). 기본값(backfill 시작일) 사용: %s", key, fallback)
    return datetime.strptime(fallback, DATE_FMT).date()


def write_watermark(processed_date: date, extra: dict = None, watermark_key: str = None) -> None:
    """
    파이프라인이 해당 날짜까지 "전부 성공"했을 때만 호출해야 한다.
    부분 실패 상태에서 여기를 호출하면 데이터 누락이 발생하므로 절대 금지.
    """
    settings = config.SETTINGS
    key = watermark_key or settings.watermark_key
    payload = {
        "last_processed_date": processed_date.strftime(DATE_FMT),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if extra:
        payload.update(extra)
    put_json(settings.raw_bucket, key, payload)
    logger.info("워터마크 갱신(%s): %s", key, payload["last_processed_date"])