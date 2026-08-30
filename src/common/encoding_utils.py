"""
EUC-KR -> UTF-8 인코딩 변환

Spark 4.x는 charset 제약으로 EUC-KR을 직접 읽지 못하므로, Spark에 태우기 전에
파이썬 레벨에서 UTF-8로 사전 변환한다.

원본 데이터에 손상된 바이트 시퀀스가 소량 존재하는 것이 실측으로 확인되었으므로
(source_data: "iconv 인코딩 변환 시 Illegal byte sequence 발생"),
`iconv -c` 와 동일하게 손상 바이트는 버리고 계속 진행한다 (파일 전체를 실패시키지 않음) -
단, 아래 EncodingMismatchError 참고: "소량"이 아니라 대부분이 깨지면 다른 문제다.

⚠️ 파일이 EUC-KR이 아니라 UTF-8이면 어떻게 되는가: EUC-KR 코덱은 대부분의 UTF-8
멀티바이트 시퀀스를 유효하지 않은 바이트로 판정해 버린다 - 실측 확인(2026-08-22):
UTF-8 한글 텍스트를 EUC-KR로 디코딩하면 바이트의 60%+ 가 드롭되면서도 예외 없이
조용히 깨진 문자열을 반환한다. 서울시 공공데이터는 지금까지 전부 EUC-KR/CP949였지만
(chardet 실측 확인), 향후 데이터 제공 방식이 UTF-8로 바뀌는 경우까지 이 함수가 조용히
깨진 데이터를 만들어내면 안 된다 - 그래서 손상 비율이 임계값을 넘으면 예외를 던진다.
"""
import codecs
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# 실측 확인된 정상 케이스(malformed 행 몇 개로 인한 소량 손상)는 전체 바이트 대비
# 비율이 0.1% 미만이었다 - UTF-8 파일을 EUC-KR로 잘못 디코딩한 경우(60%+ 드롭)와는
# 자릿수 자체가 다르므로, 여유를 크게 둔 1%를 임계값으로 잡아도 오탐 위험이 낮다.
MAX_DROPPED_BYTE_RATIO = 0.01


class EncodingMismatchError(Exception):
    """드롭된 바이트 비율이 임계값을 넘음 - EUC-KR/CP949가 아닌 다른 인코딩(예: UTF-8)일
    가능성이 높다. 소량의 손상 바이트를 버리고 계속 진행하는 것과는 다른 문제이므로
    별도 예외로 분리해, 호출부가 "이 파일은 깨진 게 아니라 인코딩이 다르다"고 구분해
    처리(스킵 또는 실패)할 수 있게 한다."""


# codecs 커스텀 에러 핸들러는 전역 함수여야 하므로, 호출 사이 카운터는
# 모듈 전역 변수로 관리한다. 이 모듈은 단일 스레드에서 파일 단위로 순차 호출되는
# 배치 스크립트 용도로만 사용할 것 (동시 호출 시 카운트가 섞일 수 있음).
_dropped_byte_count = 0


def _count_and_ignore(error: UnicodeDecodeError):
    global _dropped_byte_count
    _dropped_byte_count += error.end - error.start
    return ("", error.end)


codecs.register_error("count_ignore", _count_and_ignore)


def convert_euckr_file_to_utf8(src_path: Path, dst_path: Path) -> dict:
    """
    EUC-KR 인코딩 파일을 UTF-8로 변환.

    Returns:
        {"dropped_bytes": int, "src": str, "dst": str}

    Raises:
        EncodingMismatchError: 드롭된 바이트 비율이 MAX_DROPPED_BYTE_RATIO를 넘음
            (EUC-KR/CP949가 아닌 다른 인코딩일 가능성이 높음).
    """
    global _dropped_byte_count
    _dropped_byte_count = 0

    dst_path.parent.mkdir(parents=True, exist_ok=True)

    with open(src_path, "rb") as fin:
        raw = fin.read()

    text = raw.decode("euc-kr", errors="count_ignore")

    dropped_ratio = (_dropped_byte_count / len(raw)) if raw else 0.0
    if dropped_ratio > MAX_DROPPED_BYTE_RATIO:
        raise EncodingMismatchError(
            f"{src_path}: 손상 바이트 비율 {dropped_ratio:.1%} ({_dropped_byte_count}/"
            f"{len(raw)}바이트)가 임계값({MAX_DROPPED_BYTE_RATIO:.0%})을 초과함 - "
            "EUC-KR/CP949가 아닌 다른 인코딩일 가능성이 높음"
        )

    with open(dst_path, "w", encoding="utf-8", newline="") as fout:
        fout.write(text)

    if _dropped_byte_count > 0:
        logger.warning(
            "인코딩 변환 중 손상 바이트 %d개 폐기: %s -> %s",
            _dropped_byte_count,
            src_path,
            dst_path,
        )

    return {
        "dropped_bytes": _dropped_byte_count,
        "src": str(src_path),
        "dst": str(dst_path),
    }
