"""
EUC-KR -> UTF-8 인코딩 변환

Spark 4.x는 charset 제약으로 EUC-KR을 직접 읽지 못하므로, Spark에 태우기 전에
파이썬 레벨에서 UTF-8로 사전 변환한다.

원본 데이터에 손상된 바이트 시퀀스가 존재하는 것이 실측으로 확인되었으므로
(source_data: "iconv 인코딩 변환 시 Illegal byte sequence 발생"),
`iconv -c` 와 동일하게 손상 바이트는 버리고 계속 진행한다 (파일 전체를 실패시키지 않음).
"""
import codecs
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

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
    """
    global _dropped_byte_count
    _dropped_byte_count = 0

    dst_path.parent.mkdir(parents=True, exist_ok=True)

    with open(src_path, "rb") as fin:
        raw = fin.read()

    text = raw.decode("euc-kr", errors="count_ignore")

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
