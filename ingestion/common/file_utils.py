"""
파일 백필 공통 유틸리티

여러 데이터셋(대여이력/고장신고/대여소정보)의 백필 잡이 공통으로 쓰는 로직:
- 확장자가 아니라 실제 파일 내용(zip 매직바이트)으로 압축 여부 판별
  (서울시 공공데이터에서 .csv 확장자인데 실제론 zip인 파일이 실측으로 확인됨)
- .xlsx(내부적으로도 zip 포맷이지만 csv 추출 대상이 아님)는 pandas로 별도 변환
"""
import logging
import zipfile
from pathlib import Path

logger = logging.getLogger(__name__)


class NotThisDatasetError(Exception):
    """입력 파일이 처리 대상 데이터셋이 아닌 것으로 판단됨 (다른 팀원 데이터셋 등).
    스키마가 깨진 게 아니라 애초에 대상이 아니므로 실패가 아니라 정상 스킵으로 처리해야 한다."""


def is_xlsx(path: Path) -> bool:
    """
    확장자가 아니라 zip 내부 구조(OOXML 스프레드시트 시그니처)로 xlsx 여부를 판별한다.
    서울시 공공데이터에서 .csv 확장자인데 실제 내용은 xlsx인 파일이 실측으로 확인됨
    (unzip_if_needed가 zip을 열었는데 .csv 항목이 하나도 없어 빈 리스트를 반환하고
    그대로 유실되는 사례의 원인 - 확장자 대신 내용으로 판별해야 잡힌다).
    """
    if path.suffix.lower() == ".xlsx":
        return True
    try:
        with open(path, "rb") as f:
            if f.read(2) != b"PK":
                return False
        with zipfile.ZipFile(path) as zf:
            return "xl/workbook.xml" in zf.namelist()
    except zipfile.BadZipFile:
        return False


def unzip_if_needed(path: Path, workdir: Path) -> list[Path]:
    """
    확장자가 아니라 매직바이트로 압축 여부를 판별해 내부 csv를 추출한다.
    xlsx(확장자가 .xlsx든, .csv로 위장했든)는 여기서 추출하지 않고 그대로 반환한다
    (csv가 아니라 xml 구조라 추출 대상이 아님 - 호출부가 pandas로 별도 처리).

    csv/xlsx/zip(csv 포함) 중 하나도 아니면 빈 리스트를 반환한다 - 호출부가 그 결과로
    for 루프를 그냥 스킵해버리면 파일 하나가 통째로 조용히 유실될 수 있어서, 그 두
    경우(알 수 없는 파일 형식 / zip인데 csv가 하나도 없음) 모두 여기서 명시적으로
    경고를 남긴다.
    """
    if is_xlsx(path):
        return [path]

    with open(path, "rb") as f:
        magic = f.read(4)
    is_zip = magic[:2] == b"PK"

    if not is_zip:
        if path.suffix.lower() == ".csv":
            return [path]
        logger.warning(
            "알 수 없는 파일 형식(zip/xlsx/csv 아님) - 이 파일은 처리 대상에서 조용히 "
            "빠짐: %s (magic=%r)",
            path, magic,
        )
        return []

    extracted = []
    with zipfile.ZipFile(path) as zf:
        zf.extractall(workdir)
        names = zf.namelist()
        for name in names:
            if name.lower().endswith(".csv"):
                extracted.append(workdir / name)

    if not extracted:
        logger.warning(
            "zip 안에 csv가 하나도 없음 - 이 파일은 처리 대상에서 조용히 빠짐: %s "
            "(내부 항목: %s)",
            path, names,
        )
    return extracted


def expand_archives(paths: list[Path], workdir: Path) -> list[Path]:
    """각 경로가 zip이면 workdir에 풀어서 개별 파일로 치환하고, 아니면 그대로 둔다.
    Dynamic Task Mapping이 zip 파일 하나가 아니라 그 안의 개별 파일(월) 단위로 태스크를
    나눌 수 있게 하기 위함 - unzip_if_needed가 이미 압축 여부를 판별해 알맞은 리스트를
    반환하므로 그 결과를 이어 붙이기만 하면 된다."""
    expanded = []
    for path in paths:
        expanded.extend(unzip_if_needed(path, workdir))
    return expanded

