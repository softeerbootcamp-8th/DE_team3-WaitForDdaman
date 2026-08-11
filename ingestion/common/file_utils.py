"""
파일 백필 공통 유틸리티

여러 데이터셋(대여이력/고장신고/대여소정보)의 백필 잡이 공통으로 쓰는 로직:
- 확장자가 아니라 실제 파일 내용(zip 매직바이트)으로 압축 여부 판별
  (서울시 공공데이터에서 .csv 확장자인데 실제론 zip인 파일이 실측으로 확인됨)
- .xlsx(내부적으로도 zip 포맷이지만 csv 추출 대상이 아님)는 pandas로 별도 변환
"""
import zipfile
from pathlib import Path


class NotThisDatasetError(Exception):
    """입력 파일이 처리 대상 데이터셋이 아닌 것으로 판단됨 (다른 팀원 데이터셋 등).
    스키마가 깨진 게 아니라 애초에 대상이 아니므로 실패가 아니라 정상 스킵으로 처리해야 한다."""


def unzip_if_needed(path: Path, workdir: Path) -> list[Path]:
    """
    확장자가 아니라 매직바이트로 압축 여부를 판별해 내부 csv를 추출한다.
    .xlsx는 내부적으로 zip 포맷이지만 여기서는 그대로 반환한다
    (csv가 아니라 xml 구조라 추출 대상이 아님 - convert_xlsx_to_utf8_csv로 별도 처리).
    """
    if path.suffix.lower() == ".xlsx":
        return [path]

    with open(path, "rb") as f:
        magic = f.read(4)
    is_zip = magic[:2] == b"PK"

    if not is_zip:
        return [path] if path.suffix.lower() == ".csv" else []

    extracted = []
    with zipfile.ZipFile(path) as zf:
        zf.extractall(workdir)
        for name in zf.namelist():
            if name.lower().endswith(".csv"):
                extracted.append(workdir / name)
    return extracted


def convert_xlsx_to_utf8_csv(path: Path, workdir: Path, skiprows: int = 0, header=0) -> Path:
    """
    pandas+openpyxl로 xlsx를 읽어 UTF-8 CSV로 변환한다 (Spark는 xlsx를 직접 못 읽음).
    skiprows/header는 파일마다 헤더 구조가 다를 수 있어(병합 셀 등) 호출부에서 조정한다.
    """
    import pandas as pd

    df = pd.read_excel(path, skiprows=skiprows, header=header, dtype=str)
    out_path = workdir / f"{path.stem}.from_xlsx.csv"
    df.to_csv(out_path, index=False, encoding="utf-8")
    return out_path
