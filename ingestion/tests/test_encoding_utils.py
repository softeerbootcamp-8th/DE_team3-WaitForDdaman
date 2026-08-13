from pathlib import Path

from common.encoding_utils import convert_euckr_file_to_utf8


def test_normal_euckr_file_converts_cleanly(tmp_path: Path):
    src = tmp_path / "clean.csv"
    src.write_bytes("자전거ID,대여일시\nSPB-00001,20260101".encode("euc-kr"))
    dst = tmp_path / "clean.utf8.csv"

    result = convert_euckr_file_to_utf8(src, dst)

    assert result["dropped_bytes"] == 0
    assert dst.read_text(encoding="utf-8") == "자전거ID,대여일시\nSPB-00001,20260101"


def test_corrupted_bytes_are_dropped_not_fatal(tmp_path: Path):
    """iconv -c 와 동일하게: 손상 바이트가 있어도 파일 전체를 실패시키지 않는다."""
    good = "자전거ID,대여소명\nSPB-00001,강남역".encode("euc-kr")
    # ASCII 구간(멀티바이트 한글 문자 경계가 아닌 곳)에 삽입해야 손상 바이트 수가
    # 삽입한 바이트 수와 정확히 일치한다. 한글 멀티바이트 중간에 넣으면 그 문자까지
    # 함께 깨져서 dropped_bytes가 더 커지는데, 이는 버그가 아니라 실제 디코더의 정상 동작이다.
    insertion_point = good.index(b"SPB")
    corrupted = good[:insertion_point] + b"\xff\xfe\xff" + good[insertion_point:]
    src = tmp_path / "corrupted.csv"
    src.write_bytes(corrupted)
    dst = tmp_path / "corrupted.utf8.csv"

    result = convert_euckr_file_to_utf8(src, dst)

    assert result["dropped_bytes"] == 3
    assert dst.exists()
    assert "SPB-00001" in dst.read_text(encoding="utf-8")


def test_counter_resets_between_calls(tmp_path: Path):
    """전역 카운터가 호출 간에 누적되지 않고 매번 리셋되는지 확인 (버그 방지 회귀 테스트)."""
    good = "a,b".encode("euc-kr")
    corrupted = good + b"\xff\xfe"

    src1 = tmp_path / "f1.csv"
    src1.write_bytes(corrupted)
    r1 = convert_euckr_file_to_utf8(src1, tmp_path / "f1.utf8.csv")
    assert r1["dropped_bytes"] == 2

    src2 = tmp_path / "f2.csv"
    src2.write_bytes(good)  # 손상 없음
    r2 = convert_euckr_file_to_utf8(src2, tmp_path / "f2.utf8.csv")
    assert r2["dropped_bytes"] == 0  # 이전 호출의 카운트가 누적되면 실패하는 케이스
