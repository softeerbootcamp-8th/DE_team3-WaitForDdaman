"""app/geo.py 투영 검증.

지도 전체가 이 순수 함수 하나에 달려 있다. 상수 오타는 예외를 던지지 않고 조용히
폴리곤을 비틀거나 대여소 점을 구 밖으로 밀어내므로, 눈으로는 늦게 발견된다.
"""
from __future__ import annotations

import math
import re
from pathlib import Path

import pytest

from services.api.app.geo import (
    LAT_MAX,
    LAT_MIN,
    LNG_MAX,
    LNG_MIN,
    MAP_PAD,
    SCALE,
    VIEW_BOX_H,
    VIEW_BOX_W,
    project,
)


def test_북서_모서리는_패딩_위치로_간다():
    # 위도 최대 + 경도 최소 = 지도의 좌상단. 여기가 (MAP_PAD, MAP_PAD)여야
    # 폴리곤이 뷰박스 왼쪽/위쪽으로 잘려나가지 않는다.
    x, y = project(LAT_MAX, LNG_MIN)
    assert x == pytest.approx(MAP_PAD)
    assert y == pytest.approx(MAP_PAD)


def test_동쪽_끝은_뷰박스_너비에서_패딩만큼_안쪽():
    x, _ = project(LAT_MAX, LNG_MAX)
    assert x == pytest.approx(VIEW_BOX_W - MAP_PAD)


def test_남쪽_끝은_뷰박스_높이에서_패딩만큼_안쪽():
    _, y = project(LAT_MIN, LNG_MIN)
    assert y == pytest.approx(VIEW_BOX_H - MAP_PAD)


def test_바운딩박스_중심은_뷰박스_중심으로_간다():
    x, y = project((LAT_MIN + LAT_MAX) / 2, (LNG_MIN + LNG_MAX) / 2)
    assert x == pytest.approx(VIEW_BOX_W / 2)
    assert y == pytest.approx(VIEW_BOX_H / 2)


def test_북쪽이_위다():
    # SVG는 y가 아래로 증가한다. 위도가 커지면 y는 작아져야 한다.
    _, y_north = project(LAT_MAX, LNG_MIN)
    _, y_south = project(LAT_MIN, LNG_MIN)
    assert y_north < y_south


def test_동쪽이_오른쪽이다():
    x_west, _ = project(LAT_MAX, LNG_MIN)
    x_east, _ = project(LAT_MAX, LNG_MAX)
    assert x_west < x_east


def test_x와_y가_같은_스케일을_공유한다():
    """등장방형도법 + cos(lat) 보정의 핵심. x/y 스케일이 갈라지면 폴리곤이 찌그러진다.

    보정된 경도 1단위와 위도 1단위가 같은 픽셀 수로 매핑되는지 본다.
    """
    d = 0.01
    lat0, lng0 = 37.5, 126.9
    x0, y0 = project(lat0, lng0)
    x1, _ = project(lat0, lng0 + d)
    _, y1 = project(lat0 + d, lng0)

    from services.api.app.geo import _COS_LAT_MID

    px_per_lng_adjusted = (x1 - x0) / (d * _COS_LAT_MID)
    px_per_lat = (y0 - y1) / d  # 북쪽이 위라 부호 반전

    assert px_per_lng_adjusted == pytest.approx(px_per_lat)
    assert px_per_lat == pytest.approx(SCALE)


def test_cos_보정상수가_실제_중위도_코사인이다():
    """하드코딩된 _COS_LAT_MID가 독스트링이 주장하는 값인지 확인.

    이 상수가 어긋나면 동서 방향만 늘어나거나 줄어들어 지도가 미묘하게 틀어진다.
    """
    from services.api.app.geo import _COS_LAT_MID

    expected = math.cos(math.radians((LAT_MIN + LAT_MAX) / 2))
    assert _COS_LAT_MID == pytest.approx(expected, abs=1e-6)


def test_뷰박스_높이가_위도_스팬에서_유도된다():
    assert VIEW_BOX_H == pytest.approx((LAT_MAX - LAT_MIN) * SCALE + 2 * MAP_PAD)


def test_시드_스크립트가_투영을_복제하지_않고_공유한다():
    """dim_district의 path/cx/cy와 요청 시점의 station x/y가 같은 투영을 써야
    대여소 점이 소속 구 폴리곤 안에 놓인다(geo.py 독스트링).

    상수를 복사해 두면 한쪽만 수정됐을 때 조용히 어긋나므로, 시드 스크립트가
    geo에서 import하고 있고 상수를 자체 정의하지 않았는지 소스 레벨에서 잠근다.
    """
    seed = Path(__file__).resolve().parents[1] / "scripts" / "seed_dim_district.py"
    src = seed.read_text(encoding="utf-8")

    assert re.search(r"from\s+services\.api\.app\.geo\s+import\s+.*\bproject\b", src), (
        "seed_dim_district.py가 geo.project를 import하지 않는다"
    )
    for const in ("LAT_MIN", "LNG_MIN", "SCALE", "MAP_PAD", "_COS_LAT_MID"):
        assert not re.search(rf"^{const}\s*=", src, re.MULTILINE), (
            f"seed_dim_district.py가 {const}를 자체 정의한다 - geo에서 import해야 한다"
        )


def test_투영_상수가_고정값에서_바뀌지_않았다():
    """상수 자체를 못으로 박는다.

    위 테스트들은 전부 MAP_PAD/SCALE/VIEW_BOX_* 로 기댓값을 표현하므로 내부 정합성만
    본다 — 상수를 다 같이 바꾸면 통째로 통과한다. 그런데 이 값들은 코드 내부 사정이
    아니라 **이미 적재된 데이터와의 계약**이다: seed_dim_district.py가 구 폴리곤을
    이 투영으로 미리 계산해 serving.dim_district의 path/cx/cy와 view_box_w/h에 넣어
    두었고, 대여소 x/y는 요청 시점에 투영된다. 상수가 바뀌면 재시드 전까지 두 좌표계가
    어긋나 대여소 점이 구 밖으로 나간다.

    바꿔야 한다면 이 테스트를 함께 고치고 dim_district를 재시드할 것.
    """
    assert (LAT_MIN, LAT_MAX) == (37.4283, 37.7013)
    assert (LNG_MIN, LNG_MAX) == (126.7646, 127.1831)
    assert MAP_PAD == 24.0
    assert VIEW_BOX_W == 880.0
    assert SCALE == pytest.approx(2508.0646638396, abs=1e-6)
    assert VIEW_BOX_H == pytest.approx(732.7016532282, abs=1e-6)
