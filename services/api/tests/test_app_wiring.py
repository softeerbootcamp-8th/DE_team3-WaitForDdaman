"""앱 배선 검증 — docker-build-check가 잡지 못하는 영역.

이미지 빌드는 pip install이 되는지만 본다. 절대 임포트 체인
(services.api.app.* — services/ 와 services/api/ 에 __init__.py가 없어
네임스페이스 패키지로 해석된다), 라우터 등록, state 객체 배선, 응답 모델이
깨져도 이미지는 정상 빌드되고 컨테이너를 띄울 때야 터진다.

db.py가 import 시점에 create_engine을 부르지만 실제 연결은 지연되므로
이 테스트는 Postgres 없이 돈다.

라우트 목록은 app.routes를 직접 훑지 않고 OpenAPI 스키마에서 읽는다 —
include_router로 붙은 라우트가 app.routes 최상위에 평평하게 나오는지는
FastAPI/Starlette 버전마다 다르지만(0.141에서 실제로 바뀜), OpenAPI는
프론트가 소비하는 공개 계약이라 안정적이다.
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from services.api.app.main import app

EXPECTED_ROUTES = {
    ("get", "/api/health"),
    ("get", "/api/meta"),
    ("get", "/api/map"),
    ("get", "/api/bikes"),
    ("get", "/api/actions/confirm"),
    ("get", "/api/actions/confirmed"),
    ("post", "/api/actions/confirm"),
}


@pytest.fixture(scope="module")
def schema() -> dict:
    # 응답 모델(schemas.py)이 라우터 시그니처와 맞지 않으면 여기서 터진다.
    return app.openapi()


def _registered(schema: dict) -> set[tuple[str, str]]:
    return {
        (method, path)
        for path, ops in schema["paths"].items()
        for method in ops
        if method in {"get", "post", "put", "patch", "delete"}
    }


def test_헬스체크는_DB없이_응답한다():
    with TestClient(app) as client:
        res = client.get("/api/health")
    assert res.status_code == 200
    assert res.json() == {"status": "ok"}


def test_기대하는_라우트가_모두_등록됐다(schema):
    missing = EXPECTED_ROUTES - _registered(schema)
    assert not missing, f"등록되지 않은 라우트: {sorted(missing)}"


def test_confirm은_GET과_POST_둘_다_있다(schema):
    # 프론트가 로드 시 GET으로 확정 상태를 복원하고 POST로 확정한다.
    # 같은 경로에 메서드가 둘이라 한쪽이 조용히 사라져도 눈에 안 띈다.
    registered = _registered(schema)
    assert ("get", "/api/actions/confirm") in registered
    assert ("post", "/api/actions/confirm") in registered


def test_응답모델이_스키마에_연결됐다(schema):
    """라우터의 response_model이 빠지면 OpenAPI에 구체 스키마 대신 빈 응답이 남는다.
    프론트가 타입을 그대로 신뢰하므로 계약이 조용히 헐거워지는 걸 막는다."""
    for method, path in sorted(EXPECTED_ROUTES):
        if path == "/api/health":
            continue  # health는 response_model이 없다 (의도)
        content = schema["paths"][path][method]["responses"]["200"]["content"]
        ref = content["application/json"]["schema"]
        assert "$ref" in ref or ref.get("type") == "object", (
            f"{method.upper()} {path} 에 응답 스키마가 연결되지 않았다"
        )


def test_CORS가_프론트_오리진을_허용한다():
    with TestClient(app) as client:
        res = client.get("/api/health", headers={"Origin": "http://localhost:5173"})
    assert res.headers.get("access-control-allow-origin") == "http://localhost:5173"


def test_confirm_요청_본문_검증():
    # bike_ids가 없으면 422여야 한다. DB에 닿기 전에 걸러진다.
    with TestClient(app) as client:
        res = client.post("/api/actions/confirm", json={})
    assert res.status_code == 422
