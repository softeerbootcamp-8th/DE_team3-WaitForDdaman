from fastapi import APIRouter

from services.api.app.schemas import ConfirmedBikes, ConfirmRequest, ConfirmResponse
from services.api.app.state import state

router = APIRouter(prefix="/api/actions", tags=["actions"])


@router.get("/confirm", response_model=ConfirmResponse)
def get_confirmation():
    """오늘 스냅샷의 마지막 확정 상태. 프론트가 로드 시 읽어 확정 여부를 복원한다."""
    return state.latest_confirmation


@router.get("/confirmed", response_model=ConfirmedBikes)
def get_confirmed_bikes():
    """가장 최근 확정 배치의 자전거 목록 (확정 내역 조회 화면용)."""
    return state.confirmed_bikes


@router.post("/confirm", response_model=ConfirmResponse)
def confirm_collection(payload: ConfirmRequest):
    return state.confirm_collection(payload.bike_ids)
