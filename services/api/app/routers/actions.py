from fastapi import APIRouter

from services.api.app.schemas import ConfirmRequest, ConfirmResponse
from services.api.app.state import state

router = APIRouter(prefix="/api/actions", tags=["actions"])


@router.post("/confirm", response_model=ConfirmResponse)
def confirm_collection(payload: ConfirmRequest):
    return state.confirm_collection(payload.bike_ids)
