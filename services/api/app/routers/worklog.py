from fastapi import APIRouter

from services.api.app.schemas import ConfirmResponse, WorklogEntry
from services.api.app.state import state

router = APIRouter(prefix="/api/worklog", tags=["worklog"])


@router.post("/confirm", response_model=ConfirmResponse)
def confirm():
    return state.confirm_today()


@router.get("", response_model=list[WorklogEntry])
def list_worklog():
    return state.worklog()
