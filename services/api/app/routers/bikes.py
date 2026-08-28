from fastapi import APIRouter

from services.api.app.schemas import BikeLists
from services.api.app.state import state

router = APIRouter(prefix="/api", tags=["bikes"])


@router.get("/bikes", response_model=BikeLists)
def get_bikes():
    source, dest = state.bikes()
    return {"source": source, "dest": dest}
