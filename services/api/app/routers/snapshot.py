from fastapi import APIRouter

from services.api.app.schemas import MapData, SnapshotMeta
from services.api.app.state import state

router = APIRouter(prefix="/api", tags=["snapshot"])


@router.get("/meta", response_model=SnapshotMeta)
def get_meta():
    return state.meta


@router.get("/map", response_model=MapData)
def get_map():
    return state.map_data
