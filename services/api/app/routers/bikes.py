from fastapi import APIRouter, HTTPException

from services.api.app.schemas import BikeLists, CapacityUpdateRequest, SnapshotMeta, TransferRequest
from services.api.app.state import state

router = APIRouter(prefix="/api", tags=["bikes"])


@router.get("/bikes", response_model=BikeLists)
def get_bikes():
    source, dest = state.bikes()
    return {"source": source, "dest": dest}


@router.post("/bikes/transfer", response_model=BikeLists)
def transfer_bikes(payload: TransferRequest):
    source, dest = state.transfer(payload.ids, payload.fromList)
    return {"source": source, "dest": dest}


@router.patch("/capacity", response_model=SnapshotMeta)
def update_capacity(payload: CapacityUpdateRequest):
    if payload.max < 0:
        raise HTTPException(status_code=400, detail="max must be >= 0")
    state.set_capacity(payload.max)
    return state.meta
