from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from services.api.app.routers import worklog
from services.api.app.routers import bikes, snapshot

app = FastAPI(title="따맨 재배치 운영 콘솔 API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(snapshot.router)
app.include_router(bikes.router)
app.include_router(worklog.router)


@app.get("/api/health")
def health():
    return {"status": "ok"}
