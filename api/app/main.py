from fastapi import FastAPI

from app.db import close_db_pool, init_db_pool
from app.routers.latest import router as latest_router
from app.routers.map import router as map_router
from app.routers.quality import router as quality_router
from app.routers.timeseries import router as timeseries_router


app = FastAPI(title="Air Quality API", version="0.1.0")

app.include_router(latest_router)
app.include_router(timeseries_router)
app.include_router(map_router)
app.include_router(quality_router)


@app.on_event("startup")
def on_startup() -> None:
    app.state.db_pool = init_db_pool()


@app.on_event("shutdown")
def on_shutdown() -> None:
    close_db_pool(getattr(app.state, "db_pool", None))


@app.get("/healthz")
def healthz():
    return {"status": "ok"}
