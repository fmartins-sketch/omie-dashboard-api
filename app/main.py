from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.auth_and_scheduler import router as auth_router, start_scheduler, stop_scheduler
from app.api.routes_real import router as dashboard_router
from app.core.config import settings
from app.integrations.omie.integration_module import init_db

app = FastAPI(title="Omie Dashboard API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(dashboard_router)
app.include_router(auth_router)


@app.get("/")
def root():
    return {"name": "Omie Dashboard API", "status": "running", "docs": "/docs"}


@app.on_event("startup")
def on_startup() -> None:
    init_db()
    start_scheduler()


@app.on_event("shutdown")
def on_shutdown() -> None:
    stop_scheduler()
