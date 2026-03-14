import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt

from app.core.config import settings
from app.integrations.omie.integration_module import sync_all_modules

router = APIRouter(prefix="/api/v1", tags=["auth"])
security = HTTPBearer()
scheduler: Optional[BackgroundScheduler] = None
last_sync_status: Dict[str, Any] = {"status": "idle", "last_run": None, "result": None, "error": None}


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    to_encode.update({"exp": datetime.utcnow() + (expires_delta or timedelta(hours=settings.access_token_expire_hours))})
    return jwt.encode(to_encode, settings.jwt_secret, algorithm="HS256")


def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)) -> Dict[str, Any]:
    try:
        payload = jwt.decode(credentials.credentials, settings.jwt_secret, algorithms=["HS256"])
        if not payload.get("sub"):
            raise HTTPException(status_code=401, detail="Token inválido")
        return payload
    except JWTError as exc:
        raise HTTPException(status_code=401, detail="Token inválido ou expirado") from exc


@router.post("/auth/login")
def login(payload: Dict[str, str]) -> Dict[str, Any]:
    if payload.get("username") != settings.admin_username or payload.get("password") != settings.admin_password:
        raise HTTPException(status_code=401, detail="Usuário ou senha inválidos")
    return {"access_token": create_access_token({"sub": settings.admin_username, "role": "admin"}), "token_type": "bearer"}


@router.get("/auth/me")
def me(payload: Dict[str, Any] = Depends(verify_token)) -> Dict[str, Any]:
    return {"username": payload.get("sub"), "role": payload.get("role", "admin")}


async def _run_sync_job() -> None:
    global last_sync_status
    last_sync_status.update({"status": "running", "error": None})
    try:
        result = await sync_all_modules()
        last_sync_status.update({"status": "success", "result": result, "last_run": datetime.utcnow().isoformat()})
    except Exception as exc:
        last_sync_status.update({"status": "error", "error": str(exc), "last_run": datetime.utcnow().isoformat()})


def run_sync_job_wrapper() -> None:
    asyncio.run(_run_sync_job())


def start_scheduler() -> None:
    global scheduler
    if scheduler:
        return
    scheduler = BackgroundScheduler()
    scheduler.add_job(run_sync_job_wrapper, "interval", minutes=settings.sync_interval_minutes, id="omie_sync_job", replace_existing=True)
    scheduler.start()


def stop_scheduler() -> None:
    global scheduler
    if scheduler:
        scheduler.shutdown()
        scheduler = None


@router.post("/sync/omie/manual-secure")
def run_manual_sync(_: Dict[str, Any] = Depends(verify_token)) -> Dict[str, Any]:
    run_sync_job_wrapper()
    return {"status": "ok", "last_sync": last_sync_status}


@router.get("/sync/status")
def sync_status(_: Dict[str, Any] = Depends(verify_token)) -> Dict[str, Any]:
    return last_sync_status
