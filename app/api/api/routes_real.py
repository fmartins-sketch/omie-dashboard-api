from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.integrations.omie.integration_module import init_db, sync_all_modules
from app.services.kpis import KPIService

router = APIRouter(prefix="/api/v1", tags=["dashboard"])


@router.get("/health")
def healthcheck() -> Dict[str, str]:
    return {"status": "ok"}


@router.get("/dashboard/ceo-real")
def get_ceo_dashboard_real(empresa: str = "consolidado", periodo: str = "mes_atual", db: Session = Depends(get_db)) -> Dict[str, Any]:
    return KPIService(db).ceo_dashboard(empresa=empresa, periodo=periodo)


@router.get("/dashboard/financeiro-real")
def get_financeiro_dashboard_real(db: Session = Depends(get_db)) -> Dict[str, Any]:
    return KPIService(db).financeiro_dashboard()


@router.get("/dashboard/comercial-real")
def get_comercial_dashboard_real(db: Session = Depends(get_db)) -> Dict[str, Any]:
    return KPIService(db).comercial_dashboard()


@router.get("/dashboard/resumo-executivo")
def get_resumo_executivo(db: Session = Depends(get_db)) -> Dict[str, Any]:
    service = KPIService(db)
    ceo = service.ceo_dashboard()
    financeiro = service.financeiro_dashboard()
    comercial = service.comercial_dashboard()
    return {
        "ceo": ceo,
        "financeiro": {"receber_30": ceo["receber_30"], "pagar_30": ceo["pagar_30"], "inadimplencia": ceo["inadimplencia"], "top_inadimplentes": financeiro["top_inadimplentes"]},
        "comercial": {"pipeline_bruto": comercial["pipeline_bruto"], "pipeline_ponderado": comercial["pipeline_ponderado"], "oportunidades_abertas": comercial["oportunidades_abertas"], "ticket_medio": comercial["ticket_medio"]},
    }


@router.post("/sync/omie/full")
async def run_full_sync() -> Dict[str, Any]:
    try:
        return {"status": "ok", "message": "Sincronização concluída com sucesso.", "synced": await sync_all_modules()}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erro ao sincronizar Omie: {str(exc)}") from exc
