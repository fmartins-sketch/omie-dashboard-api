from typing import Any, Dict

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.integrations.omie.integration_module import OMIE_APP_KEY, OMIE_APP_SECRET, OmieClient, sync_all_modules
from app.services.kpis import KPIService

router = APIRouter(prefix="/api/v1", tags=["dashboard"])


@router.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@router.post("/sync/omie/full")
async def sync_omie_full() -> Dict[str, Any]:
    return await sync_all_modules()


@router.get("/dashboard/ceo-real")
def dashboard_ceo_real(db: Session = Depends(get_db)) -> Dict[str, Any]:
    service = KPIService(db)
    return service.ceo_dashboard()


@router.get("/dashboard/financeiro-real")
def dashboard_financeiro_real(db: Session = Depends(get_db)) -> Dict[str, Any]:
    service = KPIService(db)
    return service.financeiro_dashboard()


@router.get("/dashboard/comercial-real")
def dashboard_comercial_real(db: Session = Depends(get_db)) -> Dict[str, Any]:
    service = KPIService(db)
    return service.comercial_dashboard()


@router.get("/debug/contas-correntes/first")
def debug_contas_correntes_first(db: Session = Depends(get_db)) -> Dict[str, Any]:
    from app.integrations.omie.integration_module import ContaCorrente

    row = db.query(ContaCorrente).first()
    if not row:
        return {"status": "nenhum registro"}
    return {
        "omie_id": row.omie_id,
        "banco": row.banco,
        "agencia": row.agencia,
        "conta": row.conta,
        "descricao": row.descricao,
        "saldo": float(row.saldo or 0),
        "status_conta": row.status,
    }


@router.get("/debug/omie/fases-crm/raw")
async def debug_omie_fases_crm_raw() -> Dict[str, Any]:
    client = OmieClient(app_key=OMIE_APP_KEY, app_secret=OMIE_APP_SECRET)
    try:
        return await client.listar_fases(pagina=1, registros_por_pagina=20)
    except Exception as exc:
        return {"status": "erro", "erro": str(exc)}

@router.get("/debug/omie/vendedores-crm/raw")
async def debug_omie_vendedores_crm_raw() -> Dict[str, Any]:
    client = OmieClient(app_key=OMIE_APP_KEY, app_secret=OMIE_APP_SECRET)
    try:
        return await client.listar_vendedores_crm(pagina=1, registros_por_pagina=20)
    except Exception as exc:
        return {"status": "erro", "erro": str(exc)}

@router.get("/debug/omie/contas-crm/raw")
async def debug_omie_contas_crm_raw() -> Dict[str, Any]:
    client = OmieClient(app_key=OMIE_APP_KEY, app_secret=OMIE_APP_SECRET)
    try:
        return await client.listar_contas_crm(pagina=1, registros_por_pagina=20)
    except Exception as exc:
        return {"status": "erro", "erro": str(exc)}


@router.get("/debug/omie/crm-usuarios/raw")
async def debug_omie_crm_usuarios_raw() -> Dict[str, Any]:
    client = OmieClient(app_key=OMIE_APP_KEY, app_secret=OMIE_APP_SECRET)
    try:
        return await client.call(
            endpoint="crm/usuarios",
            call="ListarUsuarios",
            param=[{"pagina": 1, "registros_por_pagina": 20}],
        )
    except Exception as exc:
        return {"status": "erro", "erro": str(exc)}


@router.get("/debug/omie/vendas-vendedores/raw")
async def debug_omie_vendas_vendedores_raw() -> Dict[str, Any]:
    client = OmieClient(app_key=OMIE_APP_KEY, app_secret=OMIE_APP_SECRET)
    try:
        return await client.call(
            endpoint="geral/vendedores",
            call="ListarVendedores",
            param=[{"pagina": 1, "registros_por_pagina": 20}],
        )
    except Exception as exc:
        return {"status": "erro", "erro": str(exc)}
