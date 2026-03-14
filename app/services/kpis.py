from fastapi import APIRouter, Depends, HTTPException
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.db.session import get_db
from app.integrations.omie.integration_module import (
     init_db,
    sync_all_modules,
    ContaReceber,
    ContaPagar,
    PedidoVenda,
    Oportunidade,
    OmieClient,
    OMIE_APP_KEY,
    OMIE_APP_SECRET,
)



router = APIRouter(prefix="/api/v1", tags=["dashboard"])


@router.get("/health")
def healthcheck() -> Dict[str, str]:
    return {"status": "ok"}


@router.on_event("startup")
def startup_init() -> None:
    init_db()


@router.get("/dashboard/ceo-real")
def get_ceo_dashboard_real(
    empresa: str = "consolidado",
    periodo: str = "mes_atual",
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    service = KPIService(db)
    return service.ceo_dashboard(empresa=empresa, periodo=periodo)


@router.get("/dashboard/financeiro-real")
def get_financeiro_dashboard_real(db: Session = Depends(get_db)) -> Dict[str, Any]:
    service = KPIService(db)
    return service.financeiro_dashboard()


@router.get("/dashboard/comercial-real")
def get_comercial_dashboard_real(db: Session = Depends(get_db)) -> Dict[str, Any]:
    service = KPIService(db)
    return service.comercial_dashboard()


@router.post("/sync/omie/full")
async def run_full_sync() -> Dict[str, Any]:
    try:
        result = await sync_all_modules()
        return {
            "status": "ok",
            "message": "Sincronização concluída com sucesso.",
            "synced": result,
        }
    except ValueError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erro ao sincronizar Omie: {str(exc)}")


@router.get("/dashboard/resumo-executivo")
def get_resumo_executivo(db: Session = Depends(get_db)) -> Dict[str, Any]:
    service = KPIService(db)
    ceo = service.ceo_dashboard()
    financeiro = service.financeiro_dashboard()
    comercial = service.comercial_dashboard()

    return {
        "ceo": ceo,
        "financeiro": {
            "receber_30": ceo.get("receber_30", 0),
            "pagar_30": ceo.get("pagar_30", 0),
            "inadimplencia": ceo.get("inadimplencia", 0),
            "top_inadimplentes": financeiro.get("top_inadimplentes", []),
        },
        "comercial": {
            "pipeline_bruto": comercial.get("pipeline_bruto", 0),
            "pipeline_ponderado": comercial.get("pipeline_ponderado", 0),
            "oportunidades_abertas": comercial.get("oportunidades_abertas", 0),
            "ticket_medio": comercial.get("ticket_medio", 0),
        },
    }


@router.get("/debug/receber/first")
def debug_receber_first(db: Session = Depends(get_db)) -> Dict[str, Any]:
    row = db.query(ContaReceber).first()
    if not row:
        return {"message": "nenhum registro"}
    return {
        "omie_id": row.omie_id,
        "valor_documento": float(row.valor_documento or 0),
        "valor_saldo": float(row.valor_saldo or 0),
        "data_vencimento": row.data_vencimento,
        "nome_cliente": row.nome_cliente,
        "payload_json": row.payload_json,
    }


@router.get("/debug/pagar/first")
def debug_pagar_first(db: Session = Depends(get_db)) -> Dict[str, Any]:
    row = db.query(ContaPagar).first()
    if not row:
        return {"message": "nenhum registro"}
    return {
        "omie_id": row.omie_id,
        "valor_documento": float(row.valor_documento or 0),
        "valor_saldo": float(row.valor_saldo or 0),
        "data_vencimento": row.data_vencimento,
        "nome_fornecedor": row.nome_fornecedor,
        "payload_json": row.payload_json,
    }


@router.get("/debug/pedidos/first")
def debug_pedidos_first(db: Session = Depends(get_db)) -> Dict[str, Any]:
    row = db.query(PedidoVenda).first()
    if not row:
        return {"message": "nenhum registro"}
    return {
        "omie_id": row.omie_id,
        "numero_pedido": row.numero_pedido,
        "cliente": row.cliente,
        "etapa": row.etapa,
        "status": row.status,
        "data_emissao": row.data_emissao,
        "valor_total": float(row.valor_total or 0),
        "payload_json": row.payload_json,
    }


@router.get("/debug/oportunidades/first")
def debug_oportunidades_first(db: Session = Depends(get_db)) -> Dict[str, Any]:
    row = db.query(Oportunidade).first()
    if not row:
        return {"message": "nenhum registro"}
    return {
        "omie_id": row.omie_id,
        "titulo": row.titulo,
        "cliente": row.cliente,
        "etapa": row.etapa,
        "vendedor": row.vendedor,
        "previsao_fechamento": row.previsao_fechamento,
        "valor_total": float(row.valor_total or 0),
        "probabilidade": float(row.probabilidade or 0),
        "valor_ponderado": float(row.valor_ponderado or 0),
        "status": row.status,
        "payload_json": row.payload_json,
    }


@router.get("/debug/omie/pedidos/raw")
async def debug_omie_pedidos_raw() -> Dict[str, Any]:
    client = OmieClient(app_key=OMIE_APP_KEY, app_secret=OMIE_APP_SECRET)
    return await client.listar_pedidos_venda(pagina=1, registros_por_pagina=5)


@router.get("/debug/omie/oportunidades/raw")
async def debug_omie_oportunidades_raw() -> Dict[str, Any]:
    client = OmieClient(app_key=OMIE_APP_KEY, app_secret=OMIE_APP_SECRET)
    return await client.listar_oportunidades(pagina=1, registros_por_pagina=5)
