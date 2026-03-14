import json
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
from sqlalchemy import Column, DateTime, Integer, Numeric, String, Text
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.session import Base, SessionLocal, engine

OMIE_BASE_URL = "https://app.omie.com.br/api/v1"


class ContaReceber(Base):
    __tablename__ = "contas_receber"
    id = Column(Integer, primary_key=True, index=True)
    omie_id = Column(String(100), unique=True, index=True, nullable=False)
    codigo_cliente = Column(String(100), nullable=True)
    nome_cliente = Column(String(255), nullable=True)
    numero_documento = Column(String(100), nullable=True)
    status_titulo = Column(String(100), nullable=True)
    categoria = Column(String(255), nullable=True)
    data_vencimento = Column(String(20), nullable=True)
    data_emissao = Column(String(20), nullable=True)
    valor_documento = Column(Numeric(15, 2), nullable=True)
    valor_saldo = Column(Numeric(15, 2), nullable=True)
    payload_json = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ContaPagar(Base):
    __tablename__ = "contas_pagar"
    id = Column(Integer, primary_key=True, index=True)
    omie_id = Column(String(100), unique=True, index=True, nullable=False)
    codigo_fornecedor = Column(String(100), nullable=True)
    nome_fornecedor = Column(String(255), nullable=True)
    numero_documento = Column(String(100), nullable=True)
    status_titulo = Column(String(100), nullable=True)
    categoria = Column(String(255), nullable=True)
    data_vencimento = Column(String(20), nullable=True)
    data_emissao = Column(String(20), nullable=True)
    valor_documento = Column(Numeric(15, 2), nullable=True)
    valor_saldo = Column(Numeric(15, 2), nullable=True)
    payload_json = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Oportunidade(Base):
    __tablename__ = "oportunidades"
    id = Column(Integer, primary_key=True, index=True)
    omie_id = Column(String(100), unique=True, index=True, nullable=False)
    titulo = Column(String(255), nullable=True)
    cliente = Column(String(255), nullable=True)
    etapa = Column(String(100), nullable=True)
    vendedor = Column(String(255), nullable=True)
    previsao_fechamento = Column(String(20), nullable=True)
    valor_total = Column(Numeric(15, 2), nullable=True)
    probabilidade = Column(Numeric(5, 2), nullable=True)
    valor_ponderado = Column(Numeric(15, 2), nullable=True)
    status = Column(String(100), nullable=True)
    payload_json = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class PedidoVenda(Base):
    __tablename__ = "pedidos_venda"
    id = Column(Integer, primary_key=True, index=True)
    omie_id = Column(String(100), unique=True, index=True, nullable=False)
    numero_pedido = Column(String(100), nullable=True)
    cliente = Column(String(255), nullable=True)
    etapa = Column(String(100), nullable=True)
    status = Column(String(100), nullable=True)
    data_emissao = Column(String(20), nullable=True)
    valor_total = Column(Numeric(15, 2), nullable=True)
    payload_json = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


def init_db() -> None:
    Base.metadata.create_all(bind=engine)


class OmieClient:
    def __init__(self):
        if not settings.omie_app_key or not settings.omie_app_secret:
            raise ValueError("OMIE_APP_KEY e OMIE_APP_SECRET devem estar configurados.")

    async def call(self, endpoint: str, call: str, param: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        payload = {
            "call": call,
            "app_key": settings.omie_app_key,
            "app_secret": settings.omie_app_secret,
            "param": param or [{}],
        }
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(f"{OMIE_BASE_URL}/{endpoint}/", json=payload)
            response.raise_for_status()
            data = response.json()
            if isinstance(data, dict) and data.get("faultstring"):
                raise RuntimeError(data["faultstring"])
            return data

    async def listar_contas_receber(self, pagina: int = 1, registros_por_pagina: int = 50):
        return await self.call("financas/contareceber", "ListarContasReceber", [{"pagina": pagina, "registros_por_pagina": registros_por_pagina}])

    async def listar_contas_pagar(self, pagina: int = 1, registros_por_pagina: int = 50):
        return await self.call("financas/contapagar", "ListarContasPagar", [{"pagina": pagina, "registros_por_pagina": registros_por_pagina}])

    async def listar_oportunidades(self, pagina: int = 1, registros_por_pagina: int = 50):
        return await self.call("crm/oportunidades", "ListarOportunidades", [{"pagina": pagina, "registros_por_pagina": registros_por_pagina}])

    async def listar_pedidos_venda(self, pagina: int = 1, registros_por_pagina: int = 50):
        return await self.call("produtos/pedido", "ListarPedidos", [{"pagina": pagina, "registros_por_pagina": registros_por_pagina}])


def _safe_str(value: Any) -> Optional[str]:
    return None if value is None else str(value)


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0)
    except Exception:
        return 0.0

def normalize_receber(item: Dict[str, Any]) -> Dict[str, Any]:
    omie_id = (
        item.get("codigo_lancamento_omie")
        or item.get("nCodTitulo")
        or item.get("codigo_lancamento_integracao")
    )

    nome_cliente = (
        item.get("nome_fantasia")
        or item.get("razao_social")
        or item.get("cliente")
        or item.get("nome_cliente")
    )

    data_vencimento = item.get("data_vencimento") or item.get("data_previsao")
    data_emissao = item.get("data_emissao") or item.get("data_registro")

    valor_documento = _safe_float(item.get("valor_documento"))
    valor_saldo = _safe_float(item.get("valor_saldo"))

    return {
        "omie_id": _safe_str(omie_id),
        "codigo_cliente": _safe_str(item.get("codigo_cliente_fornecedor")),
        "nome_cliente": _safe_str(nome_cliente),
        "numero_documento": _safe_str(item.get("numero_documento_fiscal") or item.get("numero_documento")),
        "status_titulo": _safe_str(item.get("status_titulo")),
        "categoria": _safe_str(item.get("codigo_categoria")),
        "data_vencimento": _safe_str(data_vencimento),
        "data_emissao": _safe_str(data_emissao),
        "valor_documento": valor_documento,
        "valor_saldo": valor_saldo,
        "payload_json": str(item),
    }

def normalize_pagar(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "omie_id": _safe_str(item.get("codigo_lancamento_omie") or item.get("nCodTitulo") or item.get("codigo_lancamento_integracao")),
        "codigo_fornecedor": _safe_str(item.get("codigo_cliente_fornecedor")),
        "nome_fornecedor": _safe_str(item.get("nome_fantasia") or item.get("razao_social")),
        "numero_documento": _safe_str(item.get("numero_documento")),
        "status_titulo": _safe_str(item.get("status_titulo")),
        "categoria": _safe_str(item.get("codigo_categoria")),
        "data_vencimento": _safe_str(item.get("data_vencimento")),
        "data_emissao": _safe_str(item.get("data_emissao")),
        "valor_documento": _safe_float(item.get("valor_documento")),
        "valor_saldo": _safe_float(item.get("valor_saldo")),
        "payload_json": json.dumps(item, ensure_ascii=False),
    }


def normalize_oportunidade(item: Dict[str, Any]) -> Dict[str, Any]:
    valor_total = _safe_float(item.get("valor_total") or item.get("valor"))
    probabilidade = _safe_float(item.get("probabilidade"))
    return {
        "omie_id": _safe_str(item.get("codigo_oportunidade") or item.get("codigo") or item.get("id")),
        "titulo": _safe_str(item.get("titulo") or item.get("nome_oportunidade")),
        "cliente": _safe_str(item.get("cliente") or item.get("nome_cliente")),
        "etapa": _safe_str(item.get("etapa") or item.get("descricao_etapa")),
        "vendedor": _safe_str(item.get("vendedor") or item.get("nome_vendedor")),
        "previsao_fechamento": _safe_str(item.get("previsao_fechamento")),
        "valor_total": valor_total,
        "probabilidade": probabilidade,
        "valor_ponderado": valor_total * (probabilidade / 100),
        "status": _safe_str(item.get("status")),
        "payload_json": json.dumps(item, ensure_ascii=False),
    }


def normalize_pedido(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "omie_id": _safe_str(item.get("codigo_pedido") or item.get("codigo_pedido_omie") or item.get("id")),
        "numero_pedido": _safe_str(item.get("numero_pedido") or item.get("numero")),
        "cliente": _safe_str(item.get("nome_fantasia") or item.get("cliente")),
        "etapa": _safe_str(item.get("etapa")),
        "status": _safe_str(item.get("status_pedido") or item.get("status")),
        "data_emissao": _safe_str(item.get("data_previsao") or item.get("data_emissao")),
        "valor_total": _safe_float(item.get("valor_total_pedido") or item.get("total_pedido") or item.get("valor_total")),
        "payload_json": json.dumps(item, ensure_ascii=False),
    }


def _upsert(db: Session, model, key: str, data: Dict[str, Any]) -> None:
    existing = db.query(model).filter(getattr(model, key) == data[key]).first()
    if existing:
        for field, value in data.items():
            setattr(existing, field, value)
    else:
        db.add(model(**data))


async def sync_all_modules() -> Dict[str, int]:
    init_db()
    client = OmieClient()
    db = SessionLocal()
    result = {"receber": 0, "pagar": 0, "oportunidades": 0, "pedidos": 0}
    try:
        data = await client.listar_contas_receber()
        for item in data.get("conta_receber_cadastro") or data.get("lista_contas_receber") or []:
            normalized = normalize_receber(item)
            if normalized["omie_id"]:
                _upsert(db, ContaReceber, "omie_id", normalized)
                result["receber"] += 1

        data = await client.listar_contas_pagar()
        for item in data.get("conta_pagar_cadastro") or data.get("lista_contas_pagar") or []:
            normalized = normalize_pagar(item)
            if normalized["omie_id"]:
                _upsert(db, ContaPagar, "omie_id", normalized)
                result["pagar"] += 1

        data = await client.listar_oportunidades()
        for item in data.get("cadastro") or data.get("oportunidades") or data.get("lista_oportunidades") or []:
            normalized = normalize_oportunidade(item)
            if normalized["omie_id"]:
                _upsert(db, Oportunidade, "omie_id", normalized)
                result["oportunidades"] += 1

        data = await client.listar_pedidos_venda()
        for item in data.get("pedido_venda_produto") or data.get("pedidos") or data.get("lista_pedidos") or []:
            normalized = normalize_pedido(item)
            if normalized["omie_id"]:
                _upsert(db, PedidoVenda, "omie_id", normalized)
                result["pedidos"] += 1

        db.commit()
        return result
    finally:
        db.close()
