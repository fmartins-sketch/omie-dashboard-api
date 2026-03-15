import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
from sqlalchemy import Column, DateTime, Integer, Numeric, String, Text
from sqlalchemy.orm import Session

from app.db.session import Base, SessionLocal, engine

OMIE_APP_KEY = os.getenv("OMIE_APP_KEY", "")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET", "")
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


class ContaCorrente(Base):
    __tablename__ = "contas_correntes"

    id = Column(Integer, primary_key=True, index=True)
    omie_id = Column(String(100), unique=True, index=True, nullable=False)
    codigo_banco = Column(String(50), nullable=True)
    banco = Column(String(255), nullable=True)
    agencia = Column(String(50), nullable=True)
    conta = Column(String(100), nullable=True)
    descricao = Column(String(255), nullable=True)
    saldo = Column(Numeric(15, 2), nullable=True)
    status = Column(String(50), nullable=True)
    payload_json = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class CRMFase(Base):
    __tablename__ = "crm_fases"

    id = Column(Integer, primary_key=True, index=True)
    omie_id = Column(String(100), unique=True, index=True, nullable=False)
    nome = Column(String(255), nullable=True)
    payload_json = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class CRMVendedor(Base):
    __tablename__ = "crm_vendedores"

    id = Column(Integer, primary_key=True, index=True)
    omie_id = Column(String(100), unique=True, index=True, nullable=False)
    nome = Column(String(255), nullable=True)
    email = Column(String(255), nullable=True)
    status = Column(String(50), nullable=True)
    payload_json = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class CRMConta(Base):
    __tablename__ = "crm_contas"

    id = Column(Integer, primary_key=True, index=True)
    omie_id = Column(String(100), unique=True, index=True, nullable=False)
    nome = Column(String(255), nullable=True)
    documento = Column(String(100), nullable=True)
    cidade = Column(String(100), nullable=True)
    estado = Column(String(20), nullable=True)
    status = Column(String(50), nullable=True)
    payload_json = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


def init_db() -> None:
    Base.metadata.create_all(bind=engine)


class OmieClient:
    def __init__(self, app_key: str, app_secret: str, timeout: int = 60):
        if not app_key or not app_secret:
            raise ValueError("OMIE_APP_KEY e OMIE_APP_SECRET devem estar configurados.")
        self.app_key = app_key
        self.app_secret = app_secret
        self.timeout = timeout

    async def call(
        self,
        endpoint: str,
        call: str,
        param: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        url = f"{OMIE_BASE_URL}/{endpoint}/"
        payload = {
            "call": call,
            "app_key": self.app_key,
            "app_secret": self.app_secret,
            "param": param or [{}],
        }

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(url, json=payload)
            response.raise_for_status()
            data = response.json()
            if isinstance(data, dict) and data.get("faultstring"):
                raise RuntimeError(f"Erro Omie: {data['faultstring']}")
            return data

    async def listar_contas_receber(self, pagina: int = 1, registros_por_pagina: int = 50) -> Dict[str, Any]:
        return await self.call(
            endpoint="financas/contareceber",
            call="ListarContasReceber",
            param=[{
                "pagina": pagina,
                "registros_por_pagina": registros_por_pagina,
                "apenas_importado_api": "N",
            }],
        )

    async def listar_contas_pagar(self, pagina: int = 1, registros_por_pagina: int = 50) -> Dict[str, Any]:
        return await self.call(
            endpoint="financas/contapagar",
            call="ListarContasPagar",
            param=[{
                "pagina": pagina,
                "registros_por_pagina": registros_por_pagina,
                "apenas_importado_api": "N",
            }],
        )

    async def listar_oportunidades(self, pagina: int = 1, registros_por_pagina: int = 50) -> Dict[str, Any]:
        return await self.call(
            endpoint="crm/oportunidades",
            call="ListarOportunidades",
            param=[{
                "pagina": pagina,
                "registros_por_pagina": registros_por_pagina,
            }],
        )

    async def listar_pedidos_venda(self, pagina: int = 1, registros_por_pagina: int = 50) -> Dict[str, Any]:
        return await self.call(
            endpoint="produtos/pedido",
            call="ListarPedidos",
            param=[{
                "pagina": pagina,
                "registros_por_pagina": registros_por_pagina,
                "apenas_importado_api": "N",
            }],
        )

    async def listar_contas_correntes(self, pagina: int = 1, registros_por_pagina: int = 50) -> Dict[str, Any]:
        return await self.call(
            endpoint="geral/contacorrente",
            call="ListarContasCorrentes",
            param=[{
                "pagina": pagina,
                "registros_por_pagina": registros_por_pagina,
            }],
        )

    async def listar_fases(self, pagina: int = 1, registros_por_pagina: int = 50) -> Dict[str, Any]:
        return await self.call(
            endpoint="crm/fases",
            call="ListarFases",
            param=[{
                "pagina": pagina,
                "registros_por_pagina": registros_por_pagina,
            }],
        )

    async def listar_vendedores_crm(self, pagina: int = 1, registros_por_pagina: int = 50) -> Dict[str, Any]:
        return await self.call(
            endpoint="crm/vendedores",
            call="ListarVendedores",
            param=[{
                "pagina": pagina,
                "registros_por_pagina": registros_por_pagina,
            }],
        )

    async def listar_contas_crm(self, pagina: int = 1, registros_por_pagina: int = 50) -> Dict[str, Any]:
        return await self.call(
            endpoint="crm/contas",
            call="ListarContas",
            param=[{
                "pagina": pagina,
                "registros_por_pagina": registros_por_pagina,
            }],
        )


def _safe_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    return str(value)


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

    return {
        "omie_id": _safe_str(omie_id),
        "codigo_cliente": _safe_str(item.get("codigo_cliente_fornecedor")),
        "nome_cliente": _safe_str(nome_cliente),
        "numero_documento": _safe_str(item.get("numero_documento_fiscal") or item.get("numero_documento")),
        "status_titulo": _safe_str(item.get("status_titulo")),
        "categoria": _safe_str(item.get("codigo_categoria")),
        "data_vencimento": _safe_str(data_vencimento),
        "data_emissao": _safe_str(data_emissao),
        "valor_documento": _safe_float(item.get("valor_documento")),
        "valor_saldo": _safe_float(item.get("valor_saldo")),
        "payload_json": str(item),
    }


def normalize_pagar(item: Dict[str, Any]) -> Dict[str, Any]:
    omie_id = (
        item.get("codigo_lancamento_omie")
        or item.get("nCodTitulo")
        or item.get("codigo_lancamento_integracao")
    )

    nome_fornecedor = (
        item.get("nome_fantasia")
        or item.get("razao_social")
        or item.get("fornecedor")
        or item.get("nome_fornecedor")
    )

    data_vencimento = item.get("data_vencimento") or item.get("data_previsao")
    data_emissao = item.get("data_emissao") or item.get("data_registro")

    return {
        "omie_id": _safe_str(omie_id),
        "codigo_fornecedor": _safe_str(item.get("codigo_cliente_fornecedor")),
        "nome_fornecedor": _safe_str(nome_fornecedor),
        "numero_documento": _safe_str(item.get("numero_documento_fiscal") or item.get("numero_documento")),
        "status_titulo": _safe_str(item.get("status_titulo")),
        "categoria": _safe_str(item.get("codigo_categoria")),
        "data_vencimento": _safe_str(data_vencimento),
        "data_emissao": _safe_str(data_emissao),
        "valor_documento": _safe_float(item.get("valor_documento")),
        "valor_saldo": _safe_float(item.get("valor_saldo")),
        "payload_json": str(item),
    }


def normalize_oportunidade(item: Dict[str, Any]) -> Dict[str, Any]:
    identificacao = item.get("identificacao", {}) or {}
    fases = item.get("fasesStatus", {}) or {}
    previsao = item.get("previsaoTemp", {}) or {}
    ticket = item.get("ticket", {}) or {}
    outras = item.get("outrasInf", {}) or {}

    valor_total = _safe_float(ticket.get("nTicket"))
    probabilidade = _safe_float(previsao.get("nTemperatura"))

    previsao_fechamento = None
    ano = previsao.get("nAnoPrev")
    mes = previsao.get("nMesPrev")
    if ano and mes:
        try:
            previsao_fechamento = f"{int(ano):04d}-{int(mes):02d}-01"
        except Exception:
            previsao_fechamento = None

    return {
        "omie_id": _safe_str(identificacao.get("nCodOp")),
        "titulo": _safe_str(identificacao.get("cDesOp")),
        "cliente": _safe_str(identificacao.get("nCodConta")),
        "etapa": _safe_str(fases.get("nCodFase")),
        "vendedor": _safe_str(identificacao.get("nCodVendedor")),
        "previsao_fechamento": _safe_str(previsao_fechamento or outras.get("dAlteracao")),
        "valor_total": valor_total,
        "probabilidade": probabilidade,
        "valor_ponderado": valor_total * (probabilidade / 100),
        "status": _safe_str(fases.get("nCodStatus")),
        "payload_json": str(item),
    }


def normalize_pedido(item: Dict[str, Any]) -> Dict[str, Any]:
    cab = item.get("cabecalho", {}) or {}
    total = item.get("total_pedido", {}) or {}
    info = item.get("infoCadastro", {}) or {}

    status_parts = []
    if info.get("cancelado") == "S":
        status_parts.append("CANCELADO")
    if info.get("faturado") == "S":
        status_parts.append("FATURADO")
    if not status_parts:
        status_parts.append("ABERTO")

    return {
        "omie_id": _safe_str(cab.get("codigo_pedido")),
        "numero_pedido": _safe_str(cab.get("numero_pedido")),
        "cliente": _safe_str(cab.get("codigo_cliente")),
        "etapa": _safe_str(cab.get("etapa")),
        "status": " / ".join(status_parts),
        "data_emissao": _safe_str(cab.get("data_previsao")),
        "valor_total": _safe_float(total.get("valor_total_pedido")),
        "payload_json": str(item),
    }


def normalize_conta_corrente(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "omie_id": _safe_str(item.get("nCodCC")),
        "codigo_banco": _safe_str(item.get("codigo_banco")),
        "banco": _safe_str(item.get("descricao")),
        "agencia": _safe_str(item.get("codigo_agencia")),
        "conta": _safe_str(item.get("numero_conta_corrente")),
        "descricao": _safe_str(item.get("descricao")),
        "saldo": _safe_float(item.get("saldo_inicial")),
        "status": "INATIVO" if item.get("inativo") == "S" else "ATIVO",
        "payload_json": str(item),
    }


def normalize_fase(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "omie_id": _safe_str(item.get("cCodigo")),
        "nome": _safe_str(item.get("cDescricao")),
        "payload_json": str(item),
    }


def normalize_vendedor_crm(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "omie_id": _safe_str(
            item.get("nCodVendedor")
            or item.get("codigo")
            or item.get("cCodigo")
            or item.get("id")
        ),
        "nome": _safe_str(
            item.get("cNome")
            or item.get("nome")
            or item.get("cDescricao")
            or item.get("descricao")
        ),
        "email": _safe_str(item.get("cEmail") or item.get("email")),
        "status": _safe_str(item.get("status") or item.get("cStatus") or "ATIVO"),
        "payload_json": str(item),
    }


def normalize_conta_crm(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "omie_id": _safe_str(
            item.get("nCodConta")
            or item.get("codigo")
            or item.get("id")
        ),
        "nome": _safe_str(
            item.get("cNome")
            or item.get("nome_fantasia")
            or item.get("razao_social")
            or item.get("nome")
        ),
        "documento": _safe_str(item.get("cnpj_cpf") or item.get("documento")),
        "cidade": _safe_str(item.get("cidade")),
        "estado": _safe_str(item.get("estado")),
        "status": _safe_str(item.get("status") or "ATIVO"),
        "payload_json": str(item),
    }


def upsert_conta_receber(db: Session, normalized: Dict[str, Any]) -> None:
    existing = db.query(ContaReceber).filter(ContaReceber.omie_id == normalized["omie_id"]).first()
    if existing:
        for key, value in normalized.items():
            setattr(existing, key, value)
    else:
        db.add(ContaReceber(**normalized))


def upsert_conta_pagar(db: Session, normalized: Dict[str, Any]) -> None:
    existing = db.query(ContaPagar).filter(ContaPagar.omie_id == normalized["omie_id"]).first()
    if existing:
        for key, value in normalized.items():
            setattr(existing, key, value)
    else:
        db.add(ContaPagar(**normalized))


def upsert_oportunidade(db: Session, normalized: Dict[str, Any]) -> None:
    existing = db.query(Oportunidade).filter(Oportunidade.omie_id == normalized["omie_id"]).first()
    if existing:
        for key, value in normalized.items():
            setattr(existing, key, value)
    else:
        db.add(Oportunidade(**normalized))


def upsert_pedido(db: Session, normalized: Dict[str, Any]) -> None:
    existing = db.query(PedidoVenda).filter(PedidoVenda.omie_id == normalized["omie_id"]).first()
    if existing:
        for key, value in normalized.items():
            setattr(existing, key, value)
    else:
        db.add(PedidoVenda(**normalized))


def upsert_conta_corrente(db: Session, normalized: Dict[str, Any]) -> None:
    existing = db.query(ContaCorrente).filter(ContaCorrente.omie_id == normalized["omie_id"]).first()
    if existing:
        for key, value in normalized.items():
            setattr(existing, key, value)
    else:
        db.add(ContaCorrente(**normalized))


def upsert_fase(db: Session, normalized: Dict[str, Any]) -> None:
    existing = db.query(CRMFase).filter(CRMFase.omie_id == normalized["omie_id"]).first()
    if existing:
        for key, value in normalized.items():
            setattr(existing, key, value)
    else:
        db.add(CRMFase(**normalized))


def upsert_vendedor_crm(db: Session, normalized: Dict[str, Any]) -> None:
    existing = db.query(CRMVendedor).filter(CRMVendedor.omie_id == normalized["omie_id"]).first()
    if existing:
        for key, value in normalized.items():
            setattr(existing, key, value)
    else:
        db.add(CRMVendedor(**normalized))


def upsert_conta_crm(db: Session, normalized: Dict[str, Any]) -> None:
    existing = db.query(CRMConta).filter(CRMConta.omie_id == normalized["omie_id"]).first()
    if existing:
        for key, value in normalized.items():
            setattr(existing, key, value)
    else:
        db.add(CRMConta(**normalized))


async def sync_contas_receber(db: Session, client: OmieClient, paginas: int = 3) -> int:
    total = 0
    for pagina in range(1, paginas + 1):
        data = await client.listar_contas_receber(pagina=pagina)
        items = data.get("conta_receber_cadastro") or data.get("lista_contas_receber") or []
        for item in items:
            normalized = normalize_receber(item)
            if normalized["omie_id"]:
                upsert_conta_receber(db, normalized)
                total += 1
    db.commit()
    return total


async def sync_contas_pagar(db: Session, client: OmieClient, paginas: int = 3) -> int:
    total = 0
    for pagina in range(1, paginas + 1):
        data = await client.listar_contas_pagar(pagina=pagina)
        items = data.get("conta_pagar_cadastro") or data.get("lista_contas_pagar") or []
        for item in items:
            normalized = normalize_pagar(item)
            if normalized["omie_id"]:
                upsert_conta_pagar(db, normalized)
                total += 1
    db.commit()
    return total


async def sync_oportunidades(db: Session, client: OmieClient, paginas: int = 3) -> int:
    total = 0
    for pagina in range(1, paginas + 1):
        data = await client.listar_oportunidades(pagina=pagina)
        items = data.get("cadastros") or data.get("oportunidades") or data.get("lista_oportunidades") or []
        for item in items:
            normalized = normalize_oportunidade(item)
            if normalized["omie_id"]:
                upsert_oportunidade(db, normalized)
                total += 1
    db.commit()
    return total


async def sync_pedidos(db: Session, client: OmieClient, paginas: int = 3) -> int:
    total = 0
    for pagina in range(1, paginas + 1):
        data = await client.listar_pedidos_venda(pagina=pagina)
        items = data.get("pedido_venda_produto") or data.get("pedidos") or data.get("lista_pedidos") or []
        for item in items:
            normalized = normalize_pedido(item)
            if normalized["omie_id"]:
                upsert_pedido(db, normalized)
                total += 1
    db.commit()
    return total


async def sync_contas_correntes(db: Session, client: OmieClient, paginas: int = 1) -> int:
    total = 0
    data = await client.listar_contas_correntes()
    items = data.get("ListarContasCorrentes") or []
    for item in items:
        normalized = normalize_conta_corrente(item)
        if normalized["omie_id"]:
            upsert_conta_corrente(db, normalized)
            total += 1
    db.commit()
    return total


async def sync_fases(db: Session, client: OmieClient, paginas: int = 1) -> int:
    total = 0
    for pagina in range(1, paginas + 1):
        data = await client.listar_fases(pagina=pagina)
        items = data.get("cadastros") or data.get("fases") or data.get("lista_fases") or []
        for item in items:
            normalized = normalize_fase(item)
            if normalized["omie_id"]:
                upsert_fase(db, normalized)
                total += 1
    db.commit()
    return total


async def sync_vendedores_crm(db: Session, client: OmieClient, paginas: int = 2) -> int:
    total = 0
    for pagina in range(1, paginas + 1):
        data = await client.listar_vendedores_crm(pagina=pagina)
        items = data.get("cadastros") or data.get("vendedores") or data.get("lista_vendedores") or []
        for item in items:
            normalized = normalize_vendedor_crm(item)
            if normalized["omie_id"]:
                upsert_vendedor_crm(db, normalized)
                total += 1
    db.commit()
    return total


async def sync_contas_crm(db: Session, client: OmieClient, paginas: int = 3) -> int:
    total = 0
    for pagina in range(1, paginas + 1):
        data = await client.listar_contas_crm(pagina=pagina)
        items = data.get("cadastros") or data.get("contas") or data.get("lista_contas") or []
        for item in items:
            normalized = normalize_conta_crm(item)
            if normalized["omie_id"]:
                upsert_conta_crm(db, normalized)
                total += 1
    db.commit()
    return total


async def sync_all_modules() -> Dict[str, int]:
    init_db()
    client = OmieClient(app_key=OMIE_APP_KEY, app_secret=OMIE_APP_SECRET)
    db = SessionLocal()
    try:
        receber = await sync_contas_receber(db, client)
        pagar = await sync_contas_pagar(db, client)
        oportunidades = await sync_oportunidades(db, client)
        pedidos = await sync_pedidos(db, client)

        contas_correntes = 0
        try:
            contas_correntes = await sync_contas_correntes(db, client)
        except Exception:
            contas_correntes = 0

        fases = 0
        try:
            fases = await sync_fases(db, client)
        except Exception:
            fases = 0

        vendedores_crm = 0
        try:
            vendedores_crm = await sync_vendedores_crm(db, client)
        except Exception:
            vendedores_crm = 0

        contas_crm = 0
        try:
            contas_crm = await sync_contas_crm(db, client)
        except Exception:
            contas_crm = 0

        return {
            "receber": receber,
            "pagar": pagar,
            "oportunidades": oportunidades,
            "pedidos": pedidos,
            "contas_correntes": contas_correntes,
            "fases": fases,
            "vendedores_crm": vendedores_crm,
            "contas_crm": contas_crm,
        }
    finally:
        db.close()
