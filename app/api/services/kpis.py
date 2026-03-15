from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.integrations.omie.integration_module import (
    ContaCorrente,
    ContaPagar,
    ContaReceber,
    Oportunidade,
    PedidoVenda,
)

FIXED_COST_CATEGORIES = {
    "ALUGUEL",
    "SALARIOS_ADMIN",
    "HONORARIOS",
    "SOFTWARES",
    "CONTABILIDADE",
    "INTERNET",
    "SERVICOS_RECORRENTES",
}

VARIABLE_COST_CATEGORIES = {
    "COMISSOES",
    "FRETES",
    "CUSTO_MERCADORIA",
    "IMPOSTOS_VENDA",
    "SERVICOS_TERCEIROS_VARIAVEIS",
}

INVESTMENT_CATEGORIES = {
    "INVESTIMENTO",
    "CAPEX",
    "EQUIPAMENTOS",
    "IMPLANTACAO",
}

FASE_LABELS = {
    "9559746453": "01 Prospect",
    "9559746454": "02 Qualificação",
    "9559746455": "03 Apresentação",
    "9559746456": "04 Proposta",
    "9559746457": "05 Negociação",
    "9559746458": "06 Conclusão",
}


def _to_float(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except Exception:
        return 0.0


def _safe_div(numerator: float, denominator: float) -> float:
    return 0.0 if not denominator else numerator / denominator


def _days_until(target_date_str: Optional[str]) -> Optional[int]:
    if not target_date_str:
        return None

    raw = str(target_date_str).strip()

    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S"):
        try:
            target = datetime.strptime(raw[:19], fmt).date()
            return (target - date.today()).days
        except Exception:
            pass

    return None


def _fase_label(codigo: Optional[str]) -> str:
    codigo_str = str(codigo or "").strip()
    return FASE_LABELS.get(codigo_str, codigo_str or "Sem etapa")

class KPIService:
    def __init__(self, db: Session):
        self.db = db

    def total_receber_em_aberto(self) -> float:
        total = 0.0
        for row in self.db.query(ContaReceber).all():
            status = (row.status_titulo or "").upper()
            if status not in {"RECEBIDO", "CANCELADO", "BAIXADO"}:
                total += _to_float(row.valor_saldo or row.valor_documento)
        return round(total, 2)

    def total_pagar_em_aberto(self) -> float:
        total = 0.0
        for row in self.db.query(ContaPagar).all():
            status = (row.status_titulo or "").upper()
            if status not in {"PAGO", "CANCELADO", "BAIXADO"}:
                total += _to_float(row.valor_saldo or row.valor_documento)
        return round(total, 2)

    def faturamento_total_pedidos(self) -> float:
        total = 0.0
        for row in self.db.query(PedidoVenda).all():
            status = (row.status or "").upper()
            if "CANCELADO" in status:
                continue
            total += _to_float(row.valor_total)
        return round(total, 2)

    def pipeline_bruto(self) -> float:
        return round(sum(_to_float(row.valor_total) for row in self.db.query(Oportunidade).all()), 2)

    def pipeline_ponderado(self) -> float:
        return round(sum(_to_float(row.valor_ponderado) for row in self.db.query(Oportunidade).all()), 2)

    def total_saldo_contas_correntes(self) -> float:
        return round(sum(_to_float(row.saldo) for row in self.db.query(ContaCorrente).all()), 2)

    def saldos_por_conta(self) -> List[Dict[str, Any]]:
        rows = self.db.query(ContaCorrente).all()
        return [
            {
                "banco": row.banco or "Sem banco",
                "conta": row.conta or "Sem conta",
                "agencia": row.agencia or "",
                "descricao": row.descricao or "",
                "saldo": round(_to_float(row.saldo), 2),
                "status": row.status or "",
            }
            for row in rows
        ]

    def caixa_liquido(self) -> float:
        return round(self.total_saldo_contas_correntes() - self.total_pagar_em_aberto(), 2)

    def dividas_vencidas(self) -> float:
        total = 0.0
        for row in self.db.query(ContaPagar).all():
            status = (row.status_titulo or "").upper()
            if status in {"PAGO", "CANCELADO", "BAIXADO"}:
                continue
            days = _days_until(row.data_vencimento)
            valor = _to_float(row.valor_saldo or row.valor_documento)
            if days is not None and days < 0:
                total += valor
        return round(total, 2)

    def dividas_horizonte(self, max_days: int) -> float:
        total = 0.0
        for row in self.db.query(ContaPagar).all():
            status = (row.status_titulo or "").upper()
            if status in {"PAGO", "CANCELADO", "BAIXADO"}:
                continue
            days = _days_until(row.data_vencimento)
            valor = _to_float(row.valor_saldo or row.valor_documento)
            if days is not None and 0 <= days <= max_days:
                total += valor
        return round(total, 2)

    def top_obrigacoes(self, limit: int = 10) -> List[Dict[str, Any]]:
        rows = []
        for row in self.db.query(ContaPagar).all():
            status = (row.status_titulo or "").upper()
            if status in {"PAGO", "CANCELADO", "BAIXADO"}:
                continue
            rows.append({
                "fornecedor": row.nome_fornecedor or row.codigo_fornecedor or "Sem fornecedor",
                "documento": row.numero_documento or "",
                "vencimento": row.data_vencimento or "",
                "categoria": row.categoria or "",
                "valor": round(_to_float(row.valor_saldo or row.valor_documento), 2),
                "status": row.status_titulo or "",
            })
        rows.sort(key=lambda x: x["valor"], reverse=True)
        return rows[:limit]

    def _aging_receber_rows(self) -> List[ContaReceber]:
        rows = []
        for row in self.db.query(ContaReceber).all():
            status = (row.status_titulo or "").upper()
            if status in {"RECEBIDO", "CANCELADO", "BAIXADO"}:
                continue
            rows.append(row)
        return rows

    def _aging_pagar_rows(self) -> List[ContaPagar]:
        rows = []
        for row in self.db.query(ContaPagar).all():
            status = (row.status_titulo or "").upper()
            if status in {"PAGO", "CANCELADO", "BAIXADO"}:
                continue
            rows.append(row)
        return rows

    def aging_receber(self) -> List[Dict[str, Any]]:
        return self._aging(self._aging_receber_rows(), use_documento_if_zero=True)

    def aging_pagar(self) -> List[Dict[str, Any]]:
        return self._aging(self._aging_pagar_rows(), use_documento_if_zero=True)

    def _aging(self, rows: List[Any], use_documento_if_zero: bool = False) -> List[Dict[str, Any]]:
        buckets = {
            "A vencer": 0.0,
            "1-30": 0.0,
            "31-60": 0.0,
            "61-90": 0.0,
            "+90": 0.0,
        }

        for row in rows:
            days = _days_until(row.data_vencimento)
            valor = _to_float(row.valor_saldo)

            if use_documento_if_zero and valor == 0:
                valor = _to_float(row.valor_documento)

            if days is None:
                continue

            if days >= 0:
                buckets["A vencer"] += valor
            else:
                overdue = abs(days)
                if overdue <= 30:
                    buckets["1-30"] += valor
                elif overdue <= 60:
                    buckets["31-60"] += valor
                elif overdue <= 90:
                    buckets["61-90"] += valor
                else:
                    buckets["+90"] += valor

        return [{"label": k, "value": round(v, 2)} for k, v in buckets.items()]

    def inadimplencia_total(self) -> float:
        total = 0.0
        for row in self.db.query(ContaReceber).all():
            status = (row.status_titulo or "").upper()
            if status in {"RECEBIDO", "CANCELADO", "BAIXADO"}:
                continue
            days = _days_until(row.data_vencimento)
            valor = _to_float(row.valor_saldo or row.valor_documento)
            if days is not None and days < 0:
                total += valor
        return round(total, 2)

    def receber_horizonte(self, max_days: int) -> float:
        total = 0.0
        for row in self.db.query(ContaReceber).all():
            status = (row.status_titulo or "").upper()
            if status in {"RECEBIDO", "CANCELADO", "BAIXADO"}:
                continue
            days = _days_until(row.data_vencimento)
            valor = _to_float(row.valor_saldo or row.valor_documento)
            if days is not None and 0 <= days <= max_days:
                total += valor
        return round(total, 2)

    def pagar_horizonte(self, max_days: int) -> float:
        total = 0.0
        for row in self.db.query(ContaPagar).all():
            status = (row.status_titulo or "").upper()
            if status in {"PAGO", "CANCELADO", "BAIXADO"}:
                continue
            days = _days_until(row.data_vencimento)
            valor = _to_float(row.valor_saldo or row.valor_documento)
            if days is not None and 0 <= days <= max_days:
                total += valor
        return round(total, 2)

    def top_inadimplentes(self, limit: int = 10) -> List[Dict[str, Any]]:
        grouped: Dict[str, float] = {}
        for row in self.db.query(ContaReceber).all():
            status = (row.status_titulo or "").upper()
            if status in {"RECEBIDO", "CANCELADO", "BAIXADO"}:
                continue
            days = _days_until(row.data_vencimento)
            if days is not None and days < 0:
                nome = row.nome_cliente or row.codigo_cliente or "Sem nome"
                grouped[nome] = grouped.get(nome, 0.0) + _to_float(row.valor_saldo or row.valor_documento)

        return [
            {"cliente": k, "valor": round(v, 2)}
            for k, v in sorted(grouped.items(), key=lambda x: x[1], reverse=True)[:limit]
        ]

    def receita_liquida(self) -> float:
        return self.faturamento_total_pedidos()

    def custos_variaveis(self) -> float:
        total = 0.0
        for row in self.db.query(ContaPagar).all():
            if (row.categoria or "").upper() in VARIABLE_COST_CATEGORIES:
                total += _to_float(row.valor_documento)
        return round(total, 2)

    def custos_fixos(self) -> float:
        total = 0.0
        for row in self.db.query(ContaPagar).all():
            if (row.categoria or "").upper() in FIXED_COST_CATEGORIES:
                total += _to_float(row.valor_documento)
        return round(total, 2)

    def investimentos(self) -> float:
        total = 0.0
        for row in self.db.query(ContaPagar).all():
            if (row.categoria or "").upper() in INVESTMENT_CATEGORIES:
                total += _to_float(row.valor_documento)
        return round(total, 2)

    def margem_contribuicao(self) -> float:
        return round(self.receita_liquida() - self.custos_variaveis(), 2)

    def margem_contribuicao_percentual(self) -> float:
        return round(_safe_div(self.margem_contribuicao(), self.receita_liquida()) * 100, 2)

    def ebitda(self) -> float:
        return round(self.receita_liquida() - self.custos_variaveis() - self.custos_fixos(), 2)

    def margem_ebitda(self) -> float:
        return round(_safe_div(self.ebitda(), self.receita_liquida()) * 100, 2)

    def ponto_equilibrio(self) -> float:
        indice_mc = self.margem_contribuicao_percentual() / 100
        return 0.0 if indice_mc <= 0 else round(self.custos_fixos() / indice_mc, 2)

    def margem_seguranca(self) -> float:
        return round(self.receita_liquida() - self.ponto_equilibrio(), 2)

    def roi(self) -> float:
        investimento = self.investimentos()
        return 0.0 if investimento <= 0 else round(((self.ebitda() - investimento) / investimento) * 100, 2)

    def ticket_medio(self) -> float:
        count = self.db.query(PedidoVenda).count()
        return round(_safe_div(self.faturamento_total_pedidos(), count), 2)

    def total_oportunidades_abertas(self) -> int:
        return self.db.query(Oportunidade).count()

    def funil_comercial(self) -> List[Dict[str, Any]]:
        grouped: Dict[str, int] = {}
        for row in self.db.query(Oportunidade).all():
            etapa = _fase_label(row.etapa)
            grouped[etapa] = grouped.get(etapa, 0) + 1

        return [{"fase": k, "quantidade": v} for k, v in sorted(grouped.items(), key=lambda x: x[0])]

    def top_vendedores(self, limit: int = 10) -> List[Dict[str, Any]]:
        grouped: Dict[str, Dict[str, Any]] = {}
        for row in self.db.query(Oportunidade).all():
            vendedor = row.vendedor or "Sem vendedor"
            grouped.setdefault(vendedor, {"nome": vendedor, "receita": 0.0, "pipeline": 0.0, "qtd": 0})
            grouped[vendedor]["receita"] += _to_float(row.valor_total)
            grouped[vendedor]["pipeline"] += _to_float(row.valor_ponderado)
            grouped[vendedor]["qtd"] += 1
        return sorted(grouped.values(), key=lambda x: x["pipeline"], reverse=True)[:limit]

    def top_clientes(self, limit: int = 10) -> List[Dict[str, Any]]:
        grouped: Dict[str, float] = {}
        for row in self.db.query(PedidoVenda).all():
            cliente = row.cliente or "Sem cliente"
            grouped[cliente] = grouped.get(cliente, 0.0) + _to_float(row.valor_total)
        return [{"nome": k, "receita": round(v, 2)} for k, v in sorted(grouped.items(), key=lambda x: x[1], reverse=True)[:limit]]

    def ceo_dashboard(self, empresa: str = "consolidado", periodo: str = "mes_atual") -> Dict[str, Any]:
        return {
            "empresa": empresa,
            "periodo": periodo,
            "faturamento_mes": self.receita_liquida(),
            "receita_liquida": self.receita_liquida(),
            "ebitda": self.ebitda(),
            "margem_ebitda": self.margem_ebitda(),
            "caixa_disponivel": round(self.total_receber_em_aberto() - self.total_pagar_em_aberto(), 2),
            "saldo_contas_correntes": self.total_saldo_contas_correntes(),
            "caixa_liquido": self.caixa_liquido(),
            "receber_30": self.receber_horizonte(30),
            "pagar_30": self.pagar_horizonte(30),
            "pipeline_ponderado": self.pipeline_ponderado(),
            "custo_fixo": self.custos_fixos(),
            "ponto_equilibrio": self.ponto_equilibrio(),
            "margem_seguranca": self.margem_seguranca(),
            "inadimplencia": self.inadimplencia_total(),
            "roi": self.roi(),
            "ticket_medio": self.ticket_medio(),
            "atualizado_em": datetime.utcnow().isoformat(),
        }

    def financeiro_dashboard(self) -> Dict[str, Any]:
        return {
            "receber_aging": self.aging_receber(),
            "pagar_aging": self.aging_pagar(),
            "fluxo_caixa": [
                {"horizonte": "7 dias", "receber": self.receber_horizonte(7), "pagar": self.pagar_horizonte(7)},
                {"horizonte": "15 dias", "receber": self.receber_horizonte(15), "pagar": self.pagar_horizonte(15)},
                {"horizonte": "30 dias", "receber": self.receber_horizonte(30), "pagar": self.pagar_horizonte(30)},
                {"horizonte": "60 dias", "receber": self.receber_horizonte(60), "pagar": self.pagar_horizonte(60)},
            ],
            "top_inadimplentes": self.top_inadimplentes(),
            "dividas_total": self.total_pagar_em_aberto(),
            "dividas_vencidas": self.dividas_vencidas(),
            "dividas_7": self.dividas_horizonte(7),
            "dividas_15": self.dividas_horizonte(15),
            "dividas_30": self.dividas_horizonte(30),
            "top_obrigacoes": self.top_obrigacoes(),
            "saldos_contas": self.saldos_por_conta(),
            "saldo_total_contas": self.total_saldo_contas_correntes(),
            "caixa_liquido": self.caixa_liquido(),
        }

    def comercial_dashboard(self) -> Dict[str, Any]:
        return {
            "funil": self.funil_comercial(),
            "vendedores": self.top_vendedores(),
            "clientes": self.top_clientes(),
            "pipeline_bruto": self.pipeline_bruto(),
            "pipeline_ponderado": self.pipeline_ponderado(),
            "oportunidades_abertas": self.total_oportunidades_abertas(),
            "ticket_medio": self.ticket_medio(),
        }
