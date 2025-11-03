from datetime import datetime, date
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.deps import get_current_ctx, DbSession
from app.utils.filters import is_safe_select, apply_placeholders
from app.models import Indicator
from pydantic import BaseModel, Field, ConfigDict


router = APIRouter(prefix="/indicators", tags=["indicators"])


def _date_or_default(v: Optional[date], default: Optional[date] = None):
    return v or default


@router.get('/valor-mes-a-mes')
def valor_mes_a_mes(db: DbSession, ctx=Depends(get_current_ctx), from_: Optional[date] = None, to: Optional[date] = None, uf: Optional[str] = None,
                    situacao_processo: Optional[str] = None):
    # Aggregate sum(vl_titulo) by year-month of dt_cadastro
    sql = """
    SELECT strftime('%Y-%m', dt_cadastro) as ym, COALESCE(SUM(vl_titulo), 0) AS total
    FROM curated_records
    WHERE organization_id = :tenant_id
      AND (:from IS NULL OR date(dt_cadastro) >= :from)
      AND (:to IS NULL OR date(dt_cadastro) <= :to)
      AND (:uf IS NULL OR uf = :uf)
      AND (:situacao_processo IS NULL OR situacao_processo = :situacao_processo)
    GROUP BY ym
    ORDER BY ym
    """
    # For Postgres compatibility, use generic functions when not sqlite
    if db.bind and 'postgresql' in str(db.bind.url):
        sql = sql.replace("strftime('%Y-%m', dt_cadastro)", "TO_CHAR(dt_cadastro, 'YYYY-MM')").replace("date(dt_cadastro)", "CAST(dt_cadastro AS DATE)")

    params = {
        'tenant_id': ctx.organization_id,
        'from': from_,
        'to': to,
        'uf': uf,
        'situacao_processo': situacao_processo,
    }
    rows = db.execute(text(sql), params).mappings().all()
    return {"series": rows}


@router.get('/mapa-por-uf')
def mapa_por_uf(db: DbSession, ctx=Depends(get_current_ctx), from_: Optional[date] = None, to: Optional[date] = None):
    sql = """
    SELECT uf, COALESCE(SUM(vl_titulo), 0) AS total
    FROM curated_records
    WHERE organization_id = :tenant_id
      AND (:from IS NULL OR date(dt_cadastro) >= :from)
      AND (:to IS NULL OR date(dt_cadastro) <= :to)
    GROUP BY uf
    ORDER BY total DESC
    """
    if db.bind and 'postgresql' in str(db.bind.url):
        sql = sql.replace("date(dt_cadastro)", "CAST(dt_cadastro AS DATE)")
    params = {'tenant_id': ctx.organization_id, 'from': from_, 'to': to}
    rows = db.execute(text(sql), params).mappings().all()
    return {"series": rows}


@router.get('/total-por-faixa-vencimento')
def total_por_faixa_vencimento(db: DbSession, ctx=Depends(get_current_ctx)):
    sql = """
    SELECT faixa_vencimento, COALESCE(SUM(vl_titulo), 0) AS total
    FROM curated_records
    WHERE organization_id = :tenant_id
    GROUP BY faixa_vencimento
    ORDER BY total DESC
    """
    rows = db.execute(text(sql), {'tenant_id': ctx.organization_id}).mappings().all()
    return {"series": rows}


@router.get('/recuperado-por-faixa-vencimento')
def recuperado_por_faixa_vencimento(db: DbSession, ctx=Depends(get_current_ctx)):
    sql = """
    SELECT faixa_vencimento, COALESCE(SUM(vl_total_repasse), 0) AS total
    FROM curated_records
    WHERE organization_id = :tenant_id
    GROUP BY faixa_vencimento
    ORDER BY total DESC
    """
    rows = db.execute(text(sql), {'tenant_id': ctx.organization_id}).mappings().all()
    return {"series": rows}


# -------- CRUD/Preview para Indicadores customizados --------

class IndicatorCreate(BaseModel):
    key: str
    name: str
    formula_sql: str
    dataset: str | None = None
    fmt: str | None = None
    default_filters_json: dict | None = None
    category: str | None = None


@router.get("")
def list_indicators(db: DbSession, ctx=Depends(get_current_ctx)):
    rows = db.execute(text("""
        SELECT id, key, name, dataset, fmt, category
        FROM indicators
        WHERE organization_id=:o
        ORDER BY COALESCE(NULLIF(category,''),'~'), name
    """), {"o": ctx.organization_id}).mappings().all()
    return rows


@router.post("")
def create_indicator(payload: IndicatorCreate, db: DbSession, ctx=Depends(get_current_ctx)):
    if not is_safe_select(payload.formula_sql):
        raise HTTPException(status_code=400, detail="Apenas SELECT é permitido")
    # create or update by (org, key) to evitar duplicados
    existing = db.execute(text("SELECT id FROM indicators WHERE organization_id=:o AND key=:k"), {"o": ctx.organization_id, "k": payload.key}).first()
    if existing:
        db.execute(text(
            """
            UPDATE indicators SET name=:n, dataset=:d, formula_sql=:f, default_filters_json=:df, fmt=:fmt, category=:cat
            WHERE id=:id
            """
        ), {"n": payload.name, "d": payload.dataset, "f": payload.formula_sql, "df": payload.default_filters_json, "fmt": payload.fmt, "cat": payload.category, "id": existing[0]})
        db.commit()
        row = db.execute(text("SELECT id, key, name, dataset, fmt, category FROM indicators WHERE id=:id"), {"id": existing[0]}).mappings().first()
        return row
    else:
        db.execute(text(
            """
            INSERT INTO indicators (organization_id, key, name, dataset, formula_sql, default_filters_json, fmt, category, created_at)
            VALUES (:o, :k, :n, :d, :f, :df, :fmt, :cat, CURRENT_TIMESTAMP)
            """
        ), {"o": ctx.organization_id, "k": payload.key, "n": payload.name, "d": payload.dataset, "f": payload.formula_sql, "df": payload.default_filters_json, "fmt": payload.fmt, "cat": payload.category})
        db.commit()
        row = db.execute(text("SELECT id, key, name, dataset, fmt, category FROM indicators WHERE organization_id=:o AND key=:k"), {"o": ctx.organization_id, "k": payload.key}).mappings().first()
        return row


class IndicatorRunIn(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra='ignore')

    from_: date | None = Field(default=None, alias="from")
    to: date | None = None
    uf: str | None = None
    situacao_processo: str | None = None
    credor_code: str | None = None
    date_field: str | None = None


@router.post('/preview')
def preview_indicator(body: IndicatorCreate, db: DbSession, ctx=Depends(get_current_ctx)):
    if not is_safe_select(body.formula_sql):
        raise HTTPException(status_code=400, detail="Apenas SELECT é permitido")
    sql, params = apply_placeholders(body.formula_sql, {"tenant_id": ctx.organization_id})
    wrapped = f"SELECT * FROM ({sql}) t LIMIT 200"
    rows = db.execute(text(wrapped), params).mappings().all()
    return {"rows": rows}


@router.post('/{indicator_id}/run')
def run_indicator(indicator_id: int, body: IndicatorRunIn, db: DbSession, ctx=Depends(get_current_ctx)):
    row = db.execute(text("SELECT formula_sql FROM indicators WHERE id=:i AND organization_id=:o"), {"i": indicator_id, "o": ctx.organization_id}).first()
    if not row:
        raise HTTPException(status_code=404, detail="Indicador não encontrado")
    formula_sql = row[0]
    if not is_safe_select(formula_sql):
        raise HTTPException(status_code=400, detail="SQL inválido")
    # Remove trailing semicolon for execution safety
    formula_sql = formula_sql.rstrip().rstrip(';')
    sql, params = apply_placeholders(formula_sql, {
        "tenant_id": ctx.organization_id,
        "from": body.from_,
        "to": body.to,
        "uf": body.uf,
        "situacao_processo": body.situacao_processo,
        "credor_code": body.credor_code,
        "date_field": body.date_field,
    })
    rows = db.execute(text(sql), params).mappings().all()
    return {"rows": rows}


@router.post('/bootstrap')
def bootstrap_indicators(db: DbSession, ctx=Depends(get_current_ctx)):
    # Cria/atualiza um conjunto padrão de indicadores (idempotente)
    templates = [
        ("valor_mes_a_mes", "Valor M�s a M�s", "line", "Cobran�a", """
            SELECT strftime('%Y-%m', dt_cadastro) AS ym, SUM(vl_titulo) AS total
            FROM curated_records
            WHERE organization_id={{tenant_id}}
            GROUP BY ym
            ORDER BY ym
        """),
        ("mapa_por_uf", "Mapa por UF", "map_br", "Cobran�a", """
            SELECT uf, SUM(vl_titulo) AS total
            FROM curated_records
            WHERE organization_id={{tenant_id}}
            GROUP BY uf
            ORDER BY total DESC
        """),
        ("total_por_faixa_vencimento", "Total por Faixa de Vencimento", "bar", "Cobran�a", """
            SELECT faixa_vencimento, SUM(vl_titulo) AS total
            FROM curated_records
            WHERE organization_id={{tenant_id}}
            GROUP BY faixa_vencimento
            ORDER BY total DESC
        """),
        ("recuperado_por_faixa_vencimento", "bar", "Recuperado por Faixa de Vencimento", """
            SELECT faixa_vencimento, SUM(vl_total_repasse) AS total
            FROM curated_records
            WHERE organization_id={{tenant_id}}
            GROUP BY faixa_vencimento
            ORDER BY total DESC
        """),
    ]
    created = []
    for tpl in templates:
        # handle both 3-tuple and 4-tuple for backward compatibility
        if len(tpl) == 4:
            key, name_or_fmt, fmt_or_name, sql = tpl
            # detect ordering used above for last item where fmt and name swapped accidentally
            if fmt_or_name in ("line", "bar", "pie", "kpi", "table"):
                name = name_or_fmt
                fmt = fmt_or_name
            else:
                name = fmt_or_name
                fmt = name_or_fmt
        else:
            key, name, sql = tpl
            fmt = None
        # reutiliza create_indicator behavior (upsert)
        create_indicator(IndicatorCreate(key=key, name=name, formula_sql=sql, fmt=fmt), db, ctx)
        created.append({"key": key, "name": name, "fmt": fmt})
    return {"ok": True, "created": created}


class IndicatorUpdate(BaseModel):
    fmt: str | None = None


@router.patch('/{indicator_id}')
def patch_indicator(indicator_id: int, payload: IndicatorUpdate, db: DbSession, ctx=Depends(get_current_ctx)):
    row = db.execute(text("SELECT id FROM indicators WHERE id=:i AND organization_id=:o"), {"i": indicator_id, "o": ctx.organization_id}).first()
    if not row:
        raise HTTPException(status_code=404, detail="Indicador não encontrado")
    if payload.fmt is not None:
        db.execute(text("UPDATE indicators SET fmt=:f WHERE id=:i"), {"f": payload.fmt, "i": indicator_id})
        db.commit()
    result = db.execute(text("SELECT id, key, name, dataset, fmt FROM indicators WHERE id=:i"), {"i": indicator_id}).mappings().first()
    return result


@router.delete('/{indicator_id}')
def delete_indicator(indicator_id: int, db: DbSession, ctx=Depends(get_current_ctx)):
    row = db.execute(text("SELECT id FROM indicators WHERE id=:i AND organization_id=:o"), {"i": indicator_id, "o": ctx.organization_id}).first()
    if not row:
        raise HTTPException(status_code=404, detail="Indicador não encontrado")
    db.execute(text("DELETE FROM indicators WHERE id=:i"), {"i": indicator_id})
    db.commit()
    return {"ok": True}
