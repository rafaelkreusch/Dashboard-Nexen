from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from pydantic import BaseModel, Field, ConfigDict

from app.deps import get_current_ctx, DbSession
from app.utils.filters import is_safe_select, apply_placeholders


router = APIRouter(prefix="/indicators", tags=["indicators"])  # clean ASCII-only version


@router.get('/valor-mes-a-mes')
def valor_mes_a_mes(db: DbSession, ctx=Depends(get_current_ctx), from_: Optional[date] = None, to: Optional[date] = None,
                    uf: Optional[str] = None, situacao_processo: Optional[str] = None):
    sql = """
    SELECT strftime('%Y-%m', dt_cadastro) AS ym, COALESCE(SUM(vl_titulo), 0) AS total
    FROM curated_records
    WHERE organization_id = :tenant_id
      AND (:from IS NULL OR date(dt_cadastro) >= :from)
      AND (:to IS NULL OR date(dt_cadastro) <= :to)
      AND (:uf IS NULL OR uf = :uf)
      AND (:situacao_processo IS NULL OR situacao_processo = :situacao_processo)
    GROUP BY ym
    ORDER BY ym
    """
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
    rows = db.execute(text(sql), {'tenant_id': ctx.organization_id, 'from': from_, 'to': to}).mappings().all()
    return {"series": rows}


@router.get('/total-por-faixa-vencimento')
def total_por_faixa_vencimento(db: DbSession, ctx=Depends(get_current_ctx)):
    # Calcula a faixa dinamicamente com base nos dias em atraso do dt_vencimento
    # para refletir a planilha (0-30, 31-60, 61-90, 91-180, 181-360, 361-720, >720, vazio)
    sql = """
    WITH base AS (
      SELECT
        CASE
          WHEN dt_vencimento IS NULL THEN 'vazio'
          WHEN (julianday('now') - julianday(dt_vencimento)) <= 30 THEN '0 a 30 dias'
          WHEN (julianday('now') - julianday(dt_vencimento)) <= 60 THEN '31 a 60 dias'
          WHEN (julianday('now') - julianday(dt_vencimento)) <= 90 THEN '61 a 90 dias'
          WHEN (julianday('now') - julianday(dt_vencimento)) <= 180 THEN '91 a 180 dias'
          WHEN (julianday('now') - julianday(dt_vencimento)) <= 360 THEN '181 a 360 dias'
          WHEN (julianday('now') - julianday(dt_vencimento)) <= 720 THEN '361 a 720 dias'
          ELSE 'Mais de 720 dias'
        END AS faixa_vencimento,
        vl_titulo
      FROM curated_records
      WHERE organization_id = :tenant_id
    )
    SELECT faixa_vencimento, COALESCE(SUM(vl_titulo), 0) AS total
    FROM base
    GROUP BY faixa_vencimento
    ORDER BY
      CASE faixa_vencimento
        WHEN '0 a 30 dias' THEN 1
        WHEN '31 a 60 dias' THEN 2
        WHEN '61 a 90 dias' THEN 3
        WHEN '91 a 180 dias' THEN 4
        WHEN '181 a 360 dias' THEN 5
        WHEN '361 a 720 dias' THEN 6
        WHEN 'Mais de 720 dias' THEN 7
        WHEN 'vazio' THEN 8
        ELSE 9
      END
    """
    rows = db.execute(text(sql), {'tenant_id': ctx.organization_id}).mappings().all()
    return {"series": rows}


@router.get('/recuperado-por-faixa-vencimento')
def recuperado_por_faixa_vencimento(db: DbSession, ctx=Depends(get_current_ctx)):
    sql = """
    WITH base AS (
      SELECT
        CASE
          WHEN dt_vencimento IS NULL THEN 'vazio'
          WHEN (julianday('now') - julianday(dt_vencimento)) <= 30 THEN '0 a 30 dias'
          WHEN (julianday('now') - julianday(dt_vencimento)) <= 60 THEN '31 a 60 dias'
          WHEN (julianday('now') - julianday(dt_vencimento)) <= 90 THEN '61 a 90 dias'
          WHEN (julianday('now') - julianday(dt_vencimento)) <= 180 THEN '91 a 180 dias'
          WHEN (julianday('now') - julianday(dt_vencimento)) <= 360 THEN '181 a 360 dias'
          WHEN (julianday('now') - julianday(dt_vencimento)) <= 720 THEN '361 a 720 dias'
          ELSE 'Mais de 720 dias'
        END AS faixa_vencimento,
        vl_total_repasse
      FROM curated_records
      WHERE organization_id = :tenant_id
    )
    SELECT faixa_vencimento, COALESCE(SUM(vl_total_repasse), 0) AS total
    FROM base
    GROUP BY faixa_vencimento
    ORDER BY
      CASE faixa_vencimento
        WHEN '0 a 30 dias' THEN 1
        WHEN '31 a 60 dias' THEN 2
        WHEN '61 a 90 dias' THEN 3
        WHEN '91 a 180 dias' THEN 4
        WHEN '181 a 360 dias' THEN 5
        WHEN '361 a 720 dias' THEN 6
        WHEN 'Mais de 720 dias' THEN 7
        WHEN 'vazio' THEN 8
        ELSE 9
      END
    """
    rows = db.execute(text(sql), {'tenant_id': ctx.organization_id}).mappings().all()
    return {"series": rows}


class IndicatorCreate(BaseModel):
    key: str
    name: str
    formula_sql: str
    dataset: Optional[str] = None
    fmt: Optional[str] = None
    category: Optional[str] = None
    credor_code: Optional[str] = None


@router.get("")
def list_indicators(db: DbSession, ctx=Depends(get_current_ctx), credor_code: Optional[str] = None):
    sql = (
        "SELECT id, key, name, dataset, fmt, category, credor_code "
        "FROM indicators WHERE organization_id=:o"
    )
    params = {"o": ctx.organization_id}
    if credor_code:
        sql += " AND credor_code = :c"
        params["c"] = credor_code
    sql += " ORDER BY COALESCE(NULLIF(category,''),'~'), name"
    rows = db.execute(text(sql), params).mappings().all()
    return rows


@router.post("")
def create_indicator(payload: IndicatorCreate, db: DbSession, ctx=Depends(get_current_ctx)):
    sql = (payload.formula_sql or '').lstrip('\ufeff').strip()
    if not is_safe_select(sql):
        raise HTTPException(status_code=400, detail="Only SELECT statements are allowed")
    # upsert by (org, key)
    existing = db.execute(text("SELECT id FROM indicators WHERE organization_id=:o AND key=:k"), {"o": ctx.organization_id, "k": payload.key}).first()
    if existing:
        db.execute(text(
            """
            UPDATE indicators SET name=:n, dataset=:d, formula_sql=:f, fmt=:fmt, category=:c, credor_code=:cc
            WHERE id=:id
            """
        ), {"n": payload.name, "d": payload.dataset, "f": sql, "fmt": payload.fmt, "c": payload.category, "cc": payload.credor_code, "id": existing[0]})
        db.commit()
        row = db.execute(text("SELECT id, key, name, dataset, fmt, category, credor_code FROM indicators WHERE id=:id"), {"id": existing[0]}).mappings().first()
        return row
    else:
        db.execute(text(
            """
            INSERT INTO indicators (organization_id, key, name, dataset, formula_sql, fmt, category, credor_code, created_at)
            VALUES (:o, :k, :n, :d, :f, :fmt, :c, :cc, CURRENT_TIMESTAMP)
            """
        ), {"o": ctx.organization_id, "k": payload.key, "n": payload.name, "d": payload.dataset, "f": sql, "fmt": payload.fmt, "c": payload.category, "cc": payload.credor_code})
        db.commit()
        row = db.execute(text("SELECT id, key, name, dataset, fmt, category, credor_code FROM indicators WHERE organization_id=:o AND key=:k"), {"o": ctx.organization_id, "k": payload.key}).mappings().first()
        return row


class IndicatorRunIn(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra='ignore')

    from_: Optional[date] = Field(default=None, alias="from")
    to: Optional[date] = None
    uf: Optional[str] = None
    situacao_processo: Optional[str] = None
    credor_code: Optional[str] = None
    date_field: Optional[str] = None


@router.post('/preview')
def preview_indicator(body: IndicatorCreate, db: DbSession, ctx=Depends(get_current_ctx)):
    sql = (body.formula_sql or '').lstrip('\ufeff').strip().rstrip(';')
    if not is_safe_select(sql):
        raise HTTPException(status_code=400, detail="Only SELECT is allowed")
    sql = sql.replace("{{tenant_id}}", ":tenant_id")
    wrapped = f"SELECT * FROM ({sql}) t LIMIT 200"
    rows = db.execute(text(wrapped), {"tenant_id": ctx.organization_id}).mappings().all()
    return {"rows": [dict(row) for row in rows]}


@router.post('/{indicator_id:int}/run')
def run_indicator(indicator_id: int, body: IndicatorRunIn, db: DbSession, ctx=Depends(get_current_ctx)):
    row = db.execute(text("SELECT formula_sql, credor_code FROM indicators WHERE id=:i AND organization_id=:o"), {"i": indicator_id, "o": ctx.organization_id}).first()
    if not row:
        raise HTTPException(status_code=404, detail="Indicator not found")
    sql = (row[0] or '').lstrip('\ufeff').strip().rstrip(';')
    ind_credor = row[1] if row and len(row) > 1 else None
    if not is_safe_select(sql):
        raise HTTPException(status_code=400, detail="Invalid SQL")
    # apply placeholders (tenant_id + optional filters)
    sql, params = apply_placeholders(sql, {
        'tenant_id': ctx.organization_id,
        'from': body.from_,
        'to': body.to,
        'uf': body.uf,
        'situacao_processo': body.situacao_processo,
        'credor_code': body.credor_code or ind_credor,
        'date_field': body.date_field,
    })
    try:
        rows = db.execute(text(sql), params).mappings().all()
        return {"rows": [dict(row) for row in rows]}
    except Exception as e:
        # Report SQL error clearly to the client
        raise HTTPException(status_code=400, detail=f"SQL error: {e}")


class IndicatorUpdate(BaseModel):
    fmt: Optional[str] = None
    category: Optional[str] = None
    credor_code: Optional[str] = None


@router.patch('/{indicator_id:int}')
def patch_indicator(indicator_id: int, payload: IndicatorUpdate, db: DbSession, ctx=Depends(get_current_ctx)):
    row = db.execute(text("SELECT id FROM indicators WHERE id=:i AND organization_id=:o"), {"i": indicator_id, "o": ctx.organization_id}).first()
    if not row:
        raise HTTPException(status_code=404, detail="Indicator not found")
    if payload.fmt is not None:
        db.execute(text("UPDATE indicators SET fmt=:f WHERE id=:i"), {"f": payload.fmt, "i": indicator_id})
    if payload.category is not None:
        db.execute(text("UPDATE indicators SET category=:c WHERE id=:i"), {"c": payload.category, "i": indicator_id})
    if payload.credor_code is not None:
        db.execute(text("UPDATE indicators SET credor_code=:cc WHERE id=:i"), {"cc": payload.credor_code, "i": indicator_id})
    db.commit()
    result = db.execute(text("SELECT id, key, name, dataset, fmt, category, credor_code FROM indicators WHERE id=:i"), {"i": indicator_id}).mappings().first()
    return result

@router.get('/{indicator_id:int}')
def get_indicator(indicator_id: int, db: DbSession, ctx=Depends(get_current_ctx)):
    row = db.execute(text("SELECT id, key, name, dataset, fmt, category, formula_sql FROM indicators WHERE id=:i AND organization_id=:o"), {"i": indicator_id, "o": ctx.organization_id}).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Indicator not found")
    return row

@router.delete('/{indicator_id:int}')
def delete_indicator(indicator_id: int, db: DbSession, ctx=Depends(get_current_ctx)):
    row = db.execute(text("SELECT id FROM indicators WHERE id=:i AND organization_id=:o"), {"i": indicator_id, "o": ctx.organization_id}).first()
    if not row:
        raise HTTPException(status_code=404, detail="Indicator not found")
    db.execute(text("DELETE FROM indicators WHERE id=:i"), {"i": indicator_id})
    db.commit()
    return {"ok": True}


@router.post('/bootstrap')
def bootstrap_indicators(db: DbSession, ctx=Depends(get_current_ctx)):
    templates = [
        ("valor_mes_a_mes", "Valor Mes a Mes", "line", "Cobranca", """
            SELECT strftime('%Y-%m', dt_cadastro) AS ym, SUM(vl_titulo) AS total
            FROM curated_records
            WHERE organization_id={{tenant_id}}
            GROUP BY ym
            ORDER BY ym
        """),
        ("mapa_por_uf", "Mapa por UF", "map_br", "Cobranca", """
            SELECT uf, SUM(vl_titulo) AS total
            FROM curated_records
            WHERE organization_id={{tenant_id}}
            GROUP BY uf
            ORDER BY total DESC
        """),
        ("total_por_faixa_vencimento", "Total por Faixa de Vencimento", "bar", "Cobranca", """
            SELECT faixa_vencimento, SUM(vl_titulo) AS total
            FROM curated_records
            WHERE organization_id={{tenant_id}}
            GROUP BY faixa_vencimento
            ORDER BY total DESC
        """),
        ("recuperado_por_faixa_vencimento", "Recuperado por Faixa de Vencimento", "bar", "Cobranca", """
            SELECT faixa_vencimento, SUM(vl_total_repasse) AS total
            FROM curated_records
            WHERE organization_id={{tenant_id}}
            GROUP BY faixa_vencimento
            ORDER BY total DESC
        """),
    ]
    created = []
    for key, name, fmt, category, sql in templates:
        create_indicator(IndicatorCreate(key=key, name=name, formula_sql=sql, fmt=fmt, category=category), db, ctx)
        created.append({"key": key})
    return {"ok": True, "created": created}
