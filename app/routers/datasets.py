from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select, text
from datetime import datetime

from app.deps import get_current_ctx, DbSession
from app.utils.filters import is_safe_select, apply_placeholders
from app.models import DataSource  # placeholder import to keep consistency if later we link
from pydantic import BaseModel
from typing import Optional


router = APIRouter(prefix="/datasets", tags=["datasets"]) 


class DatasetCreateIn(BaseModel):
    name: str
    description: str | None = None
    query_sql: str
    credor_code: Optional[str] = None


class DatasetOut(BaseModel):
    id: int
    name: str
    description: str | None = None
    query_sql: str
    credor_code: Optional[str] = None

    class Config:
        from_attributes = True


# We'll store datasets definitions in a simple table via raw SQL in migration.


@router.get("")
def list_datasets(db: DbSession, ctx=Depends(get_current_ctx), credor_code: Optional[str] = None):
    sql = "SELECT id, name, description, query_sql, credor_code FROM datasets WHERE organization_id = :org"
    params = {"org": ctx.organization_id}
    if credor_code:
        sql += " AND credor_code = :credor_code"
        params["credor_code"] = credor_code
    sql += " ORDER BY id DESC"
    rows = db.execute(text(sql), params).mappings().all()
    return rows


@router.post("")
def create_dataset(payload: DatasetCreateIn, db: DbSession, ctx=Depends(get_current_ctx)):
    if not is_safe_select(payload.query_sql):
        raise HTTPException(status_code=400, detail="Apenas SELECT é permitido")
    # Require tenant placeholder or an explicit filter
    if "{{tenant_id}}" not in payload.query_sql and "organization_id" not in payload.query_sql.lower():
        raise HTTPException(status_code=400, detail="Inclua {{tenant_id}} ou filtre por organization_id")
    db.execute(text("""
        INSERT INTO datasets (organization_id, name, description, query_sql, credor_code, created_at)
        VALUES (:org, :n, :d, :q, :c, :now)
    """), {"org": ctx.organization_id, "n": payload.name, "d": payload.description, "q": payload.query_sql, "c": payload.credor_code, "now": datetime.utcnow()})
    db.commit()
    row = db.execute(text("SELECT id, name, description, query_sql, credor_code FROM datasets WHERE organization_id=:org ORDER BY id DESC LIMIT 1"), {"org": ctx.organization_id}).mappings().first()
    return row


class PreviewIn(BaseModel):
    query_sql: str
    from_: str | None = None
    to: str | None = None
    uf: str | None = None
    situacao_processo: str | None = None
    credor_code: str | None = None


@router.post('/preview')
def preview_dataset(payload: PreviewIn, db: DbSession, ctx=Depends(get_current_ctx)):
    if not is_safe_select(payload.query_sql):
        raise HTTPException(status_code=400, detail="Apenas SELECT é permitido")
    sql, params = apply_placeholders(payload.query_sql, {
        "tenant_id": ctx.organization_id,
        "from": payload.from_,
        "to": payload.to,
        "uf": payload.uf,
        "situacao_processo": payload.situacao_processo,
        "credor_code": payload.credor_code,
    })
    # Wrap with LIMIT for preview
    wrapped = f"SELECT * FROM ({sql}) t LIMIT 50"
    rows = db.execute(text(wrapped), params).mappings().all()
    return {"rows": rows, "count": len(rows)}


@router.delete('/{dataset_id}')
def delete_dataset(dataset_id: int, db: DbSession, ctx=Depends(get_current_ctx)):
    row = db.execute(text("SELECT id FROM datasets WHERE id=:i AND organization_id=:o"), {"i": dataset_id, "o": ctx.organization_id}).first()
    if not row:
        raise HTTPException(status_code=404, detail="Dataset not found")
    db.execute(text("DELETE FROM datasets WHERE id=:i"), {"i": dataset_id})
    db.commit()
    return {"ok": True}
