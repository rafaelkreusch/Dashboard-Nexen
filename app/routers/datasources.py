from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select

from app.deps import get_current_ctx, DbSession
from app.schemas import DataSourceTestIn, DataSourceCreateIn, DataSourceOut
from app.utils.db_connect import test_connection
from app.models import DataSource


router = APIRouter(prefix="/datasources", tags=["datasources"])


@router.post('/test')
def test_datasource(payload: DataSourceTestIn):
    ok, err = test_connection(payload.sqlalchemy_url)
    if ok:
        return {"ok": True}
    raise HTTPException(status_code=400, detail=err)


@router.post('', response_model=DataSourceOut)
def create_datasource(payload: DataSourceCreateIn, db: DbSession, ctx=Depends(get_current_ctx)):
    ds = DataSource(
        organization_id=ctx.organization_id,
        type=payload.type,
        sqlalchemy_url=payload.sqlalchemy_url,
        config_json=payload.config_json,
        is_recurring=payload.is_recurring or False,
        interval_minutes=payload.interval_minutes,
    )
    db.add(ds)
    db.commit()
    db.refresh(ds)
    return ds


@router.get('', response_model=list[DataSourceOut])
def list_datasources(db: DbSession, ctx=Depends(get_current_ctx)):
    rows = db.scalars(select(DataSource).where(DataSource.organization_id == ctx.organization_id)).all()
    return rows


@router.delete('/{data_source_id}')
def delete_datasource(data_source_id: int, db: DbSession, ctx=Depends(get_current_ctx)):
    ds = db.get(DataSource, data_source_id)
    if not ds or ds.organization_id != ctx.organization_id:
        raise HTTPException(status_code=404, detail="Data source not found")
    db.delete(ds)
    db.commit()
    return {"ok": True}
