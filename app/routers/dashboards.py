from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select
from app.deps import get_current_ctx, DbSession
from app.schemas import DashboardCreateIn, DashboardOut
from app.models import Dashboard
import secrets


router = APIRouter(prefix="/dashboards", tags=["dashboards"])


@router.post('', response_model=DashboardOut)
def create_dashboard(payload: DashboardCreateIn, db: DbSession, ctx=Depends(get_current_ctx)):
    d = Dashboard(
        organization_id=ctx.organization_id,
        name=payload.name,
        description=payload.description,
        layout_json=payload.layout_json or {},
        is_public=payload.is_public,
        public_token=secrets.token_urlsafe(16) if payload.is_public else None,
        theme_json=payload.theme_json or {},
    )
    db.add(d)
    db.commit()
    db.refresh(d)
    return d


@router.get('', response_model=list[DashboardOut])
def list_dashboards(db: DbSession, ctx=Depends(get_current_ctx)):
    return db.scalars(select(Dashboard).where(Dashboard.organization_id == ctx.organization_id)).all()


@router.get('/{dashboard_id}', response_model=DashboardOut)
def get_dashboard(dashboard_id: int, db: DbSession, ctx=Depends(get_current_ctx)):
    d = db.get(Dashboard, dashboard_id)
    if not d or d.organization_id != ctx.organization_id:
        raise HTTPException(status_code=404, detail="Dashboard não encontrado")
    return d
