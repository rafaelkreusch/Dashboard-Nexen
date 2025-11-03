from datetime import date
from fastapi import APIRouter, Depends
from sqlalchemy import select, func, text
from sqlalchemy import inspect as sqla_inspect

from app.deps import get_current_ctx, DbSession
from app.models import CuratedRecord, Dashboard, Indicator, DataSource, JobRun


router = APIRouter(prefix="/meta", tags=["meta"]) 


@router.get("/summary")
def summary(db: DbSession, ctx=Depends(get_current_ctx)):
    org = ctx.organization_id
    datasets = db.scalar(select(func.count()).select_from(CuratedRecord).where(CuratedRecord.organization_id == org)) or 0
    indicators = db.scalar(select(func.count()).select_from(Indicator).where(Indicator.organization_id == org)) or 0
    dashboards = db.scalar(select(func.count()).select_from(Dashboard).where(Dashboard.organization_id == org)) or 0
    sources = db.scalar(select(func.count()).select_from(DataSource).where(DataSource.organization_id == org)) or 0

    today = date.today()
    queries_today = db.scalar(
        select(func.count()).select_from(JobRun).where(
            JobRun.organization_id == org,
            func.date(JobRun.started_at) == today,
        )
    ) or 0

    return {
        "datasets": datasets,
        "indicators": indicators,
        "dashboards": dashboards,
        "sources": sources,
        "queries_today": queries_today,
    }


@router.get("/curated-info")
def curated_info(db: DbSession, ctx=Depends(get_current_ctx)):
    # Columns from SQLAlchemy table metadata if available; fallback to DB inspect
    cols: list[dict] = []
    try:
        table = CuratedRecord.__table__
        cols = [{"name": c.name, "type": str(c.type)} for c in table.columns]
    except Exception:
        try:
            insp = sqla_inspect(db.bind)
            cols = [{"name": c.get("name"), "type": str(c.get("type"))} for c in insp.get_columns("curated_records")]
        except Exception:
            cols = []

    rows = db.execute(
        text("SELECT credor_code, uf, processo, devedor, cpf_cnpj, faixa_vencimento, dt_vencimento, vl_titulo, situacao_processo, vl_total_repasse, vl_saldo, dt_ultimo_credito, portador, motivo_devolucao, vl_honorario_devedor, vl_tx_contrato, comercial, cobrador, dt_encerrado, dias_vencidos_cadastro, dt_cadastro FROM curated_records WHERE organization_id=:o ORDER BY id DESC LIMIT 5"),
        {"o": ctx.organization_id},
    ).mappings().all()
    return {"columns": cols, "sample": rows}


@router.post('/clear-tenant')
def clear_tenant_data(db: DbSession, ctx=Depends(get_current_ctx)):
    # Remove dados ingeridos (curated_records) apenas do tenant atual
    try:
        res = db.execute(text("DELETE FROM curated_records WHERE organization_id=:o"), {"o": ctx.organization_id})
        db.commit()
        deleted = getattr(res, 'rowcount', 0) or 0
        return {"ok": True, "deleted": deleted}
    except Exception as e:
        db.rollback()
        return {"ok": False, "error": str(e)}
