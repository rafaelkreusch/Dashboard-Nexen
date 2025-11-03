from datetime import datetime
from typing import List
from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from sqlalchemy import text, select
from sqlalchemy.orm import Session

from app.deps import get_current_ctx, DbSession
from app.schemas import IngestSQLIn, SheetsIn
from app.models import DataSource, JobRun
from app.utils.db_connect import make_engine
from app.utils.csv_loader import load_csv_bytes, load_xlsx_bytes
from app.utils.sheets_loader import load_sheet
from app.utils.transforms import store_staging, materialize_curated


router = APIRouter(prefix="/ingest", tags=["ingest"])


@router.post('/sql')
def ingest_sql(payload: IngestSQLIn, db: DbSession, ctx=Depends(get_current_ctx)):
    ds = db.get(DataSource, payload.data_source_id)
    if not ds or ds.organization_id != ctx.organization_id:
        raise HTTPException(status_code=404, detail="Fonte não encontrada")
    if ds.type != 'sql' or not ds.sqlalchemy_url:
        raise HTTPException(status_code=400, detail="Fonte não é do tipo SQL")

    jr = JobRun(organization_id=ctx.organization_id, target_type='datasource', target_id=ds.id, status='running', started_at=datetime.utcnow())
    db.add(jr)
    db.commit()
    db.refresh(jr)
    try:
        eng = make_engine(ds.sqlalchemy_url)
        with eng.connect() as conn:
            if not payload.query.strip().lower().startswith('select'):
                raise HTTPException(status_code=400, detail="Apenas SELECT é permitido")
            result = conn.execute(text(payload.query))
            rows = [dict(r._mapping) for r in result]
        store_staging(rows, ctx.organization_id, db)
        materialize_curated(rows, ctx.organization_id, db)
        jr.status = 'success'
        jr.logs = f"Ingeridos {len(rows)} registros"
    except Exception as e:
        jr.status = 'error'
        jr.logs = str(e)
        db.commit()
        raise
    finally:
        jr.finished_at = datetime.utcnow()
        db.commit()
    return {"ok": True, "ingested": jr.logs}


@router.post('/csv')
async def ingest_csv(db: DbSession, ctx=Depends(get_current_ctx), file: UploadFile = File(...), credor_code: str | None = None):
    try:
        data = await file.read()
        rows = load_csv_bytes(data)
        store_staging(rows, ctx.organization_id, db)
        count = materialize_curated(rows, ctx.organization_id, db, credor_code)
        return {"ok": True, "rows": count}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post('/xlsx')
async def ingest_xlsx(db: DbSession, ctx=Depends(get_current_ctx), file: UploadFile = File(...), credor_code: str | None = None):
    try:
        # Only .xlsx is supported (openpyxl)
        filename = (file.filename or '').lower()
        if not filename.endswith('.xlsx'):
            raise HTTPException(status_code=400, detail='Apenas arquivos .xlsx são suportados. Salve seu Excel como .xlsx e tente novamente.')
        data = await file.read()
        rows = load_xlsx_bytes(data)
        store_staging(rows, ctx.organization_id, db)
        count = materialize_curated(rows, ctx.organization_id, db, credor_code)
        return {"ok": True, "rows": count}
    except Exception as e:
        # Melhora a mensagem para erros de engine/planilha
        msg = str(e)
        if 'openpyxl' in msg.lower():
            msg = 'Erro ao ler Excel. Verifique se o arquivo é .xlsx válido.'
        raise HTTPException(status_code=400, detail=msg)


@router.post('/sheets')
def ingest_sheets(payload: SheetsIn, db: DbSession, ctx=Depends(get_current_ctx)):
    rows = load_sheet(payload.spreadsheet_id, payload.range)
    store_staging(rows, ctx.organization_id, db)
    count = materialize_curated(rows, ctx.organization_id, db)
    return {"ok": True, "rows": count}
