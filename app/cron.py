from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy.orm import Session
from sqlalchemy import select, text

from app.settings import settings
from app.database import SessionLocal
from app.models import DataSource, JobRun
from app.utils.db_connect import make_engine
from app.utils.sheets_loader import load_sheet
from app.utils.transforms import store_staging, materialize_curated


scheduler: AsyncIOScheduler | None = None


def _run_recurring_ingest():
    db: Session = SessionLocal()
    try:
        dss = db.scalars(select(DataSource).where(DataSource.is_recurring == True)).all()  # noqa: E712
        for ds in dss:
            jr = JobRun(organization_id=ds.organization_id, target_type='datasource', target_id=ds.id, status='running', started_at=datetime.utcnow())
            db.add(jr)
            db.commit()
            db.refresh(jr)
            try:
                if ds.type == 'sql' and ds.sqlalchemy_url:
                    query = None
                    if ds.config_json and isinstance(ds.config_json, dict):
                        query = ds.config_json.get('query')
                    if not query:
                        jr.status = 'error'
                        jr.logs = 'config_json.query ausente'
                    else:
                        eng = make_engine(ds.sqlalchemy_url)
                        with eng.connect() as conn:
                            result = conn.execute(text(query))
                            rows = [dict(r._mapping) for r in result]
                        store_staging(rows, ds.organization_id, db)
                        credor = None
                        if ds.config_json and isinstance(ds.config_json, dict):
                            credor = ds.config_json.get('credor_code')
                        materialize_curated(rows, ds.organization_id, db, credor)
                        jr.status = 'success'
                        jr.logs = f"Ingeridos {len(rows)} registros"
                elif ds.type == 'google_sheets' and ds.config_json:
                    spreadsheet_id = ds.config_json.get('spreadsheet_id')
                    range_name = ds.config_json.get('range')
                    rows = load_sheet(spreadsheet_id, range_name)
                    store_staging(rows, ds.organization_id, db)
                    credor = None
                    if ds.config_json and isinstance(ds.config_json, dict):
                        credor = ds.config_json.get('credor_code')
                    materialize_curated(rows, ds.organization_id, db, credor)
                    jr.status = 'success'
                    jr.logs = f"Ingeridos {len(rows)} registros"
                else:
                    jr.status = 'success'
                    jr.logs = 'Sem ação (csv_upload não recorrente)'
            except Exception as e:
                jr.status = 'error'
                jr.logs = str(e)
            finally:
                jr.finished_at = datetime.utcnow()
                db.commit()
    finally:
        db.close()


def init_scheduler():
    global scheduler
    if scheduler:
        return scheduler
    scheduler = AsyncIOScheduler()
    minutes = settings.CRON_DEFAULT_MINUTES or 60
    scheduler.add_job(_run_recurring_ingest, 'interval', minutes=minutes, id='recurring_ingest', replace_existing=True)
    scheduler.start()
    return scheduler
