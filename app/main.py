from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from app.database import Base, engine
from sqlalchemy import text
from app.routers import auth, datasources, ingest, dashboards
from app.routers import indicators_v2
from app.routers import indicators_ext
from app.routers import datasets as datasets_router
from app.routers import meta
from app.routers import org as org_router
from fastapi.staticfiles import StaticFiles
from app.cron import init_scheduler


app = FastAPI(title="SaaS Dashboards")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(auth.router)
app.include_router(datasources.router)
app.include_router(ingest.router)
app.include_router(indicators_v2.router)
app.include_router(indicators_ext.router)
app.include_router(dashboards.router)
app.include_router(meta.router)
app.include_router(datasets_router.router)
app.include_router(org_router.router)


@app.on_event("startup")
def on_startup():
    # DB tables are managed by Alembic, but create if not exists for local dev.
    Base.metadata.create_all(bind=engine)
    # Ensure optional columns exist for local SQLite (adds without migration)
    try:
        with engine.begin() as conn:
            try:
                conn.execute(text("ALTER TABLE curated_records ADD COLUMN devedor VARCHAR(200)"))
            except Exception:
                pass
            try:
                conn.execute(text("ALTER TABLE curated_records ADD COLUMN cpf_cnpj VARCHAR(32)"))
            except Exception:
                pass
            try:
                conn.execute(text("ALTER TABLE curated_records ADD COLUMN processo VARCHAR(100)"))
            except Exception:
                pass
            try:
                conn.execute(text("ALTER TABLE curated_records ADD COLUMN credor_code VARCHAR(50)"))
            except Exception:
                pass
            # Campos adicionais (idempotentes)
            for ddl in [
                "ALTER TABLE curated_records ADD COLUMN vl_saldo FLOAT",
                "ALTER TABLE curated_records ADD COLUMN dt_ultimo_credito DATETIME",
                "ALTER TABLE curated_records ADD COLUMN portador VARCHAR(100)",
                "ALTER TABLE curated_records ADD COLUMN motivo_devolucao VARCHAR(200)",
                "ALTER TABLE curated_records ADD COLUMN vl_honorario_devedor FLOAT",
                "ALTER TABLE curated_records ADD COLUMN vl_tx_contrato FLOAT",
                "ALTER TABLE curated_records ADD COLUMN comercial VARCHAR(100)",
                "ALTER TABLE curated_records ADD COLUMN cobrador VARCHAR(100)",
                "ALTER TABLE curated_records ADD COLUMN dt_encerrado DATETIME",
                "ALTER TABLE curated_records ADD COLUMN dias_vencidos_cadastro INTEGER",
            ]:
                try:
                    conn.execute(text(ddl))
                except Exception:
                    pass
            # Add credor_code to datasets and indicators if missing
            try:
                conn.execute(text("ALTER TABLE datasets ADD COLUMN credor_code VARCHAR(50)"))
            except Exception:
                pass
            try:
                conn.execute(text("ALTER TABLE indicators ADD COLUMN credor_code VARCHAR(50)"))
            except Exception:
                pass
            # Create categories table if not exists (for indicator folders)
            try:
                conn.execute(text("""
                CREATE TABLE IF NOT EXISTS indicator_categories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    organization_id INTEGER NOT NULL,
                    name VARCHAR(200) NOT NULL,
                    color VARCHAR(7),
                    UNIQUE(organization_id, name)
                )
                """))
            except Exception:
                pass
            try:
                conn.execute(text("ALTER TABLE indicator_categories ADD COLUMN color VARCHAR(7)"))
            except Exception:
                pass
    except Exception:
        pass
    init_scheduler()


@app.get("/")
def root():
    return RedirectResponse(url="/app/login.html")


@app.get("/app/")
def app_home_redirect():
    # Serve the fixed dashboard file to avoid cached/broken index.html links
    return RedirectResponse(url="/app/index2.html")


# Compatibility redirects for older static filenames
@app.get("/app/indicators.html")
def redirect_indicators():
    return RedirectResponse(url="/app/indicators-fixed.html")

# Compatibility: some links might point to /app/static/login.html
@app.get("/app/static/login.html")
def redirect_login_static():
    return RedirectResponse(url="/app/login.html")


@app.get("/healthz")
def healthz():
    return {"status": "ok"}

# Static UI mounted at /app
app.mount("/app", StaticFiles(directory="app/static", html=True), name="app_static")
