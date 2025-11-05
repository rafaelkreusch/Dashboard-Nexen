from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from fastapi.staticfiles import StaticFiles

from app.database import Base, engine
from app.routers import auth, datasources, ingest, dashboards
from app.routers import indicators_v2
from app.routers import indicators_ext
from app.routers import datasets as datasets_router
from app.routers import meta
from app.routers import org as org_router
from app.cron import init_scheduler

# garante que todas as classes de modelo registrem no Base.metadata
from app import models as _models  # IMPORTANTE

app = FastAPI(title="SaaS Dashboards")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
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
    """
    1) Cria todas as tabelas declaradas (cobre `datasets` etc.)
    2) Aplica DDLs opcionais por dialeto (Postgres/SQLite) + fallback p/ datasets/indicators
    3) Inicia o scheduler
    """
    # 1) cria o schema base
    Base.metadata.create_all(bind=engine)

    # 2) DDLs opcionais idempotentes
    try:
        dialect = engine.dialect.name  # "postgresql" | "sqlite" | ...
        with engine.begin() as conn:
            # ----- Fallback: garantir datasets/indicators -----
            if dialect == "postgresql":
                create_datasets = """
                CREATE TABLE IF NOT EXISTS datasets (
                    id BIGSERIAL PRIMARY KEY,
                    organization_id INTEGER NOT NULL,
                    name VARCHAR(200) NOT NULL,
                    description TEXT,
                    query_sql TEXT,
                    credor_code VARCHAR(50),
                    created_at TIMESTAMP DEFAULT NOW()
                );
                """
                create_indicators = """
                CREATE TABLE IF NOT EXISTS indicators (
                    id BIGSERIAL PRIMARY KEY,
                    organization_id INTEGER NOT NULL,
                    key VARCHAR(120) UNIQUE,
                    name VARCHAR(200) NOT NULL,
                    dataset VARCHAR(120),
                    fmt VARCHAR(50),
                    credor_code VARCHAR(50),
                    created_at TIMESTAMP DEFAULT NOW()
                );
                """
                create_idx = [
                    "CREATE INDEX IF NOT EXISTS idx_datasets_org ON datasets (organization_id)",
                    "CREATE INDEX IF NOT EXISTS idx_indicators_org ON indicators (organization_id)"
                ]
            else:  # sqlite
                create_datasets = """
                CREATE TABLE IF NOT EXISTS datasets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    organization_id INTEGER NOT NULL,
                    name VARCHAR(200) NOT NULL,
                    description TEXT,
                    query_sql TEXT,
                    credor_code VARCHAR(50),
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
                """
                create_indicators = """
                CREATE TABLE IF NOT EXISTS indicators (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    organization_id INTEGER NOT NULL,
                    key VARCHAR(120) UNIQUE,
                    name VARCHAR(200) NOT NULL,
                    dataset VARCHAR(120),
                    fmt VARCHAR(50),
                    credor_code VARCHAR(50),
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
                """
                create_idx = [
                    "CREATE INDEX IF NOT EXISTS idx_datasets_org ON datasets (organization_id)",
                    "CREATE INDEX IF NOT EXISTS idx_indicators_org ON indicators (organization_id)"
                ]

            try: conn.execute(text(create_datasets))
            except Exception: pass
            try: conn.execute(text(create_indicators))
            except Exception: pass
            for sql in create_idx:
                try: conn.execute(text(sql))
                except Exception: pass

            # ----- Colunas extras em curated_records -----
            for ddl in [
                "ALTER TABLE curated_records ADD COLUMN devedor VARCHAR(200)",
                "ALTER TABLE curated_records ADD COLUMN cpf_cnpj VARCHAR(32)",
                "ALTER TABLE curated_records ADD COLUMN processo VARCHAR(100)",
                "ALTER TABLE curated_records ADD COLUMN credor_code VARCHAR(50)",
                ("postgresql", "ALTER TABLE curated_records ADD COLUMN vl_saldo DOUBLE PRECISION"),
                ("sqlite",      "ALTER TABLE curated_records ADD COLUMN vl_saldo FLOAT"),
                ("postgresql", "ALTER TABLE curated_records ADD COLUMN dt_ultimo_credito TIMESTAMP"),
                ("sqlite",      "ALTER TABLE curated_records ADD COLUMN dt_ultimo_credito DATETIME"),
                "ALTER TABLE curated_records ADD COLUMN portador VARCHAR(100)",
                "ALTER TABLE curated_records ADD COLUMN motivo_devolucao VARCHAR(200)",
                ("postgresql", "ALTER TABLE curated_records ADD COLUMN vl_honorario_devedor DOUBLE PRECISION"),
                ("sqlite",      "ALTER TABLE curated_records ADD COLUMN vl_honorario_devedor FLOAT"),
                ("postgresql", "ALTER TABLE curated_records ADD COLUMN vl_tx_contrato DOUBLE PRECISION"),
                ("sqlite",      "ALTER TABLE curated_records ADD COLUMN vl_tx_contrato FLOAT"),
                "ALTER TABLE curated_records ADD COLUMN comercial VARCHAR(100)",
                "ALTER TABLE curated_records ADD COLUMN cobrador VARCHAR(100)",
                ("postgresql", "ALTER TABLE curated_records ADD COLUMN dt_encerrado TIMESTAMP"),
                ("sqlite",      "ALTER TABLE curated_records ADD COLUMN dt_encerrado DATETIME"),
                ("postgresql", "ALTER TABLE curated_records ADD COLUMN dias_vencidos_cadastro INTEGER"),
                ("sqlite",      "ALTER TABLE curated_records ADD COLUMN dias_vencidos_cadastro INTEGER"),
            ]:
                if isinstance(ddl, tuple):
                    wanted, sql = ddl
                    if dialect == wanted:
                        try: conn.execute(text(sql))
                        except Exception: pass
                else:
                    try: conn.execute(text(ddl))
                    except Exception: pass

            # ----- Coluna credor_code em datasets/indicators (idempotente) -----
            try: conn.execute(text("ALTER TABLE datasets ADD COLUMN credor_code VARCHAR(50)"))
            except Exception: pass
            try: conn.execute(text("ALTER TABLE indicators ADD COLUMN credor_code VARCHAR(50)"))
            except Exception: pass

            # ----- Tabela de categorias -----
            if dialect == "postgresql":
                create_cat = """
                CREATE TABLE IF NOT EXISTS indicator_categories (
                    id BIGSERIAL PRIMARY KEY,
                    organization_id INTEGER NOT NULL,
                    name VARCHAR(200) NOT NULL,
                    color VARCHAR(7),
                    UNIQUE(organization_id, name)
                );
                """
            else:
                create_cat = """
                CREATE TABLE IF NOT EXISTS indicator_categories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    organization_id INTEGER NOT NULL,
                    name VARCHAR(200) NOT NULL,
                    color VARCHAR(7),
                    UNIQUE(organization_id, name)
                );
                """
            try: conn.execute(text(create_cat))
            except Exception: pass
            try: conn.execute(text("ALTER TABLE indicator_categories ADD COLUMN color VARCHAR(7)"))
            except Exception: pass

    except Exception:
        # nunca derruba o app no startup por DDL opcional
        pass

    # 3) scheduler
    init_scheduler()

@app.get("/")
def root():
    return RedirectResponse(url="/app/login.html")

@app.get("/app/")
def app_home_redirect():
    return RedirectResponse(url="/app/index2.html")

# Compatibilidade
@app.get("/app/indicators.html")
def redirect_indicators():
    return RedirectResponse(url="/app/indicators-fixed.html")

@app.get("/app/static/login.html")
def redirect_login_static():
    return RedirectResponse(url="/app/login.html")

@app.get("/healthz")
def healthz():
    return {"status": "ok"}

# Static UI
app.mount("/app", StaticFiles(directory="app/static", html=True), name="app_static")
