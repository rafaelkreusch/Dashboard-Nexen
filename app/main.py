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
    1) Cria todas as tabelas declaradas em app.models/Base (cobre `datasets` etc.)
    2) Aplica DDLs opcionais dependendo do dialeto (Postgres x SQLite)
    3) Inicia o scheduler
    """
    # 1) cria o schema base
    Base.metadata.create_all(bind=engine)

    # 2) DDLs opcionais idempotentes (ajustados por dialeto)
    try:
        dialect = engine.dialect.name  # "postgresql" | "sqlite" | ...

        with engine.begin() as conn:
            # ---- Colunas extras na curated_records (idempotentes) ----
            # comuns
            for ddl in [
                # textos e chaves
                "ALTER TABLE curated_records ADD COLUMN devedor VARCHAR(200)",
                "ALTER TABLE curated_records ADD COLUMN cpf_cnpj VARCHAR(32)",
                "ALTER TABLE curated_records ADD COLUMN processo VARCHAR(100)",
                "ALTER TABLE curated_records ADD COLUMN credor_code VARCHAR(50)",
                # valores
                ("postgresql", "ALTER TABLE curated_records ADD COLUMN vl_saldo DOUBLE PRECISION"),
                ("sqlite",      "ALTER TABLE curated_records ADD COLUMN vl_saldo FLOAT"),
                # datas
                ("postgresql", "ALTER TABLE curated_records ADD COLUMN dt_ultimo_credito TIMESTAMP"),
                ("sqlite",      "ALTER TABLE curated_records ADD COLUMN dt_ultimo_credito DATETIME"),
                # textos adicionais
                "ALTER TABLE curated_records ADD COLUMN portador VARCHAR(100)",
                "ALTER TABLE curated_records ADD COLUMN motivo_devolucao VARCHAR(200)",
                # honorários/tx/atributos
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
                # permite tupla (dialeto, ddl) ou string (ddl comum)
                if isinstance(ddl, tuple):
                    wanted, sql = ddl
                    if dialect == wanted:
                        try:
                            conn.execute(text(sql))
                        except Exception:
                            pass
                else:
                    try:
                        conn.execute(text(ddl))
                    except Exception:
                        pass

            # ---- Coluna credor_code nos módulos datasets/indicators ----
            try:
                conn.execute(text("ALTER TABLE datasets ADD COLUMN credor_code VARCHAR(50)"))
            except Exception:
                pass
            try:
                conn.execute(text("ALTER TABLE indicators ADD COLUMN credor_code VARCHAR(50)"))
            except Exception:
                pass

            # ---- Tabela indicator_categories (compatível com ambos) ----
            if dialect == "postgresql":
                create_cat = """
                CREATE TABLE IF NOT EXISTS indicator_categories (
                    id BIGSERIAL PRIMARY KEY,
                    organization_id INTEGER NOT NULL,
                    name VARCHAR(200) NOT NULL,
                    color VARCHAR(7),
                    UNIQUE(organization_id, name)
                )
                """
            else:  # sqlite e afins
                create_cat = """
                CREATE TABLE IF NOT EXISTS indicator_categories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    organization_id INTEGER NOT NULL,
                    name VARCHAR(200) NOT NULL,
                    color VARCHAR(7),
                    UNIQUE(organization_id, name)
                )
                """
            try:
                conn.execute(text(create_cat))
            except Exception:
                pass

            # garante coluna color se a tabela já existia sem esse campo
            try:
                conn.execute(text("ALTER TABLE indicator_categories ADD COLUMN color VARCHAR(7)"))
            except Exception:
                pass

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
