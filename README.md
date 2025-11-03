SaaS de Dashboards (FastAPI + Multi-tenant)

Resumo
- FastAPI com multi-tenant via `organization_id` no JWT.
- Suporte a fontes: SQL (SQLAlchemy), CSV/XLSX, Google Sheets.
- Ingestão para Staging -> Transform para Curated.
- Indicadores básicos prontos e CRUD de dashboards.
- Scheduler APScheduler a cada 1h (configurável via `.env`).
- Migrations Alembic e pronto para rodar local com uvicorn.

Pré-requisitos
- Python 3.11+
- Banco (ex.: Postgres) ou SQLite para desenvolvimento.

Instalação
1) Crie um venv e instale dependências:
   - `python -m venv .venv`
   - `./.venv/Scripts/activate` (Windows) ou `source .venv/bin/activate` (Linux/Mac)
   - `pip install -r requirements.txt`
2) Copie `.env.example` para `.env` e ajuste as variáveis (especialmente `DATABASE_URL` e `JWT_SECRET`).
3) Rode as migrations:
   - `alembic upgrade head`
4) Rode a API:
   - `uvicorn app.main:app --reload`

Endpoints de exemplo
- `POST /auth/dev-login` body:
  `{ "email": "dev@example.com", "org_slug": "demo" }` → retorna token.
- Usando o token (Bearer), chame:
  - `POST /datasources/test` body: `{ "sqlalchemy_url": "postgresql+psycopg2://user:pass@host:5432/db" }`
  - `POST /ingest/csv` (multipart) `file=@dados.csv`
  - `POST /ingest/sheets` body: `{ "spreadsheet_id": "...", "range": "Plan1" }`
  - `GET /indicators/valor-mes-a-mes?from=2024-01-01&to=2024-12-31`
  - `GET /indicators/mapa-por-uf?from=2024-01-01&to=2024-12-31`

Multi-tenant e segurança
- JWT inclui `org` (organization_id). As consultas filtram por `organization_id`.
- Sanitização simples para queries de indicadores: apenas SELECT com binds.
- RBAC básico via `memberships` (Owner/Admin/Editor/Viewer) — extensível.

Scheduler (APScheduler)
- Iniciado no `startup`. Intervalo padrão `CRON_DEFAULT_MINUTES` (60).
- Job busca `data_sources.is_recurring = true` e executa ingest conforme tipo:
  - `sql`: usa `config_json.query`.
  - `google_sheets`: usa `config_json.spreadsheet_id` e `config_json.range`.
  - `csv_upload`: ignorado por padrão.
- Logs em `job_runs`.

Drivers opcionais (SQLAlchemy URLs)
- MySQL/MariaDB: instale `pymysql` e use `mysql+pymysql://user:pass@host:3306/db`
- SQL Server: instale `pyodbc` e use `mssql+pyodbc://user:pass@dsn`
- Oracle: instale `cx_Oracle` e use `oracle+cx_oracle://user:pass@host:1521/?service_name=...`
- Snowflake: instale `snowflake-connector-python snowflake-sqlalchemy` e use `snowflake://user:pass@account/db/schema?warehouse=...&role=...`
- BigQuery: instale `bigquery-sqlalchemy` e use `bigquery://project` (requer credenciais e configs).

Notas Google Sheets
- O loader usa `gspread.oauth()`. Para dev, coloque `credentials.json` e gerará `token.json` local ao autorizar.
- Em produção, armazene tokens por tenant e injete no loader.

Estrutura
Consulte a árvore `app/` para módulos: routers, utils, cron, models, schemas.

Desenvolvimento
- Formato: Python padrão, SQLAlchemy 2.0.
- Rodar testes (se adicionados) e linters conforme preferência.

