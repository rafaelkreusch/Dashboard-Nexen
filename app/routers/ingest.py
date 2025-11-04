from datetime import datetime
from typing import List, Dict, Any
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

import pandas as pd

router = APIRouter(prefix="/ingest", tags=["ingest"])

# ---- Configuração de normalização ----
# Colunas finais esperadas em curated_records
CURATED_COLUMNS: List[str] = [
    "organization_id", "credor_code", "processo", "devedor", "cpf_cnpj",
    "dt_cadastro", "uf", "faixa_vencimento", "dt_vencimento", "vl_titulo",
    "situacao_processo", "vl_total_repasse", "vl_saldo", "dt_ultimo_credito",
    "portador", "motivo_devolucao", "vl_hono"
]

# Possíveis nomes vindos de planilhas -> nome padronizado usado no banco
HEADER_MAP = {
    # identificação
    "cód. cliente": "credor_code",
    "cod cliente": "credor_code",
    "cod. cliente": "credor_code",
    "credor_code": "credor_code",
    "processo": "processo",
    "devedor": "devedor",
    "cpf/cnpj": "cpf_cnpj",
    "cpf": "cpf_cnpj",
    "cnpj": "cpf_cnpj",

    # datas
    "dt. cadastro": "dt_cadastro",
    "data cadastro": "dt_cadastro",
    "dt cadastro": "dt_cadastro",
    "cadastro": "dt_cadastro",
    "dt. vencimento": "dt_vencimento",
    "dt vencimento": "dt_vencimento",
    "vencimento": "dt_vencimento",
    "dt. último crédito": "dt_ultimo_credito",
    "dt ultimo credito": "dt_ultimo_credito",
    "data último crédito": "dt_ultimo_credito",

    # UF e faixas
    "uf": "uf",
    "faixa de vencimento": "faixa_vencimento",
    "faixa_vencimento": "faixa_vencimento",

    # valores
    "vl. título": "vl_titulo",
    "vl titulo": "vl_titulo",
    "valor título": "vl_titulo",
    "vl. total repasse": "vl_total_repasse",
    "vl total repasse": "vl_total_repasse",
    "vl. saldo": "vl_saldo",
    "vl saldo": "vl_saldo",
    "vl. hono": "vl_hono",
    "vl hono": "vl_hono",

    # situação/portador/motivo
    "situação do processo": "situacao_processo",
    "situacao do processo": "situacao_processo",
    "situação_processo": "situacao_processo",
    "portador": "portador",
    "motivo da devolução": "motivo_devolucao",
    "motivo devolucao": "motivo_devolucao",
    "motivo_devolucao": "motivo_devolucao",
}

NUM_COLS = ["vl_titulo", "vl_total_repasse", "vl_saldo", "vl_hono"]
DATE_COLS = ["dt_cadastro", "dt_vencimento", "dt_ultimo_credito"]

def _to_snake(s: str) -> str:
    """normaliza chave para facilitar o mapeamento."""
    return (
        s.strip()
         .lower()
         .replace("_", " ")
         .replace("-", " ")
         .replace(".", "")
    )

def _normalize_numbers(df: pd.DataFrame) -> None:
    for col in NUM_COLS:
        if col in df.columns:
            # remove milhar ".", troca vírgula por ponto e converte
            df[col] = (
                df[col].astype(str)
                      .str.replace(".", "", regex=False)
                      .str.replace(",", ".", regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

def _normalize_dates(df: pd.DataFrame) -> None:
    for col in DATE_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")

def _normalize_text(df: pd.DataFrame) -> None:
    if "uf" in df.columns:
        df["uf"] = df["uf"].astype(str).str.upper().str.strip().str[:2]
    # corta campos de texto muito longos (opcional)
    for col, size in [
        ("faixa_vencimento", 120),
        ("situacao_processo", 120),
        ("portador", 120),
        ("motivo_devolucao", 240),
    ]:
        if col in df.columns:
            df[col] = df[col].astype("string").str.slice(0, size)

def _map_headers_and_normalize(rows: List[Dict[str, Any]], organization_id: int, credor_code: str | None) -> List[Dict[str, Any]]:
    """
    Recebe rows (lista de dicts) vindos do loader, padroniza nomes de colunas,
    normaliza tipos e garante todas as colunas esperadas.
    """
    if not rows:
        return []

    # 1) DataFrame para limpeza em lote
    df = pd.DataFrame(rows)

    # 2) Renomear colunas vindas do Excel/CSV para nossas colunas
    rename_map = {}
    for col in df.columns:
        key = _to_snake(str(col))
        if key in HEADER_MAP:
            rename_map[col] = HEADER_MAP[key]
        else:
            # tenta coincidência simples (já em snake)
            rename_map[col] = HEADER_MAP.get(key, key.replace(" ", "_"))

    df = df.rename(columns=rename_map)

    # 3) Garante colunas esperadas e valores padrão
    for c in CURATED_COLUMNS:
        if c not in df.columns:
            df[c] = None

    df["organization_id"] = organization_id
    if credor_code:
        # se veio credor_code no endpoint, sobrepõe
        df["credor_code"] = credor_code

    # 4) Normalizações
    _normalize_numbers(df)
    _normalize_dates(df)
    _normalize_text(df)

    # 5) Retorna apenas as colunas do curated (ordem não importa para named params, mas ajuda)
    df = df[CURATED_COLUMNS]
    return df.to_dict(orient="records")

# ---------------- Endpoints ----------------

@router.post('/sql')
def ingest_sql(payload: IngestSQLIn, db: DbSession, ctx=Depends(get_current_ctx)):
    ds = db.get(DataSource, payload.data_source_id)
    if not ds or ds.organization_id != ctx.organization_id:
        raise HTTPException(status_code=404, detail="Fonte não encontrada")
    if ds.type != 'sql' or not ds.sqlalchemy_url:
        raise HTTPException(status_code=400, detail="Fonte não é do tipo SQL")

    jr = JobRun(organization_id=ctx.organization_id, target_type='datasource',
                target_id=ds.id, status='running', started_at=datetime.utcnow())
    db.add(jr)
    db.commit()
    db.refresh(jr)
    try:
        eng = make_engine(ds.sqlalchemy_url)
        with eng.connect() as conn:
            if not payload.query.strip().lower().startswith('select'):
                raise HTTPException(status_code=400, detail="Apenas SELECT é permitido")
            result = conn.execute(text(payload.query))
            rows_raw = [dict(r._mapping) for r in result]

        rows = _map_headers_and_normalize(
            rows_raw, ctx.organization_id, credor_code=None
        )
        store_staging(rows, ctx.organization_id, db)
        # materialize_curated aceita lista de dicts; já vai tudo normalizado
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
async def ingest_csv(
    db: DbSession,
    ctx=Depends(get_current_ctx),
    file: UploadFile = File(...),
    credor_code: str | None = None
):
    try:
        data = await file.read()
        rows_raw = load_csv_bytes(data)                 # list[dict]
        rows = _map_headers_and_normalize(
            rows_raw, ctx.organization_id, credor_code
        )
        store_staging(rows, ctx.organization_id, db)
        count = materialize_curated(rows, ctx.organization_id, db, credor_code)
        return {"ok": True, "rows": count}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post('/xlsx')
async def ingest_xlsx(
    db: DbSession,
    ctx=Depends(get_current_ctx),
    file: UploadFile = File(...),
    credor_code: str | None = None
):
    try:
        filename = (file.filename or '').lower()
        if not filename.endswith('.xlsx'):
            raise HTTPException(
                status_code=400,
                detail='Apenas arquivos .xlsx são suportados. Salve seu Excel como .xlsx e tente novamente.'
            )
        data = await file.read()
        rows_raw = load_xlsx_bytes(data)                # list[dict]
        rows = _map_headers_and_normalize(
            rows_raw, ctx.organization_id, credor_code
        )
        store_staging(rows, ctx.organization_id, db)
        count = materialize_curated(rows, ctx.organization_id, db, credor_code)
        return {"ok": True, "rows": count}
    except Exception as e:
        msg = str(e)
        if 'openpyxl' in msg.lower():
            msg = 'Erro ao ler Excel. Verifique se o arquivo é .xlsx válido.'
        raise HTTPException(status_code=400, detail=msg)

@router.post('/sheets')
def ingest_sheets(payload: SheetsIn, db: DbSession, ctx=Depends(get_current_ctx)):
    rows_raw = load_sheet(payload.spreadsheet_id, payload.range)  # list[dict]
    rows = _map_headers_and_normalize(rows_raw, ctx.organization_id, credor_code=None)
    store_staging(rows, ctx.organization_id, db)
    count = materialize_curated(rows, ctx.organization_id, db)
    return {"ok": True, "rows": count}
