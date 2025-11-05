from datetime import datetime
from typing import List, Dict, Any
import os, csv, tempfile, io, time

from fastapi import APIRouter, Depends, UploadFile, File, HTTPException, BackgroundTasks
from sqlalchemy import text, select
from sqlalchemy.orm import Session

from app.deps import get_current_ctx, DbSession
from app.schemas import IngestSQLIn, SheetsIn
from app.models import DataSource, JobRun
from app.utils.db_connect import make_engine
from app.utils.csv_loader import load_csv_bytes
from app.utils.sheets_loader import load_sheet
from app.utils.transforms import store_staging, materialize_curated

import pandas as pd

router = APIRouter(prefix="/ingest", tags=["ingest"])

# ---- Configuração de normalização ----
CURATED_COLUMNS: List[str] = [
    "organization_id", "credor_code", "processo", "devedor", "cpf_cnpj",
    "dt_cadastro", "uf", "faixa_vencimento", "dt_vencimento", "vl_titulo",
    "situacao_processo", "vl_total_repasse", "vl_saldo", "dt_ultimo_credito",
    "portador", "motivo_devolucao", "vl_hono"
]

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
    # UF/faixa
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
    # status/portador/motivo
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
    for col, size in [
        ("faixa_vencimento", 120),
        ("situacao_processo", 120),
        ("portador", 120),
        ("motivo_devolucao", 240),
    ]:
        if col in df.columns:
            df[col] = df[col].astype("string").str.slice(0, size)


def _map_headers_and_normalize(rows: List[Dict[str, Any]], organization_id: int, credor_code: str | None) -> List[Dict[str, Any]]:
    if not rows:
        return []
    df = pd.DataFrame(rows)
    rename_map = {}
    for col in df.columns:
        key = _to_snake(str(col))
        rename_map[col] = HEADER_MAP.get(key, key.replace(" ", "_"))
    df = df.rename(columns=rename_map)

    for c in CURATED_COLUMNS:
        if c not in df.columns:
            df[c] = None

    df["organization_id"] = organization_id
    if credor_code:
        df["credor_code"] = credor_code

    _normalize_numbers(df)
    _normalize_dates(df)
    _normalize_text(df)

    df = df[CURATED_COLUMNS]
    return df.to_dict(orient="records")


# -------------------- COPY para Postgres --------------------
def _copy_into_curated(rows_norm: List[Dict[str, Any]], org_id: int, db: DbSession):
    """
    Recebe rows já normalizados e faz COPY para curated_records (Postgres).
    """
    cols = ["organization_id","credor_code","processo","devedor","cpf_cnpj",
            "dt_cadastro","uf","faixa_vencimento","dt_vencimento","vl_titulo",
            "situacao_processo","vl_total_repasse","vl_saldo","dt_ultimo_credito",
            "portador","motivo_devolucao","vl_hono"]

    buf = io.StringIO()
    w = csv.writer(buf)
    for r in rows_norm:
        r["organization_id"] = org_id
        w.writerow([r.get(c) for c in cols])
    buf.seek(0)

    raw = db.connection().connection  # psycopg2 connection
    with raw.cursor() as cur:
        cur.copy_expert(
            f"COPY curated_records ({', '.join(cols)}) FROM STDIN WITH (FORMAT CSV)",
            buf
        )
    db.commit()


def _worker_csv_copy(job_id: int, org_id: int, path_csv: str, db: DbSession, credor_code: str | None):
    """
    Lê CSV grande do disco em batches, normaliza e faz COPY.
    Atualiza o JobRun com sucesso/erro e remove o arquivo ao final.
    """
    try:
        t0 = time.time()

        # Detectar delimitador
        with open(path_csv, "r", encoding="utf-8", newline="") as f:
            sample = f.read(4096)
        delim = ";" if sample.count(";") > sample.count(",") else ","

        total = 0
        BATCH = 20000
        batch: List[Dict[str, Any]] = []

        with open(path_csv, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, delimiter=delim)
            header = [h.strip() for h in (reader.fieldnames or [])]
            if not header:
                raise Exception("Cabeçalho ausente no CSV.")

            for row in reader:
                batch.append({k: row.get(k, "") for k in header})
                if len(batch) >= BATCH:
                    norm = _map_headers_and_normalize(batch, org_id, credor_code)
                    if norm:
                        _copy_into_curated(norm, org_id, db)
                    total += len(batch)
                    batch.clear()

        if batch:
            norm = _map_headers_and_normalize(batch, org_id, credor_code)
            if norm:
                _copy_into_curated(norm, org_id, db)
            total += len(batch)

        dt = round(time.time() - t0, 1)
        db.execute(
            text("UPDATE job_runs SET status='success', logs=:l, finished_at=NOW() WHERE id=:i"),
            {"l": f"CSV importado: {total} linhas em {dt}s", "i": job_id},
        )
        db.commit()

    except Exception as e:
        db.execute(
            text("UPDATE job_runs SET status='error', logs=:l, finished_at=NOW() WHERE id=:i"),
            {"l": str(e), "i": job_id},
        )
        db.commit()
    finally:
        try:
            os.remove(path_csv)
        except:
            pass


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

        rows = _map_headers_and_normalize(rows_raw, ctx.organization_id, credor_code=None)
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
async def ingest_csv(
    db: DbSession,
    ctx=Depends(get_current_ctx),
    file: UploadFile = File(...),
    credor_code: str | None = None
):
    """
    CSV pequeno (memória) – mantém seu fluxo atual.
    """
    try:
        data = await file.read()
        rows_raw = load_csv_bytes(data)  # list[dict]
        rows = _map_headers_and_normalize(rows_raw, ctx.organization_id, credor_code)
        store_staging(rows, ctx.organization_id, db)
        count = materialize_curated(rows, ctx.organization_id, db, credor_code)
        return {"ok": True, "rows": count}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post('/csv/async')
async def ingest_csv_async(
    background: BackgroundTasks,
    db: DbSession,
    ctx=Depends(get_current_ctx),
    file: UploadFile = File(...),
    credor_code: str | None = None
):
    """
    CSV grande – salva em /tmp, cria JobRun e processa em background com COPY.
    """
    # salvar sem carregar tudo na memória
    fd, path = tempfile.mkstemp(suffix=".csv")
    os.close(fd)
    with open(path, "wb") as out:
        while chunk := await file.read(1024 * 1024):
            out.write(chunk)

    jr = JobRun(
        organization_id=ctx.organization_id,
        target_type='ingest_csv',
        target_id=None,
        status='queued',
        logs=path,
        started_at=datetime.utcnow()
    )
    db.add(jr)
    db.commit()
    db.refresh(jr)

    # dispara o worker
    background.add_task(_worker_csv_copy, jr.id, ctx.organization_id, path, db, credor_code)
    return {"accepted": True, "job_id": jr.id}


@router.get("/jobs/{job_id}")
def job_status(job_id: int, db: DbSession):
    row = db.execute(
        text("SELECT id, status, logs, started_at, finished_at FROM job_runs WHERE id=:i"),
        {"i": job_id}
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Job não encontrado")
    return dict(row)


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

        # STREAM (sem ler tudo pra memória)
        df = pd.read_excel(file.file, dtype=str, engine="openpyxl")
        rows_raw = df.to_dict(orient="records")

        rows = _map_headers_and_normalize(rows_raw, ctx.organization_id, credor_code)
        store_staging(rows, ctx.organization_id, db)
        count = materialize_curated(rows, ctx.organization_id, db, credor_code)
        return {"ok": True, "rows": count}
    except Exception as e:
        db.rollback()
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
