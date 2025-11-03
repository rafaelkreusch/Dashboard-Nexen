from __future__ import annotations
from datetime import datetime, date
from typing import Iterable, Dict, Any, List
import math
from typing import cast
from dateutil import parser as dateparser
from sqlalchemy.orm import Session

from app.models import CuratedRecord, StagingRecord
import unicodedata


KEY_MAP = {
    'uf': ['uf', 'estado', 'state'],
    'processo': ['processo', 'n_processo', 'num_processo', 'numero_processo', 'numero_do_processo', 'nro_processo'],
    # Accept common headers like "Faixa de Vencimento" -> faixa_de_vencimento
    'faixa_vencimento': ['faixa_vencimento', 'faixa_de_vencimento', 'faixa', 'vencimento_faixa'],
    'devedor': ['devedor', 'nome_devedor', 'devedora', 'nome', 'sacado'],
    'cpf_cnpj': ['cpf_cnpj', 'cpf', 'cnpj', 'documento', 'cpf_cgc', 'cpf_cnpj_'],
    # Código da fonte/credor pode vir do Excel (ex: "Cód. Cliente")
    'credor_code': ['credor_code', 'cod_cliente', 'codigo_cliente', 'id_cliente', 'cliente_codigo', 'codigo_credor', 'cod_credor', 'cliente_cod'],
    'dt_vencimento': ['dt_vencimento', 'vencimento', 'data_vencimento'],
    'vl_titulo': ['vl_titulo', 'valor', 'valor_titulo', 'amount'],
    'situacao_processo': ['situacao_processo', 'situacao', 'status', 'situacao_do_processo'],
    'vl_total_repasse': ['vl_total_repasse', 'valor_repasse', 'repasse'],
    # novos campos
    'vl_saldo': ['vl_saldo', 'saldo', 'valor_saldo', 'vl_sld'],
    'dt_ultimo_credito': ['dt_ultimo_credito', 'ultimo_credito', 'dt_ult_credito', 'data_ultimo_credito'],
    'portador': ['portador', 'portador_carteira'],
    'motivo_devolucao': ['motivo_devolucao', 'motivo_da_devolucao', 'motivo_devol', 'motivo_dev', 'motivo_da_devoluacao'],
    'vl_honorario_devedor': ['vl_honorario_devedor', 'honorario_devedor', 'vl_honorario', 'valor_honorario_devedor'],
    'vl_tx_contrato': ['vl_tx_contrato', 'tx_contrato', 'taxa_contrato', 'vl_tx'],
    # novos campos adicionais
    'comercial': ['comercial'],
    'cobrador': ['cobrador'],
    'dt_encerrado': ['dt_encerrado','encerrado','data_encerrado','dt_baixa','data_baixa'],
    'dias_vencidos_cadastro': ['dias_vencidos_no_cadastro','dias_vencidos_cadastro','dias_vencidos'],
    'dt_cadastro': ['dt_cadastro', 'data_cadastro', 'created_at', 'dt_criacao'],
}


def _norm_key(k: str) -> str:
    # normalize accents and common separators
    if not isinstance(k, str):
        k = str(k)
    s = unicodedata.normalize('NFKD', k)
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    s = s.strip().lower()
    for ch in [' ', '/', '.', '-', '\\']:
        s = s.replace(ch, '_')
    while '__' in s:
        s = s.replace('__', '_')
    return s


def _find(row: Dict[str, Any], target_key: str):
    candidates = set(KEY_MAP.get(target_key, []))
    # pass 1: exact match against known aliases
    for k, v in row.items():
        nk = _norm_key(k)
        if nk in candidates or nk == target_key:
            return v
    # pass 2: fuzzy contains to be resilient to variações de cabeçalhos
    tk = target_key
    for k, v in row.items():
        nk = _norm_key(k)
        if tk == 'cpf_cnpj':
            if ('cpf' in nk) or ('cnpj' in nk) or ('cpf_cgc' in nk) or ('cpfcnpj' in nk):
                return v
        elif tk == 'devedor':
            if ('devedor' in nk) or ('devedora' in nk) or ('sacado' in nk) or (nk == 'nome'):
                return v
        elif tk == 'processo':
            if 'processo' in nk or nk in ('num_processo','n_processo','numero_processo','numero_do_processo','nro_processo'):
                return v
        elif tk == 'credor_code':
            # exemplos: cod_cliente, codigo_cliente, id_cliente
            if ('cod' in nk and 'cliente' in nk) or ('codigo' in nk and 'cliente' in nk) or nk in ('credor','cod_credor','codigo_credor'):
                return v
        elif tk == 'faixa_vencimento':
            if 'faixa' in nk and 'venc' in nk:
                return v
    return None


def _to_dt(v):
    if v in (None, ""):
        return None
    try:
        import pandas as pd  # type: ignore
        if pd.isna(v):
            return None
    except Exception:
        pass
    # pandas.Timestamp
    try:
        if hasattr(v, "to_pydatetime"):
            return v.to_pydatetime()
    except Exception:
        pass
    if isinstance(v, datetime):
        return v
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day)
    # Convert known NaT/NaN strings to None
    s = str(v).strip()
    if s.lower() in ("nat", "nan", "none", "null"):
        return None
    try:
        return dateparser.parse(s)
    except Exception:
        return None


def _to_float(v):
    if v in (None, ""):
        return None
    # Handle pandas/NumPy NaN
    try:
        import math, numpy as np  # type: ignore
        if (isinstance(v, float) and (math.isnan(v) or math.isinf(v))) or (hasattr(np, 'isnan') and isinstance(v, (np.floating,)) and np.isnan(v)):
            return None
    except Exception:
        pass
    s = str(v).strip()
    if s.lower() in ("nan", "nat", "none", "null"):
        return None
    try:
        return float(s.replace('.', '').replace(',', '.')) if (',' in s and s.count(',')==1 and '.' in s) else float(s.replace(',', '.'))
    except Exception:
        return None

def _to_int(v):
    if v in (None, ""):
        return None
    try:
        import math, numpy as np  # type: ignore
        if (isinstance(v, float) and (math.isnan(v) or math.isinf(v))) or (hasattr(np, 'isnan') and isinstance(v, (np.floating,)) and np.isnan(v)):
            return None
    except Exception:
        pass
    s = str(v).strip()
    if s.lower() in ("nan", "nat", "none", "null"):
        return None
    try:
        # remove separadores e converte
        s2 = s.replace('.', '').replace(',', '.')
        return int(float(s2))
    except Exception:
        return None


def materialize_curated(staging_rows: Iterable[Dict[str, Any]], organization_id: int, db: Session, credor_code: str | None = None) -> int:
    # Ingesta em lotes para bases grandes
    batch: list[CuratedRecord] = []
    count = 0
    BATCH_SIZE = 2000
    for raw in staging_rows:
        row = {(_norm_key(k)): v for k, v in raw.items()}
        cur = CuratedRecord(
            organization_id=organization_id,
            credor_code=(credor_code or _find(row, 'credor_code')),
            uf=( _find(row, 'uf') or None ),
            processo=( _find(row, 'processo') or None ),
            devedor=( _find(row, 'devedor') or None ),
            cpf_cnpj=( _find(row, 'cpf_cnpj') or None ),
            faixa_vencimento=( _find(row, 'faixa_vencimento') or None ),
            dt_vencimento=_to_dt(_find(row, 'dt_vencimento')),
            vl_titulo=_to_float(_find(row, 'vl_titulo')),
            situacao_processo=( _find(row, 'situacao_processo') or None ),
            vl_total_repasse=_to_float(_find(row, 'vl_total_repasse')),
            vl_saldo=_to_float(_find(row, 'vl_saldo')),
            dt_ultimo_credito=_to_dt(_find(row, 'dt_ultimo_credito')),
            portador=( _find(row, 'portador') or None ),
            motivo_devolucao=( _find(row, 'motivo_devolucao') or None ),
            vl_honorario_devedor=_to_float(_find(row, 'vl_honorario_devedor')),
            vl_tx_contrato=_to_float(_find(row, 'vl_tx_contrato')),
            comercial=( _find(row, 'comercial') or None ),
            cobrador=( _find(row, 'cobrador') or None ),
            dt_encerrado=_to_dt(_find(row, 'dt_encerrado')),
            dias_vencidos_cadastro=_to_int(_find(row, 'dias_vencidos_cadastro')),
            dt_cadastro=_to_dt(_find(row, 'dt_cadastro')),
        )
        batch.append(cur)
        count += 1
        if len(batch) >= BATCH_SIZE:
            db.add_all(batch)
            db.commit()
            batch.clear()
    if batch:
        db.add_all(batch)
        db.commit()
    return count


def _to_jsonable(value: Any) -> Any:
    try:
        import numpy as np  # type: ignore
    except Exception:  # pragma: no cover - numpy may not be present but we handle generically
        np = None  # type: ignore

    # pandas Timestamp
    if hasattr(value, "to_pydatetime"):
        try:
            return cast(datetime, value.to_pydatetime()).isoformat()
        except Exception:
            pass

    # datetime/date
    if isinstance(value, (datetime, date)):
        return value.isoformat()

    # numpy types
    if np is not None:
        if isinstance(value, (np.integer,)):
            return int(value)
        if isinstance(value, (np.floating,)):
            f = float(value)
            return None if (math.isnan(f) or math.isinf(f)) else f
        if isinstance(value, (np.bool_,)):
            return bool(value)

    # floats NaN/inf
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None

    return value


def _row_to_jsonable(row: Dict[str, Any]) -> Dict[str, Any]:
    return {k: _to_jsonable(v) for k, v in row.items()}


def store_staging(rows: List[Dict[str, Any]], organization_id: int, db: Session) -> int:
    for r in rows:
        db.add(StagingRecord(organization_id=organization_id, raw_json=_row_to_jsonable(r)))
    db.commit()
    return len(rows)
