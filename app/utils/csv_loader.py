import io
from typing import List, Dict
import pandas as pd


def load_csv_bytes(data: bytes, sep: str = ",") -> List[Dict]:
    df = pd.read_csv(io.BytesIO(data), sep=sep)
    return df.to_dict(orient='records')


def load_xlsx_bytes(data: bytes) -> List[Dict]:
    # Be explicit with engine to avoid pandas guessing issues
    # Read everything as object to preserve strings like CPFs and avoid float conversions
    bio = io.BytesIO(data)
    try:
        df = pd.read_excel(bio, engine="openpyxl", dtype=object)
    except Exception:
        # Fallback: let pandas guess if openpyxl is not available for some reason
        bio.seek(0)
        df = pd.read_excel(bio, dtype=object)
    # Keep column names as-is; transform layer normalizes keys
    return df.to_dict(orient='records')
