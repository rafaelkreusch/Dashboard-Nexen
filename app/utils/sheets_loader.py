from typing import List, Dict
import gspread


def load_sheet(spreadsheet_id: str, range_name: str) -> List[Dict]:
    # This function uses the default credentials available to gspread.
    # In production, configure OAuth and store per-tenant tokens.
    gc = gspread.oauth()  # expects credentials.json and token.json in working dir
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(range_name) if ":" not in range_name else sh.worksheet(range_name.split("!")[-1])
    values = ws.get_all_records()
    return values

