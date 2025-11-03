from typing import Dict, Any


ALLOWED_SQL_PREFIX = "select"


def is_safe_select(sql: str) -> bool:
    # tolerate UTF-8 BOM and leading whitespace
    s = sql.lstrip('\ufeff').strip()
    # Allow trailing semicolon by stripping it before validation
    if s.endswith(';'):
        s = s[:-1].strip()
    lower = s.lower()
    if not lower.startswith(ALLOWED_SQL_PREFIX):
        return False
    # very naive sanitization: disallow semicolons and common DDL/DML
    banned = [";", " drop ", " delete ", " update ", " insert ", " alter ", " create "]
    return not any(b in lower for b in banned)


def apply_placeholders(sql: str, allowed_filters: Dict[str, Any]) -> tuple[str, Dict[str, Any]]:
    # Replace known placeholders with SQLAlchemy bind params
    # {{tenant_id}} -> :tenant_id, {{from}} -> :from, {{to}} -> :to, {{filter:campo}} -> AND campo=:campo
    params: Dict[str, Any] = {}
    s = sql.replace("{{tenant_id}}", ":tenant_id")

    # Optional: replace a chosen date column placeholder {{date_field}}
    # We only allow safe identifier characters to avoid SQL injection.
    df = allowed_filters.get("date_field")
    if df is not None:
        safe = ''.join(ch for ch in str(df) if ch.isalnum() or ch in ('_', '.'))
        if not safe:
            safe = 'dt_cadastro'
        s = s.replace("{{date_field}}", safe)
    else:
        # If not provided, keep as-is (so SQLs sem placeholder continuam funcionando)
        pass

    if "from" in allowed_filters and allowed_filters.get("from") is not None:
        s = s.replace("{{from}}", ":from")
        params["from"] = allowed_filters["from"]
    else:
        s = s.replace("{{from}}", "NULL")

    if "to" in allowed_filters and allowed_filters.get("to") is not None:
        s = s.replace("{{to}}", ":to")
        params["to"] = allowed_filters["to"]
    else:
        s = s.replace("{{to}}", "NULL")

    # simple filter injections
    for k, v in allowed_filters.items():
        if k in ("from", "to", "tenant_id"):
            continue
        placeholder = f"{{{{filter:{k}}}}}"
        if placeholder in s and v is not None:
            s = s.replace(placeholder, f" AND {k} = :{k} ")
            params[k] = v
        elif placeholder in s:
            s = s.replace(placeholder, "")

    if "tenant_id" in allowed_filters:
        params["tenant_id"] = allowed_filters["tenant_id"]

    return s, params
