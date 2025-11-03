from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def make_engine(url: str) -> Engine:
    return create_engine(url, pool_pre_ping=True, future=True)


def test_connection(url: str) -> tuple[bool, str | None]:
    try:
        engine = make_engine(url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, None
    except Exception as e:
        return False, str(e)

