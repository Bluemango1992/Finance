from pathlib import Path
from typing import Any

SCHEMA_SQL_PATH = Path(__file__).resolve().parent / "data" / "schema.sql"


def _connect(database: str):
    import duckdb

    return duckdb.connect(database=database)


def _load_schema_sql() -> str:
    return SCHEMA_SQL_PATH.read_text(encoding="utf-8")


def ensure_schema(database: str) -> None:
    db_path = Path(database)
    if database != ":memory:":
        db_path.parent.mkdir(parents=True, exist_ok=True)

    with _connect(database) as connection:
        connection.execute(_load_schema_sql())


def ensure_prices_table(database: str) -> None:
    """Backward-compatible wrapper. Prefer ensure_schema()."""
    ensure_schema(database)


def run_query(query: str, database: str = ":memory:") -> list[dict[str, Any]]:
    with _connect(database) as connection:
        result = connection.execute(query)
        columns = [column[0] for column in result.description]
        rows = result.fetchall()
    return [dict(zip(columns, row)) for row in rows]
