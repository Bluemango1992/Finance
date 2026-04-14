from pathlib import Path
from typing import Any

PRICES_TABLE_DDL = """
create table if not exists prices (
    asset_id varchar not null,
    date date not null,
    open double,
    high double,
    low double,
    close double,
    volume bigint,
    source varchar not null,
    ingestion_ts timestamp not null,
    primary key (asset_id, date)
)
"""


def _connect(database: str):
    import duckdb

    return duckdb.connect(database=database)


def ensure_prices_table(database: str) -> None:
    db_path = Path(database)
    if database != ":memory:":
        db_path.parent.mkdir(parents=True, exist_ok=True)

    with _connect(database) as connection:
        connection.execute(PRICES_TABLE_DDL)


def run_query(query: str, database: str = ":memory:") -> list[dict[str, Any]]:
    with _connect(database) as connection:
        result = connection.execute(query)
        columns = [column[0] for column in result.description]
        rows = result.fetchall()
    return [dict(zip(columns, row)) for row in rows]


def count_prices_rows(database: str, asset_id: str | None = None) -> int:
    with _connect(database) as connection:
        if asset_id:
            return int(
                connection.execute(
                    "select count(*) from prices where asset_id = ?", [asset_id]
                ).fetchone()[0]
            )
        return int(connection.execute("select count(*) from prices").fetchone()[0])


def insert_prices_rows(database: str, rows: list[dict[str, Any]]) -> dict[str, int]:
    if not rows:
        return {"inserted": 0, "duplicates": 0}

    ensure_prices_table(database)
    before = count_prices_rows(database, asset_id=rows[0]["asset_id"])
    payload = [
        (
            row["asset_id"],
            row["date"],
            row["open"],
            row["high"],
            row["low"],
            row["close"],
            row["volume"],
            row["source"],
            row["ingestion_ts"],
        )
        for row in rows
    ]

    with _connect(database) as connection:
        connection.executemany(
            """
            insert into prices (
                asset_id, date, open, high, low, close, volume, source, ingestion_ts
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict (asset_id, date) do nothing
            """,
            payload,
        )

    after = count_prices_rows(database, asset_id=rows[0]["asset_id"])
    inserted = max(after - before, 0)
    duplicates = max(len(rows) - inserted, 0)
    return {"inserted": inserted, "duplicates": duplicates}
