from datetime import UTC, date, datetime

from finance.db import count_prices_rows, ensure_prices_table, insert_prices_rows, run_query


def _sample_rows() -> list[dict]:
    ts = datetime(2026, 4, 14, 12, 0, tzinfo=UTC)
    return [
        {
            "asset_id": "SPY",
            "date": date(2024, 1, 2),
            "open": 100.0,
            "high": 101.0,
            "low": 99.5,
            "close": 100.5,
            "volume": 1000000,
            "source": "yahoo_finance",
            "ingestion_ts": ts,
        },
        {
            "asset_id": "SPY",
            "date": date(2024, 1, 3),
            "open": 101.0,
            "high": 102.0,
            "low": 100.0,
            "close": 101.5,
            "volume": 1100000,
            "source": "yahoo_finance",
            "ingestion_ts": ts,
        },
    ]


def test_ensure_prices_table_creates_schema(tmp_path) -> None:
    database = str(tmp_path / "schema.duckdb")
    ensure_prices_table(database)
    tables = run_query("show tables", database=database)
    assert any(row.get("name") == "prices" for row in tables)


def test_insert_is_idempotent_for_asset_date_primary_key(tmp_path) -> None:
    database = str(tmp_path / "prices.duckdb")
    ensure_prices_table(database)
    rows = _sample_rows()

    first = insert_prices_rows(database, rows)
    second = insert_prices_rows(database, rows)

    assert first["inserted"] == 2
    assert first["duplicates"] == 0
    assert second["inserted"] == 0
    assert second["duplicates"] == 2
    assert count_prices_rows(database, asset_id="SPY") == 2
