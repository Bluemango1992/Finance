from datetime import UTC, datetime

from finance.db import count_prices_rows
from finance.data.ingestion import ingest_spy_prices, validate_prices_rows


def test_validate_prices_rows_drops_null_date_and_close() -> None:
    rows = [
        {
            "asset_id": "SPY",
            "date": None,
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
            "volume": 100,
            "source": "yahoo_finance",
            "ingestion_ts": datetime.now(UTC),
        },
        {
            "asset_id": "SPY",
            "date": datetime(2026, 4, 14, tzinfo=UTC).date(),
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": None,
            "volume": 100,
            "source": "yahoo_finance",
            "ingestion_ts": datetime.now(UTC),
        },
        {
            "asset_id": "SPY",
            "date": datetime(2026, 4, 15, tzinfo=UTC).date(),
            "open": "100.5",
            "high": "101.5",
            "low": "99.5",
            "close": "100.7",
            "volume": "12345",
            "source": "yahoo_finance",
            "ingestion_ts": datetime.now(UTC),
        },
    ]

    valid, invalid = validate_prices_rows(rows)
    assert invalid == 2
    assert len(valid) == 1
    assert isinstance(valid[0]["open"], float)
    assert isinstance(valid[0]["volume"], int)


def test_ingest_spy_prices_returns_stage_summary(monkeypatch, tmp_path) -> None:
    fake_raw = [1, 2, 3]
    fake_transformed = [{"asset_id": "SPY", "date": "2024-01-02"}]
    fake_valid_rows = [{"asset_id": "SPY", "date": "2024-01-02"}]

    monkeypatch.setattr("finance.data.ingestion.load_spy_history", lambda: fake_raw)
    monkeypatch.setattr(
        "finance.data.ingestion.transform_to_prices_rows", lambda _raw: fake_transformed
    )
    monkeypatch.setattr("finance.data.ingestion.validate_prices_rows", lambda _rows: (fake_valid_rows, 1))
    monkeypatch.setattr(
        "finance.data.ingestion.insert_prices_rows",
        lambda _database, _rows: {"inserted": 1, "duplicates": 0},
    )

    summary = ingest_spy_prices(str(tmp_path / "prices.duckdb"))
    assert summary == {
        "rows_fetched": 3,
        "rows_valid": 1,
        "rows_invalid": 1,
        "rows_inserted": 1,
        "rows_duplicates": 0,
    }


def test_ingest_spy_prices_is_idempotent(monkeypatch, tmp_path) -> None:
    def fake_raw():
        return [1, 2]

    def fake_transform(_raw):
        ts = datetime.now(UTC)
        return [
            {
                "asset_id": "SPY",
                "date": datetime(2024, 1, 2, tzinfo=UTC).date(),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000,
                "source": "yahoo_finance",
                "ingestion_ts": ts,
            },
            {
                "asset_id": "SPY",
                "date": datetime(2024, 1, 3, tzinfo=UTC).date(),
                "open": 101.0,
                "high": 102.0,
                "low": 100.0,
                "close": 101.5,
                "volume": 1100,
                "source": "yahoo_finance",
                "ingestion_ts": ts,
            },
        ]

    monkeypatch.setattr("finance.data.ingestion.load_spy_history", fake_raw)
    monkeypatch.setattr("finance.data.ingestion.transform_to_prices_rows", fake_transform)

    database = str(tmp_path / "prices.duckdb")
    first = ingest_spy_prices(database)
    second = ingest_spy_prices(database)

    assert first["rows_inserted"] == 2
    assert second["rows_inserted"] == 0
    assert second["rows_duplicates"] == 2
    assert count_prices_rows(database, asset_id="SPY") == 2
