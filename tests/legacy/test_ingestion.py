from datetime import UTC, datetime

from finance.data.ingestion import validate_prices_rows


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
