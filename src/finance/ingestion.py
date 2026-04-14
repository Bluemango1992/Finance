from datetime import UTC, datetime
from typing import Any

from finance.db import ensure_prices_table, insert_prices_rows
from finance.providers import fetch_yfinance_history


def load_spy_history():
    return fetch_yfinance_history("SPY")


def transform_to_prices_rows(raw_df, ingestion_ts: datetime | None = None) -> list[dict[str, Any]]:
    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("pandas is required for ingestion. Install yfinance extras.") from exc

    required = ["Open", "High", "Low", "Close", "Volume"]
    missing = [column for column in required if column not in raw_df.columns]
    if missing:
        raise RuntimeError(f"Yahoo history payload missing columns: {missing}")

    if ingestion_ts is None:
        ts = datetime.now(UTC)
    else:
        ts = ingestion_ts.astimezone(UTC)
    ts = ts.replace(tzinfo=None)
    frame = raw_df[required].copy()

    index = pd.to_datetime(frame.index, utc=True, errors="coerce")
    frame["date"] = index.date
    frame["asset_id"] = "SPY"
    frame["source"] = "yahoo_finance"
    frame["ingestion_ts"] = ts
    frame = frame.rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )
    frame = frame[
        [
            "asset_id",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "source",
            "ingestion_ts",
        ]
    ]

    return frame.to_dict(orient="records")


def validate_prices_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    valid_rows: list[dict[str, Any]] = []
    invalid_count = 0
    numeric_fields = ("open", "high", "low", "close", "volume")

    for row in rows:
        if row.get("date") is None or row.get("close") is None:
            invalid_count += 1
            continue

        candidate = dict(row)
        rejected = False
        for field in numeric_fields:
            value = candidate.get(field)
            if value is None:
                candidate[field] = None
                continue
            try:
                if field == "volume":
                    candidate[field] = int(value)
                else:
                    candidate[field] = float(value)
            except (TypeError, ValueError):
                rejected = True
                break

        if rejected:
            invalid_count += 1
            continue

        valid_rows.append(candidate)

    return valid_rows, invalid_count


def ingest_spy_prices(database: str) -> dict[str, int]:
    ensure_prices_table(database)
    raw = load_spy_history()
    transformed = transform_to_prices_rows(raw)
    valid_rows, invalid_rows = validate_prices_rows(transformed)

    write_result = insert_prices_rows(database, valid_rows)
    return {
        "rows_fetched": len(raw),
        "rows_valid": len(valid_rows),
        "rows_invalid": invalid_rows,
        "rows_inserted": write_result["inserted"],
        "rows_duplicates": write_result["duplicates"],
    }
