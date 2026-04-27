from __future__ import annotations

from datetime import datetime

from finance.data import (
    BNO_ASSET_ID,
    BNO_SYMBOL,
    BRENT_ASSET_ID,
    BRENT_SYMBOL,
    GOLD_ASSET_ID,
    GOLD_SYMBOL,
    GLD_ASSET_ID,
    GLD_SYMBOL,
    SILVER_ASSET_ID,
    SILVER_SYMBOL,
    SLV_ASSET_ID,
    SLV_SYMBOL,
    USL_ASSET_ID,
    USL_SYMBOL,
    WTI_ASSET_ID,
    WTI_SYMBOL,
    get_bno_prices,
    get_brent_prices,
    get_gld_prices,
    get_gold_prices,
    get_slv_prices,
    get_silver_prices,
    get_usl_prices,
    get_wti_prices,
)

def _sample_history():
    import pandas as pd

    return pd.DataFrame(
        [
            {
                "Date": datetime(2024, 1, 3),
                "Open": 2050.0,
                "High": 2060.0,
                "Low": 2040.0,
                "Close": 2055.0,
                "Volume": 1000,
            },
            {
                "Date": datetime(2024, 1, 2),
                "Open": 2045.0,
                "High": 2055.0,
                "Low": 2035.0,
                "Close": 2050.0,
                "Volume": 900,
            },
        ]
    )


def test_get_gold_prices_returns_normalized_frame(monkeypatch) -> None:
    history = _sample_history()

    monkeypatch.setattr(
        "finance.data.clients.market_api.fetch_yfinance_history",
        lambda symbol, period="max": history,
    )

    frame = get_gold_prices(provider="yfinance")

    assert list(frame.columns) == [
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "asset_id",
        "source",
    ]
    assert frame["asset_id"].unique().tolist() == [GOLD_ASSET_ID]
    assert frame["source"].unique().tolist() == ["yfinance"]
    assert frame.iloc[0]["date"].isoformat() == "2024-01-02"


def test_get_gold_prices_rejects_unimplemented_endpoint() -> None:
    try:
        get_gold_prices(endpoint="duckdb", provider="yfinance")
    except RuntimeError as exc:
        assert "not implemented" in str(exc)
    else:
        raise AssertionError("Expected RuntimeError for duckdb gold endpoint")


def test_gold_constants_are_stable() -> None:
    assert GOLD_ASSET_ID == "GOLD"
    assert GOLD_SYMBOL == "GC=F"


def test_get_silver_prices_returns_normalized_frame(monkeypatch) -> None:
    history = _sample_history()

    monkeypatch.setattr(
        "finance.data.clients.market_api.fetch_yfinance_history",
        lambda symbol, period="max": history,
    )

    frame = get_silver_prices(provider="yfinance")

    assert frame["asset_id"].unique().tolist() == [SILVER_ASSET_ID]
    assert frame["source"].unique().tolist() == ["yfinance"]


def test_get_wti_prices_returns_normalized_frame(monkeypatch) -> None:
    history = _sample_history()

    monkeypatch.setattr(
        "finance.data.clients.market_api.fetch_yfinance_history",
        lambda symbol, period="max": history,
    )

    frame = get_wti_prices(provider="yfinance")

    assert frame["asset_id"].unique().tolist() == [WTI_ASSET_ID]
    assert frame["source"].unique().tolist() == ["yfinance"]


def test_get_brent_prices_returns_normalized_frame(monkeypatch) -> None:
    history = _sample_history()

    monkeypatch.setattr(
        "finance.data.clients.market_api.fetch_yfinance_history",
        lambda symbol, period="max": history,
    )

    frame = get_brent_prices(provider="yfinance")

    assert frame["asset_id"].unique().tolist() == [BRENT_ASSET_ID]
    assert frame["source"].unique().tolist() == ["yfinance"]


def test_get_gld_prices_returns_normalized_frame(monkeypatch) -> None:
    history = _sample_history()

    monkeypatch.setattr(
        "finance.data.clients.market_api.fetch_yfinance_history",
        lambda symbol, period="max": history,
    )

    frame = get_gld_prices(provider="yfinance")

    assert frame["asset_id"].unique().tolist() == [GLD_ASSET_ID]
    assert frame["source"].unique().tolist() == ["yfinance"]


def test_get_slv_prices_returns_normalized_frame(monkeypatch) -> None:
    history = _sample_history()

    monkeypatch.setattr(
        "finance.data.clients.market_api.fetch_yfinance_history",
        lambda symbol, period="max": history,
    )

    frame = get_slv_prices(provider="yfinance")

    assert frame["asset_id"].unique().tolist() == [SLV_ASSET_ID]
    assert frame["source"].unique().tolist() == ["yfinance"]


def test_get_usl_prices_returns_normalized_frame(monkeypatch) -> None:
    history = _sample_history()

    monkeypatch.setattr(
        "finance.data.clients.market_api.fetch_yfinance_history",
        lambda symbol, period="max": history,
    )

    frame = get_usl_prices(provider="yfinance")

    assert frame["asset_id"].unique().tolist() == [USL_ASSET_ID]
    assert frame["source"].unique().tolist() == ["yfinance"]


def test_get_bno_prices_returns_normalized_frame(monkeypatch) -> None:
    history = _sample_history()

    monkeypatch.setattr(
        "finance.data.clients.market_api.fetch_yfinance_history",
        lambda symbol, period="max": history,
    )

    frame = get_bno_prices(provider="yfinance")

    assert frame["asset_id"].unique().tolist() == [BNO_ASSET_ID]
    assert frame["source"].unique().tolist() == ["yfinance"]


def test_commodity_constants_are_stable() -> None:
    assert SILVER_ASSET_ID == "SILVER"
    assert SILVER_SYMBOL == "SI=F"
    assert WTI_ASSET_ID == "WTI"
    assert WTI_SYMBOL == "CL=F"
    assert BRENT_ASSET_ID == "BRENT"
    assert BRENT_SYMBOL == "BZ=F"
    assert GLD_ASSET_ID == "GLD"
    assert GLD_SYMBOL == "GLD"
    assert SLV_ASSET_ID == "SLV"
    assert SLV_SYMBOL == "SLV"
    assert USL_ASSET_ID == "USL"
    assert USL_SYMBOL == "USL"
    assert BNO_ASSET_ID == "BNO"
    assert BNO_SYMBOL == "BNO"
