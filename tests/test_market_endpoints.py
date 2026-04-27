from __future__ import annotations

from datetime import datetime

from finance.data import (
    GSPC_ASSET_ID,
    GSPC_SYMBOL,
    IEF_ASSET_ID,
    IEF_SYMBOL,
    SPY_ASSET_ID,
    SPY_SYMBOL,
    TLT_ASSET_ID,
    TLT_SYMBOL,
    get_gspc_prices,
    get_ief_prices,
    get_spy_prices,
    get_tlt_prices,
)


def _sample_history():
    import pandas as pd

    return pd.DataFrame(
        [
            {
                "Date": datetime(2024, 1, 3),
                "Open": 470.0,
                "High": 471.0,
                "Low": 468.0,
                "Close": 469.5,
                "Volume": 1000,
            },
            {
                "Date": datetime(2024, 1, 2),
                "Open": 468.0,
                "High": 469.0,
                "Low": 467.0,
                "Close": 468.5,
                "Volume": 900,
            },
        ]
    )


def test_get_spy_prices_returns_normalized_frame(monkeypatch) -> None:
    history = _sample_history()

    monkeypatch.setattr(
        "finance.data.clients.market_api.fetch_yfinance_history",
        lambda symbol, period="max": history,
    )

    frame = get_spy_prices(provider="yfinance")

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
    assert frame["asset_id"].unique().tolist() == [SPY_ASSET_ID]
    assert frame["source"].unique().tolist() == ["yfinance"]
    assert frame.iloc[0]["date"].isoformat() == "2024-01-02"


def test_get_gspc_prices_returns_normalized_frame(monkeypatch) -> None:
    history = _sample_history()

    monkeypatch.setattr(
        "finance.data.clients.market_api.fetch_yfinance_history",
        lambda symbol, period="max": history,
    )

    frame = get_gspc_prices(provider="yfinance")

    assert frame["asset_id"].unique().tolist() == [GSPC_ASSET_ID]
    assert frame["source"].unique().tolist() == ["yfinance"]


def test_get_tlt_prices_returns_normalized_frame(monkeypatch) -> None:
    history = _sample_history()

    monkeypatch.setattr(
        "finance.data.clients.market_api.fetch_yfinance_history",
        lambda symbol, period="max": history,
    )

    frame = get_tlt_prices(provider="yfinance")

    assert frame["asset_id"].unique().tolist() == [TLT_ASSET_ID]
    assert frame["source"].unique().tolist() == ["yfinance"]


def test_get_ief_prices_returns_normalized_frame(monkeypatch) -> None:
    history = _sample_history()

    monkeypatch.setattr(
        "finance.data.clients.market_api.fetch_yfinance_history",
        lambda symbol, period="max": history,
    )

    frame = get_ief_prices(provider="yfinance")

    assert frame["asset_id"].unique().tolist() == [IEF_ASSET_ID]
    assert frame["source"].unique().tolist() == ["yfinance"]


def test_market_proxy_constants_are_stable() -> None:
    assert GSPC_ASSET_ID == "GSPC"
    assert GSPC_SYMBOL == "^GSPC"
    assert SPY_ASSET_ID == "SPY"
    assert SPY_SYMBOL == "SPY"
    assert TLT_ASSET_ID == "TLT"
    assert TLT_SYMBOL == "TLT"
    assert IEF_ASSET_ID == "IEF"
    assert IEF_SYMBOL == "IEF"
