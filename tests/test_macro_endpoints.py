from __future__ import annotations

import io

from finance.data import (
    CPIAUCSL_SERIES_ID,
    DGS10_SERIES_ID,
    DGS2_SERIES_ID,
    DGS30_SERIES_ID,
    FEDFUNDS_SERIES_ID,
    get_cpi_all_items,
    get_fed_funds_rate,
    get_treasury_yield_10y,
    get_treasury_yield_2y,
    get_treasury_yield_30y,
)


def _fred_payload(series_id: str) -> io.StringIO:
    return io.StringIO(
        """{
  "observations": [
    {"date": "2024-01-01", "value": "4.25"},
    {"date": "2024-02-01", "value": "4.50"},
    {"date": "2024-03-01", "value": "."}
  ]
}"""
    )


def test_get_fed_funds_rate_returns_normalized_frame(monkeypatch) -> None:
    monkeypatch.setattr(
        "finance.data.clients.fred_api.urlopen",
        lambda url, timeout=30: _fred_payload(FEDFUNDS_SERIES_ID),
    )

    frame = get_fed_funds_rate()

    assert list(frame.columns) == [
        "observation_date",
        "FEDFUNDS",
        "series_id",
        "source",
    ]
    assert frame["series_id"].unique().tolist() == [FEDFUNDS_SERIES_ID]
    assert frame["source"].unique().tolist() == ["fred"]
    assert frame.iloc[0]["FEDFUNDS"] == 4.25


def test_get_cpi_all_items_returns_normalized_frame(monkeypatch) -> None:
    monkeypatch.setattr(
        "finance.data.clients.fred_api.urlopen",
        lambda url, timeout=30: _fred_payload(CPIAUCSL_SERIES_ID),
    )

    frame = get_cpi_all_items()

    assert frame["series_id"].unique().tolist() == [CPIAUCSL_SERIES_ID]
    assert frame["source"].unique().tolist() == ["fred"]


def test_get_treasury_yield_endpoints_return_normalized_frame(monkeypatch) -> None:
    monkeypatch.setattr(
        "finance.data.clients.fred_api.urlopen",
        lambda url, timeout=30: _fred_payload(DGS10_SERIES_ID),
    )

    frame_2y = get_treasury_yield_2y()
    frame_10y = get_treasury_yield_10y()
    frame_30y = get_treasury_yield_30y()

    assert frame_2y["series_id"].unique().tolist() == [DGS2_SERIES_ID]
    assert frame_10y["series_id"].unique().tolist() == [DGS10_SERIES_ID]
    assert frame_30y["series_id"].unique().tolist() == [DGS30_SERIES_ID]


def test_macro_endpoints_reject_non_fred_provider() -> None:
    try:
        get_fed_funds_rate(provider="yfinance")
    except RuntimeError as exc:
        assert "not implemented for macro series" in str(exc)
    else:
        raise AssertionError("Expected RuntimeError for unsupported macro provider")
