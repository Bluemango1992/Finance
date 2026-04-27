from __future__ import annotations

from dataclasses import asdict

import pandas as pd

from finance.fundamentals_pipeline import (
    PipelineConfig,
    PipelineRunSummary,
    build_sp500_universe,
    normalize_sp500_symbol,
    run_sp500_fundamentals_pipeline,
)


def _statement(rows: dict[str, list[float]]) -> pd.DataFrame:
    columns = pd.to_datetime(["2021-12-31", "2022-12-31", "2023-12-31", "2024-12-31"])
    return pd.DataFrame(rows, index=columns).T


def test_normalize_sp500_symbol_replaces_dot() -> None:
    assert normalize_sp500_symbol("brk.b") == "BRK-B"


def test_build_sp500_universe_normalizes_symbols(monkeypatch) -> None:
    monkeypatch.setattr("finance.fundamentals_pipeline.ensure_sp500_data", lambda path=None: None)
    monkeypatch.setattr(
        "finance.fundamentals_pipeline.load_sp500_data",
        lambda path=None: [
            {
                "symbol": "BRK.B",
                "security": "Berkshire Hathaway",
                "gics_sector": "Financials",
                "gics_sub_industry": "Multi-Sector Holdings",
            }
        ],
    )

    universe = build_sp500_universe("data/sp500_constituents.json")

    assert universe == [
        {
            "symbol": "BRK-B",
            "source_symbol": "BRK.B",
            "security": "Berkshire Hathaway",
            "gics_sector": "Financials",
            "gics_sub_industry": "Multi-Sector Holdings",
        }
    ]


def test_run_sp500_fundamentals_pipeline_builds_flat_and_long_tables(monkeypatch) -> None:
    monkeypatch.setattr(
        "finance.fundamentals_pipeline.build_sp500_universe",
        lambda path=None, refresh=False: [
            {
                "symbol": "MSFT",
                "source_symbol": "MSFT",
                "security": "Microsoft",
                "gics_sector": "Information Technology",
                "gics_sub_industry": "Systems Software",
            }
        ],
    )

    def fake_fetch(_symbol: str, _config: PipelineConfig):
        info = {
            "longName": "Microsoft Corporation",
            "sector": "Technology",
            "industry": "Software",
            "country": "United States",
            "currency": "USD",
            "exchange": "NMS",
            "quoteType": "EQUITY",
            "marketCap": 3_000.0,
            "enterpriseValue": 3_200.0,
            "trailingPE": 30.0,
            "returnOnEquity": 0.32,
            "profitMargins": 0.22,
            "sharesOutstanding": 100.0,
        }
        income_stmt = _statement(
            {
                "Total Revenue": [100.0, 120.0, 150.0, 180.0],
                "Net Income": [20.0, 24.0, 33.0, 45.0],
            }
        )
        cashflow = _statement(
            {
                "Free Cash Flow": [18.0, 22.0, 28.0, 40.0],
                "Operating Cash Flow": [24.0, 29.0, 36.0, 50.0],
                "Capital Expenditure": [-6.0, -7.0, -8.0, -10.0],
            }
        )
        balance_sheet = _statement(
            {
                "Cash And Cash Equivalents": [50.0, 55.0, 60.0, 70.0],
                "Total Debt": [30.0, 28.0, 26.0, 24.0],
                "Stockholders Equity": [80.0, 90.0, 100.0, 120.0],
            }
        )
        history = pd.DataFrame(
            {"Close": [250.0, 260.0, 270.0]},
            index=pd.to_datetime(["2024-04-19", "2024-04-20", "2024-04-21"]),
        )
        return object(), info, income_stmt, cashflow, balance_sheet, history

    monkeypatch.setattr("finance.fundamentals_pipeline._fetch_with_retry", fake_fetch)

    artifacts = run_sp500_fundamentals_pipeline(
        PipelineConfig(
            max_tickers=1,
            output_long=None,
            pause_between_tickers_seconds=0.0,
        )
    )

    assert artifacts.summary.successful_tickers == 1
    assert artifacts.summary.failed_tickers == 0
    assert artifacts.summary.completeness_pct == 100.0

    row = artifacts.flat_table.iloc[0].to_dict()
    assert row["symbol"] == "MSFT"
    assert row["revenue_fy0"] == 180.0
    assert row["revenue_fy3"] == 100.0
    assert round(row["revenue_growth_latest"], 6) == 0.2
    assert round(row["revenue_cagr"], 6) == round((180.0 / 100.0) ** (1 / 3) - 1.0, 6)
    assert row["net_margin_latest_pct"] == 25.0
    assert row["roe_latest_pct"] == 37.5
    assert row["debt_to_equity"] == 0.2
    assert row["cash_to_debt"] == 70.0 / 24.0
    assert row["profitable"] is True
    assert row["positive_fcf"] is True
    assert row["high_roe"] is True
    assert row["margin_expanding"] is True
    assert row["validation_issue_count"] == 0

    assert set(artifacts.long_table["metric"]) == {
        "revenue",
        "net_income",
        "operating_cash_flow",
        "free_cash_flow",
        "capital_expenditure",
        "cash",
        "total_debt",
        "total_equity",
    }


def test_run_sp500_fundamentals_pipeline_tracks_failures_and_validation(monkeypatch) -> None:
    monkeypatch.setattr(
        "finance.fundamentals_pipeline.build_sp500_universe",
        lambda path=None, refresh=False: [
            {
                "symbol": "AAPL",
                "source_symbol": "AAPL",
                "security": "Apple",
                "gics_sector": "Information Technology",
                "gics_sub_industry": "Technology Hardware",
            },
            {
                "symbol": "FAIL",
                "source_symbol": "FAIL",
                "security": "Failure Co",
                "gics_sector": "Industrials",
                "gics_sub_industry": "Industrial Machinery",
            },
        ],
    )

    def fake_fetch(symbol: str, _config: PipelineConfig):
        if symbol == "FAIL":
            raise RuntimeError("boom")
        info = {
            "longName": "Apple Inc.",
            "marketCap": 500.0,
            "trailingPE": 700.0,
        }
        income_stmt = _statement(
            {
                "Total Revenue": [120.0, 110.0, 90.0, -10.0],
                "Net Income": [10.0, 9.0, 8.0, 7.0],
            }
        )
        cashflow = _statement(
            {
                "Free Cash Flow": [5.0, 4.0, 3.0, 2.0],
                "Operating Cash Flow": [8.0, 7.0, 6.0, 5.0],
                "Capital Expenditure": [-2.0, -2.0, -2.0, -2.0],
            }
        )
        balance_sheet = _statement(
            {
                "Cash And Cash Equivalents": [20.0, 18.0, 16.0, 14.0],
                "Total Debt": [10.0, 9.0, 8.0, 7.0],
                "Stockholders Equity": [30.0, 25.0, 20.0, 15.0],
            }
        )
        history = pd.DataFrame({"Close": [100.0]}, index=pd.to_datetime(["2024-04-21"]))
        return object(), info, income_stmt, cashflow, balance_sheet, history

    monkeypatch.setattr("finance.fundamentals_pipeline._fetch_with_retry", fake_fetch)

    artifacts = run_sp500_fundamentals_pipeline(PipelineConfig(pause_between_tickers_seconds=0.0))

    summary = asdict(artifacts.summary)
    assert summary["total_tickers"] == 2
    assert summary["successful_tickers"] == 1
    assert summary["failed_tickers"] == 1
    assert summary["failed_symbols"] == ["FAIL"]
    assert summary["validation_issue_counts"]["pe_ratio_outlier"] == 1
    assert summary["validation_issue_counts"]["revenue_non_positive"] == 1

    row = artifacts.flat_table.iloc[0].to_dict()
    assert "pe_ratio_outlier" in row["validation_issues"]
    assert "revenue_non_positive" in row["validation_issues"]
    assert row["missing_critical_field_count"] == 0


def test_pipeline_run_summary_is_json_friendly() -> None:
    summary = PipelineRunSummary(
        total_tickers=500,
        processed_tickers=500,
        successful_tickers=490,
        failed_tickers=10,
        success_rate=98.0,
        completeness_pct=95.0,
        failed_symbols=["ABC"],
        missing_field_counts={"pe_ratio": 5},
        validation_issue_counts={"pe_ratio_outlier": 2},
    )

    assert asdict(summary)["success_rate"] == 98.0
