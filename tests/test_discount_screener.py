from __future__ import annotations

import pandas as pd

from finance.discount_screener import build_discount_screener


def test_build_discount_screener_ranks_lower_valuations_higher() -> None:
    frame = pd.DataFrame(
        {
            "symbol": ["AAA", "BBB", "CCC", "DDD"],
            "security": ["A", "B", "C", "D"],
            "gics_sector": ["Industrials"] * 4,
            "gics_sub_industry": ["Machinery"] * 4,
            "pe_ratio": [10.0, 14.0, 18.0, 22.0],
            "pfcf_ratio": [8.0, 10.0, 14.0, 18.0],
            "debt_to_equity": [0.5, 0.7, 0.8, 1.1],
            "revenue_growth_latest": [0.08, 0.07, 0.06, 0.05],
            "net_margin_latest_pct": [10.0, 11.0, 12.0, 9.0],
            "roe_latest_pct": [18.0, 16.0, 14.0, 12.0],
            "profitable": [True, True, True, True],
            "positive_fcf": [True, True, True, True],
            "high_roe": [True, True, False, False],
        }
    )

    screened = build_discount_screener(frame)

    assert screened.iloc[0]["symbol"] == "AAA"
    assert screened.iloc[0]["primary_discount_signal"] == "pe_ratio"
    assert bool(screened.iloc[0]["passes_quality_gate"]) is True


def test_build_discount_screener_excludes_financials_by_default() -> None:
    frame = pd.DataFrame(
        {
            "symbol": ["AAA", "BBB"],
            "security": ["A", "B"],
            "gics_sector": ["Financials", "Industrials"],
            "gics_sub_industry": ["Banks", "Machinery"],
            "pe_ratio": [10.0, 12.0],
            "pfcf_ratio": [9.0, 11.0],
            "debt_to_equity": [1.0, 0.6],
            "revenue_growth_latest": [0.04, 0.06],
            "net_margin_latest_pct": [12.0, 10.0],
            "roe_latest_pct": [14.0, 18.0],
            "profitable": [True, True],
            "positive_fcf": [True, True],
            "high_roe": [False, True],
        }
    )

    screened = build_discount_screener(frame)

    assert screened["symbol"].tolist() == ["BBB"]


def test_build_discount_screener_falls_back_to_sector_peer_group() -> None:
    frame = pd.DataFrame(
        {
            "symbol": ["AAA", "BBB", "CCC", "DDD"],
            "security": ["A", "B", "C", "D"],
            "gics_sector": ["Industrials"] * 4,
            "gics_sub_industry": ["Machinery", "Machinery", "Services", "Services"],
            "pe_ratio": [10.0, 12.0, 9.0, 11.0],
            "pfcf_ratio": [8.0, 9.0, 7.0, 8.0],
            "debt_to_equity": [0.5, 0.7, 0.4, 0.6],
            "revenue_growth_latest": [0.08, 0.07, 0.05, 0.06],
            "net_margin_latest_pct": [10.0, 11.0, 8.0, 9.0],
            "roe_latest_pct": [18.0, 16.0, 12.0, 13.0],
            "profitable": [True, True, True, True],
            "positive_fcf": [True, True, True, True],
            "high_roe": [True, True, False, False],
        }
    )

    screened = build_discount_screener(frame, min_sub_industry_size=3, min_sector_size=4)

    assert screened["peer_group"].nunique() == 1
    assert screened["peer_group"].iloc[0] == "sector::Industrials"
