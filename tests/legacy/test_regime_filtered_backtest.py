import pandas as pd

from finance.backtest.regime_filtered import backtest_regime_filtered_long


def test_regime_filtered_backtest_outputs_comparable_summary() -> None:
    idx = pd.date_range("2020-01-31", periods=8, freq="ME")
    monthly_returns = pd.Series([0.02, -0.01, 0.01, 0.03, -0.02, 0.01, 0.0, 0.02], index=idx)
    regimes = pd.Series(
        ["positive", "negative", "neutral", "positive", "neutral", "negative", "positive", "neutral"],
        index=idx,
        name="fed_regime_36m",
    )

    result = backtest_regime_filtered_long(monthly_returns, regimes, lag_periods=1)
    summary = result["summary"]

    assert list(summary.index) == ["positive_only", "negative_only", "neutral_only"]
    assert {"cumulative_return", "months_invested", "mean_return", "volatility"}.issubset(
        summary.columns
    )
    assert summary["months_invested"].sum() == len(result["aligned_input"])


def test_regime_filtered_backtest_applies_lag_no_lookahead() -> None:
    idx = pd.date_range("2021-01-31", periods=4, freq="ME")
    monthly_returns = pd.Series([0.10, 0.20, -0.10, 0.05], index=idx)
    regimes = pd.Series(["positive", "negative", "neutral", "neutral"], index=idx)

    result = backtest_regime_filtered_long(monthly_returns, regimes, lag_periods=1)
    positive_mask = result["invested_mask"]["positive"]

    # With lag=1, warmup month is dropped and month 2 is first tradable point.
    assert positive_mask.index[0] == idx[1]
    assert bool(positive_mask.iloc[0]) is True
