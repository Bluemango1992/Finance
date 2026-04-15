from __future__ import annotations


def backtest_regime_filtered_long(
    monthly_returns,
    fed_regime_36m,
    *,
    lag_periods: int = 1,
):
    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("backtest_regime_filtered_long requires pandas.") from exc

    if lag_periods < 0:
        raise ValueError("lag_periods must be >= 0")

    returns = _to_series(monthly_returns, "monthly_returns")
    regimes = _to_series(fed_regime_36m, "fed_regime_36m")

    returns = returns.astype(float).dropna()
    regimes = regimes.astype("string")
    shifted_regimes = regimes.shift(lag_periods)

    aligned = pd.concat(
        [returns.rename("monthly_return"), shifted_regimes.rename("regime_signal")], axis=1, join="inner"
    ).dropna(subset=["monthly_return", "regime_signal"])

    summaries = []
    strategy_returns = {}
    invested_masks = {}

    for regime_name in ("positive", "negative", "neutral"):
        invested = aligned["regime_signal"] == regime_name
        strat_ret = aligned["monthly_return"].where(invested, 0.0)

        strategy_returns[regime_name] = strat_ret
        invested_masks[regime_name] = invested

        summaries.append(
            {
                "strategy": f"{regime_name}_only",
                "cumulative_return": float((1.0 + strat_ret).prod() - 1.0),
                "months_invested": int(invested.sum()),
                "mean_return": float(strat_ret.mean()),
                "volatility": float(strat_ret.std(ddof=0)),
            }
        )

    summary_df = pd.DataFrame(summaries).set_index("strategy")
    returns_df = pd.DataFrame(strategy_returns)
    invested_df = pd.DataFrame(invested_masks)

    return {
        "summary": summary_df,
        "strategy_returns": returns_df,
        "invested_mask": invested_df,
        "aligned_input": aligned,
    }


def backtest_regime_weighted_long(
    monthly_returns,
    fed_regime_36m,
    *,
    lag_periods: int = 1,
    weights: dict[str, float] | None = None,
):
    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("backtest_regime_weighted_long requires pandas.") from exc

    if lag_periods < 0:
        raise ValueError("lag_periods must be >= 0")

    default_weights = {"positive": 1.0, "neutral": 1.0, "negative": 0.5}
    regime_weights = dict(default_weights if weights is None else weights)
    for regime_name in ("positive", "neutral", "negative"):
        if regime_name not in regime_weights:
            raise ValueError(f"Missing weight for regime '{regime_name}'")
        if regime_weights[regime_name] < 0:
            raise ValueError("Regime weights must be >= 0")

    returns = _to_series(monthly_returns, "monthly_returns").astype(float).dropna()
    regimes = _to_series(fed_regime_36m, "fed_regime_36m").astype("string")
    shifted_regimes = regimes.shift(lag_periods)

    aligned = pd.concat(
        [returns.rename("monthly_return"), shifted_regimes.rename("regime_signal")], axis=1, join="inner"
    ).dropna(subset=["monthly_return", "regime_signal"])

    aligned["weight"] = aligned["regime_signal"].map(regime_weights).astype(float)
    weighted_returns = aligned["monthly_return"] * aligned["weight"]
    baseline_returns = aligned["monthly_return"]

    summary_df = pd.DataFrame(
        [
            _summarize_strategy("weighted_by_regime", weighted_returns, aligned["weight"] > 0),
            _summarize_strategy("always_long", baseline_returns, aligned["weight"] > -1),
        ]
    ).set_index("strategy")

    return {
        "summary": summary_df,
        "strategy_returns": pd.DataFrame(
            {
                "weighted_by_regime": weighted_returns,
                "always_long": baseline_returns,
            }
        ),
        "weights": aligned["weight"],
        "aligned_input": aligned,
    }


def _to_series(values, name: str):
    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("backtest_regime_filtered_long requires pandas.") from exc

    if isinstance(values, pd.Series):
        return values
    return pd.Series(values, name=name)


def _summarize_strategy(strategy_name: str, returns, invested_mask):
    return {
        "strategy": strategy_name,
        "cumulative_return": float((1.0 + returns).prod() - 1.0),
        "months_invested": int(invested_mask.sum()),
        "mean_return": float(returns.mean()),
        "volatility": float(returns.std(ddof=0)),
    }
