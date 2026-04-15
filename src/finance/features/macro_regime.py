from __future__ import annotations

from typing import Literal


def compute_fed_stock_corr_36m(
    spy_prices,
    fed_rates,
    *,
    window: int = 36,
    policy_lag_months: int = 1,
    spy_date_col: str = "date",
    spy_close_col: str = "close",
    fed_date_col: str = "observation_date",
    fed_value_col: str = "FEDFUNDS",
    fed_transform: Literal["delta", "level"] = "delta",
):
    try:
        import numpy as np
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("macro_regime requires pandas and numpy.") from exc

    if window < 2:
        raise ValueError("window must be >= 2")
    if policy_lag_months < 0:
        raise ValueError("policy_lag_months must be >= 0")

    spy = spy_prices.copy()
    fed = fed_rates.copy()

    if spy_date_col not in spy.columns or spy_close_col not in spy.columns:
        raise ValueError(f"SPY input must include '{spy_date_col}' and '{spy_close_col}'")
    if fed_date_col not in fed.columns or fed_value_col not in fed.columns:
        raise ValueError(f"Fed input must include '{fed_date_col}' and '{fed_value_col}'")

    spy[spy_date_col] = pd.to_datetime(spy[spy_date_col], errors="coerce")
    spy = spy.dropna(subset=[spy_date_col]).sort_values(spy_date_col)
    spy_m = (
        spy.set_index(spy_date_col)[[spy_close_col]]
        .resample("ME")
        .last()
        .rename(columns={spy_close_col: "spy_close"})
    )
    spy_m["spy_ret_1m"] = np.log(spy_m["spy_close"]).diff()

    fed[fed_date_col] = pd.to_datetime(fed[fed_date_col], errors="coerce")
    fed = fed.dropna(subset=[fed_date_col]).sort_values(fed_date_col)
    fed_m = (
        fed.set_index(fed_date_col)[[fed_value_col]]
        .resample("ME")
        .last()
        .rename(columns={fed_value_col: "fed_funds"})
        .ffill()
    )
    if fed_transform == "delta":
        fed_m["fed_policy"] = fed_m["fed_funds"].diff()
    else:
        fed_m["fed_policy"] = fed_m["fed_funds"]
    fed_m["fed_policy"] = fed_m["fed_policy"].shift(policy_lag_months)

    aligned = spy_m[["spy_ret_1m"]].join(fed_m[["fed_policy"]], how="inner")
    corr = aligned["spy_ret_1m"].rolling(window).corr(aligned["fed_policy"])
    corr.name = "fed_stock_corr_36m"
    return corr


def classify_fed_regime(
    fed_stock_corr_36m,
    *,
    positive_threshold: float = 0.2,
    negative_threshold: float = -0.2,
):
    try:
        import numpy as np
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("macro_regime requires pandas and numpy.") from exc

    if negative_threshold >= positive_threshold:
        raise ValueError("negative_threshold must be lower than positive_threshold")

    values = fed_stock_corr_36m.astype(float)
    conditions = [values > positive_threshold, values < negative_threshold]
    labels = ["positive", "negative"]
    regime = pd.Series(np.select(conditions, labels, default="neutral"), index=values.index)
    regime.name = "fed_regime_36m"
    return regime


def build_macro_regime_features(
    spy_prices, fed_rates, *, window: int = 36, policy_lag_months: int = 1
):
    corr = compute_fed_stock_corr_36m(
        spy_prices, fed_rates, window=window, policy_lag_months=policy_lag_months
    )
    regime = classify_fed_regime(corr)
    return corr.to_frame().join(regime, how="left")
