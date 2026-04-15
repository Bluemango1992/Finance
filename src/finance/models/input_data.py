from __future__ import annotations


def prepare_model_input_dataframe(
    spy_prices,
    fed_rates,
    *,
    asset_id: str = "SPY",
    include_corr_strength: bool = True,
    corr_window: int = 36,
    policy_lag_months: int = 1,
    log_current_regime: bool = False,
):
    try:
        import numpy as np
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("prepare_model_input_dataframe requires pandas and numpy.") from exc

    from finance.features.macro_regime import build_macro_regime_features

    prices = spy_prices.copy()
    if "date" not in prices.columns or "close" not in prices.columns:
        raise ValueError("spy_prices must include 'date' and 'close' columns.")

    if "asset_id" in prices.columns:
        prices = prices[prices["asset_id"] == asset_id]
    if prices.empty:
        raise ValueError(f"No price rows available for asset_id='{asset_id}'.")

    prices["date"] = pd.to_datetime(prices["date"], errors="coerce")
    prices = prices.dropna(subset=["date"]).sort_values("date")

    spy_m = (
        prices.set_index("date")[["close"]]
        .resample("ME")
        .last()
        .rename(columns={"close": "spy_close"})
    )
    spy_m["spy_log_ret_1m"] = np.log(spy_m["spy_close"]).diff()

    macro = build_macro_regime_features(
        prices[["date", "close"]],
        fed_rates,
        window=corr_window,
        policy_lag_months=policy_lag_months,
    )
    frame = spy_m.join(macro, how="inner")

    if include_corr_strength:
        frame["fed_corr_strength_36m"] = frame["fed_stock_corr_36m"].abs()

    if log_current_regime:
        non_null_regime = frame["fed_regime_36m"].dropna()
        current = non_null_regime.iloc[-1] if not non_null_regime.empty else "unknown"
        print(f"Current macro regime: {current}")

    return frame


def get_numeric_model_frame(model_input_df, *, dropna: bool = True):
    frame = model_input_df.select_dtypes(include=["number"]).copy()
    if dropna:
        frame = frame.dropna()
    return frame
