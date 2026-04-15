import numpy as np
import pandas as pd

from finance.features.macro_regime import (
    build_macro_regime_features,
    classify_fed_regime,
    compute_fed_stock_corr_36m,
)


def _mock_spy_daily() -> pd.DataFrame:
    rng = np.random.default_rng(42)
    dates = pd.date_range("2010-01-01", "2020-12-31", freq="B")
    returns = rng.normal(0.0002, 0.01, len(dates))
    close = 100.0 * np.exp(np.cumsum(returns))
    return pd.DataFrame({"date": dates, "close": close})


def _mock_fed_monthly() -> pd.DataFrame:
    rng = np.random.default_rng(24)
    dates = pd.date_range("2010-01-31", "2020-12-31", freq="ME")
    fed = 2.0 + np.cumsum(rng.normal(0.0, 0.08, len(dates)))
    return pd.DataFrame({"observation_date": dates, "FEDFUNDS": fed})


def test_macro_regime_alignment_and_index() -> None:
    spy = _mock_spy_daily()
    fed = _mock_fed_monthly()

    corr = compute_fed_stock_corr_36m(spy, fed, window=36)

    assert corr.name == "fed_stock_corr_36m"
    assert isinstance(corr.index, pd.DatetimeIndex)
    assert corr.index.freqstr in ("ME", "M")
    assert corr.index.min() >= pd.Timestamp("2010-01-31")


def test_macro_regime_shape_and_warmup_nans() -> None:
    spy = _mock_spy_daily()
    fed = _mock_fed_monthly()

    corr = compute_fed_stock_corr_36m(spy, fed, window=36)
    assert len(corr) >= 120
    assert corr.isna().sum() <= 38
    assert corr.iloc[41:].notna().all()


def test_macro_regime_feature_frame_and_classification() -> None:
    spy = _mock_spy_daily()
    fed = _mock_fed_monthly()

    features = build_macro_regime_features(spy, fed, window=36)
    assert {"fed_stock_corr_36m", "fed_regime_36m"}.issubset(features.columns)
    assert set(features["fed_regime_36m"].dropna().unique()).issubset(
        {"positive", "negative", "neutral"}
    )

    regime = classify_fed_regime(features["fed_stock_corr_36m"])
    assert regime.name == "fed_regime_36m"


def test_macro_regime_uses_lagged_policy_by_default() -> None:
    spy = _mock_spy_daily()
    fed = _mock_fed_monthly()

    corr_lag0 = compute_fed_stock_corr_36m(spy, fed, window=36, policy_lag_months=0)
    corr_lag1 = compute_fed_stock_corr_36m(spy, fed, window=36)

    first_valid_0 = corr_lag0.first_valid_index()
    first_valid_1 = corr_lag1.first_valid_index()
    assert first_valid_0 is not None and first_valid_1 is not None
    assert first_valid_1 > first_valid_0
