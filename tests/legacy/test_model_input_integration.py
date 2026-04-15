import numpy as np
import pandas as pd
import pytest

from finance.models.dimensional.pca import fit_transform_pca
from finance.models.dimensional.umap_model import fit_transform_umap
from finance.models.input_data import get_numeric_model_frame, prepare_model_input_dataframe


def _mock_spy_daily() -> pd.DataFrame:
    rng = np.random.default_rng(123)
    dates = pd.date_range("2010-01-01", "2020-12-31", freq="B")
    returns = rng.normal(0.00015, 0.01, len(dates))
    close = 100.0 * np.exp(np.cumsum(returns))
    return pd.DataFrame({"asset_id": "SPY", "date": dates, "close": close})


def _mock_fed_monthly() -> pd.DataFrame:
    rng = np.random.default_rng(321)
    dates = pd.date_range("2010-01-31", "2020-12-31", freq="ME")
    fed = 2.0 + np.cumsum(rng.normal(0.0, 0.07, len(dates)))
    return pd.DataFrame({"observation_date": dates, "FEDFUNDS": fed})


def test_model_input_contains_macro_features() -> None:
    frame = prepare_model_input_dataframe(_mock_spy_daily(), _mock_fed_monthly())

    assert "fed_stock_corr_36m" in frame.columns
    assert "fed_corr_strength_36m" in frame.columns
    assert "fed_regime_36m" in frame.columns
    assert frame.index.freqstr in ("ME", "M")
    assert (frame["fed_corr_strength_36m"].dropna() >= 0).all()


def test_model_input_lag_impacts_warmup_alignment() -> None:
    lag0 = prepare_model_input_dataframe(
        _mock_spy_daily(), _mock_fed_monthly(), policy_lag_months=0
    )["fed_stock_corr_36m"]
    lag1 = prepare_model_input_dataframe(_mock_spy_daily(), _mock_fed_monthly())[
        "fed_stock_corr_36m"
    ]

    i0 = lag0.first_valid_index()
    i1 = lag1.first_valid_index()
    assert i0 is not None and i1 is not None
    assert i1 > i0


def test_model_input_compatible_with_dimensional_models() -> None:
    frame = prepare_model_input_dataframe(_mock_spy_daily(), _mock_fed_monthly())
    X = get_numeric_model_frame(frame)

    # Keep a stable, low-dimensional matrix for deterministic test behavior.
    X_small = X[["spy_log_ret_1m", "fed_stock_corr_36m", "fed_corr_strength_36m"]]
    pca = fit_transform_pca(X_small, n_components=2)
    assert pca["embedding"].shape[1] == 2

    pytest.importorskip("umap")
    umap_result = fit_transform_umap(X_small, n_components=2, n_neighbors=10, min_dist=0.05)
    assert umap_result["embedding"].shape[1] == 2
