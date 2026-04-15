import numpy as np
import pandas as pd

from finance.features.beta import compute_rolling_beta


def _mock_prices_with_target_beta(target_beta: float = 1.5) -> tuple[pd.DataFrame, pd.DataFrame]:
    rng = np.random.default_rng(7)
    dates = pd.date_range("2015-01-01", "2025-12-31", freq="B")

    bench_returns = rng.normal(0.00015, 0.008, len(dates))
    idio_noise = rng.normal(0.0, 0.003, len(dates))
    stock_returns = (target_beta * bench_returns) + idio_noise

    bench_close = 100.0 * np.exp(np.cumsum(bench_returns))
    stock_close = 100.0 * np.exp(np.cumsum(stock_returns))

    stock = pd.DataFrame({"date": dates, "close": stock_close})
    benchmark = pd.DataFrame({"date": dates, "close": bench_close})
    return stock, benchmark


def test_rolling_beta_index_and_name() -> None:
    stock, benchmark = _mock_prices_with_target_beta()
    beta = compute_rolling_beta(stock, benchmark, window=36)

    assert beta.name == "beta_36m"
    assert isinstance(beta.index, pd.DatetimeIndex)
    assert beta.index.freqstr in ("ME", "M")


def test_rolling_beta_has_limited_warmup_nans() -> None:
    stock, benchmark = _mock_prices_with_target_beta()
    beta = compute_rolling_beta(stock, benchmark, window=36)

    assert len(beta) >= 120
    assert beta.isna().sum() <= 37
    assert beta.iloc[41:].notna().all()


def test_rolling_beta_tracks_expected_magnitude() -> None:
    stock, benchmark = _mock_prices_with_target_beta(target_beta=1.5)
    beta = compute_rolling_beta(stock, benchmark, window=36)

    realized = beta.dropna().tail(24).mean()
    assert 1.2 <= realized <= 1.8


def test_lagged_benchmark_delays_first_valid_value() -> None:
    stock, benchmark = _mock_prices_with_target_beta()
    beta_lag0 = compute_rolling_beta(stock, benchmark, window=36, benchmark_lag_months=0)
    beta_lag1 = compute_rolling_beta(stock, benchmark, window=36, benchmark_lag_months=1)

    first_valid_0 = beta_lag0.first_valid_index()
    first_valid_1 = beta_lag1.first_valid_index()
    assert first_valid_0 is not None and first_valid_1 is not None
    assert first_valid_1 > first_valid_0
