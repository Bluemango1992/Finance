from __future__ import annotations


def compute_rolling_beta(
    stock_prices,
    benchmark_prices,
    *,
    window: int = 36,
    stock_date_col: str = "date",
    stock_close_col: str = "close",
    benchmark_date_col: str = "date",
    benchmark_close_col: str = "close",
    benchmark_lag_months: int = 0,
):
    try:
        import numpy as np
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("compute_rolling_beta requires pandas and numpy.") from exc

    if window < 2:
        raise ValueError("window must be >= 2")
    if benchmark_lag_months < 0:
        raise ValueError("benchmark_lag_months must be >= 0")

    stock = stock_prices.copy()
    benchmark = benchmark_prices.copy()

    if stock_date_col not in stock.columns or stock_close_col not in stock.columns:
        raise ValueError(f"stock_prices must include '{stock_date_col}' and '{stock_close_col}'")
    if benchmark_date_col not in benchmark.columns or benchmark_close_col not in benchmark.columns:
        raise ValueError(
            f"benchmark_prices must include '{benchmark_date_col}' and '{benchmark_close_col}'"
        )

    stock[stock_date_col] = pd.to_datetime(stock[stock_date_col], errors="coerce")
    benchmark[benchmark_date_col] = pd.to_datetime(benchmark[benchmark_date_col], errors="coerce")

    stock = stock.dropna(subset=[stock_date_col]).sort_values(stock_date_col)
    benchmark = benchmark.dropna(subset=[benchmark_date_col]).sort_values(benchmark_date_col)

    stock[stock_close_col] = pd.to_numeric(stock[stock_close_col], errors="coerce")
    benchmark[benchmark_close_col] = pd.to_numeric(benchmark[benchmark_close_col], errors="coerce")
    stock = stock.dropna(subset=[stock_close_col])
    benchmark = benchmark.dropna(subset=[benchmark_close_col])

    stock_m = (
        stock.set_index(stock_date_col)[[stock_close_col]]
        .resample("ME")
        .last()
        .rename(columns={stock_close_col: "stock_close"})
    )
    bench_m = (
        benchmark.set_index(benchmark_date_col)[[benchmark_close_col]]
        .resample("ME")
        .last()
        .rename(columns={benchmark_close_col: "benchmark_close"})
    )

    stock_m["stock_ret_1m"] = np.log(stock_m["stock_close"]).diff()
    bench_m["benchmark_ret_1m"] = np.log(bench_m["benchmark_close"]).diff()
    bench_m["benchmark_ret_1m"] = bench_m["benchmark_ret_1m"].shift(benchmark_lag_months)

    aligned = stock_m[["stock_ret_1m"]].join(bench_m[["benchmark_ret_1m"]], how="inner")
    cov = aligned["stock_ret_1m"].rolling(window).cov(aligned["benchmark_ret_1m"])
    var = aligned["benchmark_ret_1m"].rolling(window).var()

    beta = cov / var
    beta.name = f"beta_{window}m"
    return beta
