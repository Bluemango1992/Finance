from __future__ import annotations

import json
import logging
import math
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from finance.providers import _import_yfinance
from finance.scraper.sp500 import ensure_sp500_data, load_sp500_data

LOGGER = logging.getLogger(__name__)

DEFAULT_FLAT_OUTPUT = Path("artifacts/sp500_fundamentals_flat.parquet")
DEFAULT_LONG_OUTPUT = Path("artifacts/sp500_fundamentals_long.parquet")
DEFAULT_SUMMARY_OUTPUT = Path("artifacts/sp500_fundamentals_summary.json")
DEFAULT_HISTORY_PERIOD = "5y"
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_DELAY_SECONDS = 1.5
DEFAULT_LONG_METRICS = (
    "revenue",
    "net_income",
    "operating_cash_flow",
    "free_cash_flow",
    "capital_expenditure",
    "cash",
    "total_debt",
    "total_equity",
)

INCOME_ROWS: dict[str, tuple[str, ...]] = {
    "revenue": ("Total Revenue", "Operating Revenue"),
    "net_income": ("Net Income", "Net Income Common Stockholders"),
}

CASHFLOW_ROWS: dict[str, tuple[str, ...]] = {
    "free_cash_flow": ("Free Cash Flow",),
    "operating_cash_flow": (
        "Operating Cash Flow",
        "Cash Flow From Continuing Operating Activities",
    ),
    "capital_expenditure": (
        "Capital Expenditure",
        "Net PPE Purchase And Sale",
        "Purchase Of PPE",
    ),
}

BALANCE_SHEET_ROWS: dict[str, tuple[str, ...]] = {
    "cash": (
        "Cash Cash Equivalents And Short Term Investments",
        "Cash And Cash Equivalents",
    ),
    "total_debt": ("Total Debt", "Net Debt"),
    "total_equity": (
        "Stockholders Equity",
        "Common Stock Equity",
        "Total Equity Gross Minority Interest",
    ),
}

INFO_FIELDS = {
    "longName": "company_name",
    "sector": "sector",
    "industry": "industry",
    "country": "country",
    "currency": "currency",
    "exchange": "exchange",
    "quoteType": "quote_type",
    "marketCap": "market_cap",
    "enterpriseValue": "enterprise_value",
    "trailingPE": "pe_ratio",
    "returnOnEquity": "roe_info",
    "profitMargins": "net_margin_info",
    "sharesOutstanding": "shares_outstanding",
}


@dataclass(slots=True)
class PipelineConfig:
    sp500_path: str | Path | None = None
    refresh_sp500: bool = False
    output_flat: str | Path = DEFAULT_FLAT_OUTPUT
    output_long: str | Path | None = DEFAULT_LONG_OUTPUT
    output_summary: str | Path = DEFAULT_SUMMARY_OUTPUT
    history_period: str = DEFAULT_HISTORY_PERIOD
    lookback_years: int = 4
    retry_attempts: int = DEFAULT_RETRY_ATTEMPTS
    retry_delay_seconds: float = DEFAULT_RETRY_DELAY_SECONDS
    pause_between_tickers_seconds: float = 0.0
    max_tickers: int | None = None
    log_level: str = "INFO"


@dataclass(slots=True)
class PipelineRunSummary:
    total_tickers: int
    processed_tickers: int
    successful_tickers: int
    failed_tickers: int
    success_rate: float
    completeness_pct: float
    failed_symbols: list[str]
    missing_field_counts: dict[str, int]
    validation_issue_counts: dict[str, int]


@dataclass(slots=True)
class PipelineArtifacts:
    flat_table: pd.DataFrame
    long_table: pd.DataFrame
    summary: PipelineRunSummary


def normalize_sp500_symbol(symbol: str) -> str:
    return symbol.strip().upper().replace(".", "-")


def _normalize_numeric(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        if not math.isfinite(float(value)):
            return None
        return float(value)
    return None


def _safe_ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator


def _safe_pct_change(current: float | None, previous: float | None) -> float | None:
    if current is None or previous in (None, 0):
        return None
    return (current / previous) - 1.0


def _safe_cagr(latest: float | None, oldest: float | None, periods: int) -> float | None:
    if latest is None or oldest in (None, 0) or periods <= 0:
        return None
    if latest <= 0 or oldest <= 0:
        return None
    return (latest / oldest) ** (1.0 / periods) - 1.0


def _trend_from_values(values: Sequence[float | None]) -> int | None:
    usable = [value for value in values if value is not None]
    if len(usable) < 2:
        return None
    if usable[0] < usable[-1]:
        return 1
    if usable[0] > usable[-1]:
        return -1
    return 0


def _frame_to_series(frame: pd.DataFrame | None, row_names: Sequence[str]) -> pd.Series:
    if frame is None or frame.empty:
        return pd.Series(dtype="float64")

    for row_name in row_names:
        if row_name not in frame.index:
            continue
        series = frame.loc[row_name]
        if isinstance(series, pd.DataFrame):
            series = series.iloc[0]
        series = pd.to_numeric(series, errors="coerce").dropna()
        if series.empty:
            continue
        series.index = pd.to_datetime(series.index)
        return series.sort_index()

    return pd.Series(dtype="float64")


def _extract_statement_metrics(
    frame: pd.DataFrame | None,
    row_map: Mapping[str, Sequence[str]],
) -> dict[str, pd.Series]:
    return {
        metric: _frame_to_series(frame, row_names)
        for metric, row_names in row_map.items()
    }


def _series_to_period_records(
    symbol: str,
    metric: str,
    statement: str,
    series: pd.Series,
    lookback_years: int,
) -> list[dict[str, Any]]:
    if series.empty:
        return []

    selected = series.sort_index().iloc[-lookback_years:]
    records: list[dict[str, Any]] = []
    for period_end, value in selected.items():
        numeric_value = _normalize_numeric(value)
        if numeric_value is None:
            continue
        records.append(
            {
                "symbol": symbol,
                "metric": metric,
                "statement": statement,
                "period_end": pd.Timestamp(period_end).date().isoformat(),
                "fiscal_year": int(pd.Timestamp(period_end).year),
                "value": numeric_value,
            }
        )
    return records


def build_sp500_universe(
    path: str | Path | None = None,
    *,
    refresh: bool = False,
) -> list[dict[str, str]]:
    _ = refresh
    ensure_sp500_data(path=path)
    rows = load_sp500_data(path)
    universe: list[dict[str, str]] = []
    for row in rows:
        raw_symbol = str(row.get("symbol") or "").strip().upper()
        normalized_symbol = normalize_sp500_symbol(raw_symbol)
        if not raw_symbol:
            continue
        universe.append(
            {
                "symbol": normalized_symbol,
                "source_symbol": raw_symbol,
                "security": str(row.get("security") or normalized_symbol),
                "gics_sector": str(row.get("gics_sector") or ""),
                "gics_sub_industry": str(row.get("gics_sub_industry") or ""),
            }
        )
    universe.sort(key=lambda row: row["symbol"])
    return universe


def _extract_info_fields(info: Mapping[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for info_key, output_key in INFO_FIELDS.items():
        value = info.get(info_key)
        if output_key in {"pe_ratio", "market_cap", "enterprise_value", "shares_outstanding"}:
            payload[output_key] = _normalize_numeric(value)
        elif output_key in {"roe_info", "net_margin_info"}:
            numeric = _normalize_numeric(value)
            payload[output_key] = numeric if numeric is None else numeric * 100.0
        else:
            payload[output_key] = value
    return payload


def _latest_close_from_history(history: pd.DataFrame | None) -> float | None:
    if history is None or history.empty or "Close" not in history.columns:
        return None
    close = pd.to_numeric(history["Close"], errors="coerce").dropna()
    if close.empty:
        return None
    return _normalize_numeric(close.iloc[-1])


def _build_row(
    company: Mapping[str, str],
    info: Mapping[str, Any],
    income_metrics: Mapping[str, pd.Series],
    cashflow_metrics: Mapping[str, pd.Series],
    balance_metrics: Mapping[str, pd.Series],
    history: pd.DataFrame | None,
    lookback_years: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    symbol = company["symbol"]
    row: dict[str, Any] = {
        "symbol": symbol,
        "source_symbol": company["source_symbol"],
        "security": company["security"],
        "gics_sector": company["gics_sector"],
        "gics_sub_industry": company["gics_sub_industry"],
    }
    row.update(_extract_info_fields(info))

    long_rows: list[dict[str, Any]] = []
    statements = (
        ("income_statement", income_metrics),
        ("cash_flow", cashflow_metrics),
        ("balance_sheet", balance_metrics),
    )
    for statement_name, metric_map in statements:
        for metric_name, series in metric_map.items():
            long_rows.extend(
                _series_to_period_records(
                    symbol,
                    metric_name,
                    statement_name,
                    series,
                    lookback_years,
                )
            )

    metric_series = {**income_metrics, **cashflow_metrics, **balance_metrics}
    for metric_name, series in metric_series.items():
        selected = series.sort_index().iloc[-lookback_years:] if not series.empty else series
        values = [_normalize_numeric(value) for value in selected.tolist()]
        years = [int(pd.Timestamp(index).year) for index in selected.index] if not selected.empty else []

        for offset in range(lookback_years):
            reverse_index = lookback_years - offset - 1
            row[f"{metric_name}_fy{offset}"] = values[reverse_index] if reverse_index < len(values) else None
            row[f"{metric_name}_year_fy{offset}"] = (
                years[reverse_index] if reverse_index < len(years) else None
            )

        latest = row.get(f"{metric_name}_fy0")
        previous = row.get(f"{metric_name}_fy1")
        oldest = row.get(f"{metric_name}_fy{lookback_years - 1}")
        row[f"{metric_name}_growth_latest"] = _safe_pct_change(latest, previous)
        row[f"{metric_name}_cagr"] = _safe_cagr(latest, oldest, lookback_years - 1)
        row[f"{metric_name}_trend"] = _trend_from_values(list(reversed(values)))

    latest_revenue = row.get("revenue_fy0")
    latest_net_income = row.get("net_income_fy0")
    latest_operating_cash_flow = row.get("operating_cash_flow_fy0")
    latest_free_cash_flow = row.get("free_cash_flow_fy0")
    latest_capex = row.get("capital_expenditure_fy0")
    latest_cash = row.get("cash_fy0")
    latest_debt = row.get("total_debt_fy0")
    latest_equity = row.get("total_equity_fy0")

    row["net_margin_latest"] = _safe_ratio(latest_net_income, latest_revenue)
    row["net_margin_latest_pct"] = (
        row["net_margin_latest"] * 100.0 if row["net_margin_latest"] is not None else None
    )
    row["roe_latest"] = _safe_ratio(latest_net_income, latest_equity)
    row["roe_latest_pct"] = row["roe_latest"] * 100.0 if row["roe_latest"] is not None else None
    row["debt_to_equity"] = _safe_ratio(latest_debt, latest_equity)
    row["cash_to_debt"] = _safe_ratio(latest_cash, latest_debt)
    row["operating_cash_flow_margin"] = _safe_ratio(latest_operating_cash_flow, latest_revenue)
    row["fcf_margin"] = _safe_ratio(latest_free_cash_flow, latest_revenue)
    row["capex_to_ocf"] = _safe_ratio(abs(latest_capex) if latest_capex is not None else None, latest_operating_cash_flow)
    row["latest_close"] = _latest_close_from_history(history)

    market_cap = row.get("market_cap")
    row["pfcf_ratio"] = _safe_ratio(market_cap, latest_free_cash_flow)
    row["profitable"] = bool(latest_net_income is not None and latest_net_income > 0)
    row["positive_fcf"] = bool(latest_free_cash_flow is not None and latest_free_cash_flow > 0)
    row["high_roe"] = bool(row["roe_latest_pct"] is not None and row["roe_latest_pct"] >= 15.0)
    previous_margin = _safe_ratio(row.get("net_income_fy1"), row.get("revenue_fy1"))
    row["margin_expanding"] = bool(
        row.get("net_margin_latest") is not None
        and previous_margin is not None
        and row["net_margin_latest"] > previous_margin
    )
    row["leverage_flag"] = bool(row["debt_to_equity"] is not None and row["debt_to_equity"] > 2.0)

    return row, long_rows


def _validate_row(row: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    if row.get("revenue_fy0") is not None and row["revenue_fy0"] <= 0:
        issues.append("revenue_non_positive")
    if row.get("total_equity_fy0") is not None and row["total_equity_fy0"] <= 0:
        issues.append("equity_non_positive")
    if row.get("pe_ratio") is not None and not (0 < row["pe_ratio"] < 500):
        issues.append("pe_ratio_outlier")
    if row.get("pfcf_ratio") is not None and not (0 < row["pfcf_ratio"] < 500):
        issues.append("pfcf_ratio_outlier")
    if row.get("debt_to_equity") is not None and row["debt_to_equity"] < 0:
        issues.append("debt_to_equity_negative")
    return issues


def _collect_missing_fields(row: dict[str, Any], fields: Sequence[str]) -> list[str]:
    return [field for field in fields if row.get(field) is None]


def _fetch_with_retry(symbol: str, config: PipelineConfig) -> tuple[Any, dict[str, Any], pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    yf = _import_yfinance()
    last_error: Exception | None = None
    for attempt in range(1, config.retry_attempts + 1):
        try:
            ticker = yf.Ticker(symbol)
            return (
                ticker,
                ticker.info,
                ticker.income_stmt,
                ticker.cashflow,
                ticker.balance_sheet,
                ticker.history(period=config.history_period, auto_adjust=True),
            )
        except Exception as exc:  # pragma: no cover - network/provider behavior
            last_error = exc
            LOGGER.warning(
                "Ticker fetch failed for %s on attempt %s/%s: %s",
                symbol,
                attempt,
                config.retry_attempts,
                exc,
            )
            if attempt < config.retry_attempts:
                time.sleep(config.retry_delay_seconds * attempt)
    assert last_error is not None
    raise last_error


def run_sp500_fundamentals_pipeline(config: PipelineConfig) -> PipelineArtifacts:
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    universe = build_sp500_universe(path=config.sp500_path, refresh=config.refresh_sp500)
    if config.max_tickers is not None:
        universe = universe[: config.max_tickers]

    flat_rows: list[dict[str, Any]] = []
    long_rows: list[dict[str, Any]] = []
    failed_symbols: list[str] = []
    missing_fields = Counter()
    validation_issues = Counter()
    critical_fields = (
        "revenue_fy0",
        "net_income_fy0",
        "free_cash_flow_fy0",
        "total_debt_fy0",
        "total_equity_fy0",
        "pe_ratio",
    )

    total_tickers = len(universe)
    for index, company in enumerate(universe, start=1):
        symbol = company["symbol"]
        try:
            print(f"Processing {index}/{total_tickers}: {symbol} - Fetching", flush=True)
            info, income_stmt, cashflow, balance_sheet, history = _fetch_with_retry(symbol, config)[1:]
            income_metrics = _extract_statement_metrics(income_stmt, INCOME_ROWS)
            cashflow_metrics = _extract_statement_metrics(cashflow, CASHFLOW_ROWS)
            balance_metrics = _extract_statement_metrics(balance_sheet, BALANCE_SHEET_ROWS)
            print(f"Processing {index}/{total_tickers}: {symbol} - Normalizing", flush=True)
            row, company_long_rows = _build_row(
                company,
                info,
                income_metrics,
                cashflow_metrics,
                balance_metrics,
                history,
                config.lookback_years,
            )
            issues = _validate_row(row)
            row["validation_issues"] = issues
            row["validation_issue_count"] = len(issues)
            row["missing_critical_fields"] = _collect_missing_fields(row, critical_fields)
            row["missing_critical_field_count"] = len(row["missing_critical_fields"])
            flat_rows.append(row)
            long_rows.extend(company_long_rows)
            missing_fields.update(row["missing_critical_fields"])
            validation_issues.update(issues)
        except Exception as exc:  # pragma: no cover - network/provider behavior
            failed_symbols.append(symbol)
            print(f"Failed: {symbol} ({exc})", flush=True)
            LOGGER.exception("Failed to process %s: %s", symbol, exc)
        if config.pause_between_tickers_seconds > 0:
            time.sleep(config.pause_between_tickers_seconds)

    print("Saving", flush=True)
    flat_table = pd.DataFrame(flat_rows).sort_values("symbol").reset_index(drop=True)
    long_table = pd.DataFrame(long_rows)
    if not long_table.empty:
        long_table = long_table.sort_values(["symbol", "metric", "period_end"]).reset_index(drop=True)

    completeness_pct = 0.0
    if not flat_table.empty:
        total_critical_cells = len(flat_table) * len(critical_fields)
        missing_critical_cells = sum(int(count) for count in missing_fields.values())
        completeness_pct = (1.0 - (missing_critical_cells / total_critical_cells)) * 100.0

    summary = PipelineRunSummary(
        total_tickers=len(universe),
        processed_tickers=len(flat_rows) + len(failed_symbols),
        successful_tickers=len(flat_rows),
        failed_tickers=len(failed_symbols),
        success_rate=((len(flat_rows) / len(universe)) * 100.0) if universe else 0.0,
        completeness_pct=completeness_pct,
        failed_symbols=sorted(failed_symbols),
        missing_field_counts=dict(sorted(missing_fields.items())),
        validation_issue_counts=dict(sorted(validation_issues.items())),
    )
    print(f"Completed: {summary.processed_tickers} processed", flush=True)
    print(f"Failed: {summary.failed_tickers}", flush=True)
    return PipelineArtifacts(flat_table=flat_table, long_table=long_table, summary=summary)


def _resolve_output_path(path: str | Path) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return Path.cwd() / candidate


def write_dataframe(frame: pd.DataFrame, path: str | Path) -> Path:
    target = _resolve_output_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    suffix = target.suffix.lower()
    if suffix == ".csv":
        frame.to_csv(target, index=False)
    elif suffix == ".parquet":
        frame.to_parquet(target, index=False)
    else:
        raise ValueError(f"Unsupported output format for {target}. Use .csv or .parquet.")
    return target


def write_summary(summary: PipelineRunSummary, path: str | Path) -> Path:
    target = _resolve_output_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(asdict(summary), indent=2) + "\n", encoding="utf-8")
    return target


def run_and_persist_sp500_fundamentals(config: PipelineConfig) -> dict[str, Any]:
    artifacts = run_sp500_fundamentals_pipeline(config)
    flat_path = write_dataframe(artifacts.flat_table, config.output_flat)
    long_path = None
    if config.output_long:
        long_path = write_dataframe(artifacts.long_table, config.output_long)
    summary_path = write_summary(artifacts.summary, config.output_summary)
    return {
        "flat_output": str(flat_path),
        "long_output": str(long_path) if long_path else None,
        "summary_output": str(summary_path),
        "summary": asdict(artifacts.summary),
    }
