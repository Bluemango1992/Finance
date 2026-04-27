import json
from datetime import UTC, datetime
from typing import Any

from finance.providers import (
    fetch_alphavantage_balance_sheet,
    fetch_alphavantage_cash_flow,
    fetch_alphavantage_income_statement,
)


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if text in {"", "None", "null", "NULL", "-"}:
        return None
    try:
        return int(text.replace(",", ""))
    except ValueError:
        return None


def _build_income_statement_rows(
    payload: dict[str, Any],
    *,
    ingestion_ts: datetime | None = None,
) -> list[dict[str, Any]]:
    symbol = str(payload.get("symbol") or "").strip()
    if not symbol:
        raise RuntimeError("Alpha Vantage income statement payload is missing 'symbol'.")

    if ingestion_ts is None:
        ts = datetime.now(UTC)
    else:
        ts = ingestion_ts.astimezone(UTC)
    ts = ts.replace(tzinfo=None)

    rows: list[dict[str, Any]] = []
    report_sets = (
        ("annual", payload.get("annualReports")),
        ("quarterly", payload.get("quarterlyReports")),
    )
    for period_type, reports in report_sets:
        if not isinstance(reports, list):
            continue
        for item in reports:
            if not isinstance(item, dict):
                continue
            fiscal_date = item.get("fiscalDateEnding")
            if not fiscal_date:
                continue
            row = {
                "symbol": symbol,
                "period_type": period_type,
                "fiscal_date_ending": str(fiscal_date),
                "reported_currency": item.get("reportedCurrency"),
                "total_revenue": _to_int(item.get("totalRevenue")),
                "gross_profit": _to_int(item.get("grossProfit")),
                "operating_income": _to_int(item.get("operatingIncome")),
                "net_income": _to_int(item.get("netIncome")),
                "ebit": _to_int(item.get("ebit")),
                "ebitda": _to_int(item.get("ebitda")),
                "source": "alphavantage_income_statement",
                "ingestion_ts": ts,
                "raw_payload_json": json.dumps(item, separators=(",", ":"), sort_keys=True),
            }
            rows.append(row)
    return rows


def _validate_alphavantage_fundamental_payload(payload: dict[str, Any], *, endpoint_name: str) -> None:
    if "Note" in payload and "API call frequency" in str(payload["Note"]):
        raise RuntimeError(
            f"Alpha Vantage quota reached for {endpoint_name} endpoint. Try again later."
        )
    if "Information" in payload and "API call frequency" in str(payload["Information"]):
        raise RuntimeError(
            f"Alpha Vantage quota reached for {endpoint_name} endpoint. Try again later."
        )
    if "Error Message" in payload:
        raise RuntimeError(f"Alpha Vantage error: {payload['Error Message']}")


def _build_cash_flow_rows(
    payload: dict[str, Any],
    *,
    ingestion_ts: datetime | None = None,
) -> list[dict[str, Any]]:
    symbol = str(payload.get("symbol") or "").strip()
    if not symbol:
        raise RuntimeError("Alpha Vantage cash flow payload is missing 'symbol'.")

    if ingestion_ts is None:
        ts = datetime.now(UTC)
    else:
        ts = ingestion_ts.astimezone(UTC)
    ts = ts.replace(tzinfo=None)

    rows: list[dict[str, Any]] = []
    report_sets = (
        ("annual", payload.get("annualReports")),
        ("quarterly", payload.get("quarterlyReports")),
    )
    for period_type, reports in report_sets:
        if not isinstance(reports, list):
            continue
        for item in reports:
            if not isinstance(item, dict):
                continue
            fiscal_date = item.get("fiscalDateEnding")
            if not fiscal_date:
                continue
            operating_cashflow = _to_int(item.get("operatingCashflow"))
            capital_expenditures = _to_int(item.get("capitalExpenditures"))
            row = {
                "symbol": symbol,
                "period_type": period_type,
                "fiscal_date_ending": str(fiscal_date),
                "reported_currency": item.get("reportedCurrency"),
                "operating_cashflow": operating_cashflow,
                "cashflow_from_investment": _to_int(item.get("cashflowFromInvestment")),
                "cashflow_from_financing": _to_int(item.get("cashflowFromFinancing")),
                "net_income": _to_int(item.get("netIncome")),
                "capital_expenditures": capital_expenditures,
                "free_cash_flow": operating_cashflow - capital_expenditures
                if operating_cashflow is not None and capital_expenditures is not None
                else None,
                "source": "alphavantage_cash_flow",
                "ingestion_ts": ts,
                "raw_payload_json": json.dumps(item, separators=(",", ":"), sort_keys=True),
            }
            rows.append(row)
    return rows


def _build_balance_sheet_rows(
    payload: dict[str, Any],
    *,
    ingestion_ts: datetime | None = None,
) -> list[dict[str, Any]]:
    symbol = str(payload.get("symbol") or "").strip()
    if not symbol:
        raise RuntimeError("Alpha Vantage balance sheet payload is missing 'symbol'.")

    if ingestion_ts is None:
        ts = datetime.now(UTC)
    else:
        ts = ingestion_ts.astimezone(UTC)
    ts = ts.replace(tzinfo=None)

    rows: list[dict[str, Any]] = []
    report_sets = (
        ("annual", payload.get("annualReports")),
        ("quarterly", payload.get("quarterlyReports")),
    )
    for period_type, reports in report_sets:
        if not isinstance(reports, list):
            continue
        for item in reports:
            if not isinstance(item, dict):
                continue
            fiscal_date = item.get("fiscalDateEnding")
            if not fiscal_date:
                continue
            row = {
                "symbol": symbol,
                "period_type": period_type,
                "fiscal_date_ending": str(fiscal_date),
                "reported_currency": item.get("reportedCurrency"),
                "total_assets": _to_int(item.get("totalAssets")),
                "total_liabilities": _to_int(item.get("totalLiabilities")),
                "total_shareholder_equity": _to_int(item.get("totalShareholderEquity")),
                "cash_and_short_term_investments": _to_int(
                    item.get("cashAndShortTermInvestments")
                ),
                "current_debt": _to_int(item.get("currentDebt")),
                "long_term_debt": _to_int(item.get("longTermDebt")),
                "source": "alphavantage_balance_sheet",
                "ingestion_ts": ts,
                "raw_payload_json": json.dumps(item, separators=(",", ":"), sort_keys=True),
            }
            rows.append(row)
    return rows


def load_income_statement_rows_from_alphavantage(
    symbol: str,
    *,
    api_key: str | None = None,
    ingestion_ts: datetime | None = None,
) -> list[dict[str, Any]]:
    payload = fetch_alphavantage_income_statement(symbol, api_key=api_key)
    _validate_alphavantage_fundamental_payload(payload, endpoint_name="income statement")

    rows = _build_income_statement_rows(payload, ingestion_ts=ingestion_ts)
    if not rows:
        raise RuntimeError(
            f"No income statement rows returned for symbol '{symbol}'."
        )
    return rows


def load_cash_flow_rows_from_alphavantage(
    symbol: str,
    *,
    api_key: str | None = None,
    ingestion_ts: datetime | None = None,
) -> list[dict[str, Any]]:
    payload = fetch_alphavantage_cash_flow(symbol, api_key=api_key)
    _validate_alphavantage_fundamental_payload(payload, endpoint_name="cash flow")
    rows = _build_cash_flow_rows(payload, ingestion_ts=ingestion_ts)
    if not rows:
        raise RuntimeError(f"No cash flow rows returned for symbol '{symbol}'.")
    return rows


def load_balance_sheet_rows_from_alphavantage(
    symbol: str,
    *,
    api_key: str | None = None,
    ingestion_ts: datetime | None = None,
) -> list[dict[str, Any]]:
    payload = fetch_alphavantage_balance_sheet(symbol, api_key=api_key)
    _validate_alphavantage_fundamental_payload(payload, endpoint_name="balance sheet")
    rows = _build_balance_sheet_rows(payload, ingestion_ts=ingestion_ts)
    if not rows:
        raise RuntimeError(f"No balance sheet rows returned for symbol '{symbol}'.")
    return rows
