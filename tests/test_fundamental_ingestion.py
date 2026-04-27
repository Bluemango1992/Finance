import pytest

from finance.fundamental_ingestion import (
    _build_balance_sheet_rows,
    _build_cash_flow_rows,
    _build_income_statement_rows,
    load_balance_sheet_rows_from_alphavantage,
    load_cash_flow_rows_from_alphavantage,
    load_income_statement_rows_from_alphavantage,
)


def test_build_income_statement_rows_parses_annual_and_quarterly() -> None:
    payload = {
        "symbol": "MSFT",
        "annualReports": [
            {
                "fiscalDateEnding": "2024-06-30",
                "reportedCurrency": "USD",
                "totalRevenue": "1000",
                "grossProfit": "500",
                "operatingIncome": "250",
                "netIncome": "200",
                "ebit": "260",
                "ebitda": "300",
            }
        ],
        "quarterlyReports": [
            {
                "fiscalDateEnding": "2024-12-31",
                "reportedCurrency": "USD",
                "totalRevenue": "300",
                "grossProfit": "150",
                "operatingIncome": "60",
                "netIncome": "40",
                "ebit": "65",
                "ebitda": "70",
            }
        ],
    }
    rows = _build_income_statement_rows(payload)
    assert len(rows) == 2
    assert rows[0]["period_type"] == "annual"
    assert rows[1]["period_type"] == "quarterly"
    assert rows[0]["total_revenue"] == 1000
    assert rows[1]["ebitda"] == 70


def test_load_income_statement_rows_raises_on_quota(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "finance.fundamental_ingestion.fetch_alphavantage_income_statement",
        lambda _symbol, api_key=None: {"Note": "Thank you for using Alpha Vantage! API call frequency"},
    )
    with pytest.raises(RuntimeError, match="quota reached"):
        load_income_statement_rows_from_alphavantage("IBM")


def test_build_cash_flow_rows_parses_and_calculates_free_cash_flow() -> None:
    payload = {
        "symbol": "MSFT",
        "annualReports": [
            {
                "fiscalDateEnding": "2024-06-30",
                "reportedCurrency": "USD",
                "operatingCashflow": "1200",
                "cashflowFromInvestment": "-300",
                "cashflowFromFinancing": "-200",
                "netIncome": "900",
                "capitalExpenditures": "250",
            }
        ],
        "quarterlyReports": [],
    }
    rows = _build_cash_flow_rows(payload)
    assert len(rows) == 1
    assert rows[0]["operating_cashflow"] == 1200
    assert rows[0]["capital_expenditures"] == 250
    assert rows[0]["free_cash_flow"] == 950


def test_build_balance_sheet_rows_parses_annual_and_quarterly() -> None:
    payload = {
        "symbol": "MSFT",
        "annualReports": [
            {
                "fiscalDateEnding": "2024-06-30",
                "reportedCurrency": "USD",
                "totalAssets": "5000",
                "totalLiabilities": "2000",
                "totalShareholderEquity": "3000",
                "cashAndShortTermInvestments": "400",
                "currentDebt": "100",
                "longTermDebt": "600",
            }
        ],
        "quarterlyReports": [
            {
                "fiscalDateEnding": "2024-12-31",
                "reportedCurrency": "USD",
                "totalAssets": "5100",
                "totalLiabilities": "2050",
                "totalShareholderEquity": "3050",
                "cashAndShortTermInvestments": "410",
                "currentDebt": "110",
                "longTermDebt": "610",
            }
        ],
    }
    rows = _build_balance_sheet_rows(payload)
    assert len(rows) == 2
    assert rows[0]["period_type"] == "annual"
    assert rows[1]["period_type"] == "quarterly"
    assert rows[0]["total_assets"] == 5000
    assert rows[1]["long_term_debt"] == 610


def test_load_cash_flow_rows_raises_on_quota(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "finance.fundamental_ingestion.fetch_alphavantage_cash_flow",
        lambda _symbol, api_key=None: {"Note": "Thank you for using Alpha Vantage! API call frequency"},
    )
    with pytest.raises(RuntimeError, match="quota reached"):
        load_cash_flow_rows_from_alphavantage("IBM")


def test_load_balance_sheet_rows_raises_on_quota(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "finance.fundamental_ingestion.fetch_alphavantage_balance_sheet",
        lambda _symbol, api_key=None: {"Note": "Thank you for using Alpha Vantage! API call frequency"},
    )
    with pytest.raises(RuntimeError, match="quota reached"):
        load_balance_sheet_rows_from_alphavantage("IBM")
