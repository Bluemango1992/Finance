import json
from unittest.mock import patch

from finance.cli import main, parse_args
from finance.fundamental_migration import MigrationSummary
from finance.scraper.sp500 import RefreshSummary


def test_parse_args_defaults_to_api_without_symbol() -> None:
    args = parse_args([])
    assert args.endpoint is None
    assert args.ingest_balance_sheet is False
    assert args.ingest_cash_flow is False
    assert args.ingest_income_statement is False
    assert args.migration_status is False
    assert args.migrate_sp500_fundamentals is False
    assert args.max_requests == 5
    assert args.retry_delay_minutes == 30
    assert args.quota_retry_minutes == 1440
    assert args.refresh_ftse250 is False
    assert args.refresh_nikkei225 is False
    assert args.refresh_sp500 is False
    assert args.provider is None
    assert args.symbol is None


def test_parse_args_accepts_yfinance_provider() -> None:
    args = parse_args(["--provider", "yfinance", "MSFT"])
    assert args.provider == "yfinance"
    assert args.symbol == "MSFT"


@patch("finance.cli.run_query", return_value=[{"ok": 1}])
def test_main_uses_duckdb_endpoint_when_requested(mock_query, capsys) -> None:
    with patch("sys.argv", ["finance", "--endpoint", "duckdb", "--sql", "select 1 as ok"]):
        main()

    assert json.loads(capsys.readouterr().out) == [{"ok": 1}]
    mock_query.assert_called_once_with("select 1 as ok", database=":memory:")


@patch("finance.cli.fetch_alphavantage_overview", return_value={"Symbol": "IBM"})
def test_main_uses_alphavantage_by_default(mock_fetch, capsys, monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ALPHAVANTAGE_API_KEY", raising=False)
    with patch("sys.argv", ["finance", "IBM"]):
        main()

    assert json.loads(capsys.readouterr().out) == {"Symbol": "IBM"}
    mock_fetch.assert_called_once_with("IBM", api_key=None)


@patch("finance.cli.fetch_yfinance_info", return_value={"symbol": "MSFT"})
def test_main_uses_yfinance_when_requested(mock_fetch, capsys) -> None:
    with patch("sys.argv", ["finance", "--provider", "yfinance", "MSFT"]):
        main()

    assert json.loads(capsys.readouterr().out) == {"symbol": "MSFT"}
    mock_fetch.assert_called_once_with("MSFT")


@patch(
    "finance.cli.load_income_statement_rows_from_alphavantage",
    return_value=[
        {
            "symbol": "MSFT",
            "period_type": "annual",
            "fiscal_date_ending": "2024-12-31",
            "reported_currency": "USD",
            "total_revenue": 1,
            "gross_profit": 1,
            "operating_income": 1,
            "net_income": 1,
            "ebit": 1,
            "ebitda": 1,
            "source": "alphavantage_income_statement",
            "ingestion_ts": "2026-01-01 00:00:00",
            "raw_payload_json": "{}",
        }
    ],
)
@patch("finance.cli.upsert_income_statement_rows", return_value=1)
def test_main_ingests_income_statements(mock_upsert, mock_load, capsys) -> None:
    with patch(
        "sys.argv",
        [
            "finance",
            "--ingest-income-statement",
            "--duckdb-database",
            "data/fundamentals.duckdb",
            "MSFT",
        ],
    ):
        main()

    output = json.loads(capsys.readouterr().out)
    assert output["symbol"] == "MSFT"
    assert output["rows_written"] == 1
    assert output["database"] == "data/fundamentals.duckdb"
    mock_load.assert_called_once_with("MSFT", api_key=None)
    mock_upsert.assert_called_once()


@patch(
    "finance.cli.load_cash_flow_rows_from_alphavantage",
    return_value=[
        {
            "symbol": "MSFT",
            "period_type": "annual",
            "fiscal_date_ending": "2024-12-31",
            "reported_currency": "USD",
            "operating_cashflow": 1,
            "cashflow_from_investment": 1,
            "cashflow_from_financing": 1,
            "net_income": 1,
            "capital_expenditures": 1,
            "free_cash_flow": 0,
            "source": "alphavantage_cash_flow",
            "ingestion_ts": "2026-01-01 00:00:00",
            "raw_payload_json": "{}",
        }
    ],
)
@patch("finance.cli.upsert_cash_flow_rows", return_value=1)
def test_main_ingests_cash_flow(mock_upsert, mock_load, capsys) -> None:
    with patch(
        "sys.argv",
        [
            "finance",
            "--ingest-cash-flow",
            "--duckdb-database",
            "data/fundamentals.duckdb",
            "MSFT",
        ],
    ):
        main()

    output = json.loads(capsys.readouterr().out)
    assert output["symbol"] == "MSFT"
    assert output["rows_written"] == 1
    assert output["database"] == "data/fundamentals.duckdb"
    mock_load.assert_called_once_with("MSFT", api_key=None)
    mock_upsert.assert_called_once()


@patch(
    "finance.cli.load_balance_sheet_rows_from_alphavantage",
    return_value=[
        {
            "symbol": "MSFT",
            "period_type": "annual",
            "fiscal_date_ending": "2024-12-31",
            "reported_currency": "USD",
            "total_assets": 1,
            "total_liabilities": 1,
            "total_shareholder_equity": 1,
            "cash_and_short_term_investments": 1,
            "current_debt": 1,
            "long_term_debt": 1,
            "source": "alphavantage_balance_sheet",
            "ingestion_ts": "2026-01-01 00:00:00",
            "raw_payload_json": "{}",
        }
    ],
)
@patch("finance.cli.upsert_balance_sheet_rows", return_value=1)
def test_main_ingests_balance_sheet(mock_upsert, mock_load, capsys) -> None:
    with patch(
        "sys.argv",
        [
            "finance",
            "--ingest-balance-sheet",
            "--duckdb-database",
            "data/fundamentals.duckdb",
            "MSFT",
        ],
    ):
        main()

    output = json.loads(capsys.readouterr().out)
    assert output["symbol"] == "MSFT"
    assert output["rows_written"] == 1
    assert output["database"] == "data/fundamentals.duckdb"
    mock_load.assert_called_once_with("MSFT", api_key=None)
    mock_upsert.assert_called_once()


@patch(
    "finance.cli.migrate_sp500_fundamentals",
    return_value=MigrationSummary(
        database="data/fundamentals.duckdb",
        sp500_source="data/sp500_constituents.json",
        dataset_count=3,
        symbol_count=503,
        seeded_jobs=1509,
        due_jobs=5,
        processed_requests=5,
        success_jobs=4,
        retry_jobs=1,
        quota_hit=False,
        pending_jobs=1505,
        completed_jobs=4,
        next_retry_ts="2026-04-28 10:00:00",
    ),
)
def test_main_migrates_sp500_fundamentals(mock_migrate, capsys) -> None:
    with patch(
        "sys.argv",
        [
            "finance",
            "--migrate-sp500-fundamentals",
            "--duckdb-database",
            "data/fundamentals.duckdb",
            "--sp500-input",
            "data/sp500_constituents.json",
        ],
    ):
        main()

    output = json.loads(capsys.readouterr().out)
    assert output["database"] == "data/fundamentals.duckdb"
    assert output["symbol_count"] == 503
    assert output["processed_requests"] == 5
    mock_migrate.assert_called_once()


def test_main_rejects_in_memory_db_for_migration(capsys) -> None:
    with patch("sys.argv", ["finance", "--migrate-sp500-fundamentals"]):
        try:
            main()
        except SystemExit as exc:
            assert exc.code == 2
        else:
            raise AssertionError("Expected SystemExit")

    assert "requires a persistent --duckdb-database path" in capsys.readouterr().err


@patch(
    "finance.cli.get_migration_status",
    return_value={
        "database": "data/fundamentals.duckdb",
        "total_jobs": 1509,
        "completed_jobs": 5,
        "pending_jobs": 1504,
        "status_counts": {"pending": 1504, "success": 5},
        "next_retry_ts": None,
        "table_counts": {
            "income_statements": 101,
            "cash_flow_statements": 202,
            "balance_sheets": 202,
        },
        "recent_failures": [],
    },
)
def test_main_shows_migration_status(mock_status, capsys) -> None:
    with patch(
        "sys.argv",
        [
            "finance",
            "--migration-status",
            "--duckdb-database",
            "data/fundamentals.duckdb",
        ],
    ):
        main()

    output = json.loads(capsys.readouterr().out)
    assert output["database"] == "data/fundamentals.duckdb"
    assert output["pending_jobs"] == 1504
    mock_status.assert_called_once_with("data/fundamentals.duckdb")


def test_main_rejects_in_memory_db_for_migration_status(capsys) -> None:
    with patch("sys.argv", ["finance", "--migration-status"]):
        try:
            main()
        except SystemExit as exc:
            assert exc.code == 2
        else:
            raise AssertionError("Expected SystemExit")

    assert "requires a persistent --duckdb-database path" in capsys.readouterr().err


@patch(
    "finance.cli.refresh_sp500_data_safe",
    return_value=RefreshSummary(
        output_path="data/sp500_constituents.json",
        refreshed=False,
        rows=503,
        source="cache",
    ),
)
def test_main_refreshes_sp500_snapshot(mock_refresh, capsys) -> None:
    with patch("sys.argv", ["finance", "--refresh-sp500"]):
        main()

    assert json.loads(capsys.readouterr().out)["output_path"] == "data/sp500_constituents.json"
    mock_refresh.assert_called_once_with(path="data/sp500_constituents.json")


@patch(
    "finance.cli.refresh_ftse250_data_safe",
    return_value=RefreshSummary(
        output_path="data/ftse250_constituents.json",
        refreshed=False,
        rows=250,
        source="cache",
    ),
)
def test_main_refreshes_ftse250_snapshot(mock_refresh, capsys) -> None:
    with patch("sys.argv", ["finance", "--refresh-ftse250"]):
        main()

    assert json.loads(capsys.readouterr().out)["output_path"] == "data/ftse250_constituents.json"
    mock_refresh.assert_called_once_with(path="data/ftse250_constituents.json")


@patch(
    "finance.cli.refresh_nikkei225_data_safe",
    return_value=RefreshSummary(
        output_path="data/nikkei225_components.json",
        refreshed=False,
        rows=225,
        source="cache",
    ),
)
def test_main_refreshes_nikkei225_snapshot(mock_refresh, capsys) -> None:
    with patch("sys.argv", ["finance", "--refresh-nikkei225"]):
        main()

    assert json.loads(capsys.readouterr().out)["output_path"] == "data/nikkei225_components.json"
    mock_refresh.assert_called_once_with(path="data/nikkei225_components.json")
