import json
from unittest.mock import patch

from finance.cli import main, parse_args
from finance.scraper.sp500 import RefreshSummary


def test_parse_args_defaults_to_alphavantage_ibm() -> None:
    args = parse_args([])
    assert args.endpoint == "api"
    assert args.ingest_spy is False
    assert args.refresh_ftse250 is False
    assert args.refresh_nikkei225 is False
    assert args.refresh_sp500 is False
    assert args.provider == "alphavantage"
    assert args.symbol == "IBM"


def test_parse_args_accepts_yfinance_provider() -> None:
    args = parse_args(["--provider", "yfinance", "MSFT"])
    assert args.provider == "yfinance"
    assert args.symbol == "MSFT"


@patch("finance.cli.run_query", return_value=[{"ok": 1}])
def test_main_uses_duckdb_endpoint_when_requested(mock_query, capsys) -> None:
    with patch("sys.argv", ["finance", "--endpoint", "duckdb"]):
        main()

    assert json.loads(capsys.readouterr().out) == [{"ok": 1}]
    mock_query.assert_called_once_with("select 1 as ok", database=":memory:")


@patch(
    "finance.cli.ingest_spy_prices",
    return_value={
        "rows_fetched": 10,
        "rows_valid": 10,
        "rows_invalid": 0,
        "rows_inserted": 10,
        "rows_duplicates": 0,
    },
)
def test_main_runs_spy_ingestion(mock_ingest, capsys) -> None:
    with patch("sys.argv", ["finance", "--ingest-spy", "--duckdb-database", "data/prices.duckdb"]):
        main()

    assert json.loads(capsys.readouterr().out)["rows_inserted"] == 10
    mock_ingest.assert_called_once_with(database="data/prices.duckdb")


@patch("finance.cli.fetch_alphavantage_overview", return_value={"Symbol": "IBM"})
def test_main_uses_alphavantage_by_default(mock_fetch, capsys) -> None:
    with patch("sys.argv", ["finance"]):
        main()

    assert json.loads(capsys.readouterr().out) == {"Symbol": "IBM"}
    mock_fetch.assert_called_once_with("IBM")


@patch("finance.cli.fetch_yfinance_info", return_value={"symbol": "MSFT"})
def test_main_uses_yfinance_when_requested(mock_fetch, capsys) -> None:
    with patch("sys.argv", ["finance", "--provider", "yfinance", "MSFT"]):
        main()

    assert json.loads(capsys.readouterr().out) == {"symbol": "MSFT"}
    mock_fetch.assert_called_once_with("MSFT")


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
