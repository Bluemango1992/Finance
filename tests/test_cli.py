import json
from unittest.mock import patch

from finance.cli import main, parse_args
from finance.scraper.sp500 import RefreshSummary


def test_parse_args_defaults_to_api_without_symbol() -> None:
    args = parse_args([])
    assert args.endpoint is None
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
