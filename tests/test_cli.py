import json
from unittest.mock import patch

from finance.cli import main, parse_args


def test_parse_args_defaults_to_api_without_symbol() -> None:
    args = parse_args([])
    assert args.endpoint is None
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
