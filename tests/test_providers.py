from urllib.error import HTTPError
from unittest.mock import patch

import pytest

from finance.providers import fetch_alphavantage_overview, fetch_yfinance_info


def test_fetch_yfinance_info_normalizes_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeTicker:
        @property
        def info(self):
            return {"symbol": "MSFT", "employees": 1000, "flag": True, "none_field": None}

    class FakeYF:
        @staticmethod
        def Ticker(_symbol: str):
            return FakeTicker()

    monkeypatch.setattr("finance.providers._import_yfinance", lambda: FakeYF())
    payload = fetch_yfinance_info("MSFT")
    assert payload == {
        "symbol": "MSFT",
        "employees": 1000,
        "flag": True,
        "none_field": None,
    }


def test_fetch_yfinance_info_raises_rate_limit_on_response_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RateLimitError(Exception):
        def __init__(self):
            self.response = type("Response", (), {"status_code": 429})()

    class FakeTicker:
        @property
        def info(self):
            raise RateLimitError()

    class FakeYF:
        @staticmethod
        def Ticker(_symbol: str):
            return FakeTicker()

    monkeypatch.setattr("finance.providers._import_yfinance", lambda: FakeYF())
    with pytest.raises(RuntimeError, match="rate limited"):
        fetch_yfinance_info("MSFT")


def test_fetch_alphavantage_overview_raises_rate_limit_on_http_429() -> None:
    http_error = HTTPError(
        "https://example.test",
        429,
        "Too Many Requests",
        hdrs=None,
        fp=None,
    )
    with patch("finance.providers.urlopen", side_effect=http_error):
        with pytest.raises(RuntimeError, match="rate limited"):
            fetch_alphavantage_overview("IBM", api_key="demo-key")
