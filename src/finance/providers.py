import json
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from finance.config import get_api_key


def fetch_alphavantage_overview(symbol: str) -> dict:
    query = urlencode(
        {
            "function": "OVERVIEW",
            "symbol": symbol,
            "apikey": get_api_key(),
        }
    )
    url = f"https://www.alphavantage.co/query?{query}"

    try:
        with urlopen(url, timeout=30) as response:
            return json.load(response)
    except HTTPError as exc:
        raise RuntimeError(f"Alpha Vantage request failed with HTTP {exc.code}") from exc
    except URLError as exc:
        raise RuntimeError(f"Alpha Vantage request failed: {exc.reason}") from exc


def fetch_yfinance_info(symbol: str) -> dict:
    try:
        import yfinance as yf
    except ImportError as exc:
        raise RuntimeError(
            "yfinance is not installed. Run: pip install -e '.[yfinance]'"
        ) from exc

    try:
        return yf.Ticker(symbol).info
    except Exception as exc:
        if "429" in str(exc) or "Too Many Requests" in str(exc):
            raise RuntimeError(
                "Yahoo Finance rate limited the request (HTTP 429). Try again later."
            ) from exc
        raise RuntimeError(f"Yahoo Finance request failed: {exc}") from exc


def fetch_yfinance_history(symbol: str, period: str = "max"):
    try:
        import yfinance as yf
    except ImportError as exc:
        raise RuntimeError(
            "yfinance is not installed. Run: pip install -e '.[yfinance]'"
        ) from exc

    try:
        return yf.Ticker(symbol).history(period=period, auto_adjust=False)
    except Exception as exc:
        if "429" in str(exc) or "Too Many Requests" in str(exc):
            raise RuntimeError(
                "Yahoo Finance rate limited the request (HTTP 429). Try again later."
            ) from exc
        raise RuntimeError(f"Yahoo Finance history request failed: {exc}") from exc
