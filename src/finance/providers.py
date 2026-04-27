import json
from collections.abc import Mapping
from typing import Any
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from finance.config import build_settings, get_required_alphavantage_api_key


def _import_yfinance():
    try:
        import yfinance as yf
    except ImportError as exc:
        raise RuntimeError(
            "yfinance is not installed. Run: pip install -e '.[yfinance]'"
        ) from exc
    return yf


def configure_yfinance_cache(cache_dir: str | Path) -> None:
    """Optional explicit cache configuration for yfinance timezone cache."""
    yf = _import_yfinance()
    path = Path(cache_dir)
    path.mkdir(parents=True, exist_ok=True)
    yf.set_tz_cache_location(str(path.resolve()))


def _is_rate_limited_error(exc: Exception) -> bool:
    if isinstance(exc, HTTPError):
        return exc.code == 429
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    if status_code == 429:
        return True
    return False


def _normalize_value(value: Any) -> str | int | float | bool | None:
    if value is None:
        return None
    if isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, Path):
        return str(value)
    return str(value)


def _normalize_mapping(payload: Mapping[Any, Any]) -> dict[str, str | int | float | bool | None]:
    normalized: dict[str, str | int | float | bool | None] = {}
    for key, value in payload.items():
        normalized[str(key)] = _normalize_value(value)
    return normalized


def _fetch_alphavantage_mapping(
    *, function_name: str, symbol: str, api_key: str | None = None
) -> Mapping[Any, Any]:
    settings = build_settings(alphavantage_api_key=api_key)
    query = urlencode(
        {
            "function": function_name,
            "symbol": symbol,
            "apikey": get_required_alphavantage_api_key(settings),
        }
    )
    url = f"https://www.alphavantage.co/query?{query}"

    try:
        with urlopen(url, timeout=30) as response:
            payload = json.load(response)
            if not isinstance(payload, Mapping):
                raise RuntimeError("Alpha Vantage response was not a JSON object.")
            return payload
    except HTTPError as exc:
        if _is_rate_limited_error(exc):
            raise RuntimeError(
                "Alpha Vantage rate limited the request (HTTP 429). Try again later."
            ) from exc
        raise RuntimeError(f"Alpha Vantage request failed with HTTP {exc.code}") from exc
    except URLError as exc:
        raise RuntimeError(f"Alpha Vantage request failed: {exc.reason}") from exc


def fetch_alphavantage_overview(
    symbol: str, api_key: str | None = None
) -> dict[str, str | int | float | bool | None]:
    payload = _fetch_alphavantage_mapping(function_name="OVERVIEW", symbol=symbol, api_key=api_key)
    return _normalize_mapping(payload)


def fetch_alphavantage_income_statement(symbol: str, api_key: str | None = None) -> dict[str, Any]:
    payload = _fetch_alphavantage_mapping(
        function_name="INCOME_STATEMENT",
        symbol=symbol,
        api_key=api_key,
    )
    return dict(payload)


def fetch_alphavantage_cash_flow(symbol: str, api_key: str | None = None) -> dict[str, Any]:
    payload = _fetch_alphavantage_mapping(
        function_name="CASH_FLOW",
        symbol=symbol,
        api_key=api_key,
    )
    return dict(payload)


def fetch_alphavantage_balance_sheet(symbol: str, api_key: str | None = None) -> dict[str, Any]:
    payload = _fetch_alphavantage_mapping(
        function_name="BALANCE_SHEET",
        symbol=symbol,
        api_key=api_key,
    )
    return dict(payload)


def fetch_yfinance_info(symbol: str) -> dict[str, str | int | float | bool | None]:
    yf = _import_yfinance()

    try:
        payload = yf.Ticker(symbol).info
        if not isinstance(payload, Mapping):
            raise RuntimeError("Yahoo Finance info response was not a mapping.")
        return _normalize_mapping(payload)
    except Exception as exc:
        if _is_rate_limited_error(exc):
            raise RuntimeError(
                "Yahoo Finance rate limited the request (HTTP 429). Try again later."
            ) from exc
        raise RuntimeError(f"Yahoo Finance request failed: {exc}") from exc


def fetch_yfinance_history(symbol: str, period: str = "max"):
    yf = _import_yfinance()

    try:
        return yf.Ticker(symbol).history(period=period, auto_adjust=False)
    except Exception as exc:
        if _is_rate_limited_error(exc):
            raise RuntimeError(
                "Yahoo Finance rate limited the request (HTTP 429). Try again later."
            ) from exc
        raise RuntimeError(f"Yahoo Finance history request failed: {exc}") from exc
