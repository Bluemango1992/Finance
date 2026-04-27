import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

Endpoint = Literal["api", "duckdb"]
Provider = Literal["alphavantage", "yfinance", "fred"]

DEFAULT_ENDPOINT: Endpoint = "api"
DEFAULT_PROVIDER: Provider = "alphavantage"
DEFAULT_DUCKDB_DATABASE = ":memory:"


def load_env_file() -> None:
    env_path = Path.cwd() / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


@dataclass(frozen=True)
class RuntimeSettings:
    endpoint: Endpoint
    provider: Provider
    duckdb_database: str
    sql: str | None
    alphavantage_api_key: str | None
    fred_api_key: str | None


def build_settings(
    *,
    endpoint: str | None = None,
    provider: str | None = None,
    duckdb_database: str | None = None,
    sql: str | None = None,
    alphavantage_api_key: str | None = None,
    fred_api_key: str | None = None,
) -> RuntimeSettings:
    load_env_file()
    resolved_endpoint = endpoint or os.getenv("FINANCE_ENDPOINT") or DEFAULT_ENDPOINT
    resolved_provider = provider or os.getenv("FINANCE_PROVIDER") or DEFAULT_PROVIDER
    resolved_duckdb_database = (
        duckdb_database or os.getenv("FINANCE_DUCKDB_DATABASE") or DEFAULT_DUCKDB_DATABASE
    )
    resolved_sql = sql or os.getenv("FINANCE_SQL")
    resolved_api_key = alphavantage_api_key or os.getenv("ALPHAVANTAGE_API_KEY")
    resolved_fred_api_key = fred_api_key or os.getenv("FRED_API_KEY")

    if resolved_endpoint not in {"api", "duckdb"}:
        raise RuntimeError(f"Invalid endpoint: {resolved_endpoint}")
    if resolved_provider not in {"alphavantage", "yfinance", "fred"}:
        raise RuntimeError(f"Invalid provider: {resolved_provider}")

    return RuntimeSettings(
        endpoint=resolved_endpoint,
        provider=resolved_provider,
        duckdb_database=resolved_duckdb_database,
        sql=resolved_sql,
        alphavantage_api_key=resolved_api_key,
        fred_api_key=resolved_fred_api_key,
    )


def get_required_alphavantage_api_key(settings: RuntimeSettings) -> str:
    api_key = settings.alphavantage_api_key
    if not api_key:
        raise RuntimeError(
            "Missing ALPHAVANTAGE_API_KEY. Add it to your environment or .env file."
        )
    return api_key


def get_api_key() -> str:
    """Backward-compatible helper for existing call sites."""
    return get_required_alphavantage_api_key(build_settings())
