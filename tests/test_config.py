import pytest

from finance.config import build_settings, get_api_key


def test_get_api_key_reads_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALPHAVANTAGE_API_KEY", "demo-key")
    assert get_api_key() == "demo-key"


def test_get_api_key_raises_when_missing(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.delenv("ALPHAVANTAGE_API_KEY", raising=False)
    monkeypatch.chdir(tmp_path)

    with pytest.raises(RuntimeError, match="Missing ALPHAVANTAGE_API_KEY"):
        get_api_key()


def test_build_settings_uses_defaults_when_no_overrides(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("FINANCE_ENDPOINT", raising=False)
    monkeypatch.delenv("FINANCE_PROVIDER", raising=False)
    monkeypatch.delenv("FINANCE_DUCKDB_DATABASE", raising=False)
    monkeypatch.delenv("FINANCE_SQL", raising=False)
    monkeypatch.delenv("ALPHAVANTAGE_API_KEY", raising=False)
    settings = build_settings()
    assert settings.endpoint == "api"
    assert settings.provider == "alphavantage"
    assert settings.duckdb_database == ":memory:"
    assert settings.sql is None
    assert settings.alphavantage_api_key is None


def test_build_settings_cli_overrides_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FINANCE_ENDPOINT", "api")
    monkeypatch.setenv("FINANCE_PROVIDER", "alphavantage")
    monkeypatch.setenv("FINANCE_DUCKDB_DATABASE", "env.duckdb")
    monkeypatch.setenv("FINANCE_SQL", "select 'env'")
    monkeypatch.setenv("ALPHAVANTAGE_API_KEY", "env-key")
    settings = build_settings(
        endpoint="duckdb",
        provider="yfinance",
        duckdb_database="cli.duckdb",
        sql="select 'cli'",
        alphavantage_api_key="cli-key",
    )
    assert settings.endpoint == "duckdb"
    assert settings.provider == "yfinance"
    assert settings.duckdb_database == "cli.duckdb"
    assert settings.sql == "select 'cli'"
    assert settings.alphavantage_api_key == "cli-key"
