import pytest

from finance.config import get_api_key


def test_get_api_key_reads_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALPHAVANTAGE_API_KEY", "demo-key")
    assert get_api_key() == "demo-key"


def test_get_api_key_raises_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ALPHAVANTAGE_API_KEY", raising=False)

    with pytest.raises(RuntimeError, match="Missing ALPHAVANTAGE_API_KEY"):
        get_api_key()
