import json
from pathlib import Path

import pytest

import finance.fundamental_migration as migration


def _write_sp500(path: Path, symbols: list[str]) -> None:
    rows = [{"symbol": symbol} for symbol in symbols]
    path.write_text(json.dumps(rows), encoding="utf-8")


def test_migration_processes_in_batches_and_resumes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sp500_path = tmp_path / "sp500_constituents.json"
    db_path = str(tmp_path / "fundamentals.duckdb")
    _write_sp500(sp500_path, ["AAA", "BBB", "CCC"])

    def fake_loader(_symbol: str, _api_key: str | None):
        return [{"row": 1}]

    def fake_upserter(rows, _database: str) -> int:
        return len(rows)

    monkeypatch.setattr(
        migration,
        "DATASET_HANDLERS",
        {"income_statement": (fake_loader, fake_upserter)},
    )

    first = migration.migrate_sp500_fundamentals(
        database=db_path,
        sp500_path=sp500_path,
        datasets=["income_statement"],
        max_requests=2,
        retry_delay_minutes=5,
        quota_retry_minutes=60,
    )
    assert first.symbol_count == 3
    assert first.processed_requests == 2
    assert first.completed_jobs == 2
    assert first.pending_jobs == 1

    second = migration.migrate_sp500_fundamentals(
        database=db_path,
        sp500_path=sp500_path,
        datasets=["income_statement"],
        max_requests=2,
        retry_delay_minutes=5,
        quota_retry_minutes=60,
    )
    assert second.processed_requests == 1
    assert second.completed_jobs == 3
    assert second.pending_jobs == 0


def test_migration_sets_retry_and_skips_until_due(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sp500_path = tmp_path / "sp500_constituents.json"
    db_path = str(tmp_path / "fundamentals.duckdb")
    _write_sp500(sp500_path, ["AAA"])

    def failing_loader(_symbol: str, _api_key: str | None):
        raise RuntimeError("temporary provider error")

    def fake_upserter(_rows, _database: str) -> int:
        return 0

    monkeypatch.setattr(
        migration,
        "DATASET_HANDLERS",
        {"income_statement": (failing_loader, fake_upserter)},
    )

    first = migration.migrate_sp500_fundamentals(
        database=db_path,
        sp500_path=sp500_path,
        datasets=["income_statement"],
        max_requests=1,
        retry_delay_minutes=15,
        quota_retry_minutes=60,
    )
    assert first.retry_jobs == 1
    assert first.processed_requests == 1
    assert first.pending_jobs == 1
    assert first.next_retry_ts is not None

    second = migration.migrate_sp500_fundamentals(
        database=db_path,
        sp500_path=sp500_path,
        datasets=["income_statement"],
        max_requests=1,
        retry_delay_minutes=15,
        quota_retry_minutes=60,
    )
    assert second.due_jobs == 0
    assert second.processed_requests == 0
    assert second.pending_jobs == 1


def test_migration_stops_run_when_quota_hit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sp500_path = tmp_path / "sp500_constituents.json"
    db_path = str(tmp_path / "fundamentals.duckdb")
    _write_sp500(sp500_path, ["AAA", "BBB"])

    def quota_loader(_symbol: str, _api_key: str | None):
        raise RuntimeError("quota reached for endpoint")

    def fake_upserter(_rows, _database: str) -> int:
        return 0

    monkeypatch.setattr(
        migration,
        "DATASET_HANDLERS",
        {"income_statement": (quota_loader, fake_upserter)},
    )

    summary = migration.migrate_sp500_fundamentals(
        database=db_path,
        sp500_path=sp500_path,
        datasets=["income_statement"],
        max_requests=5,
        retry_delay_minutes=15,
        quota_retry_minutes=1440,
    )
    assert summary.quota_hit is True
    assert summary.processed_requests == 1
    assert summary.retry_jobs == 1
    assert summary.pending_jobs == 2
