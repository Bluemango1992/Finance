from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Callable

from finance.db import (
    ensure_schema,
    upsert_balance_sheet_rows,
    upsert_cash_flow_rows,
    upsert_income_statement_rows,
)
from finance.fundamental_ingestion import (
    load_balance_sheet_rows_from_alphavantage,
    load_cash_flow_rows_from_alphavantage,
    load_income_statement_rows_from_alphavantage,
)
from finance.scraper.sp500 import load_sp500_data

DatasetLoader = Callable[[str], list[dict]]
DatasetUpserter = Callable[[list[dict], str], int]


@dataclass(frozen=True)
class MigrationSummary:
    database: str
    sp500_source: str
    dataset_count: int
    symbol_count: int
    seeded_jobs: int
    due_jobs: int
    processed_requests: int
    success_jobs: int
    retry_jobs: int
    quota_hit: bool
    pending_jobs: int
    completed_jobs: int
    next_retry_ts: str | None


DATASET_HANDLERS: dict[str, tuple[DatasetLoader, DatasetUpserter]] = {
    "income_statement": (load_income_statement_rows_from_alphavantage, upsert_income_statement_rows),
    "cash_flow": (load_cash_flow_rows_from_alphavantage, upsert_cash_flow_rows),
    "balance_sheet": (load_balance_sheet_rows_from_alphavantage, upsert_balance_sheet_rows),
}


def _connect(database: str):
    import duckdb

    return duckdb.connect(database=database)


def _utc_now_naive() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _normalize_symbols(path: str | Path) -> list[str]:
    rows = load_sp500_data(path)
    symbols: set[str] = set()
    for row in rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        symbols.add(symbol.replace(".", "-"))
    return sorted(symbols)


def _is_quota_error(message: str) -> bool:
    text = message.lower()
    return "quota reached" in text or "rate limited" in text or "api call frequency" in text


def _seed_jobs(
    *,
    database: str,
    symbols: list[str],
    datasets: list[str],
) -> int:
    if not symbols or not datasets:
        return 0

    now = _utc_now_naive()
    values: list[tuple] = []
    for symbol in symbols:
        for dataset in datasets:
            values.append(
                (
                    symbol,
                    dataset,
                    "pending",
                    0,
                    0,
                    None,
                    None,
                    None,
                    now,
                    now,
                )
            )
    sql = """
        INSERT OR IGNORE INTO fundamental_migration_progress (
            symbol,
            dataset,
            status,
            attempts,
            rows_written,
            last_error,
            last_attempt_ts,
            next_retry_ts,
            created_ts,
            updated_ts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    with _connect(database) as connection:
        before = connection.execute("SELECT COUNT(*) FROM fundamental_migration_progress").fetchone()[0]
        connection.executemany(sql, values)
        after = connection.execute("SELECT COUNT(*) FROM fundamental_migration_progress").fetchone()[0]
    return int(after - before)


def _load_due_jobs(database: str, *, limit: int) -> list[dict[str, object]]:
    now = _utc_now_naive()
    sql = """
        SELECT symbol, dataset, attempts
        FROM fundamental_migration_progress
        WHERE status <> 'success'
          AND (next_retry_ts IS NULL OR next_retry_ts <= ?)
        ORDER BY COALESCE(next_retry_ts, TIMESTAMP '1970-01-01'), attempts, symbol, dataset
        LIMIT ?
    """
    with _connect(database) as connection:
        rows = connection.execute(sql, [now, limit]).fetchall()
    return [
        {
            "symbol": row[0],
            "dataset": row[1],
            "attempts": int(row[2]),
        }
        for row in rows
    ]


def _mark_success(
    *,
    database: str,
    symbol: str,
    dataset: str,
    rows_written: int,
) -> None:
    now = _utc_now_naive()
    sql = """
        UPDATE fundamental_migration_progress
        SET status='success',
            attempts=attempts + 1,
            rows_written=?,
            last_error=NULL,
            last_attempt_ts=?,
            next_retry_ts=NULL,
            updated_ts=?
        WHERE symbol=? AND dataset=?
    """
    with _connect(database) as connection:
        connection.execute(sql, [rows_written, now, now, symbol, dataset])


def _mark_retry(
    *,
    database: str,
    symbol: str,
    dataset: str,
    error: str,
    next_retry_ts: datetime,
) -> None:
    now = _utc_now_naive()
    sql = """
        UPDATE fundamental_migration_progress
        SET status='retry',
            attempts=attempts + 1,
            last_error=?,
            last_attempt_ts=?,
            next_retry_ts=?,
            updated_ts=?
        WHERE symbol=? AND dataset=?
    """
    with _connect(database) as connection:
        connection.execute(sql, [error, now, next_retry_ts, now, symbol, dataset])


def _status_counts(database: str) -> dict[str, int]:
    sql = """
        SELECT status, COUNT(*)
        FROM fundamental_migration_progress
        GROUP BY status
    """
    counts: dict[str, int] = {"success": 0, "pending": 0, "retry": 0}
    with _connect(database) as connection:
        rows = connection.execute(sql).fetchall()
    for status, count in rows:
        counts[str(status)] = int(count)
    return counts


def _next_retry_ts(database: str) -> str | None:
    sql = """
        SELECT MIN(next_retry_ts)
        FROM fundamental_migration_progress
        WHERE status='retry' AND next_retry_ts IS NOT NULL
    """
    with _connect(database) as connection:
        row = connection.execute(sql).fetchone()
    value = row[0]
    if value is None:
        return None
    return value.isoformat(sep=" ")


def _validate_datasets(datasets: list[str]) -> list[str]:
    normalized = [item.strip().lower() for item in datasets if item.strip()]
    if not normalized:
        raise RuntimeError("At least one dataset is required for migration.")
    invalid = [item for item in normalized if item not in DATASET_HANDLERS]
    if invalid:
        raise RuntimeError(
            "Unsupported dataset(s): "
            + ", ".join(sorted(set(invalid)))
            + ". Allowed: income_statement, cash_flow, balance_sheet."
        )
    deduped: list[str] = []
    seen: set[str] = set()
    for item in normalized:
        if item in seen:
            continue
        deduped.append(item)
        seen.add(item)
    return deduped


def migrate_sp500_fundamentals(
    *,
    database: str,
    sp500_path: str | Path = "data/sp500_constituents.json",
    datasets: list[str] | None = None,
    api_key: str | None = None,
    max_requests: int = 5,
    retry_delay_minutes: int = 30,
    quota_retry_minutes: int = 1440,
) -> MigrationSummary:
    if max_requests <= 0:
        raise RuntimeError("max_requests must be greater than 0.")
    if retry_delay_minutes <= 0:
        raise RuntimeError("retry_delay_minutes must be greater than 0.")
    if quota_retry_minutes <= 0:
        raise RuntimeError("quota_retry_minutes must be greater than 0.")

    chosen_datasets = _validate_datasets(
        datasets or ["income_statement", "cash_flow", "balance_sheet"]
    )
    ensure_schema(database)

    symbols = _normalize_symbols(sp500_path)
    if not symbols:
        raise RuntimeError(f"No symbols found in S&P 500 file: {sp500_path}")

    seeded = _seed_jobs(database=database, symbols=symbols, datasets=chosen_datasets)
    due_jobs = _load_due_jobs(database, limit=max_requests)

    processed_requests = 0
    success_jobs = 0
    retry_jobs = 0
    quota_hit = False

    for job in due_jobs:
        symbol = str(job["symbol"])
        dataset = str(job["dataset"])
        attempts = int(job["attempts"])
        loader, upserter = DATASET_HANDLERS[dataset]
        processed_requests += 1
        try:
            rows = loader(symbol, api_key=api_key)
            rows_written = upserter(rows, database)
            _mark_success(
                database=database,
                symbol=symbol,
                dataset=dataset,
                rows_written=rows_written,
            )
            success_jobs += 1
        except RuntimeError as exc:
            message = str(exc)
            if _is_quota_error(message):
                delay_minutes = quota_retry_minutes
                quota_hit = True
            else:
                exponent = min(attempts, 4)
                delay_minutes = retry_delay_minutes * (2**exponent)

            next_retry_ts = _utc_now_naive() + timedelta(minutes=delay_minutes)
            _mark_retry(
                database=database,
                symbol=symbol,
                dataset=dataset,
                error=message,
                next_retry_ts=next_retry_ts,
            )
            retry_jobs += 1
            if quota_hit:
                break

    counts = _status_counts(database)
    completed = counts.get("success", 0)
    pending = counts.get("pending", 0) + counts.get("retry", 0)

    return MigrationSummary(
        database=database,
        sp500_source=str(Path(sp500_path)),
        dataset_count=len(chosen_datasets),
        symbol_count=len(symbols),
        seeded_jobs=seeded,
        due_jobs=len(due_jobs),
        processed_requests=processed_requests,
        success_jobs=success_jobs,
        retry_jobs=retry_jobs,
        quota_hit=quota_hit,
        pending_jobs=pending,
        completed_jobs=completed,
        next_retry_ts=_next_retry_ts(database),
    )
