from pathlib import Path
from typing import Any

SCHEMA_SQL_PATH = Path(__file__).resolve().parent / "data" / "schema.sql"


def _connect(database: str):
    import duckdb

    return duckdb.connect(database=database)


def _load_schema_sql() -> str:
    return SCHEMA_SQL_PATH.read_text(encoding="utf-8")


def ensure_schema(database: str) -> None:
    db_path = Path(database)
    if database != ":memory:":
        db_path.parent.mkdir(parents=True, exist_ok=True)

    with _connect(database) as connection:
        connection.execute(_load_schema_sql())


def ensure_prices_table(database: str) -> None:
    """Backward-compatible wrapper. Prefer ensure_schema()."""
    ensure_schema(database)


def run_query(query: str, database: str = ":memory:") -> list[dict[str, Any]]:
    with _connect(database) as connection:
        result = connection.execute(query)
        columns = [column[0] for column in result.description]
        rows = result.fetchall()
    return [dict(zip(columns, row)) for row in rows]


def upsert_income_statement_rows(rows: list[dict[str, Any]], database: str) -> int:
    if not rows:
        return 0

    ensure_schema(database)
    sql = """
        INSERT OR REPLACE INTO income_statements (
            symbol,
            period_type,
            fiscal_date_ending,
            reported_currency,
            total_revenue,
            gross_profit,
            operating_income,
            net_income,
            ebit,
            ebitda,
            source,
            ingestion_ts,
            raw_payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    values = [
        (
            row["symbol"],
            row["period_type"],
            row["fiscal_date_ending"],
            row.get("reported_currency"),
            row.get("total_revenue"),
            row.get("gross_profit"),
            row.get("operating_income"),
            row.get("net_income"),
            row.get("ebit"),
            row.get("ebitda"),
            row["source"],
            row["ingestion_ts"],
            row["raw_payload_json"],
        )
        for row in rows
    ]

    with _connect(database) as connection:
        connection.executemany(sql, values)
    return len(values)


def upsert_cash_flow_rows(rows: list[dict[str, Any]], database: str) -> int:
    if not rows:
        return 0

    ensure_schema(database)
    sql = """
        INSERT OR REPLACE INTO cash_flow_statements (
            symbol,
            period_type,
            fiscal_date_ending,
            reported_currency,
            operating_cashflow,
            cashflow_from_investment,
            cashflow_from_financing,
            net_income,
            capital_expenditures,
            free_cash_flow,
            source,
            ingestion_ts,
            raw_payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    values = [
        (
            row["symbol"],
            row["period_type"],
            row["fiscal_date_ending"],
            row.get("reported_currency"),
            row.get("operating_cashflow"),
            row.get("cashflow_from_investment"),
            row.get("cashflow_from_financing"),
            row.get("net_income"),
            row.get("capital_expenditures"),
            row.get("free_cash_flow"),
            row["source"],
            row["ingestion_ts"],
            row["raw_payload_json"],
        )
        for row in rows
    ]

    with _connect(database) as connection:
        connection.executemany(sql, values)
    return len(values)


def upsert_balance_sheet_rows(rows: list[dict[str, Any]], database: str) -> int:
    if not rows:
        return 0

    ensure_schema(database)
    sql = """
        INSERT OR REPLACE INTO balance_sheets (
            symbol,
            period_type,
            fiscal_date_ending,
            reported_currency,
            total_assets,
            total_liabilities,
            total_shareholder_equity,
            cash_and_short_term_investments,
            current_debt,
            long_term_debt,
            source,
            ingestion_ts,
            raw_payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    values = [
        (
            row["symbol"],
            row["period_type"],
            row["fiscal_date_ending"],
            row.get("reported_currency"),
            row.get("total_assets"),
            row.get("total_liabilities"),
            row.get("total_shareholder_equity"),
            row.get("cash_and_short_term_investments"),
            row.get("current_debt"),
            row.get("long_term_debt"),
            row["source"],
            row["ingestion_ts"],
            row["raw_payload_json"],
        )
        for row in rows
    ]

    with _connect(database) as connection:
        connection.executemany(sql, values)
    return len(values)


def get_migration_status(database: str, *, failure_limit: int = 10) -> dict[str, Any]:
    if failure_limit <= 0:
        raise RuntimeError("failure_limit must be greater than 0.")

    ensure_schema(database)
    with _connect(database) as connection:
        status_rows = connection.execute(
            """
            SELECT status, COUNT(*) AS count
            FROM fundamental_migration_progress
            GROUP BY status
            ORDER BY status
            """
        ).fetchall()
        total_jobs = connection.execute(
            "SELECT COUNT(*) FROM fundamental_migration_progress"
        ).fetchone()[0]
        next_retry = connection.execute(
            """
            SELECT CAST(MIN(next_retry_ts) AS VARCHAR)
            FROM fundamental_migration_progress
            WHERE status = 'retry' AND next_retry_ts IS NOT NULL
            """
        ).fetchone()[0]
        recent_failures = connection.execute(
            """
            SELECT
                symbol,
                dataset,
                attempts,
                COALESCE(last_error, '') AS last_error,
                CAST(last_attempt_ts AS VARCHAR) AS last_attempt_ts,
                CAST(next_retry_ts AS VARCHAR) AS next_retry_ts
            FROM fundamental_migration_progress
            WHERE status = 'retry'
            ORDER BY COALESCE(last_attempt_ts, TIMESTAMP '1970-01-01') DESC
            LIMIT ?
            """,
            [failure_limit],
        ).fetchall()
        income_count = connection.execute(
            "SELECT COUNT(*) FROM income_statements"
        ).fetchone()[0]
        cash_count = connection.execute(
            "SELECT COUNT(*) FROM cash_flow_statements"
        ).fetchone()[0]
        balance_count = connection.execute(
            "SELECT COUNT(*) FROM balance_sheets"
        ).fetchone()[0]

    status_counts = {str(row[0]): int(row[1]) for row in status_rows}
    return {
        "database": database,
        "total_jobs": int(total_jobs),
        "completed_jobs": int(status_counts.get("success", 0)),
        "pending_jobs": int(status_counts.get("pending", 0) + status_counts.get("retry", 0)),
        "status_counts": status_counts,
        "next_retry_ts": next_retry,
        "table_counts": {
            "income_statements": int(income_count),
            "cash_flow_statements": int(cash_count),
            "balance_sheets": int(balance_count),
        },
        "recent_failures": [
            {
                "symbol": str(row[0]),
                "dataset": str(row[1]),
                "attempts": int(row[2]),
                "last_error": str(row[3]),
                "last_attempt_ts": row[4],
                "next_retry_ts": row[5],
            }
            for row in recent_failures
        ],
    }
