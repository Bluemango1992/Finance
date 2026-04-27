import argparse
from dataclasses import asdict
import json
import sys

from finance.config import build_settings
from finance.db import (
    get_migration_status,
    run_query,
    upsert_balance_sheet_rows,
    upsert_cash_flow_rows,
    upsert_income_statement_rows,
)
from finance.fundamental_ingestion import (
    load_balance_sheet_rows_from_alphavantage,
    load_cash_flow_rows_from_alphavantage,
    load_income_statement_rows_from_alphavantage,
)
from finance.fundamental_migration import migrate_sp500_fundamentals
from finance.providers import fetch_alphavantage_overview, fetch_yfinance_info
from finance.scraper.ftse250 import refresh_ftse250_data_safe
from finance.scraper.nikkei225 import refresh_nikkei225_data_safe
from finance.scraper.sp500 import refresh_sp500_data_safe


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch stock information from Alpha Vantage or Yahoo Finance."
    )
    parser.add_argument("symbol", nargs="?", default=None, help="Ticker symbol to look up.")
    parser.add_argument(
        "--endpoint",
        choices=("api", "duckdb"),
        default=None,
        help="Execution endpoint. Use 'duckdb' to run local SQL without APIs.",
    )
    parser.add_argument(
        "--provider",
        choices=("alphavantage", "yfinance"),
        default=None,
        help="Data source to use.",
    )
    parser.add_argument(
        "--sql",
        default=None,
        help="SQL query to run when --endpoint duckdb is used.",
    )
    parser.add_argument(
        "--duckdb-database",
        default=None,
        help="DuckDB database path. ':memory:' keeps everything in-memory.",
    )
    parser.add_argument(
        "--alphavantage-api-key",
        default=None,
        help="Optional API key override. If omitted, ALPHAVANTAGE_API_KEY is used.",
    )
    parser.add_argument(
        "--refresh-sp500",
        action="store_true",
        help="Refresh the local S&P 500 constituents JSON if today's snapshot is missing.",
    )
    parser.add_argument(
        "--sp500-output",
        default="data/sp500_constituents.json",
        help="Path to the root-level JSON file that stores S&P 500 constituents.",
    )
    parser.add_argument(
        "--refresh-ftse250",
        action="store_true",
        help="Refresh the local FTSE 250 constituents JSON if today's snapshot is missing.",
    )
    parser.add_argument(
        "--ftse250-output",
        default="data/ftse250_constituents.json",
        help="Path to the root-level JSON file that stores FTSE 250 constituents.",
    )
    parser.add_argument(
        "--refresh-nikkei225",
        action="store_true",
        help="Refresh the local Nikkei 225 components JSON if today's snapshot is missing.",
    )
    parser.add_argument(
        "--ingest-income-statement",
        action="store_true",
        help="Fetch historical annual/quarterly income statements from Alpha Vantage into DuckDB.",
    )
    parser.add_argument(
        "--ingest-cash-flow",
        action="store_true",
        help="Fetch historical annual/quarterly cash flow statements from Alpha Vantage into DuckDB.",
    )
    parser.add_argument(
        "--ingest-balance-sheet",
        action="store_true",
        help="Fetch historical annual/quarterly balance sheets from Alpha Vantage into DuckDB.",
    )
    parser.add_argument(
        "--migrate-sp500-fundamentals",
        action="store_true",
        help=(
            "Migrate fundamentals for all S&P 500 symbols into DuckDB and persist migration "
            "progress/retry state."
        ),
    )
    parser.add_argument(
        "--migration-status",
        action="store_true",
        help="Show migration progress, retries, and table row counts from DuckDB.",
    )
    parser.add_argument(
        "--sp500-input",
        default="data/sp500_constituents.json",
        help="Path to S&P 500 symbols JSON used by --migrate-sp500-fundamentals.",
    )
    parser.add_argument(
        "--migration-datasets",
        default="income_statement,cash_flow,balance_sheet",
        help="Comma-separated datasets for migration: income_statement,cash_flow,balance_sheet.",
    )
    parser.add_argument(
        "--max-requests",
        type=int,
        default=5,
        help="Maximum Alpha Vantage requests per run (default: 5).",
    )
    parser.add_argument(
        "--retry-delay-minutes",
        type=int,
        default=30,
        help="Base retry delay for non-quota failures in minutes (default: 30).",
    )
    parser.add_argument(
        "--quota-retry-minutes",
        type=int,
        default=1440,
        help="Retry delay in minutes when quota/rate-limit is hit (default: 1440).",
    )
    parser.add_argument(
        "--nikkei225-output",
        default="data/nikkei225_components.json",
        help="Path to the root-level JSON file that stores Nikkei 225 components.",
    )
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args(sys.argv[1:])
    if args.refresh_nikkei225:
        try:
            summary = refresh_nikkei225_data_safe(path=args.nikkei225_output)
        except RuntimeError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            raise SystemExit(1) from exc

        print(json.dumps(asdict(summary), indent=2))
        return

    if args.refresh_ftse250:
        try:
            summary = refresh_ftse250_data_safe(path=args.ftse250_output)
        except RuntimeError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            raise SystemExit(1) from exc

        print(json.dumps(asdict(summary), indent=2))
        return

    if args.refresh_sp500:
        try:
            summary = refresh_sp500_data_safe(path=args.sp500_output)
        except RuntimeError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            raise SystemExit(1) from exc

        print(json.dumps(asdict(summary), indent=2))
        return

    settings = build_settings(
        endpoint=args.endpoint,
        provider=args.provider,
        duckdb_database=args.duckdb_database,
        sql=args.sql,
        alphavantage_api_key=args.alphavantage_api_key,
    )

    if args.ingest_income_statement:
        if not args.symbol:
            print("Error: symbol is required for --ingest-income-statement.", file=sys.stderr)
            raise SystemExit(2)
        try:
            rows = load_income_statement_rows_from_alphavantage(
                args.symbol,
                api_key=args.alphavantage_api_key,
            )
            written = upsert_income_statement_rows(rows, database=settings.duckdb_database)
        except RuntimeError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            raise SystemExit(1) from exc

        print(
            json.dumps(
                {
                    "symbol": args.symbol.upper(),
                    "database": settings.duckdb_database,
                    "rows_written": written,
                },
                indent=2,
            )
        )
        return

    if args.ingest_cash_flow:
        if not args.symbol:
            print("Error: symbol is required for --ingest-cash-flow.", file=sys.stderr)
            raise SystemExit(2)
        try:
            rows = load_cash_flow_rows_from_alphavantage(
                args.symbol,
                api_key=args.alphavantage_api_key,
            )
            written = upsert_cash_flow_rows(rows, database=settings.duckdb_database)
        except RuntimeError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            raise SystemExit(1) from exc
        print(
            json.dumps(
                {
                    "symbol": args.symbol.upper(),
                    "database": settings.duckdb_database,
                    "rows_written": written,
                },
                indent=2,
            )
        )
        return

    if args.ingest_balance_sheet:
        if not args.symbol:
            print("Error: symbol is required for --ingest-balance-sheet.", file=sys.stderr)
            raise SystemExit(2)
        try:
            rows = load_balance_sheet_rows_from_alphavantage(
                args.symbol,
                api_key=args.alphavantage_api_key,
            )
            written = upsert_balance_sheet_rows(rows, database=settings.duckdb_database)
        except RuntimeError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            raise SystemExit(1) from exc
        print(
            json.dumps(
                {
                    "symbol": args.symbol.upper(),
                    "database": settings.duckdb_database,
                    "rows_written": written,
                },
                indent=2,
            )
        )
        return

    if args.migrate_sp500_fundamentals:
        if settings.duckdb_database == ":memory:":
            print(
                "Error: --migrate-sp500-fundamentals requires a persistent --duckdb-database path.",
                file=sys.stderr,
            )
            raise SystemExit(2)
        datasets = [item.strip() for item in args.migration_datasets.split(",")]
        try:
            summary = migrate_sp500_fundamentals(
                database=settings.duckdb_database,
                sp500_path=args.sp500_input,
                datasets=datasets,
                api_key=settings.alphavantage_api_key,
                max_requests=args.max_requests,
                retry_delay_minutes=args.retry_delay_minutes,
                quota_retry_minutes=args.quota_retry_minutes,
            )
        except RuntimeError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            raise SystemExit(1) from exc
        print(json.dumps(asdict(summary), indent=2))
        return

    if args.migration_status:
        if settings.duckdb_database == ":memory:":
            print(
                "Error: --migration-status requires a persistent --duckdb-database path.",
                file=sys.stderr,
            )
            raise SystemExit(2)
        try:
            status = get_migration_status(settings.duckdb_database)
        except RuntimeError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            raise SystemExit(1) from exc
        print(json.dumps(status, indent=2))
        return

    if settings.endpoint == "duckdb":
        if not settings.sql:
            print("Error: --sql is required when --endpoint duckdb is used.", file=sys.stderr)
            raise SystemExit(2)
        try:
            data = run_query(settings.sql, database=settings.duckdb_database)
        except Exception as exc:
            print(f"Error: DuckDB query failed: {exc}", file=sys.stderr)
            raise SystemExit(1) from exc
        print(json.dumps(data, indent=2))
        return

    if not args.symbol:
        print("Error: symbol is required for API requests.", file=sys.stderr)
        raise SystemExit(2)

    try:
        if settings.provider == "yfinance":
            data = fetch_yfinance_info(args.symbol)
        else:
            data = fetch_alphavantage_overview(args.symbol, api_key=settings.alphavantage_api_key)
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    print(json.dumps(data, indent=2))
