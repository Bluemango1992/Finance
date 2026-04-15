import argparse
import json
import sys

from finance.config import build_settings
from finance.db import run_query
from finance.providers import fetch_alphavantage_overview, fetch_yfinance_info


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
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args(sys.argv[1:])
    settings = build_settings(
        endpoint=args.endpoint,
        provider=args.provider,
        duckdb_database=args.duckdb_database,
        sql=args.sql,
        alphavantage_api_key=args.alphavantage_api_key,
    )

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
