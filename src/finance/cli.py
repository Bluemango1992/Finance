import argparse
import json
import sys

from finance.db import run_query
from finance.providers import fetch_alphavantage_overview, fetch_yfinance_info


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch stock information from Alpha Vantage or Yahoo Finance."
    )
    parser.add_argument("symbol", nargs="?", default="IBM", help="Ticker symbol to look up.")
    parser.add_argument(
        "--endpoint",
        choices=("api", "duckdb"),
        default="api",
        help="Execution endpoint. Use 'duckdb' to run local SQL without APIs.",
    )
    parser.add_argument(
        "--provider",
        choices=("alphavantage", "yfinance"),
        default="alphavantage",
        help="Data source to use.",
    )
    parser.add_argument(
        "--sql",
        default="select 1 as ok",
        help="SQL query to run when --endpoint duckdb is used.",
    )
    parser.add_argument(
        "--duckdb-database",
        default=":memory:",
        help="DuckDB database path. ':memory:' keeps everything in-memory.",
    )
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args(sys.argv[1:])
    if args.endpoint == "duckdb":
        try:
            data = run_query(args.sql, database=args.duckdb_database)
        except Exception as exc:
            print(f"Error: DuckDB query failed: {exc}", file=sys.stderr)
            raise SystemExit(1) from exc
        print(json.dumps(data, indent=2))
        return

    try:
        if args.provider == "yfinance":
            data = fetch_yfinance_info(args.symbol)
        else:
            data = fetch_alphavantage_overview(args.symbol)
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    print(json.dumps(data, indent=2))
