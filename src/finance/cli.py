import argparse
from dataclasses import asdict
import json
import sys

from finance.db import run_query
from finance.data.ingestion import ingest_spy_prices
from finance.providers import fetch_alphavantage_overview, fetch_yfinance_info
from finance.scraper.ftse250 import refresh_ftse250_data_safe
from finance.scraper.nikkei225 import refresh_nikkei225_data_safe
from finance.scraper.sp500 import refresh_sp500_data_safe


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
    parser.add_argument(
        "--ingest-spy",
        action="store_true",
        help="Run SPY Yahoo Finance ingestion pipeline into DuckDB.",
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

    if args.ingest_spy:
        try:
            summary = ingest_spy_prices(database=args.duckdb_database)
        except RuntimeError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            raise SystemExit(1) from exc

        if summary["rows_invalid"] > 0:
            print(f"Invalid rows skipped: {summary['rows_invalid']}", file=sys.stderr)
        if summary["rows_duplicates"] > 0:
            print(f"Duplicate rows skipped: {summary['rows_duplicates']}", file=sys.stderr)
        print(json.dumps(summary, indent=2))
        return

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
