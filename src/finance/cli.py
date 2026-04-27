import argparse
from dataclasses import asdict
import json
import sys

from finance.config import build_settings
from finance.db import run_query
from finance.fundamentals_pipeline import PipelineConfig, run_and_persist_sp500_fundamentals
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
        "--nikkei225-output",
        default="data/nikkei225_components.json",
        help="Path to the root-level JSON file that stores Nikkei 225 components.",
    )
    parser.add_argument(
        "--build-sp500-fundamentals",
        action="store_true",
        help="Build a structured S&P 500 fundamentals dataset via yfinance.",
    )
    parser.add_argument(
        "--fundamentals-flat-output",
        default="artifacts/sp500_fundamentals_flat.parquet",
        help="Flat output path for the S&P 500 fundamentals dataset.",
    )
    parser.add_argument(
        "--fundamentals-long-output",
        default="artifacts/sp500_fundamentals_long.parquet",
        help="Long output path for normalized statement history.",
    )
    parser.add_argument(
        "--fundamentals-summary-output",
        default="artifacts/sp500_fundamentals_summary.json",
        help="Summary output path for the fundamentals pipeline.",
    )
    parser.add_argument(
        "--fundamentals-history-period",
        default="5y",
        help="Price history period for the fundamentals pipeline.",
    )
    parser.add_argument(
        "--fundamentals-lookback-years",
        type=int,
        default=4,
        help="Annual statement periods to normalize into the dataset.",
    )
    parser.add_argument(
        "--fundamentals-retry-attempts",
        type=int,
        default=3,
        help="Retry attempts per ticker during fundamentals ingestion.",
    )
    parser.add_argument(
        "--fundamentals-retry-delay-seconds",
        type=float,
        default=1.5,
        help="Base retry delay per failed ticker request.",
    )
    parser.add_argument(
        "--fundamentals-pause-between-tickers-seconds",
        type=float,
        default=0.0,
        help="Optional pause inserted between ticker requests.",
    )
    parser.add_argument(
        "--fundamentals-max-tickers",
        type=int,
        default=None,
        help="Optional ticker cap for debugging the fundamentals pipeline.",
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

    if args.build_sp500_fundamentals:
        try:
            result = run_and_persist_sp500_fundamentals(
                PipelineConfig(
                    sp500_path=args.sp500_output,
                    refresh_sp500=args.refresh_sp500,
                    output_flat=args.fundamentals_flat_output,
                    output_long=args.fundamentals_long_output,
                    output_summary=args.fundamentals_summary_output,
                    history_period=args.fundamentals_history_period,
                    lookback_years=args.fundamentals_lookback_years,
                    retry_attempts=args.fundamentals_retry_attempts,
                    retry_delay_seconds=args.fundamentals_retry_delay_seconds,
                    pause_between_tickers_seconds=args.fundamentals_pause_between_tickers_seconds,
                    max_tickers=args.fundamentals_max_tickers,
                )
            )
        except RuntimeError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            raise SystemExit(1) from exc

        print(json.dumps(result, indent=2))
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
