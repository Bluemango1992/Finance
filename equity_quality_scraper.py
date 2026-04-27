from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from finance.fundamentals_pipeline import (  # noqa: E402
    DEFAULT_FLAT_OUTPUT,
    DEFAULT_LONG_OUTPUT,
    DEFAULT_SUMMARY_OUTPUT,
    PipelineConfig,
    run_and_persist_sp500_fundamentals,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a structured S&P 500 fundamentals dataset with yfinance."
    )
    parser.add_argument(
        "--refresh-sp500",
        action="store_true",
        help="Refresh the S&P 500 constituent snapshot before ingestion.",
    )
    parser.add_argument(
        "--sp500-path",
        default="data/sp500_constituents.json",
        help="Path to the local S&P 500 constituent snapshot.",
    )
    parser.add_argument(
        "--flat-output",
        default=str(DEFAULT_FLAT_OUTPUT),
        help="Output path for the flat one-row-per-company dataset (.csv or .parquet).",
    )
    parser.add_argument(
        "--long-output",
        default=str(DEFAULT_LONG_OUTPUT),
        help="Output path for the long statement-metric dataset (.csv or .parquet).",
    )
    parser.add_argument(
        "--summary-output",
        default=str(DEFAULT_SUMMARY_OUTPUT),
        help="Output path for the JSON run summary.",
    )
    parser.add_argument(
        "--history-period",
        default="5y",
        help="Price history period to request from yfinance.",
    )
    parser.add_argument(
        "--lookback-years",
        type=int,
        default=4,
        help="Number of annual statement periods to normalize per metric.",
    )
    parser.add_argument(
        "--retry-attempts",
        type=int,
        default=3,
        help="Retry attempts per ticker.",
    )
    parser.add_argument(
        "--retry-delay-seconds",
        type=float,
        default=1.5,
        help="Base delay between retries.",
    )
    parser.add_argument(
        "--pause-between-tickers-seconds",
        type=float,
        default=0.0,
        help="Optional pause between ticker requests.",
    )
    parser.add_argument(
        "--max-tickers",
        type=int,
        default=None,
        help="Optional cap for dry runs or debugging.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Python logging level.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    config = PipelineConfig(
        sp500_path=args.sp500_path,
        refresh_sp500=args.refresh_sp500,
        output_flat=args.flat_output,
        output_long=args.long_output,
        output_summary=args.summary_output,
        history_period=args.history_period,
        lookback_years=args.lookback_years,
        retry_attempts=args.retry_attempts,
        retry_delay_seconds=args.retry_delay_seconds,
        pause_between_tickers_seconds=args.pause_between_tickers_seconds,
        max_tickers=args.max_tickers,
        log_level=args.log_level,
    )
    print(json.dumps(run_and_persist_sp500_fundamentals(config), indent=2))


if __name__ == "__main__":
    main()
