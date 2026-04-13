import argparse
import json
import sys

from alphavantage.providers import fetch_alphavantage_overview, fetch_yfinance_info


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch stock information from Alpha Vantage or Yahoo Finance."
    )
    parser.add_argument("symbol", nargs="?", default="IBM", help="Ticker symbol to look up.")
    parser.add_argument(
        "--provider",
        choices=("alphavantage", "yfinance"),
        default="alphavantage",
        help="Data source to use.",
    )
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args(sys.argv[1:])
    try:
        if args.provider == "yfinance":
            data = fetch_yfinance_info(args.symbol)
        else:
            data = fetch_alphavantage_overview(args.symbol)
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    print(json.dumps(data, indent=2))
