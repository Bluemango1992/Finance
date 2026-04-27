from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from finance.discount_screener import DiscountScreenerConfig, run_and_persist_discount_screener


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Screen the S&P 500 fundamentals dataset for potentially discounted stocks."
    )
    parser.add_argument(
        "--input",
        default="artifacts/sp500_fundamentals_flat.parquet",
        help="Input flat fundamentals dataset (.parquet or .csv).",
    )
    parser.add_argument(
        "--output",
        default="artifacts/sp500_discount_screener.parquet",
        help="Output ranked screener dataset (.parquet or .csv).",
    )
    parser.add_argument(
        "--min-sub-industry-size",
        type=int,
        default=5,
        help="Minimum peer count required to use sub-industry peers.",
    )
    parser.add_argument(
        "--min-sector-size",
        type=int,
        default=12,
        help="Minimum peer count required to use sector peers.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=25,
        help="Number of top symbols to echo in the summary output.",
    )
    parser.add_argument(
        "--include-financials",
        action="store_true",
        help="Include Financials in the screener universe.",
    )
    parser.add_argument(
        "--include-real-estate",
        action="store_true",
        help="Include Real Estate in the screener universe.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    result = run_and_persist_discount_screener(
        DiscountScreenerConfig(
            input_path=args.input,
            output_path=args.output,
            min_sub_industry_size=args.min_sub_industry_size,
            min_sector_size=args.min_sector_size,
            top_n=args.top_n,
            include_financials=args.include_financials,
            include_real_estate=args.include_real_estate,
        )
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
