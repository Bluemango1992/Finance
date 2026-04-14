from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from finance.providers import fetch_yfinance_history


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch last 10 years of SPY prices from Yahoo.")
    parser.add_argument(
        "--output",
        default="data/benchmarks/spy_10y.parquet",
        help="Output file path (.parquet or .csv).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    frame = fetch_yfinance_history("SPY", period="10y")
    frame = frame.reset_index().rename(columns={"Date": "date"})

    suffix = output.suffix.lower()
    if suffix == ".parquet":
        frame.to_parquet(output, index=False)
    elif suffix == ".csv":
        frame.to_csv(output, index=False)
    else:
        raise SystemExit("Output must end with .parquet or .csv")

    print(f"Wrote {len(frame)} rows to {output}")


if __name__ == "__main__":
    main()
