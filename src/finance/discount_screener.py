from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

DEFAULT_INPUT_PATH = Path("artifacts/sp500_fundamentals_flat.parquet")
DEFAULT_OUTPUT_PATH = Path("artifacts/sp500_discount_screener.parquet")
EXCLUDED_SECTORS = {"Financials", "Real Estate"}


@dataclass(slots=True)
class DiscountScreenerConfig:
    input_path: str | Path = DEFAULT_INPUT_PATH
    output_path: str | Path = DEFAULT_OUTPUT_PATH
    min_sub_industry_size: int = 5
    min_sector_size: int = 12
    top_n: int = 50
    include_financials: bool = False
    include_real_estate: bool = False


def load_fundamentals_frame(path: str | Path) -> pd.DataFrame:
    source = Path(path)
    suffix = source.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(source)
    if suffix == ".csv":
        return pd.read_csv(source)
    raise ValueError(f"Unsupported input format for {source}. Use .parquet or .csv.")


def write_screener_frame(frame: pd.DataFrame, path: str | Path) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    suffix = target.suffix.lower()
    if suffix == ".parquet":
        frame.to_parquet(target, index=False)
    elif suffix == ".csv":
        frame.to_csv(target, index=False)
    else:
        raise ValueError(f"Unsupported output format for {target}. Use .parquet or .csv.")
    return target


def build_discount_screener(
    frame: pd.DataFrame,
    *,
    min_sub_industry_size: int = 5,
    min_sector_size: int = 12,
    include_financials: bool = False,
    include_real_estate: bool = False,
) -> pd.DataFrame:
    if frame.empty:
        raise ValueError("Input fundamentals frame is empty.")
    if "symbol" not in frame.columns:
        raise ValueError("Input fundamentals frame must include 'symbol'.")

    screened = frame.copy()
    screened = _exclude_structural_sectors(
        screened,
        include_financials=include_financials,
        include_real_estate=include_real_estate,
    )
    screened = screened.drop_duplicates(subset=["symbol"]).reset_index(drop=True)
    if screened.empty:
        raise ValueError("No rows remain after sector exclusions.")

    screened["peer_group"] = _build_peer_groups(
        screened,
        min_sub_industry_size=min_sub_industry_size,
        min_sector_size=min_sector_size,
    )

    numeric_columns = [
        "pe_ratio",
        "pfcf_ratio",
        "debt_to_equity",
        "revenue_growth_latest",
        "net_margin_latest_pct",
        "roe_latest_pct",
    ]
    for column in numeric_columns:
        if column in screened.columns:
            screened[column] = pd.to_numeric(screened[column], errors="coerce")

    screened["valuation_score"] = _build_valuation_score(screened)
    screened["quality_score"] = _build_quality_score(screened)
    screened["discount_score"] = (
        0.65 * screened["valuation_score"] + 0.35 * screened["quality_score"]
    )
    screened["passes_quality_gate"] = _passes_quality_gate(screened)

    screened["peer_group_size"] = screened.groupby("peer_group")["symbol"].transform("size")
    for metric in ("pe_ratio", "pfcf_ratio"):
        valid_metric = screened[metric].where(screened[metric] > 0)
        screened[f"{metric}_peer_median"] = valid_metric.groupby(screened["peer_group"]).transform("median")
        screened[f"{metric}_discount_to_peer"] = (
            screened[f"{metric}_peer_median"] - screened[metric]
        ) / screened[f"{metric}_peer_median"]

    screened["primary_discount_signal"] = screened.apply(_primary_discount_signal, axis=1)
    screened = screened.sort_values(
        ["passes_quality_gate", "discount_score", "valuation_score", "quality_score"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)
    return screened


def run_and_persist_discount_screener(config: DiscountScreenerConfig) -> dict[str, Any]:
    frame = load_fundamentals_frame(config.input_path)
    screened = build_discount_screener(
        frame,
        min_sub_industry_size=config.min_sub_industry_size,
        min_sector_size=config.min_sector_size,
        include_financials=config.include_financials,
        include_real_estate=config.include_real_estate,
    )
    target = write_screener_frame(screened, config.output_path)
    return {
        "output": str(target),
        "rows": int(len(screened)),
        "top_symbols": screened["symbol"].head(config.top_n).tolist(),
    }


def _exclude_structural_sectors(
    frame: pd.DataFrame,
    *,
    include_financials: bool,
    include_real_estate: bool,
) -> pd.DataFrame:
    excluded = set()
    if not include_financials:
        excluded.add("Financials")
    if not include_real_estate:
        excluded.add("Real Estate")
    if not excluded or "gics_sector" not in frame.columns:
        return frame
    return frame[~frame["gics_sector"].isin(excluded)].copy()


def _build_peer_groups(
    frame: pd.DataFrame,
    *,
    min_sub_industry_size: int,
    min_sector_size: int,
) -> pd.Series:
    sub_sizes = frame.groupby("gics_sub_industry")["symbol"].transform("size")
    sector_sizes = frame.groupby("gics_sector")["symbol"].transform("size")
    return frame.apply(
        lambda row: _resolve_peer_group(
            sub_industry=str(row.get("gics_sub_industry") or ""),
            sector=str(row.get("gics_sector") or ""),
            sub_industry_size=int(sub_sizes.loc[row.name]),
            sector_size=int(sector_sizes.loc[row.name]),
            min_sub_industry_size=min_sub_industry_size,
            min_sector_size=min_sector_size,
        ),
        axis=1,
    )


def _resolve_peer_group(
    *,
    sub_industry: str,
    sector: str,
    sub_industry_size: int,
    sector_size: int,
    min_sub_industry_size: int,
    min_sector_size: int,
) -> str:
    if sub_industry and sub_industry_size >= min_sub_industry_size:
        return f"sub_industry::{sub_industry}"
    if sector and sector_size >= min_sector_size:
        return f"sector::{sector}"
    return "market::broad"


def _rank_low_within_peer(frame: pd.DataFrame, metric: str) -> pd.Series:
    valid = frame[metric].where(frame[metric] > 0)
    peer_rank = valid.groupby(frame["peer_group"]).rank(method="average", pct=True)
    return 1.0 - peer_rank


def _build_valuation_score(frame: pd.DataFrame) -> pd.Series:
    pe_score = _rank_low_within_peer(frame, "pe_ratio")
    pfcf_score = _rank_low_within_peer(frame, "pfcf_ratio")
    valuation = pd.concat([pe_score.rename("pe"), pfcf_score.rename("pfcf")], axis=1)
    return valuation.mean(axis=1, skipna=True).fillna(0.0)


def _build_quality_score(frame: pd.DataFrame) -> pd.Series:
    checks = pd.DataFrame(index=frame.index)
    checks["profitable"] = frame["profitable"].fillna(False).astype(float)
    checks["positive_fcf"] = frame["positive_fcf"].fillna(False).astype(float)
    checks["high_roe"] = frame["high_roe"].fillna(False).astype(float)
    checks["margin_positive"] = (frame["net_margin_latest_pct"] > 0).fillna(False).astype(float)
    checks["growth_non_negative"] = (frame["revenue_growth_latest"] >= 0).fillna(False).astype(float)
    checks["debt_reasonable"] = (
        frame["debt_to_equity"].isna() | (frame["debt_to_equity"] <= 2.0)
    ).astype(float)
    return checks.mean(axis=1)


def _passes_quality_gate(frame: pd.DataFrame) -> pd.Series:
    return (
        frame["positive_fcf"].fillna(False)
        & (frame["revenue_growth_latest"].fillna(-1.0) > -0.15)
        & (
            frame["debt_to_equity"].isna()
            | (frame["debt_to_equity"] <= 2.0)
        )
    )


def _primary_discount_signal(row: pd.Series) -> str:
    pe_discount = row.get("pe_ratio_discount_to_peer")
    pfcf_discount = row.get("pfcf_ratio_discount_to_peer")
    if pd.notna(pe_discount) and (pd.isna(pfcf_discount) or pe_discount >= pfcf_discount):
        return "pe_ratio"
    if pd.notna(pfcf_discount):
        return "pfcf_ratio"
    return "none"
