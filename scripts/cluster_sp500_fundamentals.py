from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd

from finance.models.dimensional.cluster import prepare_clustering_frame, build_clustered_frame


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cluster and visualize the S&P 500 fundamentals dataset."
    )
    parser.add_argument(
        "--input",
        default="artifacts/sp500_fundamentals_flat.parquet",
        help="Input flat fundamentals dataset (.parquet or .csv).",
    )
    parser.add_argument(
        "--output",
        default="artifacts/sp500_fundamentals_clusters.parquet",
        help="Output clustered dataset (.parquet or .csv).",
    )
    parser.add_argument(
        "--plot",
        default="artifacts/sp500_fundamentals_clusters.html",
        help="Output interactive scatter plot path (.html).",
    )
    parser.add_argument(
        "--clusters",
        type=int,
        default=6,
        help="Number of K-means clusters.",
    )
    parser.add_argument(
        "--projection",
        choices=("pca", "umap"),
        default="pca",
        help="2D projection used for visualization.",
    )
    parser.add_argument(
        "--random-state",
        type=int,
        default=42,
        help="Random seed for clustering and projection.",
    )
    return parser.parse_args(argv)


def load_frame(path: str | Path) -> pd.DataFrame:
    source = Path(path)
    suffix = source.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(source)
    if suffix == ".csv":
        return pd.read_csv(source)
    raise ValueError(f"Unsupported input format for {source}. Use .parquet or .csv.")


def write_frame(frame: pd.DataFrame, path: str | Path) -> Path:
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


def save_plot(clustered: pd.DataFrame, path: str | Path, projection: str) -> Path:
    try:
        import plotly.express as px
    except ImportError as exc:
        raise RuntimeError(
            "Plotly is required for interactive plotting. Install: pip install -e '.[viz]'"
        ) from exc

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    hover_columns = [
        column
        for column in (
            "symbol",
            "security",
            "company_name",
            "sector",
            "industry",
            "gics_sector",
            "gics_sub_industry",
        )
        if column in clustered.columns
    ]
    fig = px.scatter(
        clustered,
        x="cluster_x",
        y="cluster_y",
        color=clustered["cluster"].astype(str),
        hover_data=hover_columns,
        title=(
            f"S&P 500 Fundamentals Clusters "
            f"({projection.upper()} + K-means, payload=v2_no_year_fields)"
        ),
        labels={
            "cluster_x": "Component 1",
            "cluster_y": "Component 2",
            "color": "Cluster",
        },
    )
    fig.update_traces(marker={"size": 9, "opacity": 0.8})
    feature_count = clustered.attrs.get("feature_count")
    fig.update_layout(
        legend_title_text="Cluster",
        annotations=[
            {
                "text": (
                    f"payload=v2_no_year_fields | features={feature_count}"
                    if feature_count is not None
                    else "payload=v2_no_year_fields"
                ),
                "xref": "paper",
                "yref": "paper",
                "x": 0.0,
                "y": 1.08,
                "showarrow": False,
            }
        ],
    )
    fig.write_html(target, include_plotlyjs="cdn")
    return target


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    frame = load_frame(args.input)
    _, features = prepare_clustering_frame(frame)
    clustered = build_clustered_frame(
        frame,
        n_clusters=args.clusters,
        projection=args.projection,
        random_state=args.random_state,
    )
    clustered.attrs["feature_count"] = len(features.columns)
    if "symbol" in frame.columns:
        clustered = clustered.merge(frame, on="symbol", how="left", suffixes=("", "_source"))
        clustered.attrs["feature_count"] = len(features.columns)
    output_path = write_frame(clustered, args.output)
    plot_path = save_plot(clustered, args.plot, args.projection)
    print(f"Saved clustered dataset to {output_path}")
    print(f"Saved plot to {plot_path}")


if __name__ == "__main__":
    main()
