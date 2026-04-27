from __future__ import annotations

import pandas as pd

from finance.models.dimensional.cluster import build_clustered_frame, prepare_clustering_frame


def test_prepare_clustering_frame_selects_numeric_columns_and_fills_missing() -> None:
    frame = pd.DataFrame(
        {
            "symbol": ["AAA", "BBB", "CCC"],
            "security": ["A", "B", "C"],
            "metric_a": [1.0, None, 3.0],
            "metric_b": [True, False, True],
            "revenue_year_fy0": [2024, 2024, 2024],
            "validation_issue_count": [0, 1, 2],
        }
    )

    metadata, features = prepare_clustering_frame(frame)

    assert metadata.columns.tolist() == ["symbol", "security"]
    assert features.columns.tolist() == ["metric_a", "metric_b"]
    assert features["metric_a"].isna().sum() == 0


def test_build_clustered_frame_adds_labels_and_coordinates() -> None:
    frame = pd.DataFrame(
        {
            "symbol": ["AAA", "BBB", "CCC", "DDD"],
            "security": ["A", "B", "C", "D"],
            "feature_1": [1.0, 1.2, 5.0, 5.2],
            "feature_2": [0.9, 1.1, 5.1, 5.3],
        }
    )

    clustered = build_clustered_frame(frame, n_clusters=2, projection="pca", random_state=42)

    assert {"symbol", "cluster", "cluster_x", "cluster_y"} <= set(clustered.columns)
    assert len(clustered) == 4
    assert clustered["cluster"].nunique() == 2
