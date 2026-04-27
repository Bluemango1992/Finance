from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from finance.models.dimensional.umap_model import fit_transform_umap

DEFAULT_ID_COLUMNS = (
    "symbol",
    "security",
    "company_name",
    "sector",
    "industry",
    "gics_sector",
    "gics_sub_industry",
)
DEFAULT_EXCLUDED_NUMERIC_COLUMNS = (
    "validation_issue_count",
    "missing_critical_field_count",
)
DEFAULT_EXCLUDED_NUMERIC_SUFFIXES = (
    "_year_fy0",
    "_year_fy1",
    "_year_fy2",
    "_year_fy3",
)


def prepare_clustering_frame(
    frame: pd.DataFrame,
    *,
    id_columns: tuple[str, ...] = DEFAULT_ID_COLUMNS,
    excluded_numeric_columns: tuple[str, ...] = DEFAULT_EXCLUDED_NUMERIC_COLUMNS,
    excluded_numeric_suffixes: tuple[str, ...] = DEFAULT_EXCLUDED_NUMERIC_SUFFIXES,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if frame.empty:
        raise ValueError("Input fundamentals frame is empty.")
    if "symbol" not in frame.columns:
        raise ValueError("Input fundamentals frame must include a 'symbol' column.")

    metadata_columns = [column for column in id_columns if column in frame.columns]
    metadata = frame[metadata_columns].copy()

    numeric = frame.select_dtypes(include=["number", "bool"]).copy()
    excluded_by_suffix = [
        column
        for column in numeric.columns
        if any(column.endswith(suffix) for suffix in excluded_numeric_suffixes)
    ]
    numeric = numeric.drop(columns=[column for column in excluded_numeric_columns if column in numeric.columns])
    numeric = numeric.drop(columns=excluded_by_suffix)
    numeric = numeric.replace([float("inf"), float("-inf")], pd.NA)
    numeric = numeric.dropna(axis=1, how="all")
    if numeric.empty:
        raise ValueError("No numeric features available for clustering.")

    medians = numeric.median(numeric_only=True)
    numeric = numeric.fillna(medians)
    numeric = numeric.fillna(0.0)
    return metadata.reset_index(drop=True), numeric.reset_index(drop=True)


def fit_kmeans_projection(
    features: pd.DataFrame,
    *,
    n_clusters: int = 6,
    projection: str = "pca",
    random_state: int = 42,
) -> dict[str, Any]:
    if features.empty:
        raise ValueError("Clustering features are empty.")
    if n_clusters < 2:
        raise ValueError("n_clusters must be >= 2.")
    if len(features) < n_clusters:
        raise ValueError("n_clusters cannot exceed number of rows.")

    matrix = features.to_numpy(dtype=float, copy=True)
    means = matrix.mean(axis=0, keepdims=True)
    stds = matrix.std(axis=0, keepdims=True)
    stds[stds == 0] = 1.0
    scaled = (matrix - means) / stds

    model = _fit_kmeans_numpy(scaled, n_clusters=n_clusters, random_state=random_state)
    labels = model["labels"]

    projection_key = projection.lower()
    if projection_key == "pca":
        embedding = _fit_pca_numpy(scaled, n_components=2)
    elif projection_key == "umap":
        projection_result = fit_transform_umap(
            scaled,
            n_components=2,
            random_state=random_state,
        )
        embedding = projection_result["embedding"]
    else:
        raise ValueError("projection must be 'pca' or 'umap'.")

    return {
        "labels": labels,
        "embedding": embedding,
        "model": model,
        "scaler": {
            "mean": means,
            "scale": stds,
        },
    }


def build_clustered_frame(
    frame: pd.DataFrame,
    *,
    n_clusters: int = 6,
    projection: str = "pca",
    random_state: int = 42,
) -> pd.DataFrame:
    metadata, features = prepare_clustering_frame(frame)
    result = fit_kmeans_projection(
        features,
        n_clusters=n_clusters,
        projection=projection,
        random_state=random_state,
    )

    clustered = metadata.copy()
    clustered["cluster"] = result["labels"]
    clustered["cluster_x"] = result["embedding"][:, 0]
    clustered["cluster_y"] = result["embedding"][:, 1]
    return clustered


def _fit_pca_numpy(matrix: np.ndarray, *, n_components: int = 2) -> np.ndarray:
    if matrix.ndim != 2:
        raise ValueError("matrix must be 2D for PCA.")
    if n_components < 1 or n_components > matrix.shape[1]:
        raise ValueError("n_components must be between 1 and the number of features.")

    centered = matrix - matrix.mean(axis=0, keepdims=True)
    _, _, vh = np.linalg.svd(centered, full_matrices=False)
    components = vh[:n_components].T
    return centered @ components


def _fit_kmeans_numpy(
    matrix: np.ndarray,
    *,
    n_clusters: int,
    random_state: int,
    max_iter: int = 100,
) -> dict[str, Any]:
    rng = np.random.default_rng(random_state)
    seeds = rng.choice(matrix.shape[0], size=n_clusters, replace=False)
    centroids = matrix[seeds].copy()

    labels = np.zeros(matrix.shape[0], dtype=int)
    for _ in range(max_iter):
        distances = np.linalg.norm(matrix[:, None, :] - centroids[None, :, :], axis=2)
        new_labels = distances.argmin(axis=1)
        if np.array_equal(new_labels, labels):
            break
        labels = new_labels

        new_centroids = centroids.copy()
        for cluster_index in range(n_clusters):
            members = matrix[labels == cluster_index]
            if len(members) == 0:
                new_centroids[cluster_index] = matrix[rng.integers(0, matrix.shape[0])]
            else:
                new_centroids[cluster_index] = members.mean(axis=0)
        if np.allclose(new_centroids, centroids):
            centroids = new_centroids
            break
        centroids = new_centroids

    return {
        "cluster_centers": centroids,
        "labels": labels,
        "n_iter": max_iter,
    }
