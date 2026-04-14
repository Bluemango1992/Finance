from __future__ import annotations

from typing import Any


def fit_transform_umap(
    X,
    n_components: int = 2,
    n_neighbors: int = 15,
    min_dist: float = 0.1,
    metric: str = "euclidean",
    random_state: int = 42,
) -> dict[str, Any]:
    try:
        import numpy as np
        import umap
    except ImportError as exc:
        raise RuntimeError(
            "UMAP dependencies missing. Install: pip install umap-learn numpy"
        ) from exc

    matrix = np.asarray(X, dtype=float)
    if matrix.ndim != 2:
        raise ValueError("X must be a 2D array-like.")
    if n_components < 1:
        raise ValueError("n_components must be >= 1.")
    if n_neighbors < 2:
        raise ValueError("n_neighbors must be >= 2.")
    if min_dist < 0:
        raise ValueError("min_dist must be >= 0.")

    model = umap.UMAP(
        n_components=n_components,
        n_neighbors=n_neighbors,
        min_dist=min_dist,
        metric=metric,
        random_state=random_state,
    )
    embedding = model.fit_transform(matrix)
    return {"embedding": embedding, "model": model}
