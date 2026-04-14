from __future__ import annotations

from typing import Any


def fit_transform_pca(
    X,
    n_components: int = 2,
    standardize: bool = True,
    random_state: int = 42,
) -> dict[str, Any]:
    try:
        import numpy as np
        from sklearn.decomposition import PCA
    except ImportError as exc:
        raise RuntimeError(
            "PCA dependencies missing. Install: pip install scikit-learn numpy"
        ) from exc

    matrix = np.asarray(X, dtype=float)
    if matrix.ndim != 2:
        raise ValueError("X must be a 2D array-like.")
    if n_components < 1:
        raise ValueError("n_components must be >= 1.")
    if n_components > matrix.shape[1]:
        raise ValueError("n_components cannot exceed number of input features.")

    if standardize:
        mean = matrix.mean(axis=0, keepdims=True)
        std = matrix.std(axis=0, keepdims=True)
        std[std == 0] = 1.0
        matrix = (matrix - mean) / std

    model = PCA(n_components=n_components, random_state=random_state)
    embedding = model.fit_transform(matrix)
    return {
        "embedding": embedding,
        "components": model.components_,
        "explained_variance_ratio": model.explained_variance_ratio_,
        "model": model,
    }
