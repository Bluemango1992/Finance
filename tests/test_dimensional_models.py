import numpy as np
import pytest

from finance.models.dimensional.pca import fit_transform_pca
from finance.models.dimensional.umap_model import fit_transform_umap


def test_fit_transform_pca_shape_and_variance() -> None:
    rng = np.random.default_rng(42)
    X = rng.normal(size=(120, 8))
    result = fit_transform_pca(X, n_components=3)

    assert result["embedding"].shape == (120, 3)
    assert result["components"].shape == (3, 8)
    assert result["explained_variance_ratio"].shape == (3,)
    assert float(result["explained_variance_ratio"].sum()) > 0


def test_fit_transform_pca_rejects_bad_components() -> None:
    X = np.random.default_rng(42).normal(size=(20, 4))
    with pytest.raises(ValueError, match="n_components"):
        fit_transform_pca(X, n_components=5)


def test_fit_transform_umap_shape() -> None:
    pytest.importorskip("umap")

    rng = np.random.default_rng(42)
    X = rng.normal(size=(80, 6))
    result = fit_transform_umap(X, n_components=2, n_neighbors=10, min_dist=0.05)
    assert result["embedding"].shape == (80, 2)
