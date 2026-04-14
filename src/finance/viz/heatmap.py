from __future__ import annotations


def feature_correlation_heatmap(
    df,
    feature_cols: list[str] | None = None,
    exclude_cols: list[str] | None = None,
    method: str = "spearman",
    sample_n: int | None = None,
    mask_upper: bool = True,
    title: str | None = None,
):
    try:
        import numpy as np
        import pandas as pd
        import plotly.express as px
    except ImportError as exc:
        raise RuntimeError(
            "Visualization dependencies missing. Install: pip install plotly pandas numpy"
        ) from exc

    valid_methods = {"pearson", "spearman", "kendall"}
    if method not in valid_methods:
        raise ValueError(f"Unsupported method '{method}'. Use one of: {sorted(valid_methods)}")

    if sample_n is not None and sample_n <= 0:
        raise ValueError("sample_n must be > 0 when provided.")

    working = df
    if sample_n is not None and len(working) > sample_n:
        working = working.sample(n=sample_n, random_state=42)

    excluded = set(exclude_cols or [])
    if feature_cols is None:
        numeric = working.select_dtypes(include=["number"]).columns.tolist()
        cols = [column for column in numeric if column not in excluded]
    else:
        cols = [column for column in feature_cols if column not in excluded]

    if len(cols) < 2:
        raise ValueError("Need at least 2 feature columns for a correlation heatmap.")

    corr = working[cols].corr(method=method)
    if mask_upper:
        mask = np.triu(np.ones(corr.shape), k=1).astype(bool)
        corr = corr.mask(mask)

    fig = px.imshow(
        corr,
        zmin=-1,
        zmax=1,
        color_continuous_scale="RdBu",
        origin="lower",
        title=title or f"Feature Correlation Heatmap ({method})",
    )
    fig.update_layout(coloraxis_colorbar=dict(title="corr"))
    return fig
