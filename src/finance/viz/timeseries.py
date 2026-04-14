from __future__ import annotations


def plot_over_time(
    df,
    x_col: str = "date",
    y_col: str = "close",
    color_col: str | None = None,
    title: str | None = None,
):
    try:
        import plotly.express as px
    except ImportError as exc:
        raise RuntimeError("Visualization dependency missing. Install: pip install plotly") from exc

    required = {x_col, y_col}
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required column(s): {missing}")

    figure = px.line(
        df,
        x=x_col,
        y=y_col,
        color=color_col,
        title=title or f"{y_col} over time",
    )
    figure.update_layout(xaxis_title=x_col, yaxis_title=y_col)
    return figure
