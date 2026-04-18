"""
Builds Plotly charts from aggregation results.
Chart type is chosen automatically based on query intent.
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def build_chart(
    table: pd.DataFrame | None,
    intent: dict,
    result: dict,
) -> go.Figure | None:
    """
    Returns a Plotly Figure or None if charting isn't applicable.

    intent: structured dict from query_parser
    result: dict from aggregator.compute()
    """
    if table is None or table.empty:
        return None

    query_intent = intent.get("intent", "total")
    metric       = intent.get("metric", "spend")
    group_by     = intent.get("group_by")
    dataset      = intent.get("dataset", "")

    # ── Time-series trend ──────────────────────────────────────────────────────
    date_col = next((c for c in ("Date", "date") if c in table.columns), None)
    if date_col and query_intent == "trend":
        y_col = metric if metric in table.columns else _pick_y(table)
        fig = px.line(
            table.sort_values(date_col),
            x=date_col,
            y=y_col,
            title=f"{y_col} over time",
            markers=True,
        )
        return fig

    # ── Grouped bar chart ──────────────────────────────────────────────────────
    if group_by and group_by in table.columns:
        y_col = metric if metric in table.columns else _pick_y(table)
        # Limit to top 20 for readability
        plot_df = table.head(20).sort_values(y_col, ascending=True)
        fig = px.bar(
            plot_df,
            x=y_col,
            y=group_by,
            orientation="h",
            title=f"{y_col} by {group_by}",
            text_auto=True,
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        return fig

    # ── Single-value scorecard ─────────────────────────────────────────────────
    if query_intent == "total" and result.get("metrics"):
        metrics = result["metrics"]
        display = {
            k: v for k, v in metrics.items()
            if isinstance(v, (int, float)) and k not in ("_parse_error",)
        }
        if not display:
            return None

        labels = list(display.keys())
        values = [display[k] for k in labels]

        fig = go.Figure(go.Bar(x=labels, y=values, text=values, textposition="outside"))
        fig.update_layout(title="Metric Summary", xaxis_title="Metric", yaxis_title="Value")
        return fig

    return None


def _pick_y(df: pd.DataFrame) -> str:
    """Pick the first numeric non-date column as the Y axis."""
    for col in ("Spend", "Purchases", "Clicks", "Impressions",
                "Pincode Days", "ROAS", "CTR", "CPT"):
        if col in df.columns:
            return col
    numeric = df.select_dtypes("number").columns.tolist()
    return numeric[0] if numeric else df.columns[-1]
