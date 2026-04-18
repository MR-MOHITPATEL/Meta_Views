"""
query_layer.py — Filter and select data from precomputed views.

RULES:
1. DO NOT compute aggregations (sum/avg) from scratch.
2. ONLY apply filters (Date, Campaign, Pincode).
3. Select relevant columns based on the query intent.
4. Returns: {"result": value, "data": DataFrame, "summary": dict}
"""

import pandas as pd
from typing import Optional
from analytics.config import (
    COL_DATE, COL_CAMPAIGN, COL_SPEND, COL_PINCODES
)

def apply_view_filters(
    df: pd.DataFrame,
    sheet_name: str,
    time_filter_days: Optional[int] = None,
    campaign_filter: Optional[str] = None,
    pincode_filter: Optional[str] = None,
) -> dict:
    """
    Step 3: APPLY FILTERS
    Step 4: GENERATE ANSWER
    Step 5: TABLE OUTPUT
    """
    if df.empty:
        return {"result": 0, "data": pd.DataFrame(), "summary": {"msg": "No data available."}}

    work = df.copy()

    # Apply Time Filter (Step 3)
    if time_filter_days and COL_DATE in work.columns:
        work[COL_DATE] = pd.to_datetime(work[COL_DATE]).dt.date
        today = pd.to_datetime("today").date()
        cutoff = today - pd.Timedelta(days=time_filter_days)
        work = work[work[COL_DATE] >= cutoff]

    # Apply Campaign Filter
    if campaign_filter and COL_CAMPAIGN in work.columns:
        work = work[work[COL_CAMPAIGN].str.contains(campaign_filter, case=False, na=False)]

    # Apply Pincode Filter
    pincode_col = "pincode" # Normalised name for Pincode
    if pincode_filter and pincode_col in work.columns:
        work = work[work[pincode_col].astype(str) == str(pincode_filter)]

    # Step 5: Relevant Columns
    cols_to_show = work.columns.tolist()
    
    # Hide technical or redundant columns if necessary
    hidden = ["pincode_day_count"] # example
    cols_to_show = [c for c in cols_to_show if c not in hidden]
    
    result_df = work[cols_to_show]

    # Step 4: Generate Answer — compute deterministic values from the dataset
    total_spend = round(result_df[COL_SPEND].sum(), 2) if COL_SPEND in result_df.columns else 0
    total_purchases = int(result_df["purchases"].sum()) if "purchases" in result_df.columns else 0
    total_clicks = int(result_df["link_clicks"].sum()) if "link_clicks" in result_df.columns else (
        int(result_df["clicks"].sum()) if "clicks" in result_df.columns else 0
    )
    total_impressions = int(result_df["impressions"].sum()) if "impressions" in result_df.columns else 0
    pincode_days = int(result_df["pincode_day"].sum()) if "pincode_day" in result_df.columns else len(result_df)
    
    summary = {
        "sheet_used": sheet_name,
        "total_records": len(result_df),
        "total_spend": total_spend,
        "total_purchases": total_purchases,
        "total_clicks": total_clicks,
        "total_impressions": total_impressions,
        "total_pincode_days": pincode_days,
        "columns": cols_to_show
    }

    return {
        "result": pincode_days,
        "data": result_df,
        "summary": summary
    }


def compute_direct_answer(question: str, summary: dict) -> str:
    """
    Deterministic answer engine: compute answer from pre-calculated summary.
    Returns a formatted answer string without calling any LLM.
    """
    q = question.lower()

    if "pincode day" in q or "pincode days" in q:
        val = summary.get("total_pincode_days", 0)
        return f"**{val:,} Pincode Days** (unique Date × Campaign × Pincode combinations)"

    if "spend" in q:
        val = summary.get("total_spend", 0)
        return f"**₹{val:,.2f}** total spend across {summary.get('total_records', 0):,} records"

    if "purchase" in q or "conversion" in q or "order" in q:
        val = summary.get("total_purchases", 0)
        return f"**{val:,} Purchases** from {summary.get('total_records', 0):,} records"

    if "click" in q:
        val = summary.get("total_clicks", 0)
        return f"**{val:,} Clicks** from {summary.get('total_records', 0):,} records"

    if "impression" in q:
        val = summary.get("total_impressions", 0)
        return f"**{val:,} Impressions** from {summary.get('total_records', 0):,} records"

    # Generic fallback
    return f"Found **{summary.get('total_records', 0):,} records** from `{summary.get('sheet_used', 'dataset')}`"
