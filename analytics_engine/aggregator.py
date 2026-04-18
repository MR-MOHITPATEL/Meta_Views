"""
Core aggregation engine — uses EXACT column names from Google Sheets:

Creative_Performance_View : Date, Campaign name, Ad name, pincode_day,
                             Spend, Impressions, Clicks, Purchases, Revenue,
                             CTR, CPC, CPM, CPT, CVR, ROAS
PC_Creative_Date_View     : Date, Pincode, Ad name, pincode_day,
                             Spend, Impressions, Clicks, Purchases, Revenue,
                             CTR, CPC, CPT, CVR
Daily_PC_Consumption      : Date, Pincode, pincode_day,
                             Spend, Impressions, Clicks, Purchases, Revenue,
                             CTR, CPT
Winning_Creatives_View    : Ad name, pincode_day,
                             Spend, Impressions, Clicks, Purchases, Revenue,
                             CTR, CPC, CPM, CPT, CVR, ROAS
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

# Additive columns present in each sheet (summed, not averaged)
_ADDITIVE = ["Spend", "Impressions", "Clicks", "Purchases", "Revenue"]

# Pre-computed ratio columns (keep as-is from sheet when not re-grouping)
_RATIOS = ["CTR", "CPC", "CPM", "CPT", "CVR", "ROAS", "pincode_day"]


# ── Filter ─────────────────────────────────────────────────────────────────────

def _apply_filters(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        if filters.get("last_n_days"):
            cutoff = pd.Timestamp(date.today() - timedelta(days=int(filters["last_n_days"])))
            df = df[df["Date"] >= cutoff]
        else:
            if filters.get("date_from"):
                df = df[df["Date"] >= pd.Timestamp(filters["date_from"])]
            if filters.get("date_to"):
                df = df[df["Date"] <= pd.Timestamp(filters["date_to"])]

    # Single campaign filter (legacy)
    # Adset filter
    if filters.get("adsets") and "Ad set name" in df.columns:
        adset_col = df["Ad set name"].astype(str).str.lower()
        mask = pd.Series(False, index=df.index)
        for a in filters["adsets"]:
            mask |= adset_col.str.contains(str(a).lower(), na=False)
        df = df[mask]

    if filters.get("campaign") and "Campaign name" in df.columns:
        df = df[df["Campaign name"].astype(str).str.lower().str.contains(
            filters["campaign"].lower(), na=False)]

    # Multi-campaign filter: list of names/numbers — OR match
    if filters.get("campaigns") and "Campaign name" in df.columns:
        cam_col = df["Campaign name"].astype(str).str.lower()
        mask = pd.Series(False, index=df.index)
        for c in filters["campaigns"]:
            mask |= cam_col.str.contains(str(c).lower(), na=False)
        df = df[mask]

    if filters.get("pincode") and "Pincode" in df.columns:
        df = df[df["Pincode"].astype(str) == str(filters["pincode"])]

    # ads_list: multiple ad names from sidebar (OR match)
    if filters.get("ads_list") and "Ad name" in df.columns:
        ad_col = df["Ad name"].astype(str).str.lower()
        mask = pd.Series(False, index=df.index)
        for a in filters["ads_list"]:
            mask |= ad_col.str.contains(str(a).lower(), na=False)
        df = df[mask]

    if filters.get("creative") and "Ad name" in df.columns:
        df = df[df["Ad name"].astype(str).str.lower().str.contains(
            filters["creative"].lower(), na=False)]

    # Numeric threshold filters: [{"column": "CPT", "op": "lt", "value": 250}, ...]
    _OPS = {"lt": "__lt__", "lte": "__le__", "gt": "__gt__", "gte": "__ge__", "eq": "__eq__"}
    for t in filters.get("thresholds") or []:
        col = t.get("column")
        op  = _OPS.get(t.get("op", ""), None)
        val = t.get("value")
        if col and op and val is not None and col in df.columns:
            df = df[getattr(df[col], op)(val)]

    return df


# ── Aggregation helpers ────────────────────────────────────────────────────────

def _pincode_days(df: pd.DataFrame) -> int:
    """
    Count DISTINCT (Date, Campaign name, Pincode).

    STRICT RULE:
    - Uniqueness is ALWAYS (Date, Campaign name, Pincode) only.
    - NEVER sum a pincode_day column — that double-counts pincodes shared
      across multiple creatives or adsets.
    - NEVER count rows directly.
    - Returns 0 if the 3 required columns are not all present.
    """
    _REQUIRED = ("Date", "Campaign name", "Pincode")
    key_cols = [c for c in _REQUIRED if c in df.columns]

    if len(key_cols) < 3:
        # Cannot compute correctly — do NOT fall back to summing pincode_day,
        # as that would double-count pincodes shared across creatives.
        return 0

    return len(df[list(key_cols)].drop_duplicates())


def _total_metrics(df: pd.DataFrame) -> dict:
    """Sum additive cols, recompute ratios."""
    m = {}
    for col in _ADDITIVE:
        if col in df.columns:
            m[col] = round(float(df[col].sum()), 2)
    m["Pincode Days"] = _pincode_days(df)
    _add_ratios(m)
    return m


def _add_ratios(m: dict) -> None:
    c = m.get("Clicks", 0)
    i = m.get("Impressions", 0)
    s = m.get("Spend", 0)
    p = m.get("Purchases", 0)
    r = m.get("Revenue", 0)
    if i > 0: m["CTR"]  = round(c / i, 4)
    if c > 0: m["CPC"]  = round(s / c, 2)
    if p > 0: m["CPT"]  = round(s / p, 2)
    if c > 0: m["CVR"]  = round(p / c, 4)
    if s > 0: m["ROAS"] = round(r / s, 2)


def _group_agg(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    """Group by cols, sum additive metrics, recompute ratios."""
    existing = [c for c in group_cols if c in df.columns]
    if not existing:
        return pd.DataFrame()
    add_cols = [c for c in _ADDITIVE if c in df.columns]
    agg = df.groupby(existing, dropna=False)[add_cols].sum().reset_index()

    # Carry forward image_url and ad_id — take first non-empty value per Ad name
    if "Ad name" in existing:
        for extra_col in ("image_url", "ad_id"):
            if extra_col in df.columns:
                col_map = (
                    df[["Ad name", extra_col]]
                    .replace("", pd.NA)
                    .dropna(subset=[extra_col])
                    .drop_duplicates("Ad name")
                    .set_index("Ad name")[extra_col]
                )
                agg[extra_col] = agg["Ad name"].map(col_map).fillna("")

    # Pincode Days per group.
    # STRICT RULE: uniqueness is ALWAYS (Date, Campaign name, Pincode).
    # ad_name and adset are NEVER part of the uniqueness key.
    # We NEVER sum a pincode_day column — it would double-count pincodes
    # shared across multiple creatives.
    _DEDUP = ("Date", "Campaign name", "Pincode")
    pc_key = [c for c in _DEDUP if c in df.columns]
    if len(pc_key) == 3:
        # Have all 3 dedup columns → compute correctly.
        # Select (group_cols + dedup_cols), dedup, then count per group.
        select_cols = list(dict.fromkeys(existing + pc_key))
        available   = [c for c in select_cols if c in df.columns]
        pc_map = (
            df[available]
            .drop_duplicates(subset=available)
            .groupby(existing, dropna=False)
            .size()
            .reset_index(name="Pincode Days")
        )
        agg = agg.merge(pc_map, on=existing, how="left")
    # If dedup cols are missing, skip Pincode Days entirely rather than
    # produce a wrong number by summing pincode_day across groups.

    # Recompute ratios
    c = agg.get("Clicks",      pd.Series(0, index=agg.index))
    i = agg.get("Impressions", pd.Series(0, index=agg.index))
    s = agg.get("Spend",       pd.Series(0, index=agg.index))
    p = agg.get("Purchases",   pd.Series(0, index=agg.index))
    r = agg.get("Revenue",     pd.Series(0, index=agg.index))

    if "Impressions" in agg and "Clicks" in agg:
        agg["CTR"]  = (c / i.replace(0, float("nan"))).round(4)
    if "Spend" in agg and "Clicks" in agg:
        agg["CPC"]  = (s / c.replace(0, float("nan"))).round(2)
    if "Spend" in agg and "Purchases" in agg:
        agg["CPT"]  = (s / p.replace(0, float("nan"))).round(2)
    if "Purchases" in agg and "Clicks" in agg:
        agg["CVR"]  = (p / c.replace(0, float("nan"))).round(4)
    if "Revenue" in agg and "Spend" in agg:
        agg["ROAS"] = (r / s.replace(0, float("nan"))).round(2)

    return agg


# ── 4 Query-type handlers ──────────────────────────────────────────────────────

def _q1_creative_pc_days(df: pd.DataFrame, intent: dict) -> dict:
    """
    Q1 — Creative-wise active PC Days + metrics.
    Primary group: Ad name. Optional secondary: Date (daywise).
    """
    group_cols = ["Ad name"]
    if intent.get("secondary_group_by") == "Date" and "Date" in df.columns:
        group_cols = ["Date", "Ad name"]

    table = _group_agg(df, group_cols)
    _sort(table, intent, default="Pincode Days")

    totals = _total_metrics(df)
    return _result(table, totals, intent,
                   f"Creative-wise PC Days. Group: {group_cols}. "
                   f"Total Pincode Days = {totals['Pincode Days']}")


def _q2_pc_wise(df: pd.DataFrame, intent: dict) -> dict:
    """
    Q2 — PC as primary identifier, breakdown by creative + date.
    """
    group_cols = [c for c in ("Pincode", "Ad name", "Date") if c in df.columns]
    table = _group_agg(df, group_cols)
    _sort(table, intent, default="Spend")

    totals = _total_metrics(df)
    return _result(table, totals, intent,
                   f"PC-wise breakdown. Pincode Days = {totals['Pincode Days']}")


def _q3_daily_consumption(df: pd.DataFrame, intent: dict) -> dict:
    """
    Q3 — Daily: how many / which PCs consumed, by creative.
    """
    group_cols = [c for c in ("Date", "Ad name", "Campaign name") if c in df.columns]
    table = _group_agg(df, group_cols)

    # Count unique pincodes per day
    if "Pincode" in df.columns and group_cols:
        pc_count = (
            df.groupby(group_cols, dropna=False)["Pincode"]
            .nunique()
            .reset_index(name="Unique PCs")
        )
        table = table.merge(pc_count, on=group_cols, how="left")

    _sort(table, intent, default="Date")

    totals = _total_metrics(df)
    unique_pcs = int(df["Pincode"].nunique()) if "Pincode" in df.columns else 0
    totals["Total Unique PCs"] = unique_pcs
    return _result(table, totals, intent,
                   f"Daily PC consumption. Total unique PCs = {unique_pcs}")


def _q4_winners(df: pd.DataFrame, intent: dict) -> dict:
    """
    Q4 — Winner creatives: lowest CPT + highest Purchases.

    ALWAYS re-aggregates by Ad name from whatever df is passed in.
    This means date/threshold filters applied before this call work correctly.

    Dataset routing (done in query_parser):
      - No date filter  → winning_creatives view (pre-computed lifetime totals)
      - Date filter set  → creative_performance view (has Date column, can be filtered)
    """
    group_cols = ["Ad name"] if "Ad name" in df.columns else []
    if not group_cols:
        return {"answer_value": 0, "metrics": {}, "table": None,
                "dataset_used": intent.get("dataset", ""), "method": "No Ad name column."}

    # Always re-aggregate — never use rows as-is (avoids date-filter bypass)
    table = _group_agg(df, group_cols)

    # Only creatives with real purchases are winners
    if "Purchases" in table.columns:
        table = table[table["Purchases"] > 0]
    if "CPT" in table.columns:
        table = table[table["CPT"] > 0]

    # Apply any user threshold filters (CPT<250, purchases>2 etc.)
    # These were already applied to df by _apply_filters, but re-check on the
    # aggregated table because some thresholds apply to aggregated CPT not raw CPT
    for t in (intent.get("filters") or {}).get("thresholds") or []:
        col = t.get("column")
        val = t.get("value")
        op  = t.get("op")
        _OPS = {"lt": "__lt__", "lte": "__le__", "gt": "__gt__", "gte": "__ge__", "eq": "__eq__"}
        if col and op and val is not None and col in table.columns:
            table = table[getattr(table[col], _OPS.get(op, "__gt__"))(val)]

    sort_cols = []
    sort_asc  = []
    if "CPT" in table.columns:
        sort_cols.append("CPT");       sort_asc.append(True)
    if "Purchases" in table.columns:
        sort_cols.append("Purchases"); sort_asc.append(False)
    if sort_cols:
        table = table.sort_values(sort_cols, ascending=sort_asc)

    lim   = intent.get("limit") or 20
    table = table.head(int(lim))

    filters  = intent.get("filters", {})
    has_date = filters.get("last_n_days") or filters.get("date_from") or filters.get("date_to")
    period   = f"last {filters['last_n_days']} days" if filters.get("last_n_days") \
               else (f"{filters.get('date_from')} → {filters.get('date_to')}" if has_date \
               else "all time")

    totals = _total_metrics(df)
    return _result(table, totals, intent,
                   f"Winners for {period} — ranked CPT asc + Purchases desc. Top {lim} shown.")


def _q5_pincode_count(df: pd.DataFrame, intent: dict) -> dict:
    """
    Q5 — Pincode count questions:
      "how many pincodes were used in last 7 days"
      "which pincodes were active this month"
      "list all pincodes used"
      "pincode wise daily breakdown"

    Works on pc_creative_date view which has a Pincode column.
    The Pincode column in this view is already a single code per row
    (exploded during view_builder), so nunique() is correct.

    Returns:
      - Total unique pincodes (scalar answer)
      - Daily breakdown table: Date → unique pincode count
      - List of the actual pincodes if user asked "which"/"list"
    """
    dataset = intent.get("dataset", "")
    sub     = intent.get("sub_intent", "count")   # count | list | daily

    if "Pincode" not in df.columns:
        return {
            "answer_value": 0,
            "metrics": {"Unique Pincodes": 0},
            "table": None,
            "dataset_used": dataset,
            "method": "No Pincode column available in this dataset.",
        }

    total_unique = int(df["Pincode"].nunique())

    # Daily breakdown: date → count of unique pincodes
    if "Date" in df.columns:
        daily = (
            df.groupby("Date")["Pincode"]
            .nunique()
            .reset_index()
            .rename(columns={"Pincode": "Unique Pincodes"})
            .sort_values("Date", ascending=False)
        )
        daily["Date"] = pd.to_datetime(daily["Date"]).dt.strftime("%Y-%m-%d")
    else:
        daily = pd.DataFrame()

    # List of actual pincodes (for "which" / "list" questions)
    pincode_list = (
        df["Pincode"]
        .dropna()
        .unique()
        .tolist()
    )
    pincode_list = sorted(str(p) for p in pincode_list)

    # If user asked "which" or "list", return the pincode list as the table
    if sub == "list":
        table = pd.DataFrame({"Pincode": pincode_list})
    else:
        table = daily

    totals = _total_metrics(df)
    totals["Unique Pincodes"] = total_unique

    return {
        "answer_value": total_unique,
        "metrics": totals,
        "table": table,
        "pincode_list": pincode_list,
        "dataset_used": dataset,
        "method": f"COUNT DISTINCT Pincode = {total_unique} unique pincodes",
    }


def _q8_comparison(df: pd.DataFrame, intent: dict) -> dict:
    """
    Q8 — Period-over-period or entity comparison.

    Two modes:
      A) Two time periods: "last week vs this week", "compare campaign 35 vs 36"
         → side-by-side table: entity | period1_metric | period2_metric | change%
      B) Two entities (campaigns/creatives) with same period
         → each entity summed, shown as rows for comparison
    """
    filters  = intent.get("filters", {})
    metric   = intent.get("metric", "Spend")
    group_by = intent.get("group_by", "Ad name")
    dataset  = intent.get("dataset", "creative_performance")

    p1 = filters.get("comparison_period_1") or {}
    p2 = filters.get("comparison_period_2") or {}

    # If Gemini populated comparison periods use them; otherwise split current range in half
    def _slice(f: dict) -> pd.DataFrame:
        sub = df.copy()
        if f.get("last_n_days"):
            cutoff = pd.Timestamp(date.today() - timedelta(days=int(f["last_n_days"])))
            sub = sub[sub["Date"] >= cutoff]
        if f.get("date_from"):
            sub = sub[sub["Date"] >= pd.Timestamp(f["date_from"])]
        if f.get("date_to"):
            sub = sub[sub["Date"] <= pd.Timestamp(f["date_to"])]
        return sub

    if p1 and p2:
        df1, df2 = _slice(p1), _slice(p2)
        label1 = f"Period 1 ({p1.get('date_from','')}-{p1.get('date_to','')})"
        label2 = f"Period 2 ({p2.get('date_from','')}-{p2.get('date_to','')})"
    else:
        # Split current filtered data by midpoint date
        if "Date" in df.columns and not df.empty:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            mid = df["Date"].median()
            df1 = df[df["Date"] < mid]
            df2 = df[df["Date"] >= mid]
            label1 = f"Before {mid.strftime('%Y-%m-%d')}"
            label2 = f"From {mid.strftime('%Y-%m-%d')}"
        else:
            df1, df2 = df, df
            label1, label2 = "Period 1", "Period 2"

    add_cols = [c for c in _ADDITIVE if c in df.columns]
    gcols1 = [c for c in [group_by] if c in df1.columns]
    gcols2 = [c for c in [group_by] if c in df2.columns]

    if not gcols1:
        return _result(pd.DataFrame(), _total_metrics(df), intent, "No group column for comparison.")

    t1 = df1.groupby(gcols1, dropna=False)[add_cols].sum().reset_index() if not df1.empty else pd.DataFrame()
    t2 = df2.groupby(gcols2, dropna=False)[add_cols].sum().reset_index() if not df2.empty else pd.DataFrame()

    # Rename metric columns with period labels
    if not t1.empty:
        t1 = t1.rename(columns={c: f"{c} ({label1})" for c in add_cols})
    if not t2.empty:
        t2 = t2.rename(columns={c: f"{c} ({label2})" for c in add_cols})

    if not t1.empty and not t2.empty:
        table = t1.merge(t2, on=gcols1, how="outer").fillna(0)
    elif not t1.empty:
        table = t1
    elif not t2.empty:
        table = t2
    else:
        table = pd.DataFrame()

    # Add change % for the primary metric if both periods present
    col1 = f"{metric} ({label1})"
    col2 = f"{metric} ({label2})"
    if col1 in table.columns and col2 in table.columns:
        table[f"{metric} Change%"] = (
            ((table[col2] - table[col1]) / table[col1].replace(0, float("nan"))) * 100
        ).round(1)

    totals = _total_metrics(df)
    return _result(table, totals, intent,
                   f"Comparison: {label1} vs {label2}. Grouped by {group_by}.")


def _q9_overview(df: pd.DataFrame, intent: dict) -> dict:
    """
    Q9 — Overview / health check for a campaign or creative.
    Returns: summary metrics + breakdown by Ad name + trend by Date.
    "How is Campaign 35 doing?", "Is campaign 1 profitable?", "Overall performance"
    """
    dataset  = intent.get("dataset", "creative_performance")
    group_by = intent.get("group_by", "Ad name")

    gcols = [c for c in [group_by] if c in df.columns]
    if not gcols:
        gcols = ["Ad name"] if "Ad name" in df.columns else []

    add_cols = [c for c in _ADDITIVE if c in df.columns]

    # Primary breakdown table
    if gcols:
        table = _group_agg(df, gcols)
        if "CPT" in table.columns:
            table = table.sort_values("CPT", na_position="last")
    else:
        table = pd.DataFrame()

    totals = _total_metrics(df)
    is_profitable = None
    if totals.get("ROAS"):
        is_profitable = totals["ROAS"] >= 1.0

    method_parts = [f"Overview grouped by {gcols}."]
    if is_profitable is not None:
        method_parts.append(f"ROAS={totals.get('ROAS', 0):.2f} → {'Profitable' if is_profitable else 'Not profitable'}.")

    return _result(table, totals, intent, " ".join(method_parts))


def _q7_campaign_detail(df: pd.DataFrame, intent: dict) -> dict:
    """
    Q7 — Campaign × Date × Ad name detail view.
    Shows the spreadsheet-style breakdown: one row per (Date, Campaign name, Ad name).
    Used when user asks for specific campaign(s) data with ad name context.
    """
    dataset = intent.get("dataset", "creative_performance")

    group_cols = [c for c in ("Date", "Campaign name", "Ad name") if c in df.columns]
    table = _group_agg(df, group_cols)

    if "Date" in table.columns:
        table["Date"] = pd.to_datetime(table["Date"]).dt.strftime("%Y-%m-%d")
        table = table.sort_values(
            ["Date", "Campaign name"] if "Campaign name" in table.columns else ["Date"],
            ascending=False
        ).reset_index(drop=True)

    totals = _total_metrics(df)
    camps = sorted(df["Campaign name"].unique().tolist()) if "Campaign name" in df.columns else []

    return _result(table, totals, intent,
                   f"Campaign detail — {len(camps)} campaign(s), "
                   f"{len(table)} rows (Date × Campaign × Ad name).")


def _q6_daily_pincode(df: pd.DataFrame, intent: dict) -> dict:
    """
    Q6 — Daily pincode breakdown: one row per (Date, Pincode).
    Shows which pincodes were active on each day + their Spend, Purchases, etc.

    Uses pc_creative_date view which has Date, Pincode, Ad name as the key.
    Groups by (Date, Pincode) to collapse across Ad names.
    """
    dataset = intent.get("dataset", "pc_creative_date")

    if "Pincode" not in df.columns:
        return {
            "answer_value": 0, "metrics": {}, "table": None,
            "dataset_used": dataset,
            "method": "No Pincode column available.",
        }

    group_cols = [c for c in ("Date", "Pincode") if c in df.columns]
    table = _group_agg(df, group_cols)

    if "Date" in table.columns:
        table["Date"] = pd.to_datetime(table["Date"]).dt.strftime("%Y-%m-%d")
        table = table.sort_values("Date", ascending=False).reset_index(drop=True)

    totals = _total_metrics(df)
    total_unique = int(df["Pincode"].nunique()) if "Pincode" in df.columns else 0
    totals["Total Unique PCs"] = total_unique

    return _result(table, totals, intent,
                   f"Daily Pincode breakdown — {len(table)} (Date, Pincode) rows, "
                   f"{total_unique} unique pincodes.")


# ── Helpers ────────────────────────────────────────────────────────────────────

def _sort(table: pd.DataFrame, intent: dict, default: str = "Spend") -> None:
    col = intent.get("sort_by") or default
    if col not in table.columns:
        col = default
    if col in table.columns:
        asc = intent.get("sort_order", "desc") == "asc"
        table.sort_values(col, ascending=asc, inplace=True, ignore_index=True)


def _result(table: pd.DataFrame, totals: dict, intent: dict, method: str) -> dict:
    lim = intent.get("limit")
    if lim and not table.empty:
        table = table.head(int(lim))
    return {
        "answer_value": table,
        "metrics": totals,
        "table": table,
        "dataset_used": intent.get("dataset", ""),
        "method": method,
    }


# ── Main entry point ───────────────────────────────────────────────────────────

def compute(df: pd.DataFrame, intent: dict) -> dict:
    filters    = intent.get("filters", {})
    query_type = intent.get("query_type", "generic")
    metric     = intent.get("metric", "Spend")
    group_by   = intent.get("group_by")
    dataset    = intent.get("dataset", "")

    df = _apply_filters(df, filters)

    if df.empty:
        return {
            "answer_value": 0, "metrics": {}, "table": None,
            "dataset_used": dataset,
            "method": "No rows matched the applied filters.",
        }

    if query_type == "creative_pc_days":
        return _q1_creative_pc_days(df, intent)
    if query_type == "pc_wise":
        return _q2_pc_wise(df, intent)
    if query_type == "daily_consumption":
        return _q3_daily_consumption(df, intent)
    if query_type == "winners":
        return _q4_winners(df, intent)
    if query_type == "pincode_count":
        return _q5_pincode_count(df, intent)
    if query_type == "daily_pincode":
        return _q6_daily_pincode(df, intent)
    if query_type == "campaign_detail":
        return _q7_campaign_detail(df, intent)
    if query_type == "comparison":
        return _q8_comparison(df, intent)
    if query_type == "overview":
        return _q9_overview(df, intent)

    # ── Generic ───────────────────────────────────────────────────────────────
    if metric in ("pincode_days", "pincode_day", "Pincode Days"):
        pd_count = _pincode_days(df)
        return {
            "answer_value": pd_count,
            "metrics": {"Pincode Days": pd_count},
            "table": None, "dataset_used": dataset,
            "method": "Unique (Date, Campaign name, Pincode) count",
        }

    if group_by:
        gcols = [group_by]
        for extra_key in ("secondary_group_by", "tertiary_group_by"):
            col = intent.get(extra_key)
            if col and col in df.columns and col not in gcols:
                gcols.append(col)
        table = _group_agg(df, gcols)
        _sort(table, intent, default=metric if metric in table.columns else "Spend")
        lim = intent.get("limit")
        if lim:
            table = table.head(int(lim))
        return _result(table, _total_metrics(df), intent,
                       f"Grouped by {gcols}")

    totals = _total_metrics(df)
    value  = totals.get(metric, totals.get("Spend", 0))
    return {
        "answer_value": value,
        "metrics": totals,
        "table": pd.DataFrame([totals]),
        "dataset_used": dataset,
        "method": f"Total {metric} across {len(df)} rows",
    }
