"""
Builds all 4 analytical views from the Raw Dump DataFrame.

Pipeline per view:
  1. normalize_raw_dump()  — rename columns, coerce types
  2. explode_pincodes()    — extract 6-digit codes, one row per pincode
  3. View-specific aggregation with correct pincode_day logic

Pincode Day = COUNT DISTINCT (Date, Campaign name, Pincode)
  - Always computed AFTER explosion so clustered pincodes are never undercounted
  - Always deduplicated so multi-creative / multi-adset rows never inflate the count
  - ad_name and adset are NEVER part of the uniqueness key

Supports TWO Raw Dump column name formats:
  OLD (original Meta export):
    Pincodes, Campaign name, Ad set name, Ad name, Date,
    Amount spent (INR), Link clicks, Website landing views,
    C2V Ratio, Cost per result, Roas, Adds to cart, Results value

  NEW (pipeline output / snake_case):
    pincodes, campaign_name, adset_name, ad_name, date,
    spend, link_clicks, landing_page_views, c2v_ratio,
    cost_per_result, roas, add_to_cart, revenue
"""

from __future__ import annotations

import logging
import re

import pandas as pd

logger = logging.getLogger(__name__)

# ── Column rename map: Raw Dump → internal names ───────────────────────────────
# Maps BOTH old Meta export headers AND new pipeline snake_case headers
_COL_MAP = {
    # ── OLD format (original Meta export) ───────────────────────────────────
    "Pincodes":              "Pincode",
    "Campaign name":         "Campaign name",
    "Ad set name":           "Ad set name",
    "Ad name":               "Ad name",
    "Date":                  "Date",
    "Amount spent (INR)":    "Spend",
    "Impressions":           "Impressions",
    "CPM":                   "CPM",
    "CPC":                   "CPC",
    "CTR":                   "CTR",
    "Link clicks":           "Clicks",
    "Website landing views": "Landing Page Views",
    "C2V Ratio":             "C2V Ratio",
    "Result type":           "Result type",
    "Cost per result":       "Cost per result",
    "Roas":                  "ROAS",
    "CVR":                   "CVR",
    "Adds to cart":          "Add to Cart",
    "Purchases":             "Purchases",
    "Results value":         "Revenue",

    # ── NEW format (pipeline output / snake_case) ────────────────────────────
    "date":                  "Date",
    "campaign_name":         "Campaign name",
    "adset_name":            "Ad set name",
    "ad_name":               "Ad name",
    "pincodes":              "Pincode",
    "image_url":             "image_url",   # pass through unchanged
    "ad_id":                 "ad_id",       # needed to refresh expired image URLs
    "spend":                 "Spend",
    "impressions":           "Impressions",
    "cpm":                   "CPM",
    "cpc":                   "CPC",
    "ctr":                   "CTR",
    "link_clicks":           "Clicks",
    "landing_page_views":    "Landing Page Views",
    "c2v_ratio":             "C2V Ratio",
    "cost_per_result":       "Cost per result",
    "roas":                  "ROAS",
    "cvr":                   "CVR",
    "add_to_cart":           "Add to Cart",
    "purchases":             "Purchases",
    "revenue":               "Revenue",
}

_ADDITIVE = ["Spend", "Impressions", "Clicks", "Purchases", "Revenue",
             "Add to Cart", "Landing Page Views"]

# Columns that define a unique Pincode Day — NEVER change this set
_DEDUP_COLS = ["Date", "Campaign name", "Pincode"]


# ── Step 1: Normalise ──────────────────────────────────────────────────────────

def normalize_raw_dump(df: pd.DataFrame) -> pd.DataFrame:
    """Strip column names, rename to internal standard, coerce numeric types."""
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns=_COL_MAP)

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df[df["Date"].notna()]

    for col in _ADDITIVE + ["CPM", "CPC", "CTR", "ROAS", "CVR"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df.reset_index(drop=True)


# ── Step 2: Extract & explode pincodes ────────────────────────────────────────

def _extract_pincodes(raw) -> list[str]:
    """
    Extract all 6-digit numeric pincodes from a raw Pincode cell.

    Handles all observed formats:
      "Pimpri (411018, 411019, 411033)"  → ["411018","411019","411033"]
      "Sangli-Miraj: 416410, 416414"     → ["416410","416414"]
      "Aurangabad(431001)"               → ["431001"]
      "NM: 400703, 410208"              → ["400703","410208"]

    Strategy: extract every 6-digit sequence — nothing else.

    Also handles numeric types (int/float) from Google Sheets — gspread returns
    plain-number pincode cells as int (e.g. 421301) rather than str ("421301"),
    which would otherwise be silently dropped.
    """
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return []
    # Coerce numbers (int / float) to string before regex search
    raw = str(raw).strip()
    if not raw or raw in ("nan", "None", "0"):
        return []
    return re.findall(r'\b\d{6}\b', raw)


def explode_pincodes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Step 2 — Convert each raw row into one row per extracted pincode.

    Order matters:
      • Extraction happens BEFORE deduplication.
      • Each original row's metrics are carried to every exploded child row.
      • Rows with no extractable pincodes are dropped (they carry no geo signal).

    The resulting DataFrame has one row per (date, campaign, ad_name, pincode)
    combination, ready for correct deduplication.
    """
    if "Pincode" not in df.columns:
        logger.warning("No 'Pincode' column found — skipping explosion.")
        return df

    df = df.copy()
    df["Pincode"] = df["Pincode"].apply(_extract_pincodes)

    before = len(df)
    # Drop rows that yielded no pincodes
    df = df[df["Pincode"].map(len) > 0]
    dropped = before - len(df)
    if dropped:
        logger.info(f"Dropped {dropped} rows with no extractable pincodes.")

    # Explode: one row per pincode
    df = df.explode("Pincode").reset_index(drop=True)
    df["Pincode"] = df["Pincode"].astype(str).str.strip()

    logger.info(f"After explosion: {len(df)} rows "
                f"({len(df['Pincode'].unique())} unique pincodes)")
    return df


# ── Step 3: Shared metric helpers ──────────────────────────────────────────────

def _add_ratios(agg: pd.DataFrame) -> pd.DataFrame:
    """Recompute CTR, CPC, CPT, CVR, ROAS after summing additive columns."""
    c = agg["Clicks"]      if "Clicks"      in agg.columns else pd.Series(0, index=agg.index)
    i = agg["Impressions"] if "Impressions" in agg.columns else pd.Series(0, index=agg.index)
    s = agg["Spend"]       if "Spend"       in agg.columns else pd.Series(0, index=agg.index)
    p = agg["Purchases"]   if "Purchases"   in agg.columns else pd.Series(0, index=agg.index)
    r = agg["Revenue"]     if "Revenue"     in agg.columns else pd.Series(0, index=agg.index)

    if "Impressions" in agg.columns and "Clicks" in agg.columns:
        agg["CTR"]  = (c / i.replace(0, float("nan"))).round(4)
    if "Spend" in agg.columns and "Clicks" in agg.columns:
        agg["CPC"]  = (s / c.replace(0, float("nan"))).round(2)
    if "Spend" in agg.columns and "Purchases" in agg.columns:
        agg["CPT"]  = (s / p.replace(0, float("nan"))).round(2)
    if "Purchases" in agg.columns and "Clicks" in agg.columns:
        agg["CVR"]  = (p / c.replace(0, float("nan"))).round(4)
    if "Revenue" in agg.columns and "Spend" in agg.columns:
        agg["ROAS"] = (r / s.replace(0, float("nan"))).round(2)

    return agg


def _pincode_day_per_group(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    """
    Count DISTINCT (Date, Campaign name, Pincode) combos within each group.

    RULES:
      - Uniqueness key is always ONLY (Date, Campaign name, Pincode).
      - ad_name and adset are NEVER included in the uniqueness key.
      - Called on already-exploded df, so each Pincode is a single code.
      - drop_duplicates on (group_cols + _DEDUP_COLS) removes rows where the
        same (date, campaign, pincode) appears in the same group due to multiple
        creatives or adsets — then we count the remaining rows.
    """
    missing = [c for c in _DEDUP_COLS if c not in df.columns]
    if missing:
        logger.warning("pincode_day skipped — missing columns: %s", missing)
        return pd.DataFrame(columns=group_cols + ["pincode_day"])

    select_cols = list(dict.fromkeys(group_cols + _DEDUP_COLS))
    available   = [c for c in select_cols if c in df.columns]

    return (
        df[available]
        .drop_duplicates(subset=available)
        .groupby([c for c in group_cols if c in df.columns], dropna=False)
        .size()
        .reset_index(name="pincode_day")
    )


def _fmt_date(df: pd.DataFrame) -> pd.DataFrame:
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
    return df


# ── Step 4: View builders ──────────────────────────────────────────────────────

def build_creative_performance_view(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creative_Performance_View
    Grouped by: Date, Campaign name, Ad name
    Additive:   Spend, Impressions, Clicks, Purchases, Revenue
    Derived:    CPT, CTR, CPC, CVR, ROAS
    Extra:      pincode_day = DISTINCT (Date, Campaign name, Pincode) per group
    """
    group_cols = [c for c in ("Date", "Campaign name", "Ad name") if c in df.columns]
    add_cols   = [c for c in _ADDITIVE if c in df.columns]

    # Sum additive metrics — at this point df is already exploded, so each
    # (date, campaign, ad_name, pincode) row contributes once. However the
    # spend/impressions on the original raw row should NOT be multiplied by
    # the number of pincodes. We must sum on the PRE-explosion base.
    # See note in build_all_views() — we pass pre-exploded df for metrics
    # and exploded df only for pincode_day.
    agg = df.groupby(group_cols, dropna=False)[add_cols].sum().reset_index()
    agg = _add_ratios(agg)

    pc = _pincode_day_per_group(df, group_cols)
    agg = agg.merge(pc, on=group_cols, how="left")
    agg["pincode_day"] = agg["pincode_day"].fillna(0).astype(int)

    agg = _fmt_date(agg)
    agg = agg.sort_values("Date", ascending=False).reset_index(drop=True)
    logger.info(f"Creative_Performance_View: {len(agg)} rows")
    return agg


def build_pc_creative_date_view(df: pd.DataFrame) -> pd.DataFrame:
    """
    PC_Creative_Date_View
    Grouped by: Date, Pincode, Ad name
    Additive:   Spend, Impressions, Clicks, Purchases, Revenue
    Derived:    CPT, CTR, CPC, CVR, ROAS
    Extra:      pincode_day, Campaign name (first per group)
    """
    group_cols = [c for c in ("Date", "Pincode", "Ad name") if c in df.columns]
    add_cols   = [c for c in _ADDITIVE if c in df.columns]

    agg = df.groupby(group_cols, dropna=False)[add_cols].sum().reset_index()
    agg = _add_ratios(agg)

    pc = _pincode_day_per_group(df, group_cols)
    agg = agg.merge(pc, on=group_cols, how="left")
    agg["pincode_day"] = agg["pincode_day"].fillna(0).astype(int)

    if "Campaign name" in df.columns:
        camp = df.groupby(group_cols, dropna=False)["Campaign name"].first().reset_index()
        agg = agg.merge(camp, on=group_cols, how="left")

    agg = _fmt_date(agg)
    agg = agg.sort_values("Date", ascending=False).reset_index(drop=True)
    logger.info(f"PC_Creative_Date_View: {len(agg)} rows")
    return agg


def build_daily_pc_consumption_view(df: pd.DataFrame) -> pd.DataFrame:
    """
    Daily_PC_Consumption
    Definition: pincode_day per date = DISTINCT (Date, Campaign name, Pincode).

    Steps (on already-exploded df):
      1. Select (Date, Campaign name, Pincode), drop_duplicates
         → each remaining row is one unique pincode-day atom
      2. Group by Date, count rows → pincode_day per date
      3. Join with daily summed metrics
    """
    missing = [c for c in _DEDUP_COLS if c not in df.columns]
    if missing:
        logger.error("Cannot build Daily_PC_Consumption — missing: %s", missing)
        return pd.DataFrame()

    add_cols = [c for c in _ADDITIVE if c in df.columns]

    # Step 1 + 2: canonical pincode_day per date
    deduped = (
        df[_DEDUP_COLS]
        .drop_duplicates()
    )
    pc_per_day = (
        deduped
        .groupby("Date", dropna=False)
        .size()
        .reset_index(name="pincode_day")
    )

    # Step 3: daily totals (sum over all exploded rows — see spend note below)
    daily_metrics = df.groupby("Date", dropna=False)[add_cols].sum().reset_index()
    daily_metrics = _add_ratios(daily_metrics)

    result = daily_metrics.merge(pc_per_day, on="Date", how="left")
    result["pincode_day"] = result["pincode_day"].fillna(0).astype(int)

    if "Campaign name" in df.columns:
        camp = df.groupby("Date", dropna=False)["Campaign name"].first().reset_index()
        result = result.merge(camp, on="Date", how="left")

    result = _fmt_date(result)
    result = result.sort_values("Date", ascending=False).reset_index(drop=True)
    logger.info(f"Daily_PC_Consumption: {len(result)} rows")
    return result


def build_winning_creatives_view(df: pd.DataFrame) -> pd.DataFrame:
    """
    Winning_Creatives_View
    Grouped by: Ad name (lifetime totals — no date dimension)
    Additive:   Spend, Purchases, Clicks, Impressions, Revenue
    Derived:    CPT, CTR, CPC, CVR, ROAS
    Extra:      pincode_day = total DISTINCT (Date, Campaign name, Pincode) per creative

    CPT/purchase threshold filtering is applied at query time in the UI.
    All creatives are included here.
    """
    if "Ad name" not in df.columns:
        return pd.DataFrame()

    group_cols = ["Ad name"]
    add_cols   = [c for c in _ADDITIVE if c in df.columns]

    agg = df.groupby(group_cols, dropna=False)[add_cols].sum().reset_index()
    agg = _add_ratios(agg)

    pc = _pincode_day_per_group(df, group_cols)
    agg = agg.merge(pc, on=group_cols, how="left")
    agg["pincode_day"] = agg["pincode_day"].fillna(0).astype(int)

    if "CPT" in agg.columns:
        agg = agg.sort_values(["CPT", "Purchases"], ascending=[True, False],
                              na_position="last")

    agg = agg.reset_index(drop=True)
    logger.info(f"Winning_Creatives_View: {len(agg)} rows")
    return agg


def build_pincode_creative_view(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pincode_Creative_View
    Grouped by: Pincode, Ad name (lifetime — no date dimension)
    Additive:   Spend, Impressions, Clicks, Purchases, Revenue
    Derived:    CPT, CTR, CPC, CVR, ROAS
    Extra:      pincode_day = DISTINCT (Date, Campaign name, Pincode) per group

    Use: "which pincodes generate highest purchases by creative",
         "top pincodes by ROAS", "pincode performance by creative"
    """
    if "Pincode" not in df.columns or "Ad name" not in df.columns:
        return pd.DataFrame()

    group_cols = ["Pincode", "Ad name"]
    add_cols   = [c for c in _ADDITIVE if c in df.columns]

    agg = df.groupby(group_cols, dropna=False)[add_cols].sum().reset_index()
    agg = _add_ratios(agg)

    pc = _pincode_day_per_group(df, group_cols)
    agg = agg.merge(pc, on=group_cols, how="left")
    agg["pincode_day"] = agg["pincode_day"].fillna(0).astype(int)

    if "CPT" in agg.columns:
        agg = agg.sort_values(["Purchases", "CPT"], ascending=[False, True],
                              na_position="last")

    agg = agg.reset_index(drop=True)
    logger.info(f"Pincode_Creative_View: {len(agg)} rows")
    return agg


def build_campaign_performance_view(df: pd.DataFrame) -> pd.DataFrame:
    """
    Campaign_Performance_View
    Grouped by: Date, Campaign name
    Additive:   Spend, Impressions, Clicks, Purchases, Revenue
    Derived:    CPT, CTR, CPC, CVR, ROAS
    Extra:      pincode_day = DISTINCT (Date, Campaign name, Pincode) per day

    Use: "campaign wise performance", "which campaign has highest spend",
         "daily campaign breakdown", "campaign wise purchases last 30 days"
    """
    if "Campaign name" not in df.columns:
        return pd.DataFrame()

    group_cols = [c for c in ("Date", "Campaign name") if c in df.columns]
    add_cols   = [c for c in _ADDITIVE if c in df.columns]

    agg = df.groupby(group_cols, dropna=False)[add_cols].sum().reset_index()
    agg = _add_ratios(agg)

    pc = _pincode_day_per_group(df, group_cols)
    agg = agg.merge(pc, on=group_cols, how="left")
    agg["pincode_day"] = agg["pincode_day"].fillna(0).astype(int)

    agg = _fmt_date(agg)
    agg = agg.sort_values("Date", ascending=False).reset_index(drop=True)
    logger.info(f"Campaign_Performance_View: {len(agg)} rows")
    return agg


# ── Main entry point ───────────────────────────────────────────────────────────

def build_all_views(raw_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Build all 4 views from the raw dump DataFrame.

    IMPORTANT — spend / metric inflation problem:
    When a raw row contains "Pimpri (411018, 411019, 411033)", exploding it
    into 3 rows would triple-count the spend for that row.

    Solution: keep TWO copies of the data.
      • metric_df  — pre-explosion (original rows), used for summing Spend etc.
      • pc_df      — post-explosion (one row per pincode), used ONLY for
                     pincode_day counting via _pincode_day_per_group().

    All view builders call groupby on metric_df for additive metrics, and
    _pincode_day_per_group on pc_df for the pincode_day count.
    """
    logger.info(f"Building views from {len(raw_df)} raw rows…")
    metric_df = normalize_raw_dump(raw_df)
    logger.info(f"After normalisation: {len(metric_df)} valid rows")

    pc_df = explode_pincodes(metric_df.copy())

    views = {
        "creative_performance": _build_creative_performance(metric_df, pc_df),
        "pc_creative_date":     _build_pc_creative_date(metric_df, pc_df),
        "daily_pc_consumption": _build_daily_pc_consumption(metric_df, pc_df),
        "winning_creatives":    _build_winning_creatives(metric_df, pc_df),
        "pincode_creative":     _build_pincode_creative(metric_df, pc_df),
        "campaign_performance": _build_campaign_performance(metric_df, pc_df),
    }

    for k, v in views.items():
        logger.info(f"  {k}: {len(v)} rows")

    return views


# ── Internal split builders (metric_df + pc_df) ───────────────────────────────

def _build_creative_performance(metric_df: pd.DataFrame, pc_df: pd.DataFrame) -> pd.DataFrame:
    # Include Ad set name so sidebar adset filter can work on this view
    group_cols = [c for c in ("Date", "Campaign name", "Ad set name", "Ad name")
                  if c in metric_df.columns]
    add_cols   = [c for c in _ADDITIVE if c in metric_df.columns]

    agg = metric_df.groupby(group_cols, dropna=False)[add_cols].sum().reset_index()
    agg = _add_ratios(agg)

    pc = _pincode_day_per_group(pc_df, group_cols)
    agg = agg.merge(pc, on=group_cols, how="left")
    agg["pincode_day"] = agg["pincode_day"].fillna(0).astype(int)

    # Attach image_url and ad_id — take first non-empty value per Ad name
    for extra_col in ("image_url", "ad_id"):
        if extra_col in metric_df.columns and "Ad name" in metric_df.columns:
            col_map = (
                metric_df[["Ad name", extra_col]]
                .replace("", pd.NA)
                .dropna(subset=[extra_col])
                .drop_duplicates("Ad name")
                .set_index("Ad name")[extra_col]
            )
            if "Ad name" in agg.columns:
                agg[extra_col] = agg["Ad name"].map(col_map).fillna("")

    agg = _fmt_date(agg)
    return agg.sort_values("Date", ascending=False).reset_index(drop=True)


def _build_pc_creative_date(metric_df: pd.DataFrame, pc_df: pd.DataFrame) -> pd.DataFrame:
    # Pincode dimension comes from pc_df — each row is one pincode
    # Metrics must also come from pc_df here because the groupby key includes Pincode
    # BUT spend would be inflated. Solution: divide spend by number of pincodes per
    # original row before exploding, so each exploded row carries its fair share.
    # We do this by computing per-pincode spend on the fly.
    if "Pincode" not in pc_df.columns:
        return pd.DataFrame()

    # Compute how many pincodes each original row exploded into
    # We use a row index to track back to original
    m = metric_df.copy().reset_index(drop=True)
    m["_row_id"] = m.index

    # Re-explode with row_id so we can join back
    m["Pincode"] = m["Pincode"].apply(_extract_pincodes)
    m = m[m["Pincode"].map(lambda x: len(x) > 0)]
    m["_n_pins"] = m["Pincode"].map(len)
    m = m.explode("Pincode").reset_index(drop=True)
    m["Pincode"] = m["Pincode"].astype(str).str.strip()

    # Divide additive metrics by number of pincodes for fair attribution
    add_cols = [c for c in _ADDITIVE if c in m.columns]
    for col in add_cols:
        m[col] = m[col] / m["_n_pins"]

    group_cols = [c for c in ("Date", "Pincode", "Ad name") if c in m.columns]
    agg = m.groupby(group_cols, dropna=False)[add_cols].sum().reset_index()
    agg = _add_ratios(agg)

    # pincode_day: use pc_df (already exploded without metric division)
    pc = _pincode_day_per_group(pc_df, group_cols)
    agg = agg.merge(pc, on=group_cols, how="left")
    agg["pincode_day"] = agg["pincode_day"].fillna(0).astype(int)

    if "Campaign name" in m.columns:
        camp = m.groupby(group_cols, dropna=False)["Campaign name"].first().reset_index()
        agg = agg.merge(camp, on=group_cols, how="left")

    agg = _fmt_date(agg)
    return agg.sort_values("Date", ascending=False).reset_index(drop=True)


def _build_daily_pc_consumption(metric_df: pd.DataFrame, pc_df: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in _DEDUP_COLS if c not in pc_df.columns]
    if missing:
        logger.error("Cannot build Daily_PC_Consumption — missing in pc_df: %s", missing)
        return pd.DataFrame()

    add_cols = [c for c in _ADDITIVE if c in metric_df.columns]

    # Canonical pincode_day per date from exploded + deduplicated data
    deduped = pc_df[_DEDUP_COLS].drop_duplicates()
    pc_per_day = (
        deduped.groupby("Date", dropna=False)
        .size()
        .reset_index(name="pincode_day")
    )

    # Metrics from original (non-exploded) rows to avoid inflation
    daily_metrics = metric_df.groupby("Date", dropna=False)[add_cols].sum().reset_index()
    daily_metrics = _add_ratios(daily_metrics)

    result = daily_metrics.merge(pc_per_day, on="Date", how="left")
    result["pincode_day"] = result["pincode_day"].fillna(0).astype(int)

    if "Campaign name" in metric_df.columns:
        camp = metric_df.groupby("Date", dropna=False)["Campaign name"].first().reset_index()
        result = result.merge(camp, on="Date", how="left")

    result = _fmt_date(result)
    return result.sort_values("Date", ascending=False).reset_index(drop=True)


def _build_winning_creatives(metric_df: pd.DataFrame, pc_df: pd.DataFrame) -> pd.DataFrame:
    if "Ad name" not in metric_df.columns:
        return pd.DataFrame()

    group_cols = ["Ad name"]
    add_cols   = [c for c in _ADDITIVE if c in metric_df.columns]

    agg = metric_df.groupby(group_cols, dropna=False)[add_cols].sum().reset_index()
    agg = _add_ratios(agg)

    pc = _pincode_day_per_group(pc_df, group_cols)
    agg = agg.merge(pc, on=group_cols, how="left")
    agg["pincode_day"] = agg["pincode_day"].fillna(0).astype(int)

    if "CPT" in agg.columns:
        agg = agg.sort_values(["CPT", "Purchases"], ascending=[True, False],
                              na_position="last")

    return agg.reset_index(drop=True)


def _build_pincode_creative(metric_df: pd.DataFrame, pc_df: pd.DataFrame) -> pd.DataFrame:
    """Pincode_Creative_View — lifetime totals by (Pincode, Ad name)."""
    if "Pincode" not in pc_df.columns or "Ad name" not in pc_df.columns:
        return pd.DataFrame()

    # For metrics: use pc_df with per-pincode spend attribution (same as _build_pc_creative_date)
    m = metric_df.copy().reset_index(drop=True)
    m["_row_id"] = m.index
    m["Pincode"] = m["Pincode"].apply(_extract_pincodes)
    m = m[m["Pincode"].map(lambda x: len(x) > 0)]
    m["_n_pins"] = m["Pincode"].map(len)
    m = m.explode("Pincode").reset_index(drop=True)
    m["Pincode"] = m["Pincode"].astype(str).str.strip()

    add_cols = [c for c in _ADDITIVE if c in m.columns]
    for col in add_cols:
        m[col] = m[col] / m["_n_pins"]

    group_cols = [c for c in ("Pincode", "Ad name") if c in m.columns]
    agg = m.groupby(group_cols, dropna=False)[add_cols].sum().reset_index()
    agg = _add_ratios(agg)

    pc = _pincode_day_per_group(pc_df, group_cols)
    agg = agg.merge(pc, on=group_cols, how="left")
    agg["pincode_day"] = agg["pincode_day"].fillna(0).astype(int)

    if "Purchases" in agg.columns:
        agg = agg.sort_values(["Purchases", "Spend"], ascending=[False, False],
                              na_position="last")

    return agg.reset_index(drop=True)


def _build_campaign_performance(metric_df: pd.DataFrame, pc_df: pd.DataFrame) -> pd.DataFrame:
    """Campaign_Performance_View — daily totals by (Date, Campaign name)."""
    if "Campaign name" not in metric_df.columns:
        return pd.DataFrame()

    group_cols = [c for c in ("Date", "Campaign name") if c in metric_df.columns]
    add_cols   = [c for c in _ADDITIVE if c in metric_df.columns]

    agg = metric_df.groupby(group_cols, dropna=False)[add_cols].sum().reset_index()
    agg = _add_ratios(agg)

    pc = _pincode_day_per_group(pc_df, group_cols)
    agg = agg.merge(pc, on=group_cols, how="left")
    agg["pincode_day"] = agg["pincode_day"].fillna(0).astype(int)

    agg = _fmt_date(agg)
    return agg.sort_values("Date", ascending=False).reset_index(drop=True)
