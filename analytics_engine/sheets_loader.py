"""
Loads each Google Sheet tab into a pandas DataFrame.
Uses gspread + service account credentials.
Results are cached in Streamlit for CACHE_TTL_SECONDS.

For the "raw_dump" key, if the sheet tab is empty, falls back to reading
final_combined_report.xlsx from meta_ads_raw_dump/ so that "Refresh Views"
works even before the pipeline has uploaded to Sheets.
"""

import os
import gspread
import pandas as pd
import streamlit as st
from pathlib import Path
from google.oauth2.service_account import Credentials

from config import CREDENTIALS_PATH, SHEET_ID, TAB_NAMES, CACHE_TTL_SECONDS

# Local fallback path for raw_dump
_LOCAL_EXCEL = Path(__file__).resolve().parent.parent / "meta_ads_raw_dump" / "final_combined_report.xlsx"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]


def _get_client() -> gspread.Client:
    creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=SCOPES)
    return gspread.authorize(creds)


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner="Fetching data from Google Sheets…")
def load_sheet(dataset_key: str) -> pd.DataFrame:
    """
    dataset_key: one of the keys in config.TAB_NAMES
    Returns a cleaned DataFrame for that tab.
    """
    if dataset_key not in TAB_NAMES:
        raise ValueError(f"Unknown dataset key: {dataset_key!r}. Valid keys: {list(TAB_NAMES)}")

    tab_name = TAB_NAMES[dataset_key]
    client = _get_client()
    spreadsheet = client.open_by_key(SHEET_ID)
    worksheet = spreadsheet.worksheet(tab_name)

    records = worksheet.get_all_records()
    df = pd.DataFrame(records)

    # If Raw Dump sheet is empty, fall back to local Excel file
    if df.empty and dataset_key == "raw_dump" and _LOCAL_EXCEL.exists():
        import logging
        logging.getLogger(__name__).info(
            f"'{tab_name}' sheet is empty — loading from local fallback: {_LOCAL_EXCEL}"
        )
        df = pd.read_excel(_LOCAL_EXCEL, sheet_name="final_combined")

    # Normalise column names: strip whitespace
    df.columns = [c.strip() for c in df.columns]

    # Parse date column if present
    for col in ("Date", "date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            break

    # Numeric coercion — covers view column names, old Raw Dump names, and new pipeline names
    numeric_cols = [
        # Internal view names (post-normalization)
        "Spend", "Purchases", "Clicks", "Impressions", "Revenue",
        "pincode_day", "CTR", "CPC", "CPM", "CPT", "CVR", "ROAS",
        "Add to Cart", "Landing Page Views",
        # Old Raw Dump native names (original Meta export)
        "Amount spent (INR)", "Link clicks", "Results value",
        "Adds to cart", "Website landing views",
        # New pipeline output names (snake_case from combine_pipeline_data.py)
        "spend", "impressions", "cpm", "cpc", "ctr",
        "link_clicks", "landing_page_views", "c2v_ratio",
        "cost_per_result", "roas", "cvr", "add_to_cart",
        "purchases", "revenue",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df


def load_all_sheets() -> dict[str, pd.DataFrame]:
    """Load every dataset at once. Useful for cache warm-up."""
    return {key: load_sheet(key) for key in TAB_NAMES}
