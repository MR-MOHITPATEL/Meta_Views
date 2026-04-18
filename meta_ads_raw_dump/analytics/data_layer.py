"""
data_layer.py — Load, cache, and clean data from Google Sheets or Excel fallback.

GUARANTEE:
- All numeric columns are cast to float.
- Null/invalid rows are handled gracefully.
- Returns a clean pandas DataFrame every time.
"""

import os
import logging
import pandas as pd
import streamlit as st
import gspread
from oauth2client.service_account import ServiceAccountCredentials

from analytics.config import (
    GOOGLE_SHEET_ID, GOOGLE_WORKSHEET_NAME, GOOGLE_CREDENTIALS_FILE,
    LOCAL_EXCEL_PATH, LOCAL_SHEET_NAME,
    COL_DATE, COL_SPEND, COL_IMPRESSIONS, COL_CLICKS,
    COL_PURCHASES, COL_REVENUE, COL_ROAS, COL_CPC, COL_CPM, COL_CTR,
    COL_ADSET, COL_CAMPAIGN, COL_AD, COL_PINCODES
)

logger = logging.getLogger(__name__)

# Columns that must be numeric
NUMERIC_COLS = [
    COL_SPEND, COL_IMPRESSIONS, COL_CLICKS, COL_PURCHASES,
    COL_REVENUE, COL_ROAS, COL_CPC, COL_CPM, COL_CTR,
    "ctr", "c2v_ratio", "cvr", "cost_per_result",
    "add_to_cart", "landing_page_views"
]


def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Standardise types, handle nulls, drop invalid rows."""

    # Lowercase column names for consistency
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Flexible Mapping for different Meta sheet headers
    mapping = {
        "amount_spent_(inr)": COL_SPEND,
        "amount_spent": COL_SPEND,
        "ad_set_name": COL_ADSET,
        "campaign_name": COL_CAMPAIGN,
        "ad_name": COL_AD,
        "clicks": COL_CLICKS,
        "link_clicks": COL_CLICKS,
        "pincodes": COL_PINCODES
    }
    df = df.rename(columns=mapping)

    # Parse date
    if COL_DATE in df.columns:
        df[COL_DATE] = pd.to_datetime(df[COL_DATE], errors="coerce")
        df = df.dropna(subset=[COL_DATE])  # Drop rows with unparseable dates
        df[COL_DATE] = df[COL_DATE].dt.date  # Keep as date (not datetime)

    # Cast numeric columns
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # Fill remaining string nulls
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].fillna("").astype(str).str.strip()

    # Drop rows where spend AND purchases are both 0 (empty data rows)
    if COL_SPEND in df.columns and COL_PURCHASES in df.columns:
        df = df[~((df[COL_SPEND] == 0) & (df[COL_PURCHASES] == 0))]

    df = df.reset_index(drop=True)
    return df


def _load_from_sheets() -> pd.DataFrame:
    """Authenticate and load data from Google Sheets."""
    creds_path = os.path.join(os.path.dirname(__file__), "..", "..", GOOGLE_CREDENTIALS_FILE)

    if not os.path.exists(creds_path):
        raise FileNotFoundError(f"Credentials file not found: {creds_path}")

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds   = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client  = gspread.authorize(creds)
    sheet   = client.open_by_key(GOOGLE_SHEET_ID)
    ws      = sheet.worksheet(GOOGLE_WORKSHEET_NAME)
    records = ws.get_all_records()

    return pd.DataFrame(records)


def _load_from_excel() -> pd.DataFrame:
    """Fallback: load from local Excel file."""
    if not os.path.exists(LOCAL_EXCEL_PATH):
        raise FileNotFoundError(f"Excel fallback not found: {LOCAL_EXCEL_PATH}")
    return pd.read_excel(LOCAL_EXCEL_PATH, sheet_name=LOCAL_SHEET_NAME)


@st.cache_data(ttl=3600, show_spinner="Loading campaign data...")
def get_campaign_analytics_data(worksheet_name: str = None) -> tuple[pd.DataFrame, str, str]:
    """
    Load data from a specific worksheet in Google Sheets or Excel fallback.
    Returns: (cleaned_df, source_label, error_msg)
    """
    error_msg = ""
    # Use provided worksheet name or default from .env
    ws_to_load = worksheet_name or GOOGLE_WORKSHEET_NAME

    # Try Google Sheets first
    if GOOGLE_SHEET_ID:
        try:
            creds_path = os.path.join(os.path.dirname(__file__), "..", "..", GOOGLE_CREDENTIALS_FILE)
            scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
            creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
            client = gspread.authorize(creds)
            sheet = client.open_by_key(GOOGLE_SHEET_ID)
            
            # Check if worksheet exists, fallback to default if not
            available_titles = [w.title for w in sheet.worksheets()]
            if ws_to_load not in available_titles:
                logger.warning(f"Worksheet '{ws_to_load}' not found. Falling back to '{GOOGLE_WORKSHEET_NAME}'")
                ws_to_load = GOOGLE_WORKSHEET_NAME
                
            ws = sheet.worksheet(ws_to_load)
            df = pd.DataFrame(ws.get_all_records())
            df = _clean_dataframe(df)
            
            logger.info(f"Loaded {len(df)} rows from Google Sheets [{ws_to_load}].")
            return df, f"Google Sheets [{ws_to_load}]", ""
        except Exception as e:
            error_msg = str(e)
            logger.warning(f"Google Sheets load failed for '{ws_to_load}': {e}. Trying local Excel...")

    # Fallback to Excel
    try:
        df = pd.read_excel(LOCAL_EXCEL_PATH, sheet_name=LOCAL_SHEET_NAME)
        df = _clean_dataframe(df)
        logger.info(f"Loaded {len(df)} rows from Excel.")
        return df, "Excel (local)", error_msg
    except Exception as e:
        logger.error(f"All data sources failed: {e}")
        raise RuntimeError(f"No data source available. Google Sheets Error: {error_msg}. Excel Error: {e}") from e
