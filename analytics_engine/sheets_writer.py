"""
Writes computed views back to their Google Sheets tabs.
Uses the same service account credentials as sheets_loader.
"""

from __future__ import annotations

import logging
import time

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

from config import CREDENTIALS_PATH, SHEET_ID, TAB_NAMES

logger = logging.getLogger(__name__)

_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Google Sheets API: max cells per request ~10M, but keep batches safe
_CHUNK_ROWS = 2000


def _get_client() -> gspread.Client:
    creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=_SCOPES)
    return gspread.authorize(creds)


def _df_to_values(df: pd.DataFrame) -> list[list]:
    """
    Convert DataFrame to list-of-lists for gspread.
    Numeric columns are kept as Python int/float so Sheets stores them
    as numbers (not text). NaN → 0 for numerics, "" for text columns.
    """
    import numpy as np
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d")
        elif pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(0)
        else:
            df[col] = df[col].fillna("").astype(str)

    # Convert numpy scalar types → native Python so gspread can JSON-serialise
    def _native(v):
        if isinstance(v, np.integer): return int(v)
        if isinstance(v, np.floating): return float(v)
        return v

    rows = [[_native(v) for v in row] for row in df.values.tolist()]
    return [df.columns.tolist()] + rows


def write_view(dataset_key: str, df: pd.DataFrame) -> bool:
    """
    Clear the tab and write the DataFrame to it.
    Creates the tab if it doesn't exist.
    Returns True on success.
    """
    if dataset_key not in TAB_NAMES:
        logger.error(f"Unknown dataset key: {dataset_key!r}")
        return False

    tab_name = TAB_NAMES[dataset_key]
    logger.info(f"  Writing '{tab_name}': {len(df)} rows × {len(df.columns)} cols…")

    try:
        client      = _get_client()
        spreadsheet = client.open_by_key(SHEET_ID)

        # Get or create worksheet
        try:
            worksheet = spreadsheet.worksheet(tab_name)
        except gspread.exceptions.WorksheetNotFound:
            logger.info(f"  Tab '{tab_name}' not found — creating it.")
            worksheet = spreadsheet.add_worksheet(
                title=tab_name,
                rows=max(len(df) + 10, 100),
                cols=max(len(df.columns) + 5, 20),
            )

        values = _df_to_values(df)

        # Clear existing content
        worksheet.clear()
        time.sleep(0.5)

        # Write in chunks — use 500 rows to avoid 500 payload-too-large errors
        _SAFE_CHUNK = 500
        for chunk_start in range(0, len(values), _SAFE_CHUNK):
            chunk = values[chunk_start : chunk_start + _SAFE_CHUNK]
            start_row = chunk_start + 1
            try:
                worksheet.update(f"A{start_row}", chunk, value_input_option="USER_ENTERED")
            except Exception as _chunk_err:
                logger.warning(f"  Chunk at row {start_row} failed ({_chunk_err}), retrying row-by-row…")
                for _ri, _row in enumerate(chunk[1:], start=start_row + 1):
                    try:
                        worksheet.update(f"A{_ri}", [_row], value_input_option="USER_ENTERED")
                    except Exception as _row_err:
                        logger.warning(f"    Row {_ri} skipped: {_row_err}")
            if chunk_start + _SAFE_CHUNK < len(values):
                time.sleep(1)

        logger.info(f"  '{tab_name}' written successfully.")
        return True

    except Exception as e:
        logger.error(f"  Failed to write '{tab_name}': {e}")
        return False


def write_all_views(
    views: dict[str, pd.DataFrame],
    progress_callback=None,
) -> dict[str, bool]:
    """
    Write all views to Google Sheets.

    progress_callback(message: str) — optional callable for UI progress updates.
    Returns {dataset_key: success_bool}.
    """
    results = {}
    total   = len(views)

    for idx, (key, df) in enumerate(views.items(), start=1):
        tab = TAB_NAMES.get(key, key)
        msg = f"Writing view {idx}/{total}: {tab}…"
        logger.info(msg)
        if progress_callback:
            progress_callback(msg)

        results[key] = write_view(key, df)

        # Pause between tabs to respect Google Sheets API quota (60 req/min)
        if idx < total:
            time.sleep(2)

    return results
