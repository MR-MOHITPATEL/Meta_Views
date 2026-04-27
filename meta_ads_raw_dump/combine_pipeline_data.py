import pandas as pd
import os
import logging
import subprocess
import sys
import json
import time
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

# Load .env (from root)
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

# Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def run_pipeline_script(script_name: str, working_dir: str):
    """
    Executes a sub-pipeline script using subprocess and waits for completion.
    """
    logger.info(f"====================================================")
    logger.info(f"STARTING SUB-PIPELINE: {script_name}")
    logger.info(f"Working Directory: {working_dir}")
    logger.info(f"====================================================")
    
    try:
        # Using sys.executable ensures we use the same Python environment/venv
        # capture_output=False allows the sub-script's logs to print directly to the console
        result = subprocess.run(
            [sys.executable, script_name], 
            cwd=working_dir,
            check=True
        )
        logger.info(f"SUCCESS: {script_name} finished correctly.")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"ERROR: {script_name} failed with exit code {e.returncode}.")
        return False
    except Exception as e:
        logger.error(f"FATAL: Unexpected error running {script_name}: {e}")
        return False

def combine_data():
    perf_path = os.path.join("performance-wise-data", "meta_performance_report.xlsx")
    pincode_path = os.path.join("Pincode-wise-Data", "meta_pincode_report.xlsx")
    output_path = "final_combined_report.xlsx"

    logger.info("Loading performance data from recently generated file...")
    perf_df = pd.read_excel(perf_path, sheet_name="performance_data")
    
    logger.info("Loading targeting data from recently generated file...")
    targeting_df = pd.read_excel(pincode_path, sheet_name="targeting_data")

    # Data Normalization
    for df_item in [perf_df, targeting_df]:
        df_item['campaign_id'] = df_item['campaign_id'].astype(str).str.strip()
        df_item['adset_id'] = df_item['adset_id'].astype(str).str.strip()

    # Create Join Keys
    perf_df['join_key'] = perf_df['campaign_id'] + "_" + perf_df['adset_id']
    targeting_df['join_key'] = targeting_df['campaign_id'] + "_" + targeting_df['adset_id']
    
    # Create static mapping
    t_mapping = targeting_df.sort_values(by='date', ascending=False).drop_duplicates('join_key').copy()
    t_mapping = t_mapping[['join_key', 'pincodes']]

    logger.info(f"Merging datasets using static ID-based join_key... (Base: {len(perf_df)} records)")
    combined_df = perf_df.merge(t_mapping, on='join_key', how='left')

    # Cleanup and sort
    combined_df = combined_df.drop(columns=['join_key'])
    combined_df['pincodes'] = combined_df['pincodes'].fillna("")
    combined_df['date'] = pd.to_datetime(combined_df['date'])
    combined_df = combined_df.sort_values(by='date', ascending=False)
    combined_df['date'] = combined_df['date'].dt.strftime('%Y-%m-%d')
    combined_df = combined_df.drop_duplicates()

    # Enforce canonical column order (same as RAW_DUMP_COLS) so pivot tables
    # in Google Sheets never get field references remapped to the wrong column.
    for col in RAW_DUMP_COLS:
        if col not in combined_df.columns:
            combined_df[col] = ""
    combined_df = combined_df[RAW_DUMP_COLS]

    logger.info(f"Exporting final master report to {output_path}...")
    try:
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            combined_df.to_excel(writer, sheet_name="final_combined", index=False)
            
            workbook  = writer.book
            worksheet = writer.sheets["final_combined"]
            
            pct_fmt = '0.00%'
            num_fmt = '#,##0.00'
            
            format_mapping = {
                "spend": num_fmt, "revenue": num_fmt, "cost_per_result": num_fmt,
                "cpm": num_fmt, "cpc": num_fmt, "ctr": pct_fmt,
                "c2v_ratio": pct_fmt, "cvr": pct_fmt, "roas": num_fmt
            }
            
            for col_idx, col_name in enumerate(combined_df.columns):
                if col_name in format_mapping:
                    fmt = format_mapping[col_name]
                    for r_idx in range(2, len(combined_df) + 2):
                        worksheet.cell(row=r_idx, column=col_idx + 1).number_format = fmt
                        
        logger.info("SUCCESS: Formatted Master Combined Report generated.")
    except Exception as e:
        logger.error(f"FAILED to generate combined report: {e}")

_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
_CHUNK_ROWS = 2000  # safe batch size for Sheets API

# Canonical column order — Raw Dump must ALWAYS have columns in this exact order
# so that Google Sheets native pivot tables never remap fields to the wrong column.
RAW_DUMP_COLS = [
    "date", "campaign_id", "campaign_name", "adset_id", "adset_name", "ad_name",
    "image_url", "pincodes",
    "spend", "impressions", "cpm", "cpc", "ctr", "link_clicks", "landing_page_views",
    "c2v_ratio", "cost_per_result", "roas", "cvr", "add_to_cart", "leads", "purchases", "revenue",
]

# Dedup by text fields only — IDs (campaign_id, adset_id) are integers and
# lose precision when Google Sheets stores them as floats (1202445745086 → 1200000000000).
# Text fields (campaign_name, adset_name, ad_name) are immune to this problem.
_DEDUP_KEYS = ["date", "campaign_name", "adset_name", "ad_name"]

# ID columns that must be stored as plain text strings in Google Sheets
_ID_COLS = ["campaign_id", "adset_id"]


def _normalize_id(val) -> str:
    """Convert any numeric ID representation to a plain integer string."""
    s = str(val).strip().replace(",", "")
    try:
        return str(int(float(s)))
    except (ValueError, TypeError):
        return s


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize ID and text key columns so comparisons work correctly."""
    df = df.copy()
    for col in _ID_COLS:
        if col in df.columns:
            df[col] = df[col].apply(_normalize_id)
    for col in ["campaign_name", "adset_name", "ad_name"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return df


def _read_existing_raw_dump(worksheet) -> pd.DataFrame:
    """
    Read whatever is currently in the Raw Dump worksheet.
    Uses UNFORMATTED_VALUE so numeric IDs come back as actual numbers
    (not display strings like '1.20E+12' which have lost precision).
    """
    try:
        records = worksheet.get_all_records(value_render_option="UNFORMATTED_VALUE")
        if not records:
            return pd.DataFrame()
        df = pd.DataFrame(records)
        df.columns = [c.strip() for c in df.columns]
        df = _normalize_df(df)
        return df
    except Exception as e:
        logger.warning(f"Could not read existing Raw Dump: {e}. Starting fresh.")
        return pd.DataFrame()


def _merge_with_history(existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge new_df into existing_df.
    Dedup key: date + campaign_name + adset_name + ad_name (text only — never loses precision).
    New fetch always wins so Meta's latest attribution numbers are kept.
    """
    new_df = _normalize_df(new_df)

    if existing_df.empty:
        merged = new_df.copy()
    else:
        existing_df = _normalize_df(existing_df)
        # Concat: existing first, new last → keep='last' retains fresh values
        merged = pd.concat([existing_df, new_df], ignore_index=True)

    active_keys = [k for k in _DEDUP_KEYS if k in merged.columns]
    before = len(merged)
    merged = merged.drop_duplicates(subset=active_keys, keep="last")
    logger.info(f"History merge: {before} rows → {len(merged)} after dedup "
                f"({before - len(merged)} duplicates removed).")

    # Sort: latest date first
    if "date" in merged.columns:
        merged["date"] = pd.to_datetime(merged["date"], errors="coerce")
        merged = merged.sort_values("date", ascending=False)
        merged["date"] = merged["date"].dt.strftime("%Y-%m-%d")

    # Enforce canonical column order
    for col in RAW_DUMP_COLS:
        if col not in merged.columns:
            merged[col] = ""
    merged = merged[RAW_DUMP_COLS]

    return merged


def upload_to_google_sheets(df: pd.DataFrame):
    """
    Merge-uploads the combined DataFrame to Google Sheets Raw Dump.

    Instead of wiping the sheet and writing only the last 7 days, we:
      1. Read the existing sheet contents (full history).
      2. Merge the new data in (new rows added, existing rows for the same
         date+ad updated with fresh values).
      3. Write the merged result back — history is never lost.
    """
    import numpy as np

    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    worksheet_name = os.getenv("GOOGLE_WORKSHEET_NAME", "Raw Dump")
    creds_file_name = os.getenv("GOOGLE_CREDENTIALS_FILE", "Credentials.json")
    creds_file = os.path.join(os.path.dirname(__file__), "..", creds_file_name)

    logger.info(f"Preparing to merge-upload {len(df)} new rows → '{worksheet_name}'")

    if not os.path.exists(creds_file):
        logger.error(f"Credentials file '{creds_file}' not found. Skipping upload.")
        return False
    if not sheet_id:
        logger.error("GOOGLE_SHEET_ID not set in .env. Skipping upload.")
        return False

    try:
        creds  = Credentials.from_service_account_file(creds_file, scopes=_SCOPES)
        client = gspread.authorize(creds)

        try:
            sheet = client.open_by_key(sheet_id)
        except gspread.exceptions.SpreadsheetNotFound:
            logger.error("Google Sheet not found. Check that it is shared with the service account.")
            return False

        try:
            worksheet = sheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            logger.info(f"Worksheet '{worksheet_name}' not found — creating it.")
            worksheet = sheet.add_worksheet(
                title=worksheet_name,
                rows=max(len(df) + 10, 100),
                cols=max(len(RAW_DUMP_COLS) + 5, 30),
            )

        # 1. Read history from the sheet
        logger.info("Reading existing Raw Dump from Google Sheets…")
        existing_df = _read_existing_raw_dump(worksheet)
        logger.info(f"Existing rows in sheet: {len(existing_df)}")

        # 2. Normalise incoming df before merge
        new_df = df.copy()
        for col in _DEDUP_KEYS:
            if col in new_df.columns:
                new_df[col] = new_df[col].astype(str).str.strip()

        # 3. Merge new data with full history
        merged_df = _merge_with_history(existing_df, new_df)
        logger.info(f"Total rows after merge: {len(merged_df)}")

        # 4. Prepare for upload — numerics stay numeric, strings stay string
        df_upload = merged_df.copy()
        _numeric_cols = {
            "spend", "impressions", "cpm", "cpc", "ctr", "link_clicks",
            "landing_page_views", "c2v_ratio", "cost_per_result", "roas",
            "cvr", "add_to_cart", "leads", "purchases", "revenue",
        }
        # ID columns must be stored as plain text strings so Google Sheets
        # never converts them to floats (which causes 1.20E+17 scientific notation
        # and breaks deduplication on the next fetch).
        _str_cols = set(_ID_COLS) | {"date", "campaign_name", "adset_name", "ad_name",
                                      "image_url", "pincodes"}
        for col in df_upload.columns:
            if col in _numeric_cols:
                df_upload[col] = pd.to_numeric(df_upload[col], errors="coerce").fillna(0)
            elif col in _str_cols:
                df_upload[col] = df_upload[col].fillna("").astype(str)
            else:
                df_upload[col] = df_upload[col].fillna("").astype(str)

        def _native(v):
            if isinstance(v, np.integer): return int(v)
            if isinstance(v, np.floating): return float(v)
            return v

        # Prefix ID columns with a single-quote so Sheets treats them as text,
        # preventing automatic conversion to scientific notation.
        id_col_indices = {i for i, c in enumerate(df_upload.columns) if c in _ID_COLS}

        header = df_upload.columns.tolist()
        data_rows = []
        for row in df_upload.values.tolist():
            formatted = []
            for i, v in enumerate(row):
                if i in id_col_indices:
                    formatted.append("'" + str(v))   # leading ' forces text in Sheets
                else:
                    formatted.append(_native(v))
            data_rows.append(formatted)

        all_values = [header] + data_rows

        # 5. Clear and rewrite (pivot tables survive because column order is fixed)
        worksheet.clear()
        time.sleep(0.5)

        for chunk_start in range(0, len(all_values), _CHUNK_ROWS):
            chunk = all_values[chunk_start : chunk_start + _CHUNK_ROWS]
            start_row = chunk_start + 1
            worksheet.update(f"A{start_row}", chunk, value_input_option="USER_ENTERED")
            logger.info(f"  Uploaded rows {chunk_start + 1}–{chunk_start + len(chunk)}…")
            if chunk_start + _CHUNK_ROWS < len(all_values):
                time.sleep(1)

        logger.info(f"SUCCESS: Raw Dump now has {len(merged_df)} total rows in '{worksheet_name}'.")
        return True

    except Exception as e:
        logger.error(f"FAILED to upload to Google Sheets: {e}")
        return False

def main():
    # 1. Run Performance Pipeline
    if not run_pipeline_script("meta_performance_pipeline.py", "performance-wise-data"):
        logger.error("Performance Pipeline failed. Aborting master merger.")
        sys.exit(1)
        
    # 2. Run Pincode Pipeline
    # The performance pipeline is already updated and handles its own logic.
    # The pincode pipeline remains the same as previously implemented.
    if not run_pipeline_script("meta_pincode_pipeline.py", "Pincode-wise-Data"):
        logger.error("Pincode Pipeline failed. Aborting master merger.")
        sys.exit(1)

    # 3. Perform Combination Logic
    logger.info("Averaging and Merging Master Data...")
    combine_data()
    
    # 4. Export to Google Sheets
    # Load the recently saved combined report to ensure we upload exactly what's in the file
    try:
        final_df = pd.read_excel("final_combined_report.xlsx")
        upload_to_google_sheets(final_df)
    except Exception as e:
        logger.error(f"Error loading final report for Google Sheets upload: {e}")
        
    logger.info("FULL END-TO-END PIPELINE COMPLETED SUCCESSFULLY.")

if __name__ == "__main__":
    main()
