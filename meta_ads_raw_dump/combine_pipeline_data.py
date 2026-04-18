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

    # Column ordering
    primary_cols = [
        'date', 'campaign_id', 'campaign_name', 'adset_id', 'adset_name', 'ad_name', 
        'image_url', 'pincodes'
    ]
    metric_cols = [c for c in combined_df.columns if c not in primary_cols]
    final_cols = primary_cols + metric_cols
    combined_df = combined_df[final_cols]

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


def upload_to_google_sheets(df: pd.DataFrame):
    """
    Uploads the final combined DataFrame to Google Sheets.
    Uses google.oauth2 (same library as analytics_engine).
    Clears the existing data before uploading in chunks.
    """
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    worksheet_name = os.getenv("GOOGLE_WORKSHEET_NAME", "Raw Dump")
    creds_file_name = os.getenv("GOOGLE_CREDENTIALS_FILE", "Credentials.json")
    creds_file = os.path.join(os.path.dirname(__file__), "..", creds_file_name)

    logger.info(f"Preparing to upload {len(df)} rows to Google Sheet. (Worksheet: '{worksheet_name}')")

    if not os.path.exists(creds_file):
        logger.error(f"Credentials file '{creds_file}' not found. Skipping Google Sheets upload.")
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
            logger.error("Google Sheet not found. Ensure the sheet is shared with the service account email.")
            return False

        # Get or create worksheet
        try:
            worksheet = sheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            logger.info(f"Worksheet '{worksheet_name}' not found — creating it.")
            worksheet = sheet.add_worksheet(
                title=worksheet_name,
                rows=max(len(df) + 10, 100),
                cols=max(len(df.columns) + 5, 20),
            )

        # Prepare: keep numerics as numbers so Sheets stores them in Number format.
        import numpy as np
        df_upload = df.copy()
        for col in df_upload.columns:
            if pd.api.types.is_numeric_dtype(df_upload[col]):
                df_upload[col] = df_upload[col].fillna(0)
            else:
                df_upload[col] = df_upload[col].fillna("").astype(str)

        def _native(v):
            if isinstance(v, np.integer): return int(v)
            if isinstance(v, np.floating): return float(v)
            return v

        all_values = [df_upload.columns.tolist()] + [
            [_native(v) for v in row] for row in df_upload.values.tolist()
        ]

        # Clear existing data
        worksheet.clear()
        time.sleep(0.5)

        # Upload in chunks to avoid API payload limits
        for chunk_start in range(0, len(all_values), _CHUNK_ROWS):
            chunk = all_values[chunk_start : chunk_start + _CHUNK_ROWS]
            start_row = chunk_start + 1  # 1-indexed
            worksheet.update(f"A{start_row}", chunk, value_input_option="USER_ENTERED")
            logger.info(f"  Uploaded rows {chunk_start + 1}–{chunk_start + len(chunk)}…")
            if chunk_start + _CHUNK_ROWS < len(all_values):
                time.sleep(1)  # respect quota

        logger.info(f"SUCCESS: Uploaded {len(df)} rows to '{worksheet_name}'.")
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
