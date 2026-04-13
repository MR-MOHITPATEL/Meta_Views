import pandas as pd
import os
import logging
import subprocess
import sys
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

# Load .env
load_dotenv()

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

def upload_to_google_sheets(df: pd.DataFrame):
    """
    Uploads the final combined DataFrame to Google Sheets.
    Clears the existing data before uploading.
    """
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    sheet_name = os.getenv("GOOGLE_SHEET_NAME", "Meta Raw Dump")
    worksheet_name = os.getenv("GOOGLE_WORKSHEET_NAME", "Raw Dump")
    creds_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "Credentials.json")
    
    logger.info(f"Preparing to upload {len(df)} rows to Google Sheet. (Worksheet: '{worksheet_name}')")
    
    if not os.path.exists(creds_file):
        logger.error(f"Credentials file '{creds_file}' not found. Skipping Google Sheets upload.")
        return False
        
    try:
        # Define the scope
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/drive"
        ]
        
        # Authenticate
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)
        client = gspread.authorize(creds)
        
        # Open the sheet (by ID preferred, fallback to name)
        try:
            if sheet_id:
                logger.info(f"Opening sheet by ID: {sheet_id}")
                sheet = client.open_by_key(sheet_id)
            else:
                logger.info(f"Opening sheet by Name: {sheet_name}")
                sheet = client.open(sheet_name)
        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(f"Google Sheet not found. Please ensure it is shared with the service account email.")
            return False
            
        # Get or Create Worksheet
        try:
            worksheet = sheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            logger.info(f"Worksheet '{worksheet_name}' not found. Creating it...")
            worksheet = sheet.add_worksheet(title=worksheet_name, rows="100", cols="20")
            
        # Prepare data for upload (including headers)
        # Convert all columns to string to ensure JSON serializability
        df_display = df.copy()
        for col in df_display.columns:
            df_display[col] = df_display[col].astype(str)
            
        data_to_upload = [df_display.columns.values.tolist()] + df_display.values.tolist()
        
        # Clear existing data
        worksheet.clear()
        
        # Update sheet (batch update)
        worksheet.update("A1", data_to_upload)
        
        logger.info(f"SUCCESS: Successfully uploaded {len(df)} rows to Google Sheets.")
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
