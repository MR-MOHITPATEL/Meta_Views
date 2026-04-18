"""
config.py — Central configuration for the Analytics System.
Reads from environment variables / .env file.
"""
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

# ── Google Sheets ──────────────────────────────────────────────
GOOGLE_SHEET_ID          = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_WORKSHEET_NAME    = os.getenv("GOOGLE_WORKSHEET_NAME", "Raw Dump")
GOOGLE_CREDENTIALS_FILE  = os.getenv("GOOGLE_CREDENTIALS_FILE", "Credentials.json")

# ── Local fallback ─────────────────────────────────────────────
LOCAL_EXCEL_PATH = os.path.join(os.path.dirname(__file__), "..", "final_combined_report.xlsx")
LOCAL_SHEET_NAME = "final_combined"

# ── LLM Keys (Gemini preferred, Groq fallback) ─────────────────
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GROQ_API_KEY   = os.getenv("GROQ_API_KEY", "")

# ── Query defaults ─────────────────────────────────────────────
DEFAULT_CPT_THRESHOLD      = 250    # Cost per transaction threshold
DEFAULT_PURCHASE_THRESHOLD = 2      # Minimum purchases for "winning"
DEFAULT_TOP_N              = 10     # Default rows for top/worst queries

# ── Column name aliases (normalised internally) ────────────────
COL_DATE        = "date"
COL_CAMPAIGN    = "campaign_name"
COL_ADSET       = "adset_name"
COL_AD          = "ad_name"
COL_SPEND       = "spend"
COL_IMPRESSIONS = "impressions"
COL_CLICKS      = "link_clicks"
COL_PURCHASES   = "purchases"
COL_REVENUE     = "revenue"
COL_ROAS        = "roas"
COL_CPC         = "cpc"
COL_CPM         = "cpm"
COL_CTR         = "ctr"
COL_PINCODES    = "pincodes"
COL_IMAGE_URL   = "image_url"
