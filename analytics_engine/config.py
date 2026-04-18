import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from the project root (one level above analytics_engine/)
_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_ROOT / ".env")

# ── Google Sheets ──────────────────────────────────────────────────────────────
SHEET_ID = os.environ["GOOGLE_SHEET_ID"]

# Exact tab names in your Google Sheet
TAB_NAMES = {
    "creative_performance":  "Creative_Performance_View",
    "pc_creative_date":      "PC_Creative_Date_View",
    "daily_pc_consumption":  "Daily_PC_Consumption",
    "winning_creatives":     "Winning_Creatives_View",
    "pincode_creative":      "Pincode_Creative_View",
    "campaign_performance":  "Campaign_Performance_View",
    "raw_dump":              "Raw Dump",
}

# Path to service account JSON — respects GOOGLE_CREDENTIALS_FILE from .env
_creds_file = os.environ.get("GOOGLE_CREDENTIALS_FILE", "Credentials.json")
# Search: analytics_engine/credentials/ first, then project root
_candidates = [
    Path(__file__).parent / "credentials" / _creds_file,
    _ROOT / _creds_file,
]
CREDENTIALS_PATH = next((str(p) for p in _candidates if p.exists()), str(_candidates[0]))

# ── Gemini ─────────────────────────────────────────────────────────────────────
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
GEMINI_MODEL   = "gemini-2.5-flash-preview-04-17"

# ── Cache ──────────────────────────────────────────────────────────────────────
CACHE_TTL_SECONDS = 300  # refresh sheet data every 5 minutes
