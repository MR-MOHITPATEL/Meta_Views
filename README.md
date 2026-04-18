# Meta Views — BCT Analytics Engine

A Streamlit dashboard + automated Meta Ads data pipeline for ZenJeevani.

## What it does
- **Chat interface** — ask questions about campaign data in plain English (powered by Gemini)
- **View Builder** — build and save custom pivot views, push to Google Sheets
- **Meta Ads pipeline** — fetches performance + pincode data from Meta API, merges it, uploads to `Raw Dump` sheet
- **Auto-schedule** — pipeline runs automatically at **9 AM, 2 PM, 6 PM** via Windows Task Scheduler
- **Manual fetch** — "Fetch Current Results" button in the sidebar triggers the pipeline on demand

---

## Setup (New Computer)

### Prerequisites
- Python 3.10 or higher — https://www.python.org/downloads/
- Git — https://git-scm.com/downloads

### Step 1 — Clone the repo
```bash
git clone https://github.com/MR-MOHITPATEL/Meta_Views.git
cd Meta_Views
```

### Step 2 — Create virtual environment & install dependencies
```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r requirements.txt
pip install -r analytics_engine/requirements.txt
```

### Step 3 — Add credentials (get these from Mohit)
Create a `.env` file in the project root:
```
META_ACCESS_TOKEN=your_meta_access_token
AD_ACCOUNT_ID=act_xxxxxxxxxx
GOOGLE_SHEET_ID=your_google_sheet_id
GOOGLE_CREDENTIALS_FILE=Credentials.json
GEMINI_API_KEY=your_gemini_api_key
```

Place the Google Service Account JSON file as:
```
analytics_engine/credentials/Credentials.json
```

### Step 4 — Share the Google Sheet
Open the `Credentials.json` file, find the `client_email` field, and share the Google Sheet with that email as **Editor**.

### Step 5 — Run the dashboard
```bash
cd analytics_engine
..\.venv\Scripts\streamlit run app.py
```
Opens at http://localhost:8501

---

## Auto-Schedule Setup (Windows Task Scheduler)

To have the pipeline auto-run at **9 AM, 2 PM, 6 PM** on your machine:

1. Open **PowerShell as Administrator**
2. Run:
```powershell
Set-ExecutionPolicy RemoteSigned -Scope CurrentUser   # only needed once
.\setup_scheduler.ps1
```
3. Verify tasks in Task Scheduler (`taskschd.msc`) under Task Scheduler Library

---

## Manual Pipeline Run
Either click **"📥 Fetch Current Results"** in the dashboard sidebar, or run directly:
```bash
cd meta_ads_raw_dump
..\.venv\Scripts\python run_all.py
```

---

## Project Structure
```
Meta_Views/
├── analytics_engine/        # Streamlit dashboard
│   ├── app.py               # Main UI
│   ├── config.py            # Env-based config
│   ├── credentials/         # ← NOT in git (add Credentials.json here)
│   └── requirements.txt
├── meta_ads_raw_dump/       # Meta API pipeline
│   ├── run_all.py           # Entry point
│   ├── combine_pipeline_data.py
│   ├── performance-wise-data/
│   └── Pincode-wise-Data/
├── requirements.txt         # Top-level deps
├── setup_scheduler.ps1      # Windows Task Scheduler setup
└── .env                     # ← NOT in git (create manually)
```

---

## Environment Variables Reference

| Variable | Description |
|---|---|
| `META_ACCESS_TOKEN` | Meta Marketing API long-lived access token |
| `AD_ACCOUNT_ID` | Meta Ad Account ID (e.g. `act_123456`) |
| `GOOGLE_SHEET_ID` | Google Sheet ID from the URL |
| `GOOGLE_CREDENTIALS_FILE` | Filename of the service account JSON (default: `Credentials.json`) |
| `GEMINI_API_KEY` | Google Gemini API key |
| `GROQ_API_KEY` | (Optional) Groq API key as LLM fallback |
