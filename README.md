# Meta Ads Automation Pipeline (Raw Dump)

This repository contains an automated data pipeline that fetches performance and targeting data from the Meta Marketing API, merges it, and uploads the final dataset to Google Sheets.

## Features
- **Incremental Fetching**: Fetches only new daily data with a 7-day rolling lookback for consistency.
- **Active Only**: Filters for active campaigns, adsets, and ads.
- **Deduplication**: Automatically removes duplicate records based on `date`, `campaign_id`, `adset_id`, and `ad_name`.
- **Google Sheets Integration**: Overwrites a specific worksheet tab (`Raw Dump`) with the latest combined dataset.
- **GitHub Actions Automation**: Scheduled to run 3 times daily (9 AM, 3 PM, 6 PM IST).

## Setup Instructions

### 1. GitHub Secrets
To enable the automated pipeline, you must add the following **Repository Secrets** (Settings > Secrets and variables > Actions):

| Secret Name | Description |
| :--- | :--- |
| `META_ACCESS_TOKEN` | Your Meta Marketing API Access Token. |
| `AD_ACCOUNT_ID` | Your Meta Ad Account ID (e.g., `act_123...`). |
| `GOOGLE_SHEET_ID` | The ID of your destination Google Sheet. |
| `GOOGLE_SHEET_NAME` | The name of your destination Google Sheet. |
| `GOOGLE_WORKSHEET_NAME` | The name of the worksheet tab (e.g., `Raw Dump`). |
| `GOOGLE_CREDENTIALS_JSON` | The full content of your Google Service Account JSON key. |

### 2. Google Sheet Permissions
Ensure you share your target Google Sheet with the `client_email` address found inside your `GOOGLE_CREDENTIALS_JSON` with **Editor** permissions.

## Project Structure
- `meta_performance_pipeline.py`: Fetches performance metrics.
- `meta_pincode_pipeline.py`: Fetches targeting/pincode data.
- `combine_pipeline_data.py`: Merges datasets and handles Google Sheets upload.
- `run_all.py`: Main entry point for the automation.
- `.github/workflows/pipeline.yml`: GitHub Actions schedule configuration.

## Manual Trigger
You can manually trigger the pipeline from the **Actions** tab in this repository by selecting the workflow and clicking **Run workflow**.
