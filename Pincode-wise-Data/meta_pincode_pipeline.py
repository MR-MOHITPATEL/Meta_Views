import os
import json
import logging
import datetime
import requests
import pandas as pd
from typing import List, Dict, Tuple
from dotenv import load_dotenv

# Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def load_config():
    """Load configuration from .env and validate."""
    load_dotenv()
    config = {
        "access_token": os.getenv("META_ACCESS_TOKEN"),
        "ad_account_id": os.getenv("AD_ACCOUNT_ID"),
        "api_version": os.getenv("API_VERSION", "v19.0"),
        "default_start_date": os.getenv("DEFAULT_START_DATE", "2026-02-28"),
        "output_excel_path": os.path.join(os.path.dirname(__file__), os.getenv("OUTPUT_EXCEL_PATH", "meta_pincode_report.xlsx"))
    }
    
    if not config["access_token"] or config["access_token"] == "your_access_token_here":
        logger.warning("META_ACCESS_TOKEN is not set or uses default placeholder. API calls will fail.")
    if not config["ad_account_id"] or config["ad_account_id"] == "act_your_ad_account_id_here":
        logger.warning("AD_ACCOUNT_ID is not set or uses default placeholder. API calls will fail.")
        
    return config

def fetch_meta_insights(config: dict, start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """
    Fetch daily insights for all adsets in the given ad account.
    """
    if not start_date:
        # 7-day rolling lookback to ensure late-arriving data is captured
        lookback_date = datetime.date.today() - datetime.timedelta(days=7)
        start = lookback_date.strftime("%Y-%m-%d")
    else:
        start = start_date
        
    end = end_date or datetime.date.today().strftime("%Y-%m-%d")
    
    logger.info(f"Fetching insights for {config['ad_account_id']} from {start} to {end}")
    
    url = f"https://graph.facebook.com/{config['api_version']}/{config['ad_account_id']}/insights"
    params = {
        "access_token": config["access_token"],
        "fields": "campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,date_start",
        "level": "ad",
        "filtering": json.dumps([
            {"field": "ad.effective_status", "operator": "IN", "value": ["ACTIVE"]},
            {"field": "adset.effective_status", "operator": "IN", "value": ["ACTIVE"]},
            {"field": "campaign.effective_status", "operator": "IN", "value": ["ACTIVE"]}
        ]),
        "time_increment": 1,
        "time_range": json.dumps({"since": start, "until": end})
    }
    
    all_data = []
    
    try:
        while url:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            all_data.extend(data.get("data", []))
            
            # Pagination
            paging = data.get("paging", {})
            url = paging.get("next")
            params = None
            
        logger.info(f"Fetched {len(all_data)} insights records.")
    except Exception as e:
        logger.error(f"Error fetching Meta insights: {e}")
        if 'response' in locals() and hasattr(response, 'text'):
            logger.error(f"Response: {response.text}")
    
    if not all_data:
        return pd.DataFrame(columns=["date", "campaign_id", "campaign_name", "adset_id", "adset_name", "ad_id", "ad_name"])
        
    df = pd.DataFrame(all_data)
    # Target structure: date, campaign_id, campaign_name, adset_id, adset_name, ad_id, ad_name
    df = df.rename(columns={"date_start": "date"})
    expected_cols = ["date", "campaign_id", "campaign_name", "adset_id", "adset_name", "ad_id", "ad_name"]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = pd.NA
            
    return df[expected_cols]

def fetch_adset_targeting_zips(config: dict, adset_ids: List[str]) -> pd.DataFrame:
    """
    Fetch targeting data for given adset_ids and extract only zips.
    Returns: DataFrame with adset_id and a list of pincodes.
    """
    logger.info(f"Fetching targeting data for {len(adset_ids)} unique adsets (Direct Pincodes Only).")
    
    adset_pincode_map = []
    
    batch_size = 50
    for i in range(0, len(adset_ids), batch_size):
        batch_ids = adset_ids[i:i+batch_size]
        url = f"https://graph.facebook.com/{config['api_version']}/"
        params = {
            "access_token": config["access_token"],
            "ids": ",".join(batch_ids),
            "fields": "id,targeting"
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            for adset_id, info in data.items():
                pincodes = set()
                if "targeting" in info:
                    targeting = info["targeting"]
                    geo = targeting.get("geo_locations", {})
                    zips = geo.get("zips", [])
                    
                    for z in zips:
                        # Extract pincode from 'name' (preferred) or 'key' (remove "IN:" prefix)
                        pin = z.get("name")
                        if not pin or not pin.isdigit():
                            key = z.get("key", "")
                            # Format usually "IN:400612"
                            if ":" in key:
                                pin = key.split(":")[-1]
                            else:
                                pin = key
                                
                        if pin and pin.isdigit():
                            pincodes.add(pin)
                
                logger.info(f"Extracted {len(pincodes)} pincodes for adset {adset_id}")
                adset_pincode_map.append({
                    "adset_id": adset_id,
                    "pincodes": list(pincodes)
                })
        except Exception as e:
            logger.error(f"Error fetching targeting for batch. {e}")
            if 'response' in locals() and hasattr(response, 'text'):
                logger.error(f"Response: {response.text}")
                
    if not adset_pincode_map:
        return pd.DataFrame(columns=["adset_id", "pincodes"])
        
    return pd.DataFrame(adset_pincode_map)

def generate_excel_report(output_path: str, new_table: pd.DataFrame, insights: pd.DataFrame = None):
    """Writes the final table incrementally to an Excel file."""
    logger.info(f"Preparing to export data to Excel: {output_path}")
    
    if os.path.exists(output_path):
        logger.info("Existing file found. Appending new data...")
        try:
            existing_df = pd.read_excel(output_path, sheet_name="targeting_data")
            final_table = pd.concat([existing_df, new_table], ignore_index=True)
        except Exception as e:
            logger.error(f"Error loading existing file: {e}. Defaulting to new data.")
            final_table = new_table
    else:
        logger.info("No existing file found. Creating new...")
        final_table = new_table

    # Normalize IDs and deduplicate
    # Ensure IDs are strings and stripped
    id_cols = ['campaign_id', 'adset_id', 'ad_id']
    for col in id_cols:
        if col in final_table.columns:
            final_table[col] = final_table[col].astype(str).str.strip()
            
    # Normalize ad_name (trim whitespace)
    if 'ad_name' in final_table.columns:
        final_table['ad_name'] = final_table['ad_name'].astype(str).str.strip()

    # Define uniqueness: date, campaign_id, adset_id, ad_name
    dedup_keys = ['date', 'campaign_id', 'adset_id', 'ad_name']
    
    # Ensure all dedup keys exist in columns before dropping
    existing_keys = [k for k in dedup_keys if k in final_table.columns]
    
    initial_len = len(final_table)
    # Keep 'last' ensures we keep the newest record from the latest fetch
    final_table = final_table.drop_duplicates(subset=existing_keys, keep='last')
    logger.info(f"Deduplication removed {initial_len - len(final_table)} duplicate or historical rows.")
    
    # Sort by date descending (latest first)
    if "date" in final_table.columns:
        final_table["date"] = pd.to_datetime(final_table["date"])
        final_table = final_table.sort_values(by="date", ascending=False)
        final_table["date"] = final_table["date"].dt.strftime("%Y-%m-%d")

    # Ensure final columns order
    cols = ["date", "campaign_id", "campaign_name", "adset_id", "adset_name", "ad_name", "pincodes"]
    final_table = final_table[[c for c in cols if c in final_table.columns]]

    try:
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            final_table.to_excel(writer, sheet_name="targeting_data", index=False)
            if insights is not None:
                insights.to_excel(writer, sheet_name="raw_insights", index=False)
        logger.info("Successfully generated Excel report.")
    except Exception as e:
        logger.error(f"Failed to generate Excel report: {e}")

def main():
    config = load_config()
    
    # 1. Fetch Insights
    insights_df = fetch_meta_insights(config)
    
    if insights_df.empty:
        logger.warning("No insights fetched. Generating empty output.")
        generate_excel_report(config["output_excel_path"], pd.DataFrame(columns=["date", "campaign_id", "campaign_name", "adset_id", "adset_name", "pincode"]))
        return
        
    unique_adset_ids = insights_df["adset_id"].dropna().unique().tolist()
        
    # 2. Fetch Targeting Zips Only
    targeting_df = fetch_adset_targeting_zips(config, unique_adset_ids)
    
    # 3. Merge and Aggregate
    logger.info("Merging and aggregating pincodes into strings...")
    final_table = insights_df.merge(targeting_df, on="adset_id", how="left")
    
    # Handle adsets with no pincodes (empty list) vs adsets not in targeting_df
    def format_pincodes(p_list):
        if not isinstance(p_list, list) or not p_list:
            return ""
        # Remove duplicates, sort, and join
        unique_sorted = sorted(list(set(str(p).strip() for p in p_list if str(p).strip())))
        return ", ".join(unique_sorted)

    final_table["pincodes"] = final_table["pincodes"].apply(format_pincodes)
    
    # Remove duplicates
    final_table = final_table.drop_duplicates()
    
    # Sort by date descending (latest first)
    if "date" in final_table.columns:
        final_table["date"] = pd.to_datetime(final_table["date"])
        final_table = final_table.sort_values(by="date", ascending=False)
        final_table["date"] = final_table["date"].dt.strftime("%Y-%m-%d")

    # Ensure final columns order
    cols = ["date", "campaign_id", "campaign_name", "adset_id", "adset_name", "ad_name", "pincodes"]
    final_table = final_table[cols]
    
    # 4. Generate Output
    generate_excel_report(config["output_excel_path"], final_table, insights_df)
    
    # 5. Summary
    logger.info("====================================")
    logger.info("          PIPELINE SUMMARY        ")
    logger.info("====================================")
    logger.info(f"Total Insights Records : {len(insights_df)}")
    logger.info(f"Total Adsets Processed : {len(unique_adset_ids)}")
    logger.info(f"Total Rows Expanded    : {len(final_table)}")
    logger.info(f"Unique Pincodes Found  : {final_table['pincodes'].nunique() if 'pincodes' in final_table.columns else 0}")
    logger.info("====================================")

if __name__ == "__main__":
    main()
