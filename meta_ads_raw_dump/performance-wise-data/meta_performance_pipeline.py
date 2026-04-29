import os
import json
import logging
import datetime
import requests
import pandas as pd
from typing import List, Dict, Optional
from dotenv import load_dotenv

# Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def load_config():
    """Load configuration from .env and validate."""
    # .env is expected at the project root: c:\Users\mahar\Documents\Projects\ZenJeevani\Master Data\.env
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", "..", ".env"))
    config = {
        "access_token": os.getenv("META_ACCESS_TOKEN"),
        "ad_account_id": os.getenv("AD_ACCOUNT_ID"),
        "api_version": os.getenv("API_VERSION", "v19.0"),
        "default_start_date": os.getenv("DEFAULT_START_DATE", "2026-02-28"),
        "output_excel_path": os.path.join(os.path.dirname(__file__), "meta_performance_report.xlsx")
    }
    
    if not config["access_token"] or config["access_token"] == "your_access_token_here":
        logger.warning("META_ACCESS_TOKEN is not set or uses default placeholder. API calls will fail.")
    if not config["ad_account_id"] or config["ad_account_id"] == "act_your_ad_account_id_here":
        logger.warning("AD_ACCOUNT_ID is not set or uses default placeholder. API calls will fail.")
        
    return config

def fetch_performance_insights(config: dict, start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """
    Fetch daily insights for all ads in the given ad account.
    """
    if not start_date:
        # Check for a backfill override from a higher-level script or environment
        backfill_date = os.getenv("BACKFILL_START_DATE")
        if backfill_date:
            start = backfill_date
            logger.info(f"Using BACKFILL_START_DATE override: {start}")
        else:
            # 2-day rolling lookback: fetches yesterday + today.
            # Yesterday ensures complete data (Meta finalises the previous day overnight).
            # Today captures any intraday spend already recorded.
            lookback_date = datetime.date.today() - datetime.timedelta(days=2)
            start = lookback_date.strftime("%Y-%m-%d")
    else:
        start = start_date
        
    end = end_date or datetime.date.today().strftime("%Y-%m-%d")
    
    logger.info(f"Fetching performance insights for {config['ad_account_id']} from {start} to {end}")
    
    url = f"https://graph.facebook.com/{config['api_version']}/{config['ad_account_id']}/insights"
    params = {
        "access_token": config["access_token"],
        "fields": "ad_id,campaign_id,campaign_name,adset_id,adset_name,ad_name,spend,impressions,cpm,cpc,ctr,clicks,actions,action_values,cost_per_action_type,date_start",
        "level": "ad",
        "filtering": json.dumps([
            # Include ALL statuses — same as what Meta Ads Manager shows.
            # Previously filtering ACTIVE-only caused missing data for paused/
            # not-delivering/inactive ads that still have historical spend.
            {"field": "ad.effective_status", "operator": "IN",
             "value": ["ACTIVE", "PAUSED", "CAMPAIGN_PAUSED", "ADSET_PAUSED",
                        "INACTIVE", "DISAPPROVED", "PENDING_REVIEW",
                        "PREAPPROVED", "PENDING_BILLING_INFO", "DELETED",
                        "ARCHIVED"]},
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
            
        logger.info(f"Fetched {len(all_data)} performance records.")
    except Exception as e:
        logger.error(f"Error fetching Meta insights: {e}")
        if 'response' in locals() and hasattr(response, 'text'):
            logger.error(f"Response: {response.text}")
    
    if not all_data:
        return pd.DataFrame()
        
    return pd.DataFrame(all_data)

def fetch_ad_creatives(config: dict, ad_ids: List[str]) -> Dict[str, str]:
    """
    Fetch creative image/thumbnail URLs for a list of ad_ids.
    Uses batching and caching to avoid redundant API calls.
    Returns: Dictionary mapping ad_id to image_url.
    """
    unique_ids = [str(x) for x in set(ad_ids) if x and not pd.isna(x)]
    logger.info(f"Fetching creative details for {len(unique_ids)} unique ads...")
    
    creative_cache = {}
    batch_size = 50
    
    for i in range(0, len(unique_ids), batch_size):
        batch = unique_ids[i:i+batch_size]
        url = f"https://graph.facebook.com/{config['api_version']}/"
        params = {
            "access_token": config["access_token"],
            "ids": ",".join(batch),
            "fields": "id,creative{image_url,thumbnail_url,object_story_spec}"
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            for aid, details in data.items():
                creative = details.get("creative", {})
                
                # Priority: image_url > thumbnail_url > spec extraction
                img_url = creative.get("image_url") or creative.get("thumbnail_url")
                
                # Fallback extraction from object_story_spec
                if not img_url and "object_story_spec" in creative:
                    spec = creative["object_story_spec"]
                    link_data = spec.get("link_data", {})
                    video_data = spec.get("video_data", {})
                    img_url = link_data.get("picture") or video_data.get("image_url")
                
                creative_cache[aid] = img_url or ""
        except Exception as e:
            logger.error(f"Error fetching creative batch: {e}")
            if 'response' in locals() and hasattr(response, 'text'):
                logger.error(f"Response: {response.text}")
            
    return creative_cache

def _safe_parse_actions(raw) -> list:
    """
    Safely coerce the actions/action_values field to a Python list.
    The field arrives as a list when freshly fetched from the API.
    It may arrive as a JSON string if the DataFrame was round-tripped
    through Excel or another serialisation layer.
    Returns an empty list on any parse failure.
    """
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        raw = raw.strip()
        if not raw or raw in ("nan", "None", "[]"):
            return []
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, list) else []
        except (json.JSONDecodeError, ValueError):
            return []
    return []


def get_action_value(actions_list, action_type_search):
    """Utility to extract value from Meta actions or action_values lists."""
    actions_list = _safe_parse_actions(actions_list)
    for action in actions_list:
        if not isinstance(action, dict):
            continue
        if action.get('action_type') == action_type_search:
            try:
                return float(action.get('value', 0))
            except (ValueError, TypeError):
                return 0.0
    return 0.0


def get_action_value_first(actions_list, alias_list) -> float:
    """
    Return the value for the FIRST alias that has a non-zero entry.

    Meta fires multiple overlapping action_types for the same conversion
    (e.g. 'purchase', 'offsite_conversion.fb_pixel_purchase', 'omni_purchase'
    all appear in the same row for one purchase event).

    Summing across all aliases multiplies the count. The correct approach is
    to walk the priority list and return the first non-zero value found.
    """
    actions_list = _safe_parse_actions(actions_list)
    for alias in alias_list:
        for action in actions_list:
            if not isinstance(action, dict):
                continue
            if action.get('action_type') == alias:
                try:
                    val = float(action.get('value', 0))
                    if val > 0:
                        return val
                except (ValueError, TypeError):
                    pass
    return 0.0

def process_performance_metrics(df: pd.DataFrame, creative_cache: Dict[str, str]) -> pd.DataFrame:
    """
    Process raw Meta API response into a flattened performance dataset with calculated ratios and image URLs.
    """
    if df.empty:
        return pd.DataFrame()

    processed_records = []
    
    # Pixel Aliases for Conversion metrics
    PURCHASE_ALIASES = [
        "purchase", "offsite_conversion.fb_pixel_purchase", 
        "omni_purchase", "onsite_web_purchase", "onsite_web_app_purchase",
        "web_in_store_purchase", "web_app_in_store_purchase"
    ]
    ATC_ALIASES = [
        "add_to_cart", "offsite_conversion.fb_pixel_add_to_cart", 
        "omni_add_to_cart", "onsite_web_add_to_cart", "onsite_web_app_add_to_cart"
    ]
    LPV_ALIASES = [
        "landing_page_view", "offsite_conversion.fb_pixel_landing_page_view", 
        "omni_landing_page_view"
    ]
    LEAD_ALIASES = [
        "lead", "offsite_conversion.fb_pixel_lead", "omni_lead", 
        "onsite_conversion.messaging_conversation_started_7d", 
        "offsite_content_view_add_meta_leads", "onsite_conversion.lead_grouped"
    ]

    for _, row in df.iterrows():
        actions = row.get('actions', [])
        action_values = row.get('action_values', [])
        ad_id = str(row.get('ad_id', ''))
        
        # Extract Metrics — use first-match across aliases to avoid double-counting.
        # Meta fires multiple overlapping action_types for the same event; summing
        # them multiplies the count. See get_action_value_first() for details.
        link_clicks = get_action_value(actions, "link_click")
        purchases   = get_action_value_first(actions,       PURCHASE_ALIASES)
        atcs        = get_action_value_first(actions,       ATC_ALIASES)
        lp_views    = get_action_value_first(actions,       LPV_ALIASES)
        leads       = get_action_value_first(actions,       LEAD_ALIASES)
        revenue     = get_action_value_first(action_values, PURCHASE_ALIASES)
        
        spend = float(row.get('spend', 0) or 0)

        # Calculated Performance Ratios
        # c2v_ratio : landing_page_views / link_clicks
        c2v_ratio = lp_views / link_clicks if link_clicks > 0 else 0.0
        # cvr       : purchases / link_clicks  (matches Meta Ads Manager definition)
        cvr = purchases / link_clicks if link_clicks > 0 else 0.0
        roas = revenue / spend if spend > 0 else 0.0
        # cost_per_result : spend / purchases only
        cpr = spend / purchases if purchases > 0 else 0.0
        
        # Creative Image Handling
        img_url = creative_cache.get(ad_id, "")
        
        record = {
            "date": row.get('date_start'),
            "campaign_id": row.get('campaign_id'),
            "campaign_name": row.get('campaign_name'),
            "adset_id": row.get('adset_id'),
            "adset_name": row.get('adset_name'),
            "ad_name": row.get('ad_name'),
            "image_url": img_url,
            "spend": round(spend, 2),
            "impressions": int(float(row.get('impressions', 0) or 0)),
            "cpm": round(float(row.get('cpm', 0) or 0), 2),
            "cpc": round(float(row.get('cpc', 0) or 0), 2),
            "ctr": round(float(row.get('ctr', 0) or 0), 4),
            "link_clicks": int(link_clicks),
            "landing_page_views": int(lp_views),
            "c2v_ratio": round(c2v_ratio, 4),
            "cost_per_result": round(cpr, 2),
            "roas": round(roas, 2),
            "cvr": round(cvr, 4),
            "add_to_cart": int(atcs),
            "leads": int(leads),
            "purchases": int(purchases),
            "revenue": round(revenue, 2)
        }
        processed_records.append(record)
        
    res_df = pd.DataFrame(processed_records)
    
    # Sorting: latest date at the top
    if not res_df.empty:
        res_df['date'] = pd.to_datetime(res_df['date'])
        res_df = res_df.sort_values(by='date', ascending=False)
        res_df['date'] = res_df['date'].dt.strftime('%Y-%m-%d')
        
    cols = [
        "date", "campaign_id", "campaign_name", "adset_id", "adset_name", "ad_name", "image_url", 
        "spend", "impressions", "cpm", "cpc", "ctr", "link_clicks", "landing_page_views", 
        "c2v_ratio", "cost_per_result", "roas", "cvr", "add_to_cart", "leads", "purchases", "revenue"
    ]
    return res_df[cols]

def generate_performance_report(output_path: str, new_df: pd.DataFrame):
    """Writes the performance data incrementally to Excel with professional formatting."""
    logger.info(f"Preparing to export performance report to: {output_path}")
    
    if os.path.exists(output_path):
        logger.info("Existing file found. Appending new data...")
        try:
            existing_df = pd.read_excel(output_path, sheet_name="performance_data")
            performance_df = pd.concat([existing_df, new_df], ignore_index=True)
        except Exception as e:
            logger.error(f"Error loading existing file: {e}. Defaulting to new data.")
            performance_df = new_df
    else:
        logger.info("No existing file found. Creating new...")
        performance_df = new_df

    # Normalize IDs and Deduplicate
    # Ensure IDs are strings and stripped
    id_cols = ['campaign_id', 'adset_id', 'ad_id']
    for col in id_cols:
        if col in performance_df.columns:
            performance_df[col] = performance_df[col].astype(str).str.strip()
            
    # Normalize ad_name (trim whitespace)
    if 'ad_name' in performance_df.columns:
        performance_df['ad_name'] = performance_df['ad_name'].astype(str).str.strip()

    # Define uniqueness: date, campaign_id, adset_id, ad_name
    dedup_keys = ['date', 'campaign_id', 'adset_id', 'ad_name']
    
    # Ensure all dedup keys exist in columns before dropping
    existing_keys = [k for k in dedup_keys if k in performance_df.columns]
    
    initial_len = len(performance_df)
    # Keep 'last' ensures we keep the newest record from the latest fetch
    performance_df = performance_df.drop_duplicates(subset=existing_keys, keep='last')
    logger.info(f"Deduplication removed {initial_len - len(performance_df)} duplicate or historical rows.")

    # Sort descending: latest date first
    if 'date' in performance_df.columns:
        performance_df['date'] = pd.to_datetime(performance_df['date'])
        performance_df = performance_df.sort_values(by='date', ascending=False)
        performance_df['date'] = performance_df['date'].dt.strftime('%Y-%m-%d')
        
    try:
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            performance_df.to_excel(writer, sheet_name="performance_data", index=False)
            
            workbook  = writer.book
            worksheet = writer.sheets["performance_data"]
            
            # Format definitions
            pct_fmt = '0.00%'
            num_fmt = '#,##0.00'
            
            # Column mapping for numeric formatting
            format_mapping = {
                "spend": num_fmt,
                "revenue": num_fmt,
                "cost_per_result": num_fmt,
                "cpm": num_fmt,
                "cpc": num_fmt,
                "ctr": pct_fmt,
                "c2v_ratio": pct_fmt,
                "cvr": pct_fmt,
                "roas": num_fmt
            }
            
            # Apply formatting to matched columns
            for col_idx, col_name in enumerate(performance_df.columns):
                if col_name in format_mapping:
                    fmt = format_mapping[col_name]
                    # Rows are 1-indexed. row 1 is header, data starts at row 2.
                    for r_idx in range(2, len(performance_df) + 2):
                        worksheet.cell(row=r_idx, column=col_idx + 1).number_format = fmt
            
        logger.info("Successfully generated Formatted Performance Data report.")
    except Exception as e:
        logger.error(f"Failed to generate Excel report: {e}")

def main():
    config = load_config()
    
    # 1. Fetch Insights
    raw_df = fetch_performance_insights(config)
    
    if raw_df.empty:
        logger.warning("No performance insights fetched for the selected range.")
        return
        
    # 2. Fetch Creative Asset URLs
    unique_ad_ids = raw_df["ad_id"].dropna().unique().tolist()
    creative_cache = fetch_ad_creatives(config, unique_ad_ids)
    
    # 3. Process with Creative Enrichement
    logger.info("Processing Meta metrics into Performance Dataset with Creative assets...")
    processed_df = process_performance_metrics(raw_df, creative_cache)
    
    # 4. Export
    generate_performance_report(config["output_excel_path"], processed_df)
    
    # 5. Summary Output
    logger.info("====================================")
    logger.info("          PERFORMANCE SUMMARY      ")
    logger.info("====================================")
    total_ads = raw_df['ad_id'].nunique()
    logger.info(f"Unique Ads with Creatives : {total_ads}")
    logger.info(f"Total Daily Rows Processed  : {len(processed_df)}")
    logger.info(f"Overall Total Revenue       : {processed_df['revenue'].sum()}")
    logger.info("====================================")

if __name__ == "__main__":
    main()
