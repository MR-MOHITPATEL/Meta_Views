import os
import sys
import datetime
import subprocess
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def main():
    today_str = datetime.date.today().strftime("%Y-%m-%d")
    sync_file = os.path.join(os.path.dirname(__file__), "last_full_sync.txt")
    
    last_sync_date = ""
    if os.path.exists(sync_file):
        with open(sync_file, "r") as f:
            last_sync_date = f.read().strip()
            
    env = os.environ.copy()
    
    if last_sync_date != today_str:
        logger.info(f"No full sync found for {today_str}. Initiating Full Sync (Feb 28 to Today)...")
        env["BACKFILL_START_DATE"] = "2026-02-28"
        env["BACKFILL_END_DATE"] = today_str
        is_full_sync = True
    else:
        logger.info(f"Full sync already completed for {today_str}. Initiating Fast Daily Sync (Today Only)...")
        env["BACKFILL_START_DATE"] = today_str
        env["BACKFILL_END_DATE"] = today_str
        is_full_sync = False
        
    script_path = os.path.join(os.path.dirname(__file__), "combine_pipeline_data.py")
    
    logger.info(f"Running pipeline with START: {env['BACKFILL_START_DATE']}, END: {env['BACKFILL_END_DATE']}")
    
    result = subprocess.run(
        [sys.executable, script_path],
        env=env,
        cwd=os.path.dirname(__file__)
    )
    
    if result.returncode == 0:
        logger.info("Pipeline executed successfully.")
        if is_full_sync:
            with open(sync_file, "w") as f:
                f.write(today_str)
            logger.info(f"Marked {today_str} as Full Sync Complete.")
    else:
        logger.error(f"Pipeline failed with exit code {result.returncode}")

if __name__ == "__main__":
    main()
