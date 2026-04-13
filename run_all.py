import logging
import sys
import os
from combine_pipeline_data import main

# Setup Logging for the wrapper
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    # Check for optional start date argument (e.g., python run_all.py 2026-02-28)
    if len(sys.argv) > 1:
        start_date = sys.argv[1]
        os.environ["BACKFILL_START_DATE"] = start_date
        logger.info(f"Manual Start Date Provided: {start_date}")
    
    logger.info("Starting Meta Pipeline run...")
    try:
        main()
        logger.info("Pipeline run completed successfully.")
    except Exception as e:
        logger.error(f"Pipeline run failed: {e}")
