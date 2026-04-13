import logging
from combine_pipeline_data import main

# Setup Logging for the wrapper
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("Starting scheduled Meta Pipeline run...")
    try:
        main()
        logger.info("Scheduled run completed successfully.")
    except Exception as e:
        logger.error(f"Scheduled run failed: {e}")
