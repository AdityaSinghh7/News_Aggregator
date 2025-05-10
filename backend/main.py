import asyncio
import logging
import os
from datetime import datetime
from backend.db import engine
from backend.rss_scraper.base_scraper import BaseRSSScraper, fetch_news_sources, KEYWORDS
# import os

# print("Using DB:", os.getenv("DATABASE_URL"))

# Create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)
TIMEOUT = 300

# Setup main logger
logging.basicConfig(level=logging.INFO)

# Setup special logger for failed sources
failed_sources_logger = logging.getLogger('failed_sources')
failed_sources_logger.setLevel(logging.ERROR)

# Current date for log filename
today = datetime.now().strftime('%Y-%m-%d')
file_handler = logging.FileHandler(f'logs/failed_sources_{today}.log')
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)
failed_sources_logger.addHandler(file_handler)

async def run_scraping_job():
    successful_sources_processing = 0  # Initialize counter
    failed_sources_processing = 0      # Initialize counter

    async with engine.begin() as conn:
        sources = await fetch_news_sources(conn)
        if not sources:
            logging.info("No news sources found to process.")
            return
        tasks = []
        for source_id, rss_url in sources:
            scraper = BaseRSSScraper(engine, source_id, rss_url, KEYWORDS)
            tasks.append(asyncio.wait_for(scraper.fetch_and_store(), timeout=TIMEOUT))  # 60s timeout
        
        if not tasks:
            logging.info("No scraping tasks were created.")
            return
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, result in enumerate(results):
            # Assuming sources order matches tasks order for logging purposes
            source_id_log, rss_url_log = sources[i] if i < len(sources) else ("Unknown", "Unknown")
            if isinstance(result, Exception):
                # Log detailed error for failed sources
                error_msg = f"SOURCE_ID: {source_id_log} | URL: {rss_url_log} | ERROR: "
                if isinstance(result, asyncio.TimeoutError):
                    error_msg += f"Timed out after {TIMEOUT} seconds"
                    logging.error(f"Scraping task for source ID {source_id_log} ({rss_url_log}) timed out")
                else:
                    error_msg += f"{type(result).__name__}: {str(result)}"
                    logging.error(f"Scraping task for source ID {source_id_log} ({rss_url_log}) failed: {result}")
                
                # Log to the special failed sources log file
                failed_sources_logger.error(error_msg)
                failed_sources_processing += 1
            else:
                # fetch_and_store doesn't return a meaningful value to indicate success/failure count
                # It logs errors internally. We assume completion without exception means it attempted its work.
                logging.info(f"Scraping task for source ID {source_id_log} ({rss_url_log}) completed.")
                successful_sources_processing += 1
    
    logging.info(f"RSS scraping job complete. Successful: {successful_sources_processing}, Failed: {failed_sources_processing}")
    failed_sources_logger.error(f"SUMMARY: {failed_sources_processing} of {len(sources)} sources failed. See details above.")

if __name__ == "__main__":
    asyncio.run(run_scraping_job())
