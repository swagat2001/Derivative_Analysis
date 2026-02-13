"""
Market Breadth Data Pipeline
=============================
Downloads Advances/Declines/Unchanged data from NSE and uploads to PostgreSQL

Steps:
1. Scrape data from NSE using Selenium
2. Upload to PostgreSQL database (market_breadth table)
3. Auto-creates table if not exists
4. Appends new data with ON CONFLICT handling

Table: market_breadth
Database: CashStocks_Database
"""

import os
import sys

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from dotenv import load_dotenv
load_dotenv()

import time
from datetime import datetime
from Analysis_Tools.app.utils.logger import logger
from Analysis_Tools.app.models.db_config import engine_cash as engine
from sqlalchemy import text

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium_stealth import stealth


# ===========================================
# 🔧 NSE Scraper Functions
# ===========================================

def get_headless_driver():
    """Initialize headless Chrome driver with stealth mode"""
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    stealth(driver,
        languages=["en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
    )
    return driver


def scrape_nse_count(driver, url, category):
    """Scrape stock count from NSE page"""
    try:
        logger.info(f"Scraping {category} from {url}")
        driver.get(url)

        wait = WebDriverWait(driver, 20)

        # Wait for table to load
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        time.sleep(2)

        # Find all table rows
        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        count = len(rows)

        logger.info(f"{category}: {count} stocks")
        return count

    except Exception as e:
        logger.error(f"Error scraping {category}: {str(e)}")
        return 0


def get_market_breadth():
    """
    Fetch market breadth data from NSE API
    Returns: dict with advances, declines, unchanged counts
    """
    import requests
    base_url = "https://www.nseindia.com"
    api_url = "https://www.nseindia.com/api/live-analysis-advance"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/market-data/advance-decline",
    }

    try:
        session = requests.Session()
        session.headers.update(headers)

        # Establishing session cookies
        session.get(base_url, timeout=10)

        response = session.get(api_url, timeout=10)
        if response.status_code != 200:
            raise Exception(f"NSE API returned status {response.status_code}")

        data = response.json()

        # NSE Structure: {"timestamp": "...", "advance": {"count": {"Advances": 964, "Declines": 2015, "Unchange": 180, "Total": 3159}}}
        advance_data = data.get("advance", {})
        count_data = advance_data.get("count", {})

        result = {
            "advances": count_data.get("Advances", 0),
            "declines": count_data.get("Declines", 0),
            "unchanged": count_data.get("Unchange", 0),
            "timestamp": data.get("timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            "date": datetime.now().strftime("%Y-%m-%d")
        }

        logger.info(f"✅ Market Breadth via API: {result}")
        return result

    except Exception as e:
        logger.error(f"Error in get_market_breadth API: {str(e)}")
        return {
            "advances": 0,
            "declines": 0,
            "unchanged": 0,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "date": datetime.now().strftime("%Y-%m-%d"),
            "error": str(e)
        }


# ===========================================
# 🗄️ Database Functions
# ===========================================

def create_table_if_not_exists():
    """Create market_breadth table if it doesn't exist"""
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS market_breadth (
                    id SERIAL PRIMARY KEY,
                    date DATE NOT NULL,
                    advances INTEGER NOT NULL,
                    declines INTEGER NOT NULL,
                    unchanged INTEGER NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(date, timestamp)
                )
            """))

            # Create index for faster queries
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_market_breadth_date
                ON market_breadth(date DESC)
            """))

            conn.commit()
            logger.info("✅ Table 'market_breadth' verified/created")

    except Exception as e:
        logger.error(f"Error creating table: {str(e)}")
        raise


def upload_to_database(data):
    """Upload market breadth data to database"""
    try:
        with engine.connect() as conn:
            # Insert or update data
            conn.execute(text("""
                INSERT INTO market_breadth (date, advances, declines, unchanged, timestamp)
                VALUES (:date, :advances, :declines, :unchanged, :timestamp)
                ON CONFLICT (date, timestamp)
                DO UPDATE SET
                    advances = EXCLUDED.advances,
                    declines = EXCLUDED.declines,
                    unchanged = EXCLUDED.unchanged
            """), {
                'date': data['date'],
                'advances': data['advances'],
                'declines': data['declines'],
                'unchanged': data['unchanged'],
                'timestamp': data['timestamp']
            })

            conn.commit()
            logger.info("✅ Market breadth data saved to database")
            return True

    except Exception as e:
        logger.error(f"Error uploading to database: {str(e)}")
        return False


# ===========================================
# 🚀 Main Pipeline
# ===========================================

def main():
    """Main execution pipeline"""
    logger.info("=" * 60)
    logger.info("MARKET BREADTH DATA PIPELINE")
    logger.info("=" * 60)

    try:
        # Step 1: Create table
        logger.info("\n[Step 1/3] Creating/Verifying Database Table...")
        create_table_if_not_exists()

        # Step 2: Scrape data
        logger.info("\n[Step 2/3] Scraping NSE Data...")
        data = get_market_breadth()

        if 'error' in data:
            logger.error(f"Failed to scrape data: {data['error']}")
            return

        if data['advances'] == 0 and data['declines'] == 0:
            logger.warning("No data scraped (0 advances, 0 declines). Skipping upload.")
            return

        # Step 3: Upload to database
        logger.info("\n[Step 3/3] Uploading to Database...")
        success = upload_to_database(data)

        if success:
            logger.info("\n" + "=" * 60)
            logger.info("✅ PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 60)
            logger.info(f"   Date: {data['date']}")
            logger.info(f"   Advances: {data['advances']}")
            logger.info(f"   Declines: {data['declines']}")
            logger.info(f"   Unchanged: {data['unchanged']}")
            logger.info(f"   Timestamp: {data['timestamp']}")
        else:
            logger.error("❌ Failed to upload data to database")

    except Exception as e:
        logger.error(f"Pipeline error: {str(e)}", exc_info=True)


if __name__ == "__main__":
    main()
