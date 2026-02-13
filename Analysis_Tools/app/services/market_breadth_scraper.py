"""
Market Breadth Data Scraper for NSE (SERVICE LAYER ONLY)
=========================================================
NOTE: Database operations are handled in Database/Cash/market_breadth_update.py
This file only provides the scraping service for use by the application.
"""

import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium_stealth import stealth
import logging

logger = logging.getLogger(__name__)


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
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        time.sleep(2)

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


if __name__ == "__main__":
    # Test the scraper
    logging.basicConfig(level=logging.INFO)
    data = get_market_breadth()
    print("\nMarket Breadth Data:", data)
    print("\n⚠️  NOTE: To save to database, run:")
    print("    python Database/Cash/market_breadth_update.py")
