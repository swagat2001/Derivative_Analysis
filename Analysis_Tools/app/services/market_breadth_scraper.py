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
    Scrape market breadth data from NSE
    Returns: dict with advances, declines, unchanged counts

    NOTE: This function only scrapes data.
    To save to database, use Database/Cash/market_breadth_update.py
    """
    driver = None

    try:
        driver = get_headless_driver()

        logger.info("Accessing NSE Homepage...")
        driver.get("https://www.nseindia.com")
        time.sleep(3)

        # Scrape each category
        advances = scrape_nse_count(driver, "https://www.nseindia.com/market-data/advance", "Advances")
        declines = scrape_nse_count(driver, "https://www.nseindia.com/market-data/decline", "Declines")
        unchanged = scrape_nse_count(driver, "https://www.nseindia.com/market-data/unchanged", "Unchanged")

        result = {
            "advances": advances,
            "declines": declines,
            "unchanged": unchanged,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "date": datetime.now().strftime("%Y-%m-%d")
        }

        logger.info(f"Market Breadth: {result}")
        return result

    except Exception as e:
        logger.error(f"Error in get_market_breadth: {str(e)}")
        return {
            "advances": 0,
            "declines": 0,
            "unchanged": 0,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "date": datetime.now().strftime("%Y-%m-%d"),
            "error": str(e)
        }

    finally:
        if driver:
            driver.quit()


if __name__ == "__main__":
    # Test the scraper
    logging.basicConfig(level=logging.INFO)
    data = get_market_breadth()
    print("\nMarket Breadth Data:", data)
    print("\n⚠️  NOTE: To save to database, run:")
    print("    python Database/Cash/market_breadth_update.py")
