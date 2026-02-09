
import sys
import os
import datetime
import time
import json
from pathlib import Path

# Ensure we are in the correct directory for imports
os.chdir(os.path.dirname(os.path.abspath(__file__)))

try:
    from screenerScraper import ScreenerScrape
    from manualScraper import create_directories, save_data, log_message, scrape_company, OUTPUT_DIR, SAVE_FORMAT
except ImportError:
    # If running from project root
    sys.path.append(os.path.join(os.getcwd(), "Data_scraper"))
    from Data_scraper.screenerScraper import ScreenerScrape
    from Data_scraper.manualScraper import create_directories, save_data, log_message, scrape_company, OUTPUT_DIR, SAVE_FORMAT

COMPANIES = [
    "INDIGO",
    "BEL",
    "JIOFIN",
    "TRENT",
    "SHRIRAMFIN",
    # "ETERNAL" - skipping potentially invalid one unless confirmed
]

def main():
    print("Starting scraper for missing companies...")
    create_directories()

    sc = ScreenerScrape()

    for symbol in COMPANIES:
        try:
            print(f"Scraping {symbol}...")
            scrape_company(sc, symbol)
        except Exception as e:
            print(f"Failed {symbol}: {e}")

    print("Done.")

if __name__ == "__main__":
    main()
