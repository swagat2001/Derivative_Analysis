
import sys
import os
import shutil
import time
from pathlib import Path

# Ensure we are in the correct directory for imports
os.chdir(os.path.dirname(os.path.abspath(__file__)))

try:
    from screenerScraper import ScreenerScrape
    from manualScraper import create_directories, scrape_company, OUTPUT_DIR
except ImportError:
    # If running from project root
    sys.path.append(os.path.join(os.getcwd(), "Data_scraper"))
    from Data_scraper.screenerScraper import ScreenerScrape
    from Data_scraper.manualScraper import create_directories, scrape_company, OUTPUT_DIR

# Mapping: {Target_Symbol_In_App: Scrape_Symbol_On_Screener}
TICKER_MAP = {
    # "MAXHEALTH": "MAXHEALTH", # Already done
    "ETERNAL": "543320" # Zomato BSE code
}

DATA_DIRS = [
    "quarterly", "pnl", "balance_sheet", "cashflow", "ratios",
    "shareholding", "price", "annual_reports", "corporate_announcements"
]

def rename_files(scrape_symbol, target_symbol):
    """Rename files from scrape_symbol to target_symbol in all data directories"""
    if scrape_symbol == target_symbol:
        return

    print(f"Renaming data from {scrape_symbol} to {target_symbol}...")

    renamed_count = 0
    for dtype in DATA_DIRS:
        dir_path = os.path.join(OUTPUT_DIR, dtype)

        # Check for JSON
        src_json = os.path.join(dir_path, f"{scrape_symbol}.json")
        dst_json = os.path.join(dir_path, f"{target_symbol}.json")

        if os.path.exists(src_json):
            try:
                # Remove dest if exists to allow overwrite
                if os.path.exists(dst_json):
                    os.remove(dst_json)
                shutil.move(src_json, dst_json)
                renamed_count += 1
            except Exception as e:
                print(f"  [ERROR] Failed to move {src_json}: {e}")

        # Check for CSV (if exists)
        src_csv = os.path.join(dir_path, f"{scrape_symbol}.csv")
        dst_csv = os.path.join(dir_path, f"{target_symbol}.csv")

        if os.path.exists(src_csv):
            try:
                if os.path.exists(dst_csv):
                    os.remove(dst_csv)
                shutil.move(src_csv, dst_csv)
            except Exception as e:
                print(f"  [ERROR] Failed to move {src_csv}: {e}")

    print(f"  Renamed {renamed_count} files.")

from manualScraper import log_message, CONSOLIDATED, DELAY_BETWEEN_REQUESTS, ANNOUNCEMENTS_START_DATE, ANNOUNCEMENTS_END_DATE, save_data, SAVE_FORMAT

def scrape_company_custom(sc, symbol, force_token=None):
    """Scrape all data for a single company with optional forced token"""
    print("\n" + "=" * 70)
    log_message(f"Processing: {symbol}")
    print("=" * 70)

    results = {"symbol": symbol, "success": [], "failed": [], "skipped": []}

    # Get BSE token
    if force_token:
        token = force_token
        log_message(f"Using forced token: {token}")
    else:
        log_message(f"Getting BSE token for {symbol}...")
        token = sc.getBSEToken(symbol)

    if not token:
        log_message(f"[SKIP] Token not found for {symbol}")
        results["skipped"].append("token_not_found")
        return results

    log_message(f"Token: {token}")

    # Load scraper
    try:
        sc.loadScraper(token, consolidated=CONSOLIDATED)
        log_message(f"Loaded scraper (ID: {sc.screenerID})")
        time.sleep(DELAY_BETWEEN_REQUESTS)
    except Exception as e:
        log_message(f"[ERROR] Failed to load scraper: {e}")
        results["failed"].append("scraper_load")
        return results

    # Fetch all data types
    data_tasks = [
        ("quarterly", lambda: sc.quarterlyReport(withAddon=False), "quarterly"),
        ("pnl", lambda: sc.pnlReport(withAddon=False), "pnl"),
        ("balance_sheet", lambda: sc.balanceSheet(withAddon=False), "balance_sheet"),
        ("cashflow", lambda: sc.cashFLow(withAddon=False), "cashflow"),
        ("ratios", lambda: sc.ratios(), "ratios"),
        ("shareholding", lambda: sc.shareHolding(quarterly=False, withAddon=False), "shareholding"),
        ("price", lambda: sc.closePrice(), "price"),
        ("annual_reports", lambda: sc.annualReports(), "annual_reports"),
        (
            "corporate_announcements",
            lambda: sc.corporateAnnouncements(ANNOUNCEMENTS_START_DATE, ANNOUNCEMENTS_END_DATE),
            "corporate_announcements",
        ),
    ]

    for name, fetch_func, data_type in data_tasks:
        try:
            log_message(f"Fetching {name}...")
            data = fetch_func()

            if data:
                # Save using the symbol logic (will save as 543320.json if symbol is 543320)
                if save_data(data, symbol, data_type, SAVE_FORMAT):
                    log_message(f"    [OK] {name} saved")
                    results["success"].append(name)
                else:
                    log_message(f"    [WARN] {name} save failed")
                    results["failed"].append(name)
            else:
                log_message(f"    [SKIP] No {name} data")
                results["skipped"].append(name)

            time.sleep(DELAY_BETWEEN_REQUESTS)
        except Exception as e:
             log_message(f"    [ERROR] fetching {name}: {e}")
             results["failed"].append(name)

    return results

def main():
    print("Starting scraper for missing companies (v2)...")
    create_directories()

    sc = ScreenerScrape()

    for target_symbol, scrape_symbol in TICKER_MAP.items():
        try:
            print(f"\nProcessing {target_symbol} (using {scrape_symbol})...")

            # If scrape_symbol is numeric, use it as token
            force_token = scrape_symbol if scrape_symbol.isdigit() else None

            # Pass scrape_symbol as symbol so it saves as 543320.json (which we rename later)
            scrape_company_custom(sc, scrape_symbol, force_token=force_token)

            # Post-process: Rename if needed
            rename_files(scrape_symbol, target_symbol)

        except Exception as e:
            print(f"Failed {target_symbol}: {e}")
            import traceback
            traceback.print_exc()

    print("\nDone.")

if __name__ == "__main__":
    main()
