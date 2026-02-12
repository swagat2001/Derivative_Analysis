# -*- coding: utf-8 -*-
"""
Batch Scraper - Fetches data for all stocks and saves to files
Created: 2026-01-24
"""

import copy
import datetime
import json
import os
import time
from pathlib import Path

import pandas as pd
from screenerScraper import ScreenerScrape

# ============================================
# CONFIGURATION
# ============================================
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")  # Base output directory
DELAY_BETWEEN_STOCKS = 2  # Seconds to wait between stocks (avoid rate limiting)
DELAY_BETWEEN_REQUESTS = 0.5  # Seconds between API calls for same stock
MAX_STOCKS = None  # Set to a number to limit (e.g., 10 for testing), None for all
CONSOLIDATED = True  # True for consolidated financials, False for standalone
SAVE_FORMAT = "both"  # "csv", "json", or "both"

# What data to fetch (set to False to skip)
FETCH_QUARTERLY = True
FETCH_PNL = True
FETCH_BALANCE_SHEET = True
FETCH_CASHFLOW = True
FETCH_RATIOS = True
FETCH_SHAREHOLDING = True
FETCH_PRICE = True
FETCH_ANNUAL_REPORTS = True  # Links to annual report PDFs
FETCH_CORPORATE_ANNOUNCEMENTS = True  # Company announcements from BSE
FETCH_COMPANY_INFO = True  # Market Cap, High/Low, PE, etc.

# Corporate announcements date range
ANNOUNCEMENTS_START_DATE = datetime.date(2020, 1, 1)  # Start date for announcements
ANNOUNCEMENTS_END_DATE = datetime.date.today()  # End date (today)

# ============================================
# HELPER FUNCTIONS
# ============================================


def create_directories():
    """Create output directory structure"""
    dirs = [
        f"{OUTPUT_DIR}/quarterly",
        f"{OUTPUT_DIR}/pnl",
        f"{OUTPUT_DIR}/balance_sheet",
        f"{OUTPUT_DIR}/cashflow",
        f"{OUTPUT_DIR}/ratios",
        f"{OUTPUT_DIR}/shareholding",
        f"{OUTPUT_DIR}/price",
        f"{OUTPUT_DIR}/annual_reports",
        f"{OUTPUT_DIR}/corporate_announcements",
        f"{OUTPUT_DIR}/market_announcements",
        f"{OUTPUT_DIR}/company_info",
        f"{OUTPUT_DIR}/logs",
    ]
    for d in dirs:
        Path(d).mkdir(parents=True, exist_ok=True)
    print(f"[OK] Created output directories in: {OUTPUT_DIR}")


def load_existing_data(symbol, data_type):
    """Load existing JSON data for a symbol if it exists"""
    path = f"{OUTPUT_DIR}/{data_type}/{symbol}.json"
    print(path)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            pass
    return None


def merge_data(old_data, new_data, data_type):
    """
    Merge new_data into old_data.
    Returns the merged data structure.
    """
    if not old_data:
        return new_data

    merged = copy.deepcopy(old_data)

    if data_type == "price":
        # Handle Price Data (Datasets structure)
        if "datasets" not in merged or "datasets" not in new_data:
            return new_data  # Cannot merge structure mismatch

        for new_ds in new_data["datasets"]:
            found = False
            for old_ds in merged["datasets"]:
                if old_ds.get("metric") == new_ds.get("metric") and old_ds.get("label") == new_ds.get("label"):
                    found = True
                    # Merge values
                    old_values = old_ds.get("values", [])
                    new_values = new_ds.get("values", [])

                    # Create a set for O(1) lookup of existing dates
                    existing_dates = {val[0] for val in old_values}

                    for val in new_values:
                        date = val[0]
                        if date not in existing_dates:
                            old_values.append(val)

                    # Sort by date
                    try:
                        old_values.sort(key=lambda x: x[0])
                    except:
                        pass  # robust sort

                    old_ds["values"] = old_values
                    break

            if not found:
                merged["datasets"].append(new_ds)

    else:
        # Handle Dictionary Data (Quarterly, P&L, etc.)
        # Structure: { "date": [ ...data... ] }
        if isinstance(merged, dict) and isinstance(new_data, dict):
            # Update overwrites keys. This adds new dates and updates existing ones.
            merged.update(new_data)

    return merged


def flatten_data(data_dict):
    """Convert nested dict of lists to flat DataFrame"""
    if not data_dict:
        return pd.DataFrame()

    rows = []
    for date, items in data_dict.items():
        row = {"date": date}
        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict):
                    for k, v in item.items():
                        row[k] = v
        rows.append(row)

    return pd.DataFrame(rows)


def flatten_price_data(data):
    """Flatten price/chart data structure from Screener API

    Input structure:
    {
        "datasets": [
            {
                "metric": "Price",
                "label": "Price on BSE",
                "values": [["2005-04-01", "3.74"], ...],
                "meta": {}
            },
            {
                "metric": "Volume",
                "label": "Volume on BSE",
                "values": [["2005-04-01", 12345, {"delivery": 45}], ...],
                "meta": {}
            }
        ]
    }

    Output DataFrame with columns: date, metric, value, and any metadata columns
    """
    if not isinstance(data, dict) or "datasets" not in data:
        return pd.DataFrame()

    all_rows = []

    for dataset in data["datasets"]:
        metric = dataset.get("metric", "")
        label = dataset.get("label", "")
        values = dataset.get("values", [])

        for value_item in values:
            if len(value_item) >= 2:
                row = {"date": value_item[0], "metric": metric, "label": label, "value": value_item[1]}

                # Handle metadata (like delivery percentage for volume)
                if len(value_item) > 2 and isinstance(value_item[2], dict):
                    for meta_key, meta_val in value_item[2].items():
                        row[f"{meta_key}"] = meta_val

                all_rows.append(row)

    return pd.DataFrame(all_rows)


def save_data(data, symbol, data_type, save_format="both"):
    """Save data to file(s) - Same as batchScraper.py"""
    if data is None or (isinstance(data, dict) and len(data) == 0):
        return False

    base_path = f"{OUTPUT_DIR}/{data_type}/{symbol}"

    try:
        if save_format in ["json", "both"]:
            with open(f"{base_path}.json", "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False, default=str)

        if save_format in ["csv", "both"]:
            # Special handling for price/chart data
            if data_type == "price" and isinstance(data, dict) and "datasets" in data:
                df = flatten_price_data(data)
            elif isinstance(data, dict):
                # For nested data like quarterly reports
                if all(isinstance(v, list) for v in data.values()):
                    df = flatten_data(data)
                else:
                    df = pd.DataFrame([data])
            elif isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                df = pd.DataFrame([{"value": data}])

            if not df.empty:
                df.to_csv(f"{base_path}.csv", index=False, encoding="utf-8-sig")

        return True
    except Exception as e:
        print(f"    [ERROR] Saving {data_type}: {e}")
        return False


def log_progress(message, log_file):
    """Log message to file and console"""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}"
    print(log_entry)
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(log_entry + "\n")


# ============================================
# MAIN BATCH SCRAPER
# ============================================


def load_progress():
    """Load last processed index from file"""
    progress_file = f"{OUTPUT_DIR}/logs/scrape_progress.json"
    if os.path.exists(progress_file):
        try:
            with open(progress_file, "r") as f:
                return json.load(f)
        except:
            pass
    return {"last_index": -1, "processed_count": 0}


def save_progress(index, count):
    """Save current progress"""
    progress_file = f"{OUTPUT_DIR}/logs/scrape_progress.json"
    try:
        with open(progress_file, "w") as f:
            json.dump({"last_index": index, "processed_count": count}, f)
    except Exception as e:
        print(f"Warning: Could not save progress: {e}")


def run_batch_scraper():
    """Main function to scrape all stocks with resume capability and rate limit handling"""

    # Initialize
    create_directories()
    start_time = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = f"{OUTPUT_DIR}/logs/scrape_{start_time}.log"

    # Load scraper and tokens
    sc = ScreenerScrape()
    tokens_df = sc.tokendf.copy()

    # Filter out index symbols
    tokens_df = tokens_df[~tokens_df["token"].astype(str).str.startswith("999")]
    log_progress(f"Filtered out index symbols. {len(tokens_df)} actual stocks remaining.", log_file)

    # Resume Logic
    progress = load_progress()
    last_index = progress["last_index"]

    # If starting fresh (last_index == -1), fetching market announcements
    if last_index == -1:
        log_progress(f"Starting fresh batch scrape", log_file)

        # Fetch market-wide announcements (once)
        log_progress("Fetching market-wide announcements...", log_file)
        try:
            latest_announcements = sc.latestAnnouncements()
            if save_data(latest_announcements, "latest_announcements", "market_announcements", SAVE_FORMAT):
                log_progress("[OK] Latest announcements saved", log_file)
        except Exception as e:
            log_progress(f"[WARN] Could not fetch latest announcements: {e}", log_file)

        try:
            upcoming_results = sc.upcomingResults()
            if save_data(upcoming_results, "upcoming_results", "market_announcements", SAVE_FORMAT):
                log_progress("[OK] Upcoming results saved", log_file)
        except Exception as e:
            log_progress(f"[WARN] Could not fetch upcoming results: {e}", log_file)
    else:
        log_progress(f"RESUMING scrape from index {last_index + 1}", log_file)

    total_stocks = len(tokens_df)
    if MAX_STOCKS:
        tokens_df = tokens_df.head(MAX_STOCKS)
        total_stocks = MAX_STOCKS

    log_progress(f"Targeting {total_stocks} stocks", log_file)
    log_progress(f"Output directory: {os.path.abspath(OUTPUT_DIR)}", log_file)

    # Track progress
    success_count = 0
    error_count = 0
    skipped_count = 0
    consecutive_errors = 0

    # Process each stock
    for idx, row in tokens_df.iterrows():
        # SKIP LOGIC: If we are resuming, skip rows until we reach last_index + 1
        if idx <= last_index:
            continue

        token = str(row["token"])
        symbol = row["symbol"]
        name = row["name"]

        progress_str = f"[{idx+1}/{total_stocks}]"
        log_progress(f"{progress_str} Processing: {symbol} ({name})", log_file)

        retry_count = 0
        max_retries = 3
        stock_processed = False

        while retry_count < max_retries:
            try:
                # Load the stock
                sc.loadScraper(token, consolidated=CONSOLIDATED)
                time.sleep(DELAY_BETWEEN_REQUESTS)

                # Define tasks based on flags
                data_tasks = []
                if FETCH_QUARTERLY:
                    data_tasks.append(("Quarterly", lambda: sc.quarterlyReport(withAddon=False), "quarterly"))
                if FETCH_PNL:
                    data_tasks.append(("P&L", lambda: sc.pnlReport(withAddon=False), "pnl"))
                if FETCH_BALANCE_SHEET:
                    data_tasks.append(("Balance Sheet", lambda: sc.balanceSheet(withAddon=False), "balance_sheet"))
                if FETCH_CASHFLOW:
                    data_tasks.append(("Cash Flow", lambda: sc.cashFLow(withAddon=False), "cashflow"))
                if FETCH_RATIOS:
                    data_tasks.append(("Ratios", lambda: sc.ratios(), "ratios"))
                if FETCH_SHAREHOLDING:
                    data_tasks.append(("Shareholding", lambda: sc.shareHolding(quarterly=False, withAddon=False), "shareholding"))
                if FETCH_PRICE:
                    data_tasks.append(("Price", lambda: sc.closePrice(), "price"))
                if FETCH_COMPANY_INFO:
                    data_tasks.append(("Company Info", lambda: sc.companyInfo(), "company_info"))
                if FETCH_ANNUAL_REPORTS:
                    data_tasks.append(("Annual Reports", lambda: sc.annualReports(), "annual_reports"))
                if FETCH_CORPORATE_ANNOUNCEMENTS:
                    data_tasks.append(("Corp Announcements", lambda: sc.corporateAnnouncements(ANNOUNCEMENTS_START_DATE, ANNOUNCEMENTS_END_DATE), "corporate_announcements"))

                # Process tasks
                stock_success = False
                for task_name, fetch_func, data_type in data_tasks:
                    try:
                        new_data = fetch_func()
                        if new_data:
                            # MERGE LOGIC
                            old_data = load_existing_data(symbol, data_type)
                            merged_data = merge_data(old_data, new_data, data_type)

                            if save_data(merged_data, symbol, data_type, SAVE_FORMAT):
                                log_progress(f"    [OK] {task_name} saved", log_file)
                                stock_success = True
                        time.sleep(DELAY_BETWEEN_REQUESTS)
                    except Exception as task_e:
                        log_progress(f"    [WARN] {task_name}: {str(task_e)[:100]}", log_file)

                # Consider it a success if we processed the stock without exception
                # even if no new data was saved (e.g. all disabled)
                success_count += 1
                consecutive_errors = 0 # Reset error counter on success

                stock_processed = True
                break # Success, exit retry loop

            except Exception as e:
                error_msg = str(e)

                # Check for Rate Limiting (429) via message if identifiable
                if "429" in error_msg or "Too Many Requests" in error_msg:
                    sleep_time = 60 * (retry_count + 1)
                    log_progress(f"    [RATE LIMIT] 429 Detected. Sleeping for {sleep_time}s...", log_file)
                    time.sleep(sleep_time)
                    retry_count += 1
                    consecutive_errors += 1
                elif "Unable to find screener ID" in error_msg:
                    log_progress(f"    [SKIP] Not available on Screener.in", log_file)
                    skipped_count += 1
                    stock_processed = True # Identified as skip, don't retry
                    break
                else:
                    log_progress(f"    [ERROR] Attempt {retry_count+1}/{max_retries}: {error_msg}", log_file)
                    retry_count += 1
                    consecutive_errors += 1
                    time.sleep(5) # Short sleep for other errors

        if not stock_processed:
            log_progress(f"    [FAILED] Could not process {symbol} after retries.", log_file)
            error_count += 1

        # Save check point
        save_progress(idx, success_count)

        # Safety valve for too many consecutive errors (ip ban protection)
        if consecutive_errors >= 10:
            log_progress(f"[CRITICAL] Too many consecutive errors (10). Stopping to protect IP.", log_file)
            break

        # Normal delay between stocks
        time.sleep(DELAY_BETWEEN_STOCKS)

    # Final summary
    log_progress("=" * 50, log_file)
    log_progress("BATCH SCRAPE COMPLETED", log_file)
    log_progress(f"  Total stocks: {total_stocks}", log_file)
    log_progress(f"  Successful: {success_count}", log_file)
    log_progress(f"  Skipped: {skipped_count}", log_file)
    log_progress(f"  Errors: {error_count}", log_file)
    log_progress(f"  Data saved to: {os.path.abspath(OUTPUT_DIR)}", log_file)
    log_progress("=" * 50, log_file)


# ============================================
# RUN
# ============================================

if __name__ == "__main__":
    run_batch_scraper()
