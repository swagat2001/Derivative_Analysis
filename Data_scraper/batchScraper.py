# -*- coding: utf-8 -*-
"""
Batch Scraper - Fetches data for all stocks and saves to files
Created: 2026-01-24
"""

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
OUTPUT_DIR = "./data"  # Base output directory
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
        f"{OUTPUT_DIR}/logs",
    ]
    for d in dirs:
        Path(d).mkdir(parents=True, exist_ok=True)
    print(f"[OK] Created output directories in: {OUTPUT_DIR}")


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
    """Save data to file(s)"""
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


def run_batch_scraper():
    """Main function to scrape all stocks"""

    # Initialize
    create_directories()
    log_file = f"{OUTPUT_DIR}/logs/scrape_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    # Load scraper and tokens
    sc = ScreenerScrape()
    tokens_df = sc.tokendf.copy()

    # Filter out index symbols (tokens starting with 999 are BSE indices, not stocks)
    tokens_df = tokens_df[~tokens_df["token"].astype(str).str.startswith("999")]
    log_progress(f"Filtered out index symbols. {len(tokens_df)} actual stocks remaining.", log_file)

    total_stocks = len(tokens_df)
    if MAX_STOCKS:
        tokens_df = tokens_df.head(MAX_STOCKS)
        total_stocks = MAX_STOCKS

    log_progress(f"Starting batch scrape for {total_stocks} stocks", log_file)
    log_progress(f"Output directory: {os.path.abspath(OUTPUT_DIR)}", log_file)

    # Fetch market-wide announcements (once, not per stock)
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

    # Track progress
    success_count = 0
    error_count = 0
    skipped_count = 0

    # Process each stock
    for idx, row in tokens_df.iterrows():
        token = str(row["token"])
        symbol = row["symbol"]
        name = row["name"]

        progress = f"[{idx+1}/{total_stocks}]"
        log_progress(f"{progress} Processing: {symbol} ({name})", log_file)

        try:
            # Load the stock
            sc.loadScraper(token, consolidated=CONSOLIDATED)
            time.sleep(DELAY_BETWEEN_REQUESTS)

            # Fetch and save each data type
            if FETCH_QUARTERLY:
                data = sc.quarterlyReport(withAddon=False)
                if save_data(data, symbol, "quarterly", SAVE_FORMAT):
                    log_progress(f"    [OK] Quarterly report saved", log_file)
                time.sleep(DELAY_BETWEEN_REQUESTS)

            if FETCH_PNL:
                data = sc.pnlReport(withAddon=False)
                if save_data(data, symbol, "pnl", SAVE_FORMAT):
                    log_progress(f"    [OK] P&L report saved", log_file)
                time.sleep(DELAY_BETWEEN_REQUESTS)

            if FETCH_BALANCE_SHEET:
                data = sc.balanceSheet(withAddon=False)
                if save_data(data, symbol, "balance_sheet", SAVE_FORMAT):
                    log_progress(f"    [OK] Balance sheet saved", log_file)
                time.sleep(DELAY_BETWEEN_REQUESTS)

            if FETCH_CASHFLOW:
                data = sc.cashFLow(withAddon=False)
                if save_data(data, symbol, "cashflow", SAVE_FORMAT):
                    log_progress(f"    [OK] Cash flow saved", log_file)
                time.sleep(DELAY_BETWEEN_REQUESTS)

            if FETCH_RATIOS:
                data = sc.ratios()
                if save_data(data, symbol, "ratios", SAVE_FORMAT):
                    log_progress(f"    [OK] Ratios saved", log_file)
                time.sleep(DELAY_BETWEEN_REQUESTS)

            if FETCH_SHAREHOLDING:
                data = sc.shareHolding(quarterly=False, withAddon=False)
                if save_data(data, symbol, "shareholding", SAVE_FORMAT):
                    log_progress(f"    [OK] Shareholding saved", log_file)
                time.sleep(DELAY_BETWEEN_REQUESTS)

            if FETCH_PRICE:
                data = sc.closePrice()
                if save_data(data, symbol, "price", SAVE_FORMAT):
                    log_progress(f"    [OK] Price data saved", log_file)
                time.sleep(DELAY_BETWEEN_REQUESTS)

            if FETCH_ANNUAL_REPORTS:
                try:
                    data = sc.annualReports()
                    if save_data(data, symbol, "annual_reports", SAVE_FORMAT):
                        log_progress(f"    [OK] Annual reports saved", log_file)
                except:
                    pass
                time.sleep(DELAY_BETWEEN_REQUESTS)

            if FETCH_CORPORATE_ANNOUNCEMENTS:
                try:
                    data = sc.corporateAnnouncements(ANNOUNCEMENTS_START_DATE, ANNOUNCEMENTS_END_DATE)
                    if save_data(data, symbol, "corporate_announcements", SAVE_FORMAT):
                        log_progress(f"    [OK] Corporate announcements saved", log_file)
                except Exception as ann_err:
                    log_progress(f"    [WARN] Corporate announcements: {ann_err}", log_file)

            success_count += 1

        except Exception as e:
            error_msg = str(e)
            if "Unable to find screener ID" in error_msg:
                log_progress(f"    [SKIP] Not available on Screener.in", log_file)
                skipped_count += 1
            else:
                log_progress(f"    [ERROR] {error_msg}", log_file)
                error_count += 1

        # Delay between stocks to avoid rate limiting
        time.sleep(DELAY_BETWEEN_STOCKS)

    # Final summary
    log_progress("=" * 50, log_file)
    log_progress("BATCH SCRAPE COMPLETED", log_file)
    log_progress(f"  Total stocks: {total_stocks}", log_file)
    log_progress(f"  Successful: {success_count}", log_file)
    log_progress(f"  Skipped (not on Screener): {skipped_count}", log_file)
    log_progress(f"  Errors: {error_count}", log_file)
    log_progress(f"  Data saved to: {os.path.abspath(OUTPUT_DIR)}", log_file)
    log_progress("=" * 50, log_file)


# ============================================
# RUN
# ============================================

if __name__ == "__main__":
    run_batch_scraper()
