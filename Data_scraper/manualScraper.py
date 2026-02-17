# -*- coding: utf-8 -*-
"""
Manual Company Scraper - For companies that failed in batch scraper
Saves data in same format as batchScraper.py for Flask UI compatibility
"""

import copy
import datetime
import json
import os
import time
from pathlib import Path

import pandas as pd
from screenerScraper import ScreenerScrape

# ============================================================================
# CONFIGURE YOUR COMPANIES HERE - Add symbols that failed in batch scraper
# ============================================================================
COMPANIES = [
    # Already scraped - commented out to avoid re-scraping
    "RELIANCE",
    "TCS", "HDFCBANK", "INFY", "ICICIBANK",
    "HINDUNILVR", "SBIN", "BHARTIARTL", "ITC", "KOTAKBANK",
    "LT", "AXISBANK", "ASIANPAINT", "MARUTI", "WIPRO",
    "FEDERALBNK", "BOSCHLTD",
    # New stocks to scrape - Popular F&O stocks
    "ABB",
    "ADANIENT",
    "ADANIPORTS",
    "APOLLOHOSP",
    "BAJAJ-AUTO",
    "BAJFINANCE",
    "BAJAJFINSV",
    "BPCL",
    "BRITANNIA",
    "CIPLA",
    "COALINDIA",
    "DIVISLAB",
    "DRREDDY",
    "EICHERMOT",
    "GRASIM",
    "HCLTECH",
    "HDFC",
    "HEROMOTOCO",
    "HINDALCO",
    "INDUSINDBK",
    "JSWSTEEL",
    "M&M",
    "NESTLEIND",
    "NTPC",
    "ONGC",
    "POWERGRID",
    "SBILIFE",
    "SHREECEM",
    "SUNPHARMA",
    "TATACONSUM",
    "TATAMOTORS",
    "TATASTEEL",
    "TECHM",
    "TITAN",
    "ULTRACEMCO",
    "UPL",
]

# Configuration
OUTPUT_DIR = "./data"  # Same as batch scraper
CONSOLIDATED = True
SAVE_FORMAT = "both"  # "csv", "json", or "both"
DELAY_BETWEEN_REQUESTS = 0.5
ANNOUNCEMENTS_START_DATE = datetime.date(2020, 1, 1)
ANNOUNCEMENTS_END_DATE = datetime.date.today()

# ============================================================================
# HELPER FUNCTIONS (Same as batchScraper.py)
# ============================================================================


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
        f"{OUTPUT_DIR}/company_info",
        f"{OUTPUT_DIR}/annual_reports",
        f"{OUTPUT_DIR}/corporate_announcements",
        f"{OUTPUT_DIR}/market_announcements",
        f"{OUTPUT_DIR}/logs",
    ]
    for d in dirs:
        Path(d).mkdir(parents=True, exist_ok=True)


def load_existing_data(symbol, data_type):
    """Load existing JSON data for a symbol if it exists"""
    path = f"{OUTPUT_DIR}/{data_type}/{symbol}.json"
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            # print(f"    [WARN] Failed to load existing {data_type}: {e}")
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
                    except Exception:
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
    """Flatten price/chart data structure from Screener API"""
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


def log_message(message):
    """Print formatted log message"""
    timestamp = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}")


# ============================================================================
# MAIN SCRAPER
# ============================================================================


def scrape_company(sc, symbol):
    """Scrape all data for a single company"""
    print("\n" + "=" * 70)
    log_message(f"Processing: {symbol}")
    print("=" * 70)

    results = {"symbol": symbol, "success": [], "failed": [], "skipped": []}

    # Get BSE token
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
        ("company_info", lambda: sc.companyInfo(), "company_info"),
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
            new_data = fetch_func()

            if new_data:
                # MERGE LOGIC START
                old_data = load_existing_data(symbol, data_type)
                merged_data = merge_data(old_data, new_data, data_type)
                # MERGE LOGIC END

                if save_data(merged_data, symbol, data_type, SAVE_FORMAT):
                    log_message(f"    [OK] {name} saved (merged)")
                    results["success"].append(name)
                else:
                    log_message(f"    [WARN] {name} save failed")
                    results["failed"].append(name)
            else:
                log_message(f"    [SKIP] No {name} data")
                results["skipped"].append(name)

            time.sleep(DELAY_BETWEEN_REQUESTS)

        except Exception as e:
            log_message(f"    [ERROR] {name}: {str(e)[:100]}")
            results["failed"].append(name)

    # Summary
    print("\n" + "-" * 70)
    log_message(f"Summary for {symbol}:")
    log_message(
        f"  Success: {len(results['success'])} | Failed: {len(results['failed'])} | Skipped: {len(results['skipped'])}"
    )
    if results["failed"]:
        log_message(f"  Failed: {', '.join(results['failed'])}")
    print("-" * 70)

    return results


def main():
    """Main execution function"""
    print("\n" + "=" * 70)
    print(" MANUAL COMPANY SCRAPER")
    print("=" * 70)
    print(f"Companies: {len(COMPANIES)}")
    print(f"List: {', '.join(COMPANIES)}")
    print(f"Output: {os.path.abspath(OUTPUT_DIR)}")
    print(f"Format: {SAVE_FORMAT}")
    print("=" * 70)

    # Create directories
    create_directories()
    log_message("Created output directories")

    # Initialize scraper
    sc = ScreenerScrape()

    # Track results
    all_results = []
    start_time = datetime.datetime.now()

    # Process each company
    for idx, symbol in enumerate(COMPANIES, 1):
        print("\n\n" + "#" * 70)
        log_message(f"Company {idx}/{len(COMPANIES)}: {symbol}")
        print("#" * 70)

        try:
            result = scrape_company(sc, symbol)
            all_results.append(result)
        except Exception as e:
            log_message(f"[CRITICAL ERROR] {symbol}: {e}")
            all_results.append({"symbol": symbol, "success": [], "failed": ["critical_error"], "skipped": []})

        # Small delay between companies
        time.sleep(2)

    # Final summary
    end_time = datetime.datetime.now()
    duration = (end_time - start_time).total_seconds()

    print("\n\n" + "=" * 70)
    print(" FINAL SUMMARY")
    print("=" * 70)

    total_success = sum(len(r["success"]) for r in all_results)
    total_failed = sum(len(r["failed"]) for r in all_results)
    total_skipped = sum(len(r["skipped"]) for r in all_results)

    print(f"\nCompanies processed: {len(all_results)}")
    print(f"Total datasets downloaded: {total_success}")
    print(f"Total failed: {total_failed}")
    print(f"Total skipped: {total_skipped}")
    print(f"Time taken: {duration:.1f} seconds ({duration/60:.1f} minutes)")

    print("\n" + "-" * 70)
    print("Per-company Results:")
    print("-" * 70)

    for result in all_results:
        success = len(result["success"])
        failed = len(result["failed"])
        total = success + failed + len(result["skipped"])

        if failed == 0 and success > 0:
            status = "[OK]"
        elif failed > 0 and success > 0:
            status = "[PARTIAL]"
        else:
            status = "[FAILED]"

        print(f"{status:10s} {result['symbol']:15s} - Success: {success:2d}/{total:2d}")

    # Save summary
    summary_file = f"{OUTPUT_DIR}/logs/manual_scrape_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump(
            {
                "scrape_date": datetime.datetime.now().isoformat(),
                "companies": COMPANIES,
                "results": all_results,
                "total_companies": len(all_results),
                "total_success": total_success,
                "total_failed": total_failed,
                "total_skipped": total_skipped,
                "duration_seconds": duration,
            },
            f,
            indent=2,
        )

    print("\n" + "=" * 70)
    print(" COMPLETED")
    print(f" Data saved to: {os.path.abspath(OUTPUT_DIR)}")
    print(f" Summary: {summary_file}")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
