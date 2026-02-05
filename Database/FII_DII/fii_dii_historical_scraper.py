"""
FII/DII HISTORICAL DATA SCRAPER
Scrapes historical FII/DII cash market data from MoneyControl.

This script fetches historical FII/DII activity data and stores it in PostgreSQL.
Since NSE doesn't provide historical cash market data, we use MoneyControl as the source.

Location: Database/FII_DII/fii_dii_historical_scraper.py

Usage:
    python fii_dii_historical_scraper.py                    # Fetch last 90 days
    python fii_dii_historical_scraper.py --days 180         # Fetch last 180 days
    python fii_dii_historical_scraper.py --start 2024-01-01 --end 2024-12-31
"""

import argparse
import random
import sys
import time
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text

# =============================================================
# DATABASE CONFIGURATION
# =============================================================
db_user = "postgres"
db_password = "Gallop@3104"
db_host = "localhost"
db_port = "5432"
db_name = "BhavCopy_Database"

db_password_enc = quote_plus(db_password)
engine = create_engine(
    f"postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}",
    pool_pre_ping=True,
)


# =============================================================
# DATA SCRAPING
# =============================================================
def scrape_moneycontrol_month(year, month, participant_type="fii"):
    """
    Scrape FII or DII data for a specific month from MoneyControl.

    Args:
        year: Year (e.g., 2024)
        month: Month (1-12)
        participant_type: 'fii' or 'dii'

    Returns:
        List of dict with keys: date, buy_value, sell_value, net_value
    """
    url = f"https://www.moneycontrol.com/stocks/marketstats/fii_dii_activity/index.php"

    params = {
        "opttopic": "Fii" if participant_type.lower() == "fii" else "Dii",
        "optinst": "allinstitutions",
        "sel_mth": f"{month:02d}",
        "sel_yr": str(year),
        "submit": "GO",
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    try:
        print(f"  Fetching {participant_type.upper()} data for {year}-{month:02d}...", end="")
        response = requests.get(url, params=params, headers=headers, timeout=15)

        if response.status_code != 200:
            print(f" ❌ HTTP {response.status_code}")
            return []

        soup = BeautifulSoup(response.text, "html.parser")

        # Find the data table (MoneyControl uses 'mctable1' class)
        table = soup.find("table", class_="mctable1")

        if not table:
            print(" ❌ Table not found")
            return []

        rows = table.find_all("tr")[1:]  # Skip header row
        data = []

        for row in rows:
            cols = row.find_all("td")
            if len(cols) >= 4:
                try:
                    date_str = cols[0].text.strip()
                    buy = parse_value(cols[1].text)
                    sell = parse_value(cols[2].text)
                    net = parse_value(cols[3].text)

                    # Parse date
                    trade_date = parse_moneycontrol_date(date_str)

                    if trade_date:
                        data.append({"date": trade_date, "buy_value": buy, "sell_value": sell, "net_value": net})
                except Exception as e:
                    continue

        print(f" ✅ Found {len(data)} records")
        return data

    except Exception as e:
        print(f" ❌ Error: {e}")
        return []


def parse_value(text):
    """Parse value like '1,234.56' or '(123.45)' to float."""
    try:
        text = text.strip().replace(",", "").replace("(", "-").replace(")", "")
        return float(text) if text and text != "-" else 0.0
    except:
        return 0.0


def parse_moneycontrol_date(date_str):
    """Parse MoneyControl date format '03 Feb 2024' to date object."""
    try:
        # Try multiple formats
        for fmt in ["%d %b %Y", "%d-%b-%Y", "%d %B %Y"]:
            try:
                return datetime.strptime(date_str.strip(), fmt).date()
            except:
                continue
        return None
    except:
        return None


# =============================================================
# DATABASE OPERATIONS
# =============================================================
def ensure_table_exists():
    """Create fii_dii_activity table if it doesn't exist."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS fii_dii_activity (
        id SERIAL PRIMARY KEY,
        trade_date DATE NOT NULL UNIQUE,
        fii_buy_value NUMERIC(20, 2) DEFAULT 0,
        fii_sell_value NUMERIC(20, 2) DEFAULT 0,
        fii_net_value NUMERIC(20, 2) DEFAULT 0,
        dii_buy_value NUMERIC(20, 2) DEFAULT 0,
        dii_sell_value NUMERIC(20, 2) DEFAULT 0,
        dii_net_value NUMERIC(20, 2) DEFAULT 0,
        total_net_value NUMERIC(20, 2) DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_fii_dii_trade_date ON fii_dii_activity(trade_date DESC);
    """

    try:
        with engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
        return True
    except Exception as e:
        print(f"[ERROR] Creating table: {e}")
        return False


def merge_fii_dii_data(fii_data, dii_data):
    """Merge FII and DII data by date."""
    merged = {}

    # Add FII data
    for item in fii_data:
        date = item["date"]
        merged[date] = {
            "trade_date": date,
            "fii_buy_value": item["buy_value"],
            "fii_sell_value": item["sell_value"],
            "fii_net_value": item["net_value"],
            "dii_buy_value": 0,
            "dii_sell_value": 0,
            "dii_net_value": 0,
        }

    # Add DII data
    for item in dii_data:
        date = item["date"]
        if date in merged:
            merged[date]["dii_buy_value"] = item["buy_value"]
            merged[date]["dii_sell_value"] = item["sell_value"]
            merged[date]["dii_net_value"] = item["net_value"]
        else:
            merged[date] = {
                "trade_date": date,
                "fii_buy_value": 0,
                "fii_sell_value": 0,
                "fii_net_value": 0,
                "dii_buy_value": item["buy_value"],
                "dii_sell_value": item["sell_value"],
                "dii_net_value": item["net_value"],
            }

    # Calculate total_net_value
    for rec in merged.values():
        rec["total_net_value"] = rec["fii_net_value"] + rec["dii_net_value"]

    return list(merged.values())


def save_to_database(records):
    """Save records to database with upsert logic."""
    if not records:
        return 0

    upsert_sql = """
    INSERT INTO fii_dii_activity (
        trade_date, fii_buy_value, fii_sell_value, fii_net_value,
        dii_buy_value, dii_sell_value, dii_net_value, total_net_value, updated_at
    ) VALUES (
        :trade_date, :fii_buy_value, :fii_sell_value, :fii_net_value,
        :dii_buy_value, :dii_sell_value, :dii_net_value, :total_net_value, CURRENT_TIMESTAMP
    )
    ON CONFLICT (trade_date) DO UPDATE SET
        fii_buy_value = EXCLUDED.fii_buy_value,
        fii_sell_value = EXCLUDED.fii_sell_value,
        fii_net_value = EXCLUDED.fii_net_value,
        dii_buy_value = EXCLUDED.dii_buy_value,
        dii_sell_value = EXCLUDED.dii_sell_value,
        dii_net_value = EXCLUDED.dii_net_value,
        total_net_value = EXCLUDED.total_net_value,
        updated_at = CURRENT_TIMESTAMP;
    """

    saved_count = 0
    try:
        with engine.connect() as conn:
            for rec in records:
                conn.execute(text(upsert_sql), rec)
                saved_count += 1
            conn.commit()
    except Exception as e:
        print(f"[ERROR] Saving to database: {e}")

    return saved_count


# =============================================================
# MAIN SCRAPER LOGIC
# =============================================================
def scrape_date_range(start_date, end_date):
    """Scrape FII/DII data for a date range."""
    print("\n" + "=" * 70)
    print("FII/DII HISTORICAL DATA SCRAPER")
    print("=" * 70)
    print(f"Date Range: {start_date} to {end_date}")
    print(f"Source: MoneyControl")
    print("=" * 70 + "\n")

    # Ensure table exists
    if not ensure_table_exists():
        print("Failed to create database table")
        return

    # Generate list of year-month combinations
    current_date = start_date
    months_to_fetch = []

    while current_date <= end_date:
        months_to_fetch.append((current_date.year, current_date.month))
        # Move to next month
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1, day=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1, day=1)

    print(f"Fetching data for {len(months_to_fetch)} month(s)...\n")

    all_records = []

    for year, month in months_to_fetch:
        print(f"Month {year}-{month:02d}:")

        # Fetch FII data
        fii_data = scrape_moneycontrol_month(year, month, "fii")
        time.sleep(random.uniform(1, 2))  # Be polite

        # Fetch DII data
        dii_data = scrape_moneycontrol_month(year, month, "dii")
        time.sleep(random.uniform(1, 2))

        # Merge and filter by date range
        merged = merge_fii_dii_data(fii_data, dii_data)
        filtered = [r for r in merged if start_date <= r["trade_date"] <= end_date]

        all_records.extend(filtered)
        print()

    # Sort by date
    all_records.sort(key=lambda x: x["trade_date"])

    print("\n" + "=" * 70)
    print(f"Total Records Fetched: {len(all_records)}")
    print("=" * 70 + "\n")

    if all_records:
        print("Saving to database...", end="")
        saved = save_to_database(all_records)
        print(f" ✅ Saved {saved} records\n")

        # Show sample
        print("Sample Data (Last 5 days):")
        print("-" * 70)
        for rec in all_records[-5:]:
            print(
                f"{rec['trade_date']}: FII Net = ₹{rec['fii_net_value']:+,.0f} Cr, DII Net = ₹{rec['dii_net_value']:+,.0f} Cr"
            )
        print()
    else:
        print("⚠️ No data fetched. Please check the date range or try again later.\n")


# =============================================================
# CLI
# =============================================================
def main():
    parser = argparse.ArgumentParser(description="Scrape historical FII/DII data from MoneyControl")
    parser.add_argument("--days", type=int, default=90, help="Number of days to fetch (default: 90)")
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD)")

    args = parser.parse_args()

    # Determine date range
    if args.start and args.end:
        start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end, "%Y-%m-%d").date()
    else:
        end_date = datetime.now().date() - timedelta(days=1)  # Yesterday
        start_date = end_date - timedelta(days=args.days)

    scrape_date_range(start_date, end_date)

    print("=" * 70)
    print("✅ SCRAPING COMPLETE!")
    print("=" * 70)
    print("\nNext Steps:")
    print("1. Check your Insights page: http://127.0.0.1:5000/insights/?tab=fii-dii")
    print("2. Set up daily updates: python fii_dii_daily_updater.py")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
