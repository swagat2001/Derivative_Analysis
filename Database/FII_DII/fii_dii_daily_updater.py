"""
FII/DII DAILY UPDATER
Fetches latest FII/DII cash market data and updates database.

This script is designed to run daily (after market close) to keep your
FII/DII data current. It tries NSE API first, then falls back to MoneyControl.

Location: Database/FII_DII/fii_dii_daily_updater.py

Usage:
    python fii_dii_daily_updater.py                    # Fetch latest day
    python fii_dii_daily_updater.py --date 2024-02-03  # Specific date

Automation:
    Windows Task Scheduler: Run daily at 6:30 PM
    Linux/Mac Cron: 30 18 * * 1-5 python fii_dii_daily_updater.py
"""

import os
import argparse
import random
from dotenv import load_dotenv

load_dotenv()
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
db_password = os.getenv("DB_PASSWORD")
db_host = "localhost"
db_port = "5432"
db_name = "BhavCopy_Database"

db_password_enc = quote_plus(db_password)
engine = create_engine(
    f"postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}",
    pool_pre_ping=True,
)

NSE_BASE_URL = "https://www.nseindia.com"
NSE_FII_DII_URL = "https://www.nseindia.com/api/fiidiiTradeReact"


# =============================================================
# NSE API
# =============================================================
def create_nse_session():
    """Create NSE session with proper headers and cookies."""
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )

    try:
        # Initialize session
        session.get(NSE_BASE_URL, timeout=10)
        time.sleep(1)
        session.get(f"{NSE_BASE_URL}/market-data/live-market-indices", timeout=10)
        time.sleep(1)
        return session
    except Exception as e:
        print(f"[WARN] NSE session init failed: {e}")
        return session


def fetch_nse_data(session):
    """Fetch latest FII/DII data from NSE API."""
    session.headers.update({"Accept": "application/json, text/plain, */*"})

    try:
        response = session.get(NSE_FII_DII_URL, timeout=15)
        if response.status_code == 200:
            data = response.json()
            return parse_nse_data(data)
        return None
    except Exception as e:
        print(f"[ERROR] NSE API fetch failed: {e}")
        return None


def parse_nse_data(raw_data):
    """Parse NSE API response."""
    if not raw_data:
        return None

    records = {}

    for item in raw_data:
        try:
            category = item.get("category", "").upper()
            date_str = item.get("date", "")

            # Parse date
            trade_date = None
            for fmt in ["%d-%b-%Y", "%d-%B-%Y", "%Y-%m-%d"]:
                try:
                    trade_date = datetime.strptime(date_str, fmt).date()
                    break
                except ValueError:
                    continue

            if not trade_date:
                continue

            if trade_date not in records:
                records[trade_date] = {
                    "trade_date": trade_date,
                    "fii_buy_value": 0,
                    "fii_sell_value": 0,
                    "fii_net_value": 0,
                    "dii_buy_value": 0,
                    "dii_sell_value": 0,
                    "dii_net_value": 0,
                }

            buy = float(str(item.get("buyValue", 0)).replace(",", ""))
            sell = float(str(item.get("sellValue", 0)).replace(",", ""))
            net = float(str(item.get("netValue", 0)).replace(",", ""))

            if "FII" in category or "FPI" in category:
                records[trade_date]["fii_buy_value"] = buy
                records[trade_date]["fii_sell_value"] = sell
                records[trade_date]["fii_net_value"] = net
            elif "DII" in category:
                records[trade_date]["dii_buy_value"] = buy
                records[trade_date]["dii_sell_value"] = sell
                records[trade_date]["dii_net_value"] = net
        except Exception:
            continue

    # Calculate total_net_value
    for rec in records.values():
        rec["total_net_value"] = rec["fii_net_value"] + rec["dii_net_value"]

    return list(records.values())


# =============================================================
# MONEYCONTROL FALLBACK
# =============================================================
def fetch_moneycontrol_data(target_date):
    """Fetch data for a specific date from MoneyControl."""
    year = target_date.year
    month = target_date.month

    print(f"  Trying MoneyControl fallback for {target_date}...")

    # Fetch FII
    fii_data = scrape_moneycontrol_single_day(year, month, target_date, "fii")
    time.sleep(1)

    # Fetch DII
    dii_data = scrape_moneycontrol_single_day(year, month, target_date, "dii")

    if fii_data or dii_data:
        record = {
            "trade_date": target_date,
            "fii_buy_value": fii_data.get("buy_value", 0) if fii_data else 0,
            "fii_sell_value": fii_data.get("sell_value", 0) if fii_data else 0,
            "fii_net_value": fii_data.get("net_value", 0) if fii_data else 0,
            "dii_buy_value": dii_data.get("buy_value", 0) if dii_data else 0,
            "dii_sell_value": dii_data.get("sell_value", 0) if dii_data else 0,
            "dii_net_value": dii_data.get("net_value", 0) if dii_data else 0,
        }
        record["total_net_value"] = record["fii_net_value"] + record["dii_net_value"]
        return [record]

    return None


def scrape_moneycontrol_single_day(year, month, target_date, participant_type):
    """Scrape MoneyControl for a single date."""
    url = "https://www.moneycontrol.com/stocks/marketstats/fii_dii_activity/index.php"

    params = {
        "opttopic": "Fii" if participant_type == "fii" else "Dii",
        "sel_mth": f"{month:02d}",
        "sel_yr": str(year),
        "submit": "GO",
    }

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    try:
        response = requests.get(url, params=params, headers=headers, timeout=15)
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table", class_="mctable1")

        if not table:
            return None

        for row in table.find_all("tr")[1:]:
            cols = row.find_all("td")
            if len(cols) >= 4:
                try:
                    date_str = cols[0].text.strip()
                    row_date = None
                    for fmt in ["%d %b %Y", "%d-%b-%Y"]:
                        try:
                            row_date = datetime.strptime(date_str, fmt).date()
                            break
                        except ValueError:
                            continue

                    if row_date == target_date:
                        return {
                            "buy_value": float(cols[1].text.strip().replace(",", "") or 0),
                            "sell_value": float(cols[2].text.strip().replace(",", "") or 0),
                            "net_value": float(cols[3].text.strip().replace(",", "") or 0),
                        }
                except Exception:
                    continue
        return None
    except Exception:
        return None


# =============================================================
# DATABASE
# =============================================================
def save_to_database(records):
    """Save records with upsert."""
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

    count = 0
    try:
        with engine.connect() as conn:
            for rec in records:
                conn.execute(text(upsert_sql), rec)
                count += 1
            conn.commit()
    except Exception as e:
        print(f"[ERROR] Database save: {e}")
    return count


# =============================================================
# MAIN
# =============================================================
def main():
    parser = argparse.ArgumentParser(description="Update FII/DII data daily")
    parser.add_argument("--date", type=str, help="Specific date (YYYY-MM-DD), default is yesterday")

    args = parser.parse_args()

    # Determine target date
    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        target_date = datetime.now().date() - timedelta(days=1)

    print("\n" + "=" * 60)
    print("FII/DII DAILY UPDATER")
    print("=" * 60)
    print(f"Target Date: {target_date}")
    print("=" * 60 + "\n")

    # Try NSE API first
    print("1. Trying NSE API...", end="")
    session = create_nse_session()
    nse_data = fetch_nse_data(session)

    if nse_data:
        print(f" ✅ Got {len(nse_data)} records from NSE")
        # Filter for target date
        target_records = [r for r in nse_data if r["trade_date"] == target_date]

        if target_records:
            saved = save_to_database(target_records)
            print(f"\n✅ SUCCESS! Saved {saved} record(s) for {target_date}")
            for rec in target_records:
                print(f"   FII Net: ₹{rec['fii_net_value']:+,.0f} Cr")
                print(f"   DII Net: ₹{rec['dii_net_value']:+,.0f} Cr")
            print("\n" + "=" * 60)
            return

    print(" ❌ NSE API failed or no data for target date")

    # Fallback to MoneyControl
    print("\n2. Trying MoneyControl...")
    mc_data = fetch_moneycontrol_data(target_date)

    if mc_data:
        saved = save_to_database(mc_data)
        print(f"\n✅ SUCCESS! Saved {saved} record(s) from MoneyControl")
        for rec in mc_data:
            print(f"   FII Net: ₹{rec['fii_net_value']:+,.0f} Cr")
            print(f"   DII Net: ₹{rec['dii_net_value']:+,.0f} Cr")
    else:
        print("\n❌ FAILED! Could not fetch data from any source")
        print("\nPossible reasons:")
        print("  - Market holiday (weekend/public holiday)")
        print("  - Data not yet published")
        print("  - Network/connectivity issues")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
