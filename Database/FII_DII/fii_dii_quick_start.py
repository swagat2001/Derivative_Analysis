"""
FII/DII QUICK START - NSE DATA FETCHER
Fetches available FII/DII cash market data from NSE and populates database.

NSE API typically provides last 2-3 days of data. This script fetches all available
days and saves them. For historical data beyond what NSE provides, you'll need to:
1. Run this script to get recent days
2. Run daily updates to build history over time
3. Or manually import historical CSVs if you have them

Location: Database/FII_DII/fii_dii_quick_start.py

Usage:
    python fii_dii_quick_start.py       # Fetch all available data from NSE
"""

import sys
import time
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import requests
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

NSE_BASE_URL = "https://www.nseindia.com"
NSE_FII_DII_URL = "https://www.nseindia.com/api/fiidiiTradeReact"


# =============================================================
# NSE SESSION
# =============================================================
def create_nse_session():
    """Create NSE session with proper cookies."""
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }
    )

    try:
        session.get(NSE_BASE_URL, timeout=10)
        time.sleep(1)
        session.get(f"{NSE_BASE_URL}/market-data/live-market-indices", timeout=10)
        time.sleep(1)
    except:
        pass

    return session


def fetch_nse_data(session):
    """Fetch FII/DII data from NSE API."""
    session.headers.update(
        {"Accept": "application/json, text/plain, */*", "Referer": f"{NSE_BASE_URL}/market-data/live-market-indices"}
    )

    for attempt in range(3):
        try:
            resp = session.get(NSE_FII_DII_URL, timeout=15)
            if resp.status_code == 200:
                return resp.json()
            time.sleep(2)
        except:
            time.sleep(2)

    return None


def parse_nse_data(raw_data):
    """Parse NSE API JSON response."""
    if not raw_data:
        return []

    date_records = {}

    for item in raw_data:
        try:
            category = item.get("category", "").upper()
            date_str = item.get("date", "")

            # Parse date
            trade_date = None
            for fmt in ["%d-%b-%Y", "%d-%B-%Y", "%Y-%m-%d", "%d-%m-%Y"]:
                try:
                    trade_date = datetime.strptime(date_str, fmt).date()
                    break
                except:
                    continue

            if not trade_date:
                continue

            date_key = trade_date.isoformat()

            if date_key not in date_records:
                date_records[date_key] = {
                    "trade_date": trade_date,
                    "fii_buy_value": 0,
                    "fii_sell_value": 0,
                    "fii_net_value": 0,
                    "dii_buy_value": 0,
                    "dii_sell_value": 0,
                    "dii_net_value": 0,
                }

            def parse_val(v):
                try:
                    return float(str(v).replace(",", ""))
                except:
                    return 0.0

            buy = parse_val(item.get("buyValue"))
            sell = parse_val(item.get("sellValue"))
            net = parse_val(item.get("netValue"))

            if "FII" in category or "FPI" in category:
                date_records[date_key]["fii_buy_value"] = buy
                date_records[date_key]["fii_sell_value"] = sell
                date_records[date_key]["fii_net_value"] = net
            elif "DII" in category:
                date_records[date_key]["dii_buy_value"] = buy
                date_records[date_key]["dii_sell_value"] = sell
                date_records[date_key]["dii_net_value"] = net

        except:
            continue

    records = []
    for rec in date_records.values():
        rec["total_net_value"] = rec["fii_net_value"] + rec["dii_net_value"]
        records.append(rec)

    records.sort(key=lambda x: x["trade_date"])
    return records


# =============================================================
# DATABASE
# =============================================================
def ensure_table():
    """Create table if needed."""
    sql = """
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
            conn.execute(text(sql))
            conn.commit()
        return True
    except Exception as e:
        print(f"[ERROR] Table creation: {e}")
        return False


def save_to_db(records):
    """Save with upsert."""
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
        print(f"[ERROR] Save: {e}")

    return count


def get_db_stats():
    """Get current database stats."""
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    """
                SELECT
                    COUNT(*) as total_days,
                    MIN(trade_date) as oldest_date,
                    MAX(trade_date) as latest_date
                FROM fii_dii_activity
            """
                )
            ).fetchone()
            return result
    except:
        return None


# =============================================================
# MAIN
# =============================================================
def main():
    print("\n" + "=" * 70)
    print("FII/DII QUICK START - NSE DATA FETCHER")
    print("=" * 70)
    print("\nℹ️  NSE API provides last 2-3 days of FII/DII cash market data.")
    print("   For more historical data, run daily updates to build over time.\n")
    print("=" * 70 + "\n")

    # Ensure table
    if not ensure_table():
        print("❌ Failed to create database table")
        return

    # Show current state
    stats = get_db_stats()
    if stats and stats[0] > 0:
        print(f"📊 Current Database Status:")
        print(f"   Total Days: {stats[0]}")
        print(f"   Date Range: {stats[1]} to {stats[2]}\n")
    else:
        print("📊 Database is empty. Fetching initial data...\n")

    # Fetch from NSE
    print("🌐 Connecting to NSE...", end="")
    session = create_nse_session()
    print(" ✅")

    print("📥 Fetching FII/DII data...", end="")
    raw_data = fetch_nse_data(session)

    if not raw_data:
        print(" ❌ Failed")
        print("\n⚠️  NSE API is not responding. Possible reasons:")
        print("   - Network connectivity issues")
        print("   - NSE website is down")
        print("   - Market holiday")
        return

    print(f" ✅ Got response")

    # Parse
    print("🔄 Parsing data...", end="")
    records = parse_nse_data(raw_data)
    print(f" ✅ Found {len(records)} days")

    if not records:
        print("\n⚠️  No FII/DII data available from NSE at this time.")
        return

    # Save
    print("💾 Saving to database...", end="")
    saved = save_to_db(records)
    print(f" ✅ Saved {saved} records")

    # Show results
    print("\n" + "=" * 70)
    print(f"✅ SUCCESS! Added/Updated {saved} days of data")
    print("=" * 70)

    print("\n📋 Data Summary:")
    for rec in records:
        print(
            f"   {rec['trade_date']}: FII Net = ₹{rec['fii_net_value']:+,.0f} Cr, DII Net = ₹{rec['dii_net_value']:+,.0f} Cr"
        )

    # Updated stats
    stats = get_db_stats()
    if stats:
        print(f"\n📊 Updated Database Status:")
        print(f"   Total Days: {stats[0]}")
        print(f"   Date Range: {stats[1]} to {stats[2]}")

    print("\n" + "=" * 70)
    print("📈 NEXT STEPS:")
    print("=" * 70)
    print("1. Check your charts: http://127.0.0.1:5000/insights/?tab=fii-dii")
    print("2. Set up daily updates:")
    print("   python fii_dii_daily_updater.py")
    print("3. Automate with Task Scheduler/Cron (see README.md)")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
