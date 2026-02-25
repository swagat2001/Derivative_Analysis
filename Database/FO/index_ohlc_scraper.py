import sys
import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Ensure we can import app modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "Analysis_Tools")))
from app.models.db_config import engine_cash

from io import StringIO
import urllib.request
import urllib.error

# =============================================================================
# CONSTANTS & CONFIG
# =============================================================================
NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# =============================================================================
# DATABASE INIT
# =============================================================================
def init_db():
    """Create the index_historical_data table if it doesn't exist."""
    print("[INFO] Checking database schema...")
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS index_historical_data (
        date DATE NOT NULL,
        index_name VARCHAR(100) NOT NULL,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        PRIMARY KEY (date, index_name)
    );
    """
    with engine_cash.begin() as conn:
        conn.execute(text(create_table_sql))
    print("[INFO] Database schema verified.")

# =============================================================================
# SCRAPING LOGIC
# =============================================================================
def fetch_archive_for_date(target_date: datetime) -> pd.DataFrame:
    """Fetch the ind_close_all_DDMMYYYY.csv file for a given date."""
    date_str = target_date.strftime("%d%m%Y")
    url = f"https://archives.nseindia.com/content/indices/ind_close_all_{date_str}.csv"

    try:
        req = urllib.request.Request(url, headers=NSE_HEADERS)
        res = urllib.request.urlopen(req, timeout=10)
        csv_data = res.read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_data))

        # Columns in CSV:
        # Index Name,Index Date,Open Index Value,High Index Value,Low Index Value,Closing Index Value,Points Change,Change(%),Volume,Turnover (Rs. Cr.),P/E,P/B,Div Yield
        col_mapping = {
            "Index Name": "index_name",
            "Index Date": "date",
            "Open Index Value": "open",
            "High Index Value": "high",
            "Low Index Value": "low",
            "Closing Index Value": "close"
        }

        # Strip whitespace from column names just in case
        df.columns = df.columns.str.strip()

        # Rename and filter
        df = df.rename(columns=col_mapping)
        columns_to_keep = ['date', 'index_name', 'open', 'high', 'low', 'close']
        df = df[[col for col in columns_to_keep if col in df.columns]]

        # Parse dates -> 20-02-2026
        df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')

        # Format index_name to uppercase strings for uniqueness consistency
        df['index_name'] = df['index_name'].str.strip().str.upper()

        # Sanitize index names: NIFTY 50 -> NIFTY50, but we want to map back to our INDEX_METADATA.
        return df

    except urllib.error.HTTPError as e:
        if e.code == 404:
            # Expected on weekends/holidays
            return pd.DataFrame()
        print(f"[ERROR] HTTP Error {e.code} for {date_str}: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"[ERROR] Failed to fetch data for {date_str}: {e}")
        return pd.DataFrame()

# =============================================================================
# MAIN EXECUTOR
# =============================================================================
def main():
    print("==================================================")
    print(" NSE INDEX OHLC ARCHIVE SCRAPER ")
    print("==================================================")
    init_db()

    # We want to bootstrap 2 years of history if the table is empty.
    # Otherwise, we just append from the latest date we have.
    end_date = datetime.now()

    # Get the latest date from the database
    latest_date_query = text("SELECT MAX(date) FROM index_historical_data;")
    with engine_cash.connect() as conn:
        latest_date = conn.execute(latest_date_query).scalar()

    if latest_date:
        # Convert explicit python date object to datetime
        start_date = datetime.combine(latest_date, datetime.min.time()) + timedelta(days=1)
        print(f"[INFO] Database has data up to {latest_date}. Appending missing days.")
    else:
        start_date = end_date - timedelta(days=750)
        print(f"[INFO] Database is empty. Bootstrapping archives from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    if start_date > end_date:
        print("[INFO] Database is already up to date. No new dates found.")
        return

    print(f"[INFO] Bootstrapping archives from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    current_date = start_date
    total_records_inserted = 0
    days_processed = 0

    # Create the upsert query outside the loop
    upsert_query = text("""
        INSERT INTO index_historical_data (date, index_name, open, high, low, close)
        VALUES (:date, :index_name, :open, :high, :low, :close)
        ON CONFLICT (date, index_name) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close;
    """)

    while current_date <= end_date:
        # Skip weekends before making requests
        if current_date.weekday() >= 5:
            current_date += timedelta(days=1)
            continue

        date_str_display = current_date.strftime("%Y-%m-%d")

        df = fetch_archive_for_date(current_date)

        if not df.empty:
            try:
                with engine_cash.begin() as conn:
                    batch_params = []
                    for _, row in df.iterrows():
                        # Sometimes values are strings with commas like "22,000.50"
                        def clean_float(val):
                            if pd.isna(val) or val == '-': return None
                            if isinstance(val, str):
                                val = val.replace(',', '')
                            return float(val)

                        record = {
                            "date": row['date'].date(),
                            "index_name": row['index_name'],
                            "open": clean_float(row['open']),
                            "high": clean_float(row['high']),
                            "low": clean_float(row['low']),
                            "close": clean_float(row['close']),
                        }
                        if record['date'] and record['close'] is not None:
                            batch_params.append(record)

                    if batch_params:
                        conn.execute(upsert_query, batch_params)
                        total_records_inserted += len(batch_params)
                        if days_processed % 10 == 0:
                            print(f"[OK] {date_str_display}: Saved {len(batch_params)} indices. Running total: {total_records_inserted}")
            except Exception as e:
                print(f"[ERROR] Database error on {date_str_display}: {e}")

        days_processed += 1
        current_date += timedelta(days=1)

        # Throttle heavily so we don't get IP banned grabbing 700 files
        time.sleep(0.5)

    print("==================================================")
    print(f"[SUMMARY] Total index records saved/updated: {total_records_inserted}")
    print("==================================================")

if __name__ == "__main__":
    main()
