import requests
import urllib.parse
import time
import re
from sqlalchemy import text
import sys
import os

# Add project root to sys.path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.append(project_root)

from Analysis_Tools.app.models.db_config import engine_cash
from sqlalchemy import text
import sys

def create_table():
    """Create the index_constituents table if it doesn't exist."""
    print("[INFO] Checking/Creating index_constituents table...")
    try:
        with engine_cash.connect() as conn:
            conn.execute(
                text(
                    """
                CREATE TABLE IF NOT EXISTS index_constituents (
                    index_key VARCHAR(100) NOT NULL,
                    index_name VARCHAR(200),
                    symbol VARCHAR(50) NOT NULL,
                    PRIMARY KEY (index_key, symbol)
                );
                CREATE INDEX IF NOT EXISTS idx_index_key ON index_constituents(index_key);
            """
                )
            )
            conn.commit()
        print("[SUCCESS] Table verified.")
        return True
    except Exception as e:
        print(f"[ERROR] Could not create table: {e}")
        return False

def create_session():
    s = requests.Session()
    s.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.nseindia.com/'
    })
    try:
        s.get('https://www.nseindia.com', timeout=10)
    except:
        pass
    return s

def sanitize_symbol(symbol):
    mapping = {
        "M&M": "M_M",
        "M&MFIN": "M_MFIN",
        "L&TFH": "L_TFH",
        "BAJAJ-AUTO": "BAJAJ_AUTO",
    }
    return mapping.get(symbol, symbol)

def fetch_all():
    session = create_session()
    print("Fetching master list of indices...")
    try:
        r = session.get('https://www.nseindia.com/api/allIndices', timeout=10)
        if r.status_code != 200:
            print(f"Failed to fetch master list. Status: {r.status_code}")
            return
    except Exception as e:
        print(f"Error fetching master list: {e}")
        return

    indices = [item['index'] for item in r.json().get('data', []) if 'index' in item]
    print(f"Found {len(indices)} indices.")

    # Also add fallback BSE Sensex
    sensex_stocks = ['ASIANPAINT', 'AXISBANK', 'BAJAJ_AUTO', 'BAJFINANCE', 'BHARTIARTL', 'HCLTECH', 'HDFCBANK', 'HEROMOTOCO', 'HINDUNILVR', 'ICICIBANK', 'INDUSINDBK', 'INFY', 'ITC', 'JSWSTEEL', 'KOTAKBANK', 'LT', 'M_M', 'MARUTI', 'NESTLEIND', 'NTPC', 'POWERGRID', 'RELIANCE', 'SBIN', 'SUNPHARMA', 'TATAMOTORS', 'TATASTEEL', 'TCS', 'TECHM', 'TITAN', 'ULTRACEMCO']

    # Store results
    records = []

    # Process Sensex
    for symbol in sensex_stocks:
        records.append({'index_key': 'SENSEX', 'index_name': 'SENSEX', 'symbol': symbol})

    total = len(indices)
    for i, idx_name in enumerate(indices):
        index_key = re.sub(r'[^A-Z0-9]', '', idx_name.upper())
        encoded = urllib.parse.quote(idx_name)
        url = f"https://www.nseindia.com/api/equity-stockIndices?index={encoded}"

        retries = 3
        constituents = []
        for attempt in range(retries):
            try:
                res = session.get(url, timeout=10)
                if res.status_code == 200:
                    data = res.json()
                    for item in data.get('data', []):
                        sym = item.get('symbol', '')
                        if sym and not sym.startswith('NIFTY') and sym != 'SENSEX':
                            constituents.append(sanitize_symbol(sym))
                    break
                elif res.status_code == 401:
                    print(f"[{i+1}/{total}] {idx_name}: Session expired, recreating...")
                    session = create_session()
                    time.sleep(1)
                else:
                    time.sleep(1)
            except Exception as e:
                time.sleep(1)

        if constituents:
            print(f"[{i+1}/{total}] {idx_name}: {len(constituents)} stocks")
            for c in constituents:
                records.append({'index_key': index_key, 'index_name': idx_name, 'symbol': c})
        else:
            print(f"[{i+1}/{total}] {idx_name}: FAILED or EMPTY")

        time.sleep(0.5)

    # Save to db
    if records:
        print(f"Saving {len(records)} constituents to database...")
        try:
            with engine_cash.begin() as conn:
                conn.execute(text("DELETE FROM index_constituents"))
                for r in records:
                    conn.execute(text("INSERT INTO index_constituents (index_key, index_name, symbol) VALUES (:index_key, :index_name, :symbol) ON CONFLICT DO NOTHING"), r)
            print("Successfully saved to database!")
        except Exception as e:
            print(f"Error saving to database: {e}")

if __name__ == '__main__':
    if create_table():
        fetch_all()
