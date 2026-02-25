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

# ─────────────────────────────────────────────────────────────────
#  CATEGORY MAPPING
#  NSE /api/allIndices returns a `key` field per index row with
#  the official category name in UPPER CASE.
#  We normalise these to the display strings used in the UI.
# ─────────────────────────────────────────────────────────────────

# Maps NSE's `key` field (upper-case) → our display category name.
# Any key NOT in this map is stored as-is (title-cased).
NSE_KEY_TO_CATEGORY = {
    "INDICES ELIGIBLE IN DERIVATIVES": "Indices Eligible in Derivatives",
    "BROAD MARKET INDICES":            "Broad Market Indices",
    "SECTORAL INDICES":                "Sectoral Indices",
    "THEMATIC INDICES":                "Thematic Indices",
    "STRATEGY INDICES":                "Strategy Indices",
    "FIXED INCOME INDICES":            "Fixed Income Indices",
}

# Display order for the UI dropdown (matches NSE page order)
CATEGORY_ORDER = [
    "Indices Eligible in Derivatives",
    "Broad Market Indices",
    "Sectoral Indices",
    "Thematic Indices",
    "Strategy Indices",
    "Fixed Income Indices",
]


def normalise_category(nse_key: str) -> str:
    """Convert NSE's upper-case `key` string to our display category name."""
    return NSE_KEY_TO_CATEGORY.get(nse_key.strip().upper(), nse_key.strip().title())


# ─────────────────────────────────────────────────────────────────

def create_table():
    """Create / migrate the index_constituents table."""
    print("[INFO] Checking/Creating index_constituents table...")
    try:
        with engine_cash.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS index_constituents (
                    index_key      VARCHAR(100) NOT NULL,
                    index_name     VARCHAR(200),
                    symbol         VARCHAR(50)  NOT NULL,
                    index_category VARCHAR(100) DEFAULT 'Other Indices',
                    PRIMARY KEY (index_key, symbol)
                );
            """))
            conn.execute(text("""
                ALTER TABLE index_constituents
                ADD COLUMN IF NOT EXISTS index_category VARCHAR(100) DEFAULT 'Other Indices';
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_index_key      ON index_constituents(index_key);
                CREATE INDEX IF NOT EXISTS idx_index_category ON index_constituents(index_category);
            """))
            conn.commit()
        print("[SUCCESS] Table verified.")
        return True
    except Exception as e:
        print(f"[ERROR] Could not create table: {e}")
        return False


def create_session():
    s = requests.Session()
    s.headers.update({
        'User-Agent':      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept':          'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer':         'https://www.nseindia.com/'
    })
    try:
        s.get('https://www.nseindia.com', timeout=10)
    except Exception:
        pass
    return s


def sanitize_symbol(symbol):
    mapping = {
        "M&M":       "M_M",
        "M&MFIN":    "M_MFIN",
        "L&TFH":     "L_TFH",
        "BAJAJ-AUTO": "BAJAJ_AUTO",
    }
    return mapping.get(symbol, symbol)


def fetch_all():
    session = create_session()

    # ── Step 1: Fetch the master list WITH categories from NSE ──────────────
    print("Fetching master index list from NSE /api/allIndices ...")
    try:
        r = session.get('https://www.nseindia.com/api/allIndices', timeout=10)
        if r.status_code != 200:
            print(f"Failed to fetch master list. HTTP {r.status_code}")
            return
    except Exception as e:
        print(f"Error fetching master list: {e}")
        return

    all_data = r.json().get('data', [])

    # Build a dict: index_name → category  (using the NSE `key` field directly)
    index_category_map = {}
    for item in all_data:
        idx_name = item.get('index', '').strip()
        nse_key  = item.get('key',   '').strip()
        if idx_name and nse_key:
            index_category_map[idx_name] = normalise_category(nse_key)

    # Unique index names in order of first appearance
    seen_indices = []
    seen_set = set()
    for item in all_data:
        idx_name = item.get('index', '').strip()
        if idx_name and idx_name not in seen_set:
            seen_set.add(idx_name)
            seen_indices.append(idx_name)

    print(f"Found {len(seen_indices)} indices across "
          f"{len(set(index_category_map.values()))} categories.")

    # ── Step 2: Hardcoded SENSEX fallback (not in allIndices constituents) ──
    sensex_stocks = [
        'ASIANPAINT', 'AXISBANK', 'BAJAJ_AUTO', 'BAJFINANCE', 'BHARTIARTL',
        'HCLTECH', 'HDFCBANK', 'HEROMOTOCO', 'HINDUNILVR', 'ICICIBANK',
        'INDUSINDBK', 'INFY', 'ITC', 'JSWSTEEL', 'KOTAKBANK', 'LT', 'M_M',
        'MARUTI', 'NESTLEIND', 'NTPC', 'POWERGRID', 'RELIANCE', 'SBIN',
        'SUNPHARMA', 'TATAMOTORS', 'TATASTEEL', 'TCS', 'TECHM', 'TITAN',
        'ULTRACEMCO',
    ]
    records = [
        {'index_key': 'SENSEX', 'index_name': 'SENSEX',
         'symbol': sym, 'index_category': 'Broad Market Indices'}
        for sym in sensex_stocks
    ]

    # ── Step 3: Fetch constituents for each index ────────────────────────────
    total = len(seen_indices)
    for i, idx_name in enumerate(seen_indices):
        index_key = re.sub(r'[^A-Z0-9]', '', idx_name.upper())
        category  = index_category_map.get(idx_name, 'Other Indices')
        encoded   = urllib.parse.quote(idx_name)
        url       = f"https://www.nseindia.com/api/equity-stockIndices?index={encoded}"

        constituents = []
        for attempt in range(3):
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
                    print(f"  [{i+1}/{total}] {idx_name}: session expired — regenerating…")
                    session = create_session()
                    time.sleep(1)
                else:
                    time.sleep(1)
            except Exception:
                time.sleep(1)

        if constituents:
            print(f"  [{i+1}/{total}] [{category}]  {idx_name}  → {len(constituents)} stocks")
            for c in constituents:
                records.append({
                    'index_key':      index_key,
                    'index_name':     idx_name,
                    'symbol':         c,
                    'index_category': category,
                })
        else:
            print(f"  [{i+1}/{total}] {idx_name}: FAILED or EMPTY")

        time.sleep(0.5)

    # ── Step 4: Persist to database ─────────────────────────────────────────
    if records:
        print(f"\nSaving {len(records)} rows to index_constituents …")
        try:
            with engine_cash.begin() as conn:
                conn.execute(text("DELETE FROM index_constituents"))
                conn.execute(
                    text("""
                        INSERT INTO index_constituents (index_key, index_name, symbol, index_category)
                        VALUES (:index_key, :index_name, :symbol, :index_category)
                        ON CONFLICT DO NOTHING
                    """),
                    records,
                )
            print("[SUCCESS] Saved to database!")
        except Exception as e:
            print(f"[ERROR] Saving to database: {e}")

        # ── Category summary ────────────────────────────────────────────────
        from collections import defaultdict
        cat_indices = defaultdict(set)
        for r in records:
            cat_indices[r['index_category']].add(r['index_name'])

        print("\nCategory summary:")
        for cat in CATEGORY_ORDER:
            if cat in cat_indices:
                print(f"  {cat}: {len(cat_indices[cat])} indices")
        for cat, idx_set in cat_indices.items():
            if cat not in CATEGORY_ORDER:
                print(f"  {cat}: {len(idx_set)} indices")


if __name__ == '__main__':
    if create_table():
        fetch_all()
