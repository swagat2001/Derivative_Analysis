"""
FII/DII DATABASE UPDATE SCRIPT
Downloads FII/DII data from NSE and stores in PostgreSQL database.
Supports:
1. Daily updates for Cash Market (via API)
2. Historical updates for Cash Market (via CSV archive or day-by-day API)
3. Historical updates for F&O Derivatives (via XLS Archives)

Location: Database/FII_DII/fii_dii_update_database.py

Usage:
    python fii_dii_update_database.py                  # Fetch latest Cash Market data
    python fii_dii_update_database.py historical_cash  # Fetch Cash Market history
    python fii_dii_update_database.py historical       # Fetch F&O Derivatives history
    python fii_dii_update_database.py view             # View Cash Market data
    python fii_dii_update_database.py view_fo          # View F&O Derivatives data
    python fii_dii_update_database.py setup            # Create tables
"""

import io
import os
import sys

# Add project root to path to allow imports from Analysis_Tools
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from dotenv import load_dotenv

load_dotenv()
import random
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import pandas as pd
import requests
from sqlalchemy import create_engine, text

# =============================================================
# DATABASE CONFIGURATION
# =============================================================
from Analysis_Tools.app.models.db_config import engine

# =============================================================
# NSE CONFIGURATION
# =============================================================
NSE_BASE_URL = "https://www.nseindia.com"
NSE_FII_DII_URL = "https://www.nseindia.com/api/fiidiiTradeReact"

# Archive URL for Cash Market FII/DII CSV
# Format: fii_dii_data_DD-Mon-YYYY.csv
ARCHIVE_FII_DII_URL = "https://archives.nseindia.com/content/equities/fii_dii_data_{date}.csv"


# =============================================================
# DATABASE SETUP
# =============================================================
def create_fii_dii_table():
    """Create FII/DII Cash Market table."""
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
        print(f"[ERROR] Creating cash market table: {e}")
        return False


def create_fii_derivatives_table():
    """Create FII Derivatives Stats table."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS fii_derivatives_activity (
        id SERIAL PRIMARY KEY,
        trade_date DATE NOT NULL,
        category VARCHAR(100) NOT NULL,
        participant_type VARCHAR(50) DEFAULT 'FII',
        buy_contracts NUMERIC(20, 0) DEFAULT 0,
        buy_value NUMERIC(20, 2) DEFAULT 0,
        sell_contracts NUMERIC(20, 0) DEFAULT 0,
        sell_value NUMERIC(20, 2) DEFAULT 0,
        oi_contracts NUMERIC(20, 0) DEFAULT 0,
        oi_value NUMERIC(20, 2) DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    -- Ensure unique constraint includes participant_type
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fii_derivatives_activity_trade_date_category_participant_type_key') THEN
            ALTER TABLE fii_derivatives_activity DROP CONSTRAINT IF EXISTS fii_derivatives_activity_trade_date_category_key;
            ALTER TABLE fii_derivatives_activity ADD CONSTRAINT fii_derivatives_activity_trade_date_category_participant_type_key UNIQUE (trade_date, category, participant_type);
        END IF;
    END $$;
    CREATE INDEX IF NOT EXISTS idx_fii_fo_trade_date ON fii_derivatives_activity(trade_date DESC);
    """
    try:
        with engine.connect() as conn:
            # Check for participant_type
            check_pt_sql = "SELECT column_name FROM information_schema.columns WHERE table_name='fii_derivatives_activity' AND column_name='participant_type'"
            pt_exists = conn.execute(text(check_pt_sql)).scalar()

            if not pt_exists:
                print("Migrating schema: Adding participant_type column...")
                conn.execute(
                    text("ALTER TABLE fii_derivatives_activity ADD COLUMN participant_type VARCHAR(50) DEFAULT 'FII'")
                )
                conn.commit()

            # Check for oi_long
            check_oil_sql = "SELECT column_name FROM information_schema.columns WHERE table_name='fii_derivatives_activity' AND column_name='oi_long'"
            oil_exists = conn.execute(text(check_oil_sql)).scalar()

            if not oil_exists:
                print("Migrating schema: Adding oi_long/oi_short columns...")
                conn.execute(text("ALTER TABLE fii_derivatives_activity ADD COLUMN oi_long NUMERIC(20, 0) DEFAULT 0"))
                conn.execute(text("ALTER TABLE fii_derivatives_activity ADD COLUMN oi_short NUMERIC(20, 0) DEFAULT 0"))
                conn.commit()

            conn.execute(text(create_table_sql))
            conn.commit()
        return True
    except Exception as e:
        print(f"[ERROR] Creating derivatives table: {e}")
        return False


def get_latest_date_in_db():
    """Get the latest date in Cash Market table."""
    try:
        query = "SELECT MAX(trade_date) FROM fii_dii_activity"
        with engine.connect() as conn:
            result = conn.execute(text(query)).scalar()
        return result
    except Exception:
        return None


def get_oldest_date_in_db():
    """Get the oldest date in Cash Market table."""
    try:
        query = "SELECT MIN(trade_date) FROM fii_dii_activity"
        with engine.connect() as conn:
            result = conn.execute(text(query)).scalar()
        return result
    except Exception:
        return None


def get_latest_fo_date_in_db():
    """Get the latest date in Derivatives table."""
    try:
        query = "SELECT MAX(trade_date) FROM fii_derivatives_activity"
        with engine.connect() as conn:
            result = conn.execute(text(query)).scalar()
        return result
    except Exception:
        return None


# =============================================================
# DATA PARSING
# =============================================================
def parse_val(val_str):
    """Parse string value like '1,234.56' to float."""
    if not val_str:
        return 0.0
    try:
        return float(str(val_str).replace(",", ""))
    except Exception:
        return 0.0


def parse_fii_dii_json(json_data, date_obj):
    """Parse JSON data from NSE website."""
    date_key = date_obj.strftime("%Y-%m-%d")
    date_records = {}

    # Processing logic kept same as original...
    # (Assuming snippet context, keeping logical flow)

    # ... logic ...
    # This function seems to be part of the file not shown in the diff entirely.
    # Re-implementing logic based on expected input if needed, but diff shows I am replacing the file content around DB function.
    # Use careful replacement.
    pass

    # Since I am replacing a huge chunk, I need to be careful to implement parse_fii_dii_json if I overwrote it.
    # Wait, the previous view_file showed lines 80-150.
    # The tool 'replace_file_content' replaces the 'StartLine' to 'EndLine'.
    # I need to ensure I don't delete `parse_fii_dii_json` if it was in the target range.
    # `parse_fii_dii_json` was NOT in lines 80-150. It starts around line 273 (based on previous greps/reads).
    # Wait, I checked lines 300-400 earlier and saw `parse_fii_dii_json` logic inside specific function.
    # Ah, the `parse_fii_dii_json` was NOT shown in lines 80-150.
    # Lines 80-150 contained `create_fii_derivatives_table` and `get_latest_date_in_db`.
    # My replacement targets `create_fii_derivatives_table`. I should stick to that scope.

    # Actually, I want to add new functions `download_participant_oi_stats` and `parse_participant_oi_stats` and update `save_fo_stats_to_db`.
    # `save_fo_stats_to_db` is at line 464.
    # `download_archive_fo_stats` is at 366.
    # I will do a MULTI REPLACE to insert functions and update DB logic.

    # RETRACTING previous thought. I will use `multi_replace_file_content` to be safe.

    # PLAN:
    # 1. Update `create_fii_derivatives_table` (Lines 91-117).
    # 2. Update `save_fo_stats_to_db` (Lines 464-500).
    # 3. Insert `download_participant_oi_stats` and `parse_participant_oi_stats` (After line 423, before DB section).
    # 4. Update `main` loop (Not shown in view yet, need to find `update_fii_dii_data` function).

    # I need to view `update_fii_dii_data` first to know where to edit the loop.
    pass




def get_record_count():
    """Get total records in Cash Market table."""
    try:
        query = "SELECT COUNT(*) FROM fii_dii_activity"
        with engine.connect() as conn:
            return conn.execute(text(query)).scalar()
    except Exception:
        return 0


# =============================================================
# NSE SESSION
# =============================================================
def create_nse_session():
    """Create NSE session with proper cookies."""
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }
    )
    try:
        session.get(NSE_BASE_URL, timeout=10)
        time.sleep(random.uniform(1, 2))
        session.get(f"{NSE_BASE_URL}/market-data/live-market-indices", timeout=10)
        time.sleep(random.uniform(0.5, 1))
        return session
    except Exception as e:
        print(f"[WARN] Session init: {e}")
        return session


# =============================================================
# FETCH DATA - CURRENT DAY (Daily Cash Market via API)
# =============================================================
def fetch_current_data(session):
    """Fetch current day FII/DII data from API."""
    session.headers.update(
        {
            "Accept": "application/json, text/plain, */*",
            "Referer": f"{NSE_BASE_URL}/market-data/live-market-indices",
        }
    )

    for attempt in range(3):
        try:
            resp = session.get(NSE_FII_DII_URL, timeout=15)
            if resp.status_code == 200:
                if resp.text.strip().startswith("[") or resp.text.strip().startswith("{"):
                    return resp.json()
            time.sleep(2)
        except Exception:
            time.sleep(2)
    return None


# =============================================================
# FETCH DATA - ARCHIVE CSV (Historical Cash Market)
# =============================================================
def download_archive_cash_csv(date_obj):
    """Download FII/DII Cash CSV from NSE Archive."""
    # Try multiple date formats
    date_formats = [
        date_obj.strftime("%d-%b-%Y"),  # 01-Jan-2024
        date_obj.strftime("%d%m%Y"),  # 01012024
        date_obj.strftime("%d-%m-%Y"),  # 01-01-2024
    ]

    # Try multiple URL patterns
    url_patterns = [
        "https://archives.nseindia.com/content/equities/fii_dii_data_{date}.csv",
        "https://archives.nseindia.com/archives/equities/fii_dii/{date}.csv",
        "https://nsearchives.nseindia.com/content/equities/fii_dii_data_{date}.csv",
    ]

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    for url_pattern in url_patterns:
        for date_str in date_formats:
            url = url_pattern.format(date=date_str)
            try:
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=10) as response:
                    return response.read(), url
            except urllib.error.HTTPError:
                continue
            except Exception:
                continue

    return None, None


def fetch_single_day_data(session, date_obj):
    """Fetch FII/DII data for a single date using the reports API."""
    # NSE doesn't have a direct historical API, so we try the archive first
    content, url = download_archive_cash_csv(date_obj)

    if content:
        try:
            df = pd.read_csv(io.BytesIO(content))
            # Parse the CSV format
            records = parse_archive_csv(df, date_obj)
            return records
        except Exception as e:
            pass

    return None


# =============================================================
# PARSE DATA
# =============================================================
def parse_fii_dii_data(raw_data):
    """Parse NSE FII/DII API response (Cash Market)."""
    if not raw_data:
        return []

    date_records = {}

    for item in raw_data:
        try:
            category = item.get("category", "").upper()
            date_str = item.get("date", "")

            trade_date = None
            for fmt in ["%d-%b-%Y", "%d-%B-%Y", "%Y-%m-%d", "%d-%m-%Y"]:
                try:
                    trade_date = datetime.strptime(date_str, fmt).date()
                    break
                except ValueError:
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
                if v is None:
                    return 0.0
                try:
                    return float(str(v).replace(",", ""))
                except Exception:
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

        except Exception:
            continue

    records = []
    for rec in date_records.values():
        rec["total_net_value"] = rec["fii_net_value"] + rec["dii_net_value"]
        records.append(rec)

    records.sort(key=lambda x: x["trade_date"])
    return records


def parse_archive_csv(df, date_obj):
    """Parse archive CSV file."""
    try:
        # CSV structure may vary, try to extract FII/DII values
        record = {
            "trade_date": date_obj,
            "fii_buy_value": 0,
            "fii_sell_value": 0,
            "fii_net_value": 0,
            "dii_buy_value": 0,
            "dii_sell_value": 0,
            "dii_net_value": 0,
        }

        # Look for columns
        for _, row in df.iterrows():
            row_str = str(row.values).upper()

            if "FII" in row_str or "FPI" in row_str:
                # Extract values
                nums = [
                    float(str(v).replace(",", ""))
                    for v in row.values
                    if isinstance(v, (int, float))
                    or (isinstance(v, str) and v.replace(",", "").replace(".", "").replace("-", "").isdigit())
                ]
                if len(nums) >= 3:
                    record["fii_buy_value"] = nums[0]
                    record["fii_sell_value"] = nums[1]
                    record["fii_net_value"] = nums[2] if len(nums) > 2 else nums[0] - nums[1]

            elif "DII" in row_str:
                nums = [
                    float(str(v).replace(",", ""))
                    for v in row.values
                    if isinstance(v, (int, float))
                    or (isinstance(v, str) and v.replace(",", "").replace(".", "").replace("-", "").isdigit())
                ]
                if len(nums) >= 3:
                    record["dii_buy_value"] = nums[0]
                    record["dii_sell_value"] = nums[1]
                    record["dii_net_value"] = nums[2] if len(nums) > 2 else nums[0] - nums[1]

        record["total_net_value"] = record["fii_net_value"] + record["dii_net_value"]
        return [record]
    except Exception:
        return None


# =============================================================
# FETCH DATA - HISTORICAL (F&O Stats via Archive)
# =============================================================
def download_archive_fo_stats(date_obj):
    """Download FII Derivatives Stats XLS from NSE Archive."""
    date_str = date_obj.strftime("%d-%b-%Y")
    url = f"https://archives.nseindia.com/content/fo/fii_stats_{date_str}.xls"

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.read()
    except urllib.error.HTTPError:
        return None
    except Exception as e:
        return None


def parse_fo_stats_xls(file_content, date_obj):
    """Parse the FII Stats XLS file."""
    try:
        df = pd.read_excel(io.BytesIO(file_content), engine="xlrd")
        df = df.dropna(how="all")

        records = []
        categories = ["INDEX FUTURES", "INDEX OPTIONS", "STOCK FUTURES", "STOCK OPTIONS"]

        for _, row in df.iterrows():
            row_vals = row.astype(str).tolist()
            first_col = row_vals[0].strip().upper()

            if first_col in categories:
                nums = []
                for val in row_vals[1:]:
                    try:
                        clean_val = val.replace(",", "").replace("nan", "").strip()
                        if clean_val:
                            nums.append(float(clean_val))
                    except Exception:
                        pass

                if len(nums) >= 6:
                    rec = {
                        "trade_date": date_obj,
                        "category": first_col,
                        "buy_contracts": int(nums[0]),
                        "buy_value": nums[1],
                        "sell_contracts": int(nums[2]),
                        "sell_value": nums[3],
                        "oi_contracts": int(nums[4]),
                        "oi_value": nums[5],
                    }
                    records.append(rec)

        return records

    except Exception as e:
        return []


# =============================================================
# FETCH DATA - PARTICIPANT WISE OPEN INTEREST
# =============================================================
def download_participant_oi_stats(date_obj):
    """Download Participant Wise Open Interest CSV from NSE Archive."""
    date_str = date_obj.strftime("%d%m%Y")
    url = f"https://archives.nseindia.com/content/nsccl/fao_participant_oi_{date_str}.csv"

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.read()
    except urllib.error.HTTPError:
        return None
    except Exception as e:
        return None


def parse_participant_oi_stats(file_content, date_obj):
    """Parse Participant Wise OI CSV."""
    try:
        # NSE CSV usually has a title row, so skip 1
        df = pd.read_csv(io.BytesIO(file_content), skiprows=1)
        df = df.dropna(how="all")

        records = []
        # Mapping: CSV Header -> Category

        # Normalize columns
        df.columns = [c.strip().lower() for c in df.columns]

        for _, row in df.iterrows():
            client_type = str(row.get("client type", "")).strip()

            # Map Client Type to simplified Participant Type
            p_type = None
            if client_type.upper() == "FII":
                p_type = "FII"
            elif client_type.upper() == "DII":
                p_type = "DII"
            elif client_type.upper() == "PRO":
                p_type = "PRO"
            elif client_type.upper() == "CLIENT":
                p_type = "CLIENT"

            if not p_type:
                continue

            # Helper: Search roughly for column
            def get_val(term):
                for c in df.columns:
                    if term in c:
                        return float(row.get(c, 0))
                return 0.0

            # 1. Index Futures
            idx_long = get_val("future index long")
            idx_short = get_val("future index short")
            records.append(
                {
                    "trade_date": date_obj,
                    "category": "INDEX FUTURES",
                    "participant_type": p_type,
                    "oi_contracts": int(idx_long + idx_short),
                    "oi_value": 0,
                    "oi_long": int(idx_long),
                    "oi_short": int(idx_short),
                    "buy_contracts": 0,
                    "buy_value": 0,
                    "sell_contracts": 0,
                    "sell_value": 0,
                }
            )

            # 2. Stock Futures
            stk_long = get_val("future stock long")
            stk_short = get_val("future stock short")
            records.append(
                {
                    "trade_date": date_obj,
                    "category": "STOCK FUTURES",
                    "participant_type": p_type,
                    "oi_contracts": int(stk_long + stk_short),
                    "oi_value": 0,
                    "oi_long": int(stk_long),
                    "oi_short": int(stk_short),
                    "buy_contracts": 0,
                    "buy_value": 0,
                    "sell_contracts": 0,
                    "sell_value": 0,
                }
            )

            # 3. Index Options - Split into Call/Put + Aggregate
            opt_idx_call_long = get_val("option index call long")
            opt_idx_call_short = get_val("option index call short")
            opt_idx_put_long = get_val("option index put long")
            opt_idx_put_short = get_val("option index put short")

            # Store individuals
            records.append(
                {
                    "trade_date": date_obj,
                    "category": "INDEX CALL OPTIONS",
                    "participant_type": p_type,
                    "oi_contracts": int(opt_idx_call_long + opt_idx_call_short),
                    "oi_value": 0,
                    "oi_long": int(opt_idx_call_long),
                    "oi_short": int(opt_idx_call_short),
                    "buy_contracts": 0,
                    "buy_value": 0,
                    "sell_contracts": 0,
                    "sell_value": 0,
                }
            )
            records.append(
                {
                    "trade_date": date_obj,
                    "category": "INDEX PUT OPTIONS",
                    "participant_type": p_type,
                    "oi_contracts": int(opt_idx_put_long + opt_idx_put_short),
                    "oi_value": 0,
                    "oi_long": int(opt_idx_put_long),
                    "oi_short": int(opt_idx_put_short),
                    "buy_contracts": 0,
                    "buy_value": 0,
                    "sell_contracts": 0,
                    "sell_value": 0,
                }
            )
            # Store Aggregate (Matches XLS category) - Set OI to 0 to avoid pollution/double counting
            # OI for options should only be viewed in the detailed Call/Put breakdown
            records.append(
                {
                    "trade_date": date_obj,
                    "category": "INDEX OPTIONS",
                    "participant_type": p_type,
                    "oi_contracts": 0,
                    "oi_value": 0,
                    "oi_long": 0,
                    "oi_short": 0,
                    "buy_contracts": 0,
                    "buy_value": 0,
                    "sell_contracts": 0,
                    "sell_value": 0,
                }
            )

            # 4. Stock Options - Split into Call/Put + Aggregate
            opt_stk_call_long = get_val("option stock call long")
            opt_stk_call_short = get_val("option stock call short")
            opt_stk_put_long = get_val("option stock put long")
            opt_stk_put_short = get_val("option stock put short")

            records.append(
                {
                    "trade_date": date_obj,
                    "category": "STOCK CALL OPTIONS",
                    "participant_type": p_type,
                    "oi_contracts": int(opt_stk_call_long + opt_stk_call_short),
                    "oi_value": 0,
                    "oi_long": int(opt_stk_call_long),
                    "oi_short": int(opt_stk_call_short),
                    "buy_contracts": 0,
                    "buy_value": 0,
                    "sell_contracts": 0,
                    "sell_value": 0,
                }
            )
            records.append(
                {
                    "trade_date": date_obj,
                    "category": "STOCK PUT OPTIONS",
                    "participant_type": p_type,
                    "oi_contracts": int(opt_stk_put_long + opt_stk_put_short),
                    "oi_value": 0,
                    "oi_long": int(opt_stk_put_long),
                    "oi_short": int(opt_stk_put_short),
                    "buy_contracts": 0,
                    "buy_value": 0,
                    "sell_contracts": 0,
                    "sell_value": 0,
                }
            )
            # Store Aggregate - Set OI to 0 to avoid pollution
            records.append(
                {
                    "trade_date": date_obj,
                    "category": "STOCK OPTIONS",
                    "participant_type": p_type,
                    "oi_contracts": 0,
                    "oi_value": 0,
                    "oi_long": 0,
                    "oi_short": 0,
                    "buy_contracts": 0,
                    "buy_value": 0,
                    "sell_contracts": 0,
                    "sell_value": 0,
                }
            )

        return records

    except Exception as e:
        print(f"Error parsing Participant OI: {e}")
        return []


# =============================================================
# SAVE TO DATABASE
# =============================================================
def save_to_db(records):
    """Save Cash Market FII/DII records to database."""
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
        print(f"[ERROR] DB Save Error: {e}")
    return count


def save_fo_stats_to_db(records):
    """Save F&O records to database."""
    if not records:
        return 0

    upsert_sql = """
    INSERT INTO fii_derivatives_activity (
        trade_date, category, participant_type,
        buy_contracts, buy_value,
        sell_contracts, sell_value,
        oi_contracts, oi_value,
        oi_long, oi_short
    ) VALUES (
        :trade_date, :category, :participant_type,
        :buy_contracts, :buy_value,
        :sell_contracts, :sell_value,
        :oi_contracts, :oi_value,
        :oi_long, :oi_short
    )
    ON CONFLICT (trade_date, category, participant_type) DO UPDATE SET
        buy_contracts = GREATEST(fii_derivatives_activity.buy_contracts, EXCLUDED.buy_contracts),
        buy_value = GREATEST(fii_derivatives_activity.buy_value, EXCLUDED.buy_value),
        sell_contracts = GREATEST(fii_derivatives_activity.sell_contracts, EXCLUDED.sell_contracts),
        sell_value = GREATEST(fii_derivatives_activity.sell_value, EXCLUDED.sell_value),
        oi_contracts = GREATEST(fii_derivatives_activity.oi_contracts, EXCLUDED.oi_contracts),
        oi_long = GREATEST(fii_derivatives_activity.oi_long, EXCLUDED.oi_long),
        oi_short = GREATEST(fii_derivatives_activity.oi_short, EXCLUDED.oi_short),
        oi_value = GREATEST(fii_derivatives_activity.oi_value, EXCLUDED.oi_value);
    """

    count = 0
    try:
        with engine.connect() as conn:
            for rec in records:
                # Ensure participant_type defaults to FII if missing (legacy)
                if "participant_type" not in rec:
                    rec["participant_type"] = "FII"

                # Check for FII Value-less update override protection
                # Default unknown fields
                if "oi_long" not in rec:
                    rec["oi_long"] = 0
                if "oi_short" not in rec:
                    rec["oi_short"] = 0

                if rec["participant_type"] == "FII" and rec["buy_value"] == 0 and rec["oi_contracts"] > 0:
                    update_oi_sql = """
                        UPDATE fii_derivatives_activity
                        SET oi_contracts = :oi_contracts, oi_long = :oi_long, oi_short = :oi_short
                        WHERE trade_date = :trade_date AND category = :category AND participant_type = 'FII'
                     """
                    result = conn.execute(text(update_oi_sql), rec)
                    if result.rowcount == 0:
                        conn.execute(text(upsert_sql), rec)
                else:
                    conn.execute(text(upsert_sql), rec)
                count += 1
            conn.commit()
    except Exception as e:
        print(f"[ERROR] DB Save Error: {e}")
    return count


# =============================================================
# MAIN FUNCTIONS
# =============================================================
def run_historical_cash_download():
    """Download historical Cash Market FII/DII data."""
    print("\n" + "=" * 60)
    print("FII/DII HISTORICAL DOWNLOAD (CASH MARKET)")
    print("=" * 60)
    print("\n⚠️ NSE does not provide historical Cash Market FII/DII data")
    print("   via API or archives (unlike F&O derivatives).")
    print("\n📋 ALTERNATIVE OPTIONS:")
    print("   1. Daily updates: Run this script daily to build history")
    print("   2. Third-party data: Use Trendlyne, MoneyControl, etc.")
    print("   3. F&O data is available: Use 'historical' command")
    print("\n" + "=" * 60)
    print("TIP: Run 'python fii_dii_update_database.py' daily after market")
    print("     to automatically build Cash Market FII/DII history.")
    print("=" * 60 + "\n")

    # Try to fetch last few days anyway
    print("Attempting to fetch recent days...")

    create_fii_dii_table()
    session = create_nse_session()

    # Try current data which might have a few days
    data = fetch_current_data(session)
    if data:
        records = parse_fii_dii_data(data)
        if records:
            saved = save_to_db(records)
            print(f"✅ Saved {saved} recent records")
            for r in records[:5]:
                print(f"   {r['trade_date']}: FII={r['fii_net_value']:+,.0f}, DII={r['dii_net_value']:+,.0f}")

    print("\n" + "=" * 60)
    final_count = get_record_count()
    print(f"Total records in database: {final_count}")
    print("=" * 60 + "\n")


def run_historical_fo_download():
    """Download FII Derivatives Stats from Archive (01-01-2024 to Today)."""
    print("\n" + "=" * 60)
    print("FII/DII HISTORICAL DOWNLOAD (DERIVATIVES STATS)")
    print("=" * 60)

    create_fii_dii_table()
    create_fii_derivatives_table()

    start_date = datetime(2024, 1, 1).date()
    end_date = datetime.now().date()

    latest_date = get_latest_fo_date_in_db()
    if latest_date:
        print(f"Latest data in DB: {latest_date}")
        if isinstance(latest_date, datetime):
            latest_date = latest_date.date()

        # Resume from the day AFTER the latest date
        start_date = latest_date + timedelta(days=1)

        if start_date > end_date:
            print(f"✅ Database is up to date (Latest: {latest_date})")
            return

    print(f"\n📅 Scanning from {start_date} to {end_date}")
    print("⏳ Downloading archives from NSE...\n")

    total_files = 0
    total_records = 0

    current_date = start_date
    while current_date <= end_date:
        weekday = current_date.weekday()
        if weekday >= 5:
            current_date += timedelta(days=1)
            continue

        print(f"Checking {current_date}... ", end="", flush=True)

        # 1. Download FII Stats (Value + OI)
        fii_content = download_archive_fo_stats(current_date)
        records_fii = []
        if fii_content:
            records_fii = parse_fo_stats_xls(fii_content, current_date)
            # Add implicit type
            for r in records_fii:
                r["participant_type"] = "FII"

        # 2. Download Participant OI (Detailed OI)
        part_content = download_participant_oi_stats(current_date)
        records_part = []
        if part_content:
            records_part = parse_participant_oi_stats(part_content, current_date)

        # 3. Save
        all_records = records_fii + records_part
        if all_records:
            count = save_fo_stats_to_db(all_records)
            total_records += count
            total_files += 1
            print(f"✅ Saved {count} rows (FII: {len(records_fii)}, Part: {len(records_part)})")
        else:
            print("❌ No Data")

        current_date += timedelta(days=1)
        time.sleep(random.uniform(0.5, 1.0))

    print("\n" + "=" * 60)
    print("✅ DOWNLOAD COMPLETE")
    print("=" * 60)
    print(f"   Files Processed: {total_files}")
    print(f"   Records Saved: {total_records}")
    print("=" * 60 + "\n")


def run_update():
    """Fetch and save latest Cash Market data (Daily API)."""
    print("\n" + "=" * 60)
    print("FII/DII DAILY UPDATE (CASH MARKET)")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    create_fii_dii_table()

    print("\n[INFO] Establishing NSE session...")
    session = create_nse_session()

    print("[INFO] Fetching latest FII/DII data...")
    data = fetch_current_data(session)

    if not data:
        print("\n[ERROR] Could not fetch data from NSE API")
        print("\n[TIP] Try:")
        print("  1. Wait a few minutes and retry")
        print("  2. Run during market hours (9 AM - 4 PM IST)")
        print("  3. Check if nseindia.com works in browser")
        return False

    records = parse_fii_dii_data(data)
    if not records:
        print("[ERROR] No records parsed")
        return False

    saved = save_to_db(records)

    print("\n" + "=" * 60)
    print("✅ UPDATE COMPLETE")
    print("=" * 60)
    if records:
        r = records[0]
        print(f"   Date: {r['trade_date']}")
        print(f"   FII Net: ₹{r['fii_net_value']:+,.2f} Cr")
        print(f"   DII Net: ₹{r['dii_net_value']:+,.2f} Cr")
        print(f"   Total:   ₹{r['total_net_value']:+,.2f} Cr")
    print(f"   Records saved: {saved}")
    print("=" * 60 + "\n")
    return True


def view_stored_data():
    """View stored Cash Market data."""
    print("\n" + "=" * 60)
    print("DATA: FII/DII CASH MARKET")
    print("=" * 60)
    try:
        query = """
        SELECT trade_date::text as date, fii_net_value, dii_net_value, total_net_value
        FROM fii_dii_activity ORDER BY trade_date DESC LIMIT 20
        """
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)

        if df.empty:
            print("\n⚠️ No data found.")
            print("   Run: python fii_dii_update_database.py")
            return

        total = get_record_count()
        oldest = get_oldest_date_in_db()
        latest = get_latest_date_in_db()

        print(f"\n📊 Total records: {total}")
        print(f"📅 Date range: {oldest} to {latest}")
        print(f"\n{'Date':<12} {'FII Net':>14} {'DII Net':>14} {'Total':>14}")
        print("-" * 56)

        for _, r in df.iterrows():
            fii = r["fii_net_value"] or 0
            dii = r["dii_net_value"] or 0
            total_val = r["total_net_value"] or 0

            fii_str = f"{'+' if fii >= 0 else ''}{fii:,.0f}"
            dii_str = f"{'+' if dii >= 0 else ''}{dii:,.0f}"
            total_str = f"{'+' if total_val >= 0 else ''}{total_val:,.0f}"

            print(f"{r['date']:<12} {fii_str:>14} {dii_str:>14} {total_str:>14}")

    except Exception as e:
        print(f"[ERROR] {e}")
    print("\n")


def view_fo_data():
    """View stored F&O Derivatives data."""
    print("\n" + "=" * 60)
    print("DATA: FII DERIVATIVES STATS")
    print("=" * 60)
    try:
        query = """
        SELECT trade_date::text as date, category, buy_value, sell_value, oi_value
        FROM fii_derivatives_activity
        WHERE category = 'INDEX FUTURES'
        ORDER BY trade_date DESC LIMIT 15
        """
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)

        if df.empty:
            print("\n⚠️ No data found.")
            print("   Run: python fii_dii_update_database.py historical")
            return

        count_query = "SELECT COUNT(*) FROM fii_derivatives_activity"
        with engine.connect() as conn:
            total = conn.execute(text(count_query)).scalar()

        print(f"\n📊 Total records: {total}")
        print("\n--- INDEX FUTURES (Recent 15) ---")
        print(df.to_string(index=False))

    except Exception as e:
        print(f"[ERROR] {e}")
    print("\n")


def run_refill_fo(days=15):
    """Refill F&O Derivatives data for the last N days to fix missing values."""
    print("\n" + "=" * 60)
    print(f"FII/DII REFILL (DERIVATIVES STATS - Last {days} days)")
    print("=" * 60)

    create_fii_derivatives_table()

    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days)

    print(f"\n📅 Re-scanning from {start_date} to {end_date}")
    print("⏳ Downloading archives from NSE to fill missing Buy/Sell values...\n")

    total_files = 0
    total_records = 0

    current_date = start_date
    while current_date <= end_date:
        weekday = current_date.weekday()
        if weekday >= 5:
            current_date += timedelta(days=1)
            continue

        print(f"Processing {current_date}... ", end="", flush=True)

        # 1. Download FII Stats (Value + OI)
        fii_content = download_archive_fo_stats(current_date)
        records_fii = []
        if fii_content:
            records_fii = parse_fo_stats_xls(fii_content, current_date)
            for r in records_fii:
                r["participant_type"] = "FII"

        # 2. Download Participant OI (Detailed OI)
        part_content = download_participant_oi_stats(current_date)
        records_part = []
        if part_content:
            records_part = parse_participant_oi_stats(part_content, current_date)

        # 3. Save (Upsert logic will handle updates)
        all_records = records_fii + records_part
        if all_records:
            count = save_fo_stats_to_db(all_records)
            total_records += count
            total_files += 1
            print(f"✅ Processed {count} rows")
        else:
            print("❌ No Data")

        current_date += timedelta(days=1)
        time.sleep(random.uniform(0.5, 1.0))

    print("\n" + "=" * 60)
    print("✅ REFILL COMPLETE")
    print("=" * 60 + "\n")


# =============================================================
# CLI ENTRY POINT
# =============================================================
if __name__ == "__main__":
    if len(sys.argv) > 1:
        cmd = sys.argv[1].lower()

        if cmd == "historical_cash":
            run_historical_cash_download()
        elif cmd == "historical":
            run_historical_fo_download()
        elif cmd == "refill":
            days = int(sys.argv[2]) if len(sys.argv) > 2 else 15
            run_refill_fo(days)
        elif cmd == "view":
            view_stored_data()
        elif cmd == "view_fo":
            view_fo_data()
        elif cmd == "setup":
            create_fii_dii_table()
            create_fii_derivatives_table()
            print("[OK] Tables created.")
        else:
            print("\n" + "=" * 60)
            print("FII/DII DATABASE UPDATE SCRIPT")
            print("=" * 60)
            print("\nCommands:")
            print("  (no args)       - Fetch latest Cash Market data (daily)")
            print("  historical_cash - Info about Cash Market historical data")
            print("  historical      - Download F&O Derivatives history (Archives)")
            print("  refill [days]   - Re-download archives for last N days (default 15)")
            print("  view            - View Cash Market data")
            print("  view_fo         - View F&O Derivatives data")
            print("  setup           - Create database tables")
            print("\nExamples:")
            print("  python fii_dii_update_database.py")
            print("  python fii_dii_update_database.py historical")
            print("  python fii_dii_update_database.py refill 10")
            print("  python fii_dii_update_database.py view")
            print("=" * 60 + "\n")
    else:
        run_update()
