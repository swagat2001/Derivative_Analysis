"""
HEATMAP CACHE BUILDER
================================================================================
Pre-calculates heatmap data for Insights Dashboard.
Source: CashStocks_Database (TBL_EQUITY_*)
Target: daily_market_heatmap table in CashStocks_Database
"""

import os
import sys

# Add project root to path to allow imports from Analysis_Tools
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from dotenv import load_dotenv

load_dotenv()
import time
from datetime import datetime
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text

# =============================================================
# Database Config
from Analysis_Tools.app.models.db_config import engine_cash as engine_cash

# Hardcoded constants removed - using shared engine
# db_user = "postgres"
# db_password = os.getenv("DB_PASSWORD")
# db_host = "localhost"
# db_port = "5432"
# db_name_cash = "CashStocks_Database"
#
# db_password_enc = quote_plus(db_password)
# engine_cash = create_engine(
#     f"postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name_cash}",
#     pool_size=10,
#     max_overflow=20,
#     pool_pre_ping=True,
#     echo=False,
# )

# Sector Master Paths
# Sector Master Paths
SECTOR_MASTER_PATHS = [
    os.getenv("SECTOR_MASTER_PATH"),
    "C:/NSE_EOD_CASH_WITH_INDICATORS/nse_sector_master.csv",
    "C:/Users/Admin/Desktop/Derivative_Analysis/SMA/nse_sector_master.csv",
    os.path.join(os.getcwd(), "nse_sector_master.csv"),
]

# =============================================================
# SECTOR MAPPING LOGIC (Cloned from insights_model.py)
# =============================================================

_sector_cache = {}
DEFAULT_SECTOR = "Others"


def classify_industry(industry: str) -> str:
    """Map NSE_INDUSTRY into broad sectors."""
    if not isinstance(industry, str) or not industry.strip():
        return "Diversified / Others"

    s = industry.strip().lower()

    if any(x in s for x in ["diversified", "miscellaneous", "conglomerate"]):
        return "Diversified / Others"
    if any(
        x in s
        for x in [
            "bank",
            "nbfc",
            "finance",
            "financial",
            "housing finance",
            "stockbroking",
            "broker",
            "capital market",
            "securities",
            "insurance",
            "asset management",
            "mutual fund",
            "wealth management",
            "leasing",
        ]
    ):
        return "Financials"
    if any(
        x in s
        for x in [
            "software",
            "it enabled",
            "information technology",
            "it services",
            "data processing",
            "technology services",
            "internet",
            "computers",
        ]
    ):
        return "Information Technology"
    if any(
        x in s
        for x in ["pharma", "pharmaceutical", "drug", "diagnostic", "hospital", "healthcare", "biotech", "medical"]
    ):
        return "Healthcare & Pharma"
    if any(x in s for x in ["telecom", "telecommunication", "telephone", "cellular"]):
        return "Telecom"
    if any(
        x in s
        for x in ["power generation", "power distribution", "power transmission", "electricity", "energy", "renewable"]
    ):
        return "Energy & Power"
    if any(x in s for x in ["oil", "gas", "petroleum", "refinery", "petrochemical"]):
        return "Oil & Gas"
    if any(x in s for x in ["automobile", "auto parts", "vehicle", "tyres", "ancillaries", "automotive"]):
        return "Auto & Ancillaries"
    if any(x in s for x in ["metal", "steel", "iron", "copper", "aluminum", "zinc", "mining", "mineral"]):
        return "Metals & Mining"
    if any(
        x in s
        for x in [
            "cement",
            "construction",
            "infrastructure",
            "real estate",
            "housing",
            "building",
            "engineering",
            "housing",
        ]
    ):
        return "Infrastructure & Construction"
    if any(
        x in s
        for x in [
            "fmcg",
            "consumer goods",
            "food",
            "beverages",
            "personal care",
            "tobacco",
            "sugar",
            "tea",
            "coffee",
            "diary",
        ]
    ):
        return "FMCG"
    if any(x in s for x in ["consumer durables", "electronics", "appliances"]):
        return "Consumer Durables"
    if any(x in s for x in ["textile", "apparel", "garment", "fabric", "clothing"]):
        return "Textiles"
    if any(x in s for x in ["chemical", "fertilizer", "pesticide", "agro"]):
        return "Chemicals"
    if any(x in s for x in ["transport", "logistics", "shipping", "airline", "port"]):
        return "Logistics & Transport"
    if any(x in s for x in ["media", "entertainment", "broadcasting", "publishing"]):
        return "Media & Entertainment"
    if any(x in s for x in ["retail", "trading", "department stores"]):
        return "Retail"
    if any(x in s for x in ["hotel", "resort", "restaurant", "tourism", "leisure"]):
        return "Hospitality"
    if any(x in s for x in ["defence", "defense", "aerospace"]):
        return "Defence"

    return "Diversified / Others"


_sector_load_attempted = False


def load_sector_master():
    """Load sector data from nse_sector_master.csv file."""
    global _sector_cache
    global _sector_load_attempted

    if _sector_cache or _sector_load_attempted:
        return

    _sector_load_attempted = True

    csv_path = None
    for path in SECTOR_MASTER_PATHS:
        if path and os.path.exists(path):
            csv_path = path
            break

    if not csv_path:
        print("[WARN] nse_sector_master.csv not found in any known location. Sectors will default to 'Others'.")
        return

    try:
        df = pd.read_csv(csv_path)
        # Normalize columns
        df.columns = [c.strip().upper() for c in df.columns]

        # We need SYMBOL and MACRO-SECTOR or SECTOR
        # Use first column as symbol (usually SYMBOL)
        symbol_col = df.columns[0]

        # Look for Industry column
        industry_col = next((c for c in df.columns if "INDUSTRY" in c), None)

        if not industry_col:
            # Maybe it has SECTOR directly?
            sector_col = next((c for c in df.columns if "SECTOR" in c), None)
        else:
            sector_col = industry_col

        if symbol_col and sector_col:
            for _, row in df.iterrows():
                sym = str(row[symbol_col]).strip().upper()
                raw_sector = str(row[sector_col]).strip()

                # If using Industry column, map it
                if industry_col:
                    final_sector = classify_industry(raw_sector)
                else:
                    final_sector = raw_sector

                _sector_cache[sym] = final_sector

        print(
            f"[INFO] Loaded {_sector_cache.get('NIFTY 50', 'Unknown')} sector mapping for {len(_sector_cache)} stocks"
        )

    except Exception as e:
        print(f"[ERROR] Failed to load sector master: {e}")


def get_sector(symbol: str) -> str:
    if not _sector_cache:
        load_sector_master()
    return _sector_cache.get(symbol.upper(), DEFAULT_SECTOR)


# =============================================================
# DATABASE MANAGEMENT
# =============================================================


def create_heatmap_table():
    """Create the daily_market_heatmap table if it doesn't exist."""
    print("[INFO] Checking/Creating daily_market_heatmap table...")
    try:
        with engine_cash.connect() as conn:
            conn.execute(
                text(
                    """
                CREATE TABLE IF NOT EXISTS daily_market_heatmap (
                    date DATE NOT NULL,
                    symbol VARCHAR(50) NOT NULL,
                    close NUMERIC,
                    prev_close NUMERIC,
                    high NUMERIC,
                    low NUMERIC,
                    volume NUMERIC,
                    turnover NUMERIC,
                    change_pct NUMERIC,
                    sector VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (date, symbol)
                );
                CREATE INDEX IF NOT EXISTS idx_heatmap_date ON daily_market_heatmap(date);
            """
                )
            )
            conn.commit()
        print("[SUCCESS] Table verified.")
        return True
    except Exception as e:
        print(f"[ERROR] Could not create table: {e}")
        return False


def get_all_cash_dates():
    """Get all unique dates available in the database (using any available TBL_ table)."""
    try:
        # First, find a valid table to check dates from
        query_tables = text(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name LIKE 'TBL_%'
            AND table_name != 'TBL_NIFTY'  -- Avoid if not present, though loop handles it
            LIMIT 1;
        """
        )

        with engine_cash.connect() as conn:
            # Try specific known tables first for reliability
            for table_name in ["TBL_RELIANCE", "TBL_TCS", "TBL_INFY", "TBL_SBIN"]:
                try:
                    result = conn.execute(
                        text(f'SELECT DISTINCT "BizDt" FROM public."{table_name}" ORDER BY "BizDt" DESC')
                    )
                    dates = [str(row[0]) for row in result]
                    if dates:
                        print(f"[INFO] Using {table_name} for date reference")
                        return dates
                except Exception:
                    continue

            # Fallback to any table
            result = conn.execute(query_tables)
            fallback_table = result.scalar()

            if fallback_table:
                print(f"[INFO] Using {fallback_table} for date reference")
                result = conn.execute(
                    text(f'SELECT DISTINCT "BizDt" FROM public."{fallback_table}" ORDER BY "BizDt" DESC')
                )
                dates = [str(row[0]) for row in result]
                return dates

        print("[WARN] No suitable tables found for date retrieval")
        return []

    except Exception as e:
        print(f"[ERROR] Date retrieval failed: {e}")
        return []


def get_cached_heatmap_dates():
    """Get dates already present in the heatmap cache."""
    try:
        query = text("SELECT DISTINCT date FROM daily_market_heatmap ORDER BY date DESC")
        with engine_cash.connect() as conn:
            result = conn.execute(query)
            dates = [str(row[0]) for row in result]
        return dates
    except Exception:
        return []


def calculate_heatmap_for_date(selected_date):
    """Calculate and insert heatmap data for a specific date."""
    print(f"[INFO] Calculating heatmap for {selected_date}...")
    start_time = time.time()

    # 1. Get all table names
    query_tables = text(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name LIKE 'TBL_%'
        AND table_name != 'TBL_NIFTY_DERIVED'
        ORDER BY table_name;
    """
    )

    with engine_cash.connect() as conn:
        result = conn.execute(query_tables)
        tables = [row[0] for row in result]

    if not tables:
        print("[WARN] No tables found in DB.")
        return False

    # 2. Process in batches
    BATCH_SIZE = 100
    all_data = []

    for batch_start in range(0, len(tables), BATCH_SIZE):
        batch_tables = tables[batch_start : batch_start + BATCH_SIZE]
        union_parts = []

        for table in batch_tables:
            symbol_name = table.replace("TBL_", "")
            if not symbol_name or symbol_name in ["NIFTY_DERIVED"]:
                continue

            union_parts.append(
                f"""
                (SELECT
                    '{symbol_name}' as symbol,
                    CAST("ClsPric" AS NUMERIC) as close,
                    CAST("HghPric" AS NUMERIC) as high,
                    CAST("LwPric" AS NUMERIC) as low,
                    CAST("TtlTradgVol" AS NUMERIC) as volume,
                    CAST(COALESCE("TtlTrfVal", 0) AS NUMERIC) as turnover,
                    CAST(COALESCE("PrvsClsgPric", 0) AS NUMERIC) as prev_close
                FROM public."{table}"
                WHERE CAST("BizDt" AS DATE) = DATE '{selected_date}'
                AND "ClsPric" IS NOT NULL
                AND CAST("ClsPric" AS NUMERIC) > 0
                LIMIT 1)
            """
            )

        if not union_parts:
            continue

        batch_query = "\nUNION ALL\n".join(union_parts)

        try:
            with engine_cash.connect() as conn:
                df_batch = pd.read_sql(batch_query, con=conn)
                if not df_batch.empty:
                    all_data.append(df_batch)
        except Exception as e:
            # print(f"[WARN] Batch error: {str(e)[:50]}")
            pass

    if not all_data:
        print(f"[WARN] No data aggregated for {selected_date}")
        return False

    df = pd.concat(all_data, ignore_index=True)

    # 3. Transform and Prepare
    cache_records = []
    for _, row in df.iterrows():
        try:
            symbol = str(row["symbol"])
            close = float(row["close"]) if pd.notna(row["close"]) else 0
            if close <= 0:
                continue

            high = float(row["high"]) if pd.notna(row["high"]) else close
            low = float(row["low"]) if pd.notna(row["low"]) else close
            volume = int(float(row["volume"])) if pd.notna(row["volume"]) else 0
            turnover = float(row["turnover"]) if pd.notna(row["turnover"]) else 0
            prev_close = float(row["prev_close"]) if pd.notna(row["prev_close"]) and row["prev_close"] > 0 else close

            change_pct = round(((close - prev_close) / prev_close) * 100, 2) if prev_close > 0 else 0.0
            sector = get_sector(symbol)

            cache_records.append(
                {
                    "date": selected_date,
                    "symbol": symbol,
                    "close": close,
                    "prev_close": prev_close,
                    "high": high,
                    "low": low,
                    "volume": volume,
                    "turnover": turnover,
                    "change_pct": change_pct,
                    "sector": sector,
                }
            )
        except Exception:
            continue

    # 4. Bulk Insert
    if cache_records:
        try:
            insert_query = text(
                """
                INSERT INTO daily_market_heatmap
                (date, symbol, close, prev_close, high, low, volume, turnover, change_pct, sector)
                VALUES (:date, :symbol, :close, :prev_close, :high, :low, :volume, :turnover, :change_pct, :sector)
                ON CONFLICT (date, symbol) DO UPDATE SET
                    close = EXCLUDED.close,
                    change_pct = EXCLUDED.change_pct,
                    volume = EXCLUDED.volume,
                    turnover = EXCLUDED.turnover;
            """
            )

            with engine_cash.begin() as conn:
                conn.execute(insert_query, cache_records)
            print(
                f"[SUCCESS] Saved {len(cache_records)} records for {selected_date} in {time.time() - start_time:.2f}s"
            )
            return True
        except Exception as e:
            print(f"[ERROR] Save failed: {e}")
            return False

    return False


def update_heatmap_cache():
    """
    Main function to update cache.
    Finds dates in Cash DB that are not in Heatmap Cache and processes them.
    """
    if not create_heatmap_table():
        return

    # 1. Identify missing dates
    all_dates = get_all_cash_dates()
    cached_dates = set(get_cached_heatmap_dates())

    missing_dates = [d for d in all_dates if d not in cached_dates]

    if not missing_dates:
        print("[INFO] Heatmap cache is up to date.")
        return

    print(f"[INFO] Found {len(missing_dates)} dates to process: {missing_dates[:5]}...")

    # Process newest first
    for date in missing_dates:
        calculate_heatmap_for_date(date)


if __name__ == "__main__":
    update_heatmap_cache()
