"""
DELIVERY DATA CACHE BUILDER
================================================================================
Pre-calculates delivery percentage data for the Delivery tab.
Source: CashStocks_Database (TBL_*)
Target: daily_delivery_data table in CashStocks_Database

PERFORMANCE:
    Without cache: 10-60 seconds (queries 3000+ tables)
    With cache:    < 0.1 seconds (single query)
"""

import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Reconfigure stdout for UTF-8 support (Windows console workaround)
try:
    sys.stdout.reconfigure(encoding='utf-8')
except AttributeError:
    pass

import pandas as pd
from sqlalchemy import create_engine, text

load_dotenv()

# Database connection
DB_USER = "postgres"
DB_PASS = os.getenv("DB_PASSWORD")
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "CashStocks_Database"

from urllib.parse import quote_plus
password_encoded = quote_plus(DB_PASS)
DATABASE_URL = f"postgresql://{DB_USER}:{password_encoded}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

print("="*70)
print("DELIVERY DATA CACHE BUILDER")
print("="*70)
print()

# =============================================================
# CREATE CACHE TABLE
# =============================================================

def create_cache_table():
    """Create the daily_delivery_data cache table."""
    print("[1/4] Creating cache table...")
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS daily_delivery_data (
                    date DATE NOT NULL,
                    symbol VARCHAR(50) NOT NULL,
                    close NUMERIC,
                    volume BIGINT,
                    delivery_qty BIGINT,
                    delivery_pct NUMERIC,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (date, symbol)
                );

                -- Indexes for fast filtering
                CREATE INDEX IF NOT EXISTS idx_delivery_date ON daily_delivery_data(date);
                CREATE INDEX IF NOT EXISTS idx_delivery_symbol ON daily_delivery_data(symbol);
                CREATE INDEX IF NOT EXISTS idx_delivery_pct ON daily_delivery_data(delivery_pct);
            """))
            conn.commit()
        print("    ✅ Cache table ready")
        return True
    except Exception as e:
        print(f"    ❌ Failed to create table: {e}")
        return False


# =============================================================
# GET TABLES AND DATES
# =============================================================

def get_cash_tables():
    """Get list of stock tables from CashStocks_Database."""
    try:
        query = text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name LIKE 'TBL_%'
            AND table_name != 'TBL_NIFTY_DERIVED'
            ORDER BY table_name;
        """)
        with engine.connect() as conn:
            result = conn.execute(query)
            return [row[0] for row in result]
    except Exception as e:
        print(f"[ERROR] Failed to get tables: {e}")
        return []


def get_cached_dates():
    """Get dates already in cache."""
    try:
        query = text("SELECT DISTINCT date FROM daily_delivery_data ORDER BY date DESC")
        with engine.connect() as conn:
            return {str(row[0]) for row in conn.execute(query)}
    except Exception:
        return set()


def get_available_dates():
    """Get available dates from a sample table."""
    try:
        # Use first table to get dates
        tables = get_cash_tables()
        if not tables:
            return []

        sample_table = tables[0]
        query = text(f"""
            SELECT DISTINCT CAST("BizDt" AS DATE)::text as date
            FROM public."{sample_table}"
            WHERE "BizDt" IS NOT NULL
            ORDER BY date DESC
            LIMIT 60
        """)

        with engine.connect() as conn:
            result = conn.execute(query)
            return [row[0] for row in result]
    except Exception as e:
        print(f"[ERROR] Failed to get dates: {e}")
        return []


# =============================================================
# PROCESS DATE
# =============================================================

def process_date(target_date: str, tables: list):
    """Calculate delivery data for all stocks on specific date."""
    print(f"\n[INFO] Processing {target_date}...")

    all_data = []
    processed = 0
    errors = 0

    # Process in batches for progress tracking
    batch_size = 100
    total_batches = (len(tables) + batch_size - 1) // batch_size

    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min((batch_num + 1) * batch_size, len(tables))
        batch_tables = tables[start_idx:end_idx]

        for table in batch_tables:
            symbol = table.replace("TBL_", "")
            if not symbol:
                continue

            try:
                # Query delivery data for this stock
                query = text(f"""
                    SELECT
                        '{symbol}' as symbol,
                        CAST("ClsPric" AS NUMERIC) as close,
                        CAST("TtlTradgVol" AS NUMERIC) as volume,
                        CAST(COALESCE("DlvryQty", 0) AS NUMERIC) as delivery_qty,
                        CASE
                            WHEN CAST("TtlTradgVol" AS NUMERIC) > 0
                            THEN ROUND((CAST(COALESCE("DlvryQty", 0) AS NUMERIC) / CAST("TtlTradgVol" AS NUMERIC)) * 100, 2)
                            ELSE 0
                        END as delivery_pct
                    FROM public."{table}"
                    WHERE CAST("BizDt" AS DATE) = CAST(:target_date AS DATE)
                    AND "TtlTradgVol" IS NOT NULL
                    AND CAST("TtlTradgVol" AS NUMERIC) > 0
                    ORDER BY "BizDt" DESC
                    LIMIT 1
                """)

                with engine.connect() as conn:
                    df = pd.read_sql(query, conn, params={"target_date": target_date})

                if not df.empty:
                    all_data.append(df)
                    processed += 1

            except Exception as e:
                errors += 1
                if errors <= 3:
                    print(f"    [DEBUG] Skip {symbol}: {str(e)[:50]}")

        # Progress update
        if (batch_num + 1) % 5 == 0 or batch_num == total_batches - 1:
            print(f"    Progress: {end_idx}/{len(tables)} tables | Processed: {processed} | Errors: {errors}")

    if not all_data:
        print(f"    ⚠️  No delivery data found for {target_date}")
        return

    # Combine all data
    df_combined = pd.concat(all_data, ignore_index=True)

    # Insert into cache table
    try:
        insert_query = text("""
            INSERT INTO daily_delivery_data (date, symbol, close, volume, delivery_qty, delivery_pct)
            VALUES (:date, :symbol, :close, :volume, :delivery_qty, :delivery_pct)
            ON CONFLICT (date, symbol) DO UPDATE SET
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                delivery_qty = EXCLUDED.delivery_qty,
                delivery_pct = EXCLUDED.delivery_pct
        """)

        records = []
        for _, row in df_combined.iterrows():
            records.append({
                "date": target_date,
                "symbol": str(row["symbol"]),
                "close": float(row["close"]) if pd.notna(row["close"]) else 0,
                "volume": int(float(row["volume"])) if pd.notna(row["volume"]) else 0,
                "delivery_qty": int(float(row["delivery_qty"])) if pd.notna(row["delivery_qty"]) else 0,
                "delivery_pct": float(row["delivery_pct"]) if pd.notna(row["delivery_pct"]) else 0
            })

        with engine.connect() as conn:
            conn.execute(insert_query, records)
            conn.commit()

        print(f"    ✅ Cached {len(records)} stocks with delivery data")

    except Exception as e:
        print(f"    ❌ Failed to insert data: {e}")
        import traceback
        traceback.print_exc()


# =============================================================
# MAIN EXECUTION
# =============================================================

def update_delivery_cache():
    """Main execution function."""
    # Step 1: Create table
    if not create_cache_table():
        return

    # Step 2: Get tables
    print("\n[2/4] Getting stock tables...")
    all_tables = get_cash_tables()
    print(f"    ✅ Found {len(all_tables)} stock tables")

    if not all_tables:
        print("    ❌ No tables found!")
        return

    # Step 3: Find missing dates
    print("\n[3/4] Checking for missing dates...")
    cached = get_cached_dates()
    available = get_available_dates()

    print(f"    Cached dates: {len(cached)}")
    print(f"    Available dates: {len(available)}")

    missing = [d for d in available if d not in cached]

    if not missing:
        print("    ✅ Cache is up to date!")
        return

    print(f"    Found {len(missing)} missing dates")

    # Step 4: Process missing dates
    print("\n[4/4] Building cache...")

    for i, date in enumerate(missing, 1):
        print(f"\n[{i}/{len(missing)}] Processing {date}")
        process_date(date, all_tables)

    print("\n" + "="*70)
    print("✅ DELIVERY CACHE BUILD COMPLETE")
    print("="*70)
    print(f"\nCached dates: {len(missing)}")
    print(f"Total stocks per date: ~{len(all_tables)}")
    print("\nDelivery tab should now load in < 0.1 seconds!")
    print("="*70)


if __name__ == "__main__":
    update_delivery_cache()
