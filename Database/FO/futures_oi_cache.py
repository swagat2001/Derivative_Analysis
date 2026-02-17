"""
FUTURES OI CACHE BUILDER
========================
Pre-calculates Futures OI data for CME, NME, FME expiries
Stores in futures_oi_cache table for fast retrieval

INCREMENTAL MODE: Only processes NEW dates, never drops existing data
OPTIMIZED: Single query per ticker, partial-commit safe.
"""

import os
import sys
from datetime import datetime
from dotenv import load_dotenv
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, inspect, text

load_dotenv()
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from Analysis_Tools.app.models.db_config import engine


def create_futures_oi_cache_table():
    """Create the futures_oi_cache table if it doesn't exist (NO DROP)"""
    create_query = """
    CREATE TABLE IF NOT EXISTS public.futures_oi_cache (
        id SERIAL PRIMARY KEY,
        cache_date DATE NOT NULL,
        ticker VARCHAR(50) NOT NULL,
        underlying_price NUMERIC,
        expiry_type VARCHAR(10) NOT NULL,
        expiry_date DATE,
        expiry_price NUMERIC,
        expiry_oi NUMERIC,
        expiry_oi_change NUMERIC,
        oi_percentile NUMERIC,
        price_percentile NUMERIC,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(cache_date, ticker, expiry_type)
    );

    CREATE INDEX IF NOT EXISTS idx_futures_oi_cache_date ON public.futures_oi_cache(cache_date);
    CREATE INDEX IF NOT EXISTS idx_futures_oi_cache_ticker ON public.futures_oi_cache(ticker);
    CREATE INDEX IF NOT EXISTS idx_futures_oi_cache_expiry ON public.futures_oi_cache(expiry_type);
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(create_query))
        print("✓ futures_oi_cache table ready")
    except Exception as e:
        print(f"✗ Error creating table: {e}")


def get_cached_keys():
    """Get (date, ticker) tuples already in cache to prevent re-processing"""
    try:
        q = text("SELECT DISTINCT cache_date, ticker FROM public.futures_oi_cache")
        with engine.connect() as conn:
            result = conn.execute(q).fetchall()
            # Return set of (date_str, ticker) tests
            return set(
                (
                    row[0].strftime("%Y-%m-%d") if hasattr(row[0], "strftime") else str(row[0]),
                    str(row[1]),
                )
                for row in result
            )
    except Exception as e:
        print(f"Warning reading cache keys: {e}")
        return set()


def get_derived_tables():
    """Get all DERIVED tables"""
    inspector = inspect(engine)
    tables = [t for t in inspector.get_table_names() if t.endswith("_DERIVED")]
    return sorted(tables)


def calculate_percentile_rank(value, series):
    """Calculate percentile rank of a value within a series"""
    if len(series) == 0 or pd.isna(value):
        return 50.0

    series = series.dropna()
    if len(series) == 0:
        return 50.0

    count_less = np.sum(series < value)
    count_equal = np.sum(series == value)
    percentile = (count_less + 0.5 * count_equal) / len(series) * 100
    return round(percentile, 2)


def precalculate_futures_oi_cache():
    """
    INCREMENTAL: Processes missing (date, ticker) pairs.
    OPTIMIZED: 1 Query per Ticker (vs N Dates * N Tickers)
    """
    print("\n" + "=" * 70)
    print("FUTURES OI CACHE BUILDER (OPTIMIZED INCREMENTAL)")
    print("=" * 70)

    # Create table if needed (NO DROP)
    create_futures_oi_cache_table()

    # Get cached keys (Date, Ticker) to allow granular updates
    cached_keys = get_cached_keys()
    print(f"📂 Cached items: {len(cached_keys)}")

    tables = get_derived_tables()
    if not tables:
        print("✗ No data available")
        return

    print(f"📊 Tickers to scan: {len(tables)}")

    # Window for percentile calculation
    window = 20

    # Process each ticker
    for ticker_idx, table in enumerate(tables, 1):
        ticker = table.replace("TBL_", "").replace("_DERIVED", "")
        print(f"\n[{ticker_idx}/{len(tables)}] {ticker}...", end=" ", flush=True)

        try:
            # OPTIMIZATION: Fetch ALL Stock Futures (STF) data for this ticker in ONE query
            # We need history for percentile calculation locally
            q = text(
                f"""
                SELECT "BizDt", "FininstrmActlXpryDt", "ClsPric", "OpnIntrst", "ChngInOpnIntrst", "UndrlygPric"
                FROM "{table}"
                WHERE "FinInstrmTp" = 'STF' AND "BizDt" IS NOT NULL
                ORDER BY "BizDt", "FininstrmActlXpryDt"
            """
            )
            df_full = pd.read_sql(q, engine)

            if df_full.empty:
                print("⚠ No STF data", end="")
                continue

            # Pre-process dates
            df_full["BizDt"] = pd.to_datetime(df_full["BizDt"])
            df_full["FininstrmActlXpryDt"] = pd.to_datetime(df_full["FininstrmActlXpryDt"])

            # Numeric conversion
            cols = ["ClsPric", "OpnIntrst", "ChngInOpnIntrst", "UndrlygPric"]
            for c in cols:
                df_full[c] = pd.to_numeric(df_full[c], errors="coerce").fillna(0)

            # Get unique business dates for this ticker
            biz_dates = sorted(df_full["BizDt"].unique())

            rows_to_insert = []

            # For each day, identify Near(CME), Next(NME), Far(FME) expiries
            # Much faster to group by BizDt in Pandas
            grouped = df_full.groupby("BizDt")

            # We need to maintain a history of OI/Price for percentiles
            # Structure: history[expiry_type_str] = list of values
            # BUT percentile is rolling window of 20 days.
            # We can build a DataFrame of [Date, ExpiryType, Price, OI] first, then rolling apply.

            extracted_data = []

            for bizdt, group in grouped:
                # Identification of expiries
                # Sort contracts by expiry date
                contracts = group.sort_values("FininstrmActlXpryDt")
                expiries = contracts["FininstrmActlXpryDt"].unique()
                future_expiries = [e for e in expiries if e >= bizdt] # Expiry must be >= Today

                if not future_expiries:
                    continue

                cme = future_expiries[0] if len(future_expiries) > 0 else None
                nme = future_expiries[1] if len(future_expiries) > 1 else None
                fme = future_expiries[2] if len(future_expiries) > 2 else None

                for exp_type, exp_date in [("CME", cme), ("NME", nme), ("FME", fme)]:
                    if pd.isna(exp_date):
                        continue

                    # Get exact row
                    row = contracts[contracts["FininstrmActlXpryDt"] == exp_date].iloc[0]

                    extracted_data.append({
                        "cache_date": bizdt,
                        "ticker": ticker,
                        "expiry_type": exp_type,
                        "expiry_date": exp_date,
                        "underlying_price": row["UndrlygPric"],
                        "expiry_price": row["ClsPric"],
                        "expiry_oi": row["OpnIntrst"],
                        "expiry_oi_change": row["ChngInOpnIntrst"]
                    })

            if not extracted_data:
                continue

            df_metrics = pd.DataFrame(extracted_data)

            # Calculate percentiles per expiry type
            for exp_type in ["CME", "NME", "FME"]:
                sub_df = df_metrics[df_metrics["expiry_type"] == exp_type].copy()
                if sub_df.empty:
                    continue

                # Sort by date
                sub_df = sub_df.sort_values("cache_date").reset_index(drop=True)

                # Check which rows need insertion (not in cache)
                # date string format for cache check
                sub_df["date_str"] = sub_df["cache_date"].dt.strftime("%Y-%m-%d")

                # Mask for missing rows
                # Check if (date, ticker) is in cached_keys
                # We can just iterate and check

                for i, row in sub_df.iterrows():
                    date_str = row["date_str"]

                    if (date_str, ticker) in cached_keys:
                        continue

                    # Need percentile (requires window)
                    start_idx = max(0, i - window + 1)
                    window_df = sub_df.iloc[start_idx : i + 1]

                    oi_pct = calculate_percentile_rank(row["expiry_oi"], window_df["expiry_oi"])
                    price_pct = calculate_percentile_rank(row["expiry_price"], window_df["expiry_price"])

                    rows_to_insert.append({
                        "cache_date": row["cache_date"].date(), # Object for sql
                        "ticker": ticker,
                        "underlying_price": float(row["underlying_price"]),
                        "expiry_type": exp_type,
                        "expiry_date": row["expiry_date"].date(),
                        "expiry_price": float(row["expiry_price"]),
                        "expiry_oi": float(row["expiry_oi"]),
                        "expiry_oi_change": float(row["expiry_oi_change"]),
                        "oi_percentile": float(oi_pct),
                        "price_percentile": float(price_pct)
                    })

            # Bulk Insert per ticker
            if rows_to_insert:
                insert_df = pd.DataFrame(rows_to_insert)
                # Drop duplicates just in case (e.g. same date multiple expiries logic overlap)
                insert_df = insert_df.drop_duplicates(subset=["cache_date", "ticker", "expiry_type"])

                # Insert
                with engine.begin() as conn:
                    # We use SQLAlchemy's built-in list insert via pandas to_sql
                    # But to avoid PK conflicts if cache key check failed partially, we do "ON CONFLICT DO NOTHING" via raw SQL?
                    # or just append and trust the cache check.
                    # Since we filtered by `if (date, ticker) in cached_keys`, we should be safe.
                    # But `UNIQUE(cache_date, ticker, expiry_type)` constraint exists.
                    # safer to iterate or use method that handles it. PANDAS `method`?
                    # Let's just try-except import error block or trust the check.
                    # For safety against race conditions or partials, let's use a loop or chunks if massive?
                    # 100 rows is small.

                    # We'll use pandas to_sql default. If error, we print.
                    insert_df.to_sql("futures_oi_cache", conn, if_exists="append", index=False)

                print(f"✓ {len(rows_to_insert)} new rows", end="")
            else:
                print("✓ Up to date", end="")

        except Exception as e:
            print(f"✗ Error: {e}", end="")

    print("\n\n" + "=" * 70)
    print("✓ FUTURES OI CACHE UPDATE COMPLETE!")
    print("=" * 70)


if __name__ == "__main__":
    precalculate_futures_oi_cache()
