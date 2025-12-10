"""
FUTURES OI CACHE BUILDER
========================
Pre-calculates Futures OI data for CME, NME, FME expiries
Stores in futures_oi_cache table for fast retrieval

INCREMENTAL MODE: Only processes NEW dates, never drops existing data
"""

from datetime import datetime
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, inspect, text

# Database config
db_user = "postgres"
db_password = "Gallop@3104"
db_host = "localhost"
db_port = "5432"
db_name = "BhavCopy_Database"
db_password_enc = quote_plus(db_password)
engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}")


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


def get_cached_dates():
    """Get dates already in cache"""
    try:
        q = text("SELECT DISTINCT cache_date FROM public.futures_oi_cache ORDER BY cache_date")
        df = pd.read_sql(q, engine)
        return set(d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d) for d in df["cache_date"])
    except:
        return set()


def get_derived_tables():
    """Get all DERIVED tables"""
    inspector = inspect(engine)
    tables = [t for t in inspector.get_table_names() if t.endswith("_DERIVED")]
    return sorted(tables)


def get_available_dates():
    """Get all available dates from database"""
    tables = get_derived_tables()
    if not tables:
        return []
    sample = tables[0]
    q = text(f'SELECT DISTINCT "BizDt" FROM "{sample}" WHERE "BizDt" IS NOT NULL ORDER BY "BizDt" ASC')
    try:
        df = pd.read_sql(q, engine)
        return [d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d) for d in df["BizDt"]]
    except Exception as e:
        print(f"Error getting dates: {e}")
        return []


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
    INCREMENTAL: Only processes NEW dates, appends to existing cache
    """
    print("\n" + "=" * 70)
    print("FUTURES OI CACHE BUILDER (INCREMENTAL MODE)")
    print("=" * 70)

    # Create table if needed (NO DROP)
    create_futures_oi_cache_table()

    # Get all dates and cached dates
    all_dates = get_available_dates()
    cached_dates = get_cached_dates()
    tables = get_derived_tables()

    if not all_dates or not tables:
        print("✗ No data available")
        return

    # Find NEW dates only
    new_dates = [d for d in all_dates if d not in cached_dates]

    print(f"📅 Total dates in database: {len(all_dates)}")
    print(f"📂 Already cached: {len(cached_dates)}")
    print(f"🆕 NEW dates to process: {len(new_dates)}")
    print(f"📊 Tickers: {len(tables)}")

    if not new_dates:
        print("\n✅ Cache is up to date! No new dates to process.")
        return

    # Window for percentile calculation
    window = 20

    # Process each ticker for NEW dates only
    for ticker_idx, table in enumerate(tables, 1):
        ticker = table.replace("TBL_", "").replace("_DERIVED", "")
        print(f"\n[{ticker_idx}/{len(tables)}] {ticker}...", end=" ", flush=True)

        try:
            # Build historical data for this ticker (need full history for percentile calc)
            historical_data = []

            for bizdt in all_dates:
                q_expiries = text(
                    f"""
                    SELECT DISTINCT "FininstrmActlXpryDt"
                    FROM "{table}"
                    WHERE "BizDt" = :date AND "FinInstrmTp" = 'STF'
                    ORDER BY "FininstrmActlXpryDt"
                """
                )
                df_exp = pd.read_sql(q_expiries, engine, params={"date": bizdt})

                if df_exp.empty:
                    continue

                expiries = sorted([pd.to_datetime(v) for v in df_exp["FininstrmActlXpryDt"]])
                current_dt = pd.to_datetime(bizdt)
                future_expiries = [exp for exp in expiries if exp >= current_dt]

                if len(future_expiries) == 0:
                    continue

                cme = future_expiries[0] if len(future_expiries) > 0 else None
                nme = future_expiries[1] if len(future_expiries) > 1 else None
                fme = future_expiries[2] if len(future_expiries) > 2 else None

                for exp_type, exp_date in [("CME", cme), ("NME", nme), ("FME", fme)]:
                    if exp_date is None:
                        continue

                    q_metrics = text(
                        f"""
                        SELECT "ClsPric", "OpnIntrst", "ChngInOpnIntrst", "UndrlygPric"
                        FROM "{table}"
                        WHERE "BizDt" = :date AND "FininstrmActlXpryDt" = :expiry AND "FinInstrmTp" = 'STF'
                        LIMIT 1
                    """
                    )
                    df_metrics = pd.read_sql(
                        q_metrics,
                        engine,
                        params={"date": bizdt, "expiry": exp_date.strftime("%Y-%m-%d")},
                    )

                    if df_metrics.empty:
                        continue

                    row = df_metrics.iloc[0]

                    historical_data.append(
                        {
                            "cache_date": bizdt,
                            "ticker": ticker,
                            "expiry_type": exp_type,
                            "expiry_date": exp_date.strftime("%Y-%m-%d"),
                            "underlying_price": float(row["UndrlygPric"]) if pd.notnull(row["UndrlygPric"]) else 0,
                            "expiry_price": float(row["ClsPric"]) if pd.notnull(row["ClsPric"]) else 0,
                            "expiry_oi": float(row["OpnIntrst"]) if pd.notnull(row["OpnIntrst"]) else 0,
                            "expiry_oi_change": float(row["ChngInOpnIntrst"])
                            if pd.notnull(row["ChngInOpnIntrst"])
                            else 0,
                        }
                    )

            if not historical_data:
                print("⚠ No data", end="")
                continue

            df = pd.DataFrame(historical_data)

            # Calculate percentiles and filter to NEW dates only
            cache_rows = []

            for exp_type in ["CME", "NME", "FME"]:
                exp_df = df[df["expiry_type"] == exp_type].copy()

                if exp_df.empty:
                    continue

                exp_df = exp_df.sort_values("cache_date").reset_index(drop=True)

                for i, row in exp_df.iterrows():
                    # ONLY process NEW dates
                    if row["cache_date"] not in new_dates:
                        continue

                    start_idx = max(0, i - window + 1)
                    window_df = exp_df.iloc[start_idx : i + 1]

                    oi_percentile = calculate_percentile_rank(row["expiry_oi"], window_df["expiry_oi"])
                    price_percentile = calculate_percentile_rank(row["expiry_price"], window_df["expiry_price"])

                    cache_rows.append(
                        {
                            "cache_date": row["cache_date"],
                            "ticker": row["ticker"],
                            "underlying_price": row["underlying_price"],
                            "expiry_type": row["expiry_type"],
                            "expiry_date": row["expiry_date"],
                            "expiry_price": row["expiry_price"],
                            "expiry_oi": row["expiry_oi"],
                            "expiry_oi_change": row["expiry_oi_change"],
                            "oi_percentile": oi_percentile,
                            "price_percentile": price_percentile,
                        }
                    )

            # Insert NEW rows only
            if cache_rows:
                insert_df = pd.DataFrame(cache_rows)
                insert_df.to_sql("futures_oi_cache", con=engine, if_exists="append", index=False)
                print(f"✓ {len(cache_rows)} new rows", end="")
            else:
                print("⚠ No new rows", end="")

        except Exception as e:
            print(f"✗ Error: {e}", end="")

    print("\n\n" + "=" * 70)
    print("✓ FUTURES OI CACHE UPDATE COMPLETE!")
    print("=" * 70)


if __name__ == "__main__":
    precalculate_futures_oi_cache()
