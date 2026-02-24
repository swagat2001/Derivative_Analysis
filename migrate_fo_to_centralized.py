import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from Analysis_Tools.app.models.db_config import engine
from sqlalchemy import text, inspect
import pandas as pd

def create_master_table():
    create_sql = """
    CREATE TABLE IF NOT EXISTS public.fo_eod_data (
        trade_date DATE NOT NULL,
        symbol VARCHAR(50) NOT NULL,
        expiry_date DATE NOT NULL,
        strike_price NUMERIC NOT NULL,
        option_type VARCHAR(5) NOT NULL,
        instrument_type VARCHAR(10),

        open NUMERIC,
        high NUMERIC,
        low NUMERIC,
        close NUMERIC,
        settlement_price NUMERIC,

        open_interest BIGINT,
        change_in_oi BIGINT,
        volume BIGINT,

        implied_volatility NUMERIC,
        delta NUMERIC,
        gamma NUMERIC,
        theta NUMERIC,
        vega NUMERIC,
        rho NUMERIC,

        PRIMARY KEY (trade_date, symbol, expiry_date, strike_price, option_type)
    );
    CREATE INDEX IF NOT EXISTS idx_fo_symbol_date ON public.fo_eod_data(symbol, trade_date);
    CREATE INDEX IF NOT EXISTS idx_fo_date_expiry on public.fo_eod_data(trade_date, expiry_date);
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))
    print("✓ Created fo_eod_data table and indexes.")

def migrate_table(base_table):
    symbol = base_table.replace("TBL_", "")
    derived_table = f"{base_table}_DERIVED"

    # Check if derived table exists
    inspector = inspect(engine)
    has_derived = derived_table in inspector.get_table_names()

    try:
        max_date_query = text("SELECT MAX(trade_date) FROM public.fo_eod_data WHERE symbol = :sym")
        with engine.connect() as db_conn:
            max_date = db_conn.execute(max_date_query, {"sym": symbol}).scalar()
    except:
        max_date = None

    date_filter = ""
    if max_date:
        date_filter = f" AND b.\"BizDt\" > '{max_date}' "

    if has_derived:
        # Join base and derived to get greeks
        query = text(f'''
        SELECT
            b."BizDt" as trade_date,
            '{symbol}' as symbol,
            b."FininstrmActlXpryDt" as expiry_date,
            b."StrkPric" as strike_price,
            b."OptnTp" as option_type,
            b."FinInstrmTp" as instrument_type,

            b."OpnPric" as open,
            b."HghPric" as high,
            b."LwPric" as low,
            b."ClsPric" as close,
            b."SttlmPric" as settlement_price,

            b."OpnIntrst" as open_interest,
            b."ChngInOpnIntrst" as change_in_oi,
            b."TtlTradgVol" as volume,

            d."iv" as implied_volatility,
            d."delta" as delta,
            d."gamma" as gamma,
            d."theta" as theta,
            d."vega" as vega,
            d."rho" as rho
        FROM public."{base_table}" b
        LEFT JOIN public."{derived_table}" d
          ON b."BizDt" = d."BizDt"
          AND b."FininstrmActlXpryDt" = d."FininstrmActlXpryDt"
          AND b."StrkPric" = d."StrkPric"
          AND b."OptnTp" = d."OptnTp"
        WHERE b."BizDt" IS NOT NULL {date_filter}
        ''')
    else:
        date_filter_base = ""
        if max_date:
            date_filter_base = f" AND \"BizDt\" > '{max_date}' "

        # Just use base table
        query = text(f'''
        SELECT
            "BizDt" as trade_date,
            '{symbol}' as symbol,
            "FininstrmActlXpryDt" as expiry_date,
            "StrkPric" as strike_price,
            "OptnTp" as option_type,
            "FinInstrmTp" as instrument_type,

            "OpnPric" as open,
            "HghPric" as high,
            "LwPric" as low,
            "ClsPric" as close,
            "SttlmPric" as settlement_price,

            "OpnIntrst" as open_interest,
            "ChngInOpnIntrst" as change_in_oi,
            "TtlTradgVol" as volume,

            NULL as implied_volatility,
            NULL as delta,
            NULL as gamma,
            NULL as theta,
            NULL as vega,
            NULL as rho
        FROM public."{base_table}"
        WHERE "BizDt" IS NOT NULL {date_filter_base}
        ''')

    try:
        df = pd.read_sql(query, engine)
        if df.empty:
            return 0

        # Drop rows where primary key fields are null
        df = df.dropna(subset=['trade_date', 'expiry_date', 'strike_price', 'option_type'])

        # Fix typing issues
        df['option_type'] = df['option_type'].astype(str).str.strip()
        df.loc[df['option_type'] == 'nan', 'option_type'] = 'XX'
        df.loc[df['option_type'] == '', 'option_type'] = 'XX'

        if df.empty:
            return 0

        # Replace nan with None
        df = df.where(pd.notnull(df), None)

        with engine.begin() as conn:
            data_dicts = df.to_dict(orient='records')

            insert_query = text("""
            INSERT INTO public.fo_eod_data (
                trade_date, symbol, expiry_date, strike_price, option_type,
                instrument_type, open, high, low, close, settlement_price,
                open_interest, change_in_oi, volume, implied_volatility,
                delta, gamma, theta, vega, rho
            ) VALUES (
                :trade_date, :symbol, :expiry_date, :strike_price, :option_type,
                :instrument_type, :open, :high, :low, :close, :settlement_price,
                :open_interest, :change_in_oi, :volume, :implied_volatility,
                :delta, :gamma, :theta, :vega, :rho
            ) ON CONFLICT (trade_date, symbol, expiry_date, strike_price, option_type) DO NOTHING
            """)

            # Use chunks for safety
            chunk_size = 10000
            for i in range(0, len(data_dicts), chunk_size):
                chunk = data_dicts[i:i + chunk_size]
                conn.execute(insert_query, chunk)

        return len(df)

    except Exception as e:
        print(f"Error migrating {base_table}: {e}")
        import traceback
        traceback.print_exc()
        return 0

def run_migration():
    sys.stdout.reconfigure(encoding='utf-8')
    print("="*80)
    print(" MIGRATING F&O DATABASE TO CENTRALIZED TABLE ")
    print("="*80)

    create_master_table()

    inspector = inspect(engine)
    all_tables = inspector.get_table_names()
    tbl_tables = [t for t in all_tables if t.startswith("TBL_") and not t.endswith("_DERIVED")]

    print(f"🔄 Found {len(tbl_tables)} base tables to migrate...")

    total_rows = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(migrate_table, t): t for t in tbl_tables}

        for i, future in enumerate(futures):
            res = future.result()
            total_rows += res
            if (i+1) % 10 == 0:
                print(f"[{i+1}/{len(tbl_tables)}] Migrated so far: {total_rows} rows")

    end_time = time.time()
    print(f"\n✅ Migration completed in {end_time - start_time:.1f}s")
    print(f"✅ Total rows migrated: {total_rows}")

if __name__ == "__main__":
    run_migration()
