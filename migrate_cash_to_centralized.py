import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from Analysis_Tools.app.models.db_config import engine_cash
from sqlalchemy import text, inspect
import pandas as pd

def create_master_table():
    create_sql = """
    CREATE TABLE IF NOT EXISTS public.cash_eod_data (
        trade_date DATE NOT NULL,
        symbol VARCHAR(50) NOT NULL,
        open NUMERIC,
        high NUMERIC,
        low NUMERIC,
        close NUMERIC,
        prev_close NUMERIC,
        volume BIGINT,
        turnover NUMERIC,
        deliverable_qty BIGINT,
        delivery_pct NUMERIC,
        PRIMARY KEY (trade_date, symbol)
    );
    CREATE INDEX IF NOT EXISTS idx_cash_symbol ON public.cash_eod_data(symbol);
    CREATE INDEX IF NOT EXISTS idx_cash_date ON public.cash_eod_data(trade_date);
    """
    with engine_cash.begin() as conn:
        conn.execute(text(create_sql))
    print("✓ Created cash_eod_data table and indexes.")

def migrate_table(table_name):
    symbol = table_name.replace("TBL_", "")

    query = text(f'''
    SELECT
        "BizDt" as trade_date,
        '{symbol}' as symbol,
        "OpnPric" as open,
        "HghPric" as high,
        "LwPric" as low,
        "ClsPric" as close,
        "PrvsClsgPric" as prev_close,
        "TtlTradgVol" as volume,
        "TtlTrfVal" as turnover,
        "DlvryQty" as deliverable_qty,
        "DlvryPer" as delivery_pct
    FROM public."{table_name}"
    WHERE "BizDt" IS NOT NULL AND TRIM("SERIES") = 'EQ'
    ''')

    try:
        df = pd.read_sql(query, engine_cash)
        if df.empty:
            return 0

        # Drop rows where primary key fields are null
        df = df.dropna(subset=['trade_date', 'symbol'])

        # Replace nan with None
        df = df.where(pd.notnull(df), None)

        with engine_cash.begin() as conn:
            data_dicts = df.to_dict(orient='records')

            insert_query = text("""
            INSERT INTO public.cash_eod_data (
                trade_date, symbol, open, high, low, close, prev_close,
                volume, turnover, deliverable_qty, delivery_pct
            ) VALUES (
                :trade_date, :symbol, :open, :high, :low, :close, :prev_close,
                :volume, :turnover, :deliverable_qty, :delivery_pct
            ) ON CONFLICT (trade_date, symbol) DO NOTHING
            """)

            chunk_size = 10000
            for i in range(0, len(data_dicts), chunk_size):
                chunk = data_dicts[i:i + chunk_size]
                conn.execute(insert_query, chunk)

        return len(df)

    except Exception as e:
        print(f"Error migrating {table_name}: {e}")
        import traceback
        traceback.print_exc()
        return 0

def run_migration():
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        pass
    print("="*80)
    print(" MIGRATING CASH DATABASE TO CENTRALIZED TABLE ")
    print("="*80)

    create_master_table()

    inspector = inspect(engine_cash)
    all_tables = inspector.get_table_names()
    tbl_tables = [t for t in all_tables if t.startswith("TBL_")]

    print(f"🔄 Found {len(tbl_tables)} tables to migrate...")

    total_rows = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(migrate_table, t): t for t in tbl_tables}

        for i, future in enumerate(futures):
            res = future.result()
            total_rows += res
            if (i+1) % 100 == 0:
                print(f"[{i+1}/{len(tbl_tables)}] Migrated so far: {total_rows} rows")

    end_time = time.time()
    print(f"\n✅ Migration completed in {end_time - start_time:.1f}s")
    print(f"✅ Total rows migrated: {total_rows}")

if __name__ == "__main__":
    run_migration()
