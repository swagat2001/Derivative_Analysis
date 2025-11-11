"""
Debug script to check why NIFTY is missing from futures screener
"""

from Analysis_Tools.app.models.db_config import engine
from sqlalchemy import text, inspect
import pandas as pd

print("=" * 80)
print("NIFTY FUTURES DEBUG SCRIPT")
print("=" * 80 + "\n")

# 1. Check if TBL_NIFTY exists
print("1. Checking TBL_NIFTY tables...")
try:
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    
    nifty_tables = [t for t in tables if 'NIFTY' in t and 'DERIVED' in t]
    print(f"   Found NIFTY tables: {nifty_tables}\n")
except Exception as e:
    print(f"   Error: {e}\n")

# 2. Check available dates
print("2. Checking available dates in TBL_NIFTY_DERIVED...")
try:
    query = text('''
        SELECT DISTINCT "BizDt" FROM public."TBL_NIFTY_DERIVED" 
        ORDER BY "BizDt" DESC LIMIT 10
    ''')
    df_dates = pd.read_sql(query, con=engine)
    print(df_dates.to_string(index=False))
    print()
except Exception as e:
    print(f"   Error: {e}\n")

# 3. Check NIFTY futures data for 07-Nov-2025
print("3. Checking NIFTY futures rows (OptnTp IS NULL) for 2025-11-07...")
try:
    query = text('''
        SELECT 
            "BizDt", "OptnTp", "StrkPric", "UndrlygPric", "OpnIntrst", "LastPric"
        FROM public."TBL_NIFTY_DERIVED"
        WHERE "BizDt" = '2025-11-07' AND "OptnTp" IS NULL
        LIMIT 5
    ''')
    df_nifty_fut = pd.read_sql(query, con=engine)
    
    if df_nifty_fut.empty:
        print("   ⚠️  NO FUTURES ROWS FOUND for NIFTY on 2025-11-07")
    else:
        print(f"   Found {len(df_nifty_fut)} futures rows:")
        print(df_nifty_fut.to_string(index=False))
    print()
except Exception as e:
    print(f"   Error: {e}\n")

# 4. Check previous date (06-Nov-2025)
print("4. Checking previous date (06-Nov-2025) for NIFTY futures...")
try:
    query = text('''
        SELECT 
            "BizDt", "OptnTp", "StrkPric", "UndrlygPric", "OpnIntrst"
        FROM public."TBL_NIFTY_DERIVED"
        WHERE "BizDt" = '2025-11-06' AND "OptnTp" IS NULL
        LIMIT 5
    ''')
    df_prev = pd.read_sql(query, con=engine)
    
    if df_prev.empty:
        print("   ⚠️  NO PREVIOUS DATE DATA - Cannot calculate OI change!")
    else:
        print(f"   Found {len(df_prev)} rows for 06-Nov-2025")
        print(df_prev.to_string(index=False))
    print()
except Exception as e:
    print(f"   Error: {e}\n")

# 5. Check screener_cache for NIFTY
print("5. Checking screener_cache for NIFTY (All dates and metrics)...")
try:
    query = text('''
        SELECT 
            cache_date, metric_type, option_type, ticker, underlying_price, change
        FROM public.screener_cache
        WHERE ticker = 'NIFTY'
        ORDER BY cache_date DESC, metric_type
        LIMIT 15
    ''')
    df_cache = pd.read_sql(query, con=engine)
    
    if df_cache.empty:
        print("   ⚠️  NIFTY NOT FOUND IN screener_cache TABLE AT ALL")
    else:
        print(f"   Found {len(df_cache)} cache rows for NIFTY:")
        print(df_cache.to_string(index=False))
    print()
except Exception as e:
    print(f"   Error: {e}\n")

# 6. Check screener_cache for date 2025-11-07 futures
print("6. Checking screener_cache futures data for 2025-11-07...")
try:
    query = text('''
        SELECT 
            ticker, underlying_price, change, rank
        FROM public.screener_cache
        WHERE cache_date = '2025-11-07' 
            AND metric_type = 'oi' 
            AND option_type = 'FUT'
        ORDER BY rank ASC
        LIMIT 15
    ''')
    df_fut_cache = pd.read_sql(query, con=engine)
    
    if df_fut_cache.empty:
        print("   ⚠️  NO FUTURES DATA in cache for 2025-11-07")
    else:
        print(f"   Found {len(df_fut_cache)} futures records:")
        print(df_fut_cache.to_string(index=False))
        
        if 'NIFTY' in df_fut_cache['ticker'].values:
            print("\n   ✅ NIFTY IS IN CACHE")
        else:
            print("\n   ❌ NIFTY IS NOT IN CACHE")
    print()
except Exception as e:
    print(f"   Error: {e}\n")

print("=" * 80)
print("DEBUG COMPLETE")
print("=" * 80)
