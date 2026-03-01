import os
import sys
import json
import pandas as pd
from sqlalchemy import create_engine, text

# Add parent directory to sys.path to import db_config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from Analysis_Tools.app.models.db_config import engine_cash

# Base output directory for scraper data
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

def analyze_missing_stocks():
    print("--- Analyzing Missing Fundamental Data ---")

    # 1. Get symbols from the Cash database
    print("[INFO] Fetching symbols from CashStocks_Database...")
    try:
        with engine_cash.connect() as conn:
            # Get symbols from stock_fundamentals table
            try:
                df_fund = pd.read_sql(text("SELECT symbol FROM stock_fundamentals"), conn)
                symbols_fund = set(df_fund['symbol'].astype(str).str.strip().str.upper().unique())
                print(f"[OK] Found {len(symbols_fund)} symbols in 'stock_fundamentals' table.")
            except Exception as e:
                print(f"[WARN] Could not read 'stock_fundamentals': {e}")
                symbols_fund = set()

            # Get symbols from TBL_<SYMBOL> tables
            try:
                result = conn.execute(text("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_name LIKE 'TBL_%'
                      AND table_name NOT LIKE '%_DERIVED'
                """))
                symbols_table = set([row[0].replace("TBL_", "").strip().upper() for row in result])
                print(f"[OK] Found {len(symbols_table)} 'TBL_...' tables in database.")
            except Exception as e:
                print(f"[WARN] Could not read table list: {e}")
                symbols_table = set()

        # Combine both sets as the target list
        all_target_symbols = symbols_fund.union(symbols_table)
        # Filter out common non-stock symbols if any start with numbers or are indices
        all_target_symbols = {s for s in all_target_symbols if s and not s.startswith('999') and any(c.isalpha() for c in s)}

        print(f"[INFO] Total unique stock symbols identified for analysis: {len(all_target_symbols)}")

    except Exception as e:
        print(f"[ERROR] Database connection failed: {e}")
        return

    # 2. Identify already scraped symbols
    # We check the 'pnl' or 'quarterly' directory as a proxy for fundamental data existence
    existing_symbols = set()
    check_dir = os.path.join(DATA_DIR, "pnl")

    print(f"[INFO] Checking existing data in: {check_dir}")
    if os.path.exists(check_dir):
        files = [f for f in os.listdir(check_dir) if f.endswith(".json")]
        for f in files:
            symbol = f.replace(".json", "").strip().upper()
            existing_symbols.add(symbol)
        print(f"[OK] Found {len(existing_symbols)} stocks with existing fundamental data.")
    else:
        print(f"[WARN] Directory {check_dir} does not exist yet.")

    # 3. Find discrepancies
    missing_symbols = sorted(list(all_target_symbols - existing_symbols))

    print(f"\n--- RESULTS ---")
    print(f"Total target stocks:  {len(all_target_symbols)}")
    print(f"Already scraped:      {len(existing_symbols)}")
    print(f"Missing stocks:       {len(missing_symbols)}")

    # 4. Save results to a file for the next step
    output_file = os.path.join(os.path.dirname(__file__), "missing_stocks.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(missing_symbols, f, indent=2)

    print(f"\n[SUCCESS] List of {len(missing_symbols)} missing stocks saved to {output_file}")

    if missing_symbols:
        print(f"[INFO] Sample missing stocks: {', '.join(missing_symbols[:10])}...")

if __name__ == "__main__":
    analyze_missing_stocks()
