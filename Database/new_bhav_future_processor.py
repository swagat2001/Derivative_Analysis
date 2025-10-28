from sqlalchemy import create_engine, inspect, text
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from urllib.parse import quote_plus
import psycopg2

from py_vollib.black_scholes.implied_volatility import implied_volatility
from py_vollib.black_scholes.greeks.analytical import delta, gamma, rho, theta, vega

# ===========================================
# 🔧 DB Configuration
# ===========================================
db_user = 'postgres'
db_password = 'Gallop@3104'
db_host = 'localhost'
db_port = '5432'
db_name = 'BhavCopy_Database'

# Encode password for URL
db_password_enc = quote_plus(db_password)

# Create SQLAlchemy engine
engine = create_engine(
    f'postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}'
)
conn = engine.connect()

# ===========================================
# 📅 Auto-Detect Dates to Process
# ===========================================
def get_dates_to_process():
    """Get dates that exist in base tables but not in DERIVED tables"""
    try:
        inspector = inspect(engine)
        
        # Get a sample base table (not DERIVED)
        base_tables = [t for t in inspector.get_table_names() if t.startswith("TBL_") and not t.endswith("_DERIVED")]
        
        if not base_tables:
            print("❌ No base tables found!")
            return []
        
        # Use NIFTY or first available table as reference
        sample_table = None
        for preferred in ['TBL_NIFTY', 'TBL_BANKNIFTY', 'TBL_RELIANCE']:
            if preferred in base_tables:
                sample_table = preferred
                break
        
        if not sample_table:
            sample_table = base_tables[0]
        
        print(f"📊 Using {sample_table} as reference for dates\n")
        
        # Get all dates from base table
        query_base = text(f'''
            SELECT DISTINCT "BizDt" 
            FROM public."{sample_table}"
            WHERE "BizDt" IS NOT NULL
            ORDER BY "BizDt" ASC
        ''')
        base_dates = pd.read_sql(query_base, engine)
        
        if base_dates.empty:
            print("❌ No dates found in base table!")
            return []
        
        # Check if DERIVED table exists
        derived_table = f"{sample_table}_DERIVED"
        
        if derived_table in inspector.get_table_names():
            # Get dates already in DERIVED table
            query_derived = text(f'''
                SELECT DISTINCT "BizDt" 
                FROM public."{derived_table}"
                WHERE "BizDt" IS NOT NULL
                ORDER BY "BizDt" ASC
            ''')
            derived_dates = pd.read_sql(query_derived, engine)
            derived_dates_set = set(derived_dates['BizDt'].astype(str))
        else:
            derived_dates_set = set()
        
        # Find dates that need processing
        base_dates_set = set(base_dates['BizDt'].astype(str))
        dates_to_process = sorted(list(base_dates_set - derived_dates_set))
        
        return dates_to_process
        
    except Exception as e:
        print(f"❌ Error detecting dates: {e}")
        return []

print("="*80)
print("🤖 AUTO-DETECTING DATES TO PROCESS")
print("="*80)

dates_to_process = get_dates_to_process()

if not dates_to_process:
    print("\n✅ All dates already processed!")
    print("   No new dates to calculate Greeks for.")
    exit()

print(f"📅 Found {len(dates_to_process)} date(s) to process:\n")
for i, date in enumerate(dates_to_process, 1):
    print(f"   {i}. {date}")

print("\n" + "="*80)

# Option for user to proceed
proceed = input("\nProceed with processing all these dates? (Y/N, default=Y): ").strip().upper()
if proceed == 'N':
    print("❌ Processing cancelled by user.")
    exit()

# ===========================================
# 🔍 Get Only TBL_* Tables (skip derived)
# ===========================================
inspector = inspect(engine)
ticker_tables = [t for t in inspector.get_table_names() if t.startswith("TBL_") and not t.endswith("_DERIVED")]
print(f"\n📌 Found {len(ticker_tables)} ticker tables to process")

# ===========================================
# ✳️ Greeks Function
# ===========================================
def greeks(premium, expiry, cd, asset_price, strike_price, intrest_rate, instrument_type):
    try:
        t = ((datetime(expiry.year, expiry.month, expiry.day, 15, 30) -
              datetime(cd.year, cd.month, cd.day, 15, 30)) / timedelta(days=1)) / 365
        
        if t <= 0:
            return {"IV": 0, "Delta": 0, "Gamma": 0, "Rho": 0, "Theta": 0, "Vega": 0}
        
        flag = instrument_type[0].lower()
        imp_v = implied_volatility(premium, asset_price, strike_price, t, intrest_rate, flag)
        return {
            "IV": imp_v,
            "Delta": delta(flag, asset_price, strike_price, t, intrest_rate, imp_v),
            "Gamma": gamma(flag, asset_price, strike_price, t, intrest_rate, imp_v),
            "Rho": rho(flag, asset_price, strike_price, t, intrest_rate, imp_v),
            "Theta": theta(flag, asset_price, strike_price, t, intrest_rate, imp_v),
            "Vega": vega(flag, asset_price, strike_price, t, intrest_rate, imp_v)
        }
    except Exception:
        return {"IV": 0, "Delta": 0, "Gamma": 0, "Rho": 0, "Theta": 0, "Vega": 0}

# ===========================================
# 🔁 Loop through each DATE
# ===========================================
total_dates = len(dates_to_process)

for date_idx, date_to_process in enumerate(dates_to_process, 1):
    
    print("\n" + "="*80)
    print(f"📅 PROCESSING DATE {date_idx}/{total_dates}: {date_to_process}")
    print("="*80 + "\n")
    
    processed_count = 0
    skipped_count = 0
    
    # ===========================================
    # 🔁 Loop through ticker tables
    # ===========================================
    for table_name in ticker_tables:
        try:
            ticker = table_name.replace("TBL_", "")
            print(f"  {ticker:15s}...", end=" ")

            query = f'SELECT * FROM public."{table_name}" WHERE "BizDt" = :dt'
            df = pd.read_sql(text(query), conn, params={"dt": date_to_process})

            if df.empty:
                print("⚠️ No data")
                skipped_count += 1
                continue

            # ===========================================
            # 🛡 Safe conversions
            # ===========================================
            numeric_cols = [
                "UndrlygPric", "StrkPric", "OpnIntrst", "ChngInOpnIntrst",
                "PrvsClsgPric", "LastPric"
            ]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            if "FininstrmActlXpryDt" in df.columns:
                df["FininstrmActlXpryDt"] = pd.to_datetime(df["FininstrmActlXpryDt"], errors="coerce")
            if "BizDt" in df.columns:
                df["BizDt"] = pd.to_datetime(df["BizDt"], errors="coerce")

            # ===========================================
            # 📈 Derived Columns
            # ===========================================
            df["strike_diff"] = df["UndrlygPric"] - df["StrkPric"]
            df["y_oi"] = df["OpnIntrst"] - df["ChngInOpnIntrst"]

            df["chg_oi"] = round(
                100 * ((df["y_oi"] - df["OpnIntrst"]) /
                       ((df["OpnIntrst"] + df["y_oi"]) / 2)), 2
            )

            df["chg_price"] = round(
                100 * ((df["PrvsClsgPric"] - df["LastPric"]) /
                       ((df["PrvsClsgPric"] + df["LastPric"]) / 2)), 2
            )

            # ===========================================
            # 📊 Greeks with Safe Handling
            # ===========================================
            def safe_greeks(row):
                try:
                    if "O" in str(row.get("FinInstrmTp", "")) and pd.notna(row["LastPric"]) and pd.notna(row["StrkPric"]) and row["LastPric"] > 0:
                        g = greeks(
                            premium=float(row["LastPric"]),
                            expiry=row.get("FininstrmActlXpryDt"),
                            cd=row.get("BizDt"),
                            asset_price=row.get("UndrlygPric", np.nan),
                            strike_price=row.get("StrkPric", np.nan),
                            intrest_rate=0.06,
                            instrument_type=str(row.get("OptnTp", "")).lower()
                        )
                        return pd.Series([g["Delta"], g["Vega"], g["Theta"], g["IV"]])
                    else:
                        return pd.Series([0, 0, 0, 0])
                except Exception:
                    return pd.Series([0, 0, 0, 0])

            df[["delta", "vega", "theta", "iv"]] = df.apply(safe_greeks, axis=1)

            # ===========================================
            # 💾 Save to Derived Table
            # ===========================================
            derived_table = f"{table_name}_DERIVED"

            # Create derived table if it doesn't exist
            if not engine.dialect.has_table(engine.connect(), derived_table, schema="public"):
                create_query = f"""
                CREATE TABLE public."{derived_table}" (
                    "BizDt" DATE,
                    "Sgmt" VARCHAR(50),
                    "FinInstrmTp" VARCHAR(50),
                    "TckrSymb" VARCHAR(50),
                    "FininstrmActlXpryDt" DATE,
                    "StrkPric" NUMERIC,
                    "OptnTp" VARCHAR(50),
                    "FinInstrmNm" VARCHAR(50),
                    "OpnPric" NUMERIC,
                    "HghPric" NUMERIC,
                    "LwPric" NUMERIC,
                    "ClsPric" NUMERIC,
                    "LastPric" NUMERIC,
                    "PrvsClsgPric" NUMERIC,
                    "UndrlygPric" NUMERIC,
                    "SttlmPric" NUMERIC,
                    "OpnIntrst" NUMERIC,
                    "ChngInOpnIntrst" NUMERIC,
                    "TtlTradgVol" NUMERIC,
                    "TtlTrfVal" NUMERIC,
                    "TtlNbOfTxsExctd" NUMERIC,
                    "NewBrdLotQty" NUMERIC,
                    "strike_diff" NUMERIC,
                    "y_oi" NUMERIC,
                    "chg_oi" NUMERIC,
                    "chg_price" NUMERIC,
                    "delta" NUMERIC,
                    "vega" NUMERIC,
                    "theta" NUMERIC,
                    "iv" NUMERIC
                );
                """
                with engine.begin() as conn1:
                    conn1.execute(text(create_query))

            # ✅ Append new rows only (don't delete old ones)
            df.to_sql(derived_table, con=engine, if_exists="append", index=False)

            processed_count += 1
            print("✅")

        except Exception as e:
            print(f"❌ Error: {e}")
            skipped_count += 1

    # ===========================================
    # ✅ Date Summary
    # ===========================================
    print("\n" + "-"*80)
    print(f"📊 Date {date_to_process} Summary:")
    print(f"   ✔️  Processed: {processed_count} tickers")
    print(f"   ⏭️  Skipped: {skipped_count} tickers")
    print("-"*80)

# ===========================================
# ✅ Final Summary
# ===========================================
print("\n" + "="*80)
print("✅ ALL DATES PROCESSING COMPLETE!")
print("="*80)
print(f"📅 Processed {total_dates} date(s)")
print(f"📊 All DERIVED tables are now up to date")
print("="*80)
