"""
MASTER SCRIPT - Complete BhavCopy Data Pipeline
This script performs all 3 steps:
1. Download CSV from NSE
2. Upload to PostgreSQL database
3. Calculate Greeks and create DERIVED tables
"""

import os
import sys
import urllib.request
import zipfile
import socket
import shutil
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, inspect, text
from urllib.parse import quote_plus
import numpy as np
from py_vollib.black_scholes.implied_volatility import implied_volatility
from py_vollib.black_scholes.greeks.analytical import delta, gamma, rho, theta, vega
import re

# ===========================================
# 🔧 Configuration
# ===========================================
db_user = 'postgres'
db_password = 'Gallop@3104'
db_host = 'localhost'
db_port = '5432'
db_name = 'BhavCopy_Database'

output_folder = "C:/data_fo"
save_fo_eod = "C:/NSE_EOD_FO"

# ===========================================
# 📥 STEP 1: Download CSV Data
# ===========================================
def download_csv_data():
    print("\n" + "="*80)
    print("STEP 1: DOWNLOADING CSV DATA FROM NSE")
    print("="*80 + "\n")
    
    # Setup folders
    if os.path.exists(output_folder):
        shutil.rmtree(output_folder)
    os.makedirs(output_folder)
    
    if not os.path.exists(save_fo_eod):
        os.makedirs(save_fo_eod)
    
    # Determine date range
    if os.path.isdir(save_fo_eod) and os.listdir(save_fo_eod):
        file_list = [f for f in os.listdir(save_fo_eod) if os.path.isfile(os.path.join(save_fo_eod, f))]
        last_date_str = max(f[:10] for f in file_list)
        last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
        today = datetime.now().date()
        
        if last_date == today:
            print(f"✅ Database is up to date: {last_date}")
            return True
        
        start_date = last_date + timedelta(days=1)
        end_date = today
    else:
        print("No existing data found. Starting fresh download.")
        start_date = datetime.strptime(input('Enter start date (YYYY-MM-DD): '), "%Y-%m-%d").date()
        end_date = datetime.strptime(input('Enter end date (YYYY-MM-DD): '), "%Y-%m-%d").date()
    
    # Generate date list (weekdays only)
    date_range = []
    delta = end_date - start_date
    for i in range(delta.days + 1):
        day = start_date + timedelta(days=i)
        if day.weekday() < 5:
            date_range.append(day)
    
    if not date_range:
        print("✅ No new dates to download")
        return True
    
    print(f"📅 Downloading data for {len(date_range)} date(s)")
    
    # Download files
    socket.setdefaulttimeout(1)
    downloaded_files = []
    
    for date in date_range:
        date_str = date.strftime("%Y%m%d")
        filename = f"BhavCopy_NSE_FO_0_0_0_{date_str}_F_0000.csv.zip"
        url = f"https://archives.nseindia.com/content/fo/{filename}"
        
        try:
            output_path = os.path.join(output_folder, filename)
            urllib.request.urlretrieve(url, output_path)
            downloaded_files.append(output_path)
            print(f"✅ Downloaded: {date.strftime('%Y-%m-%d')}")
        except Exception as e:
            print(f"⚠️  Skipped {date.strftime('%Y-%m-%d')} (holiday or unavailable)")
    
    if not downloaded_files:
        print("✅ No new data available")
        return True
    
    # Extract files
    for file in downloaded_files:
        try:
            with zipfile.ZipFile(file, 'r') as zip_ref:
                zip_ref.extractall(output_folder)
            os.remove(file)
        except Exception as e:
            print(f"❌ Error extracting {file}: {e}")
    
    # Rename and format files
    files = os.listdir(output_folder)
    for file in files:
        if file.endswith("0000.csv"):
            date = file[22:30]
            date_obj = datetime.strptime(date, "%Y%m%d")
            new_name = date_obj.strftime("%Y-%m-%d") + "-NSE-FO.csv"
            old_path = os.path.join(output_folder, file)
            new_path = os.path.join(output_folder, new_name)
            os.rename(old_path, new_path)
            
            # Format dates in CSV
            try:
                df = pd.read_csv(new_path)
                for col in df.columns:
                    if df[col].dtype == 'object':
                        try:
                            df[col] = pd.to_datetime(df[col], format='mixed', errors='raise')
                            df[col] = df[col].dt.strftime('%Y-%m-%d')
                        except:
                            continue
                df.to_csv(new_path, index=False)
            except Exception as e:
                print(f"⚠️  Error formatting {new_name}: {e}")
    
    # Move to final folder
    for filename in os.listdir(output_folder):
        if filename.endswith("-NSE-FO.csv"):
            shutil.copy2(os.path.join(output_folder, filename), os.path.join(save_fo_eod, filename))
    
    # Cleanup
    shutil.rmtree(output_folder)
    
    print(f"\n✅ CSV download complete! Files saved to: {save_fo_eod}")
    return True

# ===========================================
# 📤 STEP 2: Upload to Database
# ===========================================
def upload_to_database():
    print("\n" + "="*80)
    print("STEP 2: UPLOADING CSV DATA TO DATABASE")
    print("="*80 + "\n")
    
    db_password_enc = quote_plus(db_password)
    engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}')
    
    table_schema_template = """
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        "BizDt" DATE,
        "Sgmt" VARCHAR(50),
        "FinInstrmTp" VARCHAR(50),
        "TckrSymb" VARCHAR(50),
        "FininstrmActlXpryDt" DATE,
        "StrkPric" VARCHAR(50),
        "OptnTp" VARCHAR(50),
        "FinInstrmNm" VARCHAR(50),
        "OpnPric" VARCHAR(50),
        "HghPric" VARCHAR(50),
        "LwPric" VARCHAR(50),
        "ClsPric" VARCHAR(50),
        "LastPric" VARCHAR(50),
        "PrvsClsgPric" VARCHAR(50),
        "UndrlygPric" VARCHAR(50),
        "SttlmPric" VARCHAR(50),
        "OpnIntrst" VARCHAR(50),
        "ChngInOpnIntrst" VARCHAR(50),
        "TtlTradgVol" VARCHAR(50),
        "TtlTrfVal" VARCHAR(50),
        "TtlNbOfTxsExctd" VARCHAR(50),
        "NewBrdLotQty" VARCHAR(50)
    );
    """
    
    expected_columns = [
        'BizDt','Sgmt','FinInstrmTp','TckrSymb','FininstrmActlXpryDt','StrkPric','OptnTp',
        'FinInstrmNm','OpnPric','HghPric','LwPric','ClsPric','LastPric','PrvsClsgPric',
        'UndrlygPric','SttlmPric','OpnIntrst','ChngInOpnIntrst','TtlTradgVol','TtlTrfVal','TtlNbOfTxsExctd','NewBrdLotQty'
    ]
    
    def sanitize_table_name(name):
        clean = re.sub(r'\W+', '_', name).strip('_').upper()
        return f"TBL_{clean}" if clean else "TBL_UNKNOWN"
    
    # Get existing dates in DB
    inspector = inspect(engine)
    base_tables = [t for t in inspector.get_table_names() if t.startswith("TBL_") and not t.endswith("_DERIVED")]
    existing_dates = set()
    
    if base_tables:
        sample_table = next((t for t in ['TBL_NIFTY', 'TBL_BANKNIFTY'] if t in base_tables), base_tables[0])
        try:
            query = text(f'SELECT DISTINCT "BizDt" FROM "{sample_table}"')
            result = pd.read_sql(query, engine)
            existing_dates = set(result['BizDt'].astype(str))
        except:
            pass
    
    # Get CSV files
    csv_files = [f for f in os.listdir(save_fo_eod) if f.endswith('.csv')]
    
    if not csv_files:
        print("⚠️  No CSV files found")
        return False
    
    # Filter only new files
    new_files = []
    for f in csv_files:
        file_date = f[:10]
        if file_date not in existing_dates:
            new_files.append(f)
    
    if not new_files:
        print("✅ No new data to upload")
        return True
    
    print(f"📂 Found {len(new_files)} NEW CSV file(s) to upload\n")
    
    for file_name in new_files:
        file_path = os.path.join(save_fo_eod, file_name)
        print(f"📄 Processing: {file_name}")
        
        try:
            df = pd.read_csv(file_path)
            
            if "TckrSymb" not in df.columns:
                print(f"⚠️  Skipping (missing TckrSymb)")
                continue
            
            df['BizDt'] = pd.to_datetime(df['BizDt'], errors='coerce').dt.date
            df['FininstrmActlXpryDt'] = pd.to_datetime(df['FininstrmActlXpryDt'], errors='coerce').dt.date
            df = df.where(pd.notnull(df), None)
            
            unique_symbols = df["TckrSymb"].dropna().unique()
            
            # Create tables
            with engine.begin() as conn:
                for symbol in unique_symbols:
                    table_name = sanitize_table_name(symbol)
                    sql = table_schema_template.format(table_name=table_name)
                    conn.execute(text(sql))
            
            # Insert data
            batch_size = 100
            for symbol in unique_symbols:
                table_name = sanitize_table_name(symbol)
                df_symbol = df[df["TckrSymb"] == symbol]
                df_symbol = df_symbol[[col for col in expected_columns if col in df_symbol.columns]]
                
                rows_to_insert = df_symbol.to_dict(orient='records')
                if not rows_to_insert:
                    continue
                
                columns = df_symbol.columns.tolist()
                cols_str = ', '.join(f'"{col}"' for col in columns)
                values_str = ', '.join(f":{col}" for col in columns)
                insert_sql = f'INSERT INTO "{table_name}" ({cols_str}) VALUES ({values_str})'
                
                total_batches = (len(rows_to_insert) + batch_size - 1) // batch_size
                for i in range(total_batches):
                    batch = rows_to_insert[i * batch_size:(i + 1) * batch_size]
                    with engine.begin() as conn:
                        conn.execute(text(insert_sql), batch)
            
            print(f"   ✅ Uploaded successfully")
        
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    print(f"\n✅ Database upload complete!")
    return True

# ===========================================
# 🧮 STEP 3: Calculate Greeks
# ===========================================
def calculate_greeks():
    print("\n" + "="*80)
    print("STEP 3: CALCULATING GREEKS AND CREATING DERIVED TABLES")
    print("="*80 + "\n")
    
    db_password_enc = quote_plus(db_password)
    engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}')
    conn = engine.connect()
    
    def get_dates_to_process():
        try:
            inspector = inspect(engine)
            base_tables = [t for t in inspector.get_table_names() if t.startswith("TBL_") and not t.endswith("_DERIVED")]
            
            if not base_tables:
                return []
            
            sample_table = next((t for t in ['TBL_NIFTY', 'TBL_BANKNIFTY'] if t in base_tables), base_tables[0])
            
            query_base = text(f'SELECT DISTINCT "BizDt" FROM public."{sample_table}" WHERE "BizDt" IS NOT NULL ORDER BY "BizDt" ASC')
            base_dates = pd.read_sql(query_base, engine)
            
            if base_dates.empty:
                return []
            
            derived_table = f"{sample_table}_DERIVED"
            
            if derived_table in inspector.get_table_names():
                query_derived = text(f'SELECT DISTINCT "BizDt" FROM public."{derived_table}" WHERE "BizDt" IS NOT NULL ORDER BY "BizDt" ASC')
                derived_dates = pd.read_sql(query_derived, engine)
                derived_dates_set = set(derived_dates['BizDt'].astype(str))
            else:
                derived_dates_set = set()
            
            base_dates_set = set(base_dates['BizDt'].astype(str))
            dates_to_process = sorted(list(base_dates_set - derived_dates_set))
            
            return dates_to_process
        except Exception as e:
            print(f"❌ Error detecting dates: {e}")
            return []
    
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
        except:
            return {"IV": 0, "Delta": 0, "Gamma": 0, "Rho": 0, "Theta": 0, "Vega": 0}
    
    dates_to_process = get_dates_to_process()
    
    if not dates_to_process:
        print("✅ All dates already processed!")
        return True
    
    print(f"📅 Found {len(dates_to_process)} date(s) to process\n")
    
    inspector = inspect(engine)
    ticker_tables = [t for t in inspector.get_table_names() if t.startswith("TBL_") and not t.endswith("_DERIVED")]
    
    for date_idx, date_to_process in enumerate(dates_to_process, 1):
        print(f"\n📅 Processing date {date_idx}/{len(dates_to_process)}: {date_to_process}")
        print("-"*80)
        
        processed = 0
        
        for table_name in ticker_tables:
            try:
                ticker = table_name.replace("TBL_", "")
                print(f"  {ticker:15s}...", end=" ")
                
                query = f'SELECT * FROM public."{table_name}" WHERE "BizDt" = :dt'
                df = pd.read_sql(text(query), conn, params={"dt": date_to_process})
                
                if df.empty:
                    print("⚠️")
                    continue
                
                numeric_cols = ["UndrlygPric", "StrkPric", "OpnIntrst", "ChngInOpnIntrst", "PrvsClsgPric", "LastPric"]
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                
                if "FininstrmActlXpryDt" in df.columns:
                    df["FininstrmActlXpryDt"] = pd.to_datetime(df["FininstrmActlXpryDt"], errors="coerce")
                if "BizDt" in df.columns:
                    df["BizDt"] = pd.to_datetime(df["BizDt"], errors="coerce")
                
                df["strike_diff"] = df["UndrlygPric"] - df["StrkPric"]
                df["y_oi"] = df["OpnIntrst"] - df["ChngInOpnIntrst"]
                df["chg_oi"] = round(100 * ((df["y_oi"] - df["OpnIntrst"]) / ((df["OpnIntrst"] + df["y_oi"]) / 2)), 2)
                df["chg_price"] = round(100 * ((df["PrvsClsgPric"] - df["LastPric"]) / ((df["PrvsClsgPric"] + df["LastPric"]) / 2)), 2)
                
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
                    except:
                        return pd.Series([0, 0, 0, 0])
                
                df[["delta", "vega", "theta", "iv"]] = df.apply(safe_greeks, axis=1)
                
                derived_table = f"{table_name}_DERIVED"
                
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
                
                df.to_sql(derived_table, con=engine, if_exists="append", index=False)
                processed += 1
                print("✅")
            
            except Exception as e:
                print(f"❌")
        
        print(f"\n  📊 Date summary: {processed} tickers processed")
    
    print(f"\n✅ Greeks calculation complete!")
    return True

# ===========================================
# 🚀 MAIN EXECUTION
# ===========================================
def main():
    print("\n" + "="*80)
    print("             🚀 BHAVCOPY COMPLETE DATA PIPELINE")
    print("="*80)
    print("\nThis script will:")
    print("  1. Download latest CSV data from NSE")
    print("  2. Upload data to PostgreSQL database")
    print("  3. Calculate Greeks and create DERIVED tables")
    print("  4. Pre-calculate dashboard data for fast loading")
    print("\n" + "="*80)
    
    try:
        # Step 1: Download CSV
        if not download_csv_data():
            print("\n❌ CSV download failed!")
            return False
        
        # Step 2: Upload to database
        if not upload_to_database():
            print("\n❌ Database upload failed!")
            return False
        
        # Step 3: Calculate Greeks
        if not calculate_greeks():
            print("\n❌ Greeks calculation failed!")
            return False
        
        # Step 4: Pre-calculate dashboard data
        print("\n" + "="*80)
        print("STEP 4: PRE-CALCULATING DASHBOARD DATA")
        print("="*80 + "\n")
        
        import sys
        import os
        analysis_tools_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Analysis_Tools')
        sys.path.insert(0, analysis_tools_path)
        
        from precalculate_data import create_precalculated_tables, precalculate_all_dates
        
        create_precalculated_tables()
        precalculate_all_dates()
        
        print("\n" + "="*80)
        print("             ✅ PIPELINE COMPLETE!")
        print("="*80)
        print("\n✓ CSV data downloaded")
        print("✓ Data uploaded to database")
        print("✓ Greeks calculated")
        print("✓ Dashboard data pre-calculated")
        print("\nYour database is now up to date!")
        print("Run dashboard_server.py to view the dashboard")
        print("="*80 + "\n")
        
        return True
    
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = main()
    input("\nPress Enter to exit...")
    sys.exit(0 if success else 1)
