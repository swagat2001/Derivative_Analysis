"""
MASTER SCRIPT - Complete Cash BhavCopy Data Pipeline
=====================================================
This script performs all steps for Cash market data:
1. Download CSV from NSE (sec_bhavdata_full_*.csv)
2. Upload to PostgreSQL database (CashStocks_Database)
   - Creates SEPARATE TABLE PER SYMBOL (like FO pipeline)
   - e.g., TBL_RELIANCE, TBL_TCS, TBL_INFY, etc.

Similar to fo_update_database.py structure
"""

import os
import re
import socket
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, inspect, text

# ===========================================
# 🔧 Configuration
# ===========================================
db_user = "postgres"
db_password = "Gallop@3104"
db_host = "localhost"
db_port = "5432"
db_name = "CashStocks_Database"  # Cash/Equity database

save_folder = "C:/NSE_EOD_CASH"  # Where CSV files are stored

# Timeout settings for download
DOWNLOAD_TIMEOUT = 30
MAX_RETRIES = 2
RETRY_DELAY = 2


# ===========================================
# 📥 Download Helper Function
# ===========================================
def download_with_timeout(url, filepath, timeout=DOWNLOAD_TIMEOUT):
    """Download file with timeout and headers"""
    socket.setdefaulttimeout(timeout)

    try:
        req = urllib.request.Request(url)
        req.add_header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        req.add_header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")

        with urllib.request.urlopen(req, timeout=timeout) as response:
            with open(filepath, "wb") as out_file:
                out_file.write(response.read())

        return True

    except socket.timeout:
        raise TimeoutError(f"Download timeout after {timeout}s")

    except urllib.error.HTTPError as e:
        if e.code == 404:
            raise FileNotFoundError("File not available (404)")
        else:
            raise Exception(f"HTTP Error {e.code}")

    except Exception as e:
        raise Exception(f"Download failed: {str(e)[:50]}")


# ===========================================
# 📥 STEP 1: Download CSV Data
# ===========================================
def download_csv_data():
    print("\n" + "=" * 80)
    print("STEP 1: DOWNLOADING CASH BHAVCOPY DATA FROM NSE")
    print("=" * 80 + "\n")

    # Connect to database to get latest date
    db_password_enc = quote_plus(db_password)
    engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}")

    # Check existing tables to find latest date
    latest_db_date = None

    try:
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()
        base_tables = [t for t in existing_tables if t.startswith("TBL_")]

        if base_tables:
            # Try to find latest date from a sample table
            sample_table = base_tables[0]
            query = text(f'SELECT MAX("BizDt") AS last_dt FROM public."{sample_table}"')
            with engine.connect() as conn:
                result = conn.execute(query).scalar()

            if result:
                if isinstance(result, str):
                    latest_db_date = datetime.strptime(result, "%Y-%m-%d").date()
                else:
                    latest_db_date = result
    except Exception as e:
        print(f"⚠️ Could not check database: {e}")

    # Decide start date
    if latest_db_date:
        print(f"📌 Latest date in database: {latest_db_date}")
        start_date = latest_db_date + timedelta(days=1)
    else:
        print("⚠ No data in database. Enter start date for initial download.")
        start_date_str = input("Start date (DD-MM-YYYY) [e.g., 01-01-2024]: ").strip()

        if not start_date_str:
            start_date = datetime.now().date() - timedelta(days=30)
        else:
            try:
                start_date = datetime.strptime(start_date_str, "%d-%m-%Y").date()
            except:
                print("⚠️ Invalid date format. Using 30 days ago.")
                start_date = datetime.now().date() - timedelta(days=30)

    today = datetime.now().date()
    end_date = today

    if start_date > end_date:
        print("✅ Database is already up to date. No download needed.")
        return True

    # Create save folder
    os.makedirs(save_folder, exist_ok=True)

    # Generate weekday dates
    date_range = []
    delta_days = (end_date - start_date).days

    for i in range(delta_days + 1):
        d = start_date + timedelta(days=i)
        date_range.append(d)

    if not date_range:
        print("✅ No new weekday dates to download.")
        return True

    print(f"📅 Downloading data for {len(date_range)} date(s)")
    print(f"⏱️  Timeout: {DOWNLOAD_TIMEOUT}s per file | Retries: {MAX_RETRIES}\n")
    print("=" * 80)

    downloaded_files = []
    skipped_dates = []
    failed_dates = []

    start_time = time.time()

    for idx, date_obj in enumerate(date_range, 1):
        date_display = date_obj.strftime("%d-%m-%Y")
        date_str_ddmmyyyy = date_obj.strftime("%d%m%Y")

        progress = f"[{idx}/{len(date_range)}]"
        print(f"{progress:12} {date_display}...", end=" ", flush=True)

        # Build URL - NSE format: sec_bhavdata_full_DDMMYYYY.csv
        url = f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date_str_ddmmyyyy}.csv"

        filename = f"sec_bhavdata_full_{date_str_ddmmyyyy}.csv"
        filepath = os.path.join(save_folder, filename)

        # Check if already downloaded
        if os.path.exists(filepath):
            try:
                df = pd.read_csv(filepath)
                row_count = len(df)
                file_size = os.path.getsize(filepath)
                size_str = f"{file_size/1024:.1f} KB" if file_size < 1024 * 1024 else f"{file_size/(1024*1024):.2f} MB"

                print(f"⏭️  Already exists ({row_count} stocks, {size_str})")
                downloaded_files.append(
                    {
                        "date": date_display,
                        "filename": filename,
                        "filepath": filepath,
                        "rows": row_count,
                        "status": "existing",
                    }
                )
                continue
            except:
                os.remove(filepath)

        # Try download with retries
        success = False
        last_error = None

        for attempt in range(MAX_RETRIES + 1):
            try:
                if attempt > 0:
                    print(f"\n{'':12}          Retry {attempt}/{MAX_RETRIES}...", end=" ", flush=True)
                    time.sleep(RETRY_DELAY)

                download_with_timeout(url, filepath, timeout=DOWNLOAD_TIMEOUT)

                # Verify file
                df = pd.read_csv(filepath)
                row_count = len(df)
                file_size = os.path.getsize(filepath)
                size_str = f"{file_size/1024:.1f} KB" if file_size < 1024 * 1024 else f"{file_size/(1024*1024):.2f} MB"

                print(f"✅ ({row_count} stocks, {size_str})")

                downloaded_files.append(
                    {
                        "date": date_display,
                        "filename": filename,
                        "filepath": filepath,
                        "rows": row_count,
                        "status": "downloaded",
                    }
                )

                success = True
                break

            except FileNotFoundError:
                print(f"⚠️  Not available (holiday)")
                skipped_dates.append(date_display)
                success = True
                break

            except TimeoutError:
                last_error = "Timeout"
                if attempt == MAX_RETRIES:
                    print(f"❌ Timeout after {MAX_RETRIES} retries")

            except Exception as e:
                last_error = str(e)[:30]
                if attempt == MAX_RETRIES:
                    print(f"❌ {last_error}")

        if not success:
            failed_dates.append((date_display, last_error))

    total_time = time.time() - start_time

    print("\n" + "=" * 80)
    print("DOWNLOAD SUMMARY")
    print("=" * 80)
    print(f"\n⏱️  Total time: {int(total_time)}s")
    print(f"✅ Successfully downloaded: {len([f for f in downloaded_files if f['status']=='downloaded'])} new file(s)")
    print(f"⏭️  Already existed: {len([f for f in downloaded_files if f['status']=='existing'])} file(s)")
    print(f"⚠️  Skipped (holidays): {len(skipped_dates)} date(s)")
    print(f"❌ Failed: {len(failed_dates)} date(s)")
    print(f"\n📁 Location: {save_folder}")

    return len(downloaded_files) > 0 or len(skipped_dates) > 0


# ===========================================
# 📤 STEP 2: Upload to Database (Per-Symbol Tables)
# ===========================================
def upload_to_database():
    print("\n" + "=" * 80)
    print("STEP 2: UPLOADING CASH DATA TO DATABASE (Per-Symbol Tables)")
    print("=" * 80 + "\n")

    db_password_enc = quote_plus(db_password)
    engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}")

    # Helper: sanitize table names (like FO pipeline)
    def sanitize_table_name(name):
        clean = re.sub(r"\W+", "_", str(name)).strip("_").upper()
        return f"TBL_{clean}" if clean else "TBL_UNKNOWN"

    # Get existing tables
    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())

    # Find latest BizDt from database (ONLY from per-symbol tables, not old TBL_BHAVCOPY_CASH)
    latest_db_date = None
    # Exclude old combined tables - only check per-symbol tables
    excluded_tables = {"TBL_BHAVCOPY_CASH", "TBL_BHAVCOPY", "TBL_UNKNOWN"}
    base_tables = [t for t in existing_tables if t.startswith("TBL_") and t not in excluded_tables]

    if base_tables:
        # Sample from a few tables to find the latest date
        for sample_table in list(base_tables)[:5]:
            try:
                q = text(f'SELECT MAX("BizDt") FROM public."{sample_table}"')
                with engine.connect() as conn:
                    result = conn.execute(q).scalar()
                if result and (latest_db_date is None or result > latest_db_date):
                    latest_db_date = result
            except:
                pass

    if latest_db_date:
        print(f"📌 Latest DB date: {latest_db_date}")
    else:
        print("📌 Database empty (no per-symbol tables). Will upload ALL CSVs.")

    # Get list of CSV files
    csv_files = [f for f in os.listdir(save_folder) if f.startswith("sec_bhavdata_full_") and f.endswith(".csv")]

    if not csv_files:
        print("⚠ No CSV files found in save folder.")
        return False

    csv_files.sort()
    upload_count = 0
    tables_created = 0

    for file_name in csv_files:
        file_path = os.path.join(save_folder, file_name)

        print(f"\n📄 Reading file: {file_name}")

        try:
            df = pd.read_csv(file_path)

            if df.empty:
                print("   ⚠️ Empty file, skipping")
                continue

            # Standardize column names
            df.columns = df.columns.str.strip()

            # Extract date from FILENAME (same approach as cash_calcu.py)
            # Filename format: sec_bhavdata_full_DDMMYYYY.csv
            try:
                date_str = file_name.replace("sec_bhavdata_full_", "").replace(".csv", "")
                file_date = datetime.strptime(date_str, "%d%m%Y").date()
                df["BizDt"] = file_date
                print(f"   📅 Date from filename: {file_date}")
            except Exception as e:
                print(f"   ⚠️ Could not parse date from filename: {e}, skipping")
                continue

            # Get unique BizDt values in this CSV
            csv_unique_dates = set(df["BizDt"].dropna().astype(str))

            if not csv_unique_dates:
                print("   ⚠️ No valid dates found, skipping")
                continue

            print(f"   📊 Found {len(csv_unique_dates)} unique dates in CSV")

            # Filter dates that are newer than DB (ONLY if DB has data)
            if latest_db_date:
                original_count = len(csv_unique_dates)
                csv_unique_dates = {d for d in csv_unique_dates if d > str(latest_db_date)}
                print(f"   🔍 Filtered to {len(csv_unique_dates)} new dates (was {original_count})")

            if not csv_unique_dates:
                print("   ⏭ No new BizDt dates. Skipping.")
                continue

            print(f"   ➕ Uploading date: {sorted(csv_unique_dates)[0]}")

            # Replace NaN with None for database
            df = df.where(pd.notnull(df), None)

            # Get unique symbols
            if "SYMBOL" not in df.columns:
                print("   ⚠️ SYMBOL column not found, skipping")
                continue

            unique_symbols = df["SYMBOL"].dropna().unique()
            print(f"   📊 Found {len(unique_symbols)} unique symbols")

            # Create tables for each symbol and upload data
            for symbol in unique_symbols:
                table_name = sanitize_table_name(symbol)
                symbol_df = df[df["SYMBOL"] == symbol].copy()

                if symbol_df.empty:
                    continue

                # Create table if not exists
                if table_name not in existing_tables:
                    create_sql = f"""
                    CREATE TABLE IF NOT EXISTS public."{table_name}" (
                        "BizDt" DATE,
                        "TckrSymb" VARCHAR(50),
                        "SERIES" VARCHAR(20),
                        "OpnPric" NUMERIC,
                        "HghPric" NUMERIC,
                        "LwPric" NUMERIC,
                        "ClsPric" NUMERIC,
                        "LastPric" NUMERIC,
                        "PrvsClsgPric" NUMERIC,
                        "TtlTradgVol" BIGINT,
                        "TtlTrfVal" NUMERIC,
                        "TtlNbOfTxsExctd" INTEGER,
                        "DlvryQty" BIGINT,
                        "DlvryPer" NUMERIC
                    );
                    """
                    with engine.begin() as conn:
                        conn.execute(text(create_sql))
                    existing_tables.add(table_name)
                    tables_created += 1

                # Prepare data for upload (matching FO column structure)
                upload_df = pd.DataFrame()
                upload_df["BizDt"] = symbol_df["BizDt"]
                upload_df["TckrSymb"] = symbol_df["SYMBOL"]
                upload_df["SERIES"] = symbol_df.get("SERIES", "EQ")
                upload_df["OpnPric"] = pd.to_numeric(symbol_df.get("OPEN_PRICE", 0), errors="coerce")
                upload_df["HghPric"] = pd.to_numeric(symbol_df.get("HIGH_PRICE", 0), errors="coerce")
                upload_df["LwPric"] = pd.to_numeric(symbol_df.get("LOW_PRICE", 0), errors="coerce")
                upload_df["ClsPric"] = pd.to_numeric(symbol_df.get("CLOSE_PRICE", 0), errors="coerce")
                upload_df["LastPric"] = pd.to_numeric(
                    symbol_df.get("LAST_PRICE", symbol_df.get("CLOSE_PRICE", 0)), errors="coerce"
                )
                upload_df["PrvsClsgPric"] = pd.to_numeric(symbol_df.get("PREV_CLOSE", 0), errors="coerce")
                upload_df["TtlTradgVol"] = (
                    pd.to_numeric(symbol_df.get("TTL_TRD_QNTY", 0), errors="coerce").fillna(0).astype(int)
                )
                upload_df["TtlTrfVal"] = pd.to_numeric(
                    symbol_df.get("TURNOVER_LACS", symbol_df.get("TTL_TRD_VAL", 0)), errors="coerce"
                )
                upload_df["TtlNbOfTxsExctd"] = (
                    pd.to_numeric(symbol_df.get("NO_OF_TRADES", 0), errors="coerce").fillna(0).astype(int)
                )
                upload_df["DlvryQty"] = (
                    pd.to_numeric(symbol_df.get("DELIV_QTY", 0), errors="coerce").fillna(0).astype(int)
                )
                upload_df["DlvryPer"] = pd.to_numeric(symbol_df.get("DELIV_PER", 0), errors="coerce")

                # Filter to only new dates
                if latest_db_date:
                    upload_df = upload_df[upload_df["BizDt"].astype(str) > str(latest_db_date)]

                if not upload_df.empty:
                    upload_df.to_sql(table_name, con=engine, if_exists="append", index=False)

            upload_count += 1
            print(f"   ✅ Processed ({len(unique_symbols)} symbols)")

        except Exception as e:
            print(f"   ❌ Error: {e}")
            import traceback

            traceback.print_exc()

    print(f"\n✅ Upload complete!")
    print(f"   Files processed: {upload_count}")
    print(f"   Tables created: {tables_created}")
    print(f"   Total tables: {len([t for t in existing_tables if t.startswith('TBL_')])}")
    return True


# ===========================================
# 🚀 MAIN EXECUTION
# ===========================================
def main():
    print("\n" + "=" * 80)
    print("             🚀 CASH BHAVCOPY COMPLETE DATA PIPELINE")
    print("=" * 80)
    print("\nThis script will:")
    print("  1. Download latest Cash Bhavcopy CSV data from NSE")
    print("  2. Upload data to PostgreSQL (CashStocks_Database)")
    print("     → Creates SEPARATE TABLE per symbol (TBL_RELIANCE, TBL_TCS, etc.)")
    print("\n" + "=" * 80)

    try:
        # Step 1: Download CSV
        download_result = download_csv_data()
        if download_result is False:
            print("\n❌ CSV download failed!")
            return False

        # Step 2: Upload to database
        upload_result = upload_to_database()
        if upload_result is False:
            print("\n❌ Database upload failed!")
            return False

        print("\n" + "=" * 80)
        print("             ✅ PIPELINE COMPLETE!")
        print("=" * 80)
        print("\n✓ All steps completed successfully")
        print(f"\nYour CashStocks_Database is now updated!")
        print(f"Tables: TBL_RELIANCE, TBL_TCS, TBL_INFY, etc.")
        print("=" * 80 + "\n")

        return True

    except Exception as e:
        print(f"\n❌ Fatal Error: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    input("\nPress Enter to exit...")
    sys.exit(0 if success else 1)
