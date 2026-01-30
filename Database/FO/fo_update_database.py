"""
MASTER SCRIPT - Complete BhavCopy Data Pipeline
This script performs all steps:
1. Download CSV from NSE
2. Upload to PostgreSQL database
3. Calculate Greeks and create DERIVED tables
4. Pre-calculate screener cache data
5. Pre-calculate dashboard data
6. Fetch index constituents from NSE (for instant web app loading)
"""

import os
import re
import shutil
import socket
import sys
import urllib.request
import zipfile
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
from py_vollib.black_scholes.greeks.analytical import delta, gamma, rho, theta, vega
from py_vollib.black_scholes.implied_volatility import implied_volatility
from sqlalchemy import create_engine, inspect, text

# ===========================================
# 🔧 Configuration
# ===========================================
db_user = "postgres"
db_password = "Gallop@3104"
db_host = "localhost"
db_port = "5432"
db_name = "BhavCopy_Database"

output_folder = "C:/data_fo"
save_fo_eod = "C:/NSE_EOD_FO"


# ===========================================
# 📥 STEP 1: Download CSV Data
# ===========================================
def download_csv_data():
    print("\n" + "=" * 80)
    print("STEP 1: DOWNLOADING CSV DATA FROM NSE")
    print("=" * 80 + "\n")

    # -------------------------------------------
    # 1️⃣  CONNECT TO DATABASE TO GET LATEST DATE
    # -------------------------------------------
    db_password_enc = quote_plus(db_password)
    engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}")

    inspector = inspect(engine)
    base_tables = [t for t in inspector.get_table_names() if t.startswith("TBL_") and not t.endswith("_DERIVED")]

    latest_db_date = None

    if base_tables:
        # Prefer NIFTY/BANKNIFTY if present
        sample_table = next((t for t in ["TBL_NIFTY", "TBL_BANKNIFTY"] if t in base_tables), base_tables[0])

        query = text(f'SELECT MAX("BizDt") AS last_dt FROM public."{sample_table}"')
        with engine.connect() as conn:
            result = conn.execute(query).scalar()

        if result:
            if isinstance(result, str):
                latest_db_date = datetime.strptime(result, "%Y-%m-%d").date()
            else:
                latest_db_date = result

    # -------------------------------------------
    # 2️⃣  DECIDE START DATE
    # -------------------------------------------
    if latest_db_date:
        print(f"📌 Latest date in database: {latest_db_date}")
        start_date = latest_db_date + timedelta(days=1)
    else:
        print("⚠ No data in database. Starting fresh download.")
        start_date = datetime.strptime(input("Enter start date (YYYY-MM-DD): "), "%Y-%m-%d").date()

    today = datetime.now().date()
    end_date = today

    if start_date > end_date:
        print("✅ Database is already up to date. No download needed.")
        return True

    # -------------------------------------------
    # 3️⃣  CREATE TEMP FOLDER FOR DOWNLOAD
    # -------------------------------------------
    if os.path.exists(output_folder):
        shutil.rmtree(output_folder)
    os.makedirs(output_folder)

    if not os.path.exists(save_fo_eod):
        os.makedirs(save_fo_eod)

    # -------------------------------------------
    # 4️⃣  GENERATE WEEKDAY DATES
    # -------------------------------------------
    date_range = []
    delta_days = (end_date - start_date).days

    for i in range(delta_days + 1):
        d = start_date + timedelta(days=i)
        if d.weekday() < 5:
            date_range.append(d)

    if not date_range:
        print("✅ No new weekday dates to download.")
        return True

    print(f"📅 Downloading data for {len(date_range)} date(s)")

    # -------------------------------------------
    # 5️⃣  DOWNLOAD FILES
    # -------------------------------------------
    socket.setdefaulttimeout(1)
    downloaded_files = []

    for d in date_range:
        date_str = d.strftime("%Y%m%d")
        filename = f"BhavCopy_NSE_FO_0_0_0_{date_str}_F_0000.csv.zip"
        url = f"https://archives.nseindia.com/content/fo/{filename}"

        try:
            output_path = os.path.join(output_folder, filename)
            urllib.request.urlretrieve(url, output_path)
            downloaded_files.append(output_path)
            print(f"   ✅ {d} downloaded")
        except:
            print(f"   ⚠️  Skipped {d} (holiday or missing)")

    if not downloaded_files:
        print("⚠️  No files downloaded")
        return True

    # -------------------------------------------
    # 6️⃣  EXTRACT + RENAME
    # -------------------------------------------
    for file in downloaded_files:
        try:
            with zipfile.ZipFile(file, "r") as z:
                z.extractall(output_folder)
            os.remove(file)
        except Exception as e:
            print(f"❌ Error extracting {file}")

    files = os.listdir(output_folder)
    for file in files:
        if file.endswith("0000.csv"):
            date = file[22:30]
            date_obj = datetime.strptime(date, "%Y%m%d")
            new_name = date_obj.strftime("%Y-%m-%d") + "-NSE-FO.csv"
            os.rename(os.path.join(output_folder, file), os.path.join(output_folder, new_name))

    # -------------------------------------------
    # 7️⃣  MOVE TO FINAL FOLDER
    # -------------------------------------------
    for filename in os.listdir(output_folder):
        if filename.endswith("-NSE-FO.csv"):
            shutil.copy2(os.path.join(output_folder, filename), os.path.join(save_fo_eod, filename))

    shutil.rmtree(output_folder)

    print(f"\n✅ CSV download complete! Saved to {save_fo_eod}")
    return True


# ===========================================
# 📤 STEP 2: Upload to Database
# ===========================================
def upload_to_database():
    print("\n" + "=" * 80)
    print("STEP 2: UPLOADING CSV DATA TO DATABASE")
    print("=" * 80 + "\n")

    db_password_enc = quote_plus(db_password)
    engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}")

    # Expected CSV columns
    expected_columns = [
        "BizDt",
        "Sgmt",
        "FinInstrmTp",
        "TckrSymb",
        "FininstrmActlXpryDt",
        "StrkPric",
        "OptnTp",
        "FinInstrmNm",
        "OpnPric",
        "HghPric",
        "LwPric",
        "ClsPric",
        "LastPric",
        "PrvsClsgPric",
        "UndrlygPric",
        "SttlmPric",
        "OpnIntrst",
        "ChngInOpnIntrst",
        "TtlTradgVol",
        "TtlTrfVal",
        "TtlNbOfTxsExctd",
        "NewBrdLotQty",
    ]

    # Helper: sanitize table names
    def sanitize_table_name(name):
        clean = re.sub(r"\W+", "_", name).strip("_").upper()
        return f"TBL_{clean}" if clean else "TBL_UNKNOWN"

    # Get all ticker tables
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()

    # Find latest BizDt from database
    latest_db_date = None
    base_tables = [t for t in existing_tables if t.startswith("TBL_") and not t.endswith("_DERIVED")]

    if base_tables:
        sample_table = next((t for t in ["TBL_NIFTY", "TBL_BANKNIFTY"] if t in base_tables), base_tables[0])
        q = text(f'SELECT MAX("BizDt") FROM public."{sample_table}"')
        with engine.connect() as conn:
            latest_db_date = conn.execute(q).scalar()

    if latest_db_date:
        print(f"📌 Latest DB date: {latest_db_date}")
    else:
        print("📌 Database empty. Will upload all CSVs.")

    # Load all CSV files
    csv_files = [f for f in os.listdir(save_fo_eod) if f.endswith(".csv")]

    if not csv_files:
        print("⚠ No CSV files found.")
        return False

    upload_count = 0

    for file_name in sorted(csv_files):
        file_path = os.path.join(save_fo_eod, file_name)
        print(f"\n📄 Reading file: {file_name}")

        try:
            df = pd.read_csv(file_path)

            if "BizDt" not in df.columns:
                print("   ⚠ Missing BizDt column. Skipping.")
                continue

            df["BizDt"] = pd.to_datetime(df["BizDt"], errors="coerce").dt.date

            csv_unique_dates = set(df["BizDt"].dropna().astype(str))

            # Filter dates that are newer than DB
            if latest_db_date:
                csv_unique_dates = {d for d in csv_unique_dates if d > str(latest_db_date)}

            if not csv_unique_dates:
                print("   ⏭ No new BizDt inside this CSV. Skipping.")
                continue

            print(f"   ➕ Uploading dates: {sorted(csv_unique_dates)}")

            df = df.where(pd.notnull(df), None)

            unique_symbols = df["TckrSymb"].dropna().unique()

            # FIXED: Create tables with proper transaction management
            for symbol in unique_symbols:
                table_name = sanitize_table_name(symbol)
                if table_name not in existing_tables:
                    create_sql = f"""
                    CREATE TABLE IF NOT EXISTS public."{table_name}" (
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
                    # FIXED: Use engine.begin() for proper transaction management
                    with engine.begin() as conn:
                        conn.execute(text(create_sql))
                    existing_tables.append(table_name)

            # FIXED: Refresh inspector after creating new tables
            inspector = inspect(engine)
            existing_tables = inspector.get_table_names()

            # Upload rows for new BizDt only
            for d in csv_unique_dates:
                df_d = df[df["BizDt"] == datetime.strptime(d, "%Y-%m-%d").date()]

                for symbol in df_d["TckrSymb"].dropna().unique():
                    table_name = sanitize_table_name(symbol)
                    df_symbol = df_d[df_d["TckrSymb"] == symbol]

                    cols = [col for col in expected_columns if col in df_symbol.columns]
                    df_symbol = df_symbol[cols]

                    df_symbol.to_sql(table_name, con=engine, if_exists="append", index=False)

            upload_count += 1
            print("   ✅ Uploaded successfully.")

        except Exception as e:
            print(f"   ❌ Error: {e}")

    print(f"\n✅ Upload complete. {upload_count} file(s) uploaded.")
    return True


# ===========================================
# 🧮 STEP 3: Calculate Greeks
# ===========================================
def calculate_greeks():
    print("\n" + "=" * 80)
    print("STEP 3: CALCULATING GREEKS AND CREATING DERIVED TABLES")
    print("=" * 80 + "\n")

    db_password_enc = quote_plus(db_password)
    engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}")

    # ---------------------------------------------------------
    # Get pending BizDt that are in BASE tables but not in DERIVED
    # ---------------------------------------------------------
    def get_dates_to_process():
        try:
            inspector = inspect(engine)
            base_tables = [
                t for t in inspector.get_table_names() if t.startswith("TBL_") and not t.endswith("_DERIVED")
            ]

            if not base_tables:
                return []

            # choose main index table
            sample_table = next((t for t in ["TBL_NIFTY", "TBL_BANKNIFTY"] if t in base_tables), base_tables[0])

            query_base = text(
                f"""
                SELECT DISTINCT "BizDt"
                FROM public."{sample_table}"
                WHERE "BizDt" IS NOT NULL
                ORDER BY "BizDt"
            """
            )

            with engine.connect() as conn:
                base_dates = pd.read_sql(query_base, conn)

            if base_dates.empty:
                return []

            derived_table = f"{sample_table}_DERIVED"

            if derived_table in inspector.get_table_names():
                query_derived = text(
                    f"""
                    SELECT DISTINCT "BizDt"
                    FROM public."{derived_table}"
                    WHERE "BizDt" IS NOT NULL
                    ORDER BY "BizDt"
                """
                )
                with engine.connect() as conn:
                    derived_dates = pd.read_sql(query_derived, conn)
                derived_dates_set = set(derived_dates["BizDt"].astype(str))
            else:
                derived_dates_set = set()

            base_dates_set = set(base_dates["BizDt"].astype(str))
            return sorted(list(base_dates_set - derived_dates_set))

        except Exception as e:
            print(f"❌ Error detecting dates: {e}")
            return []

    # ---------------------------------------------------------
    # Greeks helper
    # ---------------------------------------------------------
    def greeks(premium, expiry, cd, asset_price, strike_price, rate, opt_type):
        try:
            if pd.isna(expiry) or pd.isna(cd):
                return {"IV": 0, "Delta": 0, "Gamma": 0, "Rho": 0, "Theta": 0, "Vega": 0}

            expiry = pd.to_datetime(expiry)
            cd = pd.to_datetime(cd)

            t = (
                (
                    datetime(expiry.year, expiry.month, expiry.day, 15, 30)
                    - datetime(cd.year, cd.month, cd.day, 15, 30)
                ).total_seconds()
            ) / (365 * 24 * 3600)

            if t <= 0:
                return {"IV": 0, "Delta": 0, "Gamma": 0, "Rho": 0, "Theta": 0, "Vega": 0}

            flag = str(opt_type)[0].lower()
            iv = implied_volatility(premium, asset_price, strike_price, t, rate, flag)

            return {
                "IV": iv,
                "Delta": delta(flag, asset_price, strike_price, t, rate, iv),
                "Gamma": gamma(flag, asset_price, strike_price, t, rate, iv),
                "Rho": rho(flag, asset_price, strike_price, t, rate, iv),
                "Theta": theta(flag, asset_price, strike_price, t, rate, iv),
                "Vega": vega(flag, asset_price, strike_price, t, rate, iv),
            }
        except:
            return {"IV": 0, "Delta": 0, "Gamma": 0, "Rho": 0, "Theta": 0, "Vega": 0}

    # ---------------------------------------------------------
    # MAIN PROCESS
    # ---------------------------------------------------------
    dates_to_process = get_dates_to_process()

    if not dates_to_process:
        print("✅ All dates already processed!")
        return True

    print(f"📅 Found {len(dates_to_process)} date(s) to process\n")

    inspector = inspect(engine)
    ticker_tables = [t for t in inspector.get_table_names() if t.startswith("TBL_") and not t.endswith("_DERIVED")]

    with engine.connect() as conn:
        for idx, bizdt in enumerate(dates_to_process, 1):
            print(f"\n📅 Processing date {idx}/{len(dates_to_process)}: {bizdt}")
            print("-" * 80)

            processed = 0

            for table_name in ticker_tables:
                try:
                    ticker = table_name.replace("TBL_", "")
                    print(f"  {ticker:15s}...", end=" ")

                    query = text(
                        f"""
                        SELECT *
                        FROM public."{table_name}"
                        WHERE "BizDt" = :d
                    """
                    )

                    df = pd.read_sql(query, conn, params={"d": bizdt})

                    if df.empty:
                        print("⚠️")
                        continue

                    # Ensure datetime conversion happens in main loop
                    if "FininstrmActlXpryDt" in df.columns:
                        df["FininstrmActlXpryDt"] = pd.to_datetime(df["FininstrmActlXpryDt"], errors="coerce")
                    if "BizDt" in df.columns:
                        df["BizDt"] = pd.to_datetime(df["BizDt"], errors="coerce")

                    # force numeric conversion
                    numeric_cols = [
                        "UndrlygPric",
                        "StrkPric",
                        "OpnIntrst",
                        "ChngInOpnIntrst",
                        "PrvsClsgPric",
                        "LastPric",
                    ]
                    for col in numeric_cols:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors="coerce")

                    # derived metrics
                    df["strike_diff"] = df["UndrlygPric"] - df["StrkPric"]
                    df["y_oi"] = df["OpnIntrst"] - df["ChngInOpnIntrst"]

                    # Division by zero protection
                    df["chg_oi"] = df.apply(
                        lambda r: 0
                        if (r["OpnIntrst"] + r["y_oi"]) == 0
                        else round(
                            100 * ((r["y_oi"] - r["OpnIntrst"]) / ((r["OpnIntrst"] + r["y_oi"]) / 2)),
                            2,
                        ),
                        axis=1,
                    )

                    df["chg_price"] = df.apply(
                        lambda r: 0
                        if (r["PrvsClsgPric"] + r["LastPric"]) == 0
                        else round(
                            100 * ((r["PrvsClsgPric"] - r["LastPric"]) / ((r["PrvsClsgPric"] + r["LastPric"]) / 2)),
                            2,
                        ),
                        axis=1,
                    )

                    # Calculate Greeks - ALL 6 INCLUDING GAMMA AND RHO
                    def safe_greeks(row):
                        try:
                            if (
                                "O" in str(row.get("FinInstrmTp", ""))
                                and pd.notna(row["LastPric"])
                                and pd.notna(row["StrkPric"])
                                and row["LastPric"] > 0
                            ):
                                g = greeks(
                                    premium=float(row["LastPric"]),
                                    expiry=row.get("FininstrmActlXpryDt"),
                                    cd=row.get("BizDt"),
                                    asset_price=row.get("UndrlygPric", np.nan),
                                    strike_price=row.get("StrkPric", np.nan),
                                    rate=0.06,
                                    opt_type=str(row.get("OptnTp", "")).lower(),
                                )
                                return pd.Series(
                                    [
                                        g["Delta"],
                                        g["Gamma"],
                                        g["Vega"],
                                        g["Theta"],
                                        g["Rho"],
                                        g["IV"],
                                    ]
                                )
                            else:
                                return pd.Series([0, 0, 0, 0, 0, 0])
                        except:
                            return pd.Series([0, 0, 0, 0, 0, 0])

                    df[["delta", "gamma", "vega", "theta", "rho", "iv"]] = df.apply(safe_greeks, axis=1)

                    # Write to derived table
                    derived_table = f"{table_name}_DERIVED"

                    if derived_table not in inspector.get_table_names():
                        ddl = f"""
                        CREATE TABLE public."{derived_table}" (
                            LIKE public."{table_name}" INCLUDING ALL,
                            "strike_diff" NUMERIC,
                            "y_oi" NUMERIC,
                            "chg_oi" NUMERIC,
                            "chg_price" NUMERIC,
                            "delta" NUMERIC,
                            "gamma" NUMERIC,
                            "vega" NUMERIC,
                            "theta" NUMERIC,
                            "rho" NUMERIC,
                            "iv" NUMERIC
                        );
                        """
                        with engine.begin() as exe:
                            exe.execute(text(ddl))
                        # Refresh inspector after table creation
                        inspector = inspect(engine)

                    df.to_sql(derived_table, engine, if_exists="append", index=False)
                    processed += 1
                    print("✅")

                except Exception as e:
                    print(f"❌ {str(e)[:50]}")

            print(f"\n  📊 Date summary: {processed} tickers processed")

    print(f"\n✅ Greeks calculation complete!")
    return True


# ===========================================
# 🚀 MAIN EXECUTION
# ===========================================
def main():
    print("\n" + "=" * 80)
    print("             🚀 BHAVCOPY COMPLETE DATA PIPELINE")
    print("=" * 80)
    print("\nThis script will:")
    print("  1. Download latest CSV data from NSE")
    print("  2. Upload data to PostgreSQL database")
    print("  3. Calculate Greeks and create DERIVED tables")
    print("\n" + "=" * 80)

    try:
        # Step 1: Download CSV (continue even if no new data)
        download_result = download_csv_data()
        if download_result is False:  # Only stop on actual error
            print("\n❌ CSV download failed!")
            return False

        # Step 2: Upload to database (continue even if no new data)
        upload_result = upload_to_database()
        if upload_result is False:  # Only stop on actual error
            print("\n❌ Database upload failed!")
            return False

        # Step 3: Calculate Greeks (continue even if already processed)
        greeks_result = calculate_greeks()
        if greeks_result is False:  # Only stop on actual error
            print("\n❌ Greeks calculation failed!")
            return False

        # Step 4: Pre-calculate screener cache data (OPTIONAL)
        try:
            import screener_cache

            precalculate_screener_cache = screener_cache.precalculate_screener_cache
            print("\n" + "=" * 80)
            print("STEP 4: PRE-CALCULATING SCREENER CACHE (OPTIONAL)")
            print("=" * 80 + "\n")
            precalculate_screener_cache()
            print("✓ Screener cache pre-calculated")
        except ImportError:
            print("\n⚠️ screener_cache module not found. Skipping Step 4.")
        except Exception as e:
            print(f"\n⚠️ Screener cache error: {e}")
            print("   Continuing with pipeline...")

        # Step 4b: Pre-calculate Futures OI cache (OPTIONAL)
        try:
            import futures_oi_cache

            precalculate_futures_oi_cache = futures_oi_cache.precalculate_futures_oi_cache
            print("\n" + "=" * 80)
            print("STEP 4b: PRE-CALCULATING FUTURES OI CACHE (OPTIONAL)")
            print("=" * 80 + "\n")
            precalculate_futures_oi_cache()
            print("✓ Futures OI cache pre-calculated")
        except ImportError:
            print("\n⚠️ futures_oi_cache module not found. Skipping Step 4b.")
        except Exception as e:
            print(f"\n⚠️ Futures OI cache error: {e}")
            print("   Continuing with pipeline...")

        # Step 4c: Pre-calculate Technical Screener cache (OPTIONAL)
        try:
            import technical_screener_cache

            precalculate_technical_screener_cache = technical_screener_cache.precalculate_technical_screener_cache
            print("\n" + "=" * 80)
            print("STEP 4c: PRE-CALCULATING TECHNICAL SCREENER CACHE (OPTIONAL)")
            print("=" * 80 + "\n")
            precalculate_technical_screener_cache()
            print("✓ Technical Screener cache pre-calculated")
        except ImportError:
            print("\n⚠️ technical_screener_cache module not found. Skipping Step 4c.")
        except Exception as e:
            print(f"\n⚠️ Technical Screener cache error: {e}")
            print("   Continuing with pipeline...")

        # Step 5: Pre-calculate dashboard data (OPTIONAL)
        try:
            print("\n" + "=" * 80)
            print("STEP 5: PRE-CALCULATING DASHBOARD DATA (OPTIONAL)")
            print("=" * 80 + "\n")

            # Import from same directory
            import precalculate_data

            create_precalculated_tables = precalculate_data.create_precalculated_tables
            precalculate_all_dates = precalculate_data.precalculate_all_dates

            if True:  # Always try
                create_precalculated_tables()
                precalculate_all_dates()
                print("✓ Dashboard data pre-calculated")
            else:
                print("⚠️ Analysis_Tools directory not found. Skipping Step 5.")
        except ImportError:
            print("\n⚠️ precalculate_data module not found. Skipping Step 5.")
        except Exception as e:
            print(f"\n⚠️ Dashboard data error: {e}")
            print("   Continuing with pipeline...")

        # Step 6: Fetch Index Constituents (for instant loading in web app)
        try:
            import index_constituents_cache

            fetch_index_constituents = index_constituents_cache.fetch_index_constituents_cache
            print("\n" + "=" * 80)
            print("STEP 6: FETCHING INDEX CONSTITUENTS FROM NSE")
            print("=" * 80 + "\n")
            fetch_index_constituents()
            print("✓ Index constituents cache updated")
        except ImportError:
            print("\n⚠️ index_constituents_cache module not found. Skipping Step 6.")
        except Exception as e:
            print(f"\n⚠️ Index constituents error: {e}")
            print("   Continuing with pipeline...")

        print("\n" + "=" * 80)
        print("             ✅ PIPELINE COMPLETE!")
        print("=" * 80)
        print("\n✓ All steps completed successfully")
        print("\nYour database is up to date!")
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
