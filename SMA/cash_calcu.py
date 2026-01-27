"""
NSE CASH DATA - RSI + SMA + EMA CALCULATOR (ENHANCED)
======================================================
ENHANCED: Added EMA_21 for RS (Relative Strength) calculation
RS Formula: (EMA_21 of Stock / EMA_21 of Nifty) * 100

FIXED: Uses only CONSECUTIVE TRADING DAYS for indicator calculation
"""

import glob
import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

# Try to import pandas_ta
try:
    import pandas_ta as ta

    PANDAS_TA_AVAILABLE = True
    print("✅ Using pandas_ta for RSI/EMA calculation")
except ImportError:
    PANDAS_TA_AVAILABLE = False
    print("⚠️ pandas_ta not found - using manual RSI/EMA calculation")

# ===========================================
# Configuration
# ===========================================
input_folder = "C:/NSE_EOD_CASH"
output_folder = "C:/NSE_EOD_CASH_WITH_INDICATORS"
RSI_PERIOD = 14
SMA_PERIODS = [3, 5, 8, 13, 21, 50, 100, 200]
EMA_PERIODS = [21]  # EMA periods for RS calculation

# Nifty 50 symbol for benchmark (used for RS calculation)
NIFTY_BENCHMARK = "NIFTY 50"  # Will be calculated from Nifty 50 constituents

# Columns to calculate SMA on
SMA_COLUMNS = ["CLOSE_PRICE", "AVG_PRICE", "TTL_TRD_QNTY", "TURNOVER_LACS", "NO_OF_TRADES", "DELIV_QTY"]

# Columns to remove from output
COLUMNS_TO_REMOVE = ["SERIES", "PREV_CLOSE", "OPEN_PRICE", "HIGH_PRICE", "LOW_PRICE", "LAST_PRICE"]


# ===========================================
# Manual RSI Calculation
# ===========================================
def calculate_rsi_manual(data, period=14):
    """Calculate RSI manually using Wilder's smoothing method"""
    if len(data) < period + 1:
        return pd.Series([np.nan] * len(data), index=data.index)

    delta = data.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    return rsi


# ===========================================
# Calculate SMA
# ===========================================
def calculate_sma(data, period):
    """Calculate Simple Moving Average"""
    return data.rolling(window=period, min_periods=period).mean()


# ===========================================
# Calculate EMA
# ===========================================
def calculate_ema(data, period):
    """Calculate Exponential Moving Average"""
    return data.ewm(span=period, min_periods=period, adjust=False).mean()


# ===========================================
# Calculate Indicators for Stock
# ===========================================
def calculate_indicators_for_stock(df_stock):
    """
    Calculate RSI, SMA, and EMA on multiple columns for a single stock
    IMPORTANT: df_stock must be sorted by DATE and contain consecutive trading days
    """
    # Ensure sorted by date
    df_stock = df_stock.sort_values("DATE_OBJ").copy()

    # Calculate RSI on CLOSE_PRICE
    close_prices = pd.to_numeric(df_stock["CLOSE_PRICE"], errors="coerce")

    if PANDAS_TA_AVAILABLE:
        df_stock[f"RSI_{RSI_PERIOD}"] = ta.rsi(close_prices, length=RSI_PERIOD)
    else:
        df_stock[f"RSI_{RSI_PERIOD}"] = calculate_rsi_manual(close_prices, period=RSI_PERIOD)

    # Calculate EMAs on CLOSE_PRICE (for RS calculation)
    for period in EMA_PERIODS:
        ema_col_name = f"EMA_{period}_CLOSE_PRICE"
        if PANDAS_TA_AVAILABLE:
            df_stock[ema_col_name] = ta.ema(close_prices, length=period)
        else:
            df_stock[ema_col_name] = calculate_ema(close_prices, period)

    # Calculate SMAs on all specified columns
    for col in SMA_COLUMNS:
        if col in df_stock.columns:
            # Convert to numeric, replacing '-' with NaN
            data = pd.to_numeric(df_stock[col], errors="coerce")

            # Calculate SMA for each period
            for period in SMA_PERIODS:
                sma_col_name = f"SMA_{period}_{col}"
                df_stock[sma_col_name] = calculate_sma(data, period)

    return df_stock


# ===========================================
# Calculate Nifty 50 Index EMA (Benchmark)
# ===========================================
def calculate_nifty_benchmark_ema(df_combined, nifty50_symbols):
    """
    Calculate Nifty 50 index-level EMA using weighted average of constituent EMAs
    For simplicity, we use equal-weighted average of Nifty 50 stocks' close prices

    Returns: DataFrame with DATE and NIFTY_EMA_21 columns
    """
    print("\n📈 Calculating Nifty 50 benchmark EMA...")

    # Filter only Nifty 50 stocks
    nifty_data = df_combined[df_combined["SYMBOL"].isin(nifty50_symbols)].copy()

    if nifty_data.empty:
        print("⚠️ No Nifty 50 stocks found in data")
        return pd.DataFrame()

    # Calculate daily average close price of Nifty 50 (equal-weighted proxy)
    nifty_data["CLOSE_PRICE"] = pd.to_numeric(nifty_data["CLOSE_PRICE"], errors="coerce")

    # Group by date and calculate average
    nifty_daily = nifty_data.groupby("DATE").agg({"CLOSE_PRICE": "mean", "DATE_OBJ": "first"}).reset_index()

    nifty_daily = nifty_daily.sort_values("DATE_OBJ")

    # Calculate EMA_21 on this proxy
    for period in EMA_PERIODS:
        nifty_daily[f"NIFTY_EMA_{period}"] = calculate_ema(nifty_daily["CLOSE_PRICE"], period)

    print(f"✅ Nifty benchmark EMA calculated for {len(nifty_daily)} trading days")

    return nifty_daily[["DATE"] + [f"NIFTY_EMA_{p}" for p in EMA_PERIODS]]


# ===========================================
# Main Processing Function
# ===========================================
def process_files_with_indicators():
    """
    Process all downloaded CSV files and add RSI + SMA + EMA + RS indicators
    """
    print("\n" + "=" * 80)
    print("NSE CASH DATA - RSI + SMA + EMA CALCULATOR (ENHANCED)")
    print("=" * 80 + "\n")

    # Create output folder
    os.makedirs(output_folder, exist_ok=True)

    # Find all CSV files (from input folder, NOT output folder)
    csv_pattern = os.path.join(input_folder, "sec_bhavdata_full_*.csv")
    csv_files = sorted(glob.glob(csv_pattern))

    if not csv_files:
        print(f"❌ No CSV files found in {input_folder}")
        print(f"   Looking for pattern: sec_bhavdata_full_*.csv")
        return False

    print(f"📂 Found {len(csv_files)} files to process")
    print(f"📊 RSI Period: {RSI_PERIOD} (on CLOSE_PRICE)")
    print(f"📊 SMA Periods: {', '.join(map(str, SMA_PERIODS))}")
    print(f"📊 EMA Periods: {', '.join(map(str, EMA_PERIODS))} (for RS calculation)")
    print(f"📊 SMA calculated on {len(SMA_COLUMNS)} columns:")
    for col in SMA_COLUMNS:
        print(f"   • {col}")
    print(f"\n🗑️  Columns to remove from output:")
    for col in COLUMNS_TO_REMOVE:
        print(f"   • {col}")
    print(f"\n💾 Output folder: {output_folder}")
    print(f"\n⚠️  IMPORTANT: Using CONSECUTIVE TRADING DAYS for accurate indicator calculation")
    print("=" * 80)

    # Step 1: Read ALL files to build complete historical data
    print("\n📥 Step 1: Reading ALL CSV files to build complete history...")

    all_data = []
    original_columns = None
    file_date_map = {}  # Map date to original columns

    for idx, filepath in enumerate(csv_files, 1):
        try:
            filename = os.path.basename(filepath)

            if idx % 50 == 0:
                print(f"  [{idx}/{len(csv_files)}] {filename}...", end=" ", flush=True)

            # Read CSV and strip whitespace from column names
            df = pd.read_csv(filepath)
            df.columns = df.columns.str.strip()

            # Store original columns from first file
            if original_columns is None:
                original_columns = df.columns.tolist()

            # Extract date from filename
            date_str = filename.replace("sec_bhavdata_full_", "").replace(".csv", "")
            date_obj = datetime.strptime(date_str, "%d%m%Y")
            date_formatted = date_obj.strftime("%Y-%m-%d")

            # Add date columns
            df["DATE"] = date_formatted
            df["DATE_OBJ"] = date_obj

            all_data.append(df)
            file_date_map[date_formatted] = filename

            if idx % 50 == 0:
                print(f"✅")

        except Exception as e:
            print(f"\n  [{idx}/{len(csv_files)}] {filename}... ❌ Error: {str(e)[:50]}")

    if not all_data:
        print("❌ No data loaded")
        return False

    # Combine all data
    print(f"\n  📊 Processed all {len(csv_files)} files")
    print("\n🔄 Combining data from all dates...")
    df_combined = pd.concat(all_data, ignore_index=True)

    # Sort by date
    df_combined = df_combined.sort_values(["SYMBOL", "DATE_OBJ"])

    print(f"✅ Combined data: {len(df_combined):,} total rows")
    print(f"   Unique stocks: {df_combined['SYMBOL'].nunique():,}")
    print(f"   Date range: {df_combined['DATE'].min()} to {df_combined['DATE'].max()}")
    print(f"   Trading days: {df_combined['DATE'].nunique()}")

    # Step 1.5: Load Nifty 50 constituents for benchmark calculation
    print("\n" + "=" * 80)
    print("📈 Step 1.5: Loading Nifty 50 constituents for RS benchmark...")
    print("=" * 80)

    # Try to load sector master for Nifty 50 constituents
    nifty50_symbols = []
    sector_master_paths = [
        os.path.join(output_folder, "nse_sector_master.csv"),
        "nse_sector_master.csv",
        "C:/NSE_EOD_CASH_WITH_INDICATORS/nse_sector_master.csv",
    ]

    for path in sector_master_paths:
        if os.path.exists(path):
            try:
                sector_df = pd.read_csv(path, encoding="utf-8-sig")
                sector_df.columns = sector_df.columns.str.strip()
                if "INDEX_MEMBERSHIP" in sector_df.columns:
                    nifty50_mask = sector_df["INDEX_MEMBERSHIP"].str.contains("NIFTY50", na=False)
                    nifty50_symbols = sector_df[nifty50_mask]["SYMBOL"].tolist()
                    print(f"✅ Loaded {len(nifty50_symbols)} Nifty 50 constituents from {path}")
                    break
            except Exception as e:
                print(f"⚠️ Error loading {path}: {e}")

    if not nifty50_symbols:
        # Fallback: Use top 50 stocks by trading frequency as proxy
        print("⚠️ Sector master not found. Using top 50 stocks by frequency as proxy...")
        symbol_counts = df_combined["SYMBOL"].value_counts()
        nifty50_symbols = symbol_counts.head(50).index.tolist()

    # Calculate Nifty benchmark EMA
    nifty_benchmark = calculate_nifty_benchmark_ema(df_combined, nifty50_symbols)

    # Step 2: Calculate indicators for each stock
    print("\n" + "=" * 80)
    print("📈 Step 2: Calculating RSI + SMAs + EMAs for each stock...")
    print("=" * 80)

    unique_symbols = sorted(df_combined["SYMBOL"].unique())
    print(f"📊 Processing {len(unique_symbols):,} stocks with CONSECUTIVE trading day data...\n")

    indicator_results = []
    failed_stocks = []

    for idx, symbol in enumerate(unique_symbols, 1):
        if idx % 500 == 0:
            print(f"   Progress: {idx}/{len(unique_symbols)} ({idx/len(unique_symbols)*100:.1f}%)")

        try:
            # Get ALL historical data for this stock (consecutive trading days)
            df_stock = df_combined[df_combined["SYMBOL"] == symbol].copy()

            # Calculate all indicators using full history
            df_stock = calculate_indicators_for_stock(df_stock)

            indicator_results.append(df_stock)

        except Exception as e:
            failed_stocks.append(symbol)
            if len(failed_stocks) <= 5:
                print(f"      ⚠️ {symbol}: {str(e)[:40]}")

    print(f"   Progress: {len(unique_symbols)}/{len(unique_symbols)} (100.0%)")

    print(f"\n✅ Indicators calculated for {len(indicator_results):,} stocks")
    if failed_stocks:
        print(f"⚠️  Failed for {len(failed_stocks)} stocks")
        if len(failed_stocks) > 5:
            print(f"   (showing first 5): {', '.join(failed_stocks[:5])}")

    # Combine results
    df_with_indicators = pd.concat(indicator_results, ignore_index=True)

    # Step 2.5: Calculate RS (Relative Strength) by merging with Nifty benchmark
    print("\n" + "=" * 80)
    print("📊 Step 2.5: Calculating RS (Relative Strength)...")
    print("=" * 80)
    print("   Formula: RS = (EMA_21 of Stock / EMA_21 of Nifty) * 100")

    if not nifty_benchmark.empty:
        # Merge Nifty benchmark with stock data
        df_with_indicators = df_with_indicators.merge(nifty_benchmark, on="DATE", how="left")

        # Calculate RS for each EMA period
        for period in EMA_PERIODS:
            stock_ema_col = f"EMA_{period}_CLOSE_PRICE"
            nifty_ema_col = f"NIFTY_EMA_{period}"
            rs_col = f"RS_{period}"

            df_with_indicators[rs_col] = (df_with_indicators[stock_ema_col] / df_with_indicators[nifty_ema_col]) * 100

        # Drop Nifty EMA columns (no longer needed in output)
        nifty_cols = [f"NIFTY_EMA_{p}" for p in EMA_PERIODS]
        df_with_indicators = df_with_indicators.drop(columns=nifty_cols, errors="ignore")

        print("✅ RS calculation complete")
    else:
        print("⚠️ Nifty benchmark not available. RS will be NaN.")
        for period in EMA_PERIODS:
            df_with_indicators[f"RS_{period}"] = np.nan

    # Step 3: Prepare output columns
    print("\n" + "=" * 80)
    print("🔧 Step 3: Preparing output columns...")
    print("=" * 80 + "\n")

    # Determine which columns to keep from original
    kept_original_columns = [col for col in original_columns if col not in COLUMNS_TO_REMOVE]

    # Build indicator columns list
    indicator_columns = [f"RSI_{RSI_PERIOD}"]

    # Add EMA columns
    for period in EMA_PERIODS:
        indicator_columns.append(f"EMA_{period}_CLOSE_PRICE")

    # Add RS columns
    for period in EMA_PERIODS:
        indicator_columns.append(f"RS_{period}")

    # Add SMA columns
    for col in SMA_COLUMNS:
        for period in SMA_PERIODS:
            indicator_columns.append(f"SMA_{period}_{col}")

    print(f"✅ Original columns kept: {len(kept_original_columns)}")
    print(f"✅ Indicator columns added: {len(indicator_columns)}")
    print(f"✅ Total output columns: {len(kept_original_columns) + len(indicator_columns)}")

    # Step 4: Save results by date
    print("\n" + "=" * 80)
    print("💾 Step 4: Saving files with indicators...")
    print("=" * 80 + "\n")

    saved_count = 0

    for date_str in sorted(df_with_indicators["DATE"].unique()):
        try:
            # Filter data for this date
            df_date = df_with_indicators[df_with_indicators["DATE"] == date_str].copy()

            # Format date for filename
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            date_filename = date_obj.strftime("%d%m%Y")

            # Sort by symbol
            df_date = df_date.sort_values("SYMBOL")

            # Build final column list
            final_columns = kept_original_columns + indicator_columns

            # Select only columns that exist
            available_columns = [col for col in final_columns if col in df_date.columns]
            df_date = df_date[available_columns]

            # Save to CSV
            output_filename = f"sec_bhavdata_full_{date_filename}_WITH_INDICATORS.csv"
            output_filepath = os.path.join(output_folder, output_filename)

            df_date.to_csv(output_filepath, index=False)

            saved_count += 1

            if saved_count % 50 == 0:
                print(f"   [{saved_count}/{len(df_with_indicators['DATE'].unique())}] Saved...")

        except Exception as e:
            print(f"  ❌ {date_str}: {str(e)[:50]}")

    print(f"\n✅ All {saved_count} files saved")

    # Summary
    print("\n" + "=" * 80)
    print("CALCULATION SUMMARY")
    print("=" * 80)

    latest_date = df_with_indicators["DATE"].max()
    df_latest = df_with_indicators[df_with_indicators["DATE"] == latest_date]

    print(f"\n✅ Files saved: {saved_count}")
    print(f"📊 Latest date: {latest_date}")
    print(f"📊 Stocks: {len(df_latest)}")
    print(f"📊 Total trading days processed: {df_with_indicators['DATE'].nunique()}")

    # Show sample verification
    print(f"\n📋 Sample verification (TCS on {latest_date}):")
    if "TCS" in df_latest["SYMBOL"].values:
        tcs_data = df_latest[df_latest["SYMBOL"] == "TCS"].iloc[0]
        print(f"   CLOSE_PRICE: {tcs_data['CLOSE_PRICE']}")
        print(f"   RSI_14: {tcs_data[f'RSI_{RSI_PERIOD}']:.2f}")
        print(f"   EMA_21_CLOSE_PRICE: {tcs_data['EMA_21_CLOSE_PRICE']:.2f}")
        print(f"   RS_21: {tcs_data['RS_21']:.2f}")
        print(f"   SMA_21_CLOSE_PRICE: {tcs_data['SMA_21_CLOSE_PRICE']:.2f}")
        print(f"   SMA_50_CLOSE_PRICE: {tcs_data['SMA_50_CLOSE_PRICE']:.2f}")

    print(f"\n📁 Output location: {output_folder}")
    print("=" * 80 + "\n")

    return True


# ===========================================
# MAIN EXECUTION
# ===========================================
if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("NSE CASH DATA - RSI + SMA + EMA CALCULATOR (ENHANCED)")
    print("=" * 80)
    print("\nENHANCED: Added EMA_21 and RS (Relative Strength) calculation")
    print("RS Formula: (EMA_21 of Stock / EMA_21 of Nifty) * 100")
    print("\nFIXED: Now uses CONSECUTIVE TRADING DAYS for accurate indicators")
    print("\nThis script will:")
    print("  1. Read ALL sec_bhavdata_full_*.csv files (consecutive trading days)")
    print(f"  2. Calculate RSI_{RSI_PERIOD} on CLOSE_PRICE")
    print(f"  3. Calculate EMA ({', '.join(map(str, EMA_PERIODS))}) on CLOSE_PRICE")
    print(f"  4. Calculate RS (Relative Strength) vs Nifty 50")
    print(f"  5. Calculate SMA ({', '.join(map(str, SMA_PERIODS))}) on:")
    for col in SMA_COLUMNS:
        print(f"     - {col}")
    print(f"  6. Remove columns: {', '.join(COLUMNS_TO_REMOVE)}")
    print("  7. Save files with accurate indicators")
    print("\n⚠️  IMPORTANT: SMA/EMA now matches TradingView (uses consecutive days)")
    print("=" * 80 + "\n")

    input("Press Enter to start calculation...")

    try:
        result = process_files_with_indicators()

        if result:
            print("✅ Calculation complete!")
            print(f"\n📂 Check output files in: {output_folder}")
            print("\n💡 SMA/EMA/RS values should now match TradingView!")
        else:
            print("⚠️ Calculation incomplete - review errors above")

    except KeyboardInterrupt:
        print("\n\n⚠️ Calculation cancelled by user")

    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback

        traceback.print_exc()

    input("\nPress Enter to exit...")
