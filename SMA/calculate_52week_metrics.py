"""
52-WEEK HIGH/LOW METRICS CALCULATOR
====================================
Calculates 52-week high/low metrics from NSE EOD data
and merges with sector classification for heatmap analysis.

Output: 52week_analysis.csv with metrics for Industry, Sector, and Index-wise analysis
"""

import glob
import os
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# ===========================================
# Configuration
# ===========================================
DATA_FOLDER = "C:/NSE_EOD_CASH_WITH_INDICATORS"
SECTOR_MASTER_FILE = "nse_sector_master.csv"
OUTPUT_FILE = "52week_analysis.csv"
LOOKBACK_DAYS = 260  # ~52 weeks (252 trading days + buffer)

# ===========================================
# Helper Functions
# ===========================================


def load_sector_master():
    """Load sector classification data"""
    if not Path(SECTOR_MASTER_FILE).exists():
        raise FileNotFoundError(f"{SECTOR_MASTER_FILE} not found. Run nse_sector_pipeline_unified.py first.")

    df = pd.read_csv(SECTOR_MASTER_FILE)
    print(f"✅ Loaded sector master: {len(df)} symbols")
    return df


def load_historical_data(lookback_days=LOOKBACK_DAYS):
    """
    Load last N days of historical price data
    Returns DataFrame with all stocks' historical data
    """
    pattern = os.path.join(DATA_FOLDER, "sec_bhavdata_full_*_WITH_INDICATORS.csv")
    files = sorted(glob.glob(pattern))

    if not files:
        raise FileNotFoundError(f"No data files found in {DATA_FOLDER}")

    # Get last N files
    files_to_load = files[-lookback_days:] if len(files) > lookback_days else files

    print(f"\n📥 Loading last {len(files_to_load)} trading days...")

    all_data = []
    for idx, filepath in enumerate(files_to_load, 1):
        if idx % 50 == 0:
            print(f"  Progress: [{idx}/{len(files_to_load)}]")

        try:
            df = pd.read_csv(filepath, low_memory=False)
            df.columns = df.columns.str.strip()

            # Extract date from filename
            filename = os.path.basename(filepath)
            date_str = filename.replace("sec_bhavdata_full_", "").replace("_WITH_INDICATORS.csv", "")
            date_obj = datetime.strptime(date_str, "%d%m%Y")

            df["DATE"] = date_obj.strftime("%Y-%m-%d")
            df["DATE_OBJ"] = date_obj

            # Keep only required columns
            required_cols = ["SYMBOL", "CLOSE_PRICE", "TTL_TRD_QNTY", "TURNOVER_LACS", "DATE", "DATE_OBJ"]
            df = df[[col for col in required_cols if col in df.columns]]

            # Convert to numeric
            df["CLOSE_PRICE"] = pd.to_numeric(df["CLOSE_PRICE"], errors="coerce")
            df["TTL_TRD_QNTY"] = pd.to_numeric(df["TTL_TRD_QNTY"], errors="coerce")
            df["TURNOVER_LACS"] = pd.to_numeric(df["TURNOVER_LACS"], errors="coerce")

            all_data.append(df)

        except Exception as e:
            print(f"  ⚠️ Error loading {filename}: {e}")

    print(f"  Progress: [{len(files_to_load)}/{len(files_to_load)}] ✅")

    combined = pd.concat(all_data, ignore_index=True)
    print(f"✅ Loaded {len(combined):,} rows from {len(files_to_load)} days")

    return combined


def calculate_52week_metrics(df_hist):
    """
    Calculate 52-week high/low metrics for each stock
    """
    print("\n📊 Calculating 52-week metrics...")

    metrics = []
    symbols = df_hist["SYMBOL"].unique()

    for idx, symbol in enumerate(symbols, 1):
        if idx % 500 == 0:
            print(f"  Progress: [{idx}/{len(symbols)}] ({idx*100//len(symbols)}%)")

        df_stock = df_hist[df_hist["SYMBOL"] == symbol].copy()
        df_stock = df_stock.sort_values("DATE_OBJ")

        # Get latest date data
        latest_row = df_stock.iloc[-1]
        current_price = latest_row["CLOSE_PRICE"]
        current_date = latest_row["DATE"]

        if pd.isna(current_price):
            continue

        # Calculate 52-week high/low
        week_52_high = df_stock["CLOSE_PRICE"].max()
        week_52_low = df_stock["CLOSE_PRICE"].min()

        # Distance from 52-week high/low
        pct_from_high = ((current_price - week_52_high) / week_52_high) * 100
        pct_from_low = ((current_price - week_52_low) / week_52_low) * 100

        # Additional metrics
        avg_volume = df_stock["TTL_TRD_QNTY"].mean()
        avg_turnover = df_stock["TURNOVER_LACS"].mean()

        metrics.append(
            {
                "SYMBOL": symbol,
                "CURRENT_PRICE": current_price,
                "WEEK_52_HIGH": week_52_high,
                "WEEK_52_LOW": week_52_low,
                "PCT_FROM_HIGH": pct_from_high,
                "PCT_FROM_LOW": pct_from_low,
                "PRICE_RANGE": week_52_high - week_52_low,
                "RANGE_POSITION": ((current_price - week_52_low) / (week_52_high - week_52_low)) * 100
                if week_52_high != week_52_low
                else 50,
                "AVG_VOLUME": avg_volume,
                "AVG_TURNOVER": avg_turnover,
                "LATEST_DATE": current_date,
            }
        )

    print(f"  Progress: [{len(symbols)}/{len(symbols)}] (100%) ✅")

    metrics_df = pd.DataFrame(metrics)
    print(f"✅ Calculated metrics for {len(metrics_df)} stocks")

    return metrics_df


def merge_with_sector_data(metrics_df, sector_df):
    """
    Merge 52-week metrics with sector classification
    """
    print("\n🔗 Merging with sector classification...")

    merged = metrics_df.merge(
        sector_df[["SYMBOL", "COMPANY_NAME_API", "NSE_INDUSTRY", "SECTOR", "INDEX_MEMBERSHIP"]], on="SYMBOL", how="left"
    )

    # Fill missing sector data
    merged["NSE_INDUSTRY"].fillna("Unknown", inplace=True)
    merged["SECTOR"].fillna("Unknown", inplace=True)
    merged["INDEX_MEMBERSHIP"].fillna("", inplace=True)

    print(f"✅ Merged data: {len(merged)} rows")
    print(f"  - With sector data: {(merged['SECTOR'] != 'Unknown').sum()}")
    print(f"  - Missing sector: {(merged['SECTOR'] == 'Unknown').sum()}")

    return merged


# ===========================================
# Main Execution
# ===========================================


def main():
    print("\n" + "=" * 80)
    print("52-WEEK HIGH/LOW METRICS CALCULATOR")
    print("=" * 80)

    try:
        # Step 1: Load sector master
        sector_df = load_sector_master()

        # Step 2: Load historical data
        hist_df = load_historical_data(lookback_days=LOOKBACK_DAYS)

        # Step 3: Calculate 52-week metrics
        metrics_df = calculate_52week_metrics(hist_df)

        # Step 4: Merge with sector data
        final_df = merge_with_sector_data(metrics_df, sector_df)

        # Step 5: Save output
        print("\n💾 Saving output...")
        final_df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
        print(f"✅ Output saved: {Path(OUTPUT_FILE).resolve()}")

        # Summary
        print("\n" + "=" * 80)
        print("CALCULATION SUMMARY")
        print("=" * 80)
        print(f"\n📊 Total stocks analyzed: {len(final_df)}")
        print(f"📅 Latest date: {final_df['LATEST_DATE'].iloc[0]}")
        print(f"📈 Stocks near 52-week high (>-5%): {(final_df['PCT_FROM_HIGH'] > -5).sum()}")
        print(f"📉 Stocks near 52-week low (<10%): {(final_df['PCT_FROM_LOW'] < 10).sum()}")

        print(f"\n🎯 Sector distribution:")
        sector_counts = final_df["SECTOR"].value_counts().head(10)
        for sector, count in sector_counts.items():
            print(f"  {sector:30s}: {count:4d} stocks")

        print(f"\n📁 Output file: {OUTPUT_FILE}")
        print(f"💡 Use heatmap_dashboard.py to visualize this data")
        print("=" * 80 + "\n")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
