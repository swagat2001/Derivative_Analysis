"""
ENHANCED SIGNAL SCANNER CACHE BUILDER
================================
Scans ALL contracts for signals WITH RSI, Pivot Levels, and Spot Price
Stores in daily_signal_scanner table for fast retrieval

INCREMENTAL MODE: Only processes NEW dates
ENHANCED: Includes RSI, Pivot Points, High/Low, Spot Price
"""

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="[INFO] %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()
from Analysis_Tools.app.models.db_config import engine


def calc_rsi(series, period=14):
    """Calculate RSI (Relative Strength Index)."""
    try:
        delta = series.diff()
        gain = delta.clip(lower=0).rolling(period).mean()
        loss = -delta.clip(upper=0).rolling(period).mean()
        rs = gain / loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        return rsi.fillna(50)  # Default to neutral
    except:
        return pd.Series([50] * len(series), index=series.index)


def calc_pivot_levels(high, low, close):
    """Calculate Standard Pivot Points."""
    pp = (high + low + close) / 3
    r1 = 2 * pp - low
    s1 = 2 * pp - high
    r2 = pp + (high - low)
    s2 = pp - (high - low)
    r3 = high + 2 * (pp - low)
    s3 = low - 2 * (high - pp)
    return {
        'pp': round(pp, 2),
        'r1': round(r1, 2),
        's1': round(s1, 2),
        'r2': round(r2, 2),
        's2': round(s2, 2),
        'r3': round(r3, 2),
        's3': round(s3, 2)
    }


def full_history_vp(df, bins=50):
    """Calculate full Volume Profile (POC, VAH, VAL) from history."""
    if 'HghPric' not in df.columns or 'LwPric' not in df.columns:
        return {'poc': 0, 'vah': 0, 'val': 0}

    high = df["HghPric"].max()
    low = df["LwPric"].min()

    if pd.isna(high) or pd.isna(low) or high == low:
        return {'poc': 0, 'vah': 0, 'val': 0}

    levels = np.linspace(low, high, bins)
    vol_dist = np.zeros(bins)

    for _, row in df.iterrows():
        close_price = row["ClsPric"]
        vol = row.get("TtlTradgVol", 0)

        if pd.isna(close_price) or pd.isna(vol) or vol == 0:
            continue

        dist = np.abs(levels - close_price)
        inv = 1 / (dist + 0.01)
        weighted = inv / inv.sum() * vol
        vol_dist += weighted

    if vol_dist.sum() == 0:
        return {'poc': 0, 'vah': 0, 'val': 0}

    poc_idx = np.argmax(vol_dist)
    poc = levels[poc_idx]

    sorted_idx = np.argsort(vol_dist)[::-1]
    vol_sorted = vol_dist[sorted_idx]
    levels_sorted = levels[sorted_idx]
    cum_vol = np.cumsum(vol_sorted)

    cutoff_idx = np.where(cum_vol >= cum_vol[-1] * 0.70)[0]
    if len(cutoff_idx) > 0:
        cutoff = cutoff_idx[0]
        vah = np.max(levels_sorted[:cutoff + 1])
        val = np.min(levels_sorted[:cutoff + 1])
    else:
        vah = poc
        val = poc

    return {
        'poc': round(poc, 2),
        'vah': round(vah, 2),
        'val': round(val, 2)
    }


def check_and_fix_schema():
    """Confirms schema is valid, else recreates table"""
    try:
        inspector = inspect(engine)
        if "daily_signal_scanner" in inspector.get_table_names():
            cols = [c["name"] for c in inspector.get_columns("daily_signal_scanner")]
            required = ["signal_date", "ticker", "close_price", "rsi", "spot_price", "poc", "vah", "val"]
            missing = [c for c in required if c not in cols]

            if missing:
                logger.warning(f"⚠ Schema incomplete (missing {missing}). Recreating table...")
                with engine.begin() as conn:
                    conn.execute(text("DROP TABLE daily_signal_scanner CASCADE"))
    except Exception as e:
        logger.error(f"Schema check error: {e}")


def create_signal_scanner_table():
    """Create enhanced signal scanner table"""

    check_and_fix_schema()

    create_query = """
    CREATE TABLE IF NOT EXISTS public.daily_signal_scanner (
        id SERIAL PRIMARY KEY,
        signal_date DATE NOT NULL,
        ticker VARCHAR(50) NOT NULL,
        expiry_date DATE,
        strike_price NUMERIC,
        option_type VARCHAR(10),

        -- Price Data
        close_price NUMERIC,
        spot_price NUMERIC,
        high_price NUMERIC,
        low_price NUMERIC,
        price_change_pct NUMERIC,

        -- Volume & OI
        volume BIGINT,
        volume_change_pct NUMERIC,
        oi BIGINT,
        oi_change_pct NUMERIC,

        -- Technical Indicators
        rsi NUMERIC,
        pp NUMERIC,
        r1 NUMERIC,
        s1 NUMERIC,
        r2 NUMERIC,
        s2 NUMERIC,
        r3 NUMERIC,
        s3 NUMERIC,

        -- Volume Profile
        poc NUMERIC,
        vah NUMERIC,
        val NUMERIC,

        -- Signals (JSONB for flexibility)
        signals JSONB DEFAULT '{}'::jsonb,

        -- Metadata
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

        UNIQUE(signal_date, ticker, expiry_date, strike_price, option_type)
    );

    CREATE INDEX IF NOT EXISTS idx_signal_date ON public.daily_signal_scanner(signal_date);
    CREATE INDEX IF NOT EXISTS idx_signal_ticker ON public.daily_signal_scanner(ticker);
    CREATE INDEX IF NOT EXISTS idx_signal_rsi ON public.daily_signal_scanner(rsi);
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(create_query))
        logger.info("✓ Enhanced daily_signal_scanner table ready")
    except Exception as e:
        logger.error(f"✗ Error creating table: {e}")


def get_cached_keys():
    """Get (date, ticker) tuples already in cache"""
    try:
        inspector = inspect(engine)
        if "daily_signal_scanner" not in inspector.get_table_names():
            return set()

        q = text("SELECT DISTINCT signal_date, ticker FROM public.daily_signal_scanner")
        with engine.connect() as conn:
            result = conn.execute(q).fetchall()
            return set(
                (
                    row[0].strftime("%Y-%m-%d") if hasattr(row[0], "strftime") else str(row[0]),
                    str(row[1]),
                )
                for row in result
            )
    except Exception as e:
        logger.warning(f"Warning reading cache keys: {e}")
        return set()


def get_available_dates(engine):
    """Get all unique trade dates"""
    try:
        query = text('SELECT DISTINCT "BizDt" FROM "TBL_NIFTY_DERIVED" ORDER BY "BizDt" DESC')
        df = pd.read_sql(query, engine)
        return [d.strftime('%Y-%m-%d') for d in df['BizDt']]
    except Exception as e:
        logger.error(f"Error fetching dates: {e}")
        return []


def get_derived_tables():
    """Get all DERIVED tables"""
    inspector = inspect(engine)
    tables = [t for t in inspector.get_table_names() if t.endswith("_DERIVED")]
    return sorted(tables)


def process_ticker(ticker, table_name, dates_to_process):
    """
    Process specific ticker for a list of dates.
    Fetches ALL history for this ticker in one go to avoid N+1 queries.
    """
    if not dates_to_process:
        return

    try:
        # 1. Fetch ALL relevant history for this ticker
        # We need history going back enough to calculate 14-day RSI and Volume Profile
        # So we fetch everything or just cut off at (min_date - 100 days)
        min_date_str = min(dates_to_process)
        min_date_ts = pd.Timestamp(min_date_str)
        buffer_date = min_date_ts - pd.Timedelta(days=150) # 150 days buffer for indicators

        q = text(f"""
            SELECT "BizDt", "FininstrmActlXpryDt", "StrkPric", "FinInstrmTp",
                   "ClsPric", "HghPric", "LwPric", "OpnPric",
                   "OpnIntrst", "ChngInOpnIntrst", "TtlTradgVol", "UndrlygPric",
                   COALESCE("OptnTp", '') AS "OptnTp"
            FROM "{table_name}"
            WHERE "BizDt" >= :buffer_date
            ORDER BY "BizDt" ASC
        """)

        df_raw = pd.read_sql(q, engine, params={"buffer_date": buffer_date})

        if df_raw.empty:
            return

        df_raw["BizDt"] = pd.to_datetime(df_raw["BizDt"])

        # Ensure numeric
        numeric_cols = ["ClsPric", "HghPric", "LwPric", "OpnPric", "OpnIntrst",
                       "ChngInOpnIntrst", "TtlTradgVol", "UndrlygPric", "StrkPric"]
        for col in numeric_cols:
            if col in df_raw.columns:
                 df_raw[col] = pd.to_numeric(df_raw[col], errors='coerce').fillna(0)

        # 2. Iterate through contracts (Expiry + Strike + OptionType)
        # This is much faster in memory
        grouped = df_raw.groupby(["FininstrmActlXpryDt", "StrkPric", "FinInstrmTp", "OptnTp"])

        all_ticker_signals = []

        for (expiry, strike, opt_type, optn_tp), group in grouped:
            group = group.reset_index(drop=True)

            # 3. For each contract, we need to check if we have data for the requested dates
            # We can iterate through the group

            # Optimization: Pre-calculate indicators on the whole series if possible
            # or iterate row by row?
            # Iterating row by row is safest for "simulation" of that day.

            if len(group) < 25:
                continue

            # Calculate RSI for the whole series at once (Vectorized)
            try:
                group["RSI"] = calc_rsi(group["ClsPric"], period=14)
            except:
                continue

            # We iterate dates present in this group that are also in dates_to_process
            group_dates = set(group["BizDt"].dt.strftime('%Y-%m-%d'))
            relevant_dates = set(dates_to_process).intersection(group_dates)

            if not relevant_dates:
                continue

            # Iterate through indices of relevant dates
            # We need to find the index of each relevant date in the group
            for target_date_str in relevant_dates:
                target_ts = pd.Timestamp(target_date_str)

                # Find the row for this date
                # Using boolean masking is okay here since group side is relatively small (one contract history)
                idx_list = group.index[group["BizDt"] == target_ts].tolist()
                if not idx_list:
                    continue

                i = idx_list[0]

                if i < 15: # Need at least 15 days of history for indicators
                    continue

                today = group.iloc[i]
                prev = group.iloc[i-1]

                # Extract Data
                high = float(today["HghPric"])
                low = float(today["LwPric"])
                close = float(today["ClsPric"])
                volume = float(today["TtlTradgVol"])
                oi = float(today["OpnIntrst"])
                spot = float(today["UndrlygPric"]) if today["UndrlygPric"] > 0 else 0

                if pd.isna(high) or pd.isna(low) or pd.isna(close) or high == low or close == 0:
                    continue
                if volume < 10:
                    continue

                # Pivot Levels
                pivots = calc_pivot_levels(high, low, close)

                # RSI values
                rsi = float(today["RSI"]) if not pd.isna(today["RSI"]) else 50
                prev_rsi = float(prev["RSI"]) if not pd.isna(prev["RSI"]) else 50

                # Full Volume Profile Calculation
                # We need history up to today for VPVR
                # Slicing is cheap
                history_subset = group.iloc[:i+1] # Includes today
                vp = full_history_vp(history_subset, bins=50)
                if vp['poc'] == 0:
                    continue

                # Change Calcs
                prev_vol = float(prev["TtlTradgVol"])
                prev_close = float(prev["ClsPric"])
                prev_oi = float(prev["OpnIntrst"])

                price_chg = ((close - prev_close) / prev_close * 100) if prev_close > 0 else 0
                vol_chg = ((volume - prev_vol) / prev_vol * 100) if prev_vol > 0 else 0
                oi_chg = ((oi - prev_oi) / prev_oi * 100) if prev_oi > 0 else 0

                # -------- Signal Logic --------
                signals = []

                # Support/Resistance Detection
                day_range = (high - low)
                tolerance = day_range * 0.20

                if abs(close - pivots['s1']) <= tolerance: signals.append("S1 Support")
                elif abs(close - pivots['s2']) <= tolerance: signals.append("S2 Support")
                elif abs(close - pivots['s3']) <= tolerance: signals.append("S3 Support")
                elif abs(close - pivots['r1']) <= tolerance: signals.append("R1 Resistance")
                elif abs(close - pivots['r2']) <= tolerance: signals.append("R2 Resistance")
                elif abs(close - pivots['r3']) <= tolerance: signals.append("R3 Resistance")

                # High Volume
                if prev_vol > 0 and volume > prev_vol * 1.5:
                    signals.append("High Volume")

                # OI Spike
                if prev_oi > 0 and oi > 0 and (oi - prev_oi) / prev_oi >= 0.15:
                    signals.append("OI Spike")

                # RSI Divergence Matches Snippet Logic (i vs i-2)
                # i is current, so i-2 is group.iloc[i-2]
                if i >= 2 and not pd.isna(rsi) and not pd.isna(group["RSI"].iloc[i-2]):
                    price_i_minus_2 = group["ClsPric"].iloc[i-2]
                    rsi_i_minus_2 = group["RSI"].iloc[i-2]

                    price_LL = close < price_i_minus_2
                    price_HH = close > price_i_minus_2
                    rsi_HL = rsi > rsi_i_minus_2
                    rsi_LH = rsi < rsi_i_minus_2

                    if price_LL and rsi_HL:
                        signals.append("Bullish Divergence")
                    if price_HH and rsi_LH:
                        signals.append("Bearish Divergence")

                # RSI Cross Signals
                if prev_rsi < 30 and rsi > 30: signals.append("RSI Cross Up")
                if prev_rsi > 70 and rsi < 70: signals.append("RSI Cross Down")

                # RSI Trend Label
                if rsi < 30: signals.append("RSI Oversold")
                elif rsi > 70: signals.append("RSI Overbought")

                if not signals:
                    continue

                all_ticker_signals.append({
                    "signal_date": target_date_str,
                    "ticker": ticker,
                    "expiry_date": expiry,
                    "strike_price": float(strike),
                    "option_type": optn_tp if optn_tp else opt_type,
                    "close_price": close,
                    "spot_price": spot,
                    "high_price": high,
                    "low_price": low,
                    "price_change_pct": round(price_chg, 2),
                    "volume": int(volume),
                    "volume_change_pct": round(vol_chg, 2),
                    "oi": int(oi),
                    "oi_change_pct": round(oi_chg, 2),
                    "rsi": rsi,
                    "pp": pivots['pp'],
                    "r1": pivots['r1'],
                    "s1": pivots['s1'],
                    "r2": pivots['r2'],
                    "s2": pivots['s2'],
                    "r3": pivots['r3'],
                    "s3": pivots['s3'],
                    "poc": vp['poc'],
                    "vah": vp['vah'],
                    "val": vp['val'],
                    "signals": signals
                })

        # 4. Insert ALL signals for this ticker in one transaction
        if all_ticker_signals:
            df_insert = pd.DataFrame(all_ticker_signals)
            import json
            df_insert["signals"] = df_insert["signals"].apply(json.dumps)

            with engine.begin() as conn:
                # Delete EXISTING signals for this ticker on ANY of the processed dates
                # We need to be careful not to delete dates we didn't process
                # Ideally, we delete only for (ticker, date) tuples we have new data for is safest?
                # Or just delete where ticker = T and signal_date IN (dates_to_process)

                # Constructing IN clause
                dates_tuple = tuple(dates_to_process)
                if len(dates_to_process) == 1:
                     # Tuple with one element needs trailing comma in SQL usually, or param binding
                     # SQLAlchemy handles tuple binding with IN operator if we pass list/tuple
                     pass

                conn.execute(
                    text("DELETE FROM daily_signal_scanner WHERE ticker = :t AND signal_date IN :dates"),
                    {"t": ticker, "dates": tuple(dates_to_process)}
                )

                df_insert.to_sql("daily_signal_scanner", conn, if_exists="append", index=False)

            logger.info(f"✅ {ticker}: processed & inserted {len(all_ticker_signals)} signals for {len(dates_to_process)} dates")
        else:
             logger.info(f"ℹ️  {ticker}: 0 signals found")

    except Exception as e:
        logger.error(f"Error processing {ticker}: {e}")
        import traceback
        traceback.print_exc()


def update_signal_scanner_cache():
    print("\n" + "=" * 70)
    print("ENHANCED SIGNAL SCANNER CACHE BUILDER (OPTIMIZED)")
    print("=" * 70)

    create_signal_scanner_table()

    # 1. Get all available dates in the system
    available_dates = get_available_dates(engine)
    if not available_dates:
        print("No data available.")
        return
    available_dates.sort(reverse=True)

    # 2. Get what's already cached
    # Map: Ticker -> Set of Dates present
    processed_map = {} # Ticker -> Set(Dates)
    try:
        q = text("SELECT ticker, signal_date FROM daily_signal_scanner")
        with engine.connect() as conn:
            result = conn.execute(q).fetchall()
            for r in result:
                t = str(r[0])
                d = r[1].strftime("%Y-%m-%d") if hasattr(r[1], "strftime") else str(r[1])
                if t not in processed_map:
                    processed_map[t] = set()
                processed_map[t].add(d)
    except Exception as e:
        logger.warning(f"Cache read warning: {e}")

    # 3. Get all tickers
    all_tables = get_derived_tables()

    logger.info(f"Found {len(all_tables)} tickers. Starting batched processing...")

    # 4. Process Ticker by Ticker
    count = 0
    for table in all_tables:
        count += 1
        ticker = table.replace("TBL_", "").replace("_DERIVED", "")

        # Determine which dates are missing for this ticker
        cached_dates = processed_map.get(ticker, set())

        # We want to process dates that are NOT in cache
        # OR we can force re-process last few dates if needed?
        # For now, stick to incremental logic + maybe refreshing last date

        # Let's say we check against 'available_dates'
        # We want to process top N available dates that are missing?
        # Or just ALL missing dates?
        # Given performance boost, we can check all missing dates.

        missing_dates = [d for d in available_dates if d not in cached_dates]

        if not missing_dates:
             # logger.info(f"⏩ {ticker} up to date.")
             continue

        logger.info(f"[{count}/{len(all_tables)}] Processing {ticker} ({len(missing_dates)} missing dates)...")
        process_ticker(ticker, table, missing_dates)

    print("\n✅ Signal Scanner Cache Update Complete!")


if __name__ == "__main__":
    try:
        update_signal_scanner_cache()
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user. Exiting safely.")
