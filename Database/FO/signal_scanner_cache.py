"""
SIGNAL SCANNER CACHE BUILDER
================================================================================
Pre-calculates F&O signals (RSI, Pivots, Volume Profile, etc.) for the Signal Scanner.
Source: BhavCopy_Database (TBL_*)
Target: daily_signal_scanner table in BhavCopy_Database
"""

import os
import sys
import json
from dotenv import load_dotenv
import logging
import math
import sys
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
load_dotenv()
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

# Setup Logging
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# =============================================================
# CONFIGURATION
# =============================================================

# Database Config
from Analysis_Tools.app.models.db_config import engine

# Hardcoded constants removed - using shared engine
DB_USER = "postgres"
DB_PASS = os.getenv("DB_PASSWORD")
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "BhavCopy_Database"

MIN_DATA_POINTS = 25
MIN_VOLUME = 10
BINS = 50

# =============================================================
# CALCULATION LOGIC (Cloned from signal_scanner_model.py)
# =============================================================


def calc_pivot_levels(high, low, close):
    """Calculate Standard Pivot Points."""
    pp = (high + low + close) / 3
    r1 = 2 * pp - low
    s1 = 2 * pp - high
    r2 = pp + (high - low)
    s2 = pp - (high - low)
    r3 = high + 2 * (pp - low)
    s3 = low - 2 * (high - pp)
    return pp, r1, s1, r2, s2, r3, s3


def calc_rsi(series, period=14):
    """Calculate RSI."""
    try:
        delta = series.diff()
        gain = delta.clip(lower=0).rolling(period).mean()
        loss = -delta.clip(upper=0).rolling(period).mean()
        rs = gain / loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        return rsi
    except Exception:
        return pd.Series([np.nan] * len(series), index=series.index)


def calc_volume_profile(df, bins=50):
    """Calculate Volume Profile - POC, VAH, VAL."""
    if "HghPric" not in df.columns or "LwPric" not in df.columns:
        return None, None, None

    # Filter out invalid prices
    df_valid = df[(df["HghPric"] > 0) & (df["LwPric"] > 0) & (df["ClsPric"] > 0)].copy()

    if df_valid.empty:
        return None, None, None

    high = df_valid["HghPric"].max()
    low = df_valid["LwPric"].min()

    if pd.isna(high) or pd.isna(low) or high == low:
        return None, None, None

    levels = np.linspace(low, high, bins)
    vol_dist = np.zeros(bins)

    for _, row in df.iterrows():
        close_price = row.get("ClsPric", 0)
        vol = row.get("Volume", 0)

        if pd.isna(close_price) or pd.isna(vol) or vol == 0:
            continue

        dist = np.abs(levels - close_price)
        inv = 1 / (dist + 0.01)
        weighted = inv / inv.sum() * vol  # Matches POC: inv / inv.sum() * vol
        vol_dist += weighted

    if vol_dist.sum() == 0:
        return None, None, None

    poc_idx = np.argmax(vol_dist)
    poc = levels[poc_idx]

    sorted_idx = np.argsort(vol_dist)[::-1]
    vol_sorted = vol_dist[sorted_idx]
    levels_sorted = levels[sorted_idx]
    cum_vol = np.cumsum(vol_sorted)

    try:
        cutoff = np.where(cum_vol >= cum_vol[-1] * 0.70)[0][0]
        vah = np.max(levels_sorted[: cutoff + 1])
        val = np.min(levels_sorted[: cutoff + 1])
    except IndexError:
        vah = poc
        val = poc

    return round(poc, 2), round(vah, 2), round(val, 2)


# =============================================================
# DATABASE MANAGEMENT
# =============================================================


def create_cache_table():
    """Create the daily_signal_scanner table with Multi-Contract Support."""
    logger.info("Checking/Creating daily_signal_scanner table...")
    try:
        with engine.connect() as conn:
            conn.execute(
                text(
                    """
                CREATE TABLE IF NOT EXISTS daily_signal_scanner (
                    date DATE NOT NULL,
                    symbol VARCHAR(50) NOT NULL,
                    expiry DATE NOT NULL,
                    strike NUMERIC NOT NULL,
                    option_type VARCHAR(10) NOT NULL,

                    close NUMERIC,
                    volume BIGINT,
                    oi BIGINT,
                    rsi NUMERIC,
                    indicators JSONB,
                    signals JSONB,
                    metadata JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (date, symbol, expiry, strike, option_type)
                );

                -- Indexes for fast filtering/sorting
                CREATE INDEX IF NOT EXISTS idx_scanner_date ON daily_signal_scanner(date);
                CREATE INDEX IF NOT EXISTS idx_scanner_symbol ON daily_signal_scanner(symbol);
                CREATE INDEX IF NOT EXISTS idx_scanner_close ON daily_signal_scanner(close);
                CREATE INDEX IF NOT EXISTS idx_scanner_volume ON daily_signal_scanner(volume);
                CREATE INDEX IF NOT EXISTS idx_scanner_oi ON daily_signal_scanner(oi);
                CREATE INDEX IF NOT EXISTS idx_scanner_rsi ON daily_signal_scanner(rsi);
            """
                )
            )
            conn.commit()
        logger.info("Table schema verified (Multi-Contract).")
        return True
    except Exception as e:
        logger.error(f"Could not create table: {e}")
        return False


def get_target_tables() -> List[str]:
    """Get list of F&O tables (excluding derived)."""
    try:
        query = text(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name LIKE 'TBL_%'
            AND table_name NOT LIKE '%_DERIVED'
            ORDER BY table_name;
        """
        )
        with engine.connect() as conn:
            result = conn.execute(query)
            return [row[0] for row in result]
    except Exception as e:
        logger.error(f"Failed to get tables: {e}")
        return []


def get_cached_dates() -> set:
    """Get dates already in the cache."""
    try:
        query = text("SELECT DISTINCT date FROM daily_signal_scanner")
        with engine.connect() as conn:
            return {str(row[0]) for row in conn.execute(query)}
    except Exception:
        return set()


def get_available_dates(tables: List[str]) -> List[str]:
    """Get all unique dates available across F&O tables."""
    sample_symbols = ["TBL_NIFTY", "TBL_BANKNIFTY", "TBL_RELIANCE", "TBL_INFY", "TBL_TCS"]
    found_dates = set()

    with engine.connect() as conn:
        for tbl in sample_symbols:
            if tbl in tables:
                try:
                    q = text(f'SELECT DISTINCT "BizDt" FROM "{tbl}" ORDER BY "BizDt" DESC LIMIT 60')
                    res = conn.execute(q)
                    found_dates.update(str(row[0]) for row in res)
                except Exception:
                    continue

    return sorted(list(found_dates), reverse=True)


def clean_for_json(obj):
    """Recursively replace NaN/Inf with None for valid JSON."""
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    elif isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_for_json(v) for v in obj]
    return obj


def process_date(target_date: str, tables: List[str]):
    """
    Calculate signals for all symbols on specific date.
    POC-ALIGNED: Processes entire symbol history, iterates through days from index 15,
    only stores rows with signals.
    """
    logger.info(f"Processing date: {target_date}...")

    start_dt_obj = datetime.strptime(target_date, "%Y-%m-%d")
    lookback_dt_obj = start_dt_obj - timedelta(days=90)  # Extended lookback for more history
    lookback_date = lookback_dt_obj.strftime("%Y-%m-%d")

    batch_size = 20

    for i in range(0, len(tables), batch_size):
        batch_tables = tables[i : i + batch_size]

        for table in batch_tables:
            try:
                # Fetch ALL history for this symbol (POC approach)
                query = text(
                    f"""
                    SELECT
                        "BizDt" as "Timestamp",
                        "TckrSymb" as "Symbol",
                        CAST(REPLACE("HghPric", ',', '') AS NUMERIC) as "HghPric",
                        CAST(REPLACE("LwPric", ',', '') AS NUMERIC) as "LwPric",
                        CAST(REPLACE("ClsPric", ',', '') AS NUMERIC) as "ClsPric",
                        CAST(REPLACE("TtlTradgVol", ',', '') AS NUMERIC) as "Volume",
                        CAST(REPLACE("OpnIntrst", ',', '') AS NUMERIC) as "OI",
                        "FininstrmActlXpryDt" as "Expiry",
                        CAST(REPLACE("StrkPric", ',', '') AS NUMERIC) as "Strike",
                        "OptnTp" as "OptionType",
                        CAST(REPLACE("UndrlygPric", ',', '') AS NUMERIC) as "Spot"
                    FROM "{table}"
                    WHERE "BizDt" BETWEEN :start AND :end
                    AND "ClsPric" IS NOT NULL
                    ORDER BY "BizDt" ASC
                """
                )

                with engine.connect() as conn:
                    df = pd.read_sql(query, conn, params={"start": lookback_date, "end": target_date})

                if df.empty or len(df) < MIN_DATA_POINTS:
                    continue

                # Skip if close prices are not numeric or all NaN
                if df["ClsPric"].isna().all():
                    continue

                df["TimestampStr"] = pd.to_datetime(df["Timestamp"]).dt.strftime("%Y-%m-%d")
                df["Expiry"] = pd.to_datetime(df["Expiry"], errors="coerce")
                df["Strike"] = df["Strike"].fillna(0)
                df["OptionType"] = df["OptionType"].fillna("XX")

                # POC: Calculate RSI on ENTIRE symbol history
                try:
                    df["RSI"] = calc_rsi(df["ClsPric"])
                except:
                    df["RSI"] = np.nan

                # POC: Calculate Volume Profile on ENTIRE symbol history (Client Requirement: Include Options data)
                try:
                    # NOTE: Client POC calc_volume_profile uses the ENTIRE dataframe (including options).
                    # This results in POC ~0.05 due to low premium prices, but we must match their logic exactly.
                    poc, vah, val = calc_volume_profile(df, bins=BINS)

                    if poc is None:
                        continue
                except:
                    continue

                symbol_name = table.replace("TBL_", "")
                rows_to_insert = []

                # POC: Iterate through days starting from index 15
                for idx in range(15, len(df)):
                    today = df.iloc[idx]
                    prev = df.iloc[idx - 1]

                    # Only process target_date
                    if today.get("TimestampStr") != target_date:
                        continue

                    high = float(today.get("HghPric") or 0)
                    low = float(today.get("LwPric") or 0)
                    close = float(today.get("ClsPric") or 0)
                    volume = int(float(today.get("Volume") or 0))
                    oi = int(float(today.get("OI") or 0))
                    rsi = float(today.get("RSI") or 0)
                    if pd.isna(rsi):
                        rsi = 0.0

                    expiry = today.get("Expiry")
                    strike = float(today.get("Strike") or 0)
                    type_opt = str(today.get("OptionType") or "XX")
                    spot_price = float(today.get("Spot") or 0)

                    # POC: Skip if essential data is missing
                    if pd.isna(high) or pd.isna(low) or pd.isna(close) or high == low or close == 0:
                        continue

                    # POC: Skip low volume contracts
                    if volume < MIN_VOLUME:
                        continue

                    # Calculate Pivot Levels
                    pp, r1, s1, r2, s2, r3, s3 = calc_pivot_levels(high, low, close)

                    # Support/Resistance Detection at ALL levels
                    day_range = high - low
                    tol = day_range * 0.20

                    s1_sup = abs(close - s1) <= tol
                    s2_sup = abs(close - s2) <= tol
                    s3_sup = abs(close - s3) <= tol
                    r1_res = abs(close - r1) <= tol
                    r2_res = abs(close - r2) <= tol
                    r3_res = abs(close - r3) <= tol

                    # High Volume
                    prev_vol = float(prev.get("Volume") or 0)
                    high_vol = volume > prev_vol * 1.5 if prev_vol > 0 else False

                    # OI Spike
                    prev_oi = float(prev.get("OI") or 0)
                    oi_spike = prev_oi > 0 and oi > 0 and (oi - prev_oi) / prev_oi >= 0.15

                    # POC: RSI Divergence (checks i-2)
                    if idx >= 2 and not pd.isna(rsi) and not pd.isna(df.iloc[idx - 2]["RSI"]):
                        price_LL = close < df.iloc[idx - 2]["ClsPric"]
                        price_HH = close > df.iloc[idx - 2]["ClsPric"]
                        rsi_HL = rsi > df.iloc[idx - 2]["RSI"]
                        rsi_LH = rsi < df.iloc[idx - 2]["RSI"]

                        bull_div = price_LL and rsi_HL
                        bear_div = price_HH and rsi_LH
                    else:
                        bull_div = False
                        bear_div = False

                    # RSI Trend Label
                    if pd.isna(rsi):
                        rsi_signal = "NEUTRAL"
                    elif rsi < 30:
                        rsi_signal = "OVERSOLD"
                    elif rsi > 70:
                        rsi_signal = "OVERBOUGHT"
                    else:
                        rsi_signal = "NEUTRAL"

                    # RSI Cross Signals
                    prev_rsi = float(prev.get("RSI") or 0)
                    if pd.isna(prev_rsi):
                        rsi_cross_up = False
                        rsi_cross_down = False
                    else:
                        rsi_cross_up = prev_rsi < 30 and rsi > 30
                        rsi_cross_down = prev_rsi > 70 and rsi < 70

                    # Pivot Signal Label
                    if s1_sup:
                        pivot_signal = "S1"
                    elif s2_sup:
                        pivot_signal = "S2"
                    elif s3_sup:
                        pivot_signal = "S3"
                    elif r1_res:
                        pivot_signal = "R1"
                    elif r2_res:
                        pivot_signal = "R2"
                    elif r3_res:
                        pivot_signal = "R3"
                    else:
                        pivot_signal = None

                    # POC: Store ALL rows (Modified to match POC output count)
                    # if not (high_vol or oi_spike or bull_div or bear_div or pivot_signal or rsi_cross_up or rsi_cross_down):
                    #    continue

                    # Prepare data for insertion
                    indicators = clean_for_json(
                        {
                            "pp": round(pp, 2),
                            "r1": round(r1, 2),
                            "s1": round(s1, 2),
                            "r2": round(r2, 2),
                            "s2": round(s2, 2),
                            "r3": round(r3, 2),
                            "s3": round(s3, 2),
                            "poc": poc,
                            "vah": vah,
                            "val": val,
                        }
                    )

                    signals = clean_for_json(
                        {
                            "high_volume": high_vol,
                            "oi_spike": oi_spike,
                            "rsi_trend": rsi_signal,
                            "rsi_cross": "UP" if rsi_cross_up else ("DOWN" if rsi_cross_down else None),
                            "pivot_signal": pivot_signal,
                            "divergence": "BULL" if bull_div else ("BEAR" if bear_div else None),
                        }
                    )

                    metadata = clean_for_json({"high": high, "low": low, "spot": spot_price})

                    # Sanitize expiry
                    expiry_db = expiry
                    if pd.isna(expiry_db):
                        expiry_db = datetime(1900, 1, 1).date()

                    # Sanitize numeric fields
                    strike_safe = float(strike)
                    if math.isnan(strike_safe) or math.isinf(strike_safe):
                        strike_safe = 0.0

                    opt_type_safe = type_opt[:10]

                    close_safe = float(close)
                    if math.isnan(close_safe) or math.isinf(close_safe):
                        close_safe = 0.0

                    vol_safe = int(volume)
                    if vol_safe > 9223372036854775807:
                        vol_safe = 0

                    oi_safe = int(oi)
                    if oi_safe > 9223372036854775807:
                        oi_safe = 0

                    rsi_safe = round(rsi, 2)
                    if math.isnan(rsi_safe) or math.isinf(rsi_safe):
                        rsi_safe = 0.0

                    rows_to_insert.append(
                        {
                            "date": target_date,
                            "symbol": symbol_name,
                            "expiry": expiry_db,
                            "strike": strike_safe,
                            "option_type": opt_type_safe,
                            "close": close_safe,
                            "volume": vol_safe,
                            "oi": oi_safe,
                            "rsi": rsi_safe,
                            "indicators": json.dumps(indicators),
                            "signals": json.dumps(signals),
                            "metadata": json.dumps(metadata),
                        }
                    )

                # Insert rows for this symbol
                if rows_to_insert:
                    try:
                        insert_query = text(
                            """
                            INSERT INTO daily_signal_scanner
                            (date, symbol, expiry, strike, option_type, close, volume, oi, rsi, indicators, signals, metadata)
                            VALUES (:date, :symbol, :expiry, :strike, :option_type, :close, :volume, :oi, :rsi, :indicators, :signals, :metadata)
                            ON CONFLICT (date, symbol, expiry, strike, option_type) DO UPDATE SET
                                close = EXCLUDED.close,
                                volume = EXCLUDED.volume,
                                oi = EXCLUDED.oi,
                                rsi = EXCLUDED.rsi,
                                indicators = EXCLUDED.indicators,
                                signals = EXCLUDED.signals,
                                metadata = EXCLUDED.metadata;
                        """
                        )

                        with engine.connect() as conn:
                            conn.execute(insert_query, rows_to_insert)
                            conn.commit()

                        logger.info(f"✅ {table}: inserted {len(rows_to_insert)} signal rows")
                    except Exception as e:
                        logger.error(f"Insert failed for {table}: {e}")

            except Exception as e:
                logger.error(f"Error processing {table}: {e}")
                import traceback

                traceback.print_exc()
                continue


def update_signal_scanner_cache():
    """Main execution function."""
    if not create_cache_table():
        return

    all_tables = get_target_tables()
    if not all_tables:
        logger.warning("No F&O tables found.")
        return

    # Find missing dates
    # Since checking ALL tables for dates is slow, we rely on cached_dates vs a sample
    cached = get_cached_dates()
    available_dates = get_available_dates(all_tables)  # This checks a few major stocks

    # Filter 30 days only? Or populate whatever we find?
    # Let's stick to last 30 days to keep it fast initially, then expand if needed
    # Or just populate all missing dates found.

    missing_dates = [d for d in available_dates if d not in cached]

    if not missing_dates:
        logger.info("Signal Scanner Cache is up to date.")
        return

    logger.info(f"Found {len(missing_dates)} missing dates. Processing...")

    # Process newest first
    for date in missing_dates:
        process_date(date, all_tables)


if __name__ == "__main__":
    update_signal_scanner_cache()
