# =============================================================
#  SIGNAL SCANNER MODEL
#  Purpose: RSI-based signal scanner with:
#  - Pivot Levels (PP, R1-R3, S1-S3)
#  - Volume Profile (POC, VAH, VAL)
#  - RSI Signals (Oversold/Overbought/Divergence)
#  - High Volume & OI Spike Detection
# =============================================================

from datetime import datetime, timedelta
from functools import lru_cache

import numpy as np
import pandas as pd
from sqlalchemy import text

from .db_config import engine

# =============================================================
# CONFIGURATION
# =============================================================
BINS = 50
MIN_DATA_POINTS = 5  # Reduced from 25 to allow shorter-lived option contracts
MIN_VOLUME = 10


# =============================================================
# HELPER FUNCTIONS
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
    """Calculate RSI (Relative Strength Index)."""
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

    high = df["HghPric"].max()
    low = df["LwPric"].min()

    if pd.isna(high) or pd.isna(low) or high == low:
        return None, None, None

    levels = np.linspace(low, high, bins)
    vol_dist = np.zeros(bins)

    for _, row in df.iterrows():
        close_price = row.get("ClsPric", 0)
        # Use "Volume" column (aliased in SQL query)
        vol = row.get("Volume", 0)

        if pd.isna(close_price) or pd.isna(vol) or vol == 0:
            continue

        dist = np.abs(levels - close_price)
        inv = 1 / (dist + 0.01)
        weighted = inv / inv.sum() * vol
        vol_dist += weighted

    if vol_dist.sum() == 0:
        return None, None, None

    poc_idx = np.argmax(vol_dist)
    poc = levels[poc_idx]

    sorted_idx = np.argsort(vol_dist)[::-1]
    vol_sorted = vol_dist[sorted_idx]
    levels_sorted = levels[sorted_idx]
    cum_vol = np.cumsum(vol_sorted)

    cutoff = np.where(cum_vol >= cum_vol[-1] * 0.70)[0][0]

    vah = np.max(levels_sorted[: cutoff + 1])
    val = np.min(levels_sorted[: cutoff + 1])

    return round(poc, 2), round(vah, 2), round(val, 2)


# =============================================================
# DATA RETRIEVAL
# =============================================================


def _get_fo_tables():
    """Get list of F&O tables (excluding _DERIVED)."""
    try:
        query = text(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name LIKE 'TBL_%'
            AND table_name NOT LIKE '%_DERIVED'
            ORDER BY table_name
            LIMIT 500;
        """
        )
        with engine.connect() as conn:
            result = conn.execute(query)
            return [row[0] for row in result]
    except Exception as e:
        print(f"[ERROR] _get_fo_tables(): {e}")
        return []


@lru_cache(maxsize=1)
def get_scanner_dates():
    """Get available dates for the scanner."""
    try:
        tables = _get_fo_tables()
        if not tables:
            return []

        # Try first few tables to get dates
        for table in tables[:5]:
            try:
                query = text(
                    f"""
                    SELECT DISTINCT "BizDt"::date::text AS date
                    FROM public."{table}"
                    WHERE "BizDt" IS NOT NULL
                    ORDER BY date DESC
                    LIMIT 60;
                """
                )
                with engine.connect() as conn:
                    result = conn.execute(query)
                    dates = [row[0] for row in result]
                    if dates:
                        return dates
            except:
                continue
        return []
    except Exception as e:
        print(f"[ERROR] get_scanner_dates(): {e}")
        return []


def _load_fo_data(start_date: str):
    """Load F&O data from all tables since start_date."""
    tables = _get_fo_tables()
    print(f"[DEBUG] Found {len(tables)} tables. First 5: {tables[:5]}")

    if not tables:
        print("[DEBUG] No tables found!")
        return pd.DataFrame()

    all_data = []
    batch_size = 30
    errors_count = 0

    for i in range(0, len(tables), batch_size):
        batch_tables = tables[i : i + batch_size]

        for table in batch_tables:
            try:
                query = text(
                    f"""
                    SELECT
                        "BizDt" as "Timestamp",
                        "Sgmt" as "Segment",
                        "TckrSymb" as "Symbol",
                        CAST("HghPric" AS NUMERIC) as "HghPric",
                        CAST("LwPric" AS NUMERIC) as "LwPric",
                        CAST("ClsPric" AS NUMERIC) as "ClsPric",
                        CAST("OpnPric" AS NUMERIC) as "OpnPric",
                        CAST("TtlTradgVol" AS NUMERIC) as "Volume",
                        CAST("OpnIntrst" AS NUMERIC) as "OI",
                        "FininstrmActlXpryDt" as "Expiry",
                        CAST("StrkPric" AS NUMERIC) as "Strike",
                        "OptnTp" as "OptionType",
                        CAST("UndrlygPric" AS NUMERIC) as "Spot"
                    FROM public."{table}"
                    WHERE "BizDt" >= :start_date
                    AND "ClsPric" IS NOT NULL
                    AND CAST("ClsPric" AS NUMERIC) > 0
                    ORDER BY "BizDt"
                """
                )
                df = pd.read_sql(query, con=engine, params={"start_date": start_date})
                if not df.empty:
                    all_data.append(df)
                    if len(all_data) <= 3:
                        print(f"[DEBUG] {table}: {len(df)} rows loaded")
            except Exception as e:
                errors_count += 1
                if errors_count <= 3:
                    print(f"[DEBUG] Error in {table}: {e}")
                continue

    print(f"[DEBUG] Total tables with data: {len(all_data)}, Total errors: {errors_count}")

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()


# =============================================================
# MAIN SCANNER FUNCTION
# =============================================================


def run_signal_scanner(start_date: str = None, days_back: int = 7, limit: int = 10000):
    """
    Run the signal scanner and return results.
    OPTIMIZED: Reads from 'daily_signal_scanner' cache table.


    Args:
        start_date: Starting date (YYYY-MM-DD) or None for auto-calculate
        days_back: Number of days to look back (default: 7, reduced from 30)
        limit: Maximum rows to fetch from database (default: 10000)
    """
    if not start_date:
        # Default to 7 days back (reduced from 30 for performance)
        start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    print(f"[INFO] Fetching cached signals from {start_date} (limit: {limit:,} rows)...")

    # Query Cache Table - Multi-Contract
    # JSONB columns are returned as dicts by psycopg2 automatically
    query = text(
        """
        SELECT
            date, symbol, expiry, strike, option_type,
            close, volume, oi, rsi, indicators, signals, metadata
        FROM daily_signal_scanner
        WHERE date >= :start_date
        ORDER BY date DESC, symbol ASC, expiry ASC, strike ASC
        LIMIT :limit
    """
    )

    import json

    try:
        with engine.connect() as conn:
            result = conn.execute(query, {"start_date": start_date, "limit": limit})
            rows = result.fetchall()

        if not rows:
            print("[WARN] No cached data found. Please run signal_scanner_cache.py")
            return []

        print(f"[DEBUG] Fetched {len(rows)} rows from database")

        # Transform to expected dict format
        output_rows = []
        for row in rows:
            # 0:date, 1:symbol, 2:expiry, 3:strike, 4:option_type
            # 5:close, 6:volume, 7:oi, 8:rsi, 9:indicators, 10:signals, 11:metadata

            # psycopg2 returns JSONB columns as dicts automatically
            indicators = row[9] if row[9] else {}
            signals = row[10] if row[10] else {}
            meta = row[11] if row[11] else {}

            # Helper safely gets keys
            def get_ind(k, default=0.0):
                val = indicators.get(k, default)
                if val is None:
                    return default
                return float(val)

            item = {
                "signal_date": str(row[0]),
                "symbol": str(row[1]),
                "expiry": str(row[2]),
                "strike": float(row[3] or 0),
                "option_type": str(row[4] or ""),
                "close": float(row[5] or 0),
                "volume": int(row[6] or 0),
                "oi": int(row[7] or 0),
                "rsi": float(row[8] or 0),
                # Indicators
                "pp": get_ind("pp"),
                "r1": get_ind("r1"),
                "s1": get_ind("s1"),
                "r2": get_ind("r2"),
                "s2": get_ind("s2"),
                "r3": get_ind("r3"),
                "s3": get_ind("s3"),
                "poc": get_ind("poc"),
                "vah": get_ind("vah"),
                "val": get_ind("val"),
                # Signals
                "high_volume": bool(signals.get("high_volume")),
                "oi_spike": bool(signals.get("oi_spike")),
                "rsi_trend": str(signals.get("rsi_trend") or "NEUTRAL"),
                "rsi_cross": signals.get("rsi_cross"),  # string or None
                "pivot_signal": signals.get("pivot_signal"),
                "divergence": signals.get("divergence"),
                # Metadata (Fallback/Extras)
                "high": float(meta.get("high") or 0),
                "low": float(meta.get("low") or 0),
                "spot": float(meta.get("spot") or 0),
            }
            output_rows.append(item)

            if len(output_rows) <= 3:
                print(
                    f"[DEBUG] Processed row {len(output_rows)}: {item['symbol']} {item['option_type']} Strike:{item['strike']}"
                )

        print(f"[INFO] Loaded {len(output_rows)} cached signals in < 0.1s")
        return output_rows

    except Exception as e:
        print(f"[ERROR] run_signal_scanner failed: {e}")
        import traceback

        traceback.print_exc()
        print(f"[DEBUG] Exception type: {type(e)}")
        print(f"[DEBUG] Exception args: {e.args}")
        return []


def get_scanner_summary(signals: list):
    """Generate summary statistics from scanner results."""
    if not signals:
        return {}

    df = pd.DataFrame(signals)

    return {
        "total_signals": int(len(signals)),
        "unique_symbols": int(df["symbol"].nunique()),
        "high_volume_count": int(df["high_volume"].sum()),
        "oi_spike_count": int(df["oi_spike"].sum()),
        "pivot_signals": int((df["pivot_signal"].notna()).sum()),
        "bull_divergence": int((df["divergence"] == "BULL").sum()),
        "bear_divergence": int((df["divergence"] == "BEAR").sum()),
        "rsi_cross_up": int((df["rsi_cross"] == "UP").sum()),
        "rsi_cross_down": int((df["rsi_cross"] == "DOWN").sum()),
        "oversold_count": int((df["rsi_trend"] == "OVERSOLD").sum()),
        "overbought_count": int((df["rsi_trend"] == "OVERBOUGHT").sum()),
    }


def filter_signals(signals: list, filters: dict):
    """Filter signals based on criteria."""
    if not signals:
        return signals

    filtered = signals.copy()

    # Filter by signal type
    signal_type = filters.get("signal_type", "all")
    if signal_type == "high_volume":
        filtered = [s for s in filtered if s["high_volume"]]
    elif signal_type == "oi_spike":
        filtered = [s for s in filtered if s["oi_spike"]]
    elif signal_type == "pivot":
        filtered = [s for s in filtered if s["pivot_signal"]]
    elif signal_type == "divergence":
        filtered = [s for s in filtered if s["divergence"]]
    elif signal_type == "rsi_cross":
        filtered = [s for s in filtered if s["rsi_cross"]]
    elif signal_type == "oversold":
        filtered = [s for s in filtered if s["rsi_trend"] == "OVERSOLD"]
    elif signal_type == "overbought":
        filtered = [s for s in filtered if s["rsi_trend"] == "OVERBOUGHT"]

    # Filter by option type
    option_type = filters.get("option_type", "all")
    if option_type == "CE":
        filtered = [s for s in filtered if s["option_type"] == "CE"]
    elif option_type == "PE":
        filtered = [s for s in filtered if s["option_type"] == "PE"]
    elif option_type == "FUT":
        filtered = [s for s in filtered if not s["option_type"] or s["option_type"] == ""]

    # Filter by symbol search
    symbol_search = filters.get("symbol", "").upper().strip()
    if symbol_search:
        filtered = [s for s in filtered if symbol_search in s["symbol"].upper()]

    # Sort
    sort_by = filters.get("sort_by", "signal_date")
    sort_order = filters.get("sort_order", "desc")
    reverse = sort_order == "desc"

    if sort_by == "signal_date":
        filtered.sort(key=lambda x: x.get("signal_date", ""), reverse=reverse)
    elif sort_by == "symbol":
        filtered.sort(key=lambda x: x.get("symbol", ""), reverse=reverse)
    elif sort_by == "rsi":
        filtered.sort(key=lambda x: x.get("rsi", 0), reverse=reverse)
    elif sort_by == "volume":
        filtered.sort(key=lambda x: x.get("volume", 0), reverse=reverse)
    elif sort_by == "oi":
        filtered.sort(key=lambda x: x.get("oi", 0), reverse=reverse)

    return filtered


def clear_scanner_cache():
    """Clear scanner caches."""
    get_scanner_dates.cache_clear()
    print("[INFO] Scanner cache cleared")
