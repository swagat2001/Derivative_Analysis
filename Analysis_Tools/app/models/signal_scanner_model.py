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


def run_signal_scanner(start_date: str = None, days_back: int = 30):
    """
    Run the signal scanner and return results.

    Returns list of signal dictionaries with:
    - Symbol, Expiry, Strike, OptionType, Spot
    - High, Low, Close, Volume, OI
    - Pivot Levels, Volume Profile
    - RSI signals, Volume/OI signals
    """
    if not start_date:
        # Default to 30 days back
        start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    print(f"[INFO] Running signal scanner from {start_date}...")

    # Load data
    data = _load_fo_data(start_date)

    if data.empty:
        print("[WARN] No data loaded for scanner")
        return []

    # Convert numeric columns
    numeric_cols = ["HghPric", "LwPric", "ClsPric", "OpnPric", "Volume", "OI", "Strike", "Spot"]
    for col in numeric_cols:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors="coerce")

    # Filter zero volume
    data = data[data["Volume"] > 0]

    # Sort by symbol and date
    data = data.sort_values(["Symbol", "Timestamp"]).reset_index(drop=True)

    symbols = data["Symbol"].unique()
    print(f"[INFO] Processing {len(symbols)} symbols...")

    output_rows = []
    skipped_min_data = 0
    skipped_na_close = 0
    skipped_rsi = 0
    skipped_vol_profile = 0

    for symbol in symbols:
        df = data[data["Symbol"] == symbol].reset_index(drop=True)

        if len(df) < MIN_DATA_POINTS:
            skipped_min_data += 1
            continue

        if df["ClsPric"].isna().all():
            skipped_na_close += 1
            continue

        # Calculate RSI
        try:
            df["RSI"] = calc_rsi(df["ClsPric"])
        except:
            skipped_rsi += 1
            continue

        # Calculate Volume Profile
        try:
            poc, vah, val = calc_volume_profile(df, bins=BINS)
            if poc is None:
                skipped_vol_profile += 1
                continue
        except:
            skipped_vol_profile += 1
            continue

        # Process each day (starting from day 15 for enough history)
        for i in range(15, len(df)):
            today = df.iloc[i]
            prev = df.iloc[i - 1]

            high = today.get("HghPric", 0)
            low = today.get("LwPric", 0)
            close = today.get("ClsPric", 0)
            volume = today.get("Volume", 0)
            oi = today.get("OI", 0)
            rsi = today.get("RSI", 0)

            # Skip invalid data
            if pd.isna(high) or pd.isna(low) or pd.isna(close) or high == low or close == 0:
                continue

            if volume < MIN_VOLUME:
                continue

            # Calculate Pivot Levels
            pp, r1, s1, r2, s2, r3, s3 = calc_pivot_levels(high, low, close)

            # Detect Pivot Signals (within 20% of day's range)
            day_range = high - low
            tolerance = day_range * 0.20

            s1_sup = abs(close - s1) <= tolerance
            s2_sup = abs(close - s2) <= tolerance
            s3_sup = abs(close - s3) <= tolerance
            r1_res = abs(close - r1) <= tolerance
            r2_res = abs(close - r2) <= tolerance
            r3_res = abs(close - r3) <= tolerance

            # High Volume (>1.5x previous)
            prev_vol = prev.get("Volume", 0)
            high_vol = volume > prev_vol * 1.5 if prev_vol > 0 else False

            # OI Spike (>15% increase)
            prev_oi = prev.get("OI", 0)
            oi_spike = prev_oi > 0 and oi > 0 and (oi - prev_oi) / prev_oi >= 0.15

            # RSI Divergence
            bull_div = False
            bear_div = False
            if i >= 2 and not pd.isna(rsi) and not pd.isna(df.iloc[i - 2]["RSI"]):
                price_LL = close < df.iloc[i - 2]["ClsPric"]
                price_HH = close > df.iloc[i - 2]["ClsPric"]
                rsi_HL = rsi > df.iloc[i - 2]["RSI"]
                rsi_LH = rsi < df.iloc[i - 2]["RSI"]
                bull_div = price_LL and rsi_HL
                bear_div = price_HH and rsi_LH

            # RSI Trend
            if pd.isna(rsi):
                rsi_signal = "NEUTRAL"
            elif rsi < 30:
                rsi_signal = "OVERSOLD"
            elif rsi > 70:
                rsi_signal = "OVERBOUGHT"
            else:
                rsi_signal = "NEUTRAL"

            # RSI Cross Signals
            prev_rsi = prev.get("RSI", 0)
            rsi_cross_up = False
            rsi_cross_down = False
            if not pd.isna(prev_rsi):
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

            # RSI Cross Label
            if rsi_cross_up:
                rsi_cross = "UP"
            elif rsi_cross_down:
                rsi_cross = "DOWN"
            else:
                rsi_cross = None

            # Divergence Label
            if bull_div:
                divergence = "BULL"
            elif bear_div:
                divergence = "BEAR"
            else:
                divergence = None

            # Only store if any signal is present
            if high_vol or oi_spike or pivot_signal or rsi_cross or divergence:
                signal_date = today.get("Timestamp", "")
                if hasattr(signal_date, "strftime"):
                    signal_date = signal_date.strftime("%Y-%m-%d")

                expiry = today.get("Expiry", "")
                if hasattr(expiry, "strftime"):
                    expiry = expiry.strftime("%Y-%m-%d")

                # Helper to safely convert numpy types to Python native types
                def to_python_int(val):
                    if pd.isna(val) or val is None:
                        return 0
                    return int(float(val))  # float first handles numpy types

                def to_python_float(val, decimals=2):
                    if pd.isna(val) or val is None:
                        return 0.0
                    return round(float(val), decimals)

                output_rows.append(
                    {
                        "signal_date": str(signal_date) if signal_date else "",
                        "expiry": str(expiry) if expiry else "",
                        "symbol": str(symbol),
                        "spot": to_python_float(today.get("Spot", 0)),
                        "strike": to_python_float(today.get("Strike", 0)),
                        "option_type": str(today.get("OptionType", "") or ""),
                        "high": to_python_float(high),
                        "low": to_python_float(low),
                        "close": to_python_float(close),
                        "volume": to_python_int(volume),
                        "oi": to_python_int(oi),
                        "pp": to_python_float(pp),
                        "r1": to_python_float(r1),
                        "s1": to_python_float(s1),
                        "r2": to_python_float(r2),
                        "s2": to_python_float(s2),
                        "r3": to_python_float(r3),
                        "s3": to_python_float(s3),
                        "poc": to_python_float(poc),
                        "vah": to_python_float(vah),
                        "val": to_python_float(val),
                        "rsi": to_python_float(rsi) if not pd.isna(rsi) else 0.0,
                        "rsi_trend": str(rsi_signal) if rsi_signal else "NEUTRAL",
                        "rsi_cross": str(rsi_cross) if rsi_cross else None,
                        "high_volume": bool(high_vol),
                        "oi_spike": bool(oi_spike),
                        "pivot_signal": str(pivot_signal) if pivot_signal else None,
                        "divergence": str(divergence) if divergence else None,
                    }
                )

    print(
        f"[DEBUG] Skipped: min_data={skipped_min_data}, na_close={skipped_na_close}, rsi={skipped_rsi}, vol_profile={skipped_vol_profile}"
    )
    print(f"[INFO] Scanner complete: {len(output_rows)} signals found")
    return output_rows


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
