"""
TECHNICAL SCREENER CACHE BUILDER
================================
Pre-calculates technical indicators (RSI, MACD, SMA, Bollinger Bands, ADX)
Stores in technical_screener_cache table for fast retrieval

INCREMENTAL MODE: Only processes NEW dates, never drops existing data
"""

import os
import sys

# Add project root to path to allow imports from Analysis_Tools
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, inspect, text

# Database config
from Analysis_Tools.app.models.db_config import engine, get_available_dates


def create_technical_screener_table():
    """Create the technical_screener_cache table if not exists (NO DROP)"""
    create_query = """
    CREATE TABLE IF NOT EXISTS public.technical_screener_cache (
        id SERIAL PRIMARY KEY,
        cache_date DATE NOT NULL,
        ticker VARCHAR(50) NOT NULL,
        underlying_price NUMERIC,

        -- RSI
        rsi_14 NUMERIC,
        rsi_above_80 BOOLEAN DEFAULT FALSE,
        rsi_60_80 BOOLEAN DEFAULT FALSE,
        rsi_40_60 BOOLEAN DEFAULT FALSE,
        rsi_20_40 BOOLEAN DEFAULT FALSE,
        rsi_below_20 BOOLEAN DEFAULT FALSE,

        -- MACD
        macd NUMERIC,
        macd_signal NUMERIC,
        macd_histogram NUMERIC,
        macd_pos_cross BOOLEAN DEFAULT FALSE,
        macd_neg_cross BOOLEAN DEFAULT FALSE,

        -- SMA
        sma_50 NUMERIC,
        sma_200 NUMERIC,
        above_50_sma BOOLEAN DEFAULT FALSE,
        above_200_sma BOOLEAN DEFAULT FALSE,
        below_50_sma BOOLEAN DEFAULT FALSE,
        below_200_sma BOOLEAN DEFAULT FALSE,
        dist_from_50sma_pct NUMERIC,
        dist_from_200sma_pct NUMERIC,

        -- Bollinger Bands
        bb_upper NUMERIC,
        bb_middle NUMERIC,
        bb_lower NUMERIC,
        bb_width NUMERIC,

        -- ADX
        adx_14 NUMERIC,
        strong_trend BOOLEAN DEFAULT FALSE,

        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(cache_date, ticker)
    );

    CREATE INDEX IF NOT EXISTS idx_tech_cache_date ON public.technical_screener_cache(cache_date);
    CREATE INDEX IF NOT EXISTS idx_tech_cache_ticker ON public.technical_screener_cache(ticker);
    CREATE INDEX IF NOT EXISTS idx_tech_cache_rsi ON public.technical_screener_cache(rsi_14);
    CREATE INDEX IF NOT EXISTS idx_tech_cache_adx ON public.technical_screener_cache(adx_14);
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(create_query))
        print("✓ technical_screener_cache table ready")
    except Exception as e:
        print(f"✗ Error creating table: {e}")

    # Add new columns for pivot points, breakouts, momentum, and BB squeeze
    alter_queries = """
    -- Pivot Points
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS pivot_point NUMERIC;
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS r1 NUMERIC;
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS r2 NUMERIC;
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS r3 NUMERIC;
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS s1 NUMERIC;
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS s2 NUMERIC;
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS s3 NUMERIC;

    -- Breakout Detection Flags
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS r1_breakout BOOLEAN DEFAULT FALSE;
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS r2_breakout BOOLEAN DEFAULT FALSE;
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS r3_breakout BOOLEAN DEFAULT FALSE;
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS s1_breakout BOOLEAN DEFAULT FALSE;
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS s2_breakout BOOLEAN DEFAULT FALSE;
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS s3_breakout BOOLEAN DEFAULT FALSE;

    -- Momentum and Squeeze
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS momentum_score NUMERIC;
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS is_high_momentum BOOLEAN DEFAULT FALSE;
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS bb_squeeze BOOLEAN DEFAULT FALSE;

    -- NEW: Price & Volume Screener Columns
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS open_price NUMERIC;
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS high_price NUMERIC;
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS low_price NUMERIC;
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS volume BIGINT;
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS price_change_pct NUMERIC;
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS volume_change_pct NUMERIC;

    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS week1_high NUMERIC;
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS week1_low NUMERIC;
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS week4_high NUMERIC;
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS week4_low NUMERIC;
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS week52_high NUMERIC;
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS week52_low NUMERIC;

    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS is_week1_high_breakout BOOLEAN DEFAULT FALSE;
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS is_week1_low_breakout BOOLEAN DEFAULT FALSE;
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS is_week4_high_breakout BOOLEAN DEFAULT FALSE;
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS is_week4_low_breakout BOOLEAN DEFAULT FALSE;
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS is_week52_high_breakout BOOLEAN DEFAULT FALSE;
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS is_week52_low_breakout BOOLEAN DEFAULT FALSE;

    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS is_potential_high_vol BOOLEAN DEFAULT FALSE;
    ALTER TABLE public.technical_screener_cache ADD COLUMN IF NOT EXISTS is_unusually_high_vol BOOLEAN DEFAULT FALSE;

    -- Indexes for new columns
    CREATE INDEX IF NOT EXISTS idx_tech_r1_breakout ON public.technical_screener_cache(r1_breakout) WHERE r1_breakout = TRUE;
    CREATE INDEX IF NOT EXISTS idx_tech_r2_breakout ON public.technical_screener_cache(r2_breakout) WHERE r2_breakout = TRUE;
    CREATE INDEX IF NOT EXISTS idx_tech_r3_breakout ON public.technical_screener_cache(r3_breakout) WHERE r3_breakout = TRUE;
    CREATE INDEX IF NOT EXISTS idx_tech_s1_breakout ON public.technical_screener_cache(s1_breakout) WHERE s1_breakout = TRUE;
    CREATE INDEX IF NOT EXISTS idx_tech_s2_breakout ON public.technical_screener_cache(s2_breakout) WHERE s2_breakout = TRUE;
    CREATE INDEX IF NOT EXISTS idx_tech_s3_breakout ON public.technical_screener_cache(s3_breakout) WHERE s3_breakout = TRUE;
    CREATE INDEX IF NOT EXISTS idx_tech_momentum ON public.technical_screener_cache(is_high_momentum) WHERE is_high_momentum = TRUE;
    CREATE INDEX IF NOT EXISTS idx_tech_squeeze ON public.technical_screener_cache(bb_squeeze) WHERE bb_squeeze = TRUE;

    CREATE INDEX IF NOT EXISTS idx_tech_w1_hi_bo ON public.technical_screener_cache(is_week1_high_breakout) WHERE is_week1_high_breakout = TRUE;
    CREATE INDEX IF NOT EXISTS idx_tech_w1_lo_bo ON public.technical_screener_cache(is_week1_low_breakout) WHERE is_week1_low_breakout = TRUE;
    CREATE INDEX IF NOT EXISTS idx_tech_w4_hi_bo ON public.technical_screener_cache(is_week4_high_breakout) WHERE is_week4_high_breakout = TRUE;
    CREATE INDEX IF NOT EXISTS idx_tech_w4_lo_bo ON public.technical_screener_cache(is_week4_low_breakout) WHERE is_week4_low_breakout = TRUE;
    CREATE INDEX IF NOT EXISTS idx_tech_w52_hi_bo ON public.technical_screener_cache(is_week52_high_breakout) WHERE is_week52_high_breakout = TRUE;
    CREATE INDEX IF NOT EXISTS idx_tech_w52_lo_bo ON public.technical_screener_cache(is_week52_low_breakout) WHERE is_week52_low_breakout = TRUE;
    CREATE INDEX IF NOT EXISTS idx_tech_pot_hi_vol ON public.technical_screener_cache(is_potential_high_vol) WHERE is_potential_high_vol = TRUE;
    CREATE INDEX IF NOT EXISTS idx_tech_unu_hi_vol ON public.technical_screener_cache(is_unusually_high_vol) WHERE is_unusually_high_vol = TRUE;
    """
    try:
        with engine.begin() as conn:
            for stmt in alter_queries.strip().split(';'):
                stmt = stmt.strip()
                if stmt:
                    conn.execute(text(stmt))
        print("✓ New columns (pivot points, breakouts, momentum, squeeze) added")
    except Exception as e:
        print(f"⚠ Column addition note: {e}")


def get_cached_dates():
    """Get dates already in cache"""
    try:
        q = text("SELECT DISTINCT cache_date FROM public.technical_screener_cache ORDER BY cache_date")
        df = pd.read_sql(q, engine)
        return set(d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d) for d in df["cache_date"])
    except:
        return set()


def get_derived_tables():
    """Get all DERIVED tables"""
    inspector = inspect(engine)
    tables = [t for t in inspector.get_table_names() if t.endswith("_DERIVED")]
    return sorted(tables)


# Technical indicator calculations
def calculate_rsi(close, period=14):
    delta = close.diff()
    gain = delta.where(delta > 0, 0)
    loss = (-delta).where(delta < 0, 0)

    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()

    rs = avg_gain / avg_loss.replace(0, np.inf)
    rsi = 100 - (100 / (1 + rs))
    return rsi


def calculate_macd(close, fast=12, slow=26, signal=9):
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()

    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram


def calculate_sma(close, period):
    return close.rolling(window=period, min_periods=period).mean()


def calculate_bollinger_bands(close, period=20, std_dev=2):
    sma = close.rolling(window=period, min_periods=period).mean()
    rolling_std = close.rolling(window=period, min_periods=period).std()

    upper = sma + (rolling_std * std_dev)
    lower = sma - (rolling_std * std_dev)
    width = (upper - lower) / sma * 100
    return upper, sma, lower, width


def calculate_adx(close, period=14):
    high = close * 1.001
    low = close * 0.999

    plus_dm = high.diff()
    minus_dm = (-low).diff()

    plus_dm = plus_dm.where(plus_dm > 0, 0)
    minus_dm = minus_dm.where(minus_dm > 0, 0)

    tr = close.diff().abs()

    atr = tr.rolling(window=period, min_periods=period).mean()
    plus_di = 100 * (plus_dm.rolling(window=period, min_periods=period).mean() / atr.replace(0, np.inf))
    minus_di = 100 * (minus_dm.rolling(window=period, min_periods=period).mean() / atr.replace(0, np.inf))

    dx = 100 * ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.inf))
    adx = dx.rolling(window=period, min_periods=period).mean()
    return adx


def calculate_pivot_points(close):
    """
    Calculate classic pivot points using previous day's close.
    Since we only have close prices, we approximate high/low as close +/- 2%.
    Returns: pivot, r1, r2, r3, s1, s2, s3 (all as Series)
    """
    prev_close = close.shift(1)
    prev_high = prev_close * 1.02   # Approximate high
    prev_low = prev_close * 0.98    # Approximate low

    pivot = (prev_high + prev_low + prev_close) / 3

    r1 = (2 * pivot) - prev_low
    r2 = pivot + (prev_high - prev_low)
    r3 = prev_high + 2 * (pivot - prev_low)

    s1 = (2 * pivot) - prev_high
    s2 = pivot - (prev_high - prev_low)
    s3 = prev_low - 2 * (prev_high - pivot)

    return pivot, r1, r2, r3, s1, s2, s3


def calculate_momentum_score(close, lookback=10):
    """
    Calculate momentum score based on price change percentage over N days.
    Momentum = Price Change % over lookback period
    """
    price_change_pct = ((close - close.shift(lookback)) / close.shift(lookback)) * 100
    return price_change_pct


def precalculate_technical_screener_cache():
    """
    INCREMENTAL: Only processes NEW dates, appends to existing cache
    """
    print("\n" + "=" * 70)
    print("TECHNICAL SCREENER CACHE BUILDER (INCREMENTAL MODE)")
    print("=" * 70)

    # Create table if needed (NO DROP)
    create_technical_screener_table()

    # Get dates
    all_dates = get_available_dates(engine)
    cached_dates = get_cached_dates()
    tables = get_derived_tables()

    if not all_dates or not tables:
        print("✗ No data available")
        return

    # Find NEW dates only
    new_dates = set(d for d in all_dates if d not in cached_dates)

    print(f"📅 Total dates in database: {len(all_dates)}")
    print(f"📂 Already cached: {len(cached_dates)}")
    print(f"🆕 NEW dates to process: {len(new_dates)}")
    print(f"📊 Tickers: {len(tables)}")

    if not new_dates:
        print("\n✅ Cache is up to date! No new dates to process.")
        return

    min_days = 50

    if len(all_dates) < min_days:
        print(f"✗ Need at least {min_days} days of data, only have {len(all_dates)}")
        return

    # Process each ticker
    all_cache_rows = []

    for ticker_idx, table in enumerate(tables, 1):
        ticker = table.replace("TBL_", "").replace("_DERIVED", "")
        print(f"\n[{ticker_idx}/{len(tables)}] {ticker}...", end=" ", flush=True)

        try:
            # Get all price history for this ticker
            q = text(
                f"""
                SELECT DISTINCT "BizDt" AS date, "UndrlygPric" AS close,
                       "OpnPric" AS open, "HghPric" AS high, "LwPric" AS low, "TtlTradgVol" AS volume
                FROM "{table}"
                WHERE "BizDt" IS NOT NULL AND "FinInstrmTp" = 'STF'
                ORDER BY "BizDt"
            """
            )
            df = pd.read_sql(q, engine)

            if df.empty or len(df) < min_days:
                print(f"⚠ Only {len(df)} days", end="")
                continue

            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").drop_duplicates(subset=["date"], keep="first")
            df["close"] = pd.to_numeric(df["close"], errors="coerce")
            df["open"] = pd.to_numeric(df["open"], errors="coerce")
            df["high"] = pd.to_numeric(df["high"], errors="coerce")
            df["low"] = pd.to_numeric(df["low"], errors="coerce")
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
            df = df.dropna(subset=["close"])
            df = df.set_index("date")

            if len(df) < min_days:
                print(f"⚠ Only {len(df)} valid days", end="")
                continue

            close = df["close"]

            # Calculate indicators for entire history
            rsi = calculate_rsi(close, 14)
            macd_line, signal_line, histogram = calculate_macd(close, 12, 26, 9)
            sma_50 = calculate_sma(close, 50)
            sma_200 = calculate_sma(close, 200)
            bb_upper, bb_middle, bb_lower, bb_width = calculate_bollinger_bands(close, 20, 2)
            adx = calculate_adx(close, 14)

            # NEW: Calculate pivot points, momentum, and squeeze
            pivot, r1, r2, r3, s1, s2, s3 = calculate_pivot_points(close)
            momentum = calculate_momentum_score(close, lookback=10)

            # Create cache rows for NEW dates only
            ticker_rows = 0
            for i in range(len(df)):
                date_str = df.index[i].strftime("%Y-%m-%d")

                # ONLY process NEW dates
                if date_str not in new_dates:
                    continue

                latest_close = close.iloc[i]
                latest_rsi = rsi.iloc[i] if i < len(rsi) else None
                latest_macd = macd_line.iloc[i] if i < len(macd_line) else None
                latest_signal = signal_line.iloc[i] if i < len(signal_line) else None
                latest_histogram = histogram.iloc[i] if i < len(histogram) else None
                latest_sma_50 = sma_50.iloc[i] if i < len(sma_50) else None
                latest_sma_200 = sma_200.iloc[i] if i < len(sma_200) else None
                latest_bb_upper = bb_upper.iloc[i] if i < len(bb_upper) else None
                latest_bb_middle = bb_middle.iloc[i] if i < len(bb_middle) else None
                latest_bb_lower = bb_lower.iloc[i] if i < len(bb_lower) else None
                latest_bb_width = bb_width.iloc[i] if i < len(bb_width) else None
                latest_adx = adx.iloc[i] if i < len(adx) else None

                # Check for MACD crossover
                macd_pos_cross = False
                macd_neg_cross = False
                if i > 0:
                    prev_macd = macd_line.iloc[i - 1] if i - 1 < len(macd_line) else None
                    prev_signal = signal_line.iloc[i - 1] if i - 1 < len(signal_line) else None

                    if (
                        pd.notnull(prev_macd)
                        and pd.notnull(prev_signal)
                        and pd.notnull(latest_macd)
                        and pd.notnull(latest_signal)
                    ):
                        if prev_macd < prev_signal and latest_macd > latest_signal:
                            macd_pos_cross = True
                        if prev_macd > prev_signal and latest_macd < latest_signal:
                            macd_neg_cross = True

                # NEW: Get pivot, momentum values for this row
                latest_pivot = pivot.iloc[i] if i < len(pivot) and pd.notnull(pivot.iloc[i]) else None
                latest_r1 = r1.iloc[i] if i < len(r1) and pd.notnull(r1.iloc[i]) else None
                latest_r2 = r2.iloc[i] if i < len(r2) and pd.notnull(r2.iloc[i]) else None
                latest_r3 = r3.iloc[i] if i < len(r3) and pd.notnull(r3.iloc[i]) else None
                latest_s1 = s1.iloc[i] if i < len(s1) and pd.notnull(s1.iloc[i]) else None
                latest_s2 = s2.iloc[i] if i < len(s2) and pd.notnull(s2.iloc[i]) else None
                latest_s3 = s3.iloc[i] if i < len(s3) and pd.notnull(s3.iloc[i]) else None
                latest_momentum = momentum.iloc[i] if i < len(momentum) and pd.notnull(momentum.iloc[i]) else None

                # NEW: Price & Volume Metrics
                # ---------------------------
                # Rolling Highs/Lows for Breakouts
                # We need historical data up to this point 'i'
                # 5-day (1 week), 20-day (4 weeks), 250-day (52 weeks)

                # Get history window ending at i (inclusive)
                # Ensure we have enough data points

                week1_high = week1_low = None
                week4_high = week4_low = None
                week52_high = week52_low = None

                vol_sma_20 = None
                is_week1_high_bo = is_week1_low_bo = False
                is_week4_high_bo = is_week4_low_bo = False
                is_week52_high_bo = is_week52_low_bo = False

                is_pot_high_vol = False
                is_unusually_high_vol = False

                try:
                    # Rolling windows calculation (using array slicing for speed)
                    # We need PREVIOUS highs to check for breakout (i.e. Close > Max of prev N days)
                    # But typically breakout means Close > Max of Last N Days (including today? or excluding?)
                    # ScanX "Breakouts" usually mean "Crossed above the High of last N days TODAY"
                    # So we compare Today's Close vs Max of (Day-1 to Day-N)

                    if i >= 5:
                         prev_5_high = close.iloc[i-5:i].max()
                         prev_5_low = close.iloc[i-5:i].min()
                         week1_high = prev_5_high
                         week1_low = prev_5_low
                         is_week1_high_bo = bool(latest_close > prev_5_high)
                         is_week1_low_bo = bool(latest_close < prev_5_low)

                    if i >= 20:
                         prev_20_high = close.iloc[i-20:i].max()
                         prev_20_low = close.iloc[i-20:i].min()
                         week4_high = prev_20_high
                         week4_low = prev_20_low
                         is_week4_high_bo = bool(latest_close > prev_20_high)
                         is_week4_low_bo = bool(latest_close < prev_20_low)

                         # Volume SMA 20
                         if 'volume' in df.columns:
                             vol_window = df['volume'].iloc[i-20:i]
                             vol_sma_20 = vol_window.mean()
                             current_vol = df['volume'].iloc[i]

                             if vol_sma_20 and vol_sma_20 > 0:
                                 if current_vol > (1.5 * vol_sma_20):
                                     is_pot_high_vol = True
                                 if current_vol > (2.5 * vol_sma_20):
                                     is_unusually_high_vol = True

                    if i >= 250:
                         prev_250_high = close.iloc[i-250:i].max()
                         prev_250_low = close.iloc[i-250:i].min()
                         week52_high = prev_250_high
                         week52_low = prev_250_low
                         is_week52_high_bo = bool(latest_close > prev_250_high)
                         is_week52_low_bo = bool(latest_close < prev_250_low)
                    elif i >= 50: # Partial 52-week (fallback to max available if > 50 days)
                         prev_max = close.iloc[:i].max()
                         prev_min = close.iloc[:i].min()
                         week52_high = prev_max
                         week52_low = prev_min
                         is_week52_high_bo = bool(latest_close > prev_max)
                         is_week52_low_bo = bool(latest_close < prev_min)

                except Exception as e:
                    # Safety net for calculation errors
                    pass

                # Basic Price/Volume Data
                # -----------------------
                latest_open = df.iloc[i].get('open') if 'open' in df.columns else None
                latest_high = df.iloc[i].get('high') if 'high' in df.columns else None
                latest_low = df.iloc[i].get('low') if 'low' in df.columns else None
                latest_volume = df.iloc[i].get('volume') if 'volume' in df.columns else None

                # Change %
                price_change_pct = 0
                vol_change_pct = 0

                if i > 0:
                    prev_close = close.iloc[i-1]
                    price_change_pct = ((latest_close - prev_close) / prev_close) * 100

                    if 'volume' in df.columns:
                        prev_vol = df['volume'].iloc[i-1]
                        if prev_vol > 0:
                            vol_change_pct = ((latest_volume - prev_vol) / prev_vol) * 100


                # Breakout detection
                r1_bo = bool(latest_close > latest_r1) if latest_r1 is not None else False
                r2_bo = bool(latest_close > latest_r2) if latest_r2 is not None else False
                r3_bo = bool(latest_close > latest_r3) if latest_r3 is not None else False
                s1_bo = bool(latest_close < latest_s1) if latest_s1 is not None else False
                s2_bo = bool(latest_close < latest_s2) if latest_s2 is not None else False
                s3_bo = bool(latest_close < latest_s3) if latest_s3 is not None else False

                # BB Squeeze detection (width < 2%)
                bb_squeeze_flag = bool(latest_bb_width < 5.0) if pd.notnull(latest_bb_width) else False

                # High momentum (score > 5)
                high_momentum_flag = bool(latest_momentum > 5.0) if latest_momentum is not None else False

                row = {
                    "cache_date": date_str,
                    "ticker": ticker,
                    "underlying_price": latest_close,

                    # NEW: Price & Volume
                    "open_price": latest_open,
                    "high_price": latest_high,
                    "low_price": latest_low,
                    "volume": int(latest_volume) if latest_volume is not None and pd.notnull(latest_volume) else 0,
                    "price_change_pct": price_change_pct,
                    "volume_change_pct": vol_change_pct,

                    "week1_high": week1_high, "week1_low": week1_low,
                    "week4_high": week4_high, "week4_low": week4_low,
                    "week52_high": week52_high, "week52_low": week52_low,
                    #"vol_sma_20": vol_sma_20,

                    "is_week1_high_breakout": is_week1_high_bo,
                    "is_week1_low_breakout": is_week1_low_bo,
                    "is_week4_high_breakout": is_week4_high_bo,
                    "is_week4_low_breakout": is_week4_low_bo,
                    "is_week52_high_breakout": is_week52_high_bo,
                    "is_week52_low_breakout": is_week52_low_bo,
                    "is_potential_high_vol": is_pot_high_vol,
                    "is_unusually_high_vol": is_unusually_high_vol,

                    "rsi_14": latest_rsi if pd.notnull(latest_rsi) else None,
                    "rsi_above_80": bool(latest_rsi > 80) if pd.notnull(latest_rsi) else False,
                    "rsi_60_80": bool(60 < latest_rsi <= 80) if pd.notnull(latest_rsi) else False,
                    "rsi_40_60": bool(40 <= latest_rsi <= 60) if pd.notnull(latest_rsi) else False,
                    "rsi_20_40": bool(20 <= latest_rsi < 40) if pd.notnull(latest_rsi) else False,
                    "rsi_below_20": bool(latest_rsi < 20) if pd.notnull(latest_rsi) else False,
                    "macd": latest_macd if pd.notnull(latest_macd) else None,
                    "macd_signal": latest_signal if pd.notnull(latest_signal) else None,
                    "macd_histogram": latest_histogram if pd.notnull(latest_histogram) else None,
                    "macd_pos_cross": macd_pos_cross,
                    "macd_neg_cross": macd_neg_cross,
                    "sma_50": latest_sma_50 if pd.notnull(latest_sma_50) else None,
                    "sma_200": latest_sma_200 if pd.notnull(latest_sma_200) else None,
                    "above_50_sma": bool(latest_close > latest_sma_50) if pd.notnull(latest_sma_50) else False,
                    "above_200_sma": bool(latest_close > latest_sma_200) if pd.notnull(latest_sma_200) else False,
                    "below_50_sma": bool(latest_close < latest_sma_50) if pd.notnull(latest_sma_50) else False,
                    "below_200_sma": bool(latest_close < latest_sma_200) if pd.notnull(latest_sma_200) else False,
                    "dist_from_50sma_pct": ((latest_close - latest_sma_50) / latest_sma_50 * 100)
                    if pd.notnull(latest_sma_50) and latest_sma_50 != 0
                    else None,
                    "dist_from_200sma_pct": ((latest_close - latest_sma_200) / latest_sma_200 * 100)
                    if pd.notnull(latest_sma_200) and latest_sma_200 != 0
                    else None,
                    "bb_upper": latest_bb_upper if pd.notnull(latest_bb_upper) else None,
                    "bb_middle": latest_bb_middle if pd.notnull(latest_bb_middle) else None,
                    "bb_lower": latest_bb_lower if pd.notnull(latest_bb_lower) else None,
                    "bb_width": latest_bb_width if pd.notnull(latest_bb_width) else None,
                    "adx_14": latest_adx if pd.notnull(latest_adx) else None,
                    "strong_trend": bool(latest_adx > 25) if pd.notnull(latest_adx) else False,
                    # NEW: Pivot points
                    "pivot_point": latest_pivot,
                    "r1": latest_r1,
                    "r2": latest_r2,
                    "r3": latest_r3,
                    "s1": latest_s1,
                    "s2": latest_s2,
                    "s3": latest_s3,
                    # NEW: Breakout flags
                    "r1_breakout": r1_bo,
                    "r2_breakout": r2_bo,
                    "r3_breakout": r3_bo,
                    "s1_breakout": s1_bo,
                    "s2_breakout": s2_bo,
                    "s3_breakout": s3_bo,
                    # NEW: Momentum and squeeze
                    "momentum_score": latest_momentum,
                    "is_high_momentum": high_momentum_flag,
                    "bb_squeeze": bb_squeeze_flag,
                }

                all_cache_rows.append(row)
                ticker_rows += 1

            if ticker_rows > 0:
                print(f"✓ {ticker_rows} new rows", end="")
            else:
                print("⚠ No new rows", end="")

        except Exception as e:
            print(f"✗ Error: {e}", end="")

    # Insert all NEW rows into database
    if all_cache_rows:
        print(f"\n\n📥 Inserting {len(all_cache_rows)} total new rows into database...")
        try:
            insert_df = pd.DataFrame(all_cache_rows)
            insert_df.to_sql("technical_screener_cache", con=engine, if_exists="append", index=False)
            print("✓ Insert complete!")
        except Exception as e:
            print(f"✗ Insert error: {e}")
    else:
        print("\n\n✅ No new rows to insert.")

    print("\n" + "=" * 70)
    print("✓ TECHNICAL SCREENER CACHE UPDATE COMPLETE!")
    print("=" * 70)


if __name__ == "__main__":
    precalculate_technical_screener_cache()
