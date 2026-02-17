"""
TECHNICAL SCREENER CACHE BUILDER
================================
Pre-calculates technical indicators (RSI, MACD, SMA, Bollinger Bands, ADX)
Stores in technical_screener_cache table for fast retrieval

INCREMENTAL MODE: Only processes NEW dates, never drops existing data
OPTIMIZED: Per-ticker cache check to fix partial commits.
"""

import os
import sys

# Add project root to path to allow imports from Analysis_Tools
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, inspect, text

# Database config
from Analysis_Tools.app.models.db_config import engine


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


def get_cached_keys():
    """Get (date, ticker) tuples already in cache"""
    try:
        q = text("SELECT DISTINCT cache_date, ticker FROM public.technical_screener_cache")
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
        print(f"Warning reading cache keys: {e}")
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
    INCREMENTAL: Processes missing (date, ticker) pairs
    OPTIMIZED: Per-ticker cache checks preventing partial commits.
    """
    print("\n" + "=" * 70)
    print("TECHNICAL SCREENER CACHE BUILDER (OPTIMIZED INCREMENTAL)")
    print("=" * 70)

    # Create table if needed (NO DROP)
    create_technical_screener_table()

    # Get cached keys (Date, Ticker)
    cached_keys = get_cached_keys()
    tables = get_derived_tables()

    if not tables:
        print("✗ No data available")
        return

    print(f"📂 Cached items: {len(cached_keys)}")
    print(f"📊 Tickers to scan: {len(tables)}")

    min_days = 50

    # Process each ticker
    all_cache_rows = []

    for ticker_idx, table in enumerate(tables, 1):
        ticker = table.replace("TBL_", "").replace("_DERIVED", "")
        print(f"\n[{ticker_idx}/{len(tables)}] {ticker}...", end=" ", flush=True)

        try:
            # OPTIMIZATION: Check if this ticker has ANY missing dates first?
            # It's hard to know *missing* dates without fetching available dates for this ticker.
            # So we fetch "BizDt" from DB for this ticker first.

            q_dates = text(f'SELECT DISTINCT "BizDt" FROM "{table}" WHERE "FinInstrmTp" = \'STF\' AND "BizDt" IS NOT NULL ORDER BY "BizDt"')

            # Using read_sql to get dates is fast
            df_dates = pd.read_sql(q_dates, engine)
            if df_dates.empty:
                print("⚠ No dates", end="")
                continue

            ticker_dates = [d.strftime("%Y-%m-%d") for d in pd.to_datetime(df_dates["BizDt"])]

            # Filter missing
            missing_dates = [d for d in ticker_dates if (d, ticker) not in cached_keys]

            if not missing_dates:
                print("✓ Up to date", end="")
                continue

            # If we have missing dates, we need to calculate indicators.
            # Indicators require history (e.g. 200 SMA).
            # So we typically need FULL history OR (Last cached date - 250 days) to NOW.

            # For simplicity & robustness, let's fetch full history for now (it's one query per ticker).
            # Optimization: If history is HUGE (10 years), we might want to limit.
            # But let's assume < 3000 rows is fine.

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
                print(f"⚠ Low data ({len(df)})", end="")
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
                print(f"⚠ Low val data", end="")
                continue

            close = df["close"]

            # Calculate indicators for entire history (Fast in Pandas)
            rsi = calculate_rsi(close, 14)
            macd_line, signal_line, histogram = calculate_macd(close, 12, 26, 9)
            sma_50 = calculate_sma(close, 50)
            sma_200 = calculate_sma(close, 200)
            bb_upper, bb_middle, bb_lower, bb_width = calculate_bollinger_bands(close, 20, 2)
            adx = calculate_adx(close, 14)

            # NEW: Calculate pivot points, momentum, and squeeze
            pivot, r1, r2, r3, s1, s2, s3 = calculate_pivot_points(close)
            momentum = calculate_momentum_score(close, lookback=10)

            # Create cache rows for MISSING dates only
            # Convert missing_dates to sets of Timestamps for fast lookup
            missing_dates_ts = set(pd.to_datetime(missing_dates))

            rows_to_insert = []

            for i in range(len(df)):
                current_date = df.index[i]

                # ONLY process MISSING dates
                if current_date not in missing_dates_ts:
                    continue

                latest_close = close.iloc[i]

                # --- Extracts for readability ---
                latest_rsi = rsi.iloc[i] if pd.notna(rsi.iloc[i]) else None
                latest_macd = macd_line.iloc[i] if pd.notna(macd_line.iloc[i]) else None
                latest_signal = signal_line.iloc[i] if pd.notna(signal_line.iloc[i]) else None
                latest_histogram = histogram.iloc[i] if pd.notna(histogram.iloc[i]) else None
                latest_sma_50 = sma_50.iloc[i] if pd.notna(sma_50.iloc[i]) else None
                latest_sma_200 = sma_200.iloc[i] if pd.notna(sma_200.iloc[i]) else None
                latest_bb_upper = bb_upper.iloc[i] if pd.notna(bb_upper.iloc[i]) else None
                latest_bb_middle = bb_middle.iloc[i] if pd.notna(bb_middle.iloc[i]) else None
                latest_bb_lower = bb_lower.iloc[i] if pd.notna(bb_lower.iloc[i]) else None
                latest_bb_width = bb_width.iloc[i] if pd.notna(bb_width.iloc[i]) else None
                latest_adx = adx.iloc[i] if pd.notna(adx.iloc[i]) else None

                # Check for MACD crossover
                macd_pos_cross = False
                macd_neg_cross = False
                if i > 0:
                    prev_macd = macd_line.iloc[i - 1] if pd.notna(macd_line.iloc[i-1]) else None
                    prev_signal = signal_line.iloc[i - 1] if pd.notna(signal_line.iloc[i-1]) else None

                    if (
                        prev_macd is not None and prev_signal is not None
                        and latest_macd is not None and latest_signal is not None
                    ):
                        if prev_macd < prev_signal and latest_macd > latest_signal:
                            macd_pos_cross = True
                        if prev_macd > prev_signal and latest_macd < latest_signal:
                            macd_neg_cross = True

                # NEW: Get pivot, momentum values
                latest_pivot = pivot.iloc[i] if pd.notna(pivot.iloc[i]) else None
                latest_r1 = r1.iloc[i] if pd.notna(r1.iloc[i]) else None
                latest_r2 = r2.iloc[i] if pd.notna(r2.iloc[i]) else None
                latest_r3 = r3.iloc[i] if pd.notna(r3.iloc[i]) else None
                latest_s1 = s1.iloc[i] if pd.notna(s1.iloc[i]) else None
                latest_s2 = s2.iloc[i] if pd.notna(s2.iloc[i]) else None
                latest_s3 = s3.iloc[i] if pd.notna(s3.iloc[i]) else None
                latest_momentum = momentum.iloc[i] if pd.notna(momentum.iloc[i]) else None

                # NEW: Price & Volume Metrics
                # ---------------------------
                week1_high = week1_low = None
                week4_high = week4_low = None
                week52_high = week52_low = None

                is_week1_high_bo = is_week1_low_bo = False
                is_week4_high_bo = is_week4_low_bo = False
                is_week52_high_bo = is_week52_low_bo = False

                is_pot_high_vol = False
                is_unusually_high_vol = False

                try:
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

                except Exception:
                    pass

                # Basic Price/Volume Data
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
                    "cache_date": current_date.strftime("%Y-%m-%d"),
                    "ticker": ticker,
                    "underlying_price": float(latest_close),

                    # NEW: Price & Volume
                    "open_price": float(latest_open) if latest_open is not None else None,
                    "high_price": float(latest_high) if latest_high is not None else None,
                    "low_price": float(latest_low) if latest_low is not None else None,
                    "volume": int(latest_volume) if latest_volume is not None and pd.notnull(latest_volume) else 0,
                    "price_change_pct": float(price_change_pct),
                    "volume_change_pct": float(vol_change_pct),

                    "week1_high": float(week1_high) if week1_high is not None else None,
                    "week1_low": float(week1_low) if week1_low is not None else None,
                    "week4_high": float(week4_high) if week4_high is not None else None,
                    "week4_low": float(week4_low) if week4_low is not None else None,
                    "week52_high": float(week52_high) if week52_high is not None else None,
                    "week52_low": float(week52_low) if week52_low is not None else None,

                    "is_week1_high_breakout": is_week1_high_bo,
                    "is_week1_low_breakout": is_week1_low_bo,
                    "is_week4_high_breakout": is_week4_high_bo,
                    "is_week4_low_breakout": is_week4_low_bo,
                    "is_week52_high_breakout": is_week52_high_bo,
                    "is_week52_low_breakout": is_week52_low_bo,
                    "is_potential_high_vol": is_pot_high_vol,
                    "is_unusually_high_vol": is_unusually_high_vol,

                    "rsi_14": float(latest_rsi) if latest_rsi is not None else None,
                    "rsi_above_80": bool(latest_rsi > 80) if latest_rsi is not None else False,
                    "rsi_60_80": bool(60 < latest_rsi <= 80) if latest_rsi is not None else False,
                    "rsi_40_60": bool(40 <= latest_rsi <= 60) if latest_rsi is not None else False,
                    "rsi_20_40": bool(20 <= latest_rsi < 40) if latest_rsi is not None else False,
                    "rsi_below_20": bool(latest_rsi < 20) if latest_rsi is not None else False,
                    "macd": float(latest_macd) if latest_macd is not None else None,
                    "macd_signal": float(latest_signal) if latest_signal is not None else None,
                    "macd_histogram": float(latest_histogram) if latest_histogram is not None else None,
                    "macd_pos_cross": macd_pos_cross,
                    "macd_neg_cross": macd_neg_cross,
                    "sma_50": float(latest_sma_50) if latest_sma_50 is not None else None,
                    "sma_200": float(latest_sma_200) if latest_sma_200 is not None else None,
                    "above_50_sma": bool(latest_close > latest_sma_50) if latest_sma_50 is not None else False,
                    "above_200_sma": bool(latest_close > latest_sma_200) if latest_sma_200 is not None else False,
                    "below_50_sma": bool(latest_close < latest_sma_50) if latest_sma_50 is not None else False,
                    "below_200_sma": bool(latest_close < latest_sma_200) if latest_sma_200 is not None else False,
                    "dist_from_50sma_pct": float(((latest_close - latest_sma_50) / latest_sma_50 * 100))
                    if latest_sma_50 is not None and latest_sma_50 != 0
                    else None,
                    "dist_from_200sma_pct": float(((latest_close - latest_sma_200) / latest_sma_200 * 100))
                    if latest_sma_200 is not None and latest_sma_200 != 0
                    else None,
                    "bb_upper": float(latest_bb_upper) if latest_bb_upper is not None else None,
                    "bb_middle": float(latest_bb_middle) if latest_bb_middle is not None else None,
                    "bb_lower": float(latest_bb_lower) if latest_bb_lower is not None else None,
                    "bb_width": float(latest_bb_width) if latest_bb_width is not None else None,
                    "adx_14": float(latest_adx) if latest_adx is not None else None,
                    "strong_trend": bool(latest_adx > 25) if latest_adx is not None else False,
                    # NEW: Pivot points
                    "pivot_point": float(latest_pivot) if latest_pivot is not None else None,
                    "r1": float(latest_r1) if latest_r1 is not None else None,
                    "r2": float(latest_r2) if latest_r2 is not None else None,
                    "r3": float(latest_r3) if latest_r3 is not None else None,
                    "s1": float(latest_s1) if latest_s1 is not None else None,
                    "s2": float(latest_s2) if latest_s2 is not None else None,
                    "s3": float(latest_s3) if latest_s3 is not None else None,
                    # NEW: Breakout flags
                    "r1_breakout": r1_bo,
                    "r2_breakout": r2_bo,
                    "r3_breakout": r3_bo,
                    "s1_breakout": s1_bo,
                    "s2_breakout": s2_bo,
                    "s3_breakout": s3_bo,
                    # NEW: Momentum and squeeze
                    "momentum_score": float(latest_momentum) if latest_momentum is not None else None,
                    "is_high_momentum": high_momentum_flag,
                    "bb_squeeze": bb_squeeze_flag,
                }

                rows_to_insert.append(row)

            if rows_to_insert:
                print(f"✓ {len(rows_to_insert)} new rows", end="")
                all_cache_rows.extend(rows_to_insert)
            else:
                print("✓ Up to date", end="")

        except Exception as e:
            print(f"✗ Error: {e}", end="")
            import traceback
            traceback.print_exc()

    # Insert all NEW rows into database
    # BATCHING: If we have 100 tickers * 10 missing days = 1000 rows. Safe to insert.
    if all_cache_rows:
        print(f"\n\n📥 Inserting {len(all_cache_rows)} total new rows into database...")
        try:
            # Chunk it
            chunk_size = 5000
            if len(all_cache_rows) > chunk_size:
                for i in range(0, len(all_cache_rows), chunk_size):
                    chunk = all_cache_rows[i:i + chunk_size]
                    pd.DataFrame(chunk).to_sql("technical_screener_cache", con=engine, if_exists="append", index=False)
                    print(f"  Batch {i}-{i+len(chunk)} inserted")
            else:
                pd.DataFrame(all_cache_rows).to_sql("technical_screener_cache", con=engine, if_exists="append", index=False)
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
