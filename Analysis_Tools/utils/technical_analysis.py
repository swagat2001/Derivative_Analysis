# =============================================================
#  TECHNICAL ANALYSIS UTILITY MODULE
#  Purpose: Centralized logic for technical indicators
#  Location: Analysis_Tools/utils/technical_analysis.py
# =============================================================

import pandas as pd
import numpy as np

def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """
    Calculate Relative Strength Index (RSI).
    Args:
        series: Price series (usually Close)
        period: Lookback period (default 14)
    Returns:
        RSI series (0-100)
    """
    try:
        delta = series.diff()
        gain = delta.clip(lower=0).rolling(period).mean()
        loss = -delta.clip(upper=0).rolling(period).mean()

        # Avoid division by zero
        rs = gain / loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))

        # Fill/Handle NaN if needed, but returning series with NaNs at start is standard
        return rsi
    except Exception:
        return pd.Series([np.nan] * len(series), index=series.index)


def calculate_macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    """
    Calculate MACD, Signal Line, and Histogram.
    Returns:
        macd_line, signal_line, histogram
    """
    try:
        exp1 = series.ewm(span=fast, adjust=False).mean()
        exp2 = series.ewm(span=slow, adjust=False).mean()
        macd = exp1 - exp2
        signal_line = macd.ewm(span=signal, adjust=False).mean()
        histogram = macd - signal_line
        return macd, signal_line, histogram
    except Exception:
        nan_series = pd.Series([np.nan] * len(series), index=series.index)
        return nan_series, nan_series, nan_series


def calculate_bollinger_bands(series: pd.Series, period: int = 20, std_dev: int = 2):
    """
    Calculate Bollinger Bands.
    Returns:
        upper_band, middle_band, lower_band, bandwidth
    """
    try:
        middle = series.rolling(period).mean()
        std = series.rolling(period).std()
        upper = middle + (std * std_dev)
        lower = middle - (std * std_dev)
        width = (upper - lower) / middle
        return upper, middle, lower, width
    except Exception:
        nan_series = pd.Series([np.nan] * len(series), index=series.index)
        return nan_series, nan_series, nan_series, nan_series


def calculate_sma(series: pd.Series, period: int) -> pd.Series:
    """Calculate Simple Moving Average."""
    return series.rolling(period).mean()


def calculate_adx(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14):
    """
    Calculate Average Directional Index (ADX).
    Returns:
        adx_series
    """
    try:
        plus_dm = high.diff()
        minus_dm = low.diff()

        plus_dm[plus_dm < 0] = 0
        minus_dm[minus_dm > 0] = 0

        tr1 = pd.DataFrame(high - low)
        tr2 = pd.DataFrame(abs(high - close.shift(1)))
        tr3 = pd.DataFrame(abs(low - close.shift(1)))

        frames = [tr1, tr2, tr3]
        tr = pd.concat(frames, axis=1, join='outer').max(axis=1)

        atr = tr.rolling(period).mean()

        plus_di = 100 * (plus_dm.ewm(alpha=1/period).mean() / atr)
        minus_di = 100 * (abs(minus_dm).ewm(alpha=1/period).mean() / atr)

        dx = (abs(plus_di - minus_di) / abs(plus_di + minus_di)) * 100
        adx = dx.rolling(period).mean()

        return adx
    except Exception:
        return pd.Series([np.nan] * len(close), index=close.index)


def calc_pivot_levels(high: float, low: float, close: float):
    """
    Calculate Standard Pivot Points (Classic Floor Trader Pivots).
    Returns:
        pp, r1, s1, r2, s2, r3, s3
    """
    pp = (high + low + close) / 3
    r1 = 2 * pp - low
    s1 = 2 * pp - high
    r2 = pp + (high - low)
    s2 = pp - (high - low)
    r3 = high + 2 * (pp - low)
    s3 = low - 2 * (high - pp)

    return pp, r1, s1, r2, s2, r3, s3


def calc_volume_profile(df: pd.DataFrame, bins: int = 50):
    """
    Calculate Volume Profile - POC, VAH, VAL.
    Requires columns: 'HghPric', 'LwPric', 'ClsPric', 'Volume' (or mapped equivalents)
    """
    # Map common column names if needed
    col_map = {
        "HghPric": "high", "High": "high",
        "LwPric": "low", "Low": "low",
        "ClsPric": "close", "Close": "close",
        "Volume": "volume", "Vol": "volume"
    }

    # Normalize columns for calculation locally
    temp_df = df.rename(columns=lambda x: col_map.get(x, x))

    if "high" not in temp_df.columns or "low" not in temp_df.columns:
        return None, None, None

    # Filter out invalid prices
    df_valid = temp_df[(temp_df["high"] > 0) & (temp_df["low"] > 0) & (temp_df["close"] > 0)].copy()

    if df_valid.empty:
        return None, None, None

    high_val = df_valid["high"].max()
    low_val = df_valid["low"].min()

    if pd.isna(high_val) or pd.isna(low_val) or high_val == low_val:
        return None, None, None

    levels = np.linspace(low_val, high_val, bins)
    vol_dist = np.zeros(bins)

    for _, row in temp_df.iterrows():
        close_price = row.get("close", 0)
        vol = row.get("volume", 0)

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

    try:
        cutoff = np.where(cum_vol >= cum_vol[-1] * 0.70)[0][0]
        vah = np.max(levels_sorted[: cutoff + 1])
        val = np.min(levels_sorted[: cutoff + 1])
    except IndexError:
        vah = poc
        val = poc

    return round(poc, 2), round(vah, 2), round(val, 2)
