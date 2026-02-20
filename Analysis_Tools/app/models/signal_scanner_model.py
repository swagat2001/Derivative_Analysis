# =============================================================
#  SIGNAL SCANNER MODEL (ENHANCED)
#  Purpose: Signal scanner for options data with full technical indicators
#  Matches enhanced database schema
# =============================================================

from datetime import datetime, timedelta
from functools import lru_cache

import numpy as np
import pandas as pd
from sqlalchemy import text

from .db_config import engine


@lru_cache(maxsize=1)
def get_scanner_dates():
    """Get available dates for the scanner."""
    try:
        query = text("""
            SELECT DISTINCT signal_date::text AS date
            FROM daily_signal_scanner
            ORDER BY date DESC
            LIMIT 60;
        """)
        with engine.connect() as conn:
            result = conn.execute(query)
            dates = [row[0] for row in result]
            return dates if dates else []
    except Exception as e:
        print(f"[ERROR] get_scanner_dates(): {e}")
        return []


def run_signal_scanner(start_date: str = None, days_back: int = 7, limit: int = 10000):
    """
    Run the signal scanner and return results.
    Reads from 'daily_signal_scanner' cache table with full indicators.

    Args:
        start_date: Starting date (YYYY-MM-DD) or None for auto-calculate
        days_back: Number of days to look back (default: 7)
        limit: Maximum rows to fetch from database (default: 10000)
    """
    if not start_date:
        start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    print(f"[INFO] Fetching cached signals from {start_date} (limit: {limit:,} rows)...")

    # Query with ALL enhanced columns including Volume Profile
    query = text("""
    SELECT
        signal_date, ticker, expiry_date, strike_price, option_type,
        close_price, spot_price, high_price, low_price, price_change_pct,
        volume, oi, oi_change_pct,
        rsi, pp, r1, s1, r2, s2, r3, s3,
        poc, vah, val,
        signals
    FROM daily_signal_scanner
    WHERE signal_date >= :start_date
    ORDER BY signal_date DESC, ticker ASC, expiry_date ASC, strike_price ASC
    LIMIT :limit
""")

    import json

    try:
        with engine.connect() as conn:
            result = conn.execute(query, {"start_date": start_date, "limit": limit})
            rows = result.fetchall()

        if not rows:
            print("[WARN] No cached data found. Please run enhanced signal_scanner_cache.py")
            return []

        print(f"[DEBUG] Fetched {len(rows)} rows from database")

        # Transform to expected dict format
        output_rows = []
        for row in rows:
            # Row structure (25 columns total, indices 0-24):
            # 0:signal_date, 1:ticker, 2:expiry_date, 3:strike_price, 4:option_type,
            # 5:close_price, 6:spot_price, 7:high_price, 8:low_price, 9:price_change_pct,
            # 10:volume, 11:oi, 12:oi_change_pct,
            # 13:rsi, 14:pp, 15:r1, 16:s1, 17:r2, 18:s2, 19:r3, 20:s3,
            # 21:poc, 22:vah, 23:val,
            # 24:signals

            # Parse signals
            signals_raw = row[24]  # signals column (last one)

            if isinstance(signals_raw, str):
                try:
                    signals_list = json.loads(signals_raw)
                except:
                    signals_list = []
            elif isinstance(signals_raw, list):
                signals_list = signals_raw
            else:
                signals_list = []

            # Convert signals to boolean flags
            signal_dict = {
                "long_build_up": "Long Build Up" in signals_list,
                "short_build_up": "Short Build Up" in signals_list,
                "long_unwinding": "Long Unwinding" in signals_list,
                "short_covering": "Short Covering" in signals_list,
                "volume_spike": "Volume Spike" in signals_list,
                "rsi_oversold": "RSI Oversold" in signals_list,
                "rsi_overbought": "RSI Overbought" in signals_list,
                "rsi_cross_up": "RSI Cross Up" in signals_list,
                "rsi_cross_down": "RSI Cross Down" in signals_list,
                "bull_divergence": "Bullish Divergence" in signals_list,
                "bear_divergence": "Bearish Divergence" in signals_list,
                "pivot_signal": any(s in signals_list for s in ["Near S1 Support", "Near R1 Resistance", "Near Pivot Point", "S1 Support", "S2 Support", "S3 Support", "R1 Resistance", "R2 Resistance", "R3 Resistance"]),
            }

            # Determine RSI trend
            rsi_val = float(row[13] or 50)
            if rsi_val < 30:
                rsi_trend = "OVERSOLD"
            elif rsi_val > 70:
                rsi_trend = "OVERBOUGHT"
            else:
                rsi_trend = "NEUTRAL"

            item = {
                "signal_date": str(row[0]),
                "symbol": str(row[1]),
                "expiry": str(row[2]) if row[2] else "",
                "strike": float(row[3] or 0),
                "option_type": str(row[4] or ""),
                "close": float(row[5] or 0),
                "spot": float(row[6] or 0),
                "high": float(row[7] or 0),
                "low": float(row[8] or 0),
                "price_change_pct": float(row[9] or 0),
                "volume": int(row[10] or 0),
                "oi": int(row[11] or 0),
                "oi_change_pct": float(row[12] or 0),
                # Technical Indicators
                "rsi": rsi_val,
                "pp": float(row[14] or 0),
                "r1": float(row[15] or 0),
                "s1": float(row[16] or 0),
                "r2": float(row[17] or 0),
                "s2": float(row[18] or 0),
                "r3": float(row[19] or 0),
                "s3": float(row[20] or 0),
                # Signals
                "signals": signals_list,
                "signal_text": ", ".join(signals_list) if signals_list else "No Signal",
                "high_volume": signal_dict["volume_spike"] or "High Volume" in signals_list,
                "oi_spike": signal_dict["long_build_up"] or signal_dict["short_build_up"] or "OI Spike" in signals_list,
                "rsi_trend": rsi_trend,
                # Volume Profile
                "poc": float(row[21] or 0),
                "vah": float(row[22] or 0),
                "val": float(row[23] or 0),
                # Other optional fields
                "rsi_cross": None,
                "pivot_signal": None,
                "divergence": None,
            }
            output_rows.append(item)

            if len(output_rows) <= 3:
                print(
                    f"[DEBUG] Row {len(output_rows)}: {item['symbol']} {item['option_type']} "
                    f"Strike:{item['strike']} RSI:{item['rsi']:.1f}"
                )

        print(f"[INFO] Loaded {len(output_rows)} cached signals with full indicators")
        return output_rows

    except Exception as e:
        print(f"[ERROR] run_signal_scanner failed: {e}")
        import traceback

        traceback.print_exc()
        return []


def get_scanner_summary(signals: list):
    """Generate summary statistics from scanner results."""
    if not signals:
        return {}

    df = pd.DataFrame(signals)

    # Count signals by checking the signals list for each row
    pivot_count = 0
    bull_div_count = 0
    bear_div_count = 0
    rsi_up_count = 0
    rsi_down_count = 0

    for sig in signals:
        sig_list = sig.get('signals', [])
        if any(s in sig_list for s in ["Near S1 Support", "Near R1 Resistance", "Near Pivot Point", "S1 Support", "S2 Support", "S3 Support", "R1 Resistance", "R2 Resistance", "R3 Resistance"]):
            pivot_count += 1
        if "Bullish Divergence" in sig_list:
            bull_div_count += 1
        if "Bearish Divergence" in sig_list:
            bear_div_count += 1
        if "RSI Cross Up" in sig_list:
            rsi_up_count += 1
        if "RSI Cross Down" in sig_list:
            rsi_down_count += 1

    return {
        "total_signals": int(len(signals)),
        "unique_symbols": int(df["symbol"].nunique()),
        "high_volume_count": int(df["high_volume"].sum()),
        "oi_spike_count": int(df["oi_spike"].sum()),
        "pivot_signals": pivot_count,
        "bull_divergence": bull_div_count,
        "bear_divergence": bear_div_count,
        "rsi_cross_up": rsi_up_count,
        "rsi_cross_down": rsi_down_count,
        "oversold_count": int((df["rsi_trend"] == "OVERSOLD").sum()),
        "overbought_count": int((df["rsi_trend"] == "OVERBOUGHT").sum()),
    }


def filter_signals(signals: list, filters: dict):
    """Filter signals based on criteria."""
    if not signals:
        return signals

    filtered = signals.copy()

    # Filter by signal type
    signal_type_str = filters.get("signal_type", "all")
    if signal_type_str == "all" or not signal_type_str:
        pass  # No filtering needed
    else:
        # Split comma-separated values into a set
        requested_types = set(s.strip() for s in signal_type_str.split(","))

        # If "all" is somehow in the list, ignore other filters
        if "all" in requested_types:
            pass
        else:
            # OR Logic: Match any of the selected types
            matched_signals = []

            for s in filtered:
                is_match = False

                if "high_volume" in requested_types and s["high_volume"]:
                    is_match = True
                elif "oi_spike" in requested_types and s["oi_spike"]:
                    is_match = True
                elif "pivot" in requested_types and any(sig in s.get('signals', []) for sig in ["Near S1 Support", "Near R1 Resistance", "Near Pivot Point", "S1 Support", "S2 Support", "S3 Support", "R1 Resistance", "R2 Resistance", "R3 Resistance"]):
                    is_match = True
                elif "divergence" in requested_types and any(sig in s.get('signals', []) for sig in ["Bullish Divergence", "Bearish Divergence"]):
                    is_match = True
                elif "bull_divergence" in requested_types and "Bullish Divergence" in s.get('signals', []):
                    is_match = True
                elif "bear_divergence" in requested_types and "Bearish Divergence" in s.get('signals', []):
                    is_match = True
                elif "rsi_cross" in requested_types and any(sig in s.get('signals', []) for sig in ["RSI Cross Up", "RSI Cross Down"]):
                    is_match = True
                elif "oversold" in requested_types and s.get("rsi_trend") == "OVERSOLD":
                    is_match = True
                elif "overbought" in requested_types and s.get("rsi_trend") == "OVERBOUGHT":
                    is_match = True

                if is_match:
                    matched_signals.append(s)

            filtered = matched_signals

    # Filter by option type
    option_type = filters.get("option_type", "all")
    if option_type == "CE":
        filtered = [s for s in filtered if s["option_type"] == "CE"]
    elif option_type == "PE":
        filtered = [s for s in filtered if s["option_type"] == "PE"]
    elif option_type == "FUT":
        filtered = [s for s in filtered if s["option_type"] in ("", "FUT", "STF")]

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
