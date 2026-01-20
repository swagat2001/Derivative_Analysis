"""
=============================================================
SIGNAL SERVICE - SINGLE SOURCE OF TRUTH
=============================================================
Centralized signal computation for the entire application.
All signal-related logic MUST use this service.

This is the CANONICAL implementation extracted from signal_analysis/controller.py
Do NOT duplicate this logic anywhere else in the project.

USAGE:
------
    # For simple signal mapping (ticker -> signal string)
    from app.services.signal_service import compute_signals_simple
    signals = compute_signals_simple(selected_date)
    # Returns: {'RELIANCE': 'BULLISH', 'TCS': 'BEARISH', 'INFY': 'NEUTRAL', ...}

    # For full breakdown with categories
    from app.services.signal_service import compute_signals_with_breakdown
    signals = compute_signals_with_breakdown(selected_date)
    # Returns: {
    #     'RELIANCE': {
    #         'signal': 'BULLISH',
    #         'bullish_count': 5,
    #         'bearish_count': 2,
    #         'bullish_categories': ['OI Call Gainers (ALL)', ...],
    #         'bearish_categories': ['IV Put Gainers (ALL)', ...]
    #     }, ...
    # }

    # For pre-loaded screener data (avoids re-fetching from DB)
    from app.services.signal_service import compute_signals_from_screener_data
    signals = compute_signals_from_screener_data(screener_data)
=============================================================
"""

from typing import Any, Dict, List, Optional

# =============================================================
# CONSTANTS - Section Classifications (DO NOT MODIFY)
# =============================================================

# Number of top items to consider from each category
TOP_N_ITEMS = 10

# Bullish sections: These indicate bullish sentiment for a stock
# Key = internal key, Value = human-readable display name
BULLISH_SECTIONS = {
    # IV sections (bullish when calls gain IV, puts lose IV)
    "iv_call_gainers": "IV Call Gainers (ALL)",
    "iv_call_itm_gainers": "IV Call Gainers (ITM)",
    "iv_call_otm_gainers": "IV Call Gainers (OTM)",
    "iv_put_losers": "IV Put Losers (ALL)",
    "iv_put_itm_losers": "IV Put Losers (ITM)",
    "iv_put_otm_losers": "IV Put Losers (OTM)",
    # OI sections (bullish when call OI increases, put OI decreases)
    "oi_call_gainers": "OI Call Gainers (ALL)",
    "oi_call_itm_gainers": "OI Call Gainers (ITM)",
    "oi_call_otm_gainers": "OI Call Gainers (OTM)",
    "oi_put_losers": "OI Put Losers (ALL)",
    "oi_put_itm_losers": "OI Put Losers (ITM)",
    "oi_put_otm_losers": "OI Put Losers (OTM)",
    # Moneyness sections (bullish indicators)
    "moneyness_call_gainers": "Moneyness Call Gainers (ALL)",
    "moneyness_call_itm_gainers": "Moneyness Call Gainers (ITM)",
    "moneyness_call_otm_gainers": "Moneyness Call Gainers (OTM)",
    "moneyness_put_losers": "Moneyness Put Losers (ALL)",
    "moneyness_put_itm_losers": "Moneyness Put Losers (ITM)",
    "moneyness_put_otm_losers": "Moneyness Put Losers (OTM)",
    # Futures (bullish when OI increases)
    "future_oi_gainers": "Future OI Gainers",
}

# Bearish sections: These indicate bearish sentiment for a stock
BEARISH_SECTIONS = {
    # IV sections (bearish when calls lose IV, puts gain IV)
    "iv_call_losers": "IV Call Losers (ALL)",
    "iv_call_itm_losers": "IV Call Losers (ITM)",
    "iv_call_otm_losers": "IV Call Losers (OTM)",
    "iv_put_gainers": "IV Put Gainers (ALL)",
    "iv_put_itm_gainers": "IV Put Gainers (ITM)",
    "iv_put_otm_gainers": "IV Put Gainers (OTM)",
    # OI sections (bearish when call OI decreases, put OI increases)
    "oi_call_losers": "OI Call Losers (ALL)",
    "oi_call_itm_losers": "OI Call Losers (ITM)",
    "oi_call_otm_losers": "OI Call Losers (OTM)",
    "oi_put_gainers": "OI Put Gainers (ALL)",
    "oi_put_itm_gainers": "OI Put Gainers (ITM)",
    "oi_put_otm_gainers": "OI Put Gainers (OTM)",
    # Moneyness sections (bearish indicators)
    "moneyness_call_losers": "Moneyness Call Losers (ALL)",
    "moneyness_call_itm_losers": "Moneyness Call Losers (ITM)",
    "moneyness_call_otm_losers": "Moneyness Call Losers (OTM)",
    "moneyness_put_gainers": "Moneyness Put Gainers (ALL)",
    "moneyness_put_itm_gainers": "Moneyness Put Gainers (ITM)",
    "moneyness_put_otm_gainers": "Moneyness Put Gainers (OTM)",
    # Futures (bearish when OI decreases)
    "future_oi_losers": "Future OI Losers",
}


# =============================================================
# HELPER FUNCTIONS
# =============================================================


def build_screener_data_structure(all_data: Dict) -> Dict[str, List]:
    """
    Build the flattened screener data structure from raw database data.

    Args:
        all_data: Raw data from get_all_screener_data()
                  Structure: {
                      'oi': {'CE': {'ALL': [...], 'ITM': [...], ...}, 'PE': {...}, 'FUT': {...}},
                      'iv': {...},
                      'moneyness': {...}
                  }

    Returns:
        Flattened structure: {
            'oi_call_gainers': [...],
            'oi_call_itm_gainers': [...],
            ...
        }
    """
    if not all_data:
        return {}

    screener_data = {}

    try:
        # OI - Calls (6 categories)
        screener_data["oi_call_gainers"] = all_data["oi"]["CE"]["ALL"][:TOP_N_ITEMS]
        screener_data["oi_call_itm_gainers"] = all_data["oi"]["CE"]["ITM"][:TOP_N_ITEMS]
        screener_data["oi_call_otm_gainers"] = all_data["oi"]["CE"]["OTM"][:TOP_N_ITEMS]
        screener_data["oi_call_losers"] = all_data["oi"]["CE"]["ALL_LOSERS"][:TOP_N_ITEMS]
        screener_data["oi_call_itm_losers"] = all_data["oi"]["CE"]["ITM_LOSERS"][:TOP_N_ITEMS]
        screener_data["oi_call_otm_losers"] = all_data["oi"]["CE"]["OTM_LOSERS"][:TOP_N_ITEMS]

        # OI - Puts (6 categories)
        screener_data["oi_put_gainers"] = all_data["oi"]["PE"]["ALL"][:TOP_N_ITEMS]
        screener_data["oi_put_itm_gainers"] = all_data["oi"]["PE"]["ITM"][:TOP_N_ITEMS]
        screener_data["oi_put_otm_gainers"] = all_data["oi"]["PE"]["OTM"][:TOP_N_ITEMS]
        screener_data["oi_put_losers"] = all_data["oi"]["PE"]["ALL_LOSERS"][:TOP_N_ITEMS]
        screener_data["oi_put_itm_losers"] = all_data["oi"]["PE"]["ITM_LOSERS"][:TOP_N_ITEMS]
        screener_data["oi_put_otm_losers"] = all_data["oi"]["PE"]["OTM_LOSERS"][:TOP_N_ITEMS]

        # Moneyness - Calls (6 categories)
        screener_data["moneyness_call_gainers"] = all_data["moneyness"]["CE"]["ALL"][:TOP_N_ITEMS]
        screener_data["moneyness_call_itm_gainers"] = all_data["moneyness"]["CE"]["ITM"][:TOP_N_ITEMS]
        screener_data["moneyness_call_otm_gainers"] = all_data["moneyness"]["CE"]["OTM"][:TOP_N_ITEMS]
        screener_data["moneyness_call_losers"] = all_data["moneyness"]["CE"]["ALL_LOSERS"][:TOP_N_ITEMS]
        screener_data["moneyness_call_itm_losers"] = all_data["moneyness"]["CE"]["ITM_LOSERS"][:TOP_N_ITEMS]
        screener_data["moneyness_call_otm_losers"] = all_data["moneyness"]["CE"]["OTM_LOSERS"][:TOP_N_ITEMS]

        # Moneyness - Puts (6 categories)
        screener_data["moneyness_put_gainers"] = all_data["moneyness"]["PE"]["ALL"][:TOP_N_ITEMS]
        screener_data["moneyness_put_itm_gainers"] = all_data["moneyness"]["PE"]["ITM"][:TOP_N_ITEMS]
        screener_data["moneyness_put_otm_gainers"] = all_data["moneyness"]["PE"]["OTM"][:TOP_N_ITEMS]
        screener_data["moneyness_put_losers"] = all_data["moneyness"]["PE"]["ALL_LOSERS"][:TOP_N_ITEMS]
        screener_data["moneyness_put_itm_losers"] = all_data["moneyness"]["PE"]["ITM_LOSERS"][:TOP_N_ITEMS]
        screener_data["moneyness_put_otm_losers"] = all_data["moneyness"]["PE"]["OTM_LOSERS"][:TOP_N_ITEMS]

        # IV - Calls (6 categories)
        screener_data["iv_call_gainers"] = all_data["iv"]["CE"]["ALL"][:TOP_N_ITEMS]
        screener_data["iv_call_itm_gainers"] = all_data["iv"]["CE"]["ITM"][:TOP_N_ITEMS]
        screener_data["iv_call_otm_gainers"] = all_data["iv"]["CE"]["OTM"][:TOP_N_ITEMS]
        screener_data["iv_call_losers"] = all_data["iv"]["CE"]["ALL_LOSERS"][:TOP_N_ITEMS]
        screener_data["iv_call_itm_losers"] = all_data["iv"]["CE"]["ITM_LOSERS"][:TOP_N_ITEMS]
        screener_data["iv_call_otm_losers"] = all_data["iv"]["CE"]["OTM_LOSERS"][:TOP_N_ITEMS]

        # IV - Puts (6 categories)
        screener_data["iv_put_gainers"] = all_data["iv"]["PE"]["ALL"][:TOP_N_ITEMS]
        screener_data["iv_put_itm_gainers"] = all_data["iv"]["PE"]["ITM"][:TOP_N_ITEMS]
        screener_data["iv_put_otm_gainers"] = all_data["iv"]["PE"]["OTM"][:TOP_N_ITEMS]
        screener_data["iv_put_losers"] = all_data["iv"]["PE"]["ALL_LOSERS"][:TOP_N_ITEMS]
        screener_data["iv_put_itm_losers"] = all_data["iv"]["PE"]["ITM_LOSERS"][:TOP_N_ITEMS]
        screener_data["iv_put_otm_losers"] = all_data["iv"]["PE"]["OTM_LOSERS"][:TOP_N_ITEMS]

        # Futures (2 categories for signals)
        screener_data["future_oi_gainers"] = all_data["oi"]["FUT"]["ALL"][:TOP_N_ITEMS]
        screener_data["future_oi_losers"] = all_data["oi"]["FUT"]["ALL_LOSERS"][:TOP_N_ITEMS]

    except KeyError as e:
        print(f"[WARNING] Missing key in all_data structure: {e}")
    except Exception as e:
        print(f"[ERROR] build_screener_data_structure: {e}")
        import traceback

        traceback.print_exc()

    return screener_data


def _compute_signals_core(screener_data: Dict[str, List]) -> Dict[str, Dict]:
    """
    CORE signal computation logic - DO NOT MODIFY without careful consideration.
    This is the CANONICAL implementation used by Signal Analysis page.

    Args:
        screener_data: Flattened screener data structure

    Returns:
        Dictionary mapping ticker to signal details:
        {
            'RELIANCE': {
                'signal': 'BULLISH',
                'bullish_count': 5,
                'bearish_count': 2,
                'bullish_categories': ['OI Call Gainers (ALL)', ...],
                'bearish_categories': ['IV Put Gainers (ALL)', ...]
            }
        }
    """
    signals = {}

    # Track bullish membership
    for sec_key, sec_name in BULLISH_SECTIONS.items():
        for row in screener_data.get(sec_key, []):
            ticker = row.get("ticker")
            if ticker:
                if ticker not in signals:
                    signals[ticker] = {
                        "signal": "NEUTRAL",  # Default until computed
                        "bullish_count": 0,
                        "bearish_count": 0,
                        "bullish_categories": [],
                        "bearish_categories": [],
                    }
                signals[ticker]["bullish_count"] += 1
                signals[ticker]["bullish_categories"].append(sec_name)

    # Track bearish membership
    for sec_key, sec_name in BEARISH_SECTIONS.items():
        for row in screener_data.get(sec_key, []):
            ticker = row.get("ticker")
            if ticker:
                if ticker not in signals:
                    signals[ticker] = {
                        "signal": "NEUTRAL",  # Default until computed
                        "bullish_count": 0,
                        "bearish_count": 0,
                        "bullish_categories": [],
                        "bearish_categories": [],
                    }
                signals[ticker]["bearish_count"] += 1
                signals[ticker]["bearish_categories"].append(sec_name)

    # Final classification - handle ties as NEUTRAL
    for ticker, data in signals.items():
        if data["bullish_count"] > data["bearish_count"]:
            data["signal"] = "BULLISH"
        elif data["bearish_count"] > data["bullish_count"]:
            data["signal"] = "BEARISH"
        else:
            data["signal"] = "NEUTRAL"  # Equal counts = neutral

    return signals


# =============================================================
# PUBLIC API - Main Functions
# =============================================================


def compute_signals_with_breakdown(selected_date: str) -> Optional[Dict[str, Dict]]:
    """
    Compute signals with FULL BREAKDOWN for all tickers.

    This is the PRIMARY function for signal computation.
    Used by Signal Analysis page and anywhere detailed breakdown is needed.

    Args:
        selected_date: Date string in 'YYYY-MM-DD' format

    Returns:
        Dictionary mapping ticker to full signal breakdown:
        {
            'RELIANCE': {
                'signal': 'BULLISH',
                'bullish_count': 5,
                'bearish_count': 2,
                'bullish_categories': ['OI Call Gainers (ALL)', ...],
                'bearish_categories': ['IV Put Gainers (ALL)', ...]
            }, ...
        }
        Returns None if no data available.
    """
    try:
        # Import here to avoid circular imports
        from ..models.screener_model import get_all_screener_data

        # Get raw screener data
        all_data = get_all_screener_data(selected_date)

        if not all_data:
            return None

        # Build flattened structure
        screener_data = build_screener_data_structure(all_data)

        if not screener_data:
            return None

        # Compute signals using core logic
        signals = _compute_signals_core(screener_data)

        return signals

    except Exception as e:
        print(f"[ERROR] compute_signals_with_breakdown: {e}")
        import traceback

        traceback.print_exc()
        return None


def compute_signals_simple(selected_date: str) -> Dict[str, str]:
    """
    Compute SIMPLE signal mapping for all tickers.

    Convenience function that returns just the signal string.
    Used where only the final signal is needed (no breakdown).

    Args:
        selected_date: Date string in 'YYYY-MM-DD' format

    Returns:
        Dictionary mapping ticker to signal string:
        {'RELIANCE': 'BULLISH', 'TCS': 'BEARISH', 'INFY': 'NEUTRAL', ...}
        Returns empty dict if no data available.
    """
    signals_full = compute_signals_with_breakdown(selected_date)

    if not signals_full:
        return {}

    return {ticker: data["signal"] for ticker, data in signals_full.items()}


def get_signal_for_ticker(selected_date: str, ticker: str) -> Optional[Dict]:
    """
    Get signal details for a SINGLE ticker.

    Args:
        selected_date: Date string in 'YYYY-MM-DD' format
        ticker: Stock ticker symbol (e.g., 'RELIANCE')

    Returns:
        Signal details for the ticker, or None if not found:
        {
            'signal': 'BULLISH',
            'bullish_count': 5,
            'bearish_count': 2,
            'bullish_categories': [...],
            'bearish_categories': [...]
        }
    """
    signals = compute_signals_with_breakdown(selected_date)

    if not signals:
        return None

    return signals.get(ticker.upper())


def compute_signals_from_screener_data(screener_data: Dict[str, List]) -> Dict[str, str]:
    """
    Compute SIMPLE signals from already-loaded screener data.

    Use this when you already have the flattened screener_data structure
    and don't want to re-fetch from database.

    Args:
        screener_data: Flattened screener data with keys like 'oi_call_gainers', etc.

    Returns:
        Dictionary mapping ticker to signal string:
        {'RELIANCE': 'BULLISH', 'TCS': 'BEARISH', ...}
    """
    signals_full = _compute_signals_core(screener_data)
    return {ticker: data["signal"] for ticker, data in signals_full.items()}


def compute_signals_with_breakdown_from_screener_data(screener_data: Dict[str, List]) -> Dict[str, Dict]:
    """
    Compute signals with FULL BREAKDOWN from already-loaded screener data.

    Use this when you already have the flattened screener_data structure
    and don't want to re-fetch from database.

    Args:
        screener_data: Flattened screener data with keys like 'oi_call_gainers', etc.

    Returns:
        Full signal breakdown dictionary
    """
    return _compute_signals_core(screener_data)
