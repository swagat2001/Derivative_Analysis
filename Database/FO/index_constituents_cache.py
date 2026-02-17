"""
Index Constituents Cache Module
Fetches index constituents from NSE and saves to JSON file.
Called by fo_update_database.py as part of the data pipeline.

Output: Data_scraper/index_constituents.json
"""

import json
import os
from datetime import datetime

import requests

# Output file path - relative to project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUTPUT_FILE = os.path.join(PROJECT_ROOT, "Data_scraper", "index_constituents.json")

# NSE API Configuration
NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}

# NSE API URLs for all indices
NSE_INDEX_URLS = {
    "nifty50": "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050",
    "niftynext50": "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20NEXT%2050",
    "niftybank": "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20BANK",
    "niftyit": "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20IT",
    "niftypharma": "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20PHARMA",
    "niftyauto": "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20AUTO",
    "niftymetal": "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20METAL",
    "niftyfmcg": "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20FMCG",
    "niftyenergy": "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20ENERGY",
    "niftypsubank": "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20PSU%20BANK",
    "niftyfinancial": "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20FINANCIAL%20SERVICES",
}

# Fallback data for Sensex (BSE - not on NSE)
FALLBACK_SENSEX = [
    "ASIANPAINT",
    "AXISBANK",
    "BAJAJ-AUTO",
    "BAJFINANCE",
    "BHARTIARTL",
    "HCLTECH",
    "HDFCBANK",
    "HEROMOTOCO",
    "HINDUNILVR",
    "ICICIBANK",
    "INDUSINDBK",
    "INFY",
    "ITC",
    "JSWSTEEL",
    "KOTAKBANK",
    "LT",
    "M&M",
    "MARUTI",
    "NESTLEIND",
    "NTPC",
    "POWERGRID",
    "RELIANCE",
    "SBIN",
    "SUNPHARMA",
    "TATAMOTORS",
    "TATASTEEL",
    "TCS",
    "TECHM",
    "TITAN",
    "ULTRACEMCO",
]


def _create_session():
    """Create session with NSE headers."""
    session = requests.Session()
    session.headers.update(NSE_HEADERS)
    try:
        session.get("https://www.nseindia.com", timeout=5)
    except Exception:
        pass
    return session


def sanitize_symbol(symbol):
    """
    Sanitize NSE symbol to match Database ticker format.
    Handles special characters that are stored differently in DB.
    """
    symbol = str(symbol).strip().upper()

    # Mapping from NSE Website format -> Database format
    mapping = {
        "M&M": "M_M",
        "BAJAJ-AUTO": "BAJAJ_AUTO",
        "L&T": "LT",
        "M&MFIN": "M_MFIN",
        "M&M FIN": "M_MFIN",
        "TATAMTRDVR": "TATAMTRDVR",
        "BAJAJFINSV": "BAJAJFINSV",
        "ADANIPOWER": "ADANIPOWER",
        "BAJAJHLDNG": "BAJAJHLDNG"
    }

    return mapping.get(symbol, symbol)


def _fetch_single_index(session, index_key, url):
    """Fetch constituents for a single index."""
    try:
        response = session.get(url, timeout=10)

        if response.status_code == 200:
            data = response.json()
            constituents = []

            for item in data.get("data", []):
                symbol = item.get("symbol", "")

                # Basic validation
                if not symbol or symbol.startswith("NIFTY") or symbol == "SENSEX":
                    continue

                # Sanitize symbol (Handle M&M -> M_M, etc.)
                clean_symbol = sanitize_symbol(symbol)

                # Filter out known invalid entries
                if clean_symbol in ["KWIL", "ENRIN"]:
                    continue

                constituents.append(clean_symbol)

            if constituents:
                print(f"   ✓ {index_key}: {len(constituents)} stocks")
                return constituents

        print(f"   ✗ {index_key}: API returned no data")
        return []

    except Exception as e:
        print(f"   ✗ {index_key}: Error - {str(e)[:50]}")
        return []


def fetch_index_constituents_cache():
    """Main function: Fetch all index constituents from NSE and save to JSON."""
    print("Fetching index constituents from NSE...")

    session = _create_session()
    all_indices = {}

    # Fetch from NSE API
    for index_key, url in NSE_INDEX_URLS.items():
        constituents = _fetch_single_index(session, index_key, url)
        if constituents:
            all_indices[index_key] = constituents

    # Add Sensex (BSE - use fallback)
    all_indices["sensex"] = FALLBACK_SENSEX
    print(f"   ✓ sensex: {len(FALLBACK_SENSEX)} stocks (BSE)")

    # Build result with metadata
    result = {
        "fetched_at": datetime.now().isoformat(),
        "indices": all_indices,
        "total_indices": len(all_indices),
    }

    # Ensure output directory exists
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    # Save to file
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"\n✓ Saved {len(all_indices)} indices to {os.path.basename(OUTPUT_FILE)}")
    return True


# Alias for pipeline compatibility
precalculate_index_constituents = fetch_index_constituents_cache


if __name__ == "__main__":
    success = fetch_index_constituents_cache()
    exit(0 if success else 1)
