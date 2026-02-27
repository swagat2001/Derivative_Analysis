"""
Unified Index Model
Reads index constituents from pre-fetched JSON file for instant loading.
Falls back to NSE API if file not found, with hardcoded fallback if API fails.
"""

import json
import os
import time
import re
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import requests
from sqlalchemy import inspect, text

from .db_config import engine, engine_cash


INDEX_DATA_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
    "Data_scraper",
    "index_constituents.json",
)

_prefetched_indices = {}


def _load_prefetched_indices():
    """Load index data from database (primary) or pre-fetched JSON file (fallback)."""
    global _prefetched_indices

    try:
        query = text("SELECT index_key, symbol FROM index_constituents")
        with engine_cash.connect() as conn:
            result = conn.execute(query).fetchall()
            if result:
                db_data = {}
                for row in result:
                    key = row[0]
                    sym = row[1]
                    if key not in db_data:
                        db_data[key] = []
                    db_data[key].append(sym)
                _prefetched_indices = db_data
                print(f"[INFO] Loaded {len(_prefetched_indices)} indices from database")
                return True
    except Exception as e:
        import traceback
        print(f"[DEBUG] DB constituent load failed: {e}")
        # traceback.print_exc()

    if os.path.exists(INDEX_DATA_FILE):
        try:
            with open(INDEX_DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                _prefetched_indices = data.get("indices", {})
                print(f"[INFO] Loaded {len(_prefetched_indices)} indices from {os.path.basename(INDEX_DATA_FILE)}")
                return True
        except Exception as e:
            print(f"[WARNING] Failed to load index file: {e}")
    return False


_load_prefetched_indices()


# =============================================================================
# CACHE CONFIGURATION (fallback in-memory cache if file not available)
# =============================================================================
_cache = {}
CACHE_TTL_SECONDS = 86400  # 24 hours - fetches once per day


def _get_cached(key: str):
    """Get value from cache if not expired."""
    if key in _cache:
        value, timestamp = _cache[key]
        if time.time() - timestamp < CACHE_TTL_SECONDS:
            return value
    return None


def _set_cached(key: str, value):
    """Set value in cache with current timestamp."""
    _cache[key] = (value, time.time())


def clear_index_cache():
    """Clear the index cache (call after market close or for testing)."""
    global _cache
    _cache = {}


# =============================================================================
# NSE API CONFIGURATION
# =============================================================================
NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}

# NSE API URLs for index constituents
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
    "sensex": None,  # BSE - use fallback
}

# Index metadata
INDEX_METADATA = {
    "all": {"name": "All Cash Stocks", "description": "All stocks in the cash market", "icon": ""},
    "all_market": {"name": "All Market Stocks", "description": "All stocks in the cash market", "icon": ""},
    "nifty50": {"name": "Nifty 50", "description": "Top 50 companies by market cap", "icon": ""},
    "niftynext50": {"name": "Nifty Next 50", "description": "Next 50 companies after Nifty 50", "icon": ""},
    "niftybank": {"name": "Nifty Bank", "description": "Banking sector stocks", "icon": ""},
    "niftyit": {"name": "Nifty IT", "description": "Information Technology stocks", "icon": ""},
    "niftypharma": {"name": "Nifty Pharma", "description": "Pharmaceutical stocks", "icon": ""},
    "niftyauto": {"name": "Nifty Auto", "description": "Automobile sector stocks", "icon": ""},
    "niftymetal": {"name": "Nifty Metal", "description": "Metal & Mining stocks", "icon": ""},
    "niftyfmcg": {"name": "Nifty FMCG", "description": "Fast Moving Consumer Goods", "icon": ""},
    "niftyenergy": {"name": "Nifty Energy", "description": "Energy & Power stocks", "icon": ""},
    "niftypsubank": {"name": "Nifty PSU Bank", "description": "Public Sector Banks", "icon": ""},
    "niftyfinancial": {"name": "Nifty Financial", "description": "Financial Services stocks", "icon": ""},
    "sensex": {"name": "Sensex", "description": "BSE Top 30 companies", "icon": ""},
}


# =============================================================================
# NSE API SESSION
# =============================================================================
def _create_nse_session():
    """Create session with proper headers for NSE."""
    session = requests.Session()
    session.headers.update(NSE_HEADERS)
    try:
        session.get("https://www.nseindia.com", timeout=5)
    except Exception:
        pass
    return session


# =============================================================================
# CORE FUNCTIONS - INDEX FETCHING
# =============================================================================
def fetch_index_constituents(index_key: str) -> List[str]:
    """
    Fetch constituents for a given index.

    Priority order:
    1. Pre-fetched JSON file (instant) - from Data_scraper/index_constituents.json
    2. In-memory cache (instant)
    3. NSE API (slow, only if file not available)
    4. Hardcoded fallback (if all else fails)

    Args:
        index_key: Key like 'nifty50', 'niftybank', etc.

    Returns:
        List of stock symbols
    """
    if index_key == "all":
        return None

    # Normalize index key (UPPERCASE, alphanumeric only) to match DB
    norm_key = re.sub(r'[^A-Z0-9]', '', index_key.upper())

    # 1. Check pre-fetched data (which now includes DB data)
    if _prefetched_indices:
        # Check both the raw key and the normalized key
        if index_key in _prefetched_indices:
            return _prefetched_indices[index_key]
        if norm_key in _prefetched_indices:
            return _prefetched_indices[norm_key]
        # Also handle lowercase matches if any legacy JSON data exists
        if index_key.lower() in _prefetched_indices:
            return _prefetched_indices[index_key.lower()]

    # 1.5 Try querying DB directly for this index (if not in _prefetched_indices or out of sync)
    try:
        query = text("SELECT symbol FROM index_constituents WHERE index_key = :key OR index_name = :raw_key")
        with engine_cash.connect() as conn:
            result = conn.execute(query, {"key": norm_key, "raw_key": index_key}).fetchall()
            if result:
                constituents = [row[0] for row in result]
                # Update local cache for next time
                _prefetched_indices[norm_key] = constituents
                return constituents
    except Exception as e:
        print(f"[DEBUG] DB direct constituent fetch failed for {index_key}: {e}")

    # 2. Check in-memory cache
    cache_key = f"index_{norm_key}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    # 3. Try NSE API
    url = NSE_INDEX_URLS.get(index_key)
    if url:
        try:
            session = _create_nse_session()
            response = session.get(url, timeout=10)

            if response.status_code == 200:
                data = response.json()
                constituents = []

                for item in data.get("data", []):
                    symbol = item.get("symbol", "")
                    # Filter out the index name itself
                    if symbol and not symbol.startswith("NIFTY") and symbol != "SENSEX":
                        constituents.append(symbol)

                if constituents:
                    print(f"[INFO] Fetched {len(constituents)} constituents for {index_key} from NSE")
                    _set_cached(cache_key, constituents)
                    return constituents
        except Exception as e:
            print(f"[ERROR] Failed to fetch {index_key} from NSE: {e}")

    return []


def get_index_stocks(index_key: str) -> Optional[List[str]]:
    """
    Get list of stocks for a specific index.
    Returns None if index_key is 'all' or 'all_market' (means no filter).

    Uses NSE API with 24-hour caching - fetches once per day.
    After first fetch, all subsequent calls are instant from cache.

    Args:
        index_key: Key like 'nifty50', 'niftybank', 'all', etc.

    Returns:
        List of stock symbols or None for 'all'
    """
    if index_key in ["all", "all_market"]:
        return None

    return fetch_index_constituents(index_key)


def get_index_list() -> List[Dict]:
    """Return list of available indices for dropdown and Matrix."""
    indices = []
    seen = set()

    # 1. Add defaults from metadata
    for key, info in INDEX_METADATA.items():
        indices.append({"key": key, "name": info["name"], "icon": info.get("icon", "")})
        seen.add(key)

    # 2. Add all dynamically fetched indices from JSON
    if _prefetched_indices:
        for idx_key, constituents in _prefetched_indices.items():
            if idx_key not in seen and constituents:
                # Basic formatting for unknown indices (e.g. "niftymidcap50" -> "NIFTY MIDCAP 50")
                raw_name = idx_key.upper()
                if raw_name.startswith("NIFTY"):
                    # Check if it has ALPHA, QUALITY, etc to add spaces
                    # For now just use the name from the DB if available or format it
                    name = "NIFTY " + raw_name[5:]
                    # Heuristic: Add space before keywords
                    for kw in ["ALPHA", "QUALITY", "LOWVOLATILITY", "VALUE", "50", "100", "200", "500"]:
                        if kw in name and f" {kw}" not in name:
                            name = name.replace(kw, f" {kw}")
                    name = name.replace("LOWVOLATILITY", "LOW VOLATILITY")
                else:
                    name = raw_name

                indices.append({"key": idx_key, "name": name, "icon": ""})
                seen.add(idx_key)

    return indices


def get_index_info(index_key: str) -> Dict:
    """Get information about an index."""
    if index_key in INDEX_METADATA:
        return INDEX_METADATA[index_key]

    # Check dynamic indices
    indices = get_index_list()
    for idx in indices:
        if idx["key"] == index_key:
            return idx

    return INDEX_METADATA["all"]


def filter_stocks_by_index(stocks: list, index_key: str) -> list:
    """
    Filter a list of stock dicts by index.
    Expects stocks to have 'symbol' key.

    Args:
        stocks: List of stock dictionaries
        index_key: Index to filter by

    Returns:
        Filtered list of stocks
    """
    index_stocks = get_index_stocks(index_key)

    if index_stocks is None:
        return stocks  # No filter for 'all'

    # Normalize index stocks to uppercase
    index_stocks_upper = set(s.upper() for s in index_stocks)

    return [stock for stock in stocks if stock.get("symbol", "").upper() in index_stocks_upper]


def get_dynamic_indices(available_symbols: list) -> List[Dict]:
    """
    Get list of indices with count of matching stocks from available_symbols.
    Only returns indices that have at least 1 matching stock.

    Args:
        available_symbols: List of available stock symbols

    Returns:
        List of index dicts with counts
    """
    if not available_symbols:
        return [{"key": "all", "name": "All F&O Stocks", "icon": "", "count": 0}]

    available_upper = set(s.upper() for s in available_symbols)

    result = []
    indices = get_index_list()

    for idx_info in indices:
        key = idx_info["key"]
        if key == "all":
            result.insert(0, {"key": key, "name": idx_info["name"], "icon": idx_info["icon"], "count": len(available_symbols)})
        else:
            # Uses 24-hour cached data or DB data
            index_stocks = get_index_stocks(key)
            if index_stocks:
                index_upper = set(s.upper() for s in index_stocks)
                match_count = len(available_upper & index_upper)
                if match_count > 0:
                    result.append(
                        {
                            "key": key,
                            "name": f"{idx_info['name']} ({match_count})",
                            "icon": idx_info["icon"],
                            "count": match_count,
                        }
                    )

    if len(result) > 1:
        all_idx = result[0]
        rest = sorted(result[1:], key=lambda x: x["count"], reverse=True)
        result = [all_idx] + rest

    return result


# =============================================================================
# DATABASE ENRICHMENT FUNCTIONS (from index_constituents_model.py)
# =============================================================================
def get_stocks_data_from_db(tickers: List[str], date: str = None) -> pd.DataFrame:
    """
    Fetch stock data from database for given tickers.
    Uses futures_oi_cache for underlying prices.

    Args:
        tickers: List of stock symbols
        date: Date string (YYYY-MM-DD), defaults to latest date

    Returns:
        DataFrame with stock data
    """
    try:
        if not tickers:
            return pd.DataFrame()

        if not date:
            date_query = text(
                """
                SELECT DISTINCT cache_date
                FROM public.futures_oi_cache
                ORDER BY cache_date DESC
                LIMIT 1
            """
            )
            with engine.connect() as conn:
                result = conn.execute(date_query).fetchone()
                date = str(result[0]) if result else None

        if not date:
            print("[ERROR] No data available in futures_oi_cache")
            return pd.DataFrame()

        placeholders = ",".join([f"'{ticker}'" for ticker in tickers])
        query = text(
            f"""
            SELECT DISTINCT ON (ticker)
                ticker,
                underlying_price as price,
                0 as change,
                0 as change_pct,
                0 as volume,
                underlying_price as open,
                underlying_price as high,
                underlying_price as low,
                underlying_price as prev_close
            FROM public.futures_oi_cache
            WHERE cache_date = :date
            AND ticker IN ({placeholders})
            ORDER BY ticker, expiry_type
        """
        )

        df = pd.read_sql(query, engine, params={"date": date})

        inspector = inspect(engine)
        all_tables = inspector.get_table_names(schema="public")

        for _, row in df.iterrows():
            ticker = row["ticker"]
            table_name = f"TBL_{ticker}_DERIVED"

            if table_name in all_tables:
                try:
                    prev_query = text(
                        f"""
                        SELECT "UndrlygPric" as prev_price
                        FROM public."{table_name}"
                        WHERE "BizDt"::date < CAST(:date AS date)
                        GROUP BY "UndrlygPric", "BizDt"
                        ORDER BY "BizDt" DESC
                        LIMIT 1
                    """
                    )

                    prev_df = pd.read_sql(prev_query, engine, params={"date": date})
                    if not prev_df.empty:
                        prev_price = float(prev_df.iloc[0]["prev_price"])
                        curr_price = float(row["price"])
                        change = curr_price - prev_price
                        change_pct = (change / prev_price * 100) if prev_price > 0 else 0

                        df["change"] = df["change"].astype(float)
                        df["change_pct"] = df["change_pct"].astype(float)
                        df["prev_close"] = df["prev_close"].astype(float)

                        df.loc[df["ticker"] == ticker, "change"] = float(change)
                        df.loc[df["ticker"] == ticker, "change_pct"] = float(change_pct)
                        df.loc[df["ticker"] == ticker, "prev_close"] = float(prev_price)
                except Exception as e:
                    print(f"[DEBUG] Could not get detailed data for {ticker}: {e}")
                    continue

        print(f"[INFO] Fetched data for {len(df)} stocks from database for date {date}")
        return df

    except Exception as e:
        print(f"[ERROR] Failed to fetch stock data from database: {e}")
        return pd.DataFrame()


def get_stock_derivatives_data(tickers: List[str], date: str = None) -> pd.DataFrame:
    """
    Fetch derivatives data (OI, IV) from database.

    Args:
        tickers: List of stock symbols
        date: Date string

    Returns:
        DataFrame with OI and IV data
    """
    try:
        if not tickers or not date:
            return pd.DataFrame()

        placeholders = ",".join([f"'{ticker}'" for ticker in tickers])

        query = text(
            f"""
            SELECT
                ticker,
                SUM(expiry_oi) as total_oi,
                0 as avg_iv
            FROM public.futures_oi_cache
            WHERE cache_date = :date
            AND ticker IN ({placeholders})
            GROUP BY ticker
        """
        )

        df = pd.read_sql(query, engine, params={"date": date})
        return df

    except Exception as e:
        print(f"[ERROR] Failed to fetch derivatives data: {e}")
        return pd.DataFrame()


def enrich_with_signals(df: pd.DataFrame) -> pd.DataFrame:
    """Add signal column based on price change."""
    if df.empty:
        return df

    def get_signal(change_pct):
        if change_pct > 0.5:
            return "BULLISH"
        elif change_pct < -0.5:
            return "BEARISH"
        else:
            return "NEUTRAL"

    df["signal"] = df["change_pct"].apply(get_signal)
    return df


# =============================================================================
# HIGH-LEVEL FUNCTIONS FOR SCREENER PAGES (OPTIMIZED)
# =============================================================================
def get_index_stocks_with_data(index_key: str, date: str = None) -> List[Dict]:
    """
    Get stocks for an index with enriched data from multiple cache sources.

    OPTIMIZED: Uses pre-calculated cache tables for instant loading.
    Data sources:
    - Index constituents: Pre-fetched JSON file
    - Price/OI: futures_oi_cache table
    - Signals: signal_service.compute_signals_simple()

    Args:
        index_key: Index key like 'nifty50', 'niftybank', etc.
        date: Date string (YYYY-MM-DD) or None for latest

    Returns:
        List of dicts with stock data
    """
    tickers = fetch_index_constituents(index_key)
    if not tickers:
        return []

    try:
        if not date:
            date_query = text(
                """
                SELECT cache_date FROM public.futures_oi_cache
                ORDER BY cache_date DESC LIMIT 1
            """
            )
            with engine.connect() as conn:
                result = conn.execute(date_query).fetchone()
                date = str(result[0]) if result else None

        if not date:
            print(f"[WARNING] No date found in cache")
            return []

        def ticker_to_cache_name(ticker):
            return ticker.replace("-", "_").replace("&", "_")

        ticker_mapping = {t: ticker_to_cache_name(t) for t in tickers}
        cache_tickers = list(ticker_mapping.values())

        ticker_list = ",".join([f"'{t}'" for t in cache_tickers])

        cache_query = text(
            f"""
            WITH latest_data AS (
                SELECT
                    ticker,
                    underlying_price as price,
                    expiry_oi as oi,
                    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY expiry_date ASC) as rn
                FROM public.futures_oi_cache
                WHERE cache_date = :date
                AND ticker IN ({ticker_list})
                AND expiry_type = 'CME'
            )
            SELECT ticker, price, COALESCE(oi, 0) as oi
            FROM latest_data
            WHERE rn = 1
        """
        )

        with engine.connect() as conn:
            stocks_df = pd.read_sql(cache_query, conn, params={"date": date})

        prev_date_query = text(
            f"""
            WITH dates AS (
                SELECT DISTINCT cache_date
                FROM public.futures_oi_cache
                WHERE cache_date < :date
                ORDER BY cache_date DESC LIMIT 1
            ),
            prev_data AS (
                SELECT
                    ticker,
                    underlying_price as prev_price,
                    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY expiry_date ASC) as rn
                FROM public.futures_oi_cache
                WHERE cache_date = (SELECT cache_date FROM dates)
                AND ticker IN ({ticker_list})
                AND expiry_type = 'CME'
            )
            SELECT ticker, prev_price FROM prev_data WHERE rn = 1
        """
        )

        with engine.connect() as conn:
            prev_df = pd.read_sql(prev_date_query, conn, params={"date": date})
        if not prev_df.empty and not stocks_df.empty:
            stocks_df = stocks_df.merge(prev_df, on="ticker", how="left")
            stocks_df["prev_price"] = stocks_df["prev_price"].fillna(stocks_df["price"])
            stocks_df["change"] = stocks_df["price"] - stocks_df["prev_price"]
            stocks_df["change_pct"] = stocks_df.apply(
                lambda r: round((r["change"] / r["prev_price"] * 100), 2) if r["prev_price"] > 0 else 0, axis=1
            )
        else:
            stocks_df["prev_price"] = stocks_df["price"] if not stocks_df.empty else 0
            stocks_df["change"] = 0.0
            stocks_df["change_pct"] = 0.0

        reverse_mapping = {v: k for k, v in ticker_mapping.items()}
        if not stocks_df.empty:
            stocks_df["ticker"] = stocks_df["ticker"].map(reverse_mapping).fillna(stocks_df["ticker"])
        all_tickers_df = pd.DataFrame({"ticker": tickers})

        if stocks_df.empty:
            stocks_df = all_tickers_df.copy()
            stocks_df["price"] = 0.0
            stocks_df["change"] = 0.0
            stocks_df["change_pct"] = 0.0
            stocks_df["oi"] = 0
            stocks_df["iv"] = 0.0
            stocks_df["volume"] = 0
        else:
            stocks_df = all_tickers_df.merge(stocks_df, on="ticker", how="left")
            stocks_df = stocks_df.fillna({"price": 0.0, "change": 0.0, "change_pct": 0.0, "oi": 0, "prev_price": 0.0})
            stocks_df["iv"] = 0.0
            stocks_df["volume"] = 0

        try:
            inspector = inspect(engine)
            all_tables = set(inspector.get_table_names(schema="public"))

            def ticker_to_table(ticker):
                safe_ticker = ticker.replace("-", "_").replace("&", "_")
                return f"TBL_{safe_ticker}_DERIVED"

            union_parts = []
            for ticker in tickers:
                table_name = ticker_to_table(ticker)
                if table_name in all_tables:
                    union_parts.append(
                        f"""
                        SELECT
                            '{ticker}' as ticker,
                            COALESCE(SUM("TtlTradgVol"::NUMERIC), 0) as volume,
                            AVG(CASE WHEN ABS("strike_diff") <= 100 THEN "iv" ELSE NULL END) as atm_iv
                        FROM public."{table_name}"
                        WHERE "BizDt" = '{date}'
                        AND "OptnTp" IN ('CE', 'PE')
                    """
                    )

            if union_parts:
                volume_iv_query = " UNION ALL ".join(union_parts)
                with engine.connect() as conn:
                    vol_iv_df = pd.read_sql(text(volume_iv_query), conn)

                if not vol_iv_df.empty:
                    vol_iv_df["volume"] = pd.to_numeric(vol_iv_df["volume"], errors="coerce").fillna(0).astype(int)
                    vol_iv_df["atm_iv"] = pd.to_numeric(vol_iv_df["atm_iv"], errors="coerce").fillna(0).round(1)

                    for _, row in vol_iv_df.iterrows():
                        ticker = row["ticker"]
                        stocks_df.loc[stocks_df["ticker"] == ticker, "volume"] = int(row["volume"])
                        stocks_df.loc[stocks_df["ticker"] == ticker, "iv"] = float(row["atm_iv"])

                    print(f"[INFO] Got volume/IV data for {len(vol_iv_df)} stocks")
        except Exception as e:
            print(f"[WARNING] Could not get volume/IV data: {e}")

        try:
            from ..services.signal_service import compute_signals_simple

            signals = compute_signals_simple(date)
            stocks_df["signal"] = stocks_df["ticker"].apply(lambda t: signals.get(ticker_mapping.get(t, t), "NEUTRAL"))
            print(f"[INFO] Got {len(signals)} signals from signal_service")
        except Exception as e:
            print(f"[WARNING] Could not get signals from signal_service: {e}")
            import traceback

            traceback.print_exc()
        stocks_df["signal"] = stocks_df["signal"].fillna("NEUTRAL")

        stocks_df["volume"] = stocks_df["volume"].fillna(0).astype(int)
        stocks_df["oi"] = stocks_df["oi"].fillna(0).astype(int)
        stocks_df["price"] = stocks_df["price"].round(2)
        stocks_df["change"] = stocks_df["change"].round(2)
        stocks_df["change_pct"] = stocks_df["change_pct"].round(2)
        stocks_df["iv"] = stocks_df["iv"].round(1)

        result = stocks_df.to_dict("records")

        try:
            from ..services.fundamental_service import fundamental_service

            for stock in result:
                ticker = stock.get("ticker")
                fund_data = fundamental_service.get_stock_fundamentals(ticker)

                stock["market_cap"] = fund_data.get("market_cap", 0)
                stock["pe"] = fund_data.get("pe", 0)

                opm = fund_data.get('opm', 0.0) or 0.0
                net_profit = fund_data.get('net_profit', 0.0) or 0.0
                sales = fund_data.get('sales', 0.0) or 0.0
                pe = fund_data.get('pe', 0.0) or 0.0
                # Use OPM first; fall back to Net Profit Margin for banks/financials; then P/E
                if opm and opm != 0.0:
                    stock["custom_metric_value"] = f"OPM: {opm:.1f}%"
                elif sales and sales > 0 and net_profit:
                    npm = (net_profit / sales) * 100
                    stock["custom_metric_value"] = f"NPM: {npm:.1f}%"
                elif pe and pe > 0:
                    stock["custom_metric_value"] = f"P/E: {pe:.1f}x"
                else:
                    stock["custom_metric_value"] = "-"

        except Exception as e:
            print(f"[WARNING] Failed to enrich with fundamental data: {e}")

        stocks_with_data = len(stocks_df[stocks_df["price"] > 0])
        print(f"[INFO] Loaded {len(result)} {index_key} stocks from cache ({stocks_with_data} with data)")

        return result

    except Exception as e:
        print(f"[ERROR] get_index_stocks_with_data failed: {e}")
        import traceback

        traceback.print_exc()
        return []


# Convenience functions for backward compatibility
def get_nifty50_stocks_with_data(date: str = None) -> List[Dict]:
    """Get Nifty 50 stocks with enriched data."""
    return get_index_stocks_with_data("nifty50", date)


def get_banknifty_stocks_with_data(date: str = None) -> List[Dict]:
    """Get Bank Nifty stocks with enriched data."""
    return get_index_stocks_with_data("niftybank", date)


# Backward compatibility alias
def get_nifty50_constituents() -> List[str]:
    """Fetch current Nifty 50 constituents."""
    return fetch_index_constituents("nifty50")


def get_banknifty_constituents() -> List[str]:
    """Fetch current Bank Nifty constituents."""
    return fetch_index_constituents("niftybank")
