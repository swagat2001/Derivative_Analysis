# =============================================================
#  INSIGHTS MODEL MODULE
#  Purpose: Provides data for Market Insights dashboard including:
#  - Heatmap data (stock performance visualization)
#  - FII/DII activity data
#  - Delivery percentage data
#  - Market statistics
#  - 52-Week Analysis
#  - Volume Breakouts
#
#  UPDATED: Now uses CashStocks_Database as PRIMARY source for heatmap
#  This ensures REAL cash market prices (OHLCV) are displayed
#  F&O database is used only for options-specific data
# =============================================================

import json
import os
from datetime import datetime, timedelta
from functools import lru_cache

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

# Load environment variables from .env file
load_dotenv()

from .db_config import engine, engine_cash

# =============================================================
# SECTOR MAPPING FOR STOCKS
# =============================================================

# Build sector master paths from environment variables
def _get_sector_master_paths():
    """Get sector master CSV paths from environment variables with fallbacks."""
    paths = []

    # Primary path from environment
    primary_path = os.getenv("SECTOR_MASTER_PATH")
    if primary_path:
        paths.append(primary_path)

    # Fallback paths from environment (comma-separated)
    fallback_paths = os.getenv("SECTOR_MASTER_FALLBACK_PATHS", "")
    if fallback_paths:
        paths.extend([p.strip() for p in fallback_paths.split(",") if p.strip()])

    # Default fallback if no env vars set
    if not paths:
        paths = [
            "C:/Users/Admin/Desktop/Derivative_Analysis/nse_sector_master.csv",
            "C:/NSE_EOD_CASH_WITH_INDICATORS/nse_sector_master.csv",
            "C:/Users/Admin/Desktop/Derivative_Analysis/SMA/nse_sector_master.csv",
        ]

    # Always add local path as final fallback
    paths.append(os.path.join(os.path.dirname(__file__), "nse_sector_master.csv"))

    return paths

SECTOR_MASTER_PATHS = _get_sector_master_paths()

_sector_cache = {}
_sector_cache_loaded = False
DEFAULT_SECTOR = "Others"


def classify_industry(industry: str) -> str:
    """Map NSE_INDUSTRY into broad sectors."""
    if not isinstance(industry, str) or not industry.strip():
        return "Diversified / Others"

    s = industry.strip().lower()

    if any(x in s for x in ["diversified", "miscellaneous", "conglomerate"]):
        return "Diversified / Others"

    if any(
        x in s
        for x in [
            "bank",
            "nbfc",
            "finance",
            "financial",
            "housing finance",
            "stockbroking",
            "broker",
            "capital market",
            "securities",
            "insurance",
            "asset management",
            "mutual fund",
            "wealth management",
            "leasing",
        ]
    ):
        return "Financials"

    if any(
        x in s
        for x in [
            "software",
            "it enabled",
            "information technology",
            "it services",
            "data processing",
            "technology services",
            "internet",
            "computers",
        ]
    ):
        return "Information Technology"

    if any(
        x in s
        for x in ["pharma", "pharmaceutical", "drug", "diagnostic", "hospital", "healthcare", "biotech", "medical"]
    ):
        return "Healthcare & Pharma"

    if any(x in s for x in ["telecom", "telecommunication", "telephone", "cellular"]):
        return "Telecom"

    if any(
        x in s
        for x in [
            "power generation",
            "power distribution",
            "electricity",
            "utilities",
            "gas distribution",
            "water supply",
        ]
    ):
        return "Utilities"

    if any(
        x in s
        for x in [
            "oil",
            "gas",
            "refineries",
            "petroleum",
            "coal",
            "renewable energy",
            "wind energy",
            "solar energy",
            "energy",
        ]
    ):
        return "Energy"

    if any(
        x in s
        for x in [
            "steel",
            "iron",
            "metal",
            "mineral",
            "mining",
            "cement",
            "aluminium",
            "copper",
            "zinc",
            "paper",
            "glass",
            "chemicals",
            "fertilizer",
            "paints",
        ]
    ):
        return "Materials"

    if any(x in s for x in ["realty", "real estate", "developer", "construction", "contracting", "housing projects"]):
        return "Real Estate & Construction"

    if any(
        x in s
        for x in [
            "fmcg",
            "consumer non-durables",
            "food",
            "beverages",
            "tea",
            "coffee",
            "sugar",
            "dairy",
            "personal care",
            "toiletries",
            "tobacco",
        ]
    ):
        return "Consumer Staples"

    if any(
        x in s
        for x in [
            "auto",
            "automobile",
            "vehicle",
            "tyre",
            "consumer durables",
            "electronics",
            "retail",
            "hotel",
            "hospitality",
            "media",
            "entertainment",
            "textile",
            "garment",
            "apparel",
            "jewellery",
        ]
    ):
        return "Consumer Discretionary"

    if any(
        x in s
        for x in [
            "capital goods",
            "engineering",
            "machinery",
            "industrial",
            "automation",
            "electrical equipment",
            "logistics",
            "transport",
            "shipping",
            "ports",
            "airline",
            "defence",
            "aerospace",
        ]
    ):
        return "Industrials"

    return "Diversified / Others"


def _load_sector_master():
    """Load sector data from nse_sector_master.csv file."""
    global _sector_cache_loaded

    if _sector_cache_loaded:
        return

    for path in SECTOR_MASTER_PATHS:
        if os.path.exists(path):
            try:
                df = pd.read_csv(path, encoding="utf-8-sig")
                df.columns = df.columns.str.strip()

                for _, row in df.iterrows():
                    symbol = str(row.get("SYMBOL", "")).strip().upper()
                    sector = row.get("SECTOR", row.get("NSE_INDUSTRY", ""))

                    if symbol and sector:
                        # Use the sector directly from CSV instead of mapping it
                        _sector_cache[symbol] = str(sector).strip()

                print(f"[INFO] Loaded sector data for {len(_sector_cache)} stocks from {path}")
                _sector_cache_loaded = True
                # CSV loaded successfully - do NOT use JSON fallback
                return
            except Exception as e:
                print(f"[WARNING] Could not load sector master from {path}: {e}")

    # Only reach here if NO CSV file was found
    print("[WARNING] No sector master file found. Attempting to load from index_constituents.json...")

    # Fallback to JSON from Data_scraper
    json_path = r"C:\Users\Admin\Desktop\Derivative_Analysis\Data_scraper\index_constituents.json"
    if os.path.exists(json_path):
        try:
            with open(json_path, "r") as f:
                data = json.load(f)

            indices = data.get("indices", {})

            # Mapping from JSON index keys to Display Sector
            INDEX_TO_SECTOR = {
                "niftyit": "Information Technology",
                "niftybank": "Financials",
                "niftypsubank": "Financials",
                "niftyfinancial": "Financials",
                "niftypharma": "Healthcare & Pharma",
                "niftyauto": "Automobile",
                "niftymetal": "Metals & Mining",
                "niftyfmcg": "FMCG",
                "niftyenergy": "Energy",
            }

            count = 0
            for idx_key, idx_symbols in indices.items():
                sector_name = INDEX_TO_SECTOR.get(idx_key.lower())
                if sector_name:
                    for sym in idx_symbols:
                        sym_clean = str(sym).strip().upper()
                        # Only set if not already set (prioritize first assignment or overwrite?
                        # Specific sectors like IT should overwrite generic ones if any, but here we just process.
                        # Since a stock can be in Bank and Financial, it stays Financials.
                        if sym_clean not in _sector_cache:
                            _sector_cache[sym_clean] = sector_name
                            count += 1

            print(f"[INFO] Loaded sector data for {len(_sector_cache)} stocks from index_constituents.json")
            _sector_cache_loaded = True
            return

        except Exception as e:
            print(f"[WARNING] Could not load from index_constituents.json: {e}")

    print("[WARNING] No sector data found. Using classify_industry() only.")
    _sector_cache_loaded = True


def get_sector(symbol: str) -> str:
    """Get sector for a given stock symbol."""
    if not _sector_cache_loaded:
        _load_sector_master()

    symbol_upper = str(symbol).strip().upper()
    return _sector_cache.get(symbol_upper, DEFAULT_SECTOR)


def clear_sector_cache():
    """Clear sector cache."""
    global _sector_cache, _sector_cache_loaded
    _sector_cache = {}
    _sector_cache_loaded = False


# =============================================================
# 1️⃣ AVAILABLE DATES FOR INSIGHTS - FROM CASH DATABASE
# =============================================================


@lru_cache(maxsize=1)
def _get_insights_dates_cached():
    """
    Get available dates from CashStocks_Database.
    This ensures dates align with actual cash market trading days.
    """
    try:
        # Get list of tables in cash database
        query = text(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name LIKE 'TBL_%'
            ORDER BY table_name
            LIMIT 1;
        """
        )
        with engine_cash.connect() as conn:
            result = conn.execute(query)
            tables = [row[0] for row in result]

        if not tables:
            print("[WARN] No tables found in cash database for dates")
            return tuple()

        # Use first available table to get dates
        sample_table = tables[0]
        date_query = text(
            f"""
            SELECT DISTINCT CAST("BizDt" AS DATE)::text AS date
            FROM public."{sample_table}"
            WHERE "BizDt" IS NOT NULL
            ORDER BY date DESC
            LIMIT 100;
        """
        )
        df = pd.read_sql(date_query, con=engine_cash)

        if df.empty:
            print(f"[WARN] No dates found in {sample_table}")
            return tuple()

        print(f"[INFO] Found {len(df)} dates from Cash database")
        return tuple(df["date"].tolist())

    except Exception as e:
        print(f"[ERROR] _get_insights_dates_cached(): {e}")
        import traceback

        traceback.print_exc()
        return tuple()


def get_insights_dates():
    """Get available dates for insights dashboard from Cash database."""
    return list(_get_insights_dates_cached())


def clear_insights_cache():
    """Clear all insights caches."""
    _get_insights_dates_cached.cache_clear()
    _get_fo_symbols_cached.cache_clear()
    _get_heatmap_data_cached.cache_clear()
    _get_fii_dii_data_cached.cache_clear()
    _get_fii_derivatives_data_cached.cache_clear()
    _get_delivery_data_cached.cache_clear()
    _get_market_stats_cached.cache_clear()
    _get_volume_breakouts_cached.cache_clear()
    _get_52_week_data_cached.cache_clear()
    print("[INFO] Insights cache cleared")


# =============================================================
# 2️⃣ HEATMAP DATA - FROM CASH DATABASE (REAL OHLCV DATA)
#    Like ScanX - Uses actual Cash market prices
#    FILTERED: Shows ONLY F&O stocks (stocks with derivatives)
# =============================================================


@lru_cache(maxsize=1)
def _get_fo_symbols_cached():
    """
    Get list of symbols that have F&O derivatives.
    Returns set of symbols that exist in BhavCopy_Database (F&O database).
    """
    try:
        query = text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name LIKE 'TBL_%'
            AND table_name NOT LIKE '%_DERIVED'
            ORDER BY table_name;
        """)

        with engine.connect() as conn:
            result = conn.execute(query)
            tables = [row[0] for row in result]

        # Extract symbol names from table names (TBL_NIFTY -> NIFTY)
        fo_symbols = set(table.replace('TBL_', '') for table in tables if table.startswith('TBL_'))

        print(f"[INFO] Found {len(fo_symbols)} F&O symbols in BhavCopy_Database")
        return frozenset(fo_symbols)  # frozenset is hashable for caching

    except Exception as e:
        print(f"[ERROR] _get_fo_symbols_cached(): {e}")
        return frozenset()


@lru_cache(maxsize=32)
def _get_heatmap_data_cached(selected_date: str):
    """
    Get heatmap data from daily_market_heatmap table.

    Note: Table population is now handled by Database/Cash/heatmap_cache.py.
    This function is READ-ONLY.

    Returns: tuple of (symbol, close, change_pct, volume, turnover, sector, high, low)
    """
    # Try Fetching from Cache Table
    try:
        query_cache = text(
            """
            SELECT symbol, close, change_pct, volume, turnover, sector, high, low
            FROM daily_market_heatmap
            WHERE date = DATE :result_date
        """
        )

        with engine_cash.connect() as conn:
            cached_df = pd.read_sql(query_cache, con=conn, params={"result_date": selected_date})

        if not cached_df.empty:
            # FILTER: Only F&O stocks
            fo_symbols = _get_fo_symbols_cached()
            if fo_symbols:
                cached_df = cached_df[cached_df["symbol"].isin(fo_symbols)]
                print(f"[INFO] Filtered to {len(cached_df)} F&O stocks (from {len(cached_df) + len(set(cached_df['symbol']) - fo_symbols)} total)")

            if cached_df.empty:
                print(f"[WARN] No F&O stocks found for {selected_date} after filtering")
                return tuple()

            # OPTIMIZATION: Vectorized type conversion (100x faster than iterrows)
            cached_df["symbol"] = cached_df["symbol"].astype(str)
            cached_df["close"] = cached_df["close"].astype(float)
            cached_df["change_pct"] = cached_df["change_pct"].astype(float)
            cached_df["volume"] = cached_df["volume"].astype(int)
            cached_df["turnover"] = cached_df["turnover"].astype(float)
            cached_df["sector"] = cached_df["sector"].astype(str)
            cached_df["high"] = cached_df["high"].astype(float)
            cached_df["low"] = cached_df["low"].astype(float)

            # Convert to list of tuples directly
            return tuple(
                cached_df[["symbol", "close", "change_pct", "volume", "turnover", "sector", "high", "low"]].itertuples(
                    index=False, name=None
                )
            )
        else:
            print(f"[WARN] No heatmap data found for {selected_date} in cache table.")
            return tuple()

    except Exception as e:
        print(f"[ERROR] Heatmap cache fetch failed: {e}")
        return tuple()


def get_heatmap_data(selected_date: str, period: str = "1D", comparison_date: str = None):
    """
    Get heatmap data with multi-timeframe support.

    Args:
        selected_date (str): The target date (YYYY-MM-DD).
        period (str): '1D', '1W', '1M', '3M', '6M', '1Y'.
        comparison_date (str): Optional override for comparison date.

    Returns:
        list: List of dicts with symbol, close, change_pct (for period), etc.
    """
    cached_data = _get_heatmap_data_cached(selected_date)
    if not cached_data:
        return []

    # If standard daily view, return fast
    if period == "1D" and not comparison_date:
        result = []
        for row in cached_data:
            result.append(
                {
                    "symbol": row[0],
                    "close": row[1],
                    "change_pct": row[2],
                    "volume": row[3],
                    "turnover": row[4],
                    "sector": row[5],
                    "high": row[6],
                    "low": row[7],
                }
            )
        return result

    # Calculate Start Date
    if comparison_date:
        start_date = comparison_date
    else:
        try:
            date_obj = datetime.strptime(selected_date, "%Y-%m-%d")
            delta_map = {"1W": 7, "1M": 30, "3M": 90, "6M": 180, "1Y": 365}
            delta = delta_map.get(period, 0)
            if delta == 0:
                # Default fallback
                start_date = selected_date
            else:
                start_date = (date_obj - timedelta(days=delta)).strftime("%Y-%m-%d")
        except:
            start_date = selected_date

    if start_date == selected_date:
        # Same as 1D
        result = []
        for row in cached_data:
            result.append(
                {
                    "symbol": row[0],
                    "close": row[1],
                    "change_pct": row[2],
                    "volume": row[3],
                    "turnover": row[4],
                    "sector": row[5],
                    "high": row[6],
                    "low": row[7],
                }
            )
        return result

    # Fetch Timestamp Data
    # NOTE: This recursively calls the cached function, which will AUTO-CALCULATE
    # and AUTO-CACHE the historical data if it's missing!
    print(f"[PERIOD] Fetching historical data for {period} ({start_date})")
    start_data_raw = _get_heatmap_data_cached(start_date)

    # Fast Lookup
    start_prices = {row[0]: row[1] for row in start_data_raw}

    # Compute Change
    result = []
    for row in cached_data:
        symbol = row[0]
        current_close = row[1]

        start_close = start_prices.get(symbol, current_close)

        if start_close > 0:
            change_pct = ((current_close - start_close) / start_close) * 100
        else:
            change_pct = 0.0

        result.append(
            {
                "symbol": symbol,
                "close": current_close,
                "change_pct": round(change_pct, 2),
                "volume": row[3],
                "turnover": row[4],
                "sector": row[5],
                "high": row[6],
                "low": row[7],
            }
        )

    return result


def get_sector_performance(selected_date: str, comparison_date: str = None):
    """Get sector-wise performance aggregated from stock data."""
    # Load sector master to get all sectors
    if not _sector_cache_loaded:
        _load_sector_master()

    # Get all unique sectors from the CSV
    all_sectors = set(_sector_cache.values()) if _sector_cache else set()

    # Initialize sector_data with all sectors
    sector_data = {}
    for sector in all_sectors:
        sector_data[sector] = {
            "sector": sector,
            "stocks": [],
            "total_change": 0,
            "total_turnover": 0,
            "count": 0,
        }

    # Get heatmap data
    heatmap_data = get_heatmap_data(selected_date, comparison_date)

    # Populate with actual stock data
    if heatmap_data:
        for stock in heatmap_data:
            sector = stock["sector"]
            # Add sector if it wasn't in the CSV (fallback)
            if sector not in sector_data:
                sector_data[sector] = {
                    "sector": sector,
                    "stocks": [],
                    "total_change": 0,
                    "total_turnover": 0,
                    "count": 0,
                }

            sector_data[sector]["stocks"].append(stock["symbol"])
            sector_data[sector]["total_change"] += stock["change_pct"]
            sector_data[sector]["total_turnover"] += stock["turnover"]
            sector_data[sector]["count"] += 1

    result = []
    for sector, data in sector_data.items():
        result.append(
            {
                "sector": sector,
                "avg_change": round(data["total_change"] / data["count"], 2) if data["count"] > 0 else 0,
                "total_turnover": data["total_turnover"],
                "stock_count": data["count"],
                "stocks": data["stocks"][:5],  # Top 5 stocks
            }
        )

    result.sort(key=lambda x: x["avg_change"], reverse=True)
    return result


# =============================================================
# 3️⃣ FII/DII DATA - REAL INSTITUTIONAL ACTIVITY
#    Uses fii_dii_activity table with actual FII/DII cash market data
#    Data sourced from NSE via fii_dii_scraper.py
# =============================================================


def _check_fii_dii_table_exists():
    """Check if FII/DII table exists in database."""
    try:
        query = text(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = 'fii_dii_activity'
            );
        """
        )
        with engine.connect() as conn:
            result = conn.execute(query).fetchone()
            return result[0] if result else False
    except Exception as e:
        print(f"[ERROR] Checking FII/DII table: {e}")
        return False


@lru_cache(maxsize=64)
def _get_fii_dii_data_cached(start_date: str, end_date: str):
    """
    Get REAL FII/DII activity data from fii_dii_activity table.

    This returns actual institutional investor cash market activity:
    - FII (Foreign Institutional Investors) buy/sell values
    - DII (Domestic Institutional Investors) buy/sell values
    - Net investment values in Crores (₹)

    Data is sourced from NSE and stored via fii_dii_scraper.py
    """
    try:
        # First check if table exists
        if not _check_fii_dii_table_exists():
            print("[WARN] fii_dii_activity table does not exist. Run: python fii_dii_scraper.py setup")
            return tuple()

        query = text(
            """
            SELECT
                trade_date::text as date,
                COALESCE(fii_buy_value, 0) as fii_buy_value,
                COALESCE(fii_sell_value, 0) as fii_sell_value,
                COALESCE(fii_net_value, 0) as fii_net_value,
                COALESCE(dii_buy_value, 0) as dii_buy_value,
                COALESCE(dii_sell_value, 0) as dii_sell_value,
                COALESCE(dii_net_value, 0) as dii_net_value,
                COALESCE(total_net_value, 0) as total_net_value
            FROM fii_dii_activity
            WHERE trade_date BETWEEN :start_date AND :end_date
            ORDER BY trade_date DESC
        """
        )

        df = pd.read_sql(query, con=engine, params={"start_date": start_date, "end_date": end_date})

        if df.empty:
            print(f"[INFO] No FII/DII data found between {start_date} and {end_date}")
            return tuple()

        print(f"[INFO] Found {len(df)} days of FII/DII activity data")

        result = []
        for _, row in df.iterrows():
            result.append(
                (
                    row["date"],
                    float(row["fii_buy_value"]),
                    float(row["fii_sell_value"]),
                    float(row["fii_net_value"]),
                    float(row["dii_buy_value"]),
                    float(row["dii_sell_value"]),
                    float(row["dii_net_value"]),
                    float(row["total_net_value"]),
                )
            )

        return tuple(result)

    except Exception as e:
        print(f"[ERROR] _get_fii_dii_data_cached(): {e}")
        import traceback

        traceback.print_exc()
        return tuple()


def get_fii_dii_data(start_date: str, end_date: str):
    """
    Get FII/DII activity data for the given date range.

    Returns list of daily records with:
    - date: Trading date
    - fii_buy_value: FII buy value in Cr
    - fii_sell_value: FII sell value in Cr
    - fii_net_value: FII net (buy - sell) in Cr
    - dii_buy_value: DII buy value in Cr
    - dii_sell_value: DII sell value in Cr
    - dii_net_value: DII net (buy - sell) in Cr
    - total_net_value: Combined FII + DII net
    - sentiment: Bullish/Bearish/Neutral based on total net
    """
    cached_data = _get_fii_dii_data_cached(start_date, end_date)

    if not cached_data:
        return []

    result = []
    for row in cached_data:
        fii_net = float(row[3]) if row[3] is not None else 0
        dii_net = float(row[6]) if row[6] is not None else 0
        total_net = float(row[7]) if row[7] is not None else 0

        # Determine sentiment based on total institutional activity
        if total_net > 100:
            sentiment = "Bullish"
        elif total_net < -100:
            sentiment = "Bearish"
        else:
            sentiment = "Neutral"

        result.append(
            {
                "date": row[0],
                "fii_buy_value": float(row[1]) if row[1] else 0,
                "fii_sell_value": float(row[2]) if row[2] else 0,
                "fii_net_value": fii_net,
                "dii_buy_value": float(row[4]) if row[4] else 0,
                "dii_sell_value": float(row[5]) if row[5] else 0,
                "dii_net_value": dii_net,
                "total_net_value": total_net,
                "sentiment": sentiment,
            }
        )

    return result


def get_fii_dii_summary(start_date: str, end_date: str):
    """
    Get summary of FII/DII activity for the date range.

    Returns:
        dict with total FII, DII, and combined activity
    """
    data = get_fii_dii_data(start_date, end_date)

    if not data:
        return {
            "total_fii_buy": 0,
            "total_fii_sell": 0,
            "total_fii_net": 0,
            "total_dii_buy": 0,
            "total_dii_sell": 0,
            "total_dii_net": 0,
            "combined_net": 0,
            "overall_sentiment": "Neutral",
            "days_count": 0,
        }

    total_fii_buy = sum(d["fii_buy_value"] for d in data)
    total_fii_sell = sum(d["fii_sell_value"] for d in data)
    total_fii_net = sum(d["fii_net_value"] for d in data)
    total_dii_buy = sum(d["dii_buy_value"] for d in data)
    total_dii_sell = sum(d["dii_sell_value"] for d in data)
    total_dii_net = sum(d["dii_net_value"] for d in data)
    combined_net = total_fii_net + total_dii_net

    if combined_net > 500:
        sentiment = "Bullish"
    elif combined_net < -500:
        sentiment = "Bearish"
    else:
        sentiment = "Neutral"

    return {
        "total_fii_buy": round(total_fii_buy, 2),
        "total_fii_sell": round(total_fii_sell, 2),
        "total_fii_net": round(total_fii_net, 2),
        "total_dii_buy": round(total_dii_buy, 2),
        "total_dii_sell": round(total_dii_sell, 2),
        "total_dii_net": round(total_dii_net, 2),
        "combined_net": round(combined_net, 2),
        "overall_sentiment": sentiment,
        "days_count": len(data),
    }


@lru_cache(maxsize=64)
def _get_fii_derivatives_data_cached(start_date: str, end_date: str):
    """
    Get FII Derivatives data from fii_derivatives_activity table.
    """
    try:
        query = text(
            """
            SELECT
                trade_date::text as date,
                category,
                buy_value,
                sell_value,
                oi_value,
                oi_contracts,
                COALESCE(participant_type, 'FII') as participant_type,
                COALESCE(oi_long, 0) as oi_long,
                COALESCE(oi_short, 0) as oi_short
            FROM fii_derivatives_activity
            WHERE trade_date BETWEEN :start_date AND :end_date
            ORDER BY trade_date DESC, category ASC
        """
        )

        df = pd.read_sql(query, con=engine, params={"start_date": start_date, "end_date": end_date})

        if df.empty:
            return tuple()

        result = []
        for _, row in df.iterrows():
            result.append(
                (
                    row["date"],
                    row["category"],
                    float(row["buy_value"]),
                    float(row["sell_value"]),
                    float(row["oi_value"]),
                    int(row["oi_contracts"]) if row["oi_contracts"] else 0,
                    row["participant_type"],
                    int(row["oi_long"]),
                    int(row["oi_short"]),
                )
            )

        return tuple(result)
    except Exception as e:
        print(f"[ERROR] _get_fii_derivatives_data_cached(): {e}")
        return tuple()


def get_fii_derivatives_data(start_date: str, end_date: str):
    """
    Get structured FII derivatives data for API.
    Returns dictionary grouped by date.
    """
    cached_data = _get_fii_derivatives_data_cached(start_date, end_date)

    if not cached_data:
        return {}

    # Group by date
    grouped = {}
    for row in cached_data:
        date = row[0]
        # Normalize category to snake_case (e.g. "Index Futures" -> "index_futures")
        raw_cat = row[1] if row[1] else ""
        category = raw_cat.lower().replace(" ", "_")

        buy_val = row[2]
        sell_val = row[3]
        oi_val = row[4]
        oi_con = row[5]
        p_type = row[6]
        # Handle new columns safely (in case of cache mismatch before reload, though reload should clear it)
        oi_long = row[7] if len(row) > 7 else 0
        oi_short = row[8] if len(row) > 8 else 0

        if date not in grouped:
            grouped[date] = []

        net_value = buy_val - sell_val  # Buy - Sell

        grouped[date].append(
            {
                "category": category,
                "participant_type": p_type,
                "buy_value": buy_val,
                "sell_value": sell_val,
                "net_value": round(net_value, 2),
                "oi_value": oi_val,
                "oi_contracts": oi_con,
                "oi_long": oi_long,
                "oi_short": oi_short,
            }
        )

    return grouped


@lru_cache(maxsize=64)
def _get_nifty50_data_cached(start_date: str, end_date: str):
    """
    Get Nifty 50 closing prices from BhavCopy_Database .
    Looks for TBL_NIFTY_50, TBL_NIFTY, or similar table.
    """
    try:
        # Try multiple possible table names
        table_names = ["TBL_NIFTY", "TBL_NIFTY_50", "TBL_NIFTY50", "TBL_NIFTY_DERIVED"]

        for table_name in table_names:
            try:
                query = text(
                    f"""
                    SELECT
                        CAST("BizDt" AS DATE)::text as date,
                        MAX(CAST("UndrlygPric" AS NUMERIC)) as close
                    FROM "{table_name}"
                    WHERE CAST("BizDt" AS DATE) BETWEEN CAST(:start_date AS DATE) AND CAST(:end_date AS DATE)
                    AND "UndrlygPric" IS NOT NULL
                    GROUP BY "BizDt"
                    ORDER BY "BizDt" ASC
                """
                )

                with engine.connect() as conn:
                    df = pd.read_sql(query, conn, params={"start_date": start_date, "end_date": end_date})

                if not df.empty:
                    print(f"[INFO] Found {len(df)} days of Nifty 50 data from {table_name} in BhavCopy_Database")
                    result = []
                    for _, row in df.iterrows():
                        result.append((row["date"], float(row["close"])))
                    return tuple(result)
            except Exception:
                continue  # Try next table name

        print(f"[WARN] No Nifty 50 data found in BhavCopy_Database")
        return tuple()

    except Exception as e:
        print(f"[ERROR] _get_nifty50_data_cached(): {e}")
        return tuple()


def get_nifty50_data(start_date: str, end_date: str):
    """
    Get Nifty 50 closing prices for the date range.
    Returns dictionary with date -> close price mapping.
    """
    cached_data = _get_nifty50_data_cached(start_date, end_date)

    if not cached_data:
        return {}

    result = {}
    for row in cached_data:
        result[row[0]] = row[1]

    return result


# =============================================================
# 4️⃣ DELIVERY DATA - From Cash Database
# =============================================================


@lru_cache(maxsize=32)
def _get_delivery_data_cached(selected_date: str):
    """
    Get delivery percentage data from cache table.
    OPTIMIZED: Reads from daily_delivery_data table (single query).
    """
    try:
        print(f"[INFO] Fetching delivery data for {selected_date} from cache...")

        query = text("""
            SELECT symbol, close, volume, delivery_qty, delivery_pct
            FROM daily_delivery_data
            WHERE date = :selected_date
            ORDER BY delivery_pct DESC
        """)

        with engine_cash.connect() as conn:
            df = pd.read_sql(query, conn, params={"selected_date": selected_date})

        if df.empty:
            print(f"[WARN] No delivery data in cache for {selected_date}")
            print("[INFO] Run: python build_delivery_cache.py")
            return tuple()

        print(f"[INFO] Loaded {len(df)} stocks with delivery data in < 0.1s")

        result = []
        for _, row in df.iterrows():
            result.append((
                str(row["symbol"]),
                float(row["close"]) if pd.notna(row["close"]) else 0,
                int(row["volume"]) if pd.notna(row["volume"]) else 0,
                int(row["delivery_qty"]) if pd.notna(row["delivery_qty"]) else 0,
                float(row["delivery_pct"]) if pd.notna(row["delivery_pct"]) else 0,
            ))

        return tuple(result)

    except Exception as e:
        print(f"[ERROR] _get_delivery_data_cached(): {e}")
        import traceback
        traceback.print_exc()
        return tuple()


def get_delivery_data(selected_date: str, min_delivery_pct: float = 0):
    """Get stocks with high delivery percentage (F&O stocks only)."""
    cached_data = _get_delivery_data_cached(selected_date)

    if not cached_data:
        print(f"[WARN] No cached delivery data for {selected_date}, returning empty list")
        return []

    # Get F&O symbols for filtering
    fo_symbols = _get_fo_symbols_cached()

    # Get heatmap data for price change info
    heatmap_data = get_heatmap_data(selected_date)
    heatmap_map = {item['symbol']: item for item in heatmap_data}

    result = []
    for row in cached_data:
        symbol = row[0]

        # Filter: Only F&O stocks
        if fo_symbols and symbol not in fo_symbols:
            continue

        if row[4] >= min_delivery_pct:
            # Get change data from heatmap
            stock_data = heatmap_map.get(symbol, {})
            change_pct = stock_data.get('change_pct', 0)

            # Calculate absolute change
            close = row[1]
            try:
                prev_close = close / (1 + (change_pct / 100))
                change = close - prev_close
            except:
                change = 0

            result.append(
                {
                    "symbol": symbol,
                    "close": row[1],
                    "volume": row[2],
                    "delivery_qty": row[3],
                    "delivery_pct": row[4],
                    "sector": get_sector(symbol),
                    "change_pct": change_pct,
                    "change": round(change, 2)
                }
            )

    if fo_symbols:
        print(f"[INFO] Filtered to {len(result)} F&O stocks with delivery_pct >= {min_delivery_pct}")
    else:
        print(f"[INFO] Filtered to {len(result)} stocks with delivery_pct >= {min_delivery_pct}")
    return result


# =============================================================
# 5️⃣ MARKET STATS
# =============================================================


@lru_cache(maxsize=32)
def _get_market_stats_cached(selected_date: str):
    """Get overall market statistics."""
    try:
        heatmap_data = get_heatmap_data(selected_date)

        if not heatmap_data:
            return None

        advances = len([s for s in heatmap_data if s["change_pct"] > 0])
        declines = len([s for s in heatmap_data if s["change_pct"] < 0])
        unchanged = len([s for s in heatmap_data if s["change_pct"] == 0])
        total = len(heatmap_data)

        sorted_by_change = sorted(heatmap_data, key=lambda x: x["change_pct"], reverse=True)
        top_gainers = sorted_by_change[:5]
        top_losers = sorted_by_change[-5:][::-1]

        adv_dec_ratio = round(advances / declines, 2) if declines > 0 else advances
        avg_change = round(sum(s["change_pct"] for s in heatmap_data) / total, 2) if total > 0 else 0
        total_turnover = sum(s["turnover"] for s in heatmap_data)

        # Circuit Breaker Logic (Heuristic: Close = High/Low with non-zero change)
        upper_circuit_count = len([s for s in heatmap_data if s["close"] == s["high"] and s["change_pct"] > 0])
        lower_circuit_count = len([s for s in heatmap_data if s["close"] == s["low"] and s["change_pct"] < 0])

        return {
            "advances": advances,
            "declines": declines,
            "unchanged": unchanged,
            "total": total,
            "adv_dec_ratio": adv_dec_ratio,
            "avg_change": avg_change,
            "total_turnover": total_turnover,
            "top_gainers": top_gainers,
            "top_losers": top_losers,
            "market_sentiment": "Bullish" if adv_dec_ratio > 1.5 else "Bearish" if adv_dec_ratio < 0.67 else "Neutral",
            "upper_circuits": upper_circuit_count,
            "lower_circuits": lower_circuit_count,
        }

    except Exception as e:
        print(f"[ERROR] _get_market_stats_cached(): {e}")
        return None


def get_market_stats(selected_date: str):
    """Get market statistics for the given date."""
    return _get_market_stats_cached(selected_date)


# =============================================================
# 6️⃣ 52-WEEK HIGH/LOW ANALYSIS
# =============================================================


@lru_cache(maxsize=32)
def _get_52_week_data_cached(selected_date: str):
    """Calculates 52-week High/Low for F&O stocks using CashStocks_Database."""
    try:
        heatmap_data = get_heatmap_data(selected_date)
        if not heatmap_data:
            return {"near_high": [], "near_low": [], "at_high": [], "at_low": []}

        start_date = (pd.to_datetime(selected_date) - pd.DateOffset(years=1)).strftime("%Y-%m-%d")

        at_high_threshold = 0.02  # Within 2% of High
        near_high_threshold = 0.05  # Within 5% of High
        at_low_threshold = 0.02  # Within 2% of Low
        near_low_threshold = 0.05  # Within 5% of Low

        results = {"at_high": [], "near_high": [], "at_low": [], "near_low": []}

        print(f"[INFO] Calculating 52-Week High/Low for {len(heatmap_data)} stocks...")

        with engine_cash.connect() as conn:
            for stock in heatmap_data:
                symbol = stock["symbol"]
                current_close = stock["close"]
                table_name = f"TBL_{symbol}"

                # Check if table exists (simple try-except specific to query failure)
                try:
                    query = text(
                        f'SELECT MAX(CAST("HghPric" AS NUMERIC)) as high_52w, '
                        f'MIN(CAST("LwPric" AS NUMERIC)) as low_52w '
                        f'FROM public."{table_name}" '
                        f'WHERE CAST("BizDt" AS DATE) BETWEEN CAST(:start_date AS DATE) AND CAST(:end_date AS DATE)'
                    )

                    row = conn.execute(query, {"start_date": start_date, "end_date": selected_date}).fetchone()

                    if row and row[0] is not None and row[1] is not None:
                        high_52w = float(row[0])
                        low_52w = float(row[1])

                        if high_52w <= 0 or low_52w <= 0:
                            continue

                        # Check Categories

                        # High Side
                        diff_high = (high_52w - current_close) / high_52w
                        if diff_high <= at_high_threshold:
                            stock_copy = stock.copy()
                            stock_copy["52w_high"] = high_52w
                            stock_copy["away_pct"] = round(diff_high * 100, 2)
                            results["at_high"].append(stock_copy)
                        elif diff_high <= near_high_threshold:
                            stock_copy = stock.copy()
                            stock_copy["52w_high"] = high_52w
                            stock_copy["away_pct"] = round(diff_high * 100, 2)
                            results["near_high"].append(stock_copy)

                        # Low Side
                        diff_low = (current_close - low_52w) / low_52w
                        if diff_low <= at_low_threshold:
                            stock_copy = stock.copy()
                            stock_copy["52w_low"] = low_52w
                            stock_copy["above_pct"] = round(diff_low * 100, 2)
                            results["at_low"].append(stock_copy)
                        elif diff_low <= near_low_threshold:
                            stock_copy = stock.copy()
                            stock_copy["52w_low"] = low_52w
                            stock_copy["above_pct"] = round(diff_low * 100, 2)
                            results["near_low"].append(stock_copy)

                except Exception as e:
                    # Table might not exist in Cash DB or permission error
                    pass

        return results

    except Exception as e:
        print(f"[ERROR] _get_52_week_data_cached(): {e}")
        return {"near_high": [], "near_low": [], "at_high": [], "at_low": []}


def get_52_week_analysis(selected_date: str):
    """Get stocks near 52-week high/low using heatmap data."""
    return _get_52_week_data_cached(selected_date)


# =============================================================
# 7️⃣ VOLUME BREAKOUTS - REAL IMPLEMENTATION
#    Calculates actual 20-day average volume from cash database
# =============================================================


@lru_cache(maxsize=32)
def _get_volume_breakouts_cached(selected_date: str):
    """
    Get stocks with unusual volume by calculating REAL 20-day average volume.
    Returns stocks where today's volume >= multiplier * avg_20d_volume.
    """
    try:
        heatmap_data = get_heatmap_data(selected_date)
        if not heatmap_data:
            return tuple()

        # Calculate date range for 20-day average (exclude current date)
        end_dt = pd.to_datetime(selected_date)
        start_dt = end_dt - pd.DateOffset(days=30)  # Get 30 days to ensure 20 trading days
        start_date = start_dt.strftime("%Y-%m-%d")

        print(f"[INFO] Calculating volume breakouts for {len(heatmap_data)} stocks...")
        print(f"[INFO] Fetching 20-day avg volume from {start_date} to {selected_date}")

        results = []
        processed = 0
        errors = 0

        with engine_cash.connect() as conn:
            for stock in heatmap_data:
                symbol = stock["symbol"]
                table_name = f"TBL_{symbol}"

                try:
                    # Query to get today's volume AND 20-day average (excluding today)
                    query = text(
                        f"""
                        WITH today_data AS (
                            SELECT
                                CAST("TtlTradgVol" AS NUMERIC) as today_volume,
                                CAST("ClsPric" AS NUMERIC) as close_price
                            FROM public."{table_name}"
                            WHERE CAST("BizDt" AS DATE) = CAST(:selected_date AS DATE)
                            AND "TtlTradgVol" IS NOT NULL
                            LIMIT 1
                        ),
                        historical_avg AS (
                            SELECT
                                AVG(CAST("TtlTradgVol" AS NUMERIC)) as avg_volume,
                                COUNT(*) as trading_days
                            FROM (
                                SELECT "TtlTradgVol"
                                FROM public."{table_name}"
                                WHERE CAST("BizDt" AS DATE) >= CAST(:start_date AS DATE)
                                AND CAST("BizDt" AS DATE) < CAST(:selected_date AS DATE)
                                AND "TtlTradgVol" IS NOT NULL
                                AND CAST("TtlTradgVol" AS NUMERIC) > 0
                                ORDER BY "BizDt" DESC
                                LIMIT 20
                            ) as recent_data
                        )
                        SELECT
                            t.today_volume,
                            t.close_price,
                            h.avg_volume,
                            h.trading_days
                        FROM today_data t, historical_avg h
                    """
                    )

                    result = conn.execute(query, {"selected_date": selected_date, "start_date": start_date}).fetchone()

                    if result and result[0] and result[2] and result[2] > 0:
                        today_volume = float(result[0])
                        close_price = float(result[1]) if result[1] else stock["close"]
                        avg_volume = float(result[2])
                        trading_days = int(result[3]) if result[3] else 0

                        # Only consider if we have at least 10 trading days of history
                        if trading_days >= 10 and avg_volume > 0:
                            volume_ratio = round(today_volume / avg_volume, 2)

                            results.append(
                                {
                                    "symbol": symbol,
                                    "close": close_price,
                                    "volume": int(today_volume),
                                    "avg_volume": int(avg_volume),
                                    "volume_ratio": volume_ratio,
                                    "trading_days": trading_days,
                                    "sector": get_sector(symbol),
                                    "change_pct": stock.get("change_pct", 0),
                                }
                            )
                            processed += 1

                except Exception as e:
                    errors += 1
                    if errors <= 3:
                        print(f"[DEBUG] Volume breakout skip {symbol}: {str(e)[:50]}")

        print(f"[INFO] Volume breakouts: processed {processed} stocks, {errors} errors")

        # Convert to tuple for caching
        return tuple(
            (
                r["symbol"],
                r["close"],
                r["volume"],
                r["avg_volume"],
                r["volume_ratio"],
                r["trading_days"],
                r["sector"],
                r["change_pct"],
            )
            for r in results
        )

    except Exception as e:
        print(f"[ERROR] _get_volume_breakouts_cached(): {e}")
        import traceback

        traceback.print_exc()
        return tuple()


def get_volume_breakouts(selected_date: str, multiplier: float = 2.0):
    """
    Get stocks with unusual volume (volume breakouts).

    Args:
        selected_date: Date to check
        multiplier: Minimum volume ratio (today_vol / avg_20d_vol)

    Returns:
        List of stocks where today's volume >= multiplier * 20-day average
    """
    try:
        cached_data = _get_volume_breakouts_cached(selected_date)

        if not cached_data:
            print(f"[WARN] No volume breakout data for {selected_date}")
            return []

        # Filter by multiplier and sort by volume_ratio descending
        result = []
        for row in cached_data:
            volume_ratio = row[4]
            if volume_ratio >= multiplier:
                result.append(
                    {
                        "symbol": row[0],
                        "close": row[1],
                        "volume": row[2],
                        "avg_volume": row[3],
                        "volume_ratio": volume_ratio,
                        "trading_days": row[5],
                        "sector": row[6],
                        "change_pct": row[7],
                    }
                )

        # Sort by volume ratio (highest first)
        result.sort(key=lambda x: x["volume_ratio"], reverse=True)

        print(f"[INFO] Found {len(result)} stocks with volume >= {multiplier}x average")
        return result

    except Exception as e:
        print(f"[ERROR] get_volume_breakouts(): {e}")
        return []


# =============================================================
# 8️⃣ ENHANCED HEATMAP DATA WITH REAL HIGH/LOW/VOLUME
#    Fetches actual intraday data from cash database
# =============================================================


def get_enhanced_heatmap_data(selected_date: str):
    """
    Get heatmap data with REAL high, low, and volume from cash database.
    This function enriches the basic heatmap data with actual market data.
    """
    try:
        # Get basic heatmap data first
        basic_data = get_heatmap_data(selected_date)
        if not basic_data:
            return []

        print(f"[INFO] Enhancing heatmap data with real high/low/volume for {len(basic_data)} stocks...")

        enhanced_data = []
        enriched_count = 0

        with engine_cash.connect() as conn:
            for stock in basic_data:
                symbol = stock["symbol"]
                table_name = f"TBL_{symbol}"

                try:
                    # Query real high, low, volume from cash database
                    query = text(
                        f"""
                        SELECT
                            CAST("HghPric" AS NUMERIC) as high,
                            CAST("LwPric" AS NUMERIC) as low,
                            CAST("TtlTradgVol" AS NUMERIC) as volume,
                            CAST("ClsPric" AS NUMERIC) as close
                        FROM public."{table_name}"
                        WHERE CAST("BizDt" AS DATE) = CAST(:selected_date AS DATE)
                        LIMIT 1
                    """
                    )

                    result = conn.execute(query, {"selected_date": selected_date}).fetchone()

                    if result and result[0] and result[1]:
                        # Use real data from cash database
                        stock_copy = stock.copy()
                        stock_copy["high"] = float(result[0])
                        stock_copy["low"] = float(result[1])
                        stock_copy["volume"] = int(float(result[2])) if result[2] else stock["volume"]
                        # Optionally update close if available
                        if result[3]:
                            stock_copy["close"] = float(result[3])
                        enhanced_data.append(stock_copy)
                        enriched_count += 1
                    else:
                        # Keep original data if cash data not available
                        enhanced_data.append(stock)

                except Exception:
                    # Keep original data on error
                    enhanced_data.append(stock)

        print(f"[INFO] Enhanced {enriched_count}/{len(basic_data)} stocks with real high/low/volume")
        return enhanced_data

    except Exception as e:
        print(f"[ERROR] get_enhanced_heatmap_data(): {e}")
        # Fallback to basic data
        return get_heatmap_data(selected_date)


# =============================================================
# 9️⃣ NIFTY PE RATIO - REAL CALCULATION
#    Aggregates Market Cap and Net Profit of Nifty 50 stocks
# =============================================================


@lru_cache(maxsize=1)
def _get_nifty_pe_cached(date_key):
    """
    Calculate Nifty 50 PE Ratio.
    Cached for performance (date_key ensures daily refresh).
    """
    try:
        from ..models.index_model import fetch_index_constituents
        from ..services.fundamental_service import fundamental_service

        # Get Nifty 50 constituents
        tickers = fetch_index_constituents("nifty50")
        if not tickers:
            return None

        total_market_cap = 0
        total_earnings = 0
        count = 0

        for ticker in tickers:
            try:
                stats = fundamental_service.get_stock_fundamentals(ticker)
                mc = stats.get("market_cap", 0)
                np = stats.get("net_profit", 0)

                if mc > 0 and np > 0:
                    total_market_cap += mc
                    total_earnings += np
                    count += 1
            except Exception:
                continue

        if total_earnings > 0:
            pe = total_market_cap / total_earnings
        else:
            return None

        # Determine Valuation Status
        if pe < 20:
            status = "Undervalued"
            color = "green"
        elif 20 <= pe <= 25:
            status = "Fairly Valued"
            color = "yellow"
        else:
            status = "Overvalued"
            color = "red"

        print(f"[INFO] Calculated Nifty PE: {pe:.2f} (based on {count} stocks)")

        return {
            "pe": round(pe, 2),
            "status": status,
            "color": color,
            "min": 15,
            "max": 30
        }

    except Exception as e:
        print(f"[ERROR] _get_nifty_pe_cached(): {e}")
        return None


def get_nifty_pe():
    """Get Nifty 50 PE ratio (EOD only - No live adjustments)."""
    try:
        # Get EOD PE (Calculated from Fundamentals)
        # Use simple caching (daily refresh)
        date_key = datetime.now().strftime("%Y-%m-%d")
        pe_data = _get_nifty_pe_cached(date_key)

        if not pe_data:
            return None

        # Return EOD data without any live adjustments
        print(f"[INFO] Nifty PE (EOD): {pe_data['pe']}")
        return pe_data

    except Exception as e:
        print(f"[ERROR] get_nifty_pe: {e}")
        return None
