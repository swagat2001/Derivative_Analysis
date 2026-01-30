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
#  Uses BhavCopy_Database (options_dashboard_cache) as primary source
#  Does NOT filter by stock list Excel - shows ALL available F&O stocks
# =============================================================

import json
import os
from datetime import datetime, timedelta
from functools import lru_cache

import pandas as pd
from sqlalchemy import text

from .db_config import engine, engine_cash

# =============================================================
# SECTOR MAPPING FOR STOCKS
# =============================================================

SECTOR_MASTER_PATHS = [
    "C:/NSE_EOD_CASH_WITH_INDICATORS/nse_sector_master.csv",
    "C:/Users/Admin/Desktop/Derivative_Analysis/SMA/nse_sector_master.csv",
    os.path.join(os.path.dirname(__file__), "nse_sector_master.csv"),
]

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
                        _sector_cache[symbol] = classify_industry(str(sector))

                print(f"[INFO] Loaded sector data for {len(_sector_cache)} stocks from {path}")
                _sector_cache_loaded = True
                return
            except Exception as e:
                print(f"[WARNING] Could not load sector master from {path}: {e}")

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
# 1️⃣ AVAILABLE DATES FOR INSIGHTS
# =============================================================


@lru_cache(maxsize=1)
def _get_insights_dates_cached():
    """Get available dates from options_dashboard_cache."""
    try:
        query = text(
            """
            SELECT DISTINCT biz_date::date::text AS date
            FROM options_dashboard_cache
            ORDER BY date DESC
            LIMIT 100;
        """
        )
        df = pd.read_sql(query, con=engine)
        return tuple(df["date"].tolist())
    except Exception as e:
        print(f"[ERROR] _get_insights_dates_cached(): {e}")
        return tuple()


def get_insights_dates():
    """Get available dates for insights dashboard."""
    return list(_get_insights_dates_cached())


def clear_insights_cache():
    """Clear all insights caches."""
    _get_insights_dates_cached.cache_clear()
    _get_heatmap_data_cached.cache_clear()
    _get_fii_dii_data_cached.cache_clear()
    _get_delivery_data_cached.cache_clear()
    _get_market_stats_cached.cache_clear()
    print("[INFO] Insights cache cleared")


# =============================================================
# 2️⃣ HEATMAP DATA - From options_dashboard_cache
#    NO FILTERING BY EXCEL - Shows ALL F&O stocks
# =============================================================


@lru_cache(maxsize=32)
def _get_heatmap_data_cached(selected_date: str):
    """
    Get heatmap data from options_dashboard_cache table.
    Returns ALL F&O stocks without filtering by Excel file.
    """
    try:
        # Query options_dashboard_cache for TOTAL view
        query = text(
            """
            SELECT data_json
            FROM options_dashboard_cache
            WHERE biz_date = :biz_date
              AND moneyness_type = 'TOTAL'
            LIMIT 1;
        """
        )
        df = pd.read_sql(query, con=engine, params={"biz_date": selected_date})

        if df.empty:
            print(f"[INFO] No heatmap data found for {selected_date}")
            return tuple()

        # Parse JSON
        raw_json = df.iloc[0]["data_json"]
        parsed = json.loads(raw_json)

        if isinstance(parsed, dict):
            parsed = parsed.get("data", [])
        elif not isinstance(parsed, list):
            return tuple()

        print(f"[INFO] Found {len(parsed)} stocks in options_dashboard_cache for {selected_date}")

        # Get previous day's data for change calculation
        prev_date_query = text(
            """
            SELECT biz_date::date::text AS date
            FROM options_dashboard_cache
            WHERE biz_date < :biz_date
              AND moneyness_type = 'TOTAL'
            ORDER BY biz_date DESC
            LIMIT 1;
        """
        )
        prev_df = pd.read_sql(prev_date_query, con=engine, params={"biz_date": selected_date})

        prev_prices = {}
        if not prev_df.empty:
            prev_date = prev_df.iloc[0]["date"]
            prev_query = text(
                """
                SELECT data_json
                FROM options_dashboard_cache
                WHERE biz_date = :biz_date
                  AND moneyness_type = 'TOTAL'
                LIMIT 1;
            """
            )
            prev_data_df = pd.read_sql(prev_query, con=engine, params={"biz_date": prev_date})

            if not prev_data_df.empty:
                prev_json = json.loads(prev_data_df.iloc[0]["data_json"])
                if isinstance(prev_json, dict):
                    prev_json = prev_json.get("data", [])

                for item in prev_json:
                    if "stock" in item and "closing_price" in item:
                        try:
                            prev_prices[item["stock"].upper()] = (
                                float(item["closing_price"]) if item["closing_price"] else 0
                            )
                        except:
                            pass

        # Build result - NO FILTERING
        result = []
        for item in parsed:
            if "stock" not in item:
                continue

            symbol = str(item.get("stock", "")).upper()

            try:
                close = float(item.get("closing_price", 0)) if item.get("closing_price") else 0
            except:
                close = 0

            if close <= 0:
                continue

            # Calculate change from previous day
            prev_close = prev_prices.get(symbol, close)
            if prev_close > 0:
                change_pct = round(((close - prev_close) / prev_close) * 100, 2)
            else:
                change_pct = 0.0

            # Get turnover from call + put totals
            try:
                call_tradval = float(item.get("call_total_tradval", 0)) if item.get("call_total_tradval") else 0
                put_tradval = float(item.get("put_total_tradval", 0)) if item.get("put_total_tradval") else 0
            except:
                call_tradval = 0
                put_tradval = 0

            turnover = call_tradval + put_tradval

            # Volume approximation (tradval / price)
            volume = int(turnover / close) if close > 0 else 0

            result.append(
                (
                    symbol,
                    close,
                    change_pct,
                    volume,
                    turnover,
                    get_sector(symbol),
                    close * 1.02,  # Approx high
                    close * 0.98,  # Approx low
                )
            )

        print(f"[INFO] Returning {len(result)} stocks for heatmap")
        return tuple(result)

    except Exception as e:
        print(f"[ERROR] _get_heatmap_data_cached(): {e}")
        import traceback

        traceback.print_exc()
        return tuple()


def get_heatmap_data(selected_date: str, comparison_date: str = None):
    """Get heatmap data for visualization."""
    cached_data = _get_heatmap_data_cached(selected_date)

    if not cached_data:
        return []

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


def get_sector_performance(selected_date: str, comparison_date: str = None):
    """Get sector-wise performance aggregated from stock data."""
    heatmap_data = get_heatmap_data(selected_date, comparison_date)

    if not heatmap_data:
        return []

    sector_data = {}
    for stock in heatmap_data:
        sector = stock["sector"]
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
                "stocks": data["stocks"][:5],
            }
        )

    result.sort(key=lambda x: x["avg_change"], reverse=True)
    return result


# =============================================================
# 3️⃣ FII/DII DATA
# =============================================================


@lru_cache(maxsize=64)
def _get_fii_dii_data_cached(start_date: str, end_date: str):
    """Get FII/DII activity data from Nifty options."""
    try:
        query = text(
            """
            SELECT
                "BizDt"::date::text as date,
                SUM(CASE WHEN "OptnTp" = 'CE' THEN CAST("ChngInOpnIntrst" AS BIGINT) ELSE 0 END) as ce_oi_change,
                SUM(CASE WHEN "OptnTp" = 'PE' THEN CAST("ChngInOpnIntrst" AS BIGINT) ELSE 0 END) as pe_oi_change,
                SUM(CAST("TtlTradgVol" AS BIGINT)) as total_volume
            FROM public."TBL_NIFTY_DERIVED"
            WHERE "BizDt" BETWEEN :start_date AND :end_date
            AND "OptnTp" IN ('CE', 'PE')
            GROUP BY "BizDt"
            ORDER BY "BizDt" DESC
        """
        )

        df = pd.read_sql(query, con=engine, params={"start_date": start_date, "end_date": end_date})

        if df.empty:
            return tuple()

        result = []
        for _, row in df.iterrows():
            ce_change = int(row["ce_oi_change"]) if pd.notna(row["ce_oi_change"]) else 0
            pe_change = int(row["pe_oi_change"]) if pd.notna(row["pe_oi_change"]) else 0
            net_index = pe_change - ce_change

            result.append(
                (
                    row["date"],
                    ce_change,
                    pe_change,
                    net_index,
                    int(row["total_volume"]) if pd.notna(row["total_volume"]) else 0,
                )
            )

        return tuple(result)

    except Exception as e:
        print(f"[ERROR] _get_fii_dii_data_cached(): {e}")
        return tuple()


def get_fii_dii_data(start_date: str, end_date: str):
    """Get FII/DII activity data for the given date range."""
    cached_data = _get_fii_dii_data_cached(start_date, end_date)

    if not cached_data:
        return []

    result = []
    for row in cached_data:
        # Ensure all values are proper integers/numbers
        ce_oi_change = int(row[1]) if row[1] is not None else 0
        pe_oi_change = int(row[2]) if row[2] is not None else 0
        net_index_oi = int(row[3]) if row[3] is not None else 0
        total_volume = int(row[4]) if row[4] is not None else 0

        result.append(
            {
                "date": row[0],
                "ce_oi_change": ce_oi_change,
                "pe_oi_change": pe_oi_change,
                "net_index_oi": net_index_oi,
                "total_volume": total_volume,
                "sentiment": "Bullish" if net_index_oi > 0 else "Bearish" if net_index_oi < 0 else "Neutral",
            }
        )

    return result


# =============================================================
# 4️⃣ DELIVERY DATA - From Cash Database
# =============================================================


@lru_cache(maxsize=32)
def _get_delivery_data_cached(selected_date: str):
    """Get delivery percentage data for stocks."""
    try:
        print(f"[DEBUG] Getting delivery data for date: {selected_date}")

        # Get all per-symbol tables from cash database
        query = text(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name LIKE 'TBL_%'
            AND table_name != 'TBL_NIFTY_DERIVED'
            ORDER BY table_name;
        """
        )
        with engine_cash.connect() as conn:
            result = conn.execute(query)
            tables = [row[0] for row in result]

        print(f"[DEBUG] Found {len(tables)} tables in cash database")

        if not tables:
            print("[WARN] No tables found in cash database")
            return tuple()

        # Debug: Check what dates exist in a sample table
        if tables:
            sample_table = tables[0]
            try:
                date_check_query = (
                    f'SELECT DISTINCT "BizDt"::date as date FROM public."{sample_table}" ORDER BY date DESC LIMIT 5'
                )
                date_df = pd.read_sql(date_check_query, con=engine_cash)
                if not date_df.empty:
                    available_dates = date_df["date"].tolist()
                    print(f"[DEBUG] Available dates in {sample_table}: {available_dates}")
                    print(f"[DEBUG] Looking for date: {selected_date}")
                else:
                    print(f"[DEBUG] No dates found in {sample_table}")
            except Exception as e:
                print(f"[DEBUG] Could not check dates: {str(e)[:100]}")

        # Query each table INDIVIDUALLY to avoid UNION errors
        all_data = []
        successful_queries = 0
        failed_queries = 0

        for table in tables:
            symbol_name = table.replace("TBL_", "")

            # Skip empty symbol names
            if not symbol_name:
                continue

            # Build query - embed date directly since parameterization is causing issues
            query_str = (
                "SELECT '" + symbol_name + "' as symbol, "
                'CAST("ClsPric" AS NUMERIC) as close, '
                'CAST("TtlTradgVol" AS NUMERIC) as volume, '
                'CAST(COALESCE("DlvryQty", 0) AS NUMERIC) as delivery_qty, '
                'CASE WHEN CAST("TtlTradgVol" AS NUMERIC) > 0 '
                'THEN ROUND((CAST(COALESCE("DlvryQty", 0) AS NUMERIC) / CAST("TtlTradgVol" AS NUMERIC)) * 100, 2) '
                "ELSE 0 END as delivery_pct "
                'FROM public."' + table + '" '
                'WHERE CAST("BizDt" AS DATE) = CAST(\'' + selected_date + "' AS DATE) "
                'AND "TtlTradgVol" IS NOT NULL '
                'AND CAST("TtlTradgVol" AS NUMERIC) > 0 '
                'ORDER BY "BizDt" DESC '
                "LIMIT 1"
            )

            # Debug: Print first query to see the actual SQL
            if successful_queries == 0 and failed_queries == 0:
                print(f"[DEBUG] Example query for {table}:")
                print(f"[DEBUG] {query_str[:200]}...")

            try:
                df_row = pd.read_sql(query_str, con=engine_cash)

                if not df_row.empty:
                    all_data.append(df_row)
                    successful_queries += 1
            except Exception as e:
                failed_queries += 1
                # Only log first 5 errors to avoid console spam
                if failed_queries <= 5:
                    error_str = str(e)[:100]
                    print(f"[DEBUG] Skip {table}: {error_str}")

        print(f"[DEBUG] Successfully queried {successful_queries} tables, {failed_queries} failed")

        if not all_data:
            print(f"[WARN] No delivery data found for {selected_date}")
            return tuple()

        df = pd.concat(all_data, ignore_index=True)

        if df.empty:
            print(f"[WARN] Combined dataframe is empty for {selected_date}")
            return tuple()

        print(f"[DEBUG] Found {len(df)} stocks with delivery data")
        df = df.sort_values("delivery_pct", ascending=False)

        result = []
        for _, row in df.iterrows():
            result.append(
                (
                    str(row["symbol"]),
                    float(row["close"]) if pd.notna(row["close"]) else 0,
                    int(float(row["volume"])) if pd.notna(row["volume"]) else 0,
                    int(float(row["delivery_qty"])) if pd.notna(row["delivery_qty"]) else 0,
                    float(row["delivery_pct"]) if pd.notna(row["delivery_pct"]) else 0,
                )
            )

        print(f"[INFO] Returning {len(result)} stocks with delivery data")
        return tuple(result)

    except Exception as e:
        print(f"[ERROR] _get_delivery_data_cached(): {e}")
        import traceback

        traceback.print_exc()
        return tuple()


def get_delivery_data(selected_date: str, min_delivery_pct: float = 0):
    """Get stocks with high delivery percentage."""
    cached_data = _get_delivery_data_cached(selected_date)

    if not cached_data:
        print(f"[WARN] No cached delivery data for {selected_date}, returning empty list")
        return []

    result = []
    for row in cached_data:
        if row[4] >= min_delivery_pct:
            result.append(
                {
                    "symbol": row[0],
                    "close": row[1],
                    "volume": row[2],
                    "delivery_qty": row[3],
                    "delivery_pct": row[4],
                    "sector": get_sector(row[0]),
                }
            )

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
# 7️⃣ VOLUME BREAKOUTS
# =============================================================


def get_volume_breakouts(selected_date: str, multiplier: float = 2.0):
    """Get stocks with unusual volume."""
    try:
        heatmap_data = get_heatmap_data(selected_date)

        if not heatmap_data:
            return []

        # Sort by turnover descending to find high volume stocks
        sorted_data = sorted(heatmap_data, key=lambda x: x["turnover"], reverse=True)

        # Return top 30 by turnover as volume breakouts
        result = []
        for stock in sorted_data[:30]:
            result.append(
                {
                    "symbol": stock["symbol"],
                    "close": stock["close"],
                    "volume": stock["volume"],
                    "avg_volume": int(stock["volume"] / 2),  # Approximation
                    "volume_ratio": 2.0,  # Placeholder
                    "sector": stock["sector"],
                }
            )

        return result

    except Exception as e:
        print(f"[ERROR] get_volume_breakouts(): {e}")
        return []
