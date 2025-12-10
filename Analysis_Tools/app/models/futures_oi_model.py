"""
FUTURES OI MODEL
================
Database queries for Futures OI cache data
"""

import pandas as pd
from sqlalchemy import text

from .db_config import engine


def get_futures_oi_available_dates():
    """Get all available dates from futures_oi_cache"""
    try:
        q = text(
            """
            SELECT DISTINCT cache_date
            FROM public.futures_oi_cache
            ORDER BY cache_date ASC
        """
        )
        df = pd.read_sql(q, engine)
        return [str(d) for d in df["cache_date"]]
    except Exception as e:
        print(f"Error getting dates: {e}")
        return []


def get_futures_oi_data(date):
    """
    Get all futures OI data for a specific date
    Returns dict with CME, NME, FME arrays
    Columns: ticker, underlying_price, expiry_date, expiry_price, expiry_oi, oi_percentile (20P), price_percentile
    """
    try:
        q = text(
            """
            SELECT
                ticker,
                underlying_price,
                expiry_type,
                expiry_date,
                expiry_price,
                expiry_oi,
                expiry_oi_change,
                oi_percentile,
                price_percentile
            FROM public.futures_oi_cache
            WHERE cache_date = :date
            ORDER BY expiry_type, expiry_oi DESC
        """
        )
        df = pd.read_sql(q, engine, params={"date": date})

        if df.empty:
            return None

        result = {"CME": [], "NME": [], "FME": []}

        for _, row in df.iterrows():
            item = {
                "ticker": row["ticker"],
                "underlying_price": float(row["underlying_price"]) if pd.notnull(row["underlying_price"]) else 0,
                "expiry_date": str(row["expiry_date"]) if pd.notnull(row["expiry_date"]) else "",
                "expiry_price": float(row["expiry_price"]) if pd.notnull(row["expiry_price"]) else 0,
                "expiry_oi": float(row["expiry_oi"]) if pd.notnull(row["expiry_oi"]) else 0,
                "expiry_oi_change": float(row["expiry_oi_change"]) if pd.notnull(row["expiry_oi_change"]) else 0,
                "oi_percentile": float(row["oi_percentile"]) if pd.notnull(row["oi_percentile"]) else 0,
                "price_percentile": float(row["price_percentile"]) if pd.notnull(row["price_percentile"]) else 0,
            }

            expiry_type = row["expiry_type"]
            if expiry_type in result:
                result[expiry_type].append(item)

        return result

    except Exception as e:
        print(f"Error getting futures OI data: {e}")
        return None


def get_futures_oi_top_by_oi(date, expiry_type="CME", limit=20, ascending=False):
    """Get top stocks by OI for a specific expiry type"""
    try:
        order = "ASC" if ascending else "DESC"
        q = text(
            f"""
            SELECT
                ticker,
                underlying_price,
                expiry_date,
                expiry_price,
                expiry_oi,
                expiry_oi_change,
                oi_percentile,
                price_percentile
            FROM public.futures_oi_cache
            WHERE cache_date = :date AND expiry_type = :expiry_type
            ORDER BY expiry_oi {order}
            LIMIT :limit
        """
        )
        df = pd.read_sql(q, engine, params={"date": date, "expiry_type": expiry_type, "limit": limit})

        return df.to_dict("records")

    except Exception as e:
        print(f"Error getting top OI: {e}")
        return []
