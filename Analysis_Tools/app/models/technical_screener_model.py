"""
TECHNICAL SCREENER MODEL
========================
Database queries for Technical Screener cache data
"""

import pandas as pd
from sqlalchemy import text

from .db_config import engine


def get_technical_available_dates():
    """Get all available dates from technical_screener_cache"""
    try:
        q = text(
            """
            SELECT DISTINCT cache_date
            FROM public.technical_screener_cache
            ORDER BY cache_date ASC
        """
        )
        df = pd.read_sql(q, engine)
        return [str(d) for d in df["cache_date"]]
    except Exception as e:
        print(f"Error getting dates: {e}")
        return []


def get_all_technical_data(date):
    """Get all technical data for a specific date"""
    try:
        q = text(
            """
            SELECT * FROM public.technical_screener_cache
            WHERE cache_date = :date
            ORDER BY ticker
        """
        )
        df = pd.read_sql(q, engine, params={"date": date})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting technical data: {e}")
        return []


def get_rsi_overbought(date, limit=20):
    """Get stocks with RSI > 80 (overbought)"""
    try:
        q = text(
            """
            SELECT ticker, underlying_price, rsi_14, adx_14
            FROM public.technical_screener_cache
            WHERE cache_date = :date AND rsi_above_80 = TRUE
            ORDER BY rsi_14 DESC
            LIMIT :limit
        """
        )
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting RSI overbought: {e}")
        return []


def get_rsi_oversold(date, limit=20):
    """Get stocks with RSI < 20 (oversold)"""
    try:
        q = text(
            """
            SELECT ticker, underlying_price, rsi_14, adx_14
            FROM public.technical_screener_cache
            WHERE cache_date = :date AND rsi_below_20 = TRUE
            ORDER BY rsi_14 ASC
            LIMIT :limit
        """
        )
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting RSI oversold: {e}")
        return []


def get_macd_bullish_crossover(date, limit=20):
    """Get stocks with MACD bullish crossover"""
    try:
        q = text(
            """
            SELECT ticker, underlying_price, macd, macd_signal, rsi_14
            FROM public.technical_screener_cache
            WHERE cache_date = :date AND macd_pos_cross = TRUE
            ORDER BY macd DESC
            LIMIT :limit
        """
        )
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting MACD bullish: {e}")
        return []


def get_macd_bearish_crossover(date, limit=20):
    """Get stocks with MACD bearish crossover"""
    try:
        q = text(
            """
            SELECT ticker, underlying_price, macd, macd_signal, rsi_14
            FROM public.technical_screener_cache
            WHERE cache_date = :date AND macd_neg_cross = TRUE
            ORDER BY macd ASC
            LIMIT :limit
        """
        )
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting MACD bearish: {e}")
        return []


def get_above_both_sma(date, limit=20):
    """Get stocks above both 50 and 200 SMA"""
    try:
        q = text(
            """
            SELECT ticker, underlying_price, sma_50, sma_200,
                   dist_from_50sma_pct, dist_from_200sma_pct, rsi_14
            FROM public.technical_screener_cache
            WHERE cache_date = :date AND above_50_sma = TRUE AND above_200_sma = TRUE
            ORDER BY dist_from_200sma_pct DESC
            LIMIT :limit
        """
        )
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting above SMA: {e}")
        return []


def get_below_both_sma(date, limit=20):
    """Get stocks below both 50 and 200 SMA"""
    try:
        q = text(
            """
            SELECT ticker, underlying_price, sma_50, sma_200,
                   dist_from_50sma_pct, dist_from_200sma_pct, rsi_14
            FROM public.technical_screener_cache
            WHERE cache_date = :date AND below_50_sma = TRUE AND below_200_sma = TRUE
            ORDER BY dist_from_200sma_pct ASC
            LIMIT :limit
        """
        )
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting below SMA: {e}")
        return []


def get_strong_trending_stocks(date, limit=20):
    """Get stocks with ADX > 25 (strong trend)"""
    try:
        q = text(
            """
            SELECT ticker, underlying_price, adx_14, rsi_14, macd
            FROM public.technical_screener_cache
            WHERE cache_date = :date AND strong_trend = TRUE
            ORDER BY adx_14 DESC
            LIMIT :limit
        """
        )
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting strong trending: {e}")
        return []


def get_heatmap_data(date):
    """
    Get data for all 4 heatmap visualizations (RSI, MACD, SMA, ADX)
    Returns all stocks with categories for each indicator
    """
    try:
        q = text(
            """
            SELECT
                ticker,
                underlying_price,
                rsi_14,
                adx_14,
                macd,
                macd_signal,
                macd_pos_cross,
                macd_neg_cross,
                above_50_sma,
                above_200_sma,
                below_50_sma,
                below_200_sma,
                dist_from_200sma_pct,
                strong_trend,
                -- RSI Category
                CASE
                    WHEN rsi_above_80 THEN 'overbought'
                    WHEN rsi_60_80 THEN 'bullish'
                    WHEN rsi_40_60 THEN 'neutral'
                    WHEN rsi_20_40 THEN 'bearish'
                    WHEN rsi_below_20 THEN 'oversold'
                    ELSE 'neutral'
                END as rsi_category,
                -- MACD Category
                CASE
                    WHEN macd_pos_cross THEN 'bullish-cross'
                    WHEN macd_neg_cross THEN 'bearish-cross'
                    WHEN macd > macd_signal THEN 'bullish'
                    WHEN macd < macd_signal THEN 'bearish'
                    ELSE 'neutral'
                END as macd_category,
                -- SMA Category
                CASE
                    WHEN above_50_sma AND above_200_sma THEN 'strong-bullish'
                    WHEN above_200_sma THEN 'bullish'
                    WHEN below_50_sma AND below_200_sma THEN 'strong-bearish'
                    WHEN below_200_sma THEN 'bearish'
                    ELSE 'neutral'
                END as sma_category,
                -- ADX Category
                CASE
                    WHEN adx_14 > 40 THEN 'very-strong'
                    WHEN adx_14 > 25 THEN 'strong'
                    ELSE 'weak'
                END as adx_category
            FROM public.technical_screener_cache
            WHERE cache_date = :date
            ORDER BY ticker
        """
        )
        df = pd.read_sql(q, engine, params={"date": date})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting heatmap data: {e}")
        return []


def get_golden_crossover_stocks(date, limit=50):
    """
    Get stocks with Golden Crossover (50-day SMA crosses above 200-day SMA)
    Returns stocks where 50 SMA > 200 SMA (bullish signal)
    """
    try:
        q = text(
            """
            SELECT
                ticker,
                underlying_price,
                sma_50,
                sma_200,
                rsi_14,
                adx_14,
                dist_from_50sma_pct,
                dist_from_200sma_pct,
                ROUND(((sma_50 - sma_200) / sma_200 * 100)::numeric, 2) as sma_diff_pct
            FROM public.technical_screener_cache
            WHERE cache_date = :date
                AND above_50_sma = TRUE
                AND above_200_sma = TRUE
                AND sma_50 > sma_200
            ORDER BY sma_diff_pct DESC
            LIMIT :limit
        """
        )
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting golden crossover: {e}")
        return []


def get_death_crossover_stocks(date, limit=50):
    """
    Get stocks with Death Crossover (50-day SMA crosses below 200-day SMA)
    Returns stocks where 50 SMA < 200 SMA (bearish signal)
    """
    try:
        q = text(
            """
            SELECT
                ticker,
                underlying_price,
                sma_50,
                sma_200,
                rsi_14,
                adx_14,
                dist_from_50sma_pct,
                dist_from_200sma_pct,
                ROUND(((sma_50 - sma_200) / sma_200 * 100)::numeric, 2) as sma_diff_pct
            FROM public.technical_screener_cache
            WHERE cache_date = :date
                AND below_50_sma = TRUE
                AND below_200_sma = TRUE
                AND sma_50 < sma_200
            ORDER BY sma_diff_pct ASC
            LIMIT :limit
        """
        )
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting death crossover: {e}")
        return []
