"""
TECHNICAL SCREENER MODEL
========================
Database queries for Technical Screener cache data
"""

import pandas as pd
from sqlalchemy import text

from .db_config import engine_cash as engine


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
            ORDER BY sma_diff_pct ASC
            LIMIT :limit
        """
        )
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting golden crossover: {e}")
        return []


# ====== NEW SCREENER QUERY FUNCTIONS ======

def get_rsi_overbought_stocks(date, limit=100):
    """Get stocks with RSI > 75 (ScanX threshold for overbought)"""
    try:
        q = text("""
            SELECT ticker, underlying_price, rsi_14, adx_14, macd,
                   bb_upper, bb_lower, sma_50, sma_200
            FROM public.technical_screener_cache
            WHERE cache_date = :date AND rsi_14 > 75
            ORDER BY rsi_14 DESC
            LIMIT :limit
        """)
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting RSI overbought stocks: {e}")
        return []


def get_rsi_oversold_stocks(date, limit=100):
    """Get stocks with RSI < 25 (ScanX threshold for oversold)"""
    try:
        q = text("""
            SELECT ticker, underlying_price, rsi_14, adx_14, macd,
                   bb_upper, bb_lower, sma_50, sma_200
            FROM public.technical_screener_cache
            WHERE cache_date = :date AND rsi_14 < 25
            ORDER BY rsi_14 ASC
            LIMIT :limit
        """)
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting RSI oversold stocks: {e}")
        return []


def get_r1_breakout_stocks(date, limit=100):
    """Get stocks breaking above R1 resistance"""
    try:
        q = text("""
            SELECT ticker, underlying_price, r1, rsi_14, adx_14,
                   ROUND(((underlying_price - r1) / r1 * 100)::numeric, 2) as breakout_pct
            FROM public.technical_screener_cache
            WHERE cache_date = :date AND r1_breakout = TRUE
            ORDER BY breakout_pct DESC
            LIMIT :limit
        """)
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting R1 breakouts: {e}")
        return []


def get_r2_breakout_stocks(date, limit=100):
    """Get stocks breaking above R2 resistance"""
    try:
        q = text("""
            SELECT ticker, underlying_price, r2, rsi_14, adx_14,
                   ROUND(((underlying_price - r2) / r2 * 100)::numeric, 2) as breakout_pct
            FROM public.technical_screener_cache
            WHERE cache_date = :date AND r2_breakout = TRUE
            ORDER BY breakout_pct DESC
            LIMIT :limit
        """)
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting R2 breakouts: {e}")
        return []


def get_r3_breakout_stocks(date, limit=100):
    """Get stocks breaking above R3 resistance"""
    try:
        q = text("""
            SELECT ticker, underlying_price, r3, rsi_14, adx_14,
                   ROUND(((underlying_price - r3) / r3 * 100)::numeric, 2) as breakout_pct
            FROM public.technical_screener_cache
            WHERE cache_date = :date AND r3_breakout = TRUE
            ORDER BY breakout_pct DESC
            LIMIT :limit
        """)
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting R3 breakouts: {e}")
        return []


def get_s1_breakout_stocks(date, limit=100):
    """Get stocks breaking below S1 support"""
    try:
        q = text("""
            SELECT ticker, underlying_price, s1, rsi_14, adx_14,
                   ROUND(((s1 - underlying_price) / s1 * 100)::numeric, 2) as breakdown_pct
            FROM public.technical_screener_cache
            WHERE cache_date = :date AND s1_breakout = TRUE
            ORDER BY breakdown_pct DESC
            LIMIT :limit
        """)
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting S1 breakouts: {e}")
        return []


def get_s2_breakout_stocks(date, limit=100):
    """Get stocks breaking below S2 support"""
    try:
        q = text("""
            SELECT ticker, underlying_price, s2, rsi_14, adx_14,
                   ROUND(((s2 - underlying_price) / s2 * 100)::numeric, 2) as breakdown_pct
            FROM public.technical_screener_cache
            WHERE cache_date = :date AND s2_breakout = TRUE
            ORDER BY breakdown_pct DESC
            LIMIT :limit
        """)
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting S2 breakouts: {e}")
        return []


def get_s3_breakout_stocks(date, limit=100):
    """Get stocks breaking below S3 support"""
    try:
        q = text("""
            SELECT ticker, underlying_price, s3, rsi_14, adx_14,
                   ROUND(((s3 - underlying_price) / s3 * 100)::numeric, 2) as breakdown_pct
            FROM public.technical_screener_cache
            WHERE cache_date = :date AND s3_breakout = TRUE
            ORDER BY breakdown_pct DESC
            LIMIT :limit
        """)
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting S3 breakouts: {e}")
        return []


def get_momentum_stocks(date, limit=100):
    """Get stocks with high momentum score"""
    try:
        q = text("""
            SELECT ticker, underlying_price, momentum_score, rsi_14, adx_14,
                   sma_50, sma_200
            FROM public.technical_screener_cache
            WHERE cache_date = :date AND is_high_momentum = TRUE
            ORDER BY momentum_score DESC
            LIMIT :limit
        """)
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting momentum stocks: {e}")
        return []


def get_squeezing_range_stocks(date, limit=100):
    """Get stocks with Bollinger Band squeeze (tightening bands)"""
    try:
        q = text("""
            SELECT ticker, underlying_price, bb_width, bb_upper, bb_lower,
                   bb_middle, rsi_14, adx_14
            FROM public.technical_screener_cache
            WHERE cache_date = :date AND bb_squeeze = TRUE
            ORDER BY bb_width ASC
            LIMIT :limit
        """)
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting squeezing range stocks: {e}")
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
            ORDER BY sma_diff_pct DESC
            LIMIT :limit
        """
        )
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting death crossover: {e}")
        return []




# ====== PRICE & VOLUME SCREENER QUERIES ======

def get_week1_high_breakout_stocks(date, limit=100):
    """Stocks breaking above 1-week high"""
    try:
        q = text("""
            SELECT ticker, underlying_price, week1_high, rsi_14, adx_14, volume,
                ROUND(((underlying_price - week1_high) / week1_high * 100)::numeric, 2) as breakout_pct
            FROM public.technical_screener_cache
            WHERE cache_date = :date AND is_week1_high_breakout = TRUE
            ORDER BY breakout_pct DESC
            LIMIT :limit
        """)
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting week1 high breakout: {e}")
        return []

def get_week1_low_breakout_stocks(date, limit=100):
    """Stocks breaking below 1-week low"""
    try:
        q = text("""
            SELECT ticker, underlying_price, week1_low, rsi_14, adx_14, volume,
                ROUND(((week1_low - underlying_price) / week1_low * 100)::numeric, 2) as breakdown_pct
            FROM public.technical_screener_cache
            WHERE cache_date = :date AND is_week1_low_breakout = TRUE
            ORDER BY breakdown_pct DESC
            LIMIT :limit
        """)
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting week1 low breakout: {e}")
        return []

def get_week4_high_breakout_stocks(date, limit=100):
    """Stocks breaking above 4-week high"""
    try:
        q = text("""
            SELECT ticker, underlying_price, week4_high, rsi_14, adx_14, volume,
                ROUND(((underlying_price - week4_high) / week4_high * 100)::numeric, 2) as breakout_pct
            FROM public.technical_screener_cache
            WHERE cache_date = :date AND is_week4_high_breakout = TRUE
            ORDER BY breakout_pct DESC
            LIMIT :limit
        """)
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting week4 high breakout: {e}")
        return []

def get_week4_low_breakout_stocks(date, limit=100):
    """Stocks breaking below 4-week low"""
    try:
        q = text("""
            SELECT ticker, underlying_price, week4_low, rsi_14, adx_14, volume,
                ROUND(((week4_low - underlying_price) / week4_low * 100)::numeric, 2) as breakdown_pct
            FROM public.technical_screener_cache
            WHERE cache_date = :date AND is_week4_low_breakout = TRUE
            ORDER BY breakdown_pct DESC
            LIMIT :limit
        """)
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting week4 low breakout: {e}")
        return []

def get_week52_high_breakout_stocks(date, limit=100):
    """Stocks breaking above 52-week high"""
    try:
        q = text("""
            SELECT ticker, underlying_price, week52_high, rsi_14, adx_14, volume,
                ROUND(((underlying_price - week52_high) / week52_high * 100)::numeric, 2) as breakout_pct
            FROM public.technical_screener_cache
            WHERE cache_date = :date AND is_week52_high_breakout = TRUE
            ORDER BY breakout_pct DESC
            LIMIT :limit
        """)
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting week52 high breakout: {e}")
        return []

def get_week52_low_breakout_stocks(date, limit=100):
    """Stocks breaking below 52-week low"""
    try:
        q = text("""
            SELECT ticker, underlying_price, week52_low, rsi_14, adx_14, volume,
                ROUND(((week52_low - underlying_price) / week52_low * 100)::numeric, 2) as breakdown_pct
            FROM public.technical_screener_cache
            WHERE cache_date = :date AND is_week52_low_breakout = TRUE
            ORDER BY breakdown_pct DESC
            LIMIT :limit
        """)
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting week52 low breakout: {e}")
        return []

def get_potential_high_volume_stocks(date, limit=100):
    """Stocks with potential high volume (>1.5x avg)"""
    try:
        q = text("""
            SELECT ticker, underlying_price, volume, volume_change_pct, rsi_14, adx_14
            FROM public.technical_screener_cache
            WHERE cache_date = :date AND is_potential_high_vol = TRUE
            ORDER BY volume_change_pct DESC
            LIMIT :limit
        """)
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting potential high volume: {e}")
        return []


def get_unusually_high_volume_stocks(date, limit=100):
    """Stocks with unusually high volume (>2.5x avg)"""
    try:
        q = text("""
            SELECT ticker, underlying_price, volume, volume_change_pct, rsi_14, adx_14
            FROM public.technical_screener_cache
            WHERE cache_date = :date AND is_unusually_high_vol = TRUE
            ORDER BY volume_change_pct DESC
            LIMIT :limit
        """)
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting unusually high volume: {e}")
        return []


def get_price_gainers_stocks(date, limit=100):
    """Get top price gainers"""
    try:
        q = text("""
            SELECT ticker, underlying_price, price_change_pct, volume, rsi_14, adx_14
            FROM public.technical_screener_cache
            WHERE cache_date = :date AND price_change_pct > 0
            ORDER BY price_change_pct DESC
            LIMIT :limit
        """)
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting price gainers: {e}")
        return []


def get_price_losers_stocks(date, limit=100):
    """Get top price losers"""
    try:
        q = text("""
            SELECT ticker, underlying_price, price_change_pct, volume, rsi_14, adx_14
            FROM public.technical_screener_cache
            WHERE cache_date = :date AND price_change_pct < 0
            ORDER BY price_change_pct ASC
            LIMIT :limit
        """)
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting price losers: {e}")
        return []


def get_high_volume_stocks(date, limit=100):
    """Get high volume stocks"""
    try:
        q = text("""
            SELECT ticker, underlying_price, volume, volume_change_pct, rsi_14, adx_14
            FROM public.technical_screener_cache
            WHERE cache_date = :date
            ORDER BY volume DESC
            LIMIT :limit
        """)
        df = pd.read_sql(q, engine, params={"date": date, "limit": limit})
        return df.to_dict("records")
    except Exception as e:
        print(f"Error getting high volume stocks: {e}")
        return []
