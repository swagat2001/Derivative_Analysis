"""
CASH STOCK MODEL
================
Data layer for cash (non-F&O) stock detail pages.

Provides:
  - is_fo_stock(ticker)                  -> bool
  - get_cash_stock_info(ticker)          -> price, OHLCV, technicals
  - get_stock_scanner_appearances(ticker)-> list of scanner badges
"""

import pandas as pd
from sqlalchemy import text

from .db_config import engine, engine_cash


#
# F&O DETECTION
#

def is_fo_stock(ticker: str) -> bool:
    """
    Return True if ticker has a table in the F&O database.
    That means it has options/futures data → use the existing F&O detail page.
    """
    ticker = ticker.upper().strip()
    derived = f"TBL_{ticker}_DERIVED"
    base    = f"TBL_{ticker}"
    try:
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = ANY(:names)
            """), {"names": [derived, base]}).fetchone()
            return (row[0] > 0) if row else False
    except Exception as e:
        print(f"[WARN] is_fo_stock({ticker}): {e}")
        return False


#
# CASH OHLCV + TECHNICALS
#

def get_cash_stock_info(ticker: str) -> dict:
    """
    Fetch all data needed for the cash stock detail page.
    Returns a dict. Empty dict on failure.
    """
    ticker = ticker.upper().strip()
    result = {}

    #  1. Technical snapshot from technical_screener_cache
    try:
        with engine_cash.connect() as conn:
            row = conn.execute(text("""
                SELECT
                    ticker,
                    cache_date,
                    underlying_price      AS price,
                    price_change_pct      AS change_pct,
                    volume,
                    rsi_14,
                    adx_14,
                    macd,
                    macd_signal,
                    sma_50,
                    sma_200,
                    bb_upper,
                    bb_lower,
                    bb_middle,
                    bb_width,
                    r1, r2, r3,
                    s1, s2, s3,
                    momentum_score,
                    week52_high,
                    week52_low,
                    week1_high,
                    week1_low,
                    week4_high,
                    week4_low,
                    above_50_sma,
                    above_200_sma,
                    below_50_sma,
                    below_200_sma,
                    strong_trend,
                    rsi_above_80,
                    rsi_below_20,
                    macd_pos_cross,
                    macd_neg_cross,
                    dist_from_50sma_pct,
                    dist_from_200sma_pct
                FROM public.technical_screener_cache
                WHERE ticker = :ticker
                ORDER BY cache_date DESC
                LIMIT 1
            """), {"ticker": ticker}).fetchone()

        if row:
            result = dict(row._mapping)
            # Convert numpy scalars to plain Python types
            for k, v in result.items():
                if hasattr(v, 'item'):
                    result[k] = v.item()
            result["has_technical"] = True
        else:
            result["has_technical"] = False
            result["price"]      = 0
            result["change_pct"] = 0

    except Exception as e:
        print(f"[WARN] get_cash_stock_info technical cache ({ticker}): {e}")
        result["has_technical"] = False
        result["price"]         = 0
        result["change_pct"]    = 0

    #  2. OHLCV history — try CashStocks_Database first, then F&O database fallback
    result["ohlcv"] = []
    cash_table = f"TBL_{ticker}"

    def _parse_ohlcv_rows(rows):
        seen_dates = set()
        data = []
        for r in rows:
            dt = str(r[0])
            if dt in seen_dates:
                continue
            seen_dates.add(dt)
            data.append({
                "date":   dt,
                "open":   float(r[1] or 0),
                "high":   float(r[2] or 0),
                "low":    float(r[3] or 0),
                "close":  float(r[4] or 0),
                "volume": int(r[5]   or 0),
            })
            # Safety checks for high/low
            idx = len(data) - 1
            if data[idx]["high"] == 0: data[idx]["high"] = max(data[idx]["open"], data[idx]["close"])
            if data[idx]["low"] == 0:  data[idx]["low"]  = min(data[idx]["open"], data[idx]["close"])

        # Return in ascending order for the chart
        return data[::-1]

    # ── 2a. Cash database (CashStocks_Database) ──────────────────
    try:
        with engine_cash.connect() as conn:
            cash_exists = conn.execute(text("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_name    = :tname
                )
            """), {"tname": cash_table}).scalar()

        if cash_exists:
            with engine_cash.connect() as conn:
                rows = conn.execute(text(f"""
                    SELECT DISTINCT ON ("BizDt")
                        "BizDt"::text           AS date,
                        "OpnPric"::float        AS open,
                        "HghPric"::float        AS high,
                        "LwPric"::float         AS low,
                        "ClsPric"::float        AS close,
                        "TtlTradgVol"::bigint   AS volume
                    FROM public."{cash_table}"
                    WHERE "ClsPric" IS NOT NULL AND "ClsPric" > 0
                    ORDER BY "BizDt" DESC
                    LIMIT 120
                """)).fetchall()
            result["ohlcv"] = _parse_ohlcv_rows(rows)
    except Exception as e:
        print(f"[WARN] get_cash_stock_info OHLCV cash_db ({ticker}): {e}")

    # ── 2b. F&O database fallback (BhavCopy_Database) ────────────
    # Used for F&O stocks whose cash OHLCV lives in BhavCopy_Database
    # as the base table rows where OptnTp IS NULL (futures/EQ rows)
    if not result["ohlcv"]:
        try:
            with engine.connect() as conn:
                fo_exists = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = 'public'
                          AND table_name    = :tname
                    )
                """), {"tname": cash_table}).scalar()

            if fo_exists:
                with engine.connect() as conn:
                    rows = conn.execute(text(f"""
                        SELECT DISTINCT ON ("BizDt")
                            "BizDt"::text                AS date,
                            "OpnPric"::float             AS open,
                            "HghPric"::float             AS high,
                            "LwPric"::float              AS low,
                            "ClsPric"::float             AS close,
                            COALESCE("TtlTradgVol", 0)::bigint AS volume
                        FROM public."{cash_table}"
                        WHERE "OptnTp" IS NULL
                          AND "ClsPric" IS NOT NULL AND "ClsPric" > 0
                        ORDER BY "BizDt" DESC, "FininstrmActlXpryDt" ASC
                        LIMIT 120
                    """)).fetchall()

                if rows:
                    result["ohlcv"] = _parse_ohlcv_rows(rows)
                    print(f"[INFO] get_cash_stock_info: used F&O base table for OHLCV ({ticker}), {len(rows)} rows")
        except Exception as e:
            print(f"[WARN] get_cash_stock_info OHLCV fo_db ({ticker}): {e}")

    # ── 2c. Derive price from OHLCV if technical cache missed ────
    if result["ohlcv"]:
        if not result["has_technical"] or not result.get("price"):
            latest = result["ohlcv"][-1]
            prev   = result["ohlcv"][-2] if len(result["ohlcv"]) > 1 else latest
            result["price"]      = latest["close"]
            result["volume"]     = latest["volume"]
            pchg = ((latest["close"] - prev["close"]) / prev["close"] * 100) if prev["close"] else 0
            if not result["has_technical"]:
                result["change_pct"] = round(pchg, 2)

    result["ticker"] = ticker
    return result


#
# SCANNER APPEARANCES
#

_SCANNER_CHECKS = [
    ("Golden Crossover",      "/scanner/technical-indicators/golden-crossover",  "bullish",
     lambda r: r.get("above_50_sma") and r.get("above_200_sma")
               and (r.get("sma_50") or 0) > (r.get("sma_200") or 0)),

    ("Death Crossover",       "/scanner/technical-indicators/death-crossover",   "bearish",
     lambda r: r.get("below_50_sma") and r.get("below_200_sma")
               and (r.get("sma_50") or 0) < (r.get("sma_200") or 0)),

    ("RSI Overbought",        "/scanner/technical-indicators/rsi-overbought",    "bearish",
     lambda r: (r.get("rsi_14") or 0) > 75),

    ("RSI Oversold",          "/scanner/technical-indicators/rsi-oversold",      "bullish",
     lambda r: (r.get("rsi_14") or 0) < 25),

    ("MACD Bullish Cross",    "/scanner/technical-indicators/macd-bullish-cross",   "bullish",
     lambda r: r.get("macd_pos_cross")),

    ("MACD Bearish Cross",    "/scanner/technical-indicators/macd-bearish-cross",   "bearish",
     lambda r: r.get("macd_neg_cross")),

    ("Momentum Stock",        "/scanner/technical-indicators/momentum-stocks",   "bullish",
     lambda r: (r.get("momentum_score") or 0) > 0),

    ("Squeezing Range",       "/scanner/technical-indicators/squeezing-range",   "neutral",
     lambda r: (r.get("bb_width") or 999) < 5),

    ("R1 Breakout",           "/scanner/technical-indicators/r1-breakout",       "bullish",
     lambda r: r.get("r1_breakout")),

    ("R2 Breakout",           "/scanner/technical-indicators/r2-breakout",       "bullish",
     lambda r: r.get("r2_breakout")),

    ("R3 Breakout",           "/scanner/technical-indicators/r3-breakout",       "bullish",
     lambda r: r.get("r3_breakout")),

    ("S1 Breakdown",          "/scanner/technical-indicators/s1-breakout",       "bearish",
     lambda r: r.get("s1_breakout")),

    ("S2 Breakdown",          "/scanner/technical-indicators/s2-breakout",       "bearish",
     lambda r: r.get("s2_breakout")),

    ("S3 Breakdown",          "/scanner/technical-indicators/s3-breakout",       "bearish",
     lambda r: r.get("s3_breakout")),

    ("52W High Breakout",     "/scanner/technical-indicators/week52-high-breakout", "bullish",
     lambda r: r.get("is_week52_high_breakout")),

    ("52W Low Breakdown",     "/scanner/technical-indicators/week52-low-breakout",  "bearish",
     lambda r: r.get("is_week52_low_breakout")),

    ("4W High Breakout",      "/scanner/technical-indicators/week4-high-breakout",  "bullish",
     lambda r: r.get("is_week4_high_breakout")),

    ("4W Low Breakdown",      "/scanner/technical-indicators/week4-low-breakout",   "bearish",
     lambda r: r.get("is_week4_low_breakout")),

    ("1W High Breakout",      "/scanner/technical-indicators/week1-high-breakout",  "bullish",
     lambda r: r.get("is_week1_high_breakout")),

    ("1W Low Breakdown",      "/scanner/technical-indicators/week1-low-breakout",   "bearish",
     lambda r: r.get("is_week1_low_breakout")),

    ("Unusually High Volume", "/scanner/technical-indicators/unusually-high-volume","neutral",
     lambda r: r.get("is_unusually_high_vol")),

    ("Potential High Volume", "/scanner/technical-indicators/potential-high-volume","neutral",
     lambda r: r.get("is_potential_high_vol")),

    ("Strong ADX Trend",      "/scanner/technical-indicators/strong-adx-trend",   "neutral",
     lambda r: r.get("strong_trend")),

    ("Above 50 & 200 SMA",    "/scanner/technical-indicators/above-50-200-sma",   "bullish",
     lambda r: r.get("above_50_sma") and r.get("above_200_sma")),

    ("Below 50 & 200 SMA",    "/scanner/technical-indicators/below-50-200-sma",   "bearish",
     lambda r: r.get("below_50_sma") and r.get("below_200_sma")),
]


def get_stock_scanner_appearances(ticker: str) -> list:
    """
    Return list of scanners the stock currently appears in.
    Each item: { label, url, sentiment }
    """
    ticker = ticker.upper().strip()
    appearances = []

    try:
        with engine_cash.connect() as conn:
            row = conn.execute(text("""
                SELECT *
                FROM public.technical_screener_cache
                WHERE ticker = :ticker
                ORDER BY cache_date DESC
                LIMIT 1
            """), {"ticker": ticker}).fetchone()

        if not row:
            return appearances

        row_dict = dict(row._mapping)

        for label, url, sentiment, check_fn in _SCANNER_CHECKS:
            try:
                if check_fn(row_dict):
                    appearances.append({
                        "label":     label,
                        "url":       url,
                        "sentiment": sentiment,
                    })
            except Exception:
                pass

    except Exception as e:
        print(f"[WARN] get_stock_scanner_appearances({ticker}): {e}")

    return appearances
