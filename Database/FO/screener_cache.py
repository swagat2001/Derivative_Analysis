"""
SCREENER CACHE BUILDER - CORRECTED WITH STRIKE PRICES + BULL/BEAR CLASSIFICATION
================================================================================
Pre-calculates all screener data (OI, Moneyness, IV changes) with strike prices
Added: Bullish/Bearish classification injected and final_signal attached to cache rows
"""

import json
from datetime import datetime
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, inspect, text

# Database config
db_user = "postgres"
db_password = "Gallop@3104"
db_host = "localhost"
db_port = "5432"
db_name = "BhavCopy_Database"
db_password_enc = quote_plus(db_password)
engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}")

# ==================================================================================
# BULLISH / BEARISH CATEGORY DEFINITIONS (reference)
# Note: we will derive category names programmatically per-row (see build below)
# ==================================================================================

BULLISH_CATEGORIES = [
    "CALL_IV_GAINERS_ALL",
    "CALL_IV_GAINERS_ITM",
    "CALL_IV_GAINERS_OTM",
    "CALL_MONEYNESS_GAINERS_ALL",
    "CALL_MONEYNESS_GAINERS_ITM",
    "CALL_MONEYNESS_GAINERS_OTM",
    "PUT_IV_LOSERS_ALL",
    "PUT_IV_LOSERS_ITM",
    "PUT_IV_LOSERS_OTM",
    "PUT_MONEYNESS_LOSERS_ALL",
    "PUT_MONEYNESS_LOSERS_ITM",
    "PUT_MONEYNESS_LOSERS_OTM",
    "FUT_OI_GAINERS_ALL",
    "CALL_OI_GAINERS_ALL",
    "CALL_OI_GAINERS_ITM",
    "CALL_OI_GAINERS_OTM",
    "PUT_OI_LOSERS_ALL",
    "PUT_OI_LOSERS_ITM",
    "PUT_OI_LOSERS_OTM",
]

BEARISH_CATEGORIES = [
    "CALL_IV_LOSERS_ALL",
    "CALL_IV_LOSERS_ITM",
    "CALL_IV_LOSERS_OTM",
    "CALL_MONEYNESS_LOSERS_ALL",
    "CALL_MONEYNESS_LOSERS_ITM",
    "CALL_MONEYNESS_LOSERS_OTM",
    "PUT_IV_GAINERS_ALL",
    "PUT_IV_GAINERS_ITM",
    "PUT_IV_GAINERS_OTM",
    "PUT_MONEYNESS_GAINERS_ALL",
    "PUT_MONEYNESS_GAINERS_ITM",
    "PUT_MONEYNESS_GAINERS_OTM",
    "FUT_OI_LOSERS_ALL",
    "CALL_OI_LOSERS_ALL",
    "CALL_OI_LOSERS_ITM",
    "CALL_OI_LOSERS_OTM",
    "PUT_OI_GAINERS_ALL",
    "PUT_OI_GAINERS_ITM",
    "PUT_OI_GAINERS_OTM",
]


# ==================================================================================
# FINAL CLASSIFICATION LOGIC
# ==================================================================================
def classify_final_signal(ticker, bullish_dict, bearish_dict):
    """
    Final binary rule:
      - If bullish_signals > bearish_signals => BULLISH
      - Else => BEARISH (ties go to BEARISH per your manager rule)
    """
    bull = bullish_dict.get(ticker, 0)
    bear = bearish_dict.get(ticker, 0)
    return "BULLISH" if bull > bear else "BEARISH"


# =============================================================
# CREATE SCREENER CACHE TABLE WITH STRIKE PRICE
# =============================================================
def create_screener_cache_table():
    """Create the screener_cache table if it doesn't exist - with strike_price column"""
    try:
        # First check if table exists and add strike_price column if missing
        check_query = """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM information_schema.tables
                      WHERE table_schema = 'public'
                      AND table_name = 'screener_cache') THEN

                -- Add strike_price column if it doesn't exist
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                              WHERE table_schema = 'public'
                              AND table_name = 'screener_cache'
                              AND column_name = 'strike_price') THEN
                    ALTER TABLE public.screener_cache ADD COLUMN strike_price NUMERIC;
                END IF;
            END IF;
        END $$;
        """

        with engine.begin() as conn:
            conn.execute(text(check_query))

        # Create table with strike_price column
        create_query = """
        CREATE TABLE IF NOT EXISTS public.screener_cache (
            id SERIAL PRIMARY KEY,
            cache_date DATE NOT NULL,
            metric_type VARCHAR(50) NOT NULL,
            option_type VARCHAR(10) NOT NULL,
            moneyness_filter VARCHAR(50) NOT NULL,
            rank INT NOT NULL,
            ticker VARCHAR(50) NOT NULL,
            strike_price NUMERIC,
            underlying_price NUMERIC NOT NULL,
            change NUMERIC NOT NULL,
            bullish_count INT DEFAULT 0,
            bearish_count INT DEFAULT 0,
            final_signal VARCHAR(10) DEFAULT 'BEARISH',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Create index for fast lookups
        CREATE INDEX IF NOT EXISTS idx_screener_cache_date_metric
            ON public.screener_cache(cache_date, metric_type, option_type, moneyness_filter);

        CREATE INDEX IF NOT EXISTS idx_screener_cache_date
            ON public.screener_cache(cache_date);
        """

        with engine.begin() as conn:
            conn.execute(text(create_query))

        print("✅ Screener cache table created/verified with strike_price column")
        return True

    except Exception as e:
        print(f"❌ Error creating screener cache table: {e}")
        return False


# =============================================================
# HELPER FUNCTIONS
# =============================================================
def get_prev_date(selected_date: str, all_dates: list):
    """Get previous trading date"""
    try:
        if not all_dates:
            return None

        try:
            idx = all_dates.index(selected_date)
            if idx < len(all_dates) - 1:
                return all_dates[idx + 1]
        except ValueError:
            for date in all_dates:
                if date < selected_date:
                    return date

        return None
    except Exception as e:
        print(f"❌ Error in get_prev_date: {e}")
        return None


def get_all_tables(engine):
    """Get all derived tables"""
    try:
        inspector = inspect(engine)
        all_tables = [t for t in inspector.get_table_names(schema="public") if t.endswith("_DERIVED")]
        return all_tables
    except Exception as e:
        print(f"❌ Error getting tables: {e}")
        return []


# =============================================================
# CALCULATE SCREENER DATA WITH STRIKE PRICES - CORRECTED
# =============================================================
def calculate_screener_data_for_date(selected_date: str, all_dates: list):
    """
    Calculate all screener data for a given date INCLUDING strike prices
    CORRECTED: Proper loop structure, strike_price in base_data
    """
    try:
        prev_date = get_prev_date(selected_date, all_dates)
        if not prev_date:
            print(f"   ⚠️  No previous date found for {selected_date}")
            return []

        all_tables = get_all_tables(engine)
        if not all_tables:
            return []

        # Initialize result structure to collect all data
        result = {
            "oi": {
                "CE": {"ALL": [], "ITM": [], "OTM": []},
                "PE": {"ALL": [], "ITM": [], "OTM": []},
                "FUT": {"ALL": []},
            },
            "moneyness": {
                "CE": {"ALL": [], "ITM": [], "OTM": []},
                "PE": {"ALL": [], "ITM": [], "OTM": []},
                "FUT": {"ALL": []},
            },
            "iv": {
                "CE": {"ALL": [], "ITM": [], "OTM": []},
                "PE": {"ALL": [], "ITM": [], "OTM": []},
                "FUT": {"ALL": []},
            },
        }

        # Process each table
        for table_name in all_tables:
            try:
                ticker = table_name.replace("TBL_", "").replace("_DERIVED", "")

                # Single optimized query to get ALL data for this ticker
                query = text(
                    f"""
                    WITH curr_data AS (
                        SELECT
                            "StrkPric",
                            "OptnTp",
                            "UndrlygPric",
                            "OpnIntrst",
                            "ChngInOpnIntrst",
                            "LastPric",
                            CASE
                                WHEN "UndrlygPric" IS NOT NULL AND "StrkPric" IS NOT NULL AND "UndrlygPric"::NUMERIC != 0
                                THEN (("UndrlygPric"::NUMERIC - "StrkPric"::NUMERIC) / "UndrlygPric"::NUMERIC)
                                ELSE NULL
                            END AS moneyness_curr,
                            "iv" AS iv_curr
                        FROM public."{table_name}"
                        WHERE "BizDt" = :curr_date
                            AND ("OptnTp" = 'CE' OR "OptnTp" = 'PE' OR "OptnTp" IS NULL)
                    ),
                    prev_data AS (
                        SELECT
                            "StrkPric",
                            "OptnTp",
                            "OpnIntrst" AS prev_oi,
                            "LastPric" AS prev_ltp,
                            CASE
                                WHEN "UndrlygPric" IS NOT NULL AND "StrkPric" IS NOT NULL AND "UndrlygPric"::NUMERIC != 0
                                THEN (("UndrlygPric"::NUMERIC - "StrkPric"::NUMERIC) / "UndrlygPric"::NUMERIC)
                                ELSE NULL
                            END AS moneyness_prev,
                            "iv" AS iv_prev
                        FROM public."{table_name}"
                        WHERE "BizDt" = :prev_date
                            AND ("OptnTp" = 'CE' OR "OptnTp" = 'PE' OR "OptnTp" IS NULL)
                    )
                    SELECT
                        c."StrkPric",
                        c."OptnTp",
                        c."UndrlygPric",
                        c."OpnIntrst" AS current_oi,
                        c."LastPric" AS current_ltp,
                        -- Calculate OI percentage change properly
                        CASE
                            WHEN COALESCE(p.prev_oi::NUMERIC, 0) != 0
                            THEN ((c."OpnIntrst"::NUMERIC - COALESCE(p.prev_oi::NUMERIC, c."OpnIntrst"::NUMERIC)) / COALESCE(p.prev_oi::NUMERIC, c."OpnIntrst"::NUMERIC)) * 100
                            ELSE 0
                        END AS oi_change,
                        (c."OpnIntrst"::NUMERIC * c."LastPric"::NUMERIC) - (COALESCE(p.prev_oi::NUMERIC, c."OpnIntrst"::NUMERIC) * COALESCE(p.prev_ltp::NUMERIC, c."LastPric"::NUMERIC)) AS moneyness_change,
                        -- Calculate IV percentage change properly
                        CASE
                            WHEN COALESCE(p.iv_prev::NUMERIC, 0) != 0
                            THEN ((c.iv_curr::NUMERIC - COALESCE(p.iv_prev::NUMERIC, c.iv_curr::NUMERIC)) / COALESCE(p.iv_prev::NUMERIC, c.iv_curr::NUMERIC)) * 100
                            ELSE 0
                        END AS iv_change,
                        c.moneyness_curr,
                        c.iv_curr,
                        COALESCE(p.prev_oi::NUMERIC, c."OpnIntrst"::NUMERIC) AS prev_oi,
                        COALESCE(p.prev_ltp::NUMERIC, c."LastPric"::NUMERIC) AS prev_ltp
                    FROM curr_data c
                    LEFT JOIN prev_data p ON
                        (c."StrkPric" = p."StrkPric" OR (c."StrkPric" IS NULL AND p."StrkPric" IS NULL))
                        AND (c."OptnTp" = p."OptnTp" OR (c."OptnTp" IS NULL AND p."OptnTp" IS NULL))
                    WHERE c."UndrlygPric" IS NOT NULL
                """
                )

                df = pd.read_sql(query, con=engine, params={"curr_date": selected_date, "prev_date": prev_date})

                if df.empty:
                    continue

                # Convert to numeric
                numeric_cols = [
                    "StrkPric",
                    "UndrlygPric",
                    "current_oi",
                    "current_ltp",
                    "oi_change",
                    "moneyness_change",
                    "iv_change",
                    "prev_oi",
                    "prev_ltp",
                ]
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce")

                # Calculate strike_diff for ITM/OTM
                df["strike_diff"] = df["UndrlygPric"] - df["StrkPric"]

                # Get latest underlying price for this ticker
                latest_underlying = (
                    df["UndrlygPric"].iloc[-1] if not df["UndrlygPric"].empty else df["UndrlygPric"].max()
                )

                # ========================================================
                # SECTION 1: Process FUTURES (OptnTp IS NULL)
                # ========================================================
                df_fut = df[df["OptnTp"].isna()].copy()
                if not df_fut.empty:
                    # ✅ FIX: Add strike_price to base_data_fut
                    base_data_fut = {
                        "ticker": ticker,
                        "underlying_price": float(latest_underlying) if pd.notna(latest_underlying) else 0.0,
                        "strike_price": 0.0,  # Futures don't have strikes
                    }

                    # OI changes for futures
                    if "current_oi" in df_fut.columns and "prev_oi" in df_fut.columns:
                        total_curr_oi_fut = df_fut["current_oi"].sum()
                        total_prev_oi_fut = df_fut["prev_oi"].sum()
                        oi_fut = (
                            ((total_curr_oi_fut - total_prev_oi_fut) / total_prev_oi_fut * 100)
                            if total_prev_oi_fut != 0
                            else 0
                        )
                        if pd.notna(oi_fut):
                            result["oi"]["FUT"]["ALL"].append({**base_data_fut, "change": float(oi_fut)})

                    # Moneyness changes for futures
                    if "current_oi" in df_fut.columns and "current_ltp" in df_fut.columns:
                        df_fut["curr_value"] = df_fut["current_oi"] * df_fut["current_ltp"]
                        df_fut["prev_value"] = df_fut["prev_oi"] * df_fut["prev_ltp"]
                        total_curr_value_fut = df_fut["curr_value"].sum()
                        total_prev_value_fut = df_fut["prev_value"].sum()
                        moneyness_fut = (
                            ((total_curr_value_fut - total_prev_value_fut) / total_prev_value_fut * 100)
                            if total_prev_value_fut != 0
                            else 0
                        )
                        if pd.notna(moneyness_fut):
                            result["moneyness"]["FUT"]["ALL"].append({**base_data_fut, "change": float(moneyness_fut)})

                    # IV changes for futures
                    if "iv_change" in df_fut.columns and "current_oi" in df_fut.columns:
                        if df_fut["current_oi"].sum() != 0:
                            iv_fut = (df_fut["iv_change"] * df_fut["current_oi"]).sum() / df_fut["current_oi"].sum()
                        else:
                            iv_fut = df_fut["iv_change"].mean()
                        if pd.notna(iv_fut):
                            result["iv"]["FUT"]["ALL"].append({**base_data_fut, "change": float(iv_fut)})

                # ========================================================
                # SECTION 2: Process OPTIONS (CE and PE) with strike prices
                # ========================================================
                for opt_type in ["CE", "PE"]:
                    df_opt = df[df["OptnTp"] == opt_type].copy()
                    if df_opt.empty:
                        continue

                    # ✅ FIX: Get strike price with highest OI for this option type
                    if not df_opt.empty and "current_oi" in df_opt.columns:
                        max_oi_idx = df_opt["current_oi"].idxmax()
                        max_oi_strike = (
                            float(df_opt.loc[max_oi_idx, "StrkPric"])
                            if pd.notna(df_opt.loc[max_oi_idx, "StrkPric"])
                            else 0.0
                        )
                    else:
                        max_oi_strike = 0.0

                    # Filter ITM/OTM
                    if opt_type == "CE":
                        df_itm = df_opt[df_opt["strike_diff"] > 0].copy()
                        df_otm = df_opt[df_opt["strike_diff"] < 0].copy()
                    else:  # PE
                        df_itm = df_opt[df_opt["strike_diff"] < 0].copy()
                        df_otm = df_opt[df_opt["strike_diff"] > 0].copy()

                    # ✅ FIX: Aggregate data with strike_price
                    base_data = {
                        "ticker": ticker,
                        "underlying_price": float(latest_underlying) if pd.notna(latest_underlying) else 0.0,
                        "strike_price": max_oi_strike,  # Add strike price
                    }

                    # OI changes - Calculate TOTAL OI change, then convert to percentage
                    # FIXED: Always include entries regardless of change value (removed != 0 filters)
                    if "current_oi" in df_opt.columns and "prev_oi" in df_opt.columns:
                        total_curr_oi_all = df_opt["current_oi"].sum()
                        total_prev_oi_all = df_opt["prev_oi"].sum()
                        total_curr_oi_itm = df_itm["current_oi"].sum() if not df_itm.empty else 0
                        total_prev_oi_itm = df_itm["prev_oi"].sum() if not df_itm.empty else 0
                        total_curr_oi_otm = df_otm["current_oi"].sum() if not df_otm.empty else 0
                        total_prev_oi_otm = df_otm["prev_oi"].sum() if not df_otm.empty else 0

                        oi_all = (
                            ((total_curr_oi_all - total_prev_oi_all) / total_prev_oi_all * 100)
                            if total_prev_oi_all != 0
                            else 0
                        )
                        oi_itm = (
                            ((total_curr_oi_itm - total_prev_oi_itm) / total_prev_oi_itm * 100)
                            if total_prev_oi_itm != 0
                            else 0
                        )
                        oi_otm = (
                            ((total_curr_oi_otm - total_prev_oi_otm) / total_prev_oi_otm * 100)
                            if total_prev_oi_otm != 0
                            else 0
                        )

                        # Always include entries (removed != 0 condition)
                        if pd.notna(oi_all):
                            result["oi"][opt_type]["ALL"].append({**base_data, "change": float(oi_all)})
                        if pd.notna(oi_itm):
                            result["oi"][opt_type]["ITM"].append({**base_data, "change": float(oi_itm)})
                        if pd.notna(oi_otm):
                            result["oi"][opt_type]["OTM"].append({**base_data, "change": float(oi_otm)})

                    # Moneyness changes
                    # FIXED: Always include entries regardless of change value
                    if "current_oi" in df_opt.columns and "current_ltp" in df_opt.columns:
                        df_opt["curr_value"] = df_opt["current_oi"] * df_opt["current_ltp"]
                        df_opt["prev_value"] = df_opt["prev_oi"] * df_opt["prev_ltp"]

                        total_curr_value_all = df_opt["curr_value"].sum()
                        total_prev_value_all = df_opt["prev_value"].sum()
                        money_all = (
                            ((total_curr_value_all - total_prev_value_all) / total_prev_value_all * 100)
                            if total_prev_value_all != 0
                            else 0
                        )

                        if not df_itm.empty:
                            df_itm["curr_value"] = df_itm["current_oi"] * df_itm["current_ltp"]
                            df_itm["prev_value"] = df_itm["prev_oi"] * df_itm["prev_ltp"]
                            total_curr_value_itm = df_itm["curr_value"].sum()
                            total_prev_value_itm = df_itm["prev_value"].sum()
                            money_itm = (
                                ((total_curr_value_itm - total_prev_value_itm) / total_prev_value_itm * 100)
                                if total_prev_value_itm != 0
                                else 0
                            )
                        else:
                            money_itm = 0

                        if not df_otm.empty:
                            df_otm["curr_value"] = df_otm["current_oi"] * df_otm["current_ltp"]
                            df_otm["prev_value"] = df_otm["prev_oi"] * df_otm["prev_ltp"]
                            total_curr_value_otm = df_otm["curr_value"].sum()
                            total_prev_value_otm = df_otm["prev_value"].sum()
                            money_otm = (
                                ((total_curr_value_otm - total_prev_value_otm) / total_prev_value_otm * 100)
                                if total_prev_value_otm != 0
                                else 0
                            )
                        else:
                            money_otm = 0

                        # Always include entries (removed != 0 condition)
                        if pd.notna(money_all):
                            result["moneyness"][opt_type]["ALL"].append({**base_data, "change": float(money_all)})
                        if pd.notna(money_itm):
                            result["moneyness"][opt_type]["ITM"].append({**base_data, "change": float(money_itm)})
                        if pd.notna(money_otm):
                            result["moneyness"][opt_type]["OTM"].append({**base_data, "change": float(money_otm)})

                    # IV changes
                    # FIXED: Always include entries regardless of change value
                    if "iv_change" in df_opt.columns and "current_oi" in df_opt.columns:
                        if df_opt["current_oi"].sum() != 0:
                            iv_all = (df_opt["iv_change"] * df_opt["current_oi"]).sum() / df_opt["current_oi"].sum()
                        else:
                            iv_all = df_opt["iv_change"].mean()

                        if not df_itm.empty and df_itm["current_oi"].sum() != 0:
                            iv_itm = (df_itm["iv_change"] * df_itm["current_oi"]).sum() / df_itm["current_oi"].sum()
                        else:
                            iv_itm = df_itm["iv_change"].mean() if not df_itm.empty else 0

                        if not df_otm.empty and df_otm["current_oi"].sum() != 0:
                            iv_otm = (df_otm["iv_change"] * df_otm["current_oi"]).sum() / df_otm["current_oi"].sum()
                        else:
                            iv_otm = df_otm["iv_change"].mean() if not df_otm.empty else 0

                        # Always include entries (removed != 0 condition)
                        if pd.notna(iv_all):
                            result["iv"][opt_type]["ALL"].append({**base_data, "change": float(iv_all)})
                        if pd.notna(iv_itm):
                            result["iv"][opt_type]["ITM"].append({**base_data, "change": float(iv_itm)})
                        if pd.notna(iv_otm):
                            result["iv"][opt_type]["OTM"].append({**base_data, "change": float(iv_otm)})

            except Exception as e:
                print(f"      ⚠️  Error processing {table_name}: {str(e)[:200]}")
                continue

        # ========================================================
        # SECTION 3: Build cache rows (AFTER all tables processed)
        # ========================================================
        cache_rows = []

        for metric_type in ["oi", "moneyness", "iv"]:
            for opt_type in ["CE", "PE", "FUT"]:
                if opt_type == "FUT":
                    all_data = result[metric_type][opt_type]["ALL"]

                    if not all_data:
                        continue

                    # FIXED: Sort all data for top 10 instead of filtering by positive/negative
                    sorted_for_gainers = sorted(all_data, key=lambda x: x["change"], reverse=True)
                    top_10_gainers = sorted_for_gainers[:10]

                    sorted_for_losers = sorted(all_data, key=lambda x: x["change"])
                    top_10_losers = sorted_for_losers[:10]

                    for rank, item in enumerate(top_10_gainers, 1):
                        cache_rows.append(
                            {
                                "cache_date": selected_date,
                                "metric_type": metric_type,
                                "option_type": opt_type,
                                "moneyness_filter": "ALL",
                                "rank": rank,
                                "ticker": item["ticker"],
                                "underlying_price": item["underlying_price"],
                                "strike_price": item.get("strike_price", 0),
                                "change": item["change"],
                            }
                        )

                    for rank, item in enumerate(top_10_losers, 1):
                        cache_rows.append(
                            {
                                "cache_date": selected_date,
                                "metric_type": metric_type,
                                "option_type": opt_type,
                                "moneyness_filter": "ALL_LOSERS",
                                "rank": rank,
                                "ticker": item["ticker"],
                                "underlying_price": item["underlying_price"],
                                "strike_price": item.get("strike_price", 0),
                                "change": item["change"],
                            }
                        )

                else:
                    for filter_type in ["ALL", "ITM", "OTM"]:
                        if filter_type not in result[metric_type][opt_type]:
                            continue

                        all_data = result[metric_type][opt_type][filter_type]

                        if not all_data:
                            continue

                        # FIXED: Sort all data by change descending for gainers (top 10 highest)
                        # and ascending for losers (top 10 lowest/most negative)
                        # This ensures we always get 10 entries even if some have 0 or negative change

                        # For GAINERS: Sort by change descending, take top 10
                        # This includes the highest changes (positive first, then zeros, then negative)
                        sorted_for_gainers = sorted(all_data, key=lambda x: x["change"], reverse=True)
                        top_10_gainers = sorted_for_gainers[:10]

                        # For LOSERS: Sort by change ascending, take top 10
                        # This includes the lowest changes (most negative first)
                        sorted_for_losers = sorted(all_data, key=lambda x: x["change"])
                        top_10_losers = sorted_for_losers[:10]

                        for rank, item in enumerate(top_10_gainers, 1):
                            cache_rows.append(
                                {
                                    "cache_date": selected_date,
                                    "metric_type": metric_type,
                                    "option_type": opt_type,
                                    "moneyness_filter": filter_type,
                                    "rank": rank,
                                    "ticker": item["ticker"],
                                    "underlying_price": item["underlying_price"],
                                    "strike_price": item.get("strike_price", 0),
                                    "change": item["change"],
                                }
                            )

                        for rank, item in enumerate(top_10_losers, 1):
                            cache_rows.append(
                                {
                                    "cache_date": selected_date,
                                    "metric_type": metric_type,
                                    "option_type": opt_type,
                                    "moneyness_filter": f"{filter_type}_LOSERS",
                                    "rank": rank,
                                    "ticker": item["ticker"],
                                    "underlying_price": item["underlying_price"],
                                    "strike_price": item.get("strike_price", 0),
                                    "change": item["change"],
                                }
                            )

        # ========================================================
        # SECTION 4: Build bullish/bearish counts & final signal
        # ========================================================
        try:
            bullish_dict = {}
            bearish_dict = {}

            # metric -> token map
            metric_map = {"OI": "OI", "IV": "IV", "MONEYNESS": "MONEYNESS"}

            for row in cache_rows:
                metric_type = str(row.get("metric_type", "")).upper()  # 'oi', 'iv', 'moneyness'
                option_type = str(row.get("option_type", "")).upper()  # 'CE' / 'PE' / 'FUT'
                moneyness_filter = str(row.get("moneyness_filter", "")).upper()  # 'ALL', 'ITM', 'ITM_LOSERS', etc.
                change = row.get("change", 0)
                ticker = row.get("ticker")
                if not ticker:
                    continue

                # Determine direction
                direction = "GAINERS" if (change is not None and change > 0) else "LOSERS"

                # Metric label
                metric_label = metric_type.upper()
                if metric_label == "OI":
                    metric_label_token = "OI"
                elif metric_label == "IV":
                    metric_label_token = "IV"
                else:
                    metric_label_token = "MONEYNESS"

                # Normalize moneyness token: if filter endswith '_LOSERS', remove suffix for token position
                # Our desired canonical naming is: <OPTION>_<METRIC>_<DIRECTION>_<BUCKET>
                # Where BUCKET is ONE OF: ALL / ITM / OTM
                bucket = moneyness_filter
                # If bucket contains '_LOSERS' (like 'ALL_LOSERS' or 'ITM_LOSERS') remove suffix for naming
                if bucket.endswith("_LOSERS"):
                    bucket_base = bucket.replace("_LOSERS", "")
                else:
                    bucket_base = bucket

                # For FUT, bucket_base should be 'ALL'
                if option_type == "FUT":
                    bucket_base = "ALL"

                # Build canonical category
                canonical = f"{option_type}_{metric_label_token}_{direction}_{bucket_base}"
                canonical = canonical.replace(" ", "_")

                # Now count towards bullish or bearish depending on membership
                if canonical in BULLISH_CATEGORIES:
                    bullish_dict[ticker] = bullish_dict.get(ticker, 0) + 1
                if canonical in BEARISH_CATEGORIES:
                    bearish_dict[ticker] = bearish_dict.get(ticker, 0) + 1

                # Additional heuristic mapping: (cover cases where list used reversed sign logic)
                # e.g., PUT_IV_LOSERS_ALL is bullish (IV down on puts == bullish) — already handled above
                # If no exact match found, try alternate canonical forms (swap direction) and check membership
                if canonical not in BULLISH_CATEGORIES and canonical not in BEARISH_CATEGORIES:
                    # Try alternate: sometimes metric naming used CALL_IV_GAINERS_ALL vs 'CE_IV_GAINERS_ALL'
                    alt_opt = "CALL" if option_type == "CE" else ("PUT" if option_type == "PE" else option_type)
                    alt_canonical = f"{alt_opt}_{metric_label_token}_{direction}_{bucket_base}"
                    if alt_canonical in BULLISH_CATEGORIES:
                        bullish_dict[ticker] = bullish_dict.get(ticker, 0) + 1
                    if alt_canonical in BEARISH_CATEGORIES:
                        bearish_dict[ticker] = bearish_dict.get(ticker, 0) + 1

            # compute final signals
            final_signals = {}
            all_tickers = set(list(bullish_dict.keys()) + list(bearish_dict.keys()))
            for t in all_tickers:
                final_signals[t] = classify_final_signal(t, bullish_dict, bearish_dict)

            # attach counts and final signal to each cache row (so db insert will include)
            for row in cache_rows:
                row["bullish_count"] = int(bullish_dict.get(row.get("ticker"), 0) or 0)
                row["bearish_count"] = int(bearish_dict.get(row.get("ticker"), 0) or 0)
                row["final_signal"] = final_signals.get(row.get("ticker"), "BEARISH")
        except Exception as e:
            print(f"⚠️ Error computing final signals: {e}")

        return cache_rows

    except Exception as e:
        print(f"❌ Error calculating screener data: {e}")
        import traceback

        traceback.print_exc()
        return []


# =============================================================
# PRECALCULATE ALL SCREENER DATA
# =============================================================
def precalculate_screener_cache():
    """
    Pre-calculate screener data for all new dates and store in cache table
    Called from update_database.py after Greeks calculation
    """
    try:
        print("\n" + "=" * 80)
        print("SCREENER CACHE: PRE-CALCULATING DATA WITH STRIKE PRICES")
        print("=" * 80 + "\n")

        # Create/update cache table
        if not create_screener_cache_table():
            return False

        # Get all available dates
        inspector = inspect(engine)
        base_tables = [t for t in inspector.get_table_names() if t.startswith("TBL_") and not t.endswith("_DERIVED")]

        if not base_tables:
            print("⚠️  No base tables found")
            return False

        sample_table = next((t for t in ["TBL_NIFTY", "TBL_BANKNIFTY"] if t in base_tables), base_tables[0])

        # Get all dates
        query_base = text(
            f'SELECT DISTINCT "BizDt" FROM public."{sample_table}" WHERE "BizDt" IS NOT NULL ORDER BY "BizDt" DESC'
        )
        base_dates = pd.read_sql(query_base, engine)

        if base_dates.empty:
            print("⚠️  No dates found in database")
            return False

        all_dates = [str(d.date()) if hasattr(d, "date") else str(d) for d in base_dates["BizDt"].tolist()]

        # Get already cached dates
        try:
            query_cache = text("SELECT DISTINCT cache_date FROM public.screener_cache ORDER BY cache_date DESC")
            cached_dates = pd.read_sql(query_cache, engine)
            cached_dates_set = set(
                str(d.date()) if hasattr(d, "date") else str(d) for d in cached_dates["cache_date"].tolist()
            )
        except:
            cached_dates_set = set()

        # Find new dates to process
        new_dates = [d for d in all_dates if d not in cached_dates_set]

        if not new_dates:
            print("✅ All dates already cached!")
            return True

        print(f"📅 Found {len(new_dates)} new date(s) to cache\n")

        # Process each new date
        total_rows_inserted = 0
        for date_idx, selected_date in enumerate(new_dates, 1):
            print(f"  [{date_idx}/{len(new_dates)}] {selected_date}...", end=" ")

            cache_rows = calculate_screener_data_for_date(selected_date, all_dates)

            if cache_rows:
                # Insert into database
                df_cache = pd.DataFrame(cache_rows)
                # ensure columns exist in df before writing
                expected_cols = [
                    "cache_date",
                    "metric_type",
                    "option_type",
                    "moneyness_filter",
                    "rank",
                    "ticker",
                    "strike_price",
                    "underlying_price",
                    "change",
                    "bullish_count",
                    "bearish_count",
                    "final_signal",
                ]
                for c in expected_cols:
                    if c not in df_cache.columns:
                        df_cache[c] = None
                df_cache.to_sql("screener_cache", con=engine, if_exists="append", index=False)
                print(f"✅ ({len(cache_rows)} rows)")
                total_rows_inserted += len(cache_rows)
            else:
                print("⚠️  (0 rows)")

        print(f"\n✅ Screener cache pre-calculation complete!")
        print(f"   Total rows inserted: {total_rows_inserted}")
        return True

    except Exception as e:
        print(f"❌ Error in precalculate_screener_cache: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    precalculate_screener_cache()
