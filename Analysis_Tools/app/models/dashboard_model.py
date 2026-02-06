# =============================================================
#  DASHBOARD MODEL MODULE (Flask MVC - Model Layer)
#  Purpose: Loads pre-calculated dashboard data for TOTAL / OTM / ITM views
#  Filters stocks based on stock list.xlsx
# =============================================================

import json

import pandas as pd
from sqlalchemy import text

from .db_config import engine, get_stock_list_from_excel


def _get_available_dates_cached():
    """Internal cached function for dates."""
    try:
        query = text(
            """
            SELECT DISTINCT biz_date::date::text AS date
            FROM options_dashboard_cache
            ORDER BY date DESC;
        """
        )
        df = pd.read_sql(query, con=engine)
        return tuple(df["date"].tolist())  # Convert to tuple for caching
    except Exception as e:
        print(f"[ERROR] get_available_dates(): {e}")
        return tuple()


def get_available_dates():
    """Fetch distinct biz_date values for dropdown (with caching)."""
    return list(_get_available_dates_cached())


def clear_date_cache():
    """Clear date cache - useful when new data is added to database."""
    _get_available_dates_cached.cache_clear()
    print("[INFO] Dashboard date cache cleared")


# =============================================================
# 2️⃣ DASHBOARD DATA (TOTAL / OTM / ITM) with Excel Filter
# =============================================================


def get_dashboard_data(selected_date, mtype="TOTAL"):
    """
    Loads pre-calculated dashboard data from options_dashboard_cache
    for given date and moneyness_type.
    Filters stocks based on stock list.xlsx (with caching)
    """
    try:
        # Load allowed stocks from Excel (cached)
        allowed_stocks = get_stock_list_from_excel()

        query = text(
            """
            SELECT data_json
            FROM options_dashboard_cache
            WHERE biz_date = :biz_date
              AND moneyness_type = :mtype
            LIMIT 1;
        """
        )
        df = pd.read_sql(query, con=engine, params={"biz_date": selected_date, "mtype": mtype})

        if df.empty:
            print(f"[INFO] No dashboard data found for {selected_date}, {mtype}")
            return []

        # Parse JSON from the table
        raw_json = df.iloc[0]["data_json"]
        parsed = json.loads(raw_json)

        # Data should be a list of dicts
        if isinstance(parsed, dict):
            parsed = parsed.get("data", [])
        elif not isinstance(parsed, list):
            return []

        # Convert to DataFrame
        dashboard_df = pd.DataFrame(parsed)

        # Filter by allowed stocks from Excel
        if allowed_stocks and "stock" in dashboard_df.columns:
            # Normalize both sides for comparison (strip whitespace, uppercase)
            allowed_stocks_normalized = set(s.strip().upper() for s in allowed_stocks)

            # --- SYMBOL MAPPING FIX ---
            # Map Excel symbols to DB symbols to prevent false "missing" flags
            # M&M -> M_M, BAJAJ-AUTO -> BAJAJ_AUTO
            symbol_map = {"M&M": "M_M", "BAJAJ-AUTO": "BAJAJ_AUTO", "ARE&M": "ARE_M"}

            # Update allowed_stocks_normalized with mapped values
            # logic: if "M&M" is in allowed, also add "M_M" (or replace it)
            mapped_additions = set()
            for excel_sym, db_sym in symbol_map.items():
                if excel_sym in allowed_stocks_normalized:
                    mapped_additions.add(db_sym)

            allowed_stocks_normalized.update(mapped_additions)
            # ---------------------------

            dashboard_df["stock_upper"] = dashboard_df["stock"].str.strip().str.upper()

            # Debug: Log stocks that don't match
            # all_stocks = set(dashboard_df["stock_upper"].tolist())

            # missing_from_excel = all_stocks - allowed_stocks_normalized
            # missing_from_db = allowed_stocks_normalized - all_stocks (We refine this below)

            # Identify valid misses (exclude mapped ones)
            # filtered_out_stocks = set()
            # for s in allowed_stocks_normalized:
            #     if s in all_stocks:
            #         continue
            #     # If s is "M&M" and we mapped it to "M_M", check if "M_M" is in all_stocks
            #     if s in symbol_map and symbol_map[s] in all_stocks:
            #         continue
            #     # If s IS the "M_M" (mapped value) and works, ignore.
            #     # But 'allowed_stocks_normalized' has mixed original + mapped now (from .update)
            #     filtered_out_stocks.add(s)

            # if filtered_out_stocks:
            # Filter out pure mapped DB keys (like "M_M") from the error message if they are just artifacts
            # But 'allowed' has them.
            # Simplification: Only report if NEITHER s NOR its map is found.
            # final_missing = []
            # for s in sorted(list(filtered_out_stocks)):
            # Don't report "M_M" if "M&M" is the user-facing one (or vice versa? No, report whatever is broken)
            # Actually, if "M_M" is in allowed (added by us) and not in DB, it's missing.
            # If "M&M" is in allowed and not in DB, it's missing (but handled above).
            # Just print.
            # final_missing.append(s)

            # if final_missing:
            #     print(f"[INFO] The following {len(final_missing)} stocks from Excel are MISSING in Market Data:\n{final_missing}")

            # Apply filter
            dashboard_df = dashboard_df[dashboard_df["stock_upper"].isin(allowed_stocks_normalized)]
            dashboard_df = dashboard_df.drop(columns=["stock_upper"])

        # Standardize column naming
        rename_map = {
            "stock": "stock",
            "call_delta_pos_strike": "call_delta_pos_strike",
            "call_delta_pos_pct": "call_delta_pos_pct",
            "call_delta_neg_strike": "call_delta_neg_strike",
            "call_delta_neg_pct": "call_delta_neg_pct",
            "call_vega_pos_strike": "call_vega_pos_strike",
            "call_vega_pos_pct": "call_vega_pos_pct",
            "call_vega_neg_strike": "call_vega_neg_strike",
            "call_vega_neg_pct": "call_vega_neg_pct",
            "call_total_tradval": "call_total_tradval",
            "call_total_money": "call_total_money",
            "closing_price": "closing_price",
            "rsi": "rsi",
            "put_delta_pos_strike": "put_delta_pos_strike",
            "put_delta_pos_pct": "put_delta_pos_pct",
            "put_delta_neg_strike": "put_delta_neg_strike",
            "put_delta_neg_pct": "put_delta_neg_pct",
            "put_vega_pos_strike": "put_vega_pos_strike",
            "put_vega_pos_pct": "put_vega_pos_pct",
            "put_vega_neg_strike": "put_vega_neg_strike",
            "put_vega_neg_pct": "put_vega_neg_pct",
            "put_total_tradval": "put_total_tradval",
            "put_total_money": "put_total_money",
        }

        dashboard_df.rename(columns=rename_map, inplace=True)
        return dashboard_df.to_dict(orient="records")

    except Exception as e:
        print(f"[ERROR] get_dashboard_data(): {e}")
        return []
