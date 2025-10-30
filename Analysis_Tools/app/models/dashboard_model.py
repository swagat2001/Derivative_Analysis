# =============================================================
#  DASHBOARD MODEL MODULE (Flask MVC - Model Layer)
#  Purpose: Loads pre-calculated dashboard data for TOTAL / OTM / ITM views
# =============================================================

from sqlalchemy import create_engine, text
import pandas as pd
import json
from urllib.parse import quote_plus

# =============================================================
# DATABASE CONNECTION
# =============================================================

db_user = 'postgres'
db_password = 'Gallop@3104'
db_host = 'localhost'
db_port = '5432'
db_name = 'BhavCopy_Database'

db_password_enc = quote_plus(db_password)
engine = create_engine(
    f'postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}'
)

# =============================================================
# 1️⃣ AVAILABLE DATES
# =============================================================

def get_available_dates():
    """Fetch distinct biz_date values for dropdown."""
    try:
        query = text("""
            SELECT DISTINCT biz_date::text AS date
            FROM options_dashboard_cache
            ORDER BY date DESC;
        """)
        df = pd.read_sql(query, con=engine)
        return df['date'].tolist()
    except Exception as e:
        print(f"[ERROR] get_available_dates(): {e}")
        return []

# =============================================================
# 2️⃣ DASHBOARD DATA (TOTAL / OTM / ITM)
# =============================================================

def get_dashboard_data(selected_date, mtype="TOTAL"):
    """
    Loads pre-calculated dashboard data from options_dashboard_cache
    for given date and moneyness_type.
    Each record's data_json column contains serialized dashboard rows.
    """
    try:
        query = text("""
            SELECT data_json
            FROM options_dashboard_cache
            WHERE biz_date = :biz_date
              AND moneyness_type = :mtype
            LIMIT 1;
        """)
        df = pd.read_sql(query, con=engine, params={
            "biz_date": selected_date,
            "mtype": mtype
        })

        if df.empty:
            print(f"[INFO] No dashboard data found for {selected_date}, {mtype}")
            return []

        # Parse JSON from the table
        raw_json = df.iloc[0]["data_json"]
        parsed = json.loads(raw_json)

        # Data should be a list of dicts
        if isinstance(parsed, dict):
            # If nested dict, extract table key
            parsed = parsed.get("data", [])
        elif not isinstance(parsed, list):
            return []

        # Convert to DataFrame for any column-level cleanup if needed
        dashboard_df = pd.DataFrame(parsed)

        # (Optional) Standardize column naming for dashboard_table.html
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
            "put_total_money": "put_total_money"
        }

        dashboard_df.rename(columns=rename_map, inplace=True)
        return dashboard_df.to_dict(orient="records")

    except Exception as e:
        print(f"[ERROR] get_dashboard_data(): {e}")
        return []
