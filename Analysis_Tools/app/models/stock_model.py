from sqlalchemy import create_engine, text
import pandas as pd
import json
from urllib.parse import quote_plus
from datetime import datetime, timedelta

# Database config
db_user = 'postgres'
db_password = 'Gallop@3104'
db_host = 'localhost'
db_port = '5432'
db_name = 'BhavCopy_Database'

db_password_enc = quote_plus(db_password)
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}')


def get_available_dates():
    """Fetch distinct biz_date values from cache."""
    query = text("SELECT DISTINCT biz_date FROM options_dashboard_cache ORDER BY biz_date DESC;")
    df = pd.read_sql(query, engine)
    return [d.strftime('%Y-%m-%d') for d in df['biz_date']]


def get_dashboard_data(selected_date, moneyness_type="TOTAL"):
    """Fetch pre-calculated dashboard data from cache (fast, SQLA 2.x safe)."""
    query = text("""
        SELECT data_json
        FROM options_dashboard_cache
        WHERE biz_date = :date
          AND moneyness_type = :mtype
        LIMIT 1
    """)

    with engine.connect() as conn:
        result = conn.execute(query, {"date": selected_date, "mtype": moneyness_type})
        row = result.fetchone()

    if row:
        return json.loads(row[0])
    return []


def get_previous_trading_date(current_date, table_name):
    """Get the previous trading date before the given date."""
    try:
        query = text(f"""
            SELECT MAX("BizDt") as prev_date
            FROM {table_name}
            WHERE "BizDt" < :date
        """)

        with engine.connect() as conn:
            result = conn.execute(query, {"date": current_date})
            row = result.fetchone()

        if row and row[0]:
            return row[0].strftime('%Y-%m-%d') if hasattr(row[0], 'strftime') else str(row[0])
        return None
    except Exception as e:
        print(f"Error getting previous date: {e}")
        return None


def get_stock_detail_data(ticker, selected_date):
    """
    Fetch detailed options data for a specific ticker and date with Previous Day Comparison.
    Works with existing database columns only (no Greeks required).
    Returns list of dictionaries with option contract details.
    """
    try:
        # Construct table name with proper quoting for PostgreSQL
        table_name = f'"TBL_{ticker}_DERIVED"'

        # Get previous trading date
        prev_date = get_previous_trading_date(selected_date, table_name)
        print(f"Current date: {selected_date}, Previous date: {prev_date}")

        # Query current day data - using ONLY columns that exist in your database
        query = text(f"""
            SELECT 
                "BizDt",
                "FininstrmActlXpryDt",
                "StrkPric",
                "OptnTp",
                "OpnIntrst",
                "ChngInOpnIntrst",
                "TtlTradgVol",
                "ClsPric",
                "UndrlygPric"
            FROM {table_name}
            WHERE "BizDt" = :date
              AND "OptnTp" IN ('CE', 'PE')
              AND "StrkPric" IS NOT NULL
            ORDER BY "FininstrmActlXpryDt", "StrkPric", "OptnTp"
        """)

        # Execute query
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"date": selected_date})

        # Get previous day data for comparison if available
        prev_data_dict = {}
        if prev_date:
            prev_query = text(f"""
                SELECT 
                    "StrkPric",
                    "OptnTp",
                    "FininstrmActlXpryDt",
                    "OpnIntrst" as prev_oi,
                    "ClsPric" as prev_price
                FROM {table_name}
                WHERE "BizDt" = :prev_date
                  AND "OptnTp" IN ('CE', 'PE')
                  AND "StrkPric" IS NOT NULL
            """)

            with engine.connect() as conn:
                prev_df = pd.read_sql(prev_query, conn, params={"prev_date": prev_date})

            # Create dictionary for quick lookup
            for _, row in prev_df.iterrows():
                key = (row['StrkPric'], row['OptnTp'], row['FininstrmActlXpryDt'])
                prev_data_dict[key] = {
                    'prev_oi': row['prev_oi'],
                    'prev_price': row['prev_price']
                }

        # Convert DataFrame to list of dictionaries with enhancements
        if not df.empty:
            result = []

            for _, row in df.iterrows():
                # Convert date columns
                biz_dt = pd.to_datetime(row['BizDt']).strftime('%Y-%m-%d') if pd.notna(row['BizDt']) else None
                expiry_dt = pd.to_datetime(row['FininstrmActlXpryDt']).strftime('%Y-%m-%d') if pd.notna(row['FininstrmActlXpryDt']) else None

                # Get previous day data
                key = (row['StrkPric'], row['OptnTp'], row['FininstrmActlXpryDt'])
                prev_data = prev_data_dict.get(key, {})

                # Calculate OI change percentage
                current_oi = float(row['OpnIntrst']) if pd.notna(row['OpnIntrst']) else 0
                prev_oi = prev_data.get('prev_oi', 0)
                oi_chg_percent = 0
                if prev_oi and prev_oi > 0:
                    oi_chg_percent = ((current_oi - prev_oi) / prev_oi) * 100

                # Calculate price change percentage
                current_price = float(row['ClsPric']) if pd.notna(row['ClsPric']) else 0
                prev_price = prev_data.get('prev_price', 0)
                price_chg_percent = 0
                if prev_price and prev_price > 0:
                    price_chg_percent = ((current_price - prev_price) / prev_price) * 100

                row_dict = {
                    'BizDt': biz_dt,
                    'FininstrmActlXpryDt': expiry_dt,
                    'StrkPric': float(row['StrkPric']) if pd.notna(row['StrkPric']) else 0,
                    'OptnTp': row['OptnTp'],
                    'OpnIntrst': current_oi,
                    'ChngInOpnIntrst': float(row['ChngInOpnIntrst']) if pd.notna(row['ChngInOpnIntrst']) else 0,
                    'TtlTradgVol': float(row['TtlTradgVol']) if pd.notna(row['TtlTradgVol']) else 0,
                    'ClsPric': current_price,
                    'UndrlygPric': float(row['UndrlygPric']) if pd.notna(row['UndrlygPric']) else 0,
                    # Greeks - set to 0 since columns don't exist
                    'IV': 0,
                    'Delta': 0,
                    'Vega': 0,
                    'Gamma': 0,
                    'Theta': 0,
                    # Previous day comparison
                    'PrevOI': prev_oi,
                    'OI_Chg_%': round(oi_chg_percent, 2),
                    'PrevPrice': prev_price,
                    'Price_Chg_%': round(price_chg_percent, 2)
                }

                result.append(row_dict)

            return result

        return []

    except Exception as e:
        print(f"Error fetching stock detail data for {ticker} on {selected_date}: {e}")
        return []