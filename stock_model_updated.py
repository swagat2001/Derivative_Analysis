from sqlalchemy import create_engine, text
import pandas as pd
import json
from urllib.parse import quote_plus

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


def get_stock_detail_data(ticker, selected_date):
    """
    Fetch detailed options data for a specific ticker and date.
    Returns list of dictionaries with option contract details.
    """
    try:
        # Construct table name based on ticker
        table_name = f"TBL_{ticker}_DERIVED"
        
        # Query to get all option contract details for the selected date
        query = text(f"""
            SELECT 
                BizDt,
                FininstrmActlXpryDt,
                StrkPric,
                OptnTp,
                OpnIntrst,
                TtlTradgVol,
                ClsPric,
                UndrlygPric
            FROM {table_name}
            WHERE BizDt = :date
              AND OptnTp IN ('CE', 'PE')
              AND StrkPric IS NOT NULL
            ORDER BY FininstrmActlXpryDt, StrkPric, OptnTp
        """)
        
        # Execute query with SQLAlchemy 2.x compatible syntax
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"date": selected_date})
        
        # Convert DataFrame to list of dictionaries
        if not df.empty:
            # Convert date columns to string format
            if 'BizDt' in df.columns:
                df['BizDt'] = pd.to_datetime(df['BizDt']).dt.strftime('%Y-%m-%d')
            if 'FininstrmActlXpryDt' in df.columns:
                df['FininstrmActlXpryDt'] = pd.to_datetime(df['FininstrmActlXpryDt']).dt.strftime('%Y-%m-%d')
            
            # Convert to list of dictionaries
            return df.to_dict('records')
        
        return []
        
    except Exception as e:
        print(f"Error fetching stock detail data for {ticker} on {selected_date}: {e}")
        return []
