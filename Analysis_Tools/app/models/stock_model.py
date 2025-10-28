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
        result = conn.execute(query, {"date": selected_date, "mtype": moneyness_type}).fetchone()

    if not result or not result[0]:
        return []

    try:
        data = json.loads(result[0])
        return data
    except Exception as e:
        print(f"❌ JSON decode error: {e}")
        return []
