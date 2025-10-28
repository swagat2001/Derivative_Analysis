from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import json

db_user = 'postgres'
db_password = 'Gallop@3104'
db_host = 'localhost'
db_port = '5432'
db_name = 'BhavCopy_Database'

db_password_enc = quote_plus(db_password)
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}')

with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT data_json FROM options_dashboard_cache
        WHERE moneyness_type='TOTAL'
        ORDER BY biz_date DESC
        LIMIT 1
    """)).fetchone()

data = json.loads(result[0])
print(json.dumps(data[:3], indent=2))  # show first 3 records
