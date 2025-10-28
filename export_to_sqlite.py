"""
Export PostgreSQL Database to SQLite
"""
from sqlalchemy import create_engine, inspect, text, MetaData
from urllib.parse import quote_plus
import sqlite3

# PostgreSQL Configuration (from your existing setup)
pg_user = 'postgres'
pg_password = 'Gallop@3104'
pg_host = 'localhost'
pg_port = '5432'
pg_name = 'BhavCopy_Database'

# SQLite Configuration
sqlite_file = 'BhavCopy_Database.db'

# Create engines
pg_password_enc = quote_plus(pg_password)
pg_engine = create_engine(f'postgresql+psycopg2://{pg_user}:{pg_password_enc}@{pg_host}:{pg_port}/{pg_name}')
sqlite_engine = create_engine(f'sqlite:///{sqlite_file}')

def export_database():
    print("Starting PostgreSQL to SQLite export...\n")
    
    # Get all table names
    inspector = inspect(pg_engine)
    tables = inspector.get_table_names()
    
    print(f"Found {len(tables)} tables to export\n")
    
    for i, table_name in enumerate(tables, 1):
        try:
            print(f"[{i}/{len(tables)}] Exporting {table_name}...", end=" ")
            
            # Read from PostgreSQL
            query = f'SELECT * FROM "{table_name}"'
            df = pd.read_sql(text(query), pg_engine)
            
            # Write to SQLite
            df.to_sql(table_name, sqlite_engine, if_exists='replace', index=False)
            
            print(f"✅ ({len(df)} rows)")
            
        except Exception as e:
            print(f"❌ Error: {str(e)[:50]}")
    
    print(f"\n✅ Export complete! Database saved as: {sqlite_file}")

if __name__ == '__main__':
    import pandas as pd
    export_database()
