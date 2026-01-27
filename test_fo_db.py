# Check segment values and column names in F&O database
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text

db_user = "postgres"
db_password = quote_plus("Gallop@3104")
db_host = "localhost"
db_port = "5432"
db_name = "BhavCopy_Database"

engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

try:
    with engine.connect() as conn:
        # Get columns from TBL_RELIANCE
        result = conn.execute(
            text(
                """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'TBL_RELIANCE'
            ORDER BY ordinal_position
        """
            )
        )
        cols = [row[0] for row in result]
        print(f"Columns in TBL_RELIANCE: {cols}")

        # Check for Sgmt column
        has_sgmt = "Sgmt" in cols
        print(f"Has 'Sgmt' column: {has_sgmt}")

        # Get distinct segment values (if column exists)
        if has_sgmt:
            result = conn.execute(text("""SELECT DISTINCT "Sgmt" FROM public."TBL_RELIANCE" LIMIT 10"""))
            sgmt_values = [row[0] for row in result]
            print(f"Segment values: {sgmt_values}")

        # Get sample row to see data structure
        result = conn.execute(text('SELECT * FROM public."TBL_RELIANCE" LIMIT 1'))
        row = result.fetchone()
        if row:
            keys = result.keys()
            print(f"\nSample row from TBL_RELIANCE:")
            for k, v in zip(keys, row):
                print(f"  {k}: {v}")

except Exception as e:
    print(f"Error: {e}")
