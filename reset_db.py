# """Drop the daily_signal_scanner table for a clean reset"""
# from sqlalchemy import create_engine, text
# from urllib.parse import quote_plus

# p = quote_plus('Gallop@3104')
# e = create_engine(f'postgresql+psycopg2://postgres:{p}@localhost:5432/BhavCopy_Database')

# print("Dropping daily_signal_scanner table...")
# with e.connect() as conn:
#     conn.execute(text("DROP TABLE IF EXISTS daily_signal_scanner"))
#     conn.commit()
#     print("✅ Table dropped successfully.")
