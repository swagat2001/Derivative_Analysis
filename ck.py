import os; from dotenv import load_dotenv; import psycopg2
load_dotenv()
conn = psycopg2.connect(dbname=os.getenv("DB_NAME_CASH","CashStocks_Database"), user=os.getenv("DB_USER","postgres"), password=os.getenv("DB_PASSWORD",""), host=os.getenv("DB_HOST","localhost"), port=os.getenv("DB_PORT","5432"))
cur = conn.cursor()
cur.execute("SELECT COUNT(*), MAX(updated_at), MIN(symbol), MAX(symbol) FROM stock_fundamentals")
r = cur.fetchone()
print(f"Rows: {r[0]} | Last saved: {r[1]} | Range: {r[2]} → {r[3]}")
cur.close(); conn.close()
