
import os
import sys
import pandas as pd
from datetime import datetime
from sqlalchemy import text
import logging

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from Analysis_Tools.app.models.db_config import engine_cash as engine
from Analysis_Tools.app.utils.logger import logger

# Configuration
CASH_DATA_PATH = os.getenv("CASH_DATA_PATH", "C:/NSE_EOD_CASH")

def create_table():
    """Create market_breadth_eod table if it doesn't exist"""
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS market_breadth_eod (
                    date DATE PRIMARY KEY,
                    advances INTEGER NOT NULL,
                    declines INTEGER NOT NULL,
                    unchanged INTEGER NOT NULL,
                    total INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            conn.commit()
            logger.info("Table 'market_breadth_eod' verified/created")
    except Exception as e:
        logger.error(f"Error creating table: {e}")

def get_breadth_from_csv(filepath):
    """Calculate advances/declines from a bhavcopy CSV"""
    try:
        df = pd.read_csv(filepath)
        df.columns = df.columns.str.strip()

        # Filter for EQ series primarily
        if 'SERIES' in df.columns:
            df = df[df['SERIES'].str.strip().isin(['EQ', 'BE', 'SM', 'ST'])]

        if 'CLOSE_PRICE' not in df.columns or 'PREV_CLOSE' not in df.columns:
            return None

        advances = len(df[df['CLOSE_PRICE'] > df['PREV_CLOSE']])
        declines = len(df[df['CLOSE_PRICE'] < df['PREV_CLOSE']])
        unchanged = len(df[df['CLOSE_PRICE'] == df['PREV_CLOSE']])

        return {
            "advances": advances,
            "declines": declines,
            "unchanged": unchanged,
            "total": advances + declines + unchanged
        }
    except Exception as e:
        logger.error(f"Error processing CSV {filepath}: {e}")
        return None

def run_market_breadth_eod():
    """Main function to refresh EOD market breadth history"""
    create_table()

    # 1. Process CSV files from CASH_DATA_PATH
    if os.path.exists(CASH_DATA_PATH):
        files = [f for f in os.listdir(CASH_DATA_PATH) if f.startswith("sec_bhavdata_full_") and f.endswith(".csv")]
        files.sort()

        for file_name in files:
            try:
                # Extract date from filename: sec_bhavdata_full_DDMMYYYY.csv
                date_str = file_name.replace("sec_bhavdata_full_", "").replace(".csv", "")
                dt = datetime.strptime(date_str, "%d%m%Y").date()

                # Scrape data
                filepath = os.path.join(CASH_DATA_PATH, file_name)
                stats = get_breadth_from_csv(filepath)

                if stats:
                    with engine.connect() as conn:
                        conn.execute(text("""
                            INSERT INTO market_breadth_eod (date, advances, declines, unchanged, total)
                            VALUES (:date, :advances, :declines, :unchanged, :total)
                            ON CONFLICT (date) DO UPDATE SET
                                advances = EXCLUDED.advances,
                                declines = EXCLUDED.declines,
                                unchanged = EXCLUDED.unchanged,
                                total = EXCLUDED.total
                        """), {
                            "date": dt,
                            "advances": stats["advances"],
                            "declines": stats["declines"],
                            "unchanged": stats["unchanged"],
                            "total": stats["total"]
                        })
                        conn.commit()
            except Exception as e:
                logger.error(f"Error processing {file_name}: {e}")

    # 2. Sync from live market_breadth table (for today/latest if CSV not yet available)
    try:
        with engine.connect() as conn:
            # Get latest snapshot for each date from market_breadth table
            query = text("""
                INSERT INTO market_breadth_eod (date, advances, declines, unchanged, total)
                SELECT DISTINCT ON (date) date, advances, declines, unchanged, (advances + declines + unchanged) as total
                FROM market_breadth
                ORDER BY date, timestamp DESC
                ON CONFLICT (date) DO UPDATE SET
                    advances = EXCLUDED.advances,
                    declines = EXCLUDED.declines,
                    unchanged = EXCLUDED.unchanged,
                    total = EXCLUDED.total
            """)
            conn.execute(query)
            conn.commit()
            logger.info("Synced with market_breadth live table")
    except Exception as e:
        logger.warning(f"Failed to sync from market_breadth table: {e}")

def get_breadth_history(days=30):
    """Fetch formatted history for the frontend chart"""
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT date, advances, declines, unchanged, total
                FROM market_breadth_eod
                ORDER BY date DESC
                LIMIT :days
            """)
            rows = conn.execute(query, {"days": days}).fetchall()

            # Convert to list of dicts, and reverse to get chronological order for chart
            history = []
            for r in reversed(rows):
                history.append({
                    "date": r[0].strftime("%Y-%m-%d") if hasattr(r[0], 'strftime') else str(r[0]),
                    "advances": int(r[1]),
                    "declines": int(r[2]),
                    "unchanged": int(r[3]),
                    "total": int(r[4])
                })
            return history
    except Exception as e:
        logger.error(f"Error fetching breadth history: {e}")
        return []

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_market_breadth_eod()
    print("Market Breadth EOD update complete.")
    history = get_breadth_history(5)
    print("Latest 5 days:", history)
