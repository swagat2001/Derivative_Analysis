"""
Market Breadth Model
Database operations for market breadth data
"""

from datetime import datetime, timedelta
from .db_config import engine_cash
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)


def get_latest_market_breadth():
    """
    Get the latest market breadth data from CashStocks_Database
    Looks for most recent date available (not just CURRENT_DATE)
    Returns: dict with advances, declines, unchanged
    """
    try:
        with engine_cash.connect() as conn:
            # First, try to get the most recent date available
            query = text("""
                SELECT date, advances, declines, unchanged, timestamp
                FROM market_breadth
                ORDER BY date DESC, timestamp DESC
                LIMIT 1
            """)

            result = conn.execute(query).fetchone()

        if result and result[1] is not None:
            return {
                "date": result[0].strftime("%Y-%m-%d") if hasattr(result[0], 'strftime') else str(result[0]),
                "advances": int(result[1]) if result[1] else 0,
                "declines": int(result[2]) if result[2] else 0,
                "unchanged": int(result[3]) if result[3] else 0,
                "timestamp": result[4].strftime("%Y-%m-%d %H:%M:%S") if result[4] else "",
                "total": int(result[1] or 0) + int(result[2] or 0) + int(result[3] or 0)
            }
        else:
            # No data in table - log warning
            logger.warning("No data found in market_breadth table")
            return {
                "date": datetime.now().strftime("%Y-%m-%d"),
                "advances": 0,
                "declines": 0,
                "unchanged": 0,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "total": 0
            }

    except Exception as e:
        logger.error(f"Error fetching market breadth: {str(e)}")
        # Return zeros on error
        return {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "advances": 0,
            "declines": 0,
            "unchanged": 0,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total": 0,
            "error": str(e)
        }
def save_market_breadth(data):
    """
    Save market breadth data to CashStocks_Database
    Args:
        data (dict): dict with advances, declines, unchanged, date, timestamp
    """
    try:
        with engine_cash.connect() as conn:
            # Insert or update data
            query = text("""
                INSERT INTO market_breadth (date, advances, declines, unchanged, timestamp)
                VALUES (:date, :advances, :declines, :unchanged, :timestamp)
                ON CONFLICT (date, timestamp)
                DO UPDATE SET
                    advances = EXCLUDED.advances,
                    declines = EXCLUDED.declines,
                    unchanged = EXCLUDED.unchanged
            """)

            conn.execute(query, {
                'date': data.get('date', datetime.now().strftime("%Y-%m-%d")),
                'advances': int(data.get('advances', 0)),
                'declines': int(data.get('declines', 0)),
                'unchanged': int(data.get('unchanged', 0)),
                'timestamp': data.get('timestamp', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            })

            conn.commit()
            logger.info("✅ Market breadth data saved to database")
            return True

    except Exception as e:
        logger.error(f"Error saving market breadth: {str(e)}")
        return False
