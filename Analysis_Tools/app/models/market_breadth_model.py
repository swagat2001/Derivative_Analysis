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
    Returns: dict with advances, declines, unchanged
    """
    try:
        with engine_cash.connect() as conn:
            query = text("""
                SELECT date, advances, declines, unchanged, timestamp
                FROM market_breadth
                WHERE date = CURRENT_DATE
                ORDER BY timestamp DESC
                LIMIT 1
            """)

            result = conn.execute(query).fetchone()

        if result:
            return {
                "date": result[0].strftime("%Y-%m-%d"),
                "advances": result[1],
                "declines": result[2],
                "unchanged": result[3],
                "timestamp": result[4].strftime("%Y-%m-%d %H:%M:%S"),
                "total": result[1] + result[2] + result[3]
            }
        else:
            # Return default values if no data
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
        return {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "advances": 0,
            "declines": 0,
            "unchanged": 0,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total": 0,
            "error": str(e)
        }
