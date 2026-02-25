"""
REBUILD HEATMAP SECTORS
Fixes sector classification in daily_market_heatmap table
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from app.models.db_config import engine_cash
from app.models.insights_model import get_sector, _load_sector_master

def rebuild_sectors():
    """Update all sectors in daily_market_heatmap table."""

    print("="*60)
    print("REBUILDING HEATMAP SECTORS")
    print("="*60)

    # Load sector master data
    print("\n[1/3] Loading sector mappings...")
    _load_sector_master()

    # Get all unique symbols from heatmap table
    print("\n[2/3] Fetching stocks from heatmap table...")
    try:
        query = text("""
            SELECT DISTINCT symbol
            FROM daily_market_heatmap
            ORDER BY symbol
        """)

        with engine_cash.connect() as conn:
            result = conn.execute(query)
            symbols = [row[0] for row in result]

        print(f"   Found {len(symbols)} unique stocks")

    except Exception as e:
        print(f"   ERROR: {e}")
        return False

    # Update sectors for each stock
    print("\n[3/3] Updating sectors...")
    updated = 0
    skipped = 0

    try:
        with engine_cash.begin() as conn:
            for i, symbol in enumerate(symbols, 1):
                # Get proper sector
                sector = get_sector(symbol)

                # Update in database
                update_query = text("""
                    UPDATE daily_market_heatmap
                    SET sector = :sector
                    WHERE symbol = :symbol
                """)

                result = conn.execute(update_query, {
                    'sector': sector,
                    'symbol': symbol
                })

                if result.rowcount > 0:
                    updated += result.rowcount
                    if sector != "Others":
                        print(f"   [{i}/{len(symbols)}] {symbol:15s} -> {sector}")
                else:
                    skipped += 1

                # Progress indicator every 50 stocks
                if i % 50 == 0:
                    print(f"   Progress: {i}/{len(symbols)} stocks processed...")

        print(f"\n Updated {updated} records")
        print(f" Skipped {skipped} records")

        # Show sector distribution
        print("\n" + "="*60)
        print("SECTOR DISTRIBUTION")
        print("="*60)

        query = text("""
            SELECT sector, COUNT(*) as count
            FROM daily_market_heatmap
            WHERE date = (SELECT MAX(date) FROM daily_market_heatmap)
            GROUP BY sector
            ORDER BY count DESC
        """)

        with engine_cash.connect() as conn:
            result = conn.execute(query)
            for row in result:
                print(f"   {row[0]:30s} : {row[1]:4d} stocks")

        print("\n Sector rebuild complete!")
        return True

    except Exception as e:
        print(f"\n ERROR during update: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = rebuild_sectors()
    sys.exit(0 if success else 1)
