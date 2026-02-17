"""
Quick script to clear the signal scanner cache
Run this before rebuilding with new signal logic
"""
import sys
import os
sys.path.append(os.path.dirname(__file__))

from Analysis_Tools.app.models.db_config import engine
from sqlalchemy import text

print("=" * 70)
print("CLEARING SIGNAL SCANNER CACHE")
print("=" * 70)

try:
    with engine.begin() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM daily_signal_scanner"))
        count = result.fetchone()[0]
        print(f"\nCurrent rows in cache: {count:,}")

        if count > 0:
            print("\nDeleting all rows...")
            conn.execute(text("DELETE FROM daily_signal_scanner"))
            print("✅ Cache cleared successfully!")
        else:
            print("\n✅ Cache is already empty")

    print("\n" + "=" * 70)
    print("NEXT STEP:")
    print("=" * 70)
    print("\nRun the cache builder to populate with NEW signal logic:")
    print("  cd Database\\FO")
    print("  python signal_scanner_cache.py")
    print()

except Exception as e:
    print(f"\n❌ Error: {e}")
    print("\nAlternative: Drop and recreate table")
    print("The cache builder will auto-recreate it on next run.")
