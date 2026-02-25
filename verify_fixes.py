
import sys
import os

# Add project root to sys.path
PROJECT_ROOT = r"C:\Users\Admin\Desktop\Derivative_Analysis"
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
if os.path.join(PROJECT_ROOT, "Analysis_Tools") not in sys.path:
    sys.path.append(os.path.join(PROJECT_ROOT, "Analysis_Tools"))

from Analysis_Tools.app.models.index_model import get_index_stocks
from Analysis_Tools.app.models.stock_model import get_stock_detail_data, _clean_ticker

print("--- Testing Index Model ---")
nifty50 = get_index_stocks("nifty50")
print(f"Nifty 50 Count: {len(nifty50) if nifty50 else 'None'}")
if nifty50:
    print(f"First 5: {nifty50[:5]}")

print("\n--- Testing RELI Alias ---")
clean_reli = _clean_ticker("RELI")
print(f"Cleaned 'RELI': {clean_reli}")

print("\n--- Testing Non-existent Table ---")
# This should NOT crash with a 500/SQL error locally
try:
    fake_data = get_stock_detail_data("NONEXISTENT", "2026-02-24")
    print(f"Fake Stock Data Count: {len(fake_data)}")
except Exception as e:
    print(f"ERROR: Fake stock crashed: {e}")
