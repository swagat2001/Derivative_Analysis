# =============================================================
#  SECTOR UTILITY MODULE
#  Purpose: Centralized logic for sector and industry mapping
#  Location: Analysis_Tools/utils/sector_utils.py
# =============================================================

import os
import json
import pandas as pd
from typing import List, Dict

# =============================================================
# CONFIGURATION
# =============================================================

# Prioritized list of paths for the sector master CSV
# The user explicitly prefers: C:\Users\Admin\Desktop\Derivative_Analysis\nse_sector_master.csv
SECTOR_MASTER_PATHS = [
    r"C:\Users\Admin\Desktop\Derivative_Analysis\nse_sector_master.csv",
    os.getenv("SECTOR_MASTER_PATH"),
    "C:/NSE_EOD_CASH_WITH_INDICATORS/nse_sector_master.csv",
    "C:/Users/Admin/Desktop/Derivative_Analysis/SMA/nse_sector_master.csv",
    os.path.join(os.getcwd(), "nse_sector_master.csv"),
    # Add relative path from this file as final fallback (assuming it might be moved here)
    os.path.join(os.path.dirname(__file__), "..", "..", "nse_sector_master.csv")
]

# Internal Cache
_sector_cache: Dict[str, str] = {}
_sector_cache_loaded = False
DEFAULT_SECTOR = "Others"

# JSON Fallback Path (Data_scraper)
JSON_FALLBACK_PATH = r"C:\Users\Admin\Desktop\Derivative_Analysis\Data_scraper\index_constituents.json"


# =============================================================
# INDUSTRY CLASSIFICATION LOGIC
# =============================================================

def classify_industry(industry: str) -> str:
    """Map NSE_INDUSTRY string into broad sectors."""
    if not isinstance(industry, str) or not industry.strip():
        return "Diversified / Others"

    s = industry.strip().lower()

    # Priority Mappings
    if any(x in s for x in ["bank", "banking"]): return "Financials"
    if any(x in s for x in ["nbfc", "finance", "housing finance", "wealth", "asset management", "insurance", "broker"]): return "Financials"

    if any(x in s for x in ["it", "software", "computers", "technology", "internet"]): return "Information Technology"

    if any(x in s for x in ["pharma", "drug", "healthcare", "hospital", "diagnostic", "biotech"]): return "Healthcare & Pharma"

    if any(x in s for x in ["auto", "vehicle", "tyre", "ancillaries"]): return "Auto & Ancillaries"

    if any(x in s for x in ["fmcg", "consumer goods", "food", "sugar", "tea", "coffee", "dairy", "tobacco", "personal care"]): return "FMCG"

    if any(x in s for x in ["metal", "mining", "steel", "iron", "aluminum", "zinc", "copper"]): return "Metals & Mining"

    if any(x in s for x in ["power", "energy", "electricity", "renewable", "solar", "wind"]): return "Energy & Power"
    if any(x in s for x in ["oil", "gas", "petroleum", "refinery", "petrochemical"]): return "Oil & Gas"

    if any(x in s for x in ["telecom", "cellular", "communication"]): return "Telecom"

    if any(x in s for x in ["construction", "infrastructure", "realty", "real estate", "cement", "engineering", "housing"]): return "Infrastructure & Construction"

    if any(x in s for x in ["chemical", "fertilizer", "pesticide", "agro"]): return "Chemicals"

    if any(x in s for x in ["textile", "apparel", "garment", "fabric"]): return "Textiles"

    if any(x in s for x in ["consumer durables", "electronics", "appliances"]): return "Consumer Durables"

    if any(x in s for x in ["transport", "logistics", "shipping", "airline", "port"]): return "Logistics & Transport"

    if any(x in s for x in ["media", "entertainment", "broadcasting"]): return "Media & Entertainment"

    if any(x in s for x in ["hotel", "resort", "restaurant", "hospitality", "tourism"]): return "Hospitality"

    if any(x in s for x in ["retail", "trading", "department stores"]): return "Retail"

    if any(x in s for x in ["defence", "aerospace"]): return "Defence"

    return "Diversified / Others"


# =============================================================
# CACHE LOADING LOGIC
# =============================================================

def load_sector_master(force_reload=False):
    """Load sector data from CSV or fallback to JSON."""
    global _sector_cache, _sector_cache_loaded

    if _sector_cache_loaded and not force_reload:
        return

    # 1. Try CSV Paths
    loaded_from_csv = False
    for path in SECTOR_MASTER_PATHS:
        if path and os.path.exists(path):
            try:
                # Read CSV
                df = pd.read_csv(path, encoding="utf-8-sig")
                df.columns = df.columns.str.strip().upper()

                # Identify Columns
                symbol_col = next((c for c in df.columns if c in ["SYMBOL", "TICKER"]), df.columns[0])

                # Check for direct Sector column or Industry column
                # Priority: SECTOR > NSE_INDUSTRY > INDUSTRY
                sector_col = next((c for c in df.columns if "SECTOR" in c), None)
                if not sector_col:
                    sector_col = next((c for c in df.columns if "INDUSTRY" in c), None)

                if symbol_col and sector_col:
                    count = 0
                    for _, row in df.iterrows():
                        sym = str(row[symbol_col]).strip().upper()
                        raw_val = str(row[sector_col]).strip()

                        if not sym or not raw_val:
                            continue

                        if "INDUSTRY" in sector_col:
                            final_sector = classify_industry(raw_val)
                        else:
                            # It's a Sector column, trust it but maybe title case it
                            final_sector = raw_val
                            # Optional: map generic names to our standard ones?
                            # For now, let's blindly trust the SECTOR column if it exists.

                        _sector_cache[sym] = final_sector
                        count += 1

                    print(f"[INFO] Loaded sector mapping for {count} stocks from {path}")
                    loaded_from_csv = True
                    break # Stop after first successful load

            except Exception as e:
                print(f"[WARNING] Failed to load sector master from {path}: {e}")

    if loaded_from_csv:
        _sector_cache_loaded = True
        return

    # 2. Fallback to JSON
    print("[WARN] No valid sector master CSV found. Attempting index_constituents.json...")
    if os.path.exists(JSON_FALLBACK_PATH):
        try:
            with open(JSON_FALLBACK_PATH, "r") as f:
                data = json.load(f)

            indices = data.get("indices", {})

            # Mapping from JSON index keys to Display Sector
            # Note: This list is from insights_model.py
            INDEX_TO_SECTOR = {
                "niftyit": "Information Technology",
                "niftybank": "Financials",
                "niftypsubank": "Financials",
                "niftyfinancial": "Financials",
                "niftypharma": "Healthcare & Pharma",
                "niftyauto": "Auto & Ancillaries",
                "niftymetal": "Metals & Mining",
                "niftyfmcg": "FMCG",
                "niftyenergy": "Energy & Power",
                "niftyrealty": "Infrastructure & Construction", # Broadened
                "niftymedia": "Media & Entertainment",
                "niftyinfra": "Infrastructure & Construction"
            }

            count = 0
            for idx_key, idx_symbols in indices.items():
                sector_name = INDEX_TO_SECTOR.get(idx_key.lower())
                if sector_name:
                    for sym in idx_symbols:
                        sym_clean = str(sym).strip().upper()
                        if sym_clean not in _sector_cache:
                            _sector_cache[sym_clean] = sector_name
                            count += 1

            print(f"[INFO] Loaded sector data for {count} stocks from index_constituents.json")
            _sector_cache_loaded = True
            return

        except Exception as e:
            print(f"[ERROR] JSON fallback failed: {e}")

    print("[WARNING] No sector data found. Defaulting to 'Others'.")
    _sector_cache_loaded = True # Prevent retry loop


# =============================================================
# PUBLIC API
# =============================================================

def get_sector(symbol: str) -> str:
    """Get the sector for a given symbol."""
    if not _sector_cache_loaded:
        load_sector_master()

    return _sector_cache.get(symbol.strip().upper(), DEFAULT_SECTOR)

def get_all_sectors() -> List[str]:
    """Get list of all unique sectors present in cache."""
    if not _sector_cache_loaded:
        load_sector_master()
    return sorted(list(set(_sector_cache.values())))
