#!/usr/bin/env python3
"""
NSE Sector Classification Pipeline - Unified Script
====================================================
This script combines 3 operations into one:
1. Fetch sector/industry data from NSE API for all stocks in EQUITY_L.csv
2. Map NSE's granular industries into 12 broad sectors
3. Add Nifty index membership tags (NIFTY50, NIFTY100, etc.)

Output: nse_sector_master.csv with SYMBOL, COMPANY_NAME_API, NSE_INDUSTRY, SECTOR, INDEX_MEMBERSHIP

Author: Financial Software Developer
Date: December 2025
"""

import random
import time
from collections import defaultdict
from pathlib import Path

import pandas as pd
import requests

# ================== CONFIGURATION ==================
INPUT_EQUITY_FILE = "EQUITY_L.csv"
OUTPUT_FILE = "nse_sector_master.csv"
BACKUP_FILE = "nse_sector_master_backup.csv"

# Rate limiting to avoid NSE blocking
MIN_SLEEP = 0.35
MAX_SLEEP = 0.75

# Nifty indices to track
NIFTY_INDICES = {
    "NIFTY50": "NIFTY 50",
    "NIFTY100": "NIFTY 100",
    "NIFTY500": "NIFTY 500",
    "NIFTYMIDCAP150": "NIFTY MIDCAP 150",
    "NIFTYSMALLCAP250": "NIFTY SMALLCAP 250",
}

# NSE API endpoints
QUOTE_API_URL = "https://www.nseindia.com/api/quote-equity"
INDEX_API_URL = "https://www.nseindia.com/api/equity-stockIndices"

# Browser-like headers to avoid blocking
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Referer": "https://www.nseindia.com/",
}

# =====================================================


def bootstrap_session() -> requests.Session:
    """
    Initialize NSE session with cookies by hitting homepage first.
    This is required for NSE API authentication.
    """
    sess = requests.Session()
    sess.headers.update(HEADERS)
    home_url = "https://www.nseindia.com/"

    print(f"\n🌐 Initializing NSE session...")
    try:
        resp = sess.get(home_url, timeout=10)
        if resp.status_code == 200:
            print("✅ Session initialized successfully")
        else:
            print(f"⚠️  Warning: init status {resp.status_code}, continuing anyway")
    except Exception as e:
        print(f"⚠️  Session init warning: {e}, continuing anyway")

    return sess


def fetch_symbol_metadata(session: requests.Session, symbol: str) -> dict:
    """
    Fetch company metadata from NSE API for a given symbol.
    Returns: {SYMBOL, COMPANY_NAME_API, NSE_INDUSTRY, SECTOR}
    """
    params = {"symbol": symbol}

    try:
        resp = session.get(QUOTE_API_URL, params=params, timeout=10)
    except Exception as e:
        print(f"  ❌ [{symbol}] Request failed: {e}")
        return {
            "SYMBOL": symbol,
            "COMPANY_NAME_API": None,
            "NSE_INDUSTRY": None,
            "SECTOR": None,
        }

    if resp.status_code != 200:
        print(f"  ⚠️  [{symbol}] HTTP {resp.status_code}")
        return {
            "SYMBOL": symbol,
            "COMPANY_NAME_API": None,
            "NSE_INDUSTRY": None,
            "SECTOR": None,
        }

    try:
        data = resp.json()
    except Exception as e:
        print(f"  ⚠️  [{symbol}] JSON parse error: {e}")
        return {
            "SYMBOL": symbol,
            "COMPANY_NAME_API": None,
            "NSE_INDUSTRY": None,
            "SECTOR": None,
        }

    info = data.get("info", {}) or {}
    company_name = info.get("companyName")
    industry = info.get("industry")

    return {
        "SYMBOL": symbol,
        "COMPANY_NAME_API": company_name,
        "NSE_INDUSTRY": industry,
        "SECTOR": industry,  # Will be updated by classify_industry()
    }


def classify_industry(industry: str) -> str:
    """
    Map NSE_INDUSTRY into one of 12 broad sectors:
    - Financials
    - Information Technology
    - Energy
    - Materials
    - Industrials
    - Consumer Discretionary
    - Consumer Staples
    - Healthcare & Pharma
    - Telecom
    - Utilities
    - Real Estate & Construction
    - Diversified / Others
    """
    if not isinstance(industry, str) or not industry.strip():
        return "Diversified / Others"

    s = industry.strip().lower()

    # Diversified / Misc
    if any(x in s for x in ["diversified", "miscellaneous", "conglomerate"]):
        return "Diversified / Others"

    # Financials (all finance-related)
    if any(
        x in s
        for x in [
            "bank",
            "nbfc",
            "finance",
            "financial",
            "housing finance",
            "stockbroking",
            "stock broking",
            "broker",
            "broking",
            "capital market",
            "securities",
            "insurance",
            "life insurance",
            "general insurance",
            "reinsurance",
            "asset management",
            "mutual fund",
            "wealth management",
            "credit rating",
            "leasing",
            "investment company",
        ]
    ):
        return "Financials"

    # Infrastructure split by type
    if "infrastructure" in s or "infra" in s:
        if any(x in s for x in ["telecom", "telecommunication"]):
            return "Telecom"
        if any(x in s for x in ["power", "energy", "oil", "gas", "pipeline"]):
            return "Energy"
        if any(x in s for x in ["road", "highway", "transport", "rail", "port", "airport", "logistics"]):
            return "Industrials"
        return "Real Estate & Construction"

    # Trading - categorized by what they trade
    if "trading" in s:
        if any(x in s for x in ["metal", "steel", "iron", "mineral", "coal", "cement", "chemical", "fertilizer"]):
            return "Materials"
        if any(x in s for x in ["oil", "gas", "petroleum", "fuel", "lng", "cng"]):
            return "Energy"
        if any(x in s for x in ["sugar", "tea", "coffee", "rice", "spice", "edible oil", "grain", "agro"]):
            return "Consumer Staples"
        if any(
            x in s
            for x in [
                "auto",
                "automobile",
                "vehicle",
                "tyre",
                "footwear",
                "garment",
                "textile",
                "jewellery",
                "gems",
                "fashion",
            ]
        ):
            return "Consumer Discretionary"
        return "Consumer Discretionary"

    # Information Technology
    if any(
        x in s
        for x in [
            "software",
            "it enabled",
            "information technology",
            "computers - software",
            "it services",
            "data processing",
            "technology services",
            "internet services",
            "computers hardware",
            "computer hardware",
        ]
    ):
        return "Information Technology"

    # Healthcare & Pharma
    if any(
        x in s
        for x in [
            "pharma",
            "pharmaceutical",
            "drug",
            "formulations",
            "bulk drugs",
            "api (active pharmaceutical)",
            "diagnostic",
            "diagnostics",
            "hospital",
            "healthcare",
            "health care",
            "clinic",
            "biotech",
            "biotechnology",
            "life sciences",
            "medical equipment",
            "medical devices",
        ]
    ):
        return "Healthcare & Pharma"

    # Telecom
    if any(x in s for x in ["telecom", "telecommunication", "telephone", "cellular", "mobile services", "telephony"]):
        return "Telecom"

    # Utilities
    if any(
        x in s
        for x in [
            "power generation",
            "power distribution",
            "power -",
            "electricity",
            "electric utility",
            "utilities",
            "gas distribution",
            "city gas",
            "water supply",
            "water utility",
        ]
    ):
        return "Utilities"

    # Energy
    if any(
        x in s
        for x in [
            "oil",
            "gas",
            "refineries",
            "refinery",
            "petroleum",
            "lng",
            "cng",
            "exploration",
            "upstream",
            "downstream",
            "coal",
            "renewable energy",
            "wind energy",
            "solar energy",
            "hydro power",
            "energy",
        ]
    ):
        return "Energy"

    # Materials
    if any(
        x in s
        for x in [
            "steel",
            "iron",
            "ferro",
            "non-ferrous",
            "metal",
            "mineral",
            "mining",
            "cement",
            "cement products",
            "industrial minerals",
            "alloys",
            "aluminium",
            "copper",
            "zinc",
            "lead",
            "sponge iron",
            "pig iron",
            "paper",
            "paper products",
            "forest products",
            "glass",
            "packaging",
            "petrochemical",
            "chemicals",
            "fertilizer",
            "paints",
            "pigments",
            "ceramics",
            "plywood",
            "laminates",
        ]
    ):
        return "Materials"

    # Real Estate & Construction
    if any(
        x in s
        for x in [
            "realty",
            "real estate",
            "developer",
            "construction",
            "construction & engineering",
            "contracting",
            "civil construction",
            "housing projects",
            "township",
            "sez",
            "industrial park",
            "residential",
            "commercial projects",
            "residential commercial",
        ]
    ):
        return "Real Estate & Construction"

    # Consumer Staples
    if any(
        x in s
        for x in [
            "fmcg",
            "consumer non-durables",
            "food",
            "beverages",
            "tea",
            "coffee",
            "sugar",
            "edible oil",
            "dairy",
            "milk products",
            "bakery",
            "packaged foods",
            "personal care",
            "toiletries",
            "cosmetics",
            "household products",
            "tobacco",
            "cigarettes",
            "breweries",
            "distilleries",
            "liquor",
            "alcohol",
        ]
    ):
        return "Consumer Staples"

    # Consumer Discretionary
    if any(
        x in s
        for x in [
            "auto",
            "automobile",
            "vehicle",
            "tyre",
            "tyres",
            "auto ancillaries",
            "consumer durables",
            "white goods",
            "electronics - consumer",
            "retail",
            "department store",
            "mall",
            "hypermarket",
            "hotel",
            "hospitality",
            "tourism",
            "travel & leisure",
            "media",
            "entertainment",
            "multiplex",
            "cinema",
            "gems & jewellery",
            "jewellery",
            "textile",
            "garment",
            "apparel",
            "hosiery",
            "footwear",
            "education services",
            "training services",
            "household appliances",
            "consumer electronics",
            "diversified retail",
            "restaurants",
            "amusement parks",
            "recreation",
            "plastic products - consumer",
        ]
    ):
        return "Consumer Discretionary"

    # Industrials
    if any(
        x in s
        for x in [
            "capital goods",
            "engineering",
            "industrial machinery",
            "machinery",
            "industrial products",
            "automation",
            "electrical equipment",
            "switchgear",
            "pump",
            "compressor",
            "logistics",
            "transport services",
            "shipping",
            "ports",
            "airline",
            "airport services",
            "courier",
            "road transport",
            "railway equipment",
            "defence",
            "aerospace",
            "castings",
            "forgings",
            "electrodes",
            "refractories",
            "cables - electrical",
            "rubber",
            "plastic products - industrial",
        ]
    ):
        return "Industrials"

    return "Diversified / Others"


def fetch_index_constituents(session: requests.Session, index_name: str) -> list:
    """
    Fetch list of stock symbols for a given Nifty index.
    Returns: List of SYMBOL strings
    """
    params = {"index": index_name}

    try:
        resp = session.get(INDEX_API_URL, params=params, timeout=15)
    except Exception as e:
        print(f"  ❌ [{index_name}] Request failed: {e}")
        return []

    if resp.status_code != 200:
        print(f"  ⚠️  [{index_name}] HTTP {resp.status_code}")
        return []

    try:
        data = resp.json()
    except Exception as e:
        print(f"  ⚠️  [{index_name}] JSON parse error: {e}")
        return []

    items = data.get("data") or []
    symbols = []

    for item in items:
        sym = item.get("symbol")
        if sym:
            symbols.append(str(sym).strip().upper())

    return symbols


def main():
    """Main execution pipeline"""

    print("\n" + "=" * 80)
    print("NSE SECTOR CLASSIFICATION PIPELINE - UNIFIED")
    print("=" * 80)

    # ========== STEP 1: Load EQUITY_L.csv ==========
    input_path = Path(INPUT_EQUITY_FILE)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path.resolve()}")

    print(f"\n📥 Step 1: Loading {INPUT_EQUITY_FILE}...")
    equity_df = pd.read_csv(input_path)

    # Find SYMBOL column (case-insensitive)
    symbol_col = None
    for col in equity_df.columns:
        if col.strip().upper() == "SYMBOL":
            symbol_col = col
            break

    if symbol_col is None:
        raise KeyError(f"SYMBOL column not found. Available: {list(equity_df.columns)}")

    equity_df["SYMBOL_norm"] = equity_df[symbol_col].astype(str).str.strip().str.upper()
    symbols = sorted(equity_df["SYMBOL_norm"].dropna().unique())

    print(f"✅ Loaded {len(equity_df)} rows, {len(symbols)} unique symbols")

    # ========== STEP 2: Fetch metadata from NSE API ==========
    print(f"\n📊 Step 2: Fetching sector/industry data from NSE API...")
    print(f"⏳ This will take ~{int(len(symbols) * 0.5 / 60)} minutes (rate-limited)")

    session = bootstrap_session()
    records = []

    for i, sym in enumerate(symbols, start=1):
        if i % 50 == 0 or i == 1:
            print(f"  Progress: [{i}/{len(symbols)}] ({i*100//len(symbols)}%)")

        meta = fetch_symbol_metadata(session, sym)
        records.append(meta)

        # Rate limiting
        time.sleep(random.uniform(MIN_SLEEP, MAX_SLEEP))

    meta_df = pd.DataFrame(records)
    print(f"✅ Fetched metadata for {len(meta_df)} symbols")

    # ========== STEP 3: Classify into broad sectors ==========
    print(f"\n🔎 Step 3: Classifying industries into 12 broad sectors...")

    meta_df["SECTOR"] = meta_df["NSE_INDUSTRY"].apply(classify_industry)

    sector_counts = meta_df["SECTOR"].value_counts()
    print(f"✅ Sector classification complete:")
    for sector, count in sector_counts.items():
        print(f"  {sector:30s}: {count:4d} stocks")

    # ========== STEP 4: Add Nifty index membership ==========
    print(f"\n📈 Step 4: Adding Nifty index membership tags...")

    symbol_to_indices = defaultdict(list)

    for idx_tag, idx_name in NIFTY_INDICES.items():
        print(f"  Fetching {idx_name}...")
        constituents = fetch_index_constituents(session, idx_name)
        print(f"    Found {len(constituents)} stocks")

        for sym in constituents:
            if idx_tag not in symbol_to_indices[sym]:
                symbol_to_indices[sym].append(idx_tag)

        time.sleep(random.uniform(MIN_SLEEP, MAX_SLEEP))

    def build_membership(sym: str) -> str:
        sym_upper = str(sym).strip().upper()
        indices = symbol_to_indices.get(sym_upper, [])
        return ",".join(sorted(indices)) if indices else ""

    meta_df["INDEX_MEMBERSHIP"] = meta_df["SYMBOL"].apply(build_membership)

    has_membership = (meta_df["INDEX_MEMBERSHIP"] != "").sum()
    print(f"✅ Index membership added: {has_membership} stocks tagged")

    # ========== STEP 5: Save output ==========
    print(f"\n💾 Step 5: Saving output files...")

    output_path = Path(OUTPUT_FILE)
    backup_path = Path(BACKUP_FILE)

    # Create backup if output already exists
    if output_path.exists() and not backup_path.exists():
        meta_df.to_csv(backup_path, index=False, encoding="utf-8-sig")
        print(f"  🔐 Backup created: {backup_path.resolve()}")

    # Save final output
    meta_df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"  ✅ Output saved: {output_path.resolve()}")

    # ========== Final Summary ==========
    print("\n" + "=" * 80)
    print("PIPELINE COMPLETE - SUMMARY")
    print("=" * 80)
    print(f"\n📊 Dataset Statistics:")
    print(f"  Total stocks processed    : {len(meta_df)}")
    print(f"  Stocks with industry data : {meta_df['NSE_INDUSTRY'].notna().sum()}")
    print(f"  Stocks with index tags    : {(meta_df['INDEX_MEMBERSHIP'] != '').sum()}")
    print(f"  Missing industry data     : {meta_df['NSE_INDUSTRY'].isna().sum()}")
    print(f"\n📁 Output Files:")
    print(f"  Main output : {output_path.resolve()}")
    if backup_path.exists():
        print(f"  Backup file : {backup_path.resolve()}")
    print(f"\n✅ All done! Use {OUTPUT_FILE} for your dashboard.")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
