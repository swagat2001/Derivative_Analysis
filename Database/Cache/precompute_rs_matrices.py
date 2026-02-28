import os
import sys
import pandas as pd
from sqlalchemy import text
from datetime import datetime

# Add the Analysis_Tools directory to path so we can access application models
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Add project root and Analysis_Tools to path
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
sys.path.append(os.path.join(PROJECT_ROOT, "Analysis_Tools"))

from app.models.db_config import engine_cash
from app.models.pf_matrix_model import (
    generate_rs_matrix_html,
    generate_stock_rs_matrix_html,
    get_index_category_rs_data,
    get_stock_rs_data
)
import json
def init_cache_table():
    query = text("""
    CREATE TABLE IF NOT EXISTS rs_matrix_cache (
        cache_key VARCHAR PRIMARY KEY,
        html_content TEXT,
        updated_at TIMESTAMP
    )
    """)
    with engine_cash.connect() as conn:
        conn.execute(query)
        conn.commit()

def save_to_cache(cache_key: str, html_content: str):
    # Upsert the cache entry
    query = text("""
    INSERT INTO rs_matrix_cache (cache_key, html_content, updated_at)
    VALUES (:cache_key, :html_content, :updated_at)
    ON CONFLICT (cache_key) DO UPDATE
    SET html_content = EXCLUDED.html_content,
        updated_at = EXCLUDED.updated_at
    """)
    now = datetime.now()
    with engine_cash.connect() as conn:
        conn.execute(query, {
            "cache_key": cache_key,
            "html_content": html_content,
            "updated_at": now
        })
        conn.commit()

def main():
    print("[INFO] Starting RS Matrix Precomputation...")
    init_cache_table()

    box_percents = [0.25]

    # 1. Generate Index Matrices
    print("[INFO] Precomputing Index Matrices...")
    for pct in box_percents:
        try:
            print(f"  - Generating Index matrix for {pct}%...")
            html = generate_rs_matrix_html(pct)
            save_to_cache(f"index_{pct}", html)
        except Exception as e:
            print(f"  [ERROR] Failed to generate index matrix for {pct}%: {e}")

    # 2. Generate Stock Matrices for each click-enabled index
    print("[INFO] Precomputing Stock Matrices for Indices...")
    from app.models.index_model import get_index_list

    dyn_indices = get_index_list()
    for idx_info in dyn_indices:
        idx_key = idx_info["key"]
        index_name = idx_info["name"]

        if idx_key in ["all", "sensex"] or not index_name:
            continue

        print(f"  - Precomputing constituents for {index_name}...")
        for pct in box_percents:
             try:
                 html = generate_stock_rs_matrix_html(index_name, pct)
                 cache_key = f"stock_{index_name}_{pct}"
                 save_to_cache(cache_key, html)
             except Exception as e:
                 # The user specifically mentioned ALPHALOWVOLATILITY30 error,
                 # so we want to be noisy here if it fails
                 print(f"  [ERROR] Failed to generate constituent matrix for {index_name} @ {pct}%: {e}")

             # Precompute JSON data for Treemap (v2)
             try:
                 json_data = get_stock_rs_data(index_name, pct)
                 json_cache_key = f"stock_json_{index_name}_{pct}_v2"
                 save_to_cache(json_cache_key, json.dumps(json_data))
             except Exception as e:
                 print(f"  [ERROR] Failed to generate JSON data for {index_name} @ {pct}%: {e}")

    # 3. Generate Index Category JSON Data for Treemaps (v2)
    print("[INFO] Precomputing Index Category JSON Data...")
    categories = ["all", "broad", "sector", "thematic"]
    for cat in categories:
        for pct in box_percents:
             try:
                 print(f"  - Generating JSON data for category '{cat}' @ {pct}%...")
                 import re as _re
                 cat_slug  = _re.sub(r'[^A-Za-z0-9]', '_', cat)
                 json_data = get_index_category_rs_data(cat if cat != "all" else "", pct)
                 json_cache_key = f"indices_json_{cat_slug}_{pct}_v2"
                 save_to_cache(json_cache_key, json.dumps(json_data))
             except Exception as e:
                 print(f"  [ERROR] Failed to generate JSON data for category '{cat}' @ {pct}%: {e}")

    print("[INFO] Precomputation complete! All matrices saved to CashStocks database.")

if __name__ == "__main__":
    main()
