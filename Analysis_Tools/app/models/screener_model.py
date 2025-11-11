"""
Screener model for Important Screener page.
ULTRA-OPTIMIZED: Reads pre-calculated data from screener_cache table
(<0.5 seconds load time instead of 10+ seconds)

Data is pre-calculated daily during update_database.py execution
"""

from sqlalchemy import inspect, text
import pandas as pd
from datetime import datetime, timedelta
from .db_config import engine, get_stock_list_from_excel
from functools import lru_cache
from .dashboard_model import get_available_dates

# =============================================================
# CACHE TABLE LOOKUP - ULTRA FAST
# =============================================================

def get_all_screener_data(selected_date: str):
    """
    ULTRA-OPTIMIZED: Fetch screener data from pre-calculated cache table
    Returns data in <0.5 seconds instead of 10+ seconds
    
    Cache is built daily during update_database.py execution
    """
    try:
        print(f"[INFO] Fetching screener cache for {selected_date}...")
        
        # Query the cache table for all metrics
        query = text('''
            SELECT 
                metric_type,
                option_type,
                moneyness_filter,
                ticker,
                underlying_price,
                change
            FROM public.screener_cache
            WHERE cache_date = :cache_date
            ORDER BY metric_type, option_type, moneyness_filter, rank ASC
        ''')
        
        df = pd.read_sql(query, con=engine, params={"cache_date": selected_date})
        
        if df.empty:
            print(f"[WARNING] No cache data found for {selected_date}")
            return {}
        
        # Initialize result structure
        result = {
            'oi': {'CE': {'ALL': [], 'ITM': [], 'OTM': []}, 'PE': {'ALL': [], 'ITM': [], 'OTM': []}, 'FUT': {'ALL': []}},
            'moneyness': {'CE': {'ALL': [], 'ITM': [], 'OTM': []}, 'PE': {'ALL': [], 'ITM': [], 'OTM': []}, 'FUT': {'ALL': []}},
            'iv': {'CE': {'ALL': [], 'ITM': [], 'OTM': []}, 'PE': {'ALL': [], 'ITM': [], 'OTM': []}, 'FUT': {'ALL': []}}
        }
        
        # Populate result from cache
        for _, row in df.iterrows():
            metric = row['metric_type']
            opt_type = row['option_type']
            filter_type = row['moneyness_filter']
            
            item = {
                'ticker': row['ticker'],
                'underlying_price': float(row['underlying_price']),
                'change': float(row['change'])
            }
            
            # Add to appropriate list
            if metric in result and opt_type in result[metric]:
                if filter_type in result[metric][opt_type]:
                    result[metric][opt_type][filter_type].append(item)
        
        print(f"[INFO] Cache loaded successfully!")
        return result
        
    except Exception as e:
        print(f"[ERROR] get_all_screener_data(): {e}")
        import traceback
        traceback.print_exc()
        return {}

def get_screener_data(selected_date: str, metric_type: str, option_type: str, moneyness_filter: str = "ALL"):
    """
    Get screener data for a specific metric/option/filter combination
    Uses cached all_data for fast retrieval
    """
    # This will be called multiple times, but we'll cache the result
    cache_key = f"{selected_date}_all_screener_data"
    if not hasattr(get_screener_data, '_cache') or get_screener_data._cache.get('key') != cache_key:
        get_screener_data._cache = {
            'key': cache_key,
            'data': get_all_screener_data(selected_date)
        }
    
    all_data = get_screener_data._cache['data']
    
    # Map metric types
    metric_map = {'OI': 'oi', 'MONEYNESS': 'moneyness', 'IV': 'iv'}
    metric_key = metric_map.get(metric_type, 'oi')
    
    # Get data from cache
    if metric_key in all_data and option_type in all_data[metric_key]:
        return all_data[metric_key][option_type].get(moneyness_filter, [])[:10]
    
    return []
