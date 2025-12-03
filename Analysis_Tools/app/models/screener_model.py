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
    Fetch screener data from pre-calculated cache table.
    Now also loads FINAL SIGNAL FIELDS:
        - final_signal
    """
    try:
        print(f"[INFO] Fetching screener cache for {selected_date}...")

        # UPDATED QUERY → NOW FETCHES SIGNAL FIELDS
        query = text('''
        SELECT 
            metric_type,
            option_type,
            moneyness_filter,
            ticker,
            strike_price,
            underlying_price,
            change,
            final_signal
        FROM public.screener_cache
        WHERE cache_date = :cache_date
        ORDER BY metric_type, option_type, moneyness_filter, rank ASC
        ''')
        
        df = pd.read_sql(query, con=engine, params={"cache_date": selected_date})
        
        if df.empty:
            print(f"[WARNING] No cache data found for {selected_date}")
            return {}
        
        # Initialize result structure WITH losers keys
        result = {
            'oi': {
                'CE': {'ALL': [], 'ITM': [], 'OTM': [], 
                       'ALL_LOSERS': [], 'ITM_LOSERS': [], 'OTM_LOSERS': []}, 
                'PE': {'ALL': [], 'ITM': [], 'OTM': [], 
                       'ALL_LOSERS': [], 'ITM_LOSERS': [], 'OTM_LOSERS': []}, 
                'FUT': {'ALL': [], 'ALL_LOSERS': []}
            },
            'moneyness': {
                'CE': {'ALL': [], 'ITM': [], 'OTM': [], 
                       'ALL_LOSERS': [], 'ITM_LOSERS': [], 'OTM_LOSERS': []}, 
                'PE': {'ALL': [], 'ITM': [], 'OTM': [], 
                       'ALL_LOSERS': [], 'ITM_LOSERS': [], 'OTM_LOSERS': []}, 
                'FUT': {'ALL': [], 'ALL_LOSERS': []}
            },
            'iv': {
                'CE': {'ALL': [], 'ITM': [], 'OTM': [], 
                       'ALL_LOSERS': [], 'ITM_LOSERS': [], 'OTM_LOSERS': []}, 
                'PE': {'ALL': [], 'ITM': [], 'OTM': [], 
                       'ALL_LOSERS': [], 'ITM_LOSERS': [], 'OTM_LOSERS': []}, 
                'FUT': {'ALL': [], 'ALL_LOSERS': []}
            }
        }
        
        # Populate result from cache
        for _, row in df.iterrows():
            metric = row['metric_type']
            opt_type = row['option_type']
            filter_type = row['moneyness_filter']
            
            # UPDATED → Include signal fields
            item = {
                'ticker': row['ticker'],
                'strike_price': float(row.get('strike_price', 0) or 0),
                'underlying_price': float(row['underlying_price']),
                'change': float(row['change']),
                'final_signal': row.get('final_signal')  # 'BULLISH' or 'BEARISH'
            }
            
            if metric in result and opt_type in result[metric]:
                if filter_type in result[metric][opt_type]:
                    result[metric][opt_type][filter_type].append(item)
        
        # DEBUG: Log counts per category
        print(f"[DEBUG] Data counts for {selected_date}:")
        for metric in ['oi', 'moneyness', 'iv']:
            for opt_type in ['CE', 'PE', 'FUT']:
                for filter_type in result[metric][opt_type].keys():
                    count = len(result[metric][opt_type][filter_type])
                    if count < 10:
                        print(f"  ⚠️  {metric}/{opt_type}/{filter_type}: {count} rows (< 10)")
        
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
    
    if metric_key in all_data and option_type in all_data[metric_key]:
        return all_data[metric_key][option_type].get(moneyness_filter, [])[:10]
    
    return []
