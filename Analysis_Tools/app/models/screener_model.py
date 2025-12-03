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


# =============================================================
# FUTURES OI SCREENER FUNCTIONS
# =============================================================

def get_available_dates_for_new_screeners():
    """Get available dates from screener cache tables"""
    try:
        query = text("""
            SELECT DISTINCT cache_date 
            FROM screener_futures_oi_cache 
            ORDER BY cache_date DESC
        """)
        df = pd.read_sql(query, engine)
        return [str(d) for d in df['cache_date'].tolist()]
    except Exception as e:
        print(f"Error getting dates: {e}")
        return []


def get_futures_oi_screeners(selected_date):
    """
    Get Futures OI screener data organized by categories
    Returns dict with 8 categories of top 10 stocks each
    """
    try:
        query = text("""
            SELECT 
                stock_name,
                underlying_price,
                cme_expiry_date,
                cme_expiry_price,
                cme_oi,
                cme_oi_change,
                cme_exposure,
                cme_exposure_percentile,
                nme_expiry_date,
                nme_expiry_price,
                nme_oi,
                nme_oi_change,
                nme_exposure,
                nme_exposure_percentile,
                fme_expiry_date,
                fme_expiry_price,
                fme_oi,
                fme_oi_change,
                fme_exposure,
                fme_exposure_percentile,
                cumulative_oi,
                cum_oi_percentile
            FROM screener_futures_oi_cache
            WHERE cache_date = :date
            ORDER BY stock_name
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {"date": selected_date})
            rows = result.fetchall()
        
        if not rows:
            return {}
        
        # Convert to list of dicts
        data = []
        for row in rows:
            data.append({
                'stock_name': row[0],
                'underlying_price': float(row[1]) if row[1] else 0,
                'cme_expiry_date': str(row[2]) if row[2] else None,
                'cme_expiry_price': float(row[3]) if row[3] else 0,
                'cme_oi': int(row[4]) if row[4] else 0,
                'cme_oi_change': int(row[5]) if row[5] else 0,
                'cme_exposure': float(row[6]) if row[6] else 0,
                'cme_exposure_percentile': float(row[7]) if row[7] else 0,
                'nme_expiry_date': str(row[8]) if row[8] else None,
                'nme_expiry_price': float(row[9]) if row[9] else 0,
                'nme_oi': int(row[10]) if row[10] else 0,
                'nme_oi_change': int(row[11]) if row[11] else 0,
                'nme_exposure': float(row[12]) if row[12] else 0,
                'nme_exposure_percentile': float(row[13]) if row[13] else 0,
                'fme_expiry_date': str(row[14]) if row[14] else None,
                'fme_expiry_price': float(row[15]) if row[15] else 0,
                'fme_oi': int(row[16]) if row[16] else 0,
                'fme_oi_change': int(row[17]) if row[17] else 0,
                'fme_exposure': float(row[18]) if row[18] else 0,
                'fme_exposure_percentile': float(row[19]) if row[19] else 0,
                'cumulative_oi': int(row[20]) if row[20] else 0,
                'cum_oi_percentile': float(row[21]) if row[21] else 0
            })
        
        # Organize into 8 categories
        result = {
            'cme_exposure_gainers': sorted([d for d in data], key=lambda x: x['cme_exposure_percentile'], reverse=True)[:10],
            'cme_exposure_losers': sorted([d for d in data], key=lambda x: x['cme_exposure_percentile'])[:10],
            'nme_exposure_gainers': sorted([d for d in data], key=lambda x: x['nme_exposure_percentile'], reverse=True)[:10],
            'nme_exposure_losers': sorted([d for d in data], key=lambda x: x['nme_exposure_percentile'])[:10],
            'fme_exposure_gainers': sorted([d for d in data], key=lambda x: x['fme_exposure_percentile'], reverse=True)[:10],
            'fme_exposure_losers': sorted([d for d in data], key=lambda x: x['fme_exposure_percentile'])[:10],
            'top_cumulative_oi': sorted([d for d in data], key=lambda x: x['cum_oi_percentile'], reverse=True)[:10],
            'bottom_cumulative_oi': sorted([d for d in data], key=lambda x: x['cum_oi_percentile'])[:10]
        }
        
        return result
        
    except Exception as e:
        print(f"Error in get_futures_oi_screeners: {e}")
        import traceback
        traceback.print_exc()
        return {}


def get_technical_indicators_screeners(selected_date):
    """
    Get Technical Indicators screener data organized by categories
    Returns dict with 10 categories
    """
    try:
        query = text("""
            SELECT 
                stock_name,
                close_price,
                rsi,
                macd,
                macd_signal,
                sma_20,
                sma_50,
                bb_upper,
                bb_lower,
                adx,
                composite_score,
                strength_percentile
            FROM screener_technical_cache
            WHERE cache_date = :date
            ORDER BY stock_name
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {"date": selected_date})
            rows = result.fetchall()
        
        if not rows:
            return {}
        
        # Convert to list of dicts
        data = []
        for row in rows:
            data.append({
                'stock_name': row[0],
                'close_price': float(row[1]) if row[1] else 0,
                'rsi': float(row[2]) if row[2] else 0,
                'macd': float(row[3]) if row[3] else 0,
                'macd_signal': float(row[4]) if row[4] else 0,
                'sma_20': float(row[5]) if row[5] else 0,
                'sma_50': float(row[6]) if row[6] else 0,
                'bb_upper': float(row[7]) if row[7] else 0,
                'bb_lower': float(row[8]) if row[8] else 0,
                'adx': float(row[9]) if row[9] else 0,
                'composite_score': float(row[10]) if row[10] else 0,
                'strength_percentile': float(row[11]) if row[11] else 0
            })
        
        # Organize into categories
        result = {
            'strongest_signals': sorted([d for d in data], key=lambda x: x['strength_percentile'], reverse=True)[:10],
            'weakest_signals': sorted([d for d in data], key=lambda x: x['strength_percentile'])[:10],
            'rsi_overbought': sorted([d for d in data if d['rsi'] > 70], key=lambda x: x['rsi'], reverse=True)[:10],
            'rsi_oversold': sorted([d for d in data if d['rsi'] < 30], key=lambda x: x['rsi'])[:10],
            'macd_bullish': sorted([d for d in data if d['macd'] > d['macd_signal']], key=lambda x: abs(d['macd'] - d['macd_signal']), reverse=True)[:10],
            'macd_bearish': sorted([d for d in data if d['macd'] < d['macd_signal']], key=lambda x: abs(d['macd'] - d['macd_signal']), reverse=True)[:10],
            'above_sma': sorted([d for d in data if d['close_price'] > d['sma_50']], key=lambda x: (x['close_price'] - x['sma_50']) / x['sma_50'], reverse=True)[:10],
            'below_sma': sorted([d for d in data if d['close_price'] < d['sma_50']], key=lambda x: (x['sma_50'] - x['close_price']) / x['sma_50'], reverse=True)[:10],
            'strong_trend': sorted([d for d in data], key=lambda x: x['adx'], reverse=True)[:10],
            'weak_trend': sorted([d for d in data], key=lambda x: x['adx'])[:10]
        }
        
        return result
        
    except Exception as e:
        print(f"Error in get_technical_indicators_screeners: {e}")
        import traceback
        traceback.print_exc()
        return {}
