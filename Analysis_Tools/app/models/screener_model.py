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
            FROM futures_oi_cache 
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
        # Query the futures_oi_cache table which has ticker, expiry_type structure
        query = text("""
            SELECT 
                ticker,
                underlying_price,
                expiry_type,
                expiry_date,
                expiry_price,
                expiry_oi,
                expiry_oi_change,
                oi_percentile,
                price_percentile
            FROM futures_oi_cache
            WHERE cache_date = :date
            ORDER BY ticker, expiry_type
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {"date": selected_date})
            rows = result.fetchall()
        
        if not rows:
            return {}
        
        # Reorganize data by ticker first
        ticker_data = {}
        for row in rows:
            ticker = row[0]
            if ticker not in ticker_data:
                ticker_data[ticker] = {
                    'stock_name': ticker,
                    'underlying_price': float(row[1]) if row[1] else 0
                }
            
            expiry_type = row[2]  # CME, NME, or FME
            prefix = expiry_type.lower()
            
            ticker_data[ticker][f'{prefix}_expiry_date'] = str(row[3]) if row[3] else None
            ticker_data[ticker][f'{prefix}_expiry_price'] = float(row[4]) if row[4] else 0
            ticker_data[ticker][f'{prefix}_oi'] = int(row[5]) if row[5] else 0
            ticker_data[ticker][f'{prefix}_oi_change'] = int(row[6]) if row[6] else 0
            ticker_data[ticker][f'{prefix}_exposure'] = float(row[4]) if row[4] else 0
            ticker_data[ticker][f'{prefix}_exposure_percentile'] = float(row[8]) if row[8] else 0
        
        # Calculate cumulative OI
        data = []
        for ticker, info in ticker_data.items():
            info['cumulative_oi'] = (
                info.get('cme_oi', 0) + 
                info.get('nme_oi', 0) + 
                info.get('fme_oi', 0)
            )
            data.append(info)
        
        # Calculate cumulative OI percentiles
        cum_ois = [d['cumulative_oi'] for d in data]
        for item in data:
            item['cum_oi_percentile'] = (
                sum(1 for c in cum_ois if c < item['cumulative_oi']) / len(cum_ois) * 100 
                if cum_ois else 50
            )
        
        # Organize into 8 categories
        result = {
            'cme_exposure_gainers': sorted([d for d in data], key=lambda x: x.get('cme_exposure_percentile', 0), reverse=True)[:10],
            'cme_exposure_losers': sorted([d for d in data], key=lambda x: x.get('cme_exposure_percentile', 0))[:10],
            'nme_exposure_gainers': sorted([d for d in data], key=lambda x: x.get('nme_exposure_percentile', 0), reverse=True)[:10],
            'nme_exposure_losers': sorted([d for d in data], key=lambda x: x.get('nme_exposure_percentile', 0))[:10],
            'fme_exposure_gainers': sorted([d for d in data], key=lambda x: x.get('fme_exposure_percentile', 0), reverse=True)[:10],
            'fme_exposure_losers': sorted([d for d in data], key=lambda x: x.get('fme_exposure_percentile', 0))[:10],
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
                ticker,
                underlying_price,
                rsi_14,
                macd,
                macd_signal,
                sma_50,
                sma_200,
                bb_upper,
                bb_lower,
                adx_14
            FROM technical_screener_cache
            WHERE cache_date = :date
            ORDER BY ticker
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {"date": selected_date})
            rows = result.fetchall()
        
        if not rows:
            return {}
        
        # Convert to list of dicts
        data = []
        for row in rows:
            # Calculate composite score from available data
            rsi = float(row[2]) if row[2] else 50
            close = float(row[1]) if row[1] else 0
            sma_50 = float(row[5]) if row[5] else close
            
            composite_score = 0
            if rsi > 50:
                composite_score += (rsi - 50) / 50 * 50
            else:
                composite_score -= (50 - rsi) / 50 * 50
            
            if close > sma_50 and sma_50 > 0:
                composite_score += ((close - sma_50) / sma_50 * 100)
            
            data.append({
                'stock_name': row[0],
                'close_price': close,
                'rsi': rsi,
                'macd': float(row[3]) if row[3] else 0,
                'macd_signal': float(row[4]) if row[4] else 0,
                'sma_20': sma_50,  # Using sma_50 as proxy
                'sma_50': sma_50,
                'bb_upper': float(row[7]) if row[7] else 0,
                'bb_lower': float(row[8]) if row[8] else 0,
                'adx': float(row[9]) if row[9] else 0,
                'composite_score': composite_score,
                'strength_percentile': 50  # Will calculate below
            })
        
        # Calculate percentiles
        scores = [d['composite_score'] for d in data]
        for item in data:
            item['strength_percentile'] = sum(1 for s in scores if s < item['composite_score']) / len(scores) * 100 if scores else 50
        
        # Organize into categories
        result = {
            'strongest_signals': sorted([d for d in data], key=lambda x: x['strength_percentile'], reverse=True)[:10],
            'weakest_signals': sorted([d for d in data], key=lambda x: x['strength_percentile'])[:10],
            'rsi_overbought': sorted([d for d in data if d['rsi'] > 80], key=lambda x: x['rsi'], reverse=True)[:10],
            'rsi_oversold': sorted([d for d in data if d['rsi'] < 20], key=lambda x: x['rsi'])[:10],
            'macd_bullish': sorted([d for d in data if d['macd'] > d['macd_signal']], key=lambda x: abs(x['macd'] - x['macd_signal']), reverse=True)[:10],
            'macd_bearish': sorted([d for d in data if d['macd'] < d['macd_signal']], key=lambda x: abs(x['macd'] - x['macd_signal']), reverse=True)[:10],
            'above_sma': sorted([d for d in data if d['close_price'] > d['sma_50']], key=lambda x: (x['close_price'] - x['sma_50']) / x['sma_50'] if x['sma_50'] > 0 else 0, reverse=True)[:10],
            'below_sma': sorted([d for d in data if d['close_price'] < d['sma_50']], key=lambda x: (x['sma_50'] - x['close_price']) / x['sma_50'] if x['sma_50'] > 0 else 0, reverse=True)[:10],
            'strong_trend': sorted([d for d in data], key=lambda x: x['adx'], reverse=True)[:10],
            'weak_trend': sorted([d for d in data], key=lambda x: x['adx'])[:10]
        }
        
        return result
        
    except Exception as e:
        print(f"Error in get_technical_indicators_screeners: {e}")
        import traceback
        traceback.print_exc()
        return {}


def get_all_technical_stocks(selected_date):
    """
    Get ALL stocks for heatmap display (not just top 10 per category)
    Returns complete dataset from technical_screener_cache
    """
    try:
        from sqlalchemy import text
        query = text("""
            SELECT 
                ticker,
                underlying_price,
                rsi_14,
                macd,
                macd_signal,
                sma_50,
                sma_200,
                bb_upper,
                bb_lower,
                adx_14,
                above_50_sma,
                above_200_sma,
                below_50_sma,
                below_200_sma,
                dist_from_50sma_pct,
                dist_from_200sma_pct
            FROM technical_screener_cache
            WHERE cache_date = :date
            ORDER BY ticker
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {"date": selected_date})
            rows = result.fetchall()
        
        if not rows:
            return []
        
        # Convert ALL rows to dicts
        all_stocks = []
        for row in rows:
            all_stocks.append({
                'ticker': row[0],
                'underlying_price': float(row[1]) if row[1] else 0,
                'rsi_14': float(row[2]) if row[2] else 0,
                'macd': float(row[3]) if row[3] else 0,
                'macd_signal': float(row[4]) if row[4] else 0,
                'sma_50': float(row[5]) if row[5] else 0,
                'sma_200': float(row[6]) if row[6] else 0,
                'bb_upper': float(row[7]) if row[7] else 0,
                'bb_lower': float(row[8]) if row[8] else 0,
                'adx_14': float(row[9]) if row[9] else 0,
                'above_200_sma': bool(row[11]) if row[11] is not None else False,
                'below_200_sma': bool(row[13]) if row[13] is not None else False,
                'dist_from_200sma_pct': float(row[15]) if row[15] else 0
            })
        
        print(f"[DEBUG] get_all_technical_stocks: Loaded {len(all_stocks)} stocks for heatmap")
        return all_stocks
        
    except Exception as e:
        print(f"Error in get_all_technical_stocks: {e}")
        import traceback
        traceback.print_exc()
        return []
