"""
Screener model for Important Screener page.
OPTIMIZED: Fetches all data in single pass, then filters in memory.
"""

from sqlalchemy import inspect, text
import pandas as pd
from datetime import datetime, timedelta
from .db_config import engine, get_stock_list_from_excel
from functools import lru_cache
from .dashboard_model import get_available_dates

# =============================================================
# HELPER FUNCTIONS
# =============================================================

def _get_inspector():
    """Get cached inspector instance."""
    global _inspector_cache
    if _inspector_cache is None:
        _inspector_cache = inspect(engine)
    return _inspector_cache

_inspector_cache = None

def _get_all_tables():
    """Get all derived tables, filtered by Excel stock list."""
    try:
        inspector = _get_inspector()
        all_tables = [t for t in inspector.get_table_names(schema='public') if t.endswith('_DERIVED')]
        
        # Filter by Excel stock list for performance
        allowed_stocks = get_stock_list_from_excel()
        if allowed_stocks:
            allowed_stocks_upper = [s.upper() for s in allowed_stocks]
            filtered_tables = []
            for table in all_tables:
                ticker = table.replace("TBL_", "").replace("_DERIVED", "")
                if ticker.upper() in allowed_stocks_upper:
                    filtered_tables.append(table)
            return filtered_tables
        return all_tables
    except Exception as e:
        print(f"[ERROR] _get_all_tables(): {e}")
        return []

def _get_prev_date(selected_date: str):
    """Get previous trading date."""
    try:
        dates = get_available_dates()
        if not dates:
            return None
        
        try:
            idx = dates.index(selected_date)
            if idx < len(dates) - 1:
                return dates[idx + 1]
        except ValueError:
            for i, date in enumerate(dates):
                if date < selected_date:
                    return date
        
        return None
    except Exception as e:
        print(f"[ERROR] _get_prev_date(): {e}")
        return None

# =============================================================
# OPTIMIZED SCREENER FUNCTION - SINGLE PASS
# =============================================================

def get_all_screener_data(selected_date: str):
    """
    OPTIMIZED: Fetch ALL screener data in a single pass through all tables.
    Returns a dict with all metrics pre-calculated.
    """
    try:
        prev_date = _get_prev_date(selected_date)
        if not prev_date:
            return {}
        
        all_tables = _get_all_tables()
        if not all_tables:
            return {}
        
        print(f"[INFO] Processing {len(all_tables)} tables for screener...")
        
        # Initialize result structure
        result = {
            'oi': {'CE': {'ALL': [], 'ITM': [], 'OTM': []}, 'PE': {'ALL': [], 'ITM': [], 'OTM': []}, 'FUT': {'ALL': []}},
            'moneyness': {'CE': {'ALL': [], 'ITM': [], 'OTM': []}, 'PE': {'ALL': [], 'ITM': [], 'OTM': []}, 'FUT': {'ALL': []}},
            'iv': {'CE': {'ALL': [], 'ITM': [], 'OTM': []}, 'PE': {'ALL': [], 'ITM': [], 'OTM': []}, 'FUT': {'ALL': []}}
        }
        
        # Process each table once
        for table_idx, table_name in enumerate(all_tables):
            try:
                ticker = table_name.replace("TBL_", "").replace("_DERIVED", "")
                
                # Single optimized query to get ALL data for this ticker
                query = text(f'''
                    WITH curr_data AS (
                        SELECT 
                            "StrkPric",
                            "OptnTp",
                            "UndrlygPric",
                            "OpnIntrst",
                            "ChngInOpnIntrst",
                            CASE 
                                WHEN "UndrlygPric" IS NOT NULL AND "StrkPric" IS NOT NULL AND "UndrlygPric" != 0
                                THEN (("UndrlygPric" - "StrkPric") / "UndrlygPric")
                                ELSE NULL
                            END AS moneyness_curr,
                            "iv" AS iv_curr
                        FROM public."{table_name}"
                        WHERE "BizDt" = :curr_date
                            AND ("OptnTp" = 'CE' OR "OptnTp" = 'PE' OR "OptnTp" IS NULL)
                    ),
                    prev_data AS (
                        SELECT 
                            "StrkPric",
                            "OptnTp",
                            "OpnIntrst" AS prev_oi,
                            CASE 
                                WHEN "UndrlygPric" IS NOT NULL AND "StrkPric" IS NOT NULL AND "UndrlygPric" != 0
                                THEN (("UndrlygPric" - "StrkPric") / "UndrlygPric")
                                ELSE NULL
                            END AS moneyness_prev,
                            "iv" AS iv_prev
                        FROM public."{table_name}"
                        WHERE "BizDt" = :prev_date
                            AND ("OptnTp" = 'CE' OR "OptnTp" = 'PE' OR "OptnTp" IS NULL)
                    )
                    SELECT 
                        c."StrkPric",
                        c."OptnTp",
                        c."UndrlygPric",
                        c."OpnIntrst" AS current_oi,
                        c."ChngInOpnIntrst" AS oi_change,
                        (c.moneyness_curr - COALESCE(p.moneyness_prev, 0)) AS moneyness_change,
                        (c.iv_curr - COALESCE(p.iv_prev, 0)) AS iv_change,
                        c.moneyness_curr,
                        c.iv_curr
                    FROM curr_data c
                    LEFT JOIN prev_data p ON 
                        (c."StrkPric" = p."StrkPric" OR (c."StrkPric" IS NULL AND p."StrkPric" IS NULL))
                        AND (c."OptnTp" = p."OptnTp" OR (c."OptnTp" IS NULL AND p."OptnTp" IS NULL))
                    WHERE c."UndrlygPric" IS NOT NULL
                ''')
                
                df = pd.read_sql(query, con=engine, params={
                    "curr_date": selected_date,
                    "prev_date": prev_date
                })
                
                if df.empty:
                    continue
                
                # Convert to numeric
                numeric_cols = ['StrkPric', 'UndrlygPric', 'current_oi', 'oi_change', 'moneyness_change', 'iv_change']
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Calculate strike_diff for ITM/OTM
                df['strike_diff'] = df['UndrlygPric'] - df['StrkPric']
                
                # Get latest underlying price for this ticker
                latest_underlying = df['UndrlygPric'].iloc[-1] if not df['UndrlygPric'].empty else df['UndrlygPric'].max()
                
                # Process FUTURES (OptnTp IS NULL)
                df_fut = df[df['OptnTp'].isna()].copy()
                if not df_fut.empty:
                    base_data_fut = {
                        'ticker': ticker,
                        'underlying_price': float(latest_underlying) if pd.notna(latest_underlying) else 0.0
                    }
                    
                    # OI changes for futures
                    if 'oi_change' in df_fut.columns:
                        oi_fut = df_fut['oi_change'].sum()
                        if pd.notna(oi_fut) and oi_fut != 0:
                            result['oi']['FUT']['ALL'].append({**base_data_fut, 'change': float(oi_fut)})
                    
                    # NOTE: Futures do NOT have Moneyness (no strike price for futures contracts)
                    # Moneyness is only for Options (CE/PE)
                    
                    # NOTE: Futures do NOT have IV (Implied Volatility)
                    # IV is calculated only for Options based on option premiums and Black-Scholes model
                    # Futures have different volatility metrics like Historical Volatility
                
                # Process each option type
                for opt_type in ['CE', 'PE']:
                    df_opt = df[df['OptnTp'] == opt_type].copy()
                    if df_opt.empty:
                        continue
                    
                    # Filter ITM/OTM
                    if opt_type == 'CE':
                        df_itm = df_opt[df_opt['strike_diff'] > 0].copy()
                        df_otm = df_opt[df_opt['strike_diff'] < 0].copy()
                    else:  # PE
                        df_itm = df_opt[df_opt['strike_diff'] < 0].copy()
                        df_otm = df_opt[df_opt['strike_diff'] > 0].copy()
                    
                    # Aggregate data
                    base_data = {
                        'ticker': ticker,
                        'underlying_price': float(latest_underlying) if pd.notna(latest_underlying) else 0.0
                    }
                    
                    # OI changes
                    if 'oi_change' in df_opt.columns:
                        oi_all = df_opt['oi_change'].sum()
                        oi_itm = df_itm['oi_change'].sum() if not df_itm.empty else 0
                        oi_otm = df_otm['oi_change'].sum() if not df_otm.empty else 0
                        
                        if pd.notna(oi_all):
                            result['oi'][opt_type]['ALL'].append({**base_data, 'change': float(oi_all)})
                        if pd.notna(oi_itm) and oi_itm != 0:
                            result['oi'][opt_type]['ITM'].append({**base_data, 'change': float(oi_itm)})
                        if pd.notna(oi_otm) and oi_otm != 0:
                            result['oi'][opt_type]['OTM'].append({**base_data, 'change': float(oi_otm)})
                    
                    # Moneyness changes
                    if 'moneyness_change' in df_opt.columns:
                        money_all = df_opt['moneyness_change'].sum()
                        money_itm = df_itm['moneyness_change'].sum() if not df_itm.empty else 0
                        money_otm = df_otm['moneyness_change'].sum() if not df_otm.empty else 0
                        
                        if pd.notna(money_all):
                            result['moneyness'][opt_type]['ALL'].append({**base_data, 'change': float(money_all)})
                        if pd.notna(money_itm) and money_itm != 0:
                            result['moneyness'][opt_type]['ITM'].append({**base_data, 'change': float(money_itm)})
                        if pd.notna(money_otm) and money_otm != 0:
                            result['moneyness'][opt_type]['OTM'].append({**base_data, 'change': float(money_otm)})
                    
                    # IV changes
                    if 'iv_change' in df_opt.columns:
                        iv_all = df_opt['iv_change'].sum()
                        iv_itm = df_itm['iv_change'].sum() if not df_itm.empty else 0
                        iv_otm = df_otm['iv_change'].sum() if not df_otm.empty else 0
                        
                        if pd.notna(iv_all):
                            result['iv'][opt_type]['ALL'].append({**base_data, 'change': float(iv_all)})
                        if pd.notna(iv_itm) and iv_itm != 0:
                            result['iv'][opt_type]['ITM'].append({**base_data, 'change': float(iv_itm)})
                        if pd.notna(iv_otm) and iv_otm != 0:
                            result['iv'][opt_type]['OTM'].append({**base_data, 'change': float(iv_otm)})
                
                if (table_idx + 1) % 10 == 0:
                    print(f"[INFO] Processed {table_idx + 1}/{len(all_tables)} tables...")
                    
            except Exception as e:
                print(f"[WARNING] Error processing {table_name}: {e}")
                continue
        
        # Sort all results
        for metric_type in ['oi', 'moneyness', 'iv']:
            for opt_type in ['CE', 'PE', 'FUT']:
                if opt_type == 'FUT':
                    # Futures only have ALL
                    result[metric_type][opt_type]['ALL'].sort(key=lambda x: x['change'], reverse=True)
                else:
                    for filter_type in ['ALL', 'ITM', 'OTM']:
                        result[metric_type][opt_type][filter_type].sort(key=lambda x: x['change'], reverse=True)
        
        print(f"[INFO] Screener data processing complete!")
        return result
        
    except Exception as e:
        print(f"[ERROR] get_all_screener_data(): {e}")
        import traceback
        traceback.print_exc()
        return {}

def get_screener_data(selected_date: str, metric_type: str, option_type: str, moneyness_filter: str = "ALL"):
    """
    Legacy function - now uses cached all_data.
    Kept for backward compatibility.
    """
    # This will be called 36 times, but we'll cache the result
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
