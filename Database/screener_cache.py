"""
SCREENER CACHE BUILDER
======================
Pre-calculates all screener data (OI, Moneyness, IV changes) and stores in a cache table
This is run daily as part of the update_database.py pipeline
Results are fetched in <0.5 seconds instead of 10+ seconds
"""

from sqlalchemy import create_engine, inspect, text
import pandas as pd
from urllib.parse import quote_plus
import json
from datetime import datetime

# Database config
db_user = 'postgres'
db_password = 'Gallop@3104'
db_host = 'localhost'
db_port = '5432'
db_name = 'BhavCopy_Database'
db_password_enc = quote_plus(db_password)
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}')

# =============================================================
# CREATE SCREENER CACHE TABLE
# =============================================================
def create_screener_cache_table():
    """Create the screener_cache table if it doesn't exist"""
    try:
        create_query = """
        CREATE TABLE IF NOT EXISTS public.screener_cache (
            id SERIAL PRIMARY KEY,
            cache_date DATE NOT NULL,
            metric_type VARCHAR(50) NOT NULL,
            option_type VARCHAR(10) NOT NULL,
            moneyness_filter VARCHAR(10) NOT NULL,
            rank INT NOT NULL,
            ticker VARCHAR(50) NOT NULL,
            underlying_price NUMERIC NOT NULL,
            change NUMERIC NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Create index for fast lookups
        CREATE INDEX IF NOT EXISTS idx_screener_cache_date_metric 
            ON public.screener_cache(cache_date, metric_type, option_type, moneyness_filter);
        
        CREATE INDEX IF NOT EXISTS idx_screener_cache_date 
            ON public.screener_cache(cache_date);
        """
        
        with engine.begin() as conn:
            conn.execute(text(create_query))
        
        print("✅ Screener cache table created/verified")
        return True
    
    except Exception as e:
        print(f"❌ Error creating screener cache table: {e}")
        return False

# =============================================================
# CALCULATE SCREENER DATA
# =============================================================
def get_prev_date(selected_date: str, all_dates: list):
    """Get previous trading date"""
    try:
        if not all_dates:
            return None
        
        try:
            idx = all_dates.index(selected_date)
            if idx < len(all_dates) - 1:
                return all_dates[idx + 1]
        except ValueError:
            for date in all_dates:
                if date < selected_date:
                    return date
        
        return None
    except Exception as e:
        print(f"❌ Error in get_prev_date: {e}")
        return None

def get_all_tables(engine):
    """Get all derived tables"""
    try:
        inspector = inspect(engine)
        all_tables = [t for t in inspector.get_table_names(schema='public') if t.endswith('_DERIVED')]
        return all_tables
    except Exception as e:
        print(f"❌ Error getting tables: {e}")
        return []

def calculate_screener_data_for_date(selected_date: str, all_dates: list):
    """
    Calculate all screener data for a given date
    Returns a list of rows to insert into screener_cache
    """
    try:
        prev_date = get_prev_date(selected_date, all_dates)
        if not prev_date:
            print(f"   ⚠️  No previous date found for {selected_date}")
            return []
        
        all_tables = get_all_tables(engine)
        if not all_tables:
            return []
        
        cache_rows = []
        
        # Initialize result structure to collect all data
        result = {
            'oi': {'CE': {'ALL': [], 'ITM': [], 'OTM': []}, 'PE': {'ALL': [], 'ITM': [], 'OTM': []}, 'FUT': {'ALL': []}},
            'moneyness': {'CE': {'ALL': [], 'ITM': [], 'OTM': []}, 'PE': {'ALL': [], 'ITM': [], 'OTM': []}, 'FUT': {'ALL': []}},
            'iv': {'CE': {'ALL': [], 'ITM': [], 'OTM': []}, 'PE': {'ALL': [], 'ITM': [], 'OTM': []}, 'FUT': {'ALL': []}}
        }
        
        # Process each table
        for table_name in all_tables:
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
                            "LastPric",
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
                            "LastPric" AS prev_ltp,
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
                        c."LastPric" AS current_ltp,
                        -- Calculate OI percentage change properly
                        CASE 
                            WHEN COALESCE(p.prev_oi, 0) != 0 
                            THEN ((c."OpnIntrst" - COALESCE(p.prev_oi, c."OpnIntrst")) / COALESCE(p.prev_oi, c."OpnIntrst")) * 100
                            ELSE 0
                        END AS oi_change,
                        (c."OpnIntrst" * c."LastPric") - (COALESCE(p.prev_oi, c."OpnIntrst") * COALESCE(p.prev_ltp, c."LastPric")) AS moneyness_change,
                        -- Calculate IV percentage change properly
                        CASE 
                            WHEN COALESCE(p.iv_prev, 0) != 0 
                            THEN ((c.iv_curr - COALESCE(p.iv_prev, c.iv_curr)) / COALESCE(p.iv_prev, c.iv_curr)) * 100
                            ELSE 0
                        END AS iv_change,
                        c.moneyness_curr,
                        c.iv_curr,
                        COALESCE(p.prev_oi, c."OpnIntrst") AS prev_oi,
                        COALESCE(p.prev_ltp, c."LastPric") AS prev_ltp
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
                numeric_cols = ['StrkPric', 'UndrlygPric', 'current_oi', 'current_ltp', 'oi_change', 'moneyness_change', 'iv_change', 'prev_oi', 'prev_ltp']
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
                    
                    # OI changes for futures - Calculate percentage change properly
                    if 'current_oi' in df_fut.columns and 'prev_oi' in df_fut.columns:
                        total_curr_oi_fut = df_fut['current_oi'].sum()
                        total_prev_oi_fut = df_fut['prev_oi'].sum()
                        oi_fut = ((total_curr_oi_fut - total_prev_oi_fut) / total_prev_oi_fut * 100) if total_prev_oi_fut != 0 else 0
                        if pd.notna(oi_fut):
                            result['oi']['FUT']['ALL'].append({**base_data_fut, 'change': float(oi_fut)})
                    
                    # Moneyness changes for futures - Calculate as percentage change of total value
                    if 'current_oi' in df_fut.columns and 'current_ltp' in df_fut.columns:
                        df_fut['curr_value'] = df_fut['current_oi'] * df_fut['current_ltp']
                        df_fut['prev_value'] = df_fut['prev_oi'] * df_fut['prev_ltp']
                        total_curr_value_fut = df_fut['curr_value'].sum()
                        total_prev_value_fut = df_fut['prev_value'].sum()
                        moneyness_fut = ((total_curr_value_fut - total_prev_value_fut) / total_prev_value_fut * 100) if total_prev_value_fut != 0 else 0
                        if pd.notna(moneyness_fut):
                            result['moneyness']['FUT']['ALL'].append({**base_data_fut, 'change': float(moneyness_fut)})
                    
                    # IV changes for futures - Weighted by OI
                    if 'iv_change' in df_fut.columns and 'current_oi' in df_fut.columns:
                        if df_fut['current_oi'].sum() != 0:
                            iv_fut = (df_fut['iv_change'] * df_fut['current_oi']).sum() / df_fut['current_oi'].sum()
                        else:
                            iv_fut = df_fut['iv_change'].mean()
                        if pd.notna(iv_fut):
                            result['iv']['FUT']['ALL'].append({**base_data_fut, 'change': float(iv_fut)})
                
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
                    
                    # OI changes - Calculate TOTAL OI change, then convert to percentage
                    if 'current_oi' in df_opt.columns and 'prev_oi' in df_opt.columns:
                        # Calculate total OI for current and previous dates
                        total_curr_oi_all = df_opt['current_oi'].sum()
                        total_prev_oi_all = df_opt['prev_oi'].sum()
                        total_curr_oi_itm = df_itm['current_oi'].sum() if not df_itm.empty else 0
                        total_prev_oi_itm = df_itm['prev_oi'].sum() if not df_itm.empty else 0
                        total_curr_oi_otm = df_otm['current_oi'].sum() if not df_otm.empty else 0
                        total_prev_oi_otm = df_otm['prev_oi'].sum() if not df_otm.empty else 0
                        
                        # Calculate percentage change properly: (current - previous) / previous * 100
                        oi_all = ((total_curr_oi_all - total_prev_oi_all) / total_prev_oi_all * 100) if total_prev_oi_all != 0 else 0
                        oi_itm = ((total_curr_oi_itm - total_prev_oi_itm) / total_prev_oi_itm * 100) if total_prev_oi_itm != 0 else 0
                        oi_otm = ((total_curr_oi_otm - total_prev_oi_otm) / total_prev_oi_otm * 100) if total_prev_oi_otm != 0 else 0
                        
                        if pd.notna(oi_all):
                            result['oi'][opt_type]['ALL'].append({**base_data, 'change': float(oi_all)})
                        if pd.notna(oi_itm) and oi_itm != 0:
                            result['oi'][opt_type]['ITM'].append({**base_data, 'change': float(oi_itm)})
                        if pd.notna(oi_otm) and oi_otm != 0:
                            result['oi'][opt_type]['OTM'].append({**base_data, 'change': float(oi_otm)})
                    
                    # Moneyness changes - Calculate as percentage change of total value (OI × Price)
                    if 'current_oi' in df_opt.columns and 'current_ltp' in df_opt.columns:
                        # Calculate total notional value (OI × LTP) for current and previous
                        df_opt['curr_value'] = df_opt['current_oi'] * df_opt['current_ltp']
                        df_opt['prev_value'] = df_opt['prev_oi'] * df_opt['prev_ltp']
                        
                        # Calculate for ALL
                        total_curr_value_all = df_opt['curr_value'].sum()
                        total_prev_value_all = df_opt['prev_value'].sum()
                        money_all = ((total_curr_value_all - total_prev_value_all) / total_prev_value_all * 100) if total_prev_value_all != 0 else 0
                        
                        # Calculate for ITM
                        if not df_itm.empty:
                            df_itm['curr_value'] = df_itm['current_oi'] * df_itm['current_ltp']
                            df_itm['prev_value'] = df_itm['prev_oi'] * df_itm['prev_ltp']
                            total_curr_value_itm = df_itm['curr_value'].sum()
                            total_prev_value_itm = df_itm['prev_value'].sum()
                            money_itm = ((total_curr_value_itm - total_prev_value_itm) / total_prev_value_itm * 100) if total_prev_value_itm != 0 else 0
                        else:
                            money_itm = 0
                        
                        # Calculate for OTM
                        if not df_otm.empty:
                            df_otm['curr_value'] = df_otm['current_oi'] * df_otm['current_ltp']
                            df_otm['prev_value'] = df_otm['prev_oi'] * df_otm['prev_ltp']
                            total_curr_value_otm = df_otm['curr_value'].sum()
                            total_prev_value_otm = df_otm['prev_value'].sum()
                            money_otm = ((total_curr_value_otm - total_prev_value_otm) / total_prev_value_otm * 100) if total_prev_value_otm != 0 else 0
                        else:
                            money_otm = 0
                        
                        if pd.notna(money_all):
                            result['moneyness'][opt_type]['ALL'].append({**base_data, 'change': float(money_all)})
                        if pd.notna(money_itm) and money_itm != 0:
                            result['moneyness'][opt_type]['ITM'].append({**base_data, 'change': float(money_itm)})
                        if pd.notna(money_otm) and money_otm != 0:
                            result['moneyness'][opt_type]['OTM'].append({**base_data, 'change': float(money_otm)})
                    
                    # IV changes - Calculate weighted average IV change
                    if 'iv_change' in df_opt.columns and 'current_oi' in df_opt.columns:
                        # Weight IV change by OI to get more meaningful result
                        if df_opt['current_oi'].sum() != 0:
                            iv_all = (df_opt['iv_change'] * df_opt['current_oi']).sum() / df_opt['current_oi'].sum()
                        else:
                            iv_all = df_opt['iv_change'].mean()
                        
                        if not df_itm.empty and df_itm['current_oi'].sum() != 0:
                            iv_itm = (df_itm['iv_change'] * df_itm['current_oi']).sum() / df_itm['current_oi'].sum()
                        else:
                            iv_itm = df_itm['iv_change'].mean() if not df_itm.empty else 0
                        
                        if not df_otm.empty and df_otm['current_oi'].sum() != 0:
                            iv_otm = (df_otm['iv_change'] * df_otm['current_oi']).sum() / df_otm['current_oi'].sum()
                        else:
                            iv_otm = df_otm['iv_change'].mean() if not df_otm.empty else 0
                        
                        if pd.notna(iv_all):
                            result['iv'][opt_type]['ALL'].append({**base_data, 'change': float(iv_all)})
                        if pd.notna(iv_itm) and iv_itm != 0:
                            result['iv'][opt_type]['ITM'].append({**base_data, 'change': float(iv_itm)})
                        if pd.notna(iv_otm) and iv_otm != 0:
                            result['iv'][opt_type]['OTM'].append({**base_data, 'change': float(iv_otm)})
            
            except Exception as e:
                print(f"      ⚠️  Error processing {table_name}: {str(e)[:50]}")
                continue
        
        # Sort all results and build cache rows
        for metric_type in ['oi', 'moneyness', 'iv']:
            for opt_type in ['CE', 'PE', 'FUT']:
                if opt_type == 'FUT':
                    result[metric_type][opt_type]['ALL'].sort(key=lambda x: x['change'], reverse=True)
                    # Add all results (gainers)
                    for rank, item in enumerate(result[metric_type][opt_type]['ALL'][:10], 1):
                        cache_rows.append({
                            'cache_date': selected_date,
                            'metric_type': metric_type,
                            'option_type': opt_type,
                            'moneyness_filter': 'ALL',
                            'rank': rank,
                            'ticker': item['ticker'],
                            'underlying_price': item['underlying_price'],
                            'change': item['change']
                        })
                    
                    # Add losers (reversed)
                    losers = sorted(result[metric_type][opt_type]['ALL'], key=lambda x: x['change'])
                    for rank, item in enumerate(losers[:10], 1):
                        cache_rows.append({
                            'cache_date': selected_date,
                            'metric_type': metric_type,
                            'option_type': opt_type,
                            'moneyness_filter': 'ALL_LOSERS',
                            'rank': rank,
                            'ticker': item['ticker'],
                            'underlying_price': item['underlying_price'],
                            'change': item['change']
                        })
                else:
                    for filter_type in ['ALL', 'ITM', 'OTM']:
                        result[metric_type][opt_type][filter_type].sort(key=lambda x: x['change'], reverse=True)
                        
                        # Add gainers
                        for rank, item in enumerate(result[metric_type][opt_type][filter_type][:10], 1):
                            cache_rows.append({
                                'cache_date': selected_date,
                                'metric_type': metric_type,
                                'option_type': opt_type,
                                'moneyness_filter': filter_type,
                                'rank': rank,
                                'ticker': item['ticker'],
                                'underlying_price': item['underlying_price'],
                                'change': item['change']
                            })
                        
                        # Add losers
                        losers = sorted(result[metric_type][opt_type][filter_type], key=lambda x: x['change'])
                        for rank, item in enumerate(losers[:10], 1):
                            cache_rows.append({
                                'cache_date': selected_date,
                                'metric_type': metric_type,
                                'option_type': opt_type,
                                'moneyness_filter': f"{filter_type}_LOSERS",
                                'rank': rank,
                                'ticker': item['ticker'],
                                'underlying_price': item['underlying_price'],
                                'change': item['change']
                            })
        
        return cache_rows
    
    except Exception as e:
        print(f"❌ Error calculating screener data: {e}")
        import traceback
        traceback.print_exc()
        return []

# =============================================================
# PRECALCULATE ALL SCREENER DATA
# =============================================================
def precalculate_screener_cache():
    """
    Pre-calculate screener data for all new dates and store in cache table
    Called from update_database.py after Greeks calculation
    """
    try:
        print("\n" + "="*80)
        print("SCREENER CACHE: PRE-CALCULATING DATA FOR FAST PAGE LOADS")
        print("="*80 + "\n")
        
        # Create cache table if it doesn't exist
        if not create_screener_cache_table():
            return False
        
        # Get all available dates
        inspector = inspect(engine)
        base_tables = [t for t in inspector.get_table_names() if t.startswith("TBL_") and not t.endswith("_DERIVED")]
        
        if not base_tables:
            print("⚠️  No base tables found")
            return False
        
        sample_table = next((t for t in ['TBL_NIFTY', 'TBL_BANKNIFTY'] if t in base_tables), base_tables[0])
        
        # Get all dates
        query_base = text(f'SELECT DISTINCT "BizDt" FROM public."{sample_table}" WHERE "BizDt" IS NOT NULL ORDER BY "BizDt" DESC')
        base_dates = pd.read_sql(query_base, engine)
        
        if base_dates.empty:
            print("⚠️  No dates found in database")
            return False
        
        all_dates = [str(d.date()) if hasattr(d, 'date') else str(d) for d in base_dates['BizDt'].tolist()]
        
        # Get already cached dates
        try:
            query_cache = text('SELECT DISTINCT cache_date FROM public.screener_cache ORDER BY cache_date DESC')
            cached_dates = pd.read_sql(query_cache, engine)
            cached_dates_set = set(str(d.date()) if hasattr(d, 'date') else str(d) for d in cached_dates['cache_date'].tolist())
        except:
            cached_dates_set = set()
        
        # Find new dates to process
        new_dates = [d for d in all_dates if d not in cached_dates_set]
        
        if not new_dates:
            print("✅ All dates already cached!")
            return True
        
        print(f"📅 Found {len(new_dates)} new date(s) to cache\n")
        
        # Process each new date
        total_rows_inserted = 0
        for date_idx, selected_date in enumerate(new_dates, 1):
            print(f"  [{date_idx}/{len(new_dates)}] {selected_date}...", end=" ")
            
            cache_rows = calculate_screener_data_for_date(selected_date, all_dates)
            
            if cache_rows:
                # Insert into database
                df_cache = pd.DataFrame(cache_rows)
                df_cache.to_sql('screener_cache', con=engine, if_exists='append', index=False)
                print(f"✅ ({len(cache_rows)} rows)")
                total_rows_inserted += len(cache_rows)
            else:
                print("⚠️  (0 rows)")
        
        print(f"\n✅ Screener cache pre-calculation complete!")
        print(f"   Total rows inserted: {total_rows_inserted}")
        return True
    
    except Exception as e:
        print(f"❌ Error in precalculate_screener_cache: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    precalculate_screener_cache()
