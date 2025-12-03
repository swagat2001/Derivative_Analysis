"""
PRE-CALCULATE DASHBOARD DATA WITH DATABASE RSI(14)
===================================================

DATABASE-ONLY APPROACH:
1. RSI(14) calculated from Database using pandas_ta
2. Underlying Price from Database  
3. All other metrics from Database
4. AUTO-APPENDS only NEW dates (no manual clearing needed)
"""

from sqlalchemy import create_engine, inspect, text
import pandas as pd
from urllib.parse import quote_plus
import json

# Import pandas_ta for RSI calculation
try:
    import pandas_ta as ta
    PANDAS_TA_AVAILABLE = True
    print("✅ pandas_ta available for RSI(14) calculation")
except ImportError:
    PANDAS_TA_AVAILABLE = False
    print("⚠️ pandas_ta not installed - RSI will be skipped")
    print("Install with: pip install pandas_ta")

# Database config
db_user = 'postgres'
db_password = 'Gallop@3104'
db_host = 'localhost'
db_port = '5432'
db_name = 'BhavCopy_Database'
db_password_enc = quote_plus(db_password)
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}')

def calculate_rsi_from_database(table_name, current_date, all_dates, rsi_period=14):
    """
    Calculate RSI(14) from database using pandas_ta
    
    Parameters:
    - table_name: The ticker's derived table name (e.g., 'TBL_NIFTY_DERIVED')
    - current_date: The current business date
    - all_dates: List of all available dates (sorted descending)
    - rsi_period: RSI calculation period (default 14)
    
    Returns:
    - RSI value (float) or None if insufficient data
    """
    if not PANDAS_TA_AVAILABLE:
        return None
    
    try:
        curr_idx = all_dates.index(current_date)
        needed_days = max(50, rsi_period + 10)
        end_idx = min(curr_idx + needed_days, len(all_dates))
        date_range = all_dates[curr_idx:end_idx]
        
        if len(date_range) < rsi_period:
            print(f"      ⚠️ {table_name}: Not enough dates ({len(date_range)} < {rsi_period})")
            return None
        
        date_placeholders = ','.join([f"'{d}'" for d in date_range])
        query = f'''
            SELECT "BizDt", "ClsPric" 
            FROM "{table_name}" 
            WHERE "BizDt" IN ({date_placeholders})
            AND "ClsPric" IS NOT NULL
            ORDER BY "BizDt" ASC
        '''
        
        df = pd.read_sql(text(query), engine)
        
        if df.empty or len(df) < rsi_period:
            print(f"      ⚠️ {table_name}: Insufficient data ({len(df) if not df.empty else 0} rows)")
            return None
        
        df['ClsPric'] = pd.to_numeric(df['ClsPric'], errors='coerce')
        df = df.dropna(subset=['ClsPric'])
        
        if len(df) < rsi_period:
            print(f"      ⚠️ {table_name}: After cleaning ({len(df)} < {rsi_period})")
            return None
        
        # Calculate RSI(14) using pandas_ta
        rsi_series = ta.rsi(df['ClsPric'], length=rsi_period)
        current_rsi = rsi_series.iloc[-1] if not rsi_series.empty else None
        
        if current_rsi is None or pd.isna(current_rsi):
            print(f"      ⚠️ {table_name}: RSI calculation returned None/NaN")
            return None
        
        return round(float(current_rsi), 2)
        
    except Exception as e:
        print(f"      ❌ {table_name}: Error - {str(e)[:50]}")
        return None

def get_available_dates():
    try:
        inspector = inspect(engine)
        tables = [t for t in inspector.get_table_names() if t.endswith("_DERIVED")]
        if not tables:
            return []
        sample = next((t for t in ['TBL_NIFTY_DERIVED', 'TBL_BANKNIFTY_DERIVED'] if t in tables), tables[0])
        result = pd.read_sql(text(f'SELECT DISTINCT "BizDt" FROM "{sample}" WHERE "BizDt" IS NOT NULL ORDER BY "BizDt" DESC'), engine)
        return [d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d) for d in result['BizDt']] if not result.empty else []
    except:
        return []

def get_prev_date(curr, dates):
    try:
        i = dates.index(curr)
        return dates[i+1] if i+1 < len(dates) else None
    except:
        return None

def calculate_and_store_data(curr_date, prev_date):
    """Calculate all dashboard data with DATABASE RSI(14)"""
    inspector = inspect(engine)
    tables = [t for t in inspector.get_table_names() if t.endswith("_DERIVED")]
    all_dates = get_available_dates()
    
    total_data, otm_data, itm_data = [], [], []

    for table in tables:
        try:
            ticker = table.replace("TBL_", "").replace("_DERIVED", "")
            query = f'SELECT "BizDt","TckrSymb","StrkPric","OptnTp","UndrlygPric","delta","vega","strike_diff","TtlTrfVal","OpnIntrst","LastPric","ClsPric" FROM "{table}" WHERE "BizDt" IN (:c,:p) AND "OptnTp" IN (\'CE\',\'PE\') AND "StrkPric" IS NOT NULL'
            df = pd.read_sql(text(query), engine, params={"c": curr_date, "p": prev_date})
            
            if df.empty:
                continue
            
            for col in ['StrkPric','UndrlygPric','delta','vega','strike_diff','TtlTrfVal','OpnIntrst','LastPric','ClsPric']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            df['BizDt_str'] = df['BizDt'].astype(str)
            dfc = df[df['BizDt_str'] == curr_date].copy()
            dfp = df[df['BizDt_str'] == prev_date].copy()
            
            if dfc.empty or dfp.empty:
                continue
            
            # Get closing price from DATABASE
            closing_price = float(dfc['UndrlygPric'].iloc[0] if 'UndrlygPric' in dfc.columns else dfc['ClsPric'].iloc[0])
            
            # Calculate RSI(14) from DATABASE using pandas_ta
            rsi_value = calculate_rsi_from_database(table, curr_date, all_dates, rsi_period=14)
            
            row_total = {'stock': ticker, 'closing_price': float(closing_price), 'rsi': rsi_value}
            row_otm = {'stock': ticker, 'closing_price': float(closing_price), 'rsi': rsi_value}
            row_itm = {'stock': ticker, 'closing_price': float(closing_price), 'rsi': rsi_value}
            
            # Calculate all other metrics
            for opt_type, prefix in [('CE', 'call'), ('PE', 'put')]:
                dc = dfc[dfc['OptnTp']==opt_type].copy()
                dp = dfp[dfp['OptnTp']==opt_type].copy()
                
                if not dc.empty and not dp.empty:
                    dm = pd.merge(dc, dp, on=['TckrSymb','StrkPric'], suffixes=('_c','_p'), how='inner')
                    
                    if not dm.empty:
                        dm['delta_chg'] = dm['delta_c'] - dm['delta_p']
                        dm['vega_chg'] = dm['vega_c'] - dm['vega_p']
                        dm['tradval_chg'] = (dm['OpnIntrst_c'] * dm['LastPric_c']) - (dm['OpnIntrst_p'] * dm['LastPric_p'])
                        dm['moneyness_curr'] = (dm['UndrlygPric_c'] - dm['StrkPric']) / dm['UndrlygPric_c']
                        dm['moneyness_prev'] = (dm['UndrlygPric_p'] - dm['StrkPric']) / dm['UndrlygPric_p']
                        dm['money_chg'] = dm['moneyness_curr'] - dm['moneyness_prev']
                        
                        dv = dm[(dm['delta_c'].notna())&(dm['delta_p'].notna())&(dm['delta_c']!=0)&(dm['delta_p']!=0)].copy()
                        
                        if not dv.empty:
                            for metric, data_col in [('delta', 'delta_chg'), ('vega', 'vega_chg')]:
                                df_pos = dv[dv[data_col]>0] if metric=='vega' else dv
                                df_neg = dv[dv[data_col]<0] if metric=='vega' else dv
                                
                                if not df_pos.empty:
                                    idx = df_pos[data_col].idxmax()
                                    row_total[f'{prefix}_{metric}_pos_strike'] = f"{dv.loc[idx,'StrkPric']:.0f}"
                                    row_total[f'{prefix}_{metric}_pos_pct'] = f"{dv.loc[idx,data_col]*100:.2f}"
                                else:
                                    row_total[f'{prefix}_{metric}_pos_strike'] = 'N/A'
                                    row_total[f'{prefix}_{metric}_pos_pct'] = '0.00'
                                
                                if not df_neg.empty:
                                    idx = df_neg[data_col].idxmin()
                                    row_total[f'{prefix}_{metric}_neg_strike'] = f"{dv.loc[idx,'StrkPric']:.0f}"
                                    row_total[f'{prefix}_{metric}_neg_pct'] = f"{dv.loc[idx,data_col]*100:.2f}"
                                else:
                                    row_total[f'{prefix}_{metric}_neg_strike'] = 'N/A'
                                    row_total[f'{prefix}_{metric}_neg_pct'] = '0.00'
                            
                            row_total[f'{prefix}_total_tradval'] = float(dv['tradval_chg'].sum())
                            row_total[f'{prefix}_total_money'] = float(dv['money_chg'].sum())
                            
                            for cond_type, row_dict in [('OTM', row_otm), ('ITM', row_itm)]:
                                if cond_type == 'OTM':
                                    cond = dv['strike_diff_c']<0 if opt_type=='CE' else dv['strike_diff_c']>0
                                else:
                                    cond = dv['strike_diff_c']>0 if opt_type=='CE' else dv['strike_diff_c']<0
                                
                                dsub = dv[cond]
                                if not dsub.empty:
                                    for metric, data_col in [('delta', 'delta_chg'), ('vega', 'vega_chg')]:
                                        df_pos = dsub[dsub[data_col]>0] if metric=='vega' else dsub
                                        df_neg = dsub[dsub[data_col]<0] if metric=='vega' else dsub
                                        
                                        if not df_pos.empty:
                                            idx = df_pos[data_col].idxmax()
                                            row_dict[f'{prefix}_{metric}_pos_strike'] = f"{dsub.loc[idx,'StrkPric']:.0f}"
                                            row_dict[f'{prefix}_{metric}_pos_pct'] = f"{dsub.loc[idx,data_col]*100:.2f}"
                                        else:
                                            row_dict[f'{prefix}_{metric}_pos_strike'] = 'N/A'
                                            row_dict[f'{prefix}_{metric}_pos_pct'] = '0.00'
                                        
                                        if not df_neg.empty:
                                            idx = df_neg[data_col].idxmin()
                                            row_dict[f'{prefix}_{metric}_neg_strike'] = f"{dsub.loc[idx,'StrkPric']:.0f}"
                                            row_dict[f'{prefix}_{metric}_neg_pct'] = f"{dsub.loc[idx,data_col]*100:.2f}"
                                        else:
                                            row_dict[f'{prefix}_{metric}_neg_strike'] = 'N/A'
                                            row_dict[f'{prefix}_{metric}_neg_pct'] = '0.00'
                                    
                                    row_dict[f'{prefix}_total_tradval'] = float(dsub['tradval_chg'].sum())
                                    row_dict[f'{prefix}_total_money'] = float(dsub['money_chg'].sum())
                                else:
                                    for metric in ['delta', 'vega']:
                                        for sign in ['pos', 'neg']:
                                            row_dict[f'{prefix}_{metric}_{sign}_strike'] = 'N/A'
                                            row_dict[f'{prefix}_{metric}_{sign}_pct'] = '0.00'
                                    row_dict[f'{prefix}_total_tradval'] = 0
                                    row_dict[f'{prefix}_total_money'] = 0
                        else:
                            for r in [row_total, row_otm, row_itm]:
                                for metric in ['delta', 'vega']:
                                    for sign in ['pos', 'neg']:
                                        r[f'{prefix}_{metric}_{sign}_strike'] = 'N/A'
                                        r[f'{prefix}_{metric}_{sign}_pct'] = '0.00'
                                r[f'{prefix}_total_tradval'] = 0
                                r[f'{prefix}_total_money'] = 0
                    else:
                        for r in [row_total, row_otm, row_itm]:
                            for metric in ['delta', 'vega']:
                                for sign in ['pos', 'neg']:
                                    r[f'{prefix}_{metric}_{sign}_strike'] = 'N/A'
                                    r[f'{prefix}_{metric}_{sign}_pct'] = '0.00'
                            r[f'{prefix}_total_tradval'] = 0
                            r[f'{prefix}_total_money'] = 0
                else:
                    for r in [row_total, row_otm, row_itm]:
                        for metric in ['delta', 'vega']:
                            for sign in ['pos', 'neg']:
                                r[f'{prefix}_{metric}_{sign}_strike'] = 'N/A'
                                r[f'{prefix}_{metric}_{sign}_pct'] = '0.00'
                        r[f'{prefix}_total_tradval'] = 0
                        r[f'{prefix}_total_money'] = 0
            
            if len(row_otm) > 2:
                total_data.append(row_total)
                otm_data.append(row_otm)
                itm_data.append(row_itm)
        except:
            pass
    
    return total_data, otm_data, itm_data

def create_precalculated_tables():
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS options_dashboard_cache (
        id SERIAL PRIMARY KEY,
        biz_date DATE NOT NULL,
        prev_date DATE NOT NULL,
        moneyness_type VARCHAR(10) NOT NULL,
        data_json TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(biz_date, prev_date, moneyness_type)
    );
    CREATE INDEX IF NOT EXISTS idx_dashboard_cache_dates ON options_dashboard_cache(biz_date, prev_date, moneyness_type);
    """
    with engine.begin() as conn:
        conn.execute(text(create_table_sql))
    print("✅ Cache table ready")

def precalculate_all_dates():
    """AUTO-APPEND only NEW dates to cache"""
    print("\n" + "="*80)
    print("AUTO-APPENDING NEW DATES WITH RSI(14)")
    print("="*80 + "\n")
    
    dates = get_available_dates()
    if not dates:
        print("❌ No dates found in database")
        return
    
    # Get existing cached dates
    try:
        existing_df = pd.read_sql(text("SELECT DISTINCT biz_date FROM options_dashboard_cache"), engine)
        existing_dates = set(pd.to_datetime(existing_df['biz_date']).dt.strftime('%Y-%m-%d'))
    except:
        existing_dates = set()
    
    # Find NEW dates only
    new_dates = [d for d in dates if d not in existing_dates]
    
    if not new_dates:
        print("✅ No new dates to process - cache is up to date!")
        return
    
    print(f"📅 Total dates in database: {len(dates)}")
    print(f"📂 Already cached: {len(existing_dates)}")
    print(f"🆕 NEW dates to process: {len(new_dates)}\n")
    
    processed = 0
    
    for i, curr_date in enumerate(new_dates, 1):
        prev_date = get_prev_date(curr_date, dates)
        if not prev_date:
            continue
        
        print(f"\n[{i}/{len(new_dates)}] {curr_date}:")
        
        try:
            total, otm, itm = calculate_and_store_data(curr_date, prev_date)
            
            if total and otm and itm:
                insert_sql = """
                INSERT INTO options_dashboard_cache (biz_date, prev_date, moneyness_type, data_json)
                VALUES (:curr, :prev, :type, :data)
                ON CONFLICT (biz_date, prev_date, moneyness_type)
                DO UPDATE SET data_json = EXCLUDED.data_json, created_at = CURRENT_TIMESTAMP
                """
                with engine.begin() as conn:
                    conn.execute(text(insert_sql), {"curr": curr_date, "prev": prev_date, "type": "TOTAL", "data": json.dumps(total)})
                    conn.execute(text(insert_sql), {"curr": curr_date, "prev": prev_date, "type": "OTM", "data": json.dumps(otm)})
                    conn.execute(text(insert_sql), {"curr": curr_date, "prev": prev_date, "type": "ITM", "data": json.dumps(itm)})
                processed += 1
                print(f"  ✅ Cached {len(total)} tickers")
            else:
                print(f"  ⚠️ No data")
        except Exception as e:
            print(f"  ❌ Error: {str(e)[:50]}")
    
    print(f"\n" + "="*80)
    print(f"✅ COMPLETE: {processed} NEW dates added to cache")
    print("="*80)

if __name__ == '__main__':
    print("\n" + "="*80)
    print("DASHBOARD DATA PRE-CALCULATOR - AUTO-APPEND MODE")
    print("="*80)
    print("\nSYSTEM:")
    print("  ✅ RSI(14) using pandas_ta")
    print("  ✅ Underlying Price from Database")
    print("  ✅ AUTO-APPENDS only NEW dates")
    print("  ✅ No manual clearing needed!")
    print("\n" + "="*80)
    
    if PANDAS_TA_AVAILABLE:
        print("\n✅ pandas_ta enabled")
    else:
        print("\n⚠️ pandas_ta not available - Install: pip install pandas_ta")
    
    print("\n" + "="*80)
    input("\nPress Enter to start...")
    
    create_precalculated_tables()
    precalculate_all_dates()
    
    print("\n✅ Done! Run dashboard_server.py")
    input("\nPress Enter to exit...")
