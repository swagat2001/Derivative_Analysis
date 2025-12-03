"""
TECHNICAL SCREENER CACHE BUILDER
================================
Pre-calculates technical indicators (RSI, MACD, SMA, Bollinger Bands, ADX)
Stores in technical_screener_cache table for fast retrieval

INCREMENTAL MODE: Only processes NEW dates, never drops existing data
"""

from sqlalchemy import create_engine, inspect, text
import pandas as pd
import numpy as np
from urllib.parse import quote_plus
from datetime import datetime

# Database config
db_user = 'postgres'
db_password = 'Gallop@3104'
db_host = 'localhost'
db_port = '5432'
db_name = 'BhavCopy_Database'
db_password_enc = quote_plus(db_password)
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}')


def create_technical_screener_table():
    """Create the technical_screener_cache table if not exists (NO DROP)"""
    create_query = """
    CREATE TABLE IF NOT EXISTS public.technical_screener_cache (
        id SERIAL PRIMARY KEY,
        cache_date DATE NOT NULL,
        ticker VARCHAR(50) NOT NULL,
        underlying_price NUMERIC,
        
        -- RSI
        rsi_14 NUMERIC,
        rsi_above_80 BOOLEAN DEFAULT FALSE,
        rsi_60_80 BOOLEAN DEFAULT FALSE,
        rsi_40_60 BOOLEAN DEFAULT FALSE,
        rsi_20_40 BOOLEAN DEFAULT FALSE,
        rsi_below_20 BOOLEAN DEFAULT FALSE,
        
        -- MACD
        macd NUMERIC,
        macd_signal NUMERIC,
        macd_histogram NUMERIC,
        macd_pos_cross BOOLEAN DEFAULT FALSE,
        macd_neg_cross BOOLEAN DEFAULT FALSE,
        
        -- SMA
        sma_50 NUMERIC,
        sma_200 NUMERIC,
        above_50_sma BOOLEAN DEFAULT FALSE,
        above_200_sma BOOLEAN DEFAULT FALSE,
        below_50_sma BOOLEAN DEFAULT FALSE,
        below_200_sma BOOLEAN DEFAULT FALSE,
        dist_from_50sma_pct NUMERIC,
        dist_from_200sma_pct NUMERIC,
        
        -- Bollinger Bands
        bb_upper NUMERIC,
        bb_middle NUMERIC,
        bb_lower NUMERIC,
        bb_width NUMERIC,
        
        -- ADX
        adx_14 NUMERIC,
        strong_trend BOOLEAN DEFAULT FALSE,
        
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(cache_date, ticker)
    );
    
    CREATE INDEX IF NOT EXISTS idx_tech_cache_date ON public.technical_screener_cache(cache_date);
    CREATE INDEX IF NOT EXISTS idx_tech_cache_ticker ON public.technical_screener_cache(ticker);
    CREATE INDEX IF NOT EXISTS idx_tech_cache_rsi ON public.technical_screener_cache(rsi_14);
    CREATE INDEX IF NOT EXISTS idx_tech_cache_adx ON public.technical_screener_cache(adx_14);
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(create_query))
        print("✓ technical_screener_cache table ready")
    except Exception as e:
        print(f"✗ Error creating table: {e}")


def get_cached_dates():
    """Get dates already in cache"""
    try:
        q = text("SELECT DISTINCT cache_date FROM public.technical_screener_cache ORDER BY cache_date")
        df = pd.read_sql(q, engine)
        return set(d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d) for d in df['cache_date'])
    except:
        return set()


def get_derived_tables():
    """Get all DERIVED tables"""
    inspector = inspect(engine)
    tables = [t for t in inspector.get_table_names() if t.endswith('_DERIVED')]
    return sorted(tables)


def get_available_dates():
    """Get all available dates from database"""
    tables = get_derived_tables()
    if not tables:
        return []
    sample = tables[0]
    q = text(f'SELECT DISTINCT "BizDt" FROM "{sample}" WHERE "BizDt" IS NOT NULL ORDER BY "BizDt" ASC')
    try:
        df = pd.read_sql(q, engine)
        return [d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d) for d in df['BizDt']]
    except Exception as e:
        print(f"Error getting dates: {e}")
        return []


# Technical indicator calculations
def calculate_rsi(close, period=14):
    delta = close.diff()
    gain = delta.where(delta > 0, 0)
    loss = (-delta).where(delta < 0, 0)
    
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    
    rs = avg_gain / avg_loss.replace(0, np.inf)
    rsi = 100 - (100 / (1 + rs))
    return rsi


def calculate_macd(close, fast=12, slow=26, signal=9):
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram


def calculate_sma(close, period):
    return close.rolling(window=period, min_periods=period).mean()


def calculate_bollinger_bands(close, period=20, std_dev=2):
    sma = close.rolling(window=period, min_periods=period).mean()
    rolling_std = close.rolling(window=period, min_periods=period).std()
    
    upper = sma + (rolling_std * std_dev)
    lower = sma - (rolling_std * std_dev)
    width = (upper - lower) / sma * 100
    return upper, sma, lower, width


def calculate_adx(close, period=14):
    high = close * 1.001
    low = close * 0.999
    
    plus_dm = high.diff()
    minus_dm = (-low).diff()
    
    plus_dm = plus_dm.where(plus_dm > 0, 0)
    minus_dm = minus_dm.where(minus_dm > 0, 0)
    
    tr = close.diff().abs()
    
    atr = tr.rolling(window=period, min_periods=period).mean()
    plus_di = 100 * (plus_dm.rolling(window=period, min_periods=period).mean() / atr.replace(0, np.inf))
    minus_di = 100 * (minus_dm.rolling(window=period, min_periods=period).mean() / atr.replace(0, np.inf))
    
    dx = 100 * ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.inf))
    adx = dx.rolling(window=period, min_periods=period).mean()
    return adx


def precalculate_technical_screener_cache():
    """
    INCREMENTAL: Only processes NEW dates, appends to existing cache
    """
    print("\n" + "="*70)
    print("TECHNICAL SCREENER CACHE BUILDER (INCREMENTAL MODE)")
    print("="*70)
    
    # Create table if needed (NO DROP)
    create_technical_screener_table()
    
    # Get dates
    all_dates = get_available_dates()
    cached_dates = get_cached_dates()
    tables = get_derived_tables()
    
    if not all_dates or not tables:
        print("✗ No data available")
        return
    
    # Find NEW dates only
    new_dates = set(d for d in all_dates if d not in cached_dates)
    
    print(f"📅 Total dates in database: {len(all_dates)}")
    print(f"📂 Already cached: {len(cached_dates)}")
    print(f"🆕 NEW dates to process: {len(new_dates)}")
    print(f"📊 Tickers: {len(tables)}")
    
    if not new_dates:
        print("\n✅ Cache is up to date! No new dates to process.")
        return
    
    min_days = 50
    
    if len(all_dates) < min_days:
        print(f"✗ Need at least {min_days} days of data, only have {len(all_dates)}")
        return
    
    # Process each ticker
    all_cache_rows = []
    
    for ticker_idx, table in enumerate(tables, 1):
        ticker = table.replace('TBL_', '').replace('_DERIVED', '')
        print(f"\n[{ticker_idx}/{len(tables)}] {ticker}...", end=" ", flush=True)
        
        try:
            # Get all price history for this ticker
            q = text(f'''
                SELECT DISTINCT "BizDt" AS date, "UndrlygPric" AS close
                FROM "{table}"
                WHERE "BizDt" IS NOT NULL AND "FinInstrmTp" = 'STF'
                ORDER BY "BizDt"
            ''')
            df = pd.read_sql(q, engine)
            
            if df.empty or len(df) < min_days:
                print(f"⚠ Only {len(df)} days", end="")
                continue
            
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').drop_duplicates(subset=['date'], keep='first')
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
            df = df.dropna(subset=['close'])
            df = df.set_index('date')
            
            if len(df) < min_days:
                print(f"⚠ Only {len(df)} valid days", end="")
                continue
            
            close = df['close']
            
            # Calculate indicators for entire history
            rsi = calculate_rsi(close, 14)
            macd_line, signal_line, histogram = calculate_macd(close, 12, 26, 9)
            sma_50 = calculate_sma(close, 50)
            sma_200 = calculate_sma(close, 200)
            bb_upper, bb_middle, bb_lower, bb_width = calculate_bollinger_bands(close, 20, 2)
            adx = calculate_adx(close, 14)
            
            # Create cache rows for NEW dates only
            ticker_rows = 0
            for i in range(len(df)):
                date_str = df.index[i].strftime('%Y-%m-%d')
                
                # ONLY process NEW dates
                if date_str not in new_dates:
                    continue
                
                latest_close = close.iloc[i]
                latest_rsi = rsi.iloc[i] if i < len(rsi) else None
                latest_macd = macd_line.iloc[i] if i < len(macd_line) else None
                latest_signal = signal_line.iloc[i] if i < len(signal_line) else None
                latest_histogram = histogram.iloc[i] if i < len(histogram) else None
                latest_sma_50 = sma_50.iloc[i] if i < len(sma_50) else None
                latest_sma_200 = sma_200.iloc[i] if i < len(sma_200) else None
                latest_bb_upper = bb_upper.iloc[i] if i < len(bb_upper) else None
                latest_bb_middle = bb_middle.iloc[i] if i < len(bb_middle) else None
                latest_bb_lower = bb_lower.iloc[i] if i < len(bb_lower) else None
                latest_bb_width = bb_width.iloc[i] if i < len(bb_width) else None
                latest_adx = adx.iloc[i] if i < len(adx) else None
                
                # Check for MACD crossover
                macd_pos_cross = False
                macd_neg_cross = False
                if i > 0:
                    prev_macd = macd_line.iloc[i-1] if i-1 < len(macd_line) else None
                    prev_signal = signal_line.iloc[i-1] if i-1 < len(signal_line) else None
                    
                    if pd.notnull(prev_macd) and pd.notnull(prev_signal) and pd.notnull(latest_macd) and pd.notnull(latest_signal):
                        if prev_macd < prev_signal and latest_macd > latest_signal:
                            macd_pos_cross = True
                        if prev_macd > prev_signal and latest_macd < latest_signal:
                            macd_neg_cross = True
                
                row = {
                    'cache_date': date_str,
                    'ticker': ticker,
                    'underlying_price': latest_close,
                    'rsi_14': latest_rsi if pd.notnull(latest_rsi) else None,
                    'rsi_above_80': bool(latest_rsi > 80) if pd.notnull(latest_rsi) else False,
                    'rsi_60_80': bool(60 < latest_rsi <= 80) if pd.notnull(latest_rsi) else False,
                    'rsi_40_60': bool(40 <= latest_rsi <= 60) if pd.notnull(latest_rsi) else False,
                    'rsi_20_40': bool(20 <= latest_rsi < 40) if pd.notnull(latest_rsi) else False,
                    'rsi_below_20': bool(latest_rsi < 20) if pd.notnull(latest_rsi) else False,
                    'macd': latest_macd if pd.notnull(latest_macd) else None,
                    'macd_signal': latest_signal if pd.notnull(latest_signal) else None,
                    'macd_histogram': latest_histogram if pd.notnull(latest_histogram) else None,
                    'macd_pos_cross': macd_pos_cross,
                    'macd_neg_cross': macd_neg_cross,
                    'sma_50': latest_sma_50 if pd.notnull(latest_sma_50) else None,
                    'sma_200': latest_sma_200 if pd.notnull(latest_sma_200) else None,
                    'above_50_sma': bool(latest_close > latest_sma_50) if pd.notnull(latest_sma_50) else False,
                    'above_200_sma': bool(latest_close > latest_sma_200) if pd.notnull(latest_sma_200) else False,
                    'below_50_sma': bool(latest_close < latest_sma_50) if pd.notnull(latest_sma_50) else False,
                    'below_200_sma': bool(latest_close < latest_sma_200) if pd.notnull(latest_sma_200) else False,
                    'dist_from_50sma_pct': ((latest_close - latest_sma_50) / latest_sma_50 * 100) if pd.notnull(latest_sma_50) and latest_sma_50 != 0 else None,
                    'dist_from_200sma_pct': ((latest_close - latest_sma_200) / latest_sma_200 * 100) if pd.notnull(latest_sma_200) and latest_sma_200 != 0 else None,
                    'bb_upper': latest_bb_upper if pd.notnull(latest_bb_upper) else None,
                    'bb_middle': latest_bb_middle if pd.notnull(latest_bb_middle) else None,
                    'bb_lower': latest_bb_lower if pd.notnull(latest_bb_lower) else None,
                    'bb_width': latest_bb_width if pd.notnull(latest_bb_width) else None,
                    'adx_14': latest_adx if pd.notnull(latest_adx) else None,
                    'strong_trend': bool(latest_adx > 25) if pd.notnull(latest_adx) else False
                }
                
                all_cache_rows.append(row)
                ticker_rows += 1
            
            if ticker_rows > 0:
                print(f"✓ {ticker_rows} new rows", end="")
            else:
                print("⚠ No new rows", end="")
            
        except Exception as e:
            print(f"✗ Error: {e}", end="")
    
    # Insert all NEW rows into database
    if all_cache_rows:
        print(f"\n\n📥 Inserting {len(all_cache_rows)} total new rows into database...")
        try:
            insert_df = pd.DataFrame(all_cache_rows)
            insert_df.to_sql('technical_screener_cache', con=engine, if_exists='append', index=False)
            print("✓ Insert complete!")
        except Exception as e:
            print(f"✗ Insert error: {e}")
    else:
        print("\n\n✅ No new rows to insert.")
    
    print("\n" + "="*70)
    print("✓ TECHNICAL SCREENER CACHE UPDATE COMPLETE!")
    print("="*70)


if __name__ == '__main__':
    precalculate_technical_screener_cache()
