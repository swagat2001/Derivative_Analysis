from flask import Flask, jsonify, request, render_template
from sqlalchemy import create_engine, text, inspect
import pandas as pd
from urllib.parse import quote_plus
import threading
import webbrowser
import time
import json

# Database config
db_user = 'postgres'
db_password = 'Gallop@3104'
db_host = 'localhost'
db_port = '5432'
db_name = 'BhavCopy_Database'

db_password_enc = quote_plus(db_password)
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}')

app = Flask(__name__)

# CSV path for expiry dates
CSV_PATH = "C:\\Users\\Admin\\Desktop\\BhavCopy-Backup2\\complete.csv"

def get_available_dates():
    try:
        check_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'options_dashboard_cache'
        );
        """
        exists = pd.read_sql(text(check_query), engine).iloc[0, 0]
        
        if not exists:
            print("⚠️  Cache table doesn't exist. Run update_database.py first!")
            return []
        
        query = "SELECT DISTINCT biz_date FROM options_dashboard_cache ORDER BY biz_date DESC"
        result = pd.read_sql(text(query), engine)
        
        if result.empty:
            print("⚠️  No dates in cache. Run update_database.py first!")
            return []
        
        return [d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d) for d in result['biz_date']]
    except Exception as e:
        print(f"❌ Error getting dates: {e}")
        return []

@app.route('/')
def index():
    dates = get_available_dates()
    if not dates:
        print("\n⚠️  No dates available!")
        print("   Please run: python update_database.py")
    else:
        print(f"✅ Found {len(dates)} dates available")
    return render_template('index.html', dates=dates)

@app.route('/stock/<ticker>')
def stock_detail(ticker):
    """Stock detail page"""
    dates = get_available_dates()
    return render_template('stock_detail.html', ticker=ticker, dates=dates)

@app.route('/get_available_tickers')
def get_available_tickers():
    """Get list of all available tickers"""
    try:
        inspector = inspect(engine)
        tables = [t for t in inspector.get_table_names() if t.startswith("TBL_") and t.endswith("_DERIVED")]
        tickers = sorted([t.replace("TBL_", "").replace("_DERIVED", "") for t in tables])
        return jsonify(tickers)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_available_trading_dates')
def get_available_trading_dates():
    """Get available trading dates from database"""
    try:
        dates = get_available_dates()
        return jsonify({'dates': dates})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_expiry_dates')
def get_expiry_dates_route():
    """Get expiry dates for a ticker, optionally filtered by date"""
    ticker = request.args.get('ticker')
    date = request.args.get('date')  # Optional date filter
    
    if not ticker:
        return jsonify({'error': 'Ticker required'}), 400
    
    try:
        if date:
            # Get expiry dates that have data for the specific date
            expiry_dates = get_expiry_dates_for_ticker_and_date(ticker, date)
        else:
            # Get all expiry dates for the ticker
            expiry_dates = get_expiry_dates_for_ticker(ticker)
        return jsonify({'expiry_dates': expiry_dates})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def get_expiry_dates_for_ticker_and_date(ticker, date):
    """Get expiry dates that have data for a specific ticker and date"""
    try:
        table_name = f"TBL_{ticker}_DERIVED"
        inspector = inspect(engine)
        
        if table_name not in inspector.get_table_names():
            print(f"❌ {ticker}: Table {table_name} does not exist")
            return []
        
        # Query to get only expiry dates that have data for the specified date
        query = f"""
        SELECT DISTINCT "FininstrmActlXpryDt"
        FROM "{table_name}"
        WHERE "BizDt" = :date
        AND "FininstrmActlXpryDt" IS NOT NULL
        ORDER BY "FininstrmActlXpryDt"
        """
        result = pd.read_sql(text(query), engine, params={"date": date})
        
        if not result.empty:
            result['FininstrmActlXpryDt'] = pd.to_datetime(result['FininstrmActlXpryDt'], errors='coerce')
            expiry_dates = [d.strftime('%Y-%m-%d') for d in result['FininstrmActlXpryDt'] if pd.notna(d)]
            print(f"📅 {ticker} on {date}: Found {len(expiry_dates)} expiry dates with data: {expiry_dates}")
            return expiry_dates
        else:
            print(f"⚠️ {ticker} on {date}: No expiry dates found with data")
            return []
            
    except Exception as e:
        print(f"❌ Error getting expiry dates for {ticker} on {date}: {e}")
        return []

@app.route('/get_expiry_data_detailed')
def get_expiry_data_detailed():
    """Get detailed data for each expiry date"""
    ticker = request.args.get('ticker')
    date = request.args.get('date')
    
    if not ticker or not date:
        return jsonify({'error': 'Ticker and date required'}), 400
    
    try:
        table_name = f"TBL_{ticker}_DERIVED"
        
        # Check if table exists
        inspector = inspect(engine)
        if table_name not in inspector.get_table_names():
            return jsonify({'error': f'Ticker {ticker} not found'}), 404
        
        # Get previous trading date
        prev_date_query = f"""
        SELECT MAX("BizDt") as prev_date
        FROM "{table_name}"
        WHERE "BizDt" < :date
        """
        prev_result = pd.read_sql(text(prev_date_query), engine, params={"date": date})
        prev_date = prev_result.iloc[0]['prev_date'] if not prev_result.empty and pd.notna(prev_result.iloc[0]['prev_date']) else None
        
        # Get current day data
        # Price, OI, OI_chg from futures (null OptnTp)
        # Volume from options (CE + PE)
        query = f"""
        WITH futures_data AS (
            SELECT 
                "FininstrmActlXpryDt" as expiry,
                "ClsPric" as price,
                "OpnIntrst" as oi,
                "ChngInOpnIntrst" as oi_chg
            FROM "{table_name}"
            WHERE "BizDt" = :date
            AND "FininstrmActlXpryDt" IS NOT NULL
            AND "OptnTp" IS NULL
        ),
        options_volume AS (
            SELECT 
                "FininstrmActlXpryDt" as expiry,
                SUM("TtlTradgVol") as volume
            FROM "{table_name}"
            WHERE "BizDt" = :date
            AND "FininstrmActlXpryDt" IS NOT NULL
            AND "OptnTp" IN ('CE', 'PE')
            GROUP BY "FininstrmActlXpryDt"
        )
        SELECT 
            f.expiry,
            f.price,
            COALESCE(v.volume, 0) as volume,
            f.oi,
            f.oi_chg
        FROM futures_data f
        LEFT JOIN options_volume v ON f.expiry = v.expiry
        ORDER BY f.expiry
        """
        
        df = pd.read_sql(text(query), engine, params={"date": date})
        
        if df.empty:
            return jsonify({'expiry_data': [], 'lot_size': 0, 'fair_price': 0})
        
        # Convert to numeric
        for col in ['price', 'volume', 'oi', 'oi_chg']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Get previous day prices if available
        prev_prices = {}
        if prev_date:
            prev_query = f"""
            SELECT 
                "FininstrmActlXpryDt" as expiry,
                "ClsPric" as prev_price
            FROM "{table_name}"
            WHERE "BizDt" = :prev_date
            AND "FininstrmActlXpryDt" IS NOT NULL
            AND "OptnTp" IS NULL
            """
            prev_df = pd.read_sql(text(prev_query), engine, params={"prev_date": prev_date})
            if not prev_df.empty:
                prev_df['prev_price'] = pd.to_numeric(prev_df['prev_price'], errors='coerce')
                for _, row in prev_df.iterrows():
                    if pd.notna(row['expiry']):
                        prev_prices[row['expiry']] = float(row['prev_price'])
                print(f"Found {len(prev_prices)} previous prices for {ticker} on {prev_date}")
            else:
                print(f"No previous day data found for {ticker} on {prev_date}")
        else:
            print(f"No previous trading date found before {date} for {ticker}")
        
        # Get lot size - first try from database NewBrdLotQty, then fall back to CSV
        lot_size = 0
        try:
            if not df.empty:
                # Try to get from first row of database
                lot_query = f"""
                SELECT "NewBrdLotQty"
                FROM "{table_name}"
                WHERE "BizDt" = :date
                LIMIT 1
                """
                lot_df = pd.read_sql(text(lot_query), engine, params={"date": date})
                if not lot_df.empty and pd.notna(lot_df['NewBrdLotQty'].iloc[0]):
                    lot_size = int(lot_df['NewBrdLotQty'].iloc[0])
        except:
            pass
        
        # Fallback to CSV if not in database
        if lot_size == 0:
            try:
                csv_df = pd.read_csv(CSV_PATH)
                ticker_csv = csv_df[csv_df['name'] == ticker]
                lot_size = int(ticker_csv['lot_size'].iloc[0]) if not ticker_csv.empty else 0
            except:
                lot_size = 0
        
        # Calculate fair price - get underlying price and find nearest strike
        fair_price = 0
        try:
            # Get current underlying price
            underlying_query = f"""
            SELECT AVG("UndrlygPric") as underlying_price
            FROM "{table_name}"
            WHERE "BizDt" = :date
            LIMIT 1
            """
            underlying_df = pd.read_sql(text(underlying_query), engine, params={"date": date})
            if not underlying_df.empty and pd.notna(underlying_df['underlying_price'].iloc[0]):
                underlying_price = float(underlying_df['underlying_price'].iloc[0])
                
                # Get nearest strike price for first expiry
                first_expiry = df['expiry'].iloc[0] if not df.empty else None
                if first_expiry:
                    strike_query = f"""
                    SELECT "StrkPric", ABS("StrkPric" - :underlying) as distance
                    FROM "{table_name}"
                    WHERE "BizDt" = :date
                    AND "FininstrmActlXpryDt" = :expiry
                    AND "OptnTp" = 'CE'
                    ORDER BY distance
                    LIMIT 1
                    """
                    strike_df = pd.read_sql(text(strike_query), engine, params={
                        "date": date, 
                        "expiry": first_expiry,
                        "underlying": underlying_price
                    })
                    if not strike_df.empty:
                        fair_price = float(strike_df['StrkPric'].iloc[0])
        except Exception as e:
            print(f"Error calculating fair price: {e}")
            fair_price = float(df['price'].iloc[0]) if not df.empty else 0
        
        # Get previous day OI for percentage calculation
        prev_oi_dict = {}
        if prev_date:
            prev_oi_query = f"""
            SELECT 
                "FininstrmActlXpryDt" as expiry,
                "OpnIntrst" as prev_oi
            FROM "{table_name}"
            WHERE "BizDt" = :prev_date
            AND "FininstrmActlXpryDt" IS NOT NULL
            AND "OptnTp" IS NULL
            """
            prev_oi_df = pd.read_sql(text(prev_oi_query), engine, params={"prev_date": prev_date})
            if not prev_oi_df.empty:
                prev_oi_df['prev_oi'] = pd.to_numeric(prev_oi_df['prev_oi'], errors='coerce')
                for _, oi_row in prev_oi_df.iterrows():
                    if pd.notna(oi_row['expiry']):
                        prev_oi_dict[oi_row['expiry']] = float(oi_row['prev_oi'])
        
        # Build result
        result = []
        for _, row in df.iterrows():
            expiry_date = row['expiry']
            current_price = float(row['price']) if pd.notna(row['price']) else 0
            current_oi = float(row['oi']) if pd.notna(row['oi']) else 0
            oi_chg = float(row['oi_chg']) if pd.notna(row['oi_chg']) else 0
            
            # Calculate price change percentage
            prev_price = prev_prices.get(expiry_date, None)
            price_chg_percent = 0
            if prev_price and prev_price > 0 and current_price > 0:
                price_chg_percent = ((current_price - prev_price) / prev_price) * 100
                print(f"Expiry {expiry_date}: Current={current_price}, Prev={prev_price}, Change={price_chg_percent:.2f}%")
            else:
                print(f"Expiry {expiry_date}: Cannot calculate price change (Current={current_price}, Prev={prev_price})")
            
            # Calculate OI change percentage: (today_oi - prev_oi) / prev_oi * 100
            prev_oi = prev_oi_dict.get(expiry_date, None)
            oi_chg_percent = 0
            if prev_oi and prev_oi > 0:
                oi_chg_percent = ((current_oi - prev_oi) / prev_oi) * 100
                print(f"Expiry {expiry_date}: Current OI={current_oi}, Prev OI={prev_oi}, OI Change%={oi_chg_percent:.2f}%")
            else:
                # Fallback: if no prev_oi, calculate from oi_chg
                if current_oi > 0:
                    oi_chg_percent = (oi_chg / current_oi) * 100
            
            result.append({
                'expiry': expiry_date.strftime('%Y-%m-%d') if pd.notna(expiry_date) else None,
                'price': current_price,
                'price_chg_percent': round(price_chg_percent, 2),
                'volume': float(row['volume']) if pd.notna(row['volume']) else 0,
                'oi': current_oi,
                'oi_chg': oi_chg,
                'oi_chg_percent': round(oi_chg_percent, 2)
            })
        
        return jsonify({
            'expiry_data': result,
            'lot_size': lot_size,
            'fair_price': round(fair_price, 2)
        })
    
    except Exception as e:
        print(f"Error in get_expiry_data_detailed: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/get_stock_data')
def get_stock_data():
    """Get stock option chain data"""
    ticker = request.args.get('ticker')
    mode = request.args.get('mode', 'latest')
    expiry = request.args.get('expiry')
    date = request.args.get('date')
    
    print(f"\n{'='*80}")
    print(f"📊 GET_STOCK_DATA called:")
    print(f"   Ticker: {ticker}")
    print(f"   Mode: {mode}")
    print(f"   Expiry: {expiry}")
    print(f"   Date: {date}")
    print(f"{'='*80}")
    
    try:
        # Get expiry dates that have data for the selected date
        if date:
            expiry_dates = get_expiry_dates_for_ticker_and_date(ticker, date)
        else:
            expiry_dates = get_expiry_dates_for_ticker(ticker)
        
        print(f"✅ Found {len(expiry_dates)} expiry dates for {ticker}")
        
        # If no expiry specified, use the first one
        if not expiry and expiry_dates:
            expiry = expiry_dates[0]
            print(f"📌 No expiry specified, using first: {expiry}")
        
        # Determine which date to use
        if mode == 'latest':
            available_dates = get_available_dates()
            if not available_dates:
                print(f"❌ No dates available in cache")
                return jsonify({'error': 'No dates available'}), 404
            query_date = available_dates[0]
            print(f"📅 Using latest date: {query_date}")
        else:
            query_date = date if date else get_available_dates()[0]
            print(f"📅 Using historical date: {query_date}")
        
        # Get data from database
        table_name = f"TBL_{ticker}_DERIVED"
        
        # Check if table exists
        inspector = inspect(engine)
        if table_name not in inspector.get_table_names():
            print(f"❌ Table {table_name} not found")
            return jsonify({'error': f'Ticker {ticker} not found'}), 404
        
        print(f"✅ Table {table_name} exists")
        
        # First check if data exists for this date
        check_query = f"""
        SELECT COUNT(*) as count
        FROM "{table_name}"
        WHERE "BizDt" = :date
        """
        check_result = pd.read_sql(text(check_query), engine, params={"date": query_date})
        row_count = check_result.iloc[0]['count']
        print(f"📊 Found {row_count} rows for date {query_date}")
        
        if row_count == 0:
            print(f"❌ No data for date {query_date}")
            return jsonify({'error': f'No data found for date {query_date}'}), 404
        
        # Get option chain data
        query = f"""
        SELECT 
            "BizDt",
            "StrkPric",
            "OptnTp",
            "OpnIntrst",
            "ChngInOpnIntrst",
            "TtlTradgVol",
            "LastPric",
            "UndrlygPric",
            "ClsPric",
            "FininstrmActlXpryDt",
            "iv"
        FROM "{table_name}"
        WHERE "BizDt" = :date
        AND "OptnTp" IN ('CE', 'PE')
        AND "StrkPric" IS NOT NULL
        """
        
        if expiry and expiry != 'all':
            query += " AND \"FininstrmActlXpryDt\" = :expiry"
            print(f"📌 Filtering by expiry: {expiry}")
            df = pd.read_sql(text(query), engine, params={"date": query_date, "expiry": expiry})
        else:
            df = pd.read_sql(text(query), engine, params={"date": query_date})
            if not df.empty:
                df['FininstrmActlXpryDt'] = pd.to_datetime(df['FininstrmActlXpryDt'], errors='coerce')
                nearest_expiry = df['FininstrmActlXpryDt'].min()
                print(f"📌 No expiry filter, using nearest: {nearest_expiry}")
                df = df[df['FininstrmActlXpryDt'] == nearest_expiry]
        
        print(f"📊 Query returned {len(df)} rows")
        
        if df.empty:
            print(f"❌ DataFrame is empty after filtering")
            return jsonify({'error': 'No data found for selected parameters'}), 404
        
        # Convert columns to numeric
        for col in ['StrkPric', 'OpnIntrst', 'ChngInOpnIntrst', 'TtlTradgVol', 'LastPric', 'UndrlygPric', 'ClsPric', 'iv']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Build option chain
        option_chain = build_option_chain(df)
        
        # Calculate stats
        stats = calculate_stats(df)
        
        # Get price data with all indicators for chart
        price_data = get_price_data_with_indicators(ticker, query_date, df)
        
        # Debug: Check what we got
        print(f"📊 Returning data for {ticker}: {len(option_chain)} strikes, {len(price_data)} price points")
        
        return jsonify({
            'ticker': ticker,
            'expiry_dates': expiry_dates,
            'selected_expiry': expiry,
            'last_updated': query_date,
            'stats': stats,
            'option_chain': option_chain,
            'price_data': price_data if price_data else []
        })
        
    except Exception as e:
        print(f"Error in get_stock_data: {e}")
        return jsonify({'error': str(e)}), 500

def get_expiry_dates_for_ticker(ticker):
    """Get expiry dates from database for a specific ticker"""
    try:
        table_name = f"TBL_{ticker}_DERIVED"
        inspector = inspect(engine)
        
        if table_name in inspector.get_table_names():
            # First check what we have in the table
            count_query = f"""
            SELECT COUNT(DISTINCT "FininstrmActlXpryDt") as expiry_count
            FROM "{table_name}"
            WHERE "FininstrmActlXpryDt" IS NOT NULL
            """
            count_result = pd.read_sql(text(count_query), engine)
            print(f"\n🔍 {ticker}: Found {count_result.iloc[0]['expiry_count']} unique expiry dates in database")
            
            # Check sample expiry date format
            sample_query = f"""
            SELECT DISTINCT "FininstrmActlXpryDt" as sample_expiry
            FROM "{table_name}"
            WHERE "FininstrmActlXpryDt" IS NOT NULL
            LIMIT 5
            """
            sample_result = pd.read_sql(text(sample_query), engine)
            if not sample_result.empty:
                print(f"📝 Sample expiry formats in DB: {sample_result['sample_expiry'].tolist()}")
            
            query = f"""
            SELECT DISTINCT "FininstrmActlXpryDt"
            FROM "{table_name}"
            WHERE "FininstrmActlXpryDt" IS NOT NULL
            ORDER BY "FininstrmActlXpryDt"
            """
            result = pd.read_sql(text(query), engine)
            
            if not result.empty:
                result['FininstrmActlXpryDt'] = pd.to_datetime(result['FininstrmActlXpryDt'], errors='coerce')
                expiry_dates = [d.strftime('%Y-%m-%d') for d in result['FininstrmActlXpryDt'] if pd.notna(d)]
                print(f"📅 {ticker}: Returning {len(expiry_dates)} expiry dates: {expiry_dates[:5]}..." if len(expiry_dates) > 5 else f"📅 {ticker}: Returning {len(expiry_dates)} expiry dates: {expiry_dates}")
                return expiry_dates
            else:
                print(f"⚠️ {ticker}: Query returned empty result")
        else:
            print(f"❌ {ticker}: Table {table_name} does not exist")
        
        return []
    except Exception as e:
        print(f"❌ Error getting expiry dates for {ticker}: {e}")
        return []

def build_option_chain(df):
    """Build option chain from dataframe"""
    strikes = sorted(df['StrkPric'].unique())
    option_chain = []
    
    for strike in strikes:
        ce_data = df[(df['StrkPric'] == strike) & (df['OptnTp'] == 'CE')]
        pe_data = df[(df['StrkPric'] == strike) & (df['OptnTp'] == 'PE')]
        
        # Get IV columns (last 4 float columns named 'iv')
        call_iv = 0
        put_iv = 0
        if not ce_data.empty and 'iv' in ce_data.columns:
            call_iv = float(ce_data['iv'].iloc[0]) if pd.notna(ce_data['iv'].iloc[0]) else 0
            # Scale IV if it's in 0-1 range (multiply by 100)
            if 0 < call_iv < 1:
                call_iv = call_iv * 100
        if not pe_data.empty and 'iv' in pe_data.columns:
            put_iv = float(pe_data['iv'].iloc[0]) if pd.notna(pe_data['iv'].iloc[0]) else 0
            # Scale IV if it's in 0-1 range (multiply by 100)
            if 0 < put_iv < 1:
                put_iv = put_iv * 100
        
        row = {
            'strike': float(strike),
            'call_oi': float(ce_data['OpnIntrst'].iloc[0]) if not ce_data.empty else 0,
            'call_oi_chg': float(ce_data['ChngInOpnIntrst'].iloc[0]) if not ce_data.empty else 0,
            'call_volume': float(ce_data['TtlTradgVol'].iloc[0]) if not ce_data.empty else 0,
            'call_price': float(ce_data['LastPric'].iloc[0]) if not ce_data.empty else 0,
            'call_iv': call_iv,
            'put_price': float(pe_data['LastPric'].iloc[0]) if not pe_data.empty else 0,
            'put_volume': float(pe_data['TtlTradgVol'].iloc[0]) if not pe_data.empty else 0,
            'put_oi_chg': float(pe_data['ChngInOpnIntrst'].iloc[0]) if not pe_data.empty else 0,
            'put_oi': float(pe_data['OpnIntrst'].iloc[0]) if not pe_data.empty else 0,
            'put_iv': put_iv,
        }
        option_chain.append(row)
    
    return option_chain

def calculate_stats(df):
    """Calculate summary statistics"""
    ce_df = df[df['OptnTp'] == 'CE']
    pe_df = df[df['OptnTp'] == 'PE']
    
    total_ce_oi = float(ce_df['OpnIntrst'].sum())
    total_pe_oi = float(pe_df['OpnIntrst'].sum())
    total_ce_oi_chg = float(ce_df['ChngInOpnIntrst'].sum())
    total_pe_oi_chg = float(pe_df['ChngInOpnIntrst'].sum())
    pcr_oi = total_pe_oi / total_ce_oi if total_ce_oi > 0 else 0
    
    return {
        'total_ce_oi': total_ce_oi,
        'total_pe_oi': total_pe_oi,
        'total_ce_oi_chg': total_ce_oi_chg,
        'total_pe_oi_chg': total_pe_oi_chg,
        'pcr_oi': pcr_oi
    }

def get_yahoo_ticker_mapping():
    """Get all available tickers from database and create Yahoo Finance mapping"""
    try:
        inspector = inspect(engine)
        tables = [t for t in inspector.get_table_names() if t.startswith("TBL_") and t.endswith("_DERIVED")]
        tickers = [t.replace("TBL_", "").replace("_DERIVED", "") for t in tables]
        
        # Create mapping for all tickers
        mapping = {
            'NIFTY': '^NSEI',
            'BANKNIFTY': '^NSEBANK',
            'FINNIFTY': 'NIFTY_FIN_SERVICE.NS',
            'MIDCPNIFTY': '^NSEMDCP50',
        }
        
        # Add all other tickers with .NS suffix
        for ticker in tickers:
            if ticker not in mapping:
                mapping[ticker] = f"{ticker}.NS"
        
        print(f"✅ Created Yahoo Finance mapping for {len(mapping)} tickers")
        return mapping
    except Exception as e:
        print(f"Error creating ticker mapping: {e}")
        return {}

# Load ticker mapping once at startup
YAHOO_TICKER_MAP = get_yahoo_ticker_mapping()

def get_price_data_with_indicators(ticker, end_date, options_df):
    """Get intraday price data with OI, IV, PCR indicators"""
    try:
        import yfinance as yf
        from datetime import datetime, timedelta
        
        # Use the global mapping
        yahoo_symbol = YAHOO_TICKER_MAP.get(ticker, f"{ticker}.NS")
        
        print(f"📊 Fetching {ticker} as {yahoo_symbol}...")
        
        if isinstance(end_date, str):
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        else:
            end_dt = end_date
        
        start_dt = end_dt
        end_dt_plus = end_dt + timedelta(days=1)
        
        stock = yf.Ticker(yahoo_symbol)
        price_df = stock.history(start=start_dt, end=end_dt_plus, interval='5m')
        
        # If no data with .NS, try .BO (BSE)
        if price_df.empty and yahoo_symbol.endswith('.NS'):
            yahoo_symbol_bse = yahoo_symbol.replace('.NS', '.BO')
            print(f"⚠️ No data for {yahoo_symbol}, trying {yahoo_symbol_bse}...")
            stock = yf.Ticker(yahoo_symbol_bse)
            price_df = stock.history(start=start_dt, end=end_dt_plus, interval='5m')
        
        if price_df.empty:
            print(f"⚠️ No yfinance data for {ticker}, using fallback")
            return get_fallback_chart_data(ticker, end_date, options_df)
        
        print(f"✅ Got {len(price_df)} price data points")
        
        # Calculate indicators from options data
        ce_df = options_df[options_df['OptnTp'] == 'CE']
        pe_df = options_df[options_df['OptnTp'] == 'PE']
        
        total_ce_oi = float(ce_df['OpnIntrst'].sum()) if not ce_df.empty else 0
        total_pe_oi = float(pe_df['OpnIntrst'].sum()) if not pe_df.empty else 0
        total_ce_vol = float(ce_df['TtlTradgVol'].sum()) if not ce_df.empty else 0
        total_pe_vol = float(pe_df['TtlTradgVol'].sum()) if not pe_df.empty else 0
        
        pcr_oi = total_pe_oi / total_ce_oi if total_ce_oi > 0 else 1.0
        pcr_vol = total_pe_vol / total_ce_vol if total_ce_vol > 0 else 1.0
        
        # Average IV from all options
        avg_iv = float(options_df['iv'].mean()) if 'iv' in options_df.columns else 0
        
        chart_data = []
        
        for timestamp, row in price_df.iterrows():
            unix_timestamp = int(timestamp.timestamp())
            
            chart_data.append({
                'time': unix_timestamp,
                'open': float(row['Open']),
                'high': float(row['High']),
                'low': float(row['Low']),
                'close': float(row['Close']),
                'volume': float(row['Volume']),
                'vwap': float((row['High'] + row['Low'] + row['Close']) / 3),
                'oi': total_ce_oi + total_pe_oi,  # Total OI
                'iv': avg_iv,  # Average IV
                'pcr': pcr_oi  # PCR based on OI
            })
        
        return chart_data
        
    except Exception as e:
        print(f"⚠️ Error getting chart data: {e}")
        return get_fallback_chart_data(ticker, end_date, options_df)

def get_fallback_chart_data(ticker, end_date, options_df):
    """Fallback chart data from database"""
    try:
        from datetime import datetime
        
        ce_df = options_df[options_df['OptnTp'] == 'CE']
        pe_df = options_df[options_df['OptnTp'] == 'PE']
        
        total_ce_oi = float(ce_df['OpnIntrst'].sum()) if not ce_df.empty else 0
        total_pe_oi = float(pe_df['OpnIntrst'].sum()) if not pe_df.empty else 0
        total_vol = float(options_df['TtlTradgVol'].sum())
        
        pcr_oi = total_pe_oi / total_ce_oi if total_ce_oi > 0 else 1.0
        avg_iv = float(options_df['iv'].mean()) if 'iv' in options_df.columns else 0
        
        price = float(options_df['UndrlygPric'].iloc[0])
        
        if isinstance(end_date, str):
            date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        else:
            date_obj = end_date
        
        date_obj = date_obj.replace(hour=15, minute=30, second=0)
        unix_timestamp = int(date_obj.timestamp())
        
        return [{
            'time': unix_timestamp,
            'open': price,
            'high': price,
            'low': price,
            'close': price,
            'volume': total_vol,
            'vwap': price,
            'oi': total_ce_oi + total_pe_oi,
            'iv': avg_iv,
            'pcr': pcr_oi
        }]
    except Exception as e:
        print(f"Error in fallback chart data: {e}")
        return []

@app.route('/get_historical_data')
def get_historical_data():
    ticker = request.args.get('ticker')
    curr_date = request.args.get('date')
    option_type = request.args.get('type')
    metric = request.args.get('metric', 'money')
    strike = request.args.get('strike')
    
    try:
        query_dates = """
        SELECT DISTINCT biz_date 
        FROM options_dashboard_cache 
        WHERE biz_date <= :curr_date
        ORDER BY biz_date DESC
        LIMIT 40
        """
        dates_df = pd.read_sql(text(query_dates), engine, params={"curr_date": curr_date})
        
        if dates_df.empty:
            return jsonify({'error': 'No historical data found'}), 404
        
        dates = [d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d) for d in dates_df['biz_date']]
        dates.reverse()
        
        historical_data = []
        
        for date in dates:
            try:
                query = """
                SELECT data_json FROM options_dashboard_cache 
                WHERE biz_date = :date AND moneyness_type = 'TOTAL'
                """
                result = pd.read_sql(text(query), engine, params={"date": date})
                
                if not result.empty:
                    data = json.loads(result.iloc[0]['data_json'])
                    ticker_data = next((item for item in data if item['stock'] == ticker), None)
                    
                    if ticker_data:
                        prefix = option_type
                        table_name = f"TBL_{ticker}_DERIVED"
                        
                        if metric == 'vega' and strike and strike != 'N/A':
                            query_sql = f"""
                            SELECT 
                                SUM(CASE WHEN "OptnTp" = 'PE' THEN "TtlTradgVol" ELSE 0 END) as put_volume,
                                SUM(CASE WHEN "OptnTp" = 'CE' THEN "TtlTradgVol" ELSE 0 END) as call_volume,
                                SUM(CASE WHEN "OptnTp" = 'PE' THEN "OpnIntrst" ELSE 0 END) as put_oi,
                                SUM(CASE WHEN "OptnTp" = 'CE' THEN "OpnIntrst" ELSE 0 END) as call_oi,
                                MAX(CASE WHEN "OptnTp" = :opt_type AND "StrkPric" = :strike THEN "vega" ELSE NULL END) as strike_vega,
                                MAX("UndrlygPric") as underlying_price
                            FROM "{table_name}"
                            WHERE "BizDt" = :date
                            """
                            opt_type_param = 'CE' if option_type == 'call' else 'PE'
                            query_result = pd.read_sql(text(query_sql), engine, params={"date": date, "opt_type": opt_type_param, "strike": float(strike)})
                        else:
                            query_sql = f"""
                            SELECT 
                                SUM(CASE WHEN "OptnTp" = 'PE' THEN "TtlTradgVol" ELSE 0 END) as put_volume,
                                SUM(CASE WHEN "OptnTp" = 'CE' THEN "TtlTradgVol" ELSE 0 END) as call_volume,
                                SUM(CASE WHEN "OptnTp" = 'PE' THEN "OpnIntrst" ELSE 0 END) as put_oi,
                                SUM(CASE WHEN "OptnTp" = 'CE' THEN "OpnIntrst" ELSE 0 END) as call_oi,
                                AVG(CASE WHEN "OptnTp" = :opt_type THEN "vega" ELSE NULL END) as avg_vega,
                                MAX("UndrlygPric") as underlying_price
                            FROM "{table_name}"
                            WHERE "BizDt" = :date
                            """
                            opt_type_param = 'CE' if option_type == 'call' else 'PE'
                            query_result = pd.read_sql(text(query_sql), engine, params={"date": date, "opt_type": opt_type_param})
                        
                        if not query_result.empty:
                            put_vol = float(query_result.iloc[0]['put_volume'] or 0)
                            call_vol = float(query_result.iloc[0]['call_volume'] or 0)
                            put_oi = float(query_result.iloc[0]['put_oi'] or 0)
                            call_oi = float(query_result.iloc[0]['call_oi'] or 0)
                            underlying_price = float(query_result.iloc[0]['underlying_price'] or 0)
                            
                            pcr_volume = put_vol / call_vol if call_vol > 0 else 0
                            pcr_oi = put_oi / call_oi if call_oi > 0 else 0
                            
                            data_point = {
                                'date': date,
                                'pcr_volume': round(pcr_volume, 4),
                                'pcr_oi': round(pcr_oi, 4),
                                'underlying_price': round(underlying_price, 2),
                                'rsi': ticker_data.get('rsi', None)
                            }
                            
                            if metric == 'money':
                                moneyness = ticker_data.get(f'{prefix}_total_money', 0)
                                data_point['moneyness'] = float(moneyness)
                            elif metric == 'vega':
                                if strike and strike != 'N/A':
                                    strike_vega = float(query_result.iloc[0].get('strike_vega') or 0)
                                    data_point['strike_vega'] = round(strike_vega, 6)
                                else:
                                    avg_vega = float(query_result.iloc[0]['avg_vega'] or 0)
                                    data_point['avg_vega'] = round(avg_vega, 6)
                            
                            historical_data.append(data_point)
            except:
                continue
        
        return jsonify({
            'ticker': ticker,
            'option_type': option_type,
            'metric': metric,
            'strike': strike,
            'data': historical_data
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_data')
def get_data():
    curr_date = request.args.get('date')
    
    try:
        query = """
        SELECT biz_date, prev_date, moneyness_type, data_json 
        FROM options_dashboard_cache 
        WHERE biz_date = :date
        """
        result = pd.read_sql(text(query), engine, params={"date": curr_date})
        
        if result.empty:
            return jsonify({'error': 'No data found'}), 404
        
        total_row = result[result['moneyness_type'] == 'TOTAL'].iloc[0] if not result[result['moneyness_type'] == 'TOTAL'].empty else None
        otm_row = result[result['moneyness_type'] == 'OTM'].iloc[0]
        itm_row = result[result['moneyness_type'] == 'ITM'].iloc[0]
        
        response_data = {
            'curr_date': curr_date,
            'prev_date': str(otm_row['prev_date']),
            'otm': json.loads(otm_row['data_json']),
            'itm': json.loads(itm_row['data_json'])
        }
        
        if total_row is not None:
            response_data['total'] = json.loads(total_row['data_json'])
        else:
            response_data['total'] = []
        
        return jsonify(response_data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

import socket

def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        return "Unable to get IP"

def open_browser():
    time.sleep(1.5)
    webbrowser.open('http://localhost:5000')

if __name__ == '__main__':
    print("="*80)
    print("📊 STARTING DASHBOARD SERVER")
    print("="*80)
    
    local_ip = get_local_ip()
    
    print("\n✅ Server URLs:")
    print(f"   Local:   http://localhost:5000")
    print(f"   Network: http://{local_ip}:5000")
    print("\n📱 Share the Network URL with others on your network")
    print("✅ Auto-opening browser...")
    
    dates = get_available_dates()
    if not dates:
        print("\n" + "="*80)
        print("⚠️  WARNING: NO DATES AVAILABLE")
        print("="*80)
        print("\nThe cache table is empty or doesn't exist.")
        print("Please run this command first:")
        print("\n  cd Database")
        print("  python update_database.py")
        print("\n" + "="*80)
    
    print("\nPress Ctrl+C to stop\n")
    
    threading.Thread(target=open_browser, daemon=True).start()
    
    app.run(debug=False, host='0.0.0.0', port=5000)