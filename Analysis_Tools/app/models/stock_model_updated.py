# Analysis_Tools/app/models/stock_model.py
# Final stock_model.py — uses TBL_<SYMBOL>_DERIVED and TBL_<SYMBOL>
# Provides: get_available_dates, get_stock_detail_data, get_stock_expiry_data,
#           get_stock_stats, get_stock_chart_data
#
# Behavior:
# - Primary source: TBL_<SYMBOL>_DERIVED (contains greeks: delta, vega, gamma, theta, iv)
# - Fallback: TBL_<SYMBOL> (base option table) if derived table not present
# - Calculates PrevOI / PrevPrice (by joining with previous BizDt) and percent changes
# - All output column names match templates used in controllers/templates.
# =============================================================

from sqlalchemy import create_engine, inspect, text
import pandas as pd
from urllib.parse import quote_plus
from datetime import datetime

# -------------------------
# DB engine (same as other models)
# -------------------------
db_user = 'postgres'
db_password = 'Gallop@3104'
db_host = 'localhost'
db_port = '5432'
db_name = 'BhavCopy_Database'
db_password_enc = quote_plus(db_password)

engine = create_engine(
    f'postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}'
)


# -------------------------
# Utilities
# -------------------------
def _derived_table_name(ticker: str) -> str:
    return f'TBL_{ticker.upper()}_DERIVED'


def _base_table_name(ticker: str) -> str:
    return f'TBL_{ticker.upper()}'


def get_available_dates():
    """
    Return sorted list of BizDt strings (YYYY-MM-DD) using any *_DERIVED table if present.
    """
    try:
        inspector = inspect(engine)
        tables = [t for t in inspector.get_table_names(schema='public') if t.endswith('_DERIVED')]
        if not tables:
            # fallback: find any TBL_ table
            tables = [t for t in inspector.get_table_names(schema='public') if t.startswith('TBL_')]
            if not tables:
                return []
            sample = tables[0]
        else:
            sample = tables[0]

        # Query distinct BizDt from the sample derived table
        q = text(f'SELECT DISTINCT "BizDt" FROM public."{sample}" WHERE "BizDt" IS NOT NULL ORDER BY "BizDt" DESC')
        df = pd.read_sql(q, con=engine)
        # convert to string yyyy-mm-dd
        dates = [d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d) for d in df['BizDt']]
        return dates
    except Exception as e:
        print(f"[ERROR] get_available_dates(): {e}")
        return []


def get_all_tickers():
    """Get list of all available ticker symbols from database"""
    try:
        inspector = inspect(engine)
        tables = [t for t in inspector.get_table_names(schema='public') 
                 if t.startswith('TBL_') and t.endswith('_DERIVED')]
        tickers = sorted([t.replace('TBL_', '').replace('_DERIVED', '') for t in tables])
        return tickers
    except Exception as e:
        print(f"[ERROR] get_all_tickers(): {e}")
        return []


def _get_prev_date(selected_date, dates_list):
    """
    Given selected_date (YYYY-MM-DD) and dates_list sorted desc, find the immediate previous date.
    """
    try:
        i = dates_list.index(selected_date)
        if i + 1 < len(dates_list):
            return dates_list[i + 1]
        return None
    except ValueError:
        return None


# -------------------------
# Core functions
# -------------------------
def get_stock_detail_data(ticker: str, selected_date: str, selected_expiry: str = None):
    """
    Fetch option-chain rows merged with derived greeks (if available).
    NOW FILTERS BY EXPIRY if provided.
    Returns list of dicts with fields used by templates:
    BizDt, FininstrmActlXpryDt, StrkPric, OptnTp, OpnIntrst, ChngInOpnIntrst,
    TtlTradgVol, ClsPric, UndrlygPric, Vega, Delta, Gamma, Theta,
    PrevOI, OI_Chg_%, PrevPrice, Price_Chg_%
    """
    print(f"[DEBUG] Fetching stock detail for ticker={ticker}, selected_date={selected_date}, selected_expiry={selected_expiry}")
    try:
        # prepare names and dates
        derived_table = _derived_table_name(ticker)
        base_table = _base_table_name(ticker)
        dates = get_available_dates()
        prev_date = _get_prev_date(selected_date, dates)

        # Prefer derived table if exists
        inspector = inspect(engine)
        use_derived = derived_table in inspector.get_table_names(schema='public')

        if use_derived:
            # Inspect available columns first
            cols = [c['name'] for c in inspector.get_columns(derived_table, schema='public')]
            greeks = []
            if 'gamma' in [c.lower() for c in cols]:
                greeks.append('"gamma" AS "Gamma"')
            elif 'Gamma' in cols:
                greeks.append('"Gamma"')
            if 'theta' in [c.lower() for c in cols]:
                greeks.append('"theta" AS "Theta"')
            elif 'Theta' in cols:
                greeks.append('"Theta"')
            if 'delta' in [c.lower() for c in cols]:
                greeks.append('"delta" AS "Delta"')
            elif 'Delta' in cols:
                greeks.append('"Delta"')
            if 'vega' in [c.lower() for c in cols]:
                greeks.append('"vega" AS "Vega"')
            elif 'Vega' in cols:
                greeks.append('"Vega"')
            if 'iv' in [c.lower() for c in cols]:
                greeks.append('"iv" AS "IV"')
            elif 'IV' in cols:
                greeks.append('"IV"')

            greeks_sql = ',\n                    '.join(greeks) if greeks else ''

            params = {"bizdt": selected_date}
            q_curr = text(f'''
                SELECT
                    "BizDt",
                    "FininstrmActlXpryDt",
                    "StrkPric",
                    "OptnTp",
                    "OpnIntrst",
                    "ChngInOpnIntrst",
                    "TtlTradgVol",
                    "ClsPric",
                    "UndrlygPric",
                    {greeks_sql},
                    "LastPric"
                FROM public."{derived_table}"
                WHERE "BizDt" = :bizdt
            ''')
            df_curr = pd.read_sql(q_curr, con=engine, params=params)


            # previous day's values for comparisons
            if prev_date:
                params_p = {"bizdt": prev_date}
                q_prev = text(f'''
                    SELECT
                        "BizDt",
                        "StrkPric",
                        "OptnTp",
                        "OpnIntrst" AS PrevOI,
                        "LastPric" AS PrevLastPric
                    FROM public."{derived_table}"
                    WHERE "BizDt" = :bizdt
                ''')
                df_prev = pd.read_sql(q_prev, con=engine, params=params_p)
            else:
                df_prev = pd.DataFrame(columns=["StrkPric", "OptnTp", "PrevOI", "PrevLastPric"])

        else:
            # Fallback: use base table (no greeks)
            params = {"bizdt": selected_date}
            q_curr = text(f'''
                SELECT
                    "BizDt",
                    "FininstrmActlXpryDt",
                    "StrkPric",
                    "OptnTp",
                    "OpnIntrst",
                    "ChngInOpnIntrst",
                    "TtlTradgVol",
                    "ClsPric",
                    "UndrlygPric",
                    "LastPric"
                FROM public."{base_table}"
                WHERE "BizDt" = :bizdt
            ''')
            df_curr = pd.read_sql(q_curr, con=engine, params=params)

            if prev_date:
                q_prev = text(f'''
                    SELECT
                        "BizDt",
                        "StrkPric",
                        "OptnTp",
                        "OpnIntrst" AS PrevOI,
                        "LastPric" AS PrevLastPric
                    FROM public."{base_table}"
                    WHERE "BizDt" = :bizdt
                ''')
                df_prev = pd.read_sql(q_prev, con=engine, params={"bizdt": prev_date})
            else:
                df_prev = pd.DataFrame(columns=["StrkPric", "OptnTp", "PrevOI", "PrevLastPric"])

        if df_curr.empty:
            # No rows for selected date
            print(f"[INFO] No option rows for {ticker} on {selected_date} (derived_used={use_derived})")
            return []

        # Filter by expiry if provided
        if selected_expiry:
            df_curr['FininstrmActlXpryDt'] = pd.to_datetime(df_curr['FininstrmActlXpryDt'], errors='coerce')
            df_curr = df_curr[df_curr['FininstrmActlXpryDt'].astype(str) == selected_expiry]
            if df_curr.empty:
                print(f"[INFO] No data for expiry {selected_expiry}")
                return []

        # normalize column types
        numeric_cols = ["StrkPric", "OpnIntrst", "TtlTradgVol", "ClsPric", "UndrlygPric", "LastPric",
                        "Delta", "Vega", "Gamma", "Theta", "IV"]
        for c in numeric_cols:
            if c in df_curr.columns:
                df_curr[c] = pd.to_numeric(df_curr[c], errors='coerce')

        # prepare prev df for merge
        if not df_prev.empty:
            df_prev['StrkPric'] = pd.to_numeric(df_prev['StrkPric'], errors='coerce')

            # Dynamically detect available previous columns
            cols_prev = [c for c in df_prev.columns]
            prev_oi_col = None
            prev_price_col = None

            for c in cols_prev:
                if c.lower() in ['prevoi', 'opnintrst', 'openinterest', 'oi']:
                    prev_oi_col = c
                elif c.lower() in ['prevlastpric', 'lastpric', 'lastprice', 'clspric', 'close']:
                    prev_price_col = c

            # If not found, create empty columns so merge doesn't fail
            if prev_oi_col and 'PrevOI' not in df_prev.columns:
                df_prev.rename(columns={prev_oi_col: 'PrevOI'}, inplace=True)
            elif 'PrevOI' not in df_prev.columns:
                df_prev['PrevOI'] = None

            if prev_price_col and 'PrevLastPric' not in df_prev.columns:
                df_prev.rename(columns={prev_price_col: 'PrevLastPric'}, inplace=True)
            elif 'PrevLastPric' not in df_prev.columns:
                df_prev['PrevLastPric'] = None

            # ✅ Safe merge
            merged = pd.merge(
                df_curr,
                df_prev[['StrkPric', 'OptnTp', 'PrevOI', 'PrevLastPric']],
                on=['StrkPric', 'OptnTp'],
                how='left'
            )
        else:
            merged = df_curr.copy()
            merged['PrevOI'] = None
            merged['PrevLastPric'] = None


        # compute percent changes
        def safe_pct(curr, prev):
            try:
                if pd.isna(prev) or prev == 0:
                    return None
                return round((curr - prev) / prev * 100, 2)
            except Exception:
                return None

        # Build output rows in template-friendly names
        out_rows = []
        for _, r in merged.iterrows():
            row = {}
            row['BizDt'] = str(r.get('BizDt')) if r.get('BizDt') is not None else None
            row['FininstrmActlXpryDt'] = str(r.get('FininstrmActlXpryDt')) if r.get('FininstrmActlXpryDt') is not None else None
            row['StrkPric'] = r.get('StrkPric')
            row['OptnTp'] = r.get('OptnTp')
            row['OpnIntrst'] = int(r.get('OpnIntrst')) if pd.notna(r.get('OpnIntrst')) else None
            row['ChngInOpnIntrst'] = r.get('ChngInOpnIntrst')
            row['TtlTradgVol'] = int(r.get('TtlTradgVol')) if pd.notna(r.get('TtlTradgVol')) else None
            row['ClsPric'] = float(r.get('ClsPric')) if pd.notna(r.get('ClsPric')) else None
            row['UndrlygPric'] = float(r.get('UndrlygPric')) if pd.notna(r.get('UndrlygPric')) else None

            # Greeks (may be missing if we used base table)
            row['Delta'] = round(float(r.get('Delta')), 4) if 'Delta' in r and pd.notna(r.get('Delta')) else None
            row['Vega'] = round(float(r.get('Vega')), 4) if 'Vega' in r and pd.notna(r.get('Vega')) else None
            row['Gamma'] = round(float(r.get('Gamma')), 6) if 'Gamma' in r and pd.notna(r.get('Gamma')) else None
            row['Theta'] = round(float(r.get('Theta')), 6) if 'Theta' in r and pd.notna(r.get('Theta')) else None
            row['IV'] = round(float(r.get('IV')), 4) if 'IV' in r and pd.notna(r.get('IV')) else None


            # previous values
            prev_oi = r.get('PrevOI') if 'PrevOI' in r else None
            prev_price = r.get('PrevLastPric') if 'PrevLastPric' in r else None
            row['PrevOI'] = int(prev_oi) if pd.notna(prev_oi) else None
            row['PrevPrice'] = float(prev_price) if pd.notna(prev_price) else None

            # change percentages
            row['OI_Chg_%'] = safe_pct(row['OpnIntrst'], row['PrevOI']) if row['OpnIntrst'] is not None else None
            row['Price_Chg_%'] = safe_pct(row['ClsPric'] if row['ClsPric'] is not None else r.get('LastPric'),
                                           row['PrevPrice']) if (row.get('PrevPrice') is not None) else None

            out_rows.append(row)
        print(f"[DEBUG] {ticker} rows fetched: {len(out_rows)}")


        return out_rows

    except Exception as e:
        print(f"[ERROR] get_stock_detail_data({ticker},{selected_date},{selected_expiry}): {e}")
        import traceback
        traceback.print_exc()
        return []


def get_stock_expiry_data(ticker: str, selected_date: str):
    """
    Get expiry-wise summary with price, volume, OI and their changes.
    Returns: expiry, price, price_chg, volume, oi, oi_chg
    """
    try:
        derived_table = _derived_table_name(ticker)
        base_table = _base_table_name(ticker)
        inspector = inspect(engine)
        table_to_use = derived_table if derived_table in inspector.get_table_names(schema='public') else base_table

        # Get previous date
        dates = get_available_dates()
        prev_date = _get_prev_date(selected_date, dates)

        # Current day futures data (OptnTp IS NULL = futures)
        q_curr = text(f'''
            SELECT 
                "FininstrmActlXpryDt"::text as expiry,
                "ClsPric" as price,
                "OpnIntrst" as oi,
                "ChngInOpnIntrst" as oi_chg
            FROM public."{table_to_use}"
            WHERE "BizDt" = :bizdt
            AND "FininstrmActlXpryDt" IS NOT NULL
            AND "OptnTp" IS NULL
            ORDER BY "FininstrmActlXpryDt"
        ''')
        df_curr = pd.read_sql(q_curr, con=engine, params={"bizdt": selected_date})

        # Get volume from options (CE + PE summed)
        q_vol = text(f'''
            SELECT 
                "FininstrmActlXpryDt"::text as expiry,
                SUM("TtlTradgVol") as volume
            FROM public."{table_to_use}"
            WHERE "BizDt" = :bizdt
            AND "FininstrmActlXpryDt" IS NOT NULL
            AND "OptnTp" IN ('CE', 'PE')
            GROUP BY "FininstrmActlXpryDt"
        ''')
        df_vol = pd.read_sql(q_vol, con=engine, params={"bizdt": selected_date})

        # Merge volume
        if not df_vol.empty:
            df_curr = pd.merge(df_curr, df_vol, on='expiry', how='left')
        else:
            df_curr['volume'] = 0

        # Previous day price for price_chg calculation
        prev_prices = {}
        if prev_date:
            q_prev = text(f'''
                SELECT 
                    "FininstrmActlXpryDt"::text as expiry,
                    "ClsPric" as prev_price
                FROM public."{table_to_use}"
                WHERE "BizDt" = :bizdt
                AND "FininstrmActlXpryDt" IS NOT NULL
                AND "OptnTp" IS NULL
            ''')
            df_prev = pd.read_sql(q_prev, con=engine, params={"bizdt": prev_date})
            if not df_prev.empty:
                for _, row in df_prev.iterrows():
                    prev_prices[row['expiry']] = float(row['prev_price'])

        # Build result
        result = []
        for _, row in df_curr.iterrows():
            expiry = row['expiry']
            curr_price = float(row['price']) if pd.notna(row['price']) else 0
            prev_price = prev_prices.get(expiry, 0)
            
            price_chg = 0
            if prev_price > 0 and curr_price > 0:
                price_chg = ((curr_price - prev_price) / prev_price) * 100

            result.append({
                'expiry': expiry,
                'price': curr_price,
                'price_chg': round(price_chg, 2),
                'volume': int(row['volume']) if pd.notna(row['volume']) else 0,
                'oi': int(row['oi']) if pd.notna(row['oi']) else 0,
                'oi_chg': int(row['oi_chg']) if pd.notna(row['oi_chg']) else 0
            })

        return result
    except Exception as e:
        print(f"[ERROR] get_stock_expiry_data({ticker},{selected_date}): {e}")
        import traceback
        traceback.print_exc()
        return []


def get_stock_stats(ticker: str, selected_date: str, selected_expiry: str = None):
    """
    Comprehensive stats matching stock_detail_stats.html template.
    NOW FILTERS BY EXPIRY if provided.
    Returns all stats shown in the screenshot.
    """
    try:
        derived_table = _derived_table_name(ticker)
        base_table = _base_table_name(ticker)
        inspector = inspect(engine)
        table_to_use = derived_table if derived_table in inspector.get_table_names(schema='public') else base_table

        # Build WHERE clause and params
        query_filter = '"BizDt" = :bizdt'
        params = {"bizdt": selected_date}
        
        if selected_expiry:
            query_filter += ' AND "FininstrmActlXpryDt" = :expiry'
            params["expiry"] = selected_expiry
            print(f"[DEBUG] get_stock_stats: Filtering by expiry {selected_expiry}")

        # Main aggregation query
        q = text(f'''
            SELECT
                SUM(CASE WHEN "OptnTp" = 'CE' THEN "OpnIntrst" ELSE 0 END) AS total_ce_oi,
                SUM(CASE WHEN "OptnTp" = 'PE' THEN "OpnIntrst" ELSE 0 END) AS total_pe_oi,
                SUM(CASE WHEN "OptnTp" = 'CE' THEN "ChngInOpnIntrst" ELSE 0 END) AS total_ce_oi_chg,
                SUM(CASE WHEN "OptnTp" = 'PE' THEN "ChngInOpnIntrst" ELSE 0 END) AS total_pe_oi_chg
            FROM public."{table_to_use}"
            WHERE {query_filter}
        ''')
        df = pd.read_sql(q, con=engine, params=params)
        
        if df.empty:
            return {}
        
        row = df.iloc[0]
        total_ce_oi = int(row['total_ce_oi']) if pd.notna(row['total_ce_oi']) else 0
        total_pe_oi = int(row['total_pe_oi']) if pd.notna(row['total_pe_oi']) else 0
        total_ce_oi_chg = int(row['total_ce_oi_chg']) if pd.notna(row['total_ce_oi_chg']) else 0
        total_pe_oi_chg = int(row['total_pe_oi_chg']) if pd.notna(row['total_pe_oi_chg']) else 0

        # Calculate differences
        diff_pe_ce_oi = total_pe_oi - total_ce_oi
        diff_pe_ce_oi_chg = total_pe_oi_chg - total_ce_oi_chg

        # Determine trends (PCR > 1 = Bullish, PCR < 1 = Bearish)
        pcr_oi = total_pe_oi / total_ce_oi if total_ce_oi > 0 else 0
        trend_oi = "Bullish" if pcr_oi > 1 else "Bearish" if pcr_oi < 1 else "Neutral"
        trend_oi_chg = "Bullish" if diff_pe_ce_oi_chg > 0 else "Bearish" if diff_pe_ce_oi_chg < 0 else "Neutral"

        # Get max strikes - also filter by expiry
        q_strikes = text(f'''
            WITH ce_oi AS (
                SELECT "StrkPric", "OpnIntrst", "ChngInOpnIntrst"
                FROM public."{table_to_use}"
                WHERE {query_filter} AND "OptnTp" = 'CE'
                ORDER BY "OpnIntrst" DESC LIMIT 1
            ),
            ce_oi_chg AS (
                SELECT "StrkPric", "ChngInOpnIntrst"
                FROM public."{table_to_use}"
                WHERE {query_filter} AND "OptnTp" = 'CE'
                ORDER BY "ChngInOpnIntrst" DESC LIMIT 1
            ),
            pe_oi AS (
                SELECT "StrkPric", "OpnIntrst", "ChngInOpnIntrst"
                FROM public."{table_to_use}"
                WHERE {query_filter} AND "OptnTp" = 'PE'
                ORDER BY "OpnIntrst" DESC LIMIT 1
            ),
            pe_oi_chg AS (
                SELECT "StrkPric", "ChngInOpnIntrst"
                FROM public."{table_to_use}"
                WHERE {query_filter} AND "OptnTp" = 'PE'
                ORDER BY "ChngInOpnIntrst" DESC LIMIT 1
            )
            SELECT 
                (SELECT "StrkPric" FROM ce_oi) as max_ce_oi_strike,
                (SELECT "StrkPric" FROM ce_oi_chg) as max_ce_oi_chg_strike,
                (SELECT "StrkPric" FROM pe_oi) as max_pe_oi_strike,
                (SELECT "StrkPric" FROM pe_oi_chg) as max_pe_oi_chg_strike
        ''')
        df_strikes = pd.read_sql(q_strikes, con=engine, params=params)

        # Format helper
        def format_crores(val):
            if val >= 1e7:
                return f"{val/1e7:.2f} Cr"
            elif val >= 1e5:
                return f"{val/1e5:.2f} L"
            return f"{val:.0f}"

        result = {
            "total_ce_oi": format_crores(total_ce_oi),
            "total_pe_oi": format_crores(total_pe_oi),
            "total_ce_oi_chg": format_crores(total_ce_oi_chg),
            "total_pe_oi_chg": format_crores(total_pe_oi_chg),
            "diff_pe_ce_oi": format_crores(diff_pe_ce_oi),
            "diff_pe_ce_oi_chg": format_crores(diff_pe_ce_oi_chg),
            "trend_oi": trend_oi,
            "trend_oi_chg": trend_oi_chg,
            "pcr_oi": round(pcr_oi, 2)
        }

        # Add strikes if available
        if not df_strikes.empty:
            strike_row = df_strikes.iloc[0]
            result["max_ce_oi_strike"] = int(strike_row['max_ce_oi_strike']) if pd.notna(strike_row['max_ce_oi_strike']) else "N/A"
            result["max_ce_oi_chg_strike"] = int(strike_row['max_ce_oi_chg_strike']) if pd.notna(strike_row['max_ce_oi_chg_strike']) else "N/A"
            result["max_pe_oi_strike"] = int(strike_row['max_pe_oi_strike']) if pd.notna(strike_row['max_pe_oi_strike']) else "N/A"
            result["max_pe_oi_chg_strike"] = int(strike_row['max_pe_oi_chg_strike']) if pd.notna(strike_row['max_pe_oi_chg_strike']) else "N/A"

        print(f"[DEBUG] get_stock_stats result: PCR={result['pcr_oi']}, CE_OI={result['total_ce_oi']}, PE_OI={result['total_pe_oi']}")
        return result
        
    except Exception as e:
        print(f"[ERROR] get_stock_stats({ticker},{selected_date},{selected_expiry}): {e}")
        import traceback
        traceback.print_exc()
        return {}


def get_stock_chart_data(ticker: str, days: int = 90):
    """
    Fetch OHLC/price history for 'ticker' using base TBL_<TICKER>.
    Returns list of {date, open, high, low, close} in chronological order (oldest -> newest).
    """
    try:
        base_table = _base_table_name(ticker)
        inspector = inspect(engine)
        if base_table not in inspector.get_table_names(schema='public'):
            # fallback: try derived table for date/close columns
            derived_table = _derived_table_name(ticker)
            if derived_table not in inspector.get_table_names(schema='public'):
                return []

            q = text(f'''
                SELECT "BizDt"::text AS date, "OpnPric" AS open, "HghPric" AS high, "LwPric" AS low, "ClsPric" AS close
                FROM public."{derived_table}"
                WHERE "OpnPric" IS NOT NULL
                ORDER BY "BizDt" DESC
                LIMIT :days;
            ''')
        else:
            q = text(f'''
                SELECT "BizDt"::text AS date, "OpnPric" AS open, "HghPric" AS high, "LwPric" AS low, "ClsPric" AS close
                FROM public."{base_table}"
                WHERE "OpnPric" IS NOT NULL
                ORDER BY "BizDt" DESC
                LIMIT :days;
            ''')

        df = pd.read_sql(q, con=engine, params={"days": days})
        if df.empty:
            return []
        df = df.sort_values("date")
        # ensure numeric
        for col in ["open", "high", "low", "close"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        return df.to_dict(orient='records')
    except Exception as e:
        print(f"[ERROR] get_stock_chart_data({ticker},{days}): {e}")
        return []
