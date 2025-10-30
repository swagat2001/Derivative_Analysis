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
def get_stock_detail_data(ticker: str, selected_date: str):
    """
    Fetch option-chain rows merged with derived greeks (if available).
    Returns list of dicts with fields used by templates:
    BizDt, FininstrmActlXpryDt, StrkPric, OptnTp, OpnIntrst, ChngInOpnIntrst,
    TtlTradgVol, ClsPric, UndrlygPric, Vega, Delta, Gamma, Theta,
    PrevOI, OI_Chg_%, PrevPrice, Price_Chg_%
    """
    print(f"[DEBUG] Fetching stock detail for ticker={ticker}, selected_date={selected_date}")
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

        # normalize column types
        numeric_cols = ["StrkPric", "OpnIntrst", "TtlTradgVol", "ClsPric", "UndrlygPric", "LastPric",
                        "Delta", "Vega", "Gamma", "Theta"]
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

            # If not found, create empty columns so merge doesn’t fail
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
        print(f"[ERROR] get_stock_detail_data({ticker},{selected_date}): {e}")
        return []


def get_stock_expiry_data(ticker: str, selected_date: str):
    """
    Aggregate OI by expiry for the given ticker/date using derived table if present.
    Returns list of dict: expiry, contracts, total_oi, call_oi, put_oi, pcr
    """
    try:
        derived_table = _derived_table_name(ticker)
        base_table = _base_table_name(ticker)
        inspector = inspect(engine)
        table_to_use = derived_table if derived_table in inspector.get_table_names(schema='public') else base_table

        q = text(f'''
            WITH base AS (
                SELECT
                    "FininstrmActlXpryDt" AS expiry,
                    "OptnTp",
                    "OpnIntrst"
                FROM public."{table_to_use}"
                WHERE "BizDt" = :bizdt
            )
            SELECT
                expiry::text AS expiry,
                COUNT(*) AS contracts,
                SUM("OpnIntrst") AS total_oi,
                SUM(CASE WHEN "OptnTp" = 'CE' THEN "OpnIntrst" ELSE 0 END) AS call_oi,
                SUM(CASE WHEN "OptnTp" = 'PE' THEN "OpnIntrst" ELSE 0 END) AS put_oi,
                ROUND(
                    CASE WHEN SUM(CASE WHEN "OptnTp" = 'CE' THEN "OpnIntrst" ELSE 0 END) > 0
                    THEN SUM(CASE WHEN "OptnTp" = 'PE' THEN "OpnIntrst" ELSE 0 END) /
                         SUM(CASE WHEN "OptnTp" = 'CE' THEN "OpnIntrst" ELSE 0 END)
                    ELSE NULL END, 2
                ) AS pcr
            FROM base
            GROUP BY expiry
            ORDER BY expiry;
        ''')
        df = pd.read_sql(q, con=engine, params={"bizdt": selected_date})
        return df.to_dict(orient='records')
    except Exception as e:
        print(f"[ERROR] get_stock_expiry_data({ticker},{selected_date}): {e}")
        return []


def get_stock_stats(ticker: str, selected_date: str):
    """
    Aggregated top-level stats for the stock (current_price, expiry_count, total_call_oi, total_put_oi, pcr_oi, pcr_volume)
    """
    try:
        derived_table = _derived_table_name(ticker)
        base_table = _base_table_name(ticker)
        inspector = inspect(engine)
        table_to_use = derived_table if derived_table in inspector.get_table_names(schema='public') else base_table

        q = text(f'''
            WITH base AS (
                SELECT
                    "OptnTp",
                    "OpnIntrst",
                    "TtlTradgVol",
                    "UndrlygPric",
                    "FininstrmActlXpryDt"
                FROM public."{table_to_use}"
                WHERE "BizDt" = :bizdt
            )
            SELECT
                ROUND(MAX("UndrlygPric"), 2) AS current_price,
                COUNT(DISTINCT "FininstrmActlXpryDt") AS expiry_count,
                SUM(CASE WHEN "OptnTp" = 'CE' THEN "OpnIntrst" ELSE 0 END) AS total_call_oi,
                SUM(CASE WHEN "OptnTp" = 'PE' THEN "OpnIntrst" ELSE 0 END) AS total_put_oi,
                ROUND(
                    CASE
                        WHEN SUM(CASE WHEN "OptnTp" = 'CE' THEN "OpnIntrst" ELSE 0 END) > 0
                        THEN SUM(CASE WHEN "OptnTp" = 'PE' THEN "OpnIntrst" ELSE 0 END) /
                             SUM(CASE WHEN "OptnTp" = 'CE' THEN "OpnIntrst" ELSE 0 END)
                        ELSE NULL
                    END, 2
                ) AS pcr_oi,
                ROUND(
                    CASE
                        WHEN SUM(CASE WHEN "OptnTp" = 'CE' THEN "TtlTradgVol" ELSE 0 END) > 0
                        THEN SUM(CASE WHEN "OptnTp" = 'PE' THEN "TtlTradgVol" ELSE 0 END) /
                             SUM(CASE WHEN "OptnTp" = 'CE' THEN "TtlTradgVol" ELSE 0 END)
                        ELSE NULL
                    END, 2
                ) AS pcr_volume
            FROM base;
        ''')
        df = pd.read_sql(q, con=engine, params={"bizdt": selected_date})
        if df.empty:
            return {}
        row = df.iloc[0].to_dict()
        return {
            "current_price": float(row.get("current_price") or 0),
            "expiry_count": int(row.get("expiry_count") or 0),
            "total_call_oi": int(row.get("total_call_oi") or 0),
            "total_put_oi": int(row.get("total_put_oi") or 0),
            "pcr_oi": row.get("pcr_oi"),
            "pcr_volume": row.get("pcr_volume")
        }
    except Exception as e:
        print(f"[ERROR] get_stock_stats({ticker},{selected_date}): {e}")
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
