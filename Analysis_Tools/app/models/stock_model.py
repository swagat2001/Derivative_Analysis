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

from datetime import datetime
from functools import lru_cache

import pandas as pd
from flask_caching import Cache
from sqlalchemy import inspect, text

from .db_config import engine, get_stock_list_from_excel

# Initialize cache with 5-minute timeout
cache = Cache(config={"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300})

# -------------------------
# DB engine (imported from shared db_config)
# -------------------------


# Cache for table existence and column info
_table_cache = {}  # {table_name: exists}
_column_cache = {}  # {table_name: [column_names]}
_inspector_cache = None  # Cached inspector instance


def _get_inspector():
    """Get cached inspector instance."""
    global _inspector_cache
    if _inspector_cache is None:
        _inspector_cache = inspect(engine)
    return _inspector_cache


def _table_exists(table_name: str) -> bool:
    """Check if table exists (cached)."""
    if table_name not in _table_cache:
        try:
            inspector = _get_inspector()
            _table_cache[table_name] = table_name in inspector.get_table_names(schema="public")
        except Exception as e:
            print(f"[ERROR] _table_exists({table_name}): {e}")
            _table_cache[table_name] = False
    return _table_cache[table_name]


def _get_table_columns(table_name: str) -> list:
    """Get table columns (cached)."""
    if table_name not in _column_cache:
        try:
            inspector = _get_inspector()
            cols = [c["name"] for c in inspector.get_columns(table_name, schema="public")]
            _column_cache[table_name] = cols
        except Exception as e:
            print(f"[ERROR] _get_table_columns({table_name}): {e}")
            _column_cache[table_name] = []
    return _column_cache[table_name]


def clear_table_cache():
    """Clear table and column cache - useful when tables are added/modified."""
    global _table_cache, _column_cache, _inspector_cache
    _table_cache = {}
    _column_cache = {}
    _inspector_cache = None
    print("[INFO] Table cache cleared")


@cache.memoize(timeout=300)
def _get_table_list_cached():
    """Cached table list to avoid repeated inspector calls."""
    try:
        inspector = _get_inspector()
        tables = [t for t in inspector.get_table_names(schema="public") if t.endswith("_DERIVED")]
        if not tables:
            tables = [t for t in inspector.get_table_names(schema="public") if t.startswith("TBL_")]
        return tables[0] if tables else None
    except Exception as e:
        print(f"[ERROR] _get_table_list_cached(): {e}")
        return None


@cache.memoize(timeout=300)
def _get_available_dates_stock_cached():
    """Internal cached function for dates."""
    try:
        sample = _get_table_list_cached()
        if not sample:
            return tuple()

        # Query distinct BizDt from the sample derived table
        q = text(f'SELECT DISTINCT "BizDt" FROM public."{sample}" WHERE "BizDt" IS NOT NULL ORDER BY "BizDt" DESC')
        df = pd.read_sql(q, con=engine)
        # convert to string yyyy-mm-dd
        dates = [d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d) for d in df["BizDt"]]
        return tuple(dates)
    except Exception as e:
        print(f"[ERROR] _get_available_dates_stock_cached(): {e}")
        return tuple()


def get_available_dates():
    """
    Return sorted list of BizDt strings (YYYY-MM-DD) using any *_DERIVED table if present.
    Uses caching to avoid repeated database queries.
    """
    return list(_get_available_dates_stock_cached())


def clear_date_cache():
    """Clear date and table cache - useful when new data is added to database."""
    # With flask_caching, we can clear the entire cache or specific keys
    # For now, clearing everything is safest as everything depends on DB state
    cache.clear()

    # Also clear internal python caches just in case
    _get_available_dates_stock_cached.delete_memoized()
    _get_table_list_cached.delete_memoized()
    _get_all_tickers_cached.delete_memoized()
    _get_stock_chart_data_cached.delete_memoized()
    _get_stock_expiry_data_cached.delete_memoized()
    _get_stock_stats_cached.delete_memoized()
    _get_stock_detail_data_cached.delete_memoized()

    clear_table_cache()  # Also clear table cache
    print("[INFO] Stock cache cleared")


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
# Utilities
# -------------------------
# Utilities
# -------------------------
def _clean_ticker(ticker: str) -> str:
    """Sanitize ticker to prevent SQL injection and handle common aliases."""
    if not ticker:
        return ""
    # Handle aliases
    ticker = ticker.upper().strip()
    if ticker == "RELI":
        return "RELIANCE"
    # Allow alphanumeric, &, and -
    return "".join(c for c in ticker if c.isalnum() or c in ["&", "-"])


def _derived_table_name(ticker: str) -> str:
    ticker = _clean_ticker(ticker)
    return f"TBL_{ticker}_DERIVED"


def _base_table_name(ticker: str) -> str:
    ticker = _clean_ticker(ticker)
    return f"TBL_{ticker}"


# -------------------------
# Core functions
# -------------------------
# -------------------------
# Core functions
# -------------------------
@cache.memoize(timeout=300)
def _get_stock_detail_data_cached(
    ticker: str,
    selected_date: str,
    selected_expiry: str,
    table_name: str,
    use_derived: bool,
    prev_date: str,
    has_gamma: bool,
    has_theta: bool,
    has_delta: bool,
    has_vega: bool,
    has_iv: bool,
):
    """Cached internal function for stock detail data."""
    try:
        # Build greeks SQL based on flags
        greeks = []
        if has_gamma:
            greeks.append('"gamma" AS "Gamma"')
        if has_theta:
            greeks.append('"theta" AS "Theta"')
        if has_delta:
            greeks.append('"delta" AS "Delta"')
        if has_vega:
            greeks.append('"vega" AS "Vega"')
        if has_iv:
            greeks.append('"iv" AS "IV"')
        greeks_sql = ",\n                    ".join(greeks) if greeks else ""

        #  OPTIMIZED: Single combined query for current + previous data (instead of 2 separate queries)
        if use_derived:
            if prev_date:
                # Combined query using CTE
                # FIX #1 & #4: Add expiry filter to prev_data and use DATE casting for consistency
                combined_query = text(
                    f"""
                    WITH current_data AS (
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
                        FROM public."{table_name}"
                        WHERE "BizDt" = :curr_date
                            {' AND "FininstrmActlXpryDt"::DATE = CAST(:expiry AS DATE)' if selected_expiry else ''}
                    ),
                    prev_data AS (
                        SELECT
                            "StrkPric",
                            "OptnTp",
                            "OpnIntrst" AS "PrevOI",
                            "LastPric" AS "PrevLastPric"
                        FROM public."{table_name}"
                        WHERE "BizDt" = :prev_date
                            {' AND "FininstrmActlXpryDt"::DATE = CAST(:expiry AS DATE)' if selected_expiry else ''}
                    )
                    SELECT
                        c.*,
                        p."PrevOI",
                        p."PrevLastPric"
                    FROM current_data c
                    LEFT JOIN prev_data p
                        ON c."StrkPric" = p."StrkPric"
                        AND c."OptnTp" = p."OptnTp"
                    ORDER BY c."StrkPric", c."OptnTp"
                """
                )
                params = {"curr_date": selected_date, "prev_date": prev_date}
                if selected_expiry:
                    params["expiry"] = selected_expiry
                df_curr = pd.read_sql(combined_query, con=engine, params=params)
                df_prev = pd.DataFrame()  # Already merged in query
            else:
                # No previous date - simple query
                # FIX #4: Use DATE casting for expiry filter consistency
                params = {"bizdt": selected_date}
                if selected_expiry:
                    q_curr = text(
                        f"""
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
                        FROM public."{table_name}"
                        WHERE "BizDt" = :bizdt
                            AND "FininstrmActlXpryDt"::DATE = CAST(:expiry AS DATE)
                    """
                    )
                    params["expiry"] = selected_expiry
                else:
                    q_curr = text(
                        f"""
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
                        FROM public."{table_name}"
                        WHERE "BizDt" = :bizdt
                    """
                    )
                df_curr = pd.read_sql(q_curr, con=engine, params=params)
                df_prev = pd.DataFrame(columns=["StrkPric", "OptnTp", "PrevOI", "PrevLastPric"])

        else:
            # Fallback: use base table (no greeks) - also optimized with combined query
            # FIX #1 & #4: Add expiry filter to prev_data and use DATE casting
            if prev_date:
                combined_query = text(
                    f"""
                    WITH current_data AS (
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
                        FROM public."{table_name}"
                        WHERE "BizDt" = :curr_date
                            {' AND "FininstrmActlXpryDt"::DATE = CAST(:expiry AS DATE)' if selected_expiry else ''}
                    ),
                    prev_data AS (
                        SELECT
                            "StrkPric",
                            "OptnTp",
                            "OpnIntrst" AS "PrevOI",
                            "LastPric" AS "PrevLastPric"
                        FROM public."{table_name}"
                        WHERE "BizDt" = :prev_date
                            {' AND "FininstrmActlXpryDt"::DATE = CAST(:expiry AS DATE)' if selected_expiry else ''}
                    )
                    SELECT
                        c.*,
                        p."PrevOI",
                        p."PrevLastPric"
                    FROM current_data c
                    LEFT JOIN prev_data p
                        ON c."StrkPric" = p."StrkPric"
                        AND c."OptnTp" = p."OptnTp"
                    ORDER BY c."StrkPric", c."OptnTp"
                """
                )
                params = {"curr_date": selected_date, "prev_date": prev_date}
                if selected_expiry:
                    params["expiry"] = selected_expiry
                df_curr = pd.read_sql(combined_query, con=engine, params=params)
                df_prev = pd.DataFrame()  # Already merged
            else:
                # FIX #4: Use DATE casting for expiry filter consistency
                params = {"bizdt": selected_date}
                if selected_expiry:
                    q_curr = text(
                        f"""
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
                        FROM public."{table_name}"
                        WHERE "BizDt" = :bizdt
                            AND "FininstrmActlXpryDt"::DATE = CAST(:expiry AS DATE)
                    """
                    )
                    params["expiry"] = selected_expiry
                else:
                    q_curr = text(
                        f"""
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
                        FROM public."{table_name}"
                        WHERE "BizDt" = :bizdt
                    """
                    )
                df_curr = pd.read_sql(q_curr, con=engine, params=params)
                df_prev = pd.DataFrame(columns=["StrkPric", "OptnTp", "PrevOI", "PrevLastPric"])

        if df_curr.empty:
            return tuple()

        # FIX #3: Expiry filtering is now done in SQL with DATE casting, so we don't need pandas filtering
        # This ensures consistency and avoids date format issues
        # The query above already filters by expiry when selected_expiry is provided

        # normalize column types
        numeric_cols = [
            "StrkPric",
            "OpnIntrst",
            "ChngInOpnIntrst",
            "TtlTradgVol",
            "ClsPric",
            "UndrlygPric",
            "LastPric",
            "Delta",
            "Vega",
            "Gamma",
            "Theta",
        ]
        for c in numeric_cols:
            if c in df_curr.columns:
                df_curr[c] = pd.to_numeric(df_curr[c], errors="coerce")

        # prepare prev df for merge (only if not already merged in combined query)
        if "PrevOI" in df_curr.columns:
            # Already merged in combined query - no need to merge again
            merged = df_curr.copy()
        elif not df_prev.empty:
            # Need to merge (old code path for backward compatibility)
            df_prev["StrkPric"] = pd.to_numeric(df_prev["StrkPric"], errors="coerce")
            cols_prev = [c for c in df_prev.columns]
            prev_oi_col = None
            prev_price_col = None

            for c in cols_prev:
                if c.lower() in ["prevoi", "opnintrst", "openinterest", "oi"]:
                    prev_oi_col = c
                elif c.lower() in ["prevlastpric", "lastpric", "lastprice", "clspric", "close"]:
                    prev_price_col = c

            if prev_oi_col and "PrevOI" not in df_prev.columns:
                df_prev.rename(columns={prev_oi_col: "PrevOI"}, inplace=True)
            elif "PrevOI" not in df_prev.columns:
                df_prev["PrevOI"] = None

            if prev_price_col and "PrevLastPric" not in df_prev.columns:
                df_prev.rename(columns={prev_price_col: "PrevLastPric"}, inplace=True)
            elif "PrevLastPric" not in df_prev.columns:
                df_prev["PrevLastPric"] = None

            merged = pd.merge(
                df_curr,
                df_prev[["StrkPric", "OptnTp", "PrevOI", "PrevLastPric"]],
                on=["StrkPric", "OptnTp"],
                how="left",
            )
        else:
            # No previous data available
            merged = df_curr.copy()
            if "PrevOI" not in merged.columns:
                merged["PrevOI"] = None
            if "PrevLastPric" not in merged.columns:
                merged["PrevLastPric"] = None

        # Build result as tuple for caching
        def safe_pct(curr, prev):
            try:
                if pd.isna(prev) or prev == 0:
                    return None
                return round((curr - prev) / prev * 100, 2)
            except Exception:
                return None

        result = []
        for _, r in merged.iterrows():
            # Convert row to tuple (hashable for caching)
            row_tuple = (
                str(r.get("BizDt")) if r.get("BizDt") is not None else None,
                str(r.get("FininstrmActlXpryDt")) if r.get("FininstrmActlXpryDt") is not None else None,
                r.get("StrkPric"),
                r.get("OptnTp"),
                int(r.get("OpnIntrst")) if pd.notna(r.get("OpnIntrst")) else None,
                int(r.get("ChngInOpnIntrst")) if pd.notna(r.get("ChngInOpnIntrst")) else None,
                int(r.get("TtlTradgVol")) if pd.notna(r.get("TtlTradgVol")) else None,
                float(r.get("ClsPric")) if pd.notna(r.get("ClsPric")) else None,
                float(r.get("UndrlygPric")) if pd.notna(r.get("UndrlygPric")) else None,
                round(float(r.get("Delta")), 4) if "Delta" in r and pd.notna(r.get("Delta")) else None,
                round(float(r.get("Vega")), 4) if "Vega" in r and pd.notna(r.get("Vega")) else None,
                round(float(r.get("Gamma")), 6) if "Gamma" in r and pd.notna(r.get("Gamma")) else None,
                round(float(r.get("Theta")), 6) if "Theta" in r and pd.notna(r.get("Theta")) else None,
                round(float(r.get("IV")) * 100, 2)
                if "IV" in r and pd.notna(r.get("IV"))
                else None,
                int(r.get("PrevOI")) if pd.notna(r.get("PrevOI")) else None,
                float(r.get("PrevLastPric")) if pd.notna(r.get("PrevLastPric")) else None,
                safe_pct(
                    int(r.get("OpnIntrst")) if pd.notna(r.get("OpnIntrst")) else None,
                    int(r.get("PrevOI")) if pd.notna(r.get("PrevOI")) else None,
                ),
                safe_pct(
                    float(r.get("ClsPric"))
                    if pd.notna(r.get("ClsPric"))
                    else (r.get("LastPric") if r.get("LastPric") is not None else None),
                    float(r.get("PrevLastPric")) if pd.notna(r.get("PrevLastPric")) else None,
                ),
            )
            result.append(row_tuple)

        return tuple(result)

    except Exception as e:
        print(f"[ERROR] _get_stock_detail_data_cached({ticker},{selected_date}): {e}")
        return tuple()


def get_stock_detail_data(ticker: str, selected_date: str, selected_expiry: str = None):
    """
    Fetch option-chain rows merged with derived greeks (if available).
    NOW FILTERS BY EXPIRY if provided.
    Returns list of dicts with fields used by templates:
    BizDt, FininstrmActlXpryDt, StrkPric, OptnTp, OpnIntrst, ChngInOpnIntrst,
    TtlTradgVol, ClsPric, UndrlygPric, Vega, Delta, Gamma, Theta,
    PrevOI, OI_Chg_%, PrevPrice, Price_Chg_%
    Uses caching to avoid repeated expensive queries.
    """
    print(
        f"[DEBUG] Fetching stock detail for ticker={ticker}, selected_date={selected_date}, selected_expiry={selected_expiry}"
    )
    try:
        # prepare names and dates
        derived_table = _derived_table_name(ticker)
        base_table = _base_table_name(ticker)
        dates = get_available_dates()
        prev_date = _get_prev_date(selected_date, dates) or ""

        # Prefer derived table if exists
        use_derived = _table_exists(derived_table)
        table_to_use = derived_table if use_derived else base_table

        if not _table_exists(table_to_use):
            print(f"[WARN] Table {table_to_use} does not exist for ticker {ticker}")
            return []

        # Check which greeks are available (for cache key)
        has_gamma = False
        has_theta = False
        has_delta = False
        has_vega = False
        has_iv = False
        greeks_sql = ""

        if use_derived:
            cols = _get_table_columns(derived_table)
            greeks = []
            if "gamma" in [c.lower() for c in cols]:
                greeks.append('"gamma" AS "Gamma"')
                has_gamma = True
            elif "Gamma" in cols:
                greeks.append('"Gamma"')
                has_gamma = True
            if "theta" in [c.lower() for c in cols]:
                greeks.append('"theta" AS "Theta"')
                has_theta = True
            elif "Theta" in cols:
                greeks.append('"Theta"')
                has_theta = True
            if "delta" in [c.lower() for c in cols]:
                greeks.append('"delta" AS "Delta"')
                has_delta = True
            elif "Delta" in cols:
                greeks.append('"Delta"')
                has_delta = True
            if "vega" in [c.lower() for c in cols]:
                greeks.append('"vega" AS "Vega"')
                has_vega = True
            elif "Vega" in cols:
                greeks.append('"Vega"')
                has_vega = True
            if "iv" in [c.lower() for c in cols]:
                greeks.append('"iv" AS "IV"')
                has_iv = True
            elif "IV" in cols:
                greeks.append('"IV"')
                has_iv = True
            greeks_sql = ",\n                    ".join(greeks) if greeks else ""

        expiry_key = selected_expiry or ""

        # Get cached data (using boolean flags instead of SQL string for better cache key)
        cached_data = _get_stock_detail_data_cached(
            ticker,
            selected_date,
            expiry_key,
            table_to_use,
            use_derived,
            prev_date,
            has_gamma,
            has_theta,
            has_delta,
            has_vega,
            has_iv,
        )

        if not cached_data:
            print(f"[INFO] No option rows for {ticker} on {selected_date} (derived_used={use_derived})")
            return []

        # Convert tuple back to dict format
        result = []
        for row_tuple in cached_data:
            row = {
                "BizDt": row_tuple[0],
                "FininstrmActlXpryDt": row_tuple[1],
                "StrkPric": row_tuple[2],
                "OptnTp": row_tuple[3],
                "OpnIntrst": row_tuple[4],
                "ChngInOpnIntrst": row_tuple[5],
                "TtlTradgVol": row_tuple[6],
                "ClsPric": row_tuple[7],
                "UndrlygPric": row_tuple[8],
                "Delta": row_tuple[9],
                "Vega": row_tuple[10],
                "Gamma": row_tuple[11],
                "Theta": row_tuple[12],
                "IV": row_tuple[13],
                "PrevOI": row_tuple[14],
                "PrevPrice": row_tuple[15],
                "OI_Chg_%": row_tuple[16],
                "Price_Chg_%": row_tuple[17],
            }
            result.append(row)

        print(f"[DEBUG] {ticker} rows fetched: {len(result)}")
        return result

    except Exception as e:
        print(f"[ERROR] get_stock_detail_data({ticker},{selected_date}): {e}")
        return []


@cache.memoize(timeout=300)
def _get_stock_expiry_data_cached(ticker: str, selected_date: str, table_name: str, prev_date: str):
    """Cached internal function for expiry data."""
    try:
        # Current day futures data (OptnTp IS NULL = futures)
        # FIX: Use DISTINCT ON to get only one row per expiry date
        q_curr = text(
            f"""
            SELECT DISTINCT ON ("FininstrmActlXpryDt")
                "FininstrmActlXpryDt"::text as expiry,
                "ClsPric" as price,
                "OpnIntrst" as oi,
                "ChngInOpnIntrst" as oi_chg
            FROM public."{table_name}"
            WHERE "BizDt" = :bizdt
            AND "FininstrmActlXpryDt" IS NOT NULL
            AND "OptnTp" IS NULL
            ORDER BY "FininstrmActlXpryDt", "ClsPric" DESC
        """
        )
        df_curr = pd.read_sql(q_curr, con=engine, params={"bizdt": selected_date})

        # Get volume from options (CE + PE summed)
        q_vol = text(
            f"""
            SELECT
                "FininstrmActlXpryDt"::text as expiry,
                SUM(CAST("TtlTradgVol" AS BIGINT)) as volume
            FROM public."{table_name}"
            WHERE "BizDt" = :bizdt
            AND "FininstrmActlXpryDt" IS NOT NULL
            AND "OptnTp" IN ('CE', 'PE')
            GROUP BY "FininstrmActlXpryDt"
        """
        )
        df_vol = pd.read_sql(q_vol, con=engine, params={"bizdt": selected_date})

        # Merge volume
        if not df_vol.empty:
            df_curr = pd.merge(df_curr, df_vol, on="expiry", how="left")
        else:
            df_curr["volume"] = 0

        # Previous day price for price_chg calculation
        prev_prices = {}
        if prev_date:
            q_prev = text(
                f"""
                SELECT DISTINCT ON ("FininstrmActlXpryDt")
                    "FininstrmActlXpryDt"::text as expiry,
                    "ClsPric" as prev_price
                FROM public."{table_name}"
                WHERE "BizDt" = :bizdt
                AND "FininstrmActlXpryDt" IS NOT NULL
                AND "OptnTp" IS NULL
                ORDER BY "FininstrmActlXpryDt", "ClsPric" DESC
            """
            )
            df_prev = pd.read_sql(q_prev, con=engine, params={"bizdt": prev_date})
            if not df_prev.empty:
                for _, row in df_prev.iterrows():
                    prev_prices[row["expiry"]] = float(row["prev_price"])

        # Build result as tuple for caching
        result = []
        for _, row in df_curr.iterrows():
            expiry = row["expiry"]
            curr_price = float(row["price"]) if pd.notna(row["price"]) else 0
            prev_price = prev_prices.get(expiry, 0)

            price_chg = 0
            if prev_price > 0 and curr_price > 0:
                price_chg = ((curr_price - prev_price) / prev_price) * 100

            result.append(
                (
                    expiry,
                    curr_price,
                    round(price_chg, 2),
                    int(row["volume"]) if pd.notna(row["volume"]) else 0,
                    int(row["oi"]) if pd.notna(row["oi"]) else 0,
                    int(row["oi_chg"]) if pd.notna(row["oi_chg"]) else 0,
                )
            )

        return tuple(result)
    except Exception as e:
        print(f"[ERROR] get_stock_expiry_data({ticker},{selected_date}): {e}")
        import traceback

        traceback.print_exc()
        return tuple()


def get_stock_expiry_data(ticker: str, selected_date: str):
    """
    Get expiry-wise summary with price, volume, OI and their changes.
    Returns: expiry, price, price_chg, volume, oi, oi_chg
    Uses caching to avoid repeated queries.
    """
    try:
        derived_table = _derived_table_name(ticker)
        base_table = _base_table_name(ticker)
        table_to_use = derived_table if _table_exists(derived_table) else base_table

        if not _table_exists(table_to_use):
            print(f"[WARN] Table {table_to_use} does not exist for ticker {ticker}")
            return []

        # Get previous date
        dates = get_available_dates()
        prev_date = _get_prev_date(selected_date, dates) or ""

        # Get cached data
        cached_data = _get_stock_expiry_data_cached(ticker, selected_date, table_to_use, prev_date)

        if not cached_data:
            return []

        # Convert tuple back to dict format
        result = []
        for row in cached_data:
            result.append(
                {
                    "expiry": row[0],
                    "price": row[1],
                    "price_chg": row[2],
                    "volume": row[3],
                    "oi": row[4],
                    "oi_chg": row[5],
                }
            )

        return result
    except Exception as e:
        print(f"[ERROR] get_stock_expiry_data({ticker},{selected_date}): {e}")
        import traceback

        traceback.print_exc()
        return []


@cache.memoize(timeout=300)
def _get_stock_stats_cached(ticker: str, selected_date: str, selected_expiry: str, table_name: str):
    """Cached internal function for stats data."""
    try:
        # Main aggregation query with proper date casting for expiry
        query_filter = '"BizDt" = :bizdt'
        params = {"bizdt": selected_date}
        if selected_expiry:
            # Cast expiry to DATE to ensure proper comparison (handles string/datetime formats)
            query_filter += ' AND "FininstrmActlXpryDt"::DATE = CAST(:expiry AS DATE)'
            params["expiry"] = selected_expiry

        q = text(
            f"""
            SELECT
                SUM(CASE WHEN "OptnTp" = 'CE' THEN CAST("OpnIntrst" AS BIGINT) ELSE 0 END) AS total_ce_oi,
                SUM(CASE WHEN "OptnTp" = 'PE' THEN CAST("OpnIntrst" AS BIGINT) ELSE 0 END) AS total_pe_oi,
                SUM(CASE WHEN "OptnTp" = 'CE' THEN CAST("ChngInOpnIntrst" AS BIGINT) ELSE 0 END) AS total_ce_oi_chg,
                SUM(CASE WHEN "OptnTp" = 'PE' THEN CAST("ChngInOpnIntrst" AS BIGINT) ELSE 0 END) AS total_pe_oi_chg,
                AVG(CASE WHEN "iv" IS NOT NULL AND "OptnTp" IN ('CE','PE') THEN "iv" ELSE NULL END) AS avg_iv
            FROM public."{table_name}"
            WHERE {query_filter}
        """
        )
        df = pd.read_sql(q, con=engine, params=params)

        if df.empty:
            return None

        row = df.iloc[0]
        total_ce_oi = int(row["total_ce_oi"]) if pd.notna(row["total_ce_oi"]) else 0
        total_pe_oi = int(row["total_pe_oi"]) if pd.notna(row["total_pe_oi"]) else 0
        total_ce_oi_chg = int(row["total_ce_oi_chg"]) if pd.notna(row["total_ce_oi_chg"]) else 0
        total_pe_oi_chg = int(row["total_pe_oi_chg"]) if pd.notna(row["total_pe_oi_chg"]) else 0

        # Calculate differences
        diff_pe_ce_oi = total_pe_oi - total_ce_oi
        diff_pe_ce_oi_chg = total_pe_oi_chg - total_ce_oi_chg

        # Determine trends (PCR > 1 = Bullish, PCR < 1 = Bearish)
        # FIX: If both totals are 0, set trend to "Neutral" instead of "Bearish"
        if total_ce_oi == 0 and total_pe_oi == 0:
            pcr_oi = 0
            trend_oi = "Neutral"
        else:
            pcr_oi = total_pe_oi / total_ce_oi if total_ce_oi > 0 else 0
            trend_oi = "Bullish" if pcr_oi > 1 else "Bearish" if pcr_oi < 1 else "Neutral"

        # FIX: If both changes are 0, set trend to "Neutral"
        if total_ce_oi_chg == 0 and total_pe_oi_chg == 0:
            trend_oi_chg = "Neutral"
        else:
            trend_oi_chg = "Bullish" if diff_pe_ce_oi_chg > 0 else "Bearish" if diff_pe_ce_oi_chg < 0 else "Neutral"

        # Get max strikes - FIX: Only get strikes if there's actual OI data
        max_ce_oi_strike = None
        max_ce_oi_chg_strike = None
        max_pe_oi_strike = None
        max_pe_oi_chg_strike = None

        # Only query for max strikes if we have actual OI data (not all zeros)
        if total_ce_oi > 0 or total_pe_oi > 0 or total_ce_oi_chg != 0 or total_pe_oi_chg != 0:
            q_strikes = text(
                f"""
                WITH ce_oi AS (
                    SELECT "StrkPric", "OpnIntrst", "ChngInOpnIntrst"
                    FROM public."{table_name}"
                    WHERE {query_filter} AND "OptnTp" = 'CE' AND CAST("OpnIntrst" AS BIGINT) > 0
                    ORDER BY CAST("OpnIntrst" AS BIGINT) DESC LIMIT 1
                ),
                ce_oi_chg AS (
                    SELECT "StrkPric", "ChngInOpnIntrst"
                    FROM public."{table_name}"
                    WHERE {query_filter} AND "OptnTp" = 'CE' AND CAST("ChngInOpnIntrst" AS BIGINT) != 0
                    ORDER BY ABS(CAST("ChngInOpnIntrst" AS BIGINT)) DESC LIMIT 1
                ),
                pe_oi AS (
                    SELECT "StrkPric", "OpnIntrst", "ChngInOpnIntrst"
                    FROM public."{table_name}"
                    WHERE {query_filter} AND "OptnTp" = 'PE' AND CAST("OpnIntrst" AS BIGINT) > 0
                    ORDER BY CAST("OpnIntrst" AS BIGINT) DESC LIMIT 1
                ),
                pe_oi_chg AS (
                    SELECT "StrkPric", "ChngInOpnIntrst"
                    FROM public."{table_name}"
                    WHERE {query_filter} AND "OptnTp" = 'PE' AND CAST("ChngInOpnIntrst" AS BIGINT) != 0
                    ORDER BY ABS(CAST("ChngInOpnIntrst" AS BIGINT)) DESC LIMIT 1
                )
                SELECT
                    (SELECT "StrkPric" FROM ce_oi) as max_ce_oi_strike,
                    (SELECT "StrkPric" FROM ce_oi_chg) as max_ce_oi_chg_strike,
                    (SELECT "StrkPric" FROM pe_oi) as max_pe_oi_strike,
                    (SELECT "StrkPric" FROM pe_oi_chg) as max_pe_oi_chg_strike
            """
            )
            df_strikes = pd.read_sql(q_strikes, con=engine, params=params)

            if not df_strikes.empty:
                max_ce_oi_strike = (
                    int(float(df_strikes.iloc[0]["max_ce_oi_strike"]))
                    if pd.notna(df_strikes.iloc[0]["max_ce_oi_strike"])
                    else None
                )
                max_ce_oi_chg_strike = (
                    int(float(df_strikes.iloc[0]["max_ce_oi_chg_strike"]))
                    if pd.notna(df_strikes.iloc[0]["max_ce_oi_chg_strike"])
                    else None
                )
                max_pe_oi_strike = (
                    int(float(df_strikes.iloc[0]["max_pe_oi_strike"]))
                    if pd.notna(df_strikes.iloc[0]["max_pe_oi_strike"])
                    else None
                )
                max_pe_oi_chg_strike = (
                    int(float(df_strikes.iloc[0]["max_pe_oi_chg_strike"]))
                    if pd.notna(df_strikes.iloc[0]["max_pe_oi_chg_strike"])
                    else None
                )

        # Return as tuple for caching (raw values before formatting)
        return (
            total_ce_oi,
            total_pe_oi,
            total_ce_oi_chg,
            total_pe_oi_chg,
            diff_pe_ce_oi,
            diff_pe_ce_oi_chg,
            trend_oi,
            trend_oi_chg,
            pcr_oi,
            round(float(row["avg_iv"]), 2) if pd.notna(row.get("avg_iv")) else 0,
            max_ce_oi_strike,
            max_ce_oi_chg_strike,
            max_pe_oi_strike,
            max_pe_oi_chg_strike,
        )
    except Exception as e:
        print(f"[ERROR] _get_stock_stats_cached({ticker},{selected_date}): {e}")
        return None


def get_stock_stats(ticker: str, selected_date: str, selected_expiry: str = None):
    """
    Comprehensive stats matching stock_detail_stats.html template.
    NOW FILTERS BY EXPIRY if provided.
    Returns all stats shown in the screenshot.
    Uses caching to avoid repeated queries.
    """
    try:
        derived_table = _derived_table_name(ticker)
        base_table = _base_table_name(ticker)
        table_to_use = derived_table if _table_exists(derived_table) else base_table

        # Get cached data
        expiry_key = selected_expiry or ""
        cached_data = _get_stock_stats_cached(ticker, selected_date, expiry_key, table_to_use)

        if cached_data is None:
            return {}

        # Format helper
        def format_crores(val):
            if val >= 1e7:
                return f"{val/1e7:.2f} Cr"
            elif val >= 1e5:
                return f"{val/1e5:.2f} L"
            return f"{val:.0f}"

        # Unpack cached data
        (
            total_ce_oi,
            total_pe_oi,
            total_ce_oi_chg,
            total_pe_oi_chg,
            diff_pe_ce_oi,
            diff_pe_ce_oi_chg,
            trend_oi,
            trend_oi_chg,
            pcr_oi,
            avg_iv,
            max_ce_oi_strike,
            max_ce_oi_chg_strike,
            max_pe_oi_strike,
            max_pe_oi_chg_strike,
        ) = cached_data

        result = {
            "total_ce_oi": format_crores(total_ce_oi),
            "total_pe_oi": format_crores(total_pe_oi),
            "total_ce_oi_chg": format_crores(total_ce_oi_chg),
            "total_pe_oi_chg": format_crores(total_pe_oi_chg),
            "diff_pe_ce_oi": format_crores(diff_pe_ce_oi),
            "diff_pe_ce_oi_chg": format_crores(diff_pe_ce_oi_chg),
            "trend_oi": trend_oi,
            "trend_oi_chg": trend_oi_chg,
            "pcr_oi": round(pcr_oi, 2) if pcr_oi > 0 else 0,
            "avg_iv": avg_iv,
            "max_ce_oi_strike": max_ce_oi_strike if max_ce_oi_strike is not None else "N/A",
            "max_ce_oi_chg_strike": max_ce_oi_chg_strike if max_ce_oi_chg_strike is not None else "N/A",
            "max_pe_oi_strike": max_pe_oi_strike if max_pe_oi_strike is not None else "N/A",
            "max_pe_oi_chg_strike": max_pe_oi_chg_strike if max_pe_oi_chg_strike is not None else "N/A",
        }

        return result

    except Exception as e:
        print(f"[ERROR] get_stock_stats({ticker},{selected_date}): {e}")
        import traceback

        traceback.print_exc()
        return {}


@cache.memoize(timeout=300)
def _get_stock_chart_data_cached(ticker: str, days: int, table_name: str):
    """Cached internal function for chart data with moneyness change."""
    try:
        # Get last N+1 dates for moneyness calculation (need prev day for each)
        dates_list = get_available_dates()
        if not dates_list:
            return tuple()

        # Limit to 'days+1' dates (extra for prev day calculation)
        relevant_dates = dates_list[: min(days + 1, len(dates_list))]

        # Fetch raw data for all relevant dates
        date_placeholders = ",".join([f"'{d}'" for d in relevant_dates])
        q = text(
            f"""
            SELECT
                "BizDt"::text AS date,
                "StrkPric",
                "OptnTp",
                "UndrlygPric",
                "OpnIntrst",
                "TtlTradgVol",
                "iv",
                "OpnPric",
                "HghPric",
                "LwPric",
                "ClsPric"
            FROM public."{table_name}"
            WHERE "BizDt" IN ({date_placeholders})
            ORDER BY "BizDt" ASC
        """
        )

        df_all = pd.read_sql(q, con=engine)

        if df_all.empty:
            return tuple()

        # Convert to numeric
        for col in ["StrkPric", "UndrlygPric", "OpnIntrst", "TtlTradgVol", "iv", "OpnPric", "HghPric", "LwPric", "ClsPric"]:
            if col in df_all.columns:
                df_all[col] = pd.to_numeric(df_all[col], errors="coerce")

        # Sort relevant_dates in ascending order
        relevant_dates_sorted = sorted(relevant_dates)

        # Calculate metrics for each date (skip first date as it has no previous)
        result = []
        for i in range(1, len(relevant_dates_sorted)):  # Start from index 1
            curr_date = relevant_dates_sorted[i]
            prev_date = relevant_dates_sorted[i - 1]

            # Current date data
            df_curr = df_all[df_all["date"] == curr_date].copy()
            if df_curr.empty:
                continue

            # Basic aggregations
            # Separate Options and Underlying (Cash/Futures)
            # Options have OptnTp='CE' or 'PE'
            # Underlying/Futures have OptnTp=None or other values
            df_options = df_curr[df_curr["OptnTp"].isin(["CE", "PE"])]
            df_others = df_curr[~df_curr["OptnTp"].isin(["CE", "PE"])]

            # Basic aggregations
            # For Close/OHLC: Prefer 'df_others' (Cash/Futures) which has real OHLC
            # If not available, fallback to 'UndrlygPric' from options (which is Spot Close)

            open_price = 0
            high_price = 0
            low_price = 0
            close_price = 0

            if not df_others.empty:
                # Use the first row from others (assuming it's the valid underlying/future)
                # Ideally, we should filter for EQ or 'XX' if multiple exist
                row = df_others.iloc[0]
                open_price = float(row["OpnPric"]) if pd.notna(row["OpnPric"]) else 0
                high_price = float(row["HghPric"]) if pd.notna(row["HghPric"]) else 0
                low_price = float(row["LwPric"]) if pd.notna(row["LwPric"]) else 0
                close_price = float(row["ClsPric"]) if pd.notna(row["ClsPric"]) else 0

            # If df_others is empty (common for F&O tables), query Cash market table for OHLC
            if (open_price == 0 or high_price == 0 or low_price == 0 or close_price == 0):
                try:
                    from .db_config import engine_cash

                    # Query cash market table for this ticker and date
                    cash_query = text(f'''
                        SELECT "OpnPric", "HghPric", "LwPric", "ClsPric"
                        FROM public."TBL_{ticker}"
                        WHERE "BizDt" = '{curr_date}'
                        LIMIT 1
                    ''')

                    with engine_cash.connect() as cash_conn:
                        cash_result = cash_conn.execute(cash_query)
                        cash_row = cash_result.fetchone()

                        if cash_row:
                            # Use cash market OHLC data
                            if open_price == 0 and cash_row[0]:
                                open_price = float(cash_row[0])
                            if high_price == 0 and cash_row[1]:
                                high_price = float(cash_row[1])
                            if low_price == 0 and cash_row[2]:
                                low_price = float(cash_row[2])
                            if close_price == 0 and cash_row[3]:
                                close_price = float(cash_row[3])
                except Exception:
                    # If cash table query fails, continue with fallback
                    pass

            # Final fallback: Use spot close from options if still no data
            if close_price == 0 and not df_options.empty:
                spot_close = df_options["UndrlygPric"].max()
                close_price = float(spot_close) if pd.notna(spot_close) else 0
                # Only set OHLC to close if we have absolutely no other data
                if open_price == 0: open_price = close_price
                if high_price == 0: high_price = close_price
                if low_price == 0: low_price = close_price

            # Metrics
            oi = df_options["OpnIntrst"].sum()
            volume = df_options["TtlTradgVol"].sum()

            # Use df_options for IV and PCR logic
            # IV calculation
            iv_values = df_options[df_options["iv"].notna() & (df_options["iv"] > 0)]["iv"]
            avg_iv = iv_values.mean() if not iv_values.empty else 0
            if avg_iv > 0:
                avg_iv *= 100

            # PCR calculation
            ce_oi = df_options[df_options["OptnTp"] == "CE"]["OpnIntrst"].sum()
            pe_oi = df_options[df_options["OptnTp"] == "PE"]["OpnIntrst"].sum()
            pcr = pe_oi / ce_oi if ce_oi > 0 else 0

            # Moneyness change calculation
            moneyness_change = 0
            df_prev_options = pd.DataFrame()
            df_prev = df_all[df_all["date"] == prev_date].copy()

            if not df_prev.empty:
                df_prev_options = df_prev[df_prev["OptnTp"].isin(["CE", "PE"])]

            if not df_options.empty and not df_prev_options.empty:
                # Merge curr and prev on Strike and OptionType
                dm = pd.merge(
                    df_options[["StrkPric", "OptnTp", "UndrlygPric"]],
                    df_prev_options[["StrkPric", "OptnTp", "UndrlygPric"]],
                    on=["StrkPric", "OptnTp"],
                    suffixes=("_c", "_p"),
                    how="inner",
                )

                if not dm.empty:
                    # Calculate moneyness for current and previous
                    dm["moneyness_curr"] = (dm["UndrlygPric_c"] - dm["StrkPric"]) / dm["UndrlygPric_c"]
                    dm["moneyness_prev"] = (dm["UndrlygPric_p"] - dm["StrkPric"]) / dm["UndrlygPric_p"]
                    dm["money_chg"] = dm["moneyness_curr"] - dm["moneyness_prev"]

                    # Sum total moneyness change
                    moneyness_change = dm["money_chg"].sum()

            result.append(
                (
                    curr_date,
                    float(open_price),
                    float(high_price),
                    float(low_price),
                    float(close_price),
                    int(oi) if pd.notna(oi) else 0,
                    int(volume) if pd.notna(volume) else 0,
                    round(float(avg_iv), 2) if pd.notna(avg_iv) else 0,
                    round(float(pcr), 2) if pd.notna(pcr) else 0,
                    round(float(moneyness_change), 4) if pd.notna(moneyness_change) else 0,
                )
            )

        # Limit to requested 'days' count (we calculated days+1 for prev day logic)
        return tuple(result[:days])

    except Exception as e:
        print(f"[ERROR] _get_stock_chart_data_cached({ticker},{days}): {e}")
        import traceback

        traceback.print_exc()
        return tuple()


def get_stock_chart_data(ticker: str, days: int = 40):
    """
    Fetch comprehensive historical data: Price + OI + Volume + IV + PCR + Moneyness Change
    Returns list of {date, open, high, low, close, oi, volume, iv, pcr, moneyness_change}
    Uses caching to avoid repeated expensive queries.
    """
    try:
        derived_table = _derived_table_name(ticker)
        base_table = _base_table_name(ticker)

        # Use derived table if available
        use_derived = _table_exists(derived_table)
        table_to_use = derived_table if use_derived else base_table

        if not _table_exists(table_to_use):
            print(f"[WARN] Table {table_to_use} does not exist for ticker {ticker}")
            return []

        # Get cached data
        cached_data = _get_stock_chart_data_cached(ticker, days, table_to_use)

        if not cached_data:
            return []

        # Convert tuple back to dict format
        result = []
        for row in cached_data:
            result.append(
                {
                    "date": row[0],
                    "open": row[1],
                    "high": row[2],
                    "low": row[3],
                    "close": row[4],
                    "oi": row[5],
                    "volume": row[6],
                    "iv": row[7],
                    "pcr": row[8],
                    "moneyness_change": row[9],
                }
            )

        return result

    except Exception as e:
        print(f"[ERROR] get_stock_chart_data({ticker},{days}): {e}")
        import traceback

        traceback.print_exc()
        return []


@cache.memoize(timeout=3600)  # Tickers don't change often, cache for 1 hour
def _get_all_tickers_cached():
    """Cached function to get all tickers."""
    try:
        inspector = _get_inspector()
        tables = [
            t for t in inspector.get_table_names(schema="public") if t.startswith("TBL_") and t.endswith("_DERIVED")
        ]
        tickers = sorted([t.replace("TBL_", "").replace("_DERIVED", "") for t in tables])
        return tuple(tickers)
    except Exception as e:
        print(f"[ERROR] get_all_tickers(): {e}")
        return tuple()


def get_all_tickers():
    """Get list of all available ticker symbols from database (cached)."""
    return list(_get_all_tickers_cached())


def get_filtered_tickers():
    """Get list of tickers filtered by stock list.xlsx (with caching)"""
    try:
        # Load allowed stocks from Excel (cached)
        allowed_stocks = get_stock_list_from_excel()

        if not allowed_stocks:
            print(f"[WARNING] No stocks in Excel filter. Showing all stocks.")
            return get_all_tickers()

        # Get all tickers from database
        all_tickers = get_all_tickers()

        # Filter tickers based on Excel list
        if allowed_stocks:
            filtered = [t for t in all_tickers if t.upper() in allowed_stocks]
            return sorted(filtered)

        return all_tickers

    except Exception as e:
        print(f"[ERROR] get_filtered_tickers(): {e}")
        return get_all_tickers()


def generate_oi_chart(ticker: str, selected_date: str, selected_expiry: str = None, data=None, expiry_data=None):
    """
    Generate OI chart data for TradingView
    Returns dictionary with strikes and OI data + futures expiry prices
    Accepts data and expiry_data as parameters to avoid redundant queries.
    """
    try:
        # Use provided data if available, otherwise fetch
        if data is None:
            data = get_stock_detail_data(ticker, selected_date, selected_expiry)
        if not data:
            print(f"[DEBUG] No data for chart: {ticker}, {selected_date}, {selected_expiry}")
            return None

        # Organize by strike
        strikes_dict = {}
        underlying = None

        for row in data:
            strike = row.get("StrkPric")
            if not strike or pd.isna(strike) or strike <= 0:
                continue

            if strike not in strikes_dict:
                strikes_dict[strike] = {"ce_oi": 0, "pe_oi": 0, "ce_oi_chg": 0, "pe_oi_chg": 0}

            if row.get("OptnTp") == "CE":
                strikes_dict[strike]["ce_oi"] = row.get("OpnIntrst", 0) or 0
                strikes_dict[strike]["ce_oi_chg"] = row.get("ChngInOpnIntrst", 0) or 0
            elif row.get("OptnTp") == "PE":
                strikes_dict[strike]["pe_oi"] = row.get("OpnIntrst", 0) or 0
                strikes_dict[strike]["pe_oi_chg"] = row.get("ChngInOpnIntrst", 0) or 0

            if not underlying and row.get("UndrlygPric"):
                underlying = row.get("UndrlygPric")

        if not strikes_dict:
            print(f"[DEBUG] No strikes_dict built")
            return None

        #  Get futures expiry data (use provided or fetch)
        if expiry_data is None:
            expiry_data = get_stock_expiry_data(ticker, selected_date)
        futures_prices = []
        for exp in expiry_data[:3]:  # Take first 3 expiries
            futures_prices.append({"expiry": exp["expiry"], "price": float(exp["price"]) if exp["price"] else None})

        # Prepare data - Filter out NaN strikes
        strikes = sorted([s for s in strikes_dict.keys() if pd.notna(s) and s > 0])
        ce_oi = [strikes_dict[s]["ce_oi"] for s in strikes]
        pe_oi = [strikes_dict[s]["pe_oi"] for s in strikes]
        ce_oi_chg = [strikes_dict[s]["ce_oi_chg"] for s in strikes]
        pe_oi_chg = [strikes_dict[s]["pe_oi_chg"] for s in strikes]

        print(f"[DEBUG] Chart data: {len(strikes)} strikes, underlying={underlying}, futures={len(futures_prices)}")

        # Return JSON-serializable dict for TradingView
        return {
            "strikes": [int(s) for s in strikes],
            "ce_oi": ce_oi,
            "pe_oi": pe_oi,
            "ce_oi_chg": ce_oi_chg,
            "pe_oi_chg": pe_oi_chg,
            "underlying_price": float(underlying) if underlying else None,
            "futures_prices": futures_prices,  #  Add futures data
            "meta": {"ticker": ticker, "expiry": selected_expiry, "date": selected_date},
        }

    except Exception as e:
        print(f"[ERROR] generate_oi_chart: {e}")
        import traceback

        traceback.print_exc()
        return None
