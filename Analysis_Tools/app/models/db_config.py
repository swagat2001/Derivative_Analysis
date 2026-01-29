# =============================================================
#  SHARED DATABASE CONFIGURATION MODULE
#  Purpose: Centralized database engine with proper connection pooling
#  Prevents connection exhaustion and improves performance
# =============================================================

import os
from functools import lru_cache
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine

# =============================================================
# DATABASE CONNECTION CONFIGURATION
# =============================================================

db_user = "postgres"
db_password = "Gallop@3104"
db_host = "localhost"
db_port = "5432"
db_name = "BhavCopy_Database"
db_name_cash = "CashStocks_Database"  # Cash/Equity database

db_password_enc = quote_plus(db_password)

# Create engine for F&O database (BhavCopy_Database)
engine = create_engine(
    f"postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}",
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    pool_recycle=3600,
    echo=False,
    connect_args={"connect_timeout": 10, "application_name": "Derivatives_Analysis"},
)

# Create engine for Cash/Equity database (CashStocks_Database)
engine_cash = create_engine(
    f"postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name_cash}",
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    pool_recycle=3600,
    echo=False,
    connect_args={"connect_timeout": 10, "application_name": "Cash_Analysis"},
)

# =============================================================
# DATABASE TABLE LIST HELPER
# =============================================================

_table_list_cache = []
_table_list_cache_time = None
TABLE_LIST_CACHE_TTL = 300  # Cache for 5 minutes


def get_table_list():
    """
    Get list of all tables in the F&O database.
    Results are cached for 5 minutes to avoid repeated queries.
    """
    global _table_list_cache, _table_list_cache_time
    import time

    from sqlalchemy import text

    current_time = time.time()

    # Check if cache is valid
    if _table_list_cache_time is None or current_time - _table_list_cache_time > TABLE_LIST_CACHE_TTL:
        try:
            with engine.connect() as conn:
                result = conn.execute(
                    text(
                        """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    ORDER BY table_name
                """
                    )
                )
                _table_list_cache = [row[0] for row in result.fetchall()]
                _table_list_cache_time = current_time
                print(f"[INFO] Loaded {len(_table_list_cache)} tables from database")
        except Exception as e:
            print(f"[WARNING] Could not load table list: {e}")
            if not _table_list_cache:
                return []

    return _table_list_cache.copy()


# =============================================================
# CACHED EXCEL FILE READER
# =============================================================

_excel_cache = []
_excel_cache_time = None
EXCEL_CACHE_TTL = 300  # Cache for 5 minutes


def get_stock_list_from_excel():
    """
    Load stock list from Excel file with caching.
    Cache expires after 5 minutes to allow for file updates.

    Reads the FIRST column of the Excel file (regardless of header name).
    Converts all stock names to UPPERCASE and strips whitespace.
    """
    global _excel_cache, _excel_cache_time
    import time

    excel_path = r"C:\Users\Admin\Desktop\Derivative_Analysis\stock list.xlsx"
    current_time = time.time()

    # Check if cache is valid
    if _excel_cache_time is None or current_time - _excel_cache_time > EXCEL_CACHE_TTL:
        try:
            if not os.path.exists(excel_path):
                print(f"[WARNING] Excel file not found at {excel_path}. Using cached data if available.")
                return _excel_cache.copy() if _excel_cache else []

            # Read Excel - always use first column (index 0)
            stock_df = pd.read_excel(excel_path, header=None)  # No header assumption

            if stock_df.shape[1] > 0:
                # Get first column, skip first row if it looks like a header
                first_col = stock_df.iloc[:, 0].dropna().tolist()

                # Check if first row is a header (non-numeric, common header names)
                first_val = str(first_col[0]).strip().upper() if first_col else ""
                header_keywords = ["STOCK", "SYMBOL", "TICKER", "NAME", "SCRIP", "A"]
                if first_val in header_keywords or (not first_val[0].isalnum() if first_val else True):
                    first_col = first_col[1:]  # Skip header row

                # Normalize: strip whitespace, convert to uppercase
                _excel_cache = [str(s).strip().upper() for s in first_col if str(s).strip()]

                # Debug: Print first 5 stocks loaded
                print(f"[INFO] Loaded {len(_excel_cache)} stocks from Excel. First 5: {_excel_cache[:5]}")
            else:
                _excel_cache = []
                print("[WARNING] Excel file has no columns")

            _excel_cache_time = current_time

        except Exception as e:
            print(f"[WARNING] Could not load stock list Excel: {e}. Using cached data if available.")
            if not _excel_cache:
                return []

    return _excel_cache.copy()


def clear_excel_cache():
    """Clear Excel cache - useful for testing or when Excel file is updated."""
    global _excel_cache, _excel_cache_time
    _excel_cache = []
    _excel_cache_time = None
    print("[INFO] Excel cache cleared")
