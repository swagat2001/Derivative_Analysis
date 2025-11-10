# =============================================================
#  SHARED DATABASE CONFIGURATION MODULE
#  Purpose: Centralized database engine with proper connection pooling
#  Prevents connection exhaustion and improves performance
# =============================================================

from sqlalchemy import create_engine
from urllib.parse import quote_plus
from functools import lru_cache
import pandas as pd
import os

# =============================================================
# DATABASE CONNECTION CONFIGURATION
# =============================================================

db_user = 'postgres'
db_password = 'Gallop@3104'
db_host = 'localhost'
db_port = '5432'
db_name = 'BhavCopy_Database'

db_password_enc = quote_plus(db_password)

# Create engine with proper connection pooling configuration
# This prevents connection exhaustion and improves performance
engine = create_engine(
    f'postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}',
    pool_size=10,              # Number of connections to maintain in pool
    max_overflow=20,           # Maximum overflow connections (total = pool_size + max_overflow)
    pool_pre_ping=True,        # Verify connections before using them
    pool_recycle=3600,         # Recycle connections after 1 hour (prevents stale connections)
    echo=False,                # Set to True for SQL query logging (debug only)
    connect_args={
        'connect_timeout': 10,  # Connection timeout in seconds
        'application_name': 'Derivatives_Analysis'
    }
)

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
    """
    global _excel_cache, _excel_cache_time
    import time
    
    excel_path = r"C:\Users\Admin\Desktop\Derivative_Analysis\stock list.xlsx"
    current_time = time.time()
    
    # Check if cache is valid
    if (_excel_cache_time is None or 
        current_time - _excel_cache_time > EXCEL_CACHE_TTL):
        try:
            if not os.path.exists(excel_path):
                print(f"[WARNING] Excel file not found at {excel_path}. Using cached data if available.")
                return _excel_cache.copy() if _excel_cache else []
            
            stock_df = pd.read_excel(excel_path)
            # Try column 'A' first, then first column
            if 'A' in stock_df.columns:
                _excel_cache = [str(s).strip().upper() for s in stock_df['A'].dropna().tolist()]
            elif stock_df.shape[1] > 0:
                _excel_cache = [str(s).strip().upper() for s in stock_df.iloc[:, 0].dropna().tolist()]
            else:
                _excel_cache = []
            _excel_cache_time = current_time
            print(f"[INFO] Loaded {len(_excel_cache)} stocks from Excel filter (cached)")
        except Exception as e:
            print(f"[WARNING] Could not load stock list Excel: {e}. Using cached data if available.")
            if not _excel_cache:
                return []
    
    return _excel_cache.copy()  # Return copy to prevent external modification

def clear_excel_cache():
    """Clear Excel cache - useful for testing or when Excel file is updated."""
    global _excel_cache, _excel_cache_time
    _excel_cache = []
    _excel_cache_time = None
    print("[INFO] Excel cache cleared")

