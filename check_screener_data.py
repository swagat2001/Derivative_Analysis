"""
Diagnostic script to check screener_cache data counts
Run this to verify if database has all 10 entries per category
"""

from sqlalchemy import create_engine, text
import pandas as pd
from urllib.parse import quote_plus

# Database config (same as your project)
db_user = 'postgres'
db_password = 'Gallop@3104'
db_host = 'localhost'
db_port = '5432'
db_name = 'BhavCopy_Database'
db_password_enc = quote_plus(db_password)
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}')


def check_data_counts(date_str=None):
    """Check row counts per category for a given date"""
    
    # Get latest date if not specified
    if not date_str:
        query = text("SELECT MAX(cache_date) as latest FROM public.screener_cache")
        result = pd.read_sql(query, engine)
        date_str = str(result['latest'].iloc[0])
        print(f"\n📅 Using latest date: {date_str}")
    
    print(f"\n{'='*80}")
    print(f"SCREENER CACHE DATA COUNTS FOR {date_str}")
    print(f"{'='*80}\n")
    
    # Query to get counts per category
    query = text("""
    SELECT 
        metric_type,
        option_type,
        moneyness_filter,
        COUNT(*) as row_count,
        MAX(rank) as max_rank
    FROM public.screener_cache
    WHERE cache_date = :cache_date
    GROUP BY metric_type, option_type, moneyness_filter
    ORDER BY metric_type, option_type, moneyness_filter
    """)
    
    df = pd.read_sql(query, engine, params={"cache_date": date_str})
    
    if df.empty:
        print(f"❌ No data found for date {date_str}")
        return
    
    # Display results
    print(f"{'Metric':<12} {'Type':<6} {'Filter':<15} {'Rows':<6} {'Max Rank':<10} {'Status'}")
    print("-" * 70)
    
    incomplete_categories = []
    
    for _, row in df.iterrows():
        metric = row['metric_type']
        opt_type = row['option_type']
        filter_type = row['moneyness_filter']
        count = row['row_count']
        max_rank = row['max_rank']
        
        status = "✅ Complete" if count >= 10 else f"⚠️ INCOMPLETE ({count}/10)"
        
        print(f"{metric:<12} {opt_type:<6} {filter_type:<15} {count:<6} {max_rank:<10} {status}")
        
        if count < 10:
            incomplete_categories.append(f"{metric}/{opt_type}/{filter_type}")
    
    print("\n" + "="*70)
    print(f"\n📊 SUMMARY:")
    print(f"   Total categories found: {len(df)}")
    print(f"   Complete (10 rows): {len(df[df['row_count'] >= 10])}")
    print(f"   Incomplete (< 10 rows): {len(df[df['row_count'] < 10])}")
    
    if incomplete_categories:
        print(f"\n⚠️  INCOMPLETE CATEGORIES:")
        for cat in incomplete_categories:
            print(f"   - {cat}")
    else:
        print(f"\n✅ All categories have 10 entries!")
    
    # Also show raw sample data for one category
    print(f"\n{'='*80}")
    print("SAMPLE: First category raw data")
    print("="*80)
    
    sample_query = text("""
    SELECT ticker, strike_price, underlying_price, change, rank
    FROM public.screener_cache
    WHERE cache_date = :cache_date 
        AND metric_type = 'oi' 
        AND option_type = 'CE' 
        AND moneyness_filter = 'ALL'
    ORDER BY rank ASC
    LIMIT 15
    """)
    
    sample_df = pd.read_sql(sample_query, engine, params={"cache_date": date_str})
    print(sample_df.to_string(index=False))


def check_all_available_dates():
    """Show all available dates in cache"""
    query = text("""
    SELECT cache_date, COUNT(*) as total_rows
    FROM public.screener_cache
    GROUP BY cache_date
    ORDER BY cache_date DESC
    LIMIT 10
    """)
    
    df = pd.read_sql(query, engine)
    
    print("\n📅 AVAILABLE DATES IN CACHE:")
    print("-" * 40)
    for _, row in df.iterrows():
        print(f"   {row['cache_date']} - {row['total_rows']} rows")


if __name__ == "__main__":
    check_all_available_dates()
    check_data_counts()  # Uses latest date
    
    # To check a specific date, uncomment:
    # check_data_counts("2025-11-25")