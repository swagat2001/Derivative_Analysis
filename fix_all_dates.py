"""
Fix ALL date columns (BizDt, FininstrmActlXpryDt) in ALL tables to DATE type
"""

from sqlalchemy import create_engine, text, inspect
from urllib.parse import quote_plus

db_password_enc = quote_plus('Gallop@3104')
engine = create_engine(f'postgresql+psycopg2://postgres:{db_password_enc}@localhost:5432/BhavCopy_Database')

print("="*80)
print("FIXING ALL DATE COLUMNS IN DATABASE")
print("="*80)

inspector = inspect(engine)
all_tables = inspector.get_table_names(schema='public')

# Filter to TBL_ tables only
tbl_tables = [t for t in all_tables if t.startswith('TBL_')]

print(f"\nFound {len(tbl_tables)} tables to fix")

fixed_count = 0
error_count = 0

for table in tbl_tables:
    print(f"\n{table}:")
    
    # Get columns for this table
    columns = inspector.get_columns(table, schema='public')
    date_columns = []
    
    for col in columns:
        col_name = col['name']
        if 'BizDt' in col_name or 'XpryDt' in col_name or col_name == 'FininstrmActlXpryDt':
            date_columns.append(col_name)
    
    if not date_columns:
        print("  ⏭️  No date columns found")
        continue
    
    # Fix each date column
    for col_name in date_columns:
        try:
            with engine.begin() as conn:
                # Check current type
                type_query = text(f"""
                    SELECT data_type 
                    FROM information_schema.columns 
                    WHERE table_name = :table 
                    AND column_name = :column
                """)
                result = conn.execute(type_query, {'table': table, 'column': col_name})
                current_type = result.fetchone()[0] if result else 'unknown'
                
                if current_type == 'date':
                    print(f"  ✅ {col_name} already DATE")
                    continue
                
                # Convert to DATE
                fix_query = text(f'''
                    ALTER TABLE "{table}" 
                    ALTER COLUMN "{col_name}" TYPE DATE 
                    USING "{col_name}"::DATE
                ''')
                conn.execute(fix_query)
                print(f"  ✅ {col_name}: {current_type} → DATE")
                fixed_count += 1
                
        except Exception as e:
            print(f"  ❌ {col_name}: {e}")
            error_count += 1

print("\n" + "="*80)
print(f"SUMMARY")
print("="*80)
print(f"✅ Fixed: {fixed_count} columns")
print(f"❌ Errors: {error_count} columns")
print("\nNow run:")
print("  1. python clear_futures_cache.py")
print("  2. python screener_cache_new_CLEAN.py")
print("  3. python run.py")
print("="*80)