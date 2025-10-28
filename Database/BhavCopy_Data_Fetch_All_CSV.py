import os
import glob
import re
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# === CONFIG: PostgreSQL Connection ===
db_user = 'postgres'
db_password = 'Gallop@3104'  # ⚠️ Consider using environment variables instead
db_host = 'localhost'
db_port = '5432'
db_name = 'BhavCopy_Database'

# Encode password for URL
db_password_enc = quote_plus(db_password)

# Create SQLAlchemy engine
engine = create_engine(
    f'postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}'
)

# === Define Table Schema Template ===
table_schema_template = """
CREATE TABLE IF NOT EXISTS "{table_name}" (
    "BizDt" DATE,
    "Sgmt" VARCHAR(50),
    "FinInstrmTp" VARCHAR(50),
    "TckrSymb" VARCHAR(50),
    "FininstrmActlXpryDt" DATE,
    "StrkPric" VARCHAR(50),
    "OptnTp" VARCHAR(50),
    "FinInstrmNm" VARCHAR(50),
    "OpnPric" VARCHAR(50),
    "HghPric" VARCHAR(50),
    "LwPric" VARCHAR(50),
    "ClsPric" VARCHAR(50),
    "LastPric" VARCHAR(50),
    "PrvsClsgPric" VARCHAR(50),
    "UndrlygPric" VARCHAR(50),
    "SttlmPric" VARCHAR(50),
    "OpnIntrst" VARCHAR(50),
    "ChngInOpnIntrst" VARCHAR(50),
    "TtlTradgVol" VARCHAR(50),
    "TtlTrfVal" VARCHAR(50),
    "TtlNbOfTxsExctd" VARCHAR(50),
    "NewBrdLotQty" VARCHAR(50)
);
"""

# Columns to keep
expected_columns = [
    'BizDt','Sgmt','FinInstrmTp','TckrSymb','FininstrmActlXpryDt','StrkPric','OptnTp',
    'FinInstrmNm','OpnPric','HghPric','LwPric','ClsPric','LastPric','PrvsClsgPric',
    'UndrlygPric','SttlmPric','OpnIntrst','ChngInOpnIntrst','TtlTradgVol','TtlTrfVal','TtlNbOfTxsExctd','NewBrdLotQty'
]

# === Helper function to sanitize table names ===
def sanitize_table_name(name):
    clean = re.sub(r'\W+', '_', name).strip('_').upper()
    return f"TBL_{clean}" if clean else "TBL_UNKNOWN"

# === Step 1: Get folder path from user ===
folder_path = input("Enter the folder path containing CSV files: ").strip()

# === Step 2: Find all CSV files in the folder ===
csv_files = glob.glob(os.path.join(folder_path, '*.csv'))

if not csv_files:
    raise ValueError("❌ No CSV files found in the provided folder.")

print(f"\n📂 Found {len(csv_files)} CSV file(s) to process.\n")

# === Step 3: Process each CSV file ===
for file_path in csv_files:
    filename = os.path.basename(file_path)
    print(f"\n📄 Processing file: {filename}")

    try:
        # === Load CSV file ===
        df = pd.read_csv(file_path)

        # === Ensure required column exists ===
        if "TckrSymb" not in df.columns:
            print(f"⚠️ Skipping file (missing 'TckrSymb'): {filename}")
            continue

        # === Convert dates and clean NaNs ===
        df['TradDt'] = pd.to_datetime(df['TradDt'], errors='coerce').dt.date
        df['XpryDt'] = pd.to_datetime(df['XpryDt'], errors='coerce').dt.date
        df = df.where(pd.notnull(df), None)  # Replace NaN/NaT with None

        # === Get unique symbols in this file ===
        unique_symbols = df["TckrSymb"].dropna().unique()

        # === Create tables ===
        with engine.begin() as conn:
            for symbol in unique_symbols:
                table_name = sanitize_table_name(symbol)
                sql = table_schema_template.format(table_name=table_name)
                try:
                    conn.execute(text(sql))
                    print(f"🟢 Table ready: {table_name}")
                except Exception as e:
                    print(f"❌ Error creating table {table_name}: {e}")

        # === Insert rows in batches ===
        batch_size = 100
        for symbol in unique_symbols:
            table_name = sanitize_table_name(symbol)
            df_symbol = df[df["TckrSymb"] == symbol]

            # Filter to expected columns
            df_symbol = df_symbol[[col for col in expected_columns if col in df_symbol.columns]]

            rows_to_insert = df_symbol.to_dict(orient='records')
            if not rows_to_insert:
                continue

            columns = df_symbol.columns.tolist()
            cols_str = ', '.join(f'"{col}"' for col in columns)
            values_str = ', '.join(f":{col}" for col in columns)
            insert_sql = f'INSERT INTO "{table_name}" ({cols_str}) VALUES ({values_str})'

            total_batches = (len(rows_to_insert) + batch_size - 1) // batch_size
            for i in range(total_batches):
                batch = rows_to_insert[i * batch_size:(i + 1) * batch_size]
                try:
                    with engine.begin() as conn:
                        conn.execute(text(insert_sql), batch)
                    print(f"   ✅ Inserted batch {i+1}/{total_batches} into {table_name}")
                except Exception as e:
                    print(f"   ❌ Error inserting batch {i+1} into {table_name}: {e}")

    except Exception as e:
        print(f"❌ Error processing file {filename}: {e}")

print("\n🎉 All CSV files processed.")
