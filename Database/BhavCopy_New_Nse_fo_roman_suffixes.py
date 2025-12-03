# -*- coding: utf-8 -*-
"""
NSE FO Bhavcopy Downloader
- Keeps all columns from original CSV
- Outputs files in CSV with date format YYYY-MM-DD
- Normalizes all date columns
"""

import urllib.request
import os
import socket
import zipfile
from datetime import datetime, timedelta
import shutil
import sys
import pandas as pd

def download_file(url, output_folder):
    filename = os.path.basename(url)
    output_path = os.path.join(output_folder, filename)
    urllib.request.urlretrieve(url, output_path)
    print("Downloaded:", filename)
    return output_path

def extract_files(zip_file, output_folder):
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(output_folder)
    print("Extracted:", zip_file)

# ------------------- Setup Folders -------------------

output_folder = "C:/data_fo"
save_fo_eod = "C:/NSE_EOD_FO"

# Recreate temp output folder
if os.path.exists(output_folder):
    shutil.rmtree(output_folder)
os.makedirs(output_folder)

# Create final destination folder if not exists
if not os.path.exists(save_fo_eod):
    os.makedirs(save_fo_eod)

# ------------------- Date Range Logic -------------------

if os.path.isdir(save_fo_eod) and os.listdir(save_fo_eod):
    file_list = [f for f in os.listdir(save_fo_eod) if os.path.isfile(os.path.join(save_fo_eod, f))]
    last_date_str = max(f[:10] for f in file_list)
    last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
    today = datetime.now().date()

    if last_date == today:
        print("\nThe Bhavcopy_NSE_FO database is up to date:", last_date)
        sys.exit()

    start_date = last_date + timedelta(days=1)
    end_date = today

else:
    start_date = datetime.strptime(input('Enter start date (YYYY-MM-DD): '), "%Y-%m-%d").date()
    end_date = datetime.strptime(input('Enter end date (YYYY-MM-DD): '), "%Y-%m-%d").date()

# ------------------- Generate Date List -------------------

date_range = []
delta = end_date - start_date
for i in range(delta.days + 1):
    day = start_date + timedelta(days=i)
    if day.weekday() < 5:  # Weekdays only
        date_range.append(day)

# ------------------- Download & Extract -------------------

socket.setdefaulttimeout(1)
downloaded_files = []

for date in date_range:
    date_str = date.strftime("%Y%m%d")
    filename = f"BhavCopy_NSE_FO_0_0_0_{date_str}_F_0000.csv.zip"
    url = f"https://archives.nseindia.com/content/fo/{filename}"

    try:
        downloaded_file = download_file(url, output_folder)
        downloaded_files.append(downloaded_file)
    except Exception as e:
        print("Error downloading (probably a holiday):", filename)
        print(e)

for file in downloaded_files:
    try:
        extract_files(file, output_folder)
    except Exception as e:
        print("Error extracting:", file)
        print(e)

# Delete ZIP files after extraction
for file in downloaded_files:
    os.remove(file)
    print("Removed ZIP:", file)

# ------------------- Rename Extracted Files -------------------

files = os.listdir(output_folder)
renamed_files = []

for file in files:
    if file.endswith("0000.csv"):
        date = file[22:30]
        date_obj = datetime.strptime(date, "%Y%m%d")
        new_name = date_obj.strftime("%Y-%m-%d") + "-NSE-FO.csv"
        os.rename(os.path.join(output_folder, file), os.path.join(output_folder, new_name))
        renamed_files.append(new_name)
    else:
        print(f"Skipped: {file}")

# ------------------- Normalize Date Columns -------------------

for filename in os.listdir(output_folder):
    if filename.endswith("-NSE-FO.csv"):
        file_path = os.path.join(output_folder, filename)
        
        try:
            df = pd.read_csv(file_path)

            # Convert all date-like object columns to YYYY-MM-DD
            for col in df.columns:
                if df[col].dtype == 'object':
                    try:
                        df[col] = pd.to_datetime(df[col], format='mixed', errors='raise')
                        df[col] = df[col].dt.strftime('%Y-%m-%d')
                    except Exception:
                        continue  # Not a date column

            # Save back with headers
            df.to_csv(file_path, index=False)
            print(f"Formatted date columns in: {filename}")
        except Exception as e:
            print(f"Error processing {filename}: {e}")

# ------------------- Move CSV Files to Final Folder -------------------

for filename in os.listdir(output_folder):
    if filename.endswith("-NSE-FO.csv"):
        shutil.copy2(os.path.join(output_folder, filename), os.path.join(save_fo_eod, filename))
        print(f"Copied: {filename} -> {save_fo_eod}")

# ------------------- Cleanup -------------------

shutil.rmtree(output_folder)

print("\n✅ All NSE FO bhavcopy files saved to:", save_fo_eod)

