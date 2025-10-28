# Database Pipeline

This folder contains all scripts for downloading, processing, and storing NSE FO BhavCopy data.

## 📁 Files

### 🎯 Main Script (Use This!)
- **`RUN_DATA_PIPELINE.bat`** ⭐
  - Master batch file that runs all 3 steps automatically
  - Just double-click to update your database with latest data
  - No manual input required!

### 📥 Step 1: Download CSV Data
- **`BhavCopy_New_Nse_fo_roman_suffixes.py`**
  - Downloads NSE FO BhavCopy files from NSE archives
  - Saves to: `C:\NSE_EOD_FO`
  - Auto-detects new dates and downloads only missing files

### 📤 Step 2: Upload to Database
- **`BhavCopy_Data_Fetch_All_CSV_Auto.py`** (Automated version)
  - Automatically reads from `C:\NSE_EOD_FO`
  - Creates tables for each ticker symbol (TBL_NIFTY, TBL_BANKNIFTY, etc.)
  - Uploads all CSV data to PostgreSQL

- **`BhavCopy_Data_Fetch_All_CSV.py`** (Manual version)
  - Same functionality but asks for folder path
  - Use this if you want to upload from a different location

### 🧮 Step 3: Calculate Greeks
- **`new_bhav_future_processor.py`**
  - Auto-detects dates that need Greeks calculation
  - Calculates: Delta, Vega, Theta, IV
  - Creates DERIVED tables (TBL_NIFTY_DERIVED, etc.)

---

## 🚀 Quick Start

### Daily Workflow (Automated):
```bash
1. Double-click: RUN_DATA_PIPELINE.bat
2. Wait for completion
3. Done! Database is updated.
```

### What the Pipeline Does:
1. ✅ Downloads latest CSV from NSE (skips if already downloaded)
2. ✅ Uploads new data to database tables
3. ✅ Calculates Greeks for new dates only

---

## 📊 Database Structure

### Base Tables (Raw Data)
- Format: `TBL_[SYMBOL]`
- Examples: `TBL_NIFTY`, `TBL_BANKNIFTY`, `TBL_RELIANCE`
- Contains: 22 columns of raw NSE data

### Derived Tables (With Greeks)
- Format: `TBL_[SYMBOL]_DERIVED`
- Examples: `TBL_NIFTY_DERIVED`, `TBL_BANKNIFTY_DERIVED`
- Contains: 22 base columns + 8 calculated columns
- Calculated: strike_diff, y_oi, chg_oi, chg_price, delta, vega, theta, iv

---

## ⚙️ Configuration

### Database Settings (in all scripts):
```python
db_user = 'postgres'
db_password = 'Gallop@3104'
db_host = 'localhost'
db_port = '5432'
db_name = 'BhavCopy_Database'
```

### CSV Storage Location:
```
C:\NSE_EOD_FO
```

---

## 🔧 Manual Execution (If needed)

If you want to run steps individually:

```bash
# Step 1: Download CSV
python BhavCopy_New_Nse_fo_roman_suffixes.py

# Step 2: Upload to database
python BhavCopy_Data_Fetch_All_CSV_Auto.py

# Step 3: Calculate Greeks
python new_bhav_future_processor.py
```

---

## 🆘 Troubleshooting

### Problem: "No new dates to download"
**Solution:** Database is already up to date. This is normal.

### Problem: "No CSV files found"
**Solution:** Step 1 (download) didn't complete. Check internet connection.

### Problem: "Database connection failed"
**Solution:** 
- Check if PostgreSQL is running
- Verify database credentials
- Ensure database 'BhavCopy_Database' exists

### Problem: "No dates to process for Greeks"
**Solution:** All dates already have Greeks calculated. This is normal.

---

## 📝 Notes

- The pipeline is **incremental** - it only processes new data
- Safe to run multiple times - won't duplicate data
- CSV download automatically skips holidays and weekends
- Greeks calculation only processes missing dates

---

## 📅 Recommended Schedule

Run `RUN_DATA_PIPELINE.bat` daily after market close (after 3:30 PM IST)

---

Created: October 2025
Last Updated: October 2025
