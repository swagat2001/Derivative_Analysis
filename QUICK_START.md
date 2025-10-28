# 🚀 QUICK START GUIDE

## ⚡ 5-Minute Setup

### Step 1: Install Dependencies (One-time)
```bash
pip install pandas numpy sqlalchemy psycopg2 flask py_vollib openpyxl
```

### Step 2: Setup PostgreSQL Database (One-time)
```bash
# Create database
psql -U postgres -c "CREATE DATABASE BhavCopy_Database;"

# Run schema
psql -U postgres -d BhavCopy_Database -f BhavCopy_Database.sql
```

### Step 3: Download & Process Data (Run daily)
```bash
cd "C:\Users\Admin\Desktop\BhavCopy Backup2\Database"
python update_database.py
```
**This does everything:**
- Downloads NSE CSV
- Uploads to database
- Calculates Greeks
- Pre-calculates dashboard data

### Step 4: View Dashboard
```bash
cd "C:\Users\Admin\Desktop\BhavCopy Backup2\Analysis_Tools"
python dashboard_server.py
```
**Opens:** http://localhost:5000

---

## 📊 Using the Dashboard

### Tabs
- **TOTAL** - All strikes
- **OTM** - Out of The Money
- **ITM** - In The Money

### Interactive Features
1. **Click on Vega % cells** → Shows 40-day chart with PCR + Average Vega
2. **Click on ΔMoney cells** → Shows 40-day chart with PCR + Moneyness Change
3. **Search** - Filter by ticker name
4. **Export** - Download to Excel

---

## 🔍 Quick Troubleshooting

### Problem: "No dates available"
**Solution:**
```bash
cd Database
python update_database.py
```

### Problem: "Cache table empty"
**Solution:**
```bash
cd Analysis_Tools
python precalculate_data.py
```

### Problem: Dashboard shows old data
**Solution:**
```bash
# Run daily update
cd Database
python update_database.py
```

---

## 📝 What Each Metric Means

| Metric | Meaning | Good/Bad |
|--------|---------|----------|
| **Delta Change** | How option price sensitivity changed | Positive = More sensitive |
| **Vega Change** | How volatility sensitivity changed | Positive = More volatile |
| **ΔTradingVal** | Change in trading value | Positive = Money flowing in |
| **ΔMoney** | Change in moneyness | Positive = Moving ITM |
| **PCR (Volume)** | Put-Call Ratio by volume | >1 = Bearish, <1 = Bullish |
| **PCR (OI)** | Put-Call Ratio by OI | >1 = Bearish, <1 = Bullish |

---

## 🎯 Daily Workflow

```bash
# Morning: Update data (5 minutes)
cd "C:\Users\Admin\Desktop\BhavCopy Backup2\Database"
python update_database.py

# Anytime: View dashboard
cd "../Analysis_Tools"
python dashboard_server.py
```

---

## 📂 File Locations

| What | Where |
|------|-------|
| Database Update | `Database/update_database.py` |
| Dashboard Server | `Analysis_Tools/dashboard_server.py` |
| Pre-calculator | `Analysis_Tools/precalculate_data.py` |
| Reports | `Reports/` folder |
| Raw CSV Data | `C:/NSE_EOD_FO/` |

---

## ⚙️ Configuration (if needed)

### Database Connection
**File:** All Python files
**Lines:** Top of each file
```python
db_user = 'postgres'
db_password = 'Gallop@3104'
db_host = 'localhost'
db_port = '5432'
db_name = 'BhavCopy_Database'
```

### CSV Storage Path
**File:** `Database/update_database.py`
**Line:** ~20
```python
save_fo_eod = "C:/NSE_EOD_FO"
```

---

## 🔧 Advanced Options

### Re-process Old Dates
```bash
cd Analysis_Tools
python precalculate_data.py
# Skips existing dates automatically
```

### Generate Standalone HTML
```bash
cd Analysis_Tools
python clean_dashboard_generator.py
# Creates: Reports/Options_Clean_Dashboard_YYYY-MM-DD.html
```

### Check Database Contents
```bash
psql -U postgres -d BhavCopy_Database

# See all tables
\dt

# Count records
SELECT COUNT(*) FROM TBL_NIFTY_DERIVED;

# Check cache
SELECT COUNT(*) FROM options_dashboard_cache;

# Exit
\q
```

---

## 📖 Need More Details?

- **Complete Documentation:** `COMPLETE_README.md`
- **Data Mapping:** `DATA_MAPPING.md`
- **Analysis Tools Guide:** `Analysis_Tools/README.md`
- **Database Guide:** `Database/README.md`

---

## 💡 Tips

1. **Run `update_database.py` daily** after market close
2. **Charts take 2-3 seconds to load** (fetching 40 days of data)
3. **Search is case-insensitive** (type "nifty" or "NIFTY")
4. **Use TOTAL tab** for complete market view
5. **Click cells** to see historical trends
6. **Export before closing** if you need data in Excel

---

## 🆘 Get Help

**Find where data comes from:**
```bash
grep -rn "closing_price" Analysis_Tools/
grep -rn "pcr_volume" Analysis_Tools/
grep -rn "delta_chg" Analysis_Tools/
```

**See full data flow:**
Check `DATA_MAPPING.md`

---

**You're ready! Run the commands above and start analyzing! 🎉**
