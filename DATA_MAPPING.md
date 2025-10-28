# DATA MAPPING - Quick Reference Guide

## Where Each Data Point Comes From

### 1. CLOSING PRICE (displayed in table)
- **Source:** `UndrlygPric` field
- **File:** `Analysis_Tools/precalculate_data.py`
- **Line:** 67
- **Code:** `closing_price = dfc['UndrlygPric'].iloc[0]`
- **Find:** `grep -n "closing_price =" Analysis_Tools/precalculate_data.py`

---

### 2. DELTA CHANGE (Call Δ+/Δ- columns)
- **Source:** Current delta - Previous delta
- **File:** `Analysis_Tools/precalculate_data.py`
- **Line:** 86
- **Code:** `dm['delta_chg'] = dm['delta_c'] - dm['delta_p']`
- **Find:** `grep -n "delta_chg =" Analysis_Tools/precalculate_data.py`

---

### 3. VEGA CHANGE (Call/Put Vega+ % columns)
- **Source:** Current vega - Previous vega
- **File:** `Analysis_Tools/precalculate_data.py`
- **Line:** 87
- **Code:** `dm['vega_chg'] = dm['vega_c'] - dm['vega_p']`
- **Find:** `grep -n "vega_chg =" Analysis_Tools/precalculate_data.py`

---

### 4. TRADING VALUE CHANGE (Call/Put ΔTV columns)
- **Source:** (OI × LastPrice)_current - (OI × LastPrice)_previous
- **File:** `Analysis_Tools/precalculate_data.py`
- **Line:** 88
- **Code:** `dm['tradval_chg'] = (dm['OpnIntrst_c'] * dm['LastPric_c']) - (dm['OpnIntrst_p'] * dm['LastPric_p'])`
- **Find:** `grep -n "tradval_chg =" Analysis_Tools/precalculate_data.py`

---

### 5. MONEYNESS CHANGE (Call/Put ΔMoney columns)
- **Source:** (Moneyness_current) - (Moneyness_previous)
- **File:** `Analysis_Tools/precalculate_data.py`
- **Lines:** 89-92
- **Code:**
```python
dm['moneyness_curr'] = (dm['UndrlygPric_c'] - dm['StrkPric']) / dm['UndrlygPric_c']
dm['moneyness_prev'] = (dm['UndrlygPric_p'] - dm['StrkPric']) / dm['UndrlygPric_p']
dm['money_chg'] = dm['moneyness_curr'] - dm['moneyness_prev']
```
- **Find:** `grep -n "money_chg =" Analysis_Tools/precalculate_data.py`

---

### 6. PCR (VOLUME) - Historical Chart Line 1
- **Formula:** Put Trading Volume / Call Trading Volume
- **Source:** TBL_*ticker*_DERIVED table, TtlTradgVol column
- **File:** `Analysis_Tools/dashboard_server.py`
- **Lines:** 108-119
- **Code:**
```python
put_vol = SUM(TtlTradgVol WHERE OptnTp = 'PE')
call_vol = SUM(TtlTradgVol WHERE OptnTp = 'CE')
pcr_volume = put_vol / call_vol
```
- **Find:** `grep -n "pcr_volume =" Analysis_Tools/dashboard_server.py`

---

### 7. PCR (OI) - Historical Chart Line 2
- **Formula:** Put Open Interest / Call Open Interest
- **Source:** TBL_*ticker*_DERIVED table, OpnIntrst column
- **File:** `Analysis_Tools/dashboard_server.py`
- **Lines:** 110-120
- **Code:**
```python
put_oi = SUM(OpnIntrst WHERE OptnTp = 'PE')
call_oi = SUM(OpnIntrst WHERE OptnTp = 'CE')
pcr_oi = put_oi / call_oi
```
- **Find:** `grep -n "pcr_oi =" Analysis_Tools/dashboard_server.py`

---

### 8. AVERAGE VEGA - Historical Chart Line 3 (when clicking Vega cells)
- **Formula:** Average of vega for specific option type (CE or PE)
- **Source:** TBL_*ticker*_DERIVED table, vega column
- **File:** `Analysis_Tools/dashboard_server.py`
- **Line:** 105
- **Code:** `AVG(CASE WHEN "OptnTp" = :opt_type THEN "vega" ELSE NULL END) as avg_vega`
- **Find:** `grep -n "avg_vega" Analysis_Tools/dashboard_server.py`

---

### 9. GREEKS (delta, vega, theta, iv)
- **Source:** Black-Scholes Model calculation
- **File:** `Database/update_database.py`
- **Lines:** 250-260
- **Code:**
```python
def greeks(premium, expiry, cd, asset_price, strike_price, intrest_rate, instrument_type):
    t = (expiry - cd) / 365
    imp_v = implied_volatility(premium, asset_price, strike_price, t, intrest_rate, flag)
    delta = delta(flag, asset_price, strike_price, t, intrest_rate, imp_v)
    vega = vega(flag, asset_price, strike_price, t, intrest_rate, imp_v)
    theta = theta(flag, asset_price, strike_price, t, intrest_rate, imp_v)
```
- **Find:** `grep -n "def greeks" Database/update_database.py`

---

### 10. STRIKE DIFF
- **Formula:** UndrlygPric - StrkPric
- **Source:** Calculated from Base table fields
- **File:** `Database/update_database.py`
- **Line:** ~310
- **Code:** `df["strike_diff"] = df["UndrlygPric"] - df["StrkPric"]`
- **Find:** `grep -n "strike_diff" Database/update_database.py`

---

## Database Table Mapping

### NSE CSV → Base Tables (TBL_NIFTY, TBL_BANKNIFTY, etc.)
| CSV Column | Database Column | Type | Created By |
|------------|-----------------|------|------------|
| BizDt | BizDt | DATE | update_database.py |
| TckrSymb | TckrSymb | VARCHAR | update_database.py |
| StrkPric | StrkPric | VARCHAR | update_database.py |
| OptnTp | OptnTp | VARCHAR | update_database.py |
| UndrlygPric | UndrlygPric | VARCHAR | update_database.py |
| OpnIntrst | OpnIntrst | VARCHAR | update_database.py |
| TtlTradgVol | TtlTradgVol | VARCHAR | update_database.py |
| TtlTrfVal | TtlTrfVal | VARCHAR | update_database.py |
| LastPric | LastPric | VARCHAR | update_database.py |

**Find:** `grep -n "CREATE TABLE" Database/update_database.py`

---

### Base Tables → Derived Tables (TBL_NIFTY_DERIVED, etc.)
| Base Column | Derived Column | Calculation | Created By |
|-------------|----------------|-------------|------------|
| BizDt | BizDt | Same | update_database.py |
| StrkPric | StrkPric | Same | update_database.py |
| OptnTp | OptnTp | Same | update_database.py |
| UndrlygPric | UndrlygPric | Same | update_database.py |
| OpnIntrst | OpnIntrst | Same | update_database.py |
| - | delta | Black-Scholes | update_database.py |
| - | vega | Black-Scholes | update_database.py |
| - | theta | Black-Scholes | update_database.py |
| - | iv | Implied Vol | update_database.py |
| - | strike_diff | UndrlygPric - StrkPric | update_database.py |
| - | chg_oi | % change OI | update_database.py |

**Find:** `grep -n "CREATE TABLE.*DERIVED" Database/update_database.py`

---

### Derived Tables → Cache Table (options_dashboard_cache)
| Derived Data | Cache Field | Calculation | Created By |
|--------------|-------------|-------------|------------|
| Multiple rows | data_json | JSON with all metrics | precalculate_data.py |
| BizDt | biz_date | Current date | precalculate_data.py |
| BizDt (prev) | prev_date | Previous date | precalculate_data.py |
| - | moneyness_type | TOTAL/OTM/ITM | precalculate_data.py |

**Find:** `grep -n "CREATE TABLE.*cache" Analysis_Tools/precalculate_data.py`

---

## Data JSON Structure in Cache

```json
{
  "stock": "NIFTY",
  "closing_price": 24500.50,
  "call_delta_pos_strike": "24600",
  "call_delta_pos_pct": "2.45",
  "call_delta_neg_strike": "24400",
  "call_delta_neg_pct": "-1.23",
  "call_vega_pos_strike": "24550",
  "call_vega_pos_pct": "3.67",
  "call_vega_neg_strike": "24450",
  "call_vega_neg_pct": "-2.14",
  "call_total_tradval": 15000000.50,
  "call_total_money": 0.0023,
  "put_delta_pos_strike": "24400",
  "put_delta_pos_pct": "1.89",
  "put_delta_neg_strike": "24600",
  "put_delta_neg_pct": "-0.98",
  "put_vega_pos_strike": "24450",
  "put_vega_pos_pct": "2.34",
  "put_vega_neg_strike": "24550",
  "put_vega_neg_pct": "-1.67",
  "put_total_tradval": -8000000.25,
  "put_total_money": -0.0015
}
```

**Find in code:** `grep -n "row_total\[" Analysis_Tools/precalculate_data.py`

---

## Quick Lookup Commands

```bash
# Find where closing price comes from
grep -rn "closing_price" Analysis_Tools/

# Find PCR calculation
grep -rn "pcr_volume\|pcr_oi" Analysis_Tools/

# Find delta change calculation
grep -rn "delta_chg" Analysis_Tools/

# Find vega change calculation
grep -rn "vega_chg" Analysis_Tools/

# Find moneyness calculation
grep -rn "moneyness" Analysis_Tools/

# Find Greeks calculation
grep -rn "def greeks" Database/

# Find all database queries
grep -rn "SELECT.*FROM" Analysis_Tools/

# Find where UndrlygPric is used
grep -rn "UndrlygPric" .

# Find where OpnIntrst is used
grep -rn "OpnIntrst" .

# Find where TtlTradgVol is used
grep -rn "TtlTradgVol" .
```

---

## Critical File Locations

| Data Type | Source File | Line(s) | Search Command |
|-----------|-------------|---------|----------------|
| Closing Price | precalculate_data.py | 67 | `grep -n "closing_price =" Analysis_Tools/precalculate_data.py` |
| Delta Change | precalculate_data.py | 86 | `grep -n "delta_chg =" Analysis_Tools/precalculate_data.py` |
| Vega Change | precalculate_data.py | 87 | `grep -n "vega_chg =" Analysis_Tools/precalculate_data.py` |
| Trading Value | precalculate_data.py | 88 | `grep -n "tradval_chg =" Analysis_Tools/precalculate_data.py` |
| Moneyness | precalculate_data.py | 89-92 | `grep -n "money_chg =" Analysis_Tools/precalculate_data.py` |
| PCR Volume | dashboard_server.py | 119 | `grep -n "pcr_volume =" Analysis_Tools/dashboard_server.py` |
| PCR OI | dashboard_server.py | 120 | `grep -n "pcr_oi =" Analysis_Tools/dashboard_server.py` |
| Avg Vega | dashboard_server.py | 105 | `grep -n "avg_vega" Analysis_Tools/dashboard_server.py` |
| Greeks | update_database.py | 250-260 | `grep -n "def greeks" Database/update_database.py` |

---

## Data Flow Diagram

```
NSE Website
    ↓ (CSV Download)
Database/update_database.py (Step 1)
    ↓ (Save to disk)
C:/NSE_EOD_FO/*.csv
    ↓ (Upload to DB)
Database/update_database.py (Step 2)
    ↓ (Create tables)
TBL_NIFTY, TBL_BANKNIFTY, etc. (Base Tables)
    ↓ (Calculate Greeks)
Database/update_database.py (Step 3)
    ↓ (Add columns: delta, vega, theta, iv)
TBL_NIFTY_DERIVED, TBL_BANKNIFTY_DERIVED, etc.
    ↓ (Calculate changes)
Analysis_Tools/precalculate_data.py
    ↓ (Store JSON)
options_dashboard_cache (TOTAL/OTM/ITM)
    ↓ (Fetch & Display)
Analysis_Tools/dashboard_server.py
    ↓ (Render UI)
Browser Dashboard (http://localhost:5000)
    ↓ (Click on cell)
JavaScript: showHistoricalChart()
    ↓ (API call)
dashboard_server.py: /get_historical_data
    ↓ (Query DERIVED tables)
40-day PCR + Vega/Moneyness data
    ↓ (Display)
Chart.js Line Chart (Modal Popup)
```

---

**Created:** October 2025  
**Version:** 4.0  
**Purpose:** Quick reference for data source tracing
