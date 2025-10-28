# COMPLETE DATA SOURCE MAPPING
## Every Value, Every Function, Every File

---

## 📁 FILE STRUCTURE

```
Database/
├── update_database.py           # Master orchestrator (calls everything)
├── precalculate_data.py        # Calculates metrics + Hybrid RSI
└── clear_cache.py              # Utility to clear cache

Analysis_Tools/
├── dashboard_server.py         # Web server (Flask)
├── static/script.js            # Frontend logic
└── templates/index.html        # Dashboard UI
```

---

## 🔄 COMPLETE DATA FLOW

### STEP 1: NSE Raw Data Collection
**File:** `Database/update_database.py`

| Function | Returns | Description |
|----------|---------|-------------|
| `download_bhav_copy(date)` | `str` (filepath) | Downloads ZIP from NSE website |
| `parse_bhav_copy(filepath)` | `pandas.DataFrame` | Parses CSV, extracts options data |
| `calculate_greeks(df)` | `pandas.DataFrame` | Calculates delta, gamma, vega, theta |
| `store_in_database(df, ticker)` | `bool` | Stores in PostgreSQL `TBL_*_DERIVED` tables |

**Database Tables Created:**
```sql
TBL_NIFTY_DERIVED
TBL_BANKNIFTY_DERIVED
TBL_FINNIFTY_DERIVED
... (one table per ticker)
```

**Columns Stored:**
- `BizDt`, `TckrSymb`, `StrkPric`, `OptnTp` (CE/PE)
- `UndrlygPric`, `ClsPric`, `LastPric`
- `OpnIntrst`, `TtlTradgVol`, `TtlTrfVal`
- `delta`, `vega`, `theta`, `iv` (Greeks)
- `strike_diff`, `y_oi`, `chg_oi`, `chg_price`

**Source:** NSE BhavCopy CSV files from https://archives.nseindia.com/

---

### STEP 2: Dashboard Calculation (Hybrid RSI)
**File:** `Database/precalculate_data.py`

#### 🎯 HYBRID RSI SYSTEM

##### A. TradingView RSI (PRIMARY)
**Function:** `fetch_tradingview_data(ticker, current_date)`  
**Returns:** `(price, rsi)` tuple or `(None, None)`  
**Source:** TradingView via `tvDatafeed` library  
**Method:** Wilder's RSI (14-period EMA)

```python
# Line 53-84 in precalculate_data.py
def fetch_tradingview_data(ticker, current_date):
    tv = TvDatafeed()  # Initialize TradingView connection
    data = tv.get_hist(symbol, 'NSE', interval=Interval.in_daily, n_bars=60)
    
    # Calculate RSI using Wilder's method
    delta = data['close'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    
    return (closing_price, rsi_value)
```

**Returns:**
- `price` (float): Closing price from TradingView
- `rsi` (float): RSI value (0-100) from TradingView

##### B. Database RSI (FALLBACK)
**Function:** `calculate_db_rsi(table_name, current_date, all_dates)`  
**Returns:** `float` (RSI) or `None`  
**Source:** PostgreSQL `TBL_*_DERIVED` tables  
**Method:** TA-Lib RSI (14-period)

```python
# Line 86-112 in precalculate_data.py
def calculate_db_rsi(table_name, current_date, all_dates):
    # Get 50 days of closing prices from database
    query = f'SELECT "BizDt", "ClsPric" FROM "{table_name}" 
             WHERE "BizDt" IN ({date_range}) 
             ORDER BY "BizDt" ASC'
    df = pd.read_sql(query, engine)
    
    # Calculate RSI using TA-Lib
    prices = np.array(df['ClsPric'].values, dtype=float)
    rsi_values = talib.RSI(prices, timeperiod=14)
    
    return current_rsi
```

**Returns:**
- `rsi` (float): RSI value (0-100) from database calculation

##### C. Hybrid Decision Logic
**Function:** `calculate_and_store_data(curr_date, prev_date)`  
**Location:** Line 185-195 in `precalculate_data.py`

```python
# HYBRID RSI: Try TradingView first, fallback to Database
tv_price, tv_rsi = fetch_tradingview_data(ticker, curr_date)
if tv_price and tv_rsi:
    closing_price = tv_price      # ✅ From TradingView
    rsi_value = tv_rsi            # ✅ From TradingView
    source = "TV"
else:
    closing_price = float(dfc['UndrlygPric'].iloc[0])  # ✅ From Database
    rsi_value = calculate_db_rsi(table, curr_date, all_dates)  # ✅ From Database
    source = "DB"
```

---

### 📊 ALL DASHBOARD METRICS - COMPLETE MAPPING

#### For Each Ticker, These Values Are Calculated:

| Metric | Source | Function | File | Line | Calculation |
|--------|--------|----------|------|------|-------------|
| **stock** | Database | `calculate_and_store_data()` | precalculate_data.py | 171 | `table.replace("TBL_", "").replace("_DERIVED", "")` |
| **closing_price** | TradingView OR Database | `fetch_tradingview_data()` OR from DB | precalculate_data.py | 185-192 | Hybrid: TV first, DB fallback |
| **rsi** | TradingView OR Database | `fetch_tradingview_data()` OR `calculate_db_rsi()` | precalculate_data.py | 185-192 | Hybrid: TV first, DB fallback |

---

#### CALL OPTIONS METRICS (All from Database)

| Metric | Source | Calculation | File | Line |
|--------|--------|-------------|------|------|
| **call_delta_pos_strike** | Database | Strike with MAX delta increase | precalculate_data.py | 219-221 | `dv.loc[dv['delta_chg'].idxmax(), 'StrkPric']` |
| **call_delta_pos_pct** | Database | MAX delta change × 100 | precalculate_data.py | 222 | `dv.loc[idx, 'delta_chg'] * 100` |
| **call_delta_neg_strike** | Database | Strike with MIN delta decrease | precalculate_data.py | 227-229 | `dv.loc[dv['delta_chg'].idxmin(), 'StrkPric']` |
| **call_delta_neg_pct** | Database | MIN delta change × 100 | precalculate_data.py | 230 | `dv.loc[idx, 'delta_chg'] * 100` |
| **call_vega_pos_strike** | Database | Strike with MAX vega increase | precalculate_data.py | 235-238 | `dv.loc[df_pos['vega_chg'].idxmax(), 'StrkPric']` |
| **call_vega_pos_pct** | Database | MAX vega change × 100 | precalculate_data.py | 239 | `dv.loc[idx, 'vega_chg'] * 100` |
| **call_vega_neg_strike** | Database | Strike with MIN vega decrease | precalculate_data.py | 244-247 | `dv.loc[df_neg['vega_chg'].idxmin(), 'StrkPric']` |
| **call_vega_neg_pct** | Database | MIN vega change × 100 | precalculate_data.py | 248 | `dv.loc[idx, 'vega_chg'] * 100` |
| **call_total_tradval** | Database | Sum of trading value changes | precalculate_data.py | 253 | `(OpnIntrst_curr × LastPric_curr) - (OpnIntrst_prev × LastPric_prev)` |
| **call_total_money** | Database | Sum of moneyness changes | precalculate_data.py | 254 | `Σ(moneyness_curr - moneyness_prev)` |

---

#### PUT OPTIONS METRICS (All from Database)

| Metric | Source | Calculation | File | Line |
|--------|--------|-------------|------|------|
| **put_delta_pos_strike** | Database | Strike with MAX delta increase | precalculate_data.py | 219-221 | `dv.loc[dv['delta_chg'].idxmax(), 'StrkPric']` |
| **put_delta_pos_pct** | Database | MAX delta change × 100 | precalculate_data.py | 222 | `dv.loc[idx, 'delta_chg'] * 100` |
| **put_delta_neg_strike** | Database | Strike with MIN delta decrease | precalculate_data.py | 227-229 | `dv.loc[dv['delta_chg'].idxmin(), 'StrkPric']` |
| **put_delta_neg_pct** | Database | MIN delta change × 100 | precalculate_data.py | 230 | `dv.loc[idx, 'delta_chg'] * 100` |
| **put_vega_pos_strike** | Database | Strike with MAX vega increase | precalculate_data.py | 235-238 | `dv.loc[df_pos['vega_chg'].idxmax(), 'StrkPric']` |
| **put_vega_pos_pct** | Database | MAX vega change × 100 | precalculate_data.py | 239 | `dv.loc[idx, 'vega_chg'] * 100` |
| **put_vega_neg_strike** | Database | Strike with MIN vega decrease | precalculate_data.py | 244-247 | `dv.loc[df_neg['vega_chg'].idxmin(), 'StrkPric']` |
| **put_vega_neg_pct** | Database | MIN vega change × 100 | precalculate_data.py | 248 | `dv.loc[idx, 'vega_chg'] * 100` |
| **put_total_tradval** | Database | Sum of trading value changes | precalculate_data.py | 253 | `(OpnIntrst_curr × LastPric_curr) - (OpnIntrst_prev × LastPric_prev)` |
| **put_total_money** | Database | Sum of moneyness changes | precalculate_data.py | 254 | `Σ(moneyness_curr - moneyness_prev)` |

---

### 🧮 DETAILED CALCULATION FORMULAS

#### From Database Columns to Metrics:

```python
# File: precalculate_data.py
# Function: calculate_and_store_data()

# 1. DELTA CHANGE (Line 207)
delta_change = delta_current - delta_previous

# 2. VEGA CHANGE (Line 208)
vega_change = vega_current - vega_previous

# 3. TRADING VALUE CHANGE (Line 209)
tradval_change = (OpnIntrst_current × LastPric_current) - 
                 (OpnIntrst_previous × LastPric_previous)

# 4. MONEYNESS CURRENT (Line 210)
moneyness_current = (UndrlygPric_current - StrkPric) / UndrlygPric_current

# 5. MONEYNESS PREVIOUS (Line 211)
moneyness_previous = (UndrlygPric_previous - StrkPric) / UndrlygPric_previous

# 6. MONEYNESS CHANGE (Line 212)
moneyness_change = moneyness_current - moneyness_previous

# 7. RSI (HYBRID - Lines 53-112)
# TradingView RSI:
delta = close_price.diff()
gain = delta.clip(lower=0)
loss = -delta.clip(upper=0)
avg_gain = gain.ewm(alpha=1/14).mean()
avg_loss = loss.ewm(alpha=1/14).mean()
RS = avg_gain / avg_loss
RSI = 100 - (100 / (1 + RS))

# Database RSI (fallback):
RSI = talib.RSI(closing_prices, timeperiod=14)
```

---

### 🗂️ OTM vs ITM FILTERING

**File:** `precalculate_data.py`  
**Function:** `calculate_and_store_data()`  
**Lines:** 257-340

```python
# OTM Condition (Line 259-260)
if option_type == 'CE':
    otm_condition = strike_diff_current < 0  # Strike below underlying
else:  # PE
    otm_condition = strike_diff_current > 0  # Strike above underlying

# ITM Condition (Line 262-263)
if option_type == 'CE':
    itm_condition = strike_diff_current > 0  # Strike above underlying
else:  # PE
    itm_condition = strike_diff_current < 0  # Strike below underlying
```

**Where strike_diff comes from:**
- **Source:** Database column `strike_diff`
- **Calculated in:** `update_database.py` → `calculate_greeks()` function
- **Formula:** `strike_diff = UndrlygPric - StrkPric`

---

### 💾 CACHING SYSTEM

**File:** `precalculate_data.py`  
**Function:** `precalculate_all_dates()`  
**Lines:** 359-403

#### Cache Storage:
```sql
-- Table: options_dashboard_cache
CREATE TABLE options_dashboard_cache (
    id SERIAL PRIMARY KEY,
    biz_date DATE NOT NULL,
    prev_date DATE NOT NULL,
    moneyness_type VARCHAR(10) NOT NULL,  -- 'TOTAL', 'OTM', 'ITM'
    data_json TEXT NOT NULL,              -- JSON array of all ticker data
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### What Gets Cached:
```json
{
  "stock": "NIFTY",
  "closing_price": 23450.50,      // From TradingView OR Database
  "rsi": 54.32,                   // From TradingView OR Database
  "call_delta_pos_strike": "23500",  // From Database
  "call_delta_pos_pct": "12.50",     // From Database
  // ... all 24 metrics per ticker
}
```

**Function:** `precalculate_all_dates()` (Line 359)
- Loops through all dates
- Calls `calculate_and_store_data()` for each date
- Stores in `options_dashboard_cache` table
- Creates 3 rows per date: TOTAL, OTM, ITM

---

### 🌐 WEB SERVER DATA SERVING

**File:** `Analysis_Tools/dashboard_server.py`

#### API Endpoints:

##### 1. Main Dashboard Data
**Route:** `GET /get_data?date=YYYY-MM-DD`  
**Function:** `get_data()`  
**Returns:** JSON

```python
# File: dashboard_server.py
# Function: get_data() - Line 45

@app.route('/get_data')
def get_data():
    date = request.args.get('date')
    
    # Query from cache table
    query = """
        SELECT moneyness_type, data_json 
        FROM options_dashboard_cache 
        WHERE biz_date = :date
    """
    result = pd.read_sql(query, engine, params={"date": date})
    
    return {
        "curr_date": date,
        "prev_date": prev_date,
        "total": json.loads(result['TOTAL']),  # All strikes
        "otm": json.loads(result['OTM']),      # OTM strikes only
        "itm": json.loads(result['ITM'])       # ITM strikes only
    }
```

**Data Source:** `options_dashboard_cache` table (pre-calculated)

##### 2. Historical Chart Data
**Route:** `GET /get_historical_data?ticker=X&date=Y&type=Z&metric=M`  
**Function:** `get_historical_data()`  
**Returns:** JSON (40 days)

```python
# File: dashboard_server.py
# Function: get_historical_data() - Line 85

@app.route('/get_historical_data')
def get_historical_data():
    ticker = request.args.get('ticker')
    date = request.args.get('date')
    option_type = request.args.get('type')  # 'call' or 'put'
    metric = request.args.get('metric')      # 'money' or 'vega'
    strike = request.args.get('strike')      # Optional
    
    # Get 40 dates from cache
    query = """
        SELECT biz_date, data_json 
        FROM options_dashboard_cache 
        WHERE biz_date <= :date 
        ORDER BY biz_date DESC 
        LIMIT 40
    """
    
    # Extract data for this ticker
    # Calculate PCR, underlying price, RSI, metric values
    
    return {
        "ticker": ticker,
        "option_type": option_type,
        "metric": metric,
        "data": [
            {
                "date": "2025-10-15",
                "pcr_volume": 0.85,        # From cache JSON
                "pcr_oi": 1.12,            # From cache JSON
                "underlying_price": 23450.50,  # From cache JSON
                "rsi": 54.32,              // From cache JSON (TV or DB)
                "moneyness": 0.025         # From cache JSON
            },
            // ... 39 more days
        ]
    }
```

**Data Source:** `options_dashboard_cache` table (40 most recent dates)

---

### 🎨 FRONTEND DISPLAY

**File:** `Analysis_Tools/static/script.js`

#### Main Table Display
**Function:** `loadData()`  
**Line:** 150-200

```javascript
// Fetches data from /get_data endpoint
// Populates TOTAL, OTM, ITM tabs
// Source: options_dashboard_cache via Flask API

async function loadData() {
    const response = await fetch(`/get_data?date=${selectedDate}`);
    const data = await response.json();
    
    // data.total[0].stock          // From precalculate_data.py
    // data.total[0].closing_price  // From TradingView OR Database
    // data.total[0].rsi            // From TradingView OR Database
    // data.total[0].call_delta_pos_strike  // From Database
    // ... all metrics
}
```

#### Historical Charts
**Function:** `showHistoricalChart(ticker, optionType, metric, strike)`  
**Line:** 350-450

```javascript
async function showHistoricalChart(ticker, optionType, metric, strike) {
    const url = `/get_historical_data?ticker=${ticker}&date=${date}&type=${optionType}&metric=${metric}&strike=${strike}`;
    const response = await fetch(url);
    const data = await response.json();
    
    // data.data[].pcr_volume        // From cache
    // data.data[].pcr_oi            // From cache
    // data.data[].underlying_price  // From TradingView OR Database
    // data.data[].rsi               // From TradingView OR Database
    // data.data[].moneyness         // From Database calculation
}
```

---

## 📋 COMPLETE VALUE SOURCE SUMMARY

### Values from TradingView (PRIMARY):
1. **Underlying Price** - `fetch_tradingview_data()` in `precalculate_data.py`
2. **RSI** - `fetch_tradingview_data()` in `precalculate_data.py`

### Values from Database (FALLBACK for RSI/Price):
1. **Underlying Price** - Column `UndrlygPric` from `TBL_*_DERIVED`
2. **RSI** - `calculate_db_rsi()` in `precalculate_data.py` using TA-Lib

### Values from Database (ALWAYS):
1. **Delta changes** - Calculated from `delta` column in `TBL_*_DERIVED`
2. **Vega changes** - Calculated from `vega` column in `TBL_*_DERIVED`
3. **Trading value changes** - From `OpnIntrst` × `LastPric` columns
4. **Moneyness changes** - From `UndrlygPric` and `StrkPric` columns
5. **Strike prices** - From `StrkPric` column
6. **All percentages** - Calculated from change values × 100

---

## 🔄 COMPLETE EXECUTION FLOW

```
1. NSE Website
   ↓ (Download)
   
2. update_database.py
   ├→ download_bhav_copy()     → Downloads ZIP
   ├→ parse_bhav_copy()        → Parses CSV
   ├→ calculate_greeks()       → Calculates Greeks
   └→ store_in_database()      → Stores in PostgreSQL
   
3. precalculate_data.py (called by update_database.py)
   ├→ fetch_tradingview_data() → Gets RSI + Price from TradingView
   │   ↓ (If fails)
   ├→ calculate_db_rsi()       → Gets RSI from Database (TA-Lib)
   ├→ calculate_and_store_data() → Calculates all metrics
   └→ precalculate_all_dates() → Stores in cache table
   
4. dashboard_server.py
   ├→ get_data()               → Serves dashboard data
   └→ get_historical_data()    → Serves chart data
   
5. script.js
   ├→ loadData()               → Displays table
   └→ showHistoricalChart()    → Displays charts
   
6. Browser
   └→ Shows final dashboard with all values
```

---

## ✅ VERIFICATION CHECKLIST

When you run `python update_database.py`, you should see:

```
[1/100] 2025-10-15:
    TV: NIFTY          | Price:  23450.50 | RSI: 54.32    ← TradingView
    TV: BANKNIFTY      | Price:  48900.20 | RSI: 62.18    ← TradingView
    DB: 360ONE         | Price:   1160.40 | RSI: 50.96    ← Database
    ✅ Cached 25 tickers
```

**Legend:**
- **TV** = Value from TradingView (`fetch_tradingview_data()`)
- **DB** = Value from Database (`calculate_db_rsi()` + database columns)

---

## 📊 FINAL DATA STRUCTURE IN CACHE

```json
{
  "stock": "NIFTY",
  
  // HYBRID VALUES (TradingView OR Database)
  "closing_price": 23450.50,  // fetch_tradingview_data() OR database UndrlygPric
  "rsi": 54.32,               // fetch_tradingview_data() OR calculate_db_rsi()
  
  // DATABASE ONLY VALUES
  "call_delta_pos_strike": "23500",      // From delta column
  "call_delta_pos_pct": "12.50",         // delta_change × 100
  "call_delta_neg_strike": "23400",      // From delta column
  "call_delta_neg_pct": "-8.30",         // delta_change × 100
  "call_vega_pos_strike": "23500",       // From vega column
  "call_vega_pos_pct": "5.20",           // vega_change × 100
  "call_vega_neg_strike": "23450",       // From vega column
  "call_vega_neg_pct": "-3.10",          // vega_change × 100
  "call_total_tradval": 125000000.50,    // Σ(OI × Price changes)
  "call_total_money": 0.025,             // Σ(moneyness changes)
  
  "put_delta_pos_strike": "23400",       // From delta column
  "put_delta_pos_pct": "15.20",          // delta_change × 100
  "put_delta_neg_strike": "23500",       // From delta column
  "put_delta_neg_pct": "-10.50",         // delta_change × 100
  "put_vega_pos_strike": "23400",        // From vega column
  "put_vega_pos_pct": "4.80",            // vega_change × 100
  "put_vega_neg_strike": "23500",        // From vega column
  "put_vega_neg_pct": "-2.90",           // vega_change × 100
  "put_total_tradval": 98000000.25,      // Σ(OI × Price changes)
  "put_total_money": -0.018              // Σ(moneyness changes)
}
```

---

**Everything is documented! Every value, every function, every file!** 🎯

Now just run:
```bash
python update_database.py
```

And watch the hybrid RSI system work! 🚀
