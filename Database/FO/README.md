# Database Directory

This directory contains scripts for managing the complete data pipeline for NSE BhavCopy derivatives data. All data flows through PostgreSQL for storage, analysis, and pre-calculation of metrics.

## 📋 Overview

The Database module handles:
- **Data Ingestion**: Downloading NSE FO (Futures & Options) BhavCopy data
- **Data Storage**: Uploading CSV data to PostgreSQL tables (one table per ticker)
- **Derivatives Calculation**: Computing Greeks (Delta, Gamma, Vega, Theta, Rho) and IV
- **Performance Optimization**: Pre-calculating screener and dashboard metrics for instant loading

## 📁 Files

### 1. **`update_database.py`** ⭐ [MAIN SCRIPT]
The master orchestration script that runs the complete data pipeline in sequence.

**What it does:**
- **Step 1**: Downloads latest NSE BhavCopy CSV files from NSE archives
- **Step 2**: Uploads CSV data to PostgreSQL (creates one table per ticker: `TBL_NIFTY`, `TBL_BANKNIFTY`, etc.)
- **Step 3**: Calculates Greeks and creates derived tables (`TBL_NIFTY_DERIVED`, etc.)
- **Step 4**: Pre-calculates screener cache data for instant filtering
- **Step 5**: Pre-calculates dashboard data for fast chart/table rendering

**Usage:**
```bash
python update_database.py
```

**Database Tables Created:**
- Base tables: `TBL_TICKER_NAME` (raw BhavCopy data)
- Derived tables: `TBL_TICKER_NAME_DERIVED` (with Greeks and metrics)

---

### 2. **`precalculate_data.py`**
Pre-calculates dashboard metrics and stores them for instant dashboard loading.

**What it calculates:**
- RSI(14) - Relative Strength Index for price momentum
- Underlying prices at close
- Dashboard summaries (OI changes, price changes)
- Stores in PostgreSQL for quick retrieval

**Features:**
- Auto-detects new dates and only processes unprocessed data
- Uses pandas_ta library for accurate RSI calculation
- Outputs data as JSON for frontend consumption

---

### 3. **`screener_cache.py`**
Pre-calculates all screener filtering metrics and caches results in database.

**What it caches:**
- **OI Screeners**: Volume levels, OI rank, OI changes
- **Moneyness Screens**: ITM (In-The-Money), ATM (At-The-Money), OTM (Out-Of-The-Money)
- **IV Screening**: Implied Volatility changes, extremes

**Performance Impact:**
- Reduces screener load time from **10+ seconds** → **<0.5 seconds**
- Stores results in `screener_cache` table with indexed queries

**Included Filters:**
- Price momentum (futures, underlying)
- IV changes and extremes
- OI levels and changes
- Moneyness-based grouping
- Expiry categorization

---

### 4. **`BhavCopy_Data_Fetch_All_CSV.py`**
Standalone script for bulk downloading and uploading all historical BhavCopy data.

**Use cases:**
- Initial database setup with historical data
- Backfilling missing dates
- Data recovery

**Process:**
1. Downloads all available CSV files from NSE
2. Cleans and formats the data
3. Creates PostgreSQL tables by ticker
4. Uploads all data in batches

**Configuration:**
- Edit database credentials at the top of the script
- Specify date range for download

---

### 5. **`BhavCopy_New_Nse_fo_roman_suffixes.py`**
Utility script for handling NSE FO data with Roman numeral suffixes.

**Purpose:**
- Processes options with Roman suffixes (e.g., NIFTY22NOV24C1250I)
- Normalizes ticker symbols for database storage

---

### 6. **`new_bhav_future_processor.py`**
Processor for futures-specific data from BhavCopy files.

**Handles:**
- Futures contract details (expiry, multiplier)
- Futures-specific calculations
- Data validation and cleaning

---

## 🔧 Configuration

All scripts use the same database configuration. Update if needed:

```python
db_user = 'postgres'
db_password = 'Gallop@3104'
db_host = 'localhost'
db_port = '5432'
db_name = 'BhavCopy_Database'
```

**⚠️ Security Note**: Consider using environment variables for credentials instead of hardcoding them.

---

## 📊 Database Schema

### Base Tables (Raw Data)
- `TBL_NIFTY`, `TBL_BANKNIFTY`, `TBL_FINNIFTY`, etc.
- Schema: BizDt, Sgmt, FinInstrmTp, TckrSymb, StrkPric, OptnTp, LastPric, OpenInterest, and 17 more columns

### Derived Tables (Calculated Data)
- `TBL_NIFTY_DERIVED`, `TBL_BANKNIFTY_DERIVED`, etc.
- Extends base schema with: Delta, Gamma, Vega, Theta, Rho, IV, strike_diff, y_oi, chg_oi, chg_price

### Cache Tables
- `screener_cache` - Pre-calculated screener results
- `dashboard_precalc` - Pre-calculated dashboard metrics

---

## ⚙️ Data Flow Pipeline

```
NSE Servers
    ↓
[Download CSVs] → save_fo_eod folder
    ↓
[Upload to DB] → TBL_TICKER tables
    ↓
[Calculate Greeks] → TBL_TICKER_DERIVED tables
    ↓
[Pre-calc Screener] → screener_cache table
    ↓
[Pre-calc Dashboard] → dashboard metrics
    ↓
Frontend (Instant Loading)
```

---

## 🚀 Running the Pipeline

### Daily Update (Recommended)
```bash
python update_database.py
```
This runs all 5 steps automatically. Best run after market close (4 PM).

### Bulk Historical Load
```bash
python BhavCopy_Data_Fetch_All_CSV.py
```

### Manual Component Steps
```bash
# Only download CSVs
python -c "from update_database import download_csv_data; download_csv_data()"

# Only upload to database
python -c "from update_database import upload_to_database; upload_to_database()"

# Only calculate Greeks
python -c "from update_database import calculate_greeks; calculate_greeks()"

# Only pre-calculate screener
python screener_cache.py

# Only pre-calculate dashboard
python precalculate_data.py
```

---

## 📈 Key Metrics Calculated

### Greeks (Per Option) - Black-Scholes Model

The Greeks are calculated using the **Black-Scholes Option Pricing Model** via the `py_vollib` library. They measure the sensitivity of option prices to various market factors.

#### **1. Delta (Δ) - Price Sensitivity**
**Definition**: Rate of change of option price with respect to 1-point change in underlying price.

**Formula for Call Option:**
$$\Delta_C = N(d_1)$$

**Formula for Put Option:**
$$\Delta_P = N(d_1) - 1$$

Where:
- $N(d_1)$ = Cumulative standard normal distribution of $d_1$
- $d_1 = \frac{\ln(S/K) + (r + \frac{\sigma^2}{2})T}{\sigma\sqrt{T}}$

**Interpretation:**
- Range: -1 to +1 (Call: 0 to 1, Put: -1 to 0)
- Delta = 0.5 means option price moves ₹0.50 for every ₹1 move in underlying
- Approximates probability of option finishing in-the-money

**Example:**
- Call Delta = 0.75: If underlying ↑ ₹10, call price ↑ ~₹7.50
- Put Delta = -0.30: If underlying ↑ ₹10, put price ↓ ~₹3.00

---

#### **2. Gamma (Γ) - Delta Acceleration**
**Definition**: Rate of change of Delta with respect to change in underlying price (second derivative).

**Formula:**
$$\Gamma = \frac{N'(d_1)}{S \cdot \sigma \cdot \sqrt{T}}$$

Where:
- $N'(d_1) = \frac{1}{\sqrt{2\pi}} e^{-\frac{d_1^2}{2}}$ (Standard normal probability density function)
- $S$ = Current spot price
- $\sigma$ = Volatility (annualized)
- $T$ = Time to expiry (in years)

**Interpretation:**
- Always positive for both calls and puts
- Range: 0 to infinity (typically 0.01 to 0.50)
- High Gamma = Delta changes rapidly with price movement
- ATM options have highest Gamma
- Gamma is important for hedging and managing Delta risk

**Example:**
- Option Delta = 0.50, Gamma = 0.08
- If underlying ↑ ₹1, new Delta ≈ 0.50 + 0.08 = 0.58
- Delta is no longer constant; acceleration is 0.08

---

#### **3. Vega (ν) - Volatility Sensitivity**
**Definition**: Change in option price for 1% change in implied volatility (per annum).

**Formula:**
$$\nu = S \cdot N'(d_1) \cdot \sqrt{T}$$

Where:
- $S$ = Current spot price
- $N'(d_1)$ = Standard normal PDF of $d_1$
- $T$ = Time to expiry (in years)

**Interpretation:**
- Same for calls and puts (always positive)
- Measured per 1% change in IV
- Range: Typically 0 to 50
- High Vega: Sensitive to volatility changes
- ATM options have highest Vega
- Long options = positive Vega (benefit from IV ↑)
- Short options = negative Vega (benefit from IV ↓)

**Example:**
- Vega = 5.5 means: If IV ↑ 1%, option price ↑ ₹5.50
- If IV ↑ 2%, option price ↑ ~₹11

---

#### **4. Theta (Θ) - Time Decay**
**Definition**: Daily change in option price due to passage of time (all else equal), measured per day.

**Formula for Call:**
$$\Theta_C = -\frac{S \cdot N'(d_1) \cdot \sigma}{2\sqrt{T}} - r \cdot K \cdot e^{-rT} \cdot N(d_2)$$

**Formula for Put:**
$$\Theta_P = -\frac{S \cdot N'(d_1) \cdot \sigma}{2\sqrt{T}} + r \cdot K \cdot e^{-rT} \cdot N(-d_2)$$

Where:
- $d_2 = d_1 - \sigma\sqrt{T}$
- $r$ = Risk-free rate (0.06 used in our calculation)
- $K$ = Strike price
- $T$ = Time to expiry (in years)

**Interpretation:**
- Typically negative for long options (time decay hurts holder)
- Typically positive for short options (time decay benefits seller)
- Accelerates as expiry approaches
- ATM options have maximum Theta decay
- OTM options have lower absolute Theta but faster time decay rate

**Example:**
- Theta = -0.45 (call): Option loses ₹0.45 per day due to time decay
- Theta = +0.38 (put sold): Short put gains ₹0.38 per day from time decay

---

#### **5. Rho (ρ) - Interest Rate Sensitivity**
**Definition**: Change in option price for 1% change in risk-free interest rate.

**Formula for Call:**
$$\rho_C = K \cdot T \cdot e^{-rT} \cdot N(d_2)$$

**Formula for Put:**
$$\rho_P = -K \cdot T \cdot e^{-rT} \cdot N(-d_2)$$

Where:
- All variables as defined above

**Interpretation:**
- Call Rho positive (benefit from interest rate ↑)
- Put Rho negative (hurt from interest rate ↑)
- Range: Typically -20 to +20 (depends on strike and expiry)
- Least sensitive Greek in liquid markets
- More important for longer-dated options

**Example:**
- Call Rho = 12: If interest rate ↑ 1%, call price ↑ ₹12
- Put Rho = -8: If interest rate ↑ 1%, put price ↓ ₹8

---

#### **6. Implied Volatility (IV)**
**Definition**: The volatility level implied by the current option market price using the Black-Scholes model.

**Calculation Method:**
- We use **reverse Black-Scholes** (iterative solver)
- Given: Market option price, S, K, T, r
- Find: σ (volatility) that makes theoretical price = market price

**Formula (Black-Scholes for verification):**
$$C = S \cdot N(d_1) - K \cdot e^{-rT} \cdot N(d_2)$$

**Interpretation:**
- IV > Historical Volatility = Market expects more movement (buy premium risky)
- IV < Historical Volatility = Market expects less movement (sell premium risky)
- Higher IV = Higher option prices
- IV Rank helps identify relative volatility extremes

**Example:**
- IV = 25% means: Market expects 25% annualized price movement
- IV = 45% means: Market is more volatile/uncertain

---

### **Black-Scholes Model Parameters Used**

```
S (Spot Price)       = UndrlygPric from NSE data
K (Strike Price)     = StrkPric from NSE data
T (Time to Expiry)   = (Expiry Date - Current Date) / 365 years
r (Risk-free Rate)   = 0.06 (6% annual, typical for India)
σ (Volatility)       = Derived from option price (IV calculation)
C (Call Price)       = LastPric for CALL options
P (Put Price)        = LastPric for PUT options
```

---

### **Calculation Process in Code**

```python
def greeks(premium, expiry, cd, asset_price, strike_price, intrest_rate, instrument_type):
    # Calculate time to expiry in years
    t = ((expiry - cd) / timedelta(days=1)) / 365

    if t <= 0:
        return {"IV": 0, "Delta": 0, "Gamma": 0, "Rho": 0, "Theta": 0, "Vega": 0}

    # Determine option type (C = call, P = put)
    flag = instrument_type[0].lower()  # 'c' or 'p'

    # Calculate IV using reverse Black-Scholes
    imp_v = implied_volatility(premium, asset_price, strike_price, t, intrest_rate, flag)

    # Calculate all Greeks
    return {
        "IV": imp_v,
        "Delta": delta(flag, asset_price, strike_price, t, intrest_rate, imp_v),
        "Gamma": gamma(flag, asset_price, strike_price, t, intrest_rate, imp_v),
        "Rho": rho(flag, asset_price, strike_price, t, intrest_rate, imp_v),
        "Theta": theta(flag, asset_price, strike_price, t, intrest_rate, imp_v),
        "Vega": vega(flag, asset_price, strike_price, t, intrest_rate, imp_v)
    }
```

---

### **Key Relationships (Greeks Interactions)**

| Scenario | Delta | Gamma | Vega | Theta | Rho |
|----------|-------|-------|------|-------|-----|
| **Spot ↑** | Increases (call) | Decreases | Usually ↑ | Negative (call) | Positive (call) |
| **Vol ↑** | Increases (OTM) | Decreases (ATM) | Increases | Negative (call) | No direct impact |
| **Time ↓** | ITM increases | Decreases | Decreases | Accelerates | Decreases |
| **ATM Options** | ≈0.5 | Highest | Highest | Highest (negative) | Lowest |
| **ITM Options** | ↑(call)/↓(put) | Lower | Lower | More negative | Higher |
| **OTM Options** | ↓(call)/↑(put) | Lower | Lower | Less negative | Lower |

---

### **Greeks in Risk Management**

- **Delta Hedging**: Use Delta to hedge price risk
- **Gamma Management**: Monitor Gamma to predict Delta changes
- **Vega Hedging**: Use Vega to manage volatility exposure
- **Theta Strategies**: Exploit time decay (short options)
- **Rho Considerations**: Important for long-dated options

---

### Screener Metrics
- **OI Change %**: Percentage change in open interest
- **Price Change %**: Percentage change in closing price
- **Strike Difference**: Underlying price - Strike price (moneyness)
- **Yesterday OI**: Previous session's open interest

---

## 📝 CSV Data Format

NSE BhavCopy files contain:
- **Headers**: 22 columns (all uppercase with specific NSE naming)
- **Data**: One row per option/futures contract per day
- **Format**: Comma-separated values with date formatting

**Sample Columns:**
```
BizDt, Sgmt, FinInstrmTp, TckrSymb, FininstrmActlXpryDt, StrkPric, OptnTp,
FinInstrmNm, OpnPric, HghPric, LwPric, ClsPric, LastPric, PrvsClsgPric,
UndrlygPric, SttlmPric, OpnIntrst, ChngInOpnIntrst, TtlTradgVol, TtlTrfVal,
TtlNbOfTxsExctd, NewBrdLotQty
```

---

## ⏱️ Performance Notes

**Execution Times (Approximate):**
- CSV Download: 2-5 minutes
- Database Upload: 3-8 minutes
- Greeks Calculation: 5-15 minutes
- Screener Cache: 2-5 minutes
- Dashboard Pre-calc: 1-3 minutes
- **Total**: 15-40 minutes depending on market activity

**Optimization Strategies:**
- Pre-calculated data loaded instantly (<0.5 sec)
- Indexed database queries for fast filtering
- Batch processing for large data inserts
- Only processes new/updated data (no re-processing)

---

## 🐛 Troubleshooting

### CSV Download Fails
- Check internet connection
- Verify date is a market trading day (Mon-Fri, no holidays)
- NSE servers may be down - try again later

### Database Connection Error
- Verify PostgreSQL is running
- Check credentials in configuration
- Ensure database `BhavCopy_Database` exists

### Greeks Calculation Errors
- Requires `py_vollib` library: `pip install py_vollib`
- Some options may have 0 Greeks if calculation fails (normal)
- Check if dates are weekends (data not available)

### Screener Cache Empty
- Ensure Greeks calculation completed successfully
- Check if derived tables have data
- May need to wait for full pipeline to complete

---

## 📊 Dashboard Pre-Calculation Metrics (`precalculate_data.py`)

This script calculates all dashboard metrics by comparing Call and Put options at the same strike. It processes two trading days to compute trends and identify directional bias.

### **Location**: `Database/precalculate_data.py`

### **Main Function**: `calculate_and_store_data(curr_date, prev_date)` - Lines 107-227

This function calculates all metrics by:
1. Fetching current day data (CE & PE options)
2. Fetching previous day data (CE & PE options)
3. Merging on TckrSymb and StrkPric (matching calls and puts at same strike)
4. Computing delta_chg, vega_chg, tradval_chg, and money_chg metrics

---

### **1. Delta Change (delta_chg) - Line 163**

**Definition**: Difference in Delta between Call and Put at the SAME STRIKE on the SAME DAY (NOT comparing to previous day).

**Formula:**
$$\Delta_{\text{change}} = \Delta_{\text{Call}} - \Delta_{\text{Put}}$$

**Code Location** (line 163 in `precalculate_data.py`):
```python
dm['delta_chg'] = dm['delta_c'] - dm['delta_p']
```

**Where:**
- `dm` = DataFrame merged on TckrSymb and StrkPric
- Suffix `_c` = Call option Greeks (current day)
- Suffix `_p` = Put option Greeks (current day)
- BOTH from same trading day

**Interpretation:**
- **Positive delta_chg**: Call Delta > Put Delta (bullish bias)
- **Negative delta_chg**: Put Delta > Call Delta (bearish bias)
- **Larger absolute value**: Stronger directional signal

**Example:**
- Strike 20000:
  - Call Delta = 0.65, Put Delta = -0.35
  - Delta Change = 0.65 - (-0.35) = +1.00 (strong bullish bias)

**Aggregation**: Reports max/min delta_chg strikes and sum across all strikes

**Dashboard Use**: Identifies which strikes show strongest directional commitment from traders

---

### **2. Vega Change (vega_chg) - Line 164**

**Definition**: Difference in Vega between Call and Put at the SAME STRIKE on the SAME DAY.

**Formula:**
$$\text{Vega}_{\text{change}} = \text{Vega}_{\text{Call}} - \text{Vega}_{\text{Put}}$$

**Code Location** (line 164 in `precalculate_data.py`):
```python
dm['vega_chg'] = dm['vega_c'] - dm['vega_p']
```

**Interpretation:**
- **Positive vega_chg**: Call Vega > Put Vega (calls more sensitive to IV)
- **Negative vega_chg**: Put Vega > Call Vega (puts more sensitive to IV)
- Shows volatility exposure differences

**Example:**
- Strike 20150:
  - Call Vega = 12.5, Put Vega = 12.3
  - Vega Change = 12.5 - 12.3 = +0.2 (slight bullish volatility bias)

**Dashboard Use**: Identifies which strikes have strongest IV exposure from traders

---

### **3. Trading Value Change (tradval_chg / total_tradval) - Line 165**

**Definition**: Difference in notional value (OI × LTP) between Call and Put at SAME STRIKE on SAME DAY.

**Formula:**
$$\text{TradVal}_{\text{change}} = (\text{OI}_{\text{Call}} \times \text{LTP}_{\text{Call}}) - (\text{OI}_{\text{Put}} \times \text{LTP}_{\text{Put}})$$

**Code Location** (line 165 in `precalculate_data.py`):
```python
dm['tradval_chg'] = (dm['OpnIntrst_c'] * dm['LastPric_c']) - (dm['OpnIntrst_p'] * dm['LastPric_p'])
```

**Interpretation:**
- **Positive tradval_chg**: Call notional > Put notional (bullish positioning)
- **Negative tradval_chg**: Put notional > Call notional (bearish positioning)
- **Magnitude**: Strength of one-sided positioning

**Example:**
- Strike 20000:
  - Call: OI = 50,000, LTP = ₹125 → Notional = ₹6,250,000
  - Put: OI = 45,000, LTP = ₹120 → Notional = ₹5,400,000
  - TradVal Change = 6,250,000 - 5,400,000 = +₹850,000 (calls more active)

**Aggregation**: `total_tradval = sum(tradval_chg)` across all strikes

**Dashboard Use**: Identifies total directional trading commitment

---

### **4. Moneyness Change (money_chg / total_money) - Lines 166-168**

**Definition**: Change in strike moneyness between consecutive trading days. Measures how strikes move in/out of money as underlying price changes.

**Formula:**
$$\text{Moneyness}_{\text{current}} = \frac{S_{\text{current}} - K}{S_{\text{current}}}$$

$$\text{Moneyness}_{\text{previous}} = \frac{S_{\text{previous}} - K}{S_{\text{previous}}}$$

$$\text{Money Change} = \text{Moneyness}_{\text{current}} - \text{Moneyness}_{\text{previous}}$$

**Code Location** (lines 166-168 in `precalculate_data.py`):
```python
dm['moneyness_curr'] = (dm['UndrlygPric_c'] - dm['StrkPric']) / dm['UndrlygPric_c']
dm['moneyness_prev'] = (dm['UndrlygPric_p'] - dm['StrkPric']) / dm['UndrlygPric_p']
dm['money_chg'] = dm['moneyness_curr'] - dm['moneyness_prev']
```

**Where:**
- S_current = Underlying price on current day
- S_previous = Underlying price on previous day (from merged put data)
- K = Strike price

**Interpretation:**
- **Positive money_chg**: Strike moved towards ITM (moneyness improved)
- **Negative money_chg**: Strike moved towards OTM (moneyness worsened)
- **Ratio range**: -1 to +1 (normalized by spot price)

**Example:**
- Strike 19500:
  - Current: Spot = ₹20000 → Moneyness = (20000 - 19500) / 20000 = +0.025 (2.5% ITM)
  - Previous: Spot = ₹19800 → Moneyness = (19800 - 19500) / 19800 = +0.0152 (1.52% ITM)
  - Money Change = 0.025 - 0.0152 = +0.0098 (improved 0.98%)

**Aggregation**: `total_money = sum(money_chg)` across all strikes

**Dashboard Use**: Identifies how underlying move affects strike attractiveness

---

### **5. RSI(14) - Relative Strength Index**

**Definition**: Momentum indicator measuring magnitude of recent price changes to evaluate overbought/oversold conditions.

**Formula:**
$$\text{RSI} = 100 - \frac{100}{1 + \text{RS}}$$

$$\text{RS} = \frac{\text{Average Gain (14 periods)}}{\text{Average Loss (14 periods)}}$$

**Calculation Method:**
```
Step 1: Calculate price changes
   Change = Close_today - Close_yesterday

Step 2: Separate gains and losses
   Gains = All positive changes, Losses = All negative changes (absolute value)

Step 3: Calculate exponential moving averages (EMA)
   Avg Gain = EMA(14) of gains
   Avg Loss = EMA(14) of losses

Step 4: Calculate RS (Relative Strength)
   RS = Avg Gain / Avg Loss

Step 5: Calculate RSI
   RSI = 100 - (100 / (1 + RS))
```

**Code Location** (lines 69-70 in `calculate_rsi_from_database()` function, lines 36-92):
```python
# Calculate RSI(14) using pandas_ta from database close prices
rsi_series = ta.rsi(df['ClsPric'], length=rsi_period)
current_rsi = rsi_series.iloc[-1] if not rsi_series.empty else None
```

**Implementation Details:**
- Uses 14-period RSI (standard)
- Data source: `ClsPric` column from derived tables
- Minimum 50 data points required for accuracy
- Function: `calculate_rsi_from_database(table_name, current_date, all_dates, rsi_period=14)`

**Interpretation:**
- **RSI < 30**: Oversold (potential bounce/reversal expected)
- **RSI 30-70**: Neutral zone
- **RSI > 70**: Overbought (potential pullback expected)
- **Divergence**: RSI trend vs price trend = potential reversal signal

**Example:**
```
Date    Close   Change  Gain    Loss
Day 1   100     -       -       -
Day 2   102     +2      2       0
Day 3   104     +2      2       0
Day 4   101     -3      0       3
...
Day 14  103     +1      1       0

Avg Gain (14) = 1.2
Avg Loss (14) = 0.8
RS = 1.2 / 0.8 = 1.5
RSI = 100 - (100 / (1 + 1.5)) = 100 - 40 = 60 (neutral to overbought)
```

**Dashboard Use:** Identifies momentum extremes for contrarian plays

---

### **6. Processing Filters and Conditions**

The script processes data through multiple filters:

**Filter 1: Option Type (lines 104-105)**
```python
dfc = df[df['BizDt_str'] == curr_date].copy()
dfp = df[df['BizDt_str'] == prev_date].copy()
```
- Only uses data where both current and previous date exist

**Filter 2: Valid Greeks (line 128)**
```python
dv = dm[(dm['delta_c'].notna())&(dm['delta_p'].notna())&(dm['delta_c']!=0)&(dm['delta_p']!=0)].copy()
```
- Excludes options with missing or zero Greeks

**Filter 3: Moneyness Classification (lines 164-165)**
```python
# For CALLS: OTM if strike > spot, ITM if strike < spot
if opt_type == 'CE':
    cond = dv['strike_diff_c'] < 0  # OTM
    cond = dv['strike_diff_c'] > 0  # ITM
```

**Filter 4: Aggregation by Metric (lines 152-163)**
```python
# For each metric (Delta, Vega):
#   - Find positive changes (bulls/positive view)
#   - Find negative changes (bears/negative view)
#   - Track strike with maximum change and percentage
```

---

### **7. Database Storage Structure**

**Table**: `options_dashboard_cache`
- **Columns**:
  - `biz_date`: Current trading date
  - `prev_date`: Previous trading date
  - `moneyness_type`: "TOTAL", "OTM", or "ITM"
  - `data_json`: All metrics in JSON format for each stock

**Data Structure per Row**:
```json
{
  "stock": "NIFTY",
  "closing_price": 20150.5,
  "rsi": 62.45,
  "call_delta_pos_strike": "20200",
  "call_delta_pos_pct": "0.15",
  "call_delta_neg_strike": "20000",
  "call_delta_neg_pct": "-0.08",
  "call_vega_pos_strike": "20150",
  "call_vega_pos_pct": "2.50",
  "call_vega_neg_strike": "20050",
  "call_vega_neg_pct": "-1.80",
  "call_total_tradval": 12500000,
  "call_total_money": 0.025,
  "put_delta_pos_strike": "20100",
  "put_delta_pos_pct": "0.12",
  "put_delta_neg_strike": "19950",
  "put_delta_neg_pct": "-0.10",
  "put_vega_pos_strike": "20150",
  "put_vega_pos_pct": "2.45",
  "put_vega_neg_strike": "20050",
  "put_vega_neg_pct": "-1.75",
  "put_total_tradval": 11800000,
  "put_total_money": 0.018
}
```

---

### **8. Key Processing Functions Summary**

| Function | Lines | Purpose | Inputs | Outputs |
|----------|-------|---------|--------|---------|
| `calculate_rsi_from_database()` | 36-92 | Calculate RSI(14) from DB | table_name, date, all_dates | RSI value (0-100) |
| `get_available_dates()` | 94-99 | Fetch all dates with data | None | List of dates (desc order) |
| `get_prev_date()` | 101-105 | Get previous trading day | current_date, dates_list | Previous date string |
| `calculate_and_store_data()` | 107-227 | Main calculation engine | curr_date, prev_date | total_data, otm_data, itm_data |
| `create_precalculated_tables()` | 229-240 | Create cache table | None | Creates DB table |
| `precalculate_all_dates()` | 242-308 | Process all NEW dates | None | Inserts/updates cache |

---

### **9. Calculation Flow Diagram**

```
Raw Option Data (TBL_*_DERIVED)
    ↓
Filter by Date (Current & Previous)
    ↓
Merge Call & Put by Strike
    ↓
Calculate Changes:
  - Delta Change
  - Vega Change
  - TradVal Change
  - Moneyness Change
    ↓
Calculate RSI(14)
    ↓
Aggregate by Filters:
  - Total (all strikes)
  - OTM (out of money)
  - ITM (in the money)
    ↓
Find Extremes:
  - Max/Min changes per metric
  - Strikes with largest moves
    ↓
Store in JSON → Database
    ↓
Frontend Dashboard (instant load <0.5s)
```

---

## � Screener Cache Metrics (`screener_cache.py`)

This script pre-calculates screener filtering data for instant results. Results are sorted and ranked by metric changes (gainers/losers).

### **Location**: `Database/screener_cache.py`

### **Main Function**: `calculate_screener_data_for_date(selected_date, all_dates)` - Lines 108-410

Pre-calculates and caches screener data for ONE trading day by:
1. Fetching current and previous day data
2. Calculating OI %, Moneyness %, and IV change
3. Sorting by metric and ranking gainers/losers
4. Storing in `screener_cache` table

---

### **1. Open Interest (OI) Change - Percentage**

**Definition**: Percentage change in total Open Interest between consecutive trading days.

**Formula:**
$$\text{OI}_{\%\text{change}} = \frac{\text{OI}_{\text{current}} - \text{OI}_{\text{previous}}}{\text{OI}_{\text{previous}}} \times 100$$

**Code Location** (lines 273-278 in `screener_cache.py`):
```python
# For all options (ALL), ITM, and OTM combined
total_curr_oi_all = df_opt['current_oi'].sum()
total_prev_oi_all = df_opt['prev_oi'].sum()
oi_all = ((total_curr_oi_all - total_prev_oi_all) / total_prev_oi_all * 100)
         if total_prev_oi_all != 0 else 0
```

**Interpretation:**
- **Positive OI %**: OI increased (accumulation, more contracts opened)
- **Negative OI %**: OI decreased (distribution, contracts closed)
- **Large positive**: Strong buildup in that option type/moneyness
- **Large negative**: Strong unwinding, likely exit signals

**Example:**
- Previous Day: Total OI = 1,000,000 contracts
- Current Day: Total OI = 1,050,000 contracts
- OI Change % = (1,050,000 - 1,000,000) / 1,000,000 × 100 = +5% (bullish accumulation)

**Filters**: Calculated for:
- **Option Type**: CE (calls), PE (puts), FUT (futures)
- **Moneyness**: ALL, ITM, OTM

**Aggregation**: Top 10 gainers + Top 10 losers per filter

**Dashboard Use**: Identifies strongest OI buildup (buy signals) and unwinding (sell signals)

---

### **2. Moneyness Change - Percentage (Value-Based)**

**Definition**: Percentage change in total notional value (OI × Price) of an option chain between days.

**Formula:**
$$\text{Moneyness}_{\%\text{change}} = \frac{\text{Value}_{\text{current}} - \text{Value}_{\text{previous}}}{\text{Value}_{\text{previous}}} \times 100$$

$$\text{Where: Value} = \text{OI} \times \text{LTP}$$

**Code Location** (lines 281-307 in `screener_cache.py`):
```python
# Calculate total notional value (OI × LTP) for current and previous
df_opt['curr_value'] = df_opt['current_oi'] * df_opt['current_ltp']
df_opt['prev_value'] = df_opt['prev_oi'] * df_opt['prev_ltp']
total_curr_value_all = df_opt['curr_value'].sum()
total_prev_value_all = df_opt['prev_value'].sum()
money_all = ((total_curr_value_all - total_prev_value_all) / total_prev_value_all * 100)
            if total_prev_value_all != 0 else 0
```

**Interpretation:**
- **Positive moneyness %**: Total option value increased (stronger positioning)
- **Negative moneyness %**: Total option value decreased (weaker positioning)
- **Difference from OI %**: When moneyness % > OI % → premium increased (bullish)
- **Difference from OI %**: When moneyness % < OI % → premium decreased (bearish)

**Example:**
- Previous: OI = 1M contracts, Avg Price = ₹100 → Total Value = ₹100M
- Current: OI = 1.05M contracts, Avg Price = ₹105 → Total Value = ₹110.25M
- Moneyness Change % = (110.25 - 100) / 100 × 100 = +10.25% (strong bullish)

**Filters**: Calculated for:
- **Option Type**: CE (calls), PE (puts), FUT (futures)
- **Moneyness**: ALL, ITM, OTM

**Aggregation**: Top 10 gainers + Top 10 losers per filter

**Dashboard Use**: Identifies strongest combined position and price moves (volatility signals)

---

### **3. Implied Volatility (IV) Change - Weighted Average**

**Definition**: Weighted average percentage change in Implied Volatility, weighted by Open Interest (larger positions count more).

**Formula:**
$$\text{IV}_{\text{weighted\_change}} = \frac{\sum (\text{IV}_{\%\text{change}} \times \text{OI}_{\text{current}})}{\sum \text{OI}_{\text{current}}}$$

$$\text{IV}_{\%\text{change}} = \frac{\text{IV}_{\text{current}} - \text{IV}_{\text{previous}}}{\text{IV}_{\text{previous}}} \times 100$$

**Code Location** (lines 310-318 in `screener_cache.py`):
```python
# Calculate IV percentage change from database query (lines 139-143)
CASE
    WHEN COALESCE(p.iv_prev, 0) != 0
    THEN ((c.iv_curr - COALESCE(p.iv_prev, c.iv_curr)) / COALESCE(p.iv_prev, c.iv_curr)) * 100
    ELSE 0
END AS iv_change

# Weight by OI
if df_opt['current_oi'].sum() != 0:
    iv_all = (df_opt['iv_change'] * df_opt['current_oi']).sum() / df_opt['current_oi'].sum()
else:
    iv_all = df_opt['iv_change'].mean()
```

**Interpretation:**
- **Positive IV %**: IV increased (fear/uncertainty increased, buying premium)
- **Negative IV %**: IV decreased (confidence increased, selling premium)
- **Weighted by OI**: Positions with more contracts have more influence
- **Large positive**: Significant volatility spike in this option series
- **Large negative**: Volatility crush in this option series

**Example:**
- CE Options:
  - 20000 Strike: OI = 500K, IV was 20%, now 22% → IV change = +10%
  - 20100 Strike: OI = 400K, IV was 18%, now 19% → IV change = +5.56%
  - Weighted IV = (10% × 500K + 5.56% × 400K) / 900K = +7.98%

**Filters**: Calculated for:
- **Option Type**: CE (calls), PE (puts), FUT (futures)
- **Moneyness**: ALL, ITM, OTM

**Aggregation**: Top 10 gainers + Top 10 losers per filter

**Dashboard Use**: Identifies volatility spikes (fear), crush (confidence), and trading opportunities

---

### **4. Screener Cache Database Table**

**Table**: `screener_cache` - Lines 35-47 in `screener_cache.py`

**Columns**:
| Column | Type | Purpose |
|--------|------|---------|
| id | SERIAL | Primary key |
| cache_date | DATE | Trading date of the data |
| metric_type | VARCHAR(50) | "oi", "moneyness", or "iv" |
| option_type | VARCHAR(10) | "CE", "PE", or "FUT" |
| moneyness_filter | VARCHAR(10) | "ALL", "ITM", "OTM", "ALL_LOSERS", "ITM_LOSERS", etc. |
| rank | INT | 1-10 (gainer) or loser position |
| ticker | VARCHAR(50) | Stock name (NIFTY, BANKNIFTY, etc.) |
| underlying_price | NUMERIC | Underlying asset price |
| change | NUMERIC | % change (OI, moneyness, or IV) |
| created_at | TIMESTAMP | When row was inserted |

**Indices**:
```sql
CREATE INDEX idx_screener_cache_date_metric
    ON screener_cache(cache_date, metric_type, option_type, moneyness_filter);
CREATE INDEX idx_screener_cache_date
    ON screener_cache(cache_date);
```

**Sample Query Result**:
```
cache_date | metric_type | option_type | moneyness_filter | rank | ticker | underlying_price | change
2025-11-12 | oi          | CE          | ALL              | 1    | NIFTY  | 24500.50         | 8.35
2025-11-12 | oi          | CE          | ALL              | 2    | BANKNIFTY | 49800.00       | 6.12
2025-11-12 | oi          | CE          | ALL_LOSERS       | 1    | FINNIFTY | 22100.25        | -5.40
```

---

### **5. Screener Data Ranking Logic**

**Gainers (Top 10)**:
- Sorted descending by metric change (highest positive first)
- Represents strongest bullish signals for that metric

**Losers (Top 10)**:
- Sorted ascending by metric change (lowest negative first)
- Represents strongest bearish signals for that metric

**Example OI Gainers for CE Options (ALL)**:
1. NIFTY: +8.35% (strong call accumulation)
2. BANKNIFTY: +6.12%
3. FINNIFTY: +5.80%
...

**Example Moneyness Losers for PE Options (ITM)**:
1. NIFTY: -12.50% (strong put unwinding)
2. BANKNIFTY: -10.80%
3. FINNIFTY: -8.90%
...

---

### **6. Main Function Execution**

**Function**: `precalculate_screener_cache()` - Lines 412-506

**Execution Flow**:
1. Creates screener_cache table if not exists
2. Fetches all trading dates with data
3. Identifies NEW dates not yet cached
4. For each new date:
   - Calls `calculate_screener_data_for_date()`
   - Gets back list of ranked cache rows
   - Inserts ~200-400 rows per date (10 ranks × 3 metrics × 3 option types × 3 moneyness filters)
5. Returns total rows inserted

**Performance**:
- Screener page loads in <0.5 seconds (vs 10+ without cache)
- Processes 1 new date in ~2-5 seconds

---

## �📦 Dependencies

Required Python packages:
```
pandas
sqlalchemy
psycopg2-binary
numpy
py_vollib
pandas_ta  (optional, for RSI calculation)
```

Install all:
```bash
pip install pandas sqlalchemy psycopg2-binary numpy py_vollib pandas_ta
```

---

## 📞 Related Directories

- **`Analysis_Tools/`** - Flask web application that consumes this database
- **`Analysis_Tools2/`** - Alternative dashboard server
- **`spot_data/`** - Spot price data storage

---

## 📅 Schedule Recommendation

Run daily after market close (4 PM IST):
```bash
# Windows Task Scheduler or cron job
python C:\Users\Admin\Desktop\Derivative_Analysis\Database\update_database.py
```

This ensures:
- Latest market data is loaded
- Greeks are calculated accurately
- Dashboard and screener show current data
- All users see consistent, fresh information

---

## 📋 Logs

Check script output for:
- ✅ Successful operations
- ⚠️ Warnings (holidays, missing data)
- ❌ Errors (connection issues, data problems)

Output format includes emojis and progress indicators for easy visual debugging.
