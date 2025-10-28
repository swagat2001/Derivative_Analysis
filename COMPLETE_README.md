# Options Analysis Dashboard - Complete Documentation

## Project Overview
This is a comprehensive Options Trading Analysis Dashboard that downloads NSE F&O data, calculates Greeks, analyzes option chain changes, and displays interactive web dashboard with historical charts.

## Project Structure
```
BhavCopy Backup2/
├── Database/                   # Data pipeline scripts
├── Analysis_Tools/             # Dashboard & analysis
├── Reports/                    # Generated outputs
└── BhavCopy_Database.sql      # PostgreSQL schema
```

## Database Tables

### Base Tables (Raw NSE Data)
- `TBL_NIFTY`, `TBL_BANKNIFTY`, `TBL_RELIANCE`, etc.
- Source: NSE BhavCopy CSV files
- Created by: `Database/update_database.py`

### Derived Tables (With Greeks)
- `TBL_NIFTY_DERIVED`, `TBL_BANKNIFTY_DERIVED`, etc.
- Additional columns: delta, vega, theta, iv
- Created by: `Database/update_database.py`

### Cache Table (Dashboard Data)
- `options_dashboard_cache`
- Stores: TOTAL/OTM/ITM pre-calculated metrics
- Created by: `Analysis_Tools/precalculate_data.py`

## Data Flow & Sources

### NSE CSV Fields (Raw Data)
| Field | Description | Find in Code |
|-------|-------------|--------------|
| BizDt | Business Date | `grep -n "BizDt" Database/update_database.py` |
| StrkPric | Strike Price | `grep -n "StrkPric" Database/update_database.py` |
| OptnTp | Option Type (CE/PE) | `grep -n "OptnTp" Database/update_database.py` |
| UndrlygPric | Underlying Price | `grep -n "UndrlygPric" Database/update_database.py` |
| OpnIntrst | Open Interest | `grep -n "OpnIntrst" Database/update_database.py` |
| TtlTradgVol | Total Volume | `grep -n "TtlTradgVol" Database/update_database.py` |
| LastPric | Last Price | `grep -n "LastPric" Database/update_database.py` |

### Calculated Greeks (Derived Tables)
| Field | Calculation | Find in Code |
|-------|-------------|--------------|
| delta | Black-Scholes | `grep -n "def greeks" Database/update_database.py` |
| vega | Black-Scholes | `grep -n "vega" Database/update_database.py` |
| theta | Black-Scholes | `grep -n "theta" Database/update_database.py` |
| iv | Implied Volatility | `grep -n "implied_volatility" Database/update_database.py` |
| strike_diff | UndrlygPric - StrkPric | `grep -n "strike_diff" Database/update_database.py` |

### Dashboard Metrics (Cache Table)
| Metric | Formula | Find in Code |
|--------|---------|--------------|
| Delta Change | delta_current - delta_previous | `grep -n "delta_chg" Analysis_Tools/precalculate_data.py` |
| Vega Change | vega_current - vega_previous | `grep -n "vega_chg" Analysis_Tools/precalculate_data.py` |
| Trading Value Change | (OI×Price)_curr - (OI×Price)_prev | `grep -n "tradval_chg" Analysis_Tools/precalculate_data.py` |
| Moneyness Change | moneyness_curr - moneyness_prev | `grep -n "money_chg" Analysis_Tools/precalculate_data.py` |
| Closing Price | UndrlygPric (first row) | `grep -n "closing_price" Analysis_Tools/precalculate_data.py` |

### Historical Chart Data
| Metric | Formula | Find in Code |
|--------|---------|--------------|
| PCR (Volume) | Put Volume / Call Volume | `grep -n "pcr_volume" Analysis_Tools/dashboard_server.py` |
| PCR (OI) | Put OI / Call OI | `grep -n "pcr_oi" Analysis_Tools/dashboard_server.py` |
| Average Vega | AVG(vega) for option type | `grep -n "avg_vega" Analysis_Tools/dashboard_server.py` |

## Key Formulas with Line Numbers

### precalculate_data.py
```python
# Line 86-88: Delta & Vega Changes
delta_chg = delta_current - delta_previous
vega_chg = vega_current - vega_previous

# Line 89: Trading Value Change
tradval_chg = (OpnIntrst_c × LastPric_c) - (OpnIntrst_p × LastPric_p)

# Line 90-92: Moneyness Change
moneyness_curr = (UndrlygPric_c - StrkPric) / UndrlygPric_c
moneyness_prev = (UndrlygPric_p - StrkPric) / UndrlygPric_p
money_chg = moneyness_curr - moneyness_prev

# Line 67: Closing Price
closing_price = UndrlygPric[0]
```

### dashboard_server.py
```python
# Line 119-120: PCR Calculations
pcr_volume = put_volume / call_volume
pcr_oi = put_oi / call_oi

# Line 121: Average Vega
avg_vega = AVG(vega WHERE OptnTp = option_type)
```

### update_database.py
```python
# Line 250-260: Greeks Calculation
def greeks(premium, expiry, cd, asset_price, strike_price, intrest_rate, instrument_type):
    t = (expiry - cd) / 365
    imp_v = implied_volatility(...)
    delta = delta(...)
    vega = vega(...)
    theta = theta(...)
```

## Usage Guide

### Daily Workflow
```bash
# Step 1: Update database
cd Database
python update_database.py

# Step 2: Start dashboard
cd ../Analysis_Tools
python dashboard_server.py
```

### Manual Pre-calculation
```bash
cd Analysis_Tools
python precalculate_data.py
```

## Dashboard Features

### Three Tabs
1. **TOTAL** - All strikes
2. **OTM** - Out of The Money
3. **ITM** - In The Money

### Clickable Cells
- **Vega % cells** → Chart with PCR + Average Vega (40 days)
- **ΔMoney cells** → Chart with PCR + Moneyness (40 days)

## Quick Search Commands

```bash
# Find UndrlygPric usage
grep -rn "UndrlygPric" Analysis_Tools/

# Find delta calculation
grep -rn "def greeks" Database/

# Find PCR calculation
grep -rn "pcr_volume" Analysis_Tools/

# Find closing price
grep -rn "closing_price =" Analysis_Tools/

# Find moneyness calculation
grep -rn "moneyness_curr" Analysis_Tools/
```

## Configuration

### Database Settings
```python
db_user = 'postgres'
db_password = 'Gallop@3104'
db_host = 'localhost'
db_port = '5432'
db_name = 'BhavCopy_Database'
```
Find in code: `grep -rn "db_password" .`

### Data Paths
```python
save_fo_eod = "C:/NSE_EOD_FO"
```
Find in code: `grep -n "save_fo_eod" Database/update_database.py`

## Troubleshooting

### No dates found
```bash
python Database/update_database.py
```

### Cache table empty
```bash
python Analysis_Tools/precalculate_data.py
```

### Charts not loading
Check browser console (F12) for errors

## Key Files

| File | Purpose | Run When |
|------|---------|----------|
| `Database/update_database.py` | Master pipeline | Daily |
| `Analysis_Tools/precalculate_data.py` | Pre-calculate | After DB update |
| `Analysis_Tools/dashboard_server.py` | Web dashboard | View data |

## Trace Data Flow

1. NSE CSV → Base Tables (`update_database.py` Step 2)
2. Base Tables → Derived Tables (`update_database.py` Step 3)
3. Derived Tables → Cache Table (`precalculate_data.py`)
4. Cache Table → Dashboard (`dashboard_server.py`)

**Version:** 4.0  
**Last Updated:** October 2025
