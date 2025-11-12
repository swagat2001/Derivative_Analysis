# Quick Reference: Line Number Corrections

## precalculate_data.py - CORRECTED LINE NUMBERS

### Metrics Calculations

| Metric | Variable Name | PREVIOUS (WRONG) | CURRENT (CORRECT) | Code |
|--------|---------------|-----------------|-------------------|------|
| Delta Change | delta_chg | Line 118 | **Line 163** | `dm['delta_chg'] = dm['delta_c'] - dm['delta_p']` |
| Vega Change | vega_chg | Line 119 | **Line 164** | `dm['vega_chg'] = dm['vega_c'] - dm['vega_p']` |
| Trading Value | tradval_chg | Line 120 | **Line 165** | `dm['tradval_chg'] = (dm['OpnIntrst_c'] * dm['LastPric_c']) - ...` |
| Moneyness Ratio | moneyness_curr | N/A | **Line 166** | `dm['moneyness_curr'] = (dm['UndrlygPric_c'] - dm['StrkPric']) / dm['UndrlygPric_c']` |
| Moneyness Ratio | moneyness_prev | N/A | **Line 167** | `dm['moneyness_prev'] = (dm['UndrlygPric_p'] - dm['StrkPric']) / dm['UndrlygPric_p']` |
| Moneyness Change | money_chg | N/A | **Line 168** | `dm['money_chg'] = dm['moneyness_curr'] - dm['moneyness_prev']` |

### Key Functions

| Function | Purpose | Lines |
|----------|---------|-------|
| `calculate_rsi_from_database()` | RSI(14) calculation from DB | 36-92 |
| `get_available_dates()` | Fetch all dates with data | 94-99 |
| `get_prev_date()` | Get previous trading day | 101-105 |
| `calculate_and_store_data()` | Main calculation engine | 107-227 |
| `create_precalculated_tables()` | Create cache table | 229-240 |
| `precalculate_all_dates()` | Process all new dates | 242-308 |

---

## screener_cache.py - DOCUMENTED (Previously Missing)

### Key Calculations

| Metric | Purpose | Lines | Formula |
|--------|---------|-------|---------|
| OI % Change | Open Interest percentage change | 273-278 | `((OI_curr - OI_prev) / OI_prev) × 100` |
| Moneyness % Change | Total notional value % change | 281-307 | `((Value_curr - Value_prev) / Value_prev) × 100` |
| IV Weighted Change | Weighted average IV change | 310-318 | `Σ(IV_change × OI) / Σ(OI)` |

### Key Functions

| Function | Purpose | Lines |
|----------|---------|-------|
| `create_screener_cache_table()` | Create cache table with indices | 24-47 |
| `get_prev_date()` | Get previous trading date | 50-63 |
| `get_all_tables()` | Fetch all derived tables | 65-71 |
| `calculate_screener_data_for_date()` | Main calculation engine | 108-410 |
| `precalculate_screener_cache()` | Process all new dates | 412-506 |

### Database Table Structure

**Table**: `screener_cache` (Lines 35-47)

**Columns** (8 total):
```
id               SERIAL PRIMARY KEY
cache_date       DATE (trading date)
metric_type      VARCHAR (oi, moneyness, iv)
option_type      VARCHAR (CE, PE, FUT)
moneyness_filter VARCHAR (ALL, ITM, OTM, *_LOSERS)
rank             INT (1-10)
ticker           VARCHAR (NIFTY, BANKNIFTY, etc.)
underlying_price NUMERIC
change           NUMERIC (% change)
created_at       TIMESTAMP
```

**Indices**:
- `idx_screener_cache_date_metric` (Lines 43-44)
- `idx_screener_cache_date` (Line 45)

---

## Dashboard Cache Table (precalculate_data.py)

**Table**: `options_dashboard_cache` (Lines 262-267)

**Columns** (5 total):
```
id               SERIAL PRIMARY KEY
biz_date         DATE
prev_date        DATE
moneyness_type   VARCHAR (TOTAL, OTM, ITM)
data_json        TEXT
created_at       TIMESTAMP
```

---

## Key Concept Corrections

### WRONG (Previous Understanding)
❌ delta_chg compares current day vs previous day  
❌ vega_chg compares current day vs previous day  
❌ money_chg is the same as tradval_chg  
❌ Screener cache metrics aren't documented  

### CORRECT (After Verification)
✅ delta_chg compares Call Delta vs Put Delta at SAME strike, SAME day  
✅ vega_chg compares Call Vega vs Put Vega at SAME strike, SAME day  
✅ money_chg is moneyness RATIO change comparing across days  
✅ Screener cache calculates OI %, Moneyness %, IV % weighted by OI  

