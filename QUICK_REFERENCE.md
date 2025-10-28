# QUICK REFERENCE GUIDE

## \ud83d\ude80 Daily Workflow

```bash
# 1. Update NSE Data (After market close)
cd "C:\Users\Admin\Desktop\BhavCopy Backup2\Database"
python update_database.py

# 2. Calculate Dashboard (After step 1)
cd "C:\Users\Admin\Desktop\BhavCopy Backup2\Analysis_Tools"
python update_dashboard.py

# 3. Start Dashboard (View results)
python dashboard_server.py
# Open: http://localhost:5000
```

---

## \ud83d\udcc1 File Locations

```
Database/
  \u2514\u2500 update_database.py       \u2192 Fetches NSE data

Analysis_Tools/
  \u251c\u2500 update_dashboard.py      \u2192 Calculates metrics + RSI
  \u251c\u2500 dashboard_server.py      \u2192 Web server
  \u251c\u2500 static/script.js         \u2192 Frontend logic
  \u2514\u2500 templates/index.html     \u2192 Dashboard HTML
```

---

## \u2699\ufe0f Configuration

### Database (Both files)
```python
db_user = 'postgres'
db_password = 'Gallop@3104'
db_name = 'BhavCopy_Database'
```

### TradingView Tickers (update_dashboard.py)
```python
TICKER_MAPPING = {
    'NIFTY': 'NIFTY',
    'BANKNIFTY': 'BANKNIFTY',
    # Add more here
}
```

---

## \ud83d\udd0d Key Functions

### update_dashboard.py

**`fetch_tradingview_data(ticker, date)`**
- Returns: `(price, rsi)` or `(None, None)`
- Source: TradingView

**`calculate_db_rsi(table, date, dates)`**
- Returns: `rsi` or `None`
- Source: Database (TA-Lib)

**`calculate_data(curr_date, prev_date)`**
- Returns: `(total, otm, itm)` lists
- Calculates ALL metrics

---

## \ud83d\udcca Data Returned

### Each Ticker Object
```json
{
  "stock": "NIFTY",
  "closing_price": 23450.50,
  "rsi": 54.32,
  "call_delta_pos_strike": "23500",
  "call_delta_pos_pct": "12.50",
  "call_delta_neg_strike": "23400",
  "call_delta_neg_pct": "-8.30",
  "call_vega_pos_strike": "23500",
  "call_vega_pos_pct": "5.20",
  "call_vega_neg_strike": "23450",
  "call_vega_neg_pct": "-3.10",
  "call_total_tradval": 125000000.50,
  "call_total_money": 0.025,
  "put_delta_pos_strike": "23400",
  "put_delta_pos_pct": "15.20",
  "put_delta_neg_strike": "23500",
  "put_delta_neg_pct": "-10.50",
  "put_vega_pos_strike": "23400",
  "put_vega_pos_pct": "4.80",
  "put_vega_neg_strike": "23500",
  "put_vega_neg_pct": "-2.90",
  "put_total_tradval": 98000000.25,
  "put_total_money": -0.018
}
```

---

## \ud83c\udfaf Hybrid RSI Logic

```
\u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510
\u2502 Try TradingView    \u2502
\u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u252c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518
         \u2502
    Success? \u2500\u2500\u2500 YES \u2500\u2192 Use TV RSI
         \u2502
         NO
         \u2502
\u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2534\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510
\u2502 Try Database (TA-Lib) \u2502
\u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u252c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518
         \u2502
    Success? \u2500\u2500\u2500 YES \u2500\u2192 Use DB RSI
         \u2502
         NO
         \u2502
        RSI = None
```

---

## \ud83d\udca1 Tips

### Performance
- Run `update_database.py` off-peak hours
- TradingView may rate-limit (falls back to DB)
- Cache updates only NEW dates

### Troubleshooting
```bash
# No RSI values?
pip install tvDatafeed
pip install TA-Lib

# Dashboard empty?
# Run scripts in order: 1, 2, 3

# Charts not loading?
# Check browser console (F12)
```

---

## \ud83d\udcdd API Endpoints

```
GET /
  \u2192 Dashboard HTML

GET /get_data?date=YYYY-MM-DD
  \u2192 JSON: {curr_date, prev_date, total[], otm[], itm[]}

GET /get_historical_data?ticker=X&date=Y&type=Z&metric=M
  \u2192 JSON: {ticker, data[40 days]}
```

---

## \ud83d\udd22 Calculations

**Delta Change** = delta_current - delta_previous  
**Vega Change** = vega_current - vega_previous  
**Moneyness** = (underlying - strike) / underlying  
**RSI** = 100 - (100 / (1 + RS))  
**PCR** = put_volume / call_volume  

---

## \ud83c\udfa8 Color Codes

**RSI:**
- < 30: Green (Oversold)
- 30-70: Purple (Neutral)  
- \> 70: Red (Overbought)

**Values:**
- Positive: Green
- Negative: Red

---

## \u26a0\ufe0f Important Notes

1. **TradingView scraping may violate TOS**
2. **Always has database fallback**
3. **Only RSI + Price from TradingView**
4. **All other metrics from database**
5. **Run update_database.py first**

---

**For Full Documentation**: See README.md and ARCHITECTURE.md
