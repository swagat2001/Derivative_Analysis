# NSE Options Analysis Dashboard

## 📋 Overview
A comprehensive options analysis dashboard for NSE (National Stock Exchange) derivatives data with real-time calculations, hybrid RSI from TradingView/Database, and interactive visualizations.

---

## 🏗️ System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    NSE OPTIONS ANALYSIS SYSTEM                   │
└─────────────────────────────────────────────────────────────────┘

┌──────────────────┐         ┌──────────────────┐
│   NSE Website    │────────▶│  update_database │
│   (BhavCopy)     │         │      .py         │
└──────────────────┘         └────────┬─────────┘
                                      │
                                      ▼
                            ┌──────────────────┐
                            │   PostgreSQL     │
                            │    Database      │
                            │  (BhavCopy_DB)   │
                            └────────┬─────────┘
                                     │
                    ┌────────────────┴────────────────┐
                    │                                  │
                    ▼                                  ▼
        ┌──────────────────┐              ┌──────────────────┐
        │   TradingView    │              │   Database       │
        │   (tvDatafeed)   │              │   (TA-Lib RSI)   │
        │  RSI + Price     │              │   Fallback       │
        └────────┬─────────┘              └────────┬─────────┘
                 │                                  │
                 └────────────┬─────────────────────┘
                              ▼
                    ┌──────────────────┐
                    │ update_dashboard │
                    │      .py         │
                    │  (Hybrid RSI)    │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │  Cache Table     │
                    │ (Pre-calculated) │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ dashboard_server │
                    │      .py         │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │  Web Dashboard   │
                    │  (Port 5000)     │
                    └──────────────────┘
```

---

## 📁 Project Structure

```
BhavCopy Backup2/
├── Database/
│   ├── update_database.py          # Fetches NSE BhavCopy data
│   └── [Other utility scripts]
│
├── Analysis_Tools/
│   ├── update_dashboard.py         # Calculates metrics + Hybrid RSI
│   ├── dashboard_server.py         # Flask server for web dashboard
│   ├── cleanup_unused_files.py     # Cleanup utility
│   │
│   ├── static/
│   │   ├── script.js               # Frontend JavaScript
│   │   └── style.css               # Dashboard styling
│   │
│   └── templates/
│       └── index.html              # Dashboard HTML template
│
└── README.md                        # This file
```

---

## 🚀 Quick Start

### Prerequisites
```bash
# Required Python packages
pip install sqlalchemy pandas psycopg2-binary flask

# Optional (for TradingView RSI)
pip install tvDatafeed

# Optional (for database RSI fallback)
pip install TA-Lib
```

### Daily Workflow

```bash
# Step 1: Update raw NSE data (Run once daily after market close)
cd "C:\Users\Admin\Desktop\BhavCopy Backup2\Database"
python update_database.py

# Step 2: Calculate dashboard metrics (Run after Step 1)
cd "C:\Users\Admin\Desktop\BhavCopy Backup2\Analysis_Tools"
python update_dashboard.py

# Step 3: Start web dashboard
python dashboard_server.py
# Open browser: http://localhost:5000
```

---

## 🔧 Configuration

### Database Configuration
Edit in both `update_database.py` and `update_dashboard.py`:
```python
db_user = 'postgres'
db_password = 'your_password'
db_host = 'localhost'
db_port = '5432'
db_name = 'BhavCopy_Database'
```

### TradingView Ticker Mapping
Edit in `update_dashboard.py`:
```python
TICKER_MAPPING = {
    'NIFTY': 'NIFTY',
    'BANKNIFTY': 'BANKNIFTY',
    'FINNIFTY': 'FINNIFTY',
    '360ONE': '360ONE',
    # Add more tickers as needed
}
```

---

## 📊 Features

### 1. Hybrid RSI Calculation
- **Primary**: Fetches RSI from TradingView using tvDatafeed
- **Fallback**: Calculates RSI from database using TA-Lib
- **Period**: 14-day RSI (Wilder's smoothing method)
- **Display**: Color-coded (Green: <30, Red: >70, Purple: 30-70)

### 2. Options Metrics
- **Delta Changes**: Positive/Negative for CALL/PUT
- **Vega Changes**: Positive/Negative for CALL/PUT
- **PCR**: Put-Call Ratio (Volume & OI)
- **Moneyness**: Strike price relative to underlying
- **Trading Value**: Total traded value changes

### 3. Three Views
- **TOTAL**: All strikes combined
- **OTM**: Out-of-the-money options only
- **ITM**: In-the-money options only

### 4. Interactive Charts
- **Historical Data**: 40-day history
- **Two-Panel Layout**: Main chart (75%) + RSI panel (25%)
- **Synchronized Crosshairs**: Both panels move together
- **Multiple Indicators**: PCR, Underlying Price, Vega/Moneyness, RSI

### 5. Search & Export
- **Real-time Search**: Filter by ticker name
- **Excel Export**: Export any tab to Excel
- **Responsive Design**: Works on desktop and mobile

---

## 🗄️ Database Schema

### Raw Data Tables (Created by update_database.py)
```sql
TBL_[TICKER]_DERIVED
├── BizDt              DATE          # Business date
├── TckrSymb           VARCHAR       # Ticker symbol
├── StrkPric           DECIMAL       # Strike price
├── OptnTp             VARCHAR       # CE/PE
├── UndrlygPric        DECIMAL       # Underlying price
├── delta              DECIMAL       # Greeks: Delta
├── vega               DECIMAL       # Greeks: Vega
├── strike_diff        DECIMAL       # Strike - Underlying
├── TtlTrfVal          DECIMAL       # Total traded value
├── OpnIntrst          DECIMAL       # Open interest
├── LastPric           DECIMAL       # Last price
└── ClsPric            DECIMAL       # Closing price
```

### Cache Table (Created by update_dashboard.py)
```sql
options_dashboard_cache
├── id                 SERIAL        # Primary key
├── biz_date           DATE          # Current date
├── prev_date          DATE          # Previous trading date
├── moneyness_type     VARCHAR(10)   # TOTAL/OTM/ITM
├── data_json          TEXT          # Cached calculations (JSON)
└── created_at         TIMESTAMP     # Cache timestamp
```

---

## 🔍 Data Flow

### 1. Data Collection (update_database.py)
```
NSE BhavCopy → Download → Parse → Calculate Greeks → Store in PostgreSQL
```

### 2. Dashboard Calculation (update_dashboard.py)
```
PostgreSQL → Calculate Metrics → Try TradingView RSI → Fallback to DB RSI → Cache Results
```

### 3. Web Display (dashboard_server.py)
```
Cache Table → Flask API → Frontend JavaScript → Interactive Dashboard
```

---

## 🎨 UI Components

### Main Dashboard
- Header with date range
- Three tabs (TOTAL, OTM, ITM)
- Search box
- Export button
- Data table with color coding

### Chart Modal
- Main chart panel (PCR, Price, Vega/Moneyness)
- RSI panel (separate, synchronized)
- Legend with toggles
- Responsive design

---

## 🔐 Security Notes

- Database credentials stored in plain text (consider environment variables)
- No authentication on web dashboard (runs on localhost)
- TradingView scraping may violate TOS (use at own risk)

---

## 🐛 Troubleshooting

### Problem: No RSI values
**Solution**: Install tvDatafeed or TA-Lib
```bash
pip install tvDatafeed
pip install TA-Lib
```

### Problem: Dashboard shows no data
**Solution**: Run update scripts in order
```bash
python update_database.py    # First
python update_dashboard.py   # Second
python dashboard_server.py   # Third
```

### Problem: TradingView rate limit
**Solution**: System automatically falls back to database RSI

### Problem: Charts not loading
**Solution**: Check browser console (F12) for errors

---

## 📈 Performance

- **Database Query Time**: <2 seconds per date
- **TradingView Fetch**: ~5-10 seconds per ticker (with rate limits)
- **Dashboard Load**: <1 second (from cache)
- **Chart Rendering**: <500ms

---

## 🔄 Update Schedule

### Daily Updates
```bash
# Run after market close (typically after 4:00 PM IST)
python update_database.py      # Updates raw data
python update_dashboard.py     # Calculates metrics
```

### Continuous Usage
```bash
# Keep running during trading hours
python dashboard_server.py     # Web dashboard
```

---

## 📝 License

This project is for educational and personal use only. NSE data usage subject to NSE terms. TradingView scraping may violate their TOS.

---

## 🤝 Support

For issues or questions:
1. Check troubleshooting section
2. Review log files
3. Verify database connectivity
4. Check Python package versions

---

## 📚 Additional Resources

- [NSE India](https://www.nseindia.com/)
- [TradingView](https://www.tradingview.com/)
- [TA-Lib Documentation](https://mrjbq7.github.io/ta-lib/)
- [Flask Documentation](https://flask.palletsprojects.com/)

---

## 🔮 Future Enhancements

- [ ] Authentication system
- [ ] Real-time data updates
- [ ] Mobile app
- [ ] Email alerts
- [ ] More technical indicators
- [ ] Portfolio tracking
- [ ] Backtesting module

---

**Version**: 3.0  
**Last Updated**: 2024  
**Maintained By**: Options Analysis Team
