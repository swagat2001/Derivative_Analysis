# NSE Options Analysis Dashboard

A comprehensive, production-ready financial analysis platform for F&O (Futures & Options) trading in the Indian market. This sophisticated web application provides real-time options chain analysis, technical indicators, market screeners, and advanced derivatives analytics.

![Python](https://img.shields.io/badge/Python-3.13+-blue.svg)
![Flask](https://img.shields.io/badge/Flask-3.0.0-green.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue.svg)
![License](https://img.shields.io/badge/License-Private-red.svg)

## 🚀 Features

### 📊 **Options Chain Dashboard**
- Real-time options chain analysis with complete Greeks (Delta, Gamma, Vega, Theta, Rho, IV)
- Black-Scholes model calculations using py-vollib
- Moneyness analysis (ITM/OTM/ATM strikes)
- Open Interest (OI) tracking and changes
- Server-side DataTables with pagination, sorting, filtering
- Excel export functionality

### 🔍 **Advanced Screeners (6 Types)**
1. **Top Gainers/Losers**: 40 comprehensive tables covering:
   - OI Gainers/Losers (Call/Put, ITM/OTM)
   - Moneyness Gainers/Losers (Call/Put, ITM/OTM)
   - IV Gainers/Losers (Call/Put, ITM/OTM)
   - Futures OI analysis
   - Final signals (BULLISH/BEARISH/NEUTRAL)

2. **Signal Scanner**: RSI-based signal detection with:
   - Pivot levels (PP, R1-R3, S1-S3)
   - Volume profile (POC, VAH, VAL)
   - RSI signals (Oversold/Overbought/Divergence)
   - High volume & OI spike detection

3. **Technical Indicators Screener**:
   - RSI analysis (6 categories)
   - MACD analysis (Bullish/Bearish crossovers)
   - SMA analysis (200-day moving average)
   - ADX analysis (Trend strength)
   - Golden/Death crossover detection

4. **Futures OI Analysis**: Expiry-wise analysis (CME, NME, FME)
5. **Index Screener**: Index-specific analysis
6. **Signal Analysis**: Comprehensive signal generation

### 📈 **Market Insights Dashboard**
- Interactive heatmap visualization with index filtering
- FII/DII activity tracking
- Delivery data analysis
- Market statistics (advances/declines/breadth)
- 52-week high/low analysis
- Volume breakout detection
- Sector performance analysis

### 📰 **News & Announcements**
- Corporate announcements from BSE
- Market-wide announcements
- Upcoming results calendar

### 🔐 **Authentication & Security**
- Session-based authentication system
- User registration with validation
- Protected routes and middleware
- Default admin user creation

## 🏗️ Architecture

### **Technology Stack**
- **Backend**: Python 3.13, Flask 3.0 (MVC Architecture)
- **Database**: PostgreSQL 16 with SQLAlchemy ORM
- **Caching**: Redis 7 + Flask-Caching
- **Frontend**: HTML5, CSS3, JavaScript, Bootstrap
- **Containerization**: Docker + Docker Compose
- **Reverse Proxy**: Nginx (production)

### **Project Structure**
```
Derivative_Analysis/
├── Analysis_Tools/           # Flask web application
│   ├── app/
│   │   ├── controllers/      # Route handlers & API endpoints
│   │   │   ├── screener/     # Screener modules (6 types)
│   │   │   ├── insights/     # Market insights
│   │   │   └── *.py          # Core controllers
│   │   ├── models/           # Data models & database queries
│   │   ├── views/            # HTML templates (Jinja2)
│   │   │   ├── components/   # Reusable components
│   │   │   ├── screener/     # Screener templates
│   │   │   └── *.html        # Main templates
│   │   └── static/           # CSS, JS, images
│   │       ├── css/          # Responsive stylesheets
│   │       ├── js/           # Client-side JavaScript
│   │       └── image/        # Static assets
│   └── __init__.py           # Flask app factory
├── Database/                 # Database utilities & scripts
│   ├── FO/                   # F&O data processing
│   │   ├── fo_update_database.py    # Master data pipeline
│   │   ├── precalculate_data.py     # Cache pre-calculation
│   │   └── *.py              # Data processors
│   └── Cash/                 # Cash market data
├── Data_scraper/             # Fundamental data scraping
│   ├── batchScraper.py       # Batch scraper for Screener.in
│   └── screenerScraper.py    # Screener.in API integration
├── spot_data/                # Market data files
├── logs/                     # Application logs
├── docker-compose.yml        # Multi-container setup
├── Dockerfile               # Application container
├── requirements.txt         # Python dependencies
├── run.py                   # Application entry point
└── setup-dev.sh            # Development setup script
```

## 🗄️ Database Schema

### **Primary Databases**
- **BhavCopy_Database**: F&O (Futures & Options) data
- **CashStocks_Database**: Cash/Equity data

### **Table Structure**
- **Base Tables**: `TBL_<SYMBOL>` (raw NSE data)
- **Derived Tables**: `TBL_<SYMBOL>_DERIVED` (with Greeks calculations)
- **Cache Tables**:
  - `screener_cache`: Pre-calculated screener data
  - `options_dashboard_cache`: Pre-calculated dashboard data
  - `futures_oi_cache`: Futures OI analysis
  - `technical_screener_cache`: Technical indicators
  - `index_constituents_cache`: Index membership

### **Key Data Points**
- **Options Data**: Strike prices, expiry dates, option types, OI, volume, prices
- **Greeks**: Delta, Gamma, Vega, Theta, Rho, Implied Volatility
- **Technical Indicators**: RSI, MACD, SMA, ADX, Bollinger Bands
- **Market Data**: FII/DII activity, delivery data, sector performance

## 📊 Data Processing Pipeline

### **Complete Data Pipeline** (`fo_update_database.py`)
1. **CSV Download**: NSE BhavCopy files from archives.nseindia.com
2. **Database Upload**: Parse and insert into PostgreSQL
3. **Greeks Calculation**: Black-Scholes model using py-vollib
4. **Derived Tables**: Create tables with all Greeks calculations
5. **Cache Pre-calculation**: Screener, dashboard, technical data
6. **Index Constituents**: Fetch from NSE for instant loading

### **Fundamental Data Scraping** (`Data_scraper/`)
- **Screener.in Integration**: Quarterly reports, P&L, Balance sheet, Cash flow, Ratios
- **BSE Integration**: Corporate announcements, upcoming results
- **Batch Processing**: Automated scraping for all stocks

## 🚀 Installation & Setup

### **Prerequisites**
- Python 3.13+
- PostgreSQL 16
- Git
- Docker (optional)

### **Quick Start (Development)**
```bash
# Clone repository
git clone <repository-url>
cd Derivative_Analysis

# Run automated setup
chmod +x setup-dev.sh
./setup-dev.sh

# Edit environment variables
cp .env.template .env
# Edit .env with your database credentials

# Run application
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows
python run.py
```

### **Docker Setup (Recommended)**
```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f web

# Stop services
docker-compose down
```

### **🔐 Security Setup (CRITICAL)**

**IMPORTANT**: Before running the application, you MUST configure your environment securely:

#### **Step 1: Configure Environment Variables**
```bash
# 1. Copy the template file
cp .env.template .env

# 2. Edit .env and replace ALL placeholder values:
#    - DB_PASSWORD: Your PostgreSQL password
#    - APP_SECRET_KEY: Generate with: python -c "import secrets; print(secrets.token_hex(32))"
#    - UPSTOX_ACCESS_TOKEN: Your Upstox API token (if using live data)

# 3. Verify .env is NOT tracked by git
git status  # .env should NOT appear in the list
```

#### **Step 2: Verify Git Ignore**
```bash
# Ensure .env is in .gitignore
grep -q "^\.env$" .gitignore || echo ".env" >> .gitignore

# NEVER commit .env to version control
git check-ignore .env  # Should output: .env
```

#### **Step 3: Install Pre-commit Hooks (Recommended)**
```bash
# Install pre-commit
pip install pre-commit

# Install hooks
pre-commit install

# Test hooks
pre-commit run --all-files
```

> **⚠️ CRITICAL SECURITY WARNINGS:**
> - `.env.template` contains PLACEHOLDER values only - never real credentials
> - `.env` contains REAL credentials - never commit to git
> - Always use strong, unique passwords for production
> - Rotate credentials regularly
> - Use environment-specific `.env` files (dev, staging, prod)

### **Manual Installation**
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Setup database (PostgreSQL)
createdb BhavCopy_Database
createdb CashStocks_Database

# Run application
python run.py
```

## 🔧 Configuration

### **Environment Variables** (`.env`)
```bash
# Flask Configuration
FLASK_ENV=production
SECRET_KEY=your-secret-key-here

# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=BhavCopy_Database
DB_USER=postgres
DB_PASSWORD=your-password

# Server Configuration
SERVER_HOST=0.0.0.0
SERVER_PORT=5000

# Feature Flags
ENABLE_PDF_EXPORT=True
ENABLE_ANALYTICS=True
```

### **Database Configuration**
Update credentials in `Analysis_Tools/app/models/db_config.py`:
```python
db_user = "postgres"
db_password = "your-password"
db_host = "localhost"
db_port = "5432"
db_name = "BhavCopy_Database"
```

## 🌐 API Endpoints

### **Core APIs**
- `GET /` - Home page with live indices
- `GET /dashboard` - Options chain dashboard
- `GET /stock/<symbol>` - Individual stock analysis
- `GET /screener` - Screener landing page
- `GET /insights` - Market insights dashboard

### **Data APIs**
- `GET /api/dashboard_data` - Server-side DataTables data
- `GET /api/live-indices` - Real-time indices data
- `GET /api/heatmap` - Market heatmap data
- `GET /api/fii-dii` - FII/DII activity
- `GET /api/historical-chart-data/<symbol>` - 40-day historical data
- `POST /api/scan` - Signal scanner execution
- `GET /export` - Excel export functionality

### **Authentication APIs**
- `POST /login` - User authentication
- `POST /signup` - User registration
- `GET /logout` - User logout

## 🔍 Usage Examples

### **Dashboard Analysis**
```python
# Access options chain for NIFTY
GET /dashboard?symbol=NIFTY&date=2024-01-30&mtype=TOTAL

# Filter by moneyness
GET /dashboard?symbol=BANKNIFTY&moneyness=ITM&sort=iv&dir=desc
```

### **Signal Scanner**
```python
# Scan for RSI signals
POST /api/scan
{
    "rsi_oversold": true,
    "min_volume": 1000000,
    "min_oi": 500000
}
```

### **Market Insights**
```python
# Get heatmap data
GET /api/heatmap?index=NIFTY&metric=price_change

# FII/DII activity
GET /api/fii-dii?days=30
```

## 🚀 Deployment

### **Production Deployment**
```bash
# Build and deploy
chmod +x deploy.sh
./deploy.sh prod

# Using Docker (recommended)
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

### **Health Monitoring**
- Health Check: `GET /health`
- Application logs: `logs/app.log`
- Database monitoring via PgAdmin: `http://localhost:5050`

## 📈 Performance Optimizations

- **Caching**: Flask-Caching with 1-hour TTL for screener data
- **Pre-calculation**: Dashboard and screener data pre-calculated daily
- **Connection Pooling**: SQLAlchemy with pool_size=10, max_overflow=20
- **Indexed Tables**: Optimized queries on BizDt, TckrSymb, StrkPric
- **Client-side DataTables**: Pagination and sorting on frontend
- **Lazy Loading**: Charts and data loaded on demand

## 🔒 Security Features

- Session-based authentication with secure cookies
- CSRF protection on forms
- SQL injection prevention via SQLAlchemy ORM
- Input validation and sanitization
- Environment variable configuration
- Docker security best practices

## 📊 Key Dependencies

```
Flask==3.0.0              # Web framework
SQLAlchemy==2.0.0          # Database ORM
pandas==2.1.0              # Data processing
py-vollib==1.0.1           # Options Greeks calculation
scipy==1.11.0              # Numerical computing
pandas-ta==0.3.14b         # Technical analysis
psycopg2-binary==2.9.9     # PostgreSQL adapter
Flask-Caching              # Performance caching
reportlab                  # PDF generation
matplotlib==3.8.0          # Chart generation
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Make changes and test thoroughly
4. Run pre-commit hooks: `pre-commit run --all-files`
5. Commit changes: `git commit -am 'Add new feature'`
6. Push to branch: `git push origin feature/my-feature`
7. Submit a Pull Request

## 📝 License

**Private - All Rights Reserved**

This is proprietary software. Unauthorized copying, distribution, or modification is strictly prohibited.

## 📞 Support

For technical support or feature requests, please contact the development team.

---

**Built with ❤️ for the Indian derivatives trading community**
