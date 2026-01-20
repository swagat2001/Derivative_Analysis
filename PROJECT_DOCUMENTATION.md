# NSE Options Analysis Dashboard - Complete Project Documentation

## 📋 Project Overview

The **NSE Options Analysis Dashboard** is a comprehensive Flask-based web application for analyzing NSE (National Stock Exchange) derivatives data. It provides real-time options analysis, technical indicators, screeners, and interactive visualizations for traders and investors.

### Key Features
- **Real-time Options Analysis** with Greeks calculation (Delta, Gamma, Vega, Theta, Rho)
- **40+ Screener Categories** for OI, IV, and Moneyness analysis
- **Technical Indicators** with RSI, MACD, SMA, ADX analysis
- **Interactive Charts** with historical data visualization
- **PDF Export** functionality with professional reports
- **Authentication System** with user management
- **Responsive Design** with modern UI/UX

---

## 🏗️ System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    NSE OPTIONS ANALYSIS SYSTEM                   │
└─────────────────────────────────────────────────────────────────┘

┌──────────────────┐         ┌──────────────────┐
│   NSE Website    │────────▶│  fo_update_      │
│   (BhavCopy)     │         │  database.py     │
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
        │   Cache Tables   │              │   Derived Tables │
        │  (Pre-calculated)│              │   (Greeks Data)  │
        └────────┬─────────┘              └────────┬─────────┘
                 │                                  │
                 └────────────┬─────────────────────┘
                              ▼
                    ┌──────────────────┐
                    │ Flask Web App    │
                    │   (Port 5000)    │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │  Web Dashboard   │
                    │  (Browser UI)    │
                    └──────────────────┘
```

---

## 📁 Project Structure

```
NSE_Options_Analysis/
├── Analysis_Tools/                    # Main Flask Application
│   ├── app/
│   │   ├── __init__.py               # Flask app factory
│   │   ├── health_check.py           # Health monitoring
│   │   ├── controllers/              # MVC Controllers
│   │   │   ├── auth_controller.py    # Authentication
│   │   │   ├── dashboard_controller.py # Main dashboard
│   │   │   ├── screener_controller.py # Screener pages
│   │   │   ├── stock_controller.py   # Stock details
│   │   │   └── screener/             # Specialized screeners
│   │   │       ├── futures_oi/       # Futures OI analysis
│   │   │       ├── index_screener/   # Index constituents
│   │   │       ├── signal_analysis/  # Bull/Bear signals
│   │   │       ├── technical_screener/ # Technical indicators
│   │   │       └── top_gainers_losers/ # Market movers
│   │   ├── models/                   # MVC Models
│   │   │   ├── db_config.py         # Database configuration
│   │   │   ├── dashboard_model.py   # Dashboard data logic
│   │   │   ├── screener_model.py    # Screener data logic
│   │   │   ├── stock_model.py       # Stock data logic
│   │   │   └── auth_model.py        # User authentication
│   │   ├── views/                   # HTML Templates
│   │   │   ├── base.html           # Base template
│   │   │   ├── home.html           # Landing page
│   │   │   ├── dashboard.html      # Main dashboard
│   │   │   ├── screener/           # Screener templates
│   │   │   ├── stock_detail/       # Stock detail pages
│   │   │   └── components/         # Reusable components
│   │   └── static/                 # Static Assets
│   │       ├── css/               # Stylesheets
│   │       ├── js/                # JavaScript files
│   │       └── image/             # Images and icons
├── Database/                       # Data Pipeline
│   ├── FO/                        # F&O Data Processing
│   │   ├── fo_update_database.py  # Main data pipeline
│   │   ├── screener_cache.py      # Screener data caching
│   │   ├── futures_oi_cache.py    # Futures OI caching
│   │   ├── technical_screener_cache.py # Technical data
│   │   └── precalculate_data.py   # Dashboard data caching
│   └── Cash/                      # Cash market data
├── spot_data/                     # Live market data
├── logs/                          # Application logs
├── run.py                         # Application entry point
├── requirements.txt               # Python dependencies
├── docker-compose.yml             # Docker configuration
├── Dockerfile                     # Docker image
└── .env.template                  # Environment variables
```

---

## 🔧 Technology Stack

### Backend
- **Framework**: Flask 3.0+ (Python web framework)
- **Database**: PostgreSQL 16 (Primary data storage)
- **ORM**: SQLAlchemy 2.0+ (Database abstraction)
- **Caching**: Flask-Caching (In-memory caching)
- **Authentication**: Flask sessions (User management)

### Frontend
- **Templates**: Jinja2 (Server-side rendering)
- **CSS Framework**: Custom CSS with modern design
- **JavaScript**: Vanilla JS + Chart.js (Interactive charts)
- **Charts**: LightweightCharts (Financial charts)
- **Icons**: Custom SVG icons

### Data Processing
- **Options Pricing**: py-vollib (Black-Scholes Greeks)
- **Technical Analysis**: pandas-ta (RSI, MACD, SMA, ADX)
- **Data Manipulation**: pandas + numpy
- **PDF Generation**: Playwright + PyPDF2

### Infrastructure
- **Containerization**: Docker + Docker Compose
- **Web Server**: Gunicorn (Production)
- **Reverse Proxy**: Nginx (Production)
- **Monitoring**: Health check endpoints

---

## 🗄️ Database Schema

### Core Tables Structure

#### Base Tables (Raw NSE Data)
```sql
TBL_{TICKER} (
    "BizDt" DATE,                    -- Business date
    "Sgmt" VARCHAR(50),              -- Segment
    "FinInstrmTp" VARCHAR(50),       -- Instrument type
    "TckrSymb" VARCHAR(50),          -- Ticker symbol
    "FininstrmActlXpryDt" DATE,      -- Expiry date
    "StrkPric" VARCHAR(50),          -- Strike price
    "OptnTp" VARCHAR(50),            -- Option type (CE/PE)
    "UndrlygPric" VARCHAR(50),       -- Underlying price
    "OpnIntrst" VARCHAR(50),         -- Open interest
    "TtlTradgVol" VARCHAR(50),       -- Trading volume
    "TtlTrfVal" VARCHAR(50),         -- Traded value
    "LastPric" VARCHAR(50),          -- Last price
    "ClsPric" VARCHAR(50)            -- Closing price
);
```

#### Derived Tables (Calculated Greeks)
```sql
TBL_{TICKER}_DERIVED (
    -- All columns from base table +
    "strike_diff" NUMERIC,           -- Strike - Underlying
    "delta" NUMERIC,                 -- Delta Greek
    "gamma" NUMERIC,                 -- Gamma Greek
    "vega" NUMERIC,                  -- Vega Greek
    "theta" NUMERIC,                 -- Theta Greek
    "rho" NUMERIC,                   -- Rho Greek
    "iv" NUMERIC                     -- Implied Volatility
);
```

#### Cache Tables (Pre-calculated Data)

**Dashboard Cache**
```sql
options_dashboard_cache (
    id SERIAL PRIMARY KEY,
    biz_date DATE NOT NULL,
    prev_date DATE NOT NULL,
    moneyness_type VARCHAR(10),      -- TOTAL/OTM/ITM
    data_json TEXT,                  -- Cached calculations
    created_at TIMESTAMP
);
```

**Screener Cache**
```sql
screener_cache (
    id SERIAL PRIMARY KEY,
    cache_date DATE NOT NULL,
    metric_type VARCHAR(50),         -- oi/moneyness/iv
    option_type VARCHAR(10),         -- CE/PE/FUT
    moneyness_filter VARCHAR(50),    -- ALL/ITM/OTM/ALL_LOSERS
    rank INT,
    ticker VARCHAR(50),
    strike_price NUMERIC,
    underlying_price NUMERIC,
    change NUMERIC,
    bullish_count INT,
    bearish_count INT,
    final_signal VARCHAR(10)         -- BULLISH/BEARISH
);
```

**Technical Screener Cache**
```sql
technical_screener_cache (
    id SERIAL PRIMARY KEY,
    cache_date DATE NOT NULL,
    ticker VARCHAR(50),
    underlying_price NUMERIC,
    rsi_14 NUMERIC,
    macd NUMERIC,
    macd_signal NUMERIC,
    sma_50 NUMERIC,
    sma_200 NUMERIC,
    bb_upper NUMERIC,
    bb_lower NUMERIC,
    adx_14 NUMERIC,
    above_200_sma BOOLEAN,
    below_200_sma BOOLEAN
);
```

**Futures OI Cache**
```sql
futures_oi_cache (
    id SERIAL PRIMARY KEY,
    cache_date DATE NOT NULL,
    ticker VARCHAR(50),
    underlying_price NUMERIC,
    expiry_type VARCHAR(10),         -- CME/NME/FME
    expiry_date DATE,
    expiry_price NUMERIC,
    expiry_oi NUMERIC,
    expiry_oi_change NUMERIC,
    oi_percentile NUMERIC,
    price_percentile NUMERIC
);
```

---

## 🔄 Data Flow Pipeline

### 1. Data Collection (`fo_update_database.py`)
```python
# Daily automated process
NSE BhavCopy Download → CSV Parsing → PostgreSQL Upload → Greeks Calculation
```

**Process Steps:**
1. **Download**: Fetch latest BhavCopy ZIP files from NSE
2. **Extract**: Unzip and rename CSV files
3. **Upload**: Insert raw data into base tables (`TBL_{TICKER}`)
4. **Calculate**: Compute Greeks using Black-Scholes model
5. **Store**: Save calculated data to derived tables (`TBL_{TICKER}_DERIVED`)

### 2. Data Caching (`screener_cache.py`, `precalculate_data.py`)
```python
# Pre-calculation for fast web response
Raw Data → Analysis → Cache Tables → Web API (< 1 second response)
```

**Cache Types:**
- **Dashboard Cache**: Pre-calculated delta/vega changes for TOTAL/OTM/ITM views
- **Screener Cache**: Top 10 gainers/losers for 40+ categories
- **Technical Cache**: RSI, MACD, SMA indicators for all stocks
- **Futures Cache**: OI analysis for current/next/far month expiries

### 3. Web Application (`Flask Controllers`)
```python
# Real-time web serving
Cache Tables → Flask API → JSON Response → Interactive UI
```

---

## 🎯 Key Features Deep Dive

### 1. Dashboard Analysis
**Location**: `Analysis_Tools/app/controllers/dashboard_controller.py`

**Features:**
- **Real-time Data**: Live spot prices from CSV feed
- **Options Metrics**: Delta/Vega changes with strike-wise breakdown
- **Three Views**: TOTAL (all strikes), OTM (out-of-money), ITM (in-the-money)
- **Interactive Charts**: 40-day historical data with RSI overlay
- **Export**: Excel export with formatted data

**Key Metrics:**
```python
# Delta Analysis
call_delta_pos_strike    # Highest positive delta strike
call_delta_pos_pct       # Percentage change
call_delta_neg_strike    # Highest negative delta strike
call_delta_neg_pct       # Percentage change

# Vega Analysis  
call_vega_pos_strike     # Highest positive vega strike
call_vega_pos_pct        # Percentage change
call_vega_neg_strike     # Highest negative vega strike
call_vega_neg_pct        # Percentage change

# Trading Metrics
call_total_tradval       # Total traded value change
call_total_money         # Total moneyness change
closing_price            # Current underlying price
rsi                      # 14-period RSI
```

### 2. Screener System
**Location**: `Analysis_Tools/app/controllers/screener_controller.py`

**40 Categories Structure:**
```
OI Analysis (12 categories):
├── Call OI: ALL, ITM, OTM (Gainers + Losers)
└── Put OI: ALL, ITM, OTM (Gainers + Losers)

Moneyness Analysis (12 categories):
├── Call Moneyness: ALL, ITM, OTM (Gainers + Losers)  
└── Put Moneyness: ALL, ITM, OTM (Gainers + Losers)

IV Analysis (12 categories):
├── Call IV: ALL, ITM, OTM (Gainers + Losers)
└── Put IV: ALL, ITM, OTM (Gainers + Losers)

Futures Analysis (4 categories):
├── Future OI: Gainers + Losers
└── Future Moneyness: Gainers + Losers
```

**Signal Classification:**
```python
# Bullish Signals (18 categories)
BULLISH_CATEGORIES = [
    "CALL_OI_GAINERS_*",      # Call OI increase = Bullish
    "PUT_OI_LOSERS_*",        # Put OI decrease = Bullish  
    "CALL_IV_GAINERS_*",      # Call IV increase = Bullish
    "PUT_IV_LOSERS_*",        # Put IV decrease = Bullish
    "CALL_MONEYNESS_GAINERS_*", # Call money increase = Bullish
    "PUT_MONEYNESS_LOSERS_*",   # Put money decrease = Bullish
    "FUT_OI_GAINERS_*"        # Future OI increase = Bullish
]

# Bearish Signals (18 categories)  
BEARISH_CATEGORIES = [
    "CALL_OI_LOSERS_*",       # Call OI decrease = Bearish
    "PUT_OI_GAINERS_*",       # Put OI increase = Bearish
    "CALL_IV_LOSERS_*",       # Call IV decrease = Bearish
    "PUT_IV_GAINERS_*",       # Put IV increase = Bearish
    "CALL_MONEYNESS_LOSERS_*", # Call money decrease = Bearish
    "PUT_MONEYNESS_GAINERS_*", # Put money increase = Bearish
    "FUT_OI_LOSERS_*"         # Future OI decrease = Bearish
]

# Final Signal Calculation
final_signal = "BULLISH" if bullish_count > bearish_count else "BEARISH"
```

### 3. Technical Analysis
**Location**: `Analysis_Tools/app/controllers/screener/technical_screener/`

**Indicators Calculated:**
```python
# RSI (Relative Strength Index)
rsi_14 = ta.rsi(close_prices, length=14)

# MACD (Moving Average Convergence Divergence)
macd_line, macd_signal, macd_histogram = ta.macd(close_prices)

# SMA (Simple Moving Average)
sma_50 = ta.sma(close_prices, length=50)
sma_200 = ta.sma(close_prices, length=200)

# Bollinger Bands
bb_upper, bb_middle, bb_lower = ta.bbands(close_prices)

# ADX (Average Directional Index)
adx_14 = ta.adx(high, low, close, length=14)
```

**Heatmap Categories:**
- **RSI Analysis**: Overbought (>80), Strong Bullish (60-80), Mild Bullish (50-60), Mild Bearish (40-50), Strong Bearish (20-40), Oversold (<20)
- **MACD Analysis**: Bullish Crossover (MACD > Signal), Bearish Crossover (MACD < Signal)
- **SMA Analysis**: Above 200 SMA (Bullish), Below 200 SMA (Bearish)
- **ADX Analysis**: Strong Trend (>25), Weak Trend (<25)

### 4. PDF Export System
**Location**: `Analysis_Tools/app/controllers/screener_controller.py` (create_screener_pdf)

**PDF Structure:**
1. **Cover Page**: Professional letterhead with Goldmine branding
2. **40 Data Tables**: All screener categories with top 10 stocks each
3. **Signal Analysis**: Final bullish/bearish classification table
4. **Technical Heatmap**: RSI, MACD, SMA, ADX analysis
5. **Disclaimer**: Legal and data source information

**Generation Process:**
```python
# 1. HTML Template Preparation
cover_html = load_template("screener_cover_a4.html")
tables_html = load_template("screener_table_pages.html")

# 2. Data Injection
for category in screener_categories:
    tables_html = tables_html.replace(f"{{{category}}}", generate_table_html(data))

# 3. PDF Rendering (Playwright)
browser = playwright.chromium.launch()
cover_pdf = page.pdf(cover_html)
tables_pdf = page.pdf(tables_html)

# 4. PDF Merging (PyPDF2)
merger = PdfMerger()
merger.append(cover_pdf)
merger.append(tables_pdf)
final_pdf = merger.write()
```

---

## 🚀 Installation & Setup

### Prerequisites
- Python 3.9+
- PostgreSQL 12+
- Node.js 16+ (for frontend build tools)
- Git

### 1. Clone Repository
```bash
git clone <repository-url>
cd NSE_Options_Analysis
```

### 2. Environment Setup
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt
```

### 3. Database Setup
```bash
# Create PostgreSQL database
createdb BhavCopy_Database

# Configure environment variables
cp .env.template .env
# Edit .env with your database credentials
```

### 4. Initial Data Load
```bash
# Run data pipeline (downloads NSE data)
cd Database/FO
python fo_update_database.py

# This will:
# 1. Download BhavCopy data from NSE
# 2. Upload to PostgreSQL
# 3. Calculate Greeks
# 4. Pre-calculate cache tables
```

### 5. Run Application
```bash
# Development mode
python run.py

# Production mode (Docker)
docker-compose up -d
```

### 6. Access Application
- **Web Interface**: http://localhost:5000
- **Health Check**: http://localhost:5000/health
- **Default Login**: admin/admin (created automatically)

---

## 🔧 Configuration

### Environment Variables (`.env`)
```bash
# Flask Configuration
FLASK_APP=run.py
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
WORKERS=4

# Paths
SPOT_DATA_PATH=C:\path\to\spot_data
BACKUP_DIR=C:\path\to\backups
EXCEL_FILTER_PATH=C:\path\to\stock_list.xlsx

# Feature Flags
ENABLE_PDF_EXPORT=True
ENABLE_ANALYTICS=True
```

### Database Configuration (`Analysis_Tools/app/models/db_config.py`)
```python
# Connection pooling for high performance
engine = create_engine(
    f"postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}",
    pool_size=10,           # Base connections
    max_overflow=20,        # Additional connections
    pool_pre_ping=True,     # Validate connections
    pool_recycle=3600,      # Recycle after 1 hour
    echo=False              # SQL logging (set True for debug)
)
```

---

## 📊 Performance Optimization

### 1. Database Optimization
```sql
-- Indexes for fast queries
CREATE INDEX idx_dashboard_cache_dates ON options_dashboard_cache(biz_date, moneyness_type);
CREATE INDEX idx_screener_cache_lookup ON screener_cache(cache_date, metric_type, option_type);
CREATE INDEX idx_technical_cache_date ON technical_screener_cache(cache_date);
CREATE INDEX idx_futures_cache_date ON futures_oi_cache(cache_date);

-- Partitioning for large tables (optional)
CREATE TABLE TBL_NIFTY_DERIVED_2024 PARTITION OF TBL_NIFTY_DERIVED 
FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
```

### 2. Caching Strategy
```python
# Flask-Caching configuration
CACHE_CONFIG = {
    'CACHE_TYPE': 'simple',           # In-memory cache
    'CACHE_DEFAULT_TIMEOUT': 3600     # 1 hour cache
}

# Cache decorators
@cache.memoize(timeout=3600)
def get_screener_data_formatted(selected_date):
    # Expensive calculation cached for 1 hour
    pass

@lru_cache(maxsize=128)
def get_available_dates():
    # LRU cache for frequently accessed data
    pass
```

### 3. Query Optimization
```python
# Single batch query instead of multiple queries
query = text("""
    WITH date_range AS (
        SELECT DISTINCT "BizDt"::DATE AS "BizDt"
        FROM "{table_name}"
        WHERE "BizDt"::DATE <= :curr_date
        ORDER BY "BizDt" DESC
        LIMIT 40
    )
    SELECT d.*, b."UndrlygPric", c.data_json
    FROM derived_data d
    LEFT JOIN base_data b ON d."BizDt" = b."BizDt"  
    LEFT JOIN cache_data c ON d."BizDt" = c."BizDt"
    ORDER BY d."BizDt", d."OptnTp", d."StrkPric"
""")

# Reduces query time from 8-10 seconds to under 1 second
```

### 4. Frontend Optimization
```javascript
// DataTables for large datasets
$('#dashboard-table').DataTable({
    "processing": true,
    "serverSide": true,      // Server-side pagination
    "ajax": "/api/dashboard_data",
    "pageLength": 50,
    "deferRender": true      // Lazy rendering
});

// Chart.js optimization
const chartConfig = {
    responsive: true,
    maintainAspectRatio: false,
    animation: {
        duration: 0          // Disable animations for performance
    },
    plugins: {
        legend: {
            display: false   // Hide legend for faster rendering
        }
    }
};
```

---

## 🔒 Security Features

### 1. Authentication System
```python
# Session-based authentication
@app.before_request
def require_login():
    if request.endpoint not in ['auth.login', 'auth.signup', 'static']:
        if 'user' not in session:
            return redirect(url_for('auth.login'))

# Password hashing
from werkzeug.security import generate_password_hash, check_password_hash

def create_user(username, password):
    hashed_password = generate_password_hash(password)
    # Store in database
```

### 2. Input Validation
```python
# SQL injection prevention
query = text("SELECT * FROM table WHERE date = :date")
result = conn.execute(query, {"date": user_input})

# XSS prevention in templates
{{ user_input|e }}  # Auto-escape in Jinja2

# CSRF protection
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY')
```

### 3. Database Security
```python
# Connection string encoding
db_password_enc = quote_plus(db_password)

# Connection timeout
connect_args = {
    "connect_timeout": 10,
    "application_name": "Derivatives_Analysis"
}
```

---

## 📈 Monitoring & Logging

### 1. Health Check Endpoint
```python
@health_bp.route("/health", methods=["GET"])
def health_check():
    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "checks": {
            "database": check_database_connection(),
            "cache": check_cache_tables(),
            "filesystem": check_log_directory()
        }
    }
    return jsonify(health_status), 200
```

### 2. Application Logging
```python
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/app.log'),
        logging.StreamHandler()
    ]
)

# Usage in controllers
logger = logging.getLogger(__name__)
logger.info(f"Processing dashboard data for {selected_date}")
logger.error(f"Database error: {str(e)}")
```

### 3. Performance Monitoring
```python
import time

def monitor_performance(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logger.info(f"{func.__name__} took {end_time - start_time:.2f} seconds")
        return result
    return wrapper

@monitor_performance
def get_dashboard_data(selected_date, mtype):
    # Function implementation
    pass
```

---

## 🚀 Deployment

### 1. Docker Deployment
```yaml
# docker-compose.yml
version: '3.8'
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_DB: BhavCopy_Database
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: ${DB_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"

  web:
    build: .
    depends_on:
      - postgres
    environment:
      DB_HOST: postgres
      DB_PASSWORD: ${DB_PASSWORD}
    ports:
      - "5000:5000"
    volumes:
      - ./logs:/app/logs

  nginx:
    image: nginx:alpine
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf
    depends_on:
      - web
```

### 2. Production Configuration
```python
# Gunicorn configuration (gunicorn.conf.py)
bind = "0.0.0.0:5000"
workers = 4
worker_class = "sync"
worker_connections = 1000
timeout = 120
keepalive = 2
max_requests = 1000
max_requests_jitter = 100
```

### 3. Nginx Configuration
```nginx
server {
    listen 80;
    server_name your-domain.com;

    location / {
        proxy_pass http://web:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    location /static/ {
        alias /app/Analysis_Tools/app/static/;
        expires 1y;
        add_header Cache-Control "public, immutable";
    }
}
```

---

## 🔄 Maintenance & Updates

### 1. Daily Data Pipeline
```bash
# Automated cron job (runs daily at 6 PM IST)
0 18 * * 1-5 cd /path/to/project/Database/FO && python fo_update_database.py

# Pipeline steps:
# 1. Download latest BhavCopy from NSE
# 2. Upload to PostgreSQL
# 3. Calculate Greeks
# 4. Update cache tables
# 5. Send completion notification
```

### 2. Database Maintenance
```sql
-- Weekly maintenance (run on weekends)
-- Vacuum and analyze tables
VACUUM ANALYZE options_dashboard_cache;
VACUUM ANALYZE screener_cache;
VACUUM ANALYZE technical_screener_cache;

-- Update table statistics
ANALYZE TBL_NIFTY_DERIVED;
ANALYZE TBL_BANKNIFTY_DERIVED;

-- Clean old cache data (keep last 90 days)
DELETE FROM options_dashboard_cache 
WHERE biz_date < CURRENT_DATE - INTERVAL '90 days';
```

### 3. Cache Management
```python
# Clear cache when needed
def clear_all_caches():
    cache.clear()  # Flask cache
    _get_available_dates_cached.cache_clear()  # LRU cache
    clear_excel_cache()  # Excel cache
    print("All caches cleared")

# Warm up cache after deployment
def warm_up_cache():
    dates = get_available_dates()
    for date in dates[:5]:  # Warm up last 5 days
        get_screener_data_formatted(date)
    print("Cache warmed up")
```

---

## 🐛 Troubleshooting

### Common Issues & Solutions

#### 1. Database Connection Issues
```python
# Error: "connection pool exhausted"
# Solution: Increase pool size or check for connection leaks
engine = create_engine(
    connection_string,
    pool_size=20,        # Increase from 10
    max_overflow=30      # Increase from 20
)

# Always use context managers
with engine.connect() as conn:
    result = conn.execute(query)
# Connection automatically closed
```

#### 2. Memory Issues
```python
# Error: "Out of memory" during large data processing
# Solution: Process data in chunks
def process_large_dataset(table_name):
    chunk_size = 10000
    offset = 0
    
    while True:
        query = f"SELECT * FROM {table_name} LIMIT {chunk_size} OFFSET {offset}"
        df = pd.read_sql(query, engine)
        
        if df.empty:
            break
            
        process_chunk(df)
        offset += chunk_size
```

#### 3. PDF Generation Issues
```python
# Error: "Playwright browser launch failed"
# Solution: Install browser dependencies
# Linux: apt-get install -y chromium-browser
# Windows: Playwright installs automatically

# Error: "PDF generation timeout"
# Solution: Increase timeout and optimize HTML
page.pdf(
    format="A4",
    timeout=60000,  # 60 seconds
    print_background=True
)
```

#### 4. Cache Issues
```python
# Error: "Stale cache data"
# Solution: Implement cache invalidation
def invalidate_cache_on_new_data(date):
    cache_keys = [
        f"screener_data_{date}",
        f"dashboard_data_{date}",
        f"technical_data_{date}"
    ]
    for key in cache_keys:
        cache.delete(key)
```

---

## 📚 API Reference

### Dashboard API Endpoints

#### Get Dashboard Data
```http
GET /api/dashboard_data?date=2024-01-15&mtype=TOTAL
```

**Parameters:**
- `date` (string): Business date in YYYY-MM-DD format
- `mtype` (string): Moneyness type (TOTAL/OTM/ITM)

**Response:**
```json
{
  "draw": 1,
  "recordsTotal": 150,
  "recordsFiltered": 150,
  "data": [
    [
      "<a href='/stock/NIFTY'>NIFTY</a>",
      "25000",     // call_delta_pos_strike
      "+2.45",     // call_delta_pos_pct
      "24800",     // call_delta_neg_strike
      "-1.23",     // call_delta_neg_pct
      // ... more columns
    ]
  ]
}
```

#### Get Historical Chart Data
```http
GET /api/historical-chart-data?ticker=NIFTY&option_type=call&metric=vega&date=2024-01-15
```

**Parameters:**
- `ticker` (string): Stock ticker symbol
- `option_type` (string): call/put
- `metric` (string): vega/money
- `date` (string): Current date
- `strike` (string, optional): Specific strike price

**Response:**
```json
{
  "success": true,
  "ticker": "NIFTY",
  "data": [
    {
      "date": "2024-01-15",
      "pcr_volume": 0.85,
      "pcr_oi": 0.92,
      "underlying_price": 21500.50,
      "rsi": 67.4,
      "value": 125.30,
      "metric_label": "Vega @ 21500"
    }
  ]
}
```

### Screener API Endpoints

#### Export PDF
```http
GET /screener/export-pdf?date=2024-01-15
```

**Parameters:**
- `date` (string): Business date for report

**Response:**
- Content-Type: application/pdf
- File download with name: `Goldmine_Screener_Report_2024-01-15_timestamp.pdf`

### Health Check API

#### System Health
```http
GET /health
```

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:00Z",
  "version": "1.0.0",
  "checks": {
    "database": "connected",
    "cache": "latest: 2024-01-15",
    "filesystem": "writable"
  }
}
```

---

## 🎨 UI/UX Design System

### Color Palette
```css
/* Primary Colors */
--goldmine-maroon: #8B2432;     /* Primary brand color */
--goldmine-gold: #D4AF37;       /* Accent color */

/* Background Colors */
--bg-primary: #ffffff;          /* Main background */
--bg-secondary: #f9fafb;        /* Card backgrounds */
--bg-tertiary: #fafafa;         /* Table headers */

/* Text Colors */
--text-primary: #1a1a1a;        /* Main text */
--text-secondary: #1f2937;      /* Secondary text */
--text-muted: #6b7280;          /* Muted text */

/* Status Colors */
--success: #16a34a;             /* Green for positive */
--danger: #dc2626;              /* Red for negative */
--warning: #f59e0b;             /* Orange for warnings */
--info: #3b82f6;                /* Blue for info */

/* Border Colors */
--border-light: #e5e7eb;        /* Light borders */
--border-medium: #d1d5db;       /* Medium borders */
--border-dark: #9ca3af;         /* Dark borders */
```

### Typography
```css
/* Font Family */
font-family: 'Poppins', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;

/* Font Sizes */
--text-xs: 0.75rem;     /* 12px */
--text-sm: 0.875rem;    /* 14px */
--text-base: 1rem;      /* 16px */
--text-lg: 1.125rem;    /* 18px */
--text-xl: 1.25rem;     /* 20px */
--text-2xl: 1.5rem;     /* 24px */
--text-3xl: 1.875rem;   /* 30px */

/* Font Weights */
--font-normal: 400;
--font-medium: 500;
--font-semibold: 600;
--font-bold: 700;
```

### Component Styles
```css
/* Cards */
.card {
    background: var(--bg-primary);
    border: 1px solid var(--border-light);
    border-radius: 14px;
    box-shadow: 0 4px 10px rgba(0,0,0,0.03);
    transition: box-shadow 0.2s ease;
}

.card:hover {
    box-shadow: 0 6px 16px rgba(0,0,0,0.06);
}

/* Buttons */
.btn-primary {
    background: var(--goldmine-maroon);
    color: white;
    border: none;
    border-radius: 8px;
    padding: 12px 24px;
    font-weight: var(--font-medium);
    transition: all 0.2s ease;
}

.btn-primary:hover {
    background: #722030;
    transform: translateY(-1px);
}

/* Tables */
.table {
    width: 100%;
    border-collapse: collapse;
}

.table th {
    background: var(--bg-tertiary);
    color: var(--text-secondary);
    font-size: var(--text-xs);
    font-weight: var(--font-semibold);
    text-transform: uppercase;
    letter-spacing: 0.05em;
    padding: 12px 16px;
    border: 1px solid var(--border-light);
}

.table td {
    padding: 12px 16px;
    border: 1px solid var(--border-light);
    font-size: var(--text-sm);
}

/* Status Indicators */
.status-positive {
    color: var(--success);
    font-weight: var(--font-semibold);
}

.status-negative {
    color: var(--danger);
    font-weight: var(--font-semibold);
}

.status-neutral {
    color: var(--text-muted);
}
```

### Responsive Design
```css
/* Mobile First Approach */
.container {
    width: 100%;
    max-width: 1200px;
    margin: 0 auto;
    padding: 0 16px;
}

/* Tablet */
@media (min-width: 768px) {
    .container {
        padding: 0 24px;
    }
}

/* Desktop */
@media (min-width: 1024px) {
    .container {
        padding: 0 32px;
    }
}

/* Grid System */
.grid {
    display: grid;
    gap: 24px;
}

.grid-cols-1 { grid-template-columns: repeat(1, 1fr); }
.grid-cols-2 { grid-template-columns: repeat(2, 1fr); }
.grid-cols-3 { grid-template-columns: repeat(3, 1fr); }

@media (max-width: 768px) {
    .grid-cols-2,
    .grid-cols-3 {
        grid-template-columns: 1fr;
    }
}
```

---

## 🔮 Future Enhancements

### Planned Features

#### 1. Real-time Data Integration
```python
# WebSocket implementation for live updates
from flask_socketio import SocketIO, emit

socketio = SocketIO(app)

@socketio.on('subscribe_ticker')
def handle_subscription(data):
    ticker = data['ticker']
    # Subscribe to live data feed
    emit('price_update', {
        'ticker': ticker,
        'price': get_live_price(ticker),
        'change': get_price_change(ticker)
    })
```

#### 2. Machine Learning Integration
```python
# Predictive analytics using scikit-learn
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler

def predict_option_price(features):
    model = load_trained_model()
    scaler = load_scaler()
    
    scaled_features = scaler.transform(features)
    prediction = model.predict(scaled_features)
    
    return prediction

# Features: underlying_price, strike, dte, iv, volume, oi
```

#### 3. Advanced Charting
```javascript
// TradingView Charting Library integration
const widget = new TradingView.widget({
    symbol: "NSE:NIFTY",
    interval: "1D",
    container_id: "tradingview_chart",
    library_path: "/static/charting_library/",
    locale: "en",
    disabled_features: ["use_localstorage_for_settings"],
    enabled_features: ["study_templates"],
    charts_storage_url: "https://saveload.tradingview.com",
    charts_storage_api_version: "1.1",
    client_id: "tradingview.com",
    user_id: "public_user_id"
});
```

#### 4. Mobile Application
```javascript
// React Native app structure
const App = () => {
  return (
    <NavigationContainer>
      <Stack.Navigator>
        <Stack.Screen name="Dashboard" component={DashboardScreen} />
        <Stack.Screen name="Screener" component={ScreenerScreen} />
        <Stack.Screen name="StockDetail" component={StockDetailScreen} />
        <Stack.Screen name="Charts" component={ChartsScreen} />
      </Stack.Navigator>
    </NavigationContainer>
  );
};
```

#### 5. API Expansion
```python
# RESTful API with Flask-RESTful
from flask_restful import Api, Resource

api = Api(app)

class OptionsChainAPI(Resource):
    def get(self, ticker):
        return {
            'ticker': ticker,
            'expiries': get_expiry_dates(ticker),
            'strikes': get_strike_prices(ticker),
            'chain': get_options_chain(ticker)
        }

api.add_resource(OptionsChainAPI, '/api/v1/options/<string:ticker>')
```

### Performance Improvements

#### 1. Database Sharding
```python
# Horizontal partitioning by ticker
CREATE TABLE TBL_NIFTY_DERIVED_SHARD1 (
    CHECK (ticker_hash(TckrSymb) % 4 = 0)
) INHERITS (TBL_NIFTY_DERIVED);

CREATE TABLE TBL_NIFTY_DERIVED_SHARD2 (
    CHECK (ticker_hash(TckrSymb) % 4 = 1)
) INHERITS (TBL_NIFTY_DERIVED);
```

#### 2. Redis Caching
```python
import redis

redis_client = redis.Redis(host='localhost', port=6379, db=0)

def get_cached_data(key):
    cached = redis_client.get(key)
    if cached:
        return json.loads(cached)
    return None

def set_cached_data(key, data, ttl=3600):
    redis_client.setex(key, ttl, json.dumps(data))
```

#### 3. CDN Integration
```python
# AWS CloudFront for static assets
STATIC_URL_PREFIX = 'https://d1234567890.cloudfront.net'

@app.context_processor
def inject_cdn():
    return {'cdn_url': STATIC_URL_PREFIX}
```

---

## 📄 License & Legal

### Data Sources
- **NSE BhavCopy**: Used under NSE terms and conditions
- **Market Data**: For educational and analysis purposes only
- **Technical Indicators**: Calculated using open-source libraries

### Disclaimer
```
IMPORTANT DISCLAIMER:

This software is for educational and informational purposes only. 
It should not be considered as investment advice or recommendation 
to buy or sell any securities.

- Past performance does not guarantee future results
- All investments carry risk of loss
- Users should consult with qualified financial advisors
- The developers are not responsible for any trading losses
- Market data may have delays or inaccuracies
- Use at your own risk

By using this software, you acknowledge and accept these terms.
```

### Open Source Libraries
- **Flask**: BSD-3-Clause License
- **SQLAlchemy**: MIT License
- **pandas**: BSD-3-Clause License
- **numpy**: BSD-3-Clause License
- **py-vollib**: MIT License
- **pandas-ta**: MIT License
- **Chart.js**: MIT License
- **Playwright**: Apache-2.0 License

---

## 🤝 Contributing

### Development Setup
```bash
# Fork the repository
git clone https://github.com/yourusername/nse-options-analysis.git
cd nse-options-analysis

# Create feature branch
git checkout -b feature/new-feature

# Install development dependencies
pip install -r requirements-dev.txt

# Run tests
python -m pytest tests/

# Run linting
flake8 Analysis_Tools/
black Analysis_Tools/

# Commit changes
git commit -m "Add new feature"
git push origin feature/new-feature
```

### Code Style Guidelines
```python
# Follow PEP 8 standards
# Use type hints
def calculate_greeks(
    premium: float, 
    strike: float, 
    underlying: float, 
    expiry: datetime
) -> Dict[str, float]:
    """Calculate Black-Scholes Greeks."""
    pass

# Use docstrings
def get_dashboard_data(selected_date: str, mtype: str) -> List[Dict]:
    """
    Fetch dashboard data for given date and moneyness type.
    
    Args:
        selected_date: Business date in YYYY-MM-DD format
        mtype: Moneyness filter (TOTAL/OTM/ITM)
        
    Returns:
        List of dictionaries containing dashboard metrics
        
    Raises:
        ValueError: If date format is invalid
        DatabaseError: If database connection fails
    """
    pass
```

### Testing Guidelines
```python
# Unit tests with pytest
import pytest
from Analysis_Tools.app.models.dashboard_model import get_dashboard_data

def test_get_dashboard_data():
    """Test dashboard data retrieval."""
    result = get_dashboard_data('2024-01-15', 'TOTAL')
    assert isinstance(result, list)
    assert len(result) > 0
    assert 'stock' in result[0]
    assert 'closing_price' in result[0]

# Integration tests
def test_dashboard_endpoint(client):
    """Test dashboard API endpoint."""
    response = client.get('/api/dashboard_data?date=2024-01-15&mtype=TOTAL')
    assert response.status_code == 200
    data = response.get_json()
    assert 'data' in data
    assert 'recordsTotal' in data
```

---

## 📞 Support & Contact

### Technical Support
- **Documentation**: This comprehensive guide
- **Issue Tracking**: GitHub Issues
- **Email**: support@goldmine-analysis.com

### Community
- **Discord**: [Join our community](https://discord.gg/goldmine)
- **Telegram**: [@goldmine_analysis](https://t.me/goldmine_analysis)
- **Twitter**: [@GoldmineAnalysis](https://twitter.com/GoldmineAnalysis)

### Professional Services
- **Custom Development**: Available for enterprise clients
- **Training & Consulting**: Market analysis and trading system development
- **Data Integration**: Custom data feeds and API development

---

**Last Updated**: January 2025  
**Version**: 3.0  
**Maintained By**: Goldmine Analysis Team

---

*This documentation is continuously updated. For the latest version, please check the repository.*