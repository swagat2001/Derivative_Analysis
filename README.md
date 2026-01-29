# Derivative Analysis

A comprehensive stock market analysis platform for F&O (Futures & Options) trading in the Indian market.

## Features

- **Stock Dashboard**: Real-time stock data visualization and analysis
- **Signal Scanner**: RSI-based signal detection for F&O stocks
- **Market Insights**: Heatmaps, sector performance, and market statistics
- **Options Analysis**: Options chain analysis with Greeks calculation
- **FII/DII Activity**: Track institutional trading patterns

## Project Structure

```
Derivative_Analysis/
├── Analysis_Tools/       # Flask web application
│   ├── app/
│   │   ├── controllers/  # Route handlers
│   │   ├── models/       # Data models and database queries
│   │   ├── views/        # HTML templates
│   │   └── static/       # CSS, JS, and images
│   └── config/           # Application configuration
├── Database/             # Database utilities
│   ├── Cash/             # Cash market data
│   └── FO/               # F&O market data
└── run.py                # Application entry point
```

## Installation

1. Clone the repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   ```
3. Activate the virtual environment:
   - Windows: `venv\Scripts\activate`
   - Linux/Mac: `source venv/bin/activate`
4. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Running the Application

```bash
python run.py
```

The application will be available at `http://localhost:5000`

## Requirements

- Python 3.13+
- PostgreSQL database
- Required Python packages listed in `requirements.txt`

## License

Private - All rights reserved
