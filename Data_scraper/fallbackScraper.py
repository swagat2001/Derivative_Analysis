import os
import sys
import json
import time
import datetime
import pandas as pd
import yfinance as yf
from pathlib import Path

# Add parent directory to sys.path to import modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
try:
    from screenerScraper import ScreenerScrape
except ImportError:
    # If run from within Data_scraper dir
    from screenerScraper import ScreenerScrape

# Configuration
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
MISSING_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "missing_stocks.json")
DELAY_BETWEEN_STOCKS = 2

def create_dirs():
    dirs = ["quarterly", "pnl", "balance_sheet", "cashflow", "ratios", "shareholding", "price", "company_info"]
    for d in dirs:
        Path(os.path.join(DATA_DIR, d)).mkdir(parents=True, exist_ok=True)

def map_yfinance_pnl(ticker_obj):
    """Map yfinance Income Statement to Screener JSON format"""
    try:
        is_df = ticker_obj.financials # Annual Income Statement
        if is_df.empty: return None

        result = {}
        for date_col in is_df.columns:
            dt_str = date_col.strftime("%Y-%m-%d")
            col = is_df[date_col]

            # Map metrics (approximations)
            sales = float(col.get('Total Revenue', 0))
            op_inc = float(col.get('Operating Income', 0))
            expenses = sales - op_inc
            other_inc = float(col.get('Other Income Expense', 0))
            interest = float(col.get('Interest Expense', 0))
            depr = float(col.get('Depreciation And Amortization', 0))
            pbt = float(col.get('Pretax Income', 0))
            net_profit = float(col.get('Net Income', 0))
            eps = float(col.get('Basic EPS', 0))

            data_list = [
                {"Sales ": sales},
                {"Expenses ": expenses},
                {"OperatingProfit": op_inc},
                {"OPM%": round(op_inc / sales, 2) if sales else 0},
                {"OtherIncome ": other_inc},
                {"Interest": interest},
                {"Depreciation": depr},
                {"Profitbeforetax": pbt},
                {"Tax%": round((pbt - net_profit) / pbt, 2) if pbt else 0},
                {"NetProfit ": net_profit},
                {"EPSinRs": eps},
                {"DividendPayout%": 0.0} # Harder to get exactly right here
            ]
            result[dt_str] = data_list

        return result
    except Exception as e:
        print(f"      [yf-ERROR] P&L mapping failed: {e}")
        return None

def map_yfinance_balance_sheet(ticker_obj):
    """Map yfinance Balance Sheet to Screener JSON format"""
    try:
        bs_df = ticker_obj.balance_sheet
        if bs_df.empty: return None

        result = {}
        for date_col in bs_df.columns:
            dt_str = date_col.strftime("%Y-%m-%d")
            col = bs_df[date_col]

            equity = float(col.get('Share Capital', 0))
            reserves = float(col.get('Retained Earnings', 0)) + float(col.get('Other Equity', 0))
            borrowings = float(col.get('Total Debt', 0))
            total_assets = float(col.get('Total Assets', 0))
            other_liab = total_assets - equity - reserves - borrowings

            fixed_assets = float(col.get('Net PPE', 0))
            investments = float(col.get('Long Term Investments', 0)) + float(col.get('Other Short Term Investments', 0))
            cwip = 0.0 # Not easily found in standard yf
            other_assets = total_assets - fixed_assets - investments

            data_list = [
                {"EquityCapital": equity},
                {"Reserves": reserves},
                {"Borrowings ": borrowings},
                {"OtherLiabilities ": other_liab},
                {"TotalLiabilities": total_assets},
                {"FixedAssets ": fixed_assets},
                {"CWIP": cwip},
                {"Investments": investments},
                {"OtherAssets ": other_assets},
                {"TotalAssets": total_assets}
            ]
            result[dt_str] = data_list
        return result
    except Exception as e:
        print(f"      [yf-ERROR] Balance Sheet mapping failed: {e}")
        return None

def map_yfinance_cashflow(ticker_obj):
    """Mapping for Cash Flow"""
    try:
        cf_df = ticker_obj.cashflow
        if cf_df.empty: return None
        result = {}
        for date_col in cf_df.columns:
            dt_str = date_col.strftime("%Y-%m-%d")
            col = cf_df[date_col]
            op = float(col.get('Operating Cash Flow', 0))
            inv = float(col.get('Investing Cash Flow', 0))
            fin = float(col.get('Financing Cash Flow', 0))

            data_list = [
                {"CashfromOperatingActivity ": op},
                {"CashfromInvestingActivity ": inv},
                {"CashfromFinancingActivity ": fin},
                {"NetCashFlow": op + inv + fin}
            ]
            result[dt_str] = data_list
        return result
    except Exception: return None

def save_json(symbol, data_type, data):
    if not data: return
    path = os.path.join(DATA_DIR, data_type, f"{symbol}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def process_stock(sc, symbol):
    print(f"[PROCESS] {symbol}...")

    # 1. Try Screener
    token = sc.getBSEToken(symbol)
    if token:
        try:
            sc.loadScraper(token)
            print(f"  [OK] Found on Screener (ID: {sc.screenerID})")

            # Use logic similar to batchScraper.py
            pnl = sc.pnlReport()
            save_json(symbol, "pnl", pnl)

            bs = sc.balanceSheet()
            save_json(symbol, "balance_sheet", bs)

            cf = sc.cashFLow()
            save_json(symbol, "cashflow", cf)

            qr = sc.quarterlyReport()
            save_json(symbol, "quarterly", qr)

            print(f"  [SUCCESS] Scraped from Screener")
            return "screener"
        except Exception as e:
            print(f"  [WARN] Screener scrape failed for {symbol}: {e}")
    else:
        print(f"  [INFO] No BSE token for {symbol}, trying yfinance...")

    # 2. Try yfinance Fallback
    # Tickers are SYMBOL.NS (NSE) or SYMBOL.BO (BSE)
    for suffix in [".NS", ".BO"]:
        yf_ticker = symbol + suffix
        try:
            print(f"  [TRY] yfinance: {yf_ticker}")
            ticker_obj = yf.Ticker(yf_ticker)

            pnl = map_yfinance_pnl(ticker_obj)
            if pnl:
                save_json(symbol, "pnl", pnl)

                bs = map_yfinance_balance_sheet(ticker_obj)
                save_json(symbol, "balance_sheet", bs)

                cf = map_yfinance_cashflow(ticker_obj)
                save_json(symbol, "cashflow", cf)

                print(f"  [SUCCESS] Fetched from yfinance ({suffix})")
                return f"yfinance{suffix}"
        except Exception as e:
            print(f"    [FAIL] yfinance {yf_ticker}: {e}")

    return None

def run_fallback():
    create_dirs()
    if not os.path.exists(MISSING_FILE):
        print(f"[ERROR] {MISSING_FILE} not found. Run analysis first.")
        return

    with open(MISSING_FILE, "r") as f:
        missing_stocks = json.load(f)

    print(f"[START] Processing {len(missing_stocks)} missing stocks...")

    sc = ScreenerScrape()

    stats = {"screener": 0, "yfinance": 0, "failed": 0}

    # Process a few for testing first
    limit = 20 # Limit for initial run
    count = 0

    for symbol in missing_stocks:
        if count >= limit: break

        # Check if already exists (robustness)
        if os.path.exists(os.path.join(DATA_DIR, "pnl", f"{symbol}.json")):
            continue

        source = process_stock(sc, symbol)
        if source:
            if "yfinance" in source: stats["yfinance"] += 1
            else: stats["screener"] += 1
        else:
            stats["failed"] += 1

        count += 1
        time.sleep(DELAY_BETWEEN_STOCKS)

    print(f"\n[SUMMARY] Processed: {count}")
    print(f"  Screener: {stats['screener']}")
    print(f"  yfinance: {stats['yfinance']}")
    print(f"  Failed:   {stats['failed']}")

if __name__ == "__main__":
    run_fallback()
