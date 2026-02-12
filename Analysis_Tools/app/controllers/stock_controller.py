# controllers/stock_controller.py
import json
import os
from pathlib import Path

import pandas as pd
from flask import Blueprint, jsonify, redirect, render_template, request, url_for

from ..controllers.dashboard_controller import get_live_indices
from ..models.stock_model import (
    generate_oi_chart,
    get_all_tickers,
    get_available_dates,
    get_filtered_tickers,
    get_stock_chart_data,
    get_stock_detail_data,
    get_stock_expiry_data,
    get_stock_stats,
)

stock_bp = Blueprint("stock", __name__)

# ==============================
# 📊 Fundamental Data Functions
# ==============================
# Path to Data_scraper data directory
FUNDAMENTAL_DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent / "Data_scraper" / "data"


def load_fundamental_json(symbol, data_type):
    """Load JSON data for a symbol from fundamental data directory"""
    file_path = FUNDAMENTAL_DATA_DIR / data_type / f"{symbol}.json"
    if file_path.exists():
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def load_fundamental_csv(symbol, data_type):
    """Load CSV data for a symbol from fundamental data directory"""
    file_path = FUNDAMENTAL_DATA_DIR / data_type / f"{symbol}.csv"
    if file_path.exists():
        return pd.read_csv(file_path)
    return None


def calculate_fundamental_metrics(symbol):
    """Calculate key metrics for a stock from fundamental data"""
    metrics = {
        "current_price": 0,
        "price_change": 0,
        "high_52w": 0,
        "low_52w": 0,
        "market_cap": 0,
        "stock_pe": 0,
        "book_value": 0,
        "roce": 0,
        "roe": 0,
        "dividend_yield": 0,
        "face_value": 10.0,
        "eps": 0,
        "sales": 0,
        "net_profit": 0,
    }

    # Load price data
    price_data = load_fundamental_csv(symbol, "price")
    if price_data is not None and not price_data.empty:
        price_df = price_data[price_data["metric"] == "Price"].copy()
        if not price_df.empty:
            price_df["value"] = pd.to_numeric(price_df["value"], errors="coerce")
            latest_price = price_df.iloc[-1]["value"]
            prev_price = price_df.iloc[-2]["value"] if len(price_df) > 1 else latest_price
            metrics["current_price"] = latest_price
            metrics["price_change"] = ((latest_price - prev_price) / prev_price * 100) if prev_price else 0

            # Default to all-time or last year if company_info is missing
            metrics["high_52w"] = price_df["value"].max()
            metrics["low_52w"] = price_df["value"].min()

    # Load Company Info (Scraped Header) - PRIORITY FOR MARKET CAP & HIGH/LOW
    company_info = load_fundamental_json(symbol, "company_info")
    if company_info:
        # Helper to clean and parse float
        def parse_float(val):
            if isinstance(val, (int, float)):
                return float(val)
            if isinstance(val, str):
                return float(val.replace(",", "").replace("%", ""))
            return 0

        if "Market Cap" in company_info:
            metrics["market_cap"] = parse_float(company_info["Market Cap"])

        if "High / Low" in company_info:
            hl = company_info["High / Low"].split("/")
            if len(hl) == 2:
                metrics["high_52w"] = parse_float(hl[0])
                metrics["low_52w"] = parse_float(hl[1])

        if "Stock P/E" in company_info:
            metrics["stock_pe"] = parse_float(company_info["Stock P/E"])

        if "Book Value" in company_info:
            metrics["book_value"] = parse_float(company_info["Book Value"])

        if "ROCE" in company_info:
            metrics["roce"] = parse_float(company_info["ROCE"]) / 100

        if "ROE" in company_info:
            metrics["roe"] = parse_float(company_info["ROE"]) / 100

        if "Dividend Yield" in company_info:
            metrics["dividend_yield"] = parse_float(company_info["Dividend Yield"])

        if "Face Value" in company_info:
            metrics["face_value"] = parse_float(company_info["Face Value"])

    # Load P&L data
    pnl_data = load_fundamental_json(symbol, "pnl")
    if pnl_data:
        dates = sorted(pnl_data.keys(), reverse=True)
        if dates:
            latest_data = pnl_data[dates[0]]
            for item in latest_data:
                for key, value in item.items():
                    key_lower = key.lower()
                    if "sales" in key_lower or "revenue" in key_lower:
                        metrics["sales"] = value
                    elif "netprofit" in key_lower.replace(" ", "") or "net profit" in key_lower:
                        metrics["net_profit"] = value
                    elif (key == "EPS" or "eps" in key_lower) and metrics["eps"] == 0:
                         # Only set if not already set (though company_info doesn't have eps usually)
                        metrics["eps"] = value

    return metrics


def get_available_fundamental_stocks():
    """Get list of stocks that have fundamental data available"""
    quarterly_dir = FUNDAMENTAL_DATA_DIR / "quarterly"
    if quarterly_dir.exists():
        return sorted([f.stem for f in quarterly_dir.glob("*.json")])
    return []


@stock_bp.route("/stock/<ticker>")
def stock_detail(ticker):
    """
    Stock detail page with server-side filtering
    Query params: date, expiry
    """

    # ==============================
    # 📅 Fetch all available dates and symbols
    # ==============================
    dates = get_available_dates()
    all_symbols = get_filtered_tickers()  # ✅ Filter by Excel list

    # ==============================
    # 🧭 Determine selected date and expiry
    # ==============================
    selected_date = request.args.get("date", dates[0] if dates else None)
    selected_expiry = request.args.get("expiry", None)

    data = []
    expiry_data = []
    stats = {}

    if selected_date:
        # ==============================
        # 📘 Expiry data for left panel
        # ==============================
        expiry_data = get_stock_expiry_data(ticker, selected_date)

        # Auto-select first expiry if none chosen
        if not selected_expiry and expiry_data and len(expiry_data) > 0:
            selected_expiry = expiry_data[0]["expiry"]

        # Fetch option chain & summary stats
        data = get_stock_detail_data(ticker, selected_date, selected_expiry)
        stats = get_stock_stats(ticker, selected_date, selected_expiry)

        # ==============================
        # 🔍 Detect Underlying Price & Futures Price
        # ==============================
        underlying = None
        futures_price = None

        if data:
            for row in data:
                if row.get("UndrlygPric"):
                    underlying = row["UndrlygPric"]
                    break
                elif row.get("UnderlyingValue"):
                    underlying = row["UnderlyingValue"]
                    break
                elif row.get("underlying"):
                    underlying = row["underlying"]
                    break

        # Get futures price for selected expiry from expiry_data
        if selected_expiry and expiry_data:
            for exp_row in expiry_data:
                if exp_row.get("expiry") == selected_expiry:
                    futures_price = exp_row.get("price")
                    break

        # Fallback to underlying if futures price not found
        if futures_price is None:
            futures_price = underlying

        # Safely clean underlying and futures price strings (e.g. "5,298.00 ")
        if underlying:
            try:
                underlying = float(str(underlying).replace(",", "").strip())
            except Exception:
                underlying = None

        if futures_price:
            try:
                futures_price = float(str(futures_price).replace(",", "").strip())
            except Exception:
                futures_price = underlying

        # ==============================
        # 🎯 Find ATM Strike (Closest to Underlying)
        # ==============================
        atm = None
        if data and underlying:
            try:
                # FIX #6: Add error handling for None/NaN values
                strike_prices = sorted(
                    {
                        float(row["StrkPric"])
                        for row in data
                        if row.get("StrkPric") is not None and pd.notna(row.get("StrkPric"))
                    }
                )
                if strike_prices:
                    # Find strike with smallest distance from underlying
                    atm = min(strike_prices, key=lambda x: abs(x - underlying))
            except (ValueError, TypeError) as e:
                print(f"[WARNING] Error calculating ATM strike: {e}")
                atm = None

        # ==============================
        # 📈 Compute Average IV
        # ==============================
        # FIX #5: Only override avg_iv if stats doesn't have it or if we have better data from option chain
        if data:
            iv_values = [row.get("IV") for row in data if row.get("IV") is not None and row["IV"] > 0]
            if iv_values:
                avg_iv = sum(iv_values) / len(iv_values)
                if avg_iv < 1:
                    avg_iv *= 100
                # Only override if stats doesn't have avg_iv or if our calculation is more accurate
                if "avg_iv" not in stats or stats.get("avg_iv", 0) == 0:
                    stats["avg_iv"] = round(avg_iv, 2)
            elif "avg_iv" not in stats:
                stats["avg_iv"] = 0
        elif "avg_iv" not in stats:
            stats["avg_iv"] = 0

    # ==============================
    # 🧠 Generate OI Chart Data
    # ==============================
    chart_data = None
    if selected_date and selected_expiry:
        # Pass data and expiry_data to avoid redundant queries
        oi_chart_dict = generate_oi_chart(ticker, selected_date, selected_expiry, data=data, expiry_data=expiry_data)
        if oi_chart_dict:
            chart_data = json.dumps(oi_chart_dict)

    # ==============================
    # 🎨 Render Template
    # ==============================
    return render_template(
        "stock_detail.html",
        ticker=ticker,
        stock_symbol=ticker,  # For header navigation
        all_symbols=all_symbols,
        data=data,
        expiry_data=expiry_data,
        stats=stats,
        dates=dates,
        selected_date=selected_date,
        selected_expiry=selected_expiry,
        chart_data=chart_data,
        underlying=underlying,  # ✅ spot price (for display)
        futures_price=futures_price,  # ✅ futures price for selected expiry (for Fair Price calculation)
        atm=atm,  # ✅ correct closest strike
        indices=get_live_indices(),  # ✅ Add indices for header
        stock_list=get_filtered_tickers(),  # ✅ Add stock list for search
    )


# ==============================
# 📈 API endpoint for mini stock chart (price data)
# ==============================
@stock_bp.route("/api/stock-chart/<ticker>")
def api_stock_chart(ticker):
    days = int(request.args.get("days", 90))
    try:
        chart_data = get_stock_chart_data(ticker, days)
        return jsonify({"success": True, "data": chart_data})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ==============================
# 📊 Fundamental Analysis Page
# ==============================
@stock_bp.route("/stock/<ticker>/fundamental")
def stock_fundamental(ticker):
    """
    Fundamental analysis page - displays financial data from Data_scraper
    """
    try:
        # Load all fundamental data
        quarterly = load_fundamental_json(ticker, "quarterly")
        pnl = load_fundamental_json(ticker, "pnl")
        balance_sheet = load_fundamental_json(ticker, "balance_sheet")
        cashflow = load_fundamental_json(ticker, "cashflow")
        ratios = load_fundamental_json(ticker, "ratios")
        shareholding = load_fundamental_json(ticker, "shareholding")
        price_data = load_fundamental_csv(ticker, "price")

        # Calculate metrics
        metrics = calculate_fundamental_metrics(ticker)

        # Process price data for chart
        chart_data = []
        if price_data is not None and not price_data.empty:
            price_df = price_data[price_data["metric"] == "Price"].copy()
            price_df["value"] = pd.to_numeric(price_df["value"], errors="coerce")
            chart_data = price_df[["date", "value"]].to_dict("records")

        return render_template(
            "fundamental.html",
            symbol=ticker,
            quarterly=quarterly,
            pnl=pnl,
            balance_sheet=balance_sheet,
            cashflow=cashflow,
            ratios=ratios,
            shareholding=shareholding,
            chart_data=chart_data,
            metrics=metrics,
            indices=get_live_indices(),
            stock_list=get_filtered_tickers(),
            available_stocks=get_available_fundamental_stocks(),
        )
    except Exception as e:
        print(f"[ERROR] Fundamental page failed for {ticker}: {e}")
        import traceback

        traceback.print_exc()
        return render_template(
            "fundamental.html",
            symbol=ticker,
            quarterly=None,
            pnl=None,
            balance_sheet=None,
            cashflow=None,
            ratios=None,
            shareholding=None,
            chart_data=[],
            metrics={
                "current_price": 0,
                "price_change": 0,
                "high_52w": 0,
                "low_52w": 0,
                "market_cap": 0,
                "stock_pe": 0,
                "book_value": 0,
                "roce": 0,
                "roe": 0,
                "dividend_yield": 0,
                "face_value": 10.0,
            },
            indices=get_live_indices(),
            stock_list=get_filtered_tickers(),
            available_stocks=get_available_fundamental_stocks(),
        )
