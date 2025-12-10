# controllers/stock_controller.py
import json

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
# � Stock Search/Selection Page
# ==============================
@stock_bp.route("/stock-search")
def stock_search():
    """
    Stock search page - allows user to select a stock symbol
    """
    try:
        dates = get_available_dates()
        all_symbols = get_filtered_tickers()
        selected_date = request.args.get("date", dates[0] if dates else None)

        return render_template(
            "stock_search.html",
            dates=dates,
            selected_date=selected_date,
            all_symbols=all_symbols,
            stock_list=get_filtered_tickers(),
        )
    except Exception as e:
        print(f"[ERROR] Stock search route failed: {e}")
        import traceback

        traceback.print_exc()
        return jsonify({"error": f"Stock search failed: {str(e)}"}), 500


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
