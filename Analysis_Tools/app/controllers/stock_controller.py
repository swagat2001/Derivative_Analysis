# controllers/stock_controller.py
from flask import Blueprint, render_template, request, jsonify
from ..models.stock_model import (
    get_available_dates,
    get_all_tickers,
    get_filtered_tickers,
    get_stock_detail_data,
    get_stock_expiry_data,
    get_stock_stats,
    get_stock_chart_data,
    generate_oi_chart
)
import json

stock_bp = Blueprint('stock', __name__)

@stock_bp.route('/stock/<ticker>')
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
    selected_date = request.args.get('date', dates[0] if dates else None)
    selected_expiry = request.args.get('expiry', None)

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
            selected_expiry = expiry_data[0]['expiry']

        # Fetch option chain & summary stats
        data = get_stock_detail_data(ticker, selected_date, selected_expiry)
        stats = get_stock_stats(ticker, selected_date, selected_expiry)

        # ==============================
        # 🔍 Detect Underlying Price
        # ==============================
        underlying = None
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

        # Safely clean underlying string (e.g. "5,298.00 ")
        if underlying:
            try:
                underlying = float(str(underlying).replace(",", "").strip())
            except Exception:
                underlying = None

        # ==============================
        # 🎯 Find ATM Strike (Closest to Underlying)
        # ==============================
        atm = None
        if data and underlying:
            strike_prices = sorted({float(row["StrkPric"]) for row in data if row.get("StrkPric")})
            if strike_prices:
                # Find strike with smallest distance from underlying
                atm = min(strike_prices, key=lambda x: abs(x - underlying))

        # ==============================
        # 📈 Compute Average IV
        # ==============================
        if data:
            iv_values = [row.get('IV') for row in data if row.get('IV') and row['IV'] > 0]
            if iv_values:
                avg_iv = sum(iv_values) / len(iv_values)
                if avg_iv < 1:
                    avg_iv *= 100
                stats['avg_iv'] = round(avg_iv, 2)
            else:
                stats['avg_iv'] = 0
        else:
            stats['avg_iv'] = 0

    # ==============================
    # 🧠 Generate OI Chart Data
    # ==============================
    chart_data = None
    if selected_date and selected_expiry:
        oi_chart_dict = generate_oi_chart(ticker, selected_date, selected_expiry)
        if oi_chart_dict:
            chart_data = json.dumps(oi_chart_dict)

    # ==============================
    # 🎨 Render Template
    # ==============================
    return render_template(
        'stock_detail.html',
        ticker=ticker,
        all_symbols=all_symbols,
        data=data,
        expiry_data=expiry_data,
        stats=stats,
        dates=dates,
        selected_date=selected_date,
        selected_expiry=selected_expiry,
        chart_data=chart_data,
        underlying=underlying,  # ✅ numeric float
        atm=atm,                # ✅ correct closest strike
    )


# ==============================
# 📈 API endpoint for mini stock chart (price data)
# ==============================
@stock_bp.route('/api/stock-chart/<ticker>')
def api_stock_chart(ticker):
    days = int(request.args.get('days', 90))
    try:
        chart_data = get_stock_chart_data(ticker, days)
        return jsonify({"success": True, "data": chart_data})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
