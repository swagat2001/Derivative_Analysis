# controllers/stock_controller.py
from flask import Blueprint, render_template, request, jsonify
from ..models.stock_model import (
    get_available_dates,
    get_stock_detail_data,
    get_stock_expiry_data,
    get_stock_stats,
    get_stock_chart_data
)

stock_bp = Blueprint('stock', __name__)

@stock_bp.route('/stock/<ticker>')
def stock_detail(ticker):
    dates = get_available_dates()
    selected_date = request.args.get('date', dates[0] if dates else None)

    data = []
    expiry_data = []
    stats = {}

    if selected_date:
        # Detailed option chain rows (used by stock_detail_option_chain.html)
        data = get_stock_detail_data(ticker, selected_date)

        # Expiry summary (used by stock_detail_expiry.html)
        expiry_data = get_stock_expiry_data(ticker, selected_date)

        # Aggregated stats / gauges (used by stock_detail_stats.html)
        stats = get_stock_stats(ticker, selected_date)

    return render_template(
        'stock_detail.html',
        ticker=ticker,
        data=data,
        expiry_data=expiry_data,
        stats=stats,
        dates=dates,
        selected_date=selected_date
    )


# API endpoint used by stock_detail_chart.html fetch(`/api/stock-chart/${ticker}?days=90`)
@stock_bp.route('/api/stock-chart/<ticker>')
def api_stock_chart(ticker):
    days = int(request.args.get('days', 90))
    try:
        chart_data = get_stock_chart_data(ticker, days)
        return jsonify({"success": True, "data": chart_data})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

