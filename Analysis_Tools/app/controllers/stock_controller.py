# controllers/stock_controller.py
from flask import Blueprint, render_template, request, jsonify
from ..models.stock_model import (
    get_available_dates,
    get_all_tickers,
    get_stock_detail_data,
    get_stock_expiry_data,
    get_stock_stats,
    get_stock_chart_data
)

stock_bp = Blueprint('stock', __name__)

@stock_bp.route('/stock/<ticker>')
def stock_detail(ticker):
    """
    Stock detail page with server-side filtering
    Query params: date, expiry
    """
    # Get all available dates and tickers
    dates = get_available_dates()
    all_symbols = get_all_tickers()
    
    # Get parameters from query string or use defaults
    selected_date = request.args.get('date', dates[0] if dates else None)
    selected_expiry = request.args.get('expiry', None)  # Will be set later if None

    data = []
    expiry_data = []
    stats = {}

    if selected_date:
        # Get expiry summary first (used by stock_detail_expiry.html)
        expiry_data = get_stock_expiry_data(ticker, selected_date)
        
        # If no expiry specified in URL, use first/nearest expiry
        if not selected_expiry and expiry_data and len(expiry_data) > 0:
            selected_expiry = expiry_data[0]['expiry']
        
        # Detailed option chain rows - filtered by selected expiry
        data = get_stock_detail_data(ticker, selected_date, selected_expiry)

        # Aggregated stats / gauges for the selected expiry
        stats = get_stock_stats(ticker, selected_date, selected_expiry)
        
        # Calculate average IV from option chain data if available
        if data:
            iv_values = []
            for row in data:
                if row.get('IV') is not None and row['IV'] > 0:
                    iv_values.append(row['IV'])
            
            if iv_values:
                avg_iv = sum(iv_values) / len(iv_values)
                # Scale if in 0-1 range
                if avg_iv < 1:
                    avg_iv = avg_iv * 100
                stats['avg_iv'] = round(avg_iv, 2)
            else:
                stats['avg_iv'] = 0
        else:
            stats['avg_iv'] = 0

    return render_template(
        'stock_detail.html',
        ticker=ticker,
        all_symbols=all_symbols,
        data=data,
        expiry_data=expiry_data,
        stats=stats,
        dates=dates,
        selected_date=selected_date,
        selected_expiry=selected_expiry
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
