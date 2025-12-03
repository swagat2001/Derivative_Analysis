"""
TECHNICAL INDICATORS SCREENER CONTROLLER
Displays RSI, MACD, SMA, Bollinger Bands, ADX analysis
"""

from flask import Blueprint, render_template, request, jsonify
from flask_caching import Cache
from ....models.screener_model import get_technical_indicators_screeners, get_available_dates_for_new_screeners
from ....models.stock_model import get_filtered_tickers
from ....controllers.dashboard_controller import get_live_indices

technical_bp = Blueprint('technical_screener', __name__, url_prefix='/screener/technical-indicators')

# Initialize cache
cache = Cache(config={
    'CACHE_TYPE': 'simple',
    'CACHE_DEFAULT_TIMEOUT': 3600
})

@cache.memoize(timeout=0)  # DISABLED for testing
def get_technical_data_formatted(selected_date):
    """
    Single data fetch - cached for 1 hour.
    """
    try:
        screener_data = get_technical_indicators_screeners(selected_date)
        return screener_data if screener_data else None
    except Exception as e:
        print(f"[ERROR] get_technical_data_formatted: {e}")
        return None


@technical_bp.route('/')
def technical_screener():
    """
    Main technical indicators screener page
    """
    try:
        dates = get_available_dates_for_new_screeners()
        if not dates:
            return render_template(
                'screener/technical_indicators.html',
                dates=[],
                selected_date=None,
                screener_data={},
                indices=get_live_indices(),
                stock_list=get_filtered_tickers(),
                stock_symbol=None,
                error="No dates available"
            )
        
        selected_date = request.args.get('date', dates[0])
        screener_data = get_technical_data_formatted(selected_date)
        
        if not screener_data:
            return render_template(
                'screener/technical_indicators.html',
                dates=dates,
                selected_date=selected_date,
                screener_data={},
                indices=get_live_indices(),
                stock_list=get_filtered_tickers(),
                stock_symbol=None,
                error=f"No data available for {selected_date}"
            )
        
        return render_template(
            'screener/technical_indicators.html',
            dates=dates,
            selected_date=selected_date,
            screener_data=screener_data,
            indices=get_live_indices(),
            stock_list=get_filtered_tickers(),
            stock_symbol=None
        )
        
    except Exception as e:
        print(f"[ERROR] technical_screener route: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Screener failed: {str(e)}"}), 500


@technical_bp.route('/api/data')
def api_technical_data():
    """
    API endpoint for AJAX data fetching
    """
    try:
        selected_date = request.args.get('date')
        if not selected_date:
            return jsonify({"error": "Date parameter required"}), 400
        
        screener_data = get_technical_data_formatted(selected_date)
        
        if not screener_data:
            return jsonify({"error": "No data available"}), 404
        
        return jsonify({
            "success": True,
            "date": selected_date,
            "data": screener_data
        })
        
    except Exception as e:
        print(f"[ERROR] api_technical_data: {e}")
        return jsonify({"error": str(e)}), 500
