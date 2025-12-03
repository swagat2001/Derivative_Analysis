"""
FUTURES OI ANALYSIS SCREENER CONTROLLER
Displays CME/NME/FME expiry analysis with exposure percentiles
"""

from flask import Blueprint, render_template, request, jsonify
from flask_caching import Cache
from ....models.screener_model import get_futures_oi_screeners, get_available_dates_for_new_screeners
from ....models.stock_model import get_filtered_tickers
from ....controllers.dashboard_controller import get_live_indices

futures_oi_bp = Blueprint('futures_oi', __name__, url_prefix='/screener/futures-oi')

# Initialize cache
cache = Cache(config={
    'CACHE_TYPE': 'simple',
    'CACHE_DEFAULT_TIMEOUT': 3600
})

@cache.memoize(timeout=0)  # DISABLED - set to 0 to force fresh data
def get_futures_data_formatted(selected_date):
    """
    Single data fetch - cached for 1 hour.
    """
    try:
        print(f"[DEBUG] Calling get_futures_oi_screeners with date: {selected_date}")
        screener_data = get_futures_oi_screeners(selected_date)
        print(f"[DEBUG] Returned type: {type(screener_data)}")
        print(f"[DEBUG] Returned keys: {list(screener_data.keys()) if screener_data else 'NONE'}")
        if screener_data:
            for key in list(screener_data.keys())[:2]:
                print(f"[DEBUG]   {key}: {len(screener_data[key])} items")
        return screener_data if screener_data else None
    except Exception as e:
        print(f"[ERROR] get_futures_data_formatted: {e}")
        import traceback
        traceback.print_exc()
        return None


@futures_oi_bp.route('/')
def futures_oi_screener():
    """
    Main futures OI analysis screener page
    """
    try:
        dates = get_available_dates_for_new_screeners()
        if not dates:
            return render_template(
                'screener/futures_oi.html',
                dates=[],
                selected_date=None,
                screener_data={},
                indices=get_live_indices(),
                stock_list=get_filtered_tickers(),
                stock_symbol=None,
                error="No dates available"
            )
        
        selected_date = request.args.get('date', dates[0])
        screener_data = get_futures_data_formatted(selected_date)
        
        print(f"[DEBUG] Route received screener_data keys: {list(screener_data.keys()) if screener_data else 'NONE'}")
        if screener_data:
            for key in list(screener_data.keys())[:3]:
                print(f"[DEBUG]   {key}: {len(screener_data[key])} items")
        
        if not screener_data:
            return render_template(
                'screener/futures_oi.html',
                dates=dates,
                selected_date=selected_date,
                screener_data={},
                indices=get_live_indices(),
                stock_list=get_filtered_tickers(),
                stock_symbol=None,
                error=f"No data available for {selected_date}"
            )
        
        return render_template(
            'screener/futures_oi.html',
            dates=dates,
            selected_date=selected_date,
            screener_data=screener_data,
            indices=get_live_indices(),
            stock_list=get_filtered_tickers(),
            stock_symbol=None
        )
        
    except Exception as e:
        print(f"[ERROR] futures_oi_screener route: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Screener failed: {str(e)}"}), 500


@futures_oi_bp.route('/api/data')
def api_futures_oi_data():
    """
    API endpoint for AJAX data fetching
    """
    try:
        selected_date = request.args.get('date')
        if not selected_date:
            return jsonify({"error": "Date parameter required"}), 400
        
        screener_data = get_futures_data_formatted(selected_date)
        
        if not screener_data:
            return jsonify({"error": "No data available"}), 404
        
        return jsonify({
            "success": True,
            "date": selected_date,
            "data": screener_data
        })
        
    except Exception as e:
        print(f"[ERROR] api_futures_oi_data: {e}")
        return jsonify({"error": str(e)}), 500
