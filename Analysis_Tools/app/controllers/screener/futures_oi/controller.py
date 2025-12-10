"""
FUTURES OI ANALYSIS SCREENER CONTROLLER
Displays CME/NME/FME expiry analysis with exposure percentiles
"""

from flask import Blueprint, jsonify, render_template, request
from flask_caching import Cache

from ....controllers.dashboard_controller import get_live_indices
from ....models.screener_model import get_available_dates_for_new_screeners, get_futures_oi_screeners
from ....models.stock_model import get_filtered_tickers

futures_oi_bp = Blueprint("futures_oi", __name__, url_prefix="/screener/futures-oi")

# Initialize cache
cache = Cache(config={"CACHE_TYPE": "simple", "CACHE_DEFAULT_TIMEOUT": 3600})


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


def transform_futures_data(screener_data):
    """Transform screener data structure to match template expectations"""
    if not screener_data:
        return {}

    # Group all stocks by ticker first, then organize by expiry type
    all_tickers = {}

    # Collect all unique tickers from all categories
    for category_data in screener_data.values():
        for item in category_data:
            ticker = item.get("stock_name", "")
            if ticker and ticker not in all_tickers:
                all_tickers[ticker] = item

    # Now organize by expiry type (CME, NME, FME)
    cme_data = []
    nme_data = []
    fme_data = []

    for ticker, data in all_tickers.items():
        # Create CME entry
        if data.get("cme_expiry_date"):
            cme_data.append(
                {
                    "ticker": ticker,
                    "underlying_price": data.get("underlying_price", 0),
                    "expiry_date": data.get("cme_expiry_date"),
                    "expiry_price": data.get("cme_expiry_price", 0),
                    "expiry_oi": data.get("cme_oi", 0),
                    "oi_percentile": data.get("cme_exposure_percentile", 0),
                    "price_percentile": data.get("cme_exposure_percentile", 0),
                    "signal": "BULLISH"
                    if data.get("cme_exposure_percentile", 0) > 70
                    else ("BEARISH" if data.get("cme_exposure_percentile", 0) < 30 else "NEUTRAL"),
                }
            )

        # Create NME entry
        if data.get("nme_expiry_date"):
            nme_data.append(
                {
                    "ticker": ticker,
                    "underlying_price": data.get("underlying_price", 0),
                    "expiry_date": data.get("nme_expiry_date"),
                    "expiry_price": data.get("nme_expiry_price", 0),
                    "expiry_oi": data.get("nme_oi", 0),
                    "oi_percentile": data.get("nme_exposure_percentile", 0),
                    "price_percentile": data.get("nme_exposure_percentile", 0),
                    "signal": "BULLISH"
                    if data.get("nme_exposure_percentile", 0) > 70
                    else ("BEARISH" if data.get("nme_exposure_percentile", 0) < 30 else "NEUTRAL"),
                }
            )

        # Create FME entry
        if data.get("fme_expiry_date"):
            fme_data.append(
                {
                    "ticker": ticker,
                    "underlying_price": data.get("underlying_price", 0),
                    "expiry_date": data.get("fme_expiry_date"),
                    "expiry_price": data.get("fme_expiry_price", 0),
                    "expiry_oi": data.get("fme_oi", 0),
                    "oi_percentile": data.get("fme_exposure_percentile", 0),
                    "price_percentile": data.get("fme_exposure_percentile", 0),
                    "signal": "BULLISH"
                    if data.get("fme_exposure_percentile", 0) > 70
                    else ("BEARISH" if data.get("fme_exposure_percentile", 0) < 30 else "NEUTRAL"),
                }
            )

    return {
        "CME": sorted(cme_data, key=lambda x: x["oi_percentile"], reverse=True),
        "NME": sorted(nme_data, key=lambda x: x["oi_percentile"], reverse=True),
        "FME": sorted(fme_data, key=lambda x: x["oi_percentile"], reverse=True),
    }


@futures_oi_bp.route("/")
def futures_oi_screener():
    """
    Main futures OI analysis screener page
    """
    try:
        dates = get_available_dates_for_new_screeners()
        if not dates:
            return render_template(
                "screener/futures_oi/index.html",
                dates=[],
                selected_date=None,
                futures_data={},
                indices=get_live_indices(),
                stock_list=get_filtered_tickers(),
                stock_symbol=None,
                error="No dates available",
            )

        selected_date = request.args.get("date", dates[0])
        raw_data = get_futures_data_formatted(selected_date)
        futures_data = transform_futures_data(raw_data)

        print(f"[DEBUG] Transformed futures_data keys: {list(futures_data.keys()) if futures_data else 'NONE'}")
        if futures_data:
            for key in list(futures_data.keys())[:3]:
                print(f"[DEBUG]   {key}: {len(futures_data[key])} items")

        if not futures_data:
            return render_template(
                "screener/futures_oi/index.html",
                dates=dates,
                selected_date=selected_date,
                futures_data={},
                indices=get_live_indices(),
                stock_list=get_filtered_tickers(),
                stock_symbol=None,
                error=f"No data available for {selected_date}",
            )

        return render_template(
            "screener/futures_oi/index.html",
            dates=dates,
            selected_date=selected_date,
            futures_data=futures_data,
            indices=get_live_indices(),
            stock_list=get_filtered_tickers(),
            stock_symbol=None,
        )

    except Exception as e:
        print(f"[ERROR] futures_oi_screener route: {e}")
        import traceback

        traceback.print_exc()
        return jsonify({"error": f"Screener failed: {str(e)}"}), 500


@futures_oi_bp.route("/api/data")
def api_futures_oi_data():
    """
    API endpoint for AJAX data fetching
    """
    try:
        selected_date = request.args.get("date")
        if not selected_date:
            return jsonify({"error": "Date parameter required"}), 400

        raw_data = get_futures_data_formatted(selected_date)
        futures_data = transform_futures_data(raw_data)

        if not futures_data:
            return jsonify({"error": "No data available"}), 404

        return jsonify({"success": True, "date": selected_date, "data": futures_data})

    except Exception as e:
        print(f"[ERROR] api_futures_oi_data: {e}")
        return jsonify({"error": str(e)}), 500
