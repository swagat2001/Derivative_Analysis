"""
Signal Analysis Controller
Handles bullish/bearish signal display with filtering and detailed breakdown

NOTE: Signal computation is centralized in app.services.signal_service
      This controller imports from there to ensure consistency across the app.
"""

from flask import Blueprint, jsonify, render_template, request
from flask_caching import Cache

from ....controllers.dashboard_controller import get_live_indices
from ....models.dashboard_model import get_available_dates
from ....models.stock_model import get_filtered_tickers

# Import from centralized signal service (SINGLE SOURCE OF TRUTH)
from ....services.signal_service import compute_signals_with_breakdown

signal_analysis_bp = Blueprint("signal_analysis", __name__, url_prefix="/screener/signal-analysis")

# Initialize cache
cache = Cache(config={"CACHE_TYPE": "simple", "CACHE_DEFAULT_TIMEOUT": 3600})


@cache.memoize(timeout=3600)
def get_signal_data_formatted(selected_date):
    """
    Get formatted signal data for the signal analysis page.

    This is a cached wrapper around the centralized signal service.
    The actual computation is done in app.services.signal_service
    """
    return compute_signals_with_breakdown(selected_date)


def _apply_sorting(signals_list, sort_by, sort_order, signal_filter):
    """
    Apply sorting to signals list based on column and order

    Args:
        signals_list: list of signal dictionaries
        sort_by: 'symbol' or 'strength'
        sort_order: 'asc' or 'desc'
        signal_filter: 'all', 'bullish', 'bearish', 'neutral'

    Returns:
        Sorted list
    """
    if sort_by == "symbol":
        # Sort by ticker name (alphabetically)
        signals_list.sort(key=lambda x: x["ticker"].lower(), reverse=(sort_order == "desc"))
    elif sort_by == "strength":
        # Sort based on active filter
        if signal_filter == "bullish":
            # Sort by bullish count only
            signals_list.sort(key=lambda x: x["bullish_count"], reverse=(sort_order == "desc"))
        elif signal_filter == "bearish":
            # Sort by bearish count only
            signals_list.sort(key=lambda x: x["bearish_count"], reverse=(sort_order == "desc"))
        else:
            # For 'all' and 'neutral' - sort by net score
            signals_list.sort(key=lambda x: x["score"], reverse=(sort_order == "desc"))
    else:
        # Default: sort by net strength (score), descending
        signals_list.sort(key=lambda x: x["score"], reverse=True)

    return signals_list


@signal_analysis_bp.route("/")
def signal_analysis():
    """Display signal analysis page"""
    try:
        dates = get_available_dates()
        selected_date = request.args.get("date", dates[0] if dates else None)
        signal_filter = request.args.get("filter", "all")
        sort_by = request.args.get("sort", "strength")
        sort_order = request.args.get("order", "desc")

        if not selected_date:
            return jsonify({"error": "No dates available"}), 404

        # Get signals from centralized service
        signals_dict = get_signal_data_formatted(selected_date)

        if not signals_dict:
            signals_dict = {}

        # Convert dict to list with score calculation
        signals_list = []
        for ticker, data in signals_dict.items():
            score = data["bullish_count"] - data["bearish_count"]

            signals_list.append(
                {
                    "ticker": ticker,
                    "signal": data["signal"],
                    "bullish_count": data["bullish_count"],
                    "bearish_count": data["bearish_count"],
                    "score": score,
                    "bullish_categories": data["bullish_categories"],
                    "bearish_categories": data["bearish_categories"],
                }
            )

        # Apply filter
        if signal_filter == "bullish":
            signals_list = [s for s in signals_list if s["signal"] == "BULLISH"]
        elif signal_filter == "bearish":
            signals_list = [s for s in signals_list if s["signal"] == "BEARISH"]
        elif signal_filter == "neutral":
            signals_list = [s for s in signals_list if s["signal"] == "NEUTRAL"]

        # Apply sorting
        signals_list = _apply_sorting(signals_list, sort_by, sort_order, signal_filter)

        return render_template(
            "screener/signal_analysis/index.html",
            dates=dates,
            selected_date=selected_date,
            indices=get_live_indices(),
            signals=signals_list,
            signal_filter=signal_filter,
            sort_by=sort_by,
            sort_order=sort_order,
            stock_list=get_filtered_tickers(),
            stock_symbol=None,
        )

    except Exception as e:
        print(f"[ERROR] signal_analysis(): {e}")
        import traceback

        traceback.print_exc()
        return jsonify({"error": f"Signal analysis failed: {str(e)}"}), 500
