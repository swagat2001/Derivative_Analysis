# =============================================================
#  SIGNAL SCANNER CONTROLLER
#  Purpose: RSI-based Signal Scanner for F&O
# =============================================================

from datetime import datetime, timedelta

from flask import Blueprint, jsonify, render_template, request
from flask_caching import Cache

from ....controllers.dashboard_controller import get_live_indices
from ....models.signal_scanner_model import (
    clear_scanner_cache,
    filter_signals,
    get_scanner_dates,
    get_scanner_summary,
    run_signal_scanner,
)
from ....models.stock_model import get_filtered_tickers

# Blueprint setup
signal_scanner_bp = Blueprint(
    "signal_scanner",
    __name__,
    url_prefix="/screener/signal-scanner",
    template_folder="../../../views/screener/signal_scanner",
)

# Cache setup
cache = Cache(config={"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 600})

# Store scanner results in memory for filtering
_scanner_results_cache = {}


# =============================================================
# MAIN PAGE
# =============================================================


@signal_scanner_bp.route("/")
def scanner_page():
    """Main signal scanner page."""
    dates = get_scanner_dates()
    selected_date = request.args.get("date", dates[0] if dates else None)

    return render_template(
        "screener/signal_scanner/index.html",
        dates=dates,
        selected_date=selected_date,
        indices=get_live_indices(),
        stock_list=get_filtered_tickers(),
    )


# =============================================================
# API ENDPOINTS
# =============================================================


@signal_scanner_bp.route("/api/scan")
def api_scan():
    """Run the scanner and return results."""
    start_date = request.args.get("start_date")
    days_back = int(request.args.get("days_back", 30))

    if not start_date:
        dates = get_scanner_dates()
        if dates:
            # Calculate start date based on days_back
            latest = datetime.strptime(dates[0], "%Y-%m-%d")
            start_date = (latest - timedelta(days=days_back)).strftime("%Y-%m-%d")
        else:
            return jsonify({"success": False, "error": "No data available"}), 404

    # Run scanner
    try:
        signals = run_signal_scanner(start_date, days_back)
        _scanner_results_cache["signals"] = signals
        _scanner_results_cache["start_date"] = start_date
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

    # Apply filters
    filters = {
        "signal_type": request.args.get("signal_type", "all"),
        "option_type": request.args.get("option_type", "all"),
        "symbol": request.args.get("symbol", ""),
        "sort_by": request.args.get("sort_by", "signal_date"),
        "sort_order": request.args.get("sort_order", "desc"),
    }

    filtered_signals = filter_signals(signals, filters)
    summary = get_scanner_summary(signals)

    return jsonify(
        {
            "success": True,
            "start_date": start_date,
            "signals": filtered_signals[:500],  # Limit results
            "total_count": len(filtered_signals),
            "summary": summary,
        }
    )


@signal_scanner_bp.route("/api/filter")
def api_filter():
    """Filter existing scanner results without re-running."""
    if "signals" not in _scanner_results_cache:
        return jsonify({"success": False, "error": "Run scanner first"}), 400

    signals = _scanner_results_cache["signals"]

    # Apply filters
    filters = {
        "signal_type": request.args.get("signal_type", "all"),
        "option_type": request.args.get("option_type", "all"),
        "symbol": request.args.get("symbol", ""),
        "sort_by": request.args.get("sort_by", "signal_date"),
        "sort_order": request.args.get("sort_order", "desc"),
    }

    filtered_signals = filter_signals(signals, filters)
    summary = get_scanner_summary(filtered_signals)

    return jsonify(
        {
            "success": True,
            "signals": filtered_signals[:500],
            "total_count": len(filtered_signals),
            "summary": summary,
        }
    )


@signal_scanner_bp.route("/api/dates")
def api_dates():
    """Get available dates."""
    dates = get_scanner_dates()
    return jsonify(
        {
            "success": True,
            "dates": dates,
        }
    )


@signal_scanner_bp.route("/api/symbols")
def api_symbols():
    """Get unique symbols from scanner results."""
    if "signals" not in _scanner_results_cache:
        return jsonify({"success": True, "symbols": []})

    signals = _scanner_results_cache["signals"]
    symbols = sorted(set(s["symbol"] for s in signals))

    return jsonify(
        {
            "success": True,
            "symbols": symbols,
            "count": len(symbols),
        }
    )


@signal_scanner_bp.route("/api/clear-cache")
def api_clear_cache():
    """Clear scanner cache."""
    global _scanner_results_cache
    _scanner_results_cache = {}
    clear_scanner_cache()
    return jsonify({"success": True, "message": "Cache cleared"})
