"""
HOME CONTROLLER
Landing page for Goldmine - ScanX style

Updated: 2026-02-02 - Added FII/DII API endpoint
"""

from datetime import datetime, timedelta

from flask import Blueprint, jsonify, render_template

from ..models.insights_model import get_fii_dii_summary
from ..models.live_indices_model import get_live_indices as get_live_broker_data
from ..models.stock_model import get_filtered_tickers
from .dashboard_controller import get_live_indices

home_bp = Blueprint("home", __name__)


def get_live_fii_dii():
    """Helper to get latest FII/DII summary"""
    try:
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")

        # Get summary for last few days, but effectively we want the latest available?
        # Summary aggregates. We actually might want the latest DAY.
        # But for the home page, usually we show "Today's" or "Yesterday's" net.
        # Let's get summary for just today? If empty, it returns 0s.
        # Better: Get summary for 1 day window.

        summary = get_fii_dii_summary(start_date, end_date)
        # Map keys to match what home.html expects (fii_net, dii_net)
        return {"fii_net": summary.get("total_fii_net", 0), "dii_net": summary.get("total_dii_net", 0)}
    except Exception as e:
        print(f"Error fetching FII/DII: {e}")
        return {"fii_net": 0, "dii_net": 0}


@home_bp.route("/")
def home():
    """Landing page - ScanX style"""
    # Get FII/DII data for initial render
    fii_dii_data = get_live_fii_dii()

    return render_template(
        "home.html",
        indices=get_live_indices(),
        stock_list=get_filtered_tickers(),
        stock_symbol=None,
        fii_dii=fii_dii_data,
    )


@home_bp.route("/api/live-indices")
def api_live_indices():
    """API endpoint for real-time indices data from broker websocket"""
    try:
        data = get_live_broker_data()
        return jsonify(data), 200

    except Exception as e:
        # Return fallback data structure
        return jsonify({"success": True, "message": "Using fallback data", "indices": {}}), 200


@home_bp.route("/api/live-fii-dii")
def api_live_fii_dii():
    """API endpoint for real-time FII/DII data"""
    try:
        data = get_live_fii_dii()
        return jsonify(data), 200

    except Exception as e:
        # Return fallback data structure
        return (
            jsonify(
                {
                    "success": False,
                    "message": str(e),
                    "fii_net": 0,
                    "dii_net": 0,
                    "total_net": 0,
                }
            ),
            200,
        )
