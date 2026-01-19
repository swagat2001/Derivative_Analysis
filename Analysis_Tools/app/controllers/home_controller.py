"""
HOME CONTROLLER
Landing page for Goldmine - ScanX style
"""

from flask import Blueprint, jsonify, render_template

from ..models.live_indices_model import get_live_indices as get_live_broker_data
from ..models.stock_model import get_filtered_tickers
from .dashboard_controller import get_live_indices

home_bp = Blueprint("home", __name__)


@home_bp.route("/")
def home():
    """Landing page - ScanX style"""
    return render_template(
        "home.html",
        indices=get_live_indices(),
        stock_list=get_filtered_tickers(),
        stock_symbol=None,
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
