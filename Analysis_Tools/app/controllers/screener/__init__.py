"""
Main Screener Controller - Landing Page
"""
from flask import Blueprint, render_template

from ...controllers.dashboard_controller import get_live_indices
from ...models.dashboard_model import get_available_dates
from ...models.stock_model import get_filtered_tickers

screener_bp = Blueprint("screener", __name__, url_prefix="/screener")


@screener_bp.route("/")
def screener_landing():
    """Display screener landing page with 2 options"""
    return render_template(
        "screener_landing.html",
        dates=get_available_dates(),
        indices=get_live_indices(),
        stock_list=get_filtered_tickers(),
        stock_symbol=None,
    )
