"""
HOME CONTROLLER
Landing page for Goldmine - ScanX style

Updated: 2026-02-02 - Added FII/DII API endpoint
"""

from datetime import datetime, timedelta

from flask import Blueprint, jsonify, render_template


import os
import json
from ..models.insights_model import get_fii_dii_summary, get_market_stats, get_52_week_analysis, get_insights_dates, get_nifty_pe
from ..models.live_indices_model import get_live_indices
from ..models.stock_model import get_filtered_tickers
# from .dashboard_controller import get_live_indices # Removed to avoid conflict

home_bp = Blueprint("home", __name__)

def get_live_fii_dii():
    """Helper to get latest FII/DII summary (Live from Spot or DB Fallback)"""
    # 1. Try reading from spot_data JSON
    try:
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        spot_file = os.path.join(base_dir, "spot_data", "Data", "FiiDiiSpot.json")

        if os.path.exists(spot_file):
            with open(spot_file, "r") as f:
                data = json.load(f)
                return {
                    "fii_net": data.get("fii_net", 0),
                    "dii_net": data.get("dii_net", 0)
                }
    except Exception as e:
        print(f"[WARNING] Read FiiDiiSpot.json failed: {e}")

    # 2. Fallback to Database
    try:
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")

        summary = get_fii_dii_summary(start_date, end_date)
        return {"fii_net": summary.get("total_fii_net", 0), "dii_net": summary.get("total_dii_net", 0)}
    except Exception as e:
        print(f"Error fetching FII/DII: {e}")
        return {"fii_net": 0, "dii_net": 0}


@home_bp.route("/")
def home():
    """Landing page - ScanX style"""
    # Get FII/DII data for initial render
    fii_dii_data = get_live_fii_dii()

    # Get Market Statistics
    try:
        # Get latest available date from insights
        dates = get_insights_dates()
        if dates:
            latest_date = dates[0]
        else:
            latest_date = datetime.now().strftime("%Y-%m-%d")

        market_stats = get_market_stats(latest_date) or {}

        # Get 52 Week High/Low counts
        week52_data = get_52_week_analysis(latest_date) or {}

        # Combine into a single stats object for the template
        final_stats = {
            "date": latest_date,
            "stock_traded": market_stats.get("total", 0),
            "advances": market_stats.get("advances", 0),
            "declines": market_stats.get("declines", 0),
            "unchanged": market_stats.get("unchanged", 0),
            "upper_circuits": market_stats.get("upper_circuits", 0),
            "lower_circuits": market_stats.get("lower_circuits", 0),
            "week52_high": len(week52_data.get("at_high", [])), # Using 'at_high' as strict 52w high
            "week52_low": len(week52_data.get("at_low", [])),    # Using 'at_low' as strict 52w low
        }

        # Calculate percentages for breadth bar
        total_breadth = final_stats["advances"] + final_stats["declines"]
        final_stats["adv_pct"] = (final_stats["advances"] / total_breadth * 100) if total_breadth > 0 else 50
        final_stats["dec_pct"] = (final_stats["declines"] / total_breadth * 100) if total_breadth > 0 else 50

        # Add formatted width strings to avoid template linter errors
        final_stats["adv_width"] = f"{final_stats['adv_pct']:.1f}%"
        final_stats["dec_width"] = f"{final_stats['dec_pct']:.1f}%"


    except Exception as e:
        print(f"Error fetching market stats: {e}")
        final_stats = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "stock_traded": 0, "advances": 0, "declines": 0, "unchanged": 0,
            "upper_circuits": 0, "lower_circuits": 0, "week52_high": 0, "week52_low": 0,
            "adv_pct": 50, "dec_pct": 50,
            "adv_width": "50.0%", "dec_width": "50.0%"
        }

    # Get Nifty PE
    nifty_pe = get_nifty_pe() or {"pe": 0, "status": "N/A", "color": "gray", "min": 15, "max": 30}

    # Calculate PE position for the range bar
    pe_min = 15
    pe_max = 30
    pe_val = nifty_pe.get("pe", 0)
    pe_clamped = min(pe_max, max(pe_min, pe_val))
    nifty_pe["pe_pos"] = ((pe_clamped - pe_min) / (pe_max - pe_min)) * 100
    nifty_pe["pe_left_style"] = f"{nifty_pe['pe_pos']:.1f}%"

    return render_template(
        "home.html",
        indices=get_live_indices(),
        stock_list=get_filtered_tickers(),
        stock_symbol=None,
        fii_dii=fii_dii_data,
        market_stats=final_stats,
        nifty_pe=nifty_pe,
    )


@home_bp.route("/api/live-indices")
def api_live_indices():
    """API endpoint for real-time indices data from broker websocket"""
    try:
        data = get_live_indices()
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
