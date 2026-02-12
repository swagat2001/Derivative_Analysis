"""
HOME CONTROLLER
Landing page for Goldmine - ScanX style

Updated: 2026-02-02 - Added FII/DII API endpoint
Updated: 2026-02-11 - Added Market Breadth API endpoint
Updated: 2026-02-11 - Added dynamic sample stocks display
"""

from datetime import datetime, timedelta

from flask import Blueprint, jsonify, render_template


import os
import json
from ..models.insights_model import get_fii_dii_summary, get_market_stats, get_52_week_analysis, get_insights_dates, get_nifty_pe
from ..models.live_indices_model import get_live_indices
from ..models.stock_model import get_filtered_tickers
from ..models.homepage_model import get_homepage_sample_stocks
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
        # 1. Try Live JSON Data first
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        stats_file = os.path.join(base_dir, "spot_data", "Data", "MarketStats.json")

        market_stats = {}
        used_live_data = False

        if os.path.exists(stats_file):
            try:
                with open(stats_file, 'r') as f:
                    live_stats = json.load(f)

                    # Parse timestamp - Handle multiple formats
                    timestamp_str = live_stats['last_updated']
                    last_updated = None
                    for fmt in ("%Y-%m-%d %H:%M:%S", "%d-%b-%Y %H:%M", "%d-%b-%Y %H:%M:%S"):
                        try:
                            last_updated = datetime.strptime(timestamp_str, fmt)
                            break
                        except ValueError:
                            continue

                    if not last_updated:
                        print(f"Error parsing date: {timestamp_str}")
                        raise ValueError("Unknown date format")

                    # Check if data is recent (e.g., within 10 minutes)
                    if (datetime.now() - last_updated).total_seconds() < 600:
                         market_stats = {
                             "total": live_stats.get('stock_traded', 0),
                             "advances": live_stats.get('advances', 0),
                             "declines": live_stats.get('declines', 0),
                             "unchanged": live_stats.get('unchanged', 0),
                             "upper_circuits": live_stats.get('upper_circuits', 0),
                             "lower_circuits": live_stats.get('lower_circuits', 0),
                             "week52_high": live_stats.get('week52_high', 0),
                             "week52_low": live_stats.get('week52_low', 0),
                         }
                         used_live_data = True
                         latest_date = last_updated.strftime("%Y-%m-%d")

            except Exception as e:
                print(f"Error reading MarketStats.json: {e}")

        # 2. Fallback to DB if live data not available
        if not used_live_data:
            dates = get_insights_dates()
            if dates:
                latest_date = dates[0]
            else:
                latest_date = datetime.now().strftime("%Y-%m-%d")

            market_stats = get_market_stats(latest_date) or {}

        # 3. 52 Week Logic
        week52_high_val = market_stats.get("week52_high", 0)
        week52_low_val = market_stats.get("week52_low", 0)

        if not used_live_data:
             week52_data = get_52_week_analysis(latest_date) or {}
             week52_high_val = len(week52_data.get("at_high", []))
             week52_low_val = len(week52_data.get("at_low", []))

        # Combine into a single stats object for the template
        final_stats = {
            "date": latest_date,
            "stock_traded": market_stats.get("total", 0) if not used_live_data else market_stats.get("total", 0),
            "advances": market_stats.get("advances", 0),
            "declines": market_stats.get("declines", 0),
            "unchanged": market_stats.get("unchanged", 0),
            "upper_circuits": market_stats.get("upper_circuits", 0),
            "lower_circuits": market_stats.get("lower_circuits", 0),
            "week52_high": week52_high_val,
            "week52_low": week52_low_val,
            "is_live": used_live_data
        }

        # Calculate percentages for breadth bar
        total_breadth = final_stats["advances"] + final_stats["declines"]
        final_stats["adv_pct"] = (final_stats["advances"] / total_breadth * 100) if total_breadth > 0 else 50
        final_stats["dec_pct"] = (final_stats["declines"] / total_breadth * 100) if total_breadth > 0 else 50

        # Add formatted width strings
        final_stats["adv_width"] = f"{final_stats['adv_pct']:.1f}%"
        final_stats["dec_width"] = f"{final_stats['dec_pct']:.1f}%"

    except Exception as e:
        print(f"Error fetching market stats: {e}")
        final_stats = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "stock_traded": 0, "advances": 0, "declines": 0, "unchanged": 0,
            "upper_circuits": 0, "lower_circuits": 0, "week52_high": 0, "week52_low": 0,
            "adv_pct": 50, "dec_pct": 50,
            "adv_width": "50.0%", "dec_width": "50.0%",
            "is_live": False
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

    # Get sample stocks for homepage display
    sample_stocks = get_homepage_sample_stocks()

    return render_template(
        "home.html",
        indices=get_live_indices(),
        stock_list=get_filtered_tickers(),
        stock_symbol=None,
        fii_dii=fii_dii_data,
        market_stats=final_stats,
        nifty_pe=nifty_pe,
        sample_stocks=sample_stocks,
        fii_dii_date=latest_date
    )


@home_bp.route("/api/live-indices")
def api_live_indices():
    """API endpoint for real-time indices data from broker websocket"""
    try:
        data = get_live_indices()
        return jsonify(data), 200
    except Exception as e:
        return jsonify({"success": True, "message": "Using fallback data", "indices": {}}), 200


@home_bp.route("/api/live-fii-dii")
def api_live_fii_dii():
    """API endpoint for real-time FII/DII data"""
    try:
        data = get_live_fii_dii()
        return jsonify(data), 200
    except Exception as e:
        return jsonify({
            "success": False,
            "message": str(e),
            "fii_net": 0,
            "dii_net": 0,
            "total_net": 0,
        }), 200


@home_bp.route("/api/market-breadth")
def api_market_breadth():
    """API endpoint for market breadth data (Advances/Declines/Unchanged)"""
    try:
        from ..models.market_breadth_model import get_latest_market_breadth
        data = get_latest_market_breadth()
        return jsonify(data), 200
    except Exception as e:
        return jsonify({
            "success": False,
            "message": str(e),
            "advances": 0,
            "declines": 0,
            "unchanged": 0,
            "total": 0
        }), 200


@home_bp.route("/api/advance-decline")
def advance_decline():
    """API endpoint for live NSE advance-decline data"""
    import requests

    base_url = "https://www.nseindia.com"
    api_url = "https://www.nseindia.com/api/live-analysis-advance"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/market-data/advance-decline",
        "Connection": "keep-alive"
    }

    try:
        session = requests.Session()
        session.headers.update(headers)

        # Step 1 - Get cookies
        session.get(base_url, timeout=10)

        # Step 2 - Call API
        response = session.get(api_url, timeout=10)

        if response.status_code != 200:
            return jsonify({
                "success": False,
                "error": f"NSE blocked request",
                "status": response.status_code,
                "advances": 0,
                "declines": 0
            }), 200

        data = response.json()

        # Extract advances, declines, and timestamp from actual NSE structure
        # NSE Structure: {"timestamp": "12-Feb-2026 12:18:36", "advance": {"count": {"Advances": 964, "Declines": 2015}}}

        # Timestamp is at ROOT level
        timestamp = data.get("timestamp", "")

        # Advances/Declines are in nested structure
        advance_data = data.get("advance", {})
        count_data = advance_data.get("count", {})

        advances = count_data.get("Advances", 0)
        declines = count_data.get("Declines", 0)

        # Debug logging
        print(f"[NSE API] ✅ Advances: {advances}, Declines: {declines}, Timestamp: '{timestamp}'")
        print(f"[NSE API] Response keys: {list(data.keys())}")

        return jsonify({
            "success": True,
            "advances": advances,
            "declines": declines,
            "total": advances + declines,
            "timestamp": timestamp
        }), 200

    except requests.exceptions.Timeout:
        return jsonify({
            "success": False,
            "error": "Request timeout",
            "advances": 0,
            "declines": 0
        }), 200
    except KeyError:
        return jsonify({
            "success": False,
            "error": "Unexpected JSON structure",
            "advances": 0,
            "declines": 0
        }), 200
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e),
            "advances": 0,
            "declines": 0
        }), 200
