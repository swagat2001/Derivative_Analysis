"""
HOME CONTROLLER
Landing page for Goldmine - ScanX style

Updated: 2026-02-02 - Added FII/DII API endpoint
Updated: 2026-02-11 - Added Market Breadth API endpoint
Updated: 2026-02-11 - Added dynamic sample stocks display
Updated: 2026-02-19 - Added NSE API endpoints so charts always show data
"""

from datetime import datetime, timedelta

from flask import Blueprint, jsonify, render_template, request


import os
import json
from ..models.insights_model import get_fii_dii_summary, get_market_stats, get_52_week_analysis, get_insights_dates, get_nifty_pe
from ..models.market_breadth_model import get_latest_market_breadth
from ..models.live_indices_model import get_live_indices
from ..models.stock_model import get_filtered_tickers
from ..models.homepage_model import get_homepage_sample_stocks
from ..models.nse_indices_model import get_nse_index_data, get_nse_chart_data, get_sensex_chart_data
# from .dashboard_controller import get_live_indices # Removed to avoid conflict

home_bp = Blueprint("home", __name__)

def get_live_fii_dii():
    """Helper to get latest FII/DII summary (Live from Spot or DB Fallback)"""
    # 2. Fallback to Database
    try:
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")

        summary = get_fii_dii_summary(start_date, end_date)
        return {
            "fii_net": summary.get("total_fii_net", 0),
            "dii_net": summary.get("total_dii_net", 0),
            "date": summary.get("latest_date", end_date)
        }
    except Exception as e:
        print(f"Error fetching FII/DII: {e}")
        return {"fii_net": 0, "dii_net": 0, "date": datetime.now().strftime("%Y-%m-%d")}


@home_bp.route("/")
def home():
    """Landing page - ScanX style"""
    # Get FII/DII data for initial render
    fii_dii_data = get_live_fii_dii()
    fii_dii_date = fii_dii_data.get("date")

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
            # Get advances/declines from market_breadth table (full NSE market)
            breadth_data = get_latest_market_breadth()
            latest_date = breadth_data.get("date")

            # Check if we should trigger a one-time EOD scrape
            # Logic: If it's after 3:30 PM IST on a weekday, and the DB date is not today
            from .home_market_check import is_market_hours_ist, get_ist_now
            ist_now = get_ist_now()
            is_open, reason = is_market_hours_ist()

            # If market is closed but it's a weekday after 3:30 PM
            is_after_market = ist_now.hour > 15 or (ist_now.hour == 15 and ist_now.minute >= 30)
            is_weekday = ist_now.weekday() < 5
            today_str = ist_now.strftime("%Y-%m-%d")

            if not is_open and is_weekday and is_after_market and latest_date != today_str:
                print(f"[INFO] Triggering EOD Market Breadth scrape for {today_str}...")
                try:
                    from ..services.market_breadth_scraper import get_market_breadth
                    eod_data = get_market_breadth()
                    if eod_data and not eod_data.get("error") and (eod_data.get("advances", 0) > 0 or eod_data.get("declines", 0) > 0):
                        from ..models.market_breadth_model import save_market_breadth
                        save_market_breadth(eod_data)
                        breadth_data = eod_data
                        latest_date = today_str
                        print(f"[INFO] EOD Market Breadth saved successfully.")
                except Exception as scrape_err:
                    print(f"[ERROR] EOD Scrape failed: {scrape_err}")

            if not latest_date:
                latest_date = today_str

            # Check if breadth_data has valid data
            if breadth_data.get("total", 0) > 0:
                # Use breadth data for advances/declines (full market)
                market_stats = {
                    "total": breadth_data.get("total", 0),
                    "advances": breadth_data.get("advances", 0),
                    "declines": breadth_data.get("declines", 0),
                    "unchanged": breadth_data.get("unchanged", 0),
                    "date": latest_date
                }
            else:
                # Fallback to get_market_stats (F&O stocks only) if market_breadth table is empty
                print("[WARN] market_breadth table empty, falling back to F&O market stats")
                dates = get_insights_dates()
                if dates:
                    latest_date = dates[0]
                market_stats = get_market_stats(latest_date) or {}
                market_stats["date"] = latest_date
                print(f"[INFO] Using F&O market stats - Advances: {market_stats.get('advances', 0)}, Declines: {market_stats.get('declines', 0)}")

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
    nifty_pe = get_nifty_pe() or {"pe": 0, "status": "N/A", "color": "gray", "min": 15, "max": 30, "date": "N/A"}
    nifty_pe_date = nifty_pe.get("date")

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
        fii_dii_date=fii_dii_date,
        nifty_pe_date=nifty_pe_date
    )


@home_bp.route("/api/live-indices")
def api_live_indices():
    """API endpoint for real-time indices data from broker websocket"""
    try:
        data = get_live_indices()
        return jsonify(data), 200
    except Exception as e:
        return jsonify({"success": True, "message": "Using fallback data", "indices": {}}), 200


@home_bp.route("/api/nse-indices")
def api_nse_indices():
    """
    Always-on: current index prices fetched directly from NSE public API.
    Works 24/7 – not limited to Upstox streamer hours.
    Returns the same shape as /api/live-indices so the JS can merge them.
    """
    try:
        indices = get_nse_index_data()
        return jsonify({"success": True, "indices": indices}), 200
    except Exception as e:
        return jsonify({"success": False, "message": str(e), "indices": {}}), 200


@home_bp.route("/api/nse-chart/<index_name>")
def api_nse_chart(index_name: str):
    """
    Always-on: 1-day intraday chart series for *index_name*.
    - SENSEX: fetched from BSE via yfinance (^BSESN) — NSE doesn't carry SENSEX.
    - All others: fetched from NSE graph API.
    index_name must match an app key: nifty50, banknifty, sensex,
    niftyfin, niftynext50, nifty100, indiavix.
    Returns: { series: [[epoch_ms, price], ...], open, high, low, close, change, percent }
    """
    try:
        key = index_name.lower()
        if key == "sensex":
            data = get_sensex_chart_data()
        else:
            data = get_nse_chart_data(key)
        if "error" in data:
            return jsonify({"success": False, "message": data["error"]}), 200
        return jsonify({"success": True, **data}), 200
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 200


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


@home_bp.route("/api/market-breadth-history")
def api_market_breadth_history():
    """API endpoint for historical market breadth (Advances/Declines) chart data."""
    try:
        days = int(request.args.get("days", 30))
        from Analysis_Tools.Database.Cash.market_breadth_eod import get_breadth_history
        history = get_breadth_history(days)
        return jsonify({"success": True, "data": history}), 200
    except Exception as e:
        # Fallback: try reading directly from DB
        try:
            from ..models.db_config import engine_cash
            from sqlalchemy import text as sa_text
            days = int(request.args.get("days", 30))
            with engine_cash.connect() as conn:
                rows = conn.execute(sa_text("""
                    SELECT date::text, advances, declines, unchanged, total
                    FROM market_breadth_eod
                    ORDER BY date DESC LIMIT :d
                """), {"d": days}).fetchall()
            data = [{"date": r[0], "advances": r[1], "declines": r[2],
                     "unchanged": r[3], "total": r[4]} for r in reversed(rows)]
            return jsonify({"success": True, "data": data}), 200
        except Exception as e2:
            return jsonify({"success": False, "message": str(e2), "data": []}), 200


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
        print(f"[NSE API]  Advances: {advances}, Declines: {declines}, Timestamp: '{timestamp}'")
        print(f"[NSE API] Response keys: {list(data.keys())}")

        # Save to database for persistence
        try:
            from ..models.market_breadth_model import save_market_breadth
            from .home_market_check import get_ist_now
            ist_now = get_ist_now()

            save_market_breadth({
                "advances": advances,
                "declines": declines,
                "unchanged": 0, # NSE API doesn't provide unchanged in this endpoint easily
                "timestamp": timestamp if timestamp else ist_now.strftime("%Y-%m-%d %H:%M:%S"),
                "date": ist_now.strftime("%Y-%m-%d")
            })
        except Exception as db_err:
            print(f"[ERROR] Failed to persist breadth data: {db_err}")

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
