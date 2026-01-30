# =============================================================
#  INSIGHTS CONTROLLER
#  Purpose: Market Insights Dashboard with:
#  - Heatmap Visualization (with Index filtering like ScanX)
#  - FII/DII Activity
#  - Delivery Data
#  - Market Statistics
#  - 52-Week Analysis
#  - Volume Breakouts
# =============================================================

from datetime import datetime, timedelta

from flask import Blueprint, jsonify, render_template, request
from flask_caching import Cache

from ...controllers.dashboard_controller import get_live_indices
from ...models.index_model import (
    filter_stocks_by_index,
    get_dynamic_indices,
    get_index_info,
    get_index_list,
    get_index_stocks,
)
from ...models.insights_model import (
    clear_insights_cache,
    get_52_week_analysis,
    get_delivery_data,
    get_fii_dii_data,
    get_heatmap_data,
    get_insights_dates,
    get_market_stats,
    get_sector_performance,
    get_volume_breakouts,
)
from ...models.stock_model import get_filtered_tickers

# Blueprint setup
insights_bp = Blueprint("insights", __name__, url_prefix="/insights", template_folder="../../views/insights")

# Cache setup
cache = Cache(config={"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300})


# =============================================================
# MAIN INSIGHTS PAGE
# =============================================================


@insights_bp.route("/")
def insights_page():
    """Main insights dashboard page."""
    dates = get_insights_dates()
    selected_date = request.args.get("date", dates[0] if dates else None)
    active_tab = request.args.get("tab", "heatmap")
    selected_index = request.args.get("index", "all")

    # Get all heatmap data first to build dynamic index list
    all_heatmap_data = get_heatmap_data(selected_date) if selected_date else []
    available_symbols = [s["symbol"] for s in all_heatmap_data]

    # Get dynamic index list based on available stocks
    index_list = get_dynamic_indices(available_symbols)
    index_info = get_index_info(selected_index)

    # Get market stats for header
    market_stats = get_market_stats(selected_date) if selected_date else None

    return render_template(
        "insights/index.html",
        dates=dates,
        selected_date=selected_date,
        active_tab=active_tab,
        market_stats=market_stats,
        indices=get_live_indices(),
        stock_list=get_filtered_tickers(),
        stock_symbol=None,
        index_list=index_list,
        selected_index=selected_index,
        index_info=index_info,
        total_available=len(available_symbols),
    )


# =============================================================
# API ENDPOINTS
# =============================================================


@insights_bp.route("/api/heatmap")
def api_heatmap():
    """API endpoint for heatmap data with index filtering."""
    selected_date = request.args.get("date")
    comparison_date = request.args.get("compare_date")
    sort_by = request.args.get("sort", "change")
    selected_index = request.args.get("index", "all")

    if not selected_date:
        dates = get_insights_dates()
        selected_date = dates[0] if dates else None

    if not selected_date:
        return jsonify({"error": "No data available"}), 404

    # Get ALL heatmap data first
    all_heatmap_data = get_heatmap_data(selected_date, comparison_date)

    # Build dynamic index list
    available_symbols = [s["symbol"] for s in all_heatmap_data]
    dynamic_indices = get_dynamic_indices(available_symbols)

    # Filter by index
    heatmap_data = filter_stocks_by_index(all_heatmap_data, selected_index)

    # Recalculate sector data based on filtered stocks
    sector_data = []
    if heatmap_data:
        sector_groups = {}
        for stock in heatmap_data:
            sector = stock.get("sector", "Others")
            if sector not in sector_groups:
                sector_groups[sector] = []
            sector_groups[sector].append(stock)

        for sector, stocks in sector_groups.items():
            avg_change = sum(s["change_pct"] for s in stocks) / len(stocks) if stocks else 0
            total_turnover = sum(s.get("turnover", 0) for s in stocks)
            sector_data.append(
                {
                    "sector": sector,
                    "avg_change": round(avg_change, 2),
                    "total_turnover": total_turnover,
                    "stock_count": len(stocks),
                    "stocks": [s["symbol"] for s in stocks[:5]],
                }
            )
        sector_data.sort(key=lambda x: x["avg_change"], reverse=True)

    # Calculate market stats for filtered data
    if heatmap_data:
        advances = len([s for s in heatmap_data if s["change_pct"] > 0])
        declines = len([s for s in heatmap_data if s["change_pct"] < 0])
        unchanged = len([s for s in heatmap_data if s["change_pct"] == 0])
        total = len(heatmap_data)
        avg_change = sum(s["change_pct"] for s in heatmap_data) / total if total > 0 else 0
        adv_dec_ratio = round(advances / declines, 2) if declines > 0 else advances

        market_stats = {
            "advances": advances,
            "declines": declines,
            "unchanged": unchanged,
            "total": total,
            "avg_change": round(avg_change, 2),
            "adv_dec_ratio": adv_dec_ratio,
            "market_sentiment": "Bullish" if adv_dec_ratio > 1.5 else "Bearish" if adv_dec_ratio < 0.67 else "Neutral",
        }
    else:
        market_stats = None

    # Sort data
    if sort_by == "change":
        heatmap_data.sort(key=lambda x: x["change_pct"], reverse=True)
    elif sort_by == "volume":
        heatmap_data.sort(key=lambda x: x["volume"], reverse=True)
    elif sort_by == "turnover":
        heatmap_data.sort(key=lambda x: x["turnover"], reverse=True)

    # Get index info
    index_info = get_index_info(selected_index)

    return jsonify(
        {
            "success": True,
            "date": selected_date,
            "index": selected_index,
            "index_name": index_info["name"],
            "stocks": heatmap_data,
            "sectors": sector_data,
            "stats": market_stats,
            "total_stocks": len(heatmap_data),
            "total_available": len(all_heatmap_data),
            "indices": dynamic_indices,
        }
    )


@insights_bp.route("/api/indices")
def api_indices():
    """API endpoint to get dynamic list of available indices."""
    selected_date = request.args.get("date")

    if not selected_date:
        dates = get_insights_dates()
        selected_date = dates[0] if dates else None

    if not selected_date:
        return jsonify({"success": True, "indices": get_index_list()})

    # Get all heatmap data to build dynamic index list
    all_heatmap_data = get_heatmap_data(selected_date)
    available_symbols = [s["symbol"] for s in all_heatmap_data]

    return jsonify(
        {"success": True, "indices": get_dynamic_indices(available_symbols), "total_stocks": len(available_symbols)}
    )


@insights_bp.route("/api/fii-dii")
def api_fii_dii():
    """API endpoint for FII/DII data."""
    end_date = request.args.get("end_date")
    days = int(request.args.get("days", 30))

    if not end_date:
        dates = get_insights_dates()
        end_date = dates[0] if dates else None

    if not end_date:
        return jsonify({"error": "No data available"}), 404

    # Calculate start date
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    start_dt = end_dt - timedelta(days=days)
    start_date = start_dt.strftime("%Y-%m-%d")

    fii_dii_data = get_fii_dii_data(start_date, end_date)

    # Calculate summary - ensure all values are integers
    total_ce_change = int(sum(d["ce_oi_change"] for d in fii_dii_data))
    total_pe_change = int(sum(d["pe_oi_change"] for d in fii_dii_data))
    net_total = total_pe_change - total_ce_change

    return jsonify(
        {
            "success": True,
            "start_date": start_date,
            "end_date": end_date,
            "data": fii_dii_data,
            "summary": {
                "total_ce_oi_change": total_ce_change,
                "total_pe_oi_change": total_pe_change,
                "net_index_oi": net_total,
                "overall_sentiment": "Bullish" if net_total > 0 else "Bearish" if net_total < 0 else "Neutral",
            },
        }
    )


@insights_bp.route("/api/delivery")
def api_delivery():
    """API endpoint for delivery data."""
    selected_date = request.args.get("date")
    min_pct = float(request.args.get("min_pct", 50))
    sort_by = request.args.get("sort", "delivery_pct")
    selected_index = request.args.get("index", "all")

    if not selected_date:
        dates = get_insights_dates()
        selected_date = dates[0] if dates else None

    if not selected_date:
        return (
            jsonify({"success": False, "error": "No data available", "message": "No dates available in database"}),
            404,
        )

    print(f"[API] Fetching delivery data for {selected_date}, min_pct={min_pct}, index={selected_index}")

    delivery_data = get_delivery_data(selected_date, min_pct)

    if not delivery_data:
        print(f"[API] No delivery data returned for {selected_date}")
        return jsonify(
            {
                "success": True,
                "date": selected_date,
                "data": [],
                "summary": {
                    "total_stocks": 0,
                    "avg_delivery_pct": 0,
                    "high_delivery_count": 0,
                },
                "message": "No delivery data available for this date. The cash database may not have data for this date.",
            }
        )

    # Filter by index
    delivery_data = filter_stocks_by_index(delivery_data, selected_index)

    # Sort data
    if sort_by == "delivery_pct":
        delivery_data.sort(key=lambda x: x["delivery_pct"], reverse=True)
    elif sort_by == "volume":
        delivery_data.sort(key=lambda x: x["volume"], reverse=True)

    # Calculate summary
    if delivery_data:
        avg_delivery = sum(d["delivery_pct"] for d in delivery_data) / len(delivery_data)
        high_delivery = len([d for d in delivery_data if d["delivery_pct"] >= 70])
    else:
        avg_delivery = 0
        high_delivery = 0

    print(f"[API] Returning {len(delivery_data)} stocks with delivery data")

    return jsonify(
        {
            "success": True,
            "date": selected_date,
            "data": delivery_data[:50],
            "summary": {
                "total_stocks": len(delivery_data),
                "avg_delivery_pct": round(avg_delivery, 2),
                "high_delivery_count": high_delivery,
            },
        }
    )


@insights_bp.route("/api/market-stats")
def api_market_stats():
    """API endpoint for market statistics."""
    selected_date = request.args.get("date")

    if not selected_date:
        dates = get_insights_dates()
        selected_date = dates[0] if dates else None

    if not selected_date:
        return jsonify({"error": "No data available"}), 404

    stats = get_market_stats(selected_date)

    return jsonify(
        {
            "success": True,
            "date": selected_date,
            "stats": stats,
        }
    )


@insights_bp.route("/api/52-week")
def api_52_week():
    """API endpoint for 52-week high/low analysis."""
    selected_date = request.args.get("date")
    selected_index = request.args.get("index", "all")

    if not selected_date:
        dates = get_insights_dates()
        selected_date = dates[0] if dates else None

    if not selected_date:
        return jsonify({"error": "No data available"}), 404

    analysis = get_52_week_analysis(selected_date)

    # Filter by index if needed
    if selected_index != "all":
        index_stocks = get_index_stocks(selected_index)
        if index_stocks:
            index_stocks_upper = set(s.upper() for s in index_stocks)
            for key in ["at_high", "near_high", "at_low", "near_low"]:
                if key in analysis:
                    analysis[key] = [s for s in analysis[key] if s.get("symbol", "").upper() in index_stocks_upper]

    return jsonify(
        {
            "success": True,
            "date": selected_date,
            "data": analysis,
            "summary": {
                "at_52w_high": len(analysis.get("at_high", [])),
                "near_52w_high": len(analysis.get("near_high", [])),
                "at_52w_low": len(analysis.get("at_low", [])),
                "near_52w_low": len(analysis.get("near_low", [])),
            },
        }
    )


@insights_bp.route("/api/volume-breakouts")
def api_volume_breakouts():
    """API endpoint for volume breakout stocks."""
    selected_date = request.args.get("date")
    multiplier = float(request.args.get("multiplier", 2.0))
    selected_index = request.args.get("index", "all")

    if not selected_date:
        dates = get_insights_dates()
        selected_date = dates[0] if dates else None

    if not selected_date:
        return jsonify({"error": "No data available"}), 404

    breakouts = get_volume_breakouts(selected_date, multiplier)

    # Filter by index
    breakouts = filter_stocks_by_index(breakouts, selected_index)

    return jsonify(
        {
            "success": True,
            "date": selected_date,
            "multiplier": multiplier,
            "data": breakouts,
            "count": len(breakouts),
        }
    )


@insights_bp.route("/api/sector-performance")
def api_sector_performance():
    """API endpoint for sector-wise performance."""
    selected_date = request.args.get("date")

    if not selected_date:
        dates = get_insights_dates()
        selected_date = dates[0] if dates else None

    if not selected_date:
        return jsonify({"error": "No data available"}), 404

    sector_data = get_sector_performance(selected_date)

    return jsonify(
        {
            "success": True,
            "date": selected_date,
            "sectors": sector_data,
        }
    )


# =============================================================
# CACHE MANAGEMENT
# =============================================================


@insights_bp.route("/api/clear-cache")
def api_clear_cache():
    """Clear insights cache (admin only)."""
    clear_insights_cache()
    return jsonify({"success": True, "message": "Cache cleared"})
