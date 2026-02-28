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
from sqlalchemy import text

from ...controllers.dashboard_controller import get_live_indices
from ...models.db_config import engine, engine_cash
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
    get_enhanced_heatmap_data,
    get_fii_derivatives_data,
    get_fii_dii_data,
    get_heatmap_data,
    get_insights_dates,
    get_market_stats,
    get_nifty50_data,
    get_sector_performance,
    get_stock_delivery_history,
    get_volume_breakouts,
)
from ...models.stock_model import get_filtered_tickers
from ...models.pf_matrix_model import generate_rs_matrix_html, generate_stock_rs_matrix_html, generate_category_rs_matrix_html

# Blueprint setup
insights_bp = Blueprint("insights", __name__, url_prefix="/neev", template_folder="../../views/insights")

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

    # Get all heatmap data first (UNFILTERED) to build complete dynamic index list
    # We want index_list to show ALL possible indices, not just F&O ones
    all_heatmap_data = get_heatmap_data(selected_date, filter_fo=False) if selected_date else []
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
    period = request.args.get("period", "1D")
    sort_by = request.args.get("sort", "change")
    selected_index = request.args.get("index", "all")

    if not selected_date:
        dates = get_insights_dates()
        selected_date = dates[0] if dates else None

    if not selected_date:
        return jsonify({"error": "No data available"}), 404

    # Get heatmap data
    # If index is 'all', show all stocks (do not filter by F&O)
    should_filter_fo = False
    all_heatmap_data = get_heatmap_data(selected_date, period=period, comparison_date=comparison_date, filter_fo=should_filter_fo)

    # NEW: Fallback mechanism if no data found for selected date (DB lag scenario)
    if not all_heatmap_data and not comparison_date:
        # Check if we have data for a previous date
        from ...models.db_config import engine_cash
        from sqlalchemy import text
        try:
            with engine_cash.connect() as conn:
                # Find max date <= selected_date in daily_market_heatmap
                fallback_query = text("SELECT MAX(date) FROM daily_market_heatmap WHERE date <= :date")
                fallback_date = conn.execute(fallback_query, {"date": selected_date}).scalar()

                if fallback_date and str(fallback_date) != str(selected_date):
                    print(f"[INFO] Fallback: No heatmap data for {selected_date}, using {fallback_date}")
                    selected_date = str(fallback_date)
                    all_heatmap_data = get_heatmap_data(selected_date, period=period, filter_fo=should_filter_fo)
        except Exception as e:
            print(f"[ERROR] Heatmap fallback failed: {e}")

    # Build dynamic index list
    # Build dynamic index list for the UI dropdown (always based on ALL market stocks for consistency)
    if should_filter_fo:
        # We need the full list to build indices correctly
        full_market_data = get_heatmap_data(selected_date, filter_fo=False)
        symbols_for_indices = [s["symbol"] for s in full_market_data]
    else:
        symbols_for_indices = [s["symbol"] for s in all_heatmap_data]

    dynamic_indices = get_dynamic_indices(symbols_for_indices)

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

    # Get all heatmap data to build dynamic index list (UNFILTERED)
    all_heatmap_data = get_heatmap_data(selected_date, filter_fo=False)
    available_symbols = [s["symbol"] for s in all_heatmap_data]

    return jsonify(
        {"success": True, "indices": get_dynamic_indices(available_symbols), "total_stocks": len(available_symbols)}
    )


@insights_bp.route("/api/fii-dii")
def api_fii_dii():
    days = int(request.args.get("days", 30))
    period = request.args.get("period", "daily")
    custom_start = request.args.get("start_date")
    end_date = request.args.get("end_date")
    if not end_date:
        # Get latest date that has ACTUAL FII/DII data, not Cash DB dates
        try:
            with engine.connect() as conn:
                query = text("""
                    SELECT MAX(trade_date)::text
                    FROM fii_dii_activity
                """)
                end_date = conn.execute(query).scalar()
        except Exception:
            dates = get_insights_dates()
            end_date = dates[0] if dates else None

    if not end_date:
        return jsonify({"error": "No data available"}), 404

    # Calculate start date based on period or custom range
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    if custom_start:
        start_date = custom_start
    elif period == "yearly":
        start_dt = end_dt - timedelta(days=365)
        start_date = start_dt.strftime("%Y-%m-%d")
    elif period == "monthly":
        start_dt = end_dt - timedelta(days=90)  # ~3 months of data
        start_date = start_dt.strftime("%Y-%m-%d")
    elif period == "weekly":
        start_dt = end_dt - timedelta(days=60)  # ~2 months of weekly data
        start_date = start_dt.strftime("%Y-%m-%d")
    else:
        start_dt = end_dt - timedelta(days=days)
        start_date = start_dt.strftime("%Y-%m-%d")

    fii_dii_data = get_fii_dii_data(start_date, end_date)

    # Fetch derivatives data
    derivatives_data = get_fii_derivatives_data(start_date, end_date)

    # Fetch Nifty 50 index data for overlay
    nifty50_data = get_nifty50_data(start_date, end_date)

    # Check if we have real FII/DII data OR Derivatives Data
    if not fii_dii_data and not derivatives_data:
        return (
            jsonify(
                {
                    "success": False,
                    "error": "No FII/DII data available",
                    "message": "FII/DII data table is empty. Please run: python fii_dii_update_database.py",
                    "start_date": start_date,
                    "end_date": end_date,
                    "period": period,
                    "data": [],
                    "derivatives": {},
                    "nifty50": {},
                    "summary": {
                        "total_fii_net": 0,
                        "total_dii_net": 0,
                        "combined_net": 0,
                        "overall_sentiment": "No Data",
                    },
                }
            ),
            200,
        )

    # Aggregate data based on period
    aggregated_data = _aggregate_fii_dii_data(fii_dii_data, period)
    aggregated_derivatives = _aggregate_derivatives_data(derivatives_data, period)
    aggregated_nifty = _aggregate_nifty_data(nifty50_data, period)

    # Calculate summary from aggregated data
    total_fii_buy = sum(d["fii_buy_value"] for d in aggregated_data)
    total_fii_sell = sum(d["fii_sell_value"] for d in aggregated_data)
    total_fii_net = sum(d["fii_net_value"] for d in aggregated_data)
    total_dii_buy = sum(d["dii_buy_value"] for d in aggregated_data)
    total_dii_sell = sum(d["dii_sell_value"] for d in aggregated_data)
    total_dii_net = sum(d["dii_net_value"] for d in aggregated_data)
    combined_net = total_fii_net + total_dii_net

    # Determine overall sentiment
    if combined_net > 500:
        sentiment = "Bullish"
    elif combined_net < -500:
        sentiment = "Bearish"
    else:
        sentiment = "Neutral"

    return jsonify(
        {
            "success": True,
            "start_date": start_date,
            "end_date": end_date,
            "period": period,
            "data": aggregated_data,
            "derivatives": aggregated_derivatives,
            "nifty50": aggregated_nifty,
            "summary": {
                "total_fii_buy": round(total_fii_buy, 2),
                "total_fii_sell": round(total_fii_sell, 2),
                "total_fii_net": round(total_fii_net, 2),
                "total_dii_buy": round(total_dii_buy, 2),
                "total_dii_sell": round(total_dii_sell, 2),
                "total_dii_net": round(total_dii_net, 2),
                "combined_net": round(combined_net, 2),
                "overall_sentiment": sentiment,
                "days_count": len(fii_dii_data),
                "periods_count": len(aggregated_data),
            },
        }
    )


def _aggregate_fii_dii_data(data, period):
    """Aggregate FII/DII data based on time period."""
    if period == "daily" or not data:
        return data

    import pandas as pd

    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])

    if period == "weekly":
        df["period"] = df["date"].dt.to_period("W").apply(lambda x: x.start_time)
    elif period == "monthly":
        df["period"] = df["date"].dt.to_period("M").apply(lambda x: x.start_time)
    elif period == "yearly":
        df["period"] = df["date"].dt.to_period("Y").apply(lambda x: x.start_time)
    else:
        return data

    # Aggregate by period
    agg_df = (
        df.groupby("period")
        .agg(
            {
                "fii_buy_value": "sum",
                "fii_sell_value": "sum",
                "fii_net_value": "sum",
                "dii_buy_value": "sum",
                "dii_sell_value": "sum",
                "dii_net_value": "sum",
                "total_net_value": "sum",
            }
        )
        .reset_index()
    )

    agg_df["date"] = agg_df["period"].dt.strftime("%Y-%m-%d")
    agg_df["sentiment"] = agg_df["total_net_value"].apply(
        lambda x: "Bullish" if x > 100 else ("Bearish" if x < -100 else "Neutral")
    )

    return agg_df.drop(columns=["period"]).to_dict("records")


def _aggregate_derivatives_data(data, period):
    """Aggregate derivatives data based on time period."""
    if period == "daily" or not data:
        return data

    import pandas as pd

    # Flatten the nested structure
    records = []
    for date, items in data.items():
        for item in items:
            records.append({"date": date, **item})

    if not records:
        return data

    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"])

    if period == "weekly":
        df["period"] = df["date"].dt.to_period("W").apply(lambda x: x.start_time)
    elif period == "monthly":
        df["period"] = df["date"].dt.to_period("M").apply(lambda x: x.start_time)
    elif period == "yearly":
        df["period"] = df["date"].dt.to_period("Y").apply(lambda x: x.start_time)
    else:
        return data

    # Aggregate by period and category
    agg_df = (
        df.groupby(["period", "category", "participant_type"])
        .agg(
            {
                "buy_value": "sum",
                "sell_value": "sum",
                "net_value": "sum",
                "oi_value": "last",
                "oi_contracts": "last",
                "oi_long": "last",
                "oi_short": "last",
            }
        )
        .reset_index()
    )

    agg_df["period_str"] = agg_df["period"].dt.strftime("%Y-%m-%d")

    # Rebuild nested structure
    result = {}
    for _, row in agg_df.iterrows():
        period_key = row["period_str"]
        if period_key not in result:
            result[period_key] = []
        result[period_key].append(
            {
                "category": row["category"],
                "participant_type": row["participant_type"],
                "buy_value": float(row["buy_value"]),
                "sell_value": float(row["sell_value"]),
                "net_value": float(row["net_value"]),
                "oi_value": float(row["oi_value"]) if row["oi_value"] else 0,
                "oi_contracts": int(row["oi_contracts"]) if row["oi_contracts"] else 0,
                "oi_long": int(row["oi_long"]) if row["oi_long"] else 0,
                "oi_short": int(row["oi_short"]) if row["oi_short"] else 0,
            }
        )

    return result


def _aggregate_nifty_data(data, period):
    """Aggregate Nifty 50 data based on time period (returns last close for period)."""
    if period == "daily" or not data:
        return data

    import pandas as pd

    records = [{"date": k, "close": v} for k, v in data.items()]
    if not records:
        return data

    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"])

    if period == "weekly":
        df["period"] = df["date"].dt.to_period("W").apply(lambda x: x.start_time)
    elif period == "monthly":
        df["period"] = df["date"].dt.to_period("M").apply(lambda x: x.start_time)
    elif period == "yearly":
        df["period"] = df["date"].dt.to_period("Y").apply(lambda x: x.start_time)
    else:
        return data

    # Take last close for each period
    agg_df = df.sort_values("date").groupby("period").agg({"close": "last"}).reset_index()
    agg_df["period_str"] = agg_df["period"].dt.strftime("%Y-%m-%d")

    return dict(zip(agg_df["period_str"], agg_df["close"]))


@insights_bp.route("/api/delivery")
def api_delivery():
    """API endpoint for delivery data."""
    selected_date = request.args.get("date")
    min_pct = float(request.args.get("min_pct", 50))
    sort_by = request.args.get("sort", "delivery_pct")
    selected_index = request.args.get("index", "all")
    search_query = request.args.get("search", "").strip().upper()

    if not selected_date:
        dates = get_insights_dates()
        selected_date = dates[0] if dates else None

    if not selected_date:
        return (
            jsonify({"success": False, "error": "No data available", "message": "No dates available in database"}),
            404,
        )

    print(f"[API] Fetching delivery data for {selected_date}, min_pct={min_pct}, index={selected_index}, search={search_query}")

    should_filter_fo = False
    # If search_query is provided, we fetch everything first and then filter
    fetch_min_pct = min_pct if not search_query else 0
    delivery_data = get_delivery_data(selected_date, fetch_min_pct, filter_fo=should_filter_fo)

    # NEW: Fallback mechanism if no data found for selected date (DB lag scenario)
    if not delivery_data and not search_query:
        # Check if we have data for a previous date
        from ...models.db_config import engine_cash
        from sqlalchemy import text
        try:
            with engine_cash.connect() as conn:
                # Find max date <= selected_date in daily_delivery_data
                fallback_query = text("SELECT MAX(date) FROM daily_delivery_data WHERE date <= :date")
                fallback_date = conn.execute(fallback_query, {"date": selected_date}).scalar()

                if fallback_date and str(fallback_date) != str(selected_date):
                    print(f"[INFO] Fallback: No delivery data for {selected_date}, using {fallback_date}")
                    selected_date = str(fallback_date)
                    delivery_data = get_delivery_data(selected_date, fetch_min_pct, filter_fo=should_filter_fo)
        except Exception as e:
            print(f"[ERROR] Delivery data fallback failed: {e}")

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

    # Search filter (if provided)
    if search_query:
        delivery_data = [d for d in delivery_data if search_query in d["symbol"].upper()]

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

    # If searching, return all matches (up to 100), otherwise return top 50
    limit = 100 if search_query else 50
    return jsonify(
        {
            "success": True,
            "date": selected_date,
            "data": delivery_data[:limit],
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

    # NEW: Fallback mechanism if no data found for selected date (DB lag scenario)
    has_data = any(len(v) > 0 for v in analysis.values())
    if not has_data:
        # Check if we have data for a previous date
        from ...models.db_config import engine_cash
        from sqlalchemy import text
        try:
            with engine_cash.connect() as conn:
                # Find max date <= selected_date in daily_market_heatmap (source for 52-week too)
                fallback_query = text("SELECT MAX(date) FROM daily_market_heatmap WHERE date <= :date")
                fallback_date = conn.execute(fallback_query, {"date": selected_date}).scalar()

                if fallback_date and str(fallback_date) != str(selected_date):
                    print(f"[INFO] Fallback (52-week): No data for {selected_date}, using {fallback_date}")
                    selected_date = str(fallback_date)
                    analysis = get_52_week_analysis(selected_date)
        except Exception as e:
            print(f"[ERROR] 52-week fallback failed: {e}")

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

    # NEW: Fallback mechanism if no data found for selected date (DB lag scenario)
    if not breakouts:
        # Check if we have data for a previous date
        from ...models.db_config import engine_cash
        from sqlalchemy import text
        try:
            with engine_cash.connect() as conn:
                # Find max date <= selected_date in daily_market_heatmap (source for volume breakouts)
                fallback_query = text("SELECT MAX(date) FROM daily_market_heatmap WHERE date <= :date")
                fallback_date = conn.execute(fallback_query, {"date": selected_date}).scalar()

                if fallback_date and str(fallback_date) != str(selected_date):
                    print(f"[INFO] Fallback (Volume): No data for {selected_date}, using {fallback_date}")
                    selected_date = str(fallback_date)
                    breakouts = get_volume_breakouts(selected_date, multiplier)
        except Exception as e:
            print(f"[ERROR] Volume breakout fallback failed: {e}")

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

    # NEW: Fallback mechanism if no data found for selected date (DB lag scenario)
    if not sector_data:
        # Check if we have data for a previous date
        from ...models.db_config import engine_cash
        from sqlalchemy import text
        try:
            with engine_cash.connect() as conn:
                # Find max date <= selected_date in daily_market_heatmap (source for sector perf)
                fallback_query = text("SELECT MAX(date) FROM daily_market_heatmap WHERE date <= :date")
                fallback_date = conn.execute(fallback_query, {"date": selected_date}).scalar()

                if fallback_date and str(fallback_date) != str(selected_date):
                    print(f"[INFO] Fallback (Sector): No data for {selected_date}, using {fallback_date}")
                    selected_date = str(fallback_date)
                    sector_data = get_sector_performance(selected_date)
        except Exception as e:
            print(f"[ERROR] Sector performance fallback failed: {e}")

    return jsonify(
        {
            "success": True,
            "date": selected_date,
            "sectors": sector_data,
        }
    )


# =============================================================
# RS MATRIX API endpoints
# =============================================================

@insights_bp.route("/api/rs-matrix/index-categories")
def api_rs_matrix_index_categories():
    """
    Return all indices grouped by category from index_constituents table.
    Used to populate the RS-matrix dropdown.
    Category order: Indices Eligible in Derivatives > Broad Market > Sectoral > Strategy & Thematic > Fixed Income > Other
    """
    CATEGORY_ORDER = [
        "Indices Eligible in Derivatives",
        "Broad Market Indices",
        "Sectoral Indices",
        "Thematic Indices",
        "Strategy Indices",
        "Fixed Income Indices",
        "Other Indices",
    ]
    try:
        with engine_cash.connect() as conn:
            rows = conn.execute(text("""
                SELECT DISTINCT
                    index_name,
                    index_key,
                    COALESCE(index_category, 'Other Indices') AS index_category
                FROM index_constituents
                WHERE index_name IS NOT NULL AND index_name != ''
                ORDER BY index_name
            """)).fetchall()

        if not rows:
            # Fallback: return hardcoded minimal set
            return jsonify({
                "success": True,
                "categories": [
                    {
                        "category": "Indices Eligible in Derivatives",
                        "indices": [
                            {"name": "NIFTY 50",                 "key": "NIFTY50"},
                            {"name": "NIFTY NEXT 50",            "key": "NIFTYNEXT50"},
                            {"name": "NIFTY BANK",               "key": "NIFTYBANK"},
                            {"name": "NIFTY FINANCIAL SERVICES", "key": "NIFTYFINANCIALSERVICES"},
                            {"name": "NIFTY MIDCAP SELECT",      "key": "NIFTYMIDCAPSELECT"},
                        ]
                    },
                ]
            })

        # Group by category
        from collections import defaultdict
        grouped = defaultdict(list)
        seen = set()  # avoid duplicate index_names
        for row in rows:
            index_name, index_key, category = row[0], row[1], row[2]
            if index_name not in seen:
                seen.add(index_name)
                grouped[category].append({"name": index_name, "key": index_key})

        # Sort within each category alphabetically
        for cat in grouped:
            grouped[cat].sort(key=lambda x: x["name"])

        # Build ordered response
        result = []
        for cat in CATEGORY_ORDER:
            if cat in grouped:
                result.append({"category": cat, "indices": grouped[cat]})
        # Append any leftover categories not in our order list
        for cat, indices in grouped.items():
            if cat not in CATEGORY_ORDER:
                result.append({"category": cat, "indices": indices})

        return jsonify({"success": True, "categories": result})

    except Exception as e:
        import traceback; traceback.print_exc()
        # If index_category column doesn't exist yet (old schema), return fallback
        return jsonify({
            "success": True,
            "categories": [
                {
                    "category": "Indices Eligible in Derivatives",
                    "indices": [
                        {"name": "NIFTY 50",                 "key": "NIFTY50"},
                        {"name": "NIFTY NEXT 50",            "key": "NIFTYNEXT50"},
                        {"name": "NIFTY BANK",               "key": "NIFTYBANK"},
                        {"name": "NIFTY FINANCIAL SERVICES", "key": "NIFTYFINANCIALSERVICES"},
                        {"name": "NIFTY MIDCAP SELECT",      "key": "NIFTYMIDCAPSELECT"},
                    ]
                },
            ],
            "_note": "Run index_constituents_db_scraper.py to populate full category data"
        })


@insights_bp.route("/api/rs-matrix/ad-ratio")
def api_rs_matrix_ad_ratio():
    """
    Compute Advance/Decline ratio for a specific index on the latest date.
    Uses daily_market_heatmap filtered by index constituents.
    """
    index_name = request.args.get("index_name", type=str)
    if not index_name:
        return jsonify({"success": False, "error": "index_name is required"})

    try:
        import re
        def norm(s):
            return re.sub(r'[^A-Z0-9]', '', str(s).upper())

        from ...models.index_model import get_index_list, get_index_stocks
        idx_key = None
        norm_input = norm(index_name)
        for info in get_index_list():
            if norm(info.get("name", "")) == norm_input or norm(info.get("key", "")) == norm_input:
                idx_key = info.get("key", "")
                break

        if not idx_key:
            return jsonify({"success": False, "error": f"Unknown index: {index_name}"})

        stocks = get_index_stocks(idx_key)
        if not stocks:
            return jsonify({"success": False, "error": "No constituents found"})

        stock_list_str = str(tuple(stocks)) if len(stocks) > 1 else f"('{stocks[0]}')"

        with engine_cash.connect() as conn:
            latest_date = conn.execute(text(
                "SELECT MAX(date) FROM daily_market_heatmap"
            )).scalar()

            if not latest_date:
                return jsonify({"success": False, "error": "No heatmap data"})

            rows = conn.execute(text(f"""
                SELECT symbol, change_pct, close, volume
                FROM daily_market_heatmap
                WHERE date = :dt AND symbol IN {stock_list_str}
            """), {"dt": latest_date}).fetchall()

        if not rows:
            return jsonify({"success": False, "error": "No constituent data for latest date"})

        advances  = sum(1 for r in rows if (r[1] or 0) > 0)
        declines  = sum(1 for r in rows if (r[1] or 0) < 0)
        unchanged = sum(1 for r in rows if (r[1] or 0) == 0)
        total     = len(rows)
        ad_ratio  = round(advances / declines, 2) if declines > 0 else float(advances)

        sorted_rows = sorted(rows, key=lambda r: r[1] or 0)
        losers  = [{"symbol": r[0], "change_pct": round(r[1] or 0, 2), "close": round(float(r[2] or 0), 2)} for r in sorted_rows[:5]]
        gainers = [{"symbol": r[0], "change_pct": round(r[1] or 0, 2), "close": round(float(r[2] or 0), 2)} for r in sorted_rows[-5:][::-1]]

        sentiment = "Bullish" if ad_ratio > 1.2 else ("Bearish" if ad_ratio < 0.8 else "Neutral")

        return jsonify({
            "success":   True,
            "index":     index_name,
            "date":      str(latest_date),
            "advances":  advances,
            "declines":  declines,
            "unchanged": unchanged,
            "total":     total,
            "ad_ratio":  ad_ratio,
            "sentiment": sentiment,
            "gainers":   gainers,
            "losers":    losers,
        })

    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"success": False, "error": str(e)})


@insights_bp.route("/api/rs-matrix")
def api_rs_matrix():
    """API endpoint for Point & Figure RS Matrix."""
    box_pct = request.args.get("box_pct", 0.5, type=float)
    cache_key = f"index_{box_pct}"

    try:
        query = text("SELECT html_content FROM rs_matrix_cache WHERE cache_key = :cache_key")
        with engine_cash.connect() as conn:
            result = conn.execute(query, {"cache_key": cache_key}).scalar()

        if result:
            return jsonify({
                "success": True,
                "box_pct": box_pct,
                "html": result
            })
    except Exception as e:
        print(f"[ERROR] Cache read failed: {e}")

    # Fallback to live generation if cache misses or fails
    html_content = generate_rs_matrix_html(box_pct)
    return jsonify({
        "success": True,
        "box_pct": box_pct,
        "html": html_content
    })

@insights_bp.route("/api/rs-matrix/stock")
def api_rs_matrix_stock():
    """API endpoint for Stock Point & Figure RS Matrix within an Index."""
    box_pct = request.args.get("box_pct", 0.5, type=float)
    index_name = request.args.get("index_name", type=str)

    if not index_name:
        return jsonify({"success": False, "error": "index_name is required"})

    cache_key = f"stock_{index_name}_{box_pct}"

    try:
        query = text("SELECT html_content FROM rs_matrix_cache WHERE cache_key = :cache_key")
        with engine_cash.connect() as conn:
            result = conn.execute(query, {"cache_key": cache_key}).scalar()

        if result:
            return jsonify({
                "success": True,
                "box_pct": box_pct,
                "index_name": index_name,
                "html": result
            })
    except Exception as e:
        print(f"[ERROR] Cache read failed: {e}")

    # Fallback to live generation if cache misses
    html_content = generate_stock_rs_matrix_html(index_name, box_pct)
    return jsonify({
        "success": True,
        "box_pct": box_pct,
        "index_name": index_name,
        "html": html_content
    })


@insights_bp.route("/api/rs-matrix/indices")
def api_rs_matrix_indices():
    """
    Index-vs-Index P&F RS Matrix (with DB caching to avoid recomputation).
    Params: category (optional), box_pct (default 0.5)
    """
    box_pct  = request.args.get("box_pct", 0.5, type=float)
    category = request.args.get("category", "", type=str).strip()

    # Sanitise category for a safe cache key
    import re as _re
    cat_slug  = _re.sub(r'[^A-Za-z0-9]', '_', category) if category else "all"
    cache_key = f"indices_{cat_slug}_{box_pct}"

    # ── Try DB cache first ─────────────────────────────────────────────────────
    try:
        with engine_cash.connect() as conn:
            result = conn.execute(
                text("SELECT html_content FROM rs_matrix_cache WHERE cache_key = :k"),
                {"k": cache_key}
            ).scalar()
        if result:
            return jsonify({"success": True, "category": category or "all",
                            "box_pct": box_pct, "html": result, "cached": True})
    except Exception:
        pass   # cache miss or table not yet created — fall through

    # ── Compute (slow path) ────────────────────────────────────────────────────
    html_content = generate_category_rs_matrix_html(category, box_pct)

    # ── Write to DB cache ──────────────────────────────────────────────────────
    try:
        with engine_cash.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS rs_matrix_cache (
                    cache_key    VARCHAR(200) PRIMARY KEY,
                    html_content TEXT
                )
            """))
            conn.execute(text("""
                INSERT INTO rs_matrix_cache (cache_key, html_content)
                VALUES (:k, :h)
                ON CONFLICT (cache_key) DO UPDATE SET
                    html_content = EXCLUDED.html_content
            """), {"k": cache_key, "h": html_content})
    except Exception as e:
        print(f"[WARN] Could not write indices RS matrix to cache: {e}")

    return jsonify({"success": True, "category": category or "all",
                    "box_pct": box_pct, "html": html_content})


@insights_bp.route("/api/rs-matrix/ad-ratio-batch")
def api_rs_matrix_ad_ratio_batch():
    """
    Batch A/D ratio endpoint — returns A/D for many indices in ONE DB query
    instead of N individual calls.  Replaces the 140 parallel front-end fetches.

    Params:
      - category: optional, filter to this category's indices (empty = all)
      - date:     optional trading date (default = latest available)
    """
    category = request.args.get("category", "", type=str).strip()
    date_str = request.args.get("date", "", type=str).strip()

    try:
        # Step 1 — resolve which index_keys belong to this category
        if category:
            with engine_cash.connect() as conn:
                rows = conn.execute(text("""
                    SELECT DISTINCT index_name, index_key
                    FROM index_constituents
                    WHERE index_category = :cat
                    ORDER BY index_name
                """), {"cat": category}).fetchall()
        else:
            with engine_cash.connect() as conn:
                rows = conn.execute(text("""
                    SELECT DISTINCT index_name, index_key
                    FROM index_constituents
                    WHERE index_name IS NOT NULL
                    ORDER BY index_name
                """)).fetchall()

        if not rows:
            return jsonify({"success": True, "indices": []})

        # Step 2 — build {index_key: index_name} map
        key_to_name = {r[1]: r[0] for r in rows}

        # Step 3 — single query: advances, declines, unchanged per index
        if date_str:
            date_clause = "AND date = :dt"
            params = {"dt": date_str}
        else:
            # Use the latest available date in the table
            with engine_cash.connect() as conn:
                latest = conn.execute(text("SELECT MAX(date) FROM daily_market_heatmap")).scalar()
            date_clause = "AND date = :dt"
            params = {"dt": str(latest) if latest else "2099-01-01"}

        # We need to look up constituents in daily_market_heatmap
        index_keys = list(key_to_name.keys())
        placeholders = ", ".join([f"'{k}'" for k in index_keys])

        query = text(f"""
            SELECT
                ic.index_key,
                ic.index_name,
                COUNT(*) FILTER (WHERE h.change_pct > 0)  AS advances,
                COUNT(*) FILTER (WHERE h.change_pct < 0)  AS declines,
                COUNT(*) FILTER (WHERE h.change_pct = 0)  AS unchanged,
                COUNT(*) AS total
            FROM index_constituents ic
            JOIN daily_market_heatmap h
                ON ic.symbol = h.symbol {date_clause}
            WHERE ic.index_key IN ({placeholders})
            GROUP BY ic.index_key, ic.index_name
            ORDER BY ic.index_name
        """)

        with engine_cash.connect() as conn:
            result = conn.execute(query, params).fetchall()

        # Step 4 — format response
        indices_data = []
        for row in result:
            idx_key, idx_name, adv, dec, unch, total = row
            adv   = adv   or 0
            dec   = dec   or 0
            unch  = unch  or 0
            total = total or 1
            ad_ratio = round(adv / dec, 2) if dec else (999.0 if adv else 0.0)
            if ad_ratio > 1.5:
                sentiment = "Bullish"
            elif ad_ratio < 0.7:
                sentiment = "Bearish"
            else:
                sentiment = "Neutral"
            indices_data.append({
                "index_name": idx_name,
                "index_key":  idx_key,
                "advances":   int(adv),
                "declines":   int(dec),
                "unchanged":  int(unch),
                "total":      int(total),
                "ad_ratio":   ad_ratio,
                "sentiment":  sentiment,
                "success":    True,
            })

        return jsonify({"success": True, "category": category or "all", "indices": indices_data})

    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"success": False, "error": str(e), "indices": []})


@insights_bp.route("/api/stock-delivery-history")
def api_stock_delivery_history():
    """API endpoint for 21-day historical delivery data of a stock."""
    symbol = request.args.get("symbol")
    days = int(request.args.get("days", 21))

    if not symbol:
        return jsonify({"success": False, "error": "Symbol is required"}), 400

    history = get_stock_delivery_history(symbol, days)

    return jsonify({
        "success": True,
        "symbol": symbol.upper(),
        "days": len(history),
        "data": history
    })


# =============================================================
# CACHE MANAGEMENT
# =============================================================


@insights_bp.route("/api/clear-cache")
def api_clear_cache():
    """Clear insights cache (admin only)."""
    clear_insights_cache()
    return jsonify({"success": True, "message": "Cache cleared"})
