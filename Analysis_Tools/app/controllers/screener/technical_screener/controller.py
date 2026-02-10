"""
TECHNICAL INDICATORS SCREENER CONTROLLER
Displays RSI, MACD, SMA, Bollinger Bands, ADX analysis
"""

from flask import Blueprint, jsonify, render_template, request
from flask_caching import Cache

from ....controllers.dashboard_controller import get_live_indices
from ....models.screener_model import (
    get_all_technical_stocks,
    get_available_dates_for_new_screeners,
    get_technical_indicators_screeners,
)
from ....models.stock_model import get_filtered_tickers
from ....models.technical_screener_model import (
    get_death_crossover_stocks,
    get_golden_crossover_stocks,
    get_momentum_stocks,
    get_r1_breakout_stocks,
    get_r2_breakout_stocks,
    get_r3_breakout_stocks,
    get_rsi_overbought_stocks,
    get_rsi_oversold_stocks,
    get_s1_breakout_stocks,
    get_s2_breakout_stocks,
    get_s3_breakout_stocks,
    get_squeezing_range_stocks,

    get_technical_available_dates,
    # NEW IMPORTS
    get_week1_high_breakout_stocks,
    get_week1_low_breakout_stocks,
    get_week4_high_breakout_stocks,
    get_week4_low_breakout_stocks,
    get_week52_high_breakout_stocks,
    get_week52_low_breakout_stocks,
    get_potential_high_volume_stocks,
    get_unusually_high_volume_stocks,
    # NEW IMPORTS
    get_price_gainers_stocks,
    get_price_losers_stocks,
    get_high_volume_stocks,
)
from ....services.signal_service import compute_signals_simple

technical_screener_bp = Blueprint("technical_screener", __name__, url_prefix="/screener/technical-indicators")

# Initialize cache
cache = Cache(config={"CACHE_TYPE": "simple", "CACHE_DEFAULT_TIMEOUT": 3600})


@cache.memoize(timeout=0)  # DISABLED for testing
def get_technical_data_formatted(selected_date):
    """
    Single data fetch - cached for 1 hour.
    Returns data with proper structure for template
    """
    try:
        screener_data = get_technical_indicators_screeners(selected_date)
        if not screener_data:
            return None

        # Get ALL stocks directly from database for heatmap (not just top 10 per category)
        all_stocks_for_heatmap = get_all_technical_stocks(selected_date)
        screener_data["heatmap"] = all_stocks_for_heatmap if all_stocks_for_heatmap else []

        # Transform all category items to match template expectations
        for category in screener_data:
            if category != "heatmap":
                for item in screener_data[category]:
                    # Map all fields properly
                    item["ticker"] = item.get("stock_name", "")
                    item["underlying_price"] = item.get("close_price", 0)
                    item["rsi_14"] = item.get("rsi", 0)
                    item["adx_14"] = item.get("adx", 0)
                    item["signal"] = (
                        "BULLISH"
                        if item.get("composite_score", 0) > 0
                        else ("BEARISH" if item.get("composite_score", 0) < 0 else "NEUTRAL")
                    )

                    # Add SMA-related fields
                    sma_50 = item.get("sma_50", 0)
                    close_price = item.get("close_price", 0)
                    if sma_50 and sma_50 > 0:
                        item["dist_from_200sma_pct"] = ((close_price - sma_50) / sma_50) * 100
                    else:
                        item["dist_from_200sma_pct"] = 0

        return screener_data
    except Exception as e:
        print(f"[ERROR] get_technical_data_formatted: {e}")
        import traceback

        traceback.print_exc()
        return None


@technical_screener_bp.route("/")
def technical_screener():
    """
    Main technical indicators screener page
    """
    try:
        dates = get_available_dates_for_new_screeners()
        if not dates:
            return render_template(
                "screener/technical_screener/index.html",
                dates=[],
                selected_date=None,
                tech_data={},
                indices=get_live_indices(),
                stock_list=get_filtered_tickers(),
                stock_symbol=None,
                error="No dates available",
            )

        selected_date = request.args.get("date", dates[0])
        tech_data = get_technical_data_formatted(selected_date)

        print(f"[DEBUG] Technical screener data keys: {list(tech_data.keys()) if tech_data else 'NONE'}")
        if tech_data:
            print(f"[DEBUG]   rsi_overbought: {len(tech_data.get('rsi_overbought', []))} items")
            print(f"[DEBUG]   heatmap: {len(tech_data.get('heatmap', []))} items")

        if not tech_data:
            return render_template(
                "screener/technical_screener/index.html",
                dates=dates,
                selected_date=selected_date,
                tech_data={},
                indices=get_live_indices(),
                stock_list=get_filtered_tickers(),
                stock_symbol=None,
                error=f"No data available for {selected_date}",
            )

        return render_template(
            "screener/technical_screener/index.html",
            dates=dates,
            selected_date=selected_date,
            tech_data=tech_data,
            indices=get_live_indices(),
            stock_list=get_filtered_tickers(),
            stock_symbol=None,
        )

    except Exception as e:
        print(f"[ERROR] technical_screener route: {e}")
        import traceback

        traceback.print_exc()
        return jsonify({"error": f"Screener failed: {str(e)}"}), 500


@technical_screener_bp.route("/api/data")
def api_technical_data():
    """
    API endpoint for AJAX data fetching
    """
    try:
        selected_date = request.args.get("date")
        if not selected_date:
            return jsonify({"error": "Date parameter required"}), 400

        tech_data = get_technical_data_formatted(selected_date)

        if not tech_data:
            return jsonify({"error": "No data available"}), 404

        return jsonify({"success": True, "date": selected_date, "data": tech_data})

    except Exception as e:
        print(f"[ERROR] api_technical_data: {e}")
        return jsonify({"error": str(e)}), 500


@technical_screener_bp.route("/golden-crossover")
def golden_crossover():
    """
    Golden Crossover Screener - 50-day SMA crosses above 200-day SMA
    """
    try:
        dates = get_technical_available_dates()
        if not dates:
            return render_template(
                "screener/technical_screener/golden_crossover.html",
                dates=[],
                selected_date=None,
                stocks=[],
                indices=get_live_indices(),
                stock_list=get_filtered_tickers(),
                error="No dates available",
            )

        selected_date = request.args.get("date", dates[-1])  # Most recent date
        stocks = get_golden_crossover_stocks(selected_date, limit=100)

        # Add signal field
        for stock in stocks:
            stock["signal"] = "BULLISH"

        print(f"[DEBUG] Golden Crossover: {len(stocks)} stocks found for {selected_date}")

        return render_template(
            "screener/technical_screener/golden_crossover.html",
            dates=dates,
            selected_date=selected_date,
            stocks=stocks,
            indices=get_live_indices(),
            stock_list=get_filtered_tickers(),
        )

    except Exception as e:
        print(f"[ERROR] golden_crossover route: {e}")
        import traceback

        traceback.print_exc()
        return jsonify({"error": f"Golden Crossover failed: {str(e)}"}), 500


@technical_screener_bp.route("/death-crossover")
def death_crossover():
    """
    Death Crossover Screener - 50-day SMA crosses below 200-day SMA
    """
    try:
        dates = get_technical_available_dates()
        if not dates:
            return render_template(
                "screener/technical_screener/death_crossover.html",
                dates=[],
                selected_date=None,
                stocks=[],
                indices=get_live_indices(),
                stock_list=get_filtered_tickers(),
                error="No dates available",
            )

        selected_date = request.args.get("date", dates[-1])  # Most recent date
        stocks = get_death_crossover_stocks(selected_date, limit=100)

        # Add signal field
        for stock in stocks:
            stock["signal"] = "BEARISH"

        print(f"[DEBUG] Death Crossover: {len(stocks)} stocks found for {selected_date}")

        return render_template(
            "screener/technical_screener/death_crossover.html",
            dates=dates,
            selected_date=selected_date,
            stocks=stocks,
            indices=get_live_indices(),
            stock_list=get_filtered_tickers(),
        )

    except Exception as e:
        print(f"[ERROR] death_crossover route: {e}")
        import traceback

        traceback.print_exc()
        return jsonify({"error": f"Death Crossover failed: {str(e)}"}), 500


# ====== GENERIC SCREENER RENDERER ======
def render_generic_screener(title, desc, icon, signal, signal_color, data_func,
                            table_columns, avg_metric_func=None, avg_metric_label=None):
    """Generic renderer for all new technical screener pages"""
    try:
        dates = get_technical_available_dates()
        if not dates:
            return render_template(
                "screener/technical_screener/generic_screener.html",
                dates=[], selected_date=None, stocks=[],
                screener_title=title, screener_desc=desc,
                screener_icon=icon, signal_color=signal_color,
                table_columns=table_columns,
                avg_metric=None, avg_metric_label=None,
                indices=get_live_indices(), stock_list=get_filtered_tickers(),
                error="No dates available",
            )

        selected_date = request.args.get("date", dates[-1])
        stocks = data_func(selected_date, limit=100)

        # Fetch signals from service
        signals_map = compute_signals_simple(selected_date)

        for stock in stocks:
            ticker = stock.get("ticker")
            # Use computed signal if available, otherwise fallback to hardcoded screener signal or NEUTRAL
            if ticker and ticker in signals_map:
                stock["signal"] = signals_map[ticker]
            else:
                stock["signal"] = signal

        avg_metric = None
        if avg_metric_func and stocks:
            avg_metric = avg_metric_func(stocks)

        return render_template(
            "screener/technical_screener/generic_screener.html",
            dates=dates, selected_date=selected_date, stocks=stocks,
            screener_title=title, screener_desc=desc,
            screener_icon=icon, signal_color=signal_color,
            table_columns=table_columns,
            avg_metric=avg_metric, avg_metric_label=avg_metric_label,
            indices=get_live_indices(), stock_list=get_filtered_tickers(),
        )
    except Exception as e:
        print(f"[ERROR] {title} route: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"{title} failed: {str(e)}"}), 500


@technical_screener_bp.route("/rsi-overbought")
def rsi_overbought():
    """RSI > 75 overbought stocks screener"""
    return render_generic_screener(
        title="Overbought RSI Stocks",
        desc="Detects stocks where the RSI exceeds 75, suggesting a possible upcoming price correction.",
        icon="🔥", signal="BEARISH", signal_color="#dc2626",
        data_func=get_rsi_overbought_stocks,
        table_columns=[
            {"key": "macd", "label": "MACD", "format": "%.2f"},
        ],
        avg_metric_func=lambda s: "%.1f" % (sum(x.get("rsi_14", 0) or 0 for x in s) / len(s)),
        avg_metric_label="Avg RSI",
    )


@technical_screener_bp.route("/rsi-oversold")
def rsi_oversold():
    """RSI < 25 oversold stocks screener"""
    return render_generic_screener(
        title="Oversold RSI Stocks",
        desc="Finds stocks with RSI below 25, indicating a potential for price recovery.",
        icon="❄️", signal="BULLISH", signal_color="#16a34a",
        data_func=get_rsi_oversold_stocks,
        table_columns=[
            {"key": "macd", "label": "MACD", "format": "%.2f"},
        ],
        avg_metric_func=lambda s: "%.1f" % (sum(x.get("rsi_14", 0) or 0 for x in s) / len(s)),
        avg_metric_label="Avg RSI",
    )


@technical_screener_bp.route("/r1-breakout")
def r1_breakout():
    """R1 resistance breakout screener"""
    return render_generic_screener(
        title="R1 Resistance Breakouts",
        desc="Stocks climbing past R1 resistance levels, potentially signaling sustained upward trends.",
        icon="📈", signal="BULLISH", signal_color="#16a34a",
        data_func=get_r1_breakout_stocks,
        table_columns=[
            {"key": "r1", "label": "R1 Level", "format": "₹%.2f"},
            {"key": "breakout_pct", "label": "Breakout %", "format": "+%.2f", "suffix": "%", "css_class": "positive"},
        ],
    )


@technical_screener_bp.route("/r2-breakout")
def r2_breakout():
    """R2 resistance breakout screener"""
    return render_generic_screener(
        title="R2 Resistance Breakouts",
        desc="Stocks breaking above R2 resistance, showing strong bullish momentum.",
        icon="📈", signal="BULLISH", signal_color="#16a34a",
        data_func=get_r2_breakout_stocks,
        table_columns=[
            {"key": "r2", "label": "R2 Level", "format": "₹%.2f"},
            {"key": "breakout_pct", "label": "Breakout %", "format": "+%.2f", "suffix": "%", "css_class": "positive"},
        ],
    )


@technical_screener_bp.route("/r3-breakout")
def r3_breakout():
    """R3 resistance breakout screener"""
    return render_generic_screener(
        title="R3 Resistance Breakouts",
        desc="Stocks breaking above R3 resistance — extremely strong bullish signal.",
        icon="🚀", signal="BULLISH", signal_color="#16a34a",
        data_func=get_r3_breakout_stocks,
        table_columns=[
            {"key": "r3", "label": "R3 Level", "format": "₹%.2f"},
            {"key": "breakout_pct", "label": "Breakout %", "format": "+%.2f", "suffix": "%", "css_class": "positive"},
        ],
    )


@technical_screener_bp.route("/s1-breakout")
def s1_breakout():
    """S1 support breakout (breakdown) screener"""
    return render_generic_screener(
        title="S1 Support Breakouts",
        desc="Stocks falling through S1 support lines, possibly forecasting extended declines.",
        icon="📉", signal="BEARISH", signal_color="#dc2626",
        data_func=get_s1_breakout_stocks,
        table_columns=[
            {"key": "s1", "label": "S1 Level", "format": "₹%.2f"},
            {"key": "breakdown_pct", "label": "Breakdown %", "format": "-%.2f", "suffix": "%", "css_class": "negative"},
        ],
    )


@technical_screener_bp.route("/s2-breakout")
def s2_breakout():
    """S2 support breakout (breakdown) screener"""
    return render_generic_screener(
        title="S2 Support Breakouts",
        desc="Stocks falling through S2 support lines, indicating significant bearish pressure.",
        icon="📉", signal="BEARISH", signal_color="#dc2626",
        data_func=get_s2_breakout_stocks,
        table_columns=[
            {"key": "s2", "label": "S2 Level", "format": "₹%.2f"},
            {"key": "breakdown_pct", "label": "Breakdown %", "format": "-%.2f", "suffix": "%", "css_class": "negative"},
        ],
    )


@technical_screener_bp.route("/s3-breakout")
def s3_breakout():
    """S3 support breakout (breakdown) screener"""
    return render_generic_screener(
        title="S3 Support Breakouts",
        desc="Stocks falling through S3 support — extreme bearish signal.",
        icon="📉", signal="BEARISH", signal_color="#dc2626",
        data_func=get_s3_breakout_stocks,
        table_columns=[
            {"key": "s3", "label": "S3 Level", "format": "₹%.2f"},
            {"key": "breakdown_pct", "label": "Breakdown %", "format": "-%.2f", "suffix": "%", "css_class": "negative"},
        ],
    )


@technical_screener_bp.route("/momentum-stocks")
def momentum_stocks():
    """High momentum stocks screener"""
    return render_generic_screener(
        title="Momentum Stocks",
        desc="Securities surging in price with strong market enthusiasm and potential for further gains.",
        icon="🚀", signal="BULLISH", signal_color="#16a34a",
        data_func=get_momentum_stocks,
        table_columns=[
            {"key": "momentum_score", "label": "Momentum Score", "format": "%.2f"},
        ],
        avg_metric_func=lambda s: "%.1f" % (sum(x.get("momentum_score", 0) or 0 for x in s) / len(s)),
        avg_metric_label="Avg Momentum",
    )


@technical_screener_bp.route("/squeezing-range")
def squeezing_range():
    """Bollinger Band squeeze screener"""
    return render_generic_screener(
        title="Squeezing Range",
        desc="Pinpoints stocks with tightening Bollinger Bands, indicating a potential breakout or breakdown.",
        icon="🎯", signal="NEUTRAL", signal_color="#d97706",
        data_func=get_squeezing_range_stocks,
        table_columns=[
            {"key": "bb_width", "label": "BB Width %", "format": "%.2f", "suffix": "%"},
            {"key": "bb_upper", "label": "BB Upper", "format": "₹%.2f"},
            {"key": "bb_lower", "label": "BB Lower", "format": "₹%.2f"},
        ],
        avg_metric_func=lambda s: "%.2f%%" % (sum(x.get("bb_width", 0) or 0 for x in s) / len(s)),
        avg_metric_label="Avg BB Width",
    )


# ====== PRICE & VOLUME SCREENER ROUTES ======

@technical_screener_bp.route("/week1-high-breakout")
def week1_high_breakout():
    """1 Week High Breakouts"""
    return render_generic_screener(
        title="1 Week High Breakouts",
        desc="Securities surpassing highest price in the last week reflecting immediate bullish sentiment",
        icon="🚀", signal="BULLISH", signal_color="#16a34a",
        data_func=get_week1_high_breakout_stocks,
        table_columns=[
            {"key": "week1_high", "label": "1W High", "format": "₹%.2f"},
            {"key": "breakout_pct", "label": "Breakout %", "format": "+%.2f", "suffix": "%", "css_class": "positive"},
            {"key": "volume", "label": "Volume"},
        ],
    )

@technical_screener_bp.route("/week1-low-breakout")
def week1_low_breakout():
    """1 Week Low Breakouts"""
    return render_generic_screener(
        title="1 Week Low Breakouts",
        desc="Indicates immediate bearish sentiment with decline past their lowest price in the previous week",
        icon="📉", signal="BEARISH", signal_color="#dc2626",
        data_func=get_week1_low_breakout_stocks,
        table_columns=[
            {"key": "week1_low", "label": "1W Low", "format": "₹%.2f"},
            {"key": "breakdown_pct", "label": "Breakdown %", "format": "-%.2f", "suffix": "%", "css_class": "negative"},
            {"key": "volume", "label": "Volume"},
        ],
    )

@technical_screener_bp.route("/week4-high-breakout")
def week4_high_breakout():
    """4 Week High Breakouts"""
    return render_generic_screener(
        title="4 Week High Breakouts",
        desc="Securities exceeding their highest price in the last month indicating short-term bullish momentum",
        icon="🚀", signal="BULLISH", signal_color="#16a34a",
        data_func=get_week4_high_breakout_stocks,
        table_columns=[
            {"key": "week4_high", "label": "4W High", "format": "₹%.2f"},
            {"key": "breakout_pct", "label": "Breakout %", "format": "+%.2f", "suffix": "%", "css_class": "positive"},
            {"key": "volume", "label": "Volume"},
        ],
    )

@technical_screener_bp.route("/week4-low-breakout")
def week4_low_breakout():
    """4 Week Low Breakouts"""
    return render_generic_screener(
        title="4 Week Low Breakouts",
        desc="Pointing to short-term bearish momentum with stocks falling below their lowest price in the past month",
        icon="📉", signal="BEARISH", signal_color="#dc2626",
        data_func=get_week4_low_breakout_stocks,
        table_columns=[
            {"key": "week4_low", "label": "4W Low", "format": "₹%.2f"},
            {"key": "breakdown_pct", "label": "Breakdown %", "format": "-%.2f", "suffix": "%", "css_class": "negative"},
            {"key": "volume", "label": "Volume"},
        ],
    )

@technical_screener_bp.route("/week52-high-breakout")
def week52_high_breakout():
    """52 Week High Breakouts"""
    return render_generic_screener(
        title="52 Week High Breakouts",
        desc="Breaking past their highest price in the last year signaling strong bullish trends",
        icon="🚀", signal="BULLISH", signal_color="#16a34a",
        data_func=get_week52_high_breakout_stocks,
        table_columns=[
            {"key": "week52_high", "label": "52W High", "format": "₹%.2f"},
            {"key": "breakout_pct", "label": "Breakout %", "format": "+%.2f", "suffix": "%", "css_class": "positive"},
            {"key": "volume", "label": "Volume"},
        ],
    )

@technical_screener_bp.route("/week52-low-breakout")
def week52_low_breakout():
    """52 Week Low Breakouts"""
    return render_generic_screener(
        title="52 Week Low Breakouts",
        desc="Strong bearish trends suggested by dropping below their lowest price in the past year",
        icon="📉", signal="BEARISH", signal_color="#dc2626",
        data_func=get_week52_low_breakout_stocks,
        table_columns=[
            {"key": "week52_low", "label": "52W Low", "format": "₹%.2f"},
            {"key": "breakdown_pct", "label": "Breakdown %", "format": "-%.2f", "suffix": "%", "css_class": "negative"},
            {"key": "volume", "label": "Volume"},
        ],
    )

@technical_screener_bp.route("/potential-high-volume")
def potential_high_volume():
    """Potential High Volume"""
    return render_generic_screener(
        title="Potential High Volume",
        desc="Showing early signs of a spike in trading volume hinting at upcoming activity",
        icon="📊", signal="NEUTRAL", signal_color="#d97706",
        data_func=get_potential_high_volume_stocks,
        table_columns=[
            {"key": "volume", "label": "Volume"},
            {"key": "volume_change_pct", "label": "Vol Change %", "format": "+%.2f", "suffix": "%", "css_class": "positive"},
        ],
    )

@technical_screener_bp.route("/unusually-high-volume")
def unusually_high_volume():
    """Unusually High Volume"""
    return render_generic_screener(
        title="Unusually High Volume",
        desc="Points to increased interest or activity with volume much higher than average",
        icon="📊", signal="NEUTRAL", signal_color="#d97706",
        data_func=get_unusually_high_volume_stocks,
        table_columns=[
            {"key": "volume", "label": "Volume"},
            {"key": "volume_change_pct", "label": "Vol Change %", "format": "+%.2f", "suffix": "%", "css_class": "positive"},
        ],
    )


@technical_screener_bp.route("/price-gainers")
def price_gainers():
    """Top Price Gainers Screener"""
    return render_generic_screener(
        title="Top Price Gainers",
        desc="Stocks with the highest percentage price increase in the current session.",
        icon="🚀", signal="BULLISH", signal_color="#16a34a",
        data_func=get_price_gainers_stocks,
        table_columns=[
            {"key": "price_change_pct", "label": "Change %", "format": "+%.2f", "suffix": "%", "css_class": "positive"},
            {"key": "volume", "label": "Volume"},
        ],
    )


@technical_screener_bp.route("/price-losers")
def price_losers():
    """Top Price Losers Screener"""
    return render_generic_screener(
        title="Top Price Losers",
        desc="Stocks with the highest percentage price decrease in the current session.",
        icon="📉", signal="BEARISH", signal_color="#dc2626",
        data_func=get_price_losers_stocks,
        table_columns=[
            {"key": "price_change_pct", "label": "Change %", "format": "%.2f", "suffix": "%", "css_class": "negative"},
            {"key": "volume", "label": "Volume"},
        ],
    )


@technical_screener_bp.route("/high-volume")
def high_volume():
    """Top High Volume Screener"""
    return render_generic_screener(
        title="High Volume Stocks",
        desc="Stocks with the highest trading volume in the current session.",
        icon="📊", signal="NEUTRAL", signal_color="#d97706",
        data_func=get_high_volume_stocks,
        table_columns=[
            {"key": "volume", "label": "Volume"},
            {"key": "volume_change_pct", "label": "Vol Change %", "format": "+%.2f", "suffix": "%", "css_class": "positive"},
        ],
    )
