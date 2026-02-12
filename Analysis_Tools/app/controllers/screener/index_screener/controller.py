"""
INDEX SCREENER CONTROLLER
Handles Nifty 50, Bank Nifty, and other index-based screeners
Uses real data from NSE API and database

NOTE: Signal computation is centralized in app.services.signal_service
      This controller imports from there to ensure consistency across the app.
"""

from flask import Blueprint, jsonify, render_template, request
from flask_caching import Cache

from ....controllers.dashboard_controller import get_live_indices
from ....models.index_model import get_banknifty_stocks_with_data, get_nifty50_stocks_with_data
from ....models.stock_model import get_filtered_tickers

# Import from centralized signal service (SINGLE SOURCE OF TRUTH)
from ....services.signal_service import compute_signals_simple

index_screener_bp = Blueprint("index_screener", __name__, url_prefix="/scanner")

# Initialize cache
cache = Cache(config={"CACHE_TYPE": "simple", "CACHE_DEFAULT_TIMEOUT": 3600})


def calculate_signal_counts(stocks):
    """Calculate bullish, bearish, and neutral counts"""
    bullish = sum(1 for s in stocks if s.get("signal") == "BULLISH")
    bearish = sum(1 for s in stocks if s.get("signal") == "BEARISH")
    neutral = sum(1 for s in stocks if s.get("signal") == "NEUTRAL")
    return bullish, bearish, neutral


# ========================================================================
# ROUTES
# ========================================================================


@index_screener_bp.route("/index")
def nifty50_screener():
    """
    Nifty 50 screener page at /scanner/index
    Fetches real-time constituents from NSE and data from database
    """
    # Get date parameter or use latest
    selected_date = request.args.get("date", None)

    # Fetch Nifty 50 stocks with real data
    stocks = get_nifty50_stocks_with_data(date=selected_date)

    if not stocks:
        # Fallback to empty state with message
        return render_template(
            "screener/index_screener/index.html",
            screener_title="Nifty 50",
            screener_tag="Nifty 50",
            screener_description="The Nifty 50 is a benchmark index in the Indian stock market. It includes the top 50 large-cap companies listed on the National Stock Exchange (NSE) based on market capitalization and liquidity.",
            stocks=[],
            total_count=0,
            bullish_count=0,
            bearish_count=0,
            neutral_count=0,
            error_message="No data available. Please check if database is updated.",
            indices=get_live_indices(),
            stock_list=get_filtered_tickers(),
            stock_symbol=None,
        )

    bullish_count, bearish_count, neutral_count = calculate_signal_counts(stocks)

    return render_template(
        "screener/index_screener/index.html",
        screener_title="Nifty 50",
        screener_tag="Nifty 50",
        screener_description="The Nifty 50 is a benchmark index in the Indian stock market. It includes the top 50 large-cap companies listed on the National Stock Exchange (NSE) based on market capitalization and liquidity.",
        stocks=stocks,
        total_count=len(stocks),
        bullish_count=bullish_count,
        bearish_count=bearish_count,
        neutral_count=neutral_count,
        indices=get_live_indices(),
        stock_list=get_filtered_tickers(),
        stock_symbol=None,
    )


@index_screener_bp.route("/banknifty")
def banknifty_screener():
    """
    Bank Nifty screener page at /scanner/banknifty
    Fetches real-time constituents from NSE and data from database
    """
    # Get date parameter or use latest
    selected_date = request.args.get("date", None)

    # Fetch Bank Nifty stocks with real data
    stocks = get_banknifty_stocks_with_data(date=selected_date)

    if not stocks:
        # Fallback to empty state with message
        return render_template(
            "screener/index_screener/index.html",
            screener_title="Bank Nifty",
            screener_tag="Bank Nifty",
            screener_description="Bank Nifty is a sectoral index comprising the most liquid and large capitalized banking stocks. It provides investors and market intermediaries a benchmark that captures the capital market performance of Indian banking sector.",
            stocks=[],
            total_count=0,
            bullish_count=0,
            bearish_count=0,
            neutral_count=0,
            error_message="No data available. Please check if database is updated.",
            indices=get_live_indices(),
            stock_list=get_filtered_tickers(),
            stock_symbol=None,
        )

    bullish_count, bearish_count, neutral_count = calculate_signal_counts(stocks)

    return render_template(
        "screener/index_screener/index.html",
        screener_title="Bank Nifty",
        screener_tag="Bank Nifty",
        screener_description="Bank Nifty is a sectoral index comprising the most liquid and large capitalized banking stocks. It provides investors and market intermediaries a benchmark that captures the capital market performance of Indian banking sector.",
        stocks=stocks,
        total_count=len(stocks),
        bullish_count=bullish_count,
        bearish_count=bearish_count,
        neutral_count=neutral_count,
        indices=get_live_indices(),
        stock_list=get_filtered_tickers(),
        stock_symbol=None,
    )


@index_screener_bp.route("/high-oi")
def high_oi_screener():
    """
    High OI Buildup screener page at /scanner/high-oi
    Shows Nifty 50 stocks sorted by highest OI
    """
    selected_date = request.args.get("date", None)

    # Get Nifty 50 stocks
    stocks = get_nifty50_stocks_with_data(date=selected_date)

    # Sort by OI and take top 15
    stocks_sorted = sorted(stocks, key=lambda x: x.get("oi", 0), reverse=True)[:15]

    bullish_count, bearish_count, neutral_count = calculate_signal_counts(stocks_sorted)

    return render_template(
        "screener/index_screener/index.html",
        screener_title="High OI Buildup",
        screener_tag="High OI",
        screener_description="Stocks showing significant Open Interest accumulation. High OI buildup often indicates strong market interest and potential directional moves.",
        stocks=stocks_sorted,
        total_count=len(stocks_sorted),
        bullish_count=bullish_count,
        bearish_count=bearish_count,
        neutral_count=neutral_count,
        indices=get_live_indices(),
        stock_list=get_filtered_tickers(),
        stock_symbol=None,
    )


@index_screener_bp.route("/iv-spike")
def iv_spike_screener():
    """
    IV Spike Alert screener page at /scanner/iv-spike
    Shows Nifty 50 stocks with highest IV
    """
    selected_date = request.args.get("date", None)

    # Get Nifty 50 stocks
    stocks = get_nifty50_stocks_with_data(date=selected_date)

    # Sort by IV and take top 15
    stocks_sorted = sorted(stocks, key=lambda x: x.get("iv", 0), reverse=True)[:15]

    bullish_count, bearish_count, neutral_count = calculate_signal_counts(stocks_sorted)

    return render_template(
        "screener/index_screener/index.html",
        screener_title="IV Spike Alert",
        screener_tag="IV Spike",
        screener_description="Stocks with unusual Implied Volatility movement. IV spikes often precede significant price movements and can indicate upcoming events or market uncertainty.",
        stocks=stocks_sorted,
        total_count=len(stocks_sorted),
        bullish_count=bullish_count,
        bearish_count=bearish_count,
        neutral_count=neutral_count,
        indices=get_live_indices(),
        stock_list=get_filtered_tickers(),
        stock_symbol=None,
    )


# ========================================================================
# API ENDPOINTS (for AJAX calls from screener landing page)
# ========================================================================


@index_screener_bp.route("/api/nifty50")
def api_nifty50():
    """API endpoint for Nifty 50 data (returns JSON)"""
    selected_date = request.args.get("date", None)
    stocks = get_nifty50_stocks_with_data(date=selected_date)

    # Get derivative-based signals using CENTRALIZED signal service (SINGLE SOURCE OF TRUTH)
    derivative_signals = compute_signals_simple(selected_date)

    # Apply derivative signals to stocks
    for stock in stocks:
        ticker = stock.get("ticker")
        if ticker in derivative_signals:
            stock["signal"] = derivative_signals[ticker]
        # Keep existing signal if no derivative signal available

    # Replace NaN values with None (will become null in JSON)
    import math

    for stock in stocks:
        for key, value in stock.items():
            if isinstance(value, float) and math.isnan(value):
                stock[key] = 0

    bullish_count, bearish_count, neutral_count = calculate_signal_counts(stocks)

    return jsonify(
        {
            "title": "Nifty 50",
            "tag": "Nifty 50",
            "description": "The Nifty 50 is a benchmark index in the Indian stock market. It includes the top 50 large-cap companies listed on the National Stock Exchange (NSE) based on market capitalization and liquidity.",
            "stocks": stocks,
            "total_count": len(stocks),
            "bullish_count": bullish_count,
            "bearish_count": bearish_count,
            "neutral_count": neutral_count,
        }
    )


@index_screener_bp.route("/api/banknifty")
def api_banknifty():
    """API endpoint for Bank Nifty data (returns JSON)"""
    selected_date = request.args.get("date", None)
    stocks = get_banknifty_stocks_with_data(date=selected_date)

    # Get derivative-based signals using CENTRALIZED signal service (SINGLE SOURCE OF TRUTH)
    derivative_signals = compute_signals_simple(selected_date)

    # Apply derivative signals to stocks
    for stock in stocks:
        ticker = stock.get("ticker")
        if ticker in derivative_signals:
            stock["signal"] = derivative_signals[ticker]
        # Keep existing signal if no derivative signal available

    # Replace NaN values with None (will become null in JSON)
    import math

    for stock in stocks:
        for key, value in stock.items():
            if isinstance(value, float) and math.isnan(value):
                stock[key] = 0

    bullish_count, bearish_count, neutral_count = calculate_signal_counts(stocks)

    return jsonify(
        {
            "title": "Bank Nifty",
            "tag": "Bank Nifty",
            "description": "Bank Nifty is a sectoral index comprising the most liquid and large capitalized banking stocks. It provides investors and market intermediaries a benchmark that captures the capital market performance of Indian banking sector.",
            "stocks": stocks,
            "total_count": len(stocks),
            "bullish_count": bullish_count,
            "bearish_count": bearish_count,
            "neutral_count": neutral_count,
        }
    )


@index_screener_bp.route("/api/high-oi")
def api_high_oi():
    """API endpoint for High OI Buildup data (returns JSON)"""
    selected_date = request.args.get("date", None)
    stocks = get_nifty50_stocks_with_data(date=selected_date)
    stocks_sorted = sorted(stocks, key=lambda x: x.get("oi", 0), reverse=True)[:15]

    # Replace NaN values with None (will become null in JSON)
    import math

    for stock in stocks_sorted:
        for key, value in stock.items():
            if isinstance(value, float) and math.isnan(value):
                stock[key] = 0

    bullish_count, bearish_count, neutral_count = calculate_signal_counts(stocks_sorted)

    return jsonify(
        {
            "title": "High OI Buildup",
            "tag": "High OI",
            "description": "Stocks showing significant Open Interest accumulation. High OI buildup often indicates strong market interest and potential directional moves.",
            "stocks": stocks_sorted,
            "total_count": len(stocks_sorted),
            "bullish_count": bullish_count,
            "bearish_count": bearish_count,
            "neutral_count": neutral_count,
        }
    )


@index_screener_bp.route("/api/iv-spike")
def api_iv_spike():
    """API endpoint for IV Spike data (returns JSON)"""
    selected_date = request.args.get("date", None)
    stocks = get_nifty50_stocks_with_data(date=selected_date)
    stocks_sorted = sorted(stocks, key=lambda x: x.get("iv", 0), reverse=True)[:15]

    # Replace NaN values with None (will become null in JSON)
    import math

    for stock in stocks_sorted:
        for key, value in stock.items():
            if isinstance(value, float) and math.isnan(value):
                stock[key] = 0

    bullish_count, bearish_count, neutral_count = calculate_signal_counts(stocks_sorted)

    return jsonify(
        {
            "title": "IV Spike Alert",
            "tag": "IV Spike",
            "description": "Stocks with unusual Implied Volatility movement. IV spikes often precede significant price movements and can indicate upcoming events or market uncertainty.",
            "stocks": stocks_sorted,
            "total_count": len(stocks_sorted),
            "bullish_count": bullish_count,
            "bearish_count": bearish_count,
            "neutral_count": neutral_count,
        }
    )
