"""
INDEX SCREENER CONTROLLER
Handles Nifty 50, Bank Nifty, and other index-based screeners
Uses real data from NSE API and database
"""

from flask import Blueprint, jsonify, render_template, request
from flask_caching import Cache

from ....controllers.dashboard_controller import get_live_indices
from ....models.index_constituents_model import get_banknifty_stocks_with_data, get_nifty50_stocks_with_data
from ....models.screener_model import get_all_screener_data
from ....models.stock_model import get_filtered_tickers

index_screener_bp = Blueprint("index_screener", __name__, url_prefix="/screener")

# Initialize cache
cache = Cache(config={"CACHE_TYPE": "simple", "CACHE_DEFAULT_TIMEOUT": 3600})


def calculate_signal_counts(stocks):
    """Calculate bullish, bearish, and neutral counts"""
    bullish = sum(1 for s in stocks if s.get("signal") == "BULLISH")
    bearish = sum(1 for s in stocks if s.get("signal") == "BEARISH")
    neutral = sum(1 for s in stocks if s.get("signal") == "NEUTRAL")
    return bullish, bearish, neutral


def compute_derivative_signals(selected_date):
    """
    Compute signals using derivatives analysis (same logic as signal_analysis screener)
    Returns: dict mapping ticker -> signal ('BULLISH', 'BEARISH', 'NEUTRAL')
    """
    try:
        all_data = get_all_screener_data(selected_date)
        if not all_data:
            return {}

        # Build screener data structure
        screener_data = {}

        # OI - Calls
        screener_data["oi_call_gainers"] = all_data["oi"]["CE"]["ALL"][:10]
        screener_data["oi_call_itm_gainers"] = all_data["oi"]["CE"]["ITM"][:10]
        screener_data["oi_call_otm_gainers"] = all_data["oi"]["CE"]["OTM"][:10]
        screener_data["oi_call_losers"] = all_data["oi"]["CE"]["ALL_LOSERS"][:10]
        screener_data["oi_call_itm_losers"] = all_data["oi"]["CE"]["ITM_LOSERS"][:10]
        screener_data["oi_call_otm_losers"] = all_data["oi"]["CE"]["OTM_LOSERS"][:10]

        # OI - Puts
        screener_data["oi_put_gainers"] = all_data["oi"]["PE"]["ALL"][:10]
        screener_data["oi_put_itm_gainers"] = all_data["oi"]["PE"]["ITM"][:10]
        screener_data["oi_put_otm_gainers"] = all_data["oi"]["PE"]["OTM"][:10]
        screener_data["oi_put_losers"] = all_data["oi"]["PE"]["ALL_LOSERS"][:10]
        screener_data["oi_put_itm_losers"] = all_data["oi"]["PE"]["ITM_LOSERS"][:10]
        screener_data["oi_put_otm_losers"] = all_data["oi"]["PE"]["OTM_LOSERS"][:10]

        # Moneyness - Calls
        screener_data["moneyness_call_gainers"] = all_data["moneyness"]["CE"]["ALL"][:10]
        screener_data["moneyness_call_itm_gainers"] = all_data["moneyness"]["CE"]["ITM"][:10]
        screener_data["moneyness_call_otm_gainers"] = all_data["moneyness"]["CE"]["OTM"][:10]
        screener_data["moneyness_call_losers"] = all_data["moneyness"]["CE"]["ALL_LOSERS"][:10]
        screener_data["moneyness_call_itm_losers"] = all_data["moneyness"]["CE"]["ITM_LOSERS"][:10]
        screener_data["moneyness_call_otm_losers"] = all_data["moneyness"]["CE"]["OTM_LOSERS"][:10]

        # Moneyness - Puts
        screener_data["moneyness_put_gainers"] = all_data["moneyness"]["PE"]["ALL"][:10]
        screener_data["moneyness_put_itm_gainers"] = all_data["moneyness"]["PE"]["ITM"][:10]
        screener_data["moneyness_put_otm_gainers"] = all_data["moneyness"]["PE"]["OTM"][:10]
        screener_data["moneyness_put_losers"] = all_data["moneyness"]["PE"]["ALL_LOSERS"][:10]
        screener_data["moneyness_put_itm_losers"] = all_data["moneyness"]["PE"]["ITM_LOSERS"][:10]
        screener_data["moneyness_put_otm_losers"] = all_data["moneyness"]["PE"]["OTM_LOSERS"][:10]

        # IV - Calls
        screener_data["iv_call_gainers"] = all_data["iv"]["CE"]["ALL"][:10]
        screener_data["iv_call_itm_gainers"] = all_data["iv"]["CE"]["ITM"][:10]
        screener_data["iv_call_otm_gainers"] = all_data["iv"]["CE"]["OTM"][:10]
        screener_data["iv_call_losers"] = all_data["iv"]["CE"]["ALL_LOSERS"][:10]
        screener_data["iv_call_itm_losers"] = all_data["iv"]["CE"]["ITM_LOSERS"][:10]
        screener_data["iv_call_otm_losers"] = all_data["iv"]["CE"]["OTM_LOSERS"][:10]

        # IV - Puts
        screener_data["iv_put_gainers"] = all_data["iv"]["PE"]["ALL"][:10]
        screener_data["iv_put_itm_gainers"] = all_data["iv"]["PE"]["ITM"][:10]
        screener_data["iv_put_otm_gainers"] = all_data["iv"]["PE"]["OTM"][:10]
        screener_data["iv_put_losers"] = all_data["iv"]["PE"]["ALL_LOSERS"][:10]
        screener_data["iv_put_itm_losers"] = all_data["iv"]["PE"]["ITM_LOSERS"][:10]
        screener_data["iv_put_otm_losers"] = all_data["iv"]["PE"]["OTM_LOSERS"][:10]

        # Futures
        screener_data["future_oi_gainers"] = all_data["oi"]["FUT"]["ALL"][:10]
        screener_data["future_oi_losers"] = all_data["oi"]["FUT"]["ALL_LOSERS"][:10]

        # Define bullish and bearish categories
        bullish_sections = [
            "iv_call_gainers",
            "iv_call_itm_gainers",
            "iv_call_otm_gainers",
            "iv_put_losers",
            "iv_put_itm_losers",
            "iv_put_otm_losers",
            "oi_call_gainers",
            "oi_call_itm_gainers",
            "oi_call_otm_gainers",
            "oi_put_losers",
            "oi_put_itm_losers",
            "oi_put_otm_losers",
            "moneyness_call_gainers",
            "moneyness_call_itm_gainers",
            "moneyness_call_otm_gainers",
            "moneyness_put_losers",
            "moneyness_put_itm_losers",
            "moneyness_put_otm_losers",
            "future_oi_gainers",
        ]

        bearish_sections = [
            "iv_call_losers",
            "iv_call_itm_losers",
            "iv_call_otm_losers",
            "iv_put_gainers",
            "iv_put_itm_gainers",
            "iv_put_otm_gainers",
            "oi_call_losers",
            "oi_call_itm_losers",
            "oi_call_otm_losers",
            "oi_put_gainers",
            "oi_put_itm_gainers",
            "oi_put_otm_gainers",
            "moneyness_call_losers",
            "moneyness_call_itm_losers",
            "moneyness_call_otm_losers",
            "moneyness_put_gainers",
            "moneyness_put_itm_gainers",
            "moneyness_put_otm_gainers",
            "future_oi_losers",
        ]

        signals = {}

        # Count bullish appearances
        for sec_key in bullish_sections:
            for row in screener_data.get(sec_key, []):
                ticker = row.get("ticker")
                if ticker:
                    if ticker not in signals:
                        signals[ticker] = {"bullish_count": 0, "bearish_count": 0}
                    signals[ticker]["bullish_count"] += 1

        # Count bearish appearances
        for sec_key in bearish_sections:
            for row in screener_data.get(sec_key, []):
                ticker = row.get("ticker")
                if ticker:
                    if ticker not in signals:
                        signals[ticker] = {"bullish_count": 0, "bearish_count": 0}
                    signals[ticker]["bearish_count"] += 1

        # Compute final signal
        result = {}
        for ticker, counts in signals.items():
            if counts["bullish_count"] > counts["bearish_count"]:
                result[ticker] = "BULLISH"
            elif counts["bearish_count"] > counts["bullish_count"]:
                result[ticker] = "BEARISH"
            else:
                result[ticker] = "NEUTRAL"

        return result

    except Exception as e:
        print(f"[ERROR] compute_derivative_signals: {e}")
        import traceback

        traceback.print_exc()
        return {}


# ========================================================================
# ROUTES
# ========================================================================


@index_screener_bp.route("/index")
def nifty50_screener():
    """
    Nifty 50 screener page at /screener/index
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
    Bank Nifty screener page at /screener/banknifty
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
    High OI Buildup screener page at /screener/high-oi
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
    IV Spike Alert screener page at /screener/iv-spike
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

    # Get derivative-based signals (same as signal_analysis screener)
    derivative_signals = compute_derivative_signals(selected_date)

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

    # Get derivative-based signals (same as signal_analysis screener)
    derivative_signals = compute_derivative_signals(selected_date)

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
