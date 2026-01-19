"""
VOICE API CONTROLLER
Handles voice assistant data queries
Returns structured data for text-to-speech responses
All data is fetched dynamically from the database
"""

import pandas as pd
from flask import Blueprint, jsonify, request
from sqlalchemy import text

from ..models.db_config import engine, get_table_list
from ..models.stock_model import get_available_dates, get_stock_stats

voice_api_bp = Blueprint("voice_api", __name__, url_prefix="/api/voice")


@voice_api_bp.route("/stocks")
def get_available_stocks():
    """
    Get all available stocks from the database
    Returns list of tickers that can be searched
    """
    try:
        tables = get_table_list()

        # Extract stock tickers from table names (TBL_TICKER_DERIVED)
        stocks = []
        for table in tables:
            if table.startswith("TBL_") and table.endswith("_DERIVED"):
                ticker = table.replace("TBL_", "").replace("_DERIVED", "")
                if ticker:
                    stocks.append(ticker)

        # Remove duplicates and sort
        stocks = sorted(list(set(stocks)))

        return jsonify({"success": True, "stocks": stocks, "count": len(stocks)})

    except Exception as e:
        print(f"[ERROR] Voice API - stocks: {e}")
        return jsonify({"success": False, "stocks": [], "error": str(e)})


@voice_api_bp.route("/stock/<ticker>")
def get_stock_voice_data(ticker):
    """
    Get stock data formatted for voice response
    Returns: price, change, signal, summary
    """
    try:
        ticker = ticker.upper()

        # Check if stock exists
        tables = get_table_list()
        table_name = f"TBL_{ticker}_DERIVED"

        if table_name not in tables:
            return jsonify(
                {
                    "success": False,
                    "speech": f"Sorry, {ticker} is not available in the database. Please check the stock symbol.",
                }
            )

        dates = get_available_dates()

        if not dates:
            return jsonify({"success": False, "speech": f"Sorry, no data available for {ticker}"})

        latest_date = dates[0]

        # Get underlying price from derived table
        q = text(
            f"""
            SELECT DISTINCT "UndrlygPric", "BizDt"
            FROM public."{table_name}"
            WHERE "BizDt" = :bizdt
            AND "UndrlygPric" IS NOT NULL
            LIMIT 1
        """
        )

        df = pd.read_sql(q, con=engine, params={"bizdt": latest_date})

        if df.empty:
            return jsonify({"success": False, "speech": f"Sorry, I couldn't find recent data for {ticker}"})

        price = float(df.iloc[0]["UndrlygPric"])

        # Get stats
        stats = get_stock_stats(ticker, latest_date)

        # Build voice response
        trend = stats.get("trend_oi", "neutral")
        pcr = stats.get("pcr_oi", 0)

        speech = f"{ticker} is currently trading at {price:.2f} rupees. "

        if trend == "Bullish":
            speech += f"The trend is bullish with a PCR of {pcr:.2f}. "
        elif trend == "Bearish":
            speech += f"The trend is bearish with a PCR of {pcr:.2f}. "
        else:
            speech += f"The trend is neutral with a PCR of {pcr:.2f}. "

        return jsonify(
            {
                "success": True,
                "ticker": ticker,
                "price": price,
                "trend": trend,
                "pcr": pcr,
                "speech": speech,
                "date": latest_date,
            }
        )

    except Exception as e:
        print(f"[ERROR] Voice API - stock/{ticker}: {e}")
        return jsonify({"success": False, "speech": f"Sorry, I encountered an error getting data for {ticker}"})


@voice_api_bp.route("/market-summary")
def get_market_summary():
    """
    Get overall market summary for voice
    """
    try:
        dates = get_available_dates()

        if not dates:
            return jsonify({"success": False, "speech": "Sorry, no market data available"})

        latest_date = dates[0]
        tables = get_table_list()

        # Get Nifty data
        nifty_data = None
        if "TBL_NIFTY_DERIVED" in tables:
            try:
                q = text(
                    """
                    SELECT DISTINCT "UndrlygPric"
                    FROM public."TBL_NIFTY_DERIVED"
                    WHERE "BizDt" = :bizdt
                    AND "UndrlygPric" IS NOT NULL
                    LIMIT 1
                """
                )
                df = pd.read_sql(q, con=engine, params={"bizdt": latest_date})
                if not df.empty:
                    nifty_data = float(df.iloc[0]["UndrlygPric"])
            except:
                pass

        # Get Bank Nifty data
        banknifty_data = None
        if "TBL_BANKNIFTY_DERIVED" in tables:
            try:
                q = text(
                    """
                    SELECT DISTINCT "UndrlygPric"
                    FROM public."TBL_BANKNIFTY_DERIVED"
                    WHERE "BizDt" = :bizdt
                    AND "UndrlygPric" IS NOT NULL
                    LIMIT 1
                """
                )
                df = pd.read_sql(q, con=engine, params={"bizdt": latest_date})
                if not df.empty:
                    banknifty_data = float(df.iloc[0]["UndrlygPric"])
            except:
                pass

        # Build speech
        speech = f"Here's the market summary for {latest_date}. "

        if nifty_data:
            speech += f"Nifty is at {nifty_data:.0f}. "

        if banknifty_data:
            speech += f"Bank Nifty is at {banknifty_data:.0f}. "

        if not nifty_data and not banknifty_data:
            speech = "Sorry, I couldn't fetch the market summary right now."

        return jsonify(
            {"success": True, "nifty": nifty_data, "banknifty": banknifty_data, "date": latest_date, "speech": speech}
        )

    except Exception as e:
        print(f"[ERROR] Voice API - market-summary: {e}")
        return jsonify({"success": False, "speech": "Sorry, I encountered an error getting the market summary"})


@voice_api_bp.route("/page-context/<page>")
def get_page_context(page):
    """
    Get available commands and data for a specific page
    This makes the voice assistant fully dynamic
    """
    try:
        context = {"page": page, "commands": [], "hints": [], "data": {}}

        # Get available stocks for search
        tables = get_table_list()
        stocks = []
        for table in tables:
            if table.startswith("TBL_") and table.endswith("_DERIVED"):
                ticker = table.replace("TBL_", "").replace("_DERIVED", "")
                if ticker:
                    stocks.append(ticker)
        stocks = sorted(list(set(stocks)))
        context["data"]["available_stocks"] = stocks

        # Get available dates
        dates = get_available_dates()
        context["data"]["available_dates"] = dates[:10] if dates else []

        # Page-specific context
        if page == "home":
            context["commands"] = [
                {"phrase": "go to dashboard", "action": "navigate", "target": "/dashboard"},
                {"phrase": "open screeners", "action": "navigate", "target": "/screener/"},
                {"phrase": "search [stock]", "action": "search", "dynamic": True},
                {"phrase": "show top gainers", "action": "navigate", "target": "/screener/top-gainers-losers"},
            ]
            context["hints"] = [
                "Go to dashboard",
                "Open screeners",
                f"Search any of {len(stocks)} stocks",
                "Top gainers",
            ]

        elif page == "dashboard":
            context["commands"] = [
                {"phrase": "filter itm", "action": "click", "selector": "[data-mtype='ITM']"},
                {"phrase": "filter otm", "action": "click", "selector": "[data-mtype='OTM']"},
                {"phrase": "show all", "action": "click", "selector": "[data-mtype='TOTAL']"},
                {"phrase": "export excel", "action": "click", "selector": "[data-export]"},
                {"phrase": "next page", "action": "click", "selector": ".paginate_button.next"},
                {"phrase": "previous page", "action": "click", "selector": ".paginate_button.previous"},
                {"phrase": "change date", "action": "focus", "selector": "#dateSelect"},
                {"phrase": "search [stock]", "action": "search", "dynamic": True},
            ]
            context["hints"] = ["Filter ITM", "Filter OTM", "Export Excel", "Next page", "Change date"]

        elif page == "stock_detail":
            context["commands"] = [
                {"phrase": "next expiry", "action": "click", "selector": ".expiry-next"},
                {"phrase": "previous expiry", "action": "click", "selector": ".expiry-prev"},
                {"phrase": "change expiry", "action": "focus", "selector": "#expirySelect"},
                {"phrase": "show stats", "action": "scroll", "selector": ".stats-section"},
                {"phrase": "show chart", "action": "scroll", "selector": ".chart-section"},
                {"phrase": "what is trend", "action": "api", "endpoint": "/api/voice/stock/{ticker}"},
                {"phrase": "export data", "action": "click", "selector": "[data-export]"},
            ]
            context["hints"] = ["Next expiry", "Previous expiry", "Show stats", "What is trend", "Export"]

        elif page == "screener_landing":
            context["commands"] = [
                {"phrase": "filter bullish", "action": "click", "selector": "[data-filter='bullish']"},
                {"phrase": "filter bearish", "action": "click", "selector": "[data-filter='bearish']"},
                {"phrase": "show all", "action": "click", "selector": "[data-filter='all']"},
                {"phrase": "derivative screeners", "action": "click", "selector": "[data-category='derivative']"},
                {"phrase": "technical screeners", "action": "click", "selector": "[data-category='technical']"},
                {"phrase": "nifty 50", "action": "navigate", "target": "/screener/index"},
                {"phrase": "bank nifty", "action": "navigate", "target": "/screener/banknifty"},
            ]
            context["hints"] = ["Filter bullish", "Filter bearish", "Nifty 50", "Bank Nifty", "Back to list"]

        elif page == "top_gainers_losers":
            context["commands"] = [
                {"phrase": "export pdf", "action": "click", "selector": "#exportPdfBtn"},
                {"phrase": "show oi section", "action": "scroll", "selector": "#oiSection"},
                {"phrase": "show iv section", "action": "scroll", "selector": "#ivSection"},
                {"phrase": "show futures", "action": "scroll", "selector": "#futuresSection"},
                {"phrase": "change date", "action": "focus", "selector": "#dateSelect"},
            ]
            context["hints"] = ["Export PDF", "OI section", "IV section", "Futures section", "Change date"]

        elif page == "signal_analysis":
            context["commands"] = [
                {"phrase": "filter bullish", "action": "click", "selector": "[data-signal='bullish']"},
                {"phrase": "filter bearish", "action": "click", "selector": "[data-signal='bearish']"},
                {"phrase": "show all", "action": "click", "selector": "[data-signal='all']"},
                {"phrase": "change date", "action": "focus", "selector": "#dateSelect"},
                {"phrase": "export", "action": "click", "selector": "[data-export]"},
            ]
            context["hints"] = ["Filter bullish", "Filter bearish", "Show all", "Change date", "Export"]

        elif page == "futures_oi":
            context["commands"] = [
                {"phrase": "current month", "action": "click", "selector": "[data-expiry='cme']"},
                {"phrase": "next month", "action": "click", "selector": "[data-expiry='nme']"},
                {"phrase": "far month", "action": "click", "selector": "[data-expiry='fme']"},
                {"phrase": "change date", "action": "focus", "selector": "#dateSelect"},
                {"phrase": "export", "action": "click", "selector": "[data-export]"},
            ]
            context["hints"] = ["Current month", "Next month", "Far month", "Change date", "Export"]

        elif page == "technical_screener":
            context["commands"] = [
                {
                    "phrase": "golden crossover",
                    "action": "navigate",
                    "target": "/screener/technical-indicators/golden-crossover",
                },
                {
                    "phrase": "death crossover",
                    "action": "navigate",
                    "target": "/screener/technical-indicators/death-crossover",
                },
                {"phrase": "rsi overbought", "action": "click", "selector": "[data-indicator='rsi-overbought']"},
                {"phrase": "rsi oversold", "action": "click", "selector": "[data-indicator='rsi-oversold']"},
                {"phrase": "macd bullish", "action": "click", "selector": "[data-indicator='macd-bullish']"},
            ]
            context["hints"] = ["Golden crossover", "Death crossover", "RSI overbought", "MACD bullish"]

        elif page == "index_screener":
            context["commands"] = [
                {"phrase": "filter bullish", "action": "click", "selector": "[data-filter='bullish']"},
                {"phrase": "filter bearish", "action": "click", "selector": "[data-filter='bearish']"},
                {"phrase": "sort by price", "action": "click", "selector": "th[data-sort='price']"},
                {"phrase": "sort by change", "action": "click", "selector": "th[data-sort='change']"},
                {"phrase": "export csv", "action": "click", "selector": "[data-export-csv]"},
                {"phrase": "back", "action": "click", "selector": ".back-btn"},
            ]
            context["hints"] = ["Filter bullish", "Filter bearish", "Sort by price", "Export CSV", "Back"]

        return jsonify({"success": True, **context})

    except Exception as e:
        print(f"[ERROR] Voice API - page-context/{page}: {e}")
        return jsonify({"success": False, "error": str(e)})


@voice_api_bp.route("/validate-stock/<ticker>")
def validate_stock(ticker):
    """
    Check if a stock exists in the database
    """
    try:
        ticker = ticker.upper()
        tables = get_table_list()
        table_name = f"TBL_{ticker}_DERIVED"

        exists = table_name in tables

        return jsonify(
            {
                "success": True,
                "ticker": ticker,
                "exists": exists,
                "speech": f"{ticker} is available" if exists else f"{ticker} is not available in the database",
            }
        )

    except Exception as e:
        return jsonify({"success": False, "exists": False, "error": str(e)})


@voice_api_bp.route("/stock-aliases")
def get_stock_aliases():
    """
    Provide common stock name aliases for natural language matching.
    Returns only aliases for stocks that exist in the database.
    """
    try:
        # Get all available stocks from database
        tables = get_table_list()
        available_stocks = []
        for table in tables:
            if table.startswith("TBL_") and table.endswith("_DERIVED"):
                ticker = table.replace("TBL_", "").replace("_DERIVED", "")
                if ticker:
                    available_stocks.append(ticker)

        available_stocks = set(available_stocks)

        # Common aliases - will be filtered to only include stocks in database
        all_aliases = {
            # Indices
            "nifty": "NIFTY",
            "bank nifty": "BANKNIFTY",
            "banknifty": "BANKNIFTY",
            "finnifty": "FINNIFTY",
            # Top stocks with common names
            "reliance": "RELIANCE",
            "tcs": "TCS",
            "infosys": "INFY",
            "infy": "INFY",
            "hdfc bank": "HDFCBANK",
            "hdfc": "HDFCBANK",
            "hdfcbank": "HDFCBANK",
            "icici bank": "ICICIBANK",
            "icici": "ICICIBANK",
            "sbi": "SBIN",
            "state bank": "SBIN",
            "tata motors": "TATAMOTORS",
            "tatamotors": "TATAMOTORS",
            "tata steel": "TATASTEEL",
            "tatasteel": "TATASTEEL",
            "bharti airtel": "BHARTIARTL",
            "airtel": "BHARTIARTL",
            "bharti": "BHARTIARTL",
            "wipro": "WIPRO",
            "itc": "ITC",
            "kotak": "KOTAKBANK",
            "kotak bank": "KOTAKBANK",
            "axis bank": "AXISBANK",
            "axis": "AXISBANK",
            "maruti": "MARUTI",
            "bajaj finance": "BAJFINANCE",
            "bajaj": "BAJFINANCE",
            "asian paints": "ASIANPAINT",
            "asian paint": "ASIANPAINT",
            "hindustan unilever": "HINDUNILVR",
            "hul": "HINDUNILVR",
            "larsen": "LT",
            "l&t": "LT",
            "lt": "LT",
            "sun pharma": "SUNPHARMA",
            "sunpharma": "SUNPHARMA",
            "titan": "TITAN",
            "adani enterprises": "ADANIENT",
            "adani": "ADANIENT",
            "adani ports": "ADANIPORTS",
            "power grid": "POWERGRID",
            "ntpc": "NTPC",
            "ongc": "ONGC",
            "coal india": "COALINDIA",
            "bpcl": "BPCL",
            "grasim": "GRASIM",
            "ultratech": "ULTRACEMCO",
            "nestle": "NESTLEIND",
            "britannia": "BRITANNIA",
            "cipla": "CIPLA",
            "dr reddy": "DRREDDY",
            "drreddy": "DRREDDY",
            "divis": "DIVISLAB",
            "divislab": "DIVISLAB",
            "eicher": "EICHERMOT",
            "eicher motors": "EICHERMOT",
            "hero": "HEROMOTOCO",
            "hero motocorp": "HEROMOTOCO",
            "bajaj auto": "BAJAJ-AUTO",
            "tech mahindra": "TECHM",
            "techm": "TECHM",
            "mahindra": "M&M",
            "m&m": "M&M",
            "hindalco": "HINDALCO",
            "jswsteel": "JSWSTEEL",
            "jsw steel": "JSWSTEEL",
            "indusind": "INDUSINDBK",
            "indusind bank": "INDUSINDBK",
            "sbi life": "SBILIFE",
            "hdfc life": "HDFCLIFE",
        }

        # Filter aliases to only include stocks in database
        valid_aliases = {}
        for alias, ticker in all_aliases.items():
            if ticker in available_stocks:
                valid_aliases[alias] = ticker

        # Also add direct ticker matches (lowercase ticker -> uppercase ticker)
        for ticker in available_stocks:
            valid_aliases[ticker.lower()] = ticker

        return jsonify(
            {
                "success": True,
                "aliases": valid_aliases,
                "stock_count": len(available_stocks),
                "available_stocks": sorted(list(available_stocks)),
            }
        )

    except Exception as e:
        print(f"[ERROR] Voice API - stock-aliases: {e}")
        return jsonify({"success": False, "aliases": {}, "error": str(e)})


@voice_api_bp.route("/help")
def get_voice_help():
    """
    Return list of available voice commands
    """
    # Get available stocks count
    try:
        tables = get_table_list()
        stock_count = len([t for t in tables if t.startswith("TBL_") and t.endswith("_DERIVED")])
    except:
        stock_count = 0

    commands = {
        "navigation": [
            "Go to home",
            "Go to dashboard",
            "Open screeners",
            "Show top gainers",
            "Signal analysis",
            "Futures OI",
            "Technical screener",
            "Nifty 50",
            "Bank Nifty",
        ],
        "search": [
            f"Search [any of {stock_count} available stocks]",
            "Find [stock name]",
            "Show [stock name]",
            "Open [stock name]",
        ],
        "actions": ["Export PDF", "Export Excel", "Filter bullish", "Filter bearish", "Show all", "Refresh page"],
        "queries": ["What is [stock] price", "Market summary", "What is the trend"],
        "utility": ["Scroll down", "Scroll up", "Go to top", "Go back", "Logout", "Help"],
    }

    return jsonify(
        {
            "success": True,
            "commands": commands,
            "stock_count": stock_count,
            "speech": f"I can help you navigate, search {stock_count} stocks, filter data, and more. Try saying Go to dashboard, Search followed by any stock name, or Show top gainers.",
        }
    )
