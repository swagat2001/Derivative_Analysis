"""
TECHNICAL SCREENER CONTROLLER
=============================
Displays technical analysis data (RSI, MACD, SMA, ADX)
Signal column shows technical-based signal, not external signal analysis
"""

from flask import Blueprint, render_template, request
from flask_caching import Cache
from ....models.technical_screener_model import (
    get_technical_available_dates,
    get_rsi_overbought,
    get_rsi_oversold,
    get_macd_bullish_crossover,
    get_macd_bearish_crossover,
    get_above_both_sma,
    get_below_both_sma,
    get_strong_trending_stocks,
    get_heatmap_data
)

technical_screener_bp = Blueprint('technical_screener', __name__, url_prefix='/screener/technical')
cache = Cache(config={'CACHE_TYPE': 'simple', 'CACHE_DEFAULT_TIMEOUT': 3600})


def add_fixed_signal(data_list, signal_value):
    """Add a fixed signal value to each item in a list"""
    for item in data_list:
        item['signal'] = signal_value
    return data_list


def get_technical_data_formatted(selected_date):
    """Get all technical screener data with appropriate signals"""
    
    # Fetch all categories
    rsi_overbought = get_rsi_overbought(selected_date, limit=20)
    rsi_oversold = get_rsi_oversold(selected_date, limit=20)
    macd_bullish = get_macd_bullish_crossover(selected_date, limit=20)
    macd_bearish = get_macd_bearish_crossover(selected_date, limit=20)
    above_sma = get_above_both_sma(selected_date, limit=20)
    below_sma = get_below_both_sma(selected_date, limit=20)
    strong_trend = get_strong_trending_stocks(selected_date, limit=20)
    heatmap = get_heatmap_data(selected_date)
    
    # Add signals based on the category they belong to
    # RSI > 80 = Overbought = potential reversal = BEARISH signal
    # RSI < 20 = Oversold = potential reversal = BULLISH signal
    # MACD Bullish Crossover = BULLISH
    # MACD Bearish Crossover = BEARISH
    # Above SMA = BULLISH
    # Below SMA = BEARISH
    # Strong Trend = based on price direction (use RSI to determine)
    
    # For strong trend, determine signal based on RSI
    for item in strong_trend:
        rsi = item.get('rsi_14')
        if rsi and rsi > 50:
            item['signal'] = 'BULLISH'
        elif rsi and rsi < 50:
            item['signal'] = 'BEARISH'
        else:
            item['signal'] = 'NEUTRAL'
    
    return {
        'rsi_overbought': add_fixed_signal(rsi_overbought, 'BEARISH'),  # Overbought = potential sell
        'rsi_oversold': add_fixed_signal(rsi_oversold, 'BULLISH'),      # Oversold = potential buy
        'macd_bullish': add_fixed_signal(macd_bullish, 'BULLISH'),      # Bullish crossover
        'macd_bearish': add_fixed_signal(macd_bearish, 'BEARISH'),      # Bearish crossover
        'above_sma': add_fixed_signal(above_sma, 'BULLISH'),            # Above SMA = bullish
        'below_sma': add_fixed_signal(below_sma, 'BEARISH'),            # Below SMA = bearish
        'strong_trend': strong_trend,                                    # Based on RSI
        'heatmap': heatmap
    }


@technical_screener_bp.route('/')
def technical_screener():
    """Main technical screener page"""
    # Get available dates and reverse to show newest first
    dates = get_technical_available_dates()
    dates = list(reversed(dates))  # Newest first
    
    # Get selected date from query param or use latest
    selected_date = request.args.get('date')
    if not selected_date and dates:
        selected_date = dates[0]  # Latest date (first after reverse)
    
    # Get formatted data
    tech_data = None
    if selected_date:
        tech_data = get_technical_data_formatted(selected_date)
    
    return render_template(
        'screener/technical_screener/index.html',
        dates=dates,
        selected_date=selected_date,
        tech_data=tech_data
    )


@technical_screener_bp.route('/clear-cache')
def clear_cache():
    """Clear cache for debugging"""
    cache.clear()
    return "Cache cleared", 200
