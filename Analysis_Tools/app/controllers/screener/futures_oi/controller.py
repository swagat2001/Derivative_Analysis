"""
FUTURES OI ANALYSIS CONTROLLER
==============================
Displays Futures OI data for CME, NME, FME expiries
"""

from flask import Blueprint, render_template, request
from flask_caching import Cache
from ....models.futures_oi_model import (
    get_futures_oi_available_dates,
    get_futures_oi_data
)

futures_oi_bp = Blueprint('futures_oi', __name__, url_prefix='/screener/futures-oi')
cache = Cache(config={'CACHE_TYPE': 'simple', 'CACHE_DEFAULT_TIMEOUT': 3600})


def get_signal_for_ticker(ticker, signals_data):
    """Get signal classification for a ticker from signal analysis data"""
    if not signals_data or ticker not in signals_data:
        return 'NEUTRAL'
    
    return signals_data[ticker].get('signal', 'NEUTRAL')


def get_signals_data_for_date(selected_date):
    """Get signal analysis data for a date using the existing signal analysis logic"""
    try:
        from ..signal_analysis.controller import get_signal_data_formatted
        return get_signal_data_formatted(selected_date)
    except Exception as e:
        print(f"Error getting signals: {e}")
    return None


def get_futures_oi_formatted(selected_date):
    """Get formatted futures OI data with signals"""
    # Get raw data
    futures_data = get_futures_oi_data(selected_date)
    
    if not futures_data:
        return None
    
    # Get signal analysis data for the same date
    signals_data = get_signals_data_for_date(selected_date)
    
    # Add signals to each item
    for expiry_type in ['CME', 'NME', 'FME']:
        if expiry_type in futures_data:
            for item in futures_data[expiry_type]:
                item['signal'] = get_signal_for_ticker(item.get('ticker', ''), signals_data)
    
    return futures_data


@futures_oi_bp.route('/')
def futures_oi_analysis():
    """Main futures OI analysis page"""
    # Get available dates and reverse to show newest first
    dates = get_futures_oi_available_dates()
    dates = list(reversed(dates))  # Newest first
    
    # Get selected date from query param or use latest
    selected_date = request.args.get('date')
    if not selected_date and dates:
        selected_date = dates[0]  # Latest date (first after reverse)
    
    # Get formatted data
    futures_data = None
    if selected_date:
        futures_data = get_futures_oi_formatted(selected_date)
    
    return render_template(
        'screener/futures_oi/index.html',
        dates=dates,
        selected_date=selected_date,
        futures_data=futures_data
    )


@futures_oi_bp.route('/clear-cache')
def clear_cache():
    """Clear cache for debugging"""
    cache.clear()
    return "Cache cleared", 200
