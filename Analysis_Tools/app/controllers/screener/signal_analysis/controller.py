"""
Signal Analysis Controller
Handles bullish/bearish signal display with filtering and detailed breakdown
"""

from flask import Blueprint, render_template, request, jsonify
from flask_caching import Cache
from ....models.screener_model import get_all_screener_data
from ....models.dashboard_model import get_available_dates
from ....models.stock_model import get_filtered_tickers
from ....controllers.dashboard_controller import get_live_indices


signal_analysis_bp = Blueprint('signal_analysis', __name__, url_prefix='/screener/signal-analysis')


# Initialize cache
cache = Cache(config={
    'CACHE_TYPE': 'simple',
    'CACHE_DEFAULT_TIMEOUT': 3600
})


def _compute_final_signals_with_breakdown(screener_data):
    """
    Compute final signals WITH detailed breakdown of why each ticker is classified
    Returns: {
        'ticker': {
            'signal': 'BULLISH' or 'BEARISH',
            'bullish_count': int,
            'bearish_count': int,
            'bullish_categories': [list of categories],
            'bearish_categories': [list of categories]
        }
    }
    """
    
    bullish_sections = {
        'iv_call_gainers': 'IV Call Gainers (ALL)',
        'iv_call_itm_gainers': 'IV Call Gainers (ITM)',
        'iv_call_otm_gainers': 'IV Call Gainers (OTM)',
        'iv_put_losers': 'IV Put Losers (ALL)',
        'iv_put_itm_losers': 'IV Put Losers (ITM)',
        'iv_put_otm_losers': 'IV Put Losers (OTM)',
        'oi_call_gainers': 'OI Call Gainers (ALL)',
        'oi_call_itm_gainers': 'OI Call Gainers (ITM)',
        'oi_call_otm_gainers': 'OI Call Gainers (OTM)',
        'oi_put_losers': 'OI Put Losers (ALL)',
        'oi_put_itm_losers': 'OI Put Losers (ITM)',
        'oi_put_otm_losers': 'OI Put Losers (OTM)',
        'moneyness_call_gainers': 'Moneyness Call Gainers (ALL)',
        'moneyness_call_itm_gainers': 'Moneyness Call Gainers (ITM)',
        'moneyness_call_otm_gainers': 'Moneyness Call Gainers (OTM)',
        'moneyness_put_losers': 'Moneyness Put Losers (ALL)',
        'moneyness_put_itm_losers': 'Moneyness Put Losers (ITM)',
        'moneyness_put_otm_losers': 'Moneyness Put Losers (OTM)',
        'future_oi_gainers': 'Future OI Gainers'
    }

    bearish_sections = {
        'iv_call_losers': 'IV Call Losers (ALL)',
        'iv_call_itm_losers': 'IV Call Losers (ITM)',
        'iv_call_otm_losers': 'IV Call Losers (OTM)',
        'iv_put_gainers': 'IV Put Gainers (ALL)',
        'iv_put_itm_gainers': 'IV Put Gainers (ITM)',
        'iv_put_otm_gainers': 'IV Put Gainers (OTM)',
        'oi_call_losers': 'OI Call Losers (ALL)',
        'oi_call_itm_losers': 'OI Call Losers (ITM)',
        'oi_call_otm_losers': 'OI Call Losers (OTM)',
        'oi_put_gainers': 'OI Put Gainers (ALL)',
        'oi_put_itm_gainers': 'OI Put Gainers (ITM)',
        'oi_put_otm_gainers': 'OI Put Gainers (OTM)',
        'moneyness_call_losers': 'Moneyness Call Losers (ALL)',
        'moneyness_call_itm_losers': 'Moneyness Call Losers (ITM)',
        'moneyness_call_otm_losers': 'Moneyness Call Losers (OTM)',
        'moneyness_put_gainers': 'Moneyness Put Gainers (ALL)',
        'moneyness_put_itm_gainers': 'Moneyness Put Gainers (ITM)',
        'moneyness_put_otm_gainers': 'Moneyness Put Gainers (OTM)',
        'future_oi_losers': 'Future OI Losers'
    }

    signals = {}

    # Track bullish membership
    for sec_key, sec_name in bullish_sections.items():
        for row in screener_data.get(sec_key, []):
            ticker = row.get("ticker")
            if ticker:
                if ticker not in signals:
                    signals[ticker] = {
                        'signal': 'BEARISH',
                        'bullish_count': 0,
                        'bearish_count': 0,
                        'bullish_categories': [],
                        'bearish_categories': []
                    }
                signals[ticker]['bullish_count'] += 1
                signals[ticker]['bullish_categories'].append(sec_name)

    # Track bearish membership
    for sec_key, sec_name in bearish_sections.items():
        for row in screener_data.get(sec_key, []):
            ticker = row.get("ticker")
            if ticker:
                if ticker not in signals:
                    signals[ticker] = {
                        'signal': 'BEARISH',
                        'bullish_count': 0,
                        'bearish_count': 0,
                        'bullish_categories': [],
                        'bearish_categories': []
                    }
                signals[ticker]['bearish_count'] += 1
                signals[ticker]['bearish_categories'].append(sec_name)

    # Final classification - handle ties as NEUTRAL
    for ticker, data in signals.items():
        if data['bullish_count'] > data['bearish_count']:
            data['signal'] = 'BULLISH'
        elif data['bearish_count'] > data['bullish_count']:
            data['signal'] = 'BEARISH'
        else:
            data['signal'] = 'NEUTRAL'  # Equal counts = neutral

    return signals


@cache.memoize(timeout=3600)
def get_signal_data_formatted(selected_date):
    """Get formatted data for signal analysis"""
    try:
        # Get raw screener data
        all_data = get_all_screener_data(selected_date)
        
        if not all_data:
            return None
        
        # Build simplified structure for signal computation
        screener_data = {}
        
        # OI - Calls (6)
        screener_data['oi_call_gainers'] = all_data['oi']['CE']['ALL'][:10]
        screener_data['oi_call_itm_gainers'] = all_data['oi']['CE']['ITM'][:10]
        screener_data['oi_call_otm_gainers'] = all_data['oi']['CE']['OTM'][:10]
        screener_data['oi_call_losers'] = all_data['oi']['CE']['ALL_LOSERS'][:10]
        screener_data['oi_call_itm_losers'] = all_data['oi']['CE']['ITM_LOSERS'][:10]
        screener_data['oi_call_otm_losers'] = all_data['oi']['CE']['OTM_LOSERS'][:10]
        
        # OI - Puts (6)
        screener_data['oi_put_gainers'] = all_data['oi']['PE']['ALL'][:10]
        screener_data['oi_put_itm_gainers'] = all_data['oi']['PE']['ITM'][:10]
        screener_data['oi_put_otm_gainers'] = all_data['oi']['PE']['OTM'][:10]
        screener_data['oi_put_losers'] = all_data['oi']['PE']['ALL_LOSERS'][:10]
        screener_data['oi_put_itm_losers'] = all_data['oi']['PE']['ITM_LOSERS'][:10]
        screener_data['oi_put_otm_losers'] = all_data['oi']['PE']['OTM_LOSERS'][:10]
        
        # Moneyness - Calls (6)
        screener_data['moneyness_call_gainers'] = all_data['moneyness']['CE']['ALL'][:10]
        screener_data['moneyness_call_itm_gainers'] = all_data['moneyness']['CE']['ITM'][:10]
        screener_data['moneyness_call_otm_gainers'] = all_data['moneyness']['CE']['OTM'][:10]
        screener_data['moneyness_call_losers'] = all_data['moneyness']['CE']['ALL_LOSERS'][:10]
        screener_data['moneyness_call_itm_losers'] = all_data['moneyness']['CE']['ITM_LOSERS'][:10]
        screener_data['moneyness_call_otm_losers'] = all_data['moneyness']['CE']['OTM_LOSERS'][:10]
        
        # Moneyness - Puts (6)
        screener_data['moneyness_put_gainers'] = all_data['moneyness']['PE']['ALL'][:10]
        screener_data['moneyness_put_itm_gainers'] = all_data['moneyness']['PE']['ITM'][:10]
        screener_data['moneyness_put_otm_gainers'] = all_data['moneyness']['PE']['OTM'][:10]
        screener_data['moneyness_put_losers'] = all_data['moneyness']['PE']['ALL_LOSERS'][:10]
        screener_data['moneyness_put_itm_losers'] = all_data['moneyness']['PE']['ITM_LOSERS'][:10]
        screener_data['moneyness_put_otm_losers'] = all_data['moneyness']['PE']['OTM_LOSERS'][:10]
        
        # IV - Calls (6)
        screener_data['iv_call_gainers'] = all_data['iv']['CE']['ALL'][:10]
        screener_data['iv_call_itm_gainers'] = all_data['iv']['CE']['ITM'][:10]
        screener_data['iv_call_otm_gainers'] = all_data['iv']['CE']['OTM'][:10]
        screener_data['iv_call_losers'] = all_data['iv']['CE']['ALL_LOSERS'][:10]
        screener_data['iv_call_itm_losers'] = all_data['iv']['CE']['ITM_LOSERS'][:10]
        screener_data['iv_call_otm_losers'] = all_data['iv']['CE']['OTM_LOSERS'][:10]
        
        # IV - Puts (6)
        screener_data['iv_put_gainers'] = all_data['iv']['PE']['ALL'][:10]
        screener_data['iv_put_itm_gainers'] = all_data['iv']['PE']['ITM'][:10]
        screener_data['iv_put_otm_gainers'] = all_data['iv']['PE']['OTM'][:10]
        screener_data['iv_put_losers'] = all_data['iv']['PE']['ALL_LOSERS'][:10]
        screener_data['iv_put_itm_losers'] = all_data['iv']['PE']['ITM_LOSERS'][:10]
        screener_data['iv_put_otm_losers'] = all_data['iv']['PE']['OTM_LOSERS'][:10]
        
        # Futures (2)
        screener_data['future_oi_gainers'] = all_data['oi']['FUT']['ALL'][:10]
        screener_data['future_oi_losers'] = all_data['oi']['FUT']['ALL_LOSERS'][:10]

        # Compute signals with breakdown
        signals = _compute_final_signals_with_breakdown(screener_data)
        
        return signals
        
    except Exception as e:
        print(f"[ERROR] get_signal_data_formatted: {e}")
        import traceback
        traceback.print_exc()
        return None


def _apply_sorting(signals_list, sort_by, sort_order, signal_filter):
    """
    Apply sorting to signals list based on column and order
    
    Args:
        signals_list: list of signal dictionaries
        sort_by: 'symbol' or 'strength'
        sort_order: 'asc' or 'desc'
        signal_filter: 'all', 'bullish', 'bearish', 'neutral'
    
    Returns:
        Sorted list
    """
    if sort_by == 'symbol':
        # Sort by ticker name (alphabetically)
        signals_list.sort(
            key=lambda x: x['ticker'].lower(),
            reverse=(sort_order == 'desc')
        )
    elif sort_by == 'strength':
        # Sort based on active filter
        if signal_filter == 'bullish':
            # Sort by bullish count only
            signals_list.sort(
                key=lambda x: x['bullish_count'],
                reverse=(sort_order == 'desc')
            )
        elif signal_filter == 'bearish':
            # Sort by bearish count only
            signals_list.sort(
                key=lambda x: x['bearish_count'],
                reverse=(sort_order == 'desc')
            )
        else:
            # For 'all' and 'neutral' - sort by net score
            signals_list.sort(
                key=lambda x: x['score'],
                reverse=(sort_order == 'desc')
            )
    else:
        # Default: sort by net strength (score), descending
        signals_list.sort(
            key=lambda x: x['score'],
            reverse=True
        )
    
    return signals_list


@signal_analysis_bp.route('/')
def signal_analysis():
    """Display signal analysis page"""
    try:
        dates = get_available_dates()
        selected_date = request.args.get("date", dates[0] if dates else None)
        signal_filter = request.args.get("filter", "all")
        sort_by = request.args.get("sort", "strength")
        sort_order = request.args.get("order", "desc")
        
        if not selected_date:
            return jsonify({"error": "No dates available"}), 404
        
        signals_dict = get_signal_data_formatted(selected_date)
        
        if not signals_dict:
            signals_dict = {}
        
        # Convert dict to list with score calculation
        signals_list = []
        for ticker, data in signals_dict.items():
            score = data['bullish_count'] - data['bearish_count']
            
            signals_list.append({
                'ticker': ticker,
                'signal': data['signal'],
                'bullish_count': data['bullish_count'],
                'bearish_count': data['bearish_count'],
                'score': score,
                'bullish_categories': data['bullish_categories'],
                'bearish_categories': data['bearish_categories']
            })
        
        # Apply filter
        if signal_filter == "bullish":
            signals_list = [s for s in signals_list if s['signal'] == 'BULLISH']
        elif signal_filter == "bearish":
            signals_list = [s for s in signals_list if s['signal'] == 'BEARISH']
        elif signal_filter == "neutral":
            signals_list = [s for s in signals_list if s['signal'] == 'NEUTRAL']
        
        
        # Apply sorting
        signals_list = _apply_sorting(signals_list, sort_by, sort_order, signal_filter)
        
        return render_template(
            "screener/signal_analysis/index.html",
            dates=dates,
            selected_date=selected_date,
            indices=get_live_indices(),
            signals=signals_list,
            signal_filter=signal_filter,
            sort_by=sort_by,
            sort_order=sort_order,
            stock_list=get_filtered_tickers(),
            stock_symbol=None
        )
        
    except Exception as e:
        print(f"[ERROR] signal_analysis(): {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Signal analysis failed: {str(e)}"}), 500
