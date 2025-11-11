"""
Screener controller for Important Screener page.
OPTIMIZED: Uses single-pass data fetching.
"""

from flask import Blueprint, render_template, request, jsonify
from ..models.screener_model import get_all_screener_data
from ..models.dashboard_model import get_available_dates
from ..controllers.dashboard_controller import get_live_indices

screener_bp = Blueprint('screener', __name__)

@screener_bp.route('/screener')
def screener():
    """
    Important Screener page showing top 10 gainers/losers for:
    - OI changes (CALL/PUT, ALL/ITM/OTM)
    - Moneyness changes (CALL/PUT, ALL/ITM/OTM)
    - IV changes (CALL/PUT, ALL/ITM/OTM)
    """
    try:
        dates = get_available_dates()
        selected_date = request.args.get("date", dates[0] if dates else None)
        
        if not selected_date:
            return render_template(
                "screener.html",
                dates=[],
                selected_date=None,
                indices=get_live_indices(),
                screener_data={}
            )
        
        print(f"[INFO] Fetching screener data for {selected_date}...")
        
        # OPTIMIZED: Fetch all data in single pass
        all_data = get_all_screener_data(selected_date)
        
        if not all_data:
            return render_template(
                "screener.html",
                dates=dates,
                selected_date=selected_date,
                indices=get_live_indices(),
                screener_data={}
            )
        
        # Extract top 10 for each category
        screener_data = {}
        
        # OI Gainers (already sorted descending)
        screener_data['oi_call_gainers'] = all_data['oi']['CE']['ALL'][:10]
        screener_data['oi_call_itm_gainers'] = all_data['oi']['CE']['ITM'][:10]
        screener_data['oi_call_otm_gainers'] = all_data['oi']['CE']['OTM'][:10]
        screener_data['oi_put_gainers'] = all_data['oi']['PE']['ALL'][:10]
        screener_data['oi_put_itm_gainers'] = all_data['oi']['PE']['ITM'][:10]
        screener_data['oi_put_otm_gainers'] = all_data['oi']['PE']['OTM'][:10]
        
        # OI Losers (reverse sort - ascending)
        screener_data['oi_call_losers'] = sorted(all_data['oi']['CE']['ALL'], key=lambda x: x['change'])[:10]
        screener_data['oi_call_itm_losers'] = sorted(all_data['oi']['CE']['ITM'], key=lambda x: x['change'])[:10]
        screener_data['oi_call_otm_losers'] = sorted(all_data['oi']['CE']['OTM'], key=lambda x: x['change'])[:10]
        screener_data['oi_put_losers'] = sorted(all_data['oi']['PE']['ALL'], key=lambda x: x['change'])[:10]
        screener_data['oi_put_itm_losers'] = sorted(all_data['oi']['PE']['ITM'], key=lambda x: x['change'])[:10]
        screener_data['oi_put_otm_losers'] = sorted(all_data['oi']['PE']['OTM'], key=lambda x: x['change'])[:10]
        
        # Moneyness Gainers
        screener_data['moneyness_call_gainers'] = all_data['moneyness']['CE']['ALL'][:10]
        screener_data['moneyness_call_itm_gainers'] = all_data['moneyness']['CE']['ITM'][:10]
        screener_data['moneyness_call_otm_gainers'] = all_data['moneyness']['CE']['OTM'][:10]
        screener_data['moneyness_put_gainers'] = all_data['moneyness']['PE']['ALL'][:10]
        screener_data['moneyness_put_itm_gainers'] = all_data['moneyness']['PE']['ITM'][:10]
        screener_data['moneyness_put_otm_gainers'] = all_data['moneyness']['PE']['OTM'][:10]
        
        # Moneyness Losers
        screener_data['moneyness_call_losers'] = sorted(all_data['moneyness']['CE']['ALL'], key=lambda x: x['change'])[:10]
        screener_data['moneyness_call_itm_losers'] = sorted(all_data['moneyness']['CE']['ITM'], key=lambda x: x['change'])[:10]
        screener_data['moneyness_call_otm_losers'] = sorted(all_data['moneyness']['CE']['OTM'], key=lambda x: x['change'])[:10]
        screener_data['moneyness_put_losers'] = sorted(all_data['moneyness']['PE']['ALL'], key=lambda x: x['change'])[:10]
        screener_data['moneyness_put_itm_losers'] = sorted(all_data['moneyness']['PE']['ITM'], key=lambda x: x['change'])[:10]
        screener_data['moneyness_put_otm_losers'] = sorted(all_data['moneyness']['PE']['OTM'], key=lambda x: x['change'])[:10]
        
        # Future Moneyness Gainers and Losers (NEW)
        screener_data['future_moneyness_gainers'] = all_data['moneyness']['FUT']['ALL'][:10]
        screener_data['future_moneyness_losers'] = sorted(all_data['moneyness']['FUT']['ALL'], key=lambda x: x['change'])[:10]
        
        # IV Gainers
        screener_data['iv_call_gainers'] = all_data['iv']['CE']['ALL'][:10]
        screener_data['iv_call_itm_gainers'] = all_data['iv']['CE']['ITM'][:10]
        screener_data['iv_call_otm_gainers'] = all_data['iv']['CE']['OTM'][:10]
        screener_data['iv_put_gainers'] = all_data['iv']['PE']['ALL'][:10]
        screener_data['iv_put_itm_gainers'] = all_data['iv']['PE']['ITM'][:10]
        screener_data['iv_put_otm_gainers'] = all_data['iv']['PE']['OTM'][:10]
        
        # IV Losers
        screener_data['iv_call_losers'] = sorted(all_data['iv']['CE']['ALL'], key=lambda x: x['change'])[:10]
        screener_data['iv_call_itm_losers'] = sorted(all_data['iv']['CE']['ITM'], key=lambda x: x['change'])[:10]
        screener_data['iv_call_otm_losers'] = sorted(all_data['iv']['CE']['OTM'], key=lambda x: x['change'])[:10]
        screener_data['iv_put_losers'] = sorted(all_data['iv']['PE']['ALL'], key=lambda x: x['change'])[:10]
        screener_data['iv_put_itm_losers'] = sorted(all_data['iv']['PE']['ITM'], key=lambda x: x['change'])[:10]
        screener_data['iv_put_otm_losers'] = sorted(all_data['iv']['PE']['OTM'], key=lambda x: x['change'])[:10]
        
        # Futures - Only OI (Futures don't have Moneyness or IV)
        screener_data['future_oi_gainers'] = all_data['oi']['FUT']['ALL'][:10]
        screener_data['future_oi_losers'] = sorted(all_data['oi']['FUT']['ALL'], key=lambda x: x['change'])[:10]
        
        # Build ticker occurrence map for hover tooltip
        # Map internal keys to human-friendly section names
        section_names = {
            'oi_call_gainers': 'Top 10 OI Call Gainers (All)',
            'oi_call_itm_gainers': 'Top 10 ITM Call OI Gainers',
            'oi_call_otm_gainers': 'Top 10 OTM Call OI Gainers',
            'oi_put_gainers': 'Top 10 OI Put Gainers (All)',
            'oi_put_itm_gainers': 'Top 10 ITM Put OI Gainers',
            'oi_put_otm_gainers': 'Top 10 OTM Put OI Gainers',
            'oi_call_losers': 'Top 10 OI Call Losers (All)',
            'oi_call_itm_losers': 'Top 10 ITM Call OI Losers',
            'oi_call_otm_losers': 'Top 10 OTM Call OI Losers',
            'oi_put_losers': 'Top 10 OI Put Losers (All)',
            'oi_put_itm_losers': 'Top 10 ITM Put OI Losers',
            'oi_put_otm_losers': 'Top 10 OTM Put OI Losers',
            'moneyness_call_gainers': 'Top 10 Moneyness Call Gainers',
            'moneyness_call_itm_gainers': 'Top 10 ITM Call Moneyness Gainers',
            'moneyness_call_otm_gainers': 'Top 10 OTM Call Moneyness Gainers',
            'moneyness_put_gainers': 'Top 10 Moneyness Put Gainers',
            'moneyness_put_itm_gainers': 'Top 10 ITM Put Moneyness Gainers',
            'moneyness_put_otm_gainers': 'Top 10 OTM Put Moneyness Gainers',
            'moneyness_call_losers': 'Top 10 Moneyness Call Losers',
            'moneyness_call_itm_losers': 'Top 10 ITM Call Moneyness Losers',
            'moneyness_call_otm_losers': 'Top 10 OTM Call Moneyness Losers',
            'moneyness_put_losers': 'Top 10 Moneyness Put Losers',
            'moneyness_put_itm_losers': 'Top 10 ITM Put Moneyness Losers',
            'moneyness_put_otm_losers': 'Top 10 OTM Put Moneyness Losers',
            'future_oi_gainers': 'Top 10 Future OI Gainers',
            'future_oi_losers': 'Top 10 Future OI Losers',
            'future_moneyness_gainers': 'Top 10 Future Moneyness Gainers',
            'future_moneyness_losers': 'Top 10 Future Moneyness Losers'
        }

        # Build ticker occurrence count for hover tooltip (simplified)
        ticker_map = {}
        for key, display in section_names.items():
            items = screener_data.get(key, [])
            for idx, item in enumerate(items, 1):
                tk = item.get('ticker')
                if not tk:
                    continue
                # Just count appearances instead of listing all sections
                if tk not in ticker_map:
                    ticker_map[tk] = 0
                ticker_map[tk] += 1
        
        # Convert count to readable string
        ticker_map = {k: f"Appears in {v} list(s)" for k, v in ticker_map.items()}

        
        print(f"[INFO] Screener data prepared, rendering template...")
        
        return render_template(
            "screener.html",
            dates=dates,
            selected_date=selected_date,
            indices=get_live_indices(),
            screener_data=screener_data,
            ticker_map=ticker_map
        )
        
    except Exception as e:
        print(f"[ERROR] screener(): {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Screener rendering failed: {str(e)}"}), 500
