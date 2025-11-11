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
        
        print(f"[INFO] Screener data prepared, rendering template...")
        
        return render_template(
            "screener.html",
            dates=dates,
            selected_date=selected_date,
            indices=get_live_indices(),
            screener_data=screener_data
        )
        
    except Exception as e:
        print(f"[ERROR] screener(): {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Screener rendering failed: {str(e)}"}), 500
