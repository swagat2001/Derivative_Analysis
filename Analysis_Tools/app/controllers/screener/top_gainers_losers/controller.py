"""
Screener controller for Important Screener page.
OPTIMIZED: Uses single-pass data fetching with Flask-Caching.
Includes PDF export functionality with Goldmine letterhead design.

IMPROVEMENTS:
1. Futures tables now exclude Strike column in PDF (all values are 0)
2. Proper path handling for templates and images
3. FINAL SIGNALS (BULLISH / BEARISH) loaded from screener cache (preferred)
   - Displayed in the screener UI (final_signals)
   - Printed as a separate table in the PDF after all 40 tables and before disclaimer
"""

from flask import Blueprint, render_template, request, jsonify, send_file
from flask_caching import Cache
from ....models.screener_model import get_all_screener_data
from ....models.dashboard_model import get_available_dates
from ....models.stock_model import get_filtered_tickers
from ....controllers.dashboard_controller import get_live_indices
from io import BytesIO
from datetime import datetime, timedelta
import os
import base64
import html as html_lib

gainers_losers_bp = Blueprint('gainers_losers', __name__, url_prefix='/screener/top-gainers-losers')

# Initialize cache
cache = Cache(config={
    'CACHE_TYPE': 'simple',
    'CACHE_DEFAULT_TIMEOUT': 3600
})


# ========================================================================
# SHARED DATA FORMATTING - CACHED
# ========================================================================


def _compute_final_signals_from_db_rows(screener_data):
    """Pure membership-based bullish/bearish scoring"""

    bullish_sections = [
        'iv_call_gainers', 'iv_call_itm_gainers', 'iv_call_otm_gainers',
        'iv_put_losers', 'iv_put_itm_losers', 'iv_put_otm_losers',
        'oi_call_gainers', 'oi_call_itm_gainers', 'oi_call_otm_gainers',
        'oi_put_losers', 'oi_put_itm_losers', 'oi_put_otm_losers',
        'moneyness_call_gainers', 'moneyness_call_itm_gainers', 'moneyness_call_otm_gainers',
        'moneyness_put_losers', 'moneyness_put_itm_losers', 'moneyness_put_otm_losers',
        'future_oi_gainers'
    ]

    bearish_sections = [
        'iv_call_losers', 'iv_call_itm_losers', 'iv_call_otm_losers',
        'iv_put_gainers', 'iv_put_itm_gainers', 'iv_put_otm_gainers',
        'oi_call_losers', 'oi_call_itm_losers', 'oi_call_otm_losers',
        'oi_put_gainers', 'oi_put_itm_gainers', 'oi_put_otm_gainers',
        'moneyness_call_losers', 'moneyness_call_itm_losers', 'moneyness_call_otm_losers',
        'moneyness_put_gainers', 'moneyness_put_itm_gainers', 'moneyness_put_otm_gainers',
        'future_oi_losers'
    ]

    bullish_cnt = {}
    bearish_cnt = {}

    # count bullish membership
    for sec in bullish_sections:
        for r in screener_data.get(sec, []):
            t = r.get("ticker")
            if t:
                bullish_cnt[t] = bullish_cnt.get(t, 0) + 1

    # count bearish membership
    for sec in bearish_sections:
        for r in screener_data.get(sec, []):
            t = r.get("ticker")
            if t:
                bearish_cnt[t] = bearish_cnt.get(t, 0) + 1

    # final classification
    finals = {}
    all_tickers = set(list(bullish_cnt.keys()) + list(bearish_cnt.keys()))

    for t in all_tickers:
        b = bullish_cnt.get(t, 0)
        s = bearish_cnt.get(t, 0)
        finals[t] = "BULLISH" if b > s else "BEARISH"

    return finals


@cache.memoize(timeout=3600)
def get_screener_data_formatted(selected_date):
    """
    Single data fetch - cached for 1 hour.
    Returns None if no data available.
    """
    try:
        all_data = get_all_screener_data(selected_date)
        
        if not all_data:
            return None
        
        screener_data = {}
        
        # OI - Calls (6 tables)
        screener_data['oi_call_gainers'] = all_data['oi']['CE']['ALL'][:10]
        screener_data['oi_call_itm_gainers'] = all_data['oi']['CE']['ITM'][:10]
        screener_data['oi_call_otm_gainers'] = all_data['oi']['CE']['OTM'][:10]
        screener_data['oi_call_losers'] = all_data['oi']['CE']['ALL_LOSERS'][:10]
        screener_data['oi_call_itm_losers'] = all_data['oi']['CE']['ITM_LOSERS'][:10]
        screener_data['oi_call_otm_losers'] = all_data['oi']['CE']['OTM_LOSERS'][:10]
        
        # OI - Puts (6 tables)
        screener_data['oi_put_gainers'] = all_data['oi']['PE']['ALL'][:10]
        screener_data['oi_put_itm_gainers'] = all_data['oi']['PE']['ITM'][:10]
        screener_data['oi_put_otm_gainers'] = all_data['oi']['PE']['OTM'][:10]
        screener_data['oi_put_losers'] = all_data['oi']['PE']['ALL_LOSERS'][:10]
        screener_data['oi_put_itm_losers'] = all_data['oi']['PE']['ITM_LOSERS'][:10]
        screener_data['oi_put_otm_losers'] = all_data['oi']['PE']['OTM_LOSERS'][:10]
        
        # Moneyness - Calls (6 tables)
        screener_data['moneyness_call_gainers'] = all_data['moneyness']['CE']['ALL'][:10]
        screener_data['moneyness_call_itm_gainers'] = all_data['moneyness']['CE']['ITM'][:10]
        screener_data['moneyness_call_otm_gainers'] = all_data['moneyness']['CE']['OTM'][:10]
        screener_data['moneyness_call_losers'] = all_data['moneyness']['CE']['ALL_LOSERS'][:10]
        screener_data['moneyness_call_itm_losers'] = all_data['moneyness']['CE']['ITM_LOSERS'][:10]
        screener_data['moneyness_call_otm_losers'] = all_data['moneyness']['CE']['OTM_LOSERS'][:10]
        
        # Moneyness - Puts (6 tables)
        screener_data['moneyness_put_gainers'] = all_data['moneyness']['PE']['ALL'][:10]
        screener_data['moneyness_put_itm_gainers'] = all_data['moneyness']['PE']['ITM'][:10]
        screener_data['moneyness_put_otm_gainers'] = all_data['moneyness']['PE']['OTM'][:10]
        screener_data['moneyness_put_losers'] = all_data['moneyness']['PE']['ALL_LOSERS'][:10]
        screener_data['moneyness_put_itm_losers'] = all_data['moneyness']['PE']['ITM_LOSERS'][:10]
        screener_data['moneyness_put_otm_losers'] = all_data['moneyness']['PE']['OTM_LOSERS'][:10]
        
        # IV - Calls (6 tables)
        screener_data['iv_call_gainers'] = all_data['iv']['CE']['ALL'][:10]
        screener_data['iv_call_itm_gainers'] = all_data['iv']['CE']['ITM'][:10]
        screener_data['iv_call_otm_gainers'] = all_data['iv']['CE']['OTM'][:10]
        screener_data['iv_call_losers'] = all_data['iv']['CE']['ALL_LOSERS'][:10]
        screener_data['iv_call_itm_losers'] = all_data['iv']['CE']['ITM_LOSERS'][:10]
        screener_data['iv_call_otm_losers'] = all_data['iv']['CE']['OTM_LOSERS'][:10]
        
        # IV - Puts (6 tables)
        screener_data['iv_put_gainers'] = all_data['iv']['PE']['ALL'][:10]
        screener_data['iv_put_itm_gainers'] = all_data['iv']['PE']['ITM'][:10]
        screener_data['iv_put_otm_gainers'] = all_data['iv']['PE']['OTM'][:10]
        screener_data['iv_put_losers'] = all_data['iv']['PE']['ALL_LOSERS'][:10]
        screener_data['iv_put_itm_losers'] = all_data['iv']['PE']['ITM_LOSERS'][:10]
        screener_data['iv_put_otm_losers'] = all_data['iv']['PE']['OTM_LOSERS'][:10]
        
        # Futures (4 tables)
        screener_data['future_oi_gainers'] = all_data['oi']['FUT']['ALL'][:10]
        screener_data['future_oi_losers'] = all_data['oi']['FUT']['ALL_LOSERS'][:10]
        screener_data['future_moneyness_gainers'] = all_data['moneyness']['FUT']['ALL'][:10]
        screener_data['future_moneyness_losers'] = all_data['moneyness']['FUT']['ALL_LOSERS'][:10]

        # Build final signals (preferred: read from DB rows if present)
        screener_data['final_signals'] = _compute_final_signals_from_db_rows(screener_data)
        
        return screener_data
        
    except Exception as e:
        print(f"[ERROR] get_screener_data_formatted: {e}")
        import traceback
        traceback.print_exc()
        return None


def build_ticker_map(screener_data):
    """Build ticker to section mapping for tooltips"""
    section_names = {
        'oi_call_gainers': 'Top 10 Call OI Gainers',
        'oi_call_itm_gainers': 'Top 10 ITM Call OI Gainers',
        'oi_call_otm_gainers': 'Top 10 OTM Call OI Gainers',
        'oi_call_losers': 'Top 10 Call OI Losers',
        'oi_call_itm_losers': 'Top 10 ITM Call OI Losers',
        'oi_call_otm_losers': 'Top 10 OTM Call OI Losers',
        'oi_put_gainers': 'Top 10 Put OI Gainers',
        'oi_put_itm_gainers': 'Top 10 ITM Put OI Gainers',
        'oi_put_otm_gainers': 'Top 10 OTM Put OI Gainers',
        'oi_put_losers': 'Top 10 Put OI Losers',
        'oi_put_itm_losers': 'Top 10 ITM Put OI Losers',
        'oi_put_otm_losers': 'Top 10 OTM Put OI Losers',
        'iv_call_gainers': 'Top 10 IV Call Gainers',
        'iv_call_itm_gainers': 'Top 10 ITM Call IV Gainers',
        'iv_call_otm_gainers': 'Top 10 OTM Call IV Gainers',
        'iv_call_losers': 'Top 10 IV Call Losers',
        'iv_call_itm_losers': 'Top 10 ITM Call IV Losers',
        'iv_call_otm_losers': 'Top 10 OTM Call IV Losers',
        'iv_put_gainers': 'Top 10 IV Put Gainers',
        'iv_put_itm_gainers': 'Top 10 ITM Put IV Gainers',
        'iv_put_otm_gainers': 'Top 10 OTM Put IV Gainers',
        'iv_put_losers': 'Top 10 IV Put Losers',
        'iv_put_itm_losers': 'Top 10 ITM Put IV Losers',
        'iv_put_otm_losers': 'Top 10 OTM Put IV Losers',
        'moneyness_call_gainers': 'Top 10 Moneyness Call Gainers',
        'moneyness_call_itm_gainers': 'Top 10 ITM Call Moneyness Gainers',
        'moneyness_call_otm_gainers': 'Top 10 OTM Call Moneyness Gainers',
        'moneyness_call_losers': 'Top 10 Moneyness Call Losers',
        'moneyness_call_itm_losers': 'Top 10 ITM Call Moneyness Losers',
        'moneyness_call_otm_losers': 'Top 10 OTM Call Moneyness Losers',
        'moneyness_put_gainers': 'Top 10 Moneyness Put Gainers',
        'moneyness_put_itm_gainers': 'Top 10 ITM Put Moneyness Gainers',
        'moneyness_put_otm_gainers': 'Top 10 OTM Put Moneyness Gainers',
        'moneyness_put_losers': 'Top 10 Moneyness Put Losers',
        'moneyness_put_itm_losers': 'Top 10 ITM Put Moneyness Losers',
        'moneyness_put_otm_losers': 'Top 10 OTM Put Moneyness Losers',
        'future_oi_gainers': 'Top 10 Future OI Gainers',
        'future_oi_losers': 'Top 10 Future OI Losers',
        'future_moneyness_gainers': 'Top 10 Future Moneyness Gainers',
        'future_moneyness_losers': 'Top 10 Future Moneyness Losers',
    }
    
    ticker_map = {}
    for key, display in section_names.items():
        items = screener_data.get(key, [])
        for item in items:
            tk = item.get('ticker')
            if tk:
                ticker_map.setdefault(tk, []).append(display)
    
    return {k: "\n".join(v) for k, v in ticker_map.items()}


# ========================================================================
# MAIN SCREENER PAGE
# ========================================================================

@gainers_losers_bp.route('/')
def top_gainers_losers():
    """Display screener page - now uses cached data"""
    try:
        dates = get_available_dates()
        selected_date = request.args.get("date", dates[0] if dates else None)
        
        if not selected_date:
            return jsonify({"error": "No dates available"}), 404
        
        screener_data = get_screener_data_formatted(selected_date)
        
        if not screener_data:
            return render_template(
                "screener/top_gainers_losers/index.html",
                dates=dates,
                selected_date=selected_date,
                indices=get_live_indices(),
                screener_data={},
                ticker_map={},
                stock_list=get_filtered_tickers(),
                stock_symbol=None,
                final_signals={}
            )
        
        ticker_map = build_ticker_map(screener_data)
        
        # pass final_signals to template for UI display
        final_signals = screener_data.get('final_signals', {})
        print("FINAL SIGNALS UI →", final_signals)

        
        return render_template(
            "screener/top_gainers_losers/index.html",
            dates=dates,
            selected_date=selected_date,
            indices=get_live_indices(),
            screener_data=screener_data,
            ticker_map=ticker_map,
            stock_list=get_filtered_tickers(),
            stock_symbol=None,
            final_signals=final_signals
        )
        
    except Exception as e:
        print(f"[ERROR] screener(): {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Screener rendering failed: {str(e)}"}), 500


# ========================================================================
# PDF GENERATION HELPER FUNCTION
# ========================================================================


def create_screener_pdf(screener_data, selected_date):
    """
    Generate PDF using Playwright (Chrome rendering)
    - Loads cover HTML
    - Loads tables HTML (40 tables with footer)
    - Converts images to base64
    - Renders both to PDF with Playwright
    - Merges with PyPDF2
    
    IMPROVEMENTS:
    1. Futures tables exclude Strike column (pass is_future=True)
    2. Uses correct paths from original controller
    3. Inserts final_signal_table AFTER all 40 tables and BEFORE disclaimer
    """
    try:
        from playwright.sync_api import sync_playwright
        from PyPDF2 import PdfMerger
        import tempfile
        
        print("[INFO] 📄 Starting PDF generation with Playwright...")
        
        # ============================================================
        # CALCULATE DATE FROM SELECTED_DATE PARAMETER
        # ============================================================
        try:
            date_obj = datetime.strptime(selected_date, '%Y-%m-%d')
            d = date_obj.day
            suf = "TH" if 11 <= d <= 13 else {1: "ST", 2: "ND", 3: "RD"}.get(d % 10, "TH")
            cover_date = f"{d}{suf} {date_obj.strftime('%b').upper()} {date_obj.year}"
            print(f"[INFO] Using selected date: {cover_date}")
        except Exception as e:
            print(f"[WARN] Failed to parse selected_date '{selected_date}': {e}")
            yesterday = datetime.now() - timedelta(days=1)
            d = yesterday.day
            suf = "TH" if 11 <= d <= 13 else {1: "ST", 2: "ND", 3: "RD"}.get(d % 10, "TH")
            cover_date = f"{d}{suf} {yesterday.strftime('%b').upper()} {yesterday.year}"
        
        # ============================================================
        # PATHS (Using correct paths from your project structure)
        # ============================================================
        print("[INFO] 🗂️  Setting up paths...")
        base_views = r"C:\Users\Admin\Desktop\Derivative_Analysis\Analysis_Tools\app\views\screener\top_gainers_losers"
        cover_path = os.path.join(base_views, "screener_cover_a4.html")
        table_path = os.path.join(base_views, "screener_table_pages.html")
        asset = r"C:\Users\Admin\Desktop\Derivative_Analysis\Analysis_Tools\app\static\image"
        
        print(f"[DEBUG] Cover path: {cover_path}")
        print(f"[DEBUG] Table path: {table_path}")
        print(f"[DEBUG] Asset path: {asset}")
        
        # ============================================================
        # CONVERT IMAGES TO BASE64
        # ============================================================
        print("[INFO] 🖼️  Encoding images to base64...")
        
        def img_to_base64(img_path):
            """Convert image file to base64 data URL"""
            try:
                with open(img_path, "rb") as f:
                    return f"data:image/png;base64,{base64.b64encode(f.read()).decode()}"
            except Exception as e:
                print(f"[WARN] Failed to encode {img_path}: {e}")
                return ""
        
        logo_b64 = img_to_base64(os.path.join(asset, "screener_cover_page_logo.png"))
        bull_b64 = img_to_base64(os.path.join(asset, "bull.png"))
        fb_b64 = img_to_base64(os.path.join(asset, "facebook_icon.png"))
        ig_b64 = img_to_base64(os.path.join(asset, "instagram_icon.png"))
        li_b64 = img_to_base64(os.path.join(asset, "linkedin_icon.png"))
        yt_b64 = img_to_base64(os.path.join(asset, "youtube_icon.png"))
        
        print("[INFO] ✓ Images encoded")
        
        # ============================================================
        # HELPER FUNCTIONS
        # ============================================================
        
        def safe_text(value, format_type="str"):
            """Format value safely"""
            try:
                if value is None:
                    return "—"
                if format_type == "int":
                    return str(int(value)) if value else "0"
                if format_type == "1f":
                    return f"{float(value):.1f}" if value else "—"
                if format_type == "pct":
                    return f"{float(value):+.1f}%" if value else "—"
                return str(value)
            except:
                return "—"
        
        def safe(key):
            """Get data safely from cache"""
            return screener_data.get(key, []) or []
        
        # ============================================================
        # STEP 1: PREPARE COVER HTML
        # ============================================================
        print("[INFO] 📖 Step 1: Reading cover HTML...")
        with open(cover_path, "r", encoding="utf-8") as f:
            cover_html = f.read()

        # Replace date
        cover_html = cover_html.replace('{{DATE_PLACEHOLDER}}', cover_date)
        
        # Replace image paths with base64
        cover_html = cover_html.replace('src="../../static/image/screener_cover_page_logo.png"', f'src="{logo_b64}"')
        cover_html = cover_html.replace('src="../../static/image/bull.png"', f'src="{bull_b64}"')
        cover_html = cover_html.replace('src="/static/image/facebook_icon.png"', f'src="{fb_b64}"')
        cover_html = cover_html.replace('src="/static/image/instagram_icon.png"', f'src="{ig_b64}"')
        cover_html = cover_html.replace('src="/static/image/linkedin_icon.png"', f'src="{li_b64}"')
        cover_html = cover_html.replace('src="/static/image/youtube_icon.png"', f'src="{yt_b64}"')
        
        print("[INFO] ✓ Cover HTML prepared")
        
        # ============================================================
        # STEP 2: PREPARE TABLES HTML (Generate all 40 mini-tables)
        # ============================================================
        print("[INFO] 📊 Step 2: Generating 40 tables with mini-table HTML...")
        
        with open(table_path, "r", encoding="utf-8") as f:
            tables_html = f.read()

        def make_mini_table(data, title, is_future=False):
            """Generate HTML table WITH title. Removes Strike column for Futures."""
            
            if not isinstance(data, list):
                data = []
            rows = data[:10]  # Top 10 rows

            # Table title bar
            H = f"<div style='font-size:9pt; font-weight:bold; color:#8B2432; margin:8px 0 3px 0; padding:3px 5px; background:#f5f5f5; border-left:3px solid #8B2432;'>{title}</div>"

            # Table
            H += "<table style='width:100%; border-collapse:collapse; font-size:7pt; margin-bottom:3pt;'>"
            H += "<tr style='background:#333; color:white;'>"
            H += "<th style='padding:2pt; border:1px solid #ccc;'>#</th>"
            H += "<th style='padding:2pt; border:1px solid #ccc;'>Symbol</th>"

            # Strike column only if NOT future
            if not is_future:
                H += "<th style='padding:2pt; border:1px solid #ccc;'>Strike</th>"

            H += "<th style='padding:2pt; border:1px solid #ccc;'>Price</th>"
            H += "<th style='padding:2pt; border:1px solid #ccc;'>Change%</th>"
            H += "</tr>"

            # Empty table case
            if not rows:
                colspan = "4" if is_future else "5"
                H += f"<tr><td colspan='{colspan}' style='text-align:center; padding:6pt; color:#999;'>No data available</td></tr>"
            
            else:
                for i, itm in enumerate(rows, 1):
                    itm = itm or {}
                    tk = html_lib.escape(safe_text(itm.get("ticker", "")))
                    sp = safe_text(itm.get("strike_price"), "int")
                    up = safe_text(itm.get("underlying_price"), "1f")
                    ch = safe_text(itm.get("change"), "pct")

                    # Color coding
                    change_style = ""
                    if itm.get("change") is not None:
                        try:
                            if float(itm["change"]) > 0:
                                change_style = " style='color:#28a745; font-weight:bold;'"
                            elif float(itm["change"]) < 0:
                                change_style = " style='color:#dc3545; font-weight:bold;'"
                        except:
                            change_style = ""

                    bg = "#f5f5f5" if i % 2 == 0 else "white"

                    H += f"<tr style='background:{bg};'>"
                    H += f"<td style='padding:2pt; border:1px solid #ddd; text-align:center;'>{i}</td>"
                    H += f"<td style='padding:2pt; border:1px solid #ddd;'>{tk}</td>"

                    # Only add Strike column for non-future tables
                    if not is_future:
                        H += f"<td style='padding:2pt; border:1px solid #ddd; text-align:center;'>{sp}</td>"

                    H += f"<td style='padding:2pt; border:1px solid #ddd; text-align:right;'>{up}</td>"
                    H += f"<td style='padding:2pt; border:1px solid #ddd; text-align:right;'{change_style}>{ch}</td>"
                    H += "</tr>"

            H += "</table>"
            return H


        # final signals table generator (placed AFTER all 40 tables and BEFORE disclaimer)
        def make_final_signal_table(final_signals, max_rows=50):
            """
            final_signals: dict {ticker: 'BULLISH'|'BEARISH'}
            Sorted: All BULLISH first (alphabetical), then BEARISH (alphabetical).
            """
            if not isinstance(final_signals, dict):
                final_signals = {}

            # Convert to list and sort
            bulls = sorted([t for t, s in final_signals.items() if s == 'BULLISH'])
            bears = sorted([t for t, s in final_signals.items() if s == 'BEARISH'])

            combined = bulls + bears
            combined = combined[:max_rows]

            H = "<div style='font-size:10pt; font-weight:bold; margin:8px 0;'>Final Bullish / Bearish Signals</div>"
            H += "<table style='width:100%; border-collapse:collapse; font-size:8pt; margin-bottom:8pt;'>"
            H += "<tr style='background:#222; color:white;'>"
            H += "<th style='padding:6px; border:1px solid #ccc; text-align:left;'>#</th>"
            H += "<th style='padding:6px; border:1px solid #ccc; text-align:left;'>Symbol</th>"
            H += "<th style='padding:6px; border:1px solid #ccc; text-align:left;'>Signal</th>"
            H += "</tr>"

            if not combined:
                H += "<tr><td colspan='3' style='text-align:center; padding:8px; color:#999;'>No final signals available</td></tr>"
            else:
                for i, tk in enumerate(combined, 1):
                    sig = 'BULLISH' if tk in bulls else 'BEARISH'
                    color = "#28a745" if sig == 'BULLISH' else "#dc3545"
                    bg = "#f7f7f7" if i % 2 == 0 else "white"
                    H += f"<tr style='background:{bg};'>"
                    H += f"<td style='padding:6px; border:1px solid #ddd;'>{i}</td>"
                    H += f"<td style='padding:6px; border:1px solid #ddd;'>{html_lib.escape(tk)}</td>"
                    H += f"<td style='padding:6px; border:1px solid #ddd; color:{color}; font-weight:bold;'>{sig}</td>"
                    H += "</tr>"

            H += "</table>"
            return H


        # Generate all 40 tables (26 for Options + 4 for Futures)
        replacements = {
            # OI - Calls (6)
            '{{oi_call_gainers_table}}': make_mini_table(safe('oi_call_gainers'), 'Top 10 Call OI Gainers (ALL)'),
            '{{oi_call_itm_gainers_table}}': make_mini_table(safe('oi_call_itm_gainers'), 'Top 10 ITM Call OI Gainers'),
            '{{oi_call_otm_gainers_table}}': make_mini_table(safe('oi_call_otm_gainers'), 'Top 10 OTM Call OI Gainers'),
            '{{oi_call_losers_table}}': make_mini_table(safe('oi_call_losers'), 'Top 10 Call OI Losers (ALL)'),
            '{{oi_call_itm_losers_table}}': make_mini_table(safe('oi_call_itm_losers'), 'Top 10 ITM Call OI Losers'),
            '{{oi_call_otm_losers_table}}': make_mini_table(safe('oi_call_otm_losers'), 'Top 10 OTM Call OI Losers'),
            
            # OI - Puts (6)
            '{{oi_put_gainers_table}}': make_mini_table(safe('oi_put_gainers'), 'Top 10 Put OI Gainers (ALL)'),
            '{{oi_put_itm_gainers_table}}': make_mini_table(safe('oi_put_itm_gainers'), 'Top 10 ITM Put OI Gainers'),
            '{{oi_put_otm_gainers_table}}': make_mini_table(safe('oi_put_otm_gainers'), 'Top 10 OTM Put OI Gainers'),
            '{{oi_put_losers_table}}': make_mini_table(safe('oi_put_losers'), 'Top 10 Put OI Losers (ALL)'),
            '{{oi_put_itm_losers_table}}': make_mini_table(safe('oi_put_itm_losers'), 'Top 10 ITM Put OI Losers'),
            '{{oi_put_otm_losers_table}}': make_mini_table(safe('oi_put_otm_losers'), 'Top 10 OTM Put OI Losers'),
            
            # IV - Calls (6)
            '{{iv_call_gainers_table}}': make_mini_table(safe('iv_call_gainers'), 'Top 10 IV Call Gainers (ALL)'),
            '{{iv_call_itm_gainers_table}}': make_mini_table(safe('iv_call_itm_gainers'), 'Top 10 ITM Call IV Gainers'),
            '{{iv_call_otm_gainers_table}}': make_mini_table(safe('iv_call_otm_gainers'), 'Top 10 OTM Call IV Gainers'),
            '{{iv_call_losers_table}}': make_mini_table(safe('iv_call_losers'), 'Top 10 IV Call Losers (ALL)'),
            '{{iv_call_itm_losers_table}}': make_mini_table(safe('iv_call_itm_losers'), 'Top 10 ITM Call IV Losers'),
            '{{iv_call_otm_losers_table}}': make_mini_table(safe('iv_call_otm_losers'), 'Top 10 OTM Call IV Losers'),
            
            # IV - Puts (6)
            '{{iv_put_gainers_table}}': make_mini_table(safe('iv_put_gainers'), 'Top 10 IV Put Gainers (ALL)'),
            '{{iv_put_itm_gainers_table}}': make_mini_table(safe('iv_put_itm_gainers'), 'Top 10 ITM IV Put Gainers'),
            '{{iv_put_otm_gainers_table}}': make_mini_table(safe('iv_put_otm_gainers'), 'Top 10 OTM IV Put Gainers'),
            '{{iv_put_losers_table}}': make_mini_table(safe('iv_put_losers'), 'Top 10 IV Put Losers (ALL)'),
            '{{iv_put_itm_losers_table}}': make_mini_table(safe('iv_put_itm_losers'), 'Top 10 ITM IV Put Losers'),
            '{{iv_put_otm_losers_table}}': make_mini_table(safe('iv_put_otm_losers'), 'Top 10 OTM IV Put Losers'),
            
            # Moneyness - Calls (6)
            '{{moneyness_call_gainers_table}}': make_mini_table(safe('moneyness_call_gainers'), 'Top 10 Moneyness Call Gainers (ALL)'),
            '{{moneyness_call_itm_gainers_table}}': make_mini_table(safe('moneyness_call_itm_gainers'), 'Top 10 ITM Moneyness Call Gainers'),
            '{{moneyness_call_otm_gainers_table}}': make_mini_table(safe('moneyness_call_otm_gainers'), 'Top 10 OTM Moneyness Call Gainers'),
            '{{moneyness_call_losers_table}}': make_mini_table(safe('moneyness_call_losers'), 'Top 10 Moneyness Call Losers (ALL)'),
            '{{moneyness_call_itm_losers_table}}': make_mini_table(safe('moneyness_call_itm_losers'), 'Top 10 ITM Moneyness Call Losers'),
            '{{moneyness_call_otm_losers_table}}': make_mini_table(safe('moneyness_call_otm_losers'), 'Top 10 OTM Moneyness Call Losers'),
            
            # Moneyness - Puts (6)
            '{{moneyness_put_gainers_table}}': make_mini_table(safe('moneyness_put_gainers'), 'Top 10 Moneyness Put Gainers (ALL)'),
            '{{moneyness_put_itm_gainers_table}}': make_mini_table(safe('moneyness_put_itm_gainers'), 'Top 10 ITM Moneyness Put Gainers'),
            '{{moneyness_put_otm_gainers_table}}': make_mini_table(safe('moneyness_put_otm_gainers'), 'Top 10 OTM Moneyness Put Gainers'),
            '{{moneyness_put_losers_table}}': make_mini_table(safe('moneyness_put_losers'), 'Top 10 Moneyness Put Losers (ALL)'),
            '{{moneyness_put_itm_losers_table}}': make_mini_table(safe('moneyness_put_itm_losers'), 'Top 10 ITM Moneyness Put Losers'),
            '{{moneyness_put_otm_losers_table}}': make_mini_table(safe('moneyness_put_otm_losers'), 'Top 10 OTM Moneyness Put Losers'),
            
            # Futures (4) - NOW PASSING is_future=True TO REMOVE STRIKE COLUMN ✅
            '{{future_oi_gainers_table}}': make_mini_table(safe('future_oi_gainers'), 'Top 10 Future OI Gainers', is_future=True),
            '{{future_oi_losers_table}}': make_mini_table(safe('future_oi_losers'), 'Top 10 Future OI Losers', is_future=True),
            '{{future_moneyness_gainers_table}}': make_mini_table(safe('future_moneyness_gainers'), 'Top 10 Future Moneyness Gainers', is_future=True),
            '{{future_moneyness_losers_table}}': make_mini_table(safe('future_moneyness_losers'), 'Top 10 Future Moneyness Losers', is_future=True),
        }

        # Insert all 40 tables
        for placeholder, value in replacements.items():
            tables_html = tables_html.replace(placeholder, value)

        # Insert final signals table AFTER all 40 tables and BEFORE disclaimer
        final_signals_html = make_final_signal_table(screener_data.get('final_signals', {}), max_rows=200)
        tables_html = tables_html.replace('{{final_signal_table}}', final_signals_html)

        print("[INFO] ✓ Tables HTML prepared (40 tables + final signals table)")

        # ============================================================
        # STEP 3: RENDER TO PDF WITH PLAYWRIGHT
        # ============================================================
        print("[INFO] 🖨️  Step 3: Rendering to PDF with Playwright...")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            # Create temporary HTML files
            with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding='utf-8') as cover_file:
                cover_file.write(cover_html)
                cover_temp_path = cover_file.name

            with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding='utf-8') as tables_file:
                tables_file.write(tables_html)
                tables_temp_path = tables_file.name

            # Convert Windows path to file:// URL
            cover_url = f'file:///{cover_temp_path.replace(chr(92), "/")}'
            tables_url = f'file:///{tables_temp_path.replace(chr(92), "/")}'

            # Render cover page
            print("[INFO]   → Rendering cover page...")
            page.goto(cover_url)
            page.wait_for_load_state('networkidle')
            cover_pdf_bytes = page.pdf(
                format='A4',
                print_background=True,
                margin={'top': '0px', 'right': '0px', 'bottom': '0px', 'left': '0px'}
            )
            print("[INFO]   ✓ Cover page rendered")

            # Render tables
            print("[INFO]   → Rendering tables...")
            page.goto(tables_url)
            page.wait_for_load_state('networkidle')
            tables_pdf_bytes = page.pdf(
                format='A4',
                print_background=True,
                margin={'top': '0mm', 'right': '0mm', 'bottom': '0mm', 'left': '0mm'}
            )
            print("[INFO]   ✓ Tables rendered")

            browser.close()

            # Cleanup temp files
            try:
                os.unlink(cover_temp_path)
                os.unlink(tables_temp_path)
            except:
                pass

        # ============================================================
        # STEP 4: MERGE PDFs
        # ============================================================
        print("[INFO] 🔗 Step 4: Merging PDFs...")
        merger = PdfMerger()
        
        # Add cover
        cover_buffer = BytesIO(cover_pdf_bytes)
        merger.append(cover_buffer)
        print("[INFO]   ✓ Cover added")
        
        # Add tables
        tables_buffer = BytesIO(tables_pdf_bytes)
        merger.append(tables_buffer)
        print("[INFO]   ✓ Tables added")
        
        # Write final PDF
        final_buffer = BytesIO()
        merger.write(final_buffer)
        merger.close()
        
        final_buffer.seek(0)
        print("[INFO] ✅ PDF generation complete!")
        return final_buffer
    
    except Exception as e:
        print(f"[ERROR] create_screener_pdf: {e}")
        import traceback
        traceback.print_exc()
        return None



# ========================================================================
# PDF EXPORT - NOW USES CACHED DATA
# ========================================================================

@gainers_losers_bp.route('/export-pdf')
def export_screener_pdf():
    """Export PDF - reuses cached data"""
    try:
        dates = get_available_dates()
        selected_date = request.args.get("date", dates[0] if dates else None)
        
        if not selected_date:
            return jsonify({"error": "Date parameter required"}), 400
        
        # Reuse cached data - no duplicate query!
        screener_data = get_screener_data_formatted(selected_date)
        
        if not screener_data:
            return jsonify({"error": "No data available for selected date"}), 404
        
        pdf_buffer = create_screener_pdf(screener_data, selected_date)
        
        if not pdf_buffer:
            return jsonify({"error": "PDF generation failed"}), 500
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"Goldmine_Screener_Report_{selected_date}_{timestamp}.pdf"
        
        return send_file(
            pdf_buffer,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        print(f"[ERROR] export_screener_pdf(): {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": "PDF export failed"}), 500
