from flask import Blueprint, render_template, request, send_file, jsonify, Response, stream_with_context
from ..models.dashboard_model import get_available_dates, get_dashboard_data
from ..models.stock_model import get_available_dates
import csv
import pandas as pd
from io import BytesIO
from datetime import datetime
import os
import time
import json

dashboard_bp = Blueprint('dashboard', __name__)

# Path to your live CSV data
SPOT_DATA_PATH = r"C:\Users\Admin\Desktop\Derivative_Analysis\spot_data\Data\SpotData.csv"

# --------------------------------------------------------------------
# 🧠 Helper: Read live spot prices
# --------------------------------------------------------------------
def get_live_indices():
    try:
        if not os.path.exists(SPOT_DATA_PATH):
            print(f"[WARN] SpotData.csv not found at {SPOT_DATA_PATH}")
            return {"NIFTY": 0.0, "BANKNIFTY": 0.0, "GOLD": 0.0, "SILVER": 0.0}

        df = pd.read_csv(SPOT_DATA_PATH)
        df.columns = [c.strip().upper() for c in df.columns]

        return {
            "NIFTY": float(df.iloc[0]["NIFTY"]),
            "BANKNIFTY": float(df.iloc[0]["BANK NIFTY"]),
            "GOLD": float(df.iloc[0]["GOLD"]),
            "SILVER": float(df.iloc[0]["SILVER"])
        }

    except Exception as e:
        print(f"[ERROR] get_live_indices(): {e}")
        return {"NIFTY": 0.0, "BANKNIFTY": 0.0, "GOLD": 0.0, "SILVER": 0.0}


# --------------------------------------------------------------------
# 📊 Dashboard route
# --------------------------------------------------------------------
@dashboard_bp.route('/')
def dashboard():
    try:
        dates = get_available_dates()
        selected_date = request.args.get("date", dates[0])
        mtype = request.args.get("mtype", "TOTAL")
        data = get_dashboard_data(selected_date, mtype)

        return render_template(
            "dashboard.html",
            data=data,
            dates=dates,
            selected_date=selected_date,
            mtype=mtype,
            indices=get_live_indices()
        )
    except Exception as e:
        return jsonify({"error": f"Dashboard rendering failed: {str(e)}"}), 500

    
    
# --------------------------------------------------------------------
# 🔁 API endpoint (optional)
# --------------------------------------------------------------------
@dashboard_bp.route('/api/live_indices')
def api_live_indices():
    return jsonify(get_live_indices())


# -----------------------------------------------------------
# ðŸ“¤ Export to Excel
# -----------------------------------------------------------
@dashboard_bp.route('/export', methods=['GET'])
def export_dashboard():
    try:
        date = request.args.get("date")
        mtype = request.args.get("mtype", "TOTAL")

        # Fetch data for export
        data = get_dashboard_data(date, mtype)
        df = pd.DataFrame(data)

        # Create Excel in memory
        output = BytesIO()

        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Sheet 1: Raw Data
            df.to_excel(writer, index=False, sheet_name='Dashboard_Data')

            # Add a formatted header sheet
            workbook = writer.book
            summary_sheet = workbook.create_sheet(title="Summary")

            summary_sheet["A1"] = "Derivatives Analysis Dashboard Export"
            summary_sheet["A1"].font = summary_sheet["A1"].font.copy(bold=True, size=14)

            summary_sheet["A3"] = "Date"
            summary_sheet["B3"] = date
            summary_sheet["A4"] = "Mode"
            summary_sheet["B4"] = mtype
            summary_sheet["A6"] = "Indices Snapshot"
            summary_sheet["A6"].font = summary_sheet["A6"].font.copy(bold=True)

            # Write index values
            row = 7
            for key, val in api_live_indices.items():
                summary_sheet[f"A{row}"] = key
                summary_sheet[f"B{row}"] = val
                row += 1

        output.seek(0)

        filename = f"dashboard_export_{date}_{mtype}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except ModuleNotFoundError as e:
        return jsonify({
            "error": "Missing dependency: openpyxl not installed. Please run `pip install openpyxl`."
        }), 500

    except Exception as e:
        return jsonify({"error": f"Export failed: {str(e)}"}), 500
    

# --------------------------------------------------------------------
# 🧩 Server-Sent Events (live streaming)
# --------------------------------------------------------------------
@dashboard_bp.route("/stream/live_indices")
def stream_live_indices():
    """Continuously stream CSV changes as Server-Sent Events (SSE)."""
    def event_stream():
        last_values = None
        while True:
            try:
                if os.path.exists(SPOT_DATA_PATH):
                    with open(SPOT_DATA_PATH, "r") as f:
                        reader = csv.DictReader(f)
                        row = next(reader)
                        current = {
                            "NIFTY": float(row["Nifty"]),
                            "BANKNIFTY": float(row["Bank Nifty"]),
                            "GOLD": float(row["Gold"]),
                            "SILVER": float(row["Silver"])
                        }

                    # Only push when data changes
                    if current != last_values:
                        yield f"data: {json.dumps(current)}\n\n"
                        last_values = current
            except Exception as e:
                yield f"event: error\ndata: {str(e)}\n\n"

            time.sleep(0.1)  # checks every 100ms (~10 updates/sec)

    return Response(stream_with_context(event_stream()), mimetype="text/event-stream")

@dashboard_bp.route('/api/historical-chart-data')
def get_historical_chart_data():
    ticker = request.args.get('ticker')
    option_type = request.args.get('option_type')
    metric = request.args.get('metric')
    strike = request.args.get('strike')
    curr_date = request.args.get('date')
    
    if not all([ticker, option_type, metric, curr_date]):
        return jsonify({'error': 'Missing parameters'}), 400
    
    try:
        from sqlalchemy import text
        from ..models.dashboard_model import engine
        import json
        
        table_name = f"TBL_{ticker}_DERIVED"
        
        # Get last 40 dates from ticker table (SAME AS OLD CODE)
        query_dates = text(f'''
            SELECT DISTINCT "BizDt" FROM "{table_name}"
            WHERE "BizDt" <= :curr_date 
            ORDER BY "BizDt" DESC LIMIT 40
        ''')
        dates_df = pd.read_sql(query_dates, engine, params={"curr_date": curr_date})
        
        if dates_df.empty:
            return jsonify({'error': 'No data', 'success': False}), 404
        
        dates = [d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d) for d in dates_df['BizDt']]
        dates.reverse()
        
        historical_data = []
        opt_type_param = 'CE' if option_type == 'call' else 'PE'
        
        for date in dates:
            try:
                # Get all data for this date (SAME AS OLD CODE line 807-812)
                base_query = text(f'''
                    SELECT "OptnTp", "StrkPric", "TtlTradgVol", "OpnIntrst", 
                           "TtlTrfVal", "UndrlygPric", "vega"
                    FROM "{table_name}"
                    WHERE "BizDt" = :date
                ''')
                df_date = pd.read_sql(base_query, engine, params={"date": date})
                
                if df_date.empty:
                    continue
                
                # Calculate PCR (SAME AS OLD CODE)
                put_volume = df_date[df_date['OptnTp'] == 'PE']['TtlTradgVol'].sum()
                call_volume = df_date[df_date['OptnTp'] == 'CE']['TtlTradgVol'].sum()
                put_oi = df_date[df_date['OptnTp'] == 'PE']['OpnIntrst'].sum()
                call_oi = df_date[df_date['OptnTp'] == 'CE']['OpnIntrst'].sum()
                
                pcr_volume = round(put_volume / call_volume, 4) if call_volume > 0 else 0
                pcr_oi = round(put_oi / call_oi, 4) if call_oi > 0 else 0
                
                underlying_price = float(df_date['UndrlygPric'].iloc[0])
                
                # Get RSI from cache
                cache_query = text("""
                    SELECT data_json FROM options_dashboard_cache 
                    WHERE biz_date = :date AND moneyness_type = 'TOTAL'
                """)
                cache_result = pd.read_sql(cache_query, engine, params={"date": date})
                
                rsi_value = None
                if not cache_result.empty:
                    cache_data = json.loads(cache_result.iloc[0]['data_json'])
                    ticker_data = next((item for item in cache_data if item['stock'] == ticker), None)
                    if ticker_data:
                        rsi_value = float(ticker_data.get('rsi')) if ticker_data.get('rsi') else None
                
                data_point = {
                    'date': date,
                    'pcr_volume': pcr_volume,
                    'pcr_oi': pcr_oi,
                    'underlying_price': round(underlying_price, 2),
                    'rsi': rsi_value
                }
                
                # Calculate metric value (EXACT SAME AS OLD CODE lines 821-846)
                if metric == 'money':
                    # OLD CODE lines 821-830
                    if option_type == 'call':
                        mask = (df_date['OptnTp'] == 'CE') & (df_date['StrkPric'] < underlying_price)
                    else:
                        mask = (df_date['OptnTp'] == 'PE') & (df_date['StrkPric'] > underlying_price)
                    
                    itm_contracts = df_date[mask].copy()
                    total_money = (itm_contracts['TtlTrfVal'] * abs(itm_contracts['StrkPric'] - underlying_price)).sum()
                    data_point['value'] = float(total_money)
                    data_point['metric_label'] = 'Money'
                    
                elif metric == 'vega':
                    # OLD CODE lines 832-846
                    if strike and strike != 'N/A':
                        strike_data = df_date[(df_date['OptnTp'] == opt_type_param) & 
                                             (df_date['StrkPric'] == float(strike))]
                        if not strike_data.empty:
                            value = float(strike_data['vega'].iloc[0])
                        else:
                            value = 0
                        data_point['value'] = value
                        data_point['metric_label'] = f'Vega @ {strike}'
                    else:
                        avg_vega = df_date[df_date['OptnTp'] == opt_type_param]['vega'].mean()
                        data_point['value'] = float(avg_vega) if not pd.isna(avg_vega) else 0
                        data_point['metric_label'] = 'Avg Vega'
                
                historical_data.append(data_point)
                
            except Exception as e:
                print(f"Error processing {date}: {e}")
                continue
        
        return jsonify({'success': True, 'ticker': ticker, 'data': historical_data})
        
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e), 'success': False}), 500