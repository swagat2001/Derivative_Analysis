from flask import Blueprint, render_template, request, send_file, jsonify
from ..models.stock_model import get_available_dates, get_dashboard_data
import pandas as pd
from io import BytesIO
from datetime import datetime

dashboard_bp = Blueprint('dashboard', __name__)

# Static indices â€” can later be made dynamic from DB/API
indices = {
    "NIFTY": 255000.35,
    "BANKNIFTY": 58200.35,
    "GOLD": 255000.35,
    "SILVER": 255000.35
}

# -----------------------------------------------------------
# ðŸ“Š Dashboard Route
# -----------------------------------------------------------
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
            indices=indices
        )

    except Exception as e:
        return jsonify({"error": f"Dashboard rendering failed: {str(e)}"}), 500


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
            for key, val in indices.items():
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