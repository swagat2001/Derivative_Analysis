from flask import Blueprint, render_template, request
from ..models.stock_model import get_available_dates, get_stock_detail_data


stock_bp = Blueprint('stock', __name__)

@stock_bp.route('/stock/<ticker>')
def stock_detail(ticker):
    dates = get_available_dates()
    selected_date = request.args.get('date', dates[0] if dates else None)
    
    data = []
    if selected_date:
        data = get_stock_detail_data(ticker, selected_date)

    return render_template(
        'stock_detail.html',
        ticker=ticker,
        data=data,
        dates=dates,
        selected_date=selected_date
    )