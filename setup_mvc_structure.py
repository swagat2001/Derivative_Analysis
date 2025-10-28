import os

# Base path — adjust if running from project root
BASE_DIR = os.path.join(os.getcwd(), "Analysis_Tools", "app")

# Folder structure to create
folders = [
    "models",
    "controllers",
    "views",
    "static"
]

# Files to create with default content
files = {
    "__init__.py": """from flask import Flask
from app.controllers.dashboard_controller import dashboard_bp
from app.controllers.stock_controller import stock_bp

def create_app():
    app = Flask(__name__)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(stock_bp)
    return app
""",

    os.path.join("models", "stock_model.py"): """from sqlalchemy import create_engine, text
import pandas as pd

# Database connection (adjust credentials if needed)
engine = create_engine("postgresql+psycopg2://postgres:Gallop@3104@localhost:5432/BhavCopy_Database")

def get_available_dates():
    query = "SELECT DISTINCT biz_date FROM options_dashboard_cache ORDER BY biz_date DESC"
    df = pd.read_sql(text(query), engine)
    return [d.strftime('%Y-%m-%d') for d in df['biz_date']]

def get_dashboard_data(date):
    query = f"SELECT * FROM options_dashboard_cache WHERE biz_date='{date}'"
    df = pd.read_sql(text(query), engine)
    return df.to_dict(orient='records')

def get_stock_detail_data(ticker, date):
    query = f'SELECT * FROM "TBL_{ticker}_DERIVED" WHERE "BizDt" = :date'
    return pd.read_sql(text(query), engine, params={"date": date}).to_dict(orient='records')
""",

    os.path.join("controllers", "dashboard_controller.py"): """from flask import Blueprint, render_template, request
from app.models.stock_model import get_available_dates, get_dashboard_data

dashboard_bp = Blueprint('dashboard', __name__)

@dashboard_bp.route('/')
def dashboard():
    dates = get_available_dates()
    selected_date = request.args.get('date', dates[0])
    data = get_dashboard_data(selected_date)
    return render_template('dashboard.html', dates=dates, data=data, selected_date=selected_date)
""",

    os.path.join("controllers", "stock_controller.py"): """from flask import Blueprint, render_template, request
from app.models.stock_model import get_stock_detail_data, get_available_dates

stock_bp = Blueprint('stock', __name__)

@stock_bp.route('/stock/<ticker>')
def stock_detail(ticker):
    dates = get_available_dates()
    selected_date = request.args.get('date', dates[0])
    data = get_stock_detail_data(ticker, selected_date)
    return render_template('stock_detail.html', ticker=ticker, data=data, dates=dates)
""",

    os.path.join("views", "base.html"): """<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>{% block title %}{% endblock %}</title>
    <link href="https://cdn.jsdelivr.net/npm/tailwindcss@3.4.4/dist/tailwind.min.css" rel="stylesheet">
</head>
<body class="bg-gray-100">
    <nav class="bg-indigo-600 text-white p-4 shadow">
        <div class="text-xl font-bold">Derivatives Analysis Dashboard</div>
    </nav>
    <main class="container mx-auto p-6">
        {% block content %}{% endblock %}
    </main>
</body>
</html>
""",

    os.path.join("views", "dashboard.html"): """{% extends "base.html" %}
{% block title %}Dashboard{% endblock %}
{% block content %}
<div class="bg-white p-4 rounded shadow">
  <form method="get" class="flex gap-4 mb-4">
    <select name="date" class="border p-2 rounded">
      {% for d in dates %}
      <option value="{{ d }}" {% if d == selected_date %}selected{% endif %}>{{ d }}</option>
      {% endfor %}
    </select>
    <button class="bg-indigo-600 text-white px-4 py-2 rounded">Load</button>
  </form>

  <table class="min-w-full text-sm text-center border border-gray-200">
    <thead class="bg-indigo-600 text-white">
      <tr>
        <th class="p-2">Stock</th>
        <th>Call Δ+</th>
        <th>Put Δ-</th>
        <th>Close</th>
        <th>RSI</th>
      </tr>
    </thead>
    <tbody>
      {% for row in data %}
      <tr class="hover:bg-gray-50">
        <td class="font-semibold text-indigo-700">{{ row.stock }}</td>
        <td>{{ row.call_delta_pos_strike }}</td>
        <td>{{ row.put_delta_neg_strike }}</td>
        <td>{{ row.closing_price }}</td>
        <td>{{ row.rsi }}</td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
</div>
{% endblock %}
""",

    os.path.join("views", "stock_detail.html"): """{% extends "base.html" %}
{% block title %}Stock Detail{% endblock %}
{% block content %}
<div class="bg-white p-4 rounded shadow">
  <h2 class="text-2xl font-semibold text-indigo-700 mb-4">{{ ticker }}</h2>

  <table class="min-w-full text-sm text-center border border-gray-200">
    <thead class="bg-indigo-600 text-white">
      <tr>
        <th>Date</th>
        <th>Price</th>
        <th>Volume</th>
        <th>OI</th>
      </tr>
    </thead>
    <tbody>
      {% for row in data %}
      <tr class="hover:bg-gray-50">
        <td>{{ row.BizDt }}</td>
        <td>{{ row.ClsPric }}</td>
        <td>{{ row.TtlTradgVol }}</td>
        <td>{{ row.OpnIntrst }}</td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
</div>
{% endblock %}
"""
}

# -------------------------------------------------------------------------
# Create folders and files
# -------------------------------------------------------------------------
print(f"\n📁 Creating Flask MVC structure in: {BASE_DIR}\n")

os.makedirs(BASE_DIR, exist_ok=True)

for folder in folders:
    path = os.path.join(BASE_DIR, folder)
    os.makedirs(path, exist_ok=True)
    print(f"✅ Folder created: {path}")

for relative_path, content in files.items():
    file_path = os.path.join(BASE_DIR, relative_path)
    if not os.path.exists(file_path):
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"📝 File created: {file_path}")
    else:
        print(f"⚠️ Skipped (already exists): {file_path}")

print("\n✅ Flask MVC structure setup complete!")
