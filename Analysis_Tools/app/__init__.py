from flask import Flask
# from flask import request, redirect, url_for, session
from .controllers.dashboard_controller import dashboard_bp
from .controllers.stock_controller import stock_bp
from datetime import datetime
import os

def create_app():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    template_dir = os.path.join(base_dir, "views")
    static_dir = os.path.join(base_dir, "static")

    app = Flask(__name__, template_folder=template_dir, static_folder=static_dir)
    app.secret_key = os.environ.get("APP_SECRET_KEY", "dev-secret-key-change-me")

    # Register blueprints
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(stock_bp)
    
    # Add custom Jinja2 filter for expiry date formatting
    def format_expiry_date(date_str):
        """Format YYYY-MM-DD to DDMMMYY (e.g., 25NOV25)"""
        try:
            date = datetime.strptime(str(date_str), '%Y-%m-%d')
            monthNames = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 
                          'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
            day = date.strftime('%d')
            month = monthNames[date.month - 1]
            year = date.strftime('%y')
            return f"{day}{month}{year}"
        except:
            return date_str
    
    app.jinja_env.filters['format_expiry'] = format_expiry_date
    
    # Auth not integrated yet: blueprint unregistered, no global guard

    return app
