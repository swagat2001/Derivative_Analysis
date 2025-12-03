from flask import Flask, redirect, url_for, session, request
from .controllers.dashboard_controller import dashboard_bp
from .controllers.stock_controller import stock_bp
from .controllers.auth_controller import auth_bp
from .controllers.screener import screener_bp
from .controllers.screener.top_gainers_losers.controller import gainers_losers_bp, cache as gainers_cache
from .controllers.screener.signal_analysis.controller import signal_analysis_bp, cache as signal_cache
from .controllers.screener.futures_oi.controller import futures_oi_bp, cache as futures_cache
from .controllers.screener.technical_screener.controller import technical_screener_bp, cache as tech_cache
from datetime import datetime
import os

def create_app():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    template_dir = os.path.join(base_dir, "views")
    static_dir = os.path.join(base_dir, "static")
    app = Flask(__name__, template_folder=template_dir, static_folder=static_dir)
    app.secret_key = os.environ.get("APP_SECRET_KEY", "dev-secret-key-change-me")

    # Initialize caches
    gainers_cache.init_app(app)
    signal_cache.init_app(app)
    futures_cache.init_app(app)
    tech_cache.init_app(app)

    # Initialize authentication system (creates users table and default admin)
    from .models.auth_model import ensure_initialized
    ensure_initialized()

    # Register blueprints
    app.register_blueprint(auth_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(stock_bp)
    app.register_blueprint(screener_bp)           # Landing page at /screener
    app.register_blueprint(gainers_losers_bp)     # Tables at /screener/top-gainers-losers
    app.register_blueprint(signal_analysis_bp)    # Signals at /screener/signal-analysis
    app.register_blueprint(futures_oi_bp)         # Futures OI at /screener/futures-oi
    app.register_blueprint(technical_screener_bp) # Technical at /screener/technical

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

    # Custom filter for number formatting with commas
    def format_number(value, decimals=0):
        """Format number with commas and optional decimals."""
        try:
            num = float(value)
            if decimals == 0:
                return f"{int(num):,}"
            else:
                return f"{num:,.{decimals}f}"
        except (ValueError, TypeError):
            return str(value)

    app.jinja_env.filters['format_number'] = format_number

    # =============================================================
    # AUTHENTICATION MIDDLEWARE
    # =============================================================
    @app.before_request
    def require_login():
        # Allow access to login, signup, logout, and static files
        if request.endpoint in ['auth.login', 'auth.signup', 'auth.logout', 'static']:
            return None
        
        # Check if user is logged in
        if 'user' not in session:
            return redirect(url_for('auth.login'))
        
        return None

    # Make user info available to all templates
    @app.context_processor
    def inject_user():
        from .models.auth_model import get_user_display_name
        username = session.get('user')
        return {
            'current_user': username,
            'user_display_name': get_user_display_name(username) if username else None
        }

    return app
