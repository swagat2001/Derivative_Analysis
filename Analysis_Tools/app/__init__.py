from flask import Flask
from .controllers.dashboard_controller import dashboard_bp
from .controllers.stock_controller import stock_bp
import os

def create_app():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    template_dir = os.path.join(base_dir, "views")
    static_dir = os.path.join(base_dir, "static")

    app = Flask(__name__, template_folder=template_dir, static_folder=static_dir)

    app.register_blueprint(dashboard_bp)
    app.register_blueprint(stock_bp)
    return app
