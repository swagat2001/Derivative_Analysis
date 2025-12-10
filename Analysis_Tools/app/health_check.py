"""
Health Check Endpoint for Monitoring
"""

import os
from datetime import datetime

from flask import Blueprint, jsonify

health_bp = Blueprint("health", __name__)


@health_bp.route("/health", methods=["GET"])
def health_check():
    """
    Health check endpoint for monitoring and CI/CD
    Returns application status and database connectivity
    """

    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "version": get_version(),
        "checks": {},
    }

    # Check database connectivity
    try:
        from sqlalchemy import text

        from .models.db_config import engine

        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
        health_status["checks"]["database"] = "connected"
    except Exception as e:
        health_status["status"] = "unhealthy"
        health_status["checks"]["database"] = f"error: {str(e)}"

    # Check cache tables
    try:
        from sqlalchemy import text

        from .models.db_config import engine

        with engine.connect() as conn:
            result = conn.execute(text("SELECT MAX(biz_date::date) FROM options_dashboard_cache")).fetchone()

            if result and result[0]:
                latest_date = str(result[0])
                health_status["checks"]["cache"] = f"latest: {latest_date}"
            else:
                health_status["checks"]["cache"] = "empty"
    except Exception as e:
        health_status["checks"]["cache"] = f"error: {str(e)}"

    # Check filesystem (logs directory writable)
    try:
        log_dir = os.path.join(os.path.dirname(__file__), "..", "..", "logs")
        os.makedirs(log_dir, exist_ok=True)
        test_file = os.path.join(log_dir, ".health_check_test")
        with open(test_file, "w") as f:
            f.write("test")
        os.remove(test_file)
        health_status["checks"]["filesystem"] = "writable"
    except Exception as e:
        health_status["checks"]["filesystem"] = f"error: {str(e)}"

    status_code = 200 if health_status["status"] == "healthy" else 503

    return jsonify(health_status), status_code


def get_version():
    """Get application version from VERSION file"""
    try:
        version_file = os.path.join(os.path.dirname(__file__), "..", "..", "VERSION")
        if os.path.exists(version_file):
            with open(version_file, "r") as f:
                return f.read().strip()
    except Exception:
        pass

    return "development"
