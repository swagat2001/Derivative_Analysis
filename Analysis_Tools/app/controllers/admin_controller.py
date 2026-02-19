import sys
from datetime import datetime

from flask import Blueprint, abort, jsonify, render_template, request, session

from ..models.auth_model import get_all_users, toggle_user_active

admin_bp = Blueprint("admin", __name__, url_prefix="/admin")


def require_admin():
    """Return 403 if current user is not admin."""
    from ..models.auth_model import get_user
    username = session.get("user")
    if not username:
        abort(403)
    user = get_user(username)
    if not user or user.get("role") != "admin":
        abort(403)


def _get_active_sessions():
    """Get active sessions from the app module."""
    for mod_name in list(sys.modules.keys()):
        if mod_name.endswith('.app') or mod_name == 'app':
            mod = sys.modules[mod_name]
            fn = getattr(mod, 'get_active_sessions', None)
            if fn:
                return fn()
    return {}


# ============================================================
# USERS DASHBOARD
# ============================================================
@admin_bp.route("/users")
def users_dashboard():
    require_admin()
    users = get_all_users()
    return render_template("admin/users_dashboard.html", users=users)


# ============================================================
# API - Real-time data (polled every 10s)
# ============================================================
@admin_bp.route("/api/users")
def api_users():
    require_admin()
    users = get_all_users()
    active = _get_active_sessions()

    now = datetime.utcnow()
    result = []
    for u in users:
        uname = u["username"]
        last_seen = active.get(uname)
        u["online"] = last_seen is not None
        u["last_seen"] = last_seen.strftime("%H:%M:%S") if last_seen else None
        u["last_seen_ago"] = int((now - last_seen).total_seconds()) if last_seen else None
        u["created_at"] = u["created_at"].strftime("%d %b %Y") if u["created_at"] else "-"
        u["last_login"] = u["last_login"].strftime("%d %b %Y %H:%M") if u["last_login"] else "Never"
        result.append(u)

    online_count = sum(1 for u in result if u["online"])
    return jsonify({
        "users": result,
        "online_count": online_count,
        "total_count": len(result),
        "timestamp": now.strftime("%H:%M:%S")
    })


# ============================================================
# API - Toggle user active/inactive
# ============================================================
@admin_bp.route("/api/users/<username>/toggle", methods=["POST"])
def toggle_user(username):
    require_admin()
    success, msg = toggle_user_active(username)
    return jsonify({"success": success, "message": msg})
