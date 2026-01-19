from flask import Blueprint, flash, redirect, render_template, request, session, url_for

from ..models.auth_model import create_user, get_user_display_name, validate_user

auth_bp = Blueprint("auth", __name__)


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()

        if validate_user(username, password):
            session["user"] = username
            return redirect(url_for("home.home"))

        flash("Invalid username or password", "error")

    return render_template("login/login.html")


@auth_bp.route("/signup", methods=["GET", "POST"])
def signup():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()
        confirm_password = request.form.get("confirm_password", "").strip()
        full_name = request.form.get("full_name", "").strip()
        email = request.form.get("email", "").strip()

        # Validation
        if not username or not password:
            flash("Username and password are required", "error")
            return render_template("login/signup.html")

        if len(username) < 3:
            flash("Username must be at least 3 characters", "error")
            return render_template("login/signup.html")

        if len(password) < 6:
            flash("Password must be at least 6 characters", "error")
            return render_template("login/signup.html")

        if password != confirm_password:
            flash("Passwords do not match", "error")
            return render_template("login/signup.html")

        # Create user
        success, message = create_user(
            username=username,
            password=password,
            role="user",
            full_name=full_name if full_name else None,
            email=email if email else None,
        )

        if success:
            flash("Account created successfully! Please login.", "success")
            return redirect(url_for("auth.login"))
        else:
            flash(message, "error")
            return render_template("login/signup.html")

    return render_template("login/signup.html")


@auth_bp.route("/logout")
def logout():
    session.pop("user", None)
    return redirect(url_for("auth.login"))
