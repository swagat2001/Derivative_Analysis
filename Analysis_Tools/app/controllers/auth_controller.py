import random

from flask import Blueprint, flash, redirect, render_template, request, session, url_for

from ..models.auth_model import (create_user, get_user_display_name, resend_otp, validate_user,
                                   verify_user_email, create_password_reset_otp, verify_reset_otp, reset_password)
from ..services.email_service import send_otp_email

auth_bp = Blueprint("auth", __name__)


# ============================================================
# LOGIN
# ============================================================
@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if "user" in session:
        return redirect(url_for("home.home"))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()

        if validate_user(username, password):
            # Always store the actual username in session (not email)
            from ..models.auth_model import get_username_by_login
            actual_username = get_username_by_login(username) or username
            session["user"] = actual_username
            return redirect(url_for("home.home"))

        flash("Invalid username or password", "error")

    return render_template("login/login.html")


# ============================================================
# SIGNUP
# ============================================================
@auth_bp.route("/signup", methods=["GET", "POST"])
def signup():
    if "user" in session:
        return redirect(url_for("home.home"))

    if request.method == "POST":
        username         = request.form.get("username", "").strip()
        full_name        = request.form.get("full_name", "").strip()
        email            = request.form.get("email", "").strip()
        password         = request.form.get("password", "").strip()
        confirm_password = request.form.get("confirm_password", "").strip()

        # ---- Validation ----
        if not username:
            flash("Username is required", "error")
            return render_template("login/signup.html")

        if len(username) < 3:
            flash("Username must be at least 3 characters", "error")
            return render_template("login/signup.html")

        if not email:
            flash("Email address is required for account verification", "error")
            return render_template("login/signup.html")

        if "@" not in email or "." not in email.split("@")[-1]:
            flash("Please enter a valid email address", "error")
            return render_template("login/signup.html")

        if not password:
            flash("Password is required", "error")
            return render_template("login/signup.html")

        if len(password) < 6:
            flash("Password must be at least 6 characters", "error")
            return render_template("login/signup.html")

        if password != confirm_password:
            flash("Passwords do not match", "error")
            return render_template("login/signup.html")

        # ---- Generate OTP ----
        otp = str(random.randint(100000, 999999))

        # ---- Create user in DB ----
        success, message = create_user(
            username=username,
            password=password,
            role="user",
            full_name=full_name or None,
            email=email,
            verification_code=otp,
        )

        if not success:
            flash(message, "error")
            return render_template("login/signup.html")

        # ---- Send OTP email ----
        email_ok, email_msg = send_otp_email(to_email=email, username=username, otp=otp)

        if not email_ok:
            # Account created but email failed — show error so they can retry
            flash(f"Account created but email failed: {email_msg}. Use Resend OTP on the next page.", "error")

        return redirect(url_for("auth.verify", username=username))

    return render_template("login/signup.html")


# ============================================================
# VERIFY
# ============================================================
@auth_bp.route("/verify/<username>", methods=["GET", "POST"])
def verify(username):
    if request.method == "POST":
        code = request.form.get("code", "").strip()
        success, message = verify_user_email(username, code)

        if success:
            flash("Email verified! You can now log in.", "success")
            return redirect(url_for("auth.login"))
        else:
            flash(message, "error")

    return render_template("login/verify.html", username=username)


# ============================================================
# RESEND OTP
# ============================================================
@auth_bp.route("/resend-otp/<username>", methods=["POST"])
def resend_otp_route(username):
    success, result = resend_otp(username)

    if not success:
        flash(result, "error")
        return redirect(url_for("auth.verify", username=username))

    # result is the new OTP code
    new_otp = result

    # Fetch email from DB to send to
    from ..models.auth_model import get_user
    user = get_user(username)
    email = user.get("email") if user else None

    if not email:
        flash("No email found for this account.", "error")
        return redirect(url_for("auth.verify", username=username))

    email_ok, email_msg = send_otp_email(to_email=email, username=username, otp=new_otp)

    if email_ok:
        flash("A new verification code has been sent to your email.", "success")
    else:
        flash(f"Failed to send email: {email_msg}", "error")

    return redirect(url_for("auth.verify", username=username))


# ============================================================
# FORGOT PASSWORD - Step 1: Enter email
# ============================================================
@auth_bp.route("/forgot-password", methods=["GET", "POST"])
def forgot_password():
    if request.method == "POST":
        email = request.form.get("email", "").strip()

        if not email or "@" not in email:
            flash("Please enter a valid email address.", "error")
            return render_template("login/forgot_password.html")

        success, result = create_password_reset_otp(email)

        if not success:
            # Don't reveal if email exists — show generic message to prevent enumeration
            flash("If that email is registered, a reset code has been sent.", "success")
            return render_template("login/forgot_password.html")

        otp = result
        # Get username for email greeting
        from ..models.auth_model import get_username_by_login
        username = get_username_by_login(email) or "User"

        email_ok, email_msg = send_otp_email(to_email=email, username=username, otp=otp)
        if not email_ok:
            flash(f"Failed to send reset email: {email_msg}", "error")
            return render_template("login/forgot_password.html")

        flash("A 6-digit reset code has been sent to your email.", "success")
        # Store email in session temporarily for next steps
        session["reset_email"] = email
        return redirect(url_for("auth.reset_verify_otp"))

    return render_template("login/forgot_password.html")


# ============================================================
# FORGOT PASSWORD - Step 2: Verify OTP
# ============================================================
@auth_bp.route("/reset-verify", methods=["GET", "POST"])
def reset_verify_otp():
    email = session.get("reset_email")
    if not email:
        flash("Session expired. Please start again.", "error")
        return redirect(url_for("auth.forgot_password"))

    if request.method == "POST":
        code = request.form.get("code", "").strip()
        success, result = verify_reset_otp(email, code)

        if not success:
            flash(result, "error")
            return render_template("login/reset_verify_otp.html", email=email)

        # OTP valid — allow password reset
        session["reset_verified"] = True
        return redirect(url_for("auth.reset_new_password"))

    return render_template("login/reset_verify_otp.html", email=email)


# ============================================================
# FORGOT PASSWORD - Step 3: Set new password
# ============================================================
@auth_bp.route("/reset-password", methods=["GET", "POST"])
def reset_new_password():
    email = session.get("reset_email")
    verified = session.get("reset_verified")

    if not email or not verified:
        flash("Session expired. Please start again.", "error")
        return redirect(url_for("auth.forgot_password"))

    if request.method == "POST":
        password = request.form.get("password", "").strip()
        confirm  = request.form.get("confirm_password", "").strip()

        if len(password) < 6:
            flash("Password must be at least 6 characters.", "error")
            return render_template("login/reset_new_password.html")

        if password != confirm:
            flash("Passwords do not match.", "error")
            return render_template("login/reset_new_password.html")

        success, message = reset_password(email, password)
        if not success:
            flash(message, "error")
            return render_template("login/reset_new_password.html")

        # Clear reset session keys
        session.pop("reset_email", None)
        session.pop("reset_verified", None)

        flash("Password reset successfully! You can now log in.", "success")
        return redirect(url_for("auth.login"))

    return render_template("login/reset_new_password.html")


# ============================================================
# LOGOUT
# ============================================================
@auth_bp.route("/logout")
def logout():
    session.pop("user", None)
    return redirect(url_for("auth.login"))
