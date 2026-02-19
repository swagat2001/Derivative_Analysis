"""
Email Service - Sends OTP verification emails via Gmail SMTP.
Uses only Python's built-in smtplib (no extra packages needed).

Setup:
  1. Add MAIL_USERNAME and MAIL_PASSWORD to .env
  2. For Gmail: use an App Password (not your main password)
     -> Google Account > Security > 2-Step Verification > App Passwords
"""

import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


# ============================================================
# CONFIG (reads from .env)
# ============================================================
MAIL_SERVER   = os.environ.get("MAIL_SERVER",   "smtp.gmail.com")
MAIL_PORT     = int(os.environ.get("MAIL_PORT", 587))
MAIL_USERNAME = os.environ.get("MAIL_USERNAME", "")
MAIL_PASSWORD = os.environ.get("MAIL_PASSWORD", "")
MAIL_FROM     = os.environ.get("MAIL_FROM",     MAIL_USERNAME)
APP_NAME      = os.environ.get("APP_NAME",      "Market Neev")


# ============================================================
# HTML EMAIL TEMPLATE
# ============================================================
def _build_otp_email(otp: str, username: str) -> tuple[str, str]:
    """Returns (subject, html_body)."""
    subject = f"Your {APP_NAME} Verification Code: {otp}"
    html = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <style>
    body {{ font-family: 'Segoe UI', Arial, sans-serif; background: #f4f6f8; margin: 0; padding: 0; }}
    .wrapper {{ max-width: 520px; margin: 40px auto; background: #ffffff;
                border-radius: 12px; overflow: hidden;
                box-shadow: 0 4px 20px rgba(0,0,0,0.08); }}
    .header {{ background: linear-gradient(135deg, #8B2432 0%, #c14858 100%);
               padding: 32px 40px; text-align: center; }}
    .header h1 {{ color: #ffffff; margin: 0; font-size: 22px; font-weight: 700; }}
    .header p  {{ color: rgba(255,255,255,0.8); margin: 6px 0 0; font-size: 13px; }}
    .body {{ padding: 36px 40px; }}
    .body p {{ color: #374151; font-size: 15px; line-height: 1.6; margin: 0 0 16px; }}
    .otp-box {{ background: #f8fafc; border: 2px dashed #8B2432;
                border-radius: 10px; padding: 24px; text-align: center; margin: 24px 0; }}
    .otp-code {{ font-size: 42px; font-weight: 800; color: #8B2432;
                 letter-spacing: 10px; font-family: 'Courier New', monospace; }}
    .otp-note {{ font-size: 12px; color: #6b7280; margin-top: 8px; }}
    .footer {{ background: #f8fafc; padding: 20px 40px; text-align: center;
               border-top: 1px solid #e5e7eb; }}
    .footer p {{ color: #9ca3af; font-size: 12px; margin: 0; }}
  </style>
</head>
<body>
  <div class="wrapper">
    <div class="header">
      <h1>📊 {APP_NAME}</h1>
      <p>Professional Derivatives Analysis Platform</p>
    </div>
    <div class="body">
      <p>Hi <strong>{username}</strong>,</p>
      <p>Thanks for signing up! Use the code below to verify your email address:</p>
      <div class="otp-box">
        <div class="otp-code">{otp}</div>
        <div class="otp-note">This code expires in <strong>10 minutes</strong></div>
      </div>
      <p>If you didn't create an account, you can safely ignore this email.</p>
      <p>— The {APP_NAME} Team</p>
    </div>
    <div class="footer">
      <p>© {APP_NAME} · This is an automated message, please do not reply.</p>
    </div>
  </div>
</body>
</html>
"""
    return subject, html


# ============================================================
# SEND FUNCTION
# ============================================================
def send_otp_email(to_email: str, username: str, otp: str) -> tuple[bool, str]:
    """
    Send OTP verification email.
    Returns (success: bool, message: str).
    """
    if not MAIL_USERNAME or not MAIL_PASSWORD:
        # Fallback: print to console (dev mode)
        print(f"[EMAIL - DEV MODE] OTP for {username} ({to_email}): {otp}")
        print("[EMAIL] Set MAIL_USERNAME and MAIL_PASSWORD in .env to enable real email sending.")
        return True, "dev_mode"

    subject, html_body = _build_otp_email(otp, username)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"{APP_NAME} <{MAIL_FROM}>"
    msg["To"]      = to_email

    # Plain text fallback
    plain = f"Hi {username},\n\nYour {APP_NAME} verification code is: {otp}\n\nThis code expires in 10 minutes."
    msg.attach(MIMEText(plain, "plain"))
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP(MAIL_SERVER, MAIL_PORT, timeout=10) as server:
            server.ehlo()
            server.starttls()
            server.login(MAIL_USERNAME, MAIL_PASSWORD)
            server.sendmail(MAIL_FROM, to_email, msg.as_string())

        print(f"[EMAIL] OTP sent successfully to {to_email}")
        return True, "sent"

    except smtplib.SMTPAuthenticationError:
        print("[EMAIL ERROR] Authentication failed. Check MAIL_USERNAME / MAIL_PASSWORD in .env")
        return False, "Authentication failed. Please contact support."

    except smtplib.SMTPException as e:
        print(f"[EMAIL ERROR] SMTP error: {e}")
        return False, f"Email sending failed: {str(e)}"

    except Exception as e:
        print(f"[EMAIL ERROR] Unexpected error: {e}")
        return False, "Failed to send email. Please try again."
