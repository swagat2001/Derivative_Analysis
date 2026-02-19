"""
Authentication model with database integration.
Handles user authentication, password hashing, and user management.
"""

import hashlib
import os
from typing import Optional, Tuple
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text

from .db_config import engine  # Use shared engine from db_config


# =============================================================
# PASSWORD HASHING
# =============================================================


def hash_password(password: str) -> str:
    """Hash password using SHA256 with salt."""
    salt = os.environ.get("APP_SECRET_KEY", "dev-secret-key-change-me")
    return hashlib.sha256((password + salt).encode()).hexdigest()


def verify_password(password: str, hashed: str) -> bool:
    """Verify password against hash."""
    return hash_password(password) == hashed


# =============================================================
# DATABASE INITIALIZATION
# =============================================================


def init_users_table():
    """Create users table if it doesn't exist, and migrate existing tables."""
    try:
        # Add missing columns to existing table if they don't exist
        migration_queries = [
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS verification_code_expires_at TIMESTAMP",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS email VARCHAR(100)",
        ]
        try:
            with engine.begin() as conn:
                for q in migration_queries:
                    conn.execute(text(q))
            print("[INFO] User table migration applied")
        except Exception as me:
            print(f"[INFO] Migration skipped (table may not exist yet): {me}")

        create_table_query = text(
            """
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(50) UNIQUE NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                role VARCHAR(20) DEFAULT 'user',
                full_name VARCHAR(100),
                email VARCHAR(100) UNIQUE,
                is_active BOOLEAN DEFAULT TRUE,
                verification_code VARCHAR(6),
                verification_code_expires_at TIMESTAMP,
                is_verified BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_login TIMESTAMP
            )
        """
        )

        with engine.begin() as conn:
            conn.execute(create_table_query)

        print("[INFO] Users table initialized successfully")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to initialize users table: {e}")
        return False


def create_default_admin():
    """Create default admin user if it doesn't exist."""
    try:
        default_username = os.environ.get("APP_ADMIN_USER", "admin")
        default_password = os.environ.get("APP_ADMIN_PASS", "admin123")

        # Check if admin exists
        check_query = text("SELECT id FROM users WHERE username = :username")
        with engine.connect() as conn:
            result = conn.execute(check_query, {"username": default_username})
            if result.fetchone():
                print(f"[INFO] Admin user '{default_username}' already exists")
                return True

        # Create admin user
        password_hash = hash_password(default_password)
        insert_query = text(
            """
            INSERT INTO users (username, password_hash, role, full_name, is_active)
            VALUES (:username, :password_hash, :role, :full_name, :is_active)
        """
        )

        with engine.begin() as conn:
            conn.execute(
                insert_query,
                {
                    "username": default_username,
                    "password_hash": password_hash,
                    "role": "admin",
                    "full_name": "Administrator",
                    "is_active": True,
                },
            )

        print(f"[INFO] Default admin user created: {default_username} / {default_password}")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to create default admin: {e}")
        return False


# =============================================================
# USER AUTHENTICATION
# =============================================================


def validate_user(username: str, password: str) -> bool:
    """Validate user credentials against database. Accepts username OR email."""
    if not username or not password:
        return False

    try:
        query = text(
            """
            SELECT password_hash, is_active, role, is_verified
            FROM users
            WHERE username = :username OR email = :username
        """
        )

        with engine.connect() as conn:
            result = conn.execute(query, {"username": username})
            row = result.fetchone()

            if not row:
                return False

            stored_hash, is_active, role, is_verified = row

            if not is_active:
                return False

            # Check verification for non-admin users
            if role != 'admin' and not is_verified:
                print(f"[INFO] User {username} not verified")
                return False

            if verify_password(password, stored_hash):
                # Update last login
                update_query = text(
                    """
                    UPDATE users
                    SET last_login = CURRENT_TIMESTAMP
                    WHERE username = :username
                """
                )
                with engine.begin() as update_conn:
                    update_conn.execute(update_query, {"username": username})
                return True

            return False
    except Exception as e:
        print(f"[ERROR] Authentication error: {e}")
        return False


def get_user(username: str) -> Optional[dict]:
    """Get user information by username."""
    try:
        query = text(
            """
            SELECT id, username, role, full_name, email, is_active, created_at, last_login
            FROM users
            WHERE username = :username
        """
        )

        with engine.connect() as conn:
            result = conn.execute(query, {"username": username})
            row = result.fetchone()

            if not row:
                return None

            return {
                "id": row[0],
                "username": row[1],
                "role": row[2],
                "full_name": row[3],
                "email": row[4],
                "is_active": row[5],
                "created_at": row[6],
                "last_login": row[7],
            }
    except Exception as e:
        print(f"[ERROR] Failed to get user: {e}")
        return None


def get_username_by_login(login: str) -> Optional[str]:
    """Given a username or email, return the actual username."""
    try:
        query = text("""
            SELECT username FROM users
            WHERE username = :login OR email = :login
            LIMIT 1
        """)
        with engine.connect() as conn:
            result = conn.execute(query, {"login": login})
            row = result.fetchone()
            return row[0] if row else None
    except Exception as e:
        print(f"[ERROR] get_username_by_login: {e}")
        return None


def get_user_display_name(username: str) -> Optional[str]:
    """Get user's display name (full_name or username)."""
    user = get_user(username)
    if user:
        return user.get("full_name") or user.get("username")
    return None


def create_user(
    username: str,
    password: str,
    role: str = "user",
    full_name: str = None,
    email: str = None,
    verification_code: str = None,
) -> Tuple[bool, str]:
    """Create a new user. Returns (success, message)."""
    from datetime import datetime, timedelta
    try:
        # Check if username exists
        if get_user(username):
            return False, "Username already exists"

        # Check if email already registered
        if email:
            with engine.connect() as conn:
                result = conn.execute(
                    text("SELECT id FROM users WHERE email = :email"),
                    {"email": email}
                )
                if result.fetchone():
                    return False, "Email address is already registered"

        # Hash password
        password_hash = hash_password(password)

        # OTP expires in 10 minutes
        otp_expiry = datetime.utcnow() + timedelta(minutes=10) if verification_code else None

        # Insert user
        insert_query = text(
            """
            INSERT INTO users (
                username, password_hash, role, full_name, email,
                is_active, verification_code, verification_code_expires_at, is_verified
            )
            VALUES (
                :username, :password_hash, :role, :full_name, :email,
                :is_active, :verification_code, :otp_expiry, :is_verified
            )
        """
        )

        # Auto-verify admin only
        is_verified = role == "admin"

        with engine.begin() as conn:
            conn.execute(
                insert_query,
                {
                    "username": username,
                    "password_hash": password_hash,
                    "role": role,
                    "full_name": full_name,
                    "email": email,
                    "is_active": True,
                    "verification_code": verification_code,
                    "otp_expiry": otp_expiry,
                    "is_verified": is_verified,
                },
            )

        return True, "User created successfully"
    except Exception as e:
        print(f"[ERROR] Failed to create user: {e}")
        return False, f"Failed to create user: {str(e)}"


def update_user_password(username: str, new_password: str) -> bool:
    """Update user password."""
    try:
        password_hash = hash_password(new_password)
        update_query = text(
            """
            UPDATE users
            SET password_hash = :password_hash
            WHERE username = :username
        """
        )

        with engine.begin() as conn:
            conn.execute(update_query, {"username": username, "password_hash": password_hash})

        return True
    except Exception as e:
        print(f"[ERROR] Failed to update password: {e}")
        return False


def verify_user_email(username: str, code: str) -> Tuple[bool, str]:
    """Verify user email with code, checking expiry."""
    from datetime import datetime
    try:
        with engine.begin() as conn:
            query = text(
                """
                SELECT verification_code, verification_code_expires_at, is_verified
                FROM users
                WHERE username = :username
            """
            )
            result = conn.execute(query, {"username": username})
            row = result.fetchone()

            if not row:
                return False, "User not found"

            stored_code, expires_at, is_verified = row

            if is_verified:
                return True, "User already verified"

            # Check expiry
            if expires_at and datetime.utcnow() > expires_at:
                return False, "Verification code has expired. Please request a new one."

            if str(stored_code) != str(code):
                return False, "Invalid verification code. Please check and try again."

            # Mark verified
            update_query = text(
                """
                UPDATE users
                SET is_verified = TRUE,
                    verification_code = NULL,
                    verification_code_expires_at = NULL
                WHERE username = :username
            """
            )
            conn.execute(update_query, {"username": username})

            return True, "Email verified successfully"

    except Exception as e:
        print(f"[ERROR] Verification failed: {e}")
        return False, f"Verification failed: {str(e)}"


def resend_otp(username: str) -> Tuple[bool, str]:
    """Generate a new OTP and update expiry for a user."""
    from datetime import datetime, timedelta
    import random
    try:
        with engine.begin() as conn:
            # Check user exists and is not yet verified
            result = conn.execute(
                text("SELECT email, is_verified FROM users WHERE username = :username"),
                {"username": username}
            )
            row = result.fetchone()
            if not row:
                return False, "User not found"
            email, is_verified = row
            if is_verified:
                return False, "Account is already verified"
            if not email:
                return False, "No email on record"

            # Generate new OTP
            new_code = str(random.randint(100000, 999999))
            new_expiry = datetime.utcnow() + timedelta(minutes=10)

            conn.execute(
                text("""
                    UPDATE users
                    SET verification_code = :code,
                        verification_code_expires_at = :expiry
                    WHERE username = :username
                """),
                {"code": new_code, "expiry": new_expiry, "username": username}
            )

        return True, new_code  # Caller sends the email
    except Exception as e:
        print(f"[ERROR] Resend OTP failed: {e}")
        return False, str(e)


# =============================================================
# PASSWORD RESET
# =============================================================

def get_all_users() -> list:
    """Get all users for admin dashboard."""
    try:
        query = text("""
            SELECT id, username, full_name, email, role, is_active,
                   is_verified, created_at, last_login
            FROM users
            ORDER BY created_at DESC
        """)
        with engine.connect() as conn:
            rows = conn.execute(query).fetchall()
            return [{
                "id": r[0], "username": r[1], "full_name": r[2],
                "email": r[3], "role": r[4], "is_active": r[5],
                "is_verified": r[6], "created_at": r[7], "last_login": r[8]
            } for r in rows]
    except Exception as e:
        print(f"[ERROR] get_all_users: {e}")
        return []


def toggle_user_active(username: str) -> Tuple[bool, str]:
    """Toggle user active status."""
    try:
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE users SET is_active = NOT is_active WHERE username = :u AND role != 'admin'"),
                {"u": username}
            )
        return True, "Status updated"
    except Exception as e:
        return False, str(e)


def create_password_reset_otp(email: str) -> Tuple[bool, str]:
    """Generate a reset OTP for the given email. Returns (success, otp_or_error)."""
    from datetime import datetime, timedelta
    import random
    try:
        with engine.begin() as conn:
            result = conn.execute(
                text("SELECT username, is_active FROM users WHERE email = :email"),
                {"email": email}
            )
            row = result.fetchone()
            if not row:
                return False, "No account found with that email address."
            username, is_active = row
            if not is_active:
                return False, "This account is inactive."

            otp = str(random.randint(100000, 999999))
            expiry = datetime.utcnow() + timedelta(minutes=10)

            conn.execute(
                text("""
                    UPDATE users
                    SET verification_code = :otp,
                        verification_code_expires_at = :expiry
                    WHERE email = :email
                """),
                {"otp": otp, "expiry": expiry, "email": email}
            )
        return True, otp
    except Exception as e:
        print(f"[ERROR] create_password_reset_otp: {e}")
        return False, str(e)


def verify_reset_otp(email: str, code: str) -> Tuple[bool, str]:
    """Verify reset OTP for email. Returns (success, username_or_error)."""
    from datetime import datetime
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT username, verification_code, verification_code_expires_at
                    FROM users WHERE email = :email
                """),
                {"email": email}
            )
            row = result.fetchone()
            if not row:
                return False, "No account found with that email."
            username, stored_code, expires_at = row

            if expires_at and datetime.utcnow() > expires_at:
                return False, "OTP has expired. Please request a new one."
            if str(stored_code) != str(code):
                return False, "Invalid OTP. Please check and try again."

        return True, username
    except Exception as e:
        print(f"[ERROR] verify_reset_otp: {e}")
        return False, str(e)


def reset_password(email: str, new_password: str) -> Tuple[bool, str]:
    """Reset password for the given email and clear OTP."""
    try:
        password_hash = hash_password(new_password)
        with engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE users
                    SET password_hash = :hash,
                        verification_code = NULL,
                        verification_code_expires_at = NULL
                    WHERE email = :email
                """),
                {"hash": password_hash, "email": email}
            )
        return True, "Password reset successfully."
    except Exception as e:
        print(f"[ERROR] reset_password: {e}")
        return False, str(e)


# =============================================================
# INITIALIZE ON MODULE IMPORT
# =============================================================

# Initialize users table and create default admin on first import
_init_done = False


def ensure_initialized():
    """Ensure users table and default admin are created."""
    global _init_done
    if not _init_done:
        init_users_table()
        create_default_admin()
        _init_done = True


# Auto-initialize on import
ensure_initialized()
