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
    """Create users table if it doesn't exist."""
    try:
        create_table_query = text(
            """
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(50) UNIQUE NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                role VARCHAR(20) DEFAULT 'user',
                full_name VARCHAR(100),
                email VARCHAR(100),
                is_active BOOLEAN DEFAULT TRUE,
                verification_code VARCHAR(6),
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
    """Validate user credentials against database."""
    if not username or not password:
        return False

    try:
        query = text(
            """
            SELECT password_hash, is_active, role, is_verified
            FROM users
            WHERE username = :username
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
    try:
        # Check if user exists
        if get_user(username):
            return False, "Username already exists"

        # Hash password
        password_hash = hash_password(password)

        # Insert user
        insert_query = text(
            """
            INSERT INTO users (
                username, password_hash, role, full_name, email,
                is_active, verification_code, is_verified
            )
            VALUES (
                :username, :password_hash, :role, :full_name, :email,
                :is_active, :verification_code, :is_verified
            )
        """
        )

        # Auto-verify admin or if no code provided (legacy support)
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
    """Verify user email with code."""
    try:
        # Get user and update in single transaction
        with engine.begin() as conn:
            query = text(
                """
                SELECT verification_code, is_verified
                FROM users
                WHERE username = :username
            """
            )
            result = conn.execute(query, {"username": username})
            row = result.fetchone()

            if not row:
                return False, "User not found"

            stored_code, is_verified = row

            if is_verified:
                return True, "User already verified"

            if str(stored_code) != str(code):
                return False, "Invalid verification code"

            # Update status
            update_query = text(
                """
                UPDATE users
                SET is_verified = TRUE, verification_code = NULL
                WHERE username = :username
            """
            )
            conn.execute(update_query, {"username": username})

            return True, "Email verified successfully"

    except Exception as e:
        print(f"[ERROR] Verification failed: {e}")
        return False, f"Verification failed: {str(e)}"


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
