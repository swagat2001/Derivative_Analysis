"""
Simple authentication model.

Replace the in-memory USERS mapping with a proper users table or
environment-based configuration when available.
"""

import os
from typing import Optional


# Fallback in-memory user store (use env vars if provided)
DEFAULT_USERNAME = os.environ.get("APP_ADMIN_USER", "admin")
DEFAULT_PASSWORD = os.environ.get("APP_ADMIN_PASS", "admin123")

USERS = {
    DEFAULT_USERNAME: DEFAULT_PASSWORD,
}


def validate_user(username: str, password: str) -> bool:
    """Return True if credentials are valid."""
    if not username or not password:
        return False
    expected = USERS.get(username)
    return expected is not None and password == expected


def get_user_display_name(username: str) -> Optional[str]:
    """Optional helper for showing a friendly name in the UI."""
    return username if username in USERS else None


