"""
Market Hours Utility for IST
"""
from datetime import datetime
import pytz

def get_ist_now():
    """Get current time in IST"""
    ist = pytz.timezone('Asia/Kolkata')
    return datetime.now(ist)

def is_market_hours_ist():
    """
    Check if current time is within Indian market hours (9:15 AM - 3:30 PM)
    Returns: (is_open, reason)
    """
    ist_now = get_ist_now()

    # Weekend check
    if ist_now.weekday() >= 5:
        return False, "Weekend"

    # Time check (9:15 to 15:30)
    market_open = ist_now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = ist_now.replace(hour=15, minute=30, second=0, microsecond=0)

    if ist_now < market_open:
        return False, "Pre-market"
    if ist_now > market_close:
        return False, "Post-market"

    return True, "Trading Session"
