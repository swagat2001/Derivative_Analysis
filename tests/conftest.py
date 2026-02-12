import sys
import os
from datetime import datetime, timedelta

import pytest
import pandas as pd

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


@pytest.fixture
def sample_option_row():
    """Create a sample option data row for testing."""
    return {
        "FinInstrmTp": "OPTIDX",
        "LastPric": 100.0,
        "StrkPric": 18000.0,
        "UndrlygPric": 18000.0,
        "OptnTp": "CE",
        "FininstrmActlXpryDt": (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d"),
        "BizDt": datetime.now().strftime("%Y-%m-%d"),
        "OpnIntrst": 1000000,
        "TtlTradgVol": 50000,
    }


@pytest.fixture
def atm_call_option(sample_option_row):
    """ATM Call option (spot = strike)."""
    data = sample_option_row.copy()
    data["OptnTp"] = "CE"
    data["StrkPric"] = 18000.0
    data["UndrlygPric"] = 18000.0
    data["LastPric"] = 150.0
    return data


@pytest.fixture
def atm_put_option(sample_option_row):
    """ATM Put option (spot = strike)."""
    data = sample_option_row.copy()
    data["OptnTp"] = "PE"
    data["StrkPric"] = 18000.0
    data["UndrlygPric"] = 18000.0
    data["LastPric"] = 150.0
    return data


@pytest.fixture
def itm_call_option(sample_option_row):
    """ITM Call option (spot > strike)."""
    data = sample_option_row.copy()
    data["OptnTp"] = "CE"
    data["StrkPric"] = 17500.0
    data["UndrlygPric"] = 18000.0
    data["LastPric"] = 550.0
    return data


@pytest.fixture
def otm_call_option(sample_option_row):
    """OTM Call option (spot < strike)."""
    data = sample_option_row.copy()
    data["OptnTp"] = "CE"
    data["StrkPric"] = 18500.0
    data["UndrlygPric"] = 18000.0
    data["LastPric"] = 50.0
    return data


@pytest.fixture
def flask_test_client():
    """Create Flask test client."""
    import sys
    import os

    # Add project root to path
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    # Import app
    # Import app
    from Analysis_Tools.app import create_app
    flask_app = create_app()

    # Configure for testing
    flask_app.config['TESTING'] = True
    flask_app.config['WTF_CSRF_ENABLED'] = False  # Disable CSRF for testing

    with flask_app.test_client() as client:
        yield client
