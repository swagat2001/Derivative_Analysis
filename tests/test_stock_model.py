import pytest
from Analysis_Tools.app.models.stock_model import _clean_ticker, _base_table_name, _derived_table_name

def test_clean_ticker():
    # Standard cases
    assert _clean_ticker("RELIANCE") == "RELIANCE"
    assert _clean_ticker("TCS") == "TCS"

    # Special characters allowed
    assert _clean_ticker("M&M") == "M&M"
    assert _clean_ticker("M&M-FIN") == "M&M-FIN"

    # Sanitization cases
    assert _clean_ticker("NIFTY; DROP TABLE") == "NIFTYDROPTABLE"
    assert _clean_ticker("TATA'STEEL") == "TATASTEEL"
    assert _clean_ticker('INFY"') == "INFY"

    # Empty/None cases
    assert _clean_ticker("") == ""
    assert _clean_ticker(None) == ""

def test_table_names():
    # Verify table name construction uses cleaner
    assert _base_table_name("NIFTY; DROP") == "TBL_NIFTYDROP"
    assert _derived_table_name("NIFTY; DROP") == "TBL_NIFTYDROP_DERIVED"
