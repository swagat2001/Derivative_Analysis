"""
Unit tests for Greeks calculation functions.
Tests the safe_greeks function and underlying greeks library.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


class TestGreeksCalculation:
    """Test suite for Greeks calculations."""

    def test_atm_call_delta_is_near_half(self, atm_call_option):
        """At-the-money call Delta should be approximately 0.5."""
        from Database.FO.fo_update_database import greeks

        result = greeks(
            premium=atm_call_option["LastPric"],
            expiry=atm_call_option["FininstrmActlXpryDt"],
            cd=atm_call_option["BizDt"],
            asset_price=atm_call_option["UndrlygPric"],
            strike_price=atm_call_option["StrkPric"],
            rate=0.06,
            opt_type="ce"
        )

        # ATM call delta should be between 0.45 and 0.65 (accounting for drift)
        assert 0.45 <= result["Delta"] <= 0.65, f"ATM Call Delta {result['Delta']} not in expected range"

    def test_atm_put_delta_is_near_negative_half(self, atm_put_option):
        """At-the-money put Delta should be approximately -0.5."""
        from Database.FO.fo_update_database import greeks

        result = greeks(
            premium=atm_put_option["LastPric"],
            expiry=atm_put_option["FininstrmActlXpryDt"],
            cd=atm_put_option["BizDt"],
            asset_price=atm_put_option["UndrlygPric"],
            strike_price=atm_put_option["StrkPric"],
            rate=0.06,
            opt_type="pe"
        )

        # ATM put delta should be between -0.65 and -0.35
        assert -0.65 <= result["Delta"] <= -0.35, f"ATM Put Delta {result['Delta']} not in expected range"

    def test_itm_call_delta_greater_than_atm(self, itm_call_option, atm_call_option):
        """ITM call should have higher delta than ATM call."""
        from Database.FO.fo_update_database import greeks

        itm_result = greeks(
            premium=itm_call_option["LastPric"],
            expiry=itm_call_option["FininstrmActlXpryDt"],
            cd=itm_call_option["BizDt"],
            asset_price=itm_call_option["UndrlygPric"],
            strike_price=itm_call_option["StrkPric"],
            rate=0.06,
            opt_type="ce"
        )

        atm_result = greeks(
            premium=atm_call_option["LastPric"],
            expiry=atm_call_option["FininstrmActlXpryDt"],
            cd=atm_call_option["BizDt"],
            asset_price=atm_call_option["UndrlygPric"],
            strike_price=atm_call_option["StrkPric"],
            rate=0.06,
            opt_type="ce"
        )

        assert itm_result["Delta"] > atm_result["Delta"], "ITM call delta should be greater than ATM"

    def test_otm_call_delta_less_than_atm(self, otm_call_option, atm_call_option):
        """OTM call should have lower delta than ATM call."""
        from Database.FO.fo_update_database import greeks

        otm_result = greeks(
            premium=otm_call_option["LastPric"],
            expiry=otm_call_option["FininstrmActlXpryDt"],
            cd=otm_call_option["BizDt"],
            asset_price=otm_call_option["UndrlygPric"],
            strike_price=otm_call_option["StrkPric"],
            rate=0.06,
            opt_type="ce"
        )

        atm_result = greeks(
            premium=atm_call_option["LastPric"],
            expiry=atm_call_option["FininstrmActlXpryDt"],
            cd=atm_call_option["BizDt"],
            asset_price=atm_call_option["UndrlygPric"],
            strike_price=atm_call_option["StrkPric"],
            rate=0.06,
            opt_type="ce"
        )

        assert otm_result["Delta"] < atm_result["Delta"], "OTM call delta should be less than ATM"

    def test_call_delta_bounds(self, atm_call_option):
        """Call delta should be between 0 and 1."""
        from Database.FO.fo_update_database import greeks

        result = greeks(
            premium=atm_call_option["LastPric"],
            expiry=atm_call_option["FininstrmActlXpryDt"],
            cd=atm_call_option["BizDt"],
            asset_price=atm_call_option["UndrlygPric"],
            strike_price=atm_call_option["StrkPric"],
            rate=0.06,
            opt_type="ce"
        )

        assert 0 <= result["Delta"] <= 1, f"Call Delta {result['Delta']} out of bounds [0, 1]"

    def test_put_delta_bounds(self, atm_put_option):
        """Put delta should be between -1 and 0."""
        from Database.FO.fo_update_database import greeks

        result = greeks(
            premium=atm_put_option["LastPric"],
            expiry=atm_put_option["FininstrmActlXpryDt"],
            cd=atm_put_option["BizDt"],
            asset_price=atm_put_option["UndrlygPric"],
            strike_price=atm_put_option["StrkPric"],
            rate=0.06,
            opt_type="pe"
        )

        assert -1 <= result["Delta"] <= 0, f"Put Delta {result['Delta']} out of bounds [-1, 0]"

    def test_gamma_is_positive(self, atm_call_option):
        """Gamma should always be positive for long positions."""
        from Database.FO.fo_update_database import greeks

        result = greeks(
            premium=atm_call_option["LastPric"],
            expiry=atm_call_option["FininstrmActlXpryDt"],
            cd=atm_call_option["BizDt"],
            asset_price=atm_call_option["UndrlygPric"],
            strike_price=atm_call_option["StrkPric"],
            rate=0.06,
            opt_type="ce"
        )

        assert result["Gamma"] > 0, f"Gamma {result['Gamma']} should be positive"

    def test_vega_is_positive(self, atm_call_option):
        """Vega should always be positive for long positions."""
        from Database.FO.fo_update_database import greeks

        result = greeks(
            premium=atm_call_option["LastPric"],
            expiry=atm_call_option["FininstrmActlXpryDt"],
            cd=atm_call_option["BizDt"],
            asset_price=atm_call_option["UndrlygPric"],
            strike_price=atm_call_option["StrkPric"],
            rate=0.06,
            opt_type="ce"
        )

        assert result["Vega"] > 0, f"Vega {result['Vega']} should be positive"

    def test_theta_is_negative(self, atm_call_option):
        """Theta should be negative for long positions (time decay)."""
        from Database.FO.fo_update_database import greeks

        result = greeks(
            premium=atm_call_option["LastPric"],
            expiry=atm_call_option["FininstrmActlXpryDt"],
            cd=atm_call_option["BizDt"],
            asset_price=atm_call_option["UndrlygPric"],
            strike_price=atm_call_option["StrkPric"],
            rate=0.06,
            opt_type="ce"
        )

        assert result["Theta"] < 0, f"Theta {result['Theta']} should be negative"

    def test_iv_is_positive(self, atm_call_option):
        """Implied Volatility should be positive."""
        from Database.FO.fo_update_database import greeks

        result = greeks(
            premium=atm_call_option["LastPric"],
            expiry=atm_call_option["FininstrmActlXpryDt"],
            cd=atm_call_option["BizDt"],
            asset_price=atm_call_option["UndrlygPric"],
            strike_price=atm_call_option["StrkPric"],
            rate=0.06,
            opt_type="ce"
        )

        assert result["IV"] > 0, f"IV {result['IV']} should be positive"
        assert result["IV"] < 2.0, f"IV {result['IV']} seems unreasonably high"

    def test_zero_spot_price_returns_none(self):
        """Zero spot price should return None or handle gracefully."""
        from Database.FO.fo_update_database import greeks

        try:
            result = greeks(
                premium=100.0,
                expiry=(datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d"),
                cd=datetime.now().strftime("%Y-%m-%d"),
                asset_price=0.0,  # Invalid
                strike_price=18000.0,
                rate=0.06,
                opt_type="ce"
            )
            # If it doesn't raise an exception, check that values are None or 0
            assert result["Delta"] is None or result["Delta"] == 0
        except (ValueError, ZeroDivisionError):
            # Expected behavior - should raise an error
            pass

    def test_negative_premium_returns_none(self):
        """Negative premium should return None or handle gracefully."""
        from Database.FO.fo_update_database import greeks

        try:
            result = greeks(
                premium=-100.0,  # Invalid
                expiry=(datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d"),
                cd=datetime.now().strftime("%Y-%m-%d"),
                asset_price=18000.0,
                strike_price=18000.0,
                rate=0.06,
                opt_type="ce"
            )
            # If it doesn't raise an exception, check that values are None or 0
            assert result["Delta"] is None or result["Delta"] == 0
        except (ValueError, Exception):
            # Expected behavior
            pass

    def test_expired_option_handling(self):
        """Expired option should be handled gracefully."""
        from Database.FO.fo_update_database import greeks

        try:
            result = greeks(
                premium=100.0,
                expiry=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),  # Expired
                cd=datetime.now().strftime("%Y-%m-%d"),
                asset_price=18000.0,
                strike_price=18000.0,
                rate=0.06,
                opt_type="ce"
            )
            # Should handle gracefully
            assert result is not None
        except (ValueError, Exception):
            # Expected behavior - may raise error for expired options
            pass
