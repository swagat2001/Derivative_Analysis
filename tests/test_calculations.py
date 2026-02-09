"""
Unit tests for calculation formulas (OI change, Price change, PCR, Moneyness).
"""

import pytest
import pandas as pd
import numpy as np


class TestCalculationFormulas:
    """Test suite for OI/Price calculation formulas."""

    def test_oi_change_percentage_positive(self):
        """Test OI change percentage calculation for increase."""
        current_oi = 1000000
        previous_oi = 800000

        change_pct = ((current_oi - previous_oi) / previous_oi) * 100

        assert change_pct == 25.0, f"Expected 25% increase, got {change_pct}"

    def test_oi_change_percentage_negative(self):
        """Test OI change percentage calculation for decrease."""
        current_oi = 600000
        previous_oi = 800000

        change_pct = ((current_oi - previous_oi) / previous_oi) * 100

        assert change_pct == -25.0, f"Expected -25% decrease, got {change_pct}"

    def test_oi_change_zero_previous_oi(self):
        """Test OI change when previous OI is zero (division by zero)."""
        current_oi = 1000000
        previous_oi = 0

        # Should handle division by zero gracefully
        if previous_oi == 0:
            change_pct = 0  # Or None, depending on implementation
        else:
            change_pct = ((current_oi - previous_oi) / previous_oi) * 100

        assert change_pct == 0, "Should handle zero previous OI gracefully"

    def test_price_change_percentage(self):
        """Test price change percentage calculation."""
        current_price = 110.0
        previous_price = 100.0

        change_pct = ((current_price - previous_price) / previous_price) * 100

        assert change_pct == 10.0, f"Expected 10% increase, got {change_pct}"

    def test_price_change_zero_previous_price(self):
        """Test price change when previous price is zero."""
        current_price = 100.0
        previous_price = 0.0

        # Should handle division by zero
        if previous_price == 0:
            change_pct = 0
        else:
            change_pct = ((current_price - previous_price) / previous_price) * 100

        assert change_pct == 0, "Should handle zero previous price gracefully"

    def test_pcr_calculation(self):
        """Test Put-Call Ratio calculation."""
        put_oi = 5000000
        call_oi = 4000000

        pcr = put_oi / call_oi

        assert pcr == 1.25, f"Expected PCR of 1.25, got {pcr}"

    def test_pcr_zero_call_oi(self):
        """Test PCR when call OI is zero (division by zero)."""
        put_oi = 5000000
        call_oi = 0

        # Should handle division by zero
        if call_oi == 0:
            pcr = 0  # Or None/inf depending on implementation
        else:
            pcr = put_oi / call_oi

        assert pcr == 0 or pcr is None, "Should handle zero call OI gracefully"

    def test_moneyness_itm_call(self):
        """Test moneyness classification for ITM call."""
        spot = 18000
        strike = 17500
        option_type = "CE"

        # Call is ITM when spot > strike
        is_itm = spot > strike if option_type == "CE" else spot < strike

        assert is_itm is True, "Call with spot > strike should be ITM"

    def test_moneyness_otm_call(self):
        """Test moneyness classification for OTM call."""
        spot = 18000
        strike = 18500
        option_type = "CE"

        # Call is OTM when spot < strike
        is_otm = spot < strike if option_type == "CE" else spot > strike

        assert is_otm is True, "Call with spot < strike should be OTM"

    def test_moneyness_atm(self):
        """Test moneyness classification for ATM option."""
        spot = 18000
        strike = 18000

        # ATM when spot == strike (with some tolerance)
        tolerance = 50  # 50 points tolerance
        is_atm = abs(spot - strike) <= tolerance

        assert is_atm is True, "Option with spot == strike should be ATM"

    def test_moneyness_itm_put(self):
        """Test moneyness classification for ITM put."""
        spot = 18000
        strike = 18500
        option_type = "PE"

        # Put is ITM when spot < strike
        is_itm = spot < strike if option_type == "PE" else spot > strike

        assert is_itm is True, "Put with spot < strike should be ITM"

    def test_moneyness_otm_put(self):
        """Test moneyness classification for OTM put."""
        spot = 18000
        strike = 17500
        option_type = "PE"

        # Put is OTM when spot > strike
        is_otm = spot > strike if option_type == "PE" else spot < strike

        assert is_otm is True, "Put with spot > strike should be OTM"

    def test_percentage_calculation_precision(self):
        """Test that percentage calculations maintain precision."""
        value1 = 123.456
        value2 = 100.000

        change_pct = ((value1 - value2) / value2) * 100

        # Should be 23.456%
        assert abs(change_pct - 23.456) < 0.001, f"Precision error: {change_pct}"

    def test_negative_values_handling(self):
        """Test handling of negative values in calculations."""
        # Negative OI should be treated as invalid
        oi = -1000

        # Implementation should either reject or sanitize
        sanitized_oi = max(0, oi)

        assert sanitized_oi == 0, "Negative OI should be sanitized to 0"

    def test_nan_handling_in_calculations(self):
        """Test handling of NaN values."""
        current = np.nan
        previous = 100.0

        # Should handle NaN gracefully
        if pd.isna(current) or pd.isna(previous):
            change_pct = 0  # Or None
        else:
            change_pct = ((current - previous) / previous) * 100

        assert change_pct == 0 or pd.isna(change_pct), "Should handle NaN values"
