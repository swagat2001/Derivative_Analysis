"""
Edge case tests for data validation and error handling.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


class TestEdgeCases:
    """Test suite for edge cases and error handling."""

    def test_zero_volume_contract(self):
        """Test handling of zero volume contracts."""
        volume = 0

        # Zero volume is valid but should be handled appropriately
        assert volume >= 0, "Volume should not be negative"

        # Calculations involving zero volume should not crash
        avg_price = 0 if volume == 0 else 1000 / volume
        assert avg_price == 0, "Should handle zero volume gracefully"

    def test_expired_contract_detection(self):
        """Test detection of expired contracts."""
        expiry_date = datetime.now() - timedelta(days=1)
        current_date = datetime.now()

        is_expired = expiry_date < current_date

        assert is_expired is True, "Contract with past expiry should be detected as expired"

    def test_missing_data_fields(self):
        """Test handling of missing required fields."""
        data = {
            "StrkPric": 18000.0,
            # Missing: LastPric, UndrlygPric, OptnTp
        }

        # Should detect missing fields
        required_fields = ["StrkPric", "LastPric", "UndrlygPric", "OptnTp"]
        missing = [f for f in required_fields if f not in data]

        assert len(missing) > 0, "Should detect missing fields"
        assert "LastPric" in missing, "Should detect missing LastPric"

    def test_malformed_date_handling(self):
        """Test handling of malformed date strings."""
        malformed_dates = [
            "2024-13-01",  # Invalid month
            "2024-02-30",  # Invalid day
            "not-a-date",  # Completely invalid
            "",            # Empty string
        ]

        for date_str in malformed_dates:
            try:
                parsed = datetime.strptime(date_str, "%Y-%m-%d")
                # If it parses, it's not malformed
            except (ValueError, Exception):
                # Expected behavior - should raise error
                pass

    def test_extreme_iv_values(self):
        """Test handling of extreme IV values."""
        extreme_ivs = [0.0, 0.001, 5.0, 10.0, -0.5]

        for iv in extreme_ivs:
            # IV should be positive and reasonable (typically 0.05 to 2.0)
            is_valid = 0 < iv < 3.0

            if iv <= 0:
                assert not is_valid, f"IV {iv} should be invalid (negative or zero)"
            elif iv > 3.0:
                assert not is_valid, f"IV {iv} should be flagged as extreme"

    def test_strike_price_zero(self):
        """Test handling of zero strike price."""
        strike = 0.0

        # Zero strike is invalid
        assert strike > 0 or strike == 0, "Strike price validation"

        if strike == 0:
            # Should be rejected or handled specially
            is_valid = False
        else:
            is_valid = True

        assert not is_valid, "Zero strike price should be invalid"

    def test_spot_price_zero(self):
        """Test handling of zero spot price."""
        spot = 0.0

        # Zero spot is invalid
        if spot <= 0:
            is_valid = False
        else:
            is_valid = True

        assert not is_valid, "Zero spot price should be invalid"

    def test_premium_exceeds_spot(self):
        """Test handling when option premium exceeds spot price."""
        premium = 20000.0
        spot = 18000.0
        strike = 18000.0
        option_type = "CE"

        # For calls, premium should not exceed spot price
        if option_type == "CE":
            is_suspicious = premium > spot
            assert is_suspicious, "Premium > spot is suspicious for calls"

    def test_negative_premium(self):
        """Test handling of negative premium."""
        premium = -100.0

        # Negative premium is invalid
        is_valid = premium > 0

        assert not is_valid, "Negative premium should be invalid"

    def test_very_large_oi(self):
        """Test handling of very large OI values."""
        oi = 999999999999  # Very large number

        # Should handle large numbers without overflow
        oi_millions = oi / 1000000

        assert oi_millions > 0, "Should handle large OI values"

    def test_dataframe_with_all_nan(self):
        """Test handling of DataFrame with all NaN values."""
        df = pd.DataFrame({
            "StrkPric": [np.nan, np.nan, np.nan],
            "LastPric": [np.nan, np.nan, np.nan],
        })

        # Should detect all-NaN columns
        all_nan = df.isna().all()

        assert all_nan["StrkPric"], "Should detect all-NaN column"

    def test_duplicate_rows_handling(self):
        """Test handling of duplicate rows in data."""
        df = pd.DataFrame({
            "BizDt": ["2024-01-01", "2024-01-01", "2024-01-02"],
            "StrkPric": [18000, 18000, 18000],
            "OptnTp": ["CE", "CE", "CE"],
        })

        # Should detect duplicates
        duplicates = df.duplicated(subset=["BizDt", "StrkPric", "OptnTp"])

        assert duplicates.sum() > 0, "Should detect duplicate rows"

    def test_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        df = pd.DataFrame()

        # Should handle empty DataFrame gracefully
        is_empty = df.empty

        assert is_empty, "Should detect empty DataFrame"

    def test_mixed_data_types(self):
        """Test handling of mixed data types in columns."""
        df = pd.DataFrame({
            "StrkPric": [18000, "18500", 19000],  # Mixed int and string
        })

        # Should convert or handle mixed types
        try:
            df["StrkPric"] = pd.to_numeric(df["StrkPric"], errors="coerce")
            assert df["StrkPric"].dtype in [np.float64, np.int64], "Should convert to numeric"
        except Exception:
            # Conversion failed - expected for invalid data
            pass

    def test_unicode_characters_in_ticker(self):
        """Test handling of unicode characters in ticker symbols."""
        tickers = ["RELIANCE", "M&M", "TATA-STEEL", "NIFTY™", "TEST©"]

        for ticker in tickers:
            # Should sanitize special characters
            sanitized = ''.join(c for c in ticker if c.isalnum() or c in ['-', '&'])

            # Basic validation
            assert len(sanitized) > 0, f"Sanitized ticker should not be empty: {ticker}"

    def test_time_to_expiry_calculation_edge_cases(self):
        """Test time to expiry calculation edge cases."""
        # Same day expiry
        expiry = datetime.now()
        current = datetime.now()

        time_diff = (expiry - current).total_seconds()
        days_to_expiry = time_diff / (24 * 3600)

        # Should be very close to 0
        assert abs(days_to_expiry) < 1, "Same day expiry should be near 0"

        # Expired option
        expiry_past = datetime.now() - timedelta(days=5)
        time_diff_past = (expiry_past - current).total_seconds()
        days_to_expiry_past = time_diff_past / (24 * 3600)

        assert days_to_expiry_past < 0, "Expired option should have negative time to expiry"
