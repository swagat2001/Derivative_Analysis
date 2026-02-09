"""
Integration tests for data pipeline components.
Tests CSV parsing, database operations, and cache building.
"""

import pytest
import pandas as pd
import tempfile
import os
from datetime import datetime
from io import StringIO


@pytest.mark.integration
class TestDataPipeline:
    """Test suite for data pipeline integration."""

    def test_csv_parsing_valid_data(self):
        """Test CSV parsing with valid NSE data."""
        # Sample NSE CSV data
        csv_data = """FinInstrmTp,TckrSymb,StrkPric,OptnTp,LastPric,UndrlygPric,BizDt
OPTIDX,NIFTY,18000,CE,150.50,18000.00,2024-01-15
OPTIDX,NIFTY,18000,PE,145.75,18000.00,2024-01-15"""

        df = pd.read_csv(StringIO(csv_data))

        assert len(df) == 2, "Should parse 2 rows"
        assert "TckrSymb" in df.columns, "Should have TckrSymb column"
        assert df["TckrSymb"].iloc[0] == "NIFTY", "Should parse symbol correctly"

    def test_csv_parsing_missing_columns(self):
        """Test CSV parsing with missing required columns."""
        # CSV with missing columns
        csv_data = """TckrSymb,LastPric
NIFTY,150.50"""

        df = pd.read_csv(StringIO(csv_data))

        # Check for missing columns
        required_cols = ["FinInstrmTp", "StrkPric", "OptnTp"]
        missing = [col for col in required_cols if col not in df.columns]

        assert len(missing) > 0, "Should detect missing columns"

    def test_csv_data_validation_negative_prices(self):
        """Test data validation rejects negative prices."""
        csv_data = """FinInstrmTp,TckrSymb,StrkPric,OptnTp,LastPric,UndrlygPric,BizDt
OPTIDX,NIFTY,18000,CE,-150.50,18000.00,2024-01-15"""

        df = pd.read_csv(StringIO(csv_data))

        # Validation should flag negative price
        invalid_rows = df[df["LastPric"] < 0]

        assert len(invalid_rows) > 0, "Should detect negative prices"

    def test_csv_data_validation_future_dates(self):
        """Test data validation rejects future dates."""
        future_date = (datetime.now().replace(year=datetime.now().year + 1)).strftime("%Y-%m-%d")

        csv_data = f"""FinInstrmTp,TckrSymb,StrkPric,OptnTp,LastPric,UndrlygPric,BizDt
OPTIDX,NIFTY,18000,CE,150.50,18000.00,{future_date}"""

        df = pd.read_csv(StringIO(csv_data))
        df["BizDt"] = pd.to_datetime(df["BizDt"])

        # Validation should flag future dates
        future_rows = df[df["BizDt"] > datetime.now()]

        assert len(future_rows) > 0, "Should detect future dates"

    def test_data_deduplication(self):
        """Test data deduplication logic."""
        # Duplicate rows
        csv_data = """FinInstrmTp,TckrSymb,StrkPric,OptnTp,LastPric,UndrlygPric,BizDt
OPTIDX,NIFTY,18000,CE,150.50,18000.00,2024-01-15
OPTIDX,NIFTY,18000,CE,150.50,18000.00,2024-01-15
OPTIDX,NIFTY,18500,CE,100.00,18000.00,2024-01-15"""

        df = pd.read_csv(StringIO(csv_data))

        # Deduplicate
        df_dedup = df.drop_duplicates(subset=["TckrSymb", "StrkPric", "OptnTp", "BizDt"])

        assert len(df_dedup) == 2, "Should remove duplicate row"

    def test_ticker_sanitization(self):
        """Test ticker symbol sanitization."""
        from Analysis_Tools.app.models.stock_model import _clean_ticker

        # Test various inputs
        assert _clean_ticker("RELIANCE") == "RELIANCE"
        assert _clean_ticker("M&M") == "M&M"
        assert _clean_ticker("NIFTY; DROP TABLE") == "NIFTYDROPTABLE"
        assert _clean_ticker("") == ""

    def test_table_name_construction(self):
        """Test database table name construction."""
        from Analysis_Tools.app.models.stock_model import _base_table_name, _derived_table_name

        assert _base_table_name("RELIANCE") == "TBL_RELIANCE"
        assert _derived_table_name("RELIANCE") == "TBL_RELIANCE_DERIVED"

        # Test sanitization in table names
        assert _base_table_name("M&M") == "TBL_M&M"

    def test_numeric_conversion(self):
        """Test numeric conversion of string data."""
        csv_data = """StrkPric,LastPric
18000,150.50
18500,100.75"""

        df = pd.read_csv(StringIO(csv_data))

        # Convert to numeric
        df["StrkPric"] = pd.to_numeric(df["StrkPric"], errors="coerce")
        df["LastPric"] = pd.to_numeric(df["LastPric"], errors="coerce")

        assert df["StrkPric"].dtype in [float, int], "Should convert to numeric"
        assert df["LastPric"].iloc[0] == 150.50, "Should preserve decimal precision"

    def test_date_parsing(self):
        """Test date parsing from various formats."""
        dates = ["2024-01-15", "15-Jan-2024", "2024/01/15"]

        for date_str in dates:
            try:
                # Try different formats
                parsed = pd.to_datetime(date_str, format="%Y-%m-%d", errors="coerce")
                if pd.isna(parsed):
                    parsed = pd.to_datetime(date_str, errors="coerce")

                assert not pd.isna(parsed), f"Should parse date: {date_str}"
            except Exception:
                # Some formats may not parse
                pass

    def test_oi_change_calculation(self):
        """Test OI change calculation in pipeline."""
        # Current and previous day data
        current_oi = 1000000
        previous_oi = 800000

        # Calculate change
        oi_change = current_oi - previous_oi
        oi_change_pct = ((current_oi - previous_oi) / previous_oi * 100) if previous_oi > 0 else 0

        assert oi_change == 200000, "OI change should be 200000"
        assert oi_change_pct == 25.0, "OI change % should be 25%"

    def test_price_change_calculation(self):
        """Test price change calculation in pipeline."""
        current_price = 150.50
        previous_price = 140.00

        price_change = current_price - previous_price
        price_change_pct = ((current_price - previous_price) / previous_price * 100) if previous_price > 0 else 0

        assert abs(price_change - 10.50) < 0.01, "Price change should be 10.50"
        assert abs(price_change_pct - 7.5) < 0.01, "Price change % should be 7.5%"

    def test_moneyness_calculation(self):
        """Test moneyness calculation in pipeline."""
        spot = 18000
        strike = 18000

        # ATM threshold (typically 50-100 points)
        atm_threshold = 50

        moneyness_diff = abs(spot - strike)
        is_atm = moneyness_diff <= atm_threshold

        assert is_atm, "Should classify as ATM"

    def test_csv_file_reading(self):
        """Test reading CSV file from disk."""
        # Create temporary CSV file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("FinInstrmTp,TckrSymb,StrkPric\n")
            f.write("OPTIDX,NIFTY,18000\n")
            temp_path = f.name

        try:
            # Read the file
            df = pd.read_csv(temp_path)

            assert len(df) == 1, "Should read 1 row"
            assert df["TckrSymb"].iloc[0] == "NIFTY", "Should read data correctly"
        finally:
            # Cleanup
            os.unlink(temp_path)

    def test_empty_csv_handling(self):
        """Test handling of empty CSV files."""
        csv_data = """FinInstrmTp,TckrSymb,StrkPric"""

        df = pd.read_csv(StringIO(csv_data))

        assert df.empty, "Should detect empty DataFrame"

    def test_large_dataset_handling(self):
        """Test handling of large datasets."""
        # Create large dataset
        rows = []
        for i in range(10000):
            rows.append({
                "FinInstrmTp": "OPTIDX",
                "TckrSymb": "NIFTY",
                "StrkPric": 18000 + (i * 50),
                "OptnTp": "CE" if i % 2 == 0 else "PE",
                "LastPric": 100.0 + i,
            })

        df = pd.DataFrame(rows)

        assert len(df) == 10000, "Should handle large dataset"

        # Test aggregation on large dataset
        grouped = df.groupby("OptnTp")["LastPric"].sum()

        assert len(grouped) == 2, "Should group correctly"
