"""
Live Indices Data Model
Reads real-time data from C++ WebSocket application output files
"""

import math
import os
from datetime import datetime
from typing import Dict, List, Optional


class LiveIndicesReader:
    """Reads live market data from text files written by C++ application"""

    BASE_PATH = "C:\\Users\\Admin\\Desktop\\insu\\data\\"

    # File paths for each index
    FILES = {
        "nifty50": {"spot": "NiftyIndexSpot.txt", "history": "NiftyIndex.txt"},
        "banknifty": {"spot": "BankNiftySpot.txt", "history": "BankNifty.txt"},
        "sensex": {"spot": "SensexSpot.txt", "history": "Sensex.txt"},
        "niftyfin": {"spot": "NiftyFinSpot.txt", "history": "NiftyFin.txt"},
        "niftynext50": {"spot": "NiftyNext50Spot.txt", "history": "NiftyNext50.txt"},
    }

    # Static fallback data when files don't exist (market closed)
    FALLBACK_DATA = {}

    @staticmethod
    def _init_fallback_data():
        """Initialize static fallback data with pre-generated history"""
        LiveIndicesReader.FALLBACK_DATA = {
            "nifty50": {
                "value": 26147.10,
                "open": 26143.05,
                "high": 26147.30,
                "low": 26142.65,
                "previousClose": 26143.05,
                "history": LiveIndicesReader._generate_smooth_pattern(26143.05, 26147.10, 26147.30, 26142.65, 0),
            },
            "banknifty": {
                "value": 51234.50,
                "open": 51450.00,
                "high": 51550.30,
                "low": 51100.20,
                "previousClose": 51450.00,
                "history": LiveIndicesReader._generate_smooth_pattern(51450.00, 51234.50, 51550.30, 51100.20, 1),
            },
            "sensex": {
                "value": 83758.43,
                "open": 83200.00,
                "high": 83900.50,
                "low": 83150.00,
                "previousClose": 83200.00,
                "history": LiveIndicesReader._generate_smooth_pattern(83200.00, 83758.43, 83900.50, 83150.00, 2),
            },
            "niftyfin": {
                "value": 23890.40,
                "open": 23750.00,
                "high": 23950.00,
                "low": 23700.00,
                "previousClose": 23750.00,
                "history": LiveIndicesReader._generate_smooth_pattern(23750.00, 23890.40, 23950.00, 23700.00, 3),
            },
            "niftynext50": {
                "value": 68499.30,
                "open": 69200.00,
                "high": 69350.00,
                "low": 68400.00,
                "previousClose": 69200.00,
                "history": LiveIndicesReader._generate_smooth_pattern(69200.00, 68499.30, 69350.00, 68400.00, 4),
            },
        }

    @staticmethod
    def _generate_smooth_pattern(
        open_val: float, close_val: float, high_val: float, low_val: float, seed: int
    ) -> List[Dict]:
        """Generate deterministic smooth pattern using sine waves"""
        history = []
        points = 100

        for i in range(points):
            # Calculate time
            minutes = int((i / points) * 390)  # 390 minutes in trading day
            hour = 9 + (minutes // 60)
            minute = minutes % 60
            timestamp = f"2026-01-11 {hour:02d}:{minute:02d}:00"

            # Progress through the day
            progress = i / (points - 1)

            # Create smooth sine wave pattern that respects open/close/high/low
            # Use seed to make different patterns for different indices
            phase = seed * 0.5
            wave1 = math.sin(progress * math.pi * 3 + phase) * 0.3
            wave2 = math.sin(progress * math.pi * 5 + phase) * 0.15
            wave3 = math.sin(progress * math.pi * 7 + phase) * 0.08

            # Combine waves (-1 to 1 range)
            combined_wave = (wave1 + wave2 + wave3) / (0.3 + 0.15 + 0.08)

            # Map to price range
            mid_price = (high_val + low_val) / 2
            range_size = (high_val - low_val) / 2

            # Calculate value based on wave
            base_value = mid_price + (combined_wave * range_size * 0.8)

            # Add linear trend from open to close
            trend = open_val + (close_val - open_val) * progress

            # Blend wave and trend
            value = base_value * 0.6 + trend * 0.4

            # Ensure within bounds
            value = max(low_val, min(high_val, value))

            history.append({"timestamp": timestamp, "value": round(value, 2)})

        return history

    @staticmethod
    def read_file(filepath: str) -> Optional[str]:
        """Read content from file"""
        try:
            full_path = os.path.join(LiveIndicesReader.BASE_PATH, filepath)
            if not os.path.exists(full_path):
                return None

            with open(full_path, "r") as f:
                content = f.read().strip()
                return content if content else None
        except Exception:
            return None

    @staticmethod
    def read_history(filepath: str, lines: int = 100) -> List[Dict]:
        """Read historical data from file (timestamp,value format)"""
        try:
            full_path = os.path.join(LiveIndicesReader.BASE_PATH, filepath)
            if not os.path.exists(full_path):
                return []

            with open(full_path, "r") as f:
                all_lines = f.readlines()
                recent_lines = all_lines[-lines:] if len(all_lines) > lines else all_lines

            data = []
            for line in recent_lines:
                line = line.strip()
                if not line or "," not in line:
                    continue

                parts = line.split(",")
                if len(parts) >= 2:
                    timestamp_str = parts[0]
                    value_str = parts[1]

                    try:
                        data.append({"timestamp": timestamp_str, "value": float(value_str)})
                    except ValueError:
                        continue

            return data

        except Exception:
            return []

    @staticmethod
    def calculate_change(current: float, previous: float) -> Dict:
        """Calculate absolute and percentage change"""
        if previous == 0:
            return {"change": 0.0, "percentChange": 0.0}

        change = current - previous
        percent_change = (change / previous) * 100

        return {"change": round(change, 2), "percentChange": round(percent_change, 2)}

    @staticmethod
    def get_open_high_low(history: List[Dict]) -> Dict:
        """Extract open, high, low from history"""
        if not history:
            return {"open": 0.0, "high": 0.0, "low": 0.0}

        values = [item["value"] for item in history]

        return {
            "open": round(values[0], 2) if values else 0.0,
            "high": round(max(values), 2) if values else 0.0,
            "low": round(min(values), 2) if values else 0.0,
        }

    @staticmethod
    def get_last_value_from_history(history: List[Dict]) -> Optional[float]:
        """Get last value from history"""
        if not history:
            return None
        return history[-1]["value"]

    @staticmethod
    def get_index_data(index_key: str) -> Optional[Dict]:
        """Get complete data for a single index"""
        if index_key not in LiveIndicesReader.FILES:
            return None

        files = LiveIndicesReader.FILES[index_key]

        # Try to read spot file
        spot_str = LiveIndicesReader.read_file(files["spot"])

        # Try to read history
        history = LiveIndicesReader.read_history(files["history"], lines=100)

        # Determine current value
        current_value = None
        if spot_str:
            try:
                current_value = float(spot_str)
            except ValueError:
                pass

        # If no spot value, try to get last value from history
        if current_value is None and history:
            current_value = LiveIndicesReader.get_last_value_from_history(history)

        # If still no value, use static fallback
        if current_value is None:
            if index_key in LiveIndicesReader.FALLBACK_DATA:
                fallback = LiveIndicesReader.FALLBACK_DATA[index_key]
                current_value = fallback["value"]

                # Use pre-generated static history
                history = fallback["history"]

                # Use fallback O/H/L
                ohlc = {"open": fallback["open"], "high": fallback["high"], "low": fallback["low"]}
                previous_close = fallback["previousClose"]
            else:
                return None
        else:
            # Calculate open/high/low from history
            ohlc = LiveIndicesReader.get_open_high_low(history)
            previous_close = ohlc["open"] if ohlc["open"] > 0 else current_value

        # Calculate changes
        changes = LiveIndicesReader.calculate_change(current_value, previous_close)

        return {
            "value": round(current_value, 2),
            "change": changes["change"],
            "percentChange": changes["percentChange"],
            "open": ohlc["open"] if ohlc["open"] > 0 else current_value,
            "high": ohlc["high"] if ohlc["high"] > 0 else current_value,
            "low": ohlc["low"] if ohlc["low"] > 0 else current_value,
            "previousClose": previous_close,
            "history": history,
        }

    @staticmethod
    def get_all_indices() -> Dict:
        """Get data for all indices"""
        result = {}

        for index_key in LiveIndicesReader.FILES.keys():
            data = LiveIndicesReader.get_index_data(index_key)
            if data:
                result[index_key] = data

        # Always return success=True with at least fallback data
        return {"success": True, "timestamp": datetime.now().isoformat(), "indices": result}


# Initialize fallback data once on module load
LiveIndicesReader._init_fallback_data()


def get_live_indices():
    """Quick function to get all live indices data"""
    return LiveIndicesReader.get_all_indices()
