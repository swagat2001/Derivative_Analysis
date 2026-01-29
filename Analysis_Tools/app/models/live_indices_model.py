"""
Live Indices Data Model
Reads real-time data from spot files written by live_indices_streamer.py
"""

import os
from datetime import datetime
from typing import Dict, List, Optional


class LiveIndicesReader:
    """Reads live market data from text files written by Python WebSocket streamer"""

    # Path to spot data directory (relative to project root)
    BASE_PATH = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))),
        "spot_data",
        "Data",
    )

    # File mappings for each index
    FILES = {
        "nifty50": {"spot": "Nifty50Spot.txt", "history": "Nifty50History.txt"},
        "banknifty": {"spot": "BankNiftySpot.txt", "history": "BankNiftyHistory.txt"},
        "sensex": {"spot": "SensexSpot.txt", "history": "SensexHistory.txt"},
        "niftyfin": {"spot": "NiftyFinSpot.txt", "history": "NiftyFinHistory.txt"},
        "niftynext50": {"spot": "NiftyNext50Spot.txt", "history": "NiftyNext50History.txt"},
        "nifty100": {"spot": "Nifty100Spot.txt", "history": "Nifty100History.txt"},
    }

    # Static fallback data when files don't exist (market closed)
    FALLBACK_DATA = {
        "nifty50": {"value": 24677.80, "open": 24600.00, "high": 24700.00, "low": 24550.00, "previousClose": 24600.00},
        "banknifty": {
            "value": 51234.50,
            "open": 51450.00,
            "high": 51550.30,
            "low": 51100.20,
            "previousClose": 51450.00,
        },
        "sensex": {"value": 83758.43, "open": 83200.00, "high": 83900.50, "low": 83150.00, "previousClose": 83200.00},
        "niftyfin": {"value": 23890.40, "open": 23750.00, "high": 23950.00, "low": 23700.00, "previousClose": 23750.00},
        "niftynext50": {
            "value": 68499.30,
            "open": 69200.00,
            "high": 69350.00,
            "low": 68400.00,
            "previousClose": 69200.00,
        },
        "nifty100": {"value": 26260.30, "open": 26450.00, "high": 26500.00, "low": 26200.00, "previousClose": 26450.00},
    }

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
            current_value = history[-1]["value"]

        # If still no value, use static fallback
        if current_value is None:
            if index_key in LiveIndicesReader.FALLBACK_DATA:
                fallback = LiveIndicesReader.FALLBACK_DATA[index_key]
                current_value = fallback["value"]
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


def get_live_indices():
    """Quick function to get all live indices data"""
    return LiveIndicesReader.get_all_indices()
