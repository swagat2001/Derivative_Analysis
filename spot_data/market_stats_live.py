
import os
import json
import time
from datetime import datetime
from nsepython import nsefetch

# Configuration
UPDATE_INTERVAL_SECONDS = 60
base_dir = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(base_dir, "Data")
JSON_FILE_PATH = os.path.join(DATA_DIR, "MarketStats.json")

def ensure_data_dir():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

def get_market_statistics():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Fetching Live Market Statistics...")

    try:
        # 1. Fetch Advances, Declines, and Unchanged (Breadth)
        # Fetching all indices data
        indices_data = nsefetch('https://www.nseindia.com/api/allIndices')

        market_breadth = None

        # PRIORITY 1: Check for root-level broader market data (NSE Website Homepage style)
        if indices_data and 'advances' in indices_data:
             market_breadth = {
                 'advances': indices_data['advances'],
                 'declines': indices_data['declines'],
                 'unchanged': indices_data['unchanged'],
                 'index': 'NSE Market Status', # General name
                 'timestamp': indices_data.get('timestamp')
             }

        # PRIORITY 2: Fallback to NIFTY 500 or NIFTY 5 if root data missing
        if not market_breadth and indices_data and 'data' in indices_data:
            for item in indices_data['data']:
                if item['index'] == "NIFTY 500":
                    market_breadth = item
                    break

            if not market_breadth:
                 market_breadth = next((item for item in indices_data['data'] if item['index'] == "NIFTY 50"), None)

        if not market_breadth:
             # Fallback: Try to use 'Total' if available or just the first item
             if indices_data and 'data' in indices_data and len(indices_data['data']) > 0:
                 market_breadth = indices_data['data'][0]
                 print(f"Warning: NIFTY 50/500 not found. Using {market_breadth.get('index')} as fallback.")

        if not market_breadth:
            print("Error: Could not find any valid market breadth data.")
            return None

        # 2. Fetch 52 Week High/Low
        high_low_url = "https://www.nseindia.com/api/live-analysis-52-week-high-low?index=ALL"
        high_low_data = nsefetch(high_low_url)

        # 3. Fetch Circuit Breakers (Upper/Lower Circuits)
        circuit_url = "https://www.nseindia.com/api/live-analysis-price-band-hitters?index=ALL"
        circuit_data = nsefetch(circuit_url)

        # Construct the data dictionary
        # Safely get values with defaults
        advances = int(market_breadth.get('advances', 0))
        declines = int(market_breadth.get('declines', 0))
        unchanged = int(market_breadth.get('unchanged', 0))

        # Use API timestamp if available, else system time
        last_updated_time = market_breadth.get('timestamp')
        if not last_updated_time:
             last_updated_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Calculate stock_traded. If 0 (market closed/no data yet), maybe use prev close data?
        # For now, just show what we have.

        # Robust length checks for lists
        w52_high = 0
        if high_low_data:
             if 'data' in high_low_data and 'high' in high_low_data['data']:
                 w52_high = len(high_low_data['data']['high'])
             elif 'high' in high_low_data:
                 w52_high = len(high_low_data['high'])

        w52_low = 0
        if high_low_data:
             if 'data' in high_low_data and 'low' in high_low_data['data']:
                 w52_low = len(high_low_data['data']['low'])
             elif 'low' in high_low_data:
                 w52_low = len(high_low_data['low'])

        uc_count = 0
        if circuit_data:
            if 'data' in circuit_data and 'upper' in circuit_data['data']:
                uc_count = len(circuit_data['data']['upper'])
            elif 'upper' in circuit_data:
                 uc_count = len(circuit_data['upper'])

        lc_count = 0
        if circuit_data:
             if 'data' in circuit_data and 'lower' in circuit_data['data']:
                 lc_count = len(circuit_data['data']['lower'])
             elif 'lower' in circuit_data:
                 lc_count = len(circuit_data['lower'])

        stats = {
            "last_updated": last_updated_time,
            "advances": advances,
            "declines": declines,
            "unchanged": unchanged,
            "stock_traded": advances + declines + unchanged,
            "week52_high": w52_high,
            "week52_low": w52_low,
            "upper_circuits": uc_count,
            "lower_circuits": lc_count,
            "index_name": market_breadth.get('index', 'Unknown')
        }

        return stats

    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

def main():
    ensure_data_dir()
    print(f"Starting Market Stats Fetcher... (Interval: {UPDATE_INTERVAL_SECONDS}s)")
    print(f"Saving data to: {JSON_FILE_PATH}")

    while True:
        stats = get_market_statistics()

        if stats:
            try:
                with open(JSON_FILE_PATH, "w") as f:
                    json.dump(stats, f, indent=4)
                print(f"[{stats['last_updated']}] Stats updated successfully.")
            except Exception as e:
                print(f"Error saving JSON: {e}")

        time.sleep(UPDATE_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
