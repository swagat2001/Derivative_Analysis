"""
LIVE FII/DII DATA STREAMER
Fetches real-time FII/DII data from NSE and writes to text files for home page display.
Similar to live_indices_streamer.py

Location: spot_data/live_fii_dii_streamer.py

Usage:
    python live_fii_dii_streamer.py          # Run live updates
    python live_fii_dii_streamer.py once     # Fetch once and exit

Created: 2026-02-02
"""

import json
import os
import sys
import time
from datetime import datetime
from datetime import time as dtime

import requests
from colorama import Fore, Style, init

# Initialize colorama
init(autoreset=True)

# === Paths ===
SPOT_DATA_PATH = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(SPOT_DATA_PATH, "Data")

# Ensure data directory exists
os.makedirs(DATA_PATH, exist_ok=True)

# Output files
FII_DII_SPOT_FILE = os.path.join(DATA_PATH, "FiiDiiSpot.json")
FII_DII_HISTORY_FILE = os.path.join(DATA_PATH, "FiiDiiHistory.txt")

# === NSE Configuration ===
NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.nseindia.com/reports-indices-historical-vix",
    "Connection": "keep-alive",
}

NSE_FII_DII_URL = "https://www.nseindia.com/api/fiidiiTradeReact"
NSE_BASE_URL = "https://www.nseindia.com"

# Market hours
MARKET_OPEN = dtime(9, 0)
MARKET_CLOSE = dtime(18, 30)  # Extended for FII/DII updates

# Refresh interval (seconds)
REFRESH_INTERVAL = 300  # 5 minutes


def create_nse_session():
    """Create a session with NSE cookies."""
    session = requests.Session()
    session.headers.update(NSE_HEADERS)

    try:
        session.get(NSE_BASE_URL, timeout=15)
        time.sleep(1)
        session.get(f"{NSE_BASE_URL}/reports-indices-historical-vix", timeout=15)
        time.sleep(0.5)
        return session
    except Exception as e:
        print(f"{Fore.YELLOW}[WARN] Session issue: {e}{Style.RESET_ALL}")
        return session


def fetch_fii_dii_data(session):
    """Fetch FII/DII data from NSE."""
    try:
        response = session.get(NSE_FII_DII_URL, timeout=20)

        if response.status_code != 200:
            return None

        return response.json()

    except Exception as e:
        print(f"{Fore.RED}[ERROR] Fetch failed: {e}{Style.RESET_ALL}")
        return None


def parse_fii_dii_data(raw_data):
    """Parse NSE FII/DII API response."""
    if not raw_data:
        return None

    result = {
        "fii_buy": 0,
        "fii_sell": 0,
        "fii_net": 0,
        "dii_buy": 0,
        "dii_sell": 0,
        "dii_net": 0,
        "total_net": 0,
        "date": "",
        "timestamp": datetime.now().isoformat(),
    }

    for item in raw_data:
        try:
            category = item.get("category", "").upper()
            date_str = item.get("date", "")

            def parse_value(val):
                if val is None:
                    return 0.0
                val_str = str(val).replace(",", "").replace(" ", "")
                try:
                    return float(val_str)
                except:
                    return 0.0

            buy_val = parse_value(item.get("buyValue"))
            sell_val = parse_value(item.get("sellValue"))
            net_val = parse_value(item.get("netValue"))

            if "FII" in category or "FPI" in category:
                result["fii_buy"] = buy_val
                result["fii_sell"] = sell_val
                result["fii_net"] = net_val
                result["date"] = date_str
            elif "DII" in category:
                result["dii_buy"] = buy_val
                result["dii_sell"] = sell_val
                result["dii_net"] = net_val

        except Exception:
            continue

    result["total_net"] = result["fii_net"] + result["dii_net"]
    return result


def write_fii_dii_data(data):
    """Write FII/DII data to files."""
    try:
        # Write JSON spot file (current values)
        with open(FII_DII_SPOT_FILE, "w") as f:
            json.dump(data, f, indent=2)

        # Append to history file
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        history_line = f"{timestamp},{data['fii_net']},{data['dii_net']},{data['total_net']}\n"

        with open(FII_DII_HISTORY_FILE, "a") as f:
            f.write(history_line)

        return True

    except Exception as e:
        print(f"{Fore.RED}[ERROR] Write failed: {e}{Style.RESET_ALL}")
        return False


def display_data(data, prev_data=None):
    """Display FII/DII data in console."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Determine colors based on values
    fii_color = Fore.GREEN if data["fii_net"] >= 0 else Fore.RED
    dii_color = Fore.GREEN if data["dii_net"] >= 0 else Fore.RED
    total_color = Fore.GREEN if data["total_net"] >= 0 else Fore.RED

    fii_sign = "+" if data["fii_net"] >= 0 else ""
    dii_sign = "+" if data["dii_net"] >= 0 else ""
    total_sign = "+" if data["total_net"] >= 0 else ""

    print(f"\n{timestamp} | {Fore.CYAN}FII/DII Activity{Style.RESET_ALL} | Date: {data['date']}")
    print(f"  FII Net: {fii_color}{fii_sign}₹{data['fii_net']:,.2f} Cr{Style.RESET_ALL}")
    print(f"  DII Net: {dii_color}{dii_sign}₹{data['dii_net']:,.2f} Cr{Style.RESET_ALL}")
    print(f"  Total:   {total_color}{total_sign}₹{data['total_net']:,.2f} Cr{Style.RESET_ALL}")


def run_once():
    """Fetch data once and exit."""
    print(f"{Fore.CYAN}===== FII/DII DATA - SINGLE FETCH ====={Style.RESET_ALL}\n")

    session = create_nse_session()
    raw_data = fetch_fii_dii_data(session)

    if not raw_data:
        print(f"{Fore.RED}[ERROR] Could not fetch data{Style.RESET_ALL}")
        return False

    data = parse_fii_dii_data(raw_data)

    if not data:
        print(f"{Fore.RED}[ERROR] Could not parse data{Style.RESET_ALL}")
        return False

    write_fii_dii_data(data)
    display_data(data)

    print(f"\n{Fore.GREEN}[OK] Data saved to:{Style.RESET_ALL}")
    print(f"  - {FII_DII_SPOT_FILE}")
    print(f"  - {FII_DII_HISTORY_FILE}")

    return True


def run_continuous():
    """Run continuous updates."""
    os.system("cls" if os.name == "nt" else "clear")
    print(f"{Fore.CYAN}===== GOLDMINE LIVE FII/DII STREAMER ====={Style.RESET_ALL}\n")
    print(f"{Fore.CYAN}Streaming: FII/DII Cash Market Activity{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}Data Path: {DATA_PATH}{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}Refresh Interval: {REFRESH_INTERVAL} seconds{Style.RESET_ALL}\n")

    session = create_nse_session()
    prev_data = None

    while True:
        try:
            current_time = datetime.now().time()

            # Check if within market/update hours
            if not (MARKET_OPEN <= current_time <= MARKET_CLOSE):
                print(f"\n{Fore.YELLOW}[INFO] Outside market hours. Waiting...{Style.RESET_ALL}")
                time.sleep(60)
                continue

            # Fetch data
            raw_data = fetch_fii_dii_data(session)

            if raw_data:
                data = parse_fii_dii_data(raw_data)

                if data:
                    write_fii_dii_data(data)
                    display_data(data, prev_data)
                    prev_data = data
            else:
                # Re-establish session
                print(f"{Fore.YELLOW}[INFO] Refreshing session...{Style.RESET_ALL}")
                session = create_nse_session()

            # Wait for next update
            time.sleep(REFRESH_INTERVAL)

        except KeyboardInterrupt:
            print(f"\n{Fore.CYAN}Shutdown Requested. Exiting...{Style.RESET_ALL}")
            break
        except Exception as e:
            print(f"{Fore.RED}[ERROR] {e}{Style.RESET_ALL}")
            print(f"{Fore.YELLOW}Reconnecting in 30 seconds...{Style.RESET_ALL}")
            time.sleep(30)
            session = create_nse_session()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].lower() == "once":
        run_once()
    else:
        run_continuous()
