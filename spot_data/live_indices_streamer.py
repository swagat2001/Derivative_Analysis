import asyncio
import json
import os
import ssl
import uuid
from datetime import datetime, time
from time import localtime, strftime

import MarketDataFeedV3_pb2 as pb  # noqa: E402 - local protobuf module
import requests
import websockets
from colorama import Fore, Style, init
from dotenv import load_dotenv
from google.protobuf.json_format import MessageToDict

# Initialize colorama
init(autoreset=True)

# === Load environment variables from .env file ===
SPOT_DATA_PATH = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SPOT_DATA_PATH)
ENV_FILE = os.path.join(PROJECT_ROOT, ".env")

# Load .env file
load_dotenv(ENV_FILE)

# === Paths ===
DATA_PATH = os.path.join(SPOT_DATA_PATH, "Data")

# === Ensure data directory exists ===
os.makedirs(DATA_PATH, exist_ok=True)

# === Instrument Keys for Home Page Indices ===
INSTRUMENTS = {
    "NSE_INDEX|Nifty 50": {
        "name": "NIFTY 50",
        "spot_file": "Nifty50Spot.txt",
        "history_file": "Nifty50History.txt",
    },
    "NSE_INDEX|Nifty Bank": {
        "name": "BANK NIFTY",
        "spot_file": "BankNiftySpot.txt",
        "history_file": "BankNiftyHistory.txt",
    },
    "BSE_INDEX|SENSEX": {
        "name": "SENSEX",
        "spot_file": "SensexSpot.txt",
        "history_file": "SensexHistory.txt",
    },
    "NSE_INDEX|Nifty Financial Services": {
        "name": "NIFTY FIN",
        "spot_file": "NiftyFinSpot.txt",
        "history_file": "NiftyFinHistory.txt",
    },
    "NSE_INDEX|Nifty Next 50": {
        "name": "NIFTY NEXT 50",
        "spot_file": "NiftyNext50Spot.txt",
        "history_file": "NiftyNext50History.txt",
    },
    "NSE_INDEX|NIFTY 100": {
        "name": "NIFTY 100",
        "spot_file": "Nifty100Spot.txt",
        "history_file": "Nifty100History.txt",
    },
    "NSE_INDEX|India VIX": {
        "name": "INDIA VIX",
        "spot_file": "IndiaVIXSpot.txt",
        "history_file": "IndiaVIXHistory.txt",
    },
}

# === Time Limits ===
market_open = time(9, 10)
market_close = time(15, 35)


# === Utility Functions ===
def read_access_token():
    """Read access token from .env file"""
    token = os.getenv("UPSTOX_ACCESS_TOKEN")
    if not token or token == "your-upstox-access-token-here":
        raise ValueError(
            f"UPSTOX_ACCESS_TOKEN not set in {ENV_FILE}. " "Please update the .env file with your Upstox access token."
        )
    return token


def get_market_data_feed_authorize_v3():
    access_token = read_access_token()
    headers = {"Accept": "application/json", "Authorization": f"Bearer {access_token}"}
    url = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
    resp = requests.get(url=url, headers=headers).json()
    if "data" not in resp:
        raise ValueError(f"Authorization failed: {resp}")
    return resp


def decode_protobuf(buffer):
    feed_response = pb.FeedResponse()
    feed_response.ParseFromString(buffer)
    return feed_response


def write_spot_data(instrument_key, ltp, formatted_time, msg_time):
    """Write spot and history data for an instrument."""
    if instrument_key not in INSTRUMENTS:
        return

    config = INSTRUMENTS[instrument_key]
    spot_path = os.path.join(DATA_PATH, config["spot_file"])
    history_path = os.path.join(DATA_PATH, config["history_file"])

    # Write to files only during market hours
    if market_open <= msg_time <= market_close:
        # Write spot (current value)
        with open(spot_path, "w") as f:
            f.write(str(ltp))

        # Append to history
        with open(history_path, "a") as f:
            f.write(f"{formatted_time},{ltp}\n")


# === Main Async Function ===
async def fetch_market_data():
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    os.system("cls" if os.name == "nt" else "clear")
    print(f"{Fore.CYAN}===== GOLDMINE LIVE INDICES STREAMER ====={Style.RESET_ALL}\n")
    print(f"{Fore.CYAN}Streaming: NIFTY 50, BANK NIFTY, SENSEX, NIFTY FIN, NIFTY NEXT 50, NIFTY 100, INDIA VIX{Style.RESET_ALL}\n")
    print(f"{Fore.YELLOW}Data Path: {DATA_PATH}{Style.RESET_ALL}\n")

    previous_values = {}

    while True:
        try:
            # Fetch new WebSocket URL
            response = get_market_data_feed_authorize_v3()
            ws_url = response["data"]["authorized_redirect_uri"]

            async with websockets.connect(ws_url, ssl=ssl_context) as websocket:
                print(f"{Fore.GREEN}WebSocket connection established.{Style.RESET_ALL}")

                # Subscribe to all instruments
                instrument_keys = list(INSTRUMENTS.keys())
                sub_msg = {
                    "guid": str(uuid.uuid4()),
                    "method": "sub",
                    "data": {"mode": "full", "instrumentKeys": instrument_keys},
                }
                await websocket.send(json.dumps(sub_msg).encode("utf-8"))
                print(f"{Fore.GREEN}Subscribed to {len(instrument_keys)} instruments.{Style.RESET_ALL}\n")

                # Main data processing loop
                while True:
                    message = await websocket.recv()

                    try:
                        # Decode protobuf
                        decoded_data = decode_protobuf(message)
                        data_dict = MessageToDict(decoded_data)
                    except Exception:
                        try:
                            data_dict = json.loads(message)
                        except Exception:
                            continue

                    if data_dict.get("type") == "market_info":
                        continue

                    feeds = data_dict.get("feeds", {})
                    if not feeds:
                        continue

                    for instrument, feed in feeds.items():
                        # Handle index feeds
                        fullfeed = feed.get("fullFeed", {}).get("indexFF", {})
                        if not fullfeed:
                            continue

                        ltpc = fullfeed.get("ltpc", {})
                        ltp = ltpc.get("ltp")
                        ts = ltpc.get("ltt")

                        if ltp is None or ts is None:
                            continue

                        epoch_time = int(ts) / 1000
                        formatted_time = strftime("%Y-%m-%d %H:%M:%S", localtime(epoch_time))
                        msg_time = datetime.strptime(formatted_time, "%Y-%m-%d %H:%M:%S").time()

                        # Write data to files
                        write_spot_data(instrument, ltp, formatted_time, msg_time)

                        # Console output with color coding
                        if instrument in INSTRUMENTS:
                            config = INSTRUMENTS[instrument]
                            prev = previous_values.get(instrument)
                            color = Fore.WHITE
                            if prev is not None:
                                color = Fore.GREEN if ltp > prev else (Fore.RED if ltp < prev else Fore.WHITE)
                            previous_values[instrument] = ltp

                            print(
                                f"{formatted_time} | {Fore.YELLOW}{config['name']:15}{Style.RESET_ALL} | {color}{ltp:,.2f}{Style.RESET_ALL}"
                            )

        except Exception as e:
            print(f"{Fore.RED}Error: {e}{Style.RESET_ALL}")
            print(f"{Fore.YELLOW}Reconnecting in 5 seconds...{Style.RESET_ALL}\n")
            await asyncio.sleep(5)


# === Run with Graceful Exit ===
if __name__ == "__main__":
    try:
        asyncio.run(fetch_market_data())
    except KeyboardInterrupt:
        print(f"\n{Fore.CYAN}Shutdown Requested. Exiting...{Style.RESET_ALL}")
