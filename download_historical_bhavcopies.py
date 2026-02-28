
import os
import sys
import time
import urllib.request
from datetime import datetime, timedelta
import socket

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), ".")))

# Constants
CASH_DATA_PATH = "C:/NSE_EOD_CASH"
DOWNLOAD_TIMEOUT = 30
MAX_RETRIES = 2
RETRY_DELAY = 2

def download_with_timeout(url, filepath, timeout=DOWNLOAD_TIMEOUT):
    """Download file with timeout and headers"""
    socket.setdefaulttimeout(timeout)
    try:
        req = urllib.request.Request(url)
        req.add_header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        req.add_header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
        with urllib.request.urlopen(req, timeout=timeout) as response:
            with open(filepath, "wb") as out_file:
                out_file.write(response.read())
        return True
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return False

def main():
    os.makedirs(CASH_DATA_PATH, exist_ok=True)

    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=35) # A bit more than 30 to be safe

    date_range = []
    delta_days = (end_date - start_date).days
    for i in range(delta_days + 1):
        d = start_date + timedelta(days=i)
        if d.weekday() < 5: # Monday to Friday
            date_range.append(d)

    print(f"Starting download for {len(date_range)} potential trading days...")

    for date_obj in date_range:
        date_str_ddmmyyyy = date_obj.strftime("%d%m%Y")
        filename = f"sec_bhavdata_full_{date_str_ddmmyyyy}.csv"
        filepath = os.path.join(CASH_DATA_PATH, filename)

        if os.path.exists(filepath):
            print(f"Index {date_obj}: Already exists")
            continue

        url = f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date_str_ddmmyyyy}.csv"

        success = False
        for attempt in range(MAX_RETRIES + 1):
            if attempt > 0:
                time.sleep(RETRY_DELAY)
            if download_with_timeout(url, filepath):
                print(f"Index {date_obj}: Downloaded")
                success = True
                break

        if not success:
            print(f"Index {date_obj}: Failed or Holiday")

if __name__ == "__main__":
    main()
