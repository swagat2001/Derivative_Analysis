import os
import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup

# Folder containing the files
FOLDER_PATH = r"C:\NSE_EOD_CASH"

# Regex to match date in filename: we expect DDMMYYYY after some prefix/suffix
# e.g. sec_bhavdata_full_01012024  -> captures 01012024
DATE_PATTERN = re.compile(r"(\d{2})(\d{2})(\d{4})")


def fetch_nse_holidays(year):
    """
    Fetch NSE holiday calendar for a given year from a public site.
    Returns a set of date-strings in ISO format YYYY-MM-DD.
    """
    # Example public site with holiday list
    url = "https://www.bankbazaar.com/indian-holiday/nse-holidays.html"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    holidays = set()
    # The table rows typically have Date (like "26 January 2025")
    # We'll scan for any occurrence of the given year
    for row in soup.find_all("tr"):
        cols = row.find_all("td")
        if not cols:
            continue
        date_text = cols[1].get_text(strip=True)  # e.g. "26 January 2025"
        if str(year) in date_text:
            try:
                dt = datetime.strptime(date_text, "%d %B %Y").date()
                holidays.add(dt.isoformat())
            except ValueError:
                # Sometimes format may vary (e.g. month abbreviations) — skip bad rows
                pass
    return holidays


def is_trading_day(dt, holidays_set):
    """
    Returns True if dt (datetime.date) is a trading day for NSE.
    Trading days = Mon–Fri AND not in holiday list.
    """
    if dt.weekday() >= 5:  # Saturday = 5, Sunday = 6
        return False
    if dt.isoformat() in holidays_set:
        return False
    return True


def inspect_folder(folder_path):
    dates = set()
    for fn in os.listdir(folder_path):
        m = DATE_PATTERN.search(fn)
        if m:
            dd, mm, yyyy = m.groups()
            try:
                dt = datetime.strptime(f"{dd}{mm}{yyyy}", "%d%m%Y").date()
                dates.add(dt)
            except ValueError:
                print(f"WARNING: unable to parse date from filename '{fn}'")
    return sorted(dates)


def main():
    dates = inspect_folder(FOLDER_PATH)
    print(f"Total distinct days of data found: {len(dates)}\n")
    if not dates:
        print("No date-formatted files found — check your filename pattern.")
        return

    # Determine which years are present in data
    years = {d.year for d in dates}
    holidays = set()
    for y in years:
        try:
            h = fetch_nse_holidays(y)
            holidays.update(h)
            print(f"Fetched {len(h)} holidays for year {y}")
        except Exception as e:
            print(f"ERROR: unable to fetch holidays for year {y}: {e}")
            # fallback: treat only weekends as non-trading
            pass

    print("\nDates found and trading-status:\n")
    for d in dates:
        status = "Trading Day" if is_trading_day(d, holidays) else "Non-Trading Day"
        extra = ""
        if d.weekday() >= 5:
            extra = "(Weekend)"
        elif d.isoformat() in holidays:
            extra = "(Holiday)"
        print(f"{d.isoformat()}  -->  {status} {extra}")


if __name__ == "__main__":
    main()
