import datetime
import json
import os
from functools import lru_cache

import pandas as pd

from ..models.dashboard_model import get_available_dates, get_dashboard_data

# Path to the data directory - Adjust based on your actual structure
# Assuming we are in Analysis_Tools/app/services
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
DATA_DIR = os.path.join(BASE_DIR, "Data_scraper", "data")


class FundamentalService:
    _instance = None
    _ratios_data = {}
    _pnl_data = {}
    _cashflow_data = {}
    _shareholding_data = {}
    _market_data = {}  # New: Store live market data

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(FundamentalService, cls).__new__(cls)
            cls._instance._load_data()
        return cls._instance

    def _load_data(self):
        """Load all JSON files and latest market data into memory"""
        print(f"[INFO] Loading Fundamental Data from {DATA_DIR}...")

        # Load Market Data (Price, Change, Volume)
        try:
            dates = get_available_dates()
            if dates and len(dates) >= 1:
                latest_date = dates[0]
                print(f"[INFO] Loading Market Data for {latest_date}...")
                market_records = get_dashboard_data(latest_date, "TOTAL")
                self._market_data = {item["stock"]: item for item in market_records}

                # Fetch previous day for change calculation
                if len(dates) >= 2:
                    prev_date = dates[1]
                    print(f"[INFO] Loading Prev Market Data for {prev_date}...")
                    prev_records = get_dashboard_data(prev_date, "TOTAL")
                    # Merge previous close into market data dict
                    prev_map = {item["stock"]: item["closing_price"] for item in prev_records}
                    for ticker, data in self._market_data.items():
                        if ticker in prev_map:
                            data["prev_close"] = prev_map[ticker]

                # print(f"[INFO] Loaded market data for {len(self._market_data)} stocks.")
        except Exception as e:
            print(f"[ERROR] Failed to load market data: {e}")

        # Load Ratios
        ratios_dir = os.path.join(DATA_DIR, "ratios")
        if os.path.exists(ratios_dir):
            for filename in os.listdir(ratios_dir):
                if filename.endswith(".json"):
                    ticker = filename.replace(".json", "")
                    try:
                        with open(os.path.join(ratios_dir, filename), "r") as f:
                            self._ratios_data[ticker] = json.load(f)
                    except Exception as e:
                        print(f"[ERROR] Failed to load ratios for {ticker}: {e}")

        # Load PnL
        pnl_dir = os.path.join(DATA_DIR, "pnl")
        if os.path.exists(pnl_dir):
            for filename in os.listdir(pnl_dir):
                if filename.endswith(".json"):
                    ticker = filename.replace(".json", "")
                    try:
                        with open(os.path.join(pnl_dir, filename), "r") as f:
                            self._pnl_data[ticker] = json.load(f)
                    except Exception as e:
                        print(f"[ERROR] Failed to load pnl for {ticker}: {e}")

        # Load Cashflow
        cf_dir = os.path.join(DATA_DIR, "cashflow")
        if os.path.exists(cf_dir):
            for filename in os.listdir(cf_dir):
                if filename.endswith(".json"):
                    ticker = filename.replace(".json", "")
                    try:
                        with open(os.path.join(cf_dir, filename), "r") as f:
                            self._cashflow_data[ticker] = json.load(f)
                    except Exception as e:
                        print(f"[ERROR] Failed to load cashflow for {ticker}: {e}")

        # Load Shareholding
        sh_dir = os.path.join(DATA_DIR, "shareholding")
        if os.path.exists(sh_dir):
            for filename in os.listdir(sh_dir):
                if filename.endswith(".json"):
                    ticker = filename.replace(".json", "")
                    try:
                        with open(os.path.join(sh_dir, filename), "r") as f:
                            self._shareholding_data[ticker] = json.load(f)
                    except Exception as e:
                        print(f"[ERROR] Failed to load shareholding for {ticker}: {e}")

        # print(f"[INFO] Loaded {len(self._ratios_data)} ratios, {len(self._pnl_data)} pnl, {len(self._cashflow_data)} cashflow, {len(self._shareholding_data)} shareholding files.")

    def _get_latest_metric(self, data_dict, metric_name):
        """Helper to get the latest value for a metric from the messy JSON structure"""
        # Structure: {"date": [{"key": val}, ...]}
        latest_date = None
        latest_val = None

        # Find latest date
        sorted_dates = sorted(data_dict.keys(), reverse=True)

        for date_str in sorted_dates:
            if date_str == "TTM" and metric_name in ["Sales", "NetProfit", "EPSinRs"]:
                # Prefer TTM for PnL items if available
                items = data_dict["TTM"]
                for item in items:
                    # Keys often have trailing spaces or special chars in the JSON
                    # e.g. "Sales\u00a0"
                    clean_keys = {k.replace("\u00a0", "").replace("\u00c2", "").strip(): v for k, v in item.items()}
                    if metric_name in clean_keys:
                        return float(clean_keys[metric_name])

            # Process regular dates
            if date_str != "TTM":
                items = data_dict[date_str]
                for item in items:
                    # Robust cleaning: remove \u00a0, \u00c2, and spaces
                    clean_keys = {k.replace("\u00a0", "").replace("\u00c2", "").strip(): v for k, v in item.items()}
                    if metric_name in clean_keys:
                        # Found it! Since dates are sorted descending, this is the latest
                        return float(clean_keys[metric_name])

        return None

    def get_stock_fundamentals(self, ticker):
        """Get consolidated fundamentals for a stock"""
        data = {
            "ticker": ticker,
            "roe": 0,
            "roce": 0,
            "pe": 0,
            "debt_to_equity": 0,
            "net_profit_margin": 0,
            "opm": 0,
            "interest_coverage": 0,
            "sales_growth": 0,
            "profit_growth": 0,
            "eps": 0,
            "cash_from_ops": 0,
            "promoter_holding": 0,
            "fii_holding": 0,
            "fii_holding_prev": 0,
            "dividend_payout": 0,
            "inventory_days": 0,
            "working_capital_days": 0,
            "sales": 0,
            "net_profit": 0,
            "sales_growth_3yr": 0,
            "profit_growth_3yr": 0,
            "cash_from_investing": 0,
            "capex_growth_pct": 0,
            "market_cap": 0,
            # Market Data
            "price": 0,
            "change": 0,
            "change_pct": 0,
            "volume": 0,
            "oi": 0,
            "iv": 0,
        }

        # Merge Market Data
        # Handle Symbol Mismatches (Excel/File vs DB)
        market_ticker = ticker
        symbol_map = {"M&M": "M_M", "BAJAJ-AUTO": "BAJAJ_AUTO", "ARE&M": "ARE_M"}
        if ticker in symbol_map:
            market_ticker = symbol_map[ticker]

        if market_ticker in self._market_data:
            m = self._market_data[market_ticker]
            price = m.get("closing_price", 0) or 0
            data["price"] = price

            # Calculate Change
            prev_close = m.get("prev_close", 0)
            if prev_close and prev_close > 0 and price > 0:
                change = price - prev_close
                change_pct = (change / prev_close) * 100
                data["change"] = change
                data["change_pct"] = change_pct

            data["volume"] = (m.get("call_total_tradval", 0) or 0) + (m.get("put_total_tradval", 0) or 0)
            data["oi"] = 0  # Not available
            data["iv"] = 0  # Not available

            # Signal based on RSI
            rsi = m.get("rsi", 50) or 50
            if rsi > 70:
                data["signal"] = "BEARISH (Reversal)"
            elif rsi < 30:
                data["signal"] = "BULLISH (Reversal)"
            elif 50 < rsi <= 70:
                data["signal"] = "BULLISH"
            else:
                data["signal"] = "BEARISH"

        # Ratios
        if ticker in self._ratios_data:
            rData = self._ratios_data[ticker]
            # Try ROCE first, then fallback to ROE (common for financial stocks)
            roce = self._get_latest_metric(rData, "ROCE%") or 0
            if roce == 0:
                roce = self._get_latest_metric(rData, "ROE%") or 0

            data["roce"] = roce
            data["inventory_days"] = self._get_latest_metric(rData, "InventoryDays") or 0
            data["working_capital_days"] = self._get_latest_metric(rData, "WorkingCapitalDays") or 0

        # PnL
        if ticker in self._pnl_data:
            pData = self._pnl_data[ticker]
            data["eps"] = self._get_latest_metric(pData, "EPSinRs") or 0
            data["opm"] = self._get_latest_metric(pData, "OPM%") or 0
            data["dividend_payout"] = self._get_latest_metric(pData, "DividendPayout%") or 0
            if data["eps"] > 0 and data["price"] > 0:
                data["pe"] = data["price"] / data["eps"]

            # Sales & Profit (Latest)
            sales = self._get_latest_metric(pData, "Sales")
            profit = self._get_latest_metric(pData, "NetProfit")
            data["sales"] = sales or 0
            data["net_profit"] = profit or 0

            if sales and profit and sales > 0:
                data["net_profit_margin"] = (profit / sales) * 100

            # Interest Coverage
            op_profit = self._get_latest_metric(pData, "OperatingProfit")
            interest = self._get_latest_metric(pData, "Interest")
            if op_profit and interest and interest > 0:
                data["interest_coverage"] = op_profit / interest
            elif op_profit and (not interest or interest == 0):
                data["interest_coverage"] = 999

            # Growth Calculation (3-Year CAGR rough approx: Latest vs 3 years ago)
            # data structure: {"2024-03-01": [...], ...}
            sorted_dates = sorted([d for d in pData.keys() if d != "TTM"], reverse=True)
            if len(sorted_dates) >= 4:
                # Latest vs 3 years ago (Item 0 vs Item 3)
                latest_date = sorted_dates[0]
                past_date = sorted_dates[3]

                # Helper to extract value from list of dicts for a specific date
                def get_val(d_str, key):
                    for x in pData[d_str]:
                        clean_k = {k.replace("\u00a0", "").replace("\u00c2", "").strip(): v for k, v in x.items()}
                        if key in clean_k:
                            return float(clean_k[key])
                    return 0

                s_latest = get_val(latest_date, "Sales")
                s_past = get_val(past_date, "Sales")
                p_latest = get_val(latest_date, "NetProfit")
                p_past = get_val(past_date, "NetProfit")

                if s_past > 0:
                    data["sales_growth_3yr"] = ((s_latest / s_past) ** (1 / 3) - 1) * 100
                if p_past > 0:
                    # Handle absolute growth if base is negative? Complex. Simple calc for now.
                    if p_latest > p_past:
                        data["profit_growth_3yr"] = ((p_latest / p_past) ** (1 / 3) - 1) * 100
                    else:
                        data["profit_growth_3yr"] = -10  # Decline

        # Cashflow
        if ticker in self._cashflow_data:
            cData = self._cashflow_data[ticker]
            data["cash_from_ops"] = self._get_latest_metric(cData, "CashfromOperatingActivity") or 0
            data["cash_from_investing"] = self._get_latest_metric(cData, "CashfromInvestingActivity") or 0

            # Calculate Capex Growth (Investing Outflow Increase)
            sorted_dates = sorted([d for d in cData.keys() if d != "TTM"], reverse=True)
            if len(sorted_dates) >= 2:
                latest_date = sorted_dates[0]
                prev_date = sorted_dates[1]

                def get_cf_val(d_str, key):
                    for x in cData[d_str]:
                        clean_k = {k.replace("\u00a0", "").replace("\u00c2", "").strip(): v for k, v in x.items()}
                        if key in clean_k:
                            return float(clean_k[key])
                    return 0

                # Investing activity is usually negative for Capex.
                # We want to see if the OUTFLOW increased.
                # e.g. -100 to -150 -> 50% increase in spending.
                inv_latest = get_cf_val(latest_date, "CashfromInvestingActivity")
                inv_prev = get_cf_val(prev_date, "CashfromInvestingActivity")

                # Check if both are negative (spending)
                if inv_latest < 0 and inv_prev < 0:
                    # Calculate growth in absolute terms (spending growth)
                    # (Abs(Latest) - Abs(Prev)) / Abs(Prev)
                    abs_latest = abs(inv_latest)
                    abs_prev = abs(inv_prev)
                    data["capex_growth_pct"] = ((abs_latest - abs_prev) / abs_prev) * 100

                # If moving from pos to neg (started spending), that's huge growth?
                elif inv_latest < 0 and inv_prev >= 0:
                    data["capex_growth_pct"] = 100  # Arbitrary high growth indicator

        # Shareholding
        if ticker in self._shareholding_data:
            sData = self._shareholding_data[ticker]
            sorted_dates = sorted(sData.keys(), reverse=True)
            if sorted_dates:
                latest_date = sorted_dates[0]
                latest_items = sData[latest_date]
                for item in latest_items:
                    clean_keys = {k.replace("\u00a0", "").replace("\u00c2", "").strip(): v for k, v in item.items()}
                    if "Promoters" in clean_keys:
                        data["promoter_holding"] = float(clean_keys["Promoters"]) * 100
                    if "FIIs" in clean_keys:
                        data["fii_holding"] = float(clean_keys["FIIs"]) * 100

        # Calculate Market Cap (Proxy: NetProfit/EPS * Price)
        # Done at end to ensure NetProfit, EPS, and Price are available
        if data.get("eps", 0) > 0 and data.get("net_profit", 0) > 0 and data.get("price", 0) > 0:
            data["market_cap"] = (data["net_profit"] / data["eps"]) * data["price"]

        return data

    def filter_stocks(self, criteria_func):
        """
        Filter stocks based on a criteria function.
        criteria_func(data_dict) -> bool
        """
        results = []
        all_tickers = set(
            list(self._ratios_data.keys())
            + list(self._pnl_data.keys())
            + list(self._cashflow_data.keys())
            + list(self._shareholding_data.keys())
        )

        for ticker in all_tickers:
            stats = self.get_stock_fundamentals(ticker)
            if criteria_func(stats):
                results.append(stats)

        return results


# Singleton instance
fundamental_service = FundamentalService()
