"""
Index Constituents Model
Fetches index constituents from NSE and enriches with database data
Uses futures_oi_cache for underlying prices and your existing derivative tables
"""

from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import requests
from sqlalchemy import inspect, text

from .db_config import engine


class IndexConstituentsModel:
    """Handles fetching and processing of index constituents"""

    NSE_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/",
    }

    NIFTY_50_URL = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
    BANK_NIFTY_URL = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20BANK"

    @staticmethod
    def _create_session():
        """Create session with proper headers for NSE"""
        session = requests.Session()
        session.headers.update(IndexConstituentsModel.NSE_HEADERS)
        # Visit homepage first to get cookies
        try:
            session.get("https://www.nseindia.com", timeout=5)
        except:
            pass
        return session

    @staticmethod
    def get_nifty50_constituents():
        """
        Fetch current Nifty 50 constituents from NSE
        Returns: List of ticker symbols
        """
        try:
            session = IndexConstituentsModel._create_session()
            response = session.get(IndexConstituentsModel.NIFTY_50_URL, timeout=10)

            if response.status_code == 200:
                data = response.json()
                constituents = []

                for item in data.get("data", []):
                    symbol = item.get("symbol", "")
                    if symbol and symbol != "NIFTY 50":
                        constituents.append(symbol)

                print(f"[INFO] Fetched {len(constituents)} Nifty 50 constituents from NSE")
                return constituents
            else:
                print(f"[WARNING] NSE API returned status {response.status_code}")
                return IndexConstituentsModel._get_fallback_nifty50()

        except Exception as e:
            print(f"[ERROR] Failed to fetch Nifty 50 from NSE: {e}")
            return IndexConstituentsModel._get_fallback_nifty50()

    @staticmethod
    def get_banknifty_constituents():
        """
        Fetch current Bank Nifty constituents from NSE
        Returns: List of ticker symbols
        """
        try:
            session = IndexConstituentsModel._create_session()
            response = session.get(IndexConstituentsModel.BANK_NIFTY_URL, timeout=10)

            if response.status_code == 200:
                data = response.json()
                constituents = []

                for item in data.get("data", []):
                    symbol = item.get("symbol", "")
                    if symbol and symbol != "NIFTY BANK":
                        constituents.append(symbol)

                print(f"[INFO] Fetched {len(constituents)} Bank Nifty constituents from NSE")
                return constituents
            else:
                print(f"[WARNING] NSE API returned status {response.status_code}")
                return IndexConstituentsModel._get_fallback_banknifty()

        except Exception as e:
            print(f"[ERROR] Failed to fetch Bank Nifty from NSE: {e}")
            return IndexConstituentsModel._get_fallback_banknifty()

    @staticmethod
    def _get_fallback_nifty50():
        """Fallback list of Nifty 50 constituents (as of Jan 2025)"""
        return [
            "RELIANCE",
            "HDFCBANK",
            "ICICIBANK",
            "INFY",
            "TCS",
            "BHARTIARTL",
            "SBIN",
            "KOTAKBANK",
            "LT",
            "WIPRO",
            "AXISBANK",
            "MARUTI",
            "SUNPHARMA",
            "TATAMOTORS",
            "ASIANPAINT",
            "BAJFINANCE",
            "HCLTECH",
            "TITAN",
            "ULTRACEMCO",
            "TECHM",
            "POWERGRID",
            "NTPC",
            "M&M",
            "HINDALCO",
            "ADANIENT",
            "TATASTEEL",
            "ONGC",
            "COALINDIA",
            "JSWSTEEL",
            "GRASIM",
            "BAJAJFINSV",
            "NESTLEIND",
            "BRITANNIA",
            "HINDZINC",
            "DIVISLAB",
            "EICHERMOT",
            "DRREDDY",
            "APOLLOHOSP",
            "CIPLA",
            "HEROMOTOCO",
            "TATACONSUM",
            "INDUSINDBK",
            "SHREECEM",
            "ADANIPORTS",
            "UPL",
            "BPCL",
            "SBILIFE",
            "BAJAJ-AUTO",
            "LTIM",
            "ITC",
        ]

    @staticmethod
    def _get_fallback_banknifty():
        """Fallback list of Bank Nifty constituents"""
        return [
            "HDFCBANK",
            "ICICIBANK",
            "SBIN",
            "KOTAKBANK",
            "AXISBANK",
            "INDUSINDBK",
            "BANDHANBNK",
            "FEDERALBNK",
            "IDFCFIRSTB",
            "PNB",
            "AUBANK",
            "BANKBARODA",
        ]

    @staticmethod
    def get_stocks_data_from_db(tickers, date=None):
        """
        Fetch stock data from database for given tickers
        Uses futures_oi_cache for underlying prices and TBL_{SYMBOL}_DERIVED for detailed data
        Args:
            tickers: List of stock symbols
            date: Date string (YYYY-MM-DD), defaults to latest date
        Returns: DataFrame with stock data
        """
        try:
            if not tickers:
                return pd.DataFrame()

            # If no date provided, get latest date from futures_oi_cache
            if not date:
                date_query = text(
                    """
                    SELECT DISTINCT cache_date
                    FROM public.futures_oi_cache
                    ORDER BY cache_date DESC
                    LIMIT 1
                """
                )
                with engine.connect() as conn:
                    result = conn.execute(date_query).fetchone()
                    date = str(result[0]) if result else None

            if not date:
                print("[ERROR] No data available in futures_oi_cache")
                return pd.DataFrame()

            # Fetch stock data from futures_oi_cache (has underlying_price and OI)
            placeholders = ",".join([f"'{ticker}'" for ticker in tickers])
            query = text(
                f"""
                SELECT DISTINCT ON (ticker)
                    ticker,
                    underlying_price as price,
                    0 as change,
                    0 as change_pct,
                    0 as volume,
                    underlying_price as open,
                    underlying_price as high,
                    underlying_price as low,
                    underlying_price as prev_close
                FROM public.futures_oi_cache
                WHERE cache_date = :date
                AND ticker IN ({placeholders})
                ORDER BY ticker, expiry_type
            """
            )

            df = pd.read_sql(query, engine, params={"date": date})

            # Try to get more detailed data from individual stock tables
            inspector = inspect(engine)
            all_tables = inspector.get_table_names(schema="public")

            for _, row in df.iterrows():
                ticker = row["ticker"]
                table_name = f"TBL_{ticker}_DERIVED"

                if table_name in all_tables:
                    try:
                        # Get recent price data from stock table
                        stock_query = text(
                            f"""
                            SELECT
                                "UndrlygPric" as price,
                                SUM(CAST("TtlTradgVol" AS BIGINT)) as volume
                            FROM public."{table_name}"
                            WHERE "BizDt"::date = CAST(:date AS date)
                            GROUP BY "UndrlygPric"
                            ORDER BY COUNT(*) DESC
                            LIMIT 1
                        """
                        )

                        stock_df = pd.read_sql(stock_query, engine, params={"date": date})

                        if not stock_df.empty:
                            df.loc[df["ticker"] == ticker, "volume"] = (
                                int(stock_df.iloc[0]["volume"]) if stock_df.iloc[0]["volume"] else 0
                            )

                            # Try to get previous day price for change calculation
                            prev_query = text(
                                f"""
                                SELECT "UndrlygPric" as prev_price
                                FROM public."{table_name}"
                                WHERE "BizDt"::date < CAST(:date AS date)
                                GROUP BY "UndrlygPric", "BizDt"
                                ORDER BY "BizDt" DESC
                                LIMIT 1
                            """
                            )

                            prev_df = pd.read_sql(prev_query, engine, params={"date": date})
                            if not prev_df.empty:
                                prev_price = float(prev_df.iloc[0]["prev_price"])
                                curr_price = float(row["price"])
                                change = curr_price - prev_price
                                change_pct = (change / prev_price * 100) if prev_price > 0 else 0

                                # Convert columns to float first to avoid dtype warning
                                df["change"] = df["change"].astype(float)
                                df["change_pct"] = df["change_pct"].astype(float)
                                df["prev_close"] = df["prev_close"].astype(float)

                                df.loc[df["ticker"] == ticker, "change"] = float(change)
                                df.loc[df["ticker"] == ticker, "change_pct"] = float(change_pct)
                                df.loc[df["ticker"] == ticker, "prev_close"] = float(prev_price)
                    except Exception as e:
                        print(f"[DEBUG] Could not get detailed data for {ticker}: {e}")
                        continue

            # Log missing stocks
            found_tickers = set(df["ticker"].tolist())
            missing_tickers = set(tickers) - found_tickers
            if missing_tickers:
                print(
                    f"[WARNING] Missing data in database for {len(missing_tickers)} stocks: {list(missing_tickers)[:10]}"
                )

            print(f"[INFO] Fetched data for {len(df)} stocks from database for date {date} (requested {len(tickers)})")
            return df

        except Exception as e:
            print(f"[ERROR] Failed to fetch stock data from database: {e}")
            import traceback

            traceback.print_exc()
            return pd.DataFrame()

    @staticmethod
    def get_stock_derivatives_data(tickers, date=None):
        """
        Fetch derivatives data (OI, IV) from database
        Uses futures_oi_cache and individual stock tables
        """
        try:
            if not tickers or not date:
                return pd.DataFrame()

            placeholders = ",".join([f"'{ticker}'" for ticker in tickers])

            # Get OI from futures_oi_cache
            query = text(
                f"""
                SELECT
                    ticker,
                    SUM(expiry_oi) as total_oi,
                    0 as avg_iv
                FROM public.futures_oi_cache
                WHERE cache_date = :date
                AND ticker IN ({placeholders})
                GROUP BY ticker
            """
            )

            df = pd.read_sql(query, engine, params={"date": date})

            # Try to get IV from individual stock tables
            inspector = inspect(engine)
            all_tables = inspector.get_table_names(schema="public")

            for _, row in df.iterrows():
                ticker = row["ticker"]
                table_name = f"TBL_{ticker}_DERIVED"

                if table_name in all_tables:
                    try:
                        iv_query = text(
                            f"""
                            SELECT AVG("iv") as avg_iv
                            FROM public."{table_name}"
                            WHERE "BizDt"::date = CAST(:date AS date)
                            AND "iv" IS NOT NULL
                            AND "iv" > 0
                        """
                        )

                        iv_df = pd.read_sql(iv_query, engine, params={"date": date})

                        if not iv_df.empty and iv_df.iloc[0]["avg_iv"]:
                            avg_iv = float(iv_df.iloc[0]["avg_iv"])
                            # Convert to percentage if needed
                            if avg_iv < 1:
                                avg_iv *= 100
                            # Convert column to float first to avoid dtype warning
                            df["avg_iv"] = df["avg_iv"].astype(float)
                            df.loc[df["ticker"] == ticker, "avg_iv"] = round(avg_iv, 1)
                    except Exception as e:
                        print(f"[DEBUG] Could not get IV for {ticker}: {e}")
                        continue

            return df

        except Exception as e:
            print(f"[ERROR] Failed to fetch derivatives data: {e}")
            return pd.DataFrame()

    @staticmethod
    def enrich_with_signals(df):
        """
        Add signal column based on price change
        """
        if df.empty:
            return df

        def get_signal(change_pct):
            if change_pct > 0.5:
                return "BULLISH"
            elif change_pct < -0.5:
                return "BEARISH"
            else:
                return "NEUTRAL"

        df["signal"] = df["change_pct"].apply(get_signal)
        return df


def get_nifty50_stocks_with_data(date=None):
    """
    Get Nifty 50 stocks with enriched data from database
    Returns: List of dicts with stock data
    """
    model = IndexConstituentsModel()

    # Get constituents from NSE
    tickers = model.get_nifty50_constituents()

    # Get stock data from database
    stocks_df = model.get_stocks_data_from_db(tickers, date)

    # Get date from query result if not provided
    if not date:
        date_query = text("SELECT DISTINCT cache_date FROM public.futures_oi_cache ORDER BY cache_date DESC LIMIT 1")
        with engine.connect() as conn:
            result = conn.execute(date_query).fetchone()
            date = str(result[0]) if result else None

    # Create dataframe with ALL 50 tickers (including missing ones)
    all_tickers_df = pd.DataFrame({"ticker": tickers})

    if stocks_df.empty:
        # No data available - create placeholder for all stocks
        stocks_df = all_tickers_df.copy()
        stocks_df["price"] = 0.0
        stocks_df["change"] = 0.0
        stocks_df["change_pct"] = 0.0
        stocks_df["volume"] = 0
        stocks_df["open"] = 0.0
        stocks_df["high"] = 0.0
        stocks_df["low"] = 0.0
        stocks_df["prev_close"] = 0.0
    else:
        # Merge to include missing stocks
        stocks_df = all_tickers_df.merge(stocks_df, on="ticker", how="left")
        # Fill NaN values for stocks without data - with proper dtypes
        stocks_df["price"] = stocks_df["price"].fillna(0.0).astype(float)
        stocks_df["change"] = stocks_df["change"].fillna(0.0).astype(float)
        stocks_df["change_pct"] = stocks_df["change_pct"].fillna(0.0).astype(float)
        stocks_df["volume"] = stocks_df["volume"].fillna(0).astype(int)
        stocks_df["open"] = stocks_df["open"].fillna(0.0).astype(float)
        stocks_df["high"] = stocks_df["high"].fillna(0.0).astype(float)
        stocks_df["low"] = stocks_df["low"].fillna(0.0).astype(float)
        stocks_df["prev_close"] = stocks_df["prev_close"].fillna(0.0).astype(float)

    derivatives_df = model.get_stock_derivatives_data(tickers, date)

    # Merge derivatives data
    if not derivatives_df.empty:
        stocks_df = stocks_df.merge(derivatives_df, on="ticker", how="left")
        stocks_df["oi"] = stocks_df["total_oi"].fillna(0).astype(int)
        stocks_df["iv"] = stocks_df["avg_iv"].fillna(0).round(1)
    else:
        stocks_df["oi"] = 0
        stocks_df["iv"] = 0.0

    # Add signals
    stocks_df = model.enrich_with_signals(stocks_df)

    # Convert to list of dicts
    stocks_df["volume"] = stocks_df["volume"].fillna(0).astype(int)
    stocks_df["price"] = stocks_df["price"].round(2)
    stocks_df["change"] = stocks_df["change"].round(2)
    stocks_df["change_pct"] = stocks_df["change_pct"].round(2)

    result = stocks_df.to_dict("records")

    # Log which stocks are missing data
    stocks_with_data = stocks_df[stocks_df["price"] > 0]["ticker"].tolist()
    stocks_without_data = stocks_df[stocks_df["price"] == 0]["ticker"].tolist()

    if stocks_without_data:
        print(f"[WARNING] {len(stocks_without_data)} Nifty 50 stocks have no data: {stocks_without_data}")

    print(
        f"[INFO] Prepared {len(result)} Nifty 50 stocks ({len(stocks_with_data)} with data, {len(stocks_without_data)} without data)"
    )
    return result


def get_banknifty_stocks_with_data(date=None):
    """
    Get Bank Nifty stocks with enriched data from database
    Returns: List of dicts with stock data
    """
    model = IndexConstituentsModel()

    # Get constituents from NSE
    tickers = model.get_banknifty_constituents()

    # Get stock data from database
    stocks_df = model.get_stocks_data_from_db(tickers, date)

    # Get date from query result if not provided
    if not date:
        date_query = text("SELECT DISTINCT cache_date FROM public.futures_oi_cache ORDER BY cache_date DESC LIMIT 1")
        with engine.connect() as conn:
            result = conn.execute(date_query).fetchone()
            date = str(result[0]) if result else None

    # Create dataframe with ALL Bank Nifty tickers (including missing ones)
    all_tickers_df = pd.DataFrame({"ticker": tickers})

    if stocks_df.empty:
        # No data available - create placeholder for all stocks
        stocks_df = all_tickers_df.copy()
        stocks_df["price"] = 0.0
        stocks_df["change"] = 0.0
        stocks_df["change_pct"] = 0.0
        stocks_df["volume"] = 0
        stocks_df["open"] = 0.0
        stocks_df["high"] = 0.0
        stocks_df["low"] = 0.0
        stocks_df["prev_close"] = 0.0
    else:
        # Merge to include missing stocks
        stocks_df = all_tickers_df.merge(stocks_df, on="ticker", how="left")
        # Fill NaN values for stocks without data - with proper dtypes
        stocks_df["price"] = stocks_df["price"].fillna(0.0).astype(float)
        stocks_df["change"] = stocks_df["change"].fillna(0.0).astype(float)
        stocks_df["change_pct"] = stocks_df["change_pct"].fillna(0.0).astype(float)
        stocks_df["volume"] = stocks_df["volume"].fillna(0).astype(int)
        stocks_df["open"] = stocks_df["open"].fillna(0.0).astype(float)
        stocks_df["high"] = stocks_df["high"].fillna(0.0).astype(float)
        stocks_df["low"] = stocks_df["low"].fillna(0.0).astype(float)
        stocks_df["prev_close"] = stocks_df["prev_close"].fillna(0.0).astype(float)

    derivatives_df = model.get_stock_derivatives_data(tickers, date)

    # Merge derivatives data
    if not derivatives_df.empty:
        stocks_df = stocks_df.merge(derivatives_df, on="ticker", how="left")
        stocks_df["oi"] = stocks_df["total_oi"].fillna(0).astype(int)
        stocks_df["iv"] = stocks_df["avg_iv"].fillna(0).round(1)
    else:
        stocks_df["oi"] = 0
        stocks_df["iv"] = 0.0

    # Add signals
    stocks_df = model.enrich_with_signals(stocks_df)

    # Convert to list of dicts
    stocks_df["volume"] = stocks_df["volume"].fillna(0).astype(int)
    stocks_df["price"] = stocks_df["price"].round(2)
    stocks_df["change"] = stocks_df["change"].round(2)
    stocks_df["change_pct"] = stocks_df["change_pct"].round(2)

    result = stocks_df.to_dict("records")

    # Log which stocks are missing data
    stocks_with_data = stocks_df[stocks_df["price"] > 0]["ticker"].tolist()
    stocks_without_data = stocks_df[stocks_df["price"] == 0]["ticker"].tolist()

    if stocks_without_data:
        print(f"[WARNING] {len(stocks_without_data)} Bank Nifty stocks have no data: {stocks_without_data}")

    print(
        f"[INFO] Prepared {len(result)} Bank Nifty stocks ({len(stocks_with_data)} with data, {len(stocks_without_data)} without data)"
    )
    return result
