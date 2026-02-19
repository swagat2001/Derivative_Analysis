"""
Fundamental Service — DB-driven implementation.
Loads fundamental metrics from stock_fundamentals table (populated by
Database/Cash/fetch_fundamentals.py via yfinance).
Market data (price, volume, change, signal) from daily_market_heatmap
and technical_screener_cache. Zero JSON file dependency.
"""

from sqlalchemy import text

from ..models.db_config import engine, engine_cash


class FundamentalService:
    _instance = None
    _fundamentals_data = {}  # symbol -> {market_cap, pe_ratio, sales, ...}
    _market_data = {}        # symbol -> {price, volume, change, change_pct, signal}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(FundamentalService, cls).__new__(cls)
            cls._instance._load_data()
        return cls._instance

    def _load_data(self):
        """Load fundamental + market data from DB into memory."""
        print("[INFO] Loading FundamentalService from DB...")
        self._load_fundamentals_from_db()
        self._load_market_data()
        print(f"[INFO] FundamentalService ready: {len(self._fundamentals_data)} fundamental stocks, "
              f"{len(self._market_data)} market records.")

    # ------------------------------------------------------------------
    # FUNDAMENTAL DATA — from stock_fundamentals table
    # ------------------------------------------------------------------

    def _load_fundamentals_from_db(self):
        """Load all rows from stock_fundamentals into memory dict."""
        try:
            with engine_cash.connect() as conn:
                result = conn.execute(text("""
                    SELECT symbol, market_cap, pe_ratio, book_value, eps,
                           sales, net_profit, opm_pct, sales_growth_3yr,
                           profit_growth_3yr, capex_growth_pct, working_capital_days
                    FROM stock_fundamentals
                """))
                rows = result.fetchall()

            self._fundamentals_data = {}
            for row in rows:
                self._fundamentals_data[row[0]] = {
                    "market_cap":           self._f(row[1]),
                    "pe_ratio":             self._f(row[2]),
                    "book_value":           self._f(row[3]),
                    "eps":                  self._f(row[4]),
                    "sales":                self._f(row[5]),
                    "net_profit":           self._f(row[6]),
                    "opm_pct":              self._f(row[7]),
                    "sales_growth_3yr":     self._f(row[8]),
                    "profit_growth_3yr":    self._f(row[9]),
                    "capex_growth_pct":     self._f(row[10]),
                    "working_capital_days": self._f(row[11]),
                }
            print(f"[INFO] Loaded {len(self._fundamentals_data)} rows from stock_fundamentals.")
        except Exception as e:
            print(f"[ERROR] stock_fundamentals load failed: {e}")
            print("[WARN] Run 'py Database/Cash/fetch_fundamentals.py' to populate the table.")
            self._fundamentals_data = {}

    # ------------------------------------------------------------------
    # MARKET DATA — from daily_market_heatmap + technical_screener_cache
    # ------------------------------------------------------------------

    def _load_market_data(self):
        """Load latest price, volume, change%, and RSI signal for all stocks."""
        self._market_data = {}

        # Step 1: Latest date from heatmap
        try:
            with engine_cash.connect() as conn:
                latest_date = conn.execute(
                    text("SELECT MAX(date) FROM daily_market_heatmap")
                ).scalar()
                if not latest_date:
                    print("[WARN] No data in daily_market_heatmap.")
                    return

                # Load price, volume, change% for all stocks on latest date
                result = conn.execute(text("""
                    SELECT symbol, close, prev_close, change_pct, volume, high, low
                    FROM daily_market_heatmap
                    WHERE date = :d
                """), {"d": latest_date})
                rows = result.fetchall()

            for row in rows:
                symbol, close, prev_close, change_pct, volume, high, low = row
                change = round(close - prev_close, 2) if close and prev_close else 0
                self._market_data[symbol] = {
                    "price":      self._f(close),
                    "prev_close": self._f(prev_close),
                    "change":     change,
                    "change_pct": self._f(change_pct),
                    "volume":     int(volume) if volume else 0,
                    "signal":     "NEUTRAL",  # Will be updated from RSI below
                }

            print(f"[INFO] Loaded {len(self._market_data)} market records for {latest_date}.")
        except Exception as e:
            print(f"[ERROR] Market data load failed: {e}")

        # Step 2: RSI signals from technical_screener_cache (F&O DB)
        try:
            with engine.connect() as conn:
                latest_cache = conn.execute(
                    text("SELECT MAX(cache_date) FROM technical_screener_cache")
                ).scalar()
                if latest_cache:
                    result = conn.execute(text("""
                        SELECT ticker, rsi_14
                        FROM technical_screener_cache
                        WHERE cache_date = :d
                    """), {"d": latest_cache})
                    rsi_rows = result.fetchall()
                    for ticker, rsi in rsi_rows:
                        rsi = rsi or 50
                        if ticker in self._market_data:
                            self._market_data[ticker]["signal"] = self._rsi_to_signal(rsi)
                        # Also try without suffix variants
                        for variant in [ticker.replace("_", "&"), ticker.replace("_", "-")]:
                            if variant in self._market_data:
                                self._market_data[variant]["signal"] = self._rsi_to_signal(rsi)
            print(f"[INFO] RSI signals loaded from technical_screener_cache ({latest_cache}).")
        except Exception as e:
            print(f"[WARN] RSI signal load skipped: {e}")

    # ------------------------------------------------------------------
    # PUBLIC API — used by the fundamental scanner controllers
    # ------------------------------------------------------------------

    def get_stock_fundamentals(self, ticker: str) -> dict:
        """Return consolidated fundamental + market data dict for one stock."""
        data = {
            "ticker":               ticker,
            "market_cap":           0,
            "pe":                   0,
            "eps":                  0,
            "sales":                0,
            "net_profit":           0,
            "opm":                  0,
            "sales_growth_3yr":     0,
            "profit_growth_3yr":    0,
            "capex_growth_pct":     0,
            "working_capital_days": 0,
            # Market columns
            "price":      0,
            "change":     0,
            "change_pct": 0,
            "volume":     0,
            "signal":     "NEUTRAL",
            # Legacy fields kept for controller compatibility
            "roce":              0,
            "roe":               0,
            "promoter_holding":  0,
            "fii_holding":       0,
            "cash_from_ops":     0,
            "cash_from_investing": 0,
        }

        # Fundamental data
        if ticker in self._fundamentals_data:
            f = self._fundamentals_data[ticker]
            data["market_cap"]           = f["market_cap"]
            data["pe"]                   = f["pe_ratio"]
            data["eps"]                  = f["eps"]
            data["sales"]                = f["sales"]
            data["net_profit"]           = f["net_profit"]
            data["opm"]                  = f["opm_pct"]
            data["sales_growth_3yr"]     = f["sales_growth_3yr"]
            data["profit_growth_3yr"]    = f["profit_growth_3yr"]
            data["capex_growth_pct"]     = f["capex_growth_pct"]
            data["working_capital_days"] = f["working_capital_days"]

        # Market data — try symbol variants for special cases like M&M / BAJAJ-AUTO
        m = self._find_market_data(ticker)
        if m:
            data["price"]      = m["price"]
            data["change"]     = m["change"]
            data["change_pct"] = m["change_pct"]
            data["volume"]     = m["volume"]
            data["signal"]     = m["signal"]

        return data

    def filter_stocks(self, criteria_func) -> list:
        """
        Apply a filter function to all stocks in stock_fundamentals table.
        Returns list of matching stock dicts.
        """
        results = []
        for ticker in self._fundamentals_data:
            try:
                stats = self.get_stock_fundamentals(ticker)
                if criteria_func(stats):
                    results.append(stats)
            except Exception:
                continue
        return results

    def reload(self):
        """Force full reload from DB. Call after running fetch_fundamentals.py."""
        print("[INFO] Reloading FundamentalService from DB...")
        self._fundamentals_data = {}
        self._market_data = {}
        self._load_data()

    # ------------------------------------------------------------------
    # HELPERS
    # ------------------------------------------------------------------

    @staticmethod
    def _f(val, default=0.0) -> float:
        """Safe float conversion."""
        if val is None:
            return default
        try:
            return float(val)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _rsi_to_signal(rsi: float) -> str:
        if rsi > 70:
            return "BEARISH (Reversal)"
        elif rsi < 30:
            return "BULLISH (Reversal)"
        elif rsi > 50:
            return "BULLISH"
        else:
            return "BEARISH"

    def _find_market_data(self, ticker: str) -> dict | None:
        """Try ticker and common NSE symbol variants."""
        for variant in [
            ticker,
            ticker.replace("&", "_"),
            ticker.replace("-", "_"),
            ticker.replace("_", "&"),
            ticker.replace("_", "-"),
        ]:
            if variant in self._market_data:
                return self._market_data[variant]
        return None


fundamental_service = FundamentalService()
