# controllers/stock_controller.py
import json
import os
from pathlib import Path

import pandas as pd
from flask import Blueprint, jsonify, redirect, render_template, request, url_for
from sqlalchemy import text

from ..controllers.dashboard_controller import get_live_indices
from ..models.stock_model import (
    generate_oi_chart,
    get_all_tickers,
    get_available_dates,
    get_filtered_tickers,
    get_stock_chart_data,
    get_stock_detail_data,
    get_stock_expiry_data,
    get_stock_stats,
)
from ..models.cash_stock_model import (
    is_fo_stock,
    get_cash_stock_info,
    get_stock_scanner_appearances,
)
from ..models.db_config import engine_cash
from ..services.fundamental_service import fundamental_service

stock_bp = Blueprint("stock", __name__)

# ==============================
#  Fundamental Data Functions
# ==============================
# Path to Data_scraper data directory
FUNDAMENTAL_DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent / "Data_scraper" / "data"


def load_fundamental_json(symbol, data_type):
    """Load JSON data for a symbol from fundamental data directory"""
    file_path = FUNDAMENTAL_DATA_DIR / data_type / f"{symbol}.json"
    if file_path.exists():
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def load_fundamental_csv(symbol, data_type):
    """Load CSV data for a symbol from fundamental data directory"""
    file_path = FUNDAMENTAL_DATA_DIR / data_type / f"{symbol}.csv"
    if file_path.exists():
        return pd.read_csv(file_path)
    return None


def get_52w_and_date(symbol):
    """
    Fetch 52W high/low and cache_date from technical_screener_cache.
    Returns dict with high_52w, low_52w, price_date — or empty dict.
    """
    result = {}
    try:
        with engine_cash.connect() as conn:
            row = conn.execute(text("""
                SELECT week52_high, week52_low, cache_date
                FROM public.technical_screener_cache
                WHERE ticker = :ticker
                ORDER BY cache_date DESC
                LIMIT 1
            """), {"ticker": symbol}).fetchone()
        if row:
            result = {
                "high_52w":   float(row[0] or 0),
                "low_52w":    float(row[1] or 0),
                "price_date": str(row[2]) if row[2] else "",
            }
    except Exception as e:
        print(f"[WARN] get_52w_and_date({symbol}): {e}")
    return result


def get_db_price_chart(symbol, limit=1500):
    """
    Fetch OHLCV history from CashStocks_Database TBL_<symbol> for the price chart.
    Returns list of {date, value (close), volume} dicts, oldest-first.
    Falls back to scraped price CSV if not found in DB.
    """
    cash_table = f"TBL_{symbol}"
    try:
        with engine_cash.connect() as conn:
            exists = conn.execute(text("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_name = :tname
                )
            """), {"tname": cash_table}).scalar()

        if exists:
            with engine_cash.connect() as conn:
                rows = conn.execute(text(f"""
                    SELECT
                        "BizDt"::text      AS date,
                        "ClsPric"::float   AS close,
                        "TtlTradgVol"::bigint AS volume
                    FROM public."{cash_table}"
                    WHERE "ClsPric" IS NOT NULL
                    ORDER BY "BizDt" DESC
                    LIMIT :lim
                """), {"lim": limit}).fetchall()

            return [
                {
                    "date":   r[0],
                    "value":  float(r[1] or 0),
                    "volume": int(r[2] or 0) if r[2] else 0,
                }
                for r in reversed(rows)  # oldest first for chart
            ]
    except Exception as e:
        print(f"[WARN] get_db_price_chart({symbol}): {e}")

    # Fallback: scraped price CSV
    price_data = load_fundamental_csv(symbol, "price")
    if price_data is not None and not price_data.empty:
        price_df = price_data[price_data["metric"] == "Price"].copy()
        price_df["value"] = pd.to_numeric(price_df["value"], errors="coerce")
        # No real volume in scraped CSV — set to 0 so chart can show N/A
        price_df["volume"] = 0
        return price_df[["date", "value", "volume"]].to_dict("records")

    return []


def calculate_fundamental_metrics(symbol):
    """Build metrics dict entirely from DB sources — no JSON file dependency."""
    metrics = {
        "current_price": 0,
        "price_change":  0,
        "high_52w":      0,
        "low_52w":       0,
        "volume":        0,
        "price_date":    "",
        "pe_ratio":      0,
        "market_cap":    0,
        "book_value":    0,
        "eps":           0,
        "sales":         0,
        "net_profit":    0,
        "volume_display": "",
    }

    # ── 1. FundamentalService (same source as goldmine scanner) ───────────
    # Covers: price, volume, change%, pe_ratio, market_cap, eps, book_value
    # Price comes from daily_market_heatmap → works for ALL stocks incl. USASEEDS
    fs = fundamental_service.get_stock_fundamentals(symbol)
    if fs:
        metrics["current_price"] = fs.get("price", 0) or 0
        metrics["price_change"]  = fs.get("change_pct", 0) or 0
        metrics["volume"]        = fs.get("volume", 0) or 0
        metrics["pe_ratio"]      = fs.get("pe", 0) or 0
        metrics["market_cap"]    = fs.get("market_cap", 0) or 0
        metrics["eps"]           = fs.get("eps", 0) or 0
        metrics["book_value"]    = fs.get("book_value", 0) or 0
        metrics["sales"]         = fs.get("sales", 0) or 0
        metrics["net_profit"]    = fs.get("net_profit", 0) or 0

    # ── 2. technical_screener_cache → 52W High/Low + price date ──────────
    # FundamentalService doesn't track 52W range; we get it separately.
    w52 = get_52w_and_date(symbol)
    if w52:
        metrics["high_52w"]   = w52.get("high_52w", 0)
        metrics["low_52w"]    = w52.get("low_52w", 0)
        metrics["price_date"] = w52.get("price_date", "")

    # ── 3. Volume display format ──────────────────────────────────────────
    vol = metrics["volume"]
    if vol >= 10_000_000:
        metrics["volume_display"] = f"{vol / 10_000_000:.2f} Cr"
    elif vol >= 100_000:
        metrics["volume_display"] = f"{vol / 100_000:.2f} L"
    elif vol >= 1_000:
        metrics["volume_display"] = f"{vol / 1_000:.1f}K"
    elif vol > 0:
        metrics["volume_display"] = str(int(vol))

    return metrics


def get_available_fundamental_stocks():
    """Get list of stocks that have fundamental data available"""
    quarterly_dir = FUNDAMENTAL_DATA_DIR / "quarterly"
    if quarterly_dir.exists():
        return sorted([f.stem for f in quarterly_dir.glob("*.json")])
    return []


@stock_bp.route("/stock/<ticker>")
def stock_detail(ticker):
    """
    Stock detail page — routes to F&O page or cash stock page depending on the ticker.
    F&O stocks  → option chain, greeks, OI charts (existing page)
    Cash stocks → OHLCV, technicals, scanner appearances (new page)
    Query params: date, expiry
    """
    ticker = ticker.upper().strip()

    #  Route cash stocks to their own page
    if not is_fo_stock(ticker):
        cash_info      = get_cash_stock_info(ticker)
        scanner_hits   = get_stock_scanner_appearances(ticker)
        import json as _json
        ohlcv_json = _json.dumps(cash_info.get("ohlcv", []))
        return render_template(
            "cash_stock_detail.html",
            ticker         = ticker,
            stock_symbol   = ticker,
            cash_info      = cash_info,
            scanner_hits   = scanner_hits,
            ohlcv_json     = ohlcv_json,
            indices        = get_live_indices(),
            stock_list     = get_filtered_tickers(),
        )
    #  F&O path continues below

    # ==============================
    #  Fetch all available dates and symbols
    # ==============================
    dates = get_available_dates()
    all_symbols = get_filtered_tickers()  #  Filter by Excel list

    # ==============================
    #  Determine selected date and expiry
    # ==============================
    selected_date = request.args.get("date", dates[0] if dates else None)
    selected_expiry = request.args.get("expiry", None)

    data = []
    expiry_data = []
    stats = {}

    if selected_date:
        # ==============================
        #  Expiry data for left panel
        # ==============================
        expiry_data = get_stock_expiry_data(ticker, selected_date)

        # Auto-select first expiry if none chosen
        if not selected_expiry and expiry_data and len(expiry_data) > 0:
            selected_expiry = expiry_data[0]["expiry"]

        # Fetch option chain & summary stats
        data = get_stock_detail_data(ticker, selected_date, selected_expiry)
        stats = get_stock_stats(ticker, selected_date, selected_expiry)

        # ==============================
        #  Detect Underlying Price & Futures Price
        # ==============================
        underlying = None
        futures_price = None

        if data:
            for row in data:
                if row.get("UndrlygPric"):
                    underlying = row["UndrlygPric"]
                    break
                elif row.get("UnderlyingValue"):
                    underlying = row["UnderlyingValue"]
                    break
                elif row.get("underlying"):
                    underlying = row["underlying"]
                    break

        # Get futures price for selected expiry from expiry_data
        if selected_expiry and expiry_data:
            for exp_row in expiry_data:
                if exp_row.get("expiry") == selected_expiry:
                    futures_price = exp_row.get("price")
                    break

        # Fallback to underlying if futures price not found
        if futures_price is None:
            futures_price = underlying

        # Safely clean underlying and futures price strings (e.g. "5,298.00 ")
        if underlying:
            try:
                underlying = float(str(underlying).replace(",", "").strip())
            except Exception:
                underlying = None

        if futures_price:
            try:
                futures_price = float(str(futures_price).replace(",", "").strip())
            except Exception:
                futures_price = underlying

        # ==============================
        #  Find ATM Strike (Closest to Underlying)
        # ==============================
        atm = None
        if data and underlying:
            try:
                # FIX #6: Add error handling for None/NaN values
                strike_prices = sorted(
                    {
                        float(row["StrkPric"])
                        for row in data
                        if row.get("StrkPric") is not None and pd.notna(row.get("StrkPric"))
                    }
                )
                if strike_prices:
                    # Find strike with smallest distance from underlying
                    atm = min(strike_prices, key=lambda x: abs(x - underlying))
            except (ValueError, TypeError) as e:
                print(f"[WARNING] Error calculating ATM strike: {e}")
                atm = None

        # ==============================
        #  Compute Average IV
        # ==============================
        # FIX #5: Only override avg_iv if stats doesn't have it or if we have better data from option chain
        if data:
            iv_values = [row.get("IV") for row in data if row.get("IV") is not None and row["IV"] > 0]
            if iv_values:
                avg_iv = sum(iv_values) / len(iv_values)
                if avg_iv < 1:
                    avg_iv *= 100
                # Only override if stats doesn't have avg_iv or if our calculation is more accurate
                if "avg_iv" not in stats or stats.get("avg_iv", 0) == 0:
                    stats["avg_iv"] = round(avg_iv, 2)
            elif "avg_iv" not in stats:
                stats["avg_iv"] = 0
        elif "avg_iv" not in stats:
            stats["avg_iv"] = 0

    # ==============================
    #  Generate OI Chart Data
    # ==============================
    chart_data = None
    if selected_date and selected_expiry:
        # Pass data and expiry_data to avoid redundant queries
        oi_chart_dict = generate_oi_chart(ticker, selected_date, selected_expiry, data=data, expiry_data=expiry_data)
        if oi_chart_dict:
            chart_data = json.dumps(oi_chart_dict)

    # ==============================
    #  Render Template
    # ==============================
    return render_template(
        "stock_detail.html",
        ticker=ticker,
        stock_symbol=ticker,  # For header navigation
        all_symbols=all_symbols,
        data=data,
        expiry_data=expiry_data,
        stats=stats,
        dates=dates,
        selected_date=selected_date,
        selected_expiry=selected_expiry,
        chart_data=chart_data,
        underlying=underlying,  #  spot price (for display)
        futures_price=futures_price,  #  futures price for selected expiry (for Fair Price calculation)
        atm=atm,  #  correct closest strike
        indices=get_live_indices(),  #  Add indices for header
        stock_list=get_filtered_tickers(),  #  Add stock list for search
    )


# ==============================
#  API endpoint for mini stock chart (price data)
# ==============================
@stock_bp.route("/api/stock-chart/<ticker>")
def api_stock_chart(ticker):
    days = int(request.args.get("days", 90))
    try:
        chart_data = get_stock_chart_data(ticker, days)
        return jsonify({"success": True, "data": chart_data})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ==============================
#  Fundamental Analysis Page
# ==============================
@stock_bp.route("/stock/<ticker>/fundamental")
def stock_fundamental(ticker):
    """
    Fundamental analysis page - displays financial data from Data_scraper
    """
    try:
        # Load all fundamental data
        quarterly = load_fundamental_json(ticker, "quarterly")
        pnl = load_fundamental_json(ticker, "pnl")
        balance_sheet = load_fundamental_json(ticker, "balance_sheet")
        cashflow = load_fundamental_json(ticker, "cashflow")
        ratios = load_fundamental_json(ticker, "ratios")
        shareholding = load_fundamental_json(ticker, "shareholding")

        # Calculate metrics (DB-first, scrape-supplement)
        metrics = calculate_fundamental_metrics(ticker)

        # Price chart data: from DB (OHLCV) — includes real volume
        chart_data = get_db_price_chart(ticker)

        return render_template(
            "fundamental.html",
            symbol=ticker,
            quarterly=quarterly,
            pnl=pnl,
            balance_sheet=balance_sheet,
            cashflow=cashflow,
            ratios=ratios,
            shareholding=shareholding,
            chart_data=chart_data,
            metrics=metrics,
            indices=get_live_indices(),
            stock_list=get_filtered_tickers(),
            available_stocks=get_available_fundamental_stocks(),
        )
    except Exception as e:
        print(f"[ERROR] Fundamental page failed for {ticker}: {e}")
        import traceback

        traceback.print_exc()
        return render_template(
            "fundamental.html",
            symbol=ticker,
            quarterly=None,
            pnl=None,
            balance_sheet=None,
            cashflow=None,
            ratios=None,
            shareholding=None,
            chart_data=[],
            metrics={
                "current_price": 0,
                "price_change":  0,
                "high_52w":      0,
                "low_52w":       0,
                "volume":        0,
                "eps":           0,
                "sales":         0,
                "net_profit":    0,
            },
            indices=get_live_indices(),
            stock_list=get_filtered_tickers(),
            available_stocks=get_available_fundamental_stocks(),
        )
