"""
FUNDAMENTAL DATA FETCHER  (yfinance → stock_fundamentals DB table)
================================================================================
Fetches Market Cap, P/E, Sales, Profit, CapEx Growth from Yahoo Finance for
all NSE stocks and stores them in stock_fundamentals (CashStocks_Database).

Uses direct psycopg2 connection — does NOT import the Flask app.

Run weekly to keep data fresh:
    py Database/Cash/fetch_fundamentals.py

Test with specific symbols:
    py Database/Cash/fetch_fundamentals.py --symbols RELIANCE TCS INFY
"""

import argparse
import os
import sys
import time
from datetime import datetime

import psycopg2
import psycopg2.extras
import yfinance as yf
from dotenv import load_dotenv

# Load env vars from .env in project root
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
load_dotenv(os.path.join(_ROOT, ".env"))

DB_USER     = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME_CASH", "CashStocks_Database")

INR_TO_CR = 1e7


# =============================================================================
# DB CONNECTION
# =============================================================================

def get_conn():
    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )


# =============================================================================
# TABLE SETUP
# =============================================================================

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_fundamentals (
    symbol               VARCHAR(50) PRIMARY KEY,
    market_cap           NUMERIC,
    pe_ratio             NUMERIC,
    book_value           NUMERIC,
    eps                  NUMERIC,
    sales                NUMERIC,
    net_profit           NUMERIC,
    opm_pct              NUMERIC,
    sales_growth_3yr     NUMERIC,
    profit_growth_3yr    NUMERIC,
    capex_growth_pct     NUMERIC,
    working_capital_days NUMERIC,
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

UPSERT_SQL = """
INSERT INTO stock_fundamentals
    (symbol, market_cap, pe_ratio, book_value, eps,
     sales, net_profit, opm_pct, sales_growth_3yr, profit_growth_3yr,
     capex_growth_pct, working_capital_days, updated_at)
VALUES
    (%(symbol)s, %(market_cap)s, %(pe_ratio)s, %(book_value)s, %(eps)s,
     %(sales)s, %(net_profit)s, %(opm_pct)s, %(sales_growth_3yr)s, %(profit_growth_3yr)s,
     %(capex_growth_pct)s, %(working_capital_days)s, %(updated_at)s)
ON CONFLICT (symbol) DO UPDATE SET
    market_cap           = EXCLUDED.market_cap,
    pe_ratio             = EXCLUDED.pe_ratio,
    book_value           = EXCLUDED.book_value,
    eps                  = EXCLUDED.eps,
    sales                = EXCLUDED.sales,
    net_profit           = EXCLUDED.net_profit,
    opm_pct              = EXCLUDED.opm_pct,
    sales_growth_3yr     = EXCLUDED.sales_growth_3yr,
    profit_growth_3yr    = EXCLUDED.profit_growth_3yr,
    capex_growth_pct     = EXCLUDED.capex_growth_pct,
    working_capital_days = EXCLUDED.working_capital_days,
    updated_at           = EXCLUDED.updated_at;
"""


def create_table():
    conn = get_conn()
    conn.autocommit = True
    try:
        cur = conn.cursor()
        cur.execute(CREATE_TABLE_SQL)
        cur.execute("SELECT COUNT(*) FROM stock_fundamentals")
        cnt = cur.fetchone()[0]
        print(f"[INFO] Table ready in '{DB_NAME}'. Current rows: {cnt}")
        cur.close()
        return True
    except Exception as e:
        print(f"[ERROR] Table creation: {e}")
        return False
    finally:
        conn.close()


# =============================================================================
# SYMBOL LIST
# =============================================================================

def get_nse_symbols() -> list:
    """
    Get NSE equity stock symbols from daily_market_heatmap.
    Filters out non-equity instruments:
    - Government securities / G-Secs (e.g. 1018GS2026, 515GS2025)
    - Rights Entitlements (e.g. 3IINFO_RE)
    - Symbols starting with digits (bonds, debentures)
    - Symbols containing % (sovereign gold bonds, etc.)
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT symbol FROM daily_market_heatmap
            WHERE LENGTH(symbol) <= 15
              AND symbol ~ '^[A-Z]'           -- must start with a letter
              AND symbol NOT LIKE '%GS20%'    -- exclude G-Secs (govt securities)
              AND symbol NOT LIKE '%GS19%'    -- older G-Secs
              AND symbol NOT LIKE '%_RE'      -- exclude Rights Entitlements
              AND symbol NOT SIMILAR TO '%_RE[0-9]%'  -- numbered RE like _RE1, _RE2
              AND symbol NOT LIKE '%-RE'      -- exclude Rights (alternate suffix)
              AND symbol NOT LIKE '%SGB%'     -- exclude Sovereign Gold Bonds
              AND symbol NOT LIKE '%PCT%'     -- exclude bond instruments
              AND symbol NOT LIKE '%NCD%'     -- exclude NCDs
              AND symbol NOT LIKE '%.%'       -- exclude dotted instrument names
            ORDER BY symbol
        """)
        symbols = [r[0] for r in cur.fetchall()]
        cur.close()
        print(f"[INFO] Found {len(symbols)} NSE equity symbols (filtered from heatmap).")
        return symbols
    except Exception as e:
        print(f"[ERROR] Symbol fetch: {e}")
        return []
    finally:
        conn.close()


# =============================================================================
# HELPERS
# =============================================================================

def _f(val):
    """Safe float, returns None if NaN/None/non-numeric."""
    if val is None:
        return None
    try:
        v = float(val)
        return None if v != v else v  # NaN check
    except (TypeError, ValueError):
        return None


# =============================================================================
# YFINANCE FETCHER
# =============================================================================

def fetch_one(symbol: str) -> dict | None:
    yf_sym = f"{symbol}.NS"
    try:
        t = yf.Ticker(yf_sym)
        info = t.info or {}

        # Basic validity check
        if not info.get("marketCap") and not info.get("trailingPE"):
            return None

        # --- Info ---
        mc_inr = _f(info.get("marketCap"))
        market_cap_cr = round(mc_inr / INR_TO_CR, 2) if mc_inr else None
        pe_ratio   = _f(info.get("trailingPE"))
        book_value = _f(info.get("bookValue"))
        eps        = _f(info.get("trailingEps"))

        # --- Financials (annual income statement) ---
        sales_cr = net_profit_cr = opm_pct = sales_growth_3yr = profit_growth_3yr = None
        try:
            fin = t.financials
            if fin is not None and not fin.empty:
                rev_row = None
                for key in ["Total Revenue", "Operating Revenue"]:
                    if key in fin.index:
                        rev_row = fin.loc[key].dropna().values
                        break

                if rev_row is not None and len(rev_row) >= 1:
                    sales_cr = round(float(rev_row[0]) / INR_TO_CR, 2)
                    if len(rev_row) >= 4 and rev_row[3] and float(rev_row[3]) > 0:
                        sales_growth_3yr = round(
                            ((float(rev_row[0]) / float(rev_row[3])) ** (1 / 3) - 1) * 100, 2
                        )

                for key in ["Net Income", "Net Income Common Stockholders"]:
                    if key in fin.index:
                        ni = fin.loc[key].dropna().values
                        if len(ni) >= 1:
                            net_profit_cr = round(float(ni[0]) / INR_TO_CR, 2)
                        if len(ni) >= 4 and ni[3] and float(ni[3]) != 0 and float(ni[0]) / float(ni[3]) > 0:
                            profit_growth_3yr = round(
                                ((float(ni[0]) / float(ni[3])) ** (1 / 3) - 1) * 100, 2
                            )
                        break

                for op_key in ["Operating Income", "EBIT"]:
                    if op_key in fin.index and rev_row is not None and len(rev_row) >= 1:
                        op = fin.loc[op_key].dropna().values
                        if len(op) >= 1 and rev_row[0] and float(rev_row[0]) != 0:
                            opm_pct = round((float(op[0]) / float(rev_row[0])) * 100, 2)
                        break
        except Exception:
            pass

        # --- Cash Flow (CapEx Growth) ---
        capex_growth_pct = None
        try:
            cf = t.cashflow
            if cf is not None and not cf.empty and "Capital Expenditure" in cf.index:
                row = cf.loc["Capital Expenditure"].dropna().values
                if len(row) >= 2:
                    c1, c2 = abs(float(row[0])), abs(float(row[1]))
                    if c2 > 0:
                        capex_growth_pct = round(((c1 - c2) / c2) * 100, 2)
                    elif c1 > 0:
                        capex_growth_pct = 100.0
        except Exception:
            pass

        # --- Balance Sheet (Working Capital Days) ---
        working_capital_days = None
        try:
            bs = t.balance_sheet
            if bs is not None and not bs.empty and sales_cr and sales_cr > 0:
                ca, cl = None, None
                for key in ["Current Assets", "Total Current Assets"]:
                    if key in bs.index:
                        ca = _f(bs.loc[key].dropna().values[0])
                        break
                for key in ["Current Liabilities", "Total Current Liabilities"]:
                    if key in bs.index:
                        cl = _f(bs.loc[key].dropna().values[0])
                        break
                if ca is not None and cl is not None:
                    sales_inr = sales_cr * INR_TO_CR
                    working_capital_days = round(((ca - cl) / (sales_inr / 365)), 1)
        except Exception:
            pass

        return {
            "symbol":               symbol,
            "market_cap":           market_cap_cr,
            "pe_ratio":             pe_ratio,
            "book_value":           book_value,
            "eps":                  eps,
            "sales":                sales_cr,
            "net_profit":           net_profit_cr,
            "opm_pct":              opm_pct,
            "sales_growth_3yr":     sales_growth_3yr,
            "profit_growth_3yr":    profit_growth_3yr,
            "capex_growth_pct":     capex_growth_pct,
            "working_capital_days": working_capital_days,
            "updated_at":           datetime.now(),
        }

    except Exception as e:
        print(f"  [WARN] {symbol}: {e}")
        return None


# =============================================================================
# MAIN
# =============================================================================

def fetch_all(symbols: list, batch_size: int = 50, delay: float = 0.3):
    total = len(symbols)
    ok = fail = 0
    batch = []
    start = time.time()

    print(f"[INFO] Fetching {total} symbols (delay={delay}s)...")

    for i, symbol in enumerate(symbols, 1):
        row = fetch_one(symbol)
        if row:
            batch.append(row)
            ok += 1
        else:
            fail += 1

        if len(batch) >= batch_size or i == total:
            if batch:
                conn = get_conn()
                try:
                    cur = conn.cursor()
                    psycopg2.extras.execute_batch(cur, UPSERT_SQL, batch)
                    conn.commit()
                    print(f"  [SAVE] {len(batch)} rows → DB | {i}/{total} done")
                except Exception as e:
                    conn.rollback()
                    print(f"  [ERROR] Batch upsert failed: {e}")
                finally:
                    cur.close()
                    conn.close()
                    batch = []

        if i % 100 == 0:
            elapsed = time.time() - start
            eta = (elapsed / i) * (total - i)
            print(f"  [PROGRESS] {i}/{total} | ok={ok} fail={fail} | ETA ~{eta:.0f}s")

        time.sleep(delay)

    elapsed = time.time() - start
    print(f"\n[DONE] {ok}/{total} fetched in {elapsed:.0f}s. Failed: {fail}.")


def verify():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM stock_fundamentals")
        cnt = cur.fetchone()[0]
        cur.execute("SELECT MAX(updated_at) FROM stock_fundamentals")
        ts = cur.fetchone()[0]
        print(f"\n[VERIFY] {cnt:,} rows in stock_fundamentals | last: {ts}")

        cur.execute("""
            SELECT symbol, ROUND(market_cap::numeric,0), ROUND(pe_ratio::numeric,1),
                   ROUND(sales::numeric,0), ROUND(net_profit::numeric,0),
                   ROUND(capex_growth_pct::numeric,1), ROUND(profit_growth_3yr::numeric,1)
            FROM stock_fundamentals
            ORDER BY updated_at DESC LIMIT 8
        """)
        hdr = f"  {'Symbol':<15} {'MktCap':>12} {'PE':>7} {'Sales':>12} {'Profit':>12} {'CapEx%':>9} {'P3yr%':>8}"
        print(hdr)
        print("  " + "-" * 78)
        for r in cur.fetchall():
            sym, mc, pe, s, np_, cx, p3 = r
            print(f"  {str(sym):<15} {str(mc or '-'):>12} {str(pe or '-'):>7} "
                  f"{str(s or '-'):>12} {str(np_ or '-'):>12} "
                  f"{str(cx or '-'):>9} {str(p3 or '-'):>8}")
        cur.close()
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Populate stock_fundamentals from Yahoo Finance (yfinance)"
    )
    parser.add_argument("--symbols", nargs="*",
                        help="Symbols to fetch (default: all NSE stocks from heatmap)")
    parser.add_argument("--delay", type=float, default=0.3,
                        help="Delay between requests in seconds (default: 0.3)")
    parser.add_argument("--verify-only", action="store_true",
                        help="Show current DB contents, skip fetching")
    args = parser.parse_args()

    if not create_table():
        sys.exit(1)

    if args.verify_only:
        verify()
    else:
        targets = args.symbols if args.symbols else get_nse_symbols()
        if not targets:
            print("[ERROR] No symbols to fetch.")
            sys.exit(1)
        fetch_all(targets, delay=args.delay)
        verify()
