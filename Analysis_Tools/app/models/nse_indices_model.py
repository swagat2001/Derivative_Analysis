"""
NSE Indices Model
Fetches live index prices and 1D intraday chart data from NSE's public API.
Used as a permanent data source so charts & prices always display — even outside
Upstox streamer hours (9:10–15:35 IST).
"""

import time
import threading
import requests

# ── NSE API endpoints ─────────────────────────────────────────────────────────
_NSE_HOME      = "https://www.nseindia.com/"
_GRAPH_API     = (
    "https://www.nseindia.com/api/NextApi/apiClient"
    "?functionName=getGraphChart&type={}&flag=1D"
)
_INDEX_API     = (
    "https://www.nseindia.com/api/NextApi/apiClient"
    "?functionName=getIndexData&type=All"
)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.nseindia.com/",
    "Connection":      "keep-alive",
}

# ── App-key ↔ NSE display-name mapping ───────────────────────────────────────
# Keys match the rest of the app (live_indices_model.py / home_live_updates.js)
# Names are verified from the live NSE API (indexName field)
NSE_NAME_TO_KEY: dict[str, str] = {
    "NIFTY 50":                  "nifty50",
    "NIFTY BANK":                "banknifty",
    "NIFTY FINANCIAL SERVICES":  "niftyfin",
    "NIFTY NEXT 50":             "niftynext50",
    "NIFTY 100":                 "nifty100",
    "India VIX":                 "indiavix",
    "NIFTY FIN SERVICE":         "niftyfin",   # alternate NSE name
    "SENSEX":                    "sensex",     # may appear if BSE data included
}

# App key → NSE URL parameter used in the graph API
KEY_TO_NSE_URL: dict[str, str] = {
    "nifty50":    "NIFTY%2050",
    "banknifty":  "NIFTY%20BANK",
    "sensex":     "SENSEX",
    "niftyfin":   "NIFTY%20FINANCIAL%20SERVICES",
    "niftynext50":"NIFTY%20NEXT%2050",
    "nifty100":   "NIFTY%20100",
    "indiavix":   "India%20VIX",
}

# ── Simple in-process cache ───────────────────────────────────────────────────
_PRICE_CACHE_TTL   = 10   # seconds  – index price cards (near-real-time)
_CHART_CACHE_TTL   = 60   # seconds  – chart series (minute-level NSE data)
_SESSION_TTL       = 300  # seconds  – reuse one session to keep cookies fresh

_price_cache: dict = {"data": None, "ts": 0.0}
_chart_cache: dict[str, dict] = {}   # key → {"data": ..., "ts": float}

_session_lock   = threading.Lock()
_price_lock     = threading.Lock()
_chart_lock     = threading.Lock()

_session: requests.Session | None = None
_session_ts: float = 0.0


# ── Session management ────────────────────────────────────────────────────────

def _get_session() -> requests.Session:
    """Return a cached requests.Session with NSE cookies, refreshing if stale."""
    global _session, _session_ts
    now = time.time()
    with _session_lock:
        if _session is None or (now - _session_ts) > _SESSION_TTL:
            s = requests.Session()
            s.headers.update(_HEADERS)
            try:
                s.get(_NSE_HOME, timeout=8)          # seed cookies
            except Exception:
                pass                                  # proceed even if seed fails
            _session    = s
            _session_ts = now
        return _session


# ── Public: index prices ──────────────────────────────────────────────────────

def get_nse_index_data() -> dict:
    """
    Fetch current index prices from NSE.
    Returns a dict keyed by app-key (e.g. 'nifty50') with fields:
        value, change, percentChange, open, high, low, previousClose
    Falls back gracefully to an empty dict on any error.
    """
    now = time.time()
    with _price_lock:
        if _price_cache["data"] and (now - _price_cache["ts"]) < _PRICE_CACHE_TTL:
            return _price_cache["data"]

    try:
        session  = _get_session()
        response = session.get(_INDEX_API, timeout=8)
        response.raise_for_status()
        raw_list = response.json().get("data", [])
    except Exception as exc:
        print(f"[NSE] index-data fetch failed: {exc}")
        return _price_cache.get("data") or {}

    result: dict = {}
    for item in raw_list:
        # NSE uses 'indexName' as the key (verified from live API)
        name = item.get("indexName", "")
        key  = NSE_NAME_TO_KEY.get(name)
        if not key:
            continue
        try:
            ltp       = float(item.get("last", 0) or 0)             # LTP
            prev      = float(item.get("previousClose", ltp) or ltp) # prev close
            open_p    = float(item.get("open",  ltp) or ltp)
            high_p    = float(item.get("high",  ltp) or ltp)
            low_p     = float(item.get("low",   ltp) or ltp)
            pct       = float(item.get("percChange", 0) or 0)        # NSE field
            chg       = round(ltp - prev, 2)                          # compute absolute

            result[key] = {
                "value":         round(ltp,   2),
                "change":        chg,
                "percentChange": round(pct,   2),
                "open":          round(open_p, 2),
                "high":          round(high_p, 2),
                "low":           round(low_p,  2),
                "previousClose": round(prev,   2),
            }
        except (TypeError, ValueError):
            continue

    with _price_lock:
        if result:                                # only update cache on success
            _price_cache["data"] = result
            _price_cache["ts"]   = now

    return result or (_price_cache.get("data") or {})


# ── Public: 1-day chart series ────────────────────────────────────────────────

def get_nse_chart_data(index_key: str) -> dict:
    """
    Fetch 1D intraday chart data for *index_key* (e.g. 'nifty50').
    Returns:
        {
          "series":  [[timestamp_ms, price], ...],   # x-axis: epoch-ms
          "open":    float,
          "high":    float,
          "low":     float,
          "close":   float,
          "change":  float,
          "percent": float,
        }
    Returns an empty dict with an "error" key on failure.
    """
    now = time.time()
    with _chart_lock:
        cached = _chart_cache.get(index_key)
        if cached and (now - cached["ts"]) < _CHART_CACHE_TTL:
            return cached["data"]

    nse_param = KEY_TO_NSE_URL.get(index_key)
    if not nse_param:
        return {"error": f"Unknown index key: {index_key}"}

    url = _GRAPH_API.format(nse_param)

    try:
        session  = _get_session()
        response = session.get(url, timeout=10)
        response.raise_for_status()
        payload  = response.json()
    except Exception as exc:
        print(f"[NSE] chart fetch failed for {index_key}: {exc}")
        # Return stale cache if available
        with _chart_lock:
            stale = _chart_cache.get(index_key)
        return stale["data"] if stale else {"error": str(exc)}

    try:
        raw = payload["data"]["grapthData"]   # NSE typo is intentional
    except (KeyError, TypeError) as exc:
        print(f"[NSE] unexpected chart payload for {index_key}: {exc}")
        return {"error": "Unexpected NSE chart payload"}

    # Deduplicate on timestamp, keep only "NM" (normal-market) points
    unique: dict = {}
    for r in raw:
        try:
            ts_ms, price, status = r[0], r[1], r[2]
        except (IndexError, TypeError):
            continue
        if status == "NM":
            unique[ts_ms] = (ts_ms, price)

    if not unique:
        return {"error": "No NM data points in NSE chart"}

    series = sorted(unique.values(), key=lambda x: x[0])  # sort by time
    prices = [p for _, p in series]

    result = {
        "series":  [[ts, p] for ts, p in series],
        "open":    round(prices[0],    2),
        "high":    round(max(prices),  2),
        "low":     round(min(prices),  2),
        "close":   round(prices[-1],   2),
        "change":  round(prices[-1] - prices[0], 2),
        "percent": round(((prices[-1] - prices[0]) / prices[0]) * 100, 2)
                   if prices[0] != 0 else 0.0,
    }

    with _chart_lock:
        _chart_cache[index_key] = {"data": result, "ts": now}

    return result
