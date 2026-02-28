import html
import numpy as np
import pandas as pd
from sqlalchemy import text
from functools import lru_cache
from .db_config import engine_cash
from .index_model import get_index_stocks, INDEX_METADATA

REVERSAL = 3

# ─────────────────────────────────────────────────────────────────────────────
# Vectorised P&F direction (NumPy) — replaces the slow pure-Python loop
# ─────────────────────────────────────────────────────────────────────────────

def pf_direction_pct_close(close: pd.Series, box_pct: float, reversal: int = REVERSAL) -> int:
    s = close.dropna()
    if len(s) < 5:
        return 0

    pct = box_pct / 100.0
    dir_ = 0
    extreme = float(s.iloc[0])

    for px in s.iloc[1:]:
        px = float(px)
        box = max(px * pct, 1e-12)

        if dir_ == 0:
            if px >= extreme + box:
                dir_ = 1
                extreme = px
            elif px <= extreme - box:
                dir_ = -1
                extreme = px

        elif dir_ == 1:
            if px > extreme:
                extreme = px
            elif px <= extreme - (reversal * box):
                dir_ = -1
                extreme = px

        else:
            if px < extreme:
                extreme = px
            elif px >= extreme + (reversal * box):
                dir_ = 1
                extreme = px

    return dir_


def pf_rs_matrix(closes: pd.DataFrame, box_pct: float, reversal: int = REVERSAL) -> pd.DataFrame:
    closes = closes.sort_index().dropna(how="all")
    syms = list(closes.columns)

    out = pd.DataFrame(0, index=syms, columns=syms, dtype="int8")

    for i in syms:
        si = closes[i]
        for j in syms:
            if i == j:
                continue
            sj = closes[j]
            ratio = (si / sj).replace([np.inf, -np.inf], np.nan).dropna()
            out.loc[i, j] = pf_direction_pct_close(ratio, box_pct=box_pct, reversal=reversal)

    return out


def render_pf_matrix_boxes(mat: pd.DataFrame, title: str = "RS Matrix", link_type: str = 'index', clickable_map: dict = None) -> str:
    mat = mat.copy()
    mat = mat.loc[mat.index, mat.columns]

    n = len(mat)
    diag_mask = np.eye(n, dtype=bool)
    vals = mat.values.astype(int)

    green_counts = ((vals == 1) & (~diag_mask)).sum(axis=1)

    css = """
    <style>
      .pf-wrap {
          width: 100%;
          overflow-x: auto;
          background: white;
          padding: 5px;
          border-radius: 8px;
      }
      table.pf {
          border-collapse: separate;
          border-spacing: 1px;
          margin: 0;
      }
      table.pf th {
          color: #64748b;
          font-size: 9px;
          font-weight: 700;
          padding: 2px 1px;
          white-space: nowrap;
          vertical-align: bottom;
          min-width: 14px;
          max-width: 14px;
          overflow: hidden;
      }
      td.rowhdr {
          text-align: left;
          color: #1f2937;
          font-size: 10px;
          font-weight: 700;
          padding: 2px 8px;
          white-space: nowrap;
          background: white;
          position: sticky;
          left: 0;
          z-index: 10;
          min-width: 120px;
      }
      .count {
          display: inline-block;
          margin-left: 6px;
          padding: 1px 4px;
          border-radius: 3px;
          font-size: 9px;
          font-weight: 700;
          background: #f1f5f9;
          color: #475569;
      }
      .pf-cell {
          width: 14px;
          height: 14px;
          border-radius: 2px;
          display: inline-block;
          transition: transform 0.1s ease;
          border: 1px solid rgba(0,0,0,0.03);
      }
      .pf-cell:hover {
          transform: scale(1.5);
          z-index: 100;
          box-shadow: 0 4px 12px rgba(0,0,0,0.2);
          border: 1px solid white;
      }
      .g { background: #22c55e; }
      .r { background: #ef4444; }
      .z { background: #f8fafc; }
      .diag { background: #94a3b8; }
      .matrix-link { color: inherit; text-decoration: none; cursor: pointer; }
      .matrix-link:hover { color: #2563eb !important; text-decoration: underline; }
    </style>
    """

    cols = list(mat.columns)
    rows = list(mat.index)

    parts = [css, '<div class="pf-wrap">', '<table class="pf">']

    import re
    def norm(s):
        return re.sub(r'[^A-Z0-9]', '', str(s).upper())

    # header
    parts.append("<thead><tr>")
    parts.append("<th></th>")
    for c in cols:
        # For columns in heatmap, we often just show the ticker or vertical text
        # but here we'll use a tooltip or just keep it short
        parts.append(f'<th title="{html.escape(str(c))}">{html.escape(str(c)[:5])}</th>')
    parts.append("</tr></thead>")

    # HEATMAP_V2_MARKER
    parts.append("<tbody>")
    for i, r in enumerate(rows):
        r_str = str(r)

        # Truncate row header for denser look if too long
        display_r = r_str[:12] + '..' if len(r_str) > 14 else r_str

        is_clickable = False
        target_name = r_str

        if link_type == 'index' and clickable_map:
            normalized_row = norm(r_str)
            if normalized_row in clickable_map:
                is_clickable = True
                target_name = clickable_map[normalized_row]
        elif link_type == 'stock':
            is_clickable = True

        if is_clickable:
            if link_type == 'index':
                row_html = f'<a href="javascript:void(0)" onclick="onRsIndexChange(\'{html.escape(target_name)}\', null)" class="matrix-link">{html.escape(display_r)}</a>'
            elif link_type == 'stock':
                row_html = f'<a href="/stock/{html.escape(target_name)}" class="matrix-link" target="_blank">{html.escape(display_r)}</a>'
        else:
            row_html = html.escape(display_r)

        parts.append("<tr>")
        parts.append(
            f'<td class="rowhdr">{row_html}<span class="count">{int(green_counts[i])}</span></td>'
        )

        for j, c in enumerate(cols):
            v = int(mat.loc[r, c])
            cell_title = f"{r} vs {c}: {'Bullish' if v==1 else 'Bearish' if v==-1 else 'Neutral'}"
            if i == j:
                parts.append(f'<td style="padding:0;"><div class="pf-cell diag" title="{cell_title}"></div></td>')
            else:
                cls = "g" if v == 1 else "r" if v == -1 else "z"
                # ENSURE NO INNER TEXT
                parts.append(f'<td style="padding:0;"><div class="pf-cell {cls}" title="{cell_title}"></div></td>')
        parts.append("</tr>")
    parts.append("</tbody></table></div>")

    return "".join(parts)


@lru_cache(maxsize=4)
def get_index_history_proxy() -> pd.DataFrame:
    """
    Builds a historical 'closes' DataFrame for all indices from the index_historical_data table.
    Returns a DataFrame where the index is Date and the columns are Index values.
    """
    try:
        # Build query fetching ALL indices from the new true OHLC table
        # We exclude indices that are just generic aggregates or bonds if needed,
        # but for now we pull everything that has data.
        query = text("""
        SELECT date, index_name, close
        FROM index_historical_data
        WHERE date >= CURRENT_DATE - INTERVAL '2 years'
        ORDER BY date ASC
        """)

        with engine_cash.connect() as conn:
            df = pd.read_sql(query, conn)

        if df.empty:
            return pd.DataFrame()

        # Pivot the dataframe so that dates are the index, and index_names are the columns
        pivoted = df.pivot(index='date', columns='index_name', values='close')

        # We want to format the columns so they look clean (e.g. NIFTY 50 instead of NIFTY50)
        # However the true names from NSE archive are already "Nifty 50", "Nifty Bank", etc.
        # But our DB scraper did: df['index_name'].str.strip().str.upper()
        # So they are like "NIFTY 50"

        # Drop indices with completely missing data (all NaNs)
        pivoted.dropna(axis=1, how='all', inplace=True)

        # Forward fill missing values then fill remaining with NaN or previous
        indices_closes = pivoted.ffill()

        return indices_closes

    except Exception as e:
        print(f"[ERROR] Failed to generate index history proxy: {e}")
        return pd.DataFrame()

def generate_rs_matrix_html(box_pct: float) -> str:
    """
    Generate Point & Figure RS Matrix HTML for Indices.
    """
    closes = get_index_history_proxy()
    title = "Index Relative Strength (Point & Figure)"

    if closes.empty:
        return f"<div class='empty-state'><p>Not enough historical data to calculate {title}.</p></div>"

    mat = pf_rs_matrix(closes, box_pct=box_pct, reversal=REVERSAL)

    # Sort by number of green boxes
    gc = ((mat.values == 1) & (~np.eye(len(mat), dtype=bool))).sum(axis=1)
    order = np.argsort(-gc)  # descending
    mat = mat.iloc[order, order]

    # Clickable indices are ones that actually have component mapped
    from .index_model import get_index_list
    import re

    def norm(s):
        return re.sub(r'[^A-Z0-9]', '', str(s).upper())

    # Create a mapping from normalized DB names (like "NIFTYALPHA50")
    # to the properly formatted name the stock API expects ("NIFTY ALPHA 50")
    clickable_map = {}

    # Add all valid scraped indices
    dyn_indices = get_index_list()
    for idx_info in dyn_indices:
        idx_key = idx_info.get("key", "")
        if idx_key not in ["all", "sensex"]:
            idx_name = idx_info.get("name", "")
            if idx_name:
                clickable_map[norm(idx_name)] = idx_name
                clickable_map[norm(idx_key)] = idx_name

    return render_pf_matrix_boxes(mat, title=title, link_type='index', clickable_map=clickable_map)

@lru_cache(maxsize=16)
def get_stock_history_proxy_for_index(index_name: str) -> pd.DataFrame:
    """
    Builds a historical 'closes' DataFrame for all stocks inside a given index
    Returns a DataFrame where the index is Date and the columns are stock tickers.
    """
    try:
        from .index_model import get_index_list
        import re
        def norm(s):
            return re.sub(r'[^A-Z0-9]', '', str(s).upper())

        # 1. Reverse map index_name to idx_key
        idx_key = None
        norm_input = norm(index_name)
        dyn_indices = get_index_list()
        for info in dyn_indices:
            # Check normalized name or normalized key
            if norm(info.get("name", "")) == norm_input or norm(info.get("key", "")) == norm_input:
                idx_key = info.get("key", "")
                break

        if not idx_key:
            print(f"[DEBUG] Could not resolve idx_key for {index_name}")
            return pd.DataFrame()

        # 2. Get constituents
        stocks = get_index_stocks(idx_key)
        if not stocks:
            return pd.DataFrame()

        # 3. Query DB
        stocks_tuple = tuple(stocks)
        if len(stocks_tuple) == 1:
            stock_list_str = f"('{stocks_tuple[0]}')"
        else:
            stock_list_str = str(stocks_tuple)

        query = text(f"""
        SELECT date, symbol, change_pct
        FROM daily_market_heatmap
        WHERE date >= CURRENT_DATE - INTERVAL '2 years'
          AND symbol IN {stock_list_str}
        ORDER BY date ASC
        """)

        with engine_cash.connect() as conn:
            df = pd.read_sql(query, conn)

        if df.empty:
            return pd.DataFrame()

        # Pivot the dataframe so that dates are the index, and symbols are the columns
        pivoted = df.pivot(index='date', columns='symbol', values='change_pct')

        # Drop stocks with completely missing data (all NaNs)
        pivoted.dropna(axis=1, how='all', inplace=True)

        # Forward fill missing values then fill remaining with 0
        pivoted = pivoted.ffill().fillna(0)

        # Calculate a pseudo "Price Index" starting at 100 for all stocks
        stock_closes = (1 + pivoted / 100).cumprod() * 100

        return stock_closes

    except Exception as e:
        print(f"[ERROR] Failed to generate stock history for {index_name}: {e}")
        return pd.DataFrame()

def generate_stock_rs_matrix_html(index_name: str, box_pct: float) -> str:
    """
    Generate Point & Figure RS Matrix HTML for Stocks in an Index.
    """
    closes = get_stock_history_proxy_for_index(index_name)
    title = f"{index_name} Components Relative Strength"

    if closes.empty:
        return f"<div class='empty-state'><p>Not enough constituent data available for {html.escape(index_name)}.</p></div>"

    mat = pf_rs_matrix(closes, box_pct=box_pct, reversal=REVERSAL)

    # Sort by number of green boxes
    gc = ((mat.values == 1) & (~np.eye(len(mat), dtype=bool))).sum(axis=1)
    order = np.argsort(-gc)  # descending
    mat = mat.iloc[order, order]

    return render_pf_matrix_boxes(mat, title=title, link_type='stock')


def get_stock_rs_data(index_name: str, box_pct: float) -> list:
    """
    Returns RS scores (green count) for constituents of an index.
    Used for Treemap/Heatmap visualization.
    """
    closes = get_stock_history_proxy_for_index(index_name)
    if closes.empty:
        return []

    mat = pf_rs_matrix(closes, box_pct=box_pct, reversal=REVERSAL)

    # Calculate green counts (RS Score)
    n = len(mat)
    gc = ((mat.values == 1) & (~np.eye(n, dtype=bool))).sum(axis=1)

    # Get latest prices and change from heatmap data to size the treemap
    from .insights_model import get_heatmap_data, get_insights_dates
    dates = get_insights_dates()
    latest_date = dates[0] if dates else None

    market_data = {}
    if latest_date:
        all_stocks = get_heatmap_data(latest_date)
        market_data = {s['symbol']: s for s in all_stocks}

    results = []
    for i, symbol in enumerate(mat.index):
        m_data = market_data.get(symbol, {})
        results.append({
            "symbol": symbol,
            "rs_score": int(gc[i]),
            "max_score": n - 1,
            "rs_pct": round(int(gc[i]) / (n - 1) * 100, 2) if n > 1 else 0,
            "change_pct": m_data.get("change_pct", 0),
            "close": m_data.get("close", 0),
            "turnover": m_data.get("turnover", 1),
            "sector": m_data.get("sector", "Others")
        })

    # Sort by RS score descending
    results.sort(key=lambda x: x["rs_score"], reverse=True)
    return results


def generate_category_rs_matrix_html(category: str, box_pct: float) -> str:
    """
    Generate Point & Figure RS Matrix HTML for all indices in a given category
    (or ALL indices when category is '' / None).

    Uses index_historical_data for price series and index_constituents to know
    which index names belong to the requested category.
    """
    # --- Step 1: Get all index names for this category ---
    try:
        if category:
            query = text("""
                SELECT DISTINCT index_name
                FROM index_constituents
                WHERE index_category = :cat AND index_name IS NOT NULL
                ORDER BY index_name
            """)
            with engine_cash.connect() as conn:
                rows = conn.execute(query, {"cat": category}).fetchall()
            category_indices = {r[0].upper() for r in rows}
        else:
            category_indices = None   # None = all
    except Exception as e:
        print(f"[ERROR] generate_category_rs_matrix_html category lookup: {e}")
        category_indices = None

    # --- Step 2: Get full history then filter columns ---
    closes = get_index_history_proxy()
    if closes.empty:
        return "<div class='empty-state'><p>No historical index data available.</p></div>"

    if category_indices:
        # Keep only the columns (index names) that belong to this category
        # Column names in history are already upper-cased by the scraper
        keep = [c for c in closes.columns if c.upper() in category_indices]
        if not keep:
            return f"<div class='empty-state'><p>No historical data available for category: {html.escape(category)}.</p></div>"
        closes = closes[keep]

    # --- Step 3: Build clickable map so clicking an index row loads its stocks ---
    from .index_model import get_index_list
    import re

    def norm(s):
        return re.sub(r'[^A-Z0-9]', '', str(s).upper())

    clickable_map = {}
    dyn_indices = get_index_list()
    for idx_info in dyn_indices:
        idx_key = idx_info.get("key", "")
        if idx_key not in ["all", "sensex"]:
            idx_name = idx_info.get("name", "")
            if idx_name:
                clickable_map[norm(idx_name)] = idx_name
                clickable_map[norm(idx_key)]  = idx_name

    # --- Step 4: Compute RS matrix ---
    mat = pf_rs_matrix(closes, box_pct=box_pct, reversal=REVERSAL)

    # Sort by green score
    gc = ((mat.values == 1) & (~np.eye(len(mat), dtype=bool))).sum(axis=1)
    order = np.argsort(-gc)
    mat = mat.iloc[order, order]

    title = f"{category} — Index RS Matrix" if category else "All Indices — Relative Strength"
    return render_pf_matrix_boxes(mat, title=title, link_type='index', clickable_map=clickable_map)


def get_index_category_rs_data(category: str, box_pct: float) -> list:
    """
    Returns RS scores (green count) for indices in a category.
    Used for the summary Treemap visualization.
    """
    # --- Step 1: Get all index names for this category ---
    try:
        if category:
            query = text("""
                SELECT DISTINCT index_name
                FROM index_constituents
                WHERE index_category = :cat AND index_name IS NOT NULL
                ORDER BY index_name
            """)
            with engine_cash.connect() as conn:
                rows = conn.execute(query, {"cat": category}).fetchall()
            category_indices = {r[0].upper() for r in rows}
        else:
            category_indices = None   # None = all
    except Exception as e:
        print(f"[ERROR] get_index_category_rs_data category lookup: {e}")
        category_indices = None

    # --- Step 2: Get full history then filter columns ---
    closes = get_index_history_proxy()
    if closes.empty:
        return []

    if category_indices:
        keep = [c for c in closes.columns if c.upper() in category_indices]
        if not keep:
            return []
        closes = closes[keep]

    # --- Step 3: Compute RS matrix ---
    mat = pf_rs_matrix(closes, box_pct=box_pct, reversal=REVERSAL)

    # Calculate green counts (RS Score)
    n = len(mat)
    diag_mask = np.eye(n, dtype=bool)
    gc = ((mat.values == 1) & (~diag_mask)).sum(axis=1)

    # Get sentiment and latest data if possible
    from .index_model import get_index_list
    dyn_indices = get_index_list()
    idx_metadata = {i['name']: i for i in dyn_indices}

    results = []
    for i, idx_name in enumerate(mat.index):
        meta = idx_metadata.get(idx_name, {})
        results.append({
            "symbol": idx_name,
            "rs_score": int(gc[i]),
            "max_score": n - 1 if n > 1 else 1,
            "rs_pct": round(int(gc[i]) / (n - 1) * 100, 2) if n > 1 else 0,
            "value": 1,
            "type": "index",
            "sentiment": meta.get("sentiment", "Neutral"),
            "change_pct": meta.get("change_pct", 0)
        })

    # Sort by RS score descending
    results.sort(key=lambda x: x["rs_score"], reverse=True)
    return results
