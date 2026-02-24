import html
import numpy as np
import pandas as pd
from sqlalchemy import text
from functools import lru_cache
from .db_config import engine_cash
from .index_model import get_index_stocks, INDEX_METADATA

REVERSAL = 3

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
      .pf-wrap { width: 100%; overflow-x: auto; margin-top: 10px; }
      .pf-title { font-size: 18px; font-weight: 700; margin: 6px 0 10px 0; color: #1f2937; }
      table.pf { border-collapse: separate; border-spacing: 6px; }
      table.pf th, table.pf td { text-align: center; }
      table.pf th { color: #4b5563; font-size: 12px; font-weight: 600; padding: 6px 4px; white-space: nowrap; }
      td.rowhdr { text-align: left; color: #1f2937; font-size: 12px; font-weight: 700; padding: 6px 10px; white-space: nowrap; }
      .count { display: inline-block; margin-left: 8px; padding: 2px 8px; border-radius: 999px;
               font-size: 12px; font-weight: 900; background: #1f1f1f; color: #eaeaea; border: 1px solid #333; }
      .pf-cell { width: 44px; height: 24px; border-radius: 4px; font-size: 12px; font-weight: 900; line-height: 24px;
              border: 1px solid rgba(255,255,255,0.08); display: inline-block; }
      .g { background: #22c55e; color: #08210f; }
      .r { background: #ef4444; color: #2a0a0a; }
      .z { background: #2a2a2a; color: #a1a1a1; }
      .diag { background: #151515; color: #666; border: 1px dashed rgba(255,255,255,0.15); }
      .matrix-link { color: inherit; text-decoration: none; cursor: pointer; }
      .matrix-link:hover { text-decoration: underline !important; color: #2563eb !important; }
    </style>
    """

    cols = list(mat.columns)
    rows = list(mat.index)

    parts = [css, f'<div class="pf-title">{html.escape(title)}</div>', '<div class="pf-wrap">', '<table class="pf">']

    import re
    def norm(s):
        return re.sub(r'[^A-Z0-9]', '', str(s).upper())

    # header
    parts.append("<thead><tr>")
    parts.append("<th></th>")
    for c in cols:
        parts.append(f"<th>{html.escape(str(c))}</th>")
    parts.append("</tr></thead>")

    # body
    parts.append("<tbody>")
    for i, r in enumerate(rows):
        r_str = str(r)

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
                row_html = f'<a href="javascript:void(0)" onclick="loadStockRSMatrix(\'{html.escape(target_name)}\')" class="matrix-link">{html.escape(r_str)}</a>'
            elif link_type == 'stock':
                row_html = f'<a href="/stock/{html.escape(target_name)}" class="matrix-link" target="_blank">{html.escape(r_str)}</a>'
        else:
            row_html = html.escape(r_str)

        parts.append("<tr>")
        parts.append(
            f'<td class="rowhdr">{row_html}<span class="count">{int(green_counts[i])}</span></td>'
        )

        for j, c in enumerate(cols):
            v = int(mat.loc[r, c])
            if i == j:
                parts.append('<td><div class="pf-cell diag"></div></td>')
            else:
                if v == 1:
                    parts.append('<td><div class="pf-cell g">1</div></td>')
                elif v == -1:
                    parts.append('<td><div class="pf-cell r">-1</div></td>')
                else:
                    parts.append('<td><div class="pf-cell z"></div></td>')
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
