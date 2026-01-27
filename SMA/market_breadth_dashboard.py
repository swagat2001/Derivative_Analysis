"""
NSE CASH DATA - ENHANCED DASHBOARD WITH MARKET BREADTH ANALYSIS
================================================================
Version: 3.0
Features:
1. SMA Testing Dashboard (existing)
2. 52-Week High/Low Heatmap (existing)
3. NEW: Market Breadth Analysis
   - Index-wise Breadth Heatmap (% stocks above RS/SMA levels)
   - Sector Fundamental Table (MCap, Volume, Delivery aggregates)
   - Sector-wise Breadth Heatmap (% stocks above RS/RSI/SMA levels)

RS Formula: (EMA_21 of Stock / EMA_21 of Nifty) * 100
"""

import glob
import os
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

# ===========================================
# Configuration
# ===========================================
DATA_FOLDER = "C:/NSE_EOD_CASH_WITH_INDICATORS"
SECTOR_MASTER_FILE = "C:/NSE_EOD_CASH_WITH_INDICATORS/nse_sector_master.csv"
SMA_PERIODS = [3, 5, 8, 13, 21, 50, 100, 200]
SMA_COLUMNS = [
    "CLOSE_PRICE",
    "AVG_PRICE",
    "TTL_TRD_QNTY",
    "TURNOVER_LACS",
    "NO_OF_TRADES",
    "DELIV_QTY",
]

# Index definitions for Market Breadth
NIFTY_INDICES = [
    "NIFTY50",
    "NIFTY100",
    "NIFTY200",
    "NIFTY500",
    "NIFTYMIDCAP150",
    "NIFTYSMALLCAP250",
    "NIFTYMICROCAP250",  # If available
]

# Display names for indices
INDEX_DISPLAY_NAMES = {
    "NIFTY50": "Nifty 50",
    "NIFTY100": "Nifty 100",
    "NIFTY200": "Nifty 200",
    "NIFTY500": "Nifty 500",
    "NIFTYMIDCAP150": "Nifty Midcap 150",
    "NIFTYSMALLCAP250": "Nifty Smallcap 250",
    "NIFTYMICROCAP250": "Nifty Microcap 250",
    "NIFTYBANK": "Nifty Bank",
    "NIFTYFINSERV": "Nifty Financial Services",
    "NIFTYMIDCAPSELECT": "Nifty Midcap Select",
    "NIFTYNEXT50": "Nifty Next 50",
}

# ===========================================
# Page Configuration
# ===========================================
st.set_page_config(
    page_title="NSE Dashboard - Market Breadth Analysis",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ===========================================
# Custom CSS Styling
# ===========================================
st.markdown(
    """
<style>
    .main {
        padding: 0rem 1rem;
    }
    .stAlert {
        margin-top: 1rem;
    }
    h1 {
        color: #1f77b4;
        padding-bottom: 1rem;
        border-bottom: 2px solid #e0e0e0;
    }
    h2 {
        color: #2ca02c;
        padding-top: 1rem;
    }
    h3 {
        color: #ff7f0e;
    }
    div[data-testid="stMetricValue"] {
        font-size: 1.5rem;
        font-weight: 600;
    }
    /* Heatmap cell styling */
    .breadth-cell {
        padding: 8px;
        text-align: center;
        font-weight: bold;
    }
</style>
""",
    unsafe_allow_html=True,
)


# ===========================================
# Helper Functions
# ===========================================


@st.cache_data(ttl=600)
def load_available_files():
    """Load list of available CSV files from data folder"""
    pattern = os.path.join(DATA_FOLDER, "sec_bhavdata_full_*_WITH_INDICATORS.csv")
    files = sorted(glob.glob(pattern))

    file_dates = []
    for filepath in files:
        filename = os.path.basename(filepath)
        date_str = filename.replace("sec_bhavdata_full_", "").replace("_WITH_INDICATORS.csv", "")
        try:
            date_obj = datetime.strptime(date_str, "%d%m%Y")
            file_dates.append(
                {
                    "filepath": filepath,
                    "date": date_obj.strftime("%Y-%m-%d"),
                    "date_obj": date_obj,
                    "display": date_obj.strftime("%d %b %Y"),
                    "filename": filename,
                }
            )
        except Exception:
            pass

    file_dates.sort(key=lambda x: x["date_obj"], reverse=True)
    return file_dates


@st.cache_data(ttl=600)
def load_data(filepath: str) -> pd.DataFrame:
    """Load CSV data with proper type handling"""
    try:
        df = pd.read_csv(filepath, low_memory=False)
        df.columns = df.columns.str.strip()

        # Convert base numeric columns
        base_numeric_cols = [
            "CLOSE_PRICE",
            "AVG_PRICE",
            "TTL_TRD_QNTY",
            "TURNOVER_LACS",
            "NO_OF_TRADES",
            "DELIV_QTY",
            "DELIV_PER",
            "RSI_14",
            "EMA_21_CLOSE_PRICE",
            "RS_21",
        ]

        for col in base_numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Convert all SMA columns
        for period in SMA_PERIODS:
            for param in SMA_COLUMNS:
                col_name = f"SMA_{period}_{param}"
                if col_name in df.columns:
                    df[col_name] = pd.to_numeric(df[col_name], errors="coerce")

        return df

    except Exception as e:
        st.error(f"Error loading file: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_sector_master():
    """Load sector master file"""
    try:
        for path in [SECTOR_MASTER_FILE, "nse_sector_master.csv", os.path.join(DATA_FOLDER, "nse_sector_master.csv")]:
            if os.path.exists(path):
                df = pd.read_csv(path, encoding="utf-8-sig")
                df.columns = df.columns.str.strip()
                return df
        return pd.DataFrame()
    except:
        return pd.DataFrame()


def format_number(value, decimals: int = 2) -> str:
    """Format number for display"""
    if pd.isna(value):
        return "N/A"
    try:
        return f"{float(value):,.{decimals}f}"
    except Exception:
        return "N/A"


def format_lakhs_crores(value):
    """Format large numbers in Indian notation (Lakhs/Crores)"""
    if pd.isna(value):
        return "N/A"
    try:
        value = float(value)
        if abs(value) >= 10000000:  # 1 Crore
            return f"{value/10000000:,.2f} Cr"
        elif abs(value) >= 100000:  # 1 Lakh
            return f"{value/100000:,.2f} L"
        else:
            return f"{value:,.2f}"
    except:
        return "N/A"


def get_color_for_percentage(pct, is_green_good=True):
    """Get color based on percentage value for heatmap"""
    if pd.isna(pct):
        return "#808080"  # Gray for N/A

    pct = float(pct)

    if is_green_good:
        # Higher is better (green)
        if pct >= 80:
            return "#006400"  # Dark green
        elif pct >= 60:
            return "#228B22"  # Forest green
        elif pct >= 50:
            return "#9ACD32"  # Yellow-green
        elif pct >= 40:
            return "#FFFF00"  # Yellow
        elif pct >= 30:
            return "#FFA500"  # Orange
        elif pct >= 20:
            return "#FF6347"  # Tomato
        else:
            return "#DC143C"  # Crimson (red)
    else:
        # Lower is better (inverted)
        if pct <= 20:
            return "#006400"
        elif pct <= 30:
            return "#228B22"
        elif pct <= 40:
            return "#9ACD32"
        elif pct <= 50:
            return "#FFFF00"
        elif pct <= 60:
            return "#FFA500"
        elif pct <= 80:
            return "#FF6347"
        else:
            return "#DC143C"


# ===========================================
# Market Breadth Calculation Functions
# ===========================================


def calculate_breadth_metrics(df, sector_master):
    """
    Calculate market breadth metrics for all stocks
    Returns DataFrame with breadth indicators
    """
    # Merge with sector master
    if not sector_master.empty:
        df = df.merge(sector_master[["SYMBOL", "NSE_INDUSTRY", "SECTOR", "INDEX_MEMBERSHIP"]], on="SYMBOL", how="left")

    # Calculate breadth indicators
    # RS > 0 (RS_21 > base value, typically 100 means outperforming Nifty)
    # For "RS 55 > 0" interpretation: RS value where 55 is the normalized threshold
    # We'll use RS_21 > 55 as the condition
    df["RS_ABOVE_55"] = df["RS_21"] > 55 if "RS_21" in df.columns else False

    # RSI > 50 (bullish momentum)
    df["RSI_ABOVE_50"] = df["RSI_14"] > 50 if "RSI_14" in df.columns else False

    # Price above SMAs
    for period in [20, 50, 100, 200]:
        sma_col = f"SMA_{period}_CLOSE_PRICE"
        if sma_col in df.columns:
            df[f"ABOVE_SMA_{period}"] = df["CLOSE_PRICE"] > df[sma_col]
        else:
            # Try with closest available SMA
            closest_sma = f"SMA_{21 if period == 20 else period}_CLOSE_PRICE"
            if closest_sma in df.columns:
                df[f"ABOVE_SMA_{period}"] = df["CLOSE_PRICE"] > df[closest_sma]
            else:
                df[f"ABOVE_SMA_{period}"] = False

    return df


def check_index_membership(membership_str, idx_code):
    """
    Check if idx_code is in the comma-separated membership string.
    Uses exact matching to avoid NIFTY50 matching NIFTY500.
    """
    if pd.isna(membership_str):
        return False
    # Split by comma and check for exact match
    indices = [x.strip() for x in str(membership_str).split(",")]
    return idx_code in indices


def calculate_index_breadth(df):
    """
    Calculate breadth metrics grouped by Nifty indices
    Returns DataFrame with index-wise breadth percentages
    """
    results = []

    indices_to_check = [
        ("NIFTY50", "Nifty 50", 50),
        ("NIFTYNEXT50", "Nifty Next 50", 50),
        ("NIFTY100", "Nifty 100", 100),
        ("NIFTY200", "Nifty 200", 200),
        ("NIFTY500", "Nifty 500", 500),
        ("NIFTYMIDCAP150", "Nifty Midcap 150", 150),
        ("NIFTYSMALLCAP250", "Nifty Smallcap 250", 250),
        ("NIFTYBANK", "Nifty Bank", 12),
        ("NIFTYFINSERV", "Nifty Financial Services", 20),
        ("NIFTYMIDCAPSELECT", "Nifty Midcap Select", 25),
        ("NIFTYMICROCAP250", "Nifty Microcap 250", 250),
    ]

    for idx_code, idx_name, expected_count in indices_to_check:
        if "INDEX_MEMBERSHIP" not in df.columns:
            continue

        # Use exact matching instead of substring matching
        mask = df["INDEX_MEMBERSHIP"].apply(lambda x: check_index_membership(x, idx_code))
        idx_df = df[mask]

        if len(idx_df) == 0:
            continue

        total = len(idx_df)

        # Calculate percentages
        row = {
            "Index": idx_name,
            "Stocks": total,
            "RS-21": (idx_df["RS_ABOVE_55"].sum() / total * 100) if "RS_ABOVE_55" in idx_df.columns else 0,
            "RSI > 50": (idx_df["RSI_ABOVE_50"].sum() / total * 100) if "RSI_ABOVE_50" in idx_df.columns else 0,
            "SMA 20": (idx_df["ABOVE_SMA_20"].sum() / total * 100) if "ABOVE_SMA_20" in idx_df.columns else 0,
            "SMA 50": (idx_df["ABOVE_SMA_50"].sum() / total * 100) if "ABOVE_SMA_50" in idx_df.columns else 0,
            "SMA 100": (idx_df["ABOVE_SMA_100"].sum() / total * 100) if "ABOVE_SMA_100" in idx_df.columns else 0,
            "SMA 200": (idx_df["ABOVE_SMA_200"].sum() / total * 100) if "ABOVE_SMA_200" in idx_df.columns else 0,
        }

        results.append(row)

    return pd.DataFrame(results)


def get_index_stocks_detail(df, idx_code):
    """
    Get detailed stock-level data for a specific index
    Returns DataFrame with stock-wise breadth indicators
    """
    if "INDEX_MEMBERSHIP" not in df.columns:
        return pd.DataFrame()

    # Filter stocks belonging to this index
    mask = df["INDEX_MEMBERSHIP"].apply(lambda x: check_index_membership(x, idx_code))
    idx_stocks = df[mask].copy()

    if len(idx_stocks) == 0:
        return pd.DataFrame()

    # Get SMA values for each period
    sma_20_col = "SMA_21_CLOSE_PRICE" if "SMA_21_CLOSE_PRICE" in idx_stocks.columns else "SMA_20_CLOSE_PRICE"
    sma_50_col = "SMA_50_CLOSE_PRICE"
    sma_100_col = "SMA_100_CLOSE_PRICE"
    sma_200_col = "SMA_200_CLOSE_PRICE"

    # Create result DataFrame with exact values
    result = pd.DataFrame(
        {
            "Symbol": idx_stocks["SYMBOL"],
            "Close": idx_stocks["CLOSE_PRICE"].round(2) if "CLOSE_PRICE" in idx_stocks.columns else 0,
            "RS-21": idx_stocks["RS_21"].round(2) if "RS_21" in idx_stocks.columns else 0,
            "RSI-14": idx_stocks["RSI_14"].round(2) if "RSI_14" in idx_stocks.columns else 0,
            "SMA 20": idx_stocks[sma_20_col].round(2) if sma_20_col in idx_stocks.columns else 0,
            "SMA 50": idx_stocks[sma_50_col].round(2) if sma_50_col in idx_stocks.columns else 0,
            "SMA 100": idx_stocks[sma_100_col].round(2) if sma_100_col in idx_stocks.columns else 0,
            "SMA 200": idx_stocks[sma_200_col].round(2) if sma_200_col in idx_stocks.columns else 0,
        }
    )

    # Sort by RS-21 descending (highest RS first)
    result = result.sort_values("RS-21", ascending=False).reset_index(drop=True)
    return result


def get_sector_stocks_detail(df, sector_name):
    """
    Get detailed stock-level data for a specific sector
    Returns DataFrame with stock-wise breadth indicators
    """
    if "NSE_INDUSTRY" not in df.columns:
        return pd.DataFrame()

    # Filter stocks belonging to this sector
    sector_stocks = df[df["NSE_INDUSTRY"] == sector_name].copy()

    if len(sector_stocks) == 0:
        return pd.DataFrame()

    # Get SMA values for each period
    sma_20_col = "SMA_21_CLOSE_PRICE" if "SMA_21_CLOSE_PRICE" in sector_stocks.columns else "SMA_20_CLOSE_PRICE"
    sma_50_col = "SMA_50_CLOSE_PRICE"
    sma_100_col = "SMA_100_CLOSE_PRICE"
    sma_200_col = "SMA_200_CLOSE_PRICE"

    # Create result DataFrame with exact values
    result = pd.DataFrame(
        {
            "Symbol": sector_stocks["SYMBOL"],
            "Close": sector_stocks["CLOSE_PRICE"].round(2) if "CLOSE_PRICE" in sector_stocks.columns else 0,
            "RS-21": sector_stocks["RS_21"].round(2) if "RS_21" in sector_stocks.columns else 0,
            "RSI-14": sector_stocks["RSI_14"].round(2) if "RSI_14" in sector_stocks.columns else 0,
            "SMA 20": sector_stocks[sma_20_col].round(2) if sma_20_col in sector_stocks.columns else 0,
            "SMA 50": sector_stocks[sma_50_col].round(2) if sma_50_col in sector_stocks.columns else 0,
            "SMA 100": sector_stocks[sma_100_col].round(2) if sma_100_col in sector_stocks.columns else 0,
            "SMA 200": sector_stocks[sma_200_col].round(2) if sma_200_col in sector_stocks.columns else 0,
        }
    )

    # Sort by RS-21 descending (highest RS first)
    result = result.sort_values("RS-21", ascending=False).reset_index(drop=True)
    return result


def get_single_stock_detail(df, symbol):
    """
    Get detailed data for a single stock by symbol
    Returns tuple: (DataFrame with formatted details, Series with raw stock data)
    """
    # Filter for the specific stock (case-insensitive)
    stock_data = df[df["SYMBOL"].str.upper() == symbol.upper()].copy()

    if len(stock_data) == 0:
        return pd.DataFrame(), pd.Series()

    # Get SMA values for each period
    sma_20_col = "SMA_21_CLOSE_PRICE" if "SMA_21_CLOSE_PRICE" in stock_data.columns else "SMA_20_CLOSE_PRICE"
    sma_50_col = "SMA_50_CLOSE_PRICE"
    sma_100_col = "SMA_100_CLOSE_PRICE"
    sma_200_col = "SMA_200_CLOSE_PRICE"

    # Get the first (and only) row
    stock = stock_data.iloc[0]

    # Create result DataFrame
    result = pd.DataFrame(
        {
            "Metric": [
                "Symbol",
                "Sector",
                "Index Membership",
                "Close Price",
                "RS-21",
                "RSI-14",
                "SMA 20",
                "SMA 50",
                "SMA 100",
                "SMA 200",
                "Volume",
                "Turnover (Lacs)",
                "Delivery %",
                "No. of Trades",
            ],
            "Value": [
                stock.get("SYMBOL", "N/A"),
                stock.get("NSE_INDUSTRY", "N/A"),
                stock.get("INDEX_MEMBERSHIP", "N/A"),
                f"₹{stock.get('CLOSE_PRICE', 0):.2f}",
                f"{stock.get('RS_21', 0):.2f}",
                f"{stock.get('RSI_14', 0):.2f}",
                f"₹{stock.get(sma_20_col, 0):.2f}",
                f"₹{stock.get(sma_50_col, 0):.2f}",
                f"₹{stock.get(sma_100_col, 0):.2f}",
                f"₹{stock.get(sma_200_col, 0):.2f}",
                f"{int(stock.get('TTL_TRD_QNTY', 0)):,}",
                f"{stock.get('TURNOVER_LACS', 0):,.2f}",
                f"{stock.get('DELIV_PER', 0):.2f}%",
                f"{int(stock.get('NO_OF_TRADES', 0)):,}",
            ],
        }
    )

    # Return both formatted result AND raw stock data
    return result, stock


def calculate_sector_breadth(df):
    """
    Calculate breadth metrics grouped by sector
    Returns DataFrame with sector-wise breadth percentages
    """
    if "SECTOR" not in df.columns and "NSE_INDUSTRY" not in df.columns:
        return pd.DataFrame()

    results = []

    # Group by NSE_INDUSTRY (more granular like in the reference images)
    for industry in df["NSE_INDUSTRY"].dropna().unique():
        ind_df = df[df["NSE_INDUSTRY"] == industry]

        if len(ind_df) == 0:
            continue

        total = len(ind_df)

        # Calculate MCap (sum of TURNOVER_LACS as proxy, or use actual MCap if available)
        mcap = ind_df["TURNOVER_LACS"].sum() if "TURNOVER_LACS" in ind_df.columns else 0

        row = {
            "Sector": industry,
            "MCap (Cr.)": mcap,
            "Stocks": total,
            "RS-21": (ind_df["RS_ABOVE_55"].sum() / total * 100) if "RS_ABOVE_55" in ind_df.columns else 0,
            "RSI > 50": (ind_df["RSI_ABOVE_50"].sum() / total * 100) if "RSI_ABOVE_50" in ind_df.columns else 0,
            "SMA 20": (ind_df["ABOVE_SMA_20"].sum() / total * 100) if "ABOVE_SMA_20" in ind_df.columns else 0,
            "SMA 50": (ind_df["ABOVE_SMA_50"].sum() / total * 100) if "ABOVE_SMA_50" in ind_df.columns else 0,
            "SMA 100": (ind_df["ABOVE_SMA_100"].sum() / total * 100) if "ABOVE_SMA_100" in ind_df.columns else 0,
        }

        results.append(row)

    result_df = pd.DataFrame(results)

    # ✅ Sort by average breadth score (highest on top)
    if not result_df.empty:
        # Calculate average score across all breadth columns
        breadth_cols = ["RS-21", "RSI > 50", "SMA 20", "SMA 50", "SMA 100"]
        result_df["Avg_Score"] = result_df[breadth_cols].mean(axis=1)

        # Sort by average score descending (highest first)
        result_df = result_df.sort_values("Avg_Score", ascending=False).reset_index(drop=True)

        # Drop the temporary Avg_Score column
        result_df = result_df.drop("Avg_Score", axis=1)

    return result_df


def calculate_sector_fundamentals(df, df_prev=None):
    """
    Calculate sector fundamental data (MCap, Volume, Delivery)
    Similar to Image 2 in the reference
    """
    if "SECTOR" not in df.columns and "NSE_INDUSTRY" not in df.columns:
        return pd.DataFrame()

    # Use NSE_INDUSTRY for granular sector breakdown
    group_col = "NSE_INDUSTRY" if "NSE_INDUSTRY" in df.columns else "SECTOR"

    results = []

    for sector in df[group_col].dropna().unique():
        sect_df = df[df[group_col] == sector]

        if len(sect_df) == 0:
            continue

        # Calculate aggregates
        stocks = len(sect_df)
        mcap = sect_df["TURNOVER_LACS"].sum() if "TURNOVER_LACS" in sect_df.columns else 0
        volume = sect_df["TTL_TRD_QNTY"].sum() if "TTL_TRD_QNTY" in sect_df.columns else 0
        delivery = sect_df["DELIV_QTY"].sum() if "DELIV_QTY" in sect_df.columns else 0

        # Calculate averages
        vol_avg = volume / stocks if stocks > 0 else 0
        del_avg = delivery / stocks if stocks > 0 else 0

        # Calculate change if previous data available
        mcap_chg = 0
        mcap_chg_pct = 0
        if df_prev is not None and not df_prev.empty:
            prev_sect = df_prev[df_prev[group_col] == sector]
            if len(prev_sect) > 0:
                prev_mcap = prev_sect["TURNOVER_LACS"].sum() if "TURNOVER_LACS" in prev_sect.columns else 0
                if prev_mcap > 0:
                    mcap_chg = mcap - prev_mcap
                    mcap_chg_pct = (mcap_chg / prev_mcap) * 100

        row = {
            "Sector": sector,
            "Stocks": stocks,
            "MCap (Cr.)": mcap / 100,  # Convert to Crores
            "Chg (Cr.)": mcap_chg / 100,
            "Chg %": mcap_chg_pct,
            "Vol (Cr.)": volume / 10000000,  # Convert to Crores
            "Vol Avg": vol_avg / 10000000,
            "Del (Cr.)": delivery / 10000000,
            "Del Avg": del_avg / 10000000,
        }

        results.append(row)

    result_df = pd.DataFrame(results)

    # Sort by MCap descending (keep original sorting)
    if not result_df.empty:
        result_df = result_df.sort_values("MCap (Cr.)", ascending=False)

    return result_df


# ===========================================
# Heatmap Visualization Functions
# ===========================================


def create_breadth_heatmap(df, title, value_columns, row_label_col="Index", show_count_col="Stocks"):
    """
    Create a market breadth heatmap similar to the reference images
    """
    if df.empty:
        st.warning("No data available for heatmap")
        return

    # Prepare data
    rows = df[row_label_col].tolist()
    counts = df[show_count_col].tolist() if show_count_col in df.columns else [0] * len(rows)

    # Create z-data matrix
    z_data = []
    text_data = []

    for idx, row in df.iterrows():
        z_row = []
        text_row = []
        for col in value_columns:
            val = row.get(col, 0)
            z_row.append(val if pd.notna(val) else 0)
            text_row.append(f"{val:.0f}%" if pd.notna(val) else "N/A")
        z_data.append(z_row)
        text_data.append(text_row)

    # Create heatmap
    fig = go.Figure(
        data=go.Heatmap(
            z=z_data,
            x=value_columns,
            y=[f"{row}<br><sub>{int(cnt)}</sub>" for row, cnt in zip(rows, counts)],
            text=text_data,
            texttemplate="%{text}",
            textfont={"size": 11, "color": "black", "family": "Arial Black"},
            colorscale=[
                [0, "#DC143C"],  # Dark red (0%)
                [0.2, "#FF6347"],  # Tomato (20%)
                [0.35, "#FFA500"],  # Orange (35%)
                [0.5, "#FFFF00"],  # Yellow (50%)
                [0.65, "#9ACD32"],  # Yellow-green (65%)
                [0.8, "#32CD32"],  # Lime green (80%)
                [1, "#006400"],  # Dark green (100%)
            ],
            showscale=True,
            zmin=0,
            zmax=100,
            xgap=2,
            ygap=2,
            colorbar=dict(title="% Above", thickness=15, tickformat=".0f", ticksuffix="%"),
        )
    )

    fig.update_layout(
        title={"text": title, "x": 0.5, "xanchor": "center", "font": {"size": 16}},
        height=max(400, len(rows) * 45),
        margin=dict(l=200, r=100, t=60, b=40),
        xaxis=dict(title="Stocks Above", side="top", tickangle=0),
        yaxis=dict(title="", autorange="reversed"),
        plot_bgcolor="white",
        paper_bgcolor="white",
    )

    return fig


def create_sector_fundamental_table(df):
    """
    Create a styled table for sector fundamentals
    """
    if df.empty:
        st.warning("No data available for sector fundamentals")
        return

    # Format the dataframe for display
    display_df = df.copy()

    # Format numeric columns
    display_df["MCap (Cr.)"] = display_df["MCap (Cr.)"].apply(lambda x: f"{x:,.2f}")
    display_df["Chg (Cr.)"] = display_df.apply(
        lambda row: f"+{row['Chg (Cr.)']:,.2f}" if row["Chg (Cr.)"] > 0 else f"{row['Chg (Cr.)']:,.2f}", axis=1
    )
    display_df["Chg %"] = display_df.apply(
        lambda row: f"+{row['Chg %']:.1f}%" if row["Chg %"] > 0 else f"{row['Chg %']:.1f}%", axis=1
    )
    display_df["Vol (Cr.)"] = display_df["Vol (Cr.)"].apply(lambda x: f"{x:,.2f}")
    display_df["Del (Cr.)"] = display_df["Del (Cr.)"].apply(lambda x: f"{x:,.2f}")

    st.dataframe(display_df, hide_index=True, use_container_width=True)


# ===========================================
# Main Application
# ===========================================


def main():
    """Main application function"""

    # Header Section
    st.title("NSE Dashboard - Market Breadth Analysis")
    st.markdown("**Comprehensive Market Analysis with RS, RSI, and SMA Breadth Indicators**")
    st.markdown("---")

    # Load available files
    with st.spinner("Loading available data files..."):
        available_files = load_available_files()

    if not available_files:
        st.error(f"❌ No data files found in: `{DATA_FOLDER}`")
        st.info(
            """
        **Please ensure:**
        1. You have run the updated SMA/EMA calculator script
        2. Files are in the correct folder
        3. Files follow naming pattern: `sec_bhavdata_full_DDMMYYYY_WITH_INDICATORS.csv`
        """
        )
        return

    st.success(f"✅ Found {len(available_files)} trading days of data")

    # ===========================================
    # SIDEBAR - Controls
    # ===========================================

    with st.sidebar:
        st.header("🎛️ Control Panel")
        st.markdown("---")

        # Date Selection
        st.subheader("📅 Select Trading Date")
        date_options = {f["display"]: f for f in available_files}

        selected_date_display = st.selectbox(
            "Trading Date",
            options=list(date_options.keys()),
            index=0,
            help="Select the trading date to analyze",
        )

        selected_file = date_options[selected_date_display]
        st.info(f"**Selected Date:** {selected_file['date']}")

        # Load data for selected date
        with st.spinner(f"Loading data for {selected_file['date']}..."):
            df = load_data(selected_file["filepath"])
            sector_master = load_sector_master()

        # ===========================================
        # Stock Search Section
        # ===========================================
        st.markdown("---")
        st.subheader("🔍 Stock Search")

        # Get all available symbols
        all_symbols = sorted(df["SYMBOL"].dropna().unique().tolist())

        # Search input
        search_symbol = st.selectbox(
            "Search for a stock:", options=[""] + all_symbols, help="Type to search or scroll to select"
        )

        # Store selected symbol in session state
        if search_symbol:
            if (
                "selected_stock_symbol" not in st.session_state
                or st.session_state.selected_stock_symbol != search_symbol
            ):
                st.session_state.selected_stock_symbol = search_symbol

        st.markdown("---")

        # Load previous day data for comparison
        df_prev = None
        if len(available_files) > 1:
            prev_file = available_files[1]
            df_prev = load_data(prev_file["filepath"])
            if not df_prev.empty and not sector_master.empty:
                df_prev = df_prev.merge(
                    sector_master[["SYMBOL", "NSE_INDUSTRY", "SECTOR", "INDEX_MEMBERSHIP"]], on="SYMBOL", how="left"
                )

    # ===========================================
    # MAIN CONTENT - Tabs
    # ===========================================

    # Calculate breadth metrics
    with st.spinner("Calculating market breadth metrics..."):
        df_with_breadth = calculate_breadth_metrics(df, sector_master)

        # ===========================================
    # Stock Search Results (if any)
    # ===========================================
    if "selected_stock_symbol" in st.session_state and st.session_state.selected_stock_symbol:
        search_symbol = st.session_state.selected_stock_symbol

        st.markdown("---")
        st.header(f"🔍 Stock Details: {search_symbol}")

        stock_detail, stock_row = get_single_stock_detail(df_with_breadth, search_symbol)

        if not stock_detail.empty and not stock_row.empty:
            # Key metrics in columns
            col1, col2, col3, col4 = st.columns(4)

            close_price = stock_row.get("CLOSE_PRICE", 0)
            rs_21 = stock_row.get("RS_21", 0)
            rsi_14 = stock_row.get("RSI_14", 0)

            # SMA 50 trend
            sma_50_col = "SMA_50_CLOSE_PRICE"
            if sma_50_col in df_with_breadth.columns:
                sma_50 = stock_row.get(sma_50_col, 0)
                trend = "📈 Bullish" if close_price > sma_50 else "📉 Bearish"
            else:
                trend = "N/A"

            with col1:
                st.metric("Close Price", f"₹{close_price:.2f}")

            with col2:
                st.metric(
                    "RS-21",
                    f"{rs_21:.2f}",
                    delta="Strong" if rs_21 > 55 else "Weak",
                    delta_color="normal" if rs_21 > 55 else "inverse",
                )

            with col3:
                st.metric(
                    "RSI-14",
                    f"{rsi_14:.2f}",
                    delta="Bullish" if rsi_14 > 50 else "Bearish",
                    delta_color="normal" if rsi_14 > 50 else "inverse",
                )

            with col4:
                st.metric("Trend (SMA 50)", trend)

            # Detailed table
            col_table, col_analysis = st.columns([1, 1])

            with col_table:
                st.markdown("#### 📊 Complete Details")
                st.dataframe(stock_detail, hide_index=True, use_container_width=True, height=400)

            with col_analysis:
                st.markdown("#### 📈 Technical Analysis")

                # RS Analysis
                if rs_21 > 55:
                    st.success("✅ **Relative Strength**: Outperforming Nifty")
                else:
                    st.error("❌ **Relative Strength**: Underperforming Nifty")

                # RSI Analysis
                if rsi_14 > 70:
                    st.warning("⚠️ **RSI**: Overbought (>70) - Potential sell signal")
                elif rsi_14 > 50:
                    st.success("✅ **RSI**: Bullish momentum (>50)")
                elif rsi_14 > 30:
                    st.error("❌ **RSI**: Bearish momentum (<50)")
                else:
                    st.warning("⚠️ **RSI**: Oversold (<30) - Potential buy signal")

                # SMA Analysis
                sma_20_col = (
                    "SMA_21_CLOSE_PRICE" if "SMA_21_CLOSE_PRICE" in df_with_breadth.columns else "SMA_20_CLOSE_PRICE"
                )
                sma_100_col = "SMA_100_CLOSE_PRICE"
                sma_200_col = "SMA_200_CLOSE_PRICE"

                sma_signals = []
                if close_price > stock_row.get(sma_20_col, 0):
                    sma_signals.append("20")
                if close_price > stock_row.get(sma_50_col, 0):
                    sma_signals.append("50")
                if close_price > stock_row.get(sma_100_col, 0):
                    sma_signals.append("100")
                if close_price > stock_row.get(sma_200_col, 0):
                    sma_signals.append("200")

                if len(sma_signals) >= 3:
                    st.success(f"✅ **SMAs**: Above {', '.join(sma_signals)} - Strong uptrend")
                elif len(sma_signals) >= 1:
                    st.warning(f"⚠️ **SMAs**: Above {', '.join(sma_signals)} - Mixed")
                else:
                    st.error("❌ **SMAs**: Below major SMAs - Downtrend")

                # Download button
                st.markdown("---")
                csv_stock = stock_detail.to_csv(index=False)
                st.download_button(
                    label="📥 Download Stock Details",
                    data=csv_stock,
                    file_name=f"{search_symbol}_details_{selected_file['date']}.csv",
                    mime="text/csv",
                )

        st.markdown("---")

    # Create main tabs
    main_tab1, main_tab2, main_tab3 = st.tabs(["📈 INDEX BREADTH", "📊 SECTOR FUNDAMENTALS", "🏭 SECTOR BREADTH"])

    # ===========================================
    # TAB 1: INDEX BREADTH HEATMAP
    # ===========================================
    with main_tab1:
        st.header("Index-wise Market Breadth")
        st.markdown(f"**Date:** {selected_file['display']}")
        st.markdown(
            """
        Shows the percentage of stocks trading **ABOVE** key technical levels within each Nifty index.

        **Indicators:**
        - **RS-21**: Relative Strength using 21-period EMA
        - **RSI > 50**: RSI above 50 (bullish momentum)
        - **SMA 20/50/100/200**: Price above respective Simple Moving Averages
        """
        )

        # Calculate index breadth
        index_breadth = calculate_index_breadth(df_with_breadth)

        if not index_breadth.empty:
            # Create heatmap
            value_cols = ["RS-21", "RSI > 50", "SMA 20", "SMA 50", "SMA 100", "SMA 200"]
            fig = create_breadth_heatmap(index_breadth, "", value_cols, row_label_col="Index", show_count_col="Stocks")

            st.plotly_chart(fig, use_container_width=True)

            # ✅ NEW: Index Selection for Stock-Level Details
            st.markdown("---")
            st.subheader("🔍 View Individual Stocks")

            # Create mapping of display names to index codes
            index_name_to_code = {
                "Nifty 50": "NIFTY50",
                "Nifty Next 50": "NIFTYNEXT50",
                "Nifty 100": "NIFTY100",
                "Nifty 200": "NIFTY200",
                "Nifty 500": "NIFTY500",
                "Nifty Midcap 150": "NIFTYMIDCAP150",
                "Nifty Smallcap 250": "NIFTYSMALLCAP250",
                "Nifty Bank": "NIFTYBANK",
                "Nifty Financial Services": "NIFTYFINSERV",
                "Nifty Midcap Select": "NIFTYMIDCAPSELECT",
                "Nifty Microcap 250": "NIFTYMICROCAP250",
            }

            # Get available indices from the breadth data
            available_indices = index_breadth["Index"].tolist()

            selected_index = st.selectbox(
                "Select an index to view constituent stocks:",
                options=available_indices,
                help="Click to see detailed stock-level data",
            )

            if selected_index:
                # Get index code
                idx_code = index_name_to_code.get(selected_index, "")

                if idx_code:
                    # Get stock details
                    stock_details = get_index_stocks_detail(df_with_breadth, idx_code)

                    if not stock_details.empty:
                        st.markdown(f"### 📋 {selected_index} - Stock Details ({len(stock_details)} stocks)")

                        # Display table
                        st.dataframe(stock_details, hide_index=True, use_container_width=True, height=600)

                        # Download button
                        csv = stock_details.to_csv(index=False)
                        st.download_button(
                            label=f"📥 Download {selected_index} Stock Details",
                            data=csv,
                            file_name=f"{selected_index.replace(' ', '_')}_stocks_{selected_file['date']}.csv",
                            mime="text/csv",
                        )
                    else:
                        st.warning(f"No stock data available for {selected_index}")

            # Show data table
            with st.expander("📋 View Index Breadth Data Table"):
                display_df = index_breadth.copy()
                for col in value_cols:
                    if col in display_df.columns:
                        display_df[col] = display_df[col].apply(lambda x: f"{x:.1f}%")
                st.dataframe(display_df, hide_index=True, use_container_width=True)
        else:
            st.warning("⚠️ No index data available. Please ensure sector_master.csv has INDEX_MEMBERSHIP column.")

    # ===========================================
    # TAB 2: SECTOR FUNDAMENTALS TABLE
    # ===========================================
    with main_tab2:
        st.header(" Sector-wise Fundamental Data")
        st.markdown(f"**Date:** {selected_file['display']}")
        st.markdown(
            """
        Shows aggregated fundamental metrics for each sector/industry:
        - **MCap**: Total Market Capitalization
        - **Chg**: Daily Change in MCap
        - **Vol**: Total Trading Volume
        - **Del**: Total Delivery Volume
        """
        )

        # Calculate sector fundamentals
        sector_fundamentals = calculate_sector_fundamentals(df_with_breadth, df_prev)

        if not sector_fundamentals.empty:
            # Summary metrics
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                total_mcap = sector_fundamentals["MCap (Cr.)"].sum()
                st.metric("Total MCap", f"₹{total_mcap:,.0f} Cr")

            with col2:
                total_vol = sector_fundamentals["Vol (Cr.)"].sum()
                st.metric("Total Volume", f"{total_vol:,.2f} Cr")

            with col3:
                total_del = sector_fundamentals["Del (Cr.)"].sum()
                st.metric("Total Delivery", f"{total_del:,.2f} Cr")

            with col4:
                total_sectors = len(sector_fundamentals)
                st.metric("Sectors", f"{total_sectors}")

            st.markdown("---")

            # Display table with ALL sectors (no filtering)
            create_sector_fundamental_table(sector_fundamentals)

            # ✅ NEW: Sector Selection for Stock-Level Details
            st.markdown("---")
            st.subheader("🔍 View Individual Stocks by Sector")

            # Get available sectors from the fundamentals data
            available_sectors = sector_fundamentals["Sector"].tolist()

            selected_sector = st.selectbox(
                "Select a sector to view constituent stocks:",
                options=available_sectors,
                key="sector_fund_selector",
                help="Click to see detailed stock-level data",
            )

            if selected_sector:
                # Get stock details
                stock_details = get_sector_stocks_detail(df_with_breadth, selected_sector)

                if not stock_details.empty:
                    st.markdown(f"### 📋 {selected_sector} - Stock Details ({len(stock_details)} stocks)")

                    # Display table
                    st.dataframe(stock_details, hide_index=True, use_container_width=True, height=600)

                    # Download button for sector stocks
                    csv_sector = stock_details.to_csv(index=False)
                    st.download_button(
                        label=f"📥 Download {selected_sector} Stock Details",
                        data=csv_sector,
                        file_name=f"{selected_sector.replace(' ', '_').replace('/', '_')}_stocks_{selected_file['date']}.csv",
                        mime="text/csv",
                        key="download_sector_fund",
                    )
                else:
                    st.warning(f"No stock data available for {selected_sector}")

            # Download button for sector fundamentals
            csv = sector_fundamentals.to_csv(index=False)

            st.download_button(
                label="📥 Download Sector Fundamentals CSV",
                data=csv,
                file_name=f"sector_fundamentals_{selected_file['date']}.csv",
                mime="text/csv",
            )
        else:
            st.warning("⚠️ No sector fundamental data available.")

    # ===========================================
    # TAB 3: SECTOR BREADTH HEATMAP
    # ===========================================
    with main_tab3:
        st.header("Sector-wise Market Breadth")
        st.markdown(f"**Date:** {selected_file['display']}")
        st.markdown(
            """
        Shows the percentage of stocks trading **ABOVE** key technical levels within each sector.

        **Indicators:**
        - **RS-21**: Relative Strength using 21-period EMA
        - **RSI > 50**: RSI above 50 (bullish momentum)
        - **SMA 20/50/100**: Price above respective Simple Moving Averages
        """
        )

        # Calculate sector breadth
        sector_breadth = calculate_sector_breadth(df_with_breadth)

        if not sector_breadth.empty:
            # Display ALL sectors (no filtering)

            # Create heatmap
            value_cols = ["RS-21", "RSI > 50", "SMA 20", "SMA 50", "SMA 100"]
            fig = create_breadth_heatmap(
                sector_breadth, "", value_cols, row_label_col="Sector", show_count_col="Stocks"
            )

            st.plotly_chart(fig, use_container_width=True)

            # ✅ NEW: Sector Selection for Stock-Level Details
            st.markdown("---")
            st.subheader("🔍 View Individual Stocks by Sector")

            # Get available sectors from the breadth data
            available_sectors_breadth = sector_breadth["Sector"].tolist()

            selected_sector_breadth = st.selectbox(
                "Select a sector to view constituent stocks:",
                options=available_sectors_breadth,
                key="sector_breadth_selector",
                help="Click to see detailed stock-level data",
            )

            if selected_sector_breadth:
                # Get stock details
                stock_details_breadth = get_sector_stocks_detail(df_with_breadth, selected_sector_breadth)

                if not stock_details_breadth.empty:
                    st.markdown(
                        f"### 📋 {selected_sector_breadth} - Stock Details ({len(stock_details_breadth)} stocks)"
                    )

                    # Display table
                    st.dataframe(stock_details_breadth, hide_index=True, use_container_width=True, height=600)

                    # Download button for sector stocks
                    csv_sector_breadth = stock_details_breadth.to_csv(index=False)
                    st.download_button(
                        label=f"📥 Download {selected_sector_breadth} Stock Details",
                        data=csv_sector_breadth,
                        file_name=f"{selected_sector_breadth.replace(' ', '_').replace('/', '_')}_stocks_{selected_file['date']}.csv",
                        mime="text/csv",
                        key="download_sector_breadth",
                    )
                else:
                    st.warning(f"No stock data available for {selected_sector_breadth}")

            # Show data table
            with st.expander("📋 View Sector Breadth Data Table"):
                display_df = sector_breadth.copy()
                display_df["MCap (Cr.)"] = display_df["MCap (Cr.)"].apply(lambda x: f"{x:,.0f}")
                for col in value_cols:
                    if col in display_df.columns:
                        display_df[col] = display_df[col].apply(lambda x: f"{x:.1f}%")
                st.dataframe(display_df, hide_index=True, use_container_width=True)

            # Download button
            csv = sector_breadth.to_csv(index=False)
            st.download_button(
                label="📥 Download Sector Breadth CSV",
                data=csv,
                file_name=f"sector_breadth_{selected_file['date']}.csv",
                mime="text/csv",
            )
        else:
            st.warning("⚠️ No sector breadth data available. Please ensure sector_master.csv is available.")

    # ===========================================
    # Footer
    # ===========================================
    st.markdown("---")
    st.markdown(
        """
    <div style='text-align: center; color: #666; font-size: 12px;'>
        <p> NSE Market Breadth Dashboard | RS Formula: (EMA₂₁ of Stock / EMA₂₁ of Nifty) × 100</p>
        <p>Data Source: NSE Bhavcopy | Last Updated: """
        + selected_file["display"]
        + """</p>
    </div>
    """,
        unsafe_allow_html=True,
    )


# ===========================================
# Application Entry Point
# ===========================================

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"❌ Application Error: {str(e)}")
        st.exception(e)
