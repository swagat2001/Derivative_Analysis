"""
NSE CASH DATA - UNIFIED DASHBOARD
==================================
Version: 4.0 - Complete Unified Dashboard
Combines: SMA Testing Dashboard + Market Breadth Analysis

FEATURES:
=========
FROM SMA DASHBOARD:
- Stock symbol search and selection
- SMA Analysis for any parameter (CLOSE_PRICE, AVG_PRICE, TTL_TRD_QNTY, etc.)
- SMA Values display with metrics and comparison
- Two-Parameter Comparison Analysis (correlation, scatter plots)
- Historical Trend Analysis (dual Y-axis charts)
- RSI Historical Chart
- Historical Data Table with download
- Raw Data viewer

FROM BREADTH DASHBOARD:
- Index-wise Market Breadth Heatmap
- Drill-down to see individual stocks with actual values
- Sector-wise Breadth Heatmap
- Sector Fundamentals Table (MCap, Volume, Delivery)
- Stock Search with Technical Analysis
- Download buttons for all data

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
INDEX_NAME_TO_CODE = {
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

INDICES_TO_CHECK = [
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

# ===========================================
# Page Configuration
# ===========================================
st.set_page_config(
    page_title="NSE Unified Dashboard - SMA & Market Breadth",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ===========================================
# Custom CSS Styling
# ===========================================
st.markdown(
    """
<style>
    .main { padding: 0rem 1rem; }
    .stAlert { margin-top: 1rem; }
    h1 { color: #1f77b4; padding-bottom: 1rem; border-bottom: 2px solid #e0e0e0; }
    h2 { color: #2ca02c; padding-top: 1rem; }
    h3 { color: #ff7f0e; }
    div[data-testid="stMetricValue"] { font-size: 1.5rem; font-weight: 600; }
    .breadth-cell { padding: 8px; text-align: center; font-weight: bold; }
</style>
""",
    unsafe_allow_html=True,
)


# ===========================================
# Data Loading Functions (Cached)
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


@st.cache_data(ttl=600)
def get_unique_symbols_from_df(df: pd.DataFrame):
    """Get unique stock symbols from dataframe"""
    if df is None or df.empty or "SYMBOL" not in df.columns:
        return []
    return sorted(df["SYMBOL"].dropna().unique().tolist())


# ===========================================
# Helper Functions
# ===========================================


def format_number(value, decimals: int = 2) -> str:
    """Format number for display"""
    if pd.isna(value):
        return "N/A"
    try:
        return f"{float(value):,.{decimals}f}"
    except Exception:
        return "N/A"


def format_lakhs_crores(value):
    """Format large numbers in Indian notation"""
    if pd.isna(value):
        return "N/A"
    try:
        value = float(value)
        if abs(value) >= 10000000:
            return f"{value/10000000:,.2f} Cr"
        elif abs(value) >= 100000:
            return f"{value/100000:,.2f} L"
        else:
            return f"{value:,.2f}"
    except:
        return "N/A"


def check_index_membership(membership_str, idx_code):
    """Check if idx_code is in the comma-separated membership string (exact match)"""
    if pd.isna(membership_str):
        return False
    indices = [x.strip() for x in str(membership_str).split(",")]
    return idx_code in indices


# ===========================================
# Market Breadth Calculation Functions
# ===========================================


def calculate_breadth_metrics(df, sector_master):
    """Calculate market breadth metrics for all stocks"""
    if not sector_master.empty:
        df = df.merge(sector_master[["SYMBOL", "NSE_INDUSTRY", "SECTOR", "INDEX_MEMBERSHIP"]], on="SYMBOL", how="left")

    # RS > 55 condition
    df["RS_ABOVE_55"] = df["RS_21"] > 55 if "RS_21" in df.columns else False

    # RSI > 50 (bullish momentum)
    df["RSI_ABOVE_50"] = df["RSI_14"] > 50 if "RSI_14" in df.columns else False

    # Price above SMAs
    for period in [20, 50, 100, 200]:
        sma_col = f"SMA_{period}_CLOSE_PRICE"
        if sma_col not in df.columns:
            closest_sma = f"SMA_{21 if period == 20 else period}_CLOSE_PRICE"
            if closest_sma in df.columns:
                df[f"ABOVE_SMA_{period}"] = df["CLOSE_PRICE"] > df[closest_sma]
            else:
                df[f"ABOVE_SMA_{period}"] = False
        else:
            df[f"ABOVE_SMA_{period}"] = df["CLOSE_PRICE"] > df[sma_col]

    return df


def calculate_index_breadth(df):
    """Calculate breadth metrics grouped by Nifty indices"""
    results = []

    for idx_code, idx_name, expected_count in INDICES_TO_CHECK:
        if "INDEX_MEMBERSHIP" not in df.columns:
            continue

        mask = df["INDEX_MEMBERSHIP"].apply(lambda x: check_index_membership(x, idx_code))
        idx_df = df[mask]

        if len(idx_df) == 0:
            continue

        total = len(idx_df)

        row = {
            "Index": idx_name,
            "Index_Code": idx_code,
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
    """Get detailed stock-level data for a specific index with actual values"""
    if "INDEX_MEMBERSHIP" not in df.columns:
        return pd.DataFrame()

    mask = df["INDEX_MEMBERSHIP"].apply(lambda x: check_index_membership(x, idx_code))
    idx_stocks = df[mask].copy()

    if len(idx_stocks) == 0:
        return pd.DataFrame()

    # Get SMA columns
    sma_20_col = "SMA_21_CLOSE_PRICE" if "SMA_21_CLOSE_PRICE" in idx_stocks.columns else "SMA_20_CLOSE_PRICE"
    sma_50_col = "SMA_50_CLOSE_PRICE"
    sma_100_col = "SMA_100_CLOSE_PRICE"
    sma_200_col = "SMA_200_CLOSE_PRICE"

    # Create result DataFrame with ACTUAL VALUES
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

    # Sort by RS-21 descending
    result = result.sort_values("RS-21", ascending=False).reset_index(drop=True)
    return result


def get_sector_stocks_detail(df, sector_name):
    """Get detailed stock-level data for a specific sector with actual values"""
    if "NSE_INDUSTRY" not in df.columns:
        return pd.DataFrame()

    sector_stocks = df[df["NSE_INDUSTRY"] == sector_name].copy()

    if len(sector_stocks) == 0:
        return pd.DataFrame()

    sma_20_col = "SMA_21_CLOSE_PRICE" if "SMA_21_CLOSE_PRICE" in sector_stocks.columns else "SMA_20_CLOSE_PRICE"
    sma_50_col = "SMA_50_CLOSE_PRICE"
    sma_100_col = "SMA_100_CLOSE_PRICE"
    sma_200_col = "SMA_200_CLOSE_PRICE"

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

    result = result.sort_values("RS-21", ascending=False).reset_index(drop=True)
    return result


def get_single_stock_detail(df, symbol):
    """Get detailed data for a single stock by symbol"""
    stock_data = df[df["SYMBOL"].str.upper() == symbol.upper()].copy()

    if len(stock_data) == 0:
        return pd.DataFrame(), pd.Series()

    sma_20_col = "SMA_21_CLOSE_PRICE" if "SMA_21_CLOSE_PRICE" in stock_data.columns else "SMA_20_CLOSE_PRICE"
    sma_50_col = "SMA_50_CLOSE_PRICE"
    sma_100_col = "SMA_100_CLOSE_PRICE"
    sma_200_col = "SMA_200_CLOSE_PRICE"

    stock = stock_data.iloc[0]

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

    return result, stock


def calculate_sector_breadth(df):
    """Calculate breadth metrics grouped by sector"""
    if "NSE_INDUSTRY" not in df.columns:
        return pd.DataFrame()

    results = []

    for industry in df["NSE_INDUSTRY"].dropna().unique():
        ind_df = df[df["NSE_INDUSTRY"] == industry]

        if len(ind_df) == 0:
            continue

        total = len(ind_df)
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

    if not result_df.empty:
        breadth_cols = ["RS-21", "RSI > 50", "SMA 20", "SMA 50", "SMA 100"]
        result_df["Avg_Score"] = result_df[breadth_cols].mean(axis=1)
        result_df = result_df.sort_values("Avg_Score", ascending=False).reset_index(drop=True)
        result_df = result_df.drop("Avg_Score", axis=1)

    return result_df


def calculate_sector_fundamentals(df, df_prev=None):
    """Calculate sector fundamental data"""
    if "NSE_INDUSTRY" not in df.columns:
        return pd.DataFrame()

    results = []

    for sector in df["NSE_INDUSTRY"].dropna().unique():
        sect_df = df[df["NSE_INDUSTRY"] == sector]

        if len(sect_df) == 0:
            continue

        stocks = len(sect_df)
        mcap = sect_df["TURNOVER_LACS"].sum() if "TURNOVER_LACS" in sect_df.columns else 0
        volume = sect_df["TTL_TRD_QNTY"].sum() if "TTL_TRD_QNTY" in sect_df.columns else 0
        delivery = sect_df["DELIV_QTY"].sum() if "DELIV_QTY" in sect_df.columns else 0

        vol_avg = volume / stocks if stocks > 0 else 0
        del_avg = delivery / stocks if stocks > 0 else 0

        mcap_chg, mcap_chg_pct = 0, 0
        if df_prev is not None and not df_prev.empty and "NSE_INDUSTRY" in df_prev.columns:
            prev_sect = df_prev[df_prev["NSE_INDUSTRY"] == sector]
            if len(prev_sect) > 0:
                prev_mcap = prev_sect["TURNOVER_LACS"].sum() if "TURNOVER_LACS" in prev_sect.columns else 0
                if prev_mcap > 0:
                    mcap_chg = mcap - prev_mcap
                    mcap_chg_pct = (mcap_chg / prev_mcap) * 100

        row = {
            "Sector": sector,
            "Stocks": stocks,
            "MCap (Cr.)": mcap / 100,
            "Chg (Cr.)": mcap_chg / 100,
            "Chg %": mcap_chg_pct,
            "Vol (Cr.)": volume / 10000000,
            "Vol Avg": vol_avg / 10000000,
            "Del (Cr.)": delivery / 10000000,
            "Del Avg": del_avg / 10000000,
        }
        results.append(row)

    result_df = pd.DataFrame(results)
    if not result_df.empty:
        result_df = result_df.sort_values("MCap (Cr.)", ascending=False)

    return result_df


# ===========================================
# Visualization Functions
# ===========================================


def create_breadth_heatmap(df, title, value_columns, row_label_col="Index", show_count_col="Stocks"):
    """Create a market breadth heatmap"""
    if df.empty:
        return None

    rows = df[row_label_col].tolist()
    counts = df[show_count_col].tolist() if show_count_col in df.columns else [0] * len(rows)

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

    fig = go.Figure(
        data=go.Heatmap(
            z=z_data,
            x=value_columns,
            y=[f"{row}<br><sub>{int(cnt)}</sub>" for row, cnt in zip(rows, counts)],
            text=text_data,
            texttemplate="%{text}",
            textfont={"size": 11, "color": "black", "family": "Arial Black"},
            colorscale=[
                [0, "#DC143C"],
                [0.2, "#FF6347"],
                [0.35, "#FFA500"],
                [0.5, "#FFFF00"],
                [0.65, "#9ACD32"],
                [0.8, "#32CD32"],
                [1, "#006400"],
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
        title={"text": title, "x": 0.5, "xanchor": "center", "font": {"size": 16}} if title else None,
        height=max(400, len(rows) * 45),
        margin=dict(l=200, r=100, t=60 if title else 30, b=40),
        xaxis=dict(title="Stocks Above", side="top", tickangle=0),
        yaxis=dict(title="", autorange="reversed"),
        plot_bgcolor="white",
        paper_bgcolor="white",
    )

    return fig


def create_sector_fundamental_table(df):
    """Create a styled table for sector fundamentals"""
    if df.empty:
        st.warning("No data available for sector fundamentals")
        return

    display_df = df.copy()

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
    st.title("📊 NSE Unified Dashboard")
    st.markdown("**Complete Market Analysis: SMA Testing + Market Breadth + Stock Analysis**")
    st.markdown("---")

    # Load available files
    with st.spinner("Loading available data files..."):
        available_files = load_available_files()

    if not available_files:
        st.error(f"❌ No data files found in: `{DATA_FOLDER}`")
        st.info(
            """
        **Please ensure:**
        1. You have run the SMA/EMA calculator script
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

        if df.empty:
            st.error("Failed to load data for selected date")
            return

        available_symbols = get_unique_symbols_from_df(df)

        st.markdown("---")

        # ===========================================
        # Stock Selection (Single clean selector)
        # ===========================================
        st.subheader("🔍 Select Stock")

        selected_symbol = st.selectbox(
            "Stock",
            options=available_symbols,
            index=available_symbols.index("TCS") if "TCS" in available_symbols else 0,
            help="Select stock for analysis",
            key="unified_stock_select",
        )

        # Store in session state for breadth details
        if selected_symbol:
            st.session_state.selected_stock_symbol = selected_symbol

        # Use same symbol for SMA analysis
        selected_symbol_sma = selected_symbol

        st.markdown("---")

        # ===========================================
        # SMA Settings
        # ===========================================
        st.subheader("📊 SMA Parameter")
        selected_param = st.selectbox(
            "Price/Volume Column",
            options=SMA_COLUMNS,
            index=0,
            format_func=lambda x: x.replace("_", " ").title(),
        )

        st.markdown("---")

        st.subheader("⏱️ SMA Periods")
        selected_periods = st.multiselect(
            "Select Periods",
            options=SMA_PERIODS,
            default=[3, 5, 8, 13, 21, 50, 100, 200],
        )

        if not selected_periods:
            st.warning("⚠️ Select at least one period")

        # Hidden settings with defaults
        hist_days = 30
        show_chart = True

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
    # Calculate breadth metrics
    # ===========================================
    with st.spinner("Calculating market breadth metrics..."):
        df_with_breadth = calculate_breadth_metrics(df, sector_master)

    # ===========================================
    # Stock Search Results (from Breadth Dashboard)
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

            # ONLY Complete Details Table - NO Technical Analysis
            st.markdown("#### 📊 Complete Details")
            st.dataframe(stock_detail, hide_index=True, use_container_width=True, height=400)

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

    # ===========================================
    # MAIN CONTENT - Tabs
    # ===========================================

    # Create main tabs - COMBINING BOTH DASHBOARDS
    tab_breadth_index, tab_breadth_sector, tab_sector_fund, tab_sma_analysis, tab_two_param = st.tabs(
        ["📈 INDEX BREADTH", "🏭 SECTOR BREADTH", "📊 SECTOR FUNDAMENTALS", "📉 SMA ANALYSIS", "🔄 TWO-PARAM COMPARISON"]
    )

    # ===========================================
    # TAB 1: INDEX BREADTH (from Breadth Dashboard)
    # ===========================================
    with tab_breadth_index:
        st.header("📈 Index-wise Market Breadth")
        st.markdown(f"**Date:** {selected_file['display']}")
        st.markdown(
            """
        Shows the percentage of stocks trading **ABOVE** key technical levels within each Nifty index.

        **Indicators:**
        - **RS-21**: Relative Strength using 21-period EMA (> 55)
        - **RSI > 50**: RSI above 50 (bullish momentum)
        - **SMA 20/50/100/200**: Price above respective Simple Moving Averages
        """
        )

        # Calculate index breadth
        index_breadth = calculate_index_breadth(df_with_breadth)

        if not index_breadth.empty:
            # Create heatmap
            value_cols = ["RS-21", "RSI > 50", "SMA 20", "SMA 50", "SMA 100", "SMA 200"]
            fig = create_breadth_heatmap(index_breadth, "", value_cols, "Index", "Stocks")

            st.plotly_chart(fig, use_container_width=True)

            # Index Selection for Stock-Level Details
            st.markdown("---")
            st.subheader("🔍 View Individual Stocks")

            available_indices = index_breadth["Index"].tolist()

            selected_index = st.selectbox(
                "Select an index to view constituent stocks:",
                options=available_indices,
                help="Click to see detailed stock-level data with actual values",
                key="index_breadth_selector",
            )

            if selected_index:
                idx_code = INDEX_NAME_TO_CODE.get(selected_index, "")

                if idx_code:
                    stock_details = get_index_stocks_detail(df_with_breadth, idx_code)

                    if not stock_details.empty:
                        st.markdown(f"### 📋 {selected_index} - Stock Details ({len(stock_details)} stocks)")
                        st.markdown("*Showing actual values - compare Close with SMA values*")

                        st.dataframe(stock_details, hide_index=True, use_container_width=True, height=600)

                        csv = stock_details.to_csv(index=False)
                        st.download_button(
                            label=f"📥 Download {selected_index} Stock Details",
                            data=csv,
                            file_name=f"{selected_index.replace(' ', '_')}_stocks_{selected_file['date']}.csv",
                            mime="text/csv",
                            key="download_index_stocks",
                        )
                    else:
                        st.warning(f"No stock data available for {selected_index}")

            # Show data table
            with st.expander("📋 View Index Breadth Data Table"):
                display_df = index_breadth.drop(columns=["Index_Code"], errors="ignore").copy()
                for col in value_cols:
                    if col in display_df.columns:
                        display_df[col] = display_df[col].apply(lambda x: f"{x:.1f}%")
                st.dataframe(display_df, hide_index=True, use_container_width=True)
        else:
            st.warning("⚠️ No index data available. Please ensure sector_master.csv has INDEX_MEMBERSHIP column.")

    # ===========================================
    # TAB 2: SECTOR BREADTH (from Breadth Dashboard)
    # ===========================================
    with tab_breadth_sector:
        st.header("🏭 Sector-wise Market Breadth")
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

        sector_breadth = calculate_sector_breadth(df_with_breadth)

        if not sector_breadth.empty:
            value_cols = ["RS-21", "RSI > 50", "SMA 20", "SMA 50", "SMA 100"]
            fig = create_breadth_heatmap(sector_breadth, "", value_cols, "Sector", "Stocks")

            if fig:
                st.plotly_chart(fig, use_container_width=True)

            # Sector Selection for Stock-Level Details
            st.markdown("---")
            st.subheader("🔍 View Individual Stocks by Sector")

            available_sectors_breadth = sector_breadth["Sector"].tolist()

            selected_sector_breadth = st.selectbox(
                "Select a sector to view constituent stocks:",
                options=available_sectors_breadth,
                key="sector_breadth_selector",
                help="Click to see detailed stock-level data with actual values",
            )

            if selected_sector_breadth:
                stock_details_breadth = get_sector_stocks_detail(df_with_breadth, selected_sector_breadth)

                if not stock_details_breadth.empty:
                    st.markdown(
                        f"### 📋 {selected_sector_breadth} - Stock Details ({len(stock_details_breadth)} stocks)"
                    )
                    st.markdown("*Showing actual values - compare Close with SMA values*")

                    st.dataframe(stock_details_breadth, hide_index=True, use_container_width=True, height=600)

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

            csv = sector_breadth.to_csv(index=False)
            st.download_button(
                label="📥 Download Sector Breadth CSV",
                data=csv,
                file_name=f"sector_breadth_{selected_file['date']}.csv",
                mime="text/csv",
                key="download_sector_breadth_full",
            )
        else:
            st.warning("⚠️ No sector breadth data available.")

    # ===========================================
    # TAB 3: SECTOR FUNDAMENTALS (from Breadth Dashboard)
    # ===========================================
    with tab_sector_fund:
        st.header("📊 Sector-wise Fundamental Data")
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
            create_sector_fundamental_table(sector_fundamentals)

            # Sector Selection for Stock-Level Details
            st.markdown("---")
            st.subheader("🔍 View Individual Stocks by Sector")

            available_sectors = sector_fundamentals["Sector"].tolist()

            selected_sector = st.selectbox(
                "Select a sector to view constituent stocks:",
                options=available_sectors,
                key="sector_fund_selector",
                help="Click to see detailed stock-level data",
            )

            if selected_sector:
                stock_details = get_sector_stocks_detail(df_with_breadth, selected_sector)

                if not stock_details.empty:
                    st.markdown(f"### 📋 {selected_sector} - Stock Details ({len(stock_details)} stocks)")

                    st.dataframe(stock_details, hide_index=True, use_container_width=True, height=600)

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

            csv = sector_fundamentals.to_csv(index=False)
            st.download_button(
                label="📥 Download Sector Fundamentals CSV",
                data=csv,
                file_name=f"sector_fundamentals_{selected_file['date']}.csv",
                mime="text/csv",
                key="download_sector_fundamentals",
            )
        else:
            st.warning("⚠️ No sector fundamental data available.")

    # ===========================================
    # TAB 4: SMA ANALYSIS (from SMA Dashboard)
    # ===========================================
    with tab_sma_analysis:
        st.header(f"📉 SMA Analysis: {selected_symbol_sma}")
        st.markdown(f"**Date:** {selected_file['display']}")

        if not selected_symbol_sma:
            st.warning("Please select a stock symbol from the sidebar")
        else:
            # Filter data for selected symbol
            stock_data = df[df["SYMBOL"] == selected_symbol_sma]

            if stock_data.empty:
                st.error(f"❌ No data found for **{selected_symbol_sma}** on {selected_file['date']}")
            else:
                stock_data = stock_data.iloc[0]

                # Key Metrics Display
                st.subheader("📊 Key Metrics")

                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    st.metric(label="📌 Symbol", value=selected_symbol_sma)

                with col2:
                    close_price = stock_data.get("CLOSE_PRICE", np.nan)
                    if pd.notna(close_price):
                        st.metric(label="💰 Close Price", value=f"₹{close_price:,.2f}")
                    else:
                        st.metric(label="💰 Close Price", value="N/A")

                with col3:
                    param_value = stock_data.get(selected_param, np.nan)
                    if pd.notna(param_value):
                        param_display = selected_param.replace("_", " ").title()
                        st.metric(label=f"📈 {param_display}", value=format_number(param_value, 2))
                    else:
                        st.metric(label=f"📈 {selected_param}", value="N/A")

                with col4:
                    rsi = stock_data.get("RSI_14", np.nan)
                    if pd.notna(rsi):
                        rsi_color = "🟢" if rsi < 30 else "🔴" if rsi > 70 else "🟡"
                        st.metric(label=f"{rsi_color} RSI (14)", value=f"{rsi:.2f}")
                    else:
                        st.metric(label="🟡 RSI (14)", value="N/A")

                st.markdown("---")

                # SMA Values Display
                st.subheader(f"📊 SMA Analysis for {selected_param.replace('_', ' ').title()}")

                if not selected_periods:
                    st.warning("⚠️ Please select at least one SMA period from the sidebar")
                else:
                    # Display SMA metrics
                    st.markdown("##### 📈 Current SMA Values")

                    # Create columns for SMA display
                    if len(selected_periods) <= 4:
                        cols = st.columns(len(selected_periods))
                    else:
                        cols1 = st.columns(4)
                        if len(selected_periods) > 4:
                            cols2 = st.columns(min(len(selected_periods) - 4, 4))
                            cols = cols1 + cols2
                        else:
                            cols = cols1

                    sma_data = []

                    for idx, period in enumerate(selected_periods):
                        col_name = f"SMA_{period}_{selected_param}"
                        sma_value = stock_data.get(col_name, np.nan)

                        with cols[idx]:
                            if pd.notna(sma_value):
                                param_val = stock_data.get(selected_param, np.nan)
                                if pd.notna(param_val):
                                    diff = sma_value - param_val
                                    diff_pct = (diff / param_val) * 100
                                    st.metric(
                                        label=f"SMA {period}",
                                        value=format_number(sma_value, 2),
                                        delta=f"{diff_pct:+.2f}%",
                                    )
                                else:
                                    st.metric(label=f"SMA {period}", value=format_number(sma_value, 2))

                                sma_data.append({"Period": period, "Value": float(sma_value)})
                            else:
                                st.metric(label=f"SMA {period}", value="N/A")

                    # SMA Comparison Table
                    if sma_data:
                        st.markdown("---")
                        st.subheader("📋 Detailed SMA Comparison")

                        sma_df = pd.DataFrame(sma_data)
                        param_val = stock_data.get(selected_param, np.nan)

                        if pd.notna(param_val):
                            sma_df["Current Value"] = float(param_val)
                            sma_df["Difference"] = sma_df["Value"] - float(param_val)
                            sma_df["% Difference"] = (
                                (sma_df["Value"] - float(param_val)) / float(param_val) * 100
                            ).round(2)

                        display_df = sma_df.copy()
                        display_df["Value"] = display_df["Value"].apply(lambda x: format_number(x, 2))

                        if "Current Value" in display_df.columns:
                            display_df["Current Value"] = display_df["Current Value"].apply(
                                lambda x: format_number(x, 2)
                            )
                            display_df["Difference"] = sma_df["Difference"].apply(lambda x: format_number(x, 2))
                            display_df["% Difference"] = sma_df["% Difference"].apply(lambda x: f"{x:+.2f}%")

                        st.dataframe(display_df, use_container_width=True, hide_index=True)

                # ===========================================
                # Historical Trend Chart (from SMA Dashboard)
                # ===========================================
                if show_chart and len(available_files) > 1:
                    st.markdown("---")
                    st.subheader("📈 Historical Trend Analysis")

                    with st.spinner(f"Loading last {hist_days} days of historical data..."):
                        selected_date_obj = selected_file["date_obj"]
                        valid_files = [f for f in available_files if f["date_obj"] <= selected_date_obj]
                        valid_files.sort(key=lambda x: x["date_obj"])
                        files_to_load = valid_files[-hist_days:]

                        st.info(
                            f"📅 Date Range: **{files_to_load[0]['date']}** to **{files_to_load[-1]['date']}** ({len(files_to_load)} days)"
                        )

                        historical_data = []
                        progress_bar = st.progress(0)
                        status_text = st.empty()

                        for idx, file_info in enumerate(files_to_load):
                            status_text.text(f"Loading {file_info['date']}... ({idx + 1}/{len(files_to_load)})")

                            try:
                                hist_df = load_data(file_info["filepath"])
                                if not hist_df.empty and selected_symbol_sma in hist_df["SYMBOL"].values:
                                    hist_row = hist_df[hist_df["SYMBOL"] == selected_symbol_sma].iloc[0]

                                    row_data = {
                                        "Date": file_info["date"],
                                        "Display_Date": file_info["display"],
                                    }

                                    # Add CLOSE_PRICE and AVG_PRICE
                                    close_val = hist_row.get("CLOSE_PRICE", np.nan)
                                    if pd.notna(close_val):
                                        row_data["CLOSE_PRICE"] = float(close_val)

                                    avg_val = hist_row.get("AVG_PRICE", np.nan)
                                    if pd.notna(avg_val):
                                        row_data["AVG_PRICE"] = float(avg_val)

                                    # Add selected parameter value
                                    param_val_hist = hist_row.get(selected_param, np.nan)
                                    if pd.notna(param_val_hist):
                                        row_data[selected_param] = float(param_val_hist)

                                    # Add SMA values
                                    for period in selected_periods:
                                        col_name = f"SMA_{period}_{selected_param}"
                                        sma_val_hist = hist_row.get(col_name, np.nan)
                                        if pd.notna(sma_val_hist):
                                            row_data[f"SMA_{period}"] = float(sma_val_hist)

                                    # Add RSI_14
                                    rsi_val_hist = hist_row.get("RSI_14", np.nan)
                                    if pd.notna(rsi_val_hist):
                                        row_data["RSI_14"] = float(rsi_val_hist)

                                    # Add RS_21 (Relative Strength)
                                    rs_val_hist = hist_row.get("RS_21", np.nan)
                                    if pd.notna(rs_val_hist):
                                        row_data["RS_21"] = float(rs_val_hist)

                                    historical_data.append(row_data)

                                progress_bar.progress((idx + 1) / len(files_to_load))
                            except Exception:
                                pass

                        progress_bar.empty()
                        status_text.empty()

                    if historical_data:
                        hist_df = pd.DataFrame(historical_data).sort_values("Date")

                        # Create interactive chart with DUAL Y-AXIS
                        st.markdown("##### 📊 Interactive Price & SMA Chart")

                        fig = go.Figure()

                        use_dual_axis = selected_param not in ["CLOSE_PRICE", "AVG_PRICE"]

                        # Calculate Y-axis ranges
                        price_columns = ["CLOSE_PRICE", "AVG_PRICE"]
                        if selected_param in ["CLOSE_PRICE", "AVG_PRICE"]:
                            price_columns += [f"SMA_{p}" for p in selected_periods]

                        price_data = []
                        for col in price_columns:
                            if col in hist_df.columns:
                                price_data.extend(hist_df[col].dropna().tolist())

                        y1_min = y1_max = None
                        if price_data:
                            price_min = min(price_data)
                            price_max = max(price_data)
                            price_range = price_max - price_min
                            y1_min = price_min - (price_range * 0.1)
                            y1_max = price_max + (price_range * 0.1)

                        # Close Price line
                        if "CLOSE_PRICE" in hist_df.columns:
                            fig.add_trace(
                                go.Scatter(
                                    x=hist_df["Date"],
                                    y=hist_df["CLOSE_PRICE"],
                                    name="Close Price",
                                    line=dict(color="black", width=3),
                                    mode="lines+markers",
                                    marker=dict(size=6),
                                    yaxis="y",
                                    hovertemplate="<b>Close Price</b><br>₹%{y:,.2f}<extra></extra>",
                                )
                            )

                        # Avg Price line
                        if "AVG_PRICE" in hist_df.columns:
                            fig.add_trace(
                                go.Scatter(
                                    x=hist_df["Date"],
                                    y=hist_df["AVG_PRICE"],
                                    name="Avg Price",
                                    line=dict(color="#555555", width=2.5, dash="dot"),
                                    mode="lines",
                                    yaxis="y",
                                    hovertemplate="<b>Avg Price</b><br>₹%{y:,.2f}<extra></extra>",
                                )
                            )

                        # SMA lines
                        colors = [
                            "#1f77b4",
                            "#2ca02c",
                            "#d62728",
                            "#ff7f0e",
                            "#9467bd",
                            "#8c564b",
                            "#e377c2",
                            "#7f7f7f",
                        ]

                        for idx, period in enumerate(selected_periods):
                            col_name = f"SMA_{period}"
                            if col_name in hist_df.columns:
                                sma_axis = "y" if selected_param in ["CLOSE_PRICE", "AVG_PRICE"] else "y2"
                                fig.add_trace(
                                    go.Scatter(
                                        x=hist_df["Date"],
                                        y=hist_df[col_name],
                                        name=f"SMA {period}",
                                        line=dict(color=colors[idx % len(colors)], width=2, dash="solid"),
                                        mode="lines",
                                        yaxis=sma_axis,
                                        hovertemplate=f"<b>SMA {period}</b><br>%{{y:,.2f}}<extra></extra>",
                                    )
                                )

                        # RS-21 line (on secondary Y-axis - right side)
                        if "RS_21" in hist_df.columns and hist_df["RS_21"].notna().any():
                            fig.add_trace(
                                go.Scatter(
                                    x=hist_df["Date"],
                                    y=hist_df["RS_21"],
                                    name="RS-21",
                                    line=dict(color="#FF1493", width=3, dash="solid"),  # Deep Pink color
                                    mode="lines+markers",
                                    marker=dict(size=5, symbol="diamond"),
                                    yaxis="y3",
                                    hovertemplate="<b>RS-21</b><br>%{y:.2f}<extra></extra>",
                                )
                            )

                        # Y2 axis for volume data
                        y2_min = y2_max = None
                        if use_dual_axis and selected_param in hist_df.columns:
                            param_data = hist_df[selected_param].dropna().tolist()

                            for period in selected_periods:
                                col_name = f"SMA_{period}"
                                if col_name in hist_df.columns:
                                    param_data.extend(hist_df[col_name].dropna().tolist())

                            if param_data:
                                param_min = min(param_data)
                                param_max = max(param_data)
                                param_range = param_max - param_min
                                y2_min = param_min - (param_range * 0.1)
                                y2_max = param_max + (param_range * 0.1)

                            fig.add_trace(
                                go.Scatter(
                                    x=hist_df["Date"],
                                    y=hist_df[selected_param],
                                    name=selected_param.replace("_", " ").title(),
                                    line=dict(color="#FF6B6B", width=2.5, dash="dashdot"),
                                    mode="lines",
                                    yaxis="y2",
                                    hovertemplate=f"<b>{selected_param.replace('_', ' ').title()}</b><br>%{{y:,.2f}}<extra></extra>",
                                )
                            )

                        # Layout configuration
                        layout_config = {
                            "title": {
                                "text": f"{selected_symbol_sma} - Price & SMA Analysis ({selected_param.replace('_', ' ').title()})",
                                "x": 0.5,
                                "xanchor": "center",
                                "font": {"size": 16},
                            },
                            "xaxis": {"title": "Date", "showgrid": True, "gridcolor": "#E0E0E0"},
                            "yaxis": {
                                "title": "Price (₹)",
                                "showgrid": True,
                                "gridcolor": "#E0E0E0",
                                "side": "left",
                                "range": [y1_min, y1_max],
                            },
                            "hovermode": "x unified",
                            "height": 650,
                            "legend": dict(
                                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font={"size": 10}
                            ),
                            "template": "plotly_white",
                            "margin": dict(l=80, r=120, t=80, b=60),  # Increased right margin for RS axis
                        }

                        # Add Y3 axis for RS-21 (far right)
                        if "RS_21" in hist_df.columns and hist_df["RS_21"].notna().any():
                            rs_data = hist_df["RS_21"].dropna().tolist()
                            if rs_data:
                                rs_min = min(rs_data)
                                rs_max = max(rs_data)
                                rs_range = rs_max - rs_min if rs_max != rs_min else 10
                                # Add padding and include reference line at 55
                                y3_min = min(rs_min - (rs_range * 0.2), 40)
                                y3_max = max(rs_max + (rs_range * 0.2), 70)

                                layout_config["yaxis3"] = {
                                    "title": dict(text="RS-21", font=dict(color="#FF1493")),
                                    "tickfont": {"color": "#FF1493"},
                                    "anchor": "free",
                                    "overlaying": "y",
                                    "side": "right",
                                    "showgrid": False,
                                    "range": [y3_min, y3_max],
                                }
                                # Adjust x-axis domain to make room for RS axis
                                layout_config["xaxis"]["domain"] = [0, 0.92]

                        if use_dual_axis:
                            layout_config["yaxis2"] = {
                                "title": selected_param.replace("_", " ").title(),
                                "overlaying": "y",
                                "side": "right",
                                "showgrid": False,
                                "range": [y2_min, y2_max],
                            }

                        fig.update_layout(**layout_config)
                        st.plotly_chart(fig, use_container_width=True)

                        # RSI Historical Chart
                        if "RSI_14" in hist_df.columns and hist_df["RSI_14"].notna().any():
                            st.markdown("##### 📈 RSI (14) Historical Trend")

                            fig_rsi = go.Figure()

                            fig_rsi.add_trace(
                                go.Scatter(
                                    x=hist_df["Date"],
                                    y=hist_df["RSI_14"],
                                    name="RSI 14",
                                    mode="lines+markers",
                                    line=dict(width=2),
                                    marker=dict(size=5),
                                    hovertemplate="<b>RSI 14</b><br>%{y:.2f}<extra></extra>",
                                )
                            )

                            fig_rsi.add_hrect(
                                y0=30, y1=70, fillcolor="LightGreen", opacity=0.2, line_width=0, layer="below"
                            )
                            fig_rsi.add_hline(y=30, line_dash="dash", opacity=0.6)
                            fig_rsi.add_hline(y=70, line_dash="dash", opacity=0.6)

                            fig_rsi.update_layout(
                                title={
                                    "text": f"{selected_symbol_sma} - RSI 14",
                                    "x": 0.5,
                                    "xanchor": "center",
                                    "font": {"size": 14},
                                },
                                xaxis=dict(title="Date", showgrid=True, gridcolor="#E0E0E0"),
                                yaxis=dict(title="RSI", range=[0, 100], showgrid=True, gridcolor="#E0E0E0"),
                                hovermode="x unified",
                                height=350,
                                template="plotly_white",
                                margin=dict(l=60, r=40, t=60, b=40),
                                legend=dict(
                                    orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font={"size": 9}
                                ),
                            )

                            st.plotly_chart(fig_rsi, use_container_width=True)

                        # Historical Data Table
                        st.markdown("---")
                        st.subheader("📋 Historical Data Table")

                        display_hist = hist_df.set_index("Date").drop("Display_Date", axis=1, errors="ignore")

                        for col in display_hist.columns:
                            if display_hist[col].dtype in ["float64", "float32"]:
                                display_hist[col] = display_hist[col].round(2)

                        st.dataframe(display_hist, use_container_width=True)

                        csv_hist = display_hist.to_csv()
                        st.download_button(
                            label="📥 Download Historical Data CSV",
                            data=csv_hist,
                            file_name=f"{selected_symbol_sma}_{selected_param}_historical_{files_to_load[0]['date']}_to_{files_to_load[-1]['date']}.csv",
                            mime="text/csv",
                            help="Download complete historical data as CSV",
                        )
                    else:
                        st.warning(f"⚠️ No historical data available for **{selected_symbol_sma}**")

                # Raw Data Section
                st.markdown("---")
                with st.expander("🔍 View Complete Raw Data (All Columns)", expanded=False):
                    st.subheader(f"Complete Stock Data for {selected_file['date']}")
                    st.caption(f"Showing all available columns for **{selected_symbol_sma}**")

                    raw_data = []
                    for col in stock_data.index:
                        value = stock_data[col]
                        if pd.isna(value):
                            display_value = "N/A"
                        elif isinstance(value, (int, float, np.number)):
                            display_value = f"{float(value):,.4f}"
                        else:
                            display_value = str(value)
                        raw_data.append({"Column": col, "Value": display_value})

                    raw_df = pd.DataFrame(raw_data)
                    st.dataframe(raw_df, use_container_width=True, hide_index=True)

                    stock_series = pd.DataFrame([stock_data])
                    csv_single = stock_series.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Single Day Raw Data CSV",
                        data=csv_single,
                        file_name=f"{selected_symbol_sma}_{selected_file['date']}_raw_data.csv",
                        mime="text/csv",
                        help="Download complete raw data for this stock and date",
                    )

    # ===========================================
    # TAB 5: TWO-PARAMETER COMPARISON (from SMA Dashboard)
    # ===========================================
    with tab_two_param:
        st.header("🔄 Two-Parameter Comparison Analysis")
        st.markdown(f"**Date:** {selected_file['display']} | **Stock:** {selected_symbol_sma}")

        st.markdown(
            """
        **Compare any two parameters side-by-side** to analyze relationships and trends.
        Useful for analyzing delivery percentage, volume vs trades, turnover patterns, etc.
        """
        )

        if not selected_symbol_sma:
            st.warning("Please select a stock symbol from the sidebar")
        else:
            stock_data_2p = df[df["SYMBOL"] == selected_symbol_sma]

            if stock_data_2p.empty:
                st.error(f"❌ No data found for **{selected_symbol_sma}**")
            else:
                stock_data_2p = stock_data_2p.iloc[0]

                # Get available numeric columns
                available_params = []
                for col in df.columns:
                    if col in SMA_COLUMNS or col in ["RSI_14", "DELIV_PER"]:
                        available_params.append(col)

                # Create two columns for parameter selection
                col_param1, col_param2 = st.columns(2)

                with col_param1:
                    st.subheader("📊 Parameter 1")
                    default_param1 = "TTL_TRD_QNTY" if "TTL_TRD_QNTY" in available_params else available_params[0]
                    param1 = st.selectbox(
                        "Select First Parameter",
                        options=available_params,
                        index=available_params.index(default_param1),
                        format_func=lambda x: x.replace("_", " ").title(),
                        help="Select the first parameter for comparison",
                        key="param1_select",
                    )

                with col_param2:
                    st.subheader("📊 Parameter 2")
                    default_param2 = (
                        "DELIV_QTY"
                        if "DELIV_QTY" in available_params
                        else available_params[1 if len(available_params) > 1 else 0]
                    )
                    param2 = st.selectbox(
                        "Select Second Parameter",
                        options=available_params,
                        index=available_params.index(default_param2),
                        format_func=lambda x: x.replace("_", " ").title(),
                        help="Select the second parameter for comparison",
                        key="param2_select",
                    )

                if param1 == param2:
                    st.warning("⚠️ Please select two different parameters for meaningful comparison")
                else:
                    # Get values for current stock
                    param1_value = stock_data_2p.get(param1, np.nan)
                    param2_value = stock_data_2p.get(param2, np.nan)

                    # Display current values
                    col1, col2, col3 = st.columns(3)

                    with col1:
                        st.metric(label=f"📊 {param1.replace('_', ' ').title()}", value=format_number(param1_value, 2))

                    with col2:
                        st.metric(label=f"📊 {param2.replace('_', ' ').title()}", value=format_number(param2_value, 2))

                    with col3:
                        if pd.notna(param1_value) and pd.notna(param2_value) and param2_value != 0:
                            ratio = param1_value / param2_value
                            st.metric(label=f"📈 Ratio ({param1}/{param2})", value=format_number(ratio, 4))
                        else:
                            st.metric(label="📈 Ratio", value="N/A")

                    st.markdown("---")

                    # Historical comparison
                    if show_chart and len(available_files) > 1:
                        st.subheader("📈 Historical Two-Parameter Comparison")

                        with st.spinner(f"Loading comparison data for last {hist_days} days..."):
                            selected_date_obj = selected_file["date_obj"]
                            valid_files = [f for f in available_files if f["date_obj"] <= selected_date_obj]
                            valid_files.sort(key=lambda x: x["date_obj"])
                            files_to_load = valid_files[-hist_days:]

                            comparison_data = []

                            for file_info in files_to_load:
                                try:
                                    comp_df = load_data(file_info["filepath"])
                                    if not comp_df.empty and selected_symbol_sma in comp_df["SYMBOL"].values:
                                        comp_row = comp_df[comp_df["SYMBOL"] == selected_symbol_sma].iloc[0]

                                        p1_val = comp_row.get(param1, np.nan)
                                        p2_val = comp_row.get(param2, np.nan)

                                        row_data = {
                                            "Date": file_info["date"],
                                            "Display_Date": file_info["display"],
                                            param1: float(p1_val) if pd.notna(p1_val) else np.nan,
                                            param2: float(p2_val) if pd.notna(p2_val) else np.nan,
                                        }

                                        if pd.notna(p1_val) and pd.notna(p2_val) and float(p2_val) != 0:
                                            row_data["Ratio"] = float(p1_val) / float(p2_val)
                                            row_data["Percentage"] = (float(p1_val) / float(p2_val)) * 100
                                        else:
                                            row_data["Ratio"] = np.nan
                                            row_data["Percentage"] = np.nan

                                        comparison_data.append(row_data)
                                except Exception:
                                    pass

                        if comparison_data:
                            comp_df = pd.DataFrame(comparison_data).sort_values("Date")

                            # Display comparison table
                            display_comp_df = comp_df.copy()
                            display_comp_df = display_comp_df.set_index("Date").drop(
                                "Display_Date", axis=1, errors="ignore"
                            )

                            for col in display_comp_df.columns:
                                if display_comp_df[col].dtype in ["float64", "float32"]:
                                    display_comp_df[col] = display_comp_df[col].round(2)

                            st.dataframe(display_comp_df, use_container_width=True, height=400)

                            # Summary statistics
                            st.subheader("📊 Summary Statistics")

                            stats_col1, stats_col2, stats_col3, stats_col4 = st.columns(4)

                            with stats_col1:
                                st.metric(
                                    label=f"Avg {param1.replace('_', ' ').title()}",
                                    value=format_number(comp_df[param1].mean(), 2),
                                )

                            with stats_col2:
                                st.metric(
                                    label=f"Avg {param2.replace('_', ' ').title()}",
                                    value=format_number(comp_df[param2].mean(), 2),
                                )

                            with stats_col3:
                                st.metric(label="Avg Ratio", value=format_number(comp_df["Ratio"].mean(), 4))

                            with stats_col4:
                                st.metric(
                                    label="Avg Percentage",
                                    value=f"{comp_df['Percentage'].mean():.2f}%"
                                    if pd.notna(comp_df["Percentage"].mean())
                                    else "N/A",
                                )

                            # Correlation scatter plot
                            st.subheader("📈 Correlation Scatter Plot")

                            fig_scatter = go.Figure()

                            valid_data = comp_df.dropna(subset=[param1, param2])

                            if len(valid_data) > 0:
                                fig_scatter.add_trace(
                                    go.Scatter(
                                        x=valid_data[param1],
                                        y=valid_data[param2],
                                        mode="markers",
                                        marker=dict(
                                            size=10,
                                            color=list(range(len(valid_data))),
                                            colorscale="Viridis",
                                            showscale=True,
                                            colorbar=dict(title="Time"),
                                        ),
                                        text=valid_data["Display_Date"],
                                        hovertemplate=f"<b>%{{text}}</b><br>{param1}: %{{x:,.2f}}<br>{param2}: %{{y:,.2f}}<extra></extra>",
                                    )
                                )

                                # Add trendline
                                if len(valid_data) >= 2:
                                    z = np.polyfit(valid_data[param1], valid_data[param2], 1)
                                    p = np.poly1d(z)
                                    x_trend = np.linspace(valid_data[param1].min(), valid_data[param1].max(), 100)

                                    fig_scatter.add_trace(
                                        go.Scatter(
                                            x=x_trend,
                                            y=p(x_trend),
                                            mode="lines",
                                            name="Trend Line",
                                            line=dict(color="red", width=2, dash="dash"),
                                            hoverinfo="skip",
                                        )
                                    )

                                # Calculate correlation
                                correlation = valid_data[param1].corr(valid_data[param2])

                                fig_scatter.update_layout(
                                    title={
                                        "text": f"{selected_symbol_sma} - {param1.replace('_', ' ').title()} vs {param2.replace('_', ' ').title()}<br><sub>Correlation: {correlation:.3f}</sub>",
                                        "x": 0.5,
                                        "xanchor": "center",
                                        "font": {"size": 14},
                                    },
                                    xaxis=dict(
                                        title=param1.replace("_", " ").title(), showgrid=True, gridcolor="#E0E0E0"
                                    ),
                                    yaxis=dict(
                                        title=param2.replace("_", " ").title(), showgrid=True, gridcolor="#E0E0E0"
                                    ),
                                    hovermode="closest",
                                    height=500,
                                    template="plotly_white",
                                    showlegend=True,
                                )

                                st.plotly_chart(fig_scatter, use_container_width=True)

                                # Interpretation
                                if abs(correlation) > 0.7:
                                    corr_strength = "Strong"
                                    corr_color = "🟢"
                                elif abs(correlation) > 0.4:
                                    corr_strength = "Moderate"
                                    corr_color = "🟡"
                                else:
                                    corr_strength = "Weak"
                                    corr_color = "🔴"

                                corr_direction = "positive" if correlation > 0 else "negative"

                                st.info(
                                    f"{corr_color} **Correlation Interpretation:** {corr_strength} {corr_direction} correlation ({correlation:.3f})"
                                )

                            else:
                                st.warning("Not enough valid data points for scatter plot")

                            # Download button
                            csv_comparison = display_comp_df.to_csv()
                            st.download_button(
                                label="📥 Download Comparison Data CSV",
                                data=csv_comparison,
                                file_name=f"{selected_symbol_sma}_{param1}_vs_{param2}_{files_to_load[0]['date']}_to_{files_to_load[-1]['date']}.csv",
                                mime="text/csv",
                                help="Download two-parameter comparison data as CSV",
                            )
                        else:
                            st.warning(f"⚠️ No comparison data available for **{selected_symbol_sma}**")


# ===========================================
# Application Entry Point
# ===========================================

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"❌ Application Error: {str(e)}")
        st.exception(e)
