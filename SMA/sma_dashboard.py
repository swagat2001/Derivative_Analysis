"""
NSE CASH DATA - SMA TESTING DASHBOARD
======================================
Complete production-ready dashboard for SMA verification
Version: 1.8 (Added Two-Parameter Comparison Analysis)
"""


import glob
import os
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# ===========================================
# Configuration
# ===========================================
DATA_FOLDER = "C:/NSE_EOD_CASH_WITH_INDICATORS"
SMA_PERIODS = [3, 5, 8, 13, 21, 50, 100, 200]
SMA_COLUMNS = [
    "CLOSE_PRICE",
    "AVG_PRICE",
    "TTL_TRD_QNTY",
    "TURNOVER_LACS",
    "NO_OF_TRADES",
    "DELIV_QTY",
]


# ===========================================
# Page Configuration
# ===========================================
st.set_page_config(
    page_title="NSE SMA Calculator - Testing Dashboard",
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


@st.cache_data(ttl=600)
def get_unique_symbols_from_df(df: pd.DataFrame):
    """Get unique stock symbols from an already loaded dataframe"""
    if df is None or df.empty or "SYMBOL" not in df.columns:
        return []
    return sorted(df["SYMBOL"].dropna().unique().tolist())


def format_number(value, decimals: int = 2) -> str:
    """Format number for display"""
    if pd.isna(value):
        return "N/A"
    try:
        return f"{float(value):,.{decimals}f}"
    except Exception:
        return "N/A"


# ===========================================
# Main Application
# ===========================================


def main():
    """Main application function"""

    # Header Section
    st.title("  NSE Cash Data - SMA Calculator Testing Dashboard")
    st.markdown("**Real-time SMA verification and testing tool for NSE Bhavcopy data**")
    st.markdown("---")

    # Load available files
    with st.spinner("Loading available data files..."):
        available_files = load_available_files()

    if not available_files:
        st.error(f"❌ No data files found in: `{DATA_FOLDER}`")
        st.info(
            """
        **Please ensure:**
        1. You have run the SMA calculator script
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
        st.subheader("  Select Trading Date")
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

        if df.empty:
            st.error("Failed to load data for selected date")
            return

        available_symbols = get_unique_symbols_from_df(df)

        st.markdown("---")

        # Stock Selection
        st.subheader("🏢 Select Stock Symbol")

        # Search functionality
        search_term = st.text_input(
            "🔍 Search Symbol",
            "",
            placeholder="Type symbol name (e.g., TCS, RELIANCE)...",
            help="Search for stock symbol",
        )

        if search_term:
            filtered_symbols = [s for s in available_symbols if search_term.upper() in s.upper()]
            if not filtered_symbols:
                st.warning(f"No symbols found matching '{search_term}'")
                filtered_symbols = available_symbols
        else:
            filtered_symbols = available_symbols

        # Symbol dropdown
        default_idx = filtered_symbols.index("TCS") if "TCS" in filtered_symbols else 0
        selected_symbol = st.selectbox(
            "Stock Symbol",
            options=filtered_symbols,
            index=default_idx,
            help="Select stock to analyze",
        )

        st.markdown("---")

        # SMA Parameter Selection
        st.subheader("  SMA Analysis Parameter")
        selected_param = st.selectbox(
            "Price/Volume Column",
            options=SMA_COLUMNS,
            index=0,
            format_func=lambda x: x.replace("_", " ").title(),
            help="Select column for SMA analysis",
        )

        st.markdown("---")

        # SMA Periods Selection
        st.subheader("⏱️ SMA Periods to Display")
        selected_periods = st.multiselect(
            "Select Periods",
            options=SMA_PERIODS,
            default=[3, 5, 8, 13, 21, 50, 100, 200],
            help="Select which SMA periods to display (all 8 calculated periods included)",
        )

        if not selected_periods:
            st.warning("⚠️ Please select at least one period")

        st.markdown("---")

        # Historical Data Settings
        st.subheader("  Historical Chart Settings")
        max_hist = min(200, len(available_files))
        hist_days = st.slider(
            "Number of Days",
            min_value=10,
            max_value=max_hist,
            value=min(30, max_hist),
            step=10,
            help="Number of historical trading days to display",
        )

        show_chart = st.checkbox(
            "Show Historical Chart",
            value=True,
            help="Toggle historical trend chart",
        )

        st.markdown("---")

        # Information Box
        st.info(
            f"""
        **Data Summary:**
        - Total Stocks: {len(available_symbols):,}
        - Total Dates: {len(available_files)}
        - Date Range: {available_files[-1]['date']} to {available_files[0]['date']}
        - SMA Periods: {len(SMA_PERIODS)} (3, 5, 8, 13, 21, 50, 100, 200)
        """
        )

    # ===========================================
    # MAIN CONTENT AREA
    # ===========================================

    if not selected_symbol:
        st.warning("Please select a stock symbol from the sidebar")
        return

    # Filter data for selected symbol (single day)
    stock_data = df[df["SYMBOL"] == selected_symbol]

    if stock_data.empty:
        st.error(f"❌ No data found for **{selected_symbol}** on {selected_file['date']}")
        return

    stock_data = stock_data.iloc[0]

    # ===========================================
    # Key Metrics Display
    # ===========================================

    st.header(f"  {selected_symbol} - Key Metrics")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(label="  Symbol", value=selected_symbol)

    with col2:
        close_price = stock_data.get("CLOSE_PRICE", np.nan)
        if pd.notna(close_price):
            st.metric(label="  Close Price", value=f"₹{close_price:,.2f}")
        else:
            st.metric(label="  Close Price", value="N/A")

    with col3:
        param_value = stock_data.get(selected_param, np.nan)
        if pd.notna(param_value):
            param_display = selected_param.replace("_", " ").title()
            st.metric(
                label=f"  {param_display}",
                value=format_number(param_value, 2),
            )
        else:
            st.metric(label=f"  {selected_param}", value="N/A")

    with col4:
        rsi = stock_data.get("RSI_14", np.nan)
        if pd.notna(rsi):
            rsi_color = "🟢" if rsi < 30 else "🔴" if rsi > 70 else "🟡"
            st.metric(label=f"{rsi_color} RSI (14)", value=f"{rsi:.2f}")
        else:
            st.metric(label="🟡 RSI (14)", value="N/A")

    st.markdown("---")

    # ===========================================
    # SMA Values Display
    # ===========================================

    st.header(f"  SMA Analysis for {selected_param.replace('_', ' ').title()}")

    if not selected_periods:
        st.warning("⚠️ Please select at least one SMA period from the sidebar")
    else:
        # Display SMA metrics
        st.subheader("  Current SMA Values")

        # Create columns for SMA display (2 rows of 4)
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
                        st.metric(
                            label=f"SMA {period}",
                            value=format_number(sma_value, 2),
                        )

                    sma_data.append({"Period": period, "Value": float(sma_value)})
                else:
                    st.metric(label=f"SMA {period}", value="N/A")

        # SMA Comparison Table
        if sma_data:
            st.markdown("---")
            st.subheader("  Detailed SMA Comparison")

            sma_df = pd.DataFrame(sma_data)
            param_val = stock_data.get(selected_param, np.nan)

            if pd.notna(param_val):
                sma_df["Current Value"] = float(param_val)
                sma_df["Difference"] = sma_df["Value"] - float(param_val)
                sma_df["% Difference"] = ((sma_df["Value"] - float(param_val)) / float(param_val) * 100).round(2)

            display_df = sma_df.copy()
            display_df["Value"] = display_df["Value"].apply(lambda x: format_number(x, 2))

            if "Current Value" in display_df.columns:
                display_df["Current Value"] = display_df["Current Value"].apply(lambda x: format_number(x, 2))
                display_df["Difference"] = sma_df["Difference"].apply(lambda x: format_number(x, 2))
                display_df["% Difference"] = sma_df["% Difference"].apply(lambda x: f"{x:+.2f}%")

            st.dataframe(display_df, width="stretch", hide_index=True)

    st.markdown("---")

    # ===========================================
    # TWO-PARAMETER COMPARISON ANALYSIS - NEW FEATURE
    # ===========================================

    st.header("  Two-Parameter Comparison Analysis")

    st.markdown(
        """
    **Compare any two parameters side-by-side** to analyze relationships and trends.
    Useful for analyzing delivery percentage, volume vs trades, turnover patterns, etc.
    """
    )

    # Get all available numeric columns from the dataframe
    available_params = []
    for col in df.columns:
        if col in SMA_COLUMNS or col in ["RSI_14", "DELIV_PER"]:
            available_params.append(col)

    # Create two columns for parameter selection
    col_param1, col_param2 = st.columns(2)

    with col_param1:
        st.subheader("  Parameter 1")
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
        st.subheader("  Parameter 2")
        default_param2 = (
            "DELIV_QTY" if "DELIV_QTY" in available_params else available_params[1 if len(available_params) > 1 else 0]
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
        param1_value = stock_data.get(param1, np.nan)
        param2_value = stock_data.get(param2, np.nan)

        # Display current values
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric(label=f"  {param1.replace('_', ' ').title()}", value=format_number(param1_value, 2))

        with col2:
            st.metric(label=f"  {param2.replace('_', ' ').title()}", value=format_number(param2_value, 2))

        with col3:
            # Calculate ratio if both values are valid
            if pd.notna(param1_value) and pd.notna(param2_value) and param2_value != 0:
                ratio = param1_value / param2_value
                st.metric(label=f"  Ratio ({param1}/{param2})", value=format_number(ratio, 4))
            else:
                st.metric(label="  Ratio", value="N/A")

        st.markdown("---")

        # Historical comparison table
        st.subheader("  Historical Two-Parameter Comparison")

        if show_chart and len(available_files) > 1:
            with st.spinner(f"Loading comparison data for last {hist_days} days..."):
                # Prepare files to load
                selected_date_obj = selected_file["date_obj"]
                valid_files = [f for f in available_files if f["date_obj"] <= selected_date_obj]
                valid_files.sort(key=lambda x: x["date_obj"])
                files_to_load = valid_files[-hist_days:]

                comparison_data = []

                for file_info in files_to_load:
                    try:
                        comp_df = load_data(file_info["filepath"])
                        if not comp_df.empty and selected_symbol in comp_df["SYMBOL"].values:
                            comp_row = comp_df[comp_df["SYMBOL"] == selected_symbol].iloc[0]

                            p1_val = comp_row.get(param1, np.nan)
                            p2_val = comp_row.get(param2, np.nan)

                            row_data = {
                                "Date": file_info["date"],
                                "Display_Date": file_info["display"],
                                param1: float(p1_val) if pd.notna(p1_val) else np.nan,
                                param2: float(p2_val) if pd.notna(p2_val) else np.nan,
                            }

                            # Calculate ratio
                            if pd.notna(p1_val) and pd.notna(p2_val) and float(p2_val) != 0:
                                row_data["Ratio"] = float(p1_val) / float(p2_val)
                            else:
                                row_data["Ratio"] = np.nan

                            # Calculate percentage (param1 as % of param2)
                            if pd.notna(p1_val) and pd.notna(p2_val) and float(p2_val) != 0:
                                row_data["Percentage"] = (float(p1_val) / float(p2_val)) * 100
                            else:
                                row_data["Percentage"] = np.nan

                            comparison_data.append(row_data)

                    except Exception:
                        pass

            if comparison_data:
                comp_df = pd.DataFrame(comparison_data).sort_values("Date")

                # Display comparison table
                display_comp_df = comp_df.copy()
                display_comp_df = display_comp_df.set_index("Date").drop("Display_Date", axis=1, errors="ignore")

                # Format numeric columns
                for col in display_comp_df.columns:
                    if display_comp_df[col].dtype in ["float64", "float32"]:
                        display_comp_df[col] = display_comp_df[col].round(2)

                st.dataframe(display_comp_df, width="stretch", height=400)

                # Summary statistics
                st.subheader("  Summary Statistics")

                stats_col1, stats_col2, stats_col3, stats_col4 = st.columns(4)

                with stats_col1:
                    st.metric(
                        label=f"Avg {param1.replace('_', ' ').title()}", value=format_number(comp_df[param1].mean(), 2)
                    )

                with stats_col2:
                    st.metric(
                        label=f"Avg {param2.replace('_', ' ').title()}", value=format_number(comp_df[param2].mean(), 2)
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
                st.subheader("  Correlation Scatter Plot")

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
                            "text": f"{selected_symbol} - {param1.replace('_', ' ').title()} vs {param2.replace('_', ' ').title()}<br><sub>Correlation: {correlation:.3f}</sub>",
                            "x": 0.5,
                            "xanchor": "center",
                            "font": {"size": 14},
                        },
                        xaxis=dict(title=param1.replace("_", " ").title(), showgrid=True, gridcolor="#E0E0E0"),
                        yaxis=dict(title=param2.replace("_", " ").title(), showgrid=True, gridcolor="#E0E0E0"),
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
                    file_name=f"{selected_symbol}_{param1}_vs_{param2}_{files_to_load[0]['date']}_to_{files_to_load[-1]['date']}.csv",
                    mime="text/csv",
                    help="Download two-parameter comparison data as CSV",
                )
            else:
                st.warning(f"⚠️ No comparison data available for **{selected_symbol}**")
        else:
            st.info("💡 Enable 'Show Historical Chart' in sidebar to see historical comparison data")

    st.markdown("---")

    # ===========================================
    # Historical Trend Chart - WITH DUAL Y-AXIS
    # ===========================================

    if show_chart and len(available_files) > 1:
        st.header("  Historical Trend Analysis")

        with st.spinner(f"Loading last {hist_days} days of historical data..."):
            selected_date_obj = selected_file["date_obj"]
            valid_files = [f for f in available_files if f["date_obj"] <= selected_date_obj]
            valid_files.sort(key=lambda x: x["date_obj"])
            files_to_load = valid_files[-hist_days:]

            st.info(
                f"  Date Range: **{files_to_load[0]['date']}** to **{files_to_load[-1]['date']}** ({len(files_to_load)} days)"
            )

            historical_data = []
            progress_bar = st.progress(0)
            status_text = st.empty()

            for idx, file_info in enumerate(files_to_load):
                status_text.text(f"Loading {file_info['date']}... ({idx + 1}/{len(files_to_load)})")

                try:
                    hist_df = load_data(file_info["filepath"])
                    if not hist_df.empty and selected_symbol in hist_df["SYMBOL"].values:
                        hist_row = hist_df[hist_df["SYMBOL"] == selected_symbol].iloc[0]

                        row_data = {
                            "Date": file_info["date"],
                            "Display_Date": file_info["display"],
                        }

                        # Always add CLOSE_PRICE and AVG_PRICE
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

                        # Add SMA values (generic names SMA_{period})
                        for period in selected_periods:
                            col_name = f"SMA_{period}_{selected_param}"
                            sma_val_hist = hist_row.get(col_name, np.nan)
                            if pd.notna(sma_val_hist):
                                row_data[f"SMA_{period}"] = float(sma_val_hist)

                        # Add RSI_14 if available
                        rsi_val_hist = hist_row.get("RSI_14", np.nan)
                        if pd.notna(rsi_val_hist):
                            row_data["RSI_14"] = float(rsi_val_hist)

                        historical_data.append(row_data)

                    progress_bar.progress((idx + 1) / len(files_to_load))

                except Exception:
                    pass

            progress_bar.empty()
            status_text.empty()

        if historical_data:
            hist_df = pd.DataFrame(historical_data).sort_values("Date")

            # Create interactive chart with DUAL Y-AXIS
            st.subheader("  Interactive Price & SMA Chart")

            fig = go.Figure()

            # Determine if we need dual Y-axis
            use_dual_axis = selected_param not in ["CLOSE_PRICE", "AVG_PRICE"]

            # Calculate Y-axis range for PRICE series only
            price_columns = ["CLOSE_PRICE", "AVG_PRICE"]
            if selected_param in ["CLOSE_PRICE", "AVG_PRICE"]:
                price_columns += [f"SMA_{p}" for p in selected_periods]

            price_data = []
            for col in price_columns:
                if col in hist_df.columns:
                    price_data.extend(hist_df[col].dropna().tolist())

            if price_data:
                price_min = min(price_data)
                price_max = max(price_data)
                price_range = price_max - price_min
                y1_min = price_min - (price_range * 0.1)
                y1_max = price_max + (price_range * 0.1)
            else:
                y1_min = None
                y1_max = None

            # Close Price line - LEFT AXIS
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

            # Avg Price line - LEFT AXIS
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

            # SMA lines - on correct axis
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

            # Selected parameter - RIGHT AXIS (for volume/trade data)
            y2_min = y2_max = None
            if use_dual_axis and selected_param in hist_df.columns:
                param_data = hist_df[selected_param].dropna().tolist()

                # include SMA series sharing this axis
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
                    "text": f"{selected_symbol} - Price & SMA Analysis ({selected_param.replace('_', ' ').title()})",
                    "x": 0.5,
                    "xanchor": "center",
                    "font": {"size": 16},
                },
                "xaxis": {
                    "title": "Date",
                    "showgrid": True,
                    "gridcolor": "#E0E0E0",
                },
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
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1,
                    font={"size": 10},
                ),
                "template": "plotly_white",
                "margin": dict(l=80, r=80, t=80, b=60),
            }

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

            # ===========================================
            # RSI Historical Chart
            # ===========================================
            if "RSI_14" in hist_df.columns and hist_df["RSI_14"].notna().any():
                st.subheader("  RSI (14) Historical Trend")

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
                    y0=30,
                    y1=70,
                    fillcolor="LightGreen",
                    opacity=0.2,
                    line_width=0,
                    layer="below",
                )

                fig_rsi.add_hline(y=30, line_dash="dash", opacity=0.6)
                fig_rsi.add_hline(y=70, line_dash="dash", opacity=0.6)

                fig_rsi.update_layout(
                    title={
                        "text": f"{selected_symbol} - RSI 14",
                        "x": 0.5,
                        "xanchor": "center",
                        "font": {"size": 14},
                    },
                    xaxis=dict(title="Date", showgrid=True, gridcolor="#E0E0E0"),
                    yaxis=dict(
                        title="RSI",
                        range=[0, 100],
                        showgrid=True,
                        gridcolor="#E0E0E0",
                    ),
                    hovermode="x unified",
                    height=350,
                    template="plotly_white",
                    margin=dict(l=60, r=40, t=60, b=40),
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1,
                        font={"size": 9},
                    ),
                )

                st.plotly_chart(fig_rsi, use_container_width=True)

            # Historical Data Table
            st.markdown("---")
            st.subheader("  Historical Data Table")

            display_hist = hist_df.set_index("Date").drop("Display_Date", axis=1, errors="ignore")

            for col in display_hist.columns:
                if display_hist[col].dtype in ["float64", "float32"]:
                    display_hist[col] = display_hist[col].round(2)

            st.dataframe(display_hist, width="stretch")

            csv_hist = display_hist.to_csv()
            st.download_button(
                label="📥 Download Historical Data CSV",
                data=csv_hist,
                file_name=f"{selected_symbol}_{selected_param}_historical_{files_to_load[0]['date']}_to_{files_to_load[-1]['date']}.csv",
                mime="text/csv",
                help="Download complete historical data as CSV",
            )
        else:
            st.warning(f"⚠️ No historical data available for **{selected_symbol}**")

    # ===========================================
    # Raw Data Section
    # ===========================================

    st.markdown("---")

    with st.expander("🔍 View Complete Raw Data (All Columns)", expanded=False):
        st.subheader(f"Complete Stock Data for {selected_file['date']}")
        st.caption(f"Showing all available columns for **{selected_symbol}**")

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
        st.dataframe(raw_df, width="stretch", hide_index=True)

        stock_series = pd.DataFrame([stock_data])
        csv_single = stock_series.to_csv(index=False)
        st.download_button(
            label="📥 Download Single Day Raw Data CSV",
            data=csv_single,
            file_name=f"{selected_symbol}_{selected_file['date']}_raw_data.csv",
            mime="text/csv",
            help="Download complete raw data for this stock and date",
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
