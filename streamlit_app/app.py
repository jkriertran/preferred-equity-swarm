"""
Preferred Equity Analysis Swarm: Streamlit Demo
=================================================
A web interface for the multi-agent preferred equity analysis system.
Allows users to enter a preferred stock ticker and view the swarm's analysis.
"""

import sys
import os
import json
import streamlit as st
import plotly.graph_objects as go
import pandas as pd

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.agents.hello_world_swarm import analyze_preferred
from src.data.market_data import get_price_history


# ---------------------------------------------------------------------------
# Page Configuration
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Preferred Equity Swarm",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.title("Preferred Equity Swarm")
    st.caption("MSBA Capstone Project")
    
    st.markdown("---")
    
    st.subheader("About")
    st.write(
        "This demo showcases a multi-agent AI swarm that analyzes "
        "preferred equity securities. The swarm coordinates specialized "
        "agents to evaluate market data, interest rate context, and "
        "produce a synthesized analysis."
    )
    
    st.markdown("---")
    
    st.subheader("Swarm Agents")
    st.markdown("""
    **Active in this demo:**
    1. Market Data Agent
    2. Rate Context Agent  
    3. Synthesis Agent (Gemini)
    
    **Coming in Phase 2+:**
    4. Prospectus Parsing Agent
    5. Credit Analysis Agent
    6. Call Probability Agent
    7. Tax & Yield Agent
    8. Relative Value Agent
    """)
    
    st.markdown("---")
    st.caption("Phase 0: Hello World Prototype")


# ---------------------------------------------------------------------------
# Main Content
# ---------------------------------------------------------------------------

st.title("Preferred Equity Analysis Swarm")
st.markdown("Enter a preferred stock ticker to run the multi-agent analysis.")

# Sample tickers for easy access
col1, col2 = st.columns([3, 1])

with col1:
    ticker = st.text_input(
        "Preferred Stock Ticker",
        value="BAC-PL",
        placeholder="e.g., BAC-PL, JPM-PD, WFC-PL",
        help="Enter a preferred stock ticker symbol. Use the format ISSUER-P[SERIES] (e.g., BAC-PL for Bank of America Series L)."
    )

with col2:
    st.markdown("<br>", unsafe_allow_html=True)
    analyze_button = st.button("Analyze", type="primary", use_container_width=True)

# Quick-pick buttons
st.caption("Quick picks:")
quick_cols = st.columns(6)
quick_tickers = ["BAC-PL", "JPM-PD", "WFC-PL", "MS-PA", "GS-PD", "C-PJ"]
for i, qt in enumerate(quick_tickers):
    with quick_cols[i]:
        if st.button(qt, use_container_width=True):
            ticker = qt
            analyze_button = True

st.markdown("---")


# ---------------------------------------------------------------------------
# Analysis Execution
# ---------------------------------------------------------------------------

if analyze_button and ticker:
    # Agent execution with progress tracking
    progress_container = st.container()
    
    with progress_container:
        st.subheader(f"Analyzing: {ticker}")
        
        # Progress bar and status
        progress_bar = st.progress(0, text="Initializing swarm...")
        status_placeholder = st.empty()
        
        # Run the swarm
        try:
            status_placeholder.info("Agent 1/3: Market Data Agent fetching security data...")
            progress_bar.progress(15, text="Market Data Agent running...")
            
            result = analyze_preferred(ticker)
            
            progress_bar.progress(100, text="Analysis complete!")
            status_placeholder.success("All agents completed successfully.")
            
        except Exception as e:
            st.error(f"Analysis failed: {str(e)}")
            st.stop()
    
    st.markdown("---")
    
    # ---------------------------------------------------------------------------
    # Results Display
    # ---------------------------------------------------------------------------
    
    # Row 1: Key Metrics
    st.subheader("Key Metrics")
    
    market_data = result.get("market_data", {})
    rate_data = result.get("rate_data", {})
    
    metric_cols = st.columns(4)
    
    with metric_cols[0]:
        price = market_data.get("price", "N/A")
        st.metric("Current Price", f"${price:,.2f}" if isinstance(price, (int, float)) else "N/A")
    
    with metric_cols[1]:
        div_rate = market_data.get("dividend_rate", None)
        st.metric("Annual Dividend", f"${div_rate:,.2f}" if div_rate else "N/A")
    
    with metric_cols[2]:
        div_yield = market_data.get("dividend_yield", None)
        if div_yield:
            # yfinance may return yield as decimal (0.059) or percentage (5.9)
            # Normalize to percentage for display
            yield_pct = div_yield if div_yield > 1 else div_yield * 100
            st.metric("Current Yield", f"{yield_pct:.2f}%")
        else:
            yield_pct = None
            st.metric("Current Yield", "N/A")
    
    with metric_cols[3]:
        ten_yr = rate_data.get("10Y", rate_data.get("20Y", None))
        if ten_yr and yield_pct:
            spread = (yield_pct - ten_yr) * 100
            st.metric("Spread vs Treasury", f"{spread:.0f} bps")
        else:
            st.metric("Spread vs Treasury", "N/A")
    
    st.markdown("---")
    
    # Row 2: Charts side by side
    chart_col1, chart_col2 = st.columns(2)
    
    with chart_col1:
        st.subheader("Treasury Yield Curve vs Preferred Yield")
        
        if rate_data:
            # Build yield curve chart
            maturities = list(rate_data.keys())
            yields = list(rate_data.values())
            
            fig = go.Figure()
            
            # Treasury yield curve
            fig.add_trace(go.Scatter(
                x=maturities,
                y=yields,
                mode='lines+markers',
                name='Treasury Yields',
                line=dict(color='#1f77b4', width=2),
                marker=dict(size=8),
            ))
            
            # Preferred yield as horizontal line
            if yield_pct:
                fig.add_hline(
                    y=yield_pct,
                    line_dash="dash",
                    line_color="red",
                    annotation_text=f"{ticker} Yield: {yield_pct:.2f}%",
                    annotation_position="top right",
                )
            
            fig.update_layout(
                xaxis_title="Maturity",
                yaxis_title="Yield (%)",
                height=400,
                template="plotly_white",
                showlegend=True,
            )
            
            st.plotly_chart(fig, use_container_width=True)
    
    with chart_col2:
        st.subheader("Price History (1 Year)")
        
        price_hist = get_price_history(ticker, period="1y")
        if price_hist is not None and not price_hist.empty:
            fig2 = go.Figure()
            
            fig2.add_trace(go.Scatter(
                x=price_hist.index,
                y=price_hist["Close"],
                mode='lines',
                name='Close Price',
                line=dict(color='#2ca02c', width=2),
                fill='tozeroy',
                fillcolor='rgba(44, 160, 44, 0.1)',
            ))
            
            fig2.update_layout(
                xaxis_title="Date",
                yaxis_title="Price ($)",
                height=400,
                template="plotly_white",
            )
            
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Price history not available for this ticker.")
    
    st.markdown("---")
    
    # Row 3: AI Synthesis
    st.subheader("AI Synthesis (Gemini)")
    st.markdown(result.get("synthesis", "No synthesis available."))
    
    st.markdown("---")
    
    # Row 4: Raw Agent Outputs (expandable)
    with st.expander("View Raw Agent Outputs"):
        raw_col1, raw_col2 = st.columns(2)
        
        with raw_col1:
            st.subheader("Market Data Agent Output")
            st.json(market_data)
        
        with raw_col2:
            st.subheader("Rate Context Agent Output")
            st.json(rate_data)

elif not ticker:
    st.info("Enter a preferred stock ticker above and click 'Analyze' to begin.")
