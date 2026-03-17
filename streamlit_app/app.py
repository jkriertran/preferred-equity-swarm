"""
Preferred Equity Analysis Swarm: Streamlit Demo
=================================================
A web interface for the multi-agent preferred equity analysis system.
Demonstrates parallel execution, conditional routing, and quality gating.
"""

import sys
import os
import json
import streamlit as st
import plotly.graph_objects as go
import pandas as pd

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.agents.advanced_swarm import analyze_preferred_advanced
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
        "agents running in parallel, combining live market data with SEC "
        "prospectus extraction before a quality gate determines whether "
        "data is sufficient for AI synthesis."
    )
    
    st.markdown("---")
    
    st.subheader("Swarm Architecture")
    st.markdown("""
    **Data Agents (Parallel):**
    1. Market Data Agent
    2. Rate Context Agent
    3. Dividend Analysis Agent
    4. Prospectus Parsing Agent
    
    **Quality Gate:**
    5. Quality Check Agent
    
    **Conditional Routing:**
    6a. Synthesis Agent (Gemini) *or*
    6b. Error Report Agent
    """)
    
    st.markdown("---")
    
    st.subheader("LangGraph Patterns")
    st.markdown("""
    **Fan-Out:** 4 agents run in parallel from START
    
    **Fan-In:** All converge at Quality Check
    
    **Conditional Edge:** Routes to Synthesis or Error based on data quality score
    """)
    
    st.markdown("---")
    st.caption("Phase 2: Vertical Slice Prototype")


# ---------------------------------------------------------------------------
# Main Content
# ---------------------------------------------------------------------------

st.title("Preferred Equity Analysis Swarm")
st.markdown("Enter a preferred stock ticker to run the multi-agent analysis with parallel execution, SEC prospectus extraction, and quality gating.")

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
            status_placeholder.info("Running 4 data agents in parallel...")
            progress_bar.progress(10, text="Data agents running in parallel...")
            
            result = analyze_preferred_advanced(ticker)
            
            # Check quality outcome
            quality = result.get("quality_report", {})
            if quality.get("passed", False):
                progress_bar.progress(100, text="Analysis complete!")
                status_placeholder.success(
                    f"All agents completed. Quality score: {quality.get('overall_score', 0):.0%}. "
                    f"Route: Synthesis Agent."
                )
            else:
                progress_bar.progress(100, text="Analysis complete (with issues)")
                status_placeholder.warning(
                    f"Quality score: {quality.get('overall_score', 0):.0%} (below threshold). "
                    f"Route: Error Report Agent."
                )
            
        except Exception as e:
            st.error(f"Analysis failed: {str(e)}")
            st.stop()
    
    st.markdown("---")
    
    # ---------------------------------------------------------------------------
    # Agent Status Dashboard
    # ---------------------------------------------------------------------------
    
    st.subheader("Agent Execution Status")
    
    agent_status = result.get("agent_status", {})
    quality_report = result.get("quality_report", {})
    prospectus_terms = result.get("prospectus_terms", {})
    
    status_cols = st.columns(6)
    
    agent_labels = [
        ("market_data", "Market Data"),
        ("rate_context", "Rate Context"),
        ("dividend", "Dividend Analysis"),
        ("prospectus", "Prospectus"),
    ]
    
    for i, (key, label) in enumerate(agent_labels):
        with status_cols[i]:
            status = agent_status.get(key, "unknown")
            if status == "success":
                st.metric(label, "OK", delta="success", delta_color="normal")
            elif status == "failed":
                st.metric(label, "FAIL", delta="failed", delta_color="inverse")
            else:
                st.metric(label, "?", delta="unknown", delta_color="off")
    
    with status_cols[4]:
        qscore = quality_report.get("overall_score", 0)
        passed = quality_report.get("passed", False)
        st.metric(
            "Quality Gate",
            f"{qscore:.0%}",
            delta="passed" if passed else "failed",
            delta_color="normal" if passed else "inverse"
        )
    
    with status_cols[5]:
        route = quality_report.get("decision", "unknown")
        if route == "proceed_to_synthesis":
            st.metric("Route Taken", "Synthesis", delta="AI analysis", delta_color="normal")
        else:
            st.metric("Route Taken", "Error Report", delta="fallback", delta_color="inverse")
    
    st.markdown("---")
    
    # ---------------------------------------------------------------------------
    # Key Metrics
    # ---------------------------------------------------------------------------
    
    market_data = result.get("market_data", {})
    rate_data = result.get("rate_data", {})
    dividend_data = result.get("dividend_data", {})
    
    st.subheader("Key Metrics")
    
    metric_cols = st.columns(5)
    
    with metric_cols[0]:
        price = market_data.get("price", None)
        st.metric("Current Price", f"${price:,.2f}" if isinstance(price, (int, float)) else "N/A")
    
    with metric_cols[1]:
        div_rate = market_data.get("dividend_rate", None)
        st.metric("Annual Dividend", f"${div_rate:,.2f}" if div_rate else "N/A")
    
    with metric_cols[2]:
        div_yield = market_data.get("dividend_yield", None)
        if div_yield:
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
    
    with metric_cols[4]:
        consistency = dividend_data.get("consistency", "N/A")
        frequency = dividend_data.get("frequency", "N/A")
        st.metric("Dividend Pattern", f"{frequency.title()}", delta=consistency, delta_color="normal" if consistency in ("excellent", "good") else "off")
    
    st.markdown("---")
    
    # ---------------------------------------------------------------------------
    # Charts Row
    # ---------------------------------------------------------------------------
    
    chart_col1, chart_col2 = st.columns(2)
    
    with chart_col1:
        st.subheader("Treasury Yield Curve vs Preferred Yield")
        
        if rate_data:
            maturities = list(rate_data.keys())
            yields = list(rate_data.values())
            
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=maturities,
                y=yields,
                mode='lines+markers',
                name='Treasury Yields',
                line=dict(color='#1f77b4', width=2),
                marker=dict(size=8),
            ))
            
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
    
    # ---------------------------------------------------------------------------
    # Dividend Analysis Details
    # ---------------------------------------------------------------------------
    
    if dividend_data.get("has_dividend_history"):
        st.subheader("Dividend Analysis")
        
        div_cols = st.columns(4)
        
        with div_cols[0]:
            st.metric("Total Payments", dividend_data.get("total_payments_recorded", "N/A"))
        with div_cols[1]:
            st.metric("Avg Payment", f"${dividend_data.get('avg_payment', 0):.4f}")
        with div_cols[2]:
            is_fixed = dividend_data.get("is_fixed_rate", None)
            st.metric("Rate Type", "Fixed" if is_fixed else "Variable" if is_fixed is not None else "N/A")
        with div_cols[3]:
            st.metric("Trend", dividend_data.get("trend", "N/A").replace("_", " ").title())
        
        st.caption(
            f"History from {dividend_data.get('first_payment_date', 'N/A')} "
            f"to {dividend_data.get('last_payment_date', 'N/A')}. "
            f"Consistency score: {dividend_data.get('consistency_score', 0):.0%}."
        )
        
        st.markdown("---")

    # ---------------------------------------------------------------------------
    # Prospectus Terms
    # ---------------------------------------------------------------------------

    if prospectus_terms and not prospectus_terms.get("error"):
        st.subheader("Prospectus Terms (SEC)")

        security_name = prospectus_terms.get("security_name", "Unknown security")
        series = prospectus_terms.get("series", "N/A")
        filing_url = prospectus_terms.get("filing_url")
        filing_date = prospectus_terms.get("filing_date", "N/A")
        accession_number = prospectus_terms.get("accession_number", "N/A")
        source_label = str(prospectus_terms.get("source", "live")).replace("_", " ").title()
        resolution_label = str(prospectus_terms.get("resolution_source", "live")).replace("_", " ").title()
        mismatch_warning = prospectus_terms.get("mismatch_warning")

        st.markdown(f"**{security_name}**")
        st.caption(
            f"Series: {series} | Filing date: {filing_date} | Accession: {accession_number}"
        )
        st.caption(f"Prospectus source: {source_label} | Resolution: {resolution_label}")
        if filing_url:
            st.markdown(f"[Open filing on SEC.gov]({filing_url})")
        if mismatch_warning:
            st.warning(mismatch_warning)

        prospectus_cols = st.columns(5)

        with prospectus_cols[0]:
            coupon_rate = prospectus_terms.get("coupon_rate")
            st.metric("Coupon Rate", f"{coupon_rate:.2f}%" if isinstance(coupon_rate, (int, float)) else "N/A")
        with prospectus_cols[1]:
            st.metric("Coupon Type", str(prospectus_terms.get("coupon_type", "N/A")).title())
        with prospectus_cols[2]:
            st.metric("First Call Date", prospectus_terms.get("call_date", "N/A") or "N/A")
        with prospectus_cols[3]:
            qdi_flag = prospectus_terms.get("qdi_eligible")
            if qdi_flag is True:
                qdi_text = "Yes"
            elif qdi_flag is False:
                qdi_text = "No"
            else:
                qdi_text = "Unknown"
            st.metric("QDI Eligible", qdi_text)
        with prospectus_cols[4]:
            perpetual = prospectus_terms.get("perpetual")
            if perpetual is True:
                perpetual_text = "Yes"
            elif perpetual is False:
                perpetual_text = "No"
            else:
                perpetual_text = "Unknown"
            st.metric("Perpetual", perpetual_text)

        st.caption(
            f"Depositary shares: {prospectus_terms.get('deposit_fraction', 'No')} | "
            f"Cumulative: {prospectus_terms.get('cumulative', 'Unknown')} | "
            f"Exchange: {prospectus_terms.get('listing_exchange', 'N/A')}"
        )

        st.markdown("---")
    elif prospectus_terms.get("error"):
        st.subheader("Prospectus Terms (SEC)")
        st.warning(prospectus_terms["error"])
        st.markdown("---")
    
    # ---------------------------------------------------------------------------
    # AI Synthesis or Error Report
    # ---------------------------------------------------------------------------
    
    synthesis = result.get("synthesis", "")
    
    if quality_report.get("passed", False):
        st.subheader("AI Synthesis (Gemini)")
    else:
        st.subheader("Error Report")
    
    st.markdown(synthesis)
    
    st.markdown("---")
    
    # ---------------------------------------------------------------------------
    # Quality Check Details (expandable)
    # ---------------------------------------------------------------------------
    
    with st.expander("View Quality Check Details"):
        checks = quality_report.get("checks", {})
        
        qc_cols = st.columns(max(len(checks), 1))
        
        for i, (source, details) in enumerate(checks.items()):
            with qc_cols[i]:
                st.subheader(source.replace("_", " ").title())
                score = details.get("score", 0)
                st.progress(score, text=f"Score: {score:.0%}")
                for check_name, check_val in details.items():
                    if check_name != "score":
                        icon = "pass" if check_val else "fail"
                        st.write(f"{'✓' if check_val else '✗'} {check_name.replace('_', ' ').title()}")
    
    # ---------------------------------------------------------------------------
    # Raw Agent Outputs (expandable)
    # ---------------------------------------------------------------------------
    
    with st.expander("View Raw Agent Outputs"):
        raw_tabs = st.tabs(["Market Data", "Rate Data", "Dividend Data", "Prospectus Terms", "Agent Status"])
        
        with raw_tabs[0]:
            st.json(market_data)
        with raw_tabs[1]:
            st.json(rate_data)
        with raw_tabs[2]:
            st.json(dividend_data)
        with raw_tabs[3]:
            st.json(prospectus_terms)
        with raw_tabs[4]:
            st.json(agent_status)

elif not ticker:
    st.info("Enter a preferred stock ticker above and click 'Analyze' to begin.")
