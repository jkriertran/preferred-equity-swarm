"""
Preferred Equity Analysis Swarm: Streamlit Dashboard
=====================================================
Institutional-grade web interface for the multi-agent preferred equity
analysis system. Phase 3: 12 agent nodes across 5 layers.

UI Design Principles:
  - "Bottom line up front": synthesis and key risks appear first
  - Tabbed detail sections to reduce vertical scrolling
  - Color-coded risk badges for rapid visual scanning
  - Compact agent status bar (diagnostic, not primary)
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
from src.data.prospectus_inventory import (
    get_inventory_lookup,
    get_quick_analysis_tickers,
    load_cached_prospectus_inventory,
)
from src.data.security_resolver import (
    resolve_security,
    normalize_ticker,
    get_known_tickers,
    get_demo_tickers,
    get_pff_tickers,
    search_by_issuer,
    validate_ticker_for_analysis,
    get_universe_grouped_by_issuer,
)


cached_inventory = load_cached_prospectus_inventory()
inventory_lookup = get_inventory_lookup()
quick_tickers = get_quick_analysis_tickers(limit=8)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _risk_badge(label: str, level: str) -> str:
    """Return an HTML badge with color coding based on risk level."""
    level_lower = level.lower().replace("_", " ").strip()
    color_map = {
        "low": "#28a745",
        "very low": "#28a745",
        "moderate": "#ffc107",
        "elevated": "#fd7e14",
        "high": "#dc3545",
        "very high": "#dc3545",
        "not callable": "#6c757d",
        "fair": "#17a2b8",
        "cheap": "#28a745",
        "rich": "#fd7e14",
    }
    color = color_map.get(level_lower, "#6c757d")
    return (
        f'<div style="text-align:center;">'
        f'<span style="font-size:0.75rem;color:#888;">{label}</span><br>'
        f'<span style="background-color:{color};color:white;padding:3px 12px;'
        f'border-radius:12px;font-size:0.85rem;font-weight:600;">'
        f'{level_lower.title()}</span></div>'
    )


def _agent_dot(status: str) -> str:
    """Return a colored dot for agent status."""
    if status == "success":
        return '<span style="color:#28a745;font-size:1.2rem;">&#9679;</span>'
    elif status == "failed":
        return '<span style="color:#dc3545;font-size:1.2rem;">&#9679;</span>'
    return '<span style="color:#aaa;font-size:1.2rem;">&#9679;</span>'


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
        "A multi-agent AI swarm that analyzes preferred equity securities. "
        "The swarm coordinates 12 specialized agents running in parallel "
        "across five layers, combining live market data with SEC prospectus "
        "extraction, call analysis, tax treatment, regulatory risk, and "
        "relative value before a quality gate determines whether data is "
        "sufficient for AI synthesis."
    )

    st.markdown("---")

    # Collapsible architecture list (improvement #8)
    with st.expander("Swarm Architecture", expanded=False):
        st.markdown("""
**Layer 1: Data Collection (Parallel)**
1. Market Data Agent
2. Rate Context Agent
3. Dividend Analysis Agent
4. Prospectus Parsing Agent

**Layer 2: Deterministic Analysis**
5. Interest Rate Sensitivity Agent

**Layer 3: Analytical Agents (Parallel)**
6. Call Probability Agent
7. Tax & Yield Agent
8. Regulatory & Sector Agent
9. Relative Value Agent

**Layer 4: Quality Gate**
10. Quality Check Agent

**Layer 5: Conditional Routing**
11a. Synthesis Agent *or*
11b. Error Report Agent
        """)

    with st.expander("LangGraph Patterns", expanded=False):
        st.markdown("""
**Fan-Out:** 4 data agents run in parallel from START, then 4 analytical agents fan out from Layer 2

**Fan-In:** Layer 1 feeds Layer 2, Layer 3 feeds Quality Gate

**Conditional Edge:** Routes to Synthesis or Error based on data quality score
        """)

    st.markdown("---")

    # Security Universe Browser
    st.subheader("Security Universe")
    demo_list = get_demo_tickers()
    known_count = len(get_known_tickers())
    pff_count = len(get_pff_tickers())
    st.write(
        f"{known_count} securities in universe | "
        f"{pff_count} in PFF ETF | "
        f"{len(demo_list)} with prospectus cache"
    )

    # Issuer search
    issuer_query = st.text_input("Search by issuer", placeholder="e.g., Morgan Stanley")
    if issuer_query:
        search_results = search_by_issuer(issuer_query)
        if search_results:
            search_df = pd.DataFrame(
                [
                    {
                        "Ticker": r["ticker"],
                        "Name": (r.get("security_name") or "")[:35],
                        "PFF": "Yes" if r.get("in_pff") else "",
                        "Cache": "Yes" if r["ticker"] in demo_list else "",
                    }
                    for r in search_results
                ]
            )
            st.dataframe(search_df, use_container_width=True, hide_index=True)
        else:
            st.caption("No matching securities found.")

    # Demo securities table
    with st.expander("Cached Securities (Fast Analysis)", expanded=False):
        if cached_inventory:
            inventory_df = pd.DataFrame(
                [
                    {
                        "Ticker": row["ticker"],
                        "Issuer": row["issuer"],
                        "Series": row["series"],
                    }
                    for row in cached_inventory
                ]
            )
            st.dataframe(inventory_df, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.caption("Phase 3: Full Analytical Swarm | Security Resolver v1.0")


# ---------------------------------------------------------------------------
# Main Content: Ticker Input
# ---------------------------------------------------------------------------

st.title("Preferred Equity Analysis Swarm")
st.markdown("Enter a preferred stock ticker to run the multi-agent analysis.")

col1, col2 = st.columns([3, 1])

with col1:
    ticker = st.text_input(
        "Preferred Stock Ticker",
        value="BAC-PL",
        placeholder="e.g., BAC-PL, JPM-PD, WFC-PL",
        help="Enter a preferred stock ticker symbol. Use the format ISSUER-P[SERIES]. Cached tickers analyze faster."
    )

with col2:
    st.markdown("<br>", unsafe_allow_html=True)
    analyze_button = st.button("Analyze", type="primary", use_container_width=True)

# ---------------------------------------------------------------------------
# Curated Universe: Issuer-Grouped Ticker Picker
# ---------------------------------------------------------------------------
st.caption("Select from curated universe (65 securities, 22 issuers):")

universe_groups = get_universe_grouped_by_issuer()
demo_set = set(get_demo_tickers())

for group in universe_groups:
    issuer_label = f"**{group['issuer_name']}** ({group['parent_ticker']})"
    tickers_in_group = group["tickers"]
    num_cols = min(len(tickers_in_group), 8)
    cols = st.columns([1.8] + [1] * num_cols)
    with cols[0]:
        st.markdown(f"<div style='padding-top:6px;font-size:0.85em;'>{issuer_label}</div>", unsafe_allow_html=True)
    for j, t in enumerate(tickers_in_group):
        with cols[j + 1]:
            # Series letter only for the button label, with a visual indicator for cached
            series_letter = t.split("-P")[-1] if "-P" in t else t
            btn_label = f"{series_letter} *" if t in demo_set else series_letter
            if st.button(btn_label, key=f"univ_{t}", use_container_width=True, help=t):
                ticker = t
                analyze_button = True

st.caption("\* = prospectus cached (fast analysis)")

# Validate ticker through the security resolver
normalized = normalize_ticker(ticker.strip())
validation = validate_ticker_for_analysis(normalized)
resolution = validation["resolution"]

if resolution["resolved"]:
    source_label = resolution["resolution_source"].replace("_", " ").title()
    cache_status = "Prospectus cached" if resolution["has_prospectus_cache"] else "No prospectus cache"
    pff_status = "In PFF ETF" if resolution["in_pff"] else ""
    badges = [f"Source: {source_label}", cache_status]
    if pff_status:
        badges.append(pff_status)
    st.caption(" | ".join(badges))
    if resolution.get("warnings"):
        for w in resolution["warnings"]:
            st.warning(w)
else:
    st.caption(
        f"{normalized} is not in the curated universe. "
        "The app will attempt a live lookup, but results may be less reliable."
    )

st.markdown("---")


# ---------------------------------------------------------------------------
# Analysis Execution
# ---------------------------------------------------------------------------

if analyze_button and ticker:
    progress_container = st.container()

    with progress_container:
        st.subheader(f"Analyzing: {ticker}")
        progress_bar = st.progress(0, text="Initializing swarm...")
        status_placeholder = st.empty()

        try:
            status_placeholder.info("Running Layer 1 (data), Layer 2 (rates), Layer 3 (analytics)...")
            progress_bar.progress(10, text="Agents running across 3 layers...")

            # Use normalized ticker for analysis
            analysis_ticker = normalized
            if not validation["valid"]:
                st.error(validation["reason"])
                st.stop()
            result = analyze_preferred_advanced(analysis_ticker)

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
            # Enhanced error reporting for API key issues
            error_msg = str(e)
            if "No LLM API key is configured" in error_msg:
                st.error(f"**Analysis failed: API Key Missing**")
                st.markdown(f"""
                The swarm requires an LLM API key to perform the final synthesis.
                
                **How to fix this in Streamlit Cloud:**
                1. Go to your app settings in the Streamlit Cloud dashboard.
                2. Navigate to the **Secrets** tab.
                3. Add your key in TOML format:
                ```toml
                GOOGLE_API_KEY = "your-gemini-key-here"
                # OR
                OPENAI_API_KEY = "your-openai-key-here"
                ```
                4. Save and the app will restart.
                """)
                
                # Diagnostic info (only shown when key is missing)
                with st.expander("Diagnostic Info (Secrets Check)", expanded=True):
                    st.write("Checking available secrets...")
                    
                    # Check st.secrets safely
                    try:
                        has_google = "GOOGLE_API_KEY" in st.secrets
                        has_openai = "OPENAI_API_KEY" in st.secrets
                    except Exception:
                        has_google = False
                        has_openai = False
                        
                    has_env_google = bool(os.environ.get("GOOGLE_API_KEY"))
                    has_env_openai = bool(os.environ.get("OPENAI_API_KEY"))
                    
                    st.write(f"- Streamlit Secrets (Google): {'✅ Found' if has_google else '❌ Missing'}")
                    st.write(f"- Streamlit Secrets (OpenAI): {'✅ Found' if has_openai else '❌ Missing'}")
                    st.write(f"- Environment Vars (Google): {'✅ Found' if has_env_google else '❌ Missing'}")
                    st.write(f"- Environment Vars (OpenAI): {'✅ Found' if has_env_openai else '❌ Missing'}")
                    
                    st.info("If secrets are missing but you added them to the dashboard, try rebooting the app in the Streamlit Cloud console.")
            else:
                st.error(f"Analysis failed: {error_msg}")
            st.stop()

    # Extract all result data
    agent_status = result.get("agent_status", {})
    quality_report = result.get("quality_report", {})
    prospectus_terms = result.get("prospectus_terms", {})
    market_data = result.get("market_data", {})
    rate_data = result.get("rate_data", {})
    rate_sensitivity = result.get("rate_sensitivity", {})
    dividend_data = result.get("dividend_data", {})
    call_analysis = result.get("call_analysis", {})
    tax_analysis = result.get("tax_analysis", {})
    regulatory_analysis = result.get("regulatory_analysis", {})
    relative_value = result.get("relative_value", {})
    synthesis = result.get("synthesis", "")

    st.markdown("---")

    # ===================================================================
    # IMPROVEMENT #2: Security Header Card
    # ===================================================================

    security_name = prospectus_terms.get("security_name", ticker)
    coupon_type_label = str(prospectus_terms.get("coupon_type", "")).title()
    coupon_rate_val = prospectus_terms.get("coupon_rate")
    cumulative_label = "Cumulative" if prospectus_terms.get("cumulative") else "Non-Cumulative"
    perpetual_label = "Perpetual" if prospectus_terms.get("perpetual") else ""

    header_parts = []
    if coupon_rate_val is not None:
        header_parts.append(f"{coupon_rate_val:.2f}%")
    header_parts.append(cumulative_label)
    if perpetual_label:
        header_parts.append(perpetual_label)
    if coupon_type_label:
        header_parts.append(coupon_type_label)
    header_parts.append("Preferred")

    header_subtitle = " ".join(header_parts)

    # Check if this is snapshot data
    is_snapshot = market_data.get("is_snapshot", False)
    as_of = market_data.get("as_of", "")
    
    snapshot_badge = ""
    if is_snapshot:
        snapshot_badge = f' <span style="background-color:#6c757d;color:white;padding:2px 8px;border-radius:4px;font-size:0.7rem;vertical-align:middle;margin-left:8px;">SNAPSHOT DATA (As of {as_of})</span>'

    st.markdown(
        f'<h2 style="margin-bottom:0;">{ticker}{snapshot_badge}</h2>'
        f'<p style="font-size:1.1rem;color:#666;margin-top:0;">{security_name}<br>'
        f'{header_subtitle}</p>',
        unsafe_allow_html=True,
    )

    # ===================================================================
    # KEY METRICS ROW
    # ===================================================================

    metric_cols = st.columns(6)

    with metric_cols[0]:
        price = market_data.get("price", None)
        st.metric("Price", f"${price:,.2f}" if isinstance(price, (int, float)) else "N/A")

    with metric_cols[1]:
        div_yield = market_data.get("dividend_yield", None)
        if div_yield:
            yield_pct = div_yield if div_yield > 1 else div_yield * 100
            st.metric("Current Yield", f"{yield_pct:.2f}%")
        else:
            yield_pct = None
            st.metric("Current Yield", "N/A")

    with metric_cols[2]:
        ytw = call_analysis.get("yield_to_worst_pct") if call_analysis else None
        st.metric("Yield-to-Worst", f"{ytw:.2f}%" if isinstance(ytw, (int, float)) else "N/A")

    with metric_cols[3]:
        tey = tax_analysis.get("tax_equivalent_yield_pct") if tax_analysis else None
        st.metric("Tax-Equiv Yield", f"{tey:.2f}%" if isinstance(tey, (int, float)) else "N/A")

    with metric_cols[4]:
        ten_yr = rate_data.get("10Y", rate_data.get("20Y", None))
        if ten_yr and yield_pct:
            spread = (yield_pct - ten_yr) * 100
            st.metric("Spread to Tsy", f"{spread:.0f} bps")
        else:
            st.metric("Spread to Tsy", "N/A")

    with metric_cols[5]:
        duration = rate_sensitivity.get("effective_duration") if rate_sensitivity else None
        st.metric("Eff. Duration", f"{duration:.2f} yrs" if isinstance(duration, (int, float)) else "N/A")

    # ===================================================================
    # IMPROVEMENT #7: Key Risks Summary Row (color-coded badges)
    # ===================================================================

    st.markdown("#### Risk at a Glance")

    risk_cols = st.columns(5)

    with risk_cols[0]:
        call_prob = str(call_analysis.get("call_probability", "N/A")).replace("_", " ") if call_analysis else "N/A"
        st.markdown(_risk_badge("Call Risk", call_prob), unsafe_allow_html=True)

    with risk_cols[1]:
        rate_regime = str(rate_sensitivity.get("regime", "N/A")).replace("_", " ") if rate_sensitivity else "N/A"
        # Map regime to risk-like label for coloring
        regime_risk_map = {"fixed_rate": "moderate", "floating_rate": "low", "fixed_to_floating": "moderate"}
        rate_risk_label = str(rate_sensitivity.get("confidence", regime_risk_map.get(rate_regime, "moderate"))) if rate_sensitivity else "N/A"
        st.markdown(_risk_badge("Rate Sensitivity", rate_regime), unsafe_allow_html=True)

    with risk_cols[2]:
        reg_risk = str(regulatory_analysis.get("regulatory_risk_level", "N/A")).replace("_", " ") if regulatory_analysis else "N/A"
        st.markdown(_risk_badge("Regulatory Risk", reg_risk), unsafe_allow_html=True)

    with risk_cols[3]:
        deferral = str(regulatory_analysis.get("dividend_deferral_risk", "N/A")).replace("_", " ") if regulatory_analysis else "N/A"
        st.markdown(_risk_badge("Deferral Risk", deferral), unsafe_allow_html=True)

    with risk_cols[4]:
        val_assess = str(relative_value.get("value_assessment", "N/A")).replace("_", " ") if relative_value else "N/A"
        st.markdown(_risk_badge("Relative Value", val_assess), unsafe_allow_html=True)

    st.markdown("---")

    # ===================================================================
    # IMPROVEMENT #1: AI Synthesis moved to top (bottom line up front)
    # ===================================================================

    # Dynamic header based on which LLM was used (improvement #10)
    if quality_report.get("passed", False):
        llm_label = "AI Synthesis"
        st.subheader(f"Analyst Note ({llm_label})")
    else:
        st.subheader("Error Report")

    st.markdown(synthesis)

    st.markdown("---")

    # ===================================================================
    # IMPROVEMENT #6: Charts moved up, before detailed tabs
    # ===================================================================

    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        st.markdown("**Treasury Yield Curve vs Preferred Yield**")

        if rate_data:
            maturities = list(rate_data.keys())
            yields_list = list(rate_data.values())

            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=maturities,
                y=yields_list,
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
                height=350,
                template="plotly_white",
                showlegend=True,
                margin=dict(t=20, b=40),
            )
            st.plotly_chart(fig, use_container_width=True)

    with chart_col2:
        st.markdown("**Price History (1 Year)**")

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
                height=350,
                template="plotly_white",
                margin=dict(t=20, b=40),
            )
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Price history not available for this ticker.")

    st.markdown("---")

    # ===================================================================
    # IMPROVEMENT #3: Tabbed detail sections
    # ===================================================================

    st.subheader("Detailed Analysis")

    detail_tabs = st.tabs([
        "Call Risk",
        "Rate Sensitivity",
        "Tax & Yield",
        "Regulatory",
        "Relative Value",
        "Dividends",
        "Prospectus",
    ])

    # --- Tab 1: Call Risk ---
    with detail_tabs[0]:
        if call_analysis and call_analysis.get("call_probability") is not None:
            call_cols = st.columns(5)

            with call_cols[0]:
                prob = str(call_analysis.get("call_probability", "N/A")).replace("_", " ").title()
                score = call_analysis.get("call_probability_score")
                st.metric("Call Probability", prob, delta=f"score: {score:.2f}" if score is not None else None)

            with call_cols[1]:
                ytc = call_analysis.get("yield_to_call_pct")
                st.metric("Yield-to-Call", f"{ytc:.2f}%" if isinstance(ytc, (int, float)) else "N/A")

            with call_cols[2]:
                ytw_val = call_analysis.get("yield_to_worst_pct")
                st.metric("Yield-to-Worst", f"{ytw_val:.2f}%" if isinstance(ytw_val, (int, float)) else "N/A")

            with call_cols[3]:
                years = call_analysis.get("years_to_call")
                if years is not None and years > 0:
                    st.metric("Years to Call", f"{years:.1f}")
                elif years is not None and years <= 0:
                    st.metric("Years to Call", "Currently Callable")
                else:
                    st.metric("Years to Call", "N/A")

            with call_cols[4]:
                incentive = str(call_analysis.get("refinancing_incentive", "N/A")).replace("_", " ").title()
                st.metric("Refinancing Incentive", incentive)

            premium = call_analysis.get("premium_to_call_pct")
            if premium is not None:
                call_price = call_analysis.get("call_price_per_share")
                if premium > 0:
                    st.caption(f"Trading at a {premium:.1f}% premium to the ${call_price:.2f} call price.")
                elif premium < 0:
                    st.caption(f"Trading at a {abs(premium):.1f}% discount to the ${call_price:.2f} call price.")
                else:
                    st.caption(f"Trading at the ${call_price:.2f} call price.")

            if call_analysis.get("call_analysis_summary"):
                st.markdown("---")
                st.markdown(call_analysis["call_analysis_summary"])
        else:
            st.info("Call analysis data not available for this security.")

    # --- Tab 2: Rate Sensitivity ---
    with detail_tabs[1]:
        if rate_sensitivity and not rate_sensitivity.get("error"):
            rate_cols = st.columns(5)

            with rate_cols[0]:
                regime = str(rate_sensitivity.get("regime", "N/A")).replace("_", " ").title()
                st.metric("Rate Regime", regime)

            with rate_cols[1]:
                primary_measure = rate_sensitivity.get("primary_measure", "Primary Measure")
                primary_value = rate_sensitivity.get("primary_value")
                if isinstance(primary_value, (int, float)):
                    if "Duration" in primary_measure or "Reset" in primary_measure:
                        primary_text = f"{primary_value:.2f} yrs"
                    elif "Coupon" in primary_measure:
                        primary_text = f"{primary_value:.2f}%"
                    else:
                        primary_text = f"{primary_value:.2f}"
                else:
                    primary_text = str(primary_value or "N/A")
                st.metric(primary_measure, primary_text)

            with rate_cols[2]:
                dur = rate_sensitivity.get("effective_duration")
                st.metric("Effective Duration", f"{dur:.2f} yrs" if isinstance(dur, (int, float)) else "N/A")

            with rate_cols[3]:
                dv01 = rate_sensitivity.get("effective_dv01_per_share")
                st.metric("DV01 / Share", f"${dv01:.4f}" if isinstance(dv01, (int, float)) else "N/A")

            with rate_cols[4]:
                st.metric("Confidence", str(rate_sensitivity.get("confidence", "N/A")).title())

            if rate_sensitivity.get("summary"):
                st.caption(rate_sensitivity["summary"])
            if rate_sensitivity.get("benchmark_note"):
                st.caption(f"Note: {rate_sensitivity['benchmark_note']}")

            # Benchmark context
            benchmark_fields_present = any(
                rate_sensitivity.get(key) is not None
                for key in (
                    "contractual_benchmark",
                    "live_benchmark_label",
                    "benchmark_replacement_method",
                    "all_in_floating_coupon_pct",
                    "projected_post_reset_coupon_pct",
                )
            )
            if benchmark_fields_present:
                st.markdown("---")
                st.markdown("**Benchmark Context**")
                benchmark_cols = st.columns(4)

                with benchmark_cols[0]:
                    st.metric("Contractual Benchmark", str(rate_sensitivity.get("contractual_benchmark") or "N/A"))
                with benchmark_cols[1]:
                    st.metric("Live Benchmark Used", str(rate_sensitivity.get("live_benchmark_label") or "N/A"))
                with benchmark_cols[2]:
                    st.metric("Replacement Method", str(rate_sensitivity.get("benchmark_replacement_method") or "N/A"))

                coupon_estimate = rate_sensitivity.get("all_in_floating_coupon_pct")
                coupon_label = "All-In Reset Coupon"
                if coupon_estimate is None:
                    coupon_estimate = rate_sensitivity.get("projected_post_reset_coupon_pct")
                    coupon_label = "Projected Post-Reset Coupon"

                with benchmark_cols[3]:
                    st.metric(coupon_label, f"{coupon_estimate:.2f}%" if isinstance(coupon_estimate, (int, float)) else "N/A")

                next_reset_tenor = rate_sensitivity.get("next_reset_tenor_years")
                if isinstance(next_reset_tenor, (int, float)):
                    st.caption(f"Next reset tenor assumption: {next_reset_tenor:.2f} years")

            # Scenario table
            scenario_table = rate_sensitivity.get("scenario_table", [])
            if scenario_table:
                st.markdown("---")
                st.markdown("**Rate Shock Scenarios**")
                if rate_sensitivity.get("scenario_table_type") == "benchmark_coupon":
                    benchmark_label = rate_sensitivity.get("live_benchmark_label") or "Benchmark"
                    scenario_df = pd.DataFrame([
                        {
                            "Shock (bps)": row["shock_bps"],
                            f"{benchmark_label} Rate": f"{row['benchmark_rate_pct']:.2f}%" if isinstance(row.get("benchmark_rate_pct"), (int, float)) else "N/A",
                            "All-In Coupon": f"{row['all_in_coupon_pct']:.2f}%" if isinstance(row.get("all_in_coupon_pct"), (int, float)) else "N/A",
                        }
                        for row in scenario_table
                    ])
                else:
                    scenario_df = pd.DataFrame([
                        {
                            "Shock (bps)": row["shock_bps"],
                            "Est. Price Change": row["estimated_price_change"],
                            "Est. Price": row["estimated_price"],
                        }
                        for row in scenario_table
                    ])
                st.dataframe(scenario_df, use_container_width=True, hide_index=True)
        elif rate_sensitivity and rate_sensitivity.get("error"):
            st.warning(rate_sensitivity["error"])
        else:
            st.info("Rate sensitivity data not available for this security.")

    # --- Tab 3: Tax & Yield ---
    with detail_tabs[2]:
        if tax_analysis and tax_analysis.get("qdi_eligible") is not None:
            tax_cols = st.columns(5)

            with tax_cols[0]:
                qdi = tax_analysis.get("qdi_eligible")
                if qdi is True:
                    qdi_text = "QDI Eligible"
                    qdi_delta = "qualified"
                elif qdi is False:
                    qdi_text = "Not QDI"
                    qdi_delta = "ordinary income"
                else:
                    qdi_text = "Unknown"
                    qdi_delta = "not determined"
                st.metric("QDI Status", qdi_text, delta=qdi_delta, delta_color="normal" if qdi else "off")

            with tax_cols[1]:
                after_tax = tax_analysis.get("after_tax_yield_pct")
                st.metric("After-Tax Yield", f"{after_tax:.2f}%" if isinstance(after_tax, (int, float)) else "N/A")

            with tax_cols[2]:
                tey_val = tax_analysis.get("tax_equivalent_yield_pct")
                st.metric("Tax-Equivalent Yield", f"{tey_val:.2f}%" if isinstance(tey_val, (int, float)) else "N/A")

            with tax_cols[3]:
                eff_rate = tax_analysis.get("effective_tax_rate_pct")
                st.metric("Effective Tax Rate", f"{eff_rate:.1f}%" if isinstance(eff_rate, (int, float)) else "N/A")

            with tax_cols[4]:
                advantage = tax_analysis.get("tax_advantage_bps", 0)
                st.metric("QDI Advantage", f"{advantage:.0f} bps" if advantage else "N/A")

            if tax_analysis.get("qdi_classification_reason"):
                st.caption(tax_analysis["qdi_classification_reason"])

            if tax_analysis.get("tax_summary"):
                st.markdown("---")
                st.markdown(tax_analysis["tax_summary"])
        else:
            st.info("Tax analysis data not available for this security.")

    # --- Tab 4: Regulatory ---
    with detail_tabs[3]:
        if regulatory_analysis and regulatory_analysis.get("sector") is not None:
            reg_cols = st.columns(5)

            with reg_cols[0]:
                sector = str(regulatory_analysis.get("sector", "N/A")).title()
                st.metric("Sector", sector)

            with reg_cols[1]:
                is_gsib = regulatory_analysis.get("is_gsib", False)
                gsib_text = "Yes" if is_gsib else "No"
                bucket = regulatory_analysis.get("gsib_bucket")
                gsib_delta = f"Bucket {bucket}" if bucket else None
                st.metric("G-SIB", gsib_text, delta=gsib_delta)

            with reg_cols[2]:
                capital = str(regulatory_analysis.get("capital_treatment", "N/A")).upper()
                st.metric("Capital Treatment", capital)

            with reg_cols[3]:
                risk_level = str(regulatory_analysis.get("regulatory_risk_level", "N/A")).replace("_", " ").title()
                st.metric("Regulatory Risk", risk_level)

            with reg_cols[4]:
                deferral_risk = str(regulatory_analysis.get("dividend_deferral_risk", "N/A")).replace("_", " ").title()
                st.metric("Deferral Risk", deferral_risk)

            min_cet1 = regulatory_analysis.get("minimum_cet1_pct")
            if min_cet1 is not None:
                st.caption(f"Estimated minimum CET1 requirement: {min_cet1:.1f}%")

            if regulatory_analysis.get("stress_test_context"):
                st.markdown("---")
                st.markdown("**Stress Test Context**")
                st.markdown(regulatory_analysis["stress_test_context"])

            if regulatory_analysis.get("regulatory_summary"):
                st.markdown("---")
                st.markdown(regulatory_analysis["regulatory_summary"])
        else:
            st.info("Regulatory analysis data not available for this security.")

    # --- Tab 5: Relative Value ---
    with detail_tabs[4]:
        if relative_value and relative_value.get("value_assessment") is not None:
            rv_cols = st.columns(5)

            with rv_cols[0]:
                value = str(relative_value.get("value_assessment", "N/A")).title()
                st.metric("Value Assessment", value)

            with rv_cols[1]:
                peer_count = relative_value.get("peer_count", 0)
                rank = relative_value.get("yield_rank")
                rank_text = f"#{rank} of {peer_count}" if rank else "N/A"
                st.metric("Yield Rank", rank_text)

            with rv_cols[2]:
                spread_tsy = relative_value.get("spread_to_treasury_bps")
                st.metric("Spread to Treasury", f"{spread_tsy:.0f} bps" if spread_tsy is not None else "N/A")

            with rv_cols[3]:
                spread_common = relative_value.get("spread_to_common_bps")
                st.metric("Spread to Common", f"{spread_common:.0f} bps" if spread_common is not None else "N/A")

            with rv_cols[4]:
                common_yield = relative_value.get("common_dividend_yield_pct")
                st.metric("Common Div Yield", f"{common_yield:.2f}%" if isinstance(common_yield, (int, float)) else "N/A")

            # Peer comparison table
            peers = relative_value.get("peer_universe", [])
            if peers:
                st.markdown("---")
                st.markdown("**Peer Comparison**")
                peer_df = pd.DataFrame([
                    {
                        "Ticker": p.get("ticker", "N/A"),
                        "Issuer": p.get("issuer", "N/A"),
                        "Coupon": f"{p['coupon_rate']:.2f}%" if p.get("coupon_rate") is not None else "N/A",
                        "Type": str(p.get("coupon_type", "N/A")).title(),
                        "Cumulative": "Yes" if p.get("cumulative") else "No" if p.get("cumulative") is False else "N/A",
                        "Relationship": p.get("relationship", "N/A"),
                    }
                    for p in peers
                ])
                st.dataframe(peer_df, use_container_width=True, hide_index=True)

            if relative_value.get("relative_value_summary"):
                st.markdown("---")
                st.markdown(relative_value["relative_value_summary"])
        else:
            st.info("Relative value data not available for this security.")

    # --- Tab 6: Dividends ---
    with detail_tabs[5]:
        if dividend_data.get("has_dividend_history"):
            div_cols = st.columns(5)

            with div_cols[0]:
                div_rate = market_data.get("dividend_rate", None)
                st.metric("Annual Dividend", f"${div_rate:,.2f}" if div_rate else "N/A")

            with div_cols[1]:
                consistency = dividend_data.get("consistency", "N/A")
                frequency = dividend_data.get("frequency", "N/A")
                st.metric("Frequency", frequency.title())

            with div_cols[2]:
                st.metric("Consistency", consistency.title() if consistency else "N/A")

            with div_cols[3]:
                is_fixed = dividend_data.get("is_fixed_rate", None)
                st.metric("Rate Type", "Fixed" if is_fixed else "Variable" if is_fixed is not None else "N/A")

            with div_cols[4]:
                st.metric("Trend", dividend_data.get("trend", "N/A").replace("_", " ").title())

            st.caption(
                f"History from {dividend_data.get('first_payment_date', 'N/A')} "
                f"to {dividend_data.get('last_payment_date', 'N/A')}. "
                f"Total payments: {dividend_data.get('total_payments_recorded', 'N/A')}. "
                f"Avg payment: ${dividend_data.get('avg_payment', 0):.4f}. "
                f"Consistency score: {dividend_data.get('consistency_score', 0):.0%}."
            )
        else:
            st.info("Dividend history not available for this security.")

    # --- Tab 7: Prospectus ---
    with detail_tabs[6]:
        if prospectus_terms and not prospectus_terms.get("error"):
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

            # Show floating-rate terms if present
            floating_benchmark = prospectus_terms.get("floating_benchmark")
            floating_spread = prospectus_terms.get("floating_spread_bps")
            if floating_benchmark or floating_spread:
                st.markdown("---")
                st.markdown("**Floating Rate Terms**")
                float_cols = st.columns(3)
                with float_cols[0]:
                    st.metric("Benchmark", str(floating_benchmark or "N/A"))
                with float_cols[1]:
                    st.metric("Spread", f"{floating_spread} bps" if floating_spread else "N/A")
                with float_cols[2]:
                    ftf_date = prospectus_terms.get("fixed_to_floating_date")
                    st.metric("Conversion Date", str(ftf_date or "N/A"))

        elif prospectus_terms and prospectus_terms.get("error"):
            st.warning(prospectus_terms["error"])
        else:
            st.info("Prospectus terms not available for this security.")

    st.markdown("---")

    # ===================================================================
    # IMPROVEMENT #5: Compact Agent Status Bar
    # ===================================================================

    with st.expander("Agent Pipeline Status"):
        all_agents = [
            ("market_data", "Market"),
            ("rate_context", "Rates"),
            ("dividend", "Dividend"),
            ("prospectus", "Prospectus"),
            ("interest_rate", "IR Sens."),
            ("call_probability", "Call"),
            ("tax_yield", "Tax"),
            ("regulatory", "Regulatory"),
            ("relative_value", "Rel. Value"),
        ]

        dots_html = " &nbsp; ".join(
            f'{_agent_dot(agent_status.get(key, "unknown"))} {label}'
            for key, label in all_agents
        )

        qscore = quality_report.get("overall_score", 0)
        passed = quality_report.get("passed", False)
        gate_color = "#28a745" if passed else "#dc3545"
        gate_label = "PASS" if passed else "FAIL"
        route = "Synthesis" if quality_report.get("decision") == "proceed_to_synthesis" else "Error Report"

        st.markdown(
            f'{dots_html} &nbsp; | &nbsp; '
            f'Quality: <span style="color:{gate_color};font-weight:600;">{qscore:.0%} {gate_label}</span> '
            f'&nbsp; | &nbsp; Route: **{route}**',
            unsafe_allow_html=True,
        )

    # ===================================================================
    # Quality Check Details (expandable)
    # ===================================================================

    with st.expander("Quality Check Details"):
        checks = quality_report.get("checks", {})
        check_items = list(checks.items())
        for row_start in range(0, len(check_items), 4):
            row_items = check_items[row_start:row_start + 4]
            qc_cols = st.columns(len(row_items))
            for i, (source, details) in enumerate(row_items):
                with qc_cols[i]:
                    st.markdown(f"**{source.replace('_', ' ').title()}**")
                    score = details.get("score", 0)
                    st.progress(score, text=f"Score: {score:.0%}")
                    for check_name, check_val in details.items():
                        if check_name != "score":
                            st.write(f"{'✓' if check_val else '✗'} {check_name.replace('_', ' ').title()}")

    # ===================================================================
    # Raw Agent Outputs (expandable)
    # ===================================================================

    with st.expander("Raw Agent Outputs (JSON)"):
        raw_tabs = st.tabs([
            "Market Data", "Rate Data", "Rate Sensitivity", "Dividend Data",
            "Prospectus Terms", "Call Analysis", "Tax Analysis",
            "Regulatory Analysis", "Relative Value", "Agent Status",
        ])

        with raw_tabs[0]:
            st.json(market_data)
        with raw_tabs[1]:
            st.json(rate_data)
        with raw_tabs[2]:
            st.json(rate_sensitivity)
        with raw_tabs[3]:
            st.json(dividend_data)
        with raw_tabs[4]:
            st.json(prospectus_terms)
        with raw_tabs[5]:
            st.json(call_analysis)
        with raw_tabs[6]:
            st.json(tax_analysis)
        with raw_tabs[7]:
            st.json(regulatory_analysis)
        with raw_tabs[8]:
            st.json(relative_value)
        with raw_tabs[9]:
            st.json(agent_status)

elif not ticker:
    st.info("Enter a preferred stock ticker above and click 'Analyze' to begin.")
