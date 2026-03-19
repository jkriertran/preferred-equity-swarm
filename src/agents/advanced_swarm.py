"""
Advanced Preferred Equity Swarm
================================
Phase 3 implementation with eleven agents organized in a cache-aware sequence:

Layer 1 -- EARLY CONTEXT:
  Prospectus/cache resolution runs immediately alongside macro rate context.
    1. Prospectus Parsing Agent
    2. Rate Context Agent

Layer 2 -- SECURITY ENRICHMENT:
  Market and dividend analysis wait for prospectus/cache terms so first-run
  analyses can benefit from cached structure data.
    3. Market Data Agent
    4. Dividend Analysis Agent

Layer 3 -- DETERMINISTIC ANALYSIS:
  The Interest Rate Sensitivity Agent consumes the full upstream context.
    5. Interest Rate Sensitivity Agent

Layer 4 -- ANALYTICAL AGENTS:
  Four agents run in parallel off the merged state.
    6. Call Probability Agent
    7. Tax and Yield Agent
    8. Regulatory and Sector Agent
    9. Relative Value Agent

Layer 5 -- QUALITY GATE:
    10. Quality Check Agent

Layer 6 -- CONDITIONAL OUTPUT:
    11a. Synthesis Agent (Gemini)  -- if quality passes
    11b. Error Report Agent        -- if quality fails

Graph Structure:

                              +--> prospectus_agent ----+--> market_data_agent --+
                              |                         |                         |
    START --[fan-out]---------+                         +--> dividend_agent ------+--> interest_rate_agent
                              |                                                   |
                              +--> rate_context_agent -----------------------------+
                                                                                   |
                                                                          [fan-out Layer 4]
                                                                              /   |   |   \\
                                                                           call  tax reg  relval
                                                                              \\   |   |   /
                                                                           quality_check_agent
                                                                              /           \\
                                                                        [pass]           [fail]
                                                                          |                |
                                                                   synthesis_agent   error_report
                                                                          |                |
                                                                         END              END
"""

import os
import sys
import json
import re
from typing import TypedDict, Annotated, Literal
from langgraph.graph import StateGraph, END, START
from langchain_core.messages import HumanMessage, SystemMessage


def merge_dicts(left: dict, right: dict) -> dict:
    """Reducer that merges two dictionaries. Used for concurrent state updates."""
    merged = left.copy()
    merged.update(right)
    return merged


def merge_lists(left: list, right: list) -> list:
    """Reducer that concatenates two lists. Used for concurrent error accumulation."""
    return left + right


def _coerce_float(value):
    """Convert strings or numeric-like values to float when possible."""
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_fraction_to_float(text: str):
    """Parse strings like '1/400th' into 0.0025."""
    if not isinstance(text, str):
        return None
    match = re.search(r"(\d+)\s*/\s*(\d+)", text)
    if not match:
        return None
    numerator = int(match.group(1))
    denominator = int(match.group(2))
    if denominator == 0:
        return None
    return numerator / denominator


def _normalize_prospectus_amount(amount, prospectus_terms: dict):
    """Convert underlying preferred amounts to depositary-share equivalents."""
    base_amount = _coerce_float(amount)
    if base_amount is None:
        return None
    if prospectus_terms.get("deposit_shares"):
        fraction = _parse_fraction_to_float(prospectus_terms.get("deposit_fraction"))
        if fraction:
            return round(base_amount * fraction, 4)
    return round(base_amount, 4)


# Ensure project root is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


# ---------------------------------------------------------------------------
# State Schema (Phase 3 Extended)
# ---------------------------------------------------------------------------

class AdvancedSwarmState(TypedDict):
    """Extended state with fields for all agents plus quality tracking.

    Fields that receive concurrent updates from parallel agents use Annotated
    types with reducer functions.
    """
    ticker: str
    # Layer 1 outputs
    market_data: dict
    rate_data: dict
    dividend_data: dict
    prospectus_terms: dict
    # Layer 2 output
    rate_sensitivity: dict
    # Layer 3 outputs (Phase 3)
    call_analysis: dict
    tax_analysis: dict
    regulatory_analysis: dict
    relative_value: dict
    # Quality and synthesis
    quality_report: dict
    synthesis: str
    errors: Annotated[list, merge_lists]
    agent_status: Annotated[dict, merge_dicts]


# ---------------------------------------------------------------------------
# Layer 1: Data Collection Agents
# ---------------------------------------------------------------------------

def market_data_agent(state: AdvancedSwarmState) -> dict:
    """Fetches market data from Alpha Vantage."""
    from src.data.market_data import get_preferred_info

    ticker = state["ticker"]
    print(f"  [Market Data Agent] Fetching data for {ticker}...")

    info = get_preferred_info(ticker)

    status = state.get("agent_status", {})
    if "error" in info:
        status["market_data"] = "failed"
        errors = state.get("errors", [])
        errors.append(f"Market Data Agent failed: {info['error']}")
        print(f"  [Market Data Agent] FAILED: {info['error']}")
        return {"market_data": info, "agent_status": status, "errors": errors}
    else:
        status["market_data"] = "success"
        print(f"  [Market Data Agent] SUCCESS. Price: ${info.get('price', 'N/A')}")
        return {"market_data": info, "agent_status": status}


def rate_context_agent(state: AdvancedSwarmState) -> dict:
    """Fetches Treasury yield curve data."""
    from src.data.rate_data import get_treasury_yields

    print("  [Rate Context Agent] Fetching Treasury yield data...")

    yields = get_treasury_yields()

    status = state.get("agent_status", {})
    if not yields:
        status["rate_context"] = "failed"
        errors = state.get("errors", [])
        errors.append("Rate Context Agent failed: no yield data returned")
        print("  [Rate Context Agent] FAILED: no data")
        return {"rate_data": {}, "agent_status": status, "errors": errors}
    else:
        status["rate_context"] = "success"
        print(f"  [Rate Context Agent] SUCCESS. Got {len(yields)} yield points.")
        return {"rate_data": yields, "agent_status": status}


def dividend_agent(state: AdvancedSwarmState) -> dict:
    """Analyzes dividend payment patterns and consistency."""
    from src.data.dividend_analysis import analyze_dividend_pattern

    ticker = state["ticker"]
    print(f"  [Dividend Agent] Analyzing dividend pattern for {ticker}...")

    analysis = analyze_dividend_pattern(ticker)

    status = state.get("agent_status", {})
    has_history = analysis.get("has_dividend_history", False)
    has_frequency = analysis.get("frequency") is not None
    if not has_history and not has_frequency:
        status["dividend"] = "failed"
        errors = state.get("errors", [])
        errors.append(f"Dividend Agent: {analysis.get('error', 'no history')}")
        print(f"  [Dividend Agent] FAILED: {analysis.get('error', 'no history')}")
        return {"dividend_data": analysis, "agent_status": status, "errors": errors}
    else:
        status["dividend"] = "success"
        print(f"  [Dividend Agent] SUCCESS. Frequency: {analysis.get('frequency')}, "
              f"Consistency: {analysis.get('consistency')}")
        return {"dividend_data": analysis, "agent_status": status}


def prospectus_agent(state: AdvancedSwarmState) -> dict:
    """Search EDGAR, download the best filing, and extract structured prospectus terms."""
    from src.agents.prospectus_agent import prospectus_agent_node

    ticker = state["ticker"]
    print(f"  [Prospectus Agent] Searching EDGAR and extracting terms for {ticker}...")

    result = prospectus_agent_node(state)
    terms = result.get("prospectus_terms", {})

    if terms.get("error"):
        print(f"  [Prospectus Agent] FAILED: {terms['error']}")
    else:
        security_name = terms.get("security_name") or terms.get("series") or "prospectus terms"
        source = terms.get("source", "live")
        resolution_source = terms.get("resolution_source", "live")
        print(
            f"  [Prospectus Agent] SUCCESS. Extracted {security_name} "
            f"({source}/{resolution_source})."
        )

    return result


# ---------------------------------------------------------------------------
# Layer 2: Deterministic Analysis
# ---------------------------------------------------------------------------

def interest_rate_agent(state: AdvancedSwarmState) -> dict:
    """Build a security-specific rate sensitivity profile."""
    from src.data.rate_sensitivity import analyze_interest_rate_sensitivity

    ticker = state["ticker"]
    print(f"  [Interest Rate Agent] Building dynamic rate sensitivity view for {ticker}...")

    analysis = analyze_interest_rate_sensitivity(
        market_data=state.get("market_data", {}),
        rate_data=state.get("rate_data", {}),
        prospectus_terms=state.get("prospectus_terms", {}),
        dividend_data=state.get("dividend_data", {}),
    )

    status = state.get("agent_status", {})
    if analysis.get("error"):
        status["interest_rate"] = "failed"
        errors = state.get("errors", [])
        errors.append(f"Interest Rate Agent: {analysis['error']}")
        print(f"  [Interest Rate Agent] FAILED: {analysis['error']}")
        return {"rate_sensitivity": analysis, "agent_status": status, "errors": errors}

    status["interest_rate"] = "success"
    summary = analysis.get("summary", analysis.get("regime", "rate analysis"))
    print(f"  [Interest Rate Agent] SUCCESS. {summary}")
    return {"rate_sensitivity": analysis, "agent_status": status}


# ---------------------------------------------------------------------------
# Layer 3: Analytical Agents (Phase 3)
# ---------------------------------------------------------------------------

def call_probability_agent(state: AdvancedSwarmState) -> dict:
    """Estimate call probability, yield-to-call, and yield-to-worst."""
    from src.data.call_analysis import analyze_call_probability

    ticker = state["ticker"]
    print(f"  [Call Probability Agent] Analyzing call risk for {ticker}...")

    analysis = analyze_call_probability(
        market_data=state.get("market_data", {}),
        prospectus_terms=state.get("prospectus_terms", {}),
        rate_data=state.get("rate_data", {}),
        rate_sensitivity=state.get("rate_sensitivity", {}),
    )

    status = state.get("agent_status", {})
    prob = analysis.get("call_probability", "unknown")
    ytw = analysis.get("yield_to_worst_pct")
    status["call_probability"] = "success"
    print(f"  [Call Probability Agent] SUCCESS. Probability: {prob}, YTW: {ytw}")
    return {"call_analysis": analysis, "agent_status": status}


def tax_yield_agent(state: AdvancedSwarmState) -> dict:
    """Classify QDI eligibility and compute tax-equivalent yield."""
    from src.data.tax_analysis import analyze_tax_and_yield

    ticker = state["ticker"]
    print(f"  [Tax & Yield Agent] Analyzing tax treatment for {ticker}...")

    analysis = analyze_tax_and_yield(
        market_data=state.get("market_data", {}),
        prospectus_terms=state.get("prospectus_terms", {}),
        dividend_data=state.get("dividend_data", {}),
    )

    status = state.get("agent_status", {})
    qdi = analysis.get("qdi_eligible")
    tey = analysis.get("tax_equivalent_yield_pct")
    status["tax_yield"] = "success"
    qdi_text = "QDI" if qdi is True else "non-QDI" if qdi is False else "unknown"
    print(f"  [Tax & Yield Agent] SUCCESS. {qdi_text}, TEY: {tey}")
    return {"tax_analysis": analysis, "agent_status": status}


def regulatory_agent(state: AdvancedSwarmState) -> dict:
    """Assess regulatory and sector risk."""
    from src.data.regulatory_analysis import analyze_regulatory_risk

    ticker = state["ticker"]
    print(f"  [Regulatory Agent] Assessing regulatory risk for {ticker}...")

    analysis = analyze_regulatory_risk(
        market_data=state.get("market_data", {}),
        prospectus_terms=state.get("prospectus_terms", {}),
    )

    status = state.get("agent_status", {})
    risk_level = analysis.get("regulatory_risk_level", "unknown")
    status["regulatory"] = "success"
    print(f"  [Regulatory Agent] SUCCESS. Risk level: {risk_level}")
    return {"regulatory_analysis": analysis, "agent_status": status}


def relative_value_agent(state: AdvancedSwarmState) -> dict:
    """Rank the security against peers and assess relative value."""
    from src.data.relative_value import analyze_relative_value

    ticker = state["ticker"]
    print(f"  [Relative Value Agent] Building peer comparison for {ticker}...")

    analysis = analyze_relative_value(
        market_data=state.get("market_data", {}),
        prospectus_terms=state.get("prospectus_terms", {}),
        rate_data=state.get("rate_data", {}),
        dividend_data=state.get("dividend_data", {}),
        tax_analysis=state.get("tax_analysis", {}),
        call_analysis=state.get("call_analysis", {}),
    )

    status = state.get("agent_status", {})
    value = analysis.get("value_assessment", "unknown")
    peer_count = analysis.get("peer_count", 0)
    status["relative_value"] = "success"
    print(f"  [Relative Value Agent] SUCCESS. Value: {value}, Peers: {peer_count}")
    return {"relative_value": analysis, "agent_status": status}


# ---------------------------------------------------------------------------
# Layer 4: Quality Gate
# ---------------------------------------------------------------------------

def quality_check_agent(state: AdvancedSwarmState) -> dict:
    """Inspect all agent outputs and produce a quality report.

    This agent applies deterministic rules to decide whether the data is
    sufficient for synthesis.  Updated for Phase 3 to include the four
    new analytical agents.
    """
    print("  [Quality Check] Evaluating data completeness...")

    agent_status = state.get("agent_status", {})
    market_data = state.get("market_data", {})
    rate_data = state.get("rate_data", {})
    rate_sensitivity = state.get("rate_sensitivity", {})
    dividend_data = state.get("dividend_data", {})
    prospectus_terms = state.get("prospectus_terms", {})
    call_analysis = state.get("call_analysis", {})
    tax_analysis = state.get("tax_analysis", {})
    regulatory_analysis = state.get("regulatory_analysis", {})
    relative_value = state.get("relative_value", {})

    checks = {}

    # Market data checks
    has_price = market_data.get("price") is not None
    has_yield = market_data.get("dividend_yield") is not None
    has_name = market_data.get("name") not in (None, "Unknown")
    checks["market_data"] = {
        "has_price": has_price,
        "has_yield": has_yield,
        "has_name": has_name,
        "score": sum([has_price, has_yield, has_name]) / 3,
    }

    # Rate data checks
    has_rates = len(rate_data) >= 3
    has_long_rate = "10Y" in rate_data or "20Y" in rate_data
    has_rate_profile = rate_sensitivity.get("regime") is not None and not rate_sensitivity.get("error")
    checks["rate_data"] = {
        "has_sufficient_points": has_rates,
        "has_long_rate": has_long_rate,
        "has_sensitivity_profile": has_rate_profile,
        "score": sum([has_rates, has_long_rate, has_rate_profile]) / 3,
    }

    # Dividend data checks
    has_div_history = dividend_data.get("has_dividend_history", False)
    has_frequency = dividend_data.get("frequency") is not None
    checks["dividend_data"] = {
        "has_history": has_div_history,
        "has_frequency": has_frequency,
        "score": sum([has_div_history, has_frequency]) / 2,
    }

    # Prospectus extraction checks
    has_security_name = prospectus_terms.get("security_name") is not None
    has_coupon = prospectus_terms.get("coupon_rate") is not None
    has_structure = any(
        prospectus_terms.get(key) is not None
        for key in ("call_date", "call_price", "par_value", "perpetual", "coupon_type")
    )
    checks["prospectus_terms"] = {
        "has_security_name": has_security_name,
        "has_coupon_rate": has_coupon,
        "has_structure_terms": has_structure,
        "score": sum([has_security_name, has_coupon, has_structure]) / 3,
    }

    # Call analysis checks (Phase 3)
    has_call_prob = call_analysis.get("call_probability") is not None
    has_ytw = call_analysis.get("yield_to_worst_pct") is not None
    checks["call_analysis"] = {
        "has_call_probability": has_call_prob,
        "has_yield_to_worst": has_ytw,
        "score": sum([has_call_prob, has_ytw]) / 2,
    }

    # Tax analysis checks (Phase 3)
    has_qdi = tax_analysis.get("qdi_eligible") is not None
    has_tey = tax_analysis.get("tax_equivalent_yield_pct") is not None
    has_after_tax = tax_analysis.get("after_tax_yield_pct") is not None
    checks["tax_analysis"] = {
        "has_qdi_classification": has_qdi,
        "has_tax_equivalent_yield": has_tey,
        "has_after_tax_yield": has_after_tax,
        "score": sum([has_qdi, has_tey, has_after_tax]) / 3,
    }

    # Regulatory analysis checks (Phase 3)
    has_sector = regulatory_analysis.get("sector") is not None
    has_risk_level = regulatory_analysis.get("regulatory_risk_level") is not None
    checks["regulatory_analysis"] = {
        "has_sector": has_sector,
        "has_risk_level": has_risk_level,
        "score": sum([has_sector, has_risk_level]) / 2,
    }

    # Relative value checks (Phase 3)
    has_peers = (relative_value.get("peer_count") or 0) > 0
    has_value = relative_value.get("value_assessment") is not None
    checks["relative_value"] = {
        "has_peers": has_peers,
        "has_value_assessment": has_value,
        "score": sum([has_peers, has_value]) / 2,
    }

    # Overall quality score (weighted)
    overall_score = (
        checks["market_data"]["score"] * 0.20 +
        checks["rate_data"]["score"] * 0.10 +
        checks["dividend_data"]["score"] * 0.10 +
        checks["prospectus_terms"]["score"] * 0.25 +
        checks["call_analysis"]["score"] * 0.10 +
        checks["tax_analysis"]["score"] * 0.10 +
        checks["regulatory_analysis"]["score"] * 0.05 +
        checks["relative_value"]["score"] * 0.10
    )

    passed = overall_score >= 0.50 and (has_price or has_security_name)

    quality_report = {
        "checks": checks,
        "overall_score": round(overall_score, 2),
        "passed": passed,
        "decision": "proceed_to_synthesis" if passed else "generate_error_report",
        "missing_data": [k for k, v in checks.items() if v["score"] < 0.5],
    }

    status_icon = "PASS" if passed else "FAIL"
    print(f"  [Quality Check] {status_icon} (score: {overall_score:.2f})")

    return {"quality_report": quality_report}


# ---------------------------------------------------------------------------
# Conditional Edge
# ---------------------------------------------------------------------------

def route_after_quality_check(state: AdvancedSwarmState) -> Literal["synthesis_agent", "error_report_agent"]:
    """Conditional edge: route to synthesis or error report based on quality."""
    quality_report = state.get("quality_report", {})
    if quality_report.get("passed", False):
        return "synthesis_agent"
    else:
        return "error_report_agent"


# ---------------------------------------------------------------------------
# Layer 5: Synthesis Agent
# ---------------------------------------------------------------------------

def synthesis_agent(state: AdvancedSwarmState) -> dict:
    """Use Gemini to produce a comprehensive analysis combining all data sources."""
    print("  [Synthesis Agent] Generating analysis with Gemini...")

    from src.utils.config import get_llm
    llm = get_llm(temperature=0.3)

    market_data = state["market_data"]
    rate_data = state["rate_data"]
    rate_sensitivity = state.get("rate_sensitivity", {})
    dividend_data = state["dividend_data"]
    prospectus_terms = state.get("prospectus_terms", {})
    call_analysis = state.get("call_analysis", {})
    tax_analysis = state.get("tax_analysis", {})
    regulatory_analysis = state.get("regulatory_analysis", {})
    relative_value = state.get("relative_value", {})

    # Build pre-computed context
    ticker = market_data.get("ticker", state.get("ticker", "N/A"))
    issuer = (
        prospectus_terms.get("issuer")
        or market_data.get("name", "Unknown Issuer")
    )
    issuer = str(issuer).replace("Preferred Stock", "").strip()
    current_price = market_data.get("price", 0.0) or 0.0
    raw_yield = market_data.get("dividend_yield") or 0.0
    div_yield = raw_yield * 100 if raw_yield < 1 else raw_yield
    ten_yr_yield = rate_data.get("10Y") or rate_data.get("20Y") or 0.0
    spread_bps = int((div_yield - ten_yr_yield) * 100) if ten_yr_yield else 0
    fifty_two_week_high = market_data.get("fifty_two_week_high") or 0.0
    fifty_two_week_low = market_data.get("fifty_two_week_low") or 0.0

    if current_price > 0 and fifty_two_week_high > 0:
        if current_price >= fifty_two_week_high * 0.95:
            trading_range_desc = "near its 52-week high"
        elif fifty_two_week_low > 0 and current_price <= fifty_two_week_low * 1.05:
            trading_range_desc = "near its 52-week low"
        else:
            trading_range_desc = "in its mid-range"
    else:
        trading_range_desc = "at an undetermined range"

    call_price_equiv = _normalize_prospectus_amount(
        prospectus_terms.get("call_price"), prospectus_terms
    )
    par_value_equiv = _normalize_prospectus_amount(
        prospectus_terms.get("par_value"), prospectus_terms
    )
    comparison_anchor = call_price_equiv or par_value_equiv
    premium_to_anchor = (
        round(current_price - comparison_anchor, 2)
        if current_price and comparison_anchor is not None
        else None
    )
    anchor_label = (
        "call value"
        if call_price_equiv is not None
        else "par value"
        if par_value_equiv is not None
        else "not available"
    )

    comparison_anchor_text = f"${comparison_anchor:.2f}" if comparison_anchor is not None else "N/A"
    premium_to_anchor_text = f"{premium_to_anchor:+.2f}" if premium_to_anchor is not None else "N/A"

    # Tax context
    qdi_flag = tax_analysis.get("qdi_eligible")
    if qdi_flag is True:
        qdi_summary = "QDI eligible (qualified dividend income)"
    elif qdi_flag is False:
        qdi_summary = "Not QDI eligible (ordinary income)"
    else:
        qdi_summary = "QDI eligibility not determined"

    system_prompt = (
        "You are an expert financial analyst specializing in preferred equity securities. "
        "Your task is to synthesize the provided market, rate, dividend, SEC prospectus, "
        "call probability, tax, regulatory, and relative value data into a concise, "
        "professional research note suitable for an institutional investor. "
        "Output must be in Markdown format with clear headings. "
        "Do NOT include any raw JSON, base64 strings, or technical metadata in your output. "
        "Do not use em dashes or sentence dashes. "
        "The tone should be professional and objective. "
        "When prospectus terms are available, treat them as the ground truth for structural features."
    )

    user_prompt = f"""Produce a professional preferred equity research note for {ticker} issued by {issuer}.

Key pre-computed context:
- Current Price: ${current_price:.2f} ({trading_range_desc})
- Dividend Yield: {div_yield:.2f}%
- 10-Year Treasury Yield: {ten_yr_yield:.2f}%
- Spread over 10Y Treasury: {spread_bps} basis points
- Dividend Frequency: {dividend_data.get('frequency', 'N/A')}
- Dividend Consistency: {dividend_data.get('consistency', 'N/A')}
- Prospectus Security Name: {prospectus_terms.get('security_name', 'N/A')}
- Coupon: {prospectus_terms.get('coupon_rate', 'N/A')}% ({prospectus_terms.get('coupon_type', 'N/A')})
- First Call Date: {prospectus_terms.get('call_date', 'N/A')}
- Perpetual: {prospectus_terms.get('perpetual', 'N/A')}
- Per-depositary-share comparison anchor ({anchor_label}): {comparison_anchor_text}
- Premium / discount to comparison anchor: {premium_to_anchor_text}
- Rate Regime: {rate_sensitivity.get('regime', 'N/A')}
- Effective Duration: {rate_sensitivity.get('effective_duration', 'N/A')}
- Contractual Floating Benchmark: {rate_sensitivity.get('contractual_benchmark', 'N/A')}
- Live Benchmark Used: {rate_sensitivity.get('live_benchmark_label', 'N/A')}
- All-In Floating Coupon: {rate_sensitivity.get('all_in_floating_coupon_pct', rate_sensitivity.get('projected_post_reset_coupon_pct', 'N/A'))}
- Call Probability: {call_analysis.get('call_probability', 'N/A')} (score: {call_analysis.get('call_probability_score', 'N/A')})
- Yield-to-Call: {call_analysis.get('yield_to_call_pct', 'N/A')}%
- Yield-to-Worst: {call_analysis.get('yield_to_worst_pct', 'N/A')}%
- Refinancing Incentive: {call_analysis.get('refinancing_incentive', 'N/A')}
- QDI Status: {qdi_summary}
- After-Tax Yield: {tax_analysis.get('after_tax_yield_pct', 'N/A')}%
- Tax-Equivalent Yield: {tax_analysis.get('tax_equivalent_yield_pct', 'N/A')}%
- Tax Advantage (QDI vs ordinary): {tax_analysis.get('tax_advantage_bps', 'N/A')} bps
- Sector: {regulatory_analysis.get('sector', 'N/A')}
- G-SIB: {regulatory_analysis.get('is_gsib', 'N/A')}
- Capital Treatment: {regulatory_analysis.get('capital_treatment', 'N/A')}
- Regulatory Risk: {regulatory_analysis.get('regulatory_risk_level', 'N/A')}
- Dividend Deferral Risk: {regulatory_analysis.get('dividend_deferral_risk', 'N/A')}
- Relative Value: {relative_value.get('value_assessment', 'N/A')}
- Peer Count: {relative_value.get('peer_count', 'N/A')}
- Yield Rank: {relative_value.get('yield_rank', 'N/A')}
- Spread to Common: {relative_value.get('spread_to_common_bps', 'N/A')} bps

Agent summaries for deeper context:
- Call Analysis: {call_analysis.get('call_analysis_summary', 'N/A')}
- Tax Analysis: {tax_analysis.get('tax_summary', 'N/A')}
- Regulatory: {regulatory_analysis.get('regulatory_summary', 'N/A')}
- Relative Value: {relative_value.get('relative_value_summary', 'N/A')}

Structure your report with these sections:
1. Executive Summary
2. Security Overview (prospectus terms, structure, depositary share context)
3. Call Risk Analysis (YTC, YTW, call probability, refinancing incentive)
4. Interest Rate Sensitivity (duration, DV01, rate regime, benchmark context)
5. Tax and Yield Profile (QDI, after-tax yield, tax-equivalent yield)
6. Regulatory and Sector Risk (G-SIB status, AT1 treatment, deferral risk)
7. Relative Value (peer comparison, spread analysis, value assessment)
8. Conclusion and Key Considerations

Specific guidance:
- When the security is issued as depositary shares, compare the market price to the per-depositary-share equivalent amounts, not the underlying preference.
- If prospectus fields are missing, say so briefly instead of inventing terms.
- Discuss whether the security is trading above or below its call or par anchor.
- Keep the note concise and investment-oriented.
- Do not output any JSON or raw data."""

    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=user_prompt),
    ]

    response = llm.invoke(messages)
    print(f"  [Synthesis Agent] Gemini response received.")

    raw = response.content
    if isinstance(raw, list):
        content = "\n".join(
            block["text"] for block in raw
            if isinstance(block, dict) and block.get("type") == "text"
        ).strip()
    else:
        content = str(raw).strip()

    if content.startswith("```markdown") and content.endswith("```"):
        content = content[len("```markdown\n"):-len("\n```")].strip()
    elif content.startswith("```") and content.endswith("```"):
        content = content[len("```\n"):-len("\n```")].strip()

    return {"synthesis": content}


# ---------------------------------------------------------------------------
# Error Report Agent
# ---------------------------------------------------------------------------

def error_report_agent(state: AdvancedSwarmState) -> dict:
    """Generate a structured error report when data quality is insufficient."""
    print("  [Error Report Agent] Generating error report...")

    quality_report = state.get("quality_report", {})
    errors = state.get("errors", [])

    missing = quality_report.get("missing_data", [])
    score = quality_report.get("overall_score", 0)

    report = (
        f"## Analysis Could Not Be Completed\n\n"
        f"The data quality score was {score:.0%}, which is below the 50% threshold "
        f"required for a reliable analysis.\n\n"
        f"### Missing or Insufficient Data\n\n"
    )

    for item in missing:
        report += f"- **{item}**: Data was incomplete or unavailable\n"

    if errors:
        report += f"\n### Agent Errors\n\n"
        for err in errors:
            report += f"- {err}\n"

    report += (
        f"\n### Recommended Actions\n\n"
        f"1. Verify the ticker symbol is correct and represents a preferred stock\n"
        f"2. Check if the security is still actively traded\n"
        f"3. Try an alternative data source for the missing information\n"
    )

    print("  [Error Report Agent] Done.")
    return {"synthesis": report}


# ---------------------------------------------------------------------------
# Build the Graph
# ---------------------------------------------------------------------------

def build_advanced_graph() -> StateGraph:
    """Construct the Phase 3 swarm with a cache-aware execution order
    and conditional routing.

    Layer 1: Prospectus + Rate Context
    Layer 2: Market Data + Dividend (after prospectus terms/cache are available)
    Layer 3: Interest Rate Agent (fan-in)
    Layer 3: 4 analytical agents in parallel (fan-out from Layer 2)
    Layer 4: Quality Gate (fan-in from Layer 3)
    Layer 5: Conditional routing to Synthesis or Error Report
    """
    workflow = StateGraph(AdvancedSwarmState)

    # Register all nodes
    # Layer 1
    workflow.add_node("market_data_agent", market_data_agent)
    workflow.add_node("rate_context_agent", rate_context_agent)
    workflow.add_node("dividend_agent", dividend_agent)
    workflow.add_node("prospectus_agent", prospectus_agent)
    # Layer 2
    workflow.add_node("interest_rate_agent", interest_rate_agent)
    # Layer 3
    workflow.add_node("call_probability_agent", call_probability_agent)
    workflow.add_node("tax_yield_agent", tax_yield_agent)
    workflow.add_node("regulatory_agent", regulatory_agent)
    workflow.add_node("relative_value_agent", relative_value_agent)
    # Layer 4
    workflow.add_node("quality_check_agent", quality_check_agent)
    # Layer 5
    workflow.add_node("synthesis_agent", synthesis_agent)
    workflow.add_node("error_report_agent", error_report_agent)

    # --- Layer 1: Prospectus + Rate Context ---
    workflow.add_edge(START, "prospectus_agent")
    workflow.add_edge(START, "rate_context_agent")

    # --- Layer 2: Market Data + Dividend after Prospectus ---
    workflow.add_edge("prospectus_agent", "market_data_agent")
    workflow.add_edge("prospectus_agent", "dividend_agent")

    # --- Layer 3: Fan-In to Interest Rate Agent ---
    workflow.add_edge(
        ["market_data_agent", "rate_context_agent", "dividend_agent", "prospectus_agent"],
        "interest_rate_agent",
    )

    # --- Layer 4: Parallel Fan-Out from Interest Rate Agent ---
    workflow.add_edge("interest_rate_agent", "call_probability_agent")
    workflow.add_edge("interest_rate_agent", "tax_yield_agent")
    workflow.add_edge("interest_rate_agent", "regulatory_agent")
    workflow.add_edge("interest_rate_agent", "relative_value_agent")

    # --- Layer 5: Fan-In to Quality Gate ---
    workflow.add_edge(
        ["call_probability_agent", "tax_yield_agent", "regulatory_agent", "relative_value_agent"],
        "quality_check_agent",
    )

    # --- Layer 6: Conditional Routing ---
    workflow.add_conditional_edges(
        "quality_check_agent",
        route_after_quality_check,
        {
            "synthesis_agent": "synthesis_agent",
            "error_report_agent": "error_report_agent",
        }
    )

    workflow.add_edge("synthesis_agent", END)
    workflow.add_edge("error_report_agent", END)

    return workflow.compile()


# ---------------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------------

def analyze_preferred_advanced(ticker: str) -> dict:
    """Run the advanced swarm on a single preferred stock ticker.

    Args:
        ticker: Preferred stock ticker (e.g., 'BAC-PL')

    Returns:
        Dictionary with the complete swarm state after execution
    """
    graph = build_advanced_graph()

    initial_state = {
        "ticker": ticker,
        "market_data": {},
        "rate_data": {},
        "rate_sensitivity": {},
        "dividend_data": {},
        "prospectus_terms": {},
        "call_analysis": {},
        "tax_analysis": {},
        "regulatory_analysis": {},
        "relative_value": {},
        "quality_report": {},
        "synthesis": "",
        "errors": [],
        "agent_status": {},
    }

    print(f"\n{'='*70}")
    print(f"  PREFERRED EQUITY ANALYSIS SWARM (Phase 3): Analyzing {ticker}")
    print(f"{'='*70}")
    print(f"  Layer 1: Prospectus | Rate Context")
    print(f"  Layer 2: Market Data | Dividend")
    print(f"  Layer 3: Interest Rate Sensitivity")
    print(f"  Layer 4: Call Probability | Tax & Yield | Regulatory | Relative Value")
    print(f"  Quality Gate: Enabled")
    print(f"  Conditional Routing: Synthesis or Error Report")
    print(f"{'='*70}\n")

    result = graph.invoke(initial_state)

    quality = result.get("quality_report", {})
    print(f"\n{'='*70}")
    print(f"  ANALYSIS COMPLETE")
    print(f"  Quality Score: {quality.get('overall_score', 'N/A')}")
    print(f"  Route Taken: {quality.get('decision', 'N/A')}")
    print(f"{'='*70}\n")

    return result


if __name__ == "__main__":
    result = analyze_preferred_advanced("BAC-PL")

    print("\n--- SYNTHESIS ---\n")
    print(result["synthesis"])

    print("\n--- QUALITY REPORT ---\n")
    print(json.dumps(result["quality_report"], indent=2))
