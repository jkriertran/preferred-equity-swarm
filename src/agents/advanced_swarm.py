"""
Advanced Preferred Equity Swarm
================================
Demonstrates three key LangGraph patterns beyond the hello world version:

1. PARALLEL FAN-OUT / FAN-IN:
   Three data agents run simultaneously, then converge at a single point.

2. CONDITIONAL ROUTING:
   A quality-check agent inspects the collected data and routes to either
   the synthesis agent (if data is sufficient) or an error handler (if not).

3. FEEDBACK LOOP (CYCLE):
   If the quality check identifies missing data, it can route back to
   retry a specific agent before proceeding.

Graph Structure:
                        +-- market_data_agent ---+
                        |                        |
    START --[fan-out]---+-- rate_context_agent ---+--[fan-in]--> quality_check
                        |                        |                    |
                        +-- dividend_agent ------+              [conditional]
                                                               /         \\
                                                         [pass]         [fail]
                                                           |               |
                                                    synthesis_agent    error_report
                                                           |               |
                                                          END             END
"""

import os
import sys
import json
import operator
from typing import TypedDict, Annotated, Literal
from langgraph.graph import StateGraph, END, START
from langchain_core.messages import HumanMessage, SystemMessage
# LLM is provided by the centralized get_llm() factory in config.py


def merge_dicts(left: dict, right: dict) -> dict:
    """Reducer that merges two dictionaries. Used for concurrent state updates."""
    merged = left.copy()
    merged.update(right)
    return merged


def merge_lists(left: list, right: list) -> list:
    """Reducer that concatenates two lists. Used for concurrent error accumulation."""
    return left + right

# Ensure project root is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


# ---------------------------------------------------------------------------
# State Schema (Extended)
# ---------------------------------------------------------------------------

class AdvancedSwarmState(TypedDict):
    """Extended state with fields for all four agents plus quality tracking.
    
    Fields that receive concurrent updates from parallel agents use Annotated
    types with reducer functions. This tells LangGraph how to merge multiple
    updates that arrive at the same time.
    
    For example, when market_data_agent and rate_context_agent both write to
    agent_status simultaneously, the merge_dicts reducer combines their updates
    instead of raising a conflict error.
    """
    ticker: str
    market_data: dict
    rate_data: dict
    dividend_data: dict
    quality_report: dict
    synthesis: str
    errors: Annotated[list, merge_lists]            # Concurrent error accumulation
    agent_status: Annotated[dict, merge_dicts]      # Concurrent status tracking


# ---------------------------------------------------------------------------
# Agent 1: Market Data Agent (Tool Agent)
# ---------------------------------------------------------------------------

def market_data_agent(state: AdvancedSwarmState) -> dict:
    """Fetches market data from Yahoo Finance."""
    from src.data.market_data import get_preferred_info

    ticker = state["ticker"]
    print(f"  [Market Data Agent] Fetching data for {ticker}...")

    info = get_preferred_info(ticker)
    
    # Track agent status
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


# ---------------------------------------------------------------------------
# Agent 2: Rate Context Agent (Tool Agent)
# ---------------------------------------------------------------------------

def rate_context_agent(state: AdvancedSwarmState) -> dict:
    """Fetches Treasury yield curve data."""
    from src.data.rate_data import get_treasury_yields_from_yfinance

    print("  [Rate Context Agent] Fetching Treasury yield data...")

    yields = get_treasury_yields_from_yfinance()
    
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


# ---------------------------------------------------------------------------
# Agent 3: Dividend Analysis Agent (Tool Agent) -- NEW
# ---------------------------------------------------------------------------

def dividend_agent(state: AdvancedSwarmState) -> dict:
    """Analyzes dividend payment patterns and consistency."""
    from src.data.dividend_analysis import analyze_dividend_pattern

    ticker = state["ticker"]
    print(f"  [Dividend Agent] Analyzing dividend pattern for {ticker}...")

    analysis = analyze_dividend_pattern(ticker)
    
    status = state.get("agent_status", {})
    if not analysis.get("has_dividend_history", False):
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


# ---------------------------------------------------------------------------
# Agent 4: Quality Check Agent (Conditional Router) -- NEW PATTERN
# ---------------------------------------------------------------------------

def quality_check_agent(state: AdvancedSwarmState) -> dict:
    """
    Inspects the outputs of all data agents and produces a quality report.
    This agent does NOT use an LLM. It applies deterministic rules to decide
    whether the data is sufficient for synthesis.
    
    This is the CONDITIONAL ROUTING pattern: the quality check's output
    determines which node runs next.
    """
    print("  [Quality Check] Evaluating data completeness...")

    agent_status = state.get("agent_status", {})
    market_data = state.get("market_data", {})
    rate_data = state.get("rate_data", {})
    dividend_data = state.get("dividend_data", {})

    # Score each data source
    checks = {}
    
    # Market data checks
    has_price = market_data.get("price") is not None
    has_yield = market_data.get("dividend_yield") is not None
    has_name = market_data.get("name") not in (None, "Unknown")
    checks["market_data"] = {
        "has_price": has_price,
        "has_yield": has_yield,
        "has_name": has_name,
        "score": sum([has_price, has_yield, has_name]) / 3
    }
    
    # Rate data checks
    has_rates = len(rate_data) >= 3
    has_long_rate = "10Y" in rate_data or "20Y" in rate_data
    checks["rate_data"] = {
        "has_sufficient_points": has_rates,
        "has_long_rate": has_long_rate,
        "score": sum([has_rates, has_long_rate]) / 2
    }
    
    # Dividend data checks
    has_div_history = dividend_data.get("has_dividend_history", False)
    has_frequency = dividend_data.get("frequency") is not None
    checks["dividend_data"] = {
        "has_history": has_div_history,
        "has_frequency": has_frequency,
        "score": sum([has_div_history, has_frequency]) / 2
    }
    
    # Overall quality score
    overall_score = (
        checks["market_data"]["score"] * 0.4 +
        checks["rate_data"]["score"] * 0.3 +
        checks["dividend_data"]["score"] * 0.3
    )
    
    # Decision: pass if score >= 0.5 (we can synthesize with partial data)
    passed = overall_score >= 0.5
    
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
# Conditional Edge Function
# ---------------------------------------------------------------------------

def route_after_quality_check(state: AdvancedSwarmState) -> Literal["synthesis_agent", "error_report_agent"]:
    """
    This function is used as a CONDITIONAL EDGE in the graph.
    It reads the quality report and returns the name of the next node.
    
    LangGraph calls this function after the quality_check_agent runs,
    and uses the return value to decide which edge to follow.
    """
    quality_report = state.get("quality_report", {})
    if quality_report.get("passed", False):
        return "synthesis_agent"
    else:
        return "error_report_agent"


# ---------------------------------------------------------------------------
# Agent 5: Synthesis Agent (Reasoning Agent)
# ---------------------------------------------------------------------------

def synthesis_agent(state: AdvancedSwarmState) -> dict:
    """
    Uses Gemini to produce a comprehensive analysis combining all data sources.
    Only runs if the quality check passes.
    """
    print("  [Synthesis Agent] Generating analysis with Gemini...")

    from src.utils.config import get_llm
    llm = get_llm(temperature=0.3)

    market_data = state["market_data"]
    rate_data = state["rate_data"]
    dividend_data = state["dividend_data"]

    # Build the institutional-grade prompt inline (no external file dependency)
    ticker = market_data.get("ticker", state.get("ticker", "N/A"))
    issuer = market_data.get("name", "Unknown Issuer").replace("Preferred Stock", "").strip()
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

    system_prompt = (
        "You are an expert financial analyst specializing in preferred equity securities. "
        "Your task is to synthesize the provided market, rate, and dividend data for a given "
        "preferred stock into a concise, professional research note suitable for an institutional investor. "
        "Output must be in Markdown format with clear headings. "
        "Do NOT include any raw JSON, base64 strings, or technical metadata in your output. "
        "Do not use em dashes or sentence dashes. "
        "The tone should be professional and objective."
    )

    user_prompt = f"""Produce a professional preferred equity research note for {ticker} issued by {issuer}.

Key pre-computed context:
- Current Price: ${current_price:.2f} ({trading_range_desc})
- Dividend Yield: {div_yield:.2f}%
- 10-Year Treasury Yield: {ten_yr_yield:.2f}%
- Spread over 10Y Treasury: {spread_bps} basis points
- Dividend Frequency: {dividend_data.get('frequency', 'N/A')}
- Dividend Consistency: {dividend_data.get('consistency', 'N/A')}
- Trailing Annual Dividend: ${dividend_data.get('trailing_annual_dividends', 0.0):.4f}

Full data for deeper analysis:
MARKET DATA: {json.dumps(market_data, indent=2, default=str)}
TREASURY YIELD CURVE: {json.dumps(rate_data, indent=2)}
DIVIDEND ANALYSIS: {json.dumps(dividend_data, indent=2, default=str)}

Structure your report with these sections:
1. Executive Summary
2. Security Overview
3. Risk Analysis (Interest Rate Sensitivity, Credit Risk, Call Risk)
4. Dividend Profile (Dividend Safety and Consistency, Yield Analysis)
5. Conclusion and Key Considerations

Replace all bracketed placeholders with actual calculated values. Do not output any JSON or raw data."""

    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=user_prompt),
    ]

    response = llm.invoke(messages)
    print(f"  [Synthesis Agent] Gemini response received.")

    # Extract clean text content from the response.
    # Gemini via langchain-google-genai sometimes returns response.content as a
    # list of dicts: [{'type': 'text', 'text': '...', 'extras': {...}}]
    # rather than a plain string. We handle both cases here.
    raw = response.content
    if isinstance(raw, list):
        # Extract text from all text-type blocks and join them
        content = "\n".join(
            block["text"] for block in raw
            if isinstance(block, dict) and block.get("type") == "text"
        ).strip()
    else:
        content = str(raw).strip()

    # Clean up any potential markdown code block wrappers
    if content.startswith("```markdown") and content.endswith("```"):
        content = content[len("```markdown\n"):-len("\n```")].strip()
    elif content.startswith("```") and content.endswith("```"):
        content = content[len("```\n"):-len("\n```")].strip()

    return {"synthesis": content}


# ---------------------------------------------------------------------------
# Agent 6: Error Report Agent (Fallback)
# ---------------------------------------------------------------------------

def error_report_agent(state: AdvancedSwarmState) -> dict:
    """
    Generates a structured error report when data quality is insufficient.
    This runs instead of the synthesis agent when the quality check fails.
    """
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
# Build the Advanced Graph
# ---------------------------------------------------------------------------

def build_advanced_graph() -> StateGraph:
    """
    Constructs the advanced swarm with parallel execution and conditional routing.
    
    Key LangGraph patterns demonstrated:
    
    1. PARALLEL FAN-OUT: Three agents start from the same entry point
       and run concurrently (market_data, rate_context, dividend)
    
    2. FAN-IN: All three converge at quality_check_agent
    
    3. CONDITIONAL EDGE: quality_check routes to either synthesis or error_report
    """
    workflow = StateGraph(AdvancedSwarmState)

    # Register all agent nodes
    workflow.add_node("market_data_agent", market_data_agent)
    workflow.add_node("rate_context_agent", rate_context_agent)
    workflow.add_node("dividend_agent", dividend_agent)
    workflow.add_node("quality_check_agent", quality_check_agent)
    workflow.add_node("synthesis_agent", synthesis_agent)
    workflow.add_node("error_report_agent", error_report_agent)

    # PATTERN 1: Parallel Fan-Out
    # All three data agents start from START and run in parallel
    workflow.add_edge(START, "market_data_agent")
    workflow.add_edge(START, "rate_context_agent")
    workflow.add_edge(START, "dividend_agent")

    # PATTERN 2: Fan-In
    # All three data agents converge at the quality check
    workflow.add_edge("market_data_agent", "quality_check_agent")
    workflow.add_edge("rate_context_agent", "quality_check_agent")
    workflow.add_edge("dividend_agent", "quality_check_agent")

    # PATTERN 3: Conditional Routing
    # Quality check decides which agent runs next
    workflow.add_conditional_edges(
        "quality_check_agent",
        route_after_quality_check,
        {
            "synthesis_agent": "synthesis_agent",
            "error_report_agent": "error_report_agent",
        }
    )

    # Both terminal agents lead to END
    workflow.add_edge("synthesis_agent", END)
    workflow.add_edge("error_report_agent", END)

    return workflow.compile()


# ---------------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------------

def analyze_preferred_advanced(ticker: str) -> dict:
    """
    Run the advanced swarm on a single preferred stock ticker.
    
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
        "dividend_data": {},
        "quality_report": {},
        "synthesis": "",
        "errors": [],
        "agent_status": {},
    }

    print(f"\n{'='*60}")
    print(f"  ADVANCED PREFERRED EQUITY SWARM: Analyzing {ticker}")
    print(f"{'='*60}")
    print(f"  Agents: Market Data | Rate Context | Dividend Analysis")
    print(f"  Quality Gate: Enabled")
    print(f"  Conditional Routing: Synthesis or Error Report")
    print(f"{'='*60}\n")

    result = graph.invoke(initial_state)

    quality = result.get("quality_report", {})
    print(f"\n{'='*60}")
    print(f"  ANALYSIS COMPLETE")
    print(f"  Quality Score: {quality.get('overall_score', 'N/A')}")
    print(f"  Route Taken: {quality.get('decision', 'N/A')}")
    print(f"{'='*60}\n")

    return result


if __name__ == "__main__":
    # Demo: Analyze Bank of America Series L Preferred
    result = analyze_preferred_advanced("BAC-PL")
    
    print("\n--- SYNTHESIS ---\n")
    print(result["synthesis"])
    
    print("\n--- QUALITY REPORT ---\n")
    print(json.dumps(result["quality_report"], indent=2))
