"""
Hello World Preferred Equity Swarm
===================================
A three-agent LangGraph system that demonstrates the core swarm pattern:

1. Market Data Agent: Fetches preferred stock info from Alpha Vantage
2. Rate Context Agent: Fetches Treasury yield data for rate comparison
3. Synthesis Agent: Combines both outputs into a preliminary analysis

This is the Phase 0 "vertical slice" that proves the technology stack works
end-to-end before building the full eight-agent swarm.
"""

import os
import json
from typing import TypedDict
from langgraph.graph import StateGraph, END
from langchain_core.messages import HumanMessage, SystemMessage


# ---------------------------------------------------------------------------
# State Schema: The shared data structure that flows through the graph
# ---------------------------------------------------------------------------

class SwarmState(TypedDict):
    """State that is passed between agents in the swarm."""
    ticker: str
    market_data: dict
    rate_data: dict
    synthesis: str
    errors: list


# ---------------------------------------------------------------------------
# Agent Node Functions
# ---------------------------------------------------------------------------

def market_data_agent(state: SwarmState) -> dict:
    """
    Agent 1: Fetches market data for the preferred stock from Alpha Vantage.
    This is a 'tool agent' that executes a deterministic data fetch.
    """
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
    from src.data.market_data import get_preferred_info, get_dividend_history

    ticker = state["ticker"]
    print(f"[Market Data Agent] Fetching data for {ticker}...")

    info = get_preferred_info(ticker)
    
    # Also fetch recent dividend history
    div_hist = get_dividend_history(ticker)
    if div_hist is not None and not div_hist.empty:
        recent_divs = div_hist.tail(4)
        info["recent_dividends"] = [
            {"date": str(d), "amount": round(float(v), 4)}
            for d, v in zip(recent_divs.index, recent_divs["dividend"])
        ]
    else:
        info["recent_dividends"] = []

    print(f"[Market Data Agent] Done. Price: ${info.get('price', 'N/A')}")
    return {"market_data": info}


def rate_context_agent(state: SwarmState) -> dict:
    """
    Agent 2: Fetches Treasury yield curve data for rate context.
    This provides the interest rate backdrop for evaluating the preferred's yield.
    """
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
    from src.data.rate_data import get_treasury_yields

    print("[Rate Context Agent] Fetching Treasury yield data...")

    yields = get_treasury_yields()

    print(f"[Rate Context Agent] Done. Got {len(yields)} yield curve points.")
    return {"rate_data": yields}


def synthesis_agent(state: SwarmState) -> dict:
    """
    Agent 3: Uses Gemini to synthesize market data and rate context
    into a preliminary preferred equity analysis.
    This is the 'reasoning agent' that uses the LLM.
    """
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
    from src.utils.config import get_llm

    print("[Synthesis Agent] Generating analysis with Gemini...")

    llm = get_llm(temperature=0.3)

    market_data = state["market_data"]
    rate_data = state["rate_data"]

    system_prompt = """You are a preferred equity analyst. You receive market data 
about a preferred stock and current Treasury yield curve data. Produce a brief 
preliminary analysis covering:

1. Basic security overview (what it is, who issued it)
2. Current yield assessment (how does the preferred's yield compare to Treasuries?)
3. Key observations (anything notable about the price, volume, or dividend pattern)
4. Questions for deeper analysis (what would you want to investigate further?)

Keep the analysis concise (200-300 words). Use professional financial language.
Do not use em dashes or sentence dashes. Write in complete paragraphs."""

    user_prompt = f"""Analyze this preferred stock:

MARKET DATA:
{json.dumps(market_data, indent=2, default=str)}

TREASURY YIELD CURVE:
{json.dumps(rate_data, indent=2)}

Provide your preliminary analysis."""

    response = llm.invoke([
        SystemMessage(content=system_prompt),
        HumanMessage(content=user_prompt),
    ])

    analysis = response.content
    print("[Synthesis Agent] Done.")
    return {"synthesis": analysis}


# ---------------------------------------------------------------------------
# Build the LangGraph Workflow
# ---------------------------------------------------------------------------

def build_hello_world_graph() -> StateGraph:
    """
    Constructs the three-agent LangGraph workflow.
    
    Graph structure:
        START -> market_data_agent -> rate_context_agent -> synthesis_agent -> END
    
    This is a simple sequential chain for the hello world demo.
    Parallel execution is demonstrated in advanced_swarm.py.
    """
    workflow = StateGraph(SwarmState)

    # Add agent nodes
    workflow.add_node("market_data_agent", market_data_agent)
    workflow.add_node("rate_context_agent", rate_context_agent)
    workflow.add_node("synthesis_agent", synthesis_agent)

    # Define edges: sequential for the hello world version
    workflow.set_entry_point("market_data_agent")
    workflow.add_edge("market_data_agent", "rate_context_agent")
    workflow.add_edge("rate_context_agent", "synthesis_agent")
    workflow.add_edge("synthesis_agent", END)

    return workflow.compile()


# ---------------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------------

def analyze_preferred(ticker: str) -> dict:
    """
    Run the hello world swarm on a single preferred stock ticker.
    
    Args:
        ticker: Preferred stock ticker (e.g., 'BAC-PL')
    
    Returns:
        Dictionary with the complete swarm state after execution
    """
    graph = build_hello_world_graph()

    initial_state = {
        "ticker": ticker,
        "market_data": {},
        "rate_data": {},
        "synthesis": "",
        "errors": [],
    }

    print(f"\n{'='*60}")
    print(f"  PREFERRED EQUITY SWARM: Analyzing {ticker}")
    print(f"{'='*60}\n")

    result = graph.invoke(initial_state)

    print(f"\n{'='*60}")
    print(f"  ANALYSIS COMPLETE")
    print(f"{'='*60}\n")

    return result


if __name__ == "__main__":
    # Demo: Analyze Bank of America Series L Preferred
    result = analyze_preferred("BAC-PL")
    
    print("\n--- SYNTHESIS ---\n")
    print(result["synthesis"])
