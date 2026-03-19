"""
Regression tests for swarm graph fan-in barriers.
Run with: python3 tests/test_swarm_graph_barriers.py
"""

import os
import sys
from collections import Counter

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.agents import advanced_swarm


def test_graph_runs_fan_in_nodes_once():
    calls = Counter()

    original_funcs = {
        "market_data_agent": advanced_swarm.market_data_agent,
        "rate_context_agent": advanced_swarm.rate_context_agent,
        "dividend_agent": advanced_swarm.dividend_agent,
        "prospectus_agent": advanced_swarm.prospectus_agent,
        "interest_rate_agent": advanced_swarm.interest_rate_agent,
        "call_probability_agent": advanced_swarm.call_probability_agent,
        "tax_yield_agent": advanced_swarm.tax_yield_agent,
        "regulatory_agent": advanced_swarm.regulatory_agent,
        "relative_value_agent": advanced_swarm.relative_value_agent,
        "quality_check_agent": advanced_swarm.quality_check_agent,
        "synthesis_agent": advanced_swarm.synthesis_agent,
        "error_report_agent": advanced_swarm.error_report_agent,
    }

    def market_data_agent(state):
        calls["market_data_agent"] += 1
        return {"market_data": {"price": 25.0, "name": "Stub Preferred", "dividend_yield": 0.05}}

    def rate_context_agent(state):
        calls["rate_context_agent"] += 1
        return {"rate_data": {"2Y": 4.0, "5Y": 4.2, "10Y": 4.4}}

    def dividend_agent(state):
        calls["dividend_agent"] += 1
        assert state.get("prospectus_terms", {}).get("security_name") == "Stub Preferred"
        return {"dividend_data": {"has_dividend_history": False, "frequency": "quarterly"}}

    def prospectus_agent(state):
        calls["prospectus_agent"] += 1
        return {"prospectus_terms": {"security_name": "Stub Preferred", "coupon_rate": 5.0, "coupon_type": "fixed"}}

    def interest_rate_agent(state):
        calls["interest_rate_agent"] += 1
        assert state.get("market_data", {}).get("price") == 25.0
        assert state.get("dividend_data", {}).get("frequency") == "quarterly"
        assert state.get("prospectus_terms", {}).get("security_name") == "Stub Preferred"
        assert "10Y" in state.get("rate_data", {})
        return {"rate_sensitivity": {"regime": "neutral"}}

    def call_probability_agent(state):
        calls["call_probability_agent"] += 1
        return {"call_analysis": {"call_probability": "low", "yield_to_worst_pct": 5.0}}

    def tax_yield_agent(state):
        calls["tax_yield_agent"] += 1
        return {"tax_analysis": {"qdi_eligible": True, "tax_equivalent_yield_pct": 6.0, "after_tax_yield_pct": 4.0}}

    def regulatory_agent(state):
        calls["regulatory_agent"] += 1
        return {"regulatory_analysis": {"sector": "financials", "regulatory_risk_level": "low"}}

    def relative_value_agent(state):
        calls["relative_value_agent"] += 1
        return {"relative_value": {"peer_count": 3, "value_assessment": "fair"}}

    def quality_check_agent(state):
        calls["quality_check_agent"] += 1
        return {"quality_report": {"passed": True, "decision": "proceed_to_synthesis", "overall_score": 1.0, "checks": {}}}

    def synthesis_agent(state):
        calls["synthesis_agent"] += 1
        return {"final_report": "ok"}

    def error_report_agent(state):
        calls["error_report_agent"] += 1
        return {"final_report": "error"}

    try:
        advanced_swarm.market_data_agent = market_data_agent
        advanced_swarm.rate_context_agent = rate_context_agent
        advanced_swarm.dividend_agent = dividend_agent
        advanced_swarm.prospectus_agent = prospectus_agent
        advanced_swarm.interest_rate_agent = interest_rate_agent
        advanced_swarm.call_probability_agent = call_probability_agent
        advanced_swarm.tax_yield_agent = tax_yield_agent
        advanced_swarm.regulatory_agent = regulatory_agent
        advanced_swarm.relative_value_agent = relative_value_agent
        advanced_swarm.quality_check_agent = quality_check_agent
        advanced_swarm.synthesis_agent = synthesis_agent
        advanced_swarm.error_report_agent = error_report_agent

        graph = advanced_swarm.build_advanced_graph()
        graph.invoke(
            {
                "ticker": "TEST-PA",
                "market_data": {},
                "rate_data": {},
                "dividend_data": {},
                "prospectus_terms": {},
                "rate_sensitivity": {},
                "call_analysis": {},
                "tax_analysis": {},
                "regulatory_analysis": {},
                "relative_value": {},
                "quality_report": {},
                "agent_status": {},
                "errors": [],
            }
        )

        assert calls["prospectus_agent"] == 1
        assert calls["market_data_agent"] == 1
        assert calls["dividend_agent"] == 1
        assert calls["interest_rate_agent"] == 1
        assert calls["call_probability_agent"] == 1
        assert calls["tax_yield_agent"] == 1
        assert calls["regulatory_agent"] == 1
        assert calls["relative_value_agent"] == 1
        assert calls["quality_check_agent"] == 1
        assert calls["synthesis_agent"] == 1
        assert calls["error_report_agent"] == 0
    finally:
        for name, func in original_funcs.items():
            setattr(advanced_swarm, name, func)


if __name__ == "__main__":
    test_graph_runs_fan_in_nodes_once()
    print("Swarm graph barrier regression tests passed.")
