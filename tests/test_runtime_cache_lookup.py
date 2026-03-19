"""
Regression tests for runtime prospectus caches saved under accession numbers.
Run with: python3 tests/test_runtime_cache_lookup.py
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.data import dividend_analysis
from src.data import market_data
from src.data import prospectus_inventory
from src.data import security_resolver
from src.agents import prospectus_agent


def test_runtime_cache_lookup_finds_all_pi_by_ticker():
    terms = prospectus_inventory.load_cached_terms_for_ticker("ALL-PI")
    assert terms["ticker"] == "ALL-PI"
    assert terms["series"] == "Series I"
    assert terms["coupon_rate"] == 4.75


def test_security_resolver_marks_runtime_cache_as_available():
    result = security_resolver.validate_ticker_for_analysis("ALL-PI")
    assert result["valid"] is True
    assert result["resolution"]["has_prospectus_cache"] is True


def test_market_data_derives_dividend_fields_from_runtime_cache():
    original_quote = market_data.get_alpha_vantage_quote
    original_snapshot = market_data._get_snapshot_data
    try:
        market_data.get_alpha_vantage_quote = lambda ticker, require_preferred=True: {
            "name": "The Allstate Corporation",
            "close": "19.36",
            "currency": "USD",
            "_matched_symbol": "ALL-PI",
        }
        market_data._get_snapshot_data = lambda ticker: None

        info = market_data.get_preferred_info("ALL-PI")
        assert info["provider"] == "alpha_vantage"
        assert round(info["dividend_rate"], 4) == 1.1875
        assert round(info["dividend_yield"], 6) == round(1.1875 / 19.36, 6)
    finally:
        market_data.get_alpha_vantage_quote = original_quote
        market_data._get_snapshot_data = original_snapshot


def test_dividend_analysis_uses_runtime_cache_when_history_missing():
    original_get_dividend_history = dividend_analysis.get_dividend_history
    try:
        dividend_analysis.get_dividend_history = lambda ticker: None
        analysis = dividend_analysis.analyze_dividend_pattern("ALL-PI")
        assert analysis["has_dividend_history"] is False
        assert analysis["frequency"] == "quarterly"
        assert round(analysis["trailing_annual_dividends"], 4) == 1.1875
        assert analysis["source"] == "prospectus_terms"
    finally:
        dividend_analysis.get_dividend_history = original_get_dividend_history


def test_prospectus_agent_uses_cached_terms_before_live_lookup():
    original_load_cache = prospectus_agent.load_structured_terms_cache
    try:
        prospectus_agent.load_structured_terms_cache = (
            lambda ticker="", filing=None: {
                "ticker": "ALL-PH",
                "security_name": "Fixed Rate Noncumulative Perpetual Preferred Stock, Series H",
                "issuer": "The Allstate Corporation",
                "series": "Series H",
                "coupon_rate": 5.1,
                "coupon_type": "fixed",
                "confidence_score": 0.99,
                "resolution_source": "runtime",
                "validation": {"series_match": True, "validation_tokens": []},
            }
        )
        result = prospectus_agent.prospectus_agent_node(
            {"ticker": "ALL-PH", "errors": [], "agent_status": {}}
        )
        assert result["agent_status"]["prospectus"] == "success"
        assert result["prospectus_terms"]["ticker"] == "ALL-PH"
        assert result["prospectus_terms"]["source"] == "cache"
    finally:
        prospectus_agent.load_structured_terms_cache = original_load_cache


def test_alpha_vantage_failure_does_not_fallback_to_legacy_providers():
    original_alpha = market_data._get_preferred_info_from_alpha_vantage
    original_snapshot = market_data._get_snapshot_data
    try:
        market_data._get_preferred_info_from_alpha_vantage = (
            lambda ticker: {
                "ticker": ticker,
                "error": (
                    "No Alpha Vantage quote found for ticker. "
                    "Alpha symbology may require a provider_symbols.alpha_vantage override."
                ),
            }
        )
        market_data._get_snapshot_data = lambda ticker: None

        info = market_data.get_preferred_info("ALL-PH")
        assert info == {
            "ticker": "ALL-PH",
            "error": (
                "No Alpha Vantage quote found for ticker. "
                "Alpha symbology may require a provider_symbols.alpha_vantage override."
            ),
        }
    finally:
        market_data._get_preferred_info_from_alpha_vantage = original_alpha
        market_data._get_snapshot_data = original_snapshot


if __name__ == "__main__":
    test_runtime_cache_lookup_finds_all_pi_by_ticker()
    test_security_resolver_marks_runtime_cache_as_available()
    test_market_data_derives_dividend_fields_from_runtime_cache()
    test_dividend_analysis_uses_runtime_cache_when_history_missing()
    test_prospectus_agent_uses_cached_terms_before_live_lookup()
    test_alpha_vantage_failure_does_not_fallback_to_legacy_providers()
    print("Runtime cache lookup regression tests passed.")
