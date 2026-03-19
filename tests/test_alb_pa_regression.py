"""
Regression tests for ALB-PA mandatory convertible coverage.
Run with: python3 tests/test_alb_pa_regression.py
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.data import dividend_analysis
from src.data import market_data
from src.data import tax_analysis
from src.data.edgar_pipeline import get_demo_filing_registry_entry


def test_alb_pa_registry_entry_exists():
    entry = get_demo_filing_registry_entry("ALB-PA")
    assert entry is not None
    assert entry["expected_series"] == "Series A"
    assert entry["accession_number"] == "0001193125-24-061293"


def test_alb_pa_market_data_derives_dividend_fields_from_demo_cache():
    original_quote = market_data.get_alpha_vantage_quote
    original_snapshot = market_data._get_snapshot_data
    try:
        market_data.get_alpha_vantage_quote = lambda ticker, require_preferred=True: {
            "name": "Albemarle Corporation",
            "close": "67.27",
            "currency": "USD",
            "_matched_symbol": "ALB-PA",
        }
        market_data._get_snapshot_data = lambda ticker: None

        info = market_data.get_preferred_info("ALB-PA")
        assert info["provider"] == "alpha_vantage"
        assert info["provider_symbol"] == "ALB-PA"
        assert round(info["dividend_rate"], 4) == 3.625
        assert round(info["dividend_yield"], 6) == round(3.625 / 67.27, 6)
    finally:
        market_data.get_alpha_vantage_quote = original_quote
        market_data._get_snapshot_data = original_snapshot


def test_alb_pa_dividend_fallback_uses_cached_terms():
    original_get_dividend_history = dividend_analysis.get_dividend_history
    try:
        dividend_analysis.get_dividend_history = lambda ticker: None
        analysis = dividend_analysis.analyze_dividend_pattern("ALB-PA")
        assert analysis["has_dividend_history"] is False
        assert analysis["frequency"] == "quarterly"
        assert round(analysis["trailing_annual_dividends"], 4) == 3.625
        assert analysis["source"] == "prospectus_terms"
    finally:
        dividend_analysis.get_dividend_history = original_get_dividend_history


def test_albemarle_tax_classifies_as_qdi_eligible_c_corp():
    analysis = tax_analysis.analyze_tax_and_yield(
        market_data={
            "price": 67.27,
            "dividend_yield": 3.625 / 67.27,
            "dividend_rate": 3.625,
            "name": "Albemarle Corporation",
        },
        prospectus_terms={
            "issuer": "Albemarle Corporation",
            "security_name": "7.25% Series A Mandatory Convertible Preferred Stock",
            "qdi_eligible": None,
            "cumulative": True,
            "coupon_type": "fixed",
        },
        dividend_data={},
    )
    assert analysis["issuer_type"] == "c_corp"
    assert analysis["qdi_eligible"] is True


if __name__ == "__main__":
    test_alb_pa_registry_entry_exists()
    test_alb_pa_market_data_derives_dividend_fields_from_demo_cache()
    test_alb_pa_dividend_fallback_uses_cached_terms()
    test_albemarle_tax_classifies_as_qdi_eligible_c_corp()
    print("ALB-PA regression tests passed.")
