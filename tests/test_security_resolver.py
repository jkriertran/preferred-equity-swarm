"""
Targeted regression tests for security resolver normalization and validation.
Run with: python3 tests/test_security_resolver.py
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.data import security_resolver as sr


def test_normalize_ticker_variants():
    """Normalize common preferred ticker formats to canonical ISSUER-PX."""
    cases = {
        "C.PN": "C-PN",
        "C.PRN": "C-PN",
        "C PRN": "C-PN",
        "C PR N": "C-PN",
        "CpN": "C-PN",
        "ADC-P-A": "ADC-PA",
        "ADC P A": "ADC-PA",
        "HT-P-C": "HT-PC",
        "BAC+PL": "BAC-PL",
        "BAC PL": "BAC-PL",
    }

    for raw, expected in cases.items():
        result = sr.normalize_ticker(raw)
        assert result == expected, f"{raw} -> {result}, expected {expected}"


def test_reject_non_preferred_alpha_match():
    """Reject live matches that look like common stock instead of preferreds."""
    original_lookup = sr._try_live_lookup
    try:
        sr._try_live_lookup = lambda ticker: {
            "longName": "Citigroup Inc.",
            "price": 68.0,
            "exchange": "NYSE",
        }
        result = sr.validate_ticker_for_analysis("C")
        assert result["valid"] is False
        assert "does not appear to be a preferred stock" in result["reason"].lower()
    finally:
        sr._try_live_lookup = original_lookup


def test_allow_preferred_like_alpha_match_with_price_warning():
    """Allow preferred-like Alpha matches even when price is above depositary range."""
    original_lookup = sr._try_live_lookup
    try:
        sr._try_live_lookup = lambda ticker: {
            "longName": "Example Capital Trust Preferred Securities",
            "price": 1215.0,
            "exchange": "NYSE",
        }
        result = sr.validate_ticker_for_analysis("XYZ-PA")
        assert result["valid"] is True
        assert result["resolution"]["trusted_for_analysis"] is True
        assert "outside the typical preferred stock range" in result["reason"].lower()
    finally:
        sr._try_live_lookup = original_lookup


if __name__ == "__main__":
    test_normalize_ticker_variants()
    test_reject_non_preferred_alpha_match()
    test_allow_preferred_like_alpha_match_with_price_warning()
    print("Security resolver regression tests passed.")
