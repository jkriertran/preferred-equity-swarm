"""
Quick validation tests for the benchmark resolution and SOFR fallback logic.
Run with: python3 tests/test_benchmark_resolution.py
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.data.rate_sensitivity import _resolve_benchmark_context, _extract_tenor_label, _all_in_floating_coupon_pct
from src.data.rate_data import get_sofr_rate, get_treasury_yields_from_yfinance


def test_tenor_extraction():
    """Verify tenor labels are extracted correctly from benchmark strings."""
    cases = [
        ("3-month libor", "3M"),
        ("1 month sofr", "1M"),
        ("6m libor", "6M"),
        ("libor", "3M"),  # default
        ("3-month sofr", "3M"),
        ("sofr", "3M"),  # default
    ]
    print("=== Tenor Extraction Tests ===")
    for input_str, expected in cases:
        result = _extract_tenor_label(input_str)
        status = "PASS" if result == expected else "FAIL"
        print(f"  [{status}] '{input_str}' -> '{result}' (expected '{expected}')")
    print()


def test_sofr_fallback():
    """Verify that SOFR can be fetched even without a FRED API key."""
    print("=== SOFR Fallback Test ===")
    rate = get_sofr_rate()
    if rate is not None:
        print(f"  [PASS] SOFR rate fetched: {rate}%")
    else:
        print(f"  [FAIL] SOFR rate is None (no FRED key and yfinance fallback failed)")
    print()


def test_treasury_yields():
    """Verify that the yield curve includes a 3M point."""
    print("=== Treasury Yield Curve Test ===")
    yields = get_treasury_yields_from_yfinance()
    print(f"  Yield points returned: {list(yields.keys())}")
    if "3M" in yields:
        print(f"  [PASS] 3M Treasury yield: {yields['3M']}%")
    else:
        print(f"  [FAIL] 3M Treasury yield missing from curve")
    if "1M" in yields:
        print(f"  [PASS] 1M Treasury yield: {yields['1M']}%")
    else:
        print(f"  [WARN] 1M Treasury yield missing")
    print()


def test_benchmark_resolution_libor():
    """Test LIBOR benchmark resolution with mock rate data."""
    print("=== LIBOR Benchmark Resolution Test ===")
    mock_rate_data = {"1M": 5.25, "3M": 5.30, "10Y": 4.50}

    result = _resolve_benchmark_context("3-month LIBOR", mock_rate_data)
    print(f"  Contractual: {result['contractual_benchmark']}")
    print(f"  Live label:  {result['live_benchmark_label']}")
    print(f"  Method:      {result['benchmark_replacement_method']}")
    print(f"  Rate:        {result['benchmark_rate_pct']}")
    print(f"  Is estimate: {result['is_benchmark_replacement_estimate']}")
    print(f"  Note:        {result['benchmark_note']}")

    if result["benchmark_rate_pct"] is not None:
        print(f"  [PASS] Got a live rate for LIBOR replacement")
    else:
        print(f"  [FAIL] No live rate for LIBOR replacement")
    print()


def test_all_in_coupon():
    """Test the all-in floating coupon calculation."""
    print("=== All-In Floating Coupon Test ===")

    # MS-PA: 3-month LIBOR + 70 bps
    # 70 bps = 0.70 percentage points, stored as 70.0
    # If SOFR is ~5.30%, all-in should be ~6.00%
    result = _all_in_floating_coupon_pct(5.30, 70.0)
    print(f"  SOFR 5.30% + 70 bps spread -> {result}%")
    if result is not None and 5.9 < result < 6.1:
        print(f"  [PASS] All-in coupon is in expected range")
    else:
        print(f"  [FAIL] All-in coupon is unexpected: {result}")

    # C-PJ: 3-month LIBOR + 442 bps
    result2 = _all_in_floating_coupon_pct(5.30, 442.0)
    print(f"  SOFR 5.30% + 442 bps spread -> {result2}%")
    if result2 is not None and 9.5 < result2 < 10.0:
        print(f"  [PASS] All-in coupon is in expected range")
    else:
        print(f"  [FAIL] All-in coupon is unexpected: {result2}")

    # GS-PD: 3-month LIBOR + 67 bps
    result3 = _all_in_floating_coupon_pct(5.30, 67.0)
    print(f"  SOFR 5.30% + 67 bps spread -> {result3}%")
    if result3 is not None and 5.9 < result3 < 6.0:
        print(f"  [PASS] All-in coupon is in expected range")
    else:
        print(f"  [FAIL] All-in coupon is unexpected: {result3}")
    print()


def test_no_benchmark():
    """Test that a security with no floating benchmark returns clean nulls."""
    print("=== No Benchmark Test ===")
    result = _resolve_benchmark_context(None, {"10Y": 4.50})
    all_none = all(
        result[k] is None
        for k in ("contractual_benchmark", "live_benchmark_label", "benchmark_rate_pct")
    )
    if all_none:
        print(f"  [PASS] All benchmark fields are None for fixed-rate security")
    else:
        print(f"  [FAIL] Unexpected values: {result}")
    print()


if __name__ == "__main__":
    test_tenor_extraction()
    test_all_in_coupon()
    test_no_benchmark()
    test_sofr_fallback()
    test_treasury_yields()
    test_benchmark_resolution_libor()
    print("=== All tests complete ===")
