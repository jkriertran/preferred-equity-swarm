"""
Targeted regression tests for the Alpha Vantage adapter path.
Run with: python3 tests/test_alpha_vantage_adapter.py
"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.data import alpha_vantage
from src.data import market_data
from src.data import rate_sensitivity
from src.data import security_context
from src.data import security_resolver


def test_symbol_candidates_include_common_preferred_variants():
    original_override = alpha_vantage._provider_symbol_override
    try:
        alpha_vantage._provider_symbol_override = lambda ticker: "C.PRN"
        candidates = alpha_vantage.get_symbol_candidates("C-PN")
        assert candidates[0] == "C.PRN"
        assert "C-P-N" in candidates
        assert "C.PR.N" in candidates
        assert "C.PN" in candidates
    finally:
        alpha_vantage._provider_symbol_override = original_override


def test_lookup_reference_prefers_provider_override():
    original_override = alpha_vantage._provider_symbol_override
    original_listing_lookup = alpha_vantage._lookup_listing_status_row
    original_cache = dict(alpha_vantage._reference_cache)
    try:
        alpha_vantage._reference_cache.clear()
        alpha_vantage._provider_symbol_override = lambda ticker: "ALL.PH"
        alpha_vantage._lookup_listing_status_row = lambda ticker: None
        result = alpha_vantage.lookup_reference_symbol("ALL-PH", require_preferred=True)
        assert result["symbol"] == "ALL.PH"
        assert result["source"] == "provider_override"
    finally:
        alpha_vantage._provider_symbol_override = original_override
        alpha_vantage._lookup_listing_status_row = original_listing_lookup
        alpha_vantage._reference_cache.clear()
        alpha_vantage._reference_cache.update(original_cache)


def test_listing_status_row_resolves_official_alpha_symbol():
    original_env = os.environ.get("ALPHA_VANTAGE_LISTING_STATUS_PATH")
    original_listing_cache = alpha_vantage._listing_status_cache
    original_ref_cache = dict(alpha_vantage._reference_cache)
    try:
        with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False) as tmp:
            tmp.write("symbol,name,exchange,assetType,ipoDate,delistingDate,status\n")
            tmp.write("ALL-P-H,Allstate Corp (The),NYSE,Stock,2019-08-02,null,Active\n")
            path = tmp.name

        os.environ["ALPHA_VANTAGE_LISTING_STATUS_PATH"] = path
        alpha_vantage._listing_status_cache = None
        alpha_vantage._reference_cache.clear()

        row = alpha_vantage._lookup_listing_status_row("ALL-PH")
        assert row is not None
        assert row["symbol"] == "ALL-P-H"

        reference = alpha_vantage.lookup_reference_symbol("ALL-PH", require_preferred=True)
        assert reference is not None
        assert reference["symbol"] == "ALL-P-H"
        assert reference["source"] == "listing_status"
    finally:
        if original_env is None:
            os.environ.pop("ALPHA_VANTAGE_LISTING_STATUS_PATH", None)
        else:
            os.environ["ALPHA_VANTAGE_LISTING_STATUS_PATH"] = original_env
        alpha_vantage._listing_status_cache = original_listing_cache
        alpha_vantage._reference_cache.clear()
        alpha_vantage._reference_cache.update(original_ref_cache)
        if 'path' in locals() and os.path.exists(path):
            os.unlink(path)


def test_search_terms_include_issuer_and_series_context():
    terms = alpha_vantage._candidate_search_terms("ALL-PH")
    assert "The Allstate Corporation preferred" in terms
    assert "The Allstate Corporation preferred series H" in terms


def test_security_context_prefers_cached_terms_over_universe_and_keeps_snapshot_separate():
    original_loader = security_context.load_cached_terms_for_ticker
    original_universe_cache = security_context._universe_cache
    original_snapshot_cache = security_context._snapshot_cache
    try:
        security_context._universe_cache = {
            "TEST-PA": {
                "security_name": "Universe Name",
                "issuer": "Universe Issuer",
                "parent_ticker": "TEST",
                "coupon_rate": 4.5,
                "par_value": 25.0,
                "provider_symbols": {"alpha_vantage": "UNIVERSE-P-A"},
            }
        }
        security_context._snapshot_cache = {
            "TEST-PA": {
                "name": "Snapshot Name",
                "price": 22.10,
                "dividend_rate": 1.20,
            }
        }
        security_context.load_cached_terms_for_ticker = lambda ticker: {
            "ticker": "TEST-PA",
            "security_name": "Cached Name",
            "coupon_rate": 6.25,
            "provider_symbols": {"alpha_vantage": "CACHED-P-A"},
        }

        context = security_context.get_security_context("TEST-PA", include_snapshot=True)
        assert context["security_name"] == "Cached Name"
        assert context["coupon_rate"] == 6.25
        assert context["provider_symbols"]["alpha_vantage"] == "CACHED-P-A"
        assert context["snapshot_entry"]["name"] == "Snapshot Name"
        assert context["merged_entry"].get("price") is None
    finally:
        security_context.load_cached_terms_for_ticker = original_loader
        security_context._universe_cache = original_universe_cache
        security_context._snapshot_cache = original_snapshot_cache


def test_resolve_alpha_symbol_prefers_explicit_provider_override():
    original_context = alpha_vantage.get_security_context
    original_listing_lookup = alpha_vantage._lookup_listing_status_row
    try:
        alpha_vantage.get_security_context = lambda ticker, include_snapshot=False: {
            "ticker": "ALL-PH",
            "security_name": "Allstate Series H Preferred",
            "issuer": "The Allstate Corporation",
            "provider_symbols": {"alpha_vantage": "ALL.EXPLICIT"},
            "merged_entry": {
                "security_name": "Allstate Series H Preferred",
                "issuer": "The Allstate Corporation",
                "provider_symbols": {"alpha_vantage": "ALL.EXPLICIT"},
            },
        }
        alpha_vantage._lookup_listing_status_row = lambda ticker: {
            "symbol": "ALL-P-H",
            "name": "Allstate Corp (The)",
            "exchange": "NYSE",
        }

        resolution = alpha_vantage.resolve_alpha_symbol("ALL-PH", require_preferred=True)
        assert resolution is not None
        assert resolution["symbol"] == "ALL.EXPLICIT"
        assert resolution["source"] == "provider_override"
        assert resolution["candidates"][0] == "ALL.EXPLICIT"
    finally:
        alpha_vantage.get_security_context = original_context
        alpha_vantage._lookup_listing_status_row = original_listing_lookup


def test_resolve_alpha_symbol_prefers_listing_status_over_search():
    original_context = alpha_vantage.get_security_context
    original_listing_lookup = alpha_vantage._lookup_listing_status_row
    original_search = alpha_vantage._search_reference_symbol
    try:
        alpha_vantage.get_security_context = lambda ticker, include_snapshot=False: {
            "ticker": "ALL-PH",
            "security_name": "Allstate Series H Preferred",
            "issuer": "The Allstate Corporation",
            "provider_symbols": {},
            "merged_entry": {
                "security_name": "Allstate Series H Preferred",
                "issuer": "The Allstate Corporation",
            },
        }
        alpha_vantage._lookup_listing_status_row = lambda ticker: {
            "symbol": "ALL-P-H",
            "name": "Allstate Corp (The)",
            "exchange": "NYSE",
        }
        alpha_vantage._search_reference_symbol = (
            lambda *args, **kwargs: (_ for _ in ()).throw(
                AssertionError("listing-status resolution should short-circuit search")
            )
        )

        resolution = alpha_vantage.resolve_alpha_symbol("ALL-PH", require_preferred=True)
        assert resolution is not None
        assert resolution["symbol"] == "ALL-P-H"
        assert resolution["source"] == "listing_status"
        assert resolution["reference"]["source"] == "listing_status"
    finally:
        alpha_vantage.get_security_context = original_context
        alpha_vantage._lookup_listing_status_row = original_listing_lookup
        alpha_vantage._search_reference_symbol = original_search


def test_market_data_derives_dividend_fields_from_terms():
    original_quote = market_data.get_alpha_vantage_quote
    original_snapshot = market_data._get_snapshot_data
    try:
        market_data.get_alpha_vantage_quote = lambda ticker, require_preferred=True: {
            "name": "JPMorgan Chase & Co.",
            "close": "24.71",
            "currency": "USD",
            "_matched_symbol": "JPM-PD",
        }
        market_data._get_snapshot_data = lambda ticker: None

        info = market_data.get_preferred_info("JPM-PD")
        assert info["provider"] == "alpha_vantage"
        assert info["provider_symbol"] == "JPM-PD"
        assert round(info["price"], 2) == 24.71
        assert round(info["dividend_rate"], 4) == 1.4375
        assert info["dividend_yield"] is not None
    finally:
        market_data.get_alpha_vantage_quote = original_quote
        market_data._get_snapshot_data = original_snapshot


def test_market_data_falls_back_to_time_series_when_quote_missing():
    original_quote = market_data.get_alpha_vantage_quote
    original_time_series = market_data.get_alpha_vantage_time_series
    original_snapshot = market_data._get_snapshot_data
    try:
        market_data.get_alpha_vantage_quote = lambda ticker, require_preferred=True: None

        import pandas as pd

        history = pd.DataFrame(
            [
                {"Close": 24.88, "Volume": 9800},
                {"Close": 25.00, "Volume": 12345},
            ],
            index=pd.to_datetime(["2026-03-06", "2026-03-13"]),
        )
        history.attrs["provider_symbol"] = "ALL-P-H"
        market_data.get_alpha_vantage_time_series = (
            lambda ticker, period="1mo", require_preferred=True: history
        )
        market_data._get_snapshot_data = lambda ticker: None

        info = market_data.get_preferred_info("ALL-PH")
        assert info["provider"] == "alpha_vantage"
        assert info["provider_symbol"] == "ALL-P-H"
        assert info["price_source"] == "time_series_close"
        assert round(info["price"], 2) == 25.00
    finally:
        market_data.get_alpha_vantage_quote = original_quote
        market_data.get_alpha_vantage_time_series = original_time_series
        market_data._get_snapshot_data = original_snapshot


def test_alpha_dividend_history_normalizes_payload():
    original_request = alpha_vantage._request_json
    original_lookup = alpha_vantage.lookup_reference_symbol
    original_dividend_cache = dict(alpha_vantage._dividend_cache)
    try:
        alpha_vantage._dividend_cache.clear()
        alpha_vantage.lookup_reference_symbol = lambda ticker, require_preferred=True: {
            "symbol": "ALL-PH",
            "name": "Allstate Series H Preferred",
            "type": "preferred stock",
            "region": "United States",
        }
        alpha_vantage._request_json = lambda **params: {
            "data": [
                {"payment_date": "2025-01-15", "amount": "0.31875"},
                {"payment_date": "2025-04-15", "amount": "0.31875"},
                {"payment_date": "2025-07-15", "amount": "0.31875"},
                {"payment_date": "2025-10-15", "amount": "0.31875"},
            ]
            if params.get("function") == "DIVIDENDS"
            else {"status": "error", "message": "unexpected"}
        }

        df = alpha_vantage.get_dividends("ALL-PH", require_preferred=True)
        assert df is not None
        assert len(df) == 4
        assert round(float(df["dividend"].sum()), 4) == 1.275
    finally:
        alpha_vantage._request_json = original_request
        alpha_vantage.lookup_reference_symbol = original_lookup
        alpha_vantage._dividend_cache.clear()
        alpha_vantage._dividend_cache.update(original_dividend_cache)


def test_alpha_time_series_normalizes_weekly_payload():
    original_request = alpha_vantage._request_json
    original_lookup = alpha_vantage.lookup_reference_symbol
    try:
        alpha_vantage.lookup_reference_symbol = lambda ticker, require_preferred=True: {
            "symbol": "ALL-PH",
            "name": "Allstate Series H Preferred",
            "type": "preferred stock",
            "region": "United States",
        }
        alpha_vantage._request_json = lambda **params: {
            "Weekly Time Series": {
                "2026-03-13": {
                    "1. open": "24.90",
                    "2. high": "25.10",
                    "3. low": "24.75",
                    "4. close": "25.00",
                    "5. volume": "12345",
                },
                "2026-03-06": {
                    "1. open": "24.70",
                    "2. high": "24.95",
                    "3. low": "24.60",
                    "4. close": "24.88",
                    "5. volume": "9800",
                },
            }
        }

        df = alpha_vantage.get_time_series("ALL-PH", period="1y", require_preferred=True)
        assert df is not None
        assert list(df.columns) == ["Open", "High", "Low", "Close", "Volume"]
        assert round(float(df["Close"].iloc[-1]), 2) == 25.00
    finally:
        alpha_vantage._request_json = original_request
        alpha_vantage.lookup_reference_symbol = original_lookup


def test_alpha_fetchers_share_resolved_symbol():
    original_resolve = alpha_vantage.resolve_alpha_symbol
    original_request = alpha_vantage._request_json
    original_quote_cache = dict(alpha_vantage._quote_cache)
    original_dividend_cache = dict(alpha_vantage._dividend_cache)
    try:
        alpha_vantage._quote_cache.clear()
        alpha_vantage._dividend_cache.clear()
        alpha_vantage.resolve_alpha_symbol = lambda ticker, require_preferred=False: {
            "symbol": "ALL-P-H",
            "candidates": ["ALL-P-H"],
            "reference": {
                "symbol": "ALL-P-H",
                "name": "Allstate Corp (The)",
                "type": "preferred stock",
                "region": "United States",
            },
            "source": "listing_status",
            "metadata": {"security_name": "Allstate Series H Preferred"},
            "require_preferred": require_preferred,
        }

        seen_symbols = []

        def fake_request(**params):
            seen_symbols.append(params.get("symbol"))
            function = params.get("function")
            if function == "GLOBAL_QUOTE":
                return {
                    "Global Quote": {
                        "01. symbol": "ALL-P-H",
                        "05. price": "25.00",
                        "06. volume": "12345",
                    }
                }
            if function == "TIME_SERIES_WEEKLY":
                return {
                    "Weekly Time Series": {
                        "2026-03-13": {
                            "1. open": "24.90",
                            "2. high": "25.10",
                            "3. low": "24.75",
                            "4. close": "25.00",
                            "5. volume": "12345",
                        }
                    }
                }
            if function == "DIVIDENDS":
                return {
                    "data": [
                        {"payment_date": "2025-01-15", "amount": "0.31875"},
                    ]
                }
            return {"status": "error", "message": "unexpected"}

        alpha_vantage._request_json = fake_request

        quote = alpha_vantage.get_quote("ALL-PH", require_preferred=True)
        history = alpha_vantage.get_time_series("ALL-PH", period="1y", require_preferred=True)
        dividends = alpha_vantage.get_dividends("ALL-PH", require_preferred=True)

        assert quote is not None
        assert history is not None
        assert dividends is not None
        assert set(seen_symbols) == {"ALL-P-H"}
    finally:
        alpha_vantage.resolve_alpha_symbol = original_resolve
        alpha_vantage._request_json = original_request
        alpha_vantage._quote_cache.clear()
        alpha_vantage._quote_cache.update(original_quote_cache)
        alpha_vantage._dividend_cache.clear()
        alpha_vantage._dividend_cache.update(original_dividend_cache)


def test_market_data_alias_points_to_preferred_info():
    original_get_preferred_info = market_data.get_preferred_info
    try:
        market_data.get_preferred_info = lambda ticker: {"ticker": ticker, "provider": "stub"}
        info = market_data.get_market_data("C-PN")
        assert info == {"ticker": "C-PN", "provider": "stub"}
    finally:
        market_data.get_preferred_info = original_get_preferred_info


def test_market_data_uses_live_benchmark_for_reset_fixed_to_floating():
    original_snapshot = market_data._get_snapshot_data
    original_sofr = market_data.get_sofr_rate
    try:
        market_data._get_snapshot_data = lambda ticker: None
        market_data.get_sofr_rate = lambda: 4.59

        info = market_data._derive_dividend_fields("C-PN", price=29.65)
        assert info["coupon_type"] == "fixed-to-floating"
        assert info["dividend_source"] == "live_benchmark"
        assert round(info["effective_coupon_pct"], 4) == 10.96
        assert round(info["dividend_rate"], 4) == 2.74
        assert round(info["dividend_yield"], 6) == round(2.74 / 29.65, 6)
    finally:
        market_data._get_snapshot_data = original_snapshot
        market_data.get_sofr_rate = original_sofr


def test_market_data_falls_back_to_snapshot_when_live_data_missing():
    original_live = market_data._get_preferred_info_from_alpha_vantage
    original_snapshot = market_data._get_snapshot_data
    try:
        market_data._get_preferred_info_from_alpha_vantage = lambda ticker: {
            "ticker": ticker,
            "error": "Alpha Vantage market data unavailable: synthetic failure",
        }
        market_data._get_snapshot_data = lambda ticker: {
            "ticker": ticker,
            "provider": "snapshot",
            "price": 21.5,
            "dividend_rate": 1.275,
            "dividend_yield": 1.275 / 21.5,
            "currency": "USD",
            "is_snapshot": True,
        }

        info = market_data.get_preferred_info("ALL-PH")
        assert info["provider"] == "snapshot"
        assert info["price"] == 21.5
        assert info["is_snapshot"] is True
    finally:
        market_data._get_preferred_info_from_alpha_vantage = original_live
        market_data._get_snapshot_data = original_snapshot


def test_rate_sensitivity_reads_floating_spread_bps_schema():
    original_sofr = rate_sensitivity.get_sofr_rate
    try:
        rate_sensitivity.get_sofr_rate = lambda: 4.59
        analysis = rate_sensitivity.analyze_interest_rate_sensitivity(
            market_data={"ticker": "C-PN", "price": 29.65, "dividend_yield": 2.74 / 29.65},
            rate_data={"3M": 4.55, "10Y": 4.2},
            prospectus_terms={
                "ticker": "C-PN",
                "coupon_type": "fixed-to-floating",
                "coupon_rate": 7.875,
                "par_value": 25.0,
                "fixed_to_floating_date": "2015-10-30",
                "floating_benchmark": "3-month LIBOR",
                "floating_spread_bps": 637.0,
                "dividend_frequency": "quarterly",
                "call_date": "2015-10-30",
            },
        )
        assert analysis["floating_spread_bps"] == 637.0
        assert analysis["all_in_floating_coupon_pct"] == 10.96
    finally:
        rate_sensitivity.get_sofr_rate = original_sofr


def test_security_resolver_rejects_non_preferred_alpha_match():
    original_lookup = security_resolver.lookup_reference_symbol
    original_quote = security_resolver.get_alpha_vantage_quote
    try:
        security_resolver.lookup_reference_symbol = lambda ticker, require_preferred=False: {
            "symbol": "C",
            "name": "Citigroup Inc.",
            "type": "Common Stock",
            "region": "United States",
        }
        security_resolver.get_alpha_vantage_quote = lambda ticker, require_preferred=False: {
            "name": "Citigroup Inc.",
            "close": "68.00",
            "_matched_symbol": "C",
        }

        result = security_resolver.validate_ticker_for_analysis("C")
        assert result["valid"] is False
        assert "does not appear to be a preferred stock" in result["reason"].lower()
    finally:
        security_resolver.lookup_reference_symbol = original_lookup
        security_resolver.get_alpha_vantage_quote = original_quote


if __name__ == "__main__":
    test_symbol_candidates_include_common_preferred_variants()
    test_lookup_reference_prefers_provider_override()
    test_listing_status_row_resolves_official_alpha_symbol()
    test_search_terms_include_issuer_and_series_context()
    test_security_context_prefers_cached_terms_over_universe_and_keeps_snapshot_separate()
    test_resolve_alpha_symbol_prefers_explicit_provider_override()
    test_resolve_alpha_symbol_prefers_listing_status_over_search()
    test_market_data_derives_dividend_fields_from_terms()
    test_market_data_falls_back_to_time_series_when_quote_missing()
    test_alpha_dividend_history_normalizes_payload()
    test_alpha_time_series_normalizes_weekly_payload()
    test_alpha_fetchers_share_resolved_symbol()
    test_market_data_alias_points_to_preferred_info()
    test_market_data_uses_live_benchmark_for_reset_fixed_to_floating()
    test_market_data_falls_back_to_snapshot_when_live_data_missing()
    test_rate_sensitivity_reads_floating_spread_bps_schema()
    test_security_resolver_rejects_non_preferred_alpha_match()
    print("Alpha Vantage adapter regression tests passed.")
