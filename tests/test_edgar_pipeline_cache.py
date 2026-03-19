"""
Regression tests for EDGAR resolution caching and download cooldown behavior.
Run with: python3 tests/test_edgar_pipeline_cache.py
"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.data import edgar_pipeline


def test_resolve_preferred_filing_uses_cached_resolution_before_live_search():
    original_cache_dir = edgar_pipeline.CACHE_DIR
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            edgar_pipeline.CACHE_DIR = tmpdir
            filings = [
                {
                    "accession_number": "0000000000-00-000001",
                    "url": "https://www.sec.gov/Archives/example.htm",
                    "issuer_name": "Example Corp",
                }
            ]
            resolution = {
                "requested_ticker": "TEST-PA",
                "requested_series": "Series A",
                "source": "full_text_search",
                "selected_filing": filings[0],
                "series_match": True,
            }
            edgar_pipeline._save_resolution_cache("TEST-PA", filings, resolution)

            pipeline = edgar_pipeline.EdgarPipeline(cache_enabled=True)
            pipeline.search_preferred_prospectuses = (
                lambda *args, **kwargs: (_ for _ in ()).throw(
                    AssertionError("live search should not run when resolution is cached")
                )
            )
            pipeline.get_issuer_filings = (
                lambda *args, **kwargs: (_ for _ in ()).throw(
                    AssertionError("submissions fallback should not run when resolution is cached")
                )
            )

            cached_filings, cached_resolution = edgar_pipeline.resolve_preferred_filing(
                "TEST-PA",
                pipeline=pipeline,
            )
            assert cached_filings == filings
            assert cached_resolution == resolution
        return
    finally:
        edgar_pipeline.CACHE_DIR = original_cache_dir


def test_resolve_preferred_filing_caches_unresolved_result():
    original_cache_dir = edgar_pipeline.CACHE_DIR
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            edgar_pipeline.CACHE_DIR = tmpdir
            pipeline = edgar_pipeline.EdgarPipeline(cache_enabled=True)
            pipeline.search_preferred_prospectuses = lambda *args, **kwargs: []
            pipeline.get_issuer_filings = lambda *args, **kwargs: []

            filings, resolution = edgar_pipeline.resolve_preferred_filing(
                "TEST-PB",
                pipeline=pipeline,
            )
            cached = edgar_pipeline._load_resolution_cache("TEST-PB")

            assert filings == []
            assert resolution["source"] == "none"
            assert resolution["selected_filing"] == {}
            assert cached is not None
            assert cached["status"] == "unresolved"
            assert cached["resolution"]["requested_ticker"] == "TEST-PB"
        return
    finally:
        edgar_pipeline.CACHE_DIR = original_cache_dir


def test_download_filing_skips_recent_failed_download():
    original_cache_dir = edgar_pipeline.CACHE_DIR
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            edgar_pipeline.CACHE_DIR = tmpdir
            pipeline = edgar_pipeline.EdgarPipeline(cache_enabled=True)

            filing = {
                "accession_number": "0000000000-00-000001",
                "url": "https://www.sec.gov/Archives/example.htm",
            }
            accession = filing["accession_number"].replace("-", "")
            failure_cache_path = os.path.join(tmpdir, f"filing_{accession}.failed.json")
            edgar_pipeline._save_json_file(
                failure_cache_path,
                {
                    "accession_number": accession,
                    "url": filing["url"],
                    "failed_at": edgar_pipeline.time.time(),
                    "error": "simulated failure",
                },
            )

            calls = {"count": 0}

            def fail_if_called(*args, **kwargs):
                calls["count"] += 1
                return None

            pipeline._request = fail_if_called

            text = pipeline.download_filing(filing)
            assert text == ""
            assert calls["count"] == 0
        return
    finally:
        edgar_pipeline.CACHE_DIR = original_cache_dir


def test_download_filing_caches_failure_after_retries():
    original_cache_dir = edgar_pipeline.CACHE_DIR
    original_sleep = edgar_pipeline.time.sleep
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            edgar_pipeline.CACHE_DIR = tmpdir
            edgar_pipeline.time.sleep = lambda seconds: None
            pipeline = edgar_pipeline.EdgarPipeline(cache_enabled=True)

            filing = {
                "accession_number": "0000000000-00-000002",
                "url": "https://www.sec.gov/Archives/example2.htm",
            }
            accession = filing["accession_number"].replace("-", "")
            failure_cache_path = os.path.join(tmpdir, f"filing_{accession}.failed.json")

            pipeline._request = lambda *args, **kwargs: None

            text = pipeline.download_filing(filing, retries=2)
            failure_meta = edgar_pipeline._load_json_file(failure_cache_path)

            assert text == ""
            assert failure_meta is not None
            assert failure_meta["accession_number"] == accession
            assert failure_meta["error"] == "request failed"
        return
    finally:
        edgar_pipeline.CACHE_DIR = original_cache_dir
        edgar_pipeline.time.sleep = original_sleep


if __name__ == "__main__":
    test_resolve_preferred_filing_uses_cached_resolution_before_live_search()
    test_resolve_preferred_filing_caches_unresolved_result()
    test_download_filing_skips_recent_failed_download()
    test_download_filing_caches_failure_after_retries()
    print("EDGAR cache and cooldown regression tests passed.")
