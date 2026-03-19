"""
SEC EDGAR Data Pipeline for Preferred Stock Prospectuses.

This module provides two complementary approaches to finding preferred
stock prospectus filings on SEC EDGAR:

1. **Full-Text Search (Primary):** Uses the EFTS search API to find filings
   that contain "preferred stock" and "depositary shares" across all issuers.
   This is the fastest way to discover preferred stock prospectuses.

2. **Submissions API (Secondary):** Uses the company submissions endpoint
   to get a specific issuer's filing history and filter by form type.
   Useful when you already know the issuer.

All endpoints are free and require no API key. The only requirement is a
User-Agent header per SEC policy.

Usage:
    from src.data.edgar_pipeline import EdgarPipeline

    pipeline = EdgarPipeline()

    # Discover preferred prospectuses via full-text search
    filings = pipeline.search_preferred_prospectuses(issuer="JPMorgan Chase")

    # Or get filings for a specific issuer via submissions API
    filings = pipeline.get_issuer_filings("JPM")

    # Download the text of a filing
    text = pipeline.download_filing(filings[0])
"""

import json
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

USER_AGENT = os.getenv(
    "SEC_USER_AGENT",
    "PreferredEquitySwarm research@example.com"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept-Encoding": "gzip, deflate",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# SEC rate limit: 10 requests per second. We stay well under that.
REQUEST_DELAY = 0.35
REQUEST_RETRIES = 3
RESOLVED_CACHE_TTL_SECONDS = 30 * 24 * 60 * 60
UNRESOLVED_CACHE_TTL_SECONDS = 30 * 60
FAILED_DOWNLOAD_TTL_SECONDS = 15 * 60

# Base URLs
SUBMISSIONS_BASE = "https://data.sec.gov/submissions"
EFTS_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data"

# Prospectus filing types for preferred stock
PREFERRED_FORM_TYPES = {"424B2", "424B5", "424B3", "424B4"}
SHELF_FORM_TYPES = {"S-3", "S-3/A"}

# Data directories
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
CACHE_DIR = os.path.join(DATA_DIR, "edgar_cache")
DEMO_FILING_REGISTRY_PATH = os.path.join(DATA_DIR, "preferred_filing_registry.json")

TICKER_TO_ISSUER_NAME = {
    "ALB": "Albemarle",
    "JPM": "JPMorgan Chase",
    "BAC": "Bank of America",
    "GS": "Goldman Sachs",
    "MS": "Morgan Stanley",
    "WFC": "Wells Fargo",
    "C": "Citigroup",
    "USB": "US Bancorp",
    "PNC": "PNC Financial",
    "TFC": "Truist Financial",
    "COF": "Capital One",
    "MET": "MetLife",
    "PRU": "Prudential Financial",
    "ALL": "Allstate",
    "PSA": "Public Storage",
    "DLR": "Digital Realty",
    "SPG": "Simon Property",
    "DUK": "Duke Energy",
    "SO": "Southern Company",
    "NEE": "NextEra Energy",
    "D": "Dominion Energy",
    "T": "AT&T",
}


class EdgarPipeline:
    """Pipeline for fetching preferred stock prospectus data from SEC EDGAR."""

    def __init__(self, cache_enabled: bool = True):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.cache_enabled = cache_enabled
        self._last_request_at = 0.0
        if cache_enabled:
            os.makedirs(CACHE_DIR, exist_ok=True)

    def _wait_for_request_slot(self) -> None:
        """Throttle outbound SEC requests so bursts stay well below the limit."""
        now = time.monotonic()
        elapsed = now - self._last_request_at
        if elapsed < REQUEST_DELAY:
            time.sleep(REQUEST_DELAY - elapsed)
        self._last_request_at = time.monotonic()

    def _request(
        self,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        timeout: int = 20,
        retries: int = REQUEST_RETRIES,
        context: str = "Request",
    ) -> Optional[requests.Response]:
        """Issue a throttled SEC request with retry/backoff for transient failures."""
        last_error = ""

        for attempt in range(retries):
            self._wait_for_request_slot()
            try:
                resp = self.session.get(url, params=params, timeout=timeout)
                if resp.status_code == 503:
                    last_error = "503 rate limited"
                    print(f"  [EDGAR] {context} hit 503, retrying ({attempt + 1}/{retries})...")
                    retry_after = resp.headers.get("Retry-After")
                    try:
                        backoff = float(retry_after) if retry_after else 0.0
                    except (TypeError, ValueError):
                        backoff = 0.0
                    time.sleep(max(backoff, 1.5 * (attempt + 1)))
                    continue
                resp.raise_for_status()
                return resp
            except requests.exceptions.Timeout:
                last_error = "timeout"
                print(f"  [EDGAR] {context} timed out, retrying ({attempt + 1}/{retries})...")
                time.sleep(1.5 * (attempt + 1))
            except Exception as exc:
                last_error = str(exc)
                if attempt + 1 < retries:
                    print(f"  [EDGAR] {context} error, retrying ({attempt + 1}/{retries})...")
                    time.sleep(1.0 * (attempt + 1))
                else:
                    print(f"  [EDGAR] {context} failed: {exc}")

        return None

    # ==================================================================
    # PRIMARY: Full-Text Search for Preferred Prospectuses
    # ==================================================================

    def search_preferred_prospectuses(
        self,
        issuer: str = "",
        series_hint: str = "",
        date_start: str = "2015-01-01",
        date_end: str = "2026-12-31",
        max_results: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Search EDGAR full-text search for preferred stock prospectus filings.

        This is the primary discovery method. It searches the full text of
        SEC filings for keywords like "preferred stock" and "depositary shares"
        and returns structured metadata about each matching filing.

        Args:
            issuer: Company name to filter by (e.g., "JPMorgan Chase").
                   Leave empty to search across all issuers.
            date_start: Start date (YYYY-MM-DD).
            date_end: End date (YYYY-MM-DD).
            max_results: Maximum number of results to return.

        Returns:
            List of filing dicts with keys: accession_number, filename,
            issuer_name, issuer_cik, tickers, form_type, filing_date, url.
        """
        # Build the search query
        query_parts = ['"preferred stock"', '"depositary shares"']
        if issuer:
            query_parts.insert(0, f'"{issuer}"')
        if series_hint:
            query_parts.append(f'"{series_hint}"')

        query = " ".join(query_parts)

        params = {
            "q": query,
            "forms": "424B2,424B5",
            "dateRange": "custom",
            "startdt": date_start,
            "enddt": date_end,
            "from": "0",
            "size": str(min(max_results, 100)),
        }

        resp = self._request(
            EFTS_SEARCH_URL,
            params=params,
            timeout=20,
            context="Full-text search",
        )
        if resp is None:
            return []

        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])
        total = data.get("hits", {}).get("total", {}).get("value", 0)
        print(f"  [EDGAR] Full-text search found {total} total matches, returning {len(hits)}")

        results = []
        for hit in hits[:max_results]:
            src = hit.get("_source", {})
            file_id = hit.get("_id", "")

            # Parse the _id field: "accession:filename"
            parts = file_id.split(":")
            accession = parts[0] if parts else ""
            filename = parts[1] if len(parts) > 1 else ""

            # Extract CIK from the ciks array
            ciks = src.get("ciks", [])
            cik = ciks[0] if ciks else ""

            # Extract display name and tickers
            display_names = src.get("display_names", [])
            display_name = display_names[0] if display_names else ""

            # Parse tickers from display name (format: "COMPANY NAME (TICK1, TICK2)")
            tickers = []
            ticker_match = re.search(r"\(([^)]+)\)", display_name)
            if ticker_match:
                tickers = [t.strip() for t in ticker_match.group(1).split(",")]

            # Build the filing URL
            cik_stripped = cik.lstrip("0")
            acc_nodash = accession.replace("-", "")
            url = f"{ARCHIVES_BASE}/{cik_stripped}/{acc_nodash}/{filename}"

            results.append({
                "accession_number": accession,
                "filename": filename,
                "issuer_name": re.sub(r"\s*\([^)]*\)\s*$", "", display_name).strip(),
                "issuer_cik": cik,
                "tickers": tickers,
                "form_type": src.get("root_forms", [""])[0],
                "filing_date": src.get("file_date", ""),
                "url": url,
                "search_score": hit.get("_score", 0),
            })

        return results

    # ==================================================================
    # SECONDARY: Submissions API for Specific Issuers
    # ==================================================================

    def get_cik(self, ticker: str) -> Optional[str]:
        """
        Look up a company's 10-digit CIK from a ticker symbol.

        Args:
            ticker: Stock ticker (e.g., "JPM", "BAC", "JPM-PD").

        Returns:
            10-digit CIK string with leading zeros, or None if not found.
        """
        parent_ticker = ticker.split("-")[0].split(".")[0].upper()

        cache_path = os.path.join(CACHE_DIR, "company_tickers.json")

        if self.cache_enabled and os.path.exists(cache_path):
            with open(cache_path, "r") as f:
                tickers_data = json.load(f)
        else:
            url = "https://www.sec.gov/files/company_tickers.json"
            resp = self._request(
                url,
                timeout=15,
                context="company_tickers.json fetch",
            )
            if resp is not None:
                tickers_data = resp.json()
            else:
                # Fallback: try data.sec.gov
                url = "https://data.sec.gov/submissions/company_tickers.json"
                resp = self._request(
                    url,
                    timeout=15,
                    context="company_tickers fallback fetch",
                )
                if resp is None:
                    return None
                tickers_data = resp.json()

            if self.cache_enabled:
                with open(cache_path, "w") as f:
                    json.dump(tickers_data, f)

        for entry in tickers_data.values():
            if entry.get("ticker", "").upper() == parent_ticker:
                return str(entry["cik_str"]).zfill(10)

        return None

    def get_issuer_filings(
        self,
        ticker: str,
        form_types: Optional[set] = None,
        max_results: int = 50,
    ) -> List[Dict[str, str]]:
        """
        Get prospectus-type filings for a specific issuer via the
        Submissions API.

        Args:
            ticker: Stock ticker (e.g., "JPM" or "JPM-PD").
            form_types: Set of form types to filter for.
            max_results: Maximum number of filings to return.

        Returns:
            List of filing dicts.
        """
        if form_types is None:
            form_types = PREFERRED_FORM_TYPES | SHELF_FORM_TYPES

        cik = self.get_cik(ticker)
        if not cik:
            print(f"  [EDGAR] Could not find CIK for ticker: {ticker}")
            return []

        # Fetch submissions
        url = f"{SUBMISSIONS_BASE}/CIK{cik}.json"
        cache_path = os.path.join(CACHE_DIR, f"submissions_{cik}.json")

        if self.cache_enabled and os.path.exists(cache_path):
            with open(cache_path, "r") as f:
                submissions = json.load(f)
        else:
            resp = self._request(
                url,
                timeout=15,
                context=f"submissions fetch for {ticker}",
            )
            if resp is None:
                return []
            submissions = resp.json()
            if self.cache_enabled:
                with open(cache_path, "w") as f:
                    json.dump(submissions, f)

        recent = submissions.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accessions = recent.get("accessionNumber", [])
        primary_docs = recent.get("primaryDocument", [])
        descriptions = recent.get("primaryDocDescription", [])

        # Filter for preferred-specific filings
        preferred_keywords = [
            "preferred", "depositary shares", "depositary share",
            "fixed-to-floating", "non-cumulative", "perpetual",
        ]
        reject_keywords = [
            "pricing supplement", "structured note", "auto-callable",
            "barrier", "callable contingent", "linked note",
        ]

        results = []
        for i in range(len(forms)):
            if forms[i] not in form_types:
                continue

            desc = (descriptions[i] if i < len(descriptions) else "").lower()

            # Fast accept if description mentions preferred
            is_preferred = any(kw in desc for kw in preferred_keywords)

            # Fast reject if description is clearly not preferred
            is_rejected = any(kw in desc for kw in reject_keywords)

            # For S-3 shelf registrations, always include
            is_shelf = forms[i] in SHELF_FORM_TYPES

            if is_preferred or (is_shelf and not is_rejected):
                acc_nodash = accessions[i].replace("-", "")
                cik_stripped = cik.lstrip("0")
                doc_url = f"{ARCHIVES_BASE}/{cik_stripped}/{acc_nodash}/{primary_docs[i]}"

                results.append({
                    "accession_number": accessions[i],
                    "filename": primary_docs[i],
                    "issuer_name": submissions.get("name", ""),
                    "issuer_cik": cik,
                    "tickers": [ticker.split("-")[0].upper()],
                    "form_type": forms[i],
                    "filing_date": dates[i],
                    "description": descriptions[i] if i < len(descriptions) else "",
                    "url": doc_url,
                })

                if len(results) >= max_results:
                    break

        print(f"  [EDGAR] Found {len(results)} preferred-related filings for {ticker}")
        return results

    # ==================================================================
    # Download Filing Text
    # ==================================================================

    def download_filing(
        self,
        filing: Dict[str, Any],
        max_chars: int = 50000,
        retries: int = 3,
    ) -> str:
        """
        Download and extract the plain text content of a filing.

        Handles HTML and plain text filings. Includes retry logic for
        SEC rate limiting (503 errors).

        Args:
            filing: A filing dict from search or issuer methods.
            max_chars: Maximum characters to return.
            retries: Number of retry attempts for failed downloads.

        Returns:
            Plain text content of the filing, truncated to max_chars.
        """
        accession = filing.get("accession_number", "").replace("-", "")
        cache_path = os.path.join(CACHE_DIR, f"filing_{accession}.txt")
        failure_cache_path = os.path.join(CACHE_DIR, f"filing_{accession}.failed.json")

        # Check cache
        if self.cache_enabled and os.path.exists(cache_path):
            with open(cache_path, "r", encoding="utf-8", errors="replace") as f:
                text = f.read()
            return text[:max_chars]

        if self.cache_enabled and os.path.exists(failure_cache_path):
            failure_meta = _load_json_file(failure_cache_path)
            failed_at = float((failure_meta or {}).get("failed_at", 0.0) or 0.0)
            if failed_at and (time.time() - failed_at) < FAILED_DOWNLOAD_TTL_SECONDS:
                print(f"  [EDGAR] Skipping download for {accession}; recent failure cooldown is still active.")
                return ""

        url = filing["url"]
        last_error = ""

        for attempt in range(retries):
            resp = self._request(
                url,
                timeout=30,
                retries=1,
                context=f"filing download {accession}",
            )
            if resp is None:
                last_error = "request failed"
                time.sleep(1.5 * (attempt + 1))
                continue

            try:
                content_type = resp.headers.get("Content-Type", "")
                raw = resp.text

                if "html" in content_type or raw.strip().startswith("<"):
                    text = self._html_to_text(raw)
                else:
                    text = raw

                # Clean up whitespace
                text = re.sub(r"\n{3,}", "\n\n", text)
                text = re.sub(r" {2,}", " ", text)
                text = text.strip()

                # Cache
                if self.cache_enabled:
                    with open(cache_path, "w", encoding="utf-8") as f:
                        f.write(text)
                    if os.path.exists(failure_cache_path):
                        os.remove(failure_cache_path)

                return text[:max_chars]

            except Exception as e:
                last_error = str(e)
                print(f"  [EDGAR] Error downloading {url}: {e}")
                time.sleep(1.0 * (attempt + 1))

        if self.cache_enabled:
            _save_json_file(
                failure_cache_path,
                {
                    "accession_number": accession,
                    "url": url,
                    "failed_at": time.time(),
                    "error": last_error or "download failed",
                },
            )

        return ""

    # ==================================================================
    # Build Preferred Universe
    # ==================================================================

    def build_preferred_universe(
        self,
        issuers: Optional[List[str]] = None,
        max_per_issuer: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Build a universe of preferred stock prospectus filings by searching
        across multiple major issuers.

        Args:
            issuers: List of issuer names to search for. If None, uses
                    a default list of major preferred stock issuers.
            max_per_issuer: Maximum filings per issuer.

        Returns:
            List of filing dicts for the entire universe.
        """
        if issuers is None:
            issuers = [
                "JPMorgan Chase",
                "Bank of America",
                "Goldman Sachs",
                "Morgan Stanley",
                "Wells Fargo",
                "Citigroup",
                "US Bancorp",
                "PNC Financial",
                "Truist Financial",
                "Capital One",
                "MetLife",
                "Prudential Financial",
                "Allstate",
                "Hartford Financial",
                "Public Storage",
                "Digital Realty",
                "Simon Property",
                "Vornado Realty",
                "Duke Energy",
                "Southern Company",
                "NextEra Energy",
                "Dominion Energy",
                "AT&T",
                "Sempra Energy",
                "CenterPoint Energy",
            ]

        universe = []
        for issuer in issuers:
            print(f"\n  Searching for: {issuer}")
            filings = self.search_preferred_prospectuses(
                issuer=issuer,
                max_results=max_per_issuer,
            )
            universe.extend(filings)
            time.sleep(REQUEST_DELAY)

        print(f"\n  [EDGAR] Built universe of {len(universe)} preferred filings "
              f"across {len(issuers)} issuers")
        return universe

    def save_universe(
        self,
        universe: List[Dict[str, Any]],
        filepath: Optional[str] = None,
    ) -> str:
        """
        Save the preferred universe to a JSON file.

        Args:
            universe: List of filing dicts.
            filepath: Output file path. Defaults to data/preferred_universe.json.

        Returns:
            The filepath where the universe was saved.
        """
        if filepath is None:
            filepath = os.path.join(
                os.path.dirname(__file__), "..", "..", "data", "preferred_universe.json"
            )

        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        with open(filepath, "w") as f:
            json.dump(universe, f, indent=2)

        print(f"  [EDGAR] Saved universe to {filepath}")
        return filepath

    # ==================================================================
    # Internal Helpers
    # ==================================================================

    def _html_to_text(self, html: str) -> str:
        """Convert HTML filing content to clean plain text."""
        soup = BeautifulSoup(html, "html.parser")
        for element in soup(["script", "style", "meta", "link"]):
            element.decompose()
        text = soup.get_text(separator="\n")
        return text


# ---------------------------------------------------------------------------
# Registry + convenience functions
# ---------------------------------------------------------------------------

def _load_json_file(path: str) -> Any:
    """Safely load JSON from disk."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def _save_json_file(path: str, payload: Any) -> None:
    """Persist JSON to disk, creating parent directories when needed."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def _resolution_cache_path(ticker: str) -> str:
    """Return the on-disk cache path for filing-resolution results."""
    safe_ticker = _normalize_preferred_ticker(ticker).replace("/", "_")
    return os.path.join(CACHE_DIR, f"resolution_{safe_ticker}.json")


def _load_resolution_cache(ticker: str) -> Optional[Dict[str, Any]]:
    """Load a recent filing-resolution cache entry for a ticker."""
    cached = _load_json_file(_resolution_cache_path(ticker))
    if not isinstance(cached, dict):
        return None

    cached_at = float(cached.get("cached_at", 0.0) or 0.0)
    status = str(cached.get("status", "")).lower()
    ttl = RESOLVED_CACHE_TTL_SECONDS if status == "resolved" else UNRESOLVED_CACHE_TTL_SECONDS
    if not cached_at or (time.time() - cached_at) > ttl:
        return None
    return cached


def _save_resolution_cache(
    ticker: str,
    filings: List[Dict[str, Any]],
    resolution: Dict[str, Any],
) -> None:
    """Persist filing-resolution results for later reruns."""
    payload = {
        "requested_ticker": _normalize_preferred_ticker(ticker),
        "cached_at": time.time(),
        "status": "resolved" if resolution.get("selected_filing") else "unresolved",
        "filings": filings,
        "resolution": resolution,
    }
    _save_json_file(_resolution_cache_path(ticker), payload)


def load_demo_filing_registry() -> Dict[str, Dict[str, Any]]:
    """Load the committed quick-pick registry for cache-first demo tickers."""
    if not os.path.exists(DEMO_FILING_REGISTRY_PATH):
        return {}

    with open(DEMO_FILING_REGISTRY_PATH, "r", encoding="utf-8") as f:
        raw_registry = json.load(f)

    registry: Dict[str, Dict[str, Any]] = {}
    for ticker, entry in raw_registry.items():
        registry[_normalize_preferred_ticker(ticker)] = entry
    return registry


def get_demo_filing_registry_entry(ticker: str) -> Optional[Dict[str, Any]]:
    """Return the committed registry entry for a demo ticker, if present."""
    return load_demo_filing_registry().get(_normalize_preferred_ticker(ticker))


def resolve_preferred_filing(
    ticker: str,
    pipeline: Optional[EdgarPipeline] = None,
    max_results: int = 20,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Resolve the best SEC filing for a preferred ticker without eagerly downloading
    the full filing text.

    Resolution order:
    1. Committed quick-pick registry
    2. EDGAR full-text search
    3. EDGAR submissions fallback
    """
    pipeline = pipeline or EdgarPipeline()

    requested_ticker = _normalize_preferred_ticker(ticker)
    parent_ticker = requested_ticker.split("-")[0]

    registry_entry = get_demo_filing_registry_entry(requested_ticker)
    requested_series = (
        registry_entry.get("expected_series")
        if registry_entry
        else _derive_series_hint(requested_ticker)
    )

    if registry_entry:
        filing = _registry_entry_to_filing(requested_ticker, registry_entry)
        resolution = _build_resolution_metadata(
            requested_ticker=requested_ticker,
            requested_series=requested_series,
            selected_filing=filing,
            source="registry",
            validation_tokens=registry_entry.get("validation_tokens", []),
            force_series_match=True,
        )
        print(f"  [EDGAR] Using demo filing registry for {requested_ticker}")
        return [filing], resolution

    cached_resolution = _load_resolution_cache(requested_ticker)
    if cached_resolution:
        status = str(cached_resolution.get("status", "")).lower() or "resolved"
        print(f"  [EDGAR] Using cached {status} filing resolution for {requested_ticker}")
        return (
            cached_resolution.get("filings", []) or [],
            cached_resolution.get("resolution", {}) or {},
        )

    issuer_name = TICKER_TO_ISSUER_NAME.get(parent_ticker, parent_ticker)

    filings = pipeline.search_preferred_prospectuses(
        issuer=issuer_name,
        series_hint=requested_series,
        date_start="2000-01-01",
        max_results=max_results,
    )
    source = "full_text_search"

    if not filings:
        filings = pipeline.search_preferred_prospectuses(
            issuer=issuer_name,
            date_start="2000-01-01",
            max_results=max_results,
        )

    if not filings:
        filings = pipeline.get_issuer_filings(ticker, max_results=max_results)
        source = "submissions"

    if not filings:
        resolution = {
            "requested_ticker": requested_ticker,
            "requested_series": requested_series,
            "source": "none",
            "selected_filing": {},
            "series_match": False,
            "mismatch_warning": f"No prospectus candidates found for {requested_ticker}.",
        }
        _save_resolution_cache(requested_ticker, [], resolution)
        return [], resolution

    best_filing = _select_best_filing(filings, requested_ticker, requested_series, pipeline)
    resolution = _build_resolution_metadata(
        requested_ticker=requested_ticker,
        requested_series=requested_series,
        selected_filing=best_filing,
        source=source,
    )
    _save_resolution_cache(requested_ticker, filings, resolution)
    return filings, resolution


def fetch_preferred_prospectus(
    ticker: str,
    download_text: bool = True,
) -> Tuple[List[Dict[str, Any]], str, Dict[str, Any]]:
    """
    High-level convenience function: given a preferred stock ticker,
    find and download the most relevant prospectus filing.

    Args:
        ticker: Preferred stock ticker (e.g., "JPM-PD", "BAC-PL").

    Returns:
        Tuple of (list of candidate filings, text of selected filing, resolution metadata).
    """
    pipeline = EdgarPipeline()
    filings, resolution = resolve_preferred_filing(ticker, pipeline=pipeline)

    selected_filing = resolution.get("selected_filing", {})
    if not filings or not selected_filing or not download_text:
        return filings, "", resolution

    text = pipeline.download_filing(selected_filing)
    return filings, text, resolution


def _normalize_preferred_ticker(ticker: str) -> str:
    """Normalize user input to the uppercase quick-pick / EDGAR lookup format."""
    return ticker.strip().upper()


def _derive_series_hint(ticker: str) -> str:
    """
    Convert preferred ticker syntax into a likely SEC series label.

    Examples:
    - JPM-PD -> Series D
    - BAC-PL -> Series L
    - C-PJ   -> Series J
    """
    ticker = _normalize_preferred_ticker(ticker)

    registry_entry = get_demo_filing_registry_entry(ticker)
    if registry_entry and registry_entry.get("expected_series"):
        return registry_entry["expected_series"]

    if "-" not in ticker:
        return ""

    series_part = ticker.split("-", 1)[1].upper()
    if series_part.startswith("P") and len(series_part) > 1:
        series_part = series_part[1:]

    return f"Series {series_part}" if series_part else ""


def _select_best_filing(
    filings: List[Dict[str, Any]],
    requested_ticker: str,
    requested_series: str,
    pipeline: EdgarPipeline,
    max_candidates: int = 4,
    max_preview_downloads: int = 2,
) -> Dict[str, Any]:
    """
    Pick the filing that best matches the requested preferred ticker.

    For legacy preferreds, issuer-level EDGAR searches can return newer preferred
    series first. This heuristic looks at ticker metadata plus a quick text scan
    of the top candidate filings and prefers the one whose series appears to
    match the requested security.
    """
    if not filings:
        return {}

    ticker_token = requested_ticker.lower()
    ticker_token_spaced = requested_ticker.lower().replace("-", " ")
    series_token = requested_series.lower()

    best_filing = filings[0]
    best_score = float("-inf")
    preview_downloads = 0

    for filing in filings[:max_candidates]:
        score = 0

        filing_tickers = [t.upper() for t in filing.get("tickers", [])]
        if requested_ticker in filing_tickers:
            score += 3

        metadata_blob = " ".join(
            str(filing.get(key, "")) for key in ("issuer_name", "description", "filename", "url")
        ).lower()
        if series_token and series_token in metadata_blob:
            score += 4
        if ticker_token in metadata_blob or ticker_token_spaced in metadata_blob:
            score += 2

        should_preview = (
            preview_downloads < max_preview_downloads
            and (
                not series_token
                or series_token not in metadata_blob
            )
        )
        if should_preview:
            preview_text = pipeline.download_filing(filing, max_chars=8000)
            preview_blob = preview_text.lower() if preview_text else ""
            preview_downloads += 1
            if series_token and series_token in preview_blob:
                score += 6
            if ticker_token in preview_blob or ticker_token_spaced in preview_blob:
                score += 3

        if score > best_score:
            best_score = score
            best_filing = filing

    return best_filing


def _registry_entry_to_filing(ticker: str, entry: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a registry entry into the filing shape used by the EDGAR pipeline."""
    filing = dict(entry)
    filing["ticker"] = ticker
    filing["tickers"] = [ticker]
    filing.setdefault("issuer_name", entry.get("issuer"))
    filing.setdefault("url", entry.get("filing_url"))
    filing.setdefault("description", entry.get("expected_series", ""))
    filing.setdefault("form_type", entry.get("form_type", "registry"))
    return filing


def _build_resolution_metadata(
    requested_ticker: str,
    requested_series: str,
    selected_filing: Dict[str, Any],
    source: str,
    validation_tokens: Optional[List[str]] = None,
    force_series_match: bool = False,
) -> Dict[str, Any]:
    """Build resolution metadata for downstream cache/extraction transparency."""
    validation_tokens = validation_tokens or []

    metadata_blob = " ".join(
        str(selected_filing.get(key, ""))
        for key in (
            "issuer_name",
            "description",
            "filename",
            "url",
            "expected_series",
            "security_name",
        )
    ).lower()

    series_match = force_series_match
    if not series_match:
        tokens = [requested_series] + validation_tokens
        tokens = [token for token in tokens if token]
        series_match = any(token.lower() in metadata_blob for token in tokens)

    mismatch_warning = None
    if requested_series and not series_match:
        mismatch_warning = (
            f"Resolved filing for {requested_ticker} did not clearly match {requested_series}."
        )

    return {
        "requested_ticker": requested_ticker,
        "requested_series": requested_series,
        "source": source,
        "selected_filing": selected_filing,
        "accession_number": selected_filing.get("accession_number", ""),
        "filing_date": selected_filing.get("filing_date", ""),
        "filing_url": selected_filing.get("url", ""),
        "matched_series": selected_filing.get("expected_series") or requested_series,
        "series_match": series_match,
        "validation_tokens": validation_tokens,
        "mismatch_warning": mismatch_warning,
    }


# ---------------------------------------------------------------------------
# CLI for testing
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    ticker = sys.argv[1] if len(sys.argv) > 1 else "JPM"
    print(f"\n{'='*60}")
    print(f"SEC EDGAR Pipeline Test: {ticker}")
    print(f"{'='*60}")

    pipeline = EdgarPipeline()

    # Step 1: CIK lookup
    cik = pipeline.get_cik(ticker)
    print(f"\nCIK for {ticker}: {cik}")

    # Step 2: Full-text search for preferred prospectuses
    parent = ticker.split("-")[0].upper()
    name_map = {
        "JPM": "JPMorgan Chase", "BAC": "Bank of America",
        "GS": "Goldman Sachs", "MS": "Morgan Stanley",
        "WFC": "Wells Fargo", "C": "Citigroup",
    }
    issuer_name = name_map.get(parent, parent)

    print(f"\nSearching for preferred prospectuses: {issuer_name}")
    filings = pipeline.search_preferred_prospectuses(
        issuer=issuer_name, max_results=10
    )

    print(f"\nFound {len(filings)} preferred prospectus filings:")
    for f in filings[:10]:
        print(f"  {f['filing_date']} | {f['form_type']:8s} | {f['issuer_name'][:40]} | score: {f.get('search_score', 0):.1f}")
        print(f"    Tickers: {', '.join(f.get('tickers', []))}")
        print(f"    URL: {f['url']}")

    # Step 3: Try to download the best match
    if filings:
        print(f"\nDownloading best match: {filings[0]['accession_number']}")
        text = pipeline.download_filing(filings[0], max_chars=3000)
        if text:
            print(f"Text length: {len(text)} chars")
            print(f"\nFirst 500 chars:\n{text[:500]}")
        else:
            print("Download failed (likely SEC rate limiting from sandbox).")
            print("This will work on your local machine.")
