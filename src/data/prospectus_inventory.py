"""
Prospectus cache inventory helpers.

Builds a user-facing inventory of preferred issues that are already available
for quick analysis from structured cache, combining:

- committed demo cache
- local runtime-generated cache
"""

import json
import os
from typing import Any, Dict, List


DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
REGISTRY_PATH = os.path.join(DATA_DIR, "preferred_filing_registry.json")
DEMO_CACHE_DIR = os.path.join(DATA_DIR, "prospectus_terms", "demo")
RUNTIME_CACHE_DIR = os.path.join(DATA_DIR, "prospectus_terms", "runtime")


def load_cached_prospectus_inventory() -> List[Dict[str, Any]]:
    """Return cached preferred issues that can be analyzed quickly."""
    registry = _load_registry()
    inventory_by_ticker: Dict[str, Dict[str, Any]] = {}

    for path in _iter_cache_files(DEMO_CACHE_DIR):
        terms = _load_json(path)
        if not terms or terms.get("error"):
            continue

        ticker = str(terms.get("ticker", "")).upper()
        if not ticker:
            continue

        inventory_by_ticker[ticker] = _inventory_row_from_terms(
            ticker=ticker,
            terms=terms,
            registry_entry=registry.get(ticker),
            cache_tier="demo",
        )

    for path in _iter_cache_files(RUNTIME_CACHE_DIR):
        terms = _load_json(path)
        if not terms or terms.get("error"):
            continue

        ticker = str(terms.get("ticker", "")).upper()
        if not ticker:
            continue

        existing = inventory_by_ticker.get(ticker)
        if existing and existing.get("cache_tier") == "demo":
            continue

        inventory_by_ticker[ticker] = _inventory_row_from_terms(
            ticker=ticker,
            terms=terms,
            registry_entry=registry.get(ticker),
            cache_tier="runtime",
        )

    rows = list(inventory_by_ticker.values())
    rows.sort(key=lambda row: (0 if row["cache_tier"] == "demo" else 1, row["ticker"]))
    return rows


def get_quick_analysis_tickers(limit: int = 6) -> List[str]:
    """Return tickers to render as one-click buttons in the UI."""
    registry = _load_registry()
    demo_tickers = list(registry.keys())
    runtime_rows = [
        row["ticker"]
        for row in load_cached_prospectus_inventory()
        if row["cache_tier"] == "runtime"
    ]

    tickers: List[str] = []
    for ticker in demo_tickers + runtime_rows:
        if ticker not in tickers:
            tickers.append(ticker)

    return tickers[:limit]


def get_inventory_lookup() -> Dict[str, Dict[str, Any]]:
    """Convenience lookup by ticker for UI messaging."""
    return {row["ticker"]: row for row in load_cached_prospectus_inventory()}


def _inventory_row_from_terms(
    ticker: str,
    terms: Dict[str, Any],
    registry_entry: Dict[str, Any],
    cache_tier: str,
) -> Dict[str, Any]:
    """Normalize one cached terms file into a compact inventory row."""
    registry_entry = registry_entry or {}
    quick_analysis = cache_tier in {"demo", "runtime"}

    return {
        "ticker": ticker,
        "issuer": terms.get("issuer") or registry_entry.get("issuer") or "Unknown",
        "series": terms.get("series") or registry_entry.get("expected_series") or "Unknown",
        "security_name": terms.get("security_name") or "Unknown",
        "filing_date": terms.get("filing_date") or registry_entry.get("filing_date") or "",
        "accession_number": terms.get("accession_number") or registry_entry.get("accession_number") or "",
        "filing_url": terms.get("filing_url") or registry_entry.get("filing_url") or "",
        "confidence_score": terms.get("confidence_score"),
        "source": terms.get("source") or "cache",
        "resolution_source": terms.get("resolution_source") or "cache",
        "cache_tier": cache_tier,
        "availability": "Quick analysis" if quick_analysis else "Live search",
        "cache_label": "Demo cache" if cache_tier == "demo" else "Local runtime cache",
    }


def _load_registry() -> Dict[str, Dict[str, Any]]:
    """Load the committed preferred filing registry keyed by ticker."""
    raw = _load_json(REGISTRY_PATH)
    if not isinstance(raw, dict):
        return {}
    return {str(ticker).upper(): entry for ticker, entry in raw.items()}


def _iter_cache_files(directory: str) -> List[str]:
    """List JSON cache files from a cache directory if it exists."""
    if not os.path.isdir(directory):
        return []
    return sorted(
        os.path.join(directory, name)
        for name in os.listdir(directory)
        if name.endswith(".json")
    )


def _load_json(path: str) -> Any:
    """Safely load JSON from disk."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
