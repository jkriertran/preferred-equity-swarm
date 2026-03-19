"""
Shared local security-context helpers for preferred securities.

This module centralizes local metadata assembly so the resolver, market-data
layer, and provider adapters do not each rebuild their own view of the same
security. The precedence is:

1. Cached prospectus/runtime terms
2. Curated universe entry
3. Snapshot-only fields when explicitly requested
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from src.data.prospectus_inventory import load_cached_terms_for_ticker


_DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"
_UNIVERSE_PATH = _DATA_DIR / "preferred_universe.json"
_SNAPSHOT_PATH = _DATA_DIR / "market_snapshots.json"

_universe_cache: Optional[Dict[str, Dict[str, Any]]] = None
_snapshot_cache: Optional[Dict[str, Dict[str, Any]]] = None


def _load_json(path: Path) -> Any:
    """Safely load JSON from disk."""
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def load_preferred_universe() -> Dict[str, Dict[str, Any]]:
    """Return the curated preferred universe keyed by canonical ticker."""
    global _universe_cache
    if _universe_cache is not None:
        return _universe_cache

    raw = _load_json(_UNIVERSE_PATH)
    if isinstance(raw, dict):
        universe = raw.get("securities") or {}
        if isinstance(universe, dict):
            _universe_cache = universe
            return _universe_cache

    _universe_cache = {}
    return _universe_cache


def load_snapshot_index() -> Dict[str, Dict[str, Any]]:
    """Return the local market-snapshot index keyed by canonical ticker."""
    global _snapshot_cache
    if _snapshot_cache is not None:
        return _snapshot_cache

    raw = _load_json(_SNAPSHOT_PATH)
    if isinstance(raw, dict):
        market_data = raw.get("market_data") or raw.get("securities") or {}
        if isinstance(market_data, dict):
            _snapshot_cache = market_data
            return _snapshot_cache

    _snapshot_cache = {}
    return _snapshot_cache


def get_universe_entry(ticker: str) -> Dict[str, Any]:
    """Return the curated universe entry for a canonical ticker."""
    normalized = str(ticker or "").strip().upper()
    if not normalized:
        return {}
    entry = load_preferred_universe().get(normalized, {})
    return dict(entry) if isinstance(entry, dict) else {}


def get_snapshot_entry(ticker: str) -> Dict[str, Any]:
    """Return the local market snapshot row for a canonical ticker."""
    normalized = str(ticker or "").strip().upper()
    if not normalized:
        return {}
    entry = load_snapshot_index().get(normalized, {})
    return dict(entry) if isinstance(entry, dict) else {}


def get_cached_terms(ticker: str) -> Dict[str, Any]:
    """Return the best available cached prospectus terms for a ticker."""
    normalized = str(ticker or "").strip().upper()
    if not normalized:
        return {}
    terms = load_cached_terms_for_ticker(normalized)
    return dict(terms) if isinstance(terms, dict) else {}


def get_security_context(ticker: str, include_snapshot: bool = False) -> Dict[str, Any]:
    """Build the local security context for a canonical ticker.

    Cached prospectus/runtime terms override curated universe fields. Snapshot
    rows are returned separately and are not merged into the authoritative
    structural metadata unless a caller explicitly chooses to use them.
    """
    normalized = str(ticker or "").strip().upper()
    cached_terms = get_cached_terms(normalized)
    universe_entry = get_universe_entry(normalized)
    snapshot_entry = get_snapshot_entry(normalized) if include_snapshot else {}

    merged_entry = dict(universe_entry)
    merged_entry.update({key: value for key, value in cached_terms.items() if value is not None})

    parent_ticker = merged_entry.get("parent_ticker")
    if not parent_ticker and normalized:
        parent_ticker = normalized.split("-", 1)[0]

    return {
        "ticker": normalized,
        "security_name": merged_entry.get("security_name") or snapshot_entry.get("name"),
        "issuer": merged_entry.get("issuer"),
        "series": merged_entry.get("series"),
        "parent_ticker": parent_ticker,
        "coupon_type": merged_entry.get("coupon_type"),
        "coupon_rate": merged_entry.get("coupon_rate"),
        "par_value": merged_entry.get("par_value"),
        "provider_symbols": merged_entry.get("provider_symbols") or {},
        "has_prospectus_cache": bool(cached_terms),
        "cached_terms": cached_terms,
        "universe_entry": universe_entry,
        "snapshot_entry": snapshot_entry,
        "merged_entry": merged_entry,
    }
