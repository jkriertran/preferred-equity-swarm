"""
Preferred Security Resolver
============================
Provides authoritative identification and validation for preferred equity
securities. This module is the single source of truth for mapping user-entered
tickers to verified security metadata.

Resolution Strategy (three layers):
    Layer 1: Curated reference database (preferred_universe.json)
    Layer 2: PFF ETF holdings cross-reference (auto-detects redemptions)
    Layer 3: Yahoo Finance live lookup (fallback for unknown tickers)

Usage:
    from src.data.security_resolver import resolve_security, get_known_tickers

    info = resolve_security("C-PN")
    # Returns a dict with ticker, issuer, security_name, status, etc.

    tickers = get_known_tickers()
    # Returns a list of all known preferred tickers
"""

import json
import os
import re
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"
_UNIVERSE_PATH = _DATA_DIR / "preferred_universe.json"
_DEMO_CACHE_DIR = _DATA_DIR / "prospectus_terms" / "demo"
_SNAPSHOT_PATH = _DATA_DIR / "market_snapshots.json"

# ---------------------------------------------------------------------------
# In-memory cache (loaded once)
# ---------------------------------------------------------------------------
_universe_cache: Optional[dict] = None
_snapshot_cache: Optional[dict] = None


def _load_universe() -> dict:
    """Load the preferred universe reference database."""
    global _universe_cache
    if _universe_cache is not None:
        return _universe_cache

    if not _UNIVERSE_PATH.exists():
        logger.warning("preferred_universe.json not found at %s", _UNIVERSE_PATH)
        _universe_cache = {}
        return _universe_cache

    with open(_UNIVERSE_PATH) as f:
        data = json.load(f)

    _universe_cache = data.get("securities", {})
    logger.info("Loaded preferred universe with %d securities", len(_universe_cache))
    return _universe_cache


def _load_snapshots() -> dict:
    """Load the market snapshots for price validation."""
    global _snapshot_cache
    if _snapshot_cache is not None:
        return _snapshot_cache

    if not _SNAPSHOT_PATH.exists():
        _snapshot_cache = {}
        return _snapshot_cache

    with open(_SNAPSHOT_PATH) as f:
        data = json.load(f)

    _snapshot_cache = data.get("securities", data)
    return _snapshot_cache


# ---------------------------------------------------------------------------
# Ticker normalization
# ---------------------------------------------------------------------------
_TICKER_PATTERNS = [
    # "C-PN" -> already correct
    (r"^([A-Z]+)-P([A-Z])$", lambda m: f"{m.group(1)}-P{m.group(2)}"),
    # "C.PR.N" or "C/PR/N" -> "C-PN"
    (r"^([A-Z]+)[./]PR[./]([A-Z])$", lambda m: f"{m.group(1)}-P{m.group(2)}"),
    # "C PRN" or "C PR N" -> "C-PN"
    (r"^([A-Z]+)\s+PR\s*([A-Z])$", lambda m: f"{m.group(1)}-P{m.group(2)}"),
    # "CpN" or "C.pN" -> "C-PN"
    (r"^([A-Z]+)[.p]([A-Z])$", lambda m: f"{m.group(1)}-P{m.group(2)}"),
    # "BAC+PL" -> "BAC-PL"
    (r"^([A-Z]+)\+P([A-Z])$", lambda m: f"{m.group(1)}-P{m.group(2)}"),
    # "BAC PL" -> "BAC-PL"
    (r"^([A-Z]+)\s+P([A-Z])$", lambda m: f"{m.group(1)}-P{m.group(2)}"),
]


def normalize_ticker(raw_ticker: str) -> str:
    """
    Normalize a preferred stock ticker to the canonical format: PARENT-PX.

    Handles common variations:
        C-PN, C.PR.N, C PRN, CpN, C/PR/N -> C-PN
        BAC-PL, BAC+PL, BAC PL -> BAC-PL

    Parameters
    ----------
    raw_ticker : str
        The user-entered ticker string.

    Returns
    -------
    str
        The normalized ticker in PARENT-PX format, or the original
        string uppercased if no pattern matches.
    """
    cleaned = raw_ticker.strip().upper()

    # Already in canonical format?
    if re.match(r"^[A-Z]+-P[A-Z]$", cleaned):
        return cleaned

    # Try each pattern
    for pattern, formatter in _TICKER_PATTERNS:
        match = re.match(pattern, cleaned)
        if match:
            return formatter(match)

    # No match; return uppercased original
    return cleaned


# ---------------------------------------------------------------------------
# Core resolution
# ---------------------------------------------------------------------------
def resolve_security(raw_ticker: str) -> dict:
    """
    Resolve a preferred stock ticker to its authoritative metadata.

    Resolution layers:
        1. Check the curated preferred_universe.json database
        2. Check if a demo prospectus cache file exists
        3. Attempt a live Yahoo Finance lookup as a fallback
        4. Return a minimal "unresolved" record if all layers fail

    Parameters
    ----------
    raw_ticker : str
        The user-entered ticker (any common format).

    Returns
    -------
    dict
        A dictionary containing at minimum:
            - ticker: str (canonical format)
            - resolved: bool (True if found in a trusted source)
            - resolution_source: str ("universe", "demo_cache", "yahoo_live", "unresolved")
            - security_name: str or None
            - issuer: str or None
            - parent_ticker: str or None
            - status: str ("active", "redeemed", "unknown")
            - in_pff: bool
            - has_prospectus_cache: bool
            - warnings: list[str]
    """
    ticker = normalize_ticker(raw_ticker)
    warnings = []

    # Layer 1: Curated universe
    universe = _load_universe()
    if ticker in universe:
        entry = universe[ticker]
        result = {
            "ticker": ticker,
            "resolved": True,
            "resolution_source": "universe",
            "security_name": entry.get("security_name"),
            "issuer": entry.get("issuer"),
            "parent_ticker": entry.get("parent_ticker"),
            "status": entry.get("status", "active"),
            "in_pff": entry.get("in_pff", False),
            "has_prospectus_cache": _has_demo_cache(ticker),
            "coupon_type": entry.get("coupon_type"),
            "coupon_rate": entry.get("coupon_rate"),
            "par_value": entry.get("par_value"),
            "last_known_price": entry.get("last_known_price"),
            "warnings": warnings,
        }

        # Validate status
        if entry.get("status") == "redeemed":
            warnings.append(
                f"{ticker} has been redeemed and is no longer trading."
            )

        return result

    # Layer 2: Demo cache file exists but not in universe
    if _has_demo_cache(ticker):
        terms = _load_demo_cache(ticker)
        warnings.append(
            f"{ticker} found in demo cache but not in the curated universe. "
            "Prospectus terms are available but metadata may be incomplete."
        )
        return {
            "ticker": ticker,
            "resolved": True,
            "resolution_source": "demo_cache",
            "security_name": terms.get("security_name"),
            "issuer": terms.get("issuer"),
            "parent_ticker": ticker.split("-")[0] if "-" in ticker else ticker,
            "status": "active",
            "in_pff": False,
            "has_prospectus_cache": True,
            "coupon_type": terms.get("coupon_type"),
            "coupon_rate": terms.get("coupon_rate"),
            "par_value": terms.get("par_value"),
            "last_known_price": None,
            "warnings": warnings,
        }

    # Layer 3: Live Yahoo Finance lookup
    yahoo_result = _try_yahoo_lookup(ticker)
    if yahoo_result:
        # Validate: is this actually a preferred stock?
        name = (yahoo_result.get("longName") or "").upper()
        price = yahoo_result.get("price", 0)

        is_preferred = any(
            kw in name
            for kw in [
                "PREFERRED", "PFD", "DEPOSITARY", "DEPOSITORY",
                "TRUST", "CAPITAL", "NON-CUMULATIVE", "NONCUMULATIVE",
                "CUMULATIVE", "PERPETUAL", "TR PREF",
            ]
        )

        # Price sanity check: depositary preferred shares typically trade $5-$60
        price_reasonable = 5.0 <= price <= 60.0 if price else False

        if not is_preferred:
            warnings.append(
                f"Yahoo Finance name '{yahoo_result.get('longName')}' does not "
                "appear to be a preferred stock. This may be the common stock "
                "or a different security class."
            )

        if not price_reasonable and price:
            warnings.append(
                f"Price ${price:.2f} is outside the typical preferred stock "
                "range ($5-$60). This may be a full-share preferred or a "
                "different security class."
            )

        return {
            "ticker": ticker,
            "resolved": True,
            "resolution_source": "yahoo_live",
            "security_name": yahoo_result.get("longName"),
            "issuer": None,
            "parent_ticker": ticker.split("-")[0] if "-" in ticker else ticker,
            "status": "active" if price_reasonable else "unknown",
            "in_pff": False,
            "has_prospectus_cache": False,
            "coupon_type": None,
            "coupon_rate": None,
            "par_value": None,
            "last_known_price": round(price, 2) if price else None,
            "warnings": warnings,
        }

    # Layer 4: Unresolved
    warnings.append(
        f"{ticker} could not be found in the curated universe, demo cache, "
        "or Yahoo Finance. Please verify the ticker symbol."
    )
    return {
        "ticker": ticker,
        "resolved": False,
        "resolution_source": "unresolved",
        "security_name": None,
        "issuer": None,
        "parent_ticker": ticker.split("-")[0] if "-" in ticker else ticker,
        "status": "unknown",
        "in_pff": False,
        "has_prospectus_cache": False,
        "coupon_type": None,
        "coupon_rate": None,
        "par_value": None,
        "last_known_price": None,
        "warnings": warnings,
    }


# ---------------------------------------------------------------------------
# Helper: Demo cache
# ---------------------------------------------------------------------------
def _has_demo_cache(ticker: str) -> bool:
    """Check if a demo prospectus cache file exists for this ticker."""
    return (_DEMO_CACHE_DIR / f"{ticker}.json").exists()


def _load_demo_cache(ticker: str) -> dict:
    """Load the demo prospectus cache file for a ticker."""
    path = _DEMO_CACHE_DIR / f"{ticker}.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


# ---------------------------------------------------------------------------
# Helper: Yahoo Finance live lookup
# ---------------------------------------------------------------------------
def _try_yahoo_lookup(ticker: str) -> Optional[dict]:
    """Attempt a live Yahoo Finance lookup for a ticker."""
    try:
        import yfinance as yf
        tk = yf.Ticker(ticker)
        info = tk.info or {}
        price = info.get("regularMarketPrice") or info.get("previousClose")
        name = info.get("longName") or info.get("shortName")
        if price and price > 0:
            return {
                "longName": name,
                "price": price,
                "exchange": info.get("exchange", ""),
            }
    except Exception as e:
        logger.debug("Yahoo Finance lookup failed for %s: %s", ticker, e)

    return None


# ---------------------------------------------------------------------------
# Public utilities
# ---------------------------------------------------------------------------
def get_known_tickers() -> list:
    """
    Return a sorted list of all known preferred stock tickers
    from the curated universe.
    """
    universe = _load_universe()
    return sorted(universe.keys())


def get_demo_tickers() -> list:
    """
    Return a sorted list of tickers that have a demo prospectus cache.
    """
    if not _DEMO_CACHE_DIR.exists():
        return []
    return sorted(
        f.stem for f in _DEMO_CACHE_DIR.glob("*.json")
    )


def get_pff_tickers() -> list:
    """
    Return a sorted list of tickers that are currently held in the PFF ETF.
    """
    universe = _load_universe()
    return sorted(
        ticker for ticker, entry in universe.items()
        if entry.get("in_pff")
    )


def search_by_issuer(query: str) -> list:
    """
    Search the universe for securities matching an issuer name query.

    Parameters
    ----------
    query : str
        A partial issuer name (case-insensitive).

    Returns
    -------
    list[dict]
        A list of matching security entries with their tickers.
    """
    universe = _load_universe()
    query_upper = query.upper()
    results = []
    for ticker, entry in universe.items():
        name = (entry.get("security_name") or "").upper()
        issuer = (entry.get("issuer") or "").upper()
        pff_name = (entry.get("pff_name") or "").upper()
        if (
            query_upper in name
            or query_upper in issuer
            or query_upper in pff_name
            or query_upper in ticker
        ):
            results.append({"ticker": ticker, **entry})
    return results


def validate_ticker_for_analysis(raw_ticker: str) -> dict:
    """
    Validate whether a ticker is suitable for full pipeline analysis.

    Returns a dict with:
        - valid: bool
        - ticker: str (normalized)
        - reason: str (explanation if not valid)
        - resolution: dict (full resolve_security result)
    """
    resolution = resolve_security(raw_ticker)
    ticker = resolution["ticker"]

    if not resolution["resolved"]:
        return {
            "valid": False,
            "ticker": ticker,
            "reason": f"Could not find {ticker} in any data source. "
                      "Please check the ticker symbol.",
            "resolution": resolution,
        }

    if resolution["status"] == "redeemed":
        return {
            "valid": False,
            "ticker": ticker,
            "reason": f"{ticker} has been redeemed and is no longer trading. "
                      "Historical analysis is not supported.",
            "resolution": resolution,
        }

    if resolution["warnings"]:
        # Still valid but with caveats
        return {
            "valid": True,
            "ticker": ticker,
            "reason": " ".join(resolution["warnings"]),
            "resolution": resolution,
        }

    return {
        "valid": True,
        "ticker": ticker,
        "reason": "OK",
        "resolution": resolution,
    }
