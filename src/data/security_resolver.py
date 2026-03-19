"""
Preferred Security Resolver
============================
Provides authoritative identification and validation for preferred equity
securities. This module is the single source of truth for mapping user-entered
tickers to verified security metadata.

Resolution Strategy (three layers):
    Layer 1: Curated reference database (preferred_universe.json)
    Layer 2: PFF ETF holdings cross-reference (auto-detects redemptions)
    Layer 3: Live provider lookup (fallback for unknown tickers)

Usage:
    from src.data.security_resolver import resolve_security, get_known_tickers

    info = resolve_security("C-PN")
    # Returns a dict with ticker, issuer, security_name, status, etc.

    tickers = get_known_tickers()
    # Returns a list of all known preferred tickers
"""

import re
import logging
from pathlib import Path
from typing import Optional

from src.data.alpha_vantage import get_quote as get_alpha_vantage_quote
from src.data.alpha_vantage import is_preferred_reference, lookup_reference_symbol
from src.data.security_context import get_security_context, load_preferred_universe

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"
_DEMO_CACHE_DIR = _DATA_DIR / "prospectus_terms" / "demo"


# ---------------------------------------------------------------------------
# Ticker normalization
# ---------------------------------------------------------------------------
_CANONICAL_TICKER_RE = re.compile(r"^[A-Z]+-P[A-Z]$")
_TICKER_PATTERNS = [
    # "C-PN" -> already correct
    re.compile(r"^([A-Z]+)-P([A-Z])$", re.IGNORECASE),
    # "ADC-P-A" -> "ADC-PA"
    re.compile(r"^([A-Z]+)-P-([A-Z])$", re.IGNORECASE),
    # "C.PR.N" or "C/PR/N" or "C.PRN" -> "C-PN"
    re.compile(r"^([A-Z]+)[./]PR[./]?([A-Z])$", re.IGNORECASE),
    # "C.PN" or "C/P/N" -> "C-PN"
    re.compile(r"^([A-Z]+)[./]P[./]?([A-Z])$", re.IGNORECASE),
    # "C PRN" or "C PR N" -> "C-PN"
    re.compile(r"^([A-Z]+)\s+PR\s*([A-Z])$", re.IGNORECASE),
    # "ADC P A" or "ADC P-A" -> "ADC-PA"
    re.compile(r"^([A-Z]+)\s+P[\s-]*([A-Z])$", re.IGNORECASE),
    # "BAC+PL" -> "BAC-PL"
    re.compile(r"^([A-Z]+)\+P([A-Z])$", re.IGNORECASE),
    # "BAC PL" or "BAC P L" -> "BAC-PL"
    re.compile(r"^([A-Z]+)\s+P\s*([A-Z])$", re.IGNORECASE),
]
_INLINE_LOWER_P_PATTERN = re.compile(r"^([A-Za-z]+)p([A-Za-z])$")


def _format_canonical_ticker(base: str, series: str) -> str:
    """Format a preferred ticker in canonical ISSUER-PX form."""
    return f"{base.upper()}-P{series.upper()}"


def normalize_ticker(raw_ticker: str) -> str:
    """
    Normalize a preferred stock ticker to the canonical format: PARENT-PX.

    Handles common variations:
        C-PN, C.PR.N, C PRN, CpN, C/PR/N -> C-PN
        ADC-P-A, ADC P A -> ADC-PA
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
    raw = raw_ticker.strip()
    if not raw:
        return ""

    cleaned = re.sub(r"\s+", " ", raw.upper())

    # Already in canonical format?
    if _CANONICAL_TICKER_RE.match(cleaned):
        return cleaned

    # Try each pattern
    for pattern in _TICKER_PATTERNS:
        match = pattern.match(cleaned)
        if match:
            return _format_canonical_ticker(match.group(1), match.group(2))

    # Preserve the lowercase inline "p" shorthand (e.g. "CpN")
    inline_match = _INLINE_LOWER_P_PATTERN.match(raw)
    if inline_match:
        return _format_canonical_ticker(inline_match.group(1), inline_match.group(2))

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
        3. Attempt a live Alpha Vantage lookup as a fallback
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
            - resolution_source: str ("universe", "demo_cache", "alpha_vantage_live", "unresolved")
            - security_name: str or None
            - issuer: str or None
            - parent_ticker: str or None
            - status: str ("active", "redeemed", "unknown")
            - in_pff: bool
            - has_prospectus_cache: bool
            - warnings: list[str]
            - trusted_for_analysis: bool
    """
    ticker = normalize_ticker(raw_ticker)
    warnings = []
    context = get_security_context(ticker)
    universe_entry = context.get("universe_entry") or {}
    cached_terms = context.get("cached_terms") or {}
    merged_entry = context.get("merged_entry") or {}

    # Layer 1: Curated universe
    if universe_entry:
        result = {
            "ticker": ticker,
            "resolved": True,
            "resolution_source": "universe",
            "security_name": context.get("security_name"),
            "issuer": context.get("issuer"),
            "parent_ticker": context.get("parent_ticker"),
            "status": universe_entry.get("status", "active"),
            "in_pff": universe_entry.get("in_pff", False),
            "has_prospectus_cache": context.get("has_prospectus_cache", False),
            "coupon_type": context.get("coupon_type"),
            "coupon_rate": context.get("coupon_rate"),
            "par_value": context.get("par_value"),
            "last_known_price": merged_entry.get("last_known_price"),
            "warnings": warnings,
            "trusted_for_analysis": True,
        }

        # Validate status
        if universe_entry.get("status") == "redeemed":
            warnings.append(
                f"{ticker} has been redeemed and is no longer trading."
            )

        return result

    # Layer 2: Demo cache file exists but not in universe
    if context.get("has_prospectus_cache"):
        terms = cached_terms
        warnings.append(
            f"{ticker} found in demo cache but not in the curated universe. "
            "Prospectus terms are available but metadata may be incomplete."
        )
        return {
            "ticker": ticker,
            "resolved": True,
            "resolution_source": "demo_cache",
            "security_name": context.get("security_name"),
            "issuer": context.get("issuer"),
            "parent_ticker": context.get("parent_ticker"),
            "status": "active",
            "in_pff": False,
            "has_prospectus_cache": True,
            "coupon_type": context.get("coupon_type"),
            "coupon_rate": context.get("coupon_rate"),
            "par_value": context.get("par_value"),
            "last_known_price": None,
            "warnings": warnings,
            "trusted_for_analysis": True,
        }

    # Layer 3: Live provider lookup
    live_result = _try_live_lookup(ticker)
    if live_result:
        # Validate: is this actually a preferred stock?
        name = (live_result.get("longName") or "").upper()
        price = live_result.get("price", 0)
        instrument_type = str(live_result.get("instrument_type", "")).upper()

        is_preferred = any(
            kw in name
            for kw in [
                "PREFERRED", "PFD", "DEPOSITARY", "DEPOSITORY",
                "TRUST", "CAPITAL", "NON-CUMULATIVE", "NONCUMULATIVE",
                "CUMULATIVE", "PERPETUAL", "TR PREF",
            ]
        ) or "PREFERRED" in instrument_type

        # Price sanity check: depositary preferred shares typically trade $5-$60
        price_reasonable = 5.0 <= price <= 60.0 if price else False

        if not is_preferred:
            warnings.append(
                f"Live lookup name '{live_result.get('longName')}' does not "
                "appear to be a preferred stock. This may be the common stock "
                "or a different security class."
            )

        if not price_reasonable and price:
            warnings.append(
                f"Price ${price:.2f} is outside the typical preferred stock "
                "range ($5-$60). This may be a full-share preferred or a "
                "different security class."
            )

        trusted_for_analysis = is_preferred

        return {
            "ticker": ticker,
            "resolved": True,
            "resolution_source": live_result.get("resolution_source", "live_lookup"),
            "security_name": live_result.get("longName"),
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
            "trusted_for_analysis": trusted_for_analysis,
        }

    # Layer 4: Unresolved
    warnings.append(
        f"{ticker} could not be found in the curated universe, demo cache, "
        "or the live market-data provider. Please verify the ticker symbol."
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
        "trusted_for_analysis": False,
    }


# ---------------------------------------------------------------------------
# Helper: Demo cache
# ---------------------------------------------------------------------------
def _has_demo_cache(ticker: str) -> bool:
    """Check if any structured prospectus cache exists for this ticker."""
    return bool(get_security_context(ticker).get("has_prospectus_cache"))


def _load_demo_cache(ticker: str) -> dict:
    """Load the best available structured prospectus cache for a ticker."""
    return dict(get_security_context(ticker).get("cached_terms") or {})


# ---------------------------------------------------------------------------
# Helper: live lookup
# ---------------------------------------------------------------------------
def _try_live_lookup(ticker: str) -> Optional[dict]:
    """Attempt a live Alpha Vantage lookup for a ticker."""
    try:
        reference = lookup_reference_symbol(ticker, require_preferred=False)
        quote = get_alpha_vantage_quote(ticker, require_preferred=False)
        if not quote:
            return None

        price = quote.get("close") or quote.get("price")
        if price:
            return {
                "longName": quote.get("name"),
                "price": float(price),
                "exchange": (reference or {}).get("region", ""),
                "instrument_type": (reference or {}).get("type", ""),
                "resolution_source": "alpha_vantage_live",
                "is_preferred_reference": is_preferred_reference(reference),
            }
    except Exception as exc:
        logger.debug("Alpha Vantage lookup failed for %s: %s", ticker, exc)
    return None


# ---------------------------------------------------------------------------
# Public utilities
# ---------------------------------------------------------------------------
def get_known_tickers() -> list:
    """
    Return a sorted list of all known preferred stock tickers
    from the curated universe.
    """
    universe = load_preferred_universe()
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
    universe = load_preferred_universe()
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
    universe = load_preferred_universe()
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


def get_universe_grouped_by_issuer() -> list:
    """
    Return the curated universe grouped by parent issuer, sorted alphabetically.

    Returns
    -------
    list[dict]
        Each entry has:
            - parent_ticker: str (e.g., "BAC")
            - issuer_name: str (e.g., "Bank of America")
            - tickers: list[str] (e.g., ["BAC-PB", "BAC-PE", ...])
            - has_cache: list[str] (tickers with prospectus cache)
    """
    universe = load_preferred_universe()
    demo_tickers = set(get_demo_tickers())

    # Manual overrides for issuer display names that are hard to clean automatically
    _ISSUER_DISPLAY_NAMES = {
        "ALB": "Albemarle",
        "ALL": "Allstate",
        "APO": "Apollo Global",
        "ARES": "Ares Management",
        "ATH": "Athene Holding",
        "BAC": "Bank of America",
        "C": "Citigroup",
        "COF": "Capital One",
        "GS": "Goldman Sachs",
        "HPE": "HP Enterprise",
        "JPM": "JPMorgan Chase",
        "MET": "MetLife",
        "MS": "Morgan Stanley",
        "MTB": "M&T Bank",
        "NEE": "NextEra Energy",
        "NLY": "Annaly Capital",
        "ORCL": "Oracle",
        "PCG": "PG&E",
        "SCHW": "Charles Schwab",
        "T": "AT&T",
        "USB": "U.S. Bancorp",
        "WFC": "Wells Fargo",
    }

    groups = {}
    for ticker, entry in universe.items():
        parent = entry.get("parent_ticker", "")
        if not parent:
            continue
        if parent not in groups:
            # Use manual override if available, otherwise try to clean automatically
            if parent in _ISSUER_DISPLAY_NAMES:
                short_name = _ISSUER_DISPLAY_NAMES[parent]
            else:
                raw_name = entry.get("issuer") or entry.get("pff_name") or entry.get("security_name") or parent
                short_name = raw_name.split(" PERP ")[0].split(" PFD ")[0].split(" TR ")[0]
                for suffix in [", Inc.", ", Inc", " Inc.", " Inc", " Corporation", " Corp", " Corp.",
                               " Company", " Co.", " Co", " Ltd.", " Ltd", " LP", " L.P.",
                               " Group, Inc.", " Group", " & Co.", " & Co", " & Company"]:
                    if short_name.endswith(suffix):
                        short_name = short_name[: -len(suffix)].strip()
                        break
            groups[parent] = {
                "parent_ticker": parent,
                "issuer_name": short_name,
                "tickers": [],
                "has_cache": [],
            }
        groups[parent]["tickers"].append(ticker)
        if ticker in demo_tickers:
            groups[parent]["has_cache"].append(ticker)

    # Sort tickers within each group and sort groups by parent ticker
    result = []
    for parent in sorted(groups.keys()):
        g = groups[parent]
        g["tickers"] = sorted(g["tickers"])
        g["has_cache"] = sorted(g["has_cache"])
        result.append(g)

    return result


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

    if not resolution.get("trusted_for_analysis", resolution["resolved"]):
        return {
            "valid": False,
            "ticker": ticker,
            "reason": " ".join(resolution["warnings"]) or (
                f"{ticker} could not be confidently verified as a preferred stock."
            ),
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
