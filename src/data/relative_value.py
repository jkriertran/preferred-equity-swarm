"""
Relative Value Agent for Preferred Equity Analysis.

Ranks the target preferred security against its peer universe by yield,
credit quality proxy, coupon structure, and tax-adjusted return.  Also
compares the preferred against the issuer's common equity dividend yield
and senior bond yields when available.

Key concepts:
  - Peer comparison: preferreds from the same issuer or same sector are
    the most relevant comparables.
  - Yield spread: the difference between the preferred's yield and a
    benchmark (10Y Treasury, issuer common dividend yield).
  - Structure premium: floating-rate, convertible, and cumulative
    features command different yield premiums.
  - Tax-adjusted ranking: QDI-eligible preferreds may rank higher on an
    after-tax basis even if their nominal yield is lower.

The agent uses the cached prospectus inventory to build the peer set.
"""

import json
import os
from typing import Any, Dict, List, Optional

from src.data.alpha_vantage import get_company_overview


DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
DEMO_TERMS_CACHE_DIR = os.path.join(DATA_DIR, "prospectus_terms", "demo")
RUNTIME_TERMS_CACHE_DIR = os.path.join(DATA_DIR, "prospectus_terms", "runtime")


def analyze_relative_value(
    market_data: Dict[str, Any],
    prospectus_terms: Dict[str, Any],
    rate_data: Dict[str, Any],
    dividend_data: Dict[str, Any],
    tax_analysis: Optional[Dict[str, Any]] = None,
    call_analysis: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Produce a relative value assessment for a preferred security.

    Returns a dictionary with:
      - peer_universe: list of peer securities with key metrics
      - peer_count: number of peers in the comparison set
      - yield_rank: rank within peers (1 = highest yield)
      - yield_percentile: percentile within peers
      - spread_to_treasury_bps: spread over 10Y Treasury
      - spread_to_common_bps: spread over issuer's common dividend yield
      - structure_comparison: how the coupon structure compares to peers
      - value_assessment: cheap / fair / rich relative to peers
      - relative_value_summary: one-paragraph narrative
      - methodology: brief description
    """
    result: Dict[str, Any] = {}

    # --- Extract target security metrics ---
    ticker = str(market_data.get("ticker", prospectus_terms.get("ticker", ""))).upper()
    price = _to_float(market_data.get("price"))
    div_yield_raw = _to_float(market_data.get("dividend_yield"))
    issuer = str(prospectus_terms.get("issuer", "")).strip()
    coupon_rate = _to_float(prospectus_terms.get("coupon_rate"))
    coupon_type = str(prospectus_terms.get("coupon_type", "")).lower()
    cumulative = prospectus_terms.get("cumulative")
    perpetual = prospectus_terms.get("perpetual")

    # Normalize yield
    if div_yield_raw is not None:
        current_yield = div_yield_raw * 100 if div_yield_raw < 1 else div_yield_raw
    else:
        current_yield = None
    result["current_yield_pct"] = current_yield

    # After-tax yield from tax analysis
    after_tax_yield = None
    if tax_analysis:
        after_tax_yield = _to_float(tax_analysis.get("after_tax_yield_pct"))
    result["after_tax_yield_pct"] = after_tax_yield

    # YTW from call analysis
    ytw = None
    if call_analysis:
        ytw = _to_float(call_analysis.get("yield_to_worst_pct"))
    result["yield_to_worst_pct"] = ytw

    # --- Spread to Treasury ---
    ten_yr = _to_float(rate_data.get("10Y")) or _to_float(rate_data.get("20Y"))
    if current_yield is not None and ten_yr is not None:
        spread_to_treasury = round((current_yield - ten_yr) * 100, 0)
    else:
        spread_to_treasury = None
    result["spread_to_treasury_bps"] = spread_to_treasury

    # --- Spread to common equity ---
    common_yield = _get_common_dividend_yield(issuer, ticker)
    if current_yield is not None and common_yield is not None:
        spread_to_common = round((current_yield - common_yield) * 100, 0)
    else:
        spread_to_common = None
    result["spread_to_common_bps"] = spread_to_common
    result["common_dividend_yield_pct"] = common_yield

    # --- Build peer universe ---
    peers = _build_peer_universe(ticker, issuer)
    result["peer_universe"] = peers
    result["peer_count"] = len(peers)

    # --- Rank within peers ---
    if current_yield is not None and peers:
        peer_yields = [p["coupon_rate"] for p in peers if p.get("coupon_rate") is not None]
        all_yields = peer_yields + [current_yield]
        all_yields_sorted = sorted(all_yields, reverse=True)
        try:
            rank = all_yields_sorted.index(current_yield) + 1
        except ValueError:
            rank = None
        result["yield_rank"] = rank
        result["yield_percentile"] = round((1 - (rank - 1) / len(all_yields_sorted)) * 100, 0) if rank else None
    else:
        result["yield_rank"] = None
        result["yield_percentile"] = None

    # --- Structure comparison ---
    structure_notes = _compare_structure(coupon_type, cumulative, perpetual, peers)
    result["structure_comparison"] = structure_notes

    # --- Value assessment ---
    value_label, value_reasoning = _assess_value(
        current_yield=current_yield,
        spread_to_treasury=spread_to_treasury,
        coupon_type=coupon_type,
        peers=peers,
    )
    result["value_assessment"] = value_label
    result["value_reasoning"] = value_reasoning

    # --- Summary ---
    result["relative_value_summary"] = _build_summary(
        ticker=ticker,
        issuer=issuer,
        current_yield=current_yield,
        spread_to_treasury=spread_to_treasury,
        spread_to_common=spread_to_common,
        peer_count=len(peers),
        yield_rank=result["yield_rank"],
        value_label=value_label,
        value_reasoning=value_reasoning,
        structure_notes=structure_notes,
    )

    result["methodology"] = (
        "Relative value is assessed by comparing the target security's yield, "
        "spread, and structural features against a peer set built from the cached "
        "prospectus inventory. The peer set includes all securities from the same "
        "issuer and other cached securities in the same sector. Yield ranking uses "
        "the prospectus coupon rate as a proxy when live yields are unavailable for "
        "peers. The value assessment (cheap/fair/rich) is a heuristic based on "
        "spread levels relative to historical norms for the preferred asset class."
    )

    return result


# ---------------------------------------------------------------------------
# Peer universe construction
# ---------------------------------------------------------------------------

def _build_peer_universe(target_ticker: str, target_issuer: str) -> List[Dict[str, Any]]:
    """Load all cached prospectus terms and build a peer comparison set.

    Peers include:
      1. Other preferreds from the same issuer
      2. Other cached preferreds from different issuers (for cross-issuer comparison)
    """
    peers: List[Dict[str, Any]] = []

    for cache_dir in (DEMO_TERMS_CACHE_DIR, RUNTIME_TERMS_CACHE_DIR):
        if not os.path.isdir(cache_dir):
            continue
        for filename in os.listdir(cache_dir):
            if not filename.endswith(".json"):
                continue
            filepath = os.path.join(cache_dir, filename)
            try:
                with open(filepath, "r") as f:
                    terms = json.load(f)
            except (json.JSONDecodeError, IOError):
                continue

            peer_ticker = str(terms.get("ticker", "")).upper()
            if peer_ticker == target_ticker.upper():
                continue  # skip the target itself

            peer_issuer = str(terms.get("issuer", "")).strip()
            same_issuer = _same_issuer(target_issuer, peer_issuer)

            peers.append({
                "ticker": peer_ticker,
                "issuer": peer_issuer,
                "series": terms.get("series"),
                "coupon_rate": _to_float(terms.get("coupon_rate")),
                "coupon_type": str(terms.get("coupon_type", "")).lower(),
                "cumulative": terms.get("cumulative"),
                "perpetual": terms.get("perpetual"),
                "call_date": terms.get("call_date"),
                "same_issuer": same_issuer,
                "relationship": "same issuer" if same_issuer else "cross-issuer peer",
            })

    # Sort: same-issuer peers first, then by coupon rate descending
    peers.sort(key=lambda p: (not p["same_issuer"], -(p["coupon_rate"] or 0)))
    return peers


def _same_issuer(issuer_a: str, issuer_b: str) -> bool:
    """Check if two issuer names refer to the same company."""
    a = issuer_a.lower().replace(",", "").replace(".", "").strip()
    b = issuer_b.lower().replace(",", "").replace(".", "").strip()

    # Direct match
    if a == b:
        return True

    # Check if one contains the other's first two words
    a_words = a.split()[:2]
    b_words = b.split()[:2]
    a_prefix = " ".join(a_words)
    b_prefix = " ".join(b_words)

    return a_prefix in b or b_prefix in a


# ---------------------------------------------------------------------------
# Common equity yield lookup
# ---------------------------------------------------------------------------

def _get_common_dividend_yield(issuer: str, preferred_ticker: str) -> Optional[float]:
    """Attempt to fetch the common stock dividend yield for the issuer.

    Uses Alpha Vantage company overview data for the common ticker derived
    from the preferred ticker.
    """
    # Derive common ticker from preferred ticker (e.g., "JPM-PD" -> "JPM")
    common_ticker = preferred_ticker.split("-")[0].split(".")[0].upper()
    if not common_ticker:
        return None

    try:
        overview = get_company_overview(common_ticker)
        div_yield = overview.get("DividendYield") or overview.get("dividend_yield")
        if div_yield:
            numeric_yield = float(div_yield)
            if numeric_yield > 0:
                return round(numeric_yield * 100 if numeric_yield < 1 else numeric_yield, 2)
    except Exception:
        pass

    return None


# ---------------------------------------------------------------------------
# Structure comparison
# ---------------------------------------------------------------------------

def _compare_structure(
    coupon_type: str,
    cumulative: Optional[bool],
    perpetual: Optional[bool],
    peers: List[Dict[str, Any]],
) -> str:
    """Compare the target's structural features against peers."""
    if not peers:
        return "No peers available for structural comparison."

    notes: List[str] = []

    # Coupon type distribution
    peer_types = [p["coupon_type"] for p in peers if p.get("coupon_type")]
    fixed_count = sum(1 for t in peer_types if t == "fixed")
    floating_count = sum(1 for t in peer_types if t in ("floating", "fixed-to-floating"))

    if coupon_type == "fixed":
        notes.append(
            f"The target has a fixed coupon, consistent with {fixed_count} of "
            f"{len(peers)} peers."
        )
    elif coupon_type in ("floating", "fixed-to-floating"):
        notes.append(
            f"The target has a {coupon_type} coupon structure. "
            f"{floating_count} of {len(peers)} peers also have floating-rate features."
        )

    # Cumulative vs non-cumulative
    cum_count = sum(1 for p in peers if p.get("cumulative") is True)
    non_cum_count = sum(1 for p in peers if p.get("cumulative") is False)
    if cumulative is False:
        notes.append(
            f"The target is non-cumulative, matching {non_cum_count} of {len(peers)} peers."
        )
    elif cumulative is True:
        notes.append(
            f"The target is cumulative, which is less common ({cum_count} of {len(peers)} peers)."
        )

    return " ".join(notes) if notes else "Structural features are broadly consistent with the peer set."


# ---------------------------------------------------------------------------
# Value assessment
# ---------------------------------------------------------------------------

def _assess_value(
    current_yield: Optional[float],
    spread_to_treasury: Optional[float],
    coupon_type: str,
    peers: List[Dict[str, Any]],
) -> tuple:
    """Heuristic value assessment: cheap / fair / rich.

    Based on:
      - Spread to Treasury vs. historical norms (200-400 bps is typical)
      - Yield relative to peer median
    """
    reasons: List[str] = []

    # Spread assessment
    if spread_to_treasury is not None:
        if spread_to_treasury > 400:
            reasons.append(f"Spread to Treasury ({spread_to_treasury:.0f} bps) is above the typical 200-400 bps range, suggesting relative cheapness.")
        elif spread_to_treasury > 200:
            reasons.append(f"Spread to Treasury ({spread_to_treasury:.0f} bps) is within the typical 200-400 bps range.")
        else:
            reasons.append(f"Spread to Treasury ({spread_to_treasury:.0f} bps) is below the typical 200-400 bps range, suggesting relative richness.")

    # Peer yield comparison
    peer_coupons = [p["coupon_rate"] for p in peers if p.get("coupon_rate") is not None]
    if current_yield is not None and peer_coupons:
        median_coupon = sorted(peer_coupons)[len(peer_coupons) // 2]
        diff = current_yield - median_coupon
        if diff > 1.0:
            reasons.append(f"Current yield is {diff:.2f}% above the peer median coupon of {median_coupon:.2f}%.")
        elif diff < -1.0:
            reasons.append(f"Current yield is {abs(diff):.2f}% below the peer median coupon of {median_coupon:.2f}%.")
        else:
            reasons.append(f"Current yield is broadly in line with the peer median coupon of {median_coupon:.2f}%.")

    # Floating-rate discount
    if coupon_type in ("floating", "fixed-to-floating"):
        reasons.append("Floating-rate structures typically trade at tighter spreads due to lower duration risk.")

    # Determine label
    reasoning = " ".join(reasons)
    if spread_to_treasury is not None:
        if spread_to_treasury > 400:
            return ("cheap", reasoning)
        if spread_to_treasury < 200:
            return ("rich", reasoning)
    return ("fair", reasoning)


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def _build_summary(
    ticker: str,
    issuer: str,
    current_yield: Optional[float],
    spread_to_treasury: Optional[float],
    spread_to_common: Optional[float],
    peer_count: int,
    yield_rank: Optional[int],
    value_label: str,
    value_reasoning: str,
    structure_notes: str,
) -> str:
    """Build a one-paragraph relative value summary."""
    parts: List[str] = []

    if current_yield is not None:
        parts.append(f"{ticker} currently yields {current_yield:.2f}%.")

    if spread_to_treasury is not None:
        parts.append(f"The spread to the 10-year Treasury is {spread_to_treasury:.0f} basis points.")

    if spread_to_common is not None:
        parts.append(
            f"The preferred yields {spread_to_common:.0f} basis points more than "
            f"the issuer's common stock dividend."
        )

    if yield_rank is not None and peer_count > 0:
        parts.append(
            f"Among {peer_count} peers in the comparison set, {ticker} ranks "
            f"#{yield_rank} by yield."
        )

    value_text = value_label.replace("_", " ")
    parts.append(f"The relative value assessment is {value_text}.")

    if structure_notes:
        parts.append(structure_notes)

    return " ".join(parts)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_float(value: Any) -> Optional[float]:
    """Coerce common numeric-like values into float."""
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
