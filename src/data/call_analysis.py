"""
Call Probability Agent for Preferred Equity Analysis.

Computes yield-to-call (YTC), yield-to-worst (YTW), and an estimated call
probability based on the relationship between the security's current yield,
prevailing new-issuance yields, and the time remaining until the first call
date.

Key concepts:
  - Yield-to-call: the annualized return if the issuer redeems the security
    at the first call date and call price.
  - Yield-to-worst: the lower of yield-to-call and current yield (or yield
    to maturity when a maturity date exists).
  - Call probability: a heuristic estimate based on how far the security
    trades above its call price, the issuer's likely refinancing incentive,
    and the time horizon.

All calculations use per-depositary-share equivalents when the security is
issued as depositary shares.
"""

from datetime import date, datetime
from typing import Any, Dict, List, Optional


def analyze_call_probability(
    market_data: Dict[str, Any],
    prospectus_terms: Dict[str, Any],
    rate_data: Dict[str, Any],
    rate_sensitivity: Dict[str, Any],
) -> Dict[str, Any]:
    """Produce a call analysis for a preferred security.

    Returns a dictionary with:
      - yield_to_call_pct: annualized YTC if callable, else None
      - yield_to_worst_pct: min(YTC, current_yield)
      - call_probability: qualitative label (high / moderate / low / not_callable)
      - call_probability_score: numeric 0-1 estimate
      - years_to_call: fractional years until first call date
      - call_price_per_share: per-depositary-share call price
      - premium_to_call_pct: how far above/below call price the security trades
      - refinancing_incentive: qualitative label
      - call_analysis_summary: one-paragraph narrative
      - methodology: brief description of the approach
    """
    result: Dict[str, Any] = {}

    # --- Extract inputs ---
    price = _to_float(market_data.get("price"))
    div_yield_raw = _to_float(market_data.get("dividend_yield"))
    annual_dividend = _to_float(market_data.get("dividend_rate"))

    coupon_rate = _to_float(prospectus_terms.get("coupon_rate"))
    coupon_type = str(prospectus_terms.get("coupon_type", "")).lower()
    call_date_str = prospectus_terms.get("call_date")
    call_price_raw = _to_float(prospectus_terms.get("call_price"))
    par_value_raw = _to_float(prospectus_terms.get("par_value"))
    perpetual = prospectus_terms.get("perpetual", True)

    # Normalize to per-depositary-share amounts
    call_price = _normalize_amount(call_price_raw, prospectus_terms)
    par_value = _normalize_amount(par_value_raw, prospectus_terms)

    # Use call price as the redemption anchor; fall back to par
    redemption_price = call_price or par_value

    # Current yield (normalize from decimal to percent if needed)
    if div_yield_raw is not None:
        current_yield_pct = div_yield_raw * 100 if div_yield_raw < 1 else div_yield_raw
    else:
        current_yield_pct = None

    # All-in floating coupon from rate sensitivity (for floating/post-reset securities)
    floating_coupon = _to_float(
        rate_sensitivity.get("all_in_floating_coupon_pct")
        or rate_sensitivity.get("projected_post_reset_coupon_pct")
    )

    # --- Call date analysis ---
    years_to_call = _years_until(call_date_str)
    call_is_past = years_to_call is not None and years_to_call <= 0
    call_is_future = years_to_call is not None and years_to_call > 0

    result["call_date"] = call_date_str
    result["years_to_call"] = years_to_call
    result["call_price_per_share"] = redemption_price
    result["par_value_per_share"] = par_value
    result["coupon_type"] = coupon_type

    # --- Not callable ---
    if call_date_str is None and call_price is None:
        result.update({
            "yield_to_call_pct": None,
            "yield_to_worst_pct": current_yield_pct,
            "call_probability": "not_callable",
            "call_probability_score": 0.0,
            "premium_to_call_pct": None,
            "refinancing_incentive": "none",
            "call_analysis_summary": (
                "This security does not have a call feature in its prospectus terms. "
                "The yield-to-worst equals the current yield."
            ),
            "methodology": "No call date or call price found in prospectus terms.",
        })
        return result

    # --- Premium / discount to call price ---
    if price is not None and redemption_price is not None and redemption_price > 0:
        premium_to_call_pct = round(((price - redemption_price) / redemption_price) * 100, 2)
    else:
        premium_to_call_pct = None
    result["premium_to_call_pct"] = premium_to_call_pct

    # --- Yield to call ---
    ytc = _compute_ytc(price, redemption_price, annual_dividend, years_to_call)
    result["yield_to_call_pct"] = ytc

    # --- Yield to worst ---
    ytw_candidates = [v for v in (ytc, current_yield_pct) if v is not None]
    result["yield_to_worst_pct"] = round(min(ytw_candidates), 2) if ytw_candidates else None

    # --- New-issuance yield proxy ---
    # Use the 10Y or 20Y Treasury + a typical preferred spread (250-350 bps)
    # as a rough proxy for where the issuer could refinance today.
    ten_yr = _to_float(rate_data.get("10Y")) or _to_float(rate_data.get("20Y"))
    new_issue_proxy = round(ten_yr + 3.0, 2) if ten_yr is not None else None

    # --- Refinancing incentive ---
    refinancing_incentive = _assess_refinancing_incentive(
        coupon_rate=coupon_rate,
        floating_coupon=floating_coupon,
        coupon_type=coupon_type,
        new_issue_proxy=new_issue_proxy,
    )
    result["refinancing_incentive"] = refinancing_incentive
    result["new_issue_yield_proxy"] = new_issue_proxy

    # --- Call probability estimate ---
    prob_score, prob_label = _estimate_call_probability(
        years_to_call=years_to_call,
        call_is_past=call_is_past,
        premium_to_call_pct=premium_to_call_pct,
        refinancing_incentive=refinancing_incentive,
        coupon_type=coupon_type,
    )
    result["call_probability_score"] = prob_score
    result["call_probability"] = prob_label

    # --- Summary narrative ---
    result["call_analysis_summary"] = _build_summary(
        years_to_call=years_to_call,
        call_is_past=call_is_past,
        premium_to_call_pct=premium_to_call_pct,
        refinancing_incentive=refinancing_incentive,
        prob_label=prob_label,
        ytc=ytc,
        ytw=result["yield_to_worst_pct"],
        coupon_rate=coupon_rate,
        floating_coupon=floating_coupon,
        coupon_type=coupon_type,
        new_issue_proxy=new_issue_proxy,
        redemption_price=redemption_price,
    )

    result["methodology"] = (
        "YTC is computed using an iterative Newton approximation of the internal rate "
        "of return assuming redemption at the call price on the first call date. "
        "Call probability is a heuristic combining the premium/discount to call price, "
        "the issuer's refinancing incentive (current coupon vs. estimated new-issuance "
        "yield), and the time to call. This is not a model-implied probability."
    )

    return result


# ---------------------------------------------------------------------------
# Yield-to-call calculation
# ---------------------------------------------------------------------------

def _compute_ytc(
    price: Optional[float],
    redemption_price: Optional[float],
    annual_dividend: Optional[float],
    years_to_call: Optional[float],
) -> Optional[float]:
    """Compute annualized yield-to-call using Newton's method.

    The YTC solves for r in:
        price = sum(coupon / (1+r)^t for t in periods) + redemption / (1+r)^n

    where periods are quarterly and n is the total number of quarters.
    """
    if any(v is None for v in (price, redemption_price, annual_dividend, years_to_call)):
        return None
    if price <= 0 or years_to_call <= 0:
        return None

    quarterly_coupon = annual_dividend / 4.0
    n_quarters = max(1, round(years_to_call * 4))

    # Newton's method on the PV equation
    r = 0.05 / 4  # initial guess: 5% annualized -> ~1.25% per quarter
    for _ in range(200):
        pv = 0.0
        dpv = 0.0
        for t in range(1, n_quarters + 1):
            discount = (1 + r) ** t
            pv += quarterly_coupon / discount
            dpv -= t * quarterly_coupon / ((1 + r) ** (t + 1))

        pv += redemption_price / ((1 + r) ** n_quarters)
        dpv -= n_quarters * redemption_price / ((1 + r) ** (n_quarters + 1))

        f = pv - price
        if abs(f) < 1e-8:
            break
        if abs(dpv) < 1e-12:
            break
        r = r - f / dpv

    annualized = ((1 + r) ** 4 - 1) * 100
    if annualized < -50 or annualized > 100:
        return None  # nonsensical result
    return round(annualized, 2)


# ---------------------------------------------------------------------------
# Refinancing incentive
# ---------------------------------------------------------------------------

def _assess_refinancing_incentive(
    coupon_rate: Optional[float],
    floating_coupon: Optional[float],
    coupon_type: str,
    new_issue_proxy: Optional[float],
) -> str:
    """Qualitative assessment of the issuer's incentive to call and refinance.

    Compares the security's effective coupon to a proxy for where the issuer
    could issue new preferred today.
    """
    effective_coupon = floating_coupon if coupon_type in ("floating", "fixed-to-floating") and floating_coupon else coupon_rate
    if effective_coupon is None or new_issue_proxy is None:
        return "unknown"

    savings_bps = (effective_coupon - new_issue_proxy) * 100
    if savings_bps > 150:
        return "strong"
    if savings_bps > 50:
        return "moderate"
    if savings_bps > -50:
        return "weak"
    return "negative"  # issuer would pay more to refinance


# ---------------------------------------------------------------------------
# Call probability heuristic
# ---------------------------------------------------------------------------

def _estimate_call_probability(
    years_to_call: Optional[float],
    call_is_past: bool,
    premium_to_call_pct: Optional[float],
    refinancing_incentive: str,
    coupon_type: str,
) -> tuple:
    """Return (score, label) for call probability.

    The heuristic layers three signals:
      1. Time to call: securities already past their call date get a boost.
      2. Premium/discount: trading near or below call price suggests the
         market expects a call.
      3. Refinancing incentive: strong incentive raises probability.
    """
    score = 0.0

    # --- Time signal ---
    if call_is_past:
        score += 0.30  # already callable
    elif years_to_call is not None:
        if years_to_call < 1:
            score += 0.25
        elif years_to_call < 3:
            score += 0.15
        elif years_to_call < 5:
            score += 0.05
        # > 5 years: no time boost

    # --- Premium/discount signal ---
    if premium_to_call_pct is not None:
        if premium_to_call_pct <= -2:
            # Trading well below call price: unlikely to be called
            score -= 0.10
        elif premium_to_call_pct <= 1:
            # Trading near or slightly below call price
            score += 0.20
        elif premium_to_call_pct <= 5:
            score += 0.10
        else:
            # Trading well above call price: market may not expect a call
            score += 0.05

    # --- Refinancing incentive signal ---
    incentive_map = {
        "strong": 0.35,
        "moderate": 0.20,
        "weak": 0.05,
        "negative": -0.15,
        "unknown": 0.0,
    }
    score += incentive_map.get(refinancing_incentive, 0.0)

    # --- Floating-rate adjustment ---
    # Floating-rate securities are less likely to be called because the issuer's
    # cost resets with the market. The incentive to refinance is lower.
    if coupon_type in ("floating",):
        score -= 0.10

    score = max(0.0, min(1.0, score))

    if score >= 0.65:
        label = "high"
    elif score >= 0.40:
        label = "moderate"
    elif score >= 0.15:
        label = "low"
    else:
        label = "very_low"

    return round(score, 2), label


# ---------------------------------------------------------------------------
# Summary narrative
# ---------------------------------------------------------------------------

def _build_summary(
    years_to_call: Optional[float],
    call_is_past: bool,
    premium_to_call_pct: Optional[float],
    refinancing_incentive: str,
    prob_label: str,
    ytc: Optional[float],
    ytw: Optional[float],
    coupon_rate: Optional[float],
    floating_coupon: Optional[float],
    coupon_type: str,
    new_issue_proxy: Optional[float],
    redemption_price: Optional[float],
) -> str:
    """Build a one-paragraph call analysis summary."""
    parts: List[str] = []

    # Call date context
    if call_is_past:
        parts.append("The security is currently callable (the first call date has passed).")
    elif years_to_call is not None:
        parts.append(f"The first call date is approximately {years_to_call:.1f} years away.")
    else:
        parts.append("No call date was identified in the prospectus.")

    # Premium/discount
    if premium_to_call_pct is not None and redemption_price is not None:
        if premium_to_call_pct > 0:
            parts.append(
                f"The security trades at a {premium_to_call_pct:.1f}% premium to its "
                f"${redemption_price:.2f} call price."
            )
        elif premium_to_call_pct < 0:
            parts.append(
                f"The security trades at a {abs(premium_to_call_pct):.1f}% discount to its "
                f"${redemption_price:.2f} call price."
            )
        else:
            parts.append(f"The security trades at its ${redemption_price:.2f} call price.")

    # Refinancing incentive
    effective_coupon = floating_coupon if coupon_type in ("floating", "fixed-to-floating") and floating_coupon else coupon_rate
    if effective_coupon is not None and new_issue_proxy is not None:
        savings = effective_coupon - new_issue_proxy
        if savings > 0:
            parts.append(
                f"The issuer's effective coupon ({effective_coupon:.2f}%) exceeds the estimated "
                f"new-issuance yield ({new_issue_proxy:.2f}%) by approximately "
                f"{savings * 100:.0f} basis points, creating a {refinancing_incentive} "
                f"refinancing incentive."
            )
        else:
            parts.append(
                f"The issuer's effective coupon ({effective_coupon:.2f}%) is below the estimated "
                f"new-issuance yield ({new_issue_proxy:.2f}%), so there is {refinancing_incentive} "
                f"refinancing incentive."
            )

    # YTC / YTW
    if ytc is not None:
        parts.append(f"Yield-to-call is estimated at {ytc:.2f}%.")
    if ytw is not None:
        parts.append(f"Yield-to-worst is {ytw:.2f}%.")

    # Probability conclusion
    prob_text = prob_label.replace("_", " ")
    parts.append(f"Overall call probability is assessed as {prob_text}.")

    return " ".join(parts)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize_amount(amount: Optional[float], prospectus_terms: Dict[str, Any]) -> Optional[float]:
    """Convert underlying preferred amounts to per-depositary-share equivalents.

    Applies the depositary share fraction (e.g., 1/1000th) to convert the
    underlying preferred share par value to the per-depositary-share amount.

    Includes a sanity check: if the resulting value is less than $1.00, the
    extracted par value is likely already stated in per-depositary-share terms
    (common for utility trust preferreds like SCE-PL where par_value=25 is
    already the depositary share par, not the underlying $2,500 share par).
    In that case, the original amount is returned without applying the fraction.
    """
    if amount is None:
        return None
    if prospectus_terms.get("deposit_shares"):
        fraction_str = prospectus_terms.get("deposit_fraction")
        if isinstance(fraction_str, str):
            import re
            match = re.search(r"(\d+)\s*/\s*(\d+)", fraction_str)
            if match:
                num, den = int(match.group(1)), int(match.group(2))
                if den > 0:
                    normalized = amount * (num / den)
                    # Sanity check: if the result is less than $1, the
                    # extracted amount is almost certainly already per-
                    # depositary-share (preferred par values are typically
                    # $25 per depositary share, never sub-dollar).
                    if normalized < 1.0 and amount >= 1.0:
                        return round(amount, 4)
                    return round(normalized, 4)
    return round(amount, 4)


def _years_until(date_value: Any) -> Optional[float]:
    """Convert a YYYY-MM-DD-like date into fractional years from today."""
    if not date_value:
        return None
    try:
        if isinstance(date_value, date):
            parsed = date_value
        else:
            parsed = datetime.strptime(str(date_value), "%Y-%m-%d").date()
        delta_days = (parsed - date.today()).days
        return round(delta_days / 365.25, 2)
    except (ValueError, TypeError):
        return None


def _to_float(value: Any) -> Optional[float]:
    """Coerce common numeric-like values into float."""
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
