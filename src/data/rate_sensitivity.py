"""
Interest-rate sensitivity analysis for preferred securities.

The goal is not to force every preferred into a bond-style DV01 mold. Instead,
this module chooses the most appropriate framing based on the security's
coupon structure:

- Fixed-rate preferreds: effective duration and DV01 are meaningful.
- Floating-rate preferreds: reset lag and benchmark/spread context matter more.
- Fixed-to-floating preferreds: call/reset-aware effective duration matters
  before reset, while benchmark-linked coupon behavior matters after reset.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional

from src.data.rate_data import get_sofr_rate


def analyze_interest_rate_sensitivity(
    market_data: Dict[str, Any],
    rate_data: Dict[str, Any],
    prospectus_terms: Dict[str, Any],
    dividend_data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a dynamic rate-sensitivity view for a preferred security."""
    dividend_data = dividend_data or {}

    ticker = market_data.get("ticker") or prospectus_terms.get("ticker") or ""
    price = _to_float(market_data.get("price"))
    treasury_anchor = _first_available(rate_data, ["10Y", "20Y", "5Y", "2Y"])
    coupon_type = _determine_coupon_type(prospectus_terms, dividend_data)
    coupon_rate = _to_float(prospectus_terms.get("coupon_rate"))
    dividend_frequency = (
        prospectus_terms.get("dividend_frequency")
        or dividend_data.get("frequency")
        or "quarterly"
    )
    current_yield = _preferred_current_yield_pct(market_data, prospectus_terms, price)
    years_to_call = _years_until(prospectus_terms.get("call_date"))
    years_to_reset = _years_until(prospectus_terms.get("fixed_to_floating_date"))
    benchmark_context = _resolve_benchmark_context(
        prospectus_terms.get("floating_benchmark"),
        rate_data,
    )
    if years_to_call is not None:
        years_to_call = max(0.0, years_to_call)
    if years_to_reset is not None:
        years_to_reset = max(0.0, years_to_reset)
    comparison_anchor = _normalized_prospectus_amount(
        prospectus_terms.get("call_price"), prospectus_terms
    ) or _normalized_prospectus_amount(
        prospectus_terms.get("par_value"), prospectus_terms
    )
    premium_to_anchor = (
        round(price - comparison_anchor, 2)
        if price is not None and comparison_anchor is not None
        else None
    )

    if price is None:
        return {
            "ticker": ticker,
            "error": "Missing market price for interest-rate sensitivity analysis",
            "confidence": "low",
        }

    analysis: Dict[str, Any] = {
        "ticker": ticker,
        "coupon_type": coupon_type,
        "coupon_rate": coupon_rate,
        "current_price": price,
        "current_yield_pct": current_yield,
        "treasury_anchor_pct": treasury_anchor,
        "benchmark": prospectus_terms.get("floating_benchmark"),
        "contractual_benchmark": benchmark_context.get("contractual_benchmark"),
        "live_benchmark_label": benchmark_context.get("live_benchmark_label"),
        "benchmark_replacement_method": benchmark_context.get("benchmark_replacement_method"),
        "benchmark_rate_pct": benchmark_context.get("benchmark_rate_pct"),
        "floating_spread_bps": _floating_spread_bps_from_terms(prospectus_terms),
        "all_in_floating_coupon_pct": None,
        "projected_post_reset_coupon_pct": None,
        "is_benchmark_replacement_estimate": benchmark_context.get(
            "is_benchmark_replacement_estimate",
            False,
        ),
        "benchmark_note": benchmark_context.get("benchmark_note"),
        "call_date": prospectus_terms.get("call_date"),
        "years_to_call": years_to_call,
        "reset_date": prospectus_terms.get("fixed_to_floating_date"),
        "years_to_reset": years_to_reset,
        "next_reset_tenor_years": _reset_period_years(dividend_frequency),
        "comparison_anchor_price": comparison_anchor,
        "premium_to_anchor": premium_to_anchor,
        "dividend_frequency": dividend_frequency,
        "regime": None,
        "primary_measure": None,
        "primary_value": None,
        "effective_duration": None,
        "effective_dv01_per_share": None,
        "effective_dv01_per_1000_market_value": None,
        "scenario_table": [],
        "scenario_table_type": "price_duration",
        "rate_risk_level": None,
        "confidence": "medium",
        "summary": "",
        "methodology": "",
    }

    analysis["all_in_floating_coupon_pct"] = _all_in_floating_coupon_pct(
        analysis.get("benchmark_rate_pct"),
        analysis.get("floating_spread_bps"),
    )

    if coupon_type == "floating":
        analysis.update(_analyze_floating_security(analysis))
    elif coupon_type == "fixed-to-floating":
        analysis.update(_analyze_fixed_to_floating_security(analysis))
    else:
        analysis.update(_analyze_fixed_security(analysis, prospectus_terms))

    if not analysis.get("scenario_table"):
        analysis["scenario_table"] = _duration_scenario_table(
            price=price,
            effective_duration=analysis.get("effective_duration"),
        )
    analysis["summary"] = _build_summary(analysis)
    return analysis


def _floating_spread_bps_from_terms(prospectus_terms: Dict[str, Any]) -> Optional[float]:
    """Return floating spread in basis points across mixed prospectus schemas."""
    spread = prospectus_terms.get("floating_spread")
    if spread is None:
        spread = prospectus_terms.get("floating_spread_bps")
    return _to_float(spread)


def _analyze_fixed_security(
    analysis: Dict[str, Any],
    prospectus_terms: Dict[str, Any],
) -> Dict[str, Any]:
    """Rate framing for fixed-rate preferreds."""
    current_yield = analysis.get("current_yield_pct")
    if current_yield is None or current_yield <= 0:
        current_yield = analysis.get("coupon_rate") or 5.0

    base_duration = max(1.0, min(25.0, 100.0 / current_yield))
    effective_duration = _call_adjusted_duration(
        base_duration=base_duration,
        years_to_call=analysis.get("years_to_call"),
        premium_to_anchor=analysis.get("premium_to_anchor"),
    )
    dv01_per_share = round(analysis["current_price"] * effective_duration * 0.0001, 4)
    dv01_per_1000 = round(effective_duration * 0.1, 3)

    methodology = "Call-adjusted effective duration and DV01 are appropriate for fixed-rate preferreds."
    if prospectus_terms.get("perpetual") is True:
        methodology += " Duration is estimated using perpetual preferred heuristics and clipped for call risk."

    return {
        "regime": "fixed_rate",
        "primary_measure": "Effective Duration",
        "primary_value": round(effective_duration, 2),
        "effective_duration": round(effective_duration, 2),
        "effective_dv01_per_share": dv01_per_share,
        "effective_dv01_per_1000_market_value": dv01_per_1000,
        "rate_risk_level": _risk_bucket(effective_duration),
        "confidence": "high" if analysis.get("coupon_rate") is not None else "medium",
        "methodology": methodology,
    }


def _analyze_floating_security(analysis: Dict[str, Any]) -> Dict[str, Any]:
    """Rate framing for floating-rate preferreds."""
    reset_period_years = _reset_period_years(analysis.get("dividend_frequency"))
    effective_duration = min(
        reset_period_years,
        analysis.get("years_to_call") if analysis.get("years_to_call") else reset_period_years,
    )
    dv01_per_share = round(analysis["current_price"] * effective_duration * 0.0001, 4)
    dv01_per_1000 = round(effective_duration * 0.1, 3)
    all_in_coupon = analysis.get("all_in_floating_coupon_pct")
    live_benchmark_label = analysis.get("live_benchmark_label")
    methodology = (
        "Floating-rate preferreds are best framed through their reference-rate linkage. "
        "Duration and DV01 are secondary because the coupon resets with the benchmark."
    )
    confidence = "medium"
    if analysis.get("benchmark_rate_pct") is None:
        confidence = "low"
        methodology += " Benchmark data was unavailable, so the floating coupon estimate is omitted."
    elif "fallback" in str(analysis.get("benchmark_replacement_method", "")).lower():
        confidence = "low"
        methodology += " SOFR data was unavailable, so a Treasury proxy fallback was used."

    primary_measure = "All-In Floating Coupon" if all_in_coupon is not None else "Reset Period"
    primary_value = round(all_in_coupon, 2) if all_in_coupon is not None else round(reset_period_years, 2)

    return {
        "regime": "floating_rate",
        "primary_measure": primary_measure,
        "primary_value": primary_value,
        "effective_duration": round(effective_duration, 2),
        "effective_dv01_per_share": dv01_per_share,
        "effective_dv01_per_1000_market_value": dv01_per_1000,
        "rate_risk_level": "low",
        "confidence": confidence,
        "scenario_table_type": "benchmark_coupon" if all_in_coupon is not None else "price_duration",
        "scenario_table": _benchmark_coupon_scenario_table(
            base_benchmark_rate_pct=analysis.get("benchmark_rate_pct"),
            floating_spread_bps=analysis.get("floating_spread_bps"),
            benchmark_label=live_benchmark_label,
        ),
        "methodology": methodology,
    }


def _analyze_fixed_to_floating_security(analysis: Dict[str, Any]) -> Dict[str, Any]:
    """Rate framing for fixed-to-floating preferreds."""
    current_yield = analysis.get("current_yield_pct")
    if current_yield is None or current_yield <= 0:
        current_yield = analysis.get("coupon_rate") or 5.0

    fixed_base_duration = max(1.0, min(20.0, 100.0 / current_yield))
    years_to_reset = analysis.get("years_to_reset")
    projected_post_reset_coupon = analysis.get("all_in_floating_coupon_pct")
    reset_period_years = _reset_period_years(analysis.get("dividend_frequency"))

    if years_to_reset is None:
        effective_duration = fixed_base_duration * 0.5
        confidence = "low"
        primary_measure = "Time To Reset"
        primary_value = None
        methodology = (
            "Use call/reset-aware effective duration until the floating reset date. "
            "Reset date was not available, so the estimate is lower confidence."
        )
        scenario_table_type = "price_duration"
        scenario_table: List[Dict[str, Any]] = []
    elif years_to_reset == 0:
        effective_duration = min(
            reset_period_years,
            analysis.get("years_to_call") if analysis.get("years_to_call") else reset_period_years,
        )
        confidence = "medium"
        primary_measure = (
            "All-In Floating Coupon"
            if projected_post_reset_coupon is not None
            else "Post-Reset Duration Proxy"
        )
        primary_value = (
            round(projected_post_reset_coupon, 2)
            if projected_post_reset_coupon is not None
            else round(effective_duration, 2)
        )
        methodology = (
            "This preferred appears to be in its floating-rate regime already, "
            "so rate sensitivity is framed around its live benchmark plus spread."
        )
        if "fallback" in str(analysis.get("benchmark_replacement_method", "")).lower():
            confidence = "low"
            methodology += " SOFR data was unavailable, so a Treasury proxy fallback was used."
        scenario_table_type = (
            "benchmark_coupon" if projected_post_reset_coupon is not None else "price_duration"
        )
        scenario_table = _benchmark_coupon_scenario_table(
            base_benchmark_rate_pct=analysis.get("benchmark_rate_pct"),
            floating_spread_bps=analysis.get("floating_spread_bps"),
            benchmark_label=analysis.get("live_benchmark_label"),
        )
    elif years_to_reset <= 1:
        effective_duration = max(0.25, min(1.0, years_to_reset * 0.8))
        confidence = "medium"
        primary_measure = "Time To Reset"
        primary_value = round(years_to_reset, 2)
        methodology = (
            "Use call/reset-aware effective duration until the floating reset date. "
            "Treasury DV01 falls as the reset date approaches."
        )
        if projected_post_reset_coupon is not None:
            methodology += (
                " A projected post-reset coupon estimate is included using the live benchmark plus spread."
            )
        scenario_table_type = "price_duration"
        scenario_table = []
    else:
        effective_duration = min(fixed_base_duration, years_to_reset * 0.9)
        confidence = "high"
        primary_measure = "Time To Reset"
        primary_value = round(years_to_reset, 2)
        methodology = (
            "Use call/reset-aware effective duration until the floating reset date. "
            "Treasury DV01 falls as the reset date approaches."
        )
        if projected_post_reset_coupon is not None:
            methodology += (
                " A projected post-reset coupon estimate is included using the live benchmark plus spread."
            )
        scenario_table_type = "price_duration"
        scenario_table = []

    effective_duration = _call_adjusted_duration(
        base_duration=effective_duration,
        years_to_call=analysis.get("years_to_call"),
        premium_to_anchor=analysis.get("premium_to_anchor"),
    )
    dv01_per_share = round(analysis["current_price"] * effective_duration * 0.0001, 4)
    dv01_per_1000 = round(effective_duration * 0.1, 3)

    return {
        "regime": "fixed_to_floating",
        "primary_measure": primary_measure,
        "primary_value": primary_value,
        "effective_duration": round(effective_duration, 2),
        "effective_dv01_per_share": dv01_per_share,
        "effective_dv01_per_1000_market_value": dv01_per_1000,
        "rate_risk_level": _risk_bucket(effective_duration),
        "confidence": confidence,
        "projected_post_reset_coupon_pct": (
            round(projected_post_reset_coupon, 2)
            if projected_post_reset_coupon is not None and years_to_reset not in (None, 0)
            else analysis.get("projected_post_reset_coupon_pct")
        ),
        "scenario_table_type": scenario_table_type,
        "scenario_table": scenario_table,
        "methodology": methodology,
    }


def _build_summary(analysis: Dict[str, Any]) -> str:
    """Human-readable one-line rate summary for the UI and synthesis prompt."""
    regime = {
        "fixed_rate": "Fixed-rate preferred",
        "floating_rate": "Floating-rate preferred",
        "fixed_to_floating": "Fixed-to-floating preferred",
    }.get(analysis.get("regime"), "Preferred security")

    parts = [regime]

    if analysis.get("primary_measure") and analysis.get("primary_value") is not None:
        primary_measure = str(analysis["primary_measure"])
        if "Duration" in primary_measure or "Reset" in primary_measure:
            unit = " yrs"
        elif "Coupon" in primary_measure:
            unit = "%"
        else:
            unit = ""
        parts.append(f"{analysis['primary_measure']}: {analysis['primary_value']}{unit}")

    benchmark_label = analysis.get("live_benchmark_label")
    spread_bps = analysis.get("floating_spread_bps")
    if benchmark_label and spread_bps is not None and analysis.get("regime") in {
        "floating_rate",
        "fixed_to_floating",
    }:
        parts.append(f"Live benchmark: {benchmark_label} + {int(round(spread_bps))} bps")

    dv01 = analysis.get("effective_dv01_per_share")
    if dv01 is not None:
        parts.append(f"DV01/share: ${dv01:.4f}")

    risk_level = analysis.get("rate_risk_level")
    if risk_level:
        parts.append(f"Rate risk: {risk_level}")

    if analysis.get("is_benchmark_replacement_estimate"):
        parts.append("Benchmark replacement estimate")

    return " | ".join(parts)


def _duration_scenario_table(price: float, effective_duration: Optional[float]) -> List[Dict[str, Any]]:
    """Simple parallel-shift scenario table using effective duration."""
    if price is None or effective_duration is None:
        return []

    scenarios: List[Dict[str, Any]] = []
    for shock_bps in (-100, -50, 50, 100):
        price_change = -effective_duration * price * (shock_bps / 10000)
        shocked_price = round(price + price_change, 2)
        scenarios.append({
            "shock_bps": shock_bps,
            "estimated_price_change": round(price_change, 2),
            "estimated_price": shocked_price,
        })
    return scenarios


def _determine_coupon_type(
    prospectus_terms: Dict[str, Any],
    dividend_data: Dict[str, Any],
) -> str:
    """Prefer prospectus coupon type, with dividend-pattern fallback."""
    coupon_type = prospectus_terms.get("coupon_type")
    if isinstance(coupon_type, str) and coupon_type.strip():
        return coupon_type.strip().lower()

    is_fixed_rate = dividend_data.get("is_fixed_rate")
    if is_fixed_rate is True:
        return "fixed"
    if is_fixed_rate is False:
        return "floating"
    return "fixed"


def _preferred_current_yield_pct(
    market_data: Dict[str, Any],
    prospectus_terms: Dict[str, Any],
    price: Optional[float],
) -> Optional[float]:
    """Use market yield when available, otherwise infer from coupon and price."""
    raw_yield = _to_float(market_data.get("dividend_yield"))
    if raw_yield is not None:
        return raw_yield * 100 if raw_yield < 1 else raw_yield

    coupon_rate = _to_float(prospectus_terms.get("coupon_rate"))
    if price is None or coupon_rate is None:
        return None

    par_per_share = _normalized_prospectus_amount(prospectus_terms.get("par_value"), prospectus_terms)
    if par_per_share is None:
        par_per_share = 25.0

    annual_coupon_cash = par_per_share * (coupon_rate / 100.0)
    if price <= 0:
        return None

    return round((annual_coupon_cash / price) * 100, 2)


def _resolve_benchmark_context(
    benchmark: Optional[str],
    rate_data: Dict[str, Any],
) -> Dict[str, Any]:
    """Resolve contractual vs live benchmark context for floating coupons.

    The function is tenor-aware: it extracts the tenor from the contractual
    benchmark label (e.g. "3-month LIBOR" maps to "3M SOFR", "1-month LIBOR"
    maps to "1M SOFR") so that the replacement label accurately reflects the
    original reset frequency.  When SOFR is unavailable, the fallback chain
    looks for the matching short Treasury tenor first.
    """
    if not benchmark:
        return {
            "contractual_benchmark": None,
            "live_benchmark_label": None,
            "benchmark_replacement_method": None,
            "benchmark_rate_pct": None,
            "is_benchmark_replacement_estimate": False,
            "benchmark_note": None,
        }

    label = str(benchmark).strip()
    normalized = label.lower()
    sofr = get_sofr_rate()
    tenor_label = _extract_tenor_label(normalized)

    if "libor" in normalized:
        if sofr is not None:
            return {
                "contractual_benchmark": label,
                "live_benchmark_label": f"{tenor_label} SOFR",
                "benchmark_replacement_method": "SOFR proxy",
                "benchmark_rate_pct": sofr,
                "is_benchmark_replacement_estimate": True,
                "benchmark_note": (
                    f"Legacy LIBOR-linked terms ({label}) are modeled with a live "
                    f"{tenor_label} SOFR proxy because LIBOR is no longer published."
                ),
            }
        fallback = _first_available(rate_data, ["3M", "1M"])
        return {
            "contractual_benchmark": label,
            "live_benchmark_label": f"{tenor_label} Treasury proxy",
            "benchmark_replacement_method": "SOFR unavailable, Treasury proxy fallback",
            "benchmark_rate_pct": fallback,
            "is_benchmark_replacement_estimate": True,
            "benchmark_note": (
                "SOFR data was unavailable, so the floating benchmark estimate fell "
                "back to a short Treasury proxy."
            ),
        }

    if "sofr" in normalized:
        sofr_label = label if label else f"{tenor_label} SOFR"
        if sofr is not None:
            return {
                "contractual_benchmark": label,
                "live_benchmark_label": sofr_label,
                "benchmark_replacement_method": "Contractual benchmark",
                "benchmark_rate_pct": sofr,
                "is_benchmark_replacement_estimate": False,
                "benchmark_note": None,
            }
        fallback = _first_available(rate_data, ["3M", "1M"])
        return {
            "contractual_benchmark": label,
            "live_benchmark_label": f"{tenor_label} SOFR proxy fallback",
            "benchmark_replacement_method": "SOFR unavailable, Treasury proxy fallback",
            "benchmark_rate_pct": fallback,
            "is_benchmark_replacement_estimate": True,
            "benchmark_note": (
                "SOFR data was unavailable, so the live benchmark estimate fell back "
                "to a short Treasury proxy."
            ),
        }

    # Unknown benchmark family (e.g. Prime, Fed Funds)
    return {
        "contractual_benchmark": label,
        "live_benchmark_label": label,
        "benchmark_replacement_method": "Contractual benchmark (no live rate mapped)",
        "benchmark_rate_pct": None,
        "is_benchmark_replacement_estimate": False,
        "benchmark_note": (
            f"The contractual benchmark ({label}) is not in a recognized family "
            "(LIBOR, SOFR), so no live rate substitution was applied."
        ),
    }


def _all_in_floating_coupon_pct(
    benchmark_rate_pct: Optional[float],
    floating_spread_bps: Optional[float],
) -> Optional[float]:
    """Compute an all-in floating coupon from benchmark plus spread."""
    if benchmark_rate_pct is None or floating_spread_bps is None:
        return None
    return round(benchmark_rate_pct + (floating_spread_bps / 100.0), 2)


def _benchmark_coupon_scenario_table(
    base_benchmark_rate_pct: Optional[float],
    floating_spread_bps: Optional[float],
    benchmark_label: Optional[str],
) -> List[Dict[str, Any]]:
    """Scenario table for floating coupons under benchmark shocks."""
    if base_benchmark_rate_pct is None or floating_spread_bps is None:
        return []

    scenarios: List[Dict[str, Any]] = []
    for shock_bps in (-100, -50, 50, 100):
        shocked_benchmark = round(base_benchmark_rate_pct + (shock_bps / 100.0), 2)
        all_in_coupon = round(shocked_benchmark + (floating_spread_bps / 100.0), 2)
        scenarios.append(
            {
                "shock_bps": shock_bps,
                "scenario_type": "benchmark_coupon",
                "benchmark_label": benchmark_label or "Benchmark",
                "benchmark_rate_pct": shocked_benchmark,
                "all_in_coupon_pct": all_in_coupon,
            }
        )
    return scenarios


def _call_adjusted_duration(
    base_duration: float,
    years_to_call: Optional[float],
    premium_to_anchor: Optional[float],
) -> float:
    """Clip duration when the security is callable and trading above its likely call anchor."""
    effective_duration = base_duration

    if years_to_call is not None and years_to_call > 0:
        if premium_to_anchor is not None and premium_to_anchor > 0:
            effective_duration = min(base_duration, max(0.25, years_to_call * 0.9))
        elif years_to_call < 2:
            effective_duration = min(base_duration, max(0.5, years_to_call * 1.1))

    return max(0.25, effective_duration)


def _reset_period_years(dividend_frequency: Optional[str]) -> float:
    """Approximate coupon reset interval from dividend frequency."""
    if dividend_frequency == "monthly":
        return 1 / 12
    if dividend_frequency == "semi-annual":
        return 0.5
    if dividend_frequency == "annual":
        return 1.0
    return 0.25


def _risk_bucket(effective_duration: Optional[float]) -> str:
    """Simple label for rate-risk communication in the UI."""
    if effective_duration is None:
        return "unknown"
    if effective_duration < 1:
        return "low"
    if effective_duration < 4:
        return "moderate"
    if effective_duration < 8:
        return "elevated"
    return "high"


def _years_until(date_value: Any) -> Optional[float]:
    """Convert a YYYY-MM-DD-like date into fractional years from today."""
    parsed = _parse_date(date_value)
    if parsed is None:
        return None

    today = date.today()
    delta_days = (parsed - today).days
    return round(delta_days / 365.25, 2)


def _parse_date(value: Any) -> Optional[date]:
    """Parse YYYY-MM-DD strings into date objects."""
    if not value:
        return None

    if isinstance(value, date):
        return value

    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except ValueError:
        return None


def _normalized_prospectus_amount(amount: Any, prospectus_terms: Dict[str, Any]) -> Optional[float]:
    """Convert underlying preferred amounts to per-depositary-share equivalents."""
    base_amount = _to_float(amount)
    if base_amount is None:
        return None

    deposit_fraction = prospectus_terms.get("deposit_fraction")
    if prospectus_terms.get("deposit_shares") and isinstance(deposit_fraction, str):
        numerator, denominator = _parse_fraction(deposit_fraction)
        if numerator is not None and denominator:
            return round(base_amount * (numerator / denominator), 4)

    return round(base_amount, 4)


def _parse_fraction(text: str) -> tuple[Optional[int], Optional[int]]:
    """Parse strings like 1/400th into integer numerator/denominator."""
    import re

    match = re.search(r"(\d+)\s*/\s*(\d+)", text)
    if not match:
        return None, None
    return int(match.group(1)), int(match.group(2))


def _extract_tenor_label(normalized_benchmark: str) -> str:
    """Extract a short tenor label (e.g. '3M', '1M', '6M') from a benchmark string.

    Examples:
        '3-month libor'  -> '3M'
        '1 month sofr'   -> '1M'
        '6m libor'       -> '6M'
        'libor'          -> '3M'  (default assumption)
    """
    import re

    match = re.search(r"(\d+)\s*[-\s]?(?:month|mo|m)\b", normalized_benchmark)
    if match:
        months = int(match.group(1))
        return f"{months}M"

    # Default to 3M when no tenor is specified, as 3-month was the most common
    # LIBOR and SOFR reset frequency for preferred securities.
    return "3M"


def _first_available(rate_data: Dict[str, Any], keys: List[str]) -> Optional[float]:
    """Return the first numeric rate available from a list of maturity keys."""
    for key in keys:
        value = _to_float(rate_data.get(key))
        if value is not None:
            return value
    return None


def _to_float(value: Any) -> Optional[float]:
    """Coerce common numeric-like values into float."""
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
