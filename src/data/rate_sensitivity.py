"""
Interest-rate sensitivity analysis for preferred securities.

The goal is not to force every preferred into a bond-style DV01 mold. Instead,
this module chooses the most appropriate framing based on the security's
coupon structure:

- Fixed-rate preferreds: effective duration and DV01 are meaningful.
- Floating-rate preferreds: reset lag and benchmark/spread context matter more.
- Fixed-to-floating preferreds: call/reset-aware effective duration is most useful.
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
        "benchmark_rate_pct": _benchmark_rate(
            prospectus_terms.get("floating_benchmark"),
            rate_data,
        ),
        "floating_spread_bps": _to_float(prospectus_terms.get("floating_spread")),
        "call_date": prospectus_terms.get("call_date"),
        "years_to_call": years_to_call,
        "reset_date": prospectus_terms.get("fixed_to_floating_date"),
        "years_to_reset": years_to_reset,
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
        "rate_risk_level": None,
        "confidence": "medium",
        "summary": "",
        "methodology": "",
    }

    if coupon_type == "floating":
        analysis.update(_analyze_floating_security(analysis))
    elif coupon_type == "fixed-to-floating":
        analysis.update(_analyze_fixed_to_floating_security(analysis))
    else:
        analysis.update(_analyze_fixed_security(analysis, prospectus_terms))

    analysis["scenario_table"] = _scenario_table(
        price=price,
        effective_duration=analysis.get("effective_duration"),
    )
    analysis["summary"] = _build_summary(analysis)
    return analysis


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

    return {
        "regime": "floating_rate",
        "primary_measure": "Reset Period",
        "primary_value": round(reset_period_years, 2),
        "effective_duration": round(effective_duration, 2),
        "effective_dv01_per_share": dv01_per_share,
        "effective_dv01_per_1000_market_value": dv01_per_1000,
        "rate_risk_level": "low",
        "confidence": "medium",
        "methodology": (
            "DV01 is secondary for floating-rate preferreds. "
            "The shorter reset period and benchmark/spread linkage are more informative."
        ),
    }


def _analyze_fixed_to_floating_security(analysis: Dict[str, Any]) -> Dict[str, Any]:
    """Rate framing for fixed-to-floating preferreds."""
    current_yield = analysis.get("current_yield_pct")
    if current_yield is None or current_yield <= 0:
        current_yield = analysis.get("coupon_rate") or 5.0

    fixed_base_duration = max(1.0, min(20.0, 100.0 / current_yield))
    years_to_reset = analysis.get("years_to_reset")

    if years_to_reset is None:
        effective_duration = fixed_base_duration * 0.5
        confidence = "low"
        primary_measure = "Time To Reset"
        primary_value = None
        methodology = (
            "Use call/reset-aware effective duration until the floating reset date. "
            "Reset date was not available, so the estimate is lower confidence."
        )
    elif years_to_reset == 0:
        effective_duration = max(0.25, min(1.0, fixed_base_duration * 0.3))
        confidence = "medium"
        primary_measure = "Post-Reset Duration Proxy"
        primary_value = round(effective_duration, 2)
        methodology = (
            "This preferred appears to be in its floating-rate regime already, "
            "so rate sensitivity is framed as a short reset-based duration proxy."
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
    else:
        effective_duration = min(fixed_base_duration, years_to_reset * 0.9)
        confidence = "high"
        primary_measure = "Time To Reset"
        primary_value = round(years_to_reset, 2)
        methodology = (
            "Use call/reset-aware effective duration until the floating reset date. "
            "Treasury DV01 falls as the reset date approaches."
        )

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
        unit = " yrs" if "Duration" in str(analysis["primary_measure"]) or "Reset" in str(analysis["primary_measure"]) else ""
        parts.append(f"{analysis['primary_measure']}: {analysis['primary_value']}{unit}")

    dv01 = analysis.get("effective_dv01_per_share")
    if dv01 is not None:
        parts.append(f"DV01/share: ${dv01:.4f}")

    risk_level = analysis.get("rate_risk_level")
    if risk_level:
        parts.append(f"Rate risk: {risk_level}")

    return " | ".join(parts)


def _scenario_table(price: float, effective_duration: Optional[float]) -> List[Dict[str, Any]]:
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


def _benchmark_rate(benchmark: Optional[str], rate_data: Dict[str, Any]) -> Optional[float]:
    """Provide a current benchmark rate when one is meaningful."""
    if not benchmark:
        return None

    label = benchmark.lower()
    if "sofr" in label:
        sofr = get_sofr_rate()
        return sofr if sofr is not None else _first_available(rate_data, ["3M", "1M"])
    if "libor" in label:
        return _first_available(rate_data, ["3M", "1M"])
    return None


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
