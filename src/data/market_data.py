"""
Market data fetcher for preferred equity securities.

Alpha Vantage is the sole market-data provider. Local prospectus/cache data
remains the preferred source for structural preferred terms and dividend math.
When provider dividends are missing or noisy, the module falls back to cached
prospectus terms and, if needed, Alpha dividend history.
"""

from datetime import date
import json
import os
from typing import Optional

import pandas as pd

from src.data.alpha_vantage import get_dividends as get_alpha_vantage_dividends
from src.data.alpha_vantage import get_last_error as get_alpha_vantage_last_error
from src.data.alpha_vantage import get_quote as get_alpha_vantage_quote
from src.data.alpha_vantage import get_time_series as get_alpha_vantage_time_series
from src.data.prospectus_inventory import load_cached_terms_for_ticker
from src.data.rate_data import get_sofr_rate


DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data",
)


def _get_snapshot_data(ticker: str) -> Optional[dict]:
    """Retrieve market data from local snapshot if available."""
    try:
        snapshot_path = os.path.join(DATA_DIR, "market_snapshots.json")
        if os.path.exists(snapshot_path):
            with open(snapshot_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                market_data = data.get("market_data", {})
                if ticker in market_data:
                    snapshot = market_data[ticker]
                    return {
                        "ticker": ticker,
                        "provider": "snapshot",
                        "provider_symbol": None,
                        "name": snapshot.get("name", "Unknown"),
                        "price": snapshot.get("price"),
                        "dividend_rate": snapshot.get("dividend_rate"),
                        "dividend_yield": snapshot.get("dividend_yield"),
                        "fifty_two_week_high": None,
                        "fifty_two_week_low": None,
                        "volume": None,
                        "sector": None,
                        "industry": None,
                        "currency": snapshot.get("currency", "USD"),
                        "is_snapshot": True,
                        "as_of": snapshot.get("as_of"),
                    }
    except Exception:
        pass
    return None


def _parse_fraction_to_float(text: Optional[str]) -> Optional[float]:
    """Parse depositary-share fractions like ``1/400th`` into floats."""
    if not isinstance(text, str):
        return None
    parts = text.lower().replace("th", "").split("/")
    if len(parts) != 2:
        return None
    try:
        numerator = float(parts[0].strip())
        denominator = float(parts[1].strip())
    except ValueError:
        return None
    if denominator == 0:
        return None
    return numerator / denominator


def _load_structured_terms(ticker: str) -> dict:
    """Load the best available structured terms cache for a ticker."""
    return load_cached_terms_for_ticker(ticker)


def _load_universe_entry(ticker: str) -> dict:
    """Load the curated universe metadata for a ticker if available."""
    universe_path = os.path.join(DATA_DIR, "preferred_universe.json")
    try:
        with open(universe_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return (data.get("securities") or {}).get(ticker, {})
    except (OSError, json.JSONDecodeError):
        return {}


def _to_float(value) -> Optional[float]:
    """Convert numeric-like values to floats when possible."""
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _effective_par_per_share(terms: dict, universe_entry: dict) -> Optional[float]:
    """Normalize liquidation preference to a per-traded-share basis."""
    par_value = terms.get("par_value", universe_entry.get("par_value"))
    if par_value is None:
        return None

    effective_par = float(par_value)
    if terms.get("deposit_shares"):
        fraction = _parse_fraction_to_float(terms.get("deposit_fraction"))
        if fraction:
            effective_par = effective_par * fraction
    return effective_par


def _floating_spread_bps(terms: dict, universe_entry: dict) -> Optional[float]:
    """Return floating spread in basis points across mixed cache schemas."""
    spread = terms.get("floating_spread")
    if spread is None:
        spread = terms.get("floating_spread_bps")
    if spread is None:
        spread = universe_entry.get("floating_spread")
    if spread is None:
        spread = universe_entry.get("floating_spread_bps")
    return _to_float(spread)


def _is_currently_floating(coupon_type: Optional[str], reset_date: Optional[str]) -> bool:
    """Whether the security should be valued using its floating coupon today."""
    normalized = str(coupon_type or "").strip().lower()
    if normalized == "floating":
        return True
    if normalized != "fixed-to-floating" or not reset_date:
        return False

    try:
        return date.fromisoformat(str(reset_date)[:10]) <= date.today()
    except ValueError:
        return False


def _live_floating_benchmark_rate_pct(benchmark: Optional[str]) -> Optional[float]:
    """Resolve a live benchmark rate for supported floating-rate benchmarks."""
    label = str(benchmark or "").strip().lower()
    if not label:
        return None
    if "libor" in label or "sofr" in label:
        return get_sofr_rate()
    return None


def _derive_from_dividend_history(ticker: str, price: Optional[float]) -> dict:
    """Derive annual dividend and yield from actual provider dividend history."""
    if price is None or price <= 0:
        return {"dividend_rate": None, "dividend_yield": None, "dividend_source": None}

    df = get_dividend_history(ticker)
    if df is None or df.empty:
        return {"dividend_rate": None, "dividend_yield": None, "dividend_source": None}

    annual_dividend, dividend_yield = _compute_trailing_dividend(df["dividend"], price)
    return {
        "dividend_rate": annual_dividend,
        "dividend_yield": dividend_yield,
        "dividend_source": "provider_history" if annual_dividend is not None else None,
    }


def _derive_dividend_fields(ticker: str, price: Optional[float]) -> dict:
    """
    Derive annual dividend and current yield from cache or provider history.

    Preferred structural terms from the local prospectus cache remain the most
    reliable source. When those are unavailable, fall back to Alpha Vantage
    dividend history to estimate the current annualized dividend stream.
    """
    snapshot = _get_snapshot_data(ticker)
    if snapshot and snapshot.get("dividend_rate") is not None:
        annual_dividend = _to_float(snapshot.get("dividend_rate"))
        dividend_yield = (
            round(annual_dividend / price, 6)
            if annual_dividend and price and price > 0
            else _to_float(snapshot.get("dividend_yield"))
        )
        return {
            "security_name": snapshot.get("name"),
            "coupon_type": None,
            "dividend_rate": annual_dividend,
            "dividend_yield": dividend_yield,
            "dividend_source": "snapshot",
        }

    terms = _load_structured_terms(ticker)
    universe_entry = _load_universe_entry(ticker)

    coupon_rate = terms.get("coupon_rate", universe_entry.get("coupon_rate"))
    security_name = terms.get("security_name", universe_entry.get("security_name"))
    coupon_type = terms.get("coupon_type", universe_entry.get("coupon_type"))
    effective_par = _effective_par_per_share(terms, universe_entry)

    if effective_par is not None:
        live_coupon_pct = None
        if _is_currently_floating(coupon_type, terms.get("fixed_to_floating_date")):
            benchmark_rate_pct = _live_floating_benchmark_rate_pct(
                terms.get("floating_benchmark", universe_entry.get("floating_benchmark"))
            )
            spread_bps = _floating_spread_bps(terms, universe_entry)
            if benchmark_rate_pct is not None and spread_bps is not None:
                live_coupon_pct = round(benchmark_rate_pct + (spread_bps / 100.0), 4)

        effective_coupon_pct = live_coupon_pct
        if effective_coupon_pct is None:
            effective_coupon_pct = _to_float(coupon_rate)

        if effective_coupon_pct is not None:
            annual_dividend = round(effective_par * float(effective_coupon_pct) / 100.0, 4)
            dividend_yield = round(annual_dividend / price, 6) if price and price > 0 else None
            return {
                "security_name": security_name,
                "coupon_type": coupon_type,
                "dividend_rate": annual_dividend,
                "dividend_yield": dividend_yield,
                "effective_coupon_pct": round(float(effective_coupon_pct), 4),
                "dividend_source": "live_benchmark" if live_coupon_pct is not None else "prospectus_coupon",
            }

    history_derived = _derive_from_dividend_history(ticker, price)
    return {
        "security_name": security_name,
        "coupon_type": coupon_type,
        "dividend_rate": history_derived.get("dividend_rate"),
        "dividend_yield": history_derived.get("dividend_yield"),
        "dividend_source": history_derived.get("dividend_source"),
    }


def _get_preferred_info_from_alpha_vantage(ticker: str) -> dict:
    """Fetch preferred quote data through Alpha Vantage and enrich it locally."""
    quote = get_alpha_vantage_quote(ticker, require_preferred=True)
    quote_error = get_alpha_vantage_last_error()
    provider_symbol = None
    name = None
    volume = None
    currency = "USD"

    if quote:
        price = _to_float(quote.get("close")) or _to_float(quote.get("price"))
        provider_symbol = quote.get("_matched_symbol") or quote.get("symbol")
        name = quote.get("name")
        volume = _to_float(quote.get("volume"))
        currency = quote.get("currency", "USD")
    else:
        history = get_alpha_vantage_time_series(ticker, period="1mo", require_preferred=True)
        history_error = get_alpha_vantage_last_error()
        if history is None or history.empty:
            messages = []
            for message in (quote_error, history_error):
                if message and message not in messages:
                    messages.append(message)

            if messages:
                detail = " | ".join(messages)
                error_message = f"Alpha Vantage market data unavailable: {detail}"
            else:
                error_message = (
                    "No Alpha Vantage quote found for ticker. "
                    "Alpha symbology may require a provider_symbols.alpha_vantage override."
                )
            return {
                "ticker": ticker,
                "error": error_message,
            }
        latest_bar = history.iloc[-1]
        price = _to_float(latest_bar.get("Close"))
        volume = _to_float(latest_bar.get("Volume"))
        provider_symbol = history.attrs.get("provider_symbol")

    metadata = _load_structured_terms(ticker) or _load_universe_entry(ticker)
    dividend_fields = _derive_dividend_fields(ticker, price)

    return {
        "ticker": ticker,
        "provider": "alpha_vantage",
        "provider_symbol": provider_symbol,
        "name": name or dividend_fields.get("security_name") or metadata.get("security_name") or "Unknown",
        "price": price,
        "dividend_rate": dividend_fields.get("dividend_rate"),
        "dividend_yield": dividend_fields.get("dividend_yield"),
        "fifty_two_week_high": _to_float((quote or {}).get("fifty_two_week_high")),
        "fifty_two_week_low": _to_float((quote or {}).get("fifty_two_week_low")),
        "volume": volume,
        "sector": (quote or {}).get("sector"),
        "industry": (quote or {}).get("industry"),
        "currency": currency,
        "is_snapshot": False,
        "dividend_source": dividend_fields.get("dividend_source"),
        "price_source": "global_quote" if quote else "time_series_close",
    }


def get_preferred_info(ticker: str) -> dict:
    """
    Fetch basic information about a preferred stock from Alpha Vantage.

    Falls back to local snapshots when live quote lookup fails.
    """
    info = _get_preferred_info_from_alpha_vantage(ticker)
    if "error" not in info:
        return info

    snapshot = _get_snapshot_data(ticker)
    if snapshot:
        return snapshot
    return info


def _compute_trailing_dividend(
    dividends: pd.Series,
    price: float,
) -> tuple:
    """Compute trailing 12-month dividend rate and yield from payment history."""
    if dividends.empty or price <= 0:
        return None, None

    try:
        idx = dividends.index.tz_localize(None)
    except TypeError:
        idx = dividends.index

    now = pd.Timestamp.now()
    one_year_ago = now - pd.DateOffset(years=1)
    recent = dividends[idx >= one_year_ago]

    if len(recent) >= 2:
        annual_rate = round(float(recent.sum()), 4)
    elif len(dividends) >= 2:
        last_payments = dividends.tail(4)
        dates = last_payments.index
        if len(dates) < 2:
            return None, None

        try:
            avg_gap_days = (dates[-1] - dates[0]).days / (len(dates) - 1)
        except Exception:
            avg_gap_days = 91

        if avg_gap_days < 45:
            periods_per_year = 12
        elif avg_gap_days < 120:
            periods_per_year = 4
        elif avg_gap_days < 210:
            periods_per_year = 2
        else:
            periods_per_year = 1

        avg_payment = float(last_payments.mean())
        annual_rate = round(avg_payment * periods_per_year, 4)
    else:
        return None, None

    annual_yield = round(annual_rate / price, 6)
    return annual_rate, annual_yield


def get_price_history(ticker: str, period: str = "1y") -> Optional[pd.DataFrame]:
    """Fetch historical price data for a preferred stock from Alpha Vantage."""
    return get_alpha_vantage_time_series(ticker, period=period, require_preferred=True)


def get_dividend_history(ticker: str) -> Optional[pd.DataFrame]:
    """Fetch dividend payment history for a preferred stock from Alpha Vantage."""
    return get_alpha_vantage_dividends(ticker, require_preferred=True)


def get_market_data(ticker: str) -> dict:
    """Compatibility alias for fetching preferred security market data."""
    return get_preferred_info(ticker)
