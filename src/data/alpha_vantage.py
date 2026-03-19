"""
Lightweight Alpha Vantage client for preferred and common equity data.

This module keeps the app's canonical ``ISSUER-PX`` ticker format internal and
handles provider-specific lookup/search logic here. Alpha Vantage is used for:
  - symbol search / reference lookup
  - latest quote
  - historical price series
  - dividend history
  - common-equity overview fields when needed
"""

from __future__ import annotations

import logging
import os
import csv
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

from src.data.security_context import get_security_context
from src.utils.config import (
    ALPHA_VANTAGE_API_KEY,
    ALPHA_VANTAGE_LISTING_STATUS_PATH,
    PROJECT_ROOT,
)


logger = logging.getLogger(__name__)

_API_BASE = "https://www.alphavantage.co/query"
_DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

_REQUEST_TIMEOUT = 20
_PREFERRED_KEYWORDS = (
    "preferred",
    "depositary",
    "depository",
    "capital",
    "trust",
    "perpetual",
    "cumulative",
    "noncumulative",
    "non-cumulative",
    "series ",
    "mandatory convertible",
)

_reference_cache: Dict[Tuple[str, bool], Optional[Dict[str, Any]]] = {}
_quote_cache: Dict[str, Dict[str, Any]] = {}
_overview_cache: Dict[str, Dict[str, Any]] = {}
_dividend_cache: Dict[str, Optional[pd.DataFrame]] = {}
_listing_status_cache: Optional[Dict[str, Dict[str, Any]]] = None
_last_error_message: str = ""


def _get_api_key() -> str:
    """Read the Alpha Vantage key dynamically so env changes apply immediately."""
    return os.getenv("ALPHA_VANTAGE_API_KEY", ALPHA_VANTAGE_API_KEY)


def has_api_key() -> bool:
    """Return True when an Alpha Vantage API key is configured."""
    return bool(_get_api_key())


def _set_last_error(message: Optional[str]) -> None:
    """Persist the most recent Alpha Vantage error message for diagnostics."""
    global _last_error_message
    _last_error_message = str(message or "").strip()


def _clear_last_error() -> None:
    """Clear the last recorded Alpha Vantage error message."""
    _set_last_error("")


def get_last_error() -> str:
    """Return the most recent Alpha Vantage error message, if any."""
    return _last_error_message


def _get_listing_status_path() -> Optional[Path]:
    """Resolve the local Alpha listing-status CSV path when available."""
    env_path = os.getenv(
        "ALPHA_VANTAGE_LISTING_STATUS_PATH",
        ALPHA_VANTAGE_LISTING_STATUS_PATH,
    ).strip()
    candidates: List[Path] = []
    if env_path:
        candidates.append(Path(env_path).expanduser())

    project_root = Path(PROJECT_ROOT)
    candidates.extend(
        [
            project_root / "listing_status.csv",
            project_root.parent / "listing_status.csv",
            project_root / "data" / "listing_status.csv",
        ]
    )

    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def _load_listing_status_index() -> Dict[str, Dict[str, Any]]:
    """Load Alpha's listing-status CSV into a symbol-indexed dict."""
    global _listing_status_cache
    if _listing_status_cache is not None:
        return _listing_status_cache

    path = _get_listing_status_path()
    if path is None:
        _listing_status_cache = {}
        return _listing_status_cache

    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            _listing_status_cache = {
                str(row.get("symbol", "")).strip().upper(): row
                for row in reader
                if str(row.get("symbol", "")).strip()
            }
    except Exception:
        _listing_status_cache = {}

    return _listing_status_cache


def _load_symbol_metadata(ticker: str) -> Dict[str, Any]:
    """Return the merged local metadata for a ticker."""
    return dict(get_security_context(ticker).get("merged_entry") or {})


def _provider_symbol_override(ticker: str) -> Optional[str]:
    """Return an explicit Alpha provider override when configured locally."""
    provider_symbols = (get_security_context(ticker).get("provider_symbols") or {})
    override = provider_symbols.get("alpha_vantage")
    if isinstance(override, str) and override.strip():
        return override.strip().upper()
    return None


def _lookup_listing_status_row(ticker: str) -> Optional[Dict[str, Any]]:
    """Return an official Alpha listing-status row for the canonical ticker."""
    index = _load_listing_status_index()
    if not index:
        return None

    cleaned = ticker.strip().upper()
    if not cleaned or "-" not in cleaned:
        return None

    base, suffix = cleaned.split("-", 1)
    if suffix.startswith("P") and len(suffix) > 1:
        candidate = f"{base}-P-{suffix[1:]}"
    else:
        candidate = cleaned

    return index.get(candidate)


def _normalize_symbol(value: str) -> str:
    """Normalize a symbol for fuzzy comparison."""
    return "".join(ch for ch in str(value or "").upper() if ch.isalnum())


def _preferred_like_text(value: str) -> bool:
    """Heuristic preferred/trust/depositary security detector."""
    lowered = str(value or "").lower()
    return any(keyword in lowered for keyword in _PREFERRED_KEYWORDS)


def is_preferred_reference(row: Optional[Dict[str, Any]]) -> bool:
    """Return True when a search/reference row looks like a preferred issue."""
    if not row:
        return False
    name = row.get("name") or row.get("description") or ""
    type_label = str(row.get("type", "")).lower()
    symbol = row.get("symbol") or ""
    return (
        _preferred_like_text(name)
        or "preferred" in type_label
        or _normalize_symbol(symbol).endswith("P")
    )


def _build_local_reference(
    symbol: str,
    context: Dict[str, Any],
    source: str,
    listing_row: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a normalized reference row from local context."""
    listing_row = listing_row or {}
    return {
        "symbol": str(symbol or "").strip().upper(),
        "name": (
            listing_row.get("name")
            or context.get("security_name")
            or context.get("issuer")
            or context.get("ticker")
            or ""
        ),
        "type": "preferred stock",
        "region": listing_row.get("exchange") or "United States",
        "exchange": listing_row.get("exchange") or "",
        "source": source,
    }


def _normalize_search_match(row: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize Alpha Vantage SYMBOL_SEARCH rows into a common shape."""
    return {
        "symbol": row.get("1. symbol") or row.get("symbol") or "",
        "name": row.get("2. name") or row.get("name") or "",
        "type": row.get("3. type") or row.get("type") or "",
        "region": row.get("4. region") or row.get("region") or "",
        "market_open": row.get("5. marketOpen") or row.get("market_open") or "",
        "market_close": row.get("6. marketClose") or row.get("market_close") or "",
        "timezone": row.get("7. timezone") or row.get("timezone") or "",
        "currency": row.get("8. currency") or row.get("currency") or "",
        "match_score": row.get("9. matchScore") or row.get("match_score") or "",
    }


def _request_json(**params: Any) -> Dict[str, Any]:
    """Perform an Alpha Vantage request and return the decoded payload."""
    api_key = _get_api_key()
    if not api_key:
        return {"status": "error", "message": "ALPHA_VANTAGE_API_KEY is not configured."}

    request_params = dict(params)
    request_params["apikey"] = api_key

    try:
        response = requests.get(_API_BASE, params=request_params, timeout=_REQUEST_TIMEOUT)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            return {"status": "error", "message": "Unexpected Alpha Vantage response payload."}
        if payload.get("Error Message"):
            return {"status": "error", "message": payload["Error Message"]}
        if payload.get("Information"):
            return {"status": "error", "message": payload["Information"]}
        if payload.get("Note"):
            return {"status": "error", "message": payload["Note"]}
        return payload
    except Exception as exc:
        logger.debug("Alpha Vantage request failed for %s: %s", params, exc)
        return {"status": "error", "message": str(exc)}


def get_symbol_candidates(ticker: str) -> List[str]:
    """
    Generate likely Alpha Vantage symbol variants for a preferred ticker.

    Alpha Vantage symbology for preferreds is inconsistent across issuers, so
    these candidates are only heuristics. Explicit provider overrides and local
    cached metadata should win before these guesses are used.
    """
    cleaned = ticker.strip().upper()
    if not cleaned:
        return []

    candidates: List[str] = []
    override = _provider_symbol_override(cleaned)
    if override:
        candidates.append(override)

    candidates.append(cleaned)

    if "-" in cleaned:
        base, suffix = cleaned.split("-", 1)
        series = suffix[1:] if suffix.startswith("P") and len(suffix) > 1 else suffix
        if series:
            candidates.extend(
                [
                    f"{base}-P-{series}",
                    f"{base}.{suffix}",
                    f"{base}.PR{series}",
                    f"{base}.PR.{series}",
                    f"{base}.{series}",
                    f"{base}{suffix}",
                    f"{base}-{series}",
                    f"{base} {series}",
                ]
            )

    seen = set()
    return [value for value in candidates if value and not (value in seen or seen.add(value))]


def _candidate_search_terms(ticker: str) -> List[str]:
    """Build a short list of search keywords for Alpha Vantage symbol search."""
    metadata = _load_symbol_metadata(ticker)
    security_name = str(metadata.get("security_name") or "").strip()
    issuer = str(metadata.get("issuer") or "").strip()
    series_label = str(metadata.get("series") or "").strip()
    series_token = series_label.upper().replace("SERIES", "").strip()

    terms = [ticker]
    if "-" in ticker:
        base, suffix = ticker.split("-", 1)
        series = suffix[1:] if suffix.startswith("P") and len(suffix) > 1 else suffix
        terms.extend(
            [
                ticker.replace("-", ""),
                f"{base} {suffix}",
                f"{base} series {series}",
                f"{base} p {series}",
                f"{base}-p-{series}",
            ]
        )
    if issuer:
        terms.extend(
            [
                issuer,
                f"{issuer} preferred",
            ]
        )
        if series_token:
            terms.extend(
                [
                    f"{issuer} series {series_token}",
                    f"{issuer} preferred series {series_token}",
                    f"{issuer} depositary shares series {series_token}",
                ]
            )
    if security_name:
        terms.append(security_name)

    seen = set()
    return [value for value in terms if value and not (value in seen or seen.add(value))]


def _reference_score(
    row: Dict[str, Any],
    requested: str,
    require_preferred: bool,
    metadata: Optional[Dict[str, Any]] = None,
) -> float:
    """Rank Alpha Vantage search results so preferred matches win first."""
    score = 0.0
    symbol = str(row.get("symbol", "")).upper()
    name = str(row.get("name", ""))

    normalized_symbol = _normalize_symbol(symbol)
    normalized_requested = _normalize_symbol(requested)
    if normalized_symbol == normalized_requested:
        score += 100
    elif normalized_requested and normalized_requested in normalized_symbol:
        score += 60

    if is_preferred_reference(row):
        score += 50
    elif require_preferred:
        score -= 50

    region = str(row.get("region", "")).upper()
    if "UNITED STATES" in region or region == "US":
        score += 10

    try:
        score += float(row.get("match_score") or 0) * 10
    except (TypeError, ValueError):
        pass

    if _preferred_like_text(name):
        score += 20

    metadata = metadata or {}
    issuer = str(metadata.get("issuer") or "").strip().lower()
    if issuer:
        issuer_prefix = " ".join(issuer.split()[:2]).strip()
        if issuer_prefix and issuer_prefix in name.lower():
            score += 25

    series_label = str(metadata.get("series") or "").strip().upper()
    if series_label:
        if series_label in name.upper():
            score += 20
        else:
            series_token = series_label.replace("SERIES", "").strip()
            if series_token and f"SERIES {series_token}" in name.upper():
                score += 20

    return score


def _search_reference_symbol(
    ticker: str,
    require_preferred: bool,
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Search Alpha Vantage and return the best normalized reference row."""
    best_row: Optional[Dict[str, Any]] = None
    best_score = float("-inf")
    metadata = metadata or _load_symbol_metadata(ticker)

    for keywords in _candidate_search_terms(ticker)[:6]:
        payload = _request_json(function="SYMBOL_SEARCH", keywords=keywords)
        matches = payload.get("bestMatches") or payload.get("data") or []
        if not isinstance(matches, list):
            continue
        for raw_row in matches:
            if not isinstance(raw_row, dict):
                continue
            row = _normalize_search_match(raw_row)
            score = _reference_score(row, ticker, require_preferred, metadata=metadata)
            if score > best_score:
                best_row = row
                best_score = score

    if best_row and (not require_preferred or is_preferred_reference(best_row)):
        return best_row
    return None


def _dedupe_symbols(symbols: List[str]) -> List[str]:
    """Deduplicate symbols while preserving order."""
    seen = set()
    return [value for value in symbols if value and not (value in seen or seen.add(value))]


def resolve_alpha_symbol(
    ticker: str,
    require_preferred: bool = False,
) -> Optional[Dict[str, Any]]:
    """Resolve Alpha symbol candidates and reference metadata once per ticker."""
    cleaned = ticker.strip().upper()
    if not cleaned:
        return None

    context = get_security_context(cleaned)
    override = _provider_symbol_override(cleaned)
    listing_row = _lookup_listing_status_row(cleaned)
    reference = None
    source = "heuristic"

    if override:
        reference = _build_local_reference(
            override,
            context=context,
            source="provider_override",
            listing_row=listing_row,
        )
        source = "provider_override"
    elif listing_row:
        reference = _build_local_reference(
            str(listing_row.get("symbol", "")).strip().upper(),
            context=context,
            source="listing_status",
            listing_row=listing_row,
        )
        source = "listing_status"
    else:
        reference = _search_reference_symbol(
            cleaned,
            require_preferred=require_preferred,
            metadata=context.get("merged_entry") or {},
        )
        if reference:
            source = "search"

    candidates: List[str] = []
    if reference and reference.get("symbol"):
        candidates.append(str(reference["symbol"]).strip().upper())
    if listing_row and listing_row.get("symbol"):
        candidates.append(str(listing_row["symbol"]).strip().upper())
    candidates.extend(get_symbol_candidates(cleaned))
    candidates = _dedupe_symbols(candidates)

    if not candidates:
        return None

    return {
        "symbol": candidates[0],
        "candidates": candidates,
        "reference": reference,
        "source": source,
        "metadata": context,
        "require_preferred": require_preferred,
    }


def lookup_reference_symbol(
    ticker: str,
    require_preferred: bool = False,
) -> Optional[Dict[str, Any]]:
    """Look up the most likely Alpha Vantage symbol metadata for a ticker."""
    ticker = ticker.strip().upper()
    cache_key = (ticker, require_preferred)
    if cache_key in _reference_cache:
        return _reference_cache[cache_key]

    resolution = resolve_alpha_symbol(ticker, require_preferred=require_preferred)
    reference = resolution.get("reference") if resolution else None
    _reference_cache[cache_key] = reference
    return reference


def _normalize_quote_payload(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Normalize GLOBAL_QUOTE responses into a common quote shape."""
    quote = payload.get("Global Quote")
    if not isinstance(quote, dict):
        quote = payload

    price = quote.get("05. price") or quote.get("price")
    if not price:
        return None

    return {
        "symbol": quote.get("01. symbol") or quote.get("symbol") or "",
        "price": price,
        "previous_close": quote.get("08. previous close") or quote.get("previous_close"),
        "change_percent": quote.get("10. change percent") or quote.get("change_percent"),
        "volume": quote.get("06. volume") or quote.get("volume"),
    }


def get_quote(
    ticker: str,
    require_preferred: bool = False,
) -> Optional[Dict[str, Any]]:
    """Fetch a latest quote payload for the requested ticker."""
    _clear_last_error()
    cached = _quote_cache.get(f"{ticker.upper()}::{require_preferred}")
    if cached is not None:
        return cached

    resolution = resolve_alpha_symbol(ticker, require_preferred=require_preferred)
    if not resolution:
        _set_last_error("No Alpha Vantage symbol candidates available for the requested ticker.")
        return None

    reference = resolution.get("reference")
    metadata = resolution.get("metadata") or {}
    last_error = ""

    for symbol in resolution.get("candidates") or []:
        payload = _request_json(function="GLOBAL_QUOTE", symbol=symbol)
        if payload.get("status") == "error":
            last_error = payload.get("message", "")
            continue
        normalized = _normalize_quote_payload(payload)
        if not normalized:
            continue

        result = {
            "symbol": normalized["symbol"] or symbol,
            "price": normalized["price"],
            "close": normalized["price"],
            "volume": normalized["volume"],
            "previous_close": normalized["previous_close"],
            "change_percent": normalized["change_percent"],
            "currency": "USD",
            "name": (
                (reference or {}).get("name")
                or metadata.get("security_name")
                or metadata.get("issuer")
            ),
            "_matched_symbol": symbol,
        }
        if reference:
            result["_reference"] = reference

        _quote_cache[f"{ticker.upper()}::{require_preferred}"] = result
        return result

    _set_last_error(last_error or "No Alpha Vantage quote returned for the resolved symbol candidates.")
    return None


def get_company_overview(symbol: str) -> Dict[str, Any]:
    """Fetch and cache Alpha Vantage company overview metadata."""
    _clear_last_error()
    symbol = symbol.strip().upper()
    if symbol in _overview_cache:
        return _overview_cache[symbol]

    payload = _request_json(function="OVERVIEW", symbol=symbol)
    if payload.get("status") == "error":
        _set_last_error(payload.get("message", ""))
        _overview_cache[symbol] = {}
        return {}

    _overview_cache[symbol] = payload
    return payload


def _parse_time_series_payload(payload: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """Normalize Alpha Vantage time series payloads into a DataFrame."""
    if payload.get("status") == "error":
        return None

    series_dict = None
    for key, value in payload.items():
        if "Time Series" in key and isinstance(value, dict):
            series_dict = value
            break

    if not isinstance(series_dict, dict):
        return None

    rows = []
    for dt_str, values in series_dict.items():
        if not isinstance(values, dict):
            continue
        rows.append(
            {
                "datetime": pd.to_datetime(dt_str, errors="coerce"),
                "Open": values.get("1. open"),
                "High": values.get("2. high"),
                "Low": values.get("3. low"),
                "Close": values.get("4. close"),
                "Volume": values.get("5. volume") or values.get("6. volume"),
            }
        )

    if not rows:
        return None

    df = pd.DataFrame(rows).dropna(subset=["datetime"]).set_index("datetime").sort_index()
    for column in ("Open", "High", "Low", "Close", "Volume"):
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def get_time_series(
    ticker: str,
    period: str = "1y",
    require_preferred: bool = False,
) -> Optional[pd.DataFrame]:
    """Fetch historical OHLCV data for the requested ticker."""
    _clear_last_error()
    period_specs = {
        "5d": ("TIME_SERIES_DAILY", {"outputsize": "compact"}, 5),
        "1mo": ("TIME_SERIES_DAILY", {"outputsize": "compact"}, 22),
        "3mo": ("TIME_SERIES_DAILY", {"outputsize": "compact"}, 66),
        "6mo": ("TIME_SERIES_DAILY", {"outputsize": "compact"}, 100),
        "1y": ("TIME_SERIES_WEEKLY", {}, 52),
        "2y": ("TIME_SERIES_WEEKLY", {}, 104),
        "5y": ("TIME_SERIES_MONTHLY", {}, 60),
    }
    function, extra_params, tail_points = period_specs.get(period, period_specs["1y"])
    last_error = ""
    resolution = resolve_alpha_symbol(ticker, require_preferred=require_preferred)
    if not resolution:
        _set_last_error("No Alpha Vantage symbol candidates available for the requested ticker.")
        return None

    for symbol in resolution.get("candidates") or []:
        payload = _request_json(function=function, symbol=symbol, **extra_params)
        if payload.get("status") == "error":
            last_error = payload.get("message", "")
            continue
        df = _parse_time_series_payload(payload)
        if df is None or df.empty:
            continue
        df.attrs["provider_symbol"] = symbol
        return df.tail(tail_points)

    _set_last_error(last_error or "No Alpha Vantage time series returned for the resolved symbol candidates.")
    return None


def get_dividends(
    ticker: str,
    require_preferred: bool = False,
) -> Optional[pd.DataFrame]:
    """Fetch dividend payment history for the requested ticker."""
    _clear_last_error()
    cache_key = f"{ticker.upper()}::{require_preferred}"
    if cache_key in _dividend_cache:
        return _dividend_cache[cache_key]

    last_error = ""
    resolution = resolve_alpha_symbol(ticker, require_preferred=require_preferred)
    if not resolution:
        _set_last_error("No Alpha Vantage symbol candidates available for the requested ticker.")
        _dividend_cache[cache_key] = None
        return None

    for symbol in resolution.get("candidates") or []:
        payload = _request_json(function="DIVIDENDS", symbol=symbol)
        if payload.get("status") == "error":
            last_error = payload.get("message", "")
            continue

        rows = payload.get("data") or payload.get("dividends") or payload.get("historical")
        if not isinstance(rows, list):
            # Some responses may nest the rows under an arbitrary list key.
            rows = next(
                (
                    value
                    for value in payload.values()
                    if isinstance(value, list) and value and isinstance(value[0], dict)
                ),
                [],
            )

        parsed_rows = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            amount = (
                row.get("amount")
                or row.get("dividend_amount")
                or row.get("cash_amount")
            )
            pay_date = (
                row.get("payment_date")
                or row.get("pay_date")
                or row.get("ex_dividend_date")
                or row.get("ex_dividend")
            )
            if not amount or not pay_date:
                continue
            parsed_rows.append(
                {
                    "date": pd.to_datetime(pay_date, errors="coerce"),
                    "dividend": amount,
                }
            )

        if not parsed_rows:
            continue

        df = pd.DataFrame(parsed_rows).dropna(subset=["date"])
        df["dividend"] = pd.to_numeric(df["dividend"], errors="coerce")
        df = df.dropna(subset=["dividend"]).set_index("date").sort_index()
        _dividend_cache[cache_key] = df
        return df

    _set_last_error(last_error or "No Alpha Vantage dividend history returned for the resolved symbol candidates.")
    _dividend_cache[cache_key] = None
    return None
