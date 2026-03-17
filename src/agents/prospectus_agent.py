"""
Prospectus Parsing Agent for Preferred Stock Term Extraction.

This agent now uses a cache-first, staged extraction pipeline:

1. Resolve the filing from a committed demo registry when available.
2. Load committed structured term cache for demo tickers before any live work.
3. Fall back to focused section extraction plus regex/rule parsing.
4. Use Gemini only when deterministic parsing leaves important gaps.

The public interface stays the same for the LangGraph swarm, but the normal
demo path is much faster because the app no longer has to search EDGAR and
send large raw filings to Gemini on every run.
"""

import json
import os
import re
import sys
from typing import Any, Dict, List, Optional

from dateutil import parser as date_parser

# Allow direct script execution via:
#   python3 src/agents/prospectus_agent.py JPM-PD
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from src.utils.config import get_llm


DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
DEMO_TERMS_CACHE_DIR = os.path.join(DATA_DIR, "prospectus_terms", "demo")
RUNTIME_TERMS_CACHE_DIR = os.path.join(DATA_DIR, "prospectus_terms", "runtime")

TERM_FIELDS = [
    "security_name",
    "issuer",
    "series",
    "ticker",
    "par_value",
    "coupon_rate",
    "coupon_type",
    "floating_benchmark",
    "floating_spread",
    "fixed_to_floating_date",
    "dividend_frequency",
    "cumulative",
    "qdi_eligible",
    "call_date",
    "call_price",
    "maturity_date",
    "perpetual",
    "conversion_feature",
    "listing_exchange",
    "deposit_shares",
    "deposit_fraction",
    "seniority",
    "use_of_proceeds",
    "total_offering_amount",
    "confidence_score",
]

LLM_TRIGGER_FIELDS = {
    "security_name",
    "series",
    "coupon_rate",
    "coupon_type",
    "cumulative",
    "perpetual",
    "par_value",
}

EXTRACTION_PROMPT = """You are a fixed-income analyst specializing in preferred securities.
You are reading a focused excerpt from a SEC filing that describes a preferred security.

Known extracted fields from deterministic parsing:
{known_terms_json}

Only improve the fields that remain unresolved or low-confidence:
{requested_fields}

Return ONLY a valid JSON object using the schema below. Use null for any field you still cannot determine.

Required JSON fields:
{{
    "security_name": "Full official name of the security",
    "issuer": "Name of the issuing company",
    "series": "Series designation",
    "ticker": "Trading ticker if mentioned",
    "par_value": "Par value or liquidation preference per share in dollars (number only)",
    "coupon_rate": "Annual coupon/dividend rate as a percentage (number only)",
    "coupon_type": "One of: 'fixed', 'floating', 'fixed-to-floating', 'adjustable'",
    "floating_benchmark": "If floating or fixed-to-floating, the benchmark rate",
    "floating_spread": "If floating, the spread over the benchmark in basis points (number only)",
    "fixed_to_floating_date": "If fixed-to-floating, the date when it switches to floating (YYYY-MM-DD)",
    "dividend_frequency": "One of: 'quarterly', 'semi-annual', 'monthly', 'annual'",
    "cumulative": "true if cumulative dividends, false if non-cumulative",
    "qdi_eligible": "true if dividends qualify for QDI tax treatment, false if not, null if not mentioned",
    "call_date": "First optional redemption date (YYYY-MM-DD), or null",
    "call_price": "Redemption price per share in dollars (number only)",
    "maturity_date": "Maturity date (YYYY-MM-DD), or null if perpetual",
    "perpetual": "true if the security has no maturity date, false otherwise",
    "conversion_feature": "Brief description of any conversion feature, or null if none",
    "listing_exchange": "Exchange where the security is listed",
    "deposit_shares": "true if the security is issued as depositary shares, false otherwise",
    "deposit_fraction": "Depositary share fraction such as '1/400th', or null",
    "seniority": "Position in capital structure",
    "use_of_proceeds": "Brief summary of the intended use of proceeds, or null",
    "total_offering_amount": "Total dollar amount of the offering, or null",
    "confidence_score": "Extraction confidence from 0.0 to 1.0"
}}

Rules:
- Return JSON only.
- Dates must use YYYY-MM-DD.
- Use null instead of guessing.
- Keep coupon_type lowercase and use ASCII text only.

FOCUSED FILING TEXT:
{prospectus_text}
"""


def extract_terms_from_text(
    prospectus_text: str,
    ticker: str = "",
    max_text_length: int = 18000,
    filing_metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Extract structured terms from raw prospectus text using a staged approach.

    The function first performs deterministic extraction on the most relevant
    sections, then uses Gemini only if important fields remain unresolved.
    """
    if not prospectus_text or len(prospectus_text.strip()) < 100:
        return {
            "error": "Prospectus text is too short or empty",
            "ticker": _normalize_ticker(ticker),
            "confidence_score": 0.0,
        }

    focused_text = _extract_relevant_sections(prospectus_text, max_chars=max_text_length)
    regex_terms = _extract_terms_by_regex(
        focused_text,
        ticker=_normalize_ticker(ticker),
        filing_metadata=filing_metadata,
    )

    terms = regex_terms
    source = "regex"

    if _should_call_llm(regex_terms):
        llm_terms = _extract_terms_with_llm(
            focused_text,
            ticker=_normalize_ticker(ticker),
            known_terms=regex_terms,
        )
        if llm_terms and not llm_terms.get("error"):
            terms = _merge_terms(regex_terms, llm_terms)
            source = "mixed"

    finalized = _finalize_terms(
        terms,
        ticker=_normalize_ticker(ticker),
        filing_metadata=filing_metadata,
        source=source,
    )

    if _has_minimum_terms(finalized):
        return finalized

    return {
        "error": "Could not extract sufficient prospectus terms from filing text",
        "ticker": _normalize_ticker(ticker),
        "confidence_score": 0.0,
        "source": source,
    }


def extract_terms(
    filing: Dict[str, Any],
    pipeline=None,
    requested_ticker: str = "",
    resolution: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Extract terms from a filing dict, using committed/demo caches first and then
    a staged regex + focused LLM fallback.
    """
    requested_ticker = _normalize_ticker(requested_ticker or filing.get("ticker", ""))

    cached_terms = load_structured_terms_cache(requested_ticker, filing)
    if cached_terms:
        return _apply_resolution_metadata(
            cached_terms,
            ticker=requested_ticker,
            filing_metadata=filing,
            resolution=resolution,
            source_override="cache",
        )

    if pipeline is None:
        from src.data.edgar_pipeline import EdgarPipeline

        pipeline = EdgarPipeline()

    text = pipeline.download_filing(filing, max_chars=50000)
    if not text:
        return {
            "error": "Could not download filing text",
            "accession_number": filing.get("accession_number", ""),
            "ticker": requested_ticker,
            "confidence_score": 0.0,
        }

    terms = extract_terms_from_text(
        text,
        ticker=requested_ticker,
        filing_metadata=filing,
    )
    terms = _apply_resolution_metadata(
        terms,
        ticker=requested_ticker,
        filing_metadata=filing,
        resolution=resolution,
    )

    if not terms.get("error"):
        save_runtime_terms_cache(terms, requested_ticker, filing)

    return terms


def prospectus_agent_node(state: dict) -> dict:
    """
    LangGraph node function for the Prospectus Parsing Agent.
    """
    ticker = _normalize_ticker(state.get("ticker", ""))
    prospectus_text = state.get("prospectus_text", "")
    filing = state.get("prospectus_filing", {})

    status_updates = {}
    error_updates = []

    if prospectus_text:
        terms = extract_terms_from_text(prospectus_text, ticker=ticker, filing_metadata=filing or None)
    else:
        from src.data.edgar_pipeline import EdgarPipeline, resolve_preferred_filing

        pipeline = EdgarPipeline()

        if filing:
            resolution = {
                "requested_ticker": ticker,
                "requested_series": filing.get("expected_series", ""),
                "source": "provided",
                "selected_filing": filing,
                "accession_number": filing.get("accession_number", ""),
                "filing_date": filing.get("filing_date", ""),
                "filing_url": filing.get("url", ""),
                "matched_series": filing.get("expected_series") or filing.get("series", ""),
                "series_match": True,
                "validation_tokens": [],
                "mismatch_warning": None,
            }
            selected_filing = filing
        else:
            filings, resolution = resolve_preferred_filing(ticker, pipeline=pipeline)
            selected_filing = resolution.get("selected_filing", {}) if resolution else {}
            if not selected_filing and filings:
                selected_filing = filings[0]

        if not selected_filing:
            terms = {
                "error": f"No prospectus found for {ticker} on EDGAR",
                "ticker": ticker,
                "confidence_score": 0.0,
            }
        else:
            terms = extract_terms(
                selected_filing,
                pipeline=pipeline,
                requested_ticker=ticker,
                resolution=resolution,
            )

    mismatch_message = _series_mismatch_message(terms)
    if mismatch_message and not terms.get("error"):
        terms["error"] = mismatch_message
        terms["confidence_score"] = 0.0

    if terms.get("error"):
        error_updates.append(f"Prospectus Agent: {terms['error']}")
        status_updates["prospectus"] = "failed"
    else:
        status_updates["prospectus"] = "success"

    return {
        "prospectus_terms": terms,
        "agent_status": status_updates,
        "errors": error_updates,
    }


def load_structured_terms_cache(
    ticker: str = "",
    filing: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Load the committed demo cache or a generated runtime cache."""
    ticker = _normalize_ticker(ticker)
    accession = _normalize_accession((filing or {}).get("accession_number", ""))

    candidate_paths: List[str] = []
    if ticker:
        candidate_paths.append(os.path.join(DEMO_TERMS_CACHE_DIR, f"{ticker}.json"))
    if accession:
        candidate_paths.append(os.path.join(DEMO_TERMS_CACHE_DIR, f"{accession}.json"))
        candidate_paths.append(os.path.join(RUNTIME_TERMS_CACHE_DIR, f"{accession}.json"))
    if ticker:
        candidate_paths.append(os.path.join(RUNTIME_TERMS_CACHE_DIR, f"{ticker}.json"))

    for path in candidate_paths:
        if not os.path.exists(path):
            continue

        try:
            with open(path, "r", encoding="utf-8") as f:
                cached_terms = json.load(f)
        except (OSError, json.JSONDecodeError):
            continue

        if _cache_entry_matches(cached_terms, ticker=ticker, accession=accession):
            return cached_terms

    return None


def save_runtime_terms_cache(
    terms: Dict[str, Any],
    requested_ticker: str = "",
    filing: Optional[Dict[str, Any]] = None,
) -> None:
    """Persist generated structured terms locally for faster reruns."""
    os.makedirs(RUNTIME_TERMS_CACHE_DIR, exist_ok=True)

    accession = _normalize_accession(
        terms.get("accession_number") or (filing or {}).get("accession_number", "")
    )
    cache_key = accession or _normalize_ticker(requested_ticker)
    if not cache_key:
        return

    cache_path = os.path.join(RUNTIME_TERMS_CACHE_DIR, f"{cache_key}.json")
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(terms, f, indent=2, default=str)


def _extract_relevant_sections(
    prospectus_text: str,
    max_chars: int = 18000,
    section_window: int = 3500,
) -> str:
    """Trim the filing to the most relevant sections before parsing or LLM use."""
    normalized = prospectus_text.replace("\r\n", "\n").replace("\r", "\n")

    sections = [normalized[: min(6000, len(normalized))]]
    heading_patterns = [
        r"\nsummary\b",
        r"\noffering\b",
        r"\ndescription of .*preferred",
        r"\ndescription of .*depositary",
        r"\noptional redemption\b",
        r"\nredemption\b",
        r"\ndividends?\b",
        r"\nmaterial u\.s\. federal income tax",
        r"\ntax consequences\b",
    ]

    for pattern in heading_patterns:
        match = re.search(pattern, normalized, flags=re.IGNORECASE)
        if not match:
            continue

        start = max(match.start() - 200, 0)
        end = min(match.start() + section_window, len(normalized))
        candidate = normalized[start:end].strip()
        if candidate and candidate not in sections:
            sections.append(candidate)

    combined = "\n\n".join(sections)
    return combined[:max_chars]


def _extract_terms_by_regex(
    text: str,
    ticker: str = "",
    filing_metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Deterministically extract the easy/high-signal prospectus fields."""
    terms = _empty_terms()
    filing_metadata = filing_metadata or {}
    normalized_text = _clean_whitespace(text)

    terms["ticker"] = ticker or filing_metadata.get("ticker")
    terms["issuer"] = _clean_issuer_name(filing_metadata.get("issuer_name", ""))

    security_name = _extract_security_name(normalized_text)
    if security_name:
        terms["security_name"] = security_name

    series = _extract_series(normalized_text)
    if series:
        terms["series"] = series

    coupon_type = _extract_coupon_type(normalized_text)
    if coupon_type:
        terms["coupon_type"] = coupon_type

    coupon_info = _extract_coupon_info(normalized_text)
    terms = _merge_terms(terms, coupon_info)

    terms["dividend_frequency"] = _extract_dividend_frequency(normalized_text)
    terms["cumulative"] = _extract_cumulative_flag(normalized_text)
    terms["qdi_eligible"] = _extract_qdi_flag(normalized_text)
    terms["call_date"] = _extract_call_date(normalized_text)
    terms["call_price"] = _extract_call_price(normalized_text)
    terms["maturity_date"] = _extract_maturity_date(normalized_text)
    terms["perpetual"] = _extract_perpetual_flag(normalized_text)
    terms["conversion_feature"] = _extract_conversion_feature(normalized_text)
    terms["listing_exchange"] = _extract_listing_exchange(normalized_text)

    deposit_fraction = _extract_deposit_fraction(normalized_text)
    if deposit_fraction:
        terms["deposit_shares"] = True
        terms["deposit_fraction"] = deposit_fraction
    elif "depositary share" in normalized_text.lower():
        terms["deposit_shares"] = True
    else:
        terms["deposit_shares"] = False

    terms["par_value"] = _extract_par_value(normalized_text)
    terms["seniority"] = _extract_seniority(normalized_text)
    terms["use_of_proceeds"] = _extract_use_of_proceeds(normalized_text)
    terms["total_offering_amount"] = _extract_total_offering_amount(normalized_text)

    if terms["confidence_score"] is None:
        populated_fields = sum(1 for key in TERM_FIELDS if terms.get(key) is not None)
        terms["confidence_score"] = round(min(0.92, 0.45 + populated_fields * 0.02), 2)

    return terms


def _extract_terms_with_llm(
    focused_text: str,
    ticker: str,
    known_terms: Dict[str, Any],
) -> Dict[str, Any]:
    """Call Gemini only when important fields remain unresolved."""
    requested_fields = _requested_llm_fields(known_terms)
    if not requested_fields:
        return {}

    prompt = EXTRACTION_PROMPT.format(
        known_terms_json=json.dumps(known_terms, indent=2, default=str),
        requested_fields=", ".join(requested_fields),
        prospectus_text=focused_text,
    )

    try:
        llm = get_llm(temperature=0.0)
        response = llm.invoke(prompt)
        content = response.content
        if isinstance(content, list):
            text_parts = []
            for block in content:
                if isinstance(block, dict) and "text" in block:
                    text_parts.append(block["text"])
                elif isinstance(block, str):
                    text_parts.append(block)
            content = "\n".join(text_parts)

        terms = _parse_json_response(content)
        return _post_process_terms(terms, ticker=ticker)
    except Exception:
        return {}


def _apply_resolution_metadata(
    terms: Dict[str, Any],
    ticker: str = "",
    filing_metadata: Optional[Dict[str, Any]] = None,
    resolution: Optional[Dict[str, Any]] = None,
    source_override: Optional[str] = None,
) -> Dict[str, Any]:
    """Attach filing and resolution metadata without disturbing extracted terms."""
    finalized = _finalize_terms(
        terms,
        ticker=ticker,
        filing_metadata=filing_metadata,
        source=source_override or terms.get("source"),
    )

    resolution = resolution or {}
    validation = dict(finalized.get("validation", {}))
    expected_series = _expected_series_for_ticker(
        resolution.get("requested_ticker") or ticker or finalized.get("ticker", "")
    )
    extracted_series = finalized.get("series")
    series_match = resolution.get("series_match")
    if expected_series and extracted_series:
        series_match = extracted_series.strip().lower() == expected_series.strip().lower()
    elif series_match is None:
        series_match = True
    mismatch_warning = resolution.get("mismatch_warning")
    if expected_series and extracted_series and not series_match and not mismatch_warning:
        mismatch_warning = (
            f"Resolved filing for {resolution.get('requested_ticker') or ticker or finalized.get('ticker')} "
            f"did not clearly match {expected_series}."
        )

    validation.update({
        "requested_ticker": resolution.get("requested_ticker") or ticker or finalized.get("ticker"),
        "matched_series": resolution.get("matched_series") or finalized.get("series"),
        "confidence_score": finalized.get("confidence_score"),
        "series_match": series_match,
        "validation_tokens": resolution.get("validation_tokens", validation.get("validation_tokens", [])),
    })

    finalized["resolution_source"] = resolution.get("source", finalized.get("resolution_source", "live"))
    finalized["matched_series"] = resolution.get("matched_series") or finalized.get("series")
    finalized["mismatch_warning"] = mismatch_warning
    finalized["validation"] = validation

    if source_override:
        finalized["source"] = source_override
    elif finalized.get("source") is None:
        finalized["source"] = "live"

    return finalized


def _finalize_terms(
    terms: Dict[str, Any],
    ticker: str = "",
    filing_metadata: Optional[Dict[str, Any]] = None,
    source: Optional[str] = None,
) -> Dict[str, Any]:
    """Normalize parsed terms, add defaults, and attach filing metadata."""
    filing_metadata = filing_metadata or {}
    finalized = _empty_terms()
    finalized.update(_post_process_terms(terms, ticker=ticker))

    if ticker and not finalized.get("ticker"):
        finalized["ticker"] = ticker

    if filing_metadata:
        if not finalized.get("issuer"):
            finalized["issuer"] = _clean_issuer_name(filing_metadata.get("issuer_name", ""))
        finalized["accession_number"] = filing_metadata.get("accession_number", finalized.get("accession_number", ""))
        finalized["filing_date"] = filing_metadata.get("filing_date", finalized.get("filing_date", ""))
        finalized["filing_url"] = filing_metadata.get("url", finalized.get("filing_url", ""))
        finalized["issuer_cik"] = filing_metadata.get("issuer_cik", finalized.get("issuer_cik", ""))

    if source:
        finalized["source"] = source
    elif not finalized.get("source"):
        finalized["source"] = "live"

    if finalized.get("confidence_score") is None:
        finalized["confidence_score"] = 0.0

    return finalized


def _has_minimum_terms(terms: Dict[str, Any]) -> bool:
    """Require enough structure for the quality gate and UI."""
    return bool(
        terms.get("security_name")
        and terms.get("coupon_type")
        and (
            terms.get("coupon_rate") is not None
            or terms.get("floating_spread") is not None
        )
        and (
            terms.get("par_value") is not None
            or terms.get("deposit_fraction") is not None
            or terms.get("perpetual") is not None
        )
    )


def _expected_series_for_ticker(ticker: str) -> Optional[str]:
    """Resolve the expected SEC series label for a user-entered preferred ticker."""
    if not ticker:
        return None

    from src.data.edgar_pipeline import _derive_series_hint

    return _derive_series_hint(ticker)


def _series_mismatch_message(terms: Dict[str, Any]) -> Optional[str]:
    """Fail closed when the selected filing does not match the requested preferred."""
    validation = terms.get("validation", {})
    if validation.get("series_match", True):
        return None
    return terms.get("mismatch_warning") or "Resolved filing did not match the requested preferred series."


def _should_call_llm(terms: Dict[str, Any]) -> bool:
    """Only invoke Gemini when important fields still appear unresolved."""
    return any(terms.get(field) is None for field in LLM_TRIGGER_FIELDS)


def _requested_llm_fields(terms: Dict[str, Any]) -> List[str]:
    """Return the unresolved fields worth asking Gemini to improve."""
    return [field for field in TERM_FIELDS if terms.get(field) is None and field in LLM_TRIGGER_FIELDS]


def _empty_terms() -> Dict[str, Any]:
    """Base schema for structured prospectus terms."""
    terms = {field: None for field in TERM_FIELDS}
    terms.update({
        "accession_number": "",
        "filing_date": "",
        "filing_url": "",
        "issuer_cik": "",
        "source": None,
        "resolution_source": None,
        "matched_series": None,
        "mismatch_warning": None,
        "validation": {},
    })
    return terms


def _merge_terms(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    """Overlay non-null values from one term dict onto another."""
    merged = dict(base)
    for key, value in overlay.items():
        if value is not None and value != "":
            merged[key] = value
    return merged


def _cache_entry_matches(terms: Dict[str, Any], ticker: str, accession: str) -> bool:
    """Reject obviously stale or mismatched cache files."""
    if not isinstance(terms, dict) or terms.get("error"):
        return False

    cached_ticker = _normalize_ticker(terms.get("ticker", ""))
    cached_requested = _normalize_ticker(terms.get("validation", {}).get("requested_ticker", ""))
    cached_accession = _normalize_accession(terms.get("accession_number", ""))

    ticker_match = not ticker or ticker in {cached_ticker, cached_requested}
    accession_match = not accession or accession == cached_accession
    return ticker_match and accession_match


def _normalize_ticker(ticker: str) -> str:
    """Normalize a preferred ticker into the app's uppercase format."""
    return ticker.strip().upper()


def _normalize_accession(accession: str) -> str:
    """Normalize accession numbers so dashed and non-dashed forms both compare cleanly."""
    cleaned = accession.strip()
    digits_only = cleaned.replace("-", "")
    if len(digits_only) == 18 and digits_only.isdigit():
        return f"{digits_only[:10]}-{digits_only[10:12]}-{digits_only[12:]}"
    return cleaned


def _clean_whitespace(text: str) -> str:
    """Collapse SEC text into something easier to parse with regex."""
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _clean_issuer_name(issuer: str) -> str:
    """Remove ticker suffixes from issuer names."""
    return re.sub(r"\s*\([^)]*\)\s*$", "", issuer or "").strip()


def _extract_security_name(text: str) -> Optional[str]:
    """Extract the preferred security title from a title/header or summary."""
    patterns = [
        r"Interest in a Share of\s+([A-Za-z /-]*Preferred Stock,\s*Series\s+[A-Z]{1,2})",
        r"share of (?:perpetual )?([A-Za-z /-]*Preferred Stock,\s*Series\s+[A-Z]{1,2})",
        r"(\d+(?:\.\d+)?%\s+[A-Za-z /-]*Preferred Stock,\s*Series\s+[A-Z]{1,2})",
        r"(Floating Rate Non-Cumulative Preferred Stock,\s*Series\s+[A-Z]{1,2})",
        r"(\d+(?:\.\d+)?%\s+[A-Za-z /-]*Class A Preferred Stock,\s*Series\s+[A-Z]{1,2})",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return match.group(1).replace("  ", " ").strip()
    return None


def _extract_series(text: str) -> Optional[str]:
    """Extract the series label, including doubled bank-series letters."""
    match = re.search(r"(Series\s+[A-Z]{1,2})", text)
    return match.group(1) if match else None


def _extract_coupon_type(text: str) -> Optional[str]:
    """Infer coupon type from high-signal language."""
    lower = text.lower()
    if "fixed rate/floating rate" in lower or "fixed-to-floating" in lower:
        return "fixed-to-floating"
    if "floating rate" in lower or "libor" in lower or "sofr" in lower:
        return "floating"
    if "%" in text:
        return "fixed"
    return None


def _extract_coupon_info(text: str) -> Dict[str, Any]:
    """Parse coupon rate plus any floating benchmark/spread metadata."""
    info: Dict[str, Any] = {}

    floating_match = re.search(
        r"greater of\s*(?:\(\d\)\s*)?([0-9]+(?:\.[0-9]+)?)%\s+above\s+([A-Za-z0-9\- ]*?(?:LIBOR|SOFR))"
        r".{0,120}?(?:\(\d\)\s*)?([0-9]+(?:\.[0-9]+)?)%",
        text,
        flags=re.IGNORECASE,
    )
    if floating_match:
        spread_pct = float(floating_match.group(1))
        benchmark = floating_match.group(2).strip()
        floor_rate = float(floating_match.group(3))
        info["coupon_rate"] = floor_rate
        info["floating_benchmark"] = benchmark
        info["floating_spread"] = round(spread_pct * 100)
        if not info.get("coupon_type"):
            info["coupon_type"] = "floating"
        return info

    fixed_float_match = re.search(
        r"([0-9]+(?:\.[0-9]+)?)%\s+Fixed Rate/Floating Rate",
        text,
        flags=re.IGNORECASE,
    )
    if fixed_float_match:
        info["coupon_rate"] = float(fixed_float_match.group(1))

    rate_match = re.search(
        r"at a rate (?:equal )?(?:to )?([0-9]+(?:\.[0-9]+)?)%\s+per annum",
        text,
        flags=re.IGNORECASE,
    )
    if rate_match:
        info["coupon_rate"] = float(rate_match.group(1))
    elif not info.get("coupon_rate"):
        title_rate = re.search(r"([0-9]+(?:\.[0-9]+)?)%\s+[A-Za-z /-]*Preferred Stock", text, flags=re.IGNORECASE)
        if title_rate:
            info["coupon_rate"] = float(title_rate.group(1))

    fixed_to_floating_date = re.search(
        r"until\s+([A-Z][a-z]+\s+\d{1,2},\s+\d{4})",
        text,
    )
    if fixed_to_floating_date:
        parsed_date = _parse_date(fixed_to_floating_date.group(1))
        if parsed_date:
            info["fixed_to_floating_date"] = parsed_date

    return info


def _extract_dividend_frequency(text: str) -> Optional[str]:
    """Parse the dividend payment cadence."""
    lower = text.lower()
    if "quarterly" in lower or "quarterly in arrears" in lower:
        return "quarterly"
    if "semi-annual" in lower or "semiannual" in lower:
        return "semi-annual"
    if "monthly" in lower:
        return "monthly"
    if "annual" in lower:
        return "annual"
    return None


def _extract_cumulative_flag(text: str) -> Optional[bool]:
    """Detect cumulative vs non-cumulative language."""
    lower = text.lower()
    if "non-cumulative" in lower or "noncumulative" in lower:
        return False
    if "cumulative" in lower:
        return True
    return None


def _extract_qdi_flag(text: str) -> Optional[bool]:
    """Look for explicit QDI language."""
    lower = text.lower()
    if "qualified dividend income" in lower or "qdi" in lower:
        if "not eligible" in lower or "not be qualified dividend income" in lower:
            return False
        if "eligible" in lower or "will be qualified dividend income" in lower:
            return True
    return None


def _extract_call_date(text: str) -> Optional[str]:
    """Find the first optional redemption date when clearly stated."""
    patterns = [
        r"on or after\s+([A-Z][a-z]+\s+\d{1,2},\s+\d{4})",
        r"not redeemable prior to\s+([A-Z][a-z]+\s+\d{1,2},\s+\d{4})",
        r"redeemable .* beginning\s+([A-Z][a-z]+\s+\d{1,2},\s+\d{4})",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            parsed = _parse_date(match.group(1))
            if parsed:
                return parsed
    return None


def _extract_call_price(text: str) -> Optional[float]:
    """Find the redemption price per share."""
    match = re.search(
        r"redemption price equal to \$([0-9,]+(?:\.[0-9]+)?) per share",
        text,
        flags=re.IGNORECASE,
    )
    return _parse_number(match.group(1)) if match else None


def _extract_maturity_date(text: str) -> Optional[str]:
    """Parse maturity if this is not perpetual preferred stock."""
    match = re.search(
        r"maturity date(?: of)?\s+([A-Z][a-z]+\s+\d{1,2},\s+\d{4})",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return _parse_date(match.group(1))


def _extract_perpetual_flag(text: str) -> Optional[bool]:
    """Detect perpetual structure."""
    lower = text.lower()
    if "perpetual" in lower and "maturity" not in lower:
        return True
    if "perpetual" in lower:
        return True
    if "maturity date" in lower:
        return False
    return None


def _extract_conversion_feature(text: str) -> Optional[str]:
    """Capture one short conversion-related sentence when present."""
    match = re.search(
        r"([^.]*convert[^.]*\.)",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    sentence = match.group(1).strip()
    return sentence[:220]


def _extract_listing_exchange(text: str) -> Optional[str]:
    """Normalize exchange names into compact labels."""
    if re.search(r"New York Stock Exchange|\bNYSE\b", text, flags=re.IGNORECASE):
        return "NYSE"
    if re.search(r"\bNASDAQ\b", text, flags=re.IGNORECASE):
        return "NASDAQ"
    return None


def _extract_deposit_fraction(text: str) -> Optional[str]:
    """Extract depositary share fraction such as 1/400th or 1/1,000th."""
    match = re.search(
        r"1\s*/\s*([0-9,]+)(?:th|st|nd|rd)?",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    denominator = match.group(1).replace(",", "")
    return f"1/{denominator}th"


def _extract_par_value(text: str) -> Optional[float]:
    """Capture liquidation preference / par value per preferred share."""
    patterns = [
        r"liquidation preference of \$([0-9,]+(?:\.[0-9]+)?) per share",
        r"\$([0-9,]+(?:\.[0-9]+)?) liquidation preference per share",
        r"\$([0-9,]+(?:\.[0-9]+)?)\s+par value",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return _parse_number(match.group(1))
    return None


def _extract_seniority(text: str) -> Optional[str]:
    """Capture the short capital-structure ranking sentence when present."""
    match = re.search(
        r"([^.]*ranks [^.]*\.)",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return match.group(1).strip()[:240]


def _extract_use_of_proceeds(text: str) -> Optional[str]:
    """Capture a concise use-of-proceeds sentence when available."""
    match = re.search(
        r"(use of proceeds[^.]*\.)",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return match.group(1).strip()[:240]


def _extract_total_offering_amount(text: str) -> Optional[str]:
    """Capture the first large offering amount in dollars."""
    match = re.search(
        r"\$([0-9]{1,3}(?:,[0-9]{3}){2,})",
        text,
    )
    if not match:
        return None
    return f"${match.group(1)}"


def _post_process_terms(terms: Dict[str, Any], ticker: str = "") -> Dict[str, Any]:
    """Normalize numeric, boolean, and date fields from regex/LLM outputs."""
    processed = dict(terms)

    if ticker and not processed.get("ticker"):
        processed["ticker"] = ticker

    float_fields = {"par_value", "coupon_rate", "floating_spread", "call_price", "confidence_score"}
    date_fields = {"fixed_to_floating_date", "call_date", "maturity_date"}
    bool_fields = {"cumulative", "qdi_eligible", "perpetual", "deposit_shares"}

    for field in float_fields:
        if field in processed:
            processed[field] = _parse_number(processed.get(field))

    for field in date_fields:
        if field in processed:
            processed[field] = _parse_date(processed.get(field))

    for field in bool_fields:
        if field in processed:
            processed[field] = _parse_bool(processed.get(field))

    coupon_type = processed.get("coupon_type")
    if isinstance(coupon_type, str):
        normalized_coupon = coupon_type.strip().lower()
        if normalized_coupon in {"fixed", "floating", "fixed-to-floating", "adjustable"}:
            processed["coupon_type"] = normalized_coupon

    if processed.get("floating_benchmark") and isinstance(processed["floating_benchmark"], str):
        processed["floating_benchmark"] = processed["floating_benchmark"].strip()

    if processed.get("security_name") and isinstance(processed["security_name"], str):
        processed["security_name"] = _clean_whitespace(processed["security_name"])

    if processed.get("series") and isinstance(processed["series"], str):
        processed["series"] = _clean_whitespace(processed["series"])

    return processed


def _parse_number(value: Any) -> Optional[float]:
    """Convert a numeric-ish string into float, tolerating $, commas, and %."""
    if value in (None, "", "null"):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    cleaned = str(value).strip()
    cleaned = cleaned.replace("$", "").replace(",", "").replace("%", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_bool(value: Any) -> Optional[bool]:
    """Convert common string booleans into actual booleans."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value

    normalized = str(value).strip().lower()
    if normalized in {"true", "yes", "y"}:
        return True
    if normalized in {"false", "no", "n"}:
        return False
    return None


def _parse_date(value: Any) -> Optional[str]:
    """Convert SEC-style date strings into YYYY-MM-DD."""
    if value in (None, "", "null"):
        return None
    if isinstance(value, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", value.strip()):
        return value.strip()

    try:
        parsed = date_parser.parse(str(value), fuzzy=True, default=date_parser.parse("2000-01-01"))
        return parsed.strftime("%Y-%m-%d")
    except (ValueError, TypeError, OverflowError):
        return None


def _parse_json_response(text: str) -> Dict[str, Any]:
    """Parse a JSON object from an LLM response, handling code fences and noise."""
    text = re.sub(r"```json\s*", "", text)
    text = re.sub(r"```\s*$", "", text)
    text = text.strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{[\s\S]*\}", text)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass

    return {
        "error": "Could not parse JSON from LLM response",
        "raw_response": text[:500],
        "confidence_score": 0.0,
    }


def format_terms_report(terms: Dict[str, Any]) -> str:
    """Format extracted terms into a human-readable Markdown report."""
    if terms.get("error"):
        return f"**Extraction Error:** {terms['error']}"

    lines = []
    lines.append(f"## {terms.get('security_name', 'Unknown Security')}")
    lines.append("")
    lines.append("| Field | Value |")
    lines.append("|---|---|")

    field_labels = {
        "issuer": "Issuer",
        "series": "Series",
        "ticker": "Ticker",
        "par_value": "Par Value",
        "coupon_rate": "Coupon Rate",
        "coupon_type": "Coupon Type",
        "floating_benchmark": "Floating Benchmark",
        "floating_spread": "Floating Spread (bps)",
        "fixed_to_floating_date": "Fixed-to-Floating Date",
        "dividend_frequency": "Dividend Frequency",
        "cumulative": "Cumulative",
        "qdi_eligible": "QDI Eligible",
        "call_date": "First Call Date",
        "call_price": "Call Price",
        "maturity_date": "Maturity Date",
        "perpetual": "Perpetual",
        "listing_exchange": "Exchange",
        "deposit_shares": "Depositary Shares",
        "deposit_fraction": "Depositary Fraction",
        "seniority": "Seniority",
        "total_offering_amount": "Offering Amount",
        "confidence_score": "Confidence Score",
        "source": "Source",
        "resolution_source": "Resolution Source",
        "accession_number": "Accession",
    }

    for key, label in field_labels.items():
        value = terms.get(key)
        if value is None or value == "":
            continue
        if key == "coupon_rate":
            value = f"{value}%"
        elif key in {"par_value", "call_price"}:
            value = f"${value}"
        elif key == "floating_spread":
            value = f"{value} bps"
        elif key == "confidence_score":
            value = f"{value:.0%}" if isinstance(value, (int, float)) else value
        elif isinstance(value, bool):
            value = "Yes" if value else "No"
        lines.append(f"| {label} | {value} |")

    return "\n".join(lines)


if __name__ == "__main__":
    ticker = _normalize_ticker(sys.argv[1] if len(sys.argv) > 1 else "JPM-PD")
    print(f"\n{'='*60}")
    print(f"Prospectus Term Extraction: {ticker}")
    print(f"{'='*60}")

    result = prospectus_agent_node({"ticker": ticker, "errors": [], "agent_status": {}})
    terms = result["prospectus_terms"]

    print(f"\nSource: {terms.get('source', 'N/A')} | Resolution: {terms.get('resolution_source', 'N/A')}")
    print(f"Accession: {terms.get('accession_number', 'N/A')} | Filing date: {terms.get('filing_date', 'N/A')}")
    if terms.get("mismatch_warning"):
        print(f"Mismatch warning: {terms['mismatch_warning']}")

    if terms.get("error"):
        print(f"\nExtraction error: {terms['error']}")
        sys.exit(1)

    print(f"\n{format_terms_report(terms)}")
    print(f"\n{'='*60}")
    print("Raw extracted terms (JSON):")
    print(json.dumps(terms, indent=2, default=str))
