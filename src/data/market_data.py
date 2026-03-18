"""
Market data fetcher for preferred equity securities.
Uses yfinance for price data and basic security information.
Handles preferred stock ticker variants (e.g., C-PJ, C-PRJ, C-P-J) for better reliability.

When yfinance does not populate the top-level dividendRate or dividendYield
fields (common for trust preferreds and some utility preferreds), this module
falls back to computing the trailing annual dividend from the actual dividend
payment history.

Includes a snapshot fallback for demo securities to ensure reliability on 
Streamlit Cloud when live data is blocked.
"""

import yfinance as yf
import pandas as pd
import json
import os
from typing import Optional, List
from datetime import datetime


def get_ticker_variants(ticker: str) -> List[str]:
    """
    Generate common Yahoo Finance ticker variants for preferred stocks.

    Example: 'C-PJ' -> ['C-PJ', 'C-PRJ', 'C-P-J', 'C.PJ']
    """
    variants = [ticker]

    # If ticker has a dash (like C-PJ or JPM-PD), try common variations
    if '-' in ticker:
        parts = ticker.split('-')
        if len(parts) == 2:
            base = parts[0]
            series = parts[1]

            # Extract series letter (e.g., 'J' from 'PJ' or 'PD' from 'PD')
            series_letter = series[1:] if series.startswith('P') and len(series) > 1 else series

            # Standard variants
            variants.append(f"{base}-PR{series_letter}")   # C-PRJ
            variants.append(f"{base}-P-{series_letter}")   # C-P-J
            variants.append(f"{base}-P{series_letter}")    # C-PJ (redundant but safe)
            variants.append(f"{base}.PR{series_letter}")   # C.PRJ
            variants.append(f"{base}.P{series_letter}")    # C.PJ
            variants.append(f"{base}p{series_letter}")     # CpJ
            variants.append(f"{base}pr{series_letter}")    # CprJ
            variants.append(f"{base}-{series_letter}")     # C-J
            variants.append(f"{base}.{series_letter}")     # C.J
            variants.append(f"{base}p{series_letter.lower()}")  # Cpj

            # Special case for some brokers: C.PR.J
            variants.append(f"{base}.PR.{series_letter}")

    # Remove duplicates while preserving order
    seen = set()
    return [x for x in variants if not (x in seen or seen.add(x))]


def _get_snapshot_data(ticker: str) -> Optional[dict]:
    """Retrieve market data from local snapshot if available."""
    try:
        # Resolve path relative to project root
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        snapshot_path = os.path.join(base_dir, "data", "market_snapshots.json")
        
        if os.path.exists(snapshot_path):
            with open(snapshot_path, "r") as f:
                data = json.load(f)
                market_data = data.get("market_data", {})
                if ticker in market_data:
                    snapshot = market_data[ticker]
                    # Format to match get_preferred_info output
                    return {
                        "ticker": ticker,
                        "yahoo_ticker": f"{ticker} (Snapshot)",
                        "name": snapshot.get("name", "Unknown"),
                        "price": snapshot.get("price"),
                        "dividend_rate": snapshot.get("dividend_rate"),
                        "dividend_yield": snapshot.get("dividend_yield"),
                        "currency": snapshot.get("currency", "USD"),
                        "is_snapshot": True,
                        "as_of": snapshot.get("as_of")
                    }
    except Exception:
        pass
    return None


def get_preferred_info(ticker: str) -> dict:
    """
    Fetch basic information about a preferred stock from Yahoo Finance.
    Attempts multiple ticker variants to handle different platform conventions.

    If yfinance does not return dividendRate or dividendYield, this function
    computes them from the trailing 12-month dividend history.
    """
    variants = get_ticker_variants(ticker)
    last_error = None

    for variant in variants:
        try:
            stock = yf.Ticker(variant)
            info = stock.info

            # Check if we got meaningful data (some variants return empty dicts)
            if not info or ("regularMarketPrice" not in info and "previousClose" not in info):
                continue

            price = info.get("regularMarketPrice", info.get("previousClose", None))
            dividend_rate = info.get("dividendRate", None)
            dividend_yield = info.get("dividendYield", None)

            # ---------------------------------------------------------
            # Fallback: compute from dividend history if top-level is null
            # ---------------------------------------------------------
            if (dividend_rate is None or dividend_yield is None) and price is not None and price > 0:
                try:
                    divs = stock.dividends
                    if divs is not None and not divs.empty:
                        computed_rate, computed_yield = _compute_trailing_dividend(
                            divs, price
                        )
                        if dividend_rate is None:
                            dividend_rate = computed_rate
                        if dividend_yield is None:
                            dividend_yield = computed_yield
                except Exception:
                    pass  # non-critical; proceed with nulls

            result = {
                "ticker": ticker,
                "yahoo_ticker": variant,
                "name": info.get("longName", info.get("shortName", "Unknown")),
                "price": price,
                "dividend_rate": dividend_rate,
                "dividend_yield": dividend_yield,
                "fifty_two_week_high": info.get("fiftyTwoWeekHigh", None),
                "fifty_two_week_low": info.get("fiftyTwoWeekLow", None),
                "volume": info.get("averageVolume", None),
                "sector": info.get("sector", None),
                "industry": info.get("industry", None),
                "currency": info.get("currency", "USD"),
                "is_snapshot": False
            }
            return result
        except Exception as e:
            last_error = str(e)
            continue

    # ---------------------------------------------------------
    # Final Fallback: Check local snapshots if live fetch failed
    # ---------------------------------------------------------
    snapshot = _get_snapshot_data(ticker)
    if snapshot:
        return snapshot

    return {"ticker": ticker, "error": last_error or "No data found for ticker or variants"}


def _compute_trailing_dividend(
    dividends: pd.Series, price: float
) -> tuple:
    """Compute trailing 12-month dividend rate and yield from payment history.

    Uses the most recent 12 months of payments. If fewer than 12 months of
    history exist, annualizes based on the observed payment frequency.

    Returns (annual_rate, yield_as_decimal) or (None, None) on failure.
    """
    if dividends.empty or price <= 0:
        return None, None

    # Make timezone-naive for comparison
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
        # Less than a year of data: annualize from the last 4 payments
        # (most preferreds pay quarterly)
        last_payments = dividends.tail(4)
        n_payments = len(last_payments)

        # Estimate frequency from spacing
        dates = last_payments.index
        if len(dates) >= 2:
            try:
                avg_gap_days = (
                    (dates[-1] - dates[0]).days / (len(dates) - 1)
                )
            except Exception:
                avg_gap_days = 91  # default to quarterly

            if avg_gap_days < 45:
                periods_per_year = 12  # monthly
            elif avg_gap_days < 120:
                periods_per_year = 4   # quarterly
            elif avg_gap_days < 210:
                periods_per_year = 2   # semi-annual
            else:
                periods_per_year = 1   # annual

            avg_payment = float(last_payments.mean())
            annual_rate = round(avg_payment * periods_per_year, 4)
        else:
            return None, None
    else:
        return None, None

    annual_yield = round(annual_rate / price, 6)  # as decimal (e.g. 0.0676)
    return annual_rate, annual_yield


def get_price_history(ticker: str, period: str = "1y") -> Optional[pd.DataFrame]:
    """
    Fetch historical price data for a preferred stock.
    Attempts multiple ticker variants.
    """
    variants = get_ticker_variants(ticker)
    for variant in variants:
        try:
            stock = yf.Ticker(variant)
            hist = stock.history(period=period)
            if not hist.empty:
                return hist
        except Exception:
            continue
    return None


def get_dividend_history(ticker: str) -> Optional[pd.DataFrame]:
    """
    Fetch dividend payment history for a preferred stock.
    Attempts multiple ticker variants to find the one with dividend data.
    """
    variants = get_ticker_variants(ticker)
    for variant in variants:
        try:
            stock = yf.Ticker(variant)
            dividends = stock.dividends
            if not dividends.empty:
                return dividends.to_frame(name="dividend")
        except Exception:
            continue
    return None
