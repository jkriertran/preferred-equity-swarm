"""
Interest rate data fetcher for the Rate Sensitivity Agent.

Uses FRED API for Treasury yields and SOFR rates when a FRED_API_KEY is
configured.  Falls back to yfinance-based proxies when no key is available.

The yfinance fallback now includes:
  - A 3M Treasury proxy (via SGOV, the iShares 0-3 Month Treasury Bond ETF)
    so that the benchmark resolution layer always has a short-rate anchor.
  - A SOFR proxy derived from the same short-duration ETF, because overnight
    SOFR and 1-3 month T-bill yields track each other closely.
"""

import pandas as pd
import yfinance as yf
from typing import Optional
from src.utils.config import FRED_API_KEY


# ---------------------------------------------------------------------------
# Treasury yield curve
# ---------------------------------------------------------------------------

def get_treasury_yields_from_yfinance() -> dict:
    """
    Fetch approximate Treasury yields using Treasury ETF proxies from Yahoo Finance.
    This is a fallback when no FRED API key is available.

    Returns:
        Dictionary with maturity labels and approximate yield values (in percent).
    """
    etf_proxies = {
        "1M": "BIL",     # SPDR Bloomberg 1-3 Month T-Bill ETF
        "3M": "SGOV",    # iShares 0-3 Month Treasury Bond ETF
        "2Y": "SHY",     # iShares 1-3 Year Treasury Bond ETF
        "5Y": "IEI",     # iShares 3-7 Year Treasury Bond ETF
        "10Y": "IEF",    # iShares 7-10 Year Treasury Bond ETF
        "20Y": "TLT",    # iShares 20+ Year Treasury Bond ETF
    }

    yields = {}
    for maturity, etf_ticker in etf_proxies.items():
        try:
            etf = yf.Ticker(etf_ticker)
            info = etf.info
            div_yield = info.get("yield", info.get("dividendYield", None))
            if div_yield:
                yields[maturity] = round(div_yield * 100, 2)
        except Exception:
            pass

    return yields


def get_treasury_yields_from_fred() -> dict:
    """
    Fetch current Treasury yields from FRED API.

    Returns:
        Dictionary with maturity labels and yield values (in percent).
    """
    if not FRED_API_KEY:
        return get_treasury_yields_from_yfinance()

    try:
        from fredapi import Fred
        fred = Fred(api_key=FRED_API_KEY)

        series_map = {
            "1M": "DGS1MO",
            "3M": "DGS3MO",
            "6M": "DGS6MO",
            "1Y": "DGS1",
            "2Y": "DGS2",
            "3Y": "DGS3",
            "5Y": "DGS5",
            "7Y": "DGS7",
            "10Y": "DGS10",
            "20Y": "DGS20",
            "30Y": "DGS30",
        }

        yields = {}
        for maturity, series_id in series_map.items():
            try:
                data = fred.get_series(series_id)
                latest = data.dropna().iloc[-1]
                yields[maturity] = round(float(latest), 2)
            except Exception:
                pass

        return yields
    except Exception as e:
        print(f"FRED API error, falling back to yfinance: {e}")
        return get_treasury_yields_from_yfinance()


# ---------------------------------------------------------------------------
# SOFR
# ---------------------------------------------------------------------------

def get_sofr_rate() -> Optional[float]:
    """
    Fetch the current SOFR rate.

    Strategy:
      1. If a FRED API key is configured, pull the official daily SOFR series.
      2. Otherwise, approximate SOFR using the yield of a very short-duration
         Treasury ETF (SGOV or BIL).  Overnight SOFR and 1-3 month T-bill
         yields are highly correlated, so this is a reasonable proxy for
         analytical purposes when FRED access is unavailable.

    Returns:
        Current SOFR rate as a percentage (e.g. 5.31), or None if unavailable.
    """
    # Tier 1: FRED official SOFR
    if FRED_API_KEY:
        try:
            from fredapi import Fred
            fred = Fred(api_key=FRED_API_KEY)
            data = fred.get_series("SOFR")
            return round(float(data.dropna().iloc[-1]), 2)
        except Exception:
            pass  # fall through to yfinance proxy

    # Tier 2: yfinance short-duration ETF proxy
    for etf_ticker in ("SGOV", "BIL"):
        try:
            etf = yf.Ticker(etf_ticker)
            info = etf.info
            div_yield = info.get("yield", info.get("dividendYield", None))
            if div_yield and div_yield > 0:
                return round(div_yield * 100, 2)
        except Exception:
            pass

    return None
