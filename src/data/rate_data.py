"""
Interest rate data fetcher for the Rate Sensitivity Agent.
Uses FRED API for Treasury yields and SOFR rates.
Falls back to yfinance Treasury ETF data if no FRED API key is configured.
"""

import pandas as pd
import yfinance as yf
from typing import Optional
from src.utils.config import FRED_API_KEY


def get_treasury_yields_from_yfinance() -> dict:
    """
    Fetch approximate Treasury yields using Treasury ETF proxies from Yahoo Finance.
    This is a fallback when no FRED API key is available.
    
    Returns:
        Dictionary with maturity labels and approximate yield values
    """
    # Treasury ETF tickers as proxies for yield curve points
    etf_proxies = {
        "1M": "BIL",    # 1-3 month T-bills
        "2Y": "SHY",    # 1-3 year Treasury
        "5Y": "IEI",    # 3-7 year Treasury
        "10Y": "IEF",   # 7-10 year Treasury
        "20Y": "TLT",   # 20+ year Treasury
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
        Dictionary with maturity labels and yield values
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


def get_sofr_rate() -> Optional[float]:
    """
    Fetch the current SOFR rate from FRED.
    
    Returns:
        Current SOFR rate as a float, or None if unavailable
    """
    if not FRED_API_KEY:
        return None
    
    try:
        from fredapi import Fred
        fred = Fred(api_key=FRED_API_KEY)
        data = fred.get_series("SOFR")
        return round(float(data.dropna().iloc[-1]), 2)
    except Exception:
        return None
