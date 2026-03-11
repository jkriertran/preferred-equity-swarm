"""
Market data fetcher for preferred equity securities.
Uses yfinance for price data and basic security information.
"""

import yfinance as yf
import pandas as pd
from typing import Optional


def get_preferred_info(ticker: str) -> dict:
    """
    Fetch basic information about a preferred stock from Yahoo Finance.
    
    Args:
        ticker: The preferred stock ticker (e.g., 'BAC-PL' for Bank of America Series L)
    
    Returns:
        Dictionary with key preferred stock attributes
    """
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        result = {
            "ticker": ticker,
            "name": info.get("longName", info.get("shortName", "Unknown")),
            "price": info.get("regularMarketPrice", info.get("previousClose", None)),
            "dividend_rate": info.get("dividendRate", None),
            "dividend_yield": info.get("dividendYield", None),
            "fifty_two_week_high": info.get("fiftyTwoWeekHigh", None),
            "fifty_two_week_low": info.get("fiftyTwoWeekLow", None),
            "volume": info.get("averageVolume", None),
            "sector": info.get("sector", None),
            "industry": info.get("industry", None),
            "currency": info.get("currency", "USD"),
        }
        
        return result
    except Exception as e:
        return {"ticker": ticker, "error": str(e)}


def get_price_history(ticker: str, period: str = "1y") -> Optional[pd.DataFrame]:
    """
    Fetch historical price data for a preferred stock.
    
    Args:
        ticker: The preferred stock ticker
        period: Time period (e.g., '1y', '2y', '5y', 'max')
    
    Returns:
        DataFrame with OHLCV data, or None if fetch fails
    """
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period=period)
        return hist
    except Exception as e:
        print(f"Error fetching price history for {ticker}: {e}")
        return None


def get_dividend_history(ticker: str) -> Optional[pd.DataFrame]:
    """
    Fetch dividend payment history for a preferred stock.
    
    Args:
        ticker: The preferred stock ticker
    
    Returns:
        DataFrame with dividend dates and amounts, or None if fetch fails
    """
    try:
        stock = yf.Ticker(ticker)
        dividends = stock.dividends
        if dividends.empty:
            return None
        return dividends.to_frame(name="dividend")
    except Exception as e:
        print(f"Error fetching dividend history for {ticker}: {e}")
        return None
