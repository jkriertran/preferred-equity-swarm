"""
Interest rate data fetcher for the Rate Sensitivity Agent.

Uses the official FRED API for Treasury yields and SOFR rates when a
FRED_API_KEY is configured. Falls back to local snapshots when live access is
unavailable.
"""

import json
import os
from typing import Optional

from src.utils.config import FRED_API_KEY


def _get_snapshot_rates() -> Optional[dict]:
    """Retrieve treasury rates from the local snapshot file if available."""
    try:
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        snapshot_path = os.path.join(base_dir, "data", "market_snapshots.json")

        if os.path.exists(snapshot_path):
            with open(snapshot_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data.get("rates", {})
    except Exception:
        pass
    return None


def _snapshot_curve_as_percentages() -> dict:
    """Return locally cached Treasury yields in percentage form."""
    snapshot = _get_snapshot_rates()
    if not snapshot:
        return {}
    return {k: round(v * 100, 2) for k, v in snapshot.items() if k != "SOFR"}


def get_treasury_yields() -> dict:
    """
    Fetch current Treasury yields from FRED.

    Returns:
        Dictionary with maturity labels and yield values in percentage terms.
    """
    if not FRED_API_KEY:
        return _snapshot_curve_as_percentages()

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
    except Exception as exc:
        print(f"FRED API error, falling back to snapshots: {exc}")
        return _snapshot_curve_as_percentages()


def get_treasury_yields_from_fred() -> dict:
    """Backward-compatible alias for the provider-neutral Treasury helper."""
    return get_treasury_yields()


def get_sofr_rate() -> Optional[float]:
    """
    Fetch the current SOFR rate.

    Strategy:
      1. FRED daily SOFR series when a key is configured
      2. Local snapshot fallback when live access is unavailable
    """
    if FRED_API_KEY:
        try:
            from fredapi import Fred

            fred = Fred(api_key=FRED_API_KEY)
            data = fred.get_series("SOFR")
            return round(float(data.dropna().iloc[-1]), 2)
        except Exception:
            pass

    snapshot = _get_snapshot_rates()
    if snapshot and "SOFR" in snapshot:
        return round(snapshot["SOFR"] * 100, 2)

    return None
