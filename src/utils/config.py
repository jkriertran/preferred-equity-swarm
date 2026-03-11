"""
Configuration module for the Preferred Equity Analysis Swarm.
Loads environment variables and provides shared settings.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# LLM Configuration
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")
GEMINI_MODEL = "gemini-2.5-flash"

# SEC EDGAR Configuration
SEC_USER_AGENT = os.getenv("SEC_USER_AGENT", "PreferredEquitySwarm research@example.com")

# FRED Configuration
FRED_API_KEY = os.getenv("FRED_API_KEY", "")

# Project paths
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, "processed")
UNIVERSE_DIR = os.path.join(DATA_DIR, "universe")
