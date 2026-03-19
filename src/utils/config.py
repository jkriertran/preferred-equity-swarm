"""
Configuration module for the Preferred Equity Analysis Swarm.
Loads environment variables and provides shared settings.

LLM priority:
  1. Google Gemini via GOOGLE_API_KEY (primary)
  2. OpenAI-compatible API via OPENAI_API_KEY (fallback)

This module handles both local .env files and Streamlit Cloud secrets.
"""

import os
import sys
from dotenv import load_dotenv

# 1. Load from .env if present (local development)
load_dotenv()

def _inject_streamlit_secrets_into_env() -> None:
    """Load Streamlit secrets into the environment when available.

    Local `streamlit run` sessions often do not have a `secrets.toml` file. In
    that case Streamlit raises `StreamlitSecretNotFoundError` the first time
    `st.secrets` is probed, so we treat missing secrets as a normal local-dev
    condition and leave `.env` values in place.
    """
    st = sys.modules.get("streamlit")
    if st is None or not hasattr(st, "secrets"):
        return

    secret_not_found_error = getattr(
        getattr(st, "errors", None),
        "StreamlitSecretNotFoundError",
        None,
    )

    try:
        secrets = st.secrets
        for key in [
            "GOOGLE_API_KEY",
            "OPENAI_API_KEY",
            "FRED_API_KEY",
            "SEC_USER_AGENT",
            "ALPHA_VANTAGE_API_KEY",
            "ALPHA_VANTAGE_LISTING_STATUS_PATH",
            "MARKET_DATA_PROVIDER",
        ]:
            if key in secrets:
                os.environ[key] = str(secrets[key])
    except Exception as exc:
        if secret_not_found_error and isinstance(exc, secret_not_found_error):
            return
        raise


# 2. Check for Streamlit secrets (deployment)
_inject_streamlit_secrets_into_env()

# LLM Configuration
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "")  # leave empty to use default

# SEC EDGAR Configuration
SEC_USER_AGENT = os.getenv("SEC_USER_AGENT", "PreferredEquitySwarm research@example.com")

# FRED Configuration
FRED_API_KEY = os.getenv("FRED_API_KEY", "")

# Market data provider configuration
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "")
ALPHA_VANTAGE_LISTING_STATUS_PATH = os.getenv("ALPHA_VANTAGE_LISTING_STATUS_PATH", "")
MARKET_DATA_PROVIDER = os.getenv("MARKET_DATA_PROVIDER", "alpha_vantage").strip().lower()

# Project paths
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, "processed")
UNIVERSE_DIR = os.path.join(DATA_DIR, "universe")


def get_market_data_provider() -> str:
    """Resolve the market-data provider for the current environment.

    Alpha Vantage is now the sole supported market-data provider. The
    environment variable is still honored for compatibility, but any empty,
    auto, or unknown value resolves to ``alpha_vantage``.
    """
    provider = os.getenv("MARKET_DATA_PROVIDER", MARKET_DATA_PROVIDER).strip().lower()
    if provider in ("", "auto", "alpha_vantage"):
        return "alpha_vantage"
    return "alpha_vantage"


def get_llm(temperature: float = 0.3):
    """
    Factory function that returns the configured LLM instance.

    Priority:
      1. If GOOGLE_API_KEY is set, uses ChatGoogleGenerativeAI (Gemini).
      2. If OPENAI_API_KEY is set, uses ChatOpenAI (OpenAI-compatible API).
      3. Raises ValueError if neither key is available.

    Args:
        temperature: Controls randomness in responses (0.0 to 1.0).
                     Lower values are more deterministic.

    Returns:
        A LangChain chat model instance.
    """
    # Refresh keys from environment in case they were injected after module load
    google_key = os.getenv("GOOGLE_API_KEY", GOOGLE_API_KEY)
    openai_key = os.getenv("OPENAI_API_KEY", OPENAI_API_KEY)

    # Primary: Google Gemini
    if google_key:
        from langchain_google_genai import ChatGoogleGenerativeAI

        return ChatGoogleGenerativeAI(
            model=GEMINI_MODEL,
            google_api_key=google_key,
            temperature=temperature,
        )

    # Fallback: OpenAI-compatible API
    if openai_key:
        from langchain_openai import ChatOpenAI

        kwargs = {
            "model": OPENAI_MODEL,
            "api_key": openai_key,
            "temperature": temperature,
        }
        if OPENAI_BASE_URL:
            kwargs["base_url"] = OPENAI_BASE_URL

        return ChatOpenAI(**kwargs)

    raise ValueError(
        "No LLM API key is configured. Set either GOOGLE_API_KEY or "
        "OPENAI_API_KEY in your .env file or Streamlit secrets.\n"
        "  Google: https://aistudio.google.com/apikey\n"
        "  OpenAI: https://platform.openai.com/api-keys"
    )
