"""
Regression tests for config loading with and without Streamlit secrets.
Run with: python3 tests/test_config_streamlit_secrets.py
"""

import importlib
import os
import sys
import types

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

MODULE_NAME = "src.utils.config"
ENV_KEYS = (
    "GOOGLE_API_KEY",
    "OPENAI_API_KEY",
    "FRED_API_KEY",
    "SEC_USER_AGENT",
    "ALPHA_VANTAGE_API_KEY",
    "ALPHA_VANTAGE_LISTING_STATUS_PATH",
    "MARKET_DATA_PROVIDER",
)


def _reload_config_with_fake_streamlit(fake_streamlit):
    original_streamlit = sys.modules.get("streamlit")
    original_config = sys.modules.pop(MODULE_NAME, None)
    sys.modules.pop(MODULE_NAME, None)

    if fake_streamlit is None:
        sys.modules.pop("streamlit", None)
    else:
        sys.modules["streamlit"] = fake_streamlit

    try:
        return importlib.import_module(MODULE_NAME)
    finally:
        sys.modules.pop(MODULE_NAME, None)
        if original_config is not None:
            sys.modules[MODULE_NAME] = original_config
        if original_streamlit is not None:
            sys.modules["streamlit"] = original_streamlit
        else:
            sys.modules.pop("streamlit", None)


def _snapshot_env():
    return {key: os.environ.get(key) for key in ENV_KEYS}


def _restore_env(snapshot):
    for key, value in snapshot.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def test_missing_streamlit_secrets_do_not_break_local_import():
    class FakeMissingSecretsError(Exception):
        pass

    class FakeSecrets:
        def __contains__(self, key):
            raise FakeMissingSecretsError("No secrets file")

    fake_streamlit = types.SimpleNamespace(
        secrets=FakeSecrets(),
        errors=types.SimpleNamespace(
            StreamlitSecretNotFoundError=FakeMissingSecretsError
        ),
    )

    module = _reload_config_with_fake_streamlit(fake_streamlit)
    assert hasattr(module, "get_market_data_provider")


def test_streamlit_secrets_are_injected_when_available():
    snapshot = _snapshot_env()
    try:
        os.environ["ALPHA_VANTAGE_API_KEY"] = "env-key"
        os.environ["MARKET_DATA_PROVIDER"] = "alpha_vantage"

        fake_streamlit = types.SimpleNamespace(
            secrets={
                "ALPHA_VANTAGE_API_KEY": "streamlit-key",
                "MARKET_DATA_PROVIDER": "alpha_vantage",
            },
            errors=types.SimpleNamespace(
                StreamlitSecretNotFoundError=RuntimeError
            ),
        )

        module = _reload_config_with_fake_streamlit(fake_streamlit)
        assert os.environ["ALPHA_VANTAGE_API_KEY"] == "streamlit-key"
        assert module.get_market_data_provider() == "alpha_vantage"
    finally:
        _restore_env(snapshot)


if __name__ == "__main__":
    test_missing_streamlit_secrets_do_not_break_local_import()
    test_streamlit_secrets_are_injected_when_available()
    print("Config Streamlit secrets regression tests passed.")
