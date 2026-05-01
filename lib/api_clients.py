"""Centralized API client and key loading.

Tries Streamlit secrets first (when running inside the app), then falls
back to environment variables (when running headless in the pipeline).
"""

import os
from openai import OpenAI

from lib.constants import DEFAULT_HTTP_USER_AGENT


def _try_streamlit_secret(key):
    """Return the value of a Streamlit secret, or None if Streamlit isn't available."""
    try:
        import streamlit as st
        return st.secrets[key]
    except Exception:
        return None


def get_secret(key, default=None):
    """Read a secret from Streamlit secrets or environment variables.

    Order:
      1. Streamlit secrets (when running inside the Streamlit app)
      2. OS environment variable
      3. The provided default
    """
    val = _try_streamlit_secret(key)
    if val is not None:
        return val
    val = os.environ.get(key)
    if val is not None:
        return val
    return default


def load_api_keys():
    """Load all API keys + user agent. Returns a dict.

    Raises RuntimeError if a required key is missing.
    """
    keys = {
        "APOLLO_API_KEY":    get_secret("APOLLO_API_KEY"),
        "OPENAI_API_KEY":    get_secret("OPENAI_API_KEY"),
        "FIRECRAWL_API_KEY": get_secret("FIRECRAWL_API_KEY"),
        "HTTP_USER_AGENT":   get_secret("HTTP_USER_AGENT", DEFAULT_HTTP_USER_AGENT),
    }
    missing = [k for k in ("APOLLO_API_KEY", "OPENAI_API_KEY", "FIRECRAWL_API_KEY")
               if not keys[k]]
    if missing:
        raise RuntimeError(
            f"Missing required API keys: {', '.join(missing)}. "
            "Set them in .streamlit/secrets.toml or as environment variables."
        )
    return keys


def make_openai_client(api_key=None, timeout=30.0):
    """Create a configured OpenAI client. Uses loaded keys if api_key is None."""
    if api_key is None:
        api_key = get_secret("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is not set.")
    return OpenAI(api_key=api_key, timeout=timeout)