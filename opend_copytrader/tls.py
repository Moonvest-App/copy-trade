from __future__ import annotations

import os
import ssl
from functools import lru_cache
from pathlib import Path


def bundled_ca_file() -> str | None:
    """Return the CA bundle shipped with the app, when available."""
    try:
        import certifi

        candidate = Path(certifi.where()).resolve()
    except (ImportError, OSError):
        return None
    return str(candidate) if candidate.is_file() else None


def configure_tls_environment() -> str | None:
    """Point Python HTTPS clients at the CA store bundled with the app."""
    ca_file = bundled_ca_file()
    if ca_file:
        os.environ["SSL_CERT_FILE"] = ca_file
        os.environ["REQUESTS_CA_BUNDLE"] = ca_file
    return ca_file


@lru_cache(maxsize=1)
def trusted_ssl_context() -> ssl.SSLContext:
    ca_file = configure_tls_environment()
    return ssl.create_default_context(cafile=ca_file) if ca_file else ssl.create_default_context()
