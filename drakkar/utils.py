"""Shared utility functions for Drakkar framework."""

import re


def redact_url(url: str) -> str:
    """Redact credentials from URIs. Replaces user:pass@ with ***:***@."""
    return re.sub(r'://[^@/]+@', '://***:***@', url)
