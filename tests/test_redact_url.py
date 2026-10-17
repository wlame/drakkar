"""Tests for redact_url's query-parameter redaction.

The user:pass@ authority cases live in tests/test_utils.py; these cover
DSNs that carry the secret in the query string instead — e.g.
``postgresql://host/db?password=x`` has no ``@`` for the authority rule
to catch.
"""

import pytest

from drakkar.utils import redact_url


@pytest.mark.parametrize(
    ('url', 'expected'),
    [
        (
            'postgresql://host/db?password=hunter2',
            'postgresql://host/db?password=***',
        ),
        (
            'postgresql://host/db?sslmode=require&password=hunter2',
            'postgresql://host/db?sslmode=require&password=***',
        ),
        (
            'postgresql://host/db?password=hunter2&sslmode=require',
            'postgresql://host/db?password=***&sslmode=require',
        ),
        # Substring match catches provider-specific variants.
        (
            'postgresql://host/db?sslpassword=hunter2',
            'postgresql://host/db?sslpassword=***',
        ),
        (
            'https://api.example.com/hook?api_token=abc123',
            'https://api.example.com/hook?api_token=***',
        ),
        (
            'mongodb://host/db?authSecret=abc',
            'mongodb://host/db?authSecret=***',
        ),
        # Both the authority credentials and the query secret are redacted.
        (
            'postgresql://wlame:hunter2@host/db?password=hunter2',
            'postgresql://***:***@host/db?password=***',
        ),
    ],
)
def test_redact_url_redacts_password_like_query_params(url, expected):
    assert redact_url(url) == expected


@pytest.mark.parametrize(
    'url',
    [
        'postgresql://host/db?sslmode=require&port=5432',
        'redis://localhost:6379/0',
        'https://example.com/path?page=2&limit=10',
    ],
)
def test_redact_url_leaves_benign_query_params_alone(url):
    assert redact_url(url) == url


@pytest.mark.parametrize(
    ('text', 'expected'),
    [
        # An error text that quotes the URL goes on after the value; the cause must stay readable.
        (
            'postgresql://h/db?password=x: connection refused',
            'postgresql://h/db?password=***: connection refused',
        ),
        ('redis://h/0?token=abc def', 'redis://h/0?token=*** def'),
        ('postgresql://h/db?password=x\nnext line', 'postgresql://h/db?password=***\nnext line'),
        # Only ASCII white space ends the value: a no-break space is redacted with it.
        ('redis://h/0?token=abc\u00a0def', 'redis://h/0?token=***'),
    ],
)
def test_redact_url_keeps_the_text_after_a_secret_value(text, expected):
    assert redact_url(text) == expected


def test_redact_url_still_redacts_a_colon_inside_a_secret_value():
    assert (
        redact_url('postgresql://h/db?password=pa:ss&sslmode=require')
        == 'postgresql://h/db?password=***&sslmode=require'
    )


def test_redact_url_empty_value_still_redacts_shape():
    # An empty secret value stays empty-shaped but must not break parsing.
    assert redact_url('postgresql://host/db?password=') == 'postgresql://host/db?password=***'
