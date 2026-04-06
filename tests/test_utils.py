"""Tests for drakkar.utils module."""

from drakkar.utils import redact_url


def test_redact_url_with_credentials():
    assert redact_url('mongodb://admin:secret@host:27017/db') == 'mongodb://***:***@host:27017/db'


def test_redact_url_without_credentials():
    url = 'redis://localhost:6379/0'
    assert redact_url(url) == url


def test_redact_url_empty_string():
    assert redact_url('') == ''


def test_redact_url_plain_string_without_scheme():
    plain = 'localhost:9092'
    assert redact_url(plain) == plain
