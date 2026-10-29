"""The base of every config model, and how config messages spell a raw YAML value."""

from pydantic import BaseModel, ConfigDict

# Above this many bits, str() may refuse an integer (Python's default limit is
# 4300 digits). A message quotes only the first digits, so the rest can go.
_MAX_PRINTED_INTEGER_BITS = 4096
_KEPT_INTEGER_DIGITS = 100
_DIGITS_PER_BIT = 0.30103


def yaml_text(value: object) -> str:
    """A raw value as a message quotes it, with YAML's spelling of ``true``, ``false`` and ``null``.

    An integer too long for ``str()`` gives its first digits only.
    """
    if isinstance(value, bool):
        return str(value).lower()
    if value is None:
        return 'null'
    if isinstance(value, int) and value.bit_length() > _MAX_PRINTED_INTEGER_BITS:
        dropped_digits = int(value.bit_length() * _DIGITS_PER_BIT) - _KEPT_INTEGER_DIGITS
        sign = '-' if value < 0 else ''
        return f'{sign}{abs(value) // 10**dropped_digits}'
    return str(value)


class ConfigModel(BaseModel):
    """A config section or block: a key it does not declare fails its construction.

    The load gives one message for every unknown key (:mod:`drakkar.config.strict`).
    This setting covers a model built directly in code, where a misspelt keyword
    would otherwise be dropped and the field would keep its default.
    """

    model_config = ConfigDict(extra='forbid')
