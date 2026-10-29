"""The base of every config model."""

from pydantic import BaseModel, ConfigDict


class ConfigModel(BaseModel):
    """A config section or block: a key it does not declare fails its construction.

    The load gives one message for every unknown key (:mod:`drakkar.config.strict`).
    This setting covers a model built directly in code, where a misspelt keyword
    would otherwise be dropped and the field would keep its default.
    """

    model_config = ConfigDict(extra='forbid')
